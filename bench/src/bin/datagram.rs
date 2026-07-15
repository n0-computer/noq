//! Datagram throughput / pps benchmark for `noq::Connection`.
//!
//! Mirrors `bulk.rs` structure (server thread + client thread(s), each on its own
//! tokio runtime, self-signed cert, loopback) but exercises the unreliable datagram
//! path instead of streams. `--batch-size` selects between the single-datagram APIs
//! (`send_datagram` / `read_datagram`) and the batch APIs (`send_many_datagrams` /
//! `read_many_datagrams`).
//!
//! Run e.g.:
//!   cargo run -p bench --bin datagram -- --packet-size 1200 --total-bytes 1G
//!   cargo run -p bench --bin datagram -- --direction both --send-mode wait --congestion bbr3
//!   cargo run -p bench --bin datagram -- --batch-size 32

use std::{
    future::Future,
    net::SocketAddr,
    pin::pin,
    sync::Arc,
    time::{Duration, Instant},
};

use anyhow::{Context, Result};
use bench::stats::{DatagramCounters, DatagramReport};
use bench::{
    DatagramOpt, Direction, SendMode, configure_tracing_subscriber, connect_client, rt,
    server_endpoint,
};
use bytes::Bytes;
use clap::Parser;
use noq::{Connection, ConnectionError, Endpoint, VarInt};
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use tokio::time::timeout;

/// Application close code used to signal a clean end of the benchmark.
const DONE_CODE: u32 = 0x444f4e45; // "DONE"

/// How long the receiver keeps draining after the sender's "done" marker arrives.
///
/// The marker travels on a reliable stream and can overtake datagrams still in
/// flight, so the tail is given this long to show up before the receiver stops
/// counting. `recv_elapsed` is measured up to the last datagram received, so the
/// grace period does not skew throughput numbers.
const DRAIN_GRACE: Duration = Duration::from_millis(250);

fn main() {
    let opt = DatagramOpt::parse();
    configure_tracing_subscriber();

    let cert = rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap();
    let key = PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der());
    let cert = CertificateDer::from(cert.cert);

    let server_span = tracing::error_span!("server");
    let runtime = rt(opt.runtime_type);
    let (server_addr, endpoint) = {
        let _guard = server_span.enter();
        server_endpoint(&runtime, cert.clone(), key.into(), &opt)
    };

    let opt_for_server = opt;
    let server_thread = std::thread::spawn(move || {
        let _guard = server_span.entered();
        if let Err(e) = runtime.block_on(server(endpoint, opt_for_server)) {
            eprintln!("server failed: {e:#}");
        }
    });

    let mut handles = Vec::new();
    for id in 0..opt.clients {
        let cert = cert.clone();
        handles.push(std::thread::spawn(move || {
            let _guard = tracing::error_span!("client", id).entered();
            let runtime = rt(opt.runtime_type);
            match runtime.block_on(client(server_addr, cert, opt, id)) {
                Ok(report) => Ok(report),
                Err(e) => {
                    eprintln!("client failed: {e:#}");
                    Err(e)
                }
            }
        }));
    }

    let mut aggregate = DatagramCounters::default();
    for (id, handle) in handles.into_iter().enumerate() {
        if let Ok(report) = handle.join().expect("client thread") {
            report.print(&format!("client {id}"));
            aggregate.merge(&report.counters);
        }
    }

    if opt.clients > 1 {
        let report = build_report(direction_str(opt.direction), opt, aggregate);
        report.print("aggregate");
    }

    // Let the server finish draining its connections.
    let _ = server_thread.join();
}

fn direction_str(direction: Direction) -> &'static str {
    match direction {
        Direction::Send => "send",
        Direction::Recv => "recv",
        Direction::Both => "both",
    }
}

fn build_report(direction: &str, opt: DatagramOpt, counters: DatagramCounters) -> DatagramReport {
    DatagramReport {
        direction: direction.to_string(),
        packet_size: opt.packet_size,
        send_mode: format!("{:?}", opt.send_mode).to_lowercase(),
        congestion: format!("{:?}", opt.congestion).to_lowercase(),
        counters,
    }
}

/// Server side: accepts `opt.clients` connections, and for each runs the side
/// appropriate to `opt.direction`.
async fn server(endpoint: Endpoint, opt: DatagramOpt) -> Result<()> {
    let mut tasks = Vec::new();
    for _ in 0..opt.clients {
        let handshake = endpoint.accept().await.context("accept failed")?;
        let conn = handshake.await.context("handshake failed")?;
        tasks.push(tokio::spawn(async move {
            run_side(conn, opt, Role::Server).await
        }));
    }
    for (i, t) in tasks.into_iter().enumerate() {
        match t.await {
            Ok(Ok(counters)) => {
                let report = build_report(direction_str(opt.direction), opt, counters);
                report.print(&format!("server {i}"));
            }
            Ok(Err(e)) => eprintln!("server task error: {e:?}"),
            Err(e) => eprintln!("server task panic: {e:?}"),
        }
    }
    Ok(())
}

/// Client side: connects, then runs the side appropriate to `opt.direction`.
async fn client(
    server_addr: SocketAddr,
    server_cert: CertificateDer<'static>,
    opt: DatagramOpt,
    _id: usize,
) -> Result<DatagramReport> {
    let (endpoint, conn) = connect_client(server_addr, server_cert, opt).await?;

    let counters = run_side(conn, opt, Role::Client).await?;

    // Allow the connection to finish draining.
    endpoint.wait_all_draining().await;

    Ok(build_report(direction_str(opt.direction), opt, counters))
}

#[derive(Clone, Copy)]
enum Role {
    Client,
    Server,
}

/// Run the side of the flood appropriate to `opt.direction` for the given role.
///
/// - `send`  : client is the sender, server is the receiver.
/// - `recv`  : server is the sender, client is the receiver.
/// - `both`  : both sides send and receive concurrently.
async fn run_side(conn: Connection, opt: DatagramOpt, role: Role) -> Result<DatagramCounters> {
    if matches!(opt.direction, Direction::Both) {
        return run_both(conn, opt, role).await;
    }
    let is_sender = matches!(
        (role, opt.direction),
        (Role::Client, Direction::Send) | (Role::Server, Direction::Recv)
    );
    if is_sender {
        // Half-duplex sender: flood, signal "done" on a reliable uni stream, then
        // wait for the receiver to drain and close. Closing from this side right
        // after the last send would discard datagrams still queued or in flight.
        let stats = send_loop(&conn, opt).await?;
        let mut done = conn.open_uni().await.context("open done stream")?;
        done.write_all(b"D").await.context("write done marker")?;
        let _ = done.finish();
        conn.closed().await;
        if opt.stats {
            print_conn_stats("sender", &conn);
        }
        Ok(stats)
    } else {
        // Half-duplex receiver: drain until the sender's done marker plus grace
        // period, then close. The receiver drives the close so that no datagram it
        // could still count is cut off.
        let done = async {
            let mut stream = conn.accept_uni().await.context("accept done stream")?;
            let mut buf = [0u8; 1];
            stream
                .read_exact(&mut buf)
                .await
                .context("read done marker")?;
            Ok(())
        };
        let stats = recv_flood(&conn, opt, done).await?;
        conn.close(VarInt::from_u32(DONE_CODE), b"done");
        if opt.stats {
            print_conn_stats("receiver", &conn);
        }
        Ok(stats)
    }
}

/// Full-duplex. Both sides flood concurrently, coordinating shutdown over a
/// reliable bidi stream so neither side's close cuts off datagrams the other is
/// still counting:
///
/// 1. the client writes `S` right after opening the stream so the server's
///    `accept_bi` resolves immediately (a stream only reaches the peer once data
///    is written on it),
/// 2. each side writes `D` when its send loop finishes,
/// 3. each side reads the peer's `D`, lets the grace period drain the tail, then
///    writes `F` to say it is done receiving,
/// 4. each side closes only after reading the peer's `F`.
async fn run_both(conn: Connection, opt: DatagramOpt, role: Role) -> Result<DatagramCounters> {
    let conn = Arc::new(conn);

    let (mut coord_s, mut coord_r) = match role {
        Role::Client => conn.open_bi().await.context("open coord stream")?,
        Role::Server => conn.accept_bi().await.context("accept coord stream")?,
    };
    let mut byte = [0u8; 1];
    match role {
        Role::Client => coord_s
            .write_all(b"S")
            .await
            .context("write start marker")?,
        Role::Server => coord_r
            .read_exact(&mut byte)
            .await
            .context("read start marker")?,
    }

    let (peer_done_tx, peer_done_rx) = tokio::sync::oneshot::channel::<()>();
    let conn_for_recv = conn.clone();
    let recv_task = tokio::spawn(async move {
        let done = async { peer_done_rx.await.context("peer done signal dropped") };
        recv_flood(&conn_for_recv, opt, done).await
    });

    let send_stats = send_loop(&conn, opt).await?;
    coord_s.write_all(b"D").await.context("write done marker")?;
    coord_r
        .read_exact(&mut byte)
        .await
        .context("read peer done marker")?;
    let _ = peer_done_tx.send(());
    let recv_stats = recv_task.await??;

    // Final close handshake. The peer may close immediately after writing its `F`,
    // racing our last read, so errors here are tolerated.
    let _ = coord_s.write_all(b"F").await;
    let _ = coord_r.read_exact(&mut byte).await;
    conn.close(VarInt::from_u32(DONE_CODE), b"done");

    if opt.stats {
        print_conn_stats("both", &conn);
    }

    let mut c = DatagramCounters::default();
    c.merge(&send_stats);
    c.merge(&recv_stats);
    Ok(c)
}

/// Flood `total_bytes` worth of datagrams. Does NOT close the connection or signal
/// completion; the caller coordinates shutdown.
async fn send_loop(conn: &Connection, opt: DatagramOpt) -> Result<DatagramCounters> {
    let max_size = conn
        .max_datagram_size()
        .context("datagrams unsupported or disabled")?;
    let pkt_size = opt.packet_size.min(max_size);
    if pkt_size == 0 {
        anyhow::bail!("packet_size resolves to 0");
    }
    let batch_size = opt.batch_size.max(1);
    if batch_size > 1 && opt.send_mode == SendMode::Wait {
        anyhow::bail!("--batch-size > 1 requires --send-mode drop");
    }
    let payload = Bytes::from(vec![0xABu8; pkt_size]);

    let start = Instant::now();
    let mut sent_bytes = 0u64;
    let mut sent_packets = 0u64;
    if batch_size > 1 {
        let batch = vec![payload; batch_size];
        while sent_bytes < opt.total_bytes {
            let queued = conn
                .send_many_datagrams(&batch)
                .map_err(|e| anyhow::anyhow!("send_many_datagrams failed: {e}"))?;
            sent_bytes += (queued * pkt_size) as u64;
            sent_packets += queued as u64;
        }
    } else {
        while sent_bytes < opt.total_bytes {
            let pkt = payload.clone();
            match opt.send_mode {
                SendMode::Drop => conn
                    .send_datagram(pkt)
                    .map_err(|e| anyhow::anyhow!("send_datagram failed: {e}"))?,
                SendMode::Wait => conn
                    .send_datagram_wait(pkt)
                    .await
                    .map_err(|e| anyhow::anyhow!("send_datagram_wait failed: {e}"))?,
            }
            sent_bytes += pkt_size as u64;
            sent_packets += 1;
        }
    }
    let send_elapsed = start.elapsed();

    Ok(DatagramCounters {
        sent_bytes,
        sent_packets,
        send_elapsed,
        ..Default::default()
    })
}

/// Drain datagrams until `done` resolves (the peer finished sending), then keep
/// draining until the connection goes [`DRAIN_GRACE`] without another datagram.
async fn recv_flood(
    conn: &Connection,
    opt: DatagramOpt,
    done: impl Future<Output = Result<()>>,
) -> Result<DatagramCounters> {
    let mut out = vec![Bytes::new(); opt.batch_size.max(1)];
    let mut done = pin!(done);
    let start = Instant::now();
    let mut last_recv = start;
    let mut recv_bytes = 0u64;
    let mut recv_packets = 0u64;
    loop {
        tokio::select! {
            r = recv_batch(conn, &mut out) => {
                let n = r.map_err(|e| anyhow::anyhow!("datagram read failed: {e}"))?;
                for d in &out[..n] {
                    recv_bytes += d.len() as u64;
                }
                recv_packets += n as u64;
                last_recv = Instant::now();
            }
            r = &mut done => {
                r?;
                break;
            }
        }
    }
    // The done marker can overtake datagrams still in flight; drain the tail until
    // the connection goes quiet.
    while let Ok(Ok(n)) = timeout(DRAIN_GRACE, recv_batch(conn, &mut out)).await {
        for d in &out[..n] {
            recv_bytes += d.len() as u64;
        }
        recv_packets += n as u64;
        last_recv = Instant::now();
    }
    Ok(DatagramCounters {
        recv_bytes,
        recv_packets,
        recv_elapsed: last_recv - start,
        ..Default::default()
    })
}

/// Receive one batch: `read_datagram` when `out` is a single slot, the batch API
/// otherwise.
async fn recv_batch(conn: &Connection, out: &mut [Bytes]) -> Result<usize, ConnectionError> {
    if out.len() == 1 {
        out[0] = conn.read_datagram().await?;
        Ok(1)
    } else {
        conn.read_many_datagrams(out).await
    }
}

fn print_conn_stats(label: &str, conn: &Connection) {
    println!("\n{label} connection stats:\n{:#?}", conn.stats())
}
