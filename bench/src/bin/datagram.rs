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
    net::SocketAddr,
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

/// Application close code used to signal a clean end of the benchmark.
const DONE_CODE: u32 = 0x444f4e45; // "DONE"

/// In-band end-of-flood marker: a 1-byte datagram no flood packet can equal
/// (flood payloads are 0xAB-filled).
const DONE_DATAGRAM: [u8; 1] = [0xFF];

/// Pause between the end of the flood and each DONE marker (re)send. One
/// interval lets the datagram tail land before the first marker; the resends
/// cover marker loss.
const DONE_INTERVAL: Duration = Duration::from_millis(250);

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
        let report = build_report(opt, aggregate);
        report.print("aggregate");
    }

    // Let the server finish draining its connections.
    let _ = server_thread.join();
}

fn build_report(opt: DatagramOpt, counters: DatagramCounters) -> DatagramReport {
    DatagramReport {
        direction: opt.direction.to_string(),
        packet_size: opt.packet_size,
        send_mode: opt.send_mode.to_string(),
        congestion: opt.congestion.to_string(),
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
                let report = build_report(opt, counters);
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

    Ok(build_report(opt, counters))
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
        // Half-duplex sender: flood, then repeat the in-band DONE marker until the
        // receiver closes. The receiver drives the close so that no datagram it
        // could still count is cut off.
        let stats = send_loop(&conn, opt).await?;
        send_done_until_closed(&conn).await;
        if opt.stats {
            print_conn_stats("sender", &conn);
        }
        Ok(stats)
    } else {
        // Half-duplex receiver: count datagrams until the DONE marker, then close.
        let stats = recv_flood(&conn, opt).await?;
        conn.close(VarInt::from_u32(DONE_CODE), b"done");
        if opt.stats {
            print_conn_stats("receiver", &conn);
        }
        Ok(stats)
    }
}

/// Full-duplex. Both sides flood concurrently. End-of-flood travels in-band as a
/// DONE datagram; only the close is coordinated over a reliable bidi stream so
/// neither side's close cuts off datagrams the other is still counting:
///
/// 1. the client writes `S` right after opening the stream so the server's
///    `accept_bi` resolves immediately (a stream only reaches the peer once data
///    is written on it),
/// 2. each side floods, then repeats the DONE marker,
/// 3. each side writes `F` once it has received the peer's DONE (done receiving),
/// 4. each side closes only after reading the peer's `F` (peer got our DONE).
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

    let conn_for_recv = conn.clone();
    let recv_task = tokio::spawn(async move { recv_flood(&conn_for_recv, opt).await });

    let send_stats = send_loop(&conn, opt).await?;
    let conn_for_done = conn.clone();
    let done_task = tokio::spawn(async move { send_done_until_closed(&conn_for_done).await });

    let recv_stats = recv_task.await??;

    // Final close handshake. The peer may close immediately after writing its `F`,
    // racing our last read, so errors here are tolerated.
    let _ = coord_s.write_all(b"F").await;
    let _ = coord_r.read_exact(&mut byte).await;
    done_task.abort();
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
            wait_for_send_space(conn, batch_size * pkt_size).await;
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
                SendMode::Drop => {
                    wait_for_send_space(conn, pkt_size).await;
                    conn.send_datagram(pkt)
                        .map_err(|e| anyhow::anyhow!("send_datagram failed: {e}"))?
                }
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

/// Wait until the outgoing datagram buffer has room for `bytes`.
///
/// Paces the drop-mode flood to the driver's actual transmission rate. Without
/// this the send loop displaces its own queued datagrams (drop-oldest) faster
/// than they can hit the wire, and the benchmark measures queueing speed
/// instead of throughput.
async fn wait_for_send_space(conn: &Connection, bytes: usize) {
    while conn.datagram_send_buffer_space() < bytes {
        tokio::task::yield_now().await;
    }
}

/// Signal end-of-flood in-band: sleep one [`DONE_INTERVAL`] so the datagram tail
/// lands first, then send a [`DONE_DATAGRAM`] every interval until the peer
/// closes the connection (or, in duplex mode, this task is aborted).
async fn send_done_until_closed(conn: &Connection) {
    loop {
        tokio::select! {
            _ = conn.closed() => break,
            _ = tokio::time::sleep(DONE_INTERVAL) => {
                if conn.send_datagram(Bytes::from_static(&DONE_DATAGRAM)).is_err() {
                    break;
                }
            }
        }
    }
}

/// Count datagrams until the sender's in-band [`DONE_DATAGRAM`] marker arrives.
async fn recv_flood(conn: &Connection, opt: DatagramOpt) -> Result<DatagramCounters> {
    let mut out = vec![Bytes::new(); opt.batch_size.max(1)];
    let start = Instant::now();
    let mut last_recv = start;
    let mut recv_bytes = 0u64;
    let mut recv_packets = 0u64;
    'flood: loop {
        let n = recv_batch(conn, &mut out)
            .await
            .map_err(|e| anyhow::anyhow!("datagram read failed: {e}"))?;
        for d in &out[..n] {
            if d.as_ref() == DONE_DATAGRAM {
                break 'flood;
            }
            recv_bytes += d.len() as u64;
            recv_packets += 1;
            last_recv = Instant::now();
        }
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
