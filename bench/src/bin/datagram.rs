//! Datagram throughput / pps benchmark for `noq::Connection`.
//!
//! Mirrors `bulk.rs` structure (server thread + client thread(s), each on its own
//! tokio runtime, self-signed cert, loopback) but exercises the unreliable datagram
//! path (`Connection::send_datagram` / `read_datagram`) instead of streams.
//!
//! Run e.g.:
//!   cargo run -p bench --bin datagram -- --packet-size 1200 --total-bytes 1G
//!   cargo run -p bench --bin datagram -- --direction both --send-mode wait --congestion bbr3

use std::{net::SocketAddr, sync::Arc, time::Instant};

use anyhow::{Context, Result};
use bench::stats::{DatagramCounters, DatagramReport};
use bench::{
    DatagramOpt, Direction, SendMode, configure_tracing_subscriber, connect_client, rt,
    server_endpoint,
};
use clap::Parser;
use noq::{Connection, ConnectionError, Endpoint, VarInt};
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use tracing::{info, trace};

/// Application close code used by the sender to signal "flood complete" to the
/// receiver's `read_datagram` loop.
const DONE_CODE: u32 = 0x444f4e45; // "DONE"

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
    let mut first_id: Option<usize> = None;
    for (id, handle) in handles.into_iter().enumerate() {
        if let Ok(report) = handle.join().expect("client thread") {
            report.print(&format!("client {id}"));
            aggregate.merge(&report.counters);
            if first_id.is_none() {
                first_id = Some(id);
            }
        }
    }

    if opt.clients > 1 {
        let report = build_report("send", opt, aggregate);
        report.print("aggregate");
    }

    // Let the server finish draining its connections.
    let _ = server_thread.join();
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
                let dir = match opt.direction {
                    Direction::Send => "send",
                    Direction::Recv => "recv",
                    Direction::Both => "both",
                };
                let report = build_report(dir, opt, counters);
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

    let dir = match opt.direction {
        Direction::Send => "send",
        Direction::Recv => "recv",
        Direction::Both => "both",
    };
    Ok(build_report(dir, opt, counters))
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
        // Half-duplex sender: flood then close. No receive.
        let conn = Arc::new(conn);
        let stats = send_loop(&conn, opt).await?;
        conn.close(VarInt::from_u32(DONE_CODE), b"done");
        if opt.stats {
            print_conn_stats("sender", &conn);
        }
        Ok(stats)
    } else {
        // Half-duplex receiver: drain until the peer closes with DONE_CODE.
        let stats = recv_loop(&conn).await?;
        if opt.stats {
            print_conn_stats("receiver", &conn);
        }
        Ok(stats)
    }
}

/// Full-duplex. Both sides flood concurrently. A small reliable bidi stream is used to
/// coordinate completion: each side writes a "done" byte after finishing its send, and
/// only closes the connection once it has read the peer's "done" byte (i.e. the peer has
/// finished sending). This prevents one side's close from truncating the other's
/// still-in-flight datagrams. The receiver drains until it sees the close.
async fn run_both(conn: Connection, opt: DatagramOpt, role: Role) -> Result<DatagramCounters> {
    let conn = Arc::new(conn);

    // Client opens the coordination stream, server accepts it.
    let (mut coord_s, mut coord_r) = match role {
        Role::Client => conn.open_bi().await.context("open coord stream")?,
        Role::Server => conn.accept_bi().await.context("accept coord stream")?,
    };

    let conn_for_send = conn.clone();
    let send_task = tokio::spawn(async move {
        let stats = send_loop(&conn_for_send, opt).await?;
        // Signal "done sending" on the reliable stream.
        let _ = coord_s.write_all(b"D").await;
        let _ = coord_s.finish();
        Ok::<_, anyhow::Error>(stats)
    });

    let conn_for_recv = conn.clone();
    let recv_task = tokio::spawn(async move { recv_loop(&conn_for_recv).await });

    // Wait until the peer has finished sending. The reliable stream byte arriving means
    // all earlier datagram packets have either been received or lost — i.e. the receiver
    // (running concurrently) has drained everything drainable from the peer.
    let mut done = [0u8; 1];
    let _ = coord_r.read_exact(&mut done).await;

    // Wait for OUR send to finish before closing, so we never truncate our own
    // in-flight datagrams.
    let send_stats = send_task.await??;
    // Our send is done and the peer has signaled its send is done. Close cleanly; the
    // peer's recv_loop (draining our datagrams) will exit on this close. Our own
    // recv_loop exits on LocallyClosed (handled above).
    conn.close(VarInt::from_u32(DONE_CODE), b"done");
    let recv_stats = recv_task.await??;

    if opt.stats {
        print_conn_stats("both", &conn);
    }

    let mut c = DatagramCounters::default();
    c.merge(&send_stats);
    c.merge(&recv_stats);
    Ok(c)
}

/// Flood `total_bytes` worth of datagrams. Does NOT close the connection — the caller is
/// responsible for closing (half-duplex) or coordinating close via the coord stream
/// (full-duplex).
async fn send_loop(conn: &Connection, opt: DatagramOpt) -> Result<DatagramCounters> {
    let max_size = conn
        .max_datagram_size()
        .context("datagrams unsupported or disabled")?;
    let pkt_size = opt.packet_size.min(max_size);
    if pkt_size == 0 {
        anyhow::bail!("packet_size resolves to 0");
    }
    // Pre-build the payload. `Bytes::from_static` would require a static buffer; use a
    // heap-allocated `Bytes` so we can size it to the (runtime) `pkt_size`.
    let payload = bytes::Bytes::from(vec![0xABu8; pkt_size]);

    let start = Instant::now();
    let mut sent_bytes = 0u64;
    let mut sent_packets = 0u64;
    while sent_bytes < opt.total_bytes {
        let pkt = payload.clone();
        match opt.send_mode {
            SendMode::Drop => match conn.send_datagram(pkt) {
                Ok(()) => {}
                Err(noq::SendDatagramError::ConnectionLost(e)) => {
                    return Err(anyhow::anyhow!("send_datagram connection lost: {e}"));
                }
                Err(e) => {
                    return Err(anyhow::anyhow!("send_datagram failed: {e}"));
                }
            },
            SendMode::Wait => {
                conn.send_datagram_wait(pkt)
                    .await
                    .map_err(|e| anyhow::anyhow!("send_datagram_wait failed: {e}"))?;
            }
        }
        sent_bytes += pkt_size as u64;
        sent_packets += 1;
    }
    let send_elapsed = start.elapsed();

    Ok(DatagramCounters {
        sent_bytes,
        sent_packets,
        send_elapsed,
        ..Default::default()
    })
}

/// Drain datagrams until the peer closes the connection (with DONE_CODE or otherwise).
async fn recv_loop(conn: &Connection) -> Result<DatagramCounters> {
    let start = Instant::now();
    let mut recv_bytes = 0u64;
    let mut recv_packets = 0u64;
    loop {
        match conn.read_datagram().await {
            Ok(d) => {
                recv_bytes += d.len() as u64;
                recv_packets += 1;
            }
            Err(ConnectionError::ApplicationClosed(ac))
                if ac.error_code == VarInt::from_u32(DONE_CODE) =>
            {
                trace!("sender signaled completion (DONE)");
                break;
            }
            Err(ConnectionError::ApplicationClosed(_)) => {
                info!("connection closed by peer");
                break;
            }
            // A locally-initiated close (our own `close()` call) also surfaces here as
            // `LocallyClosed`. Treat it as a clean exit: in full-duplex mode we close
            // after our send finishes, and any peer datagrams still in flight at that
            // point are dropped (unreliable datagrams) — negligible over the benchmark's
            // total volume.
            Err(ConnectionError::LocallyClosed) => {
                trace!("connection closed locally; stopping recv");
                break;
            }
            Err(e) => {
                return Err(anyhow::anyhow!("read_datagram failed: {e}"));
            }
        }
    }
    let recv_elapsed = start.elapsed();
    Ok(DatagramCounters {
        recv_bytes,
        recv_packets,
        recv_elapsed,
        ..Default::default()
    })
}

fn print_conn_stats(label: &str, conn: &Connection) {
    println!("\n{label} connection stats:\n{:#?}", conn.stats());
}
