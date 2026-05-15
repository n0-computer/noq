//! Regression test: the connection sender must call
//! `Controller::on_packet_sent`.
//!
//! It didn't, which left BBRv3 (the only controller that needs per-packet
//! send accounting) with no data. Here we run a normal Cubic connection but
//! wrap the controller so we can count that one call. If the count is zero,
//! the bug is back.
//!
//! See `bbr3_throughput.rs` for the slower end-to-end version.
#![cfg(all(feature = "rustls", any(feature = "aws-lc-rs", feature = "ring")))]

use std::{
    any::Any,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};

use noq::{
    TransportConfig,
    congestion::{Controller, ControllerFactory, ControllerMetrics, CubicConfig},
};
use rustls::pki_types::{CertificateDer, PrivatePkcs8KeyDer};
use tokio::runtime::Builder;

/// Counts `on_packet_sent` calls; every other hook forwards to a real
/// controller so connection behaviour is identical
#[derive(Debug)]
struct SpyController {
    inner: Box<dyn Controller>,
    calls: Arc<AtomicU64>,
}

impl Controller for SpyController {
    fn on_packet_sent(&mut self, now: Instant, bytes: u16, pn: u64) {
        self.calls.fetch_add(1, Ordering::Relaxed);
        self.inner.on_packet_sent(now, bytes, pn);
    }

    fn on_sent(&mut self, now: Instant, bytes: u64, largest_pn: u64) {
        self.inner.on_sent(now, bytes, largest_pn);
    }
    fn on_congestion_event(
        &mut self,
        now: Instant,
        sent: Instant,
        persistent: bool,
        ecn: bool,
        lost_bytes: u64,
        largest_lost_pn: u64,
    ) {
        self.inner
            .on_congestion_event(now, sent, persistent, ecn, lost_bytes, largest_lost_pn);
    }
    fn on_packet_lost(&mut self, lost_bytes: u16, pn: u64, now: Instant) {
        self.inner.on_packet_lost(lost_bytes, pn, now);
    }
    fn on_spurious_congestion_event(&mut self) {
        self.inner.on_spurious_congestion_event();
    }
    fn on_mtu_update(&mut self, new_mtu: u16) {
        self.inner.on_mtu_update(new_mtu);
    }
    fn window(&self) -> u64 {
        self.inner.window()
    }
    fn metrics(&self) -> ControllerMetrics {
        self.inner.metrics()
    }
    fn initial_window(&self) -> u64 {
        self.inner.initial_window()
    }
    fn clone_box(&self) -> Box<dyn Controller> {
        Box::new(Self {
            inner: self.inner.clone_box(),
            calls: self.calls.clone(),
        })
    }
    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

struct SpyFactory {
    inner: Arc<dyn ControllerFactory + Send + Sync>,
    calls: Arc<AtomicU64>,
}

impl ControllerFactory for SpyFactory {
    fn build(self: Arc<Self>, now: Instant, mtu: u16) -> Box<dyn Controller> {
        Box::new(SpyController {
            inner: self.inner.clone().build(now, mtu),
            calls: self.calls.clone(),
        })
    }
}

fn gen_cert() -> (CertificateDer<'static>, PrivatePkcs8KeyDer<'static>) {
    let cert = rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).unwrap();
    (
        cert.cert.into(),
        PrivatePkcs8KeyDer::from(cert.signing_key.serialize_der()),
    )
}

#[test]
fn connection_sender_calls_on_packet_sent() {
    let calls = Arc::new(AtomicU64::new(0));

    let runtime = Builder::new_current_thread().enable_all().build().unwrap();
    runtime.block_on(async {
        let (cert, key) = gen_cert();
        let local = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0);

        let server_cfg =
            noq::ServerConfig::with_single_cert(vec![cert.clone()], key.into()).unwrap();
        let server = noq::Endpoint::server(server_cfg, local).unwrap();
        let server_addr = server.local_addr().unwrap();

        let server_task = tokio::spawn(async move {
            let conn = server.accept().await.unwrap().await.unwrap();
            let mut stream = conn.accept_uni().await.unwrap();
            stream.read_to_end(64 * 1024).await.unwrap();
        });

        // Client congestion control = Cubic wrapped in the spy.
        let mut transport = TransportConfig::default();
        transport.congestion_controller_factory(Arc::new(SpyFactory {
            inner: Arc::new(CubicConfig::default()),
            calls: calls.clone(),
        }));

        let mut roots = rustls::RootCertStore::empty();
        roots.add(cert).unwrap();
        let mut client_cfg = noq::ClientConfig::with_root_certificates(Arc::new(roots)).unwrap();
        client_cfg.transport_config(Arc::new(transport));

        let client = noq::Endpoint::client(local).unwrap();
        let conn = client
            .connect_with(client_cfg, server_addr, "localhost")
            .unwrap()
            .await
            .unwrap();

        // A small transfer fits the initial congestion window — no ack-driven
        // growth needed for the sender to emit (and account for) packets.
        let mut send = conn.open_uni().await.unwrap();
        send.write_all(&[0u8; 4096]).await.unwrap();
        send.finish().unwrap();
        send.stopped().await.ok();

        server_task.await.unwrap();
        client.wait_idle().await;
    });

    let count = calls.load(Ordering::Relaxed);
    println!("Controller::on_packet_sent was called {count} times");
    assert!(
        count > 0,
        "connection sender never called Controller::on_packet_sent — \
         per-packet send accounting is unwired (BBRv3 would be starved)"
    );
}
