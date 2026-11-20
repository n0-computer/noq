//! Options for running the perf client and server over AF_XDP sockets.

use std::{net::SocketAddr, time::Duration};

use anyhow::{Context, Result, bail};
use clap::{Parser, ValueEnum};
use noq::{
    AsyncUdpSocketNext, SocketRoute,
    xdp::{AttachMode, BindMode, MacAddr, XdpConfig, XdpInterface, XdpSocket},
};
use tracing::info;

/// How often the AF_XDP socket counters are printed
const STATS_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Parser)]
pub struct XdpOpt {
    /// Send and receive over AF_XDP on this interface instead of a UDP socket
    ///
    /// The local address (`--listen` or `--local-addr`) must be an IPv4 address of the
    /// interface.
    #[clap(long = "xdp")]
    pub interface: Option<String>,
    /// Number of receive queues to bind, one AF_XDP socket each, starting at queue 0
    #[clap(long = "xdp-queues", default_value = "1")]
    pub queues: u32,
    /// How to attach the XDP program
    #[clap(long = "xdp-attach", default_value = "auto")]
    pub attach: XdpAttach,
    /// Whether to use zero-copy
    #[clap(long = "xdp-bind", default_value = "auto")]
    pub bind: XdpBind,
    /// MAC address to send to when a destination's MAC is unknown
    ///
    /// Usually the peer's MAC on a point-to-point link, or the gateway's.
    #[clap(long = "xdp-gateway")]
    pub gateway: Option<MacAddr>,
    /// Maximum number of datagrams per transmit (emulated GSO)
    #[clap(long = "xdp-segments", default_value = "32")]
    pub segments: usize,
}

#[derive(Clone, Copy, ValueEnum)]
pub enum XdpAttach {
    Auto,
    Generic,
    Native,
}

#[derive(Clone, Copy, ValueEnum)]
pub enum XdpBind {
    Auto,
    Copy,
    Zerocopy,
}

impl XdpOpt {
    /// Attaches the XDP program and binds one socket per queue
    ///
    /// Spawns a task that prints the sockets' counters every few seconds.
    #[allow(clippy::type_complexity)]
    pub fn bind(
        &self,
        addr: SocketAddr,
    ) -> Result<Vec<(Box<dyn AsyncUdpSocketNext>, Vec<SocketRoute>)>> {
        let interface = self.interface.as_deref().context("no XDP interface")?;
        let SocketAddr::V4(local_addr) = addr else {
            bail!("AF_XDP needs an IPv4 local address, got {addr}");
        };
        if local_addr.ip().is_unspecified() || local_addr.port() == 0 {
            bail!("AF_XDP needs a concrete local address and port, got {addr}");
        }
        let mut config = XdpConfig::new(interface, local_addr)
            .attach_mode(match self.attach {
                XdpAttach::Auto => AttachMode::Auto,
                XdpAttach::Generic => AttachMode::Generic,
                XdpAttach::Native => AttachMode::Native,
            })
            .bind_mode(match self.bind {
                XdpBind::Auto => BindMode::Auto,
                XdpBind::Copy => BindMode::Copy,
                XdpBind::Zerocopy => BindMode::ZeroCopy,
            })
            .max_segments(self.segments);
        if let Some(gateway) = self.gateway {
            config = config.gateway(gateway);
        }
        let xdp = XdpInterface::attach(config)?;
        let sockets = (0..self.queues)
            .map(|queue| xdp.bind(queue))
            .collect::<std::io::Result<Vec<_>>>()?;
        info!(interface, queues = self.queues, mac = %xdp.mac(), "using AF_XDP");
        tokio::spawn(print_stats(sockets.clone()));
        // Every socket gets the default route, so the first one sends everything and all of
        // them receive.
        let route = SocketRoute::new("0.0.0.0/0".parse().expect("valid")).with_default(true);
        Ok(sockets
            .into_iter()
            .map(|socket| (Box::new(socket) as Box<dyn AsyncUdpSocketNext>, vec![route]))
            .collect())
    }
}

async fn print_stats(sockets: Vec<XdpSocket>) {
    let mut interval = tokio::time::interval(STATS_INTERVAL);
    interval.tick().await;
    loop {
        interval.tick().await;
        for socket in &sockets {
            eprintln!("AF_XDP queue {}: {:?}", socket.queue(), socket.stats());
        }
    }
}
