use core::str;
use std::{
    convert::TryInto,
    net::{IpAddr, Ipv6Addr, SocketAddr},
    num::ParseIntError,
    str::FromStr,
    sync::Arc,
};

use anyhow::{Context, Result};
use bytes::Bytes;
use clap::Parser;
use noq::crypto::rustls::QuicClientConfig;
use rustls::{
    RootCertStore,
    pki_types::{CertificateDer, PrivateKeyDer},
};
use tokio::runtime::{Builder, Runtime};
use tracing::trace;

pub mod stats;

pub fn configure_tracing_subscriber() {
    tracing::subscriber::set_global_default(
        tracing_subscriber::FmtSubscriber::builder()
            .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
            .finish(),
    )
    .unwrap();
}

/// Trait shared by the stream (`Opt`) and datagram (`DatagramOpt`) benchmark option
/// structs so the endpoint/connection helpers can be reused by both binaries.
pub trait BenchOpt: Copy {
    /// Desired cipher suite for the TLS handshake.
    fn cipher(&self) -> CipherSuite;
    /// Transport config to use for this benchmark run.
    fn transport_config(&self) -> noq::TransportConfig;
}

impl BenchOpt for Opt {
    fn cipher(&self) -> CipherSuite {
        self.cipher
    }
    fn transport_config(&self) -> noq::TransportConfig {
        transport_config(self)
    }
}

/// Creates a server endpoint which runs on the given runtime
pub fn server_endpoint<O: BenchOpt>(
    rt: &tokio::runtime::Runtime,
    cert: CertificateDer<'static>,
    key: PrivateKeyDer<'static>,
    opt: &O,
) -> (SocketAddr, noq::Endpoint) {
    let cert_chain = vec![cert];
    let mut server_config = noq::ServerConfig::with_single_cert(cert_chain, key).unwrap();
    server_config.transport = Arc::new(opt.transport_config());

    let endpoint = {
        let _guard = rt.enter();
        noq::Endpoint::server(
            server_config,
            SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 0),
        )
        .unwrap()
    };
    let server_addr = endpoint.local_addr().unwrap();
    (server_addr, endpoint)
}

/// Create a client endpoint and client connection
pub async fn connect_client<O: BenchOpt>(
    server_addr: SocketAddr,
    server_cert: CertificateDer<'_>,
    opt: O,
) -> Result<(noq::Endpoint, noq::Connection)> {
    let endpoint =
        noq::Endpoint::client(SocketAddr::new(IpAddr::V6(Ipv6Addr::LOCALHOST), 0)).unwrap();

    let mut roots = RootCertStore::empty();
    roots.add(server_cert)?;

    let default_provider = rustls::crypto::ring::default_provider();
    let provider = rustls::crypto::CryptoProvider {
        cipher_suites: vec![opt.cipher().as_rustls()],
        ..default_provider
    };

    let crypto = rustls::ClientConfig::builder_with_provider(provider.into())
        .with_protocol_versions(&[&rustls::version::TLS13])
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();

    let mut client_config = noq::ClientConfig::new(Arc::new(QuicClientConfig::try_from(crypto)?));
    client_config.transport_config(Arc::new(opt.transport_config()));

    let connection = endpoint
        .connect_with(client_config, server_addr, "localhost")
        .unwrap()
        .await
        .context("unable to connect")?;
    trace!("connected");

    Ok((endpoint, connection))
}

pub async fn drain_stream(mut stream: noq::RecvStream, read_unordered: bool) -> Result<usize> {
    let mut read = 0;

    if read_unordered {
        let mut stream = stream.into_unordered();
        while let Some(chunk) = stream.read_chunk(usize::MAX).await? {
            read += chunk.bytes.len();
        }
    } else {
        // These are 32 buffers, for reading approximately 32kB at once
        #[rustfmt::skip]
        let mut bufs = [
            Bytes::new(), Bytes::new(), Bytes::new(), Bytes::new(),
            Bytes::new(), Bytes::new(), Bytes::new(), Bytes::new(),
            Bytes::new(), Bytes::new(), Bytes::new(), Bytes::new(),
            Bytes::new(), Bytes::new(), Bytes::new(), Bytes::new(),
            Bytes::new(), Bytes::new(), Bytes::new(), Bytes::new(),
            Bytes::new(), Bytes::new(), Bytes::new(), Bytes::new(),
            Bytes::new(), Bytes::new(), Bytes::new(), Bytes::new(),
            Bytes::new(), Bytes::new(), Bytes::new(), Bytes::new(),
        ];

        while let Some(n) = stream.read_many_chunks(&mut bufs[..]).await? {
            read += bufs.iter().take(n).map(|buf| buf.len()).sum::<usize>();
        }
    }

    Ok(read)
}

pub async fn send_data_on_stream(stream: &mut noq::SendStream, stream_size: u64) -> Result<()> {
    const DATA: &[u8] = &[0xAB; 1024 * 1024];
    let bytes_data = Bytes::from_static(DATA);

    let full_chunks = stream_size / (DATA.len() as u64);
    let remaining = (stream_size % (DATA.len() as u64)) as usize;

    for _ in 0..full_chunks {
        stream
            .write_chunk(bytes_data.clone())
            .await
            .context("failed sending data")?;
    }

    if remaining != 0 {
        stream
            .write_chunk(bytes_data.slice(0..remaining))
            .await
            .context("failed sending data")?;
    }

    stream.finish().unwrap();
    // Wait for stream to close
    _ = stream.stopped().await;

    Ok(())
}

pub fn rt(runtime_type: RuntimeType) -> Runtime {
    match runtime_type {
        RuntimeType::Tokio => {
            let counter = std::sync::atomic::AtomicUsize::new(0);
            Builder::new_multi_thread()
                .thread_name_fn(move || {
                    format!(
                        "tokio-runtime-{}",
                        counter.fetch_add(1, std::sync::atomic::Ordering::Relaxed)
                    )
                })
                .enable_all()
                .build()
                .unwrap()
        }
        RuntimeType::TokioCurrentThread => {
            Builder::new_current_thread().enable_all().build().unwrap()
        }
    }
}

pub fn transport_config(opt: &Opt) -> noq::TransportConfig {
    // High stream windows are chosen because the amount of concurrent streams
    // is configurable as a parameter.
    let mut config = noq::TransportConfig::default();
    config.max_concurrent_uni_streams(opt.max_streams.try_into().unwrap());
    config.initial_mtu(opt.initial_mtu);

    let mut acks = noq::AckFrequencyConfig::default();
    acks.ack_eliciting_threshold(10u32.into());
    config.ack_frequency_config(Some(acks));

    config
}

#[derive(Parser, Debug, Clone, Copy)]
#[clap(name = "bulk")]
pub struct Opt {
    /// The total number of clients which should be created
    #[clap(long = "clients", short = 'c', default_value = "1")]
    pub clients: usize,
    /// The total number of streams which should be created
    #[clap(long = "streams", short = 'n', default_value = "1")]
    pub streams: usize,
    /// The amount of concurrent streams which should be used
    #[clap(long = "max_streams", short = 'm', default_value = "1")]
    pub max_streams: usize,
    /// Number of bytes to transmit from server to client
    ///
    /// This can use SI suffixes for sizes. For example, 1M will transfer
    /// 1MiB, 10G will transfer 10GiB.
    #[clap(long, default_value = "1G", value_parser = parse_byte_size)]
    pub download_size: u64,
    /// Number of bytes to transmit from client to server
    ///
    /// This can use SI suffixes for sizes. For example, 1M will transfer
    /// 1MiB, 10G will transfer 10GiB.
    #[clap(long, default_value = "0", value_parser = parse_byte_size)]
    pub upload_size: u64,
    /// Show connection stats the at the end of the benchmark
    #[clap(long = "stats")]
    pub stats: bool,
    /// Whether to use the unordered read API
    #[clap(long = "unordered")]
    pub read_unordered: bool,
    /// Allows to configure the desired cipher suite
    ///
    /// Valid options are: aes128, aes256, chacha20
    #[clap(long = "cipher", default_value = "aes128")]
    pub cipher: CipherSuite,
    /// Starting guess for maximum UDP payload size
    #[clap(long, default_value = "1200")]
    pub initial_mtu: u16,
    /// The runtime type to use
    #[clap(long, default_value = "tokio")]
    pub runtime_type: RuntimeType,
}

#[derive(Debug, Clone, Copy)]
pub enum RuntimeType {
    Tokio,
    TokioCurrentThread,
}

impl FromStr for RuntimeType {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "tokio" => Ok(Self::Tokio),
            "tokio-current-thread" => Ok(Self::TokioCurrentThread),
            _ => Err(anyhow::anyhow!("Unknown runtime type {}", s)),
        }
    }
}

fn parse_byte_size(s: &str) -> Result<u64, ParseIntError> {
    let s = s.trim();

    let multiplier = match s.chars().last() {
        Some('T') => 1024 * 1024 * 1024 * 1024,
        Some('G') => 1024 * 1024 * 1024,
        Some('M') => 1024 * 1024,
        Some('k') => 1024,
        _ => 1,
    };

    let s = match multiplier {
        1 => s,
        _ => &s[..s.len() - 1],
    };

    Ok(u64::from_str(s)? * multiplier)
}

#[derive(Debug, PartialEq, Eq, Clone, Copy)]
pub enum CipherSuite {
    Aes128,
    Aes256,
    Chacha20,
}

impl CipherSuite {
    pub fn as_rustls(self) -> rustls::SupportedCipherSuite {
        use rustls::crypto::ring::cipher_suite;
        match self {
            Self::Aes128 => cipher_suite::TLS13_AES_128_GCM_SHA256,
            Self::Aes256 => cipher_suite::TLS13_AES_256_GCM_SHA384,
            Self::Chacha20 => cipher_suite::TLS13_CHACHA20_POLY1305_SHA256,
        }
    }
}

impl FromStr for CipherSuite {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "aes128" => Ok(Self::Aes128),
            "aes256" => Ok(Self::Aes256),
            "chacha20" => Ok(Self::Chacha20),
            _ => Err(anyhow::anyhow!("Unknown cipher suite {}", s)),
        }
    }
}

// --- Datagram benchmark options ---

/// Direction of the datagram flood.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    /// Client sends, server receives.
    Send,
    /// Server sends, client receives.
    Recv,
    /// Both sides send and receive concurrently (full-duplex).
    Both,
}

impl FromStr for Direction {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "send" => Ok(Self::Send),
            "recv" | "receive" => Ok(Self::Recv),
            "both" | "duplex" => Ok(Self::Both),
            _ => Err(anyhow::anyhow!("Unknown direction {} (send|recv|both)", s)),
        }
    }
}

/// How the sender should issue datagrams.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendMode {
    /// `Connection::send_datagram` — drops the oldest queued datagram on backpressure.
    Drop,
    /// `Connection::send_datagram_wait` — backpressures (waits for buffer space).
    Wait,
}

impl FromStr for SendMode {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "drop" => Ok(Self::Drop),
            "wait" => Ok(Self::Wait),
            _ => Err(anyhow::anyhow!("Unknown send-mode {} (drop|wait)", s)),
        }
    }
}

/// Congestion controller to use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Congestion {
    Cubic,
    NewReno,
    Bbr3,
}

impl FromStr for Congestion {
    type Err = anyhow::Error;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "cubic" => Ok(Self::Cubic),
            "newreno" | "new-reno" | "reno" => Ok(Self::NewReno),
            "bbr3" | "bbr" => Ok(Self::Bbr3),
            _ => Err(anyhow::anyhow!("Unknown congestion {} (cubic|newreno|bbr3)", s)),
        }
    }
}

/// Options for the datagram benchmark.
#[derive(Parser, Debug, Clone, Copy)]
#[clap(name = "datagram")]
pub struct DatagramOpt {
    /// Number of parallel client connections (each floods independently).
    #[clap(long, short = 'c', default_value = "1")]
    pub clients: usize,

    /// Direction of the flood: client→server (send), server→client (recv), or both.
    #[clap(long, default_value = "send")]
    pub direction: Direction,

    /// Datagram payload size in bytes. Clamped to the negotiated max datagram size.
    #[clap(long, default_value = "1200")]
    pub packet_size: usize,

    /// Total bytes to send per direction. SI suffixes: 1G, 500M, 10k.
    #[clap(long, default_value = "1G", value_parser = parse_byte_size)]
    pub total_bytes: u64,

    /// How the sender issues datagrams: `drop` (send_datagram) or `wait` (send_datagram_wait).
    #[clap(long, default_value = "drop")]
    pub send_mode: SendMode,

    /// Congestion controller.
    #[clap(long, default_value = "cubic")]
    pub congestion: Congestion,

    /// `send_fairness` transport setting. Rayfish uses one datagram stream per peer, so
    /// `false` is the likely-better setting; default keeps noq's default (`true`).
    #[clap(long, default_value = "true")]
    pub send_fairness: bool,

    /// `datagram_send_buffer_size` in bytes.
    #[clap(long, default_value = "1048576")]
    pub datagram_send_buffer: usize,

    /// `datagram_receive_buffer_size` in bytes. If unset, uses noq's default.
    #[clap(long)]
    pub datagram_recv_buffer: Option<usize>,

    /// `ack_eliciting_threshold` for the ACK frequency extension. If unset, disabled.
    #[clap(long)]
    pub ack_frequency: Option<u32>,

    /// Starting guess for maximum UDP payload size.
    #[clap(long, default_value = "1200")]
    pub initial_mtu: u16,

    /// Print connection stats at the end.
    #[clap(long)]
    pub stats: bool,

    /// Runtime type.
    #[clap(long, default_value = "tokio")]
    pub runtime_type: RuntimeType,

    /// Cipher suite.
    #[clap(long, default_value = "aes128")]
    pub cipher: CipherSuite,
}

impl BenchOpt for DatagramOpt {
    fn cipher(&self) -> CipherSuite {
        self.cipher
    }
    fn transport_config(&self) -> noq::TransportConfig {
        use std::sync::Arc;

        let mut config = noq::TransportConfig::default();
        config.initial_mtu(self.initial_mtu);
        config.send_fairness(self.send_fairness);
        config.datagram_send_buffer_size(self.datagram_send_buffer);
        // Only override the receive buffer when the user explicitly sets one;
        // leaving it unset keeps noq's default (which enables datagrams). Setting
        // `None` here would disable datagram support entirely.
        if let Some(recv_buf) = self.datagram_recv_buffer {
            config.datagram_receive_buffer_size(Some(recv_buf));
        }
        config.enable_segmentation_offload(true);

        let factory: Arc<dyn noq::congestion::ControllerFactory + Send + Sync + 'static> =
            match self.congestion {
                Congestion::Cubic => Arc::new(noq::congestion::CubicConfig::default()),
                Congestion::NewReno => Arc::new(noq::congestion::NewRenoConfig::default()),
                Congestion::Bbr3 => Arc::new(noq::congestion::Bbr3Config::default()),
            };
        config.congestion_controller_factory(factory);

        if let Some(threshold) = self.ack_frequency {
            let mut acks = noq::AckFrequencyConfig::default();
            acks.ack_eliciting_threshold(threshold.into());
            config.ack_frequency_config(Some(acks));
        }

        config
    }
}
