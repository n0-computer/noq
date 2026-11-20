//! AF_XDP sockets for endpoints with multiple sockets.
//!
//! An AF_XDP socket exchanges raw Ethernet frames with one receive queue of a network
//! interface, bypassing the kernel's IP and UDP stack. An XDP program attached to the
//! interface redirects UDP packets for the socket's address to it, and passes all other
//! traffic, including ARP, to the kernel as usual. See
//! <https://docs.kernel.org/networking/af_xdp.html>.
//!
//! [`XdpInterface::attach`] loads and attaches the XDP program, and [`XdpInterface::bind`]
//! creates an [`XdpSocket`] for one queue. An [`XdpSocket`] implements [`AsyncUdpSocketNext`],
//! so it is added to an endpoint with [`Endpoint::new_with_multiple_sockets`] or
//! [`Endpoint::add_socket`].
//!
//! # Limitations
//!
//! - IPv4 only, and the socket sends from exactly one address and port, [`XdpConfig::new`].
//! - There is no routing: the destination MAC is the neighbor's, learned from received frames,
//!   configured with [`XdpConfig::neighbor`], read from the kernel's ARP table, or else the
//!   [`XdpConfig::gateway`]. A datagram without a known MAC is dropped.
//! - No GSO or GRO: a transmit with several segments is split into one frame each.
//! - Checksums are computed in software; received checksums are not verified.
//! - Every socket has its own UMEM; sockets on several queues do not share one.
//! - Requires Linux 5.9 or later, CAP_NET_ADMIN, CAP_NET_RAW and CAP_BPF (or root), and the
//!   tokio runtime with its timer. The UMEM counts against RLIMIT_MEMLOCK unless the process
//!   has CAP_IPC_LOCK.
//!
//! # Examples
//!
//! ```no_run
//! # use std::sync::Arc;
//! # async fn f(config: noq::EndpointConfig, server: noq::ServerConfig) -> std::io::Result<()> {
//! use noq::{Endpoint, SocketRoute, TokioRuntime, xdp::{XdpConfig, XdpInterface}};
//!
//! let interface = XdpInterface::attach(XdpConfig::new("veth0", "10.0.0.1:4433".parse().unwrap()))?;
//! let socket = interface.bind(0)?;
//! let route = SocketRoute::new("0.0.0.0/0".parse().unwrap()).with_default(true);
//! let endpoint = Endpoint::new_with_multiple_sockets(
//!     config,
//!     Some(server),
//!     vec![(Box::new(socket), vec![route])],
//!     Arc::new(TokioRuntime),
//! )?;
//! # Ok(()) }
//! ```
//!
//! [`AsyncUdpSocketNext`]: crate::AsyncUdpSocketNext
//! [`Endpoint::new_with_multiple_sockets`]: crate::Endpoint::new_with_multiple_sockets
//! [`Endpoint::add_socket`]: crate::Endpoint::add_socket

use std::{
    collections::HashMap,
    ffi::CString,
    fmt,
    future::Future,
    io::{self, IoSliceMut},
    net::{IpAddr, Ipv4Addr, SocketAddr, SocketAddrV4},
    num::NonZeroUsize,
    os::fd::{AsFd, OwnedFd},
    pin::Pin,
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    task::{Context, Poll, ready},
    time::{Duration, Instant},
};

use tokio::io::unix::AsyncFd;
use tracing::{debug, info, trace, warn};
use udp::{EcnCodepoint, RecvMeta, Transmit};

use crate::runtime::{AsyncUdpSocketNext, UdpReceiver, UdpSender};

mod bpf;
mod packet;
mod xsk;

pub use packet::MacAddr;

use self::{
    bpf::XdpProgram,
    packet::{FrameHeader, HEADERS_LEN, IP_UDP_HDR_LEN},
    xsk::{Ring, Umem, Xsk, XskSizes},
};

/// How long a blocked sender waits for the kernel to signal free TX space before retrying
///
/// Bounds the stall if a driver completes frames without waking the socket.
const TX_RETRY_INTERVAL: Duration = Duration::from_millis(1);
/// How often the kernel is kicked at most per send while it reports that it is busy
const MAX_TX_KICKS: usize = 8;
/// The minimum interval between two reads of the kernel's ARP table
const ARP_TABLE_INTERVAL: Duration = Duration::from_millis(100);

/// How the XDP program is attached to the interface
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum AttachMode {
    /// Native mode if the driver supports it, generic mode otherwise
    #[default]
    Auto,
    /// Generic (SKB) mode, which works with every driver but runs after SKB allocation
    Generic,
    /// Native (driver) mode, which fails if the driver has no XDP support
    Native,
}

/// Whether the AF_XDP socket copies frames between the driver and the UMEM
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum BindMode {
    /// Zero-copy if the driver supports it, copy mode otherwise
    #[default]
    Auto,
    /// Copy mode, supported by every driver
    Copy,
    /// Zero-copy mode, which fails if the driver does not support it
    ZeroCopy,
}

/// Configuration of an [`XdpInterface`]
#[derive(Debug, Clone)]
pub struct XdpConfig {
    interface: String,
    local_addr: SocketAddrV4,
    attach_mode: AttachMode,
    bind_mode: BindMode,
    frame_count: u32,
    ring_size: u32,
    max_segments: usize,
    gateway: Option<MacAddr>,
    neighbors: Vec<(Ipv4Addr, MacAddr)>,
}

impl XdpConfig {
    /// Creates a configuration for sockets on `interface` with the address `local_addr`
    ///
    /// `local_addr` must be an address of the interface: the XDP program redirects UDP packets
    /// to it, and the sockets send from it.
    pub fn new(interface: impl Into<String>, local_addr: SocketAddrV4) -> Self {
        Self {
            interface: interface.into(),
            local_addr,
            attach_mode: AttachMode::default(),
            bind_mode: BindMode::default(),
            frame_count: 4096,
            ring_size: 2048,
            max_segments: 32,
            gateway: None,
            neighbors: Vec::new(),
        }
    }

    /// Sets how the XDP program is attached, [`AttachMode::Auto`] by default
    pub fn attach_mode(mut self, mode: AttachMode) -> Self {
        self.attach_mode = mode;
        self
    }

    /// Sets whether the sockets use zero-copy, [`BindMode::Auto`] by default
    pub fn bind_mode(mut self, mode: BindMode) -> Self {
        self.bind_mode = mode;
        self
    }

    /// Sets the number of 4 KiB UMEM frames of each socket, 4096 by default
    ///
    /// Half of the frames, but at most [`XdpConfig::ring_size`], receive; the rest send.
    pub fn frame_count(mut self, count: u32) -> Self {
        self.frame_count = count;
        self
    }

    /// Sets the number of entries of each ring, a power of two, 2048 by default
    pub fn ring_size(mut self, size: u32) -> Self {
        self.ring_size = size;
        self
    }

    /// Sets the maximum number of datagrams of one transmit, 32 by default
    ///
    /// The socket splits a transmit into frames and hands them to the kernel with one
    /// syscall, which emulates GSO. Each socket caps this to its number of sending frames and
    /// to the ring size.
    pub fn max_segments(mut self, count: usize) -> Self {
        self.max_segments = count.max(1);
        self
    }

    /// Sets the MAC address to send to when a destination's MAC is unknown
    ///
    /// Usually the MAC of the default gateway, or of the peer on a point-to-point link.
    pub fn gateway(mut self, mac: MacAddr) -> Self {
        self.gateway = Some(mac);
        self
    }

    /// Adds a static neighbor entry, the MAC address to send to for `ip`
    pub fn neighbor(mut self, ip: Ipv4Addr, mac: MacAddr) -> Self {
        self.neighbors.push((ip, mac));
        self
    }
}

/// An XDP program attached to a network interface, which creates [`XdpSocket`]s
///
/// The program stays attached as long as this value or any socket it created exists. It is
/// attached through a BPF link, so the kernel also detaches it when the process exits.
#[derive(Debug, Clone)]
pub struct XdpInterface {
    shared: Arc<InterfaceShared>,
}

struct InterfaceShared {
    config: XdpConfig,
    ifindex: u32,
    mac: MacAddr,
    mtu: usize,
    queue_count: u32,
    neighbors: Neighbors,
    program: XdpProgram,
}

impl fmt::Debug for InterfaceShared {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InterfaceShared")
            .field("interface", &self.config.interface)
            .field("local_addr", &self.config.local_addr)
            .field("ifindex", &self.ifindex)
            .field("mac", &self.mac)
            .finish_non_exhaustive()
    }
}

impl XdpInterface {
    /// Loads the XDP program and attaches it to the configured interface
    ///
    /// # Errors
    ///
    /// Fails if the interface does not exist, the process lacks the privileges to load or
    /// attach XDP programs, or another XDP program is attached to the interface.
    pub fn attach(config: XdpConfig) -> io::Result<Self> {
        let name = CString::new(config.interface.as_str())
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
        // SAFETY: `name` is a valid C string.
        let ifindex = unsafe { libc::if_nametoindex(name.as_ptr()) };
        if ifindex == 0 {
            let e = io::Error::last_os_error();
            return Err(context(
                e,
                &format!("looking up interface {}", config.interface),
            ));
        }
        let sysfs = format!("/sys/class/net/{}", config.interface);
        let mac = read_sysfs(&sysfs, "address")?.parse()?;
        let mtu = read_sysfs(&sysfs, "mtu")?
            .parse()
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let queue_count = std::fs::read_dir(format!("{sysfs}/queues"))?
            .filter_map(Result::ok)
            .filter(|entry| entry.file_name().to_string_lossy().starts_with("rx-"))
            .count()
            .max(1) as u32;
        let program =
            XdpProgram::attach(ifindex, config.local_addr, queue_count, config.attach_mode)?;
        info!(
            interface = %config.interface,
            ifindex,
            %mac,
            mtu,
            queue_count,
            local_addr = %config.local_addr,
            mode = ?config.attach_mode,
            "attached XDP program"
        );
        let neighbors = Neighbors::new(&config);
        Ok(Self {
            shared: Arc::new(InterfaceShared {
                config,
                ifindex,
                mac,
                mtu,
                queue_count,
                neighbors,
                program,
            }),
        })
    }

    /// Creates an AF_XDP socket on receive queue `queue` of the interface
    ///
    /// Must be called within a tokio runtime. Only packets that the NIC steers to `queue` reach
    /// the socket; bind a socket to every queue, reduce the queues to one with
    /// `ethtool -L <interface> combined 1`, or steer the traffic with `ethtool -N`.
    ///
    /// # Errors
    ///
    /// Fails if `queue` does not exist, the process lacks CAP_NET_RAW, the configured
    /// [`BindMode`] is not supported, or there is no tokio runtime.
    pub fn bind(&self, queue: u32) -> io::Result<XdpSocket> {
        let iface = &self.shared;
        if queue >= iface.queue_count {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("queue {queue} out of {} queues", iface.queue_count),
            ));
        }
        let sizes = XskSizes {
            frame_count: iface.config.frame_count,
            frame_size: 4096,
            ring_size: iface.config.ring_size,
        };
        let Xsk {
            fd,
            umem,
            fill,
            completion,
            rx,
            tx,
            tx_frames,
            zero_copy,
        } = Xsk::new(iface.ifindex, queue, sizes, iface.config.bind_mode)?;
        if tokio::runtime::Handle::try_current().is_err() {
            return Err(io::Error::other("XdpInterface::bind needs a tokio runtime"));
        }
        // `AsyncFd::register`, which replaces `new`, needs a newer tokio than noq requires.
        // `new` is sound here: the `OwnedFd` keeps the descriptor open and unchanged.
        #[allow(deprecated)]
        let fd = AsyncFd::new(fd)?;
        iface.program.register(queue, fd.as_fd())?;
        // A transmit needs a free frame and a TX ring slot per segment, so a larger one would
        // never fit and its sender would retry forever.
        let max_segments = iface
            .config
            .max_segments
            .min(tx_frames.len())
            .min(iface.config.ring_size as usize);
        let max_segments = NonZeroUsize::new(max_segments).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                "no UMEM frames left for sending",
            )
        })?;
        info!(
            interface = %iface.config.interface,
            queue,
            zero_copy,
            "bound AF_XDP socket"
        );
        Ok(XdpSocket {
            shared: Arc::new(SocketShared {
                fd,
                tx: Mutex::new(TxState {
                    ring: tx,
                    completion,
                    frames: tx_frames,
                }),
                receiver: Mutex::new(Some((fill, rx))),
                umem,
                iface: self.shared.clone(),
                queue,
                max_segments,
                stats: Counters::default(),
                warned_no_neighbor: AtomicBool::new(false),
            }),
        })
    }

    /// Returns the number of receive queues of the interface
    pub fn queue_count(&self) -> u32 {
        self.shared.queue_count
    }

    /// Returns the MAC address of the interface
    pub fn mac(&self) -> MacAddr {
        self.shared.mac
    }
}

/// An AF_XDP socket on one queue of an [`XdpInterface`]
///
/// Sends and receives UDP over IPv4 from the configured local address. Clones share the
/// socket.
#[derive(Debug, Clone)]
pub struct XdpSocket {
    shared: Arc<SocketShared>,
}

#[derive(Debug)]
struct SocketShared {
    // Closed first, so that the kernel stops using the rings and the UMEM before they are
    // unmapped.
    fd: AsyncFd<OwnedFd>,
    tx: Mutex<TxState>,
    /// The fill and RX rings, taken by the receiver
    receiver: Mutex<Option<(Ring<u64>, Ring<libc::xdp_desc>)>>,
    umem: Umem,
    iface: Arc<InterfaceShared>,
    queue: u32,
    /// [`XdpConfig::max_segments`], capped to what fits the TX frames and ring
    max_segments: NonZeroUsize,
    stats: Counters,
    warned_no_neighbor: AtomicBool,
}

#[derive(Debug)]
struct TxState {
    ring: Ring<libc::xdp_desc>,
    completion: Ring<u64>,
    /// Free frames for sending
    frames: Vec<u64>,
}

impl XdpSocket {
    /// Returns the receive queue this socket is bound to
    pub fn queue(&self) -> u32 {
        self.shared.queue
    }

    /// Returns the socket's counters and the kernel's statistics of the socket
    pub fn stats(&self) -> XdpStats {
        let counters = &self.shared.stats;
        let kernel = Xsk::statistics(self.shared.fd.get_ref().as_fd())
            .inspect_err(|e| debug!("reading AF_XDP statistics: {e}"))
            .ok();
        XdpStats {
            rx_packets: counters.rx_packets.load(Ordering::Relaxed),
            rx_invalid: counters.rx_invalid.load(Ordering::Relaxed),
            tx_packets: counters.tx_packets.load(Ordering::Relaxed),
            tx_no_neighbor: counters.tx_no_neighbor.load(Ordering::Relaxed),
            tx_too_big: counters.tx_too_big.load(Ordering::Relaxed),
            tx_not_ipv4: counters.tx_not_ipv4.load(Ordering::Relaxed),
            tx_blocked: counters.tx_blocked.load(Ordering::Relaxed),
            kernel_rx_dropped: kernel.map(|k| k.rx_dropped),
            kernel_rx_ring_full: kernel.map(|k| k.rx_ring_full),
            kernel_rx_fill_ring_empty: kernel.map(|k| k.rx_fill_ring_empty_descs),
            kernel_tx_ring_empty: kernel.map(|k| k.tx_ring_empty_descs),
        }
    }
}

/// Counters of an [`XdpSocket`], see [`XdpSocket::stats`]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[non_exhaustive]
pub struct XdpStats {
    /// Datagrams received
    pub rx_packets: u64,
    /// Received frames that were not unfragmented UDP over IPv4 or did not fit the buffer
    pub rx_invalid: u64,
    /// Datagrams sent
    pub tx_packets: u64,
    /// Datagrams dropped because the destination's MAC address was unknown
    pub tx_no_neighbor: u64,
    /// Datagrams dropped because they exceeded the interface MTU or the frame size
    pub tx_too_big: u64,
    /// Transmits dropped because the destination was not IPv4
    pub tx_not_ipv4: u64,
    /// Sends that found no free frame or TX ring slot and had to wait
    pub tx_blocked: u64,
    /// Frames the kernel dropped for other reasons, `None` if the kernel did not report it
    pub kernel_rx_dropped: Option<u64>,
    /// Frames the kernel dropped because the RX ring was full
    pub kernel_rx_ring_full: Option<u64>,
    /// Times the kernel found the fill ring empty
    pub kernel_rx_fill_ring_empty: Option<u64>,
    /// Times the kernel found the TX ring empty when woken
    pub kernel_tx_ring_empty: Option<u64>,
}

#[derive(Debug, Default)]
struct Counters {
    rx_packets: AtomicU64,
    rx_invalid: AtomicU64,
    tx_packets: AtomicU64,
    tx_no_neighbor: AtomicU64,
    tx_too_big: AtomicU64,
    tx_not_ipv4: AtomicU64,
    tx_blocked: AtomicU64,
}

impl Counters {
    fn add(counter: &AtomicU64, n: u64) {
        counter.fetch_add(n, Ordering::Relaxed);
    }
}

impl SocketShared {
    /// Sends `transmit`, or returns [`io::ErrorKind::WouldBlock`] if there is no space
    ///
    /// Datagrams that can't be sent at all are dropped and counted, like a UDP socket drops
    /// unroutable datagrams.
    fn send(&self, transmit: &Transmit<'_>) -> io::Result<()> {
        let iface = &*self.iface;
        let stats = &self.stats;
        let SocketAddr::V4(dst) = to_canonical(transmit.destination) else {
            Counters::add(&stats.tx_not_ipv4, 1);
            trace!(dst = %transmit.destination, "dropping transmit to non-IPv4 destination");
            return Ok(());
        };
        if transmit.contents.is_empty() {
            return Ok(());
        }
        let Some(dst_mac) = iface.neighbors.lookup(*dst.ip(), &iface.config.interface) else {
            Counters::add(&stats.tx_no_neighbor, 1);
            if !self.warned_no_neighbor.swap(true, Ordering::Relaxed) {
                warn!(%dst, "dropping datagram: no MAC address known for destination, see XdpConfig::gateway");
            }
            return Ok(());
        };
        let segment = transmit
            .segment_size
            .unwrap_or(transmit.contents.len())
            .min(transmit.contents.len());
        if segment + IP_UDP_HDR_LEN > iface.mtu || segment + HEADERS_LEN > self.umem.frame_size() {
            // Like EMSGSIZE from a UDP socket, which MTU probes run into.
            Counters::add(&stats.tx_too_big, 1);
            trace!(
                segment,
                mtu = iface.mtu,
                "dropping datagram larger than the MTU"
            );
            return Ok(());
        }
        let count = transmit.contents.len().div_ceil(segment);
        let header = FrameHeader {
            src_mac: iface.mac,
            dst_mac,
            src: iface.config.local_addr,
            dst,
            ecn: transmit.ecn.map_or(0, |ecn| ecn as u8),
        };

        let mut tx = self.tx.lock().expect("poisoned");
        tx.reclaim();
        if !tx.has_room(count) {
            tx.kick(&self.fd);
            tx.reclaim();
            if !tx.has_room(count) {
                Counters::add(&stats.tx_blocked, 1);
                return Err(io::ErrorKind::WouldBlock.into());
            }
        }
        for payload in transmit.contents.chunks(segment) {
            let addr = tx.frames.pop().expect("checked by has_room");
            // SAFETY: the frame came from the free list, so the kernel does not use it and no
            // other reference into it exists.
            let frame = unsafe { self.umem.frame(addr, self.umem.frame_size()) };
            frame[HEADERS_LEN..HEADERS_LEN + payload.len()].copy_from_slice(payload);
            let len = packet::write_headers(frame, &header, payload.len());
            tx.ring.push(libc::xdp_desc {
                addr,
                len: len as u32,
                options: 0,
            });
        }
        tx.ring.submit();
        if tx.ring.needs_wakeup() {
            tx.kick(&self.fd);
        }
        Counters::add(&stats.tx_packets, count as u64);
        Ok(())
    }
}

impl TxState {
    /// Moves completed frames back to the free list
    fn reclaim(&mut self) {
        while let Some(addr) = self.completion.pop() {
            self.frames.push(addr);
        }
        self.completion.release();
    }

    fn has_room(&mut self, count: usize) -> bool {
        self.frames.len() >= count && self.ring.free() as usize >= count
    }

    /// Asks the kernel to send what is in the TX ring
    ///
    /// In copy mode, the kernel sends a limited batch per call and reports `EAGAIN` if frames
    /// remain, so this retries a few times while frames are pending.
    fn kick(&mut self, fd: &AsyncFd<OwnedFd>) {
        for _ in 0..MAX_TX_KICKS {
            match xsk::wake_tx(fd.get_ref().as_fd()) {
                Ok(()) => return,
                Err(e) => match e.raw_os_error() {
                    Some(libc::EAGAIN) | Some(libc::EBUSY) if self.ring.pending() > 0 => {}
                    Some(libc::EAGAIN) | Some(libc::EBUSY) | Some(libc::ENOBUFS) => return,
                    Some(libc::ENETDOWN) => {
                        debug!("AF_XDP send failed, interface down");
                        return;
                    }
                    _ => {
                        warn!("AF_XDP send failed: {e}");
                        return;
                    }
                },
            }
        }
    }
}

impl AsyncUdpSocketNext for XdpSocket {
    fn create_sender(&self) -> Pin<Box<dyn UdpSender>> {
        Box::pin(XdpSender {
            shared: self.shared.clone(),
            wait: None,
        })
    }

    fn create_receiver(&self) -> Option<Box<dyn UdpReceiver>> {
        let (fill, rx) = self.shared.receiver.lock().expect("poisoned").take()?;
        Some(Box::new(XdpReceiver {
            shared: self.shared.clone(),
            fill,
            rx,
        }))
    }

    fn local_addr(&self) -> io::Result<SocketAddr> {
        Ok(self.shared.iface.config.local_addr.into())
    }

    fn may_fragment(&self) -> bool {
        // Every packet has the "don't fragment" bit set.
        false
    }
}

/// A future that resolves when a blocked sender should retry
type WaitFuture = Pin<Box<dyn Future<Output = ()> + Send + Sync>>;

/// Sends on an [`XdpSocket`]
///
/// Waits for write space with its own future, so that every sender gets its own waker.
struct XdpSender {
    shared: Arc<SocketShared>,
    wait: Option<WaitFuture>,
}

impl fmt::Debug for XdpSender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("XdpSender")
            .field("queue", &self.shared.queue)
            .finish_non_exhaustive()
    }
}

impl UdpSender for XdpSender {
    fn poll_send(
        mut self: Pin<&mut Self>,
        transmit: &Transmit<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        loop {
            if let Some(wait) = &mut self.wait {
                ready!(wait.as_mut().poll(cx));
                self.wait = None;
            }
            match self.shared.send(transmit) {
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {
                    let shared = self.shared.clone();
                    self.wait = Some(Box::pin(async move {
                        // Completions wake the socket for writing. A stale readiness resolves
                        // at once and is cleared, so the retry after it waits for a new one.
                        let writable =
                            tokio::time::timeout(TX_RETRY_INTERVAL, shared.fd.writable());
                        if let Ok(Ok(mut guard)) = writable.await {
                            guard.clear_ready();
                        }
                    }));
                }
                result => return Poll::Ready(result),
            }
        }
    }

    fn max_transmit_segments(&self) -> NonZeroUsize {
        self.shared.max_segments
    }
}

/// Receives from an [`XdpSocket`]
#[derive(Debug)]
struct XdpReceiver {
    shared: Arc<SocketShared>,
    fill: Ring<u64>,
    rx: Ring<libc::xdp_desc>,
}

impl UdpReceiver for XdpReceiver {
    fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        let Self { shared, fill, rx } = self;
        loop {
            let received = receive(shared, fill, rx, bufs, meta);
            if received > 0 {
                return Poll::Ready(Ok(received));
            }
            if fill.needs_wakeup() {
                xsk::wake_rx(shared.fd.get_ref().as_fd());
            }
            let mut guard = ready!(shared.fd.poll_read_ready(cx))?;
            let received = receive(shared, fill, rx, bufs, meta);
            if received > 0 {
                return Poll::Ready(Ok(received));
            }
            guard.clear_ready();
        }
    }
}

/// Copies received datagrams into `bufs` and returns their frames to the fill ring
fn receive(
    shared: &SocketShared,
    fill: &mut Ring<u64>,
    rx: &mut Ring<libc::xdp_desc>,
    bufs: &mut [IoSliceMut<'_>],
    meta: &mut [RecvMeta],
) -> usize {
    let iface = &*shared.iface;
    let mut received = 0;
    let mut invalid = 0;
    while received < bufs.len().min(meta.len()) {
        let Some(desc) = rx.pop() else { break };
        // SAFETY: the kernel handed the frame over through the RX ring, and it stays ours
        // until it is pushed to the fill ring below.
        let frame = unsafe { shared.umem.frame(desc.addr, desc.len as usize) };
        let buf = &mut bufs[received];
        match packet::parse_frame(frame) {
            Some(datagram) if datagram.payload.len() <= buf.len() => {
                let len = datagram.payload.len();
                buf[..len].copy_from_slice(datagram.payload);
                let meta = &mut meta[received];
                *meta = RecvMeta::default();
                meta.addr = datagram.src.into();
                meta.len = len;
                meta.stride = len;
                meta.ecn = EcnCodepoint::from_bits(datagram.ecn);
                meta.dst_ip = Some(IpAddr::V4(*datagram.dst.ip()));
                meta.interface_index = Some(iface.ifindex);
                iface.neighbors.learn(*datagram.src.ip(), datagram.src_mac);
                received += 1;
            }
            _ => invalid += 1,
        }
        // The fill ring has room for every receive frame, so this never overwrites.
        fill.push(shared.umem.frame_base(desc.addr));
    }
    rx.release();
    fill.submit();
    Counters::add(&shared.stats.rx_packets, received as u64);
    Counters::add(&shared.stats.rx_invalid, invalid);
    received
}

/// The MAC addresses of the neighbors of an interface
#[derive(Debug)]
struct Neighbors {
    table: RwLock<HashMap<Ipv4Addr, MacAddr>>,
    gateway: Option<MacAddr>,
    /// When the ARP table was read last, to bound the cost of unknown destinations
    last_arp_read: Mutex<Option<Instant>>,
}

impl Neighbors {
    fn new(config: &XdpConfig) -> Self {
        Self {
            table: RwLock::new(config.neighbors.iter().copied().collect()),
            gateway: config.gateway,
            last_arp_read: Mutex::new(None),
        }
    }

    /// Returns the MAC address to send to for `ip`
    fn lookup(&self, ip: Ipv4Addr, interface: &str) -> Option<MacAddr> {
        if let Some(mac) = self.table.read().expect("poisoned").get(&ip) {
            return Some(*mac);
        }
        if let Some(mac) = self.read_arp_table(ip, interface) {
            debug!(%ip, %mac, "found neighbor in the ARP table");
            self.table.write().expect("poisoned").insert(ip, mac);
            return Some(mac);
        }
        self.gateway
    }

    /// Records that `ip` sent from `mac`
    fn learn(&self, ip: Ipv4Addr, mac: MacAddr) {
        if self.table.read().expect("poisoned").get(&ip) == Some(&mac) {
            return;
        }
        trace!(%ip, %mac, "learned neighbor");
        self.table.write().expect("poisoned").insert(ip, mac);
    }

    fn read_arp_table(&self, ip: Ipv4Addr, interface: &str) -> Option<MacAddr> {
        {
            let mut last = self.last_arp_read.lock().expect("poisoned");
            let now = Instant::now();
            if last.is_some_and(|last| now.duration_since(last) < ARP_TABLE_INTERVAL) {
                return None;
            }
            *last = Some(now);
        }
        let table = std::fs::read_to_string("/proc/net/arp").ok()?;
        parse_arp_table(&table, ip, interface)
    }
}

/// Finds the MAC address of `ip` on `interface` in the contents of `/proc/net/arp`
fn parse_arp_table(table: &str, ip: Ipv4Addr, interface: &str) -> Option<MacAddr> {
    /// The `ATF_COM` flag of a resolved entry
    const COMPLETE: u32 = 0x2;
    table.lines().skip(1).find_map(|line| {
        let fields: Vec<_> = line.split_whitespace().collect();
        let [entry_ip, _hw_type, flags, mac, _mask, device] = fields[..] else {
            return None;
        };
        let flags = u32::from_str_radix(flags.trim_start_matches("0x"), 16).ok()?;
        let matches = entry_ip.parse() == Ok(ip) && device == interface && flags & COMPLETE != 0;
        matches.then(|| mac.parse().ok()).flatten()
    })
}

/// Maps an IPv4-mapped IPv6 address to IPv4
fn to_canonical(addr: SocketAddr) -> SocketAddr {
    SocketAddr::new(addr.ip().to_canonical(), addr.port())
}

fn read_sysfs(dir: &str, file: &str) -> io::Result<String> {
    let path = format!("{dir}/{file}");
    let contents = std::fs::read_to_string(&path).map_err(|e| context(e, &path))?;
    Ok(contents.trim().to_owned())
}

/// Adds `context` to the message of `error`
fn context(error: io::Error, context: &str) -> io::Error {
    io::Error::new(error.kind(), format!("{context}: {error}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const ARP_TABLE: &str = "\
IP address       HW type     Flags       HW address            Mask     Device
10.0.0.2         0x1         0x2         02:00:00:00:00:02     *        veth0
10.0.0.3         0x1         0x0         00:00:00:00:00:00     *        veth0
10.0.0.4         0x1         0x2         02:00:00:00:00:04     *        eth0
";

    #[test]
    fn arp_table_lookup_finds_complete_entries_on_the_interface() {
        let ip = |s: &str| s.parse().unwrap();
        assert_eq!(
            parse_arp_table(ARP_TABLE, ip("10.0.0.2"), "veth0"),
            Some(MacAddr([2, 0, 0, 0, 0, 2]))
        );
        // Incomplete entry.
        assert_eq!(parse_arp_table(ARP_TABLE, ip("10.0.0.3"), "veth0"), None);
        // Other interface.
        assert_eq!(parse_arp_table(ARP_TABLE, ip("10.0.0.4"), "veth0"), None);
        assert_eq!(parse_arp_table(ARP_TABLE, ip("10.0.0.9"), "veth0"), None);
    }

    #[test]
    fn neighbors_prefer_learned_entries_over_the_gateway() {
        let gateway = MacAddr([2, 0, 0, 0, 0, 0xff]);
        let config = XdpConfig::new("does-not-exist", "10.0.0.1:4433".parse().unwrap())
            .gateway(gateway)
            .neighbor("10.0.0.5".parse().unwrap(), MacAddr([2, 0, 0, 0, 0, 5]));
        let neighbors = Neighbors::new(&config);
        let lookup = |ip: &str| neighbors.lookup(ip.parse().unwrap(), "does-not-exist");
        assert_eq!(lookup("10.0.0.5"), Some(MacAddr([2, 0, 0, 0, 0, 5])));
        assert_eq!(lookup("10.0.0.6"), Some(gateway));
        neighbors.learn("10.0.0.6".parse().unwrap(), MacAddr([2, 0, 0, 0, 0, 6]));
        assert_eq!(lookup("10.0.0.6"), Some(MacAddr([2, 0, 0, 0, 0, 6])));
    }
}
