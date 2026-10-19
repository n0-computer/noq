use std::{
    collections::VecDeque,
    fmt,
    future::Future,
    io::{self, IoSliceMut},
    mem,
    net::{SocketAddr, SocketAddrV6},
    num::NonZeroUsize,
    pin::Pin,
    str,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll, RawWaker, RawWakerVTable, Waker},
};

#[cfg(all(
    not(wasm_browser),
    any(feature = "runtime-tokio", feature = "runtime-smol"),
    any(feature = "aws-lc-rs", feature = "ring"),
))]
use crate::runtime::default_runtime;
use crate::{
    Instant,
    routing::{RouteTable, SocketRoute},
    runtime::{AsyncUdpSocket, AsyncUdpSocketNext, Runtime, UdpReceiver},
    sender::ConnectionSender,
    udp_transmit,
};
use atomic_waker::AtomicWaker;
use bytes::{Bytes, BytesMut};
use pin_project_lite::pin_project;
use proto::{
    self as proto, ClientConfig, ConnectError, ConnectionError, ConnectionHandle, DatagramEvent,
    EndpointEvent, FourTuple, NetworkChangeHint, ServerConfig,
};
use rustc_hash::FxHashMap;
#[cfg(all(
    not(wasm_browser),
    any(feature = "runtime-tokio", feature = "runtime-smol"),
    any(feature = "aws-lc-rs", feature = "ring"),
))]
use socket2::{Domain, Protocol, Socket, Type};
use tokio::sync::{Notify, futures::Notified, mpsc};
use tracing::{Instrument, Span, debug, trace, warn};
use udp::{BATCH_SIZE, RecvMeta, is_msg_size_err};

use crate::{
    ConnectionEvent, EndpointConfig, IO_LOOP_BOUND, RECV_TIME_BOUND, VarInt,
    connection::Connecting, incoming::Incoming, work_limiter::WorkLimiter,
};

/// A QUIC endpoint.
///
/// An endpoint has one or more UDP sockets, may host many connections, and may act as both
/// client and server for different connections. See [`Endpoint::new_with_multiple_sockets`]
/// for endpoints with several sockets.
///
/// May be cloned to obtain another handle to the same endpoint.
#[derive(Debug, Clone)]
pub struct Endpoint {
    pub(crate) inner: EndpointRef,
    runtime: Arc<dyn Runtime>,
}

impl Endpoint {
    /// Helper to construct an endpoint for use with outgoing connections only
    ///
    /// Note that `addr` is the *local* address to bind to, which should usually be a wildcard
    /// address like `0.0.0.0:0` or `[::]:0`, which allow communication with any reachable IPv4 or
    /// IPv6 address respectively from an OS-assigned port.
    ///
    /// If an IPv6 address is provided, attempts to make the socket dual-stack so as to allow
    /// communication with both IPv4 and IPv6 addresses. As such, calling `Endpoint::client` with
    /// the address `[::]:0` is a reasonable default to maximize the ability to connect to other
    /// address. For example:
    ///
    /// ```
    /// noq::Endpoint::client((std::net::Ipv6Addr::UNSPECIFIED, 0).into());
    /// ```
    ///
    /// Some environments may not allow creation of dual-stack sockets, in which case an IPv6
    /// client will only be able to connect to IPv6 servers. An IPv4 client is never dual-stack.
    #[cfg(all(
        not(wasm_browser),
        any(feature = "runtime-tokio", feature = "runtime-smol"),
        any(feature = "aws-lc-rs", feature = "ring"), // `EndpointConfig::default()` is only available with these
    ))]
    pub fn client(addr: SocketAddr) -> io::Result<Self> {
        let socket = Socket::new(Domain::for_address(addr), Type::DGRAM, Some(Protocol::UDP))?;
        if addr.is_ipv6()
            && let Err(e) = socket.set_only_v6(false)
        {
            tracing::debug!(%e, "unable to make socket dual-stack");
        }
        socket.bind(&addr.into())?;
        let runtime =
            default_runtime().ok_or_else(|| io::Error::other("no async runtime found"))?;
        Self::new_with_abstract_socket(
            EndpointConfig::default(),
            None,
            runtime.wrap_udp_socket(socket.into())?,
            runtime,
        )
    }

    /// Returns relevant stats from this Endpoint
    pub fn stats(&self) -> EndpointStats {
        self.inner.state.lock().unwrap().stats
    }

    /// Helper to construct an endpoint for use with both incoming and outgoing connections
    ///
    /// Note that `addr` is the *local* address to bind to, which should usually be a wildcard
    /// address like `0.0.0.0:0` or `[::]:0`, which allow communication with any reachable IPv4 or
    /// IPv6 address respectively from an OS-assigned port.
    ///
    /// If an IPv6 address is provided, attempts to make the socket dual-stack so as to allow
    /// communication with both IPv4 and IPv6 clients. As such, calling `Endpoint::server` with
    /// the address `[::]:0` is a reasonable default to maximize the ability to accept connections
    /// from any address.
    ///
    /// Some environments may not allow creation of dual-stack sockets, in which case an IPv6
    /// server will only be able to accept connections from IPv6 clients. An IPv4 server is never
    /// dual-stack.
    #[cfg(all(
        not(wasm_browser),
        any(feature = "runtime-tokio", feature = "runtime-smol"),
        any(feature = "aws-lc-rs", feature = "ring"), // `EndpointConfig::default()` is only available with these
    ))]
    pub fn server(config: ServerConfig, addr: SocketAddr) -> io::Result<Self> {
        let socket = Socket::new(Domain::for_address(addr), Type::DGRAM, Some(Protocol::UDP))?;
        if addr.is_ipv6()
            && let Err(e) = socket.set_only_v6(false)
        {
            tracing::debug!(%e, "unable to make socket dual-stack");
        }
        socket.bind(&addr.into())?;
        let runtime =
            default_runtime().ok_or_else(|| io::Error::other("no async runtime found"))?;
        Self::new_with_abstract_socket(
            EndpointConfig::default(),
            Some(config),
            runtime.wrap_udp_socket(socket.into())?,
            runtime,
        )
    }

    /// Construct an endpoint with arbitrary configuration and socket
    #[cfg(not(wasm_browser))]
    pub fn new(
        config: EndpointConfig,
        server_config: Option<ServerConfig>,
        socket: std::net::UdpSocket,
        runtime: Arc<dyn Runtime>,
    ) -> io::Result<Self> {
        let socket = runtime.wrap_udp_socket(socket)?;
        Self::new_with_abstract_socket(config, server_config, socket, runtime)
    }

    /// Construct an endpoint with arbitrary configuration and pre-constructed abstract socket
    ///
    /// Useful when `socket` has additional state (e.g. sidechannels) attached for which shared
    /// ownership is needed.
    pub fn new_with_abstract_socket(
        config: EndpointConfig,
        server_config: Option<ServerConfig>,
        socket: Box<dyn AsyncUdpSocket>,
        runtime: Arc<dyn Runtime>,
    ) -> io::Result<Self> {
        let addr = socket.local_addr()?;
        let allow_mtud = !socket.may_fragment();
        let inner = proto::Endpoint::new(Arc::new(config), server_config.map(Arc::new), allow_mtud);
        let sockets = Sockets::Single {
            buffer: RecvBuffer::new(&inner, socket.max_receive_segments()),
            socket,
            prev_socket: None,
        };
        let rc = EndpointRef::new(sockets, inner, addr.is_ipv6(), runtime.clone());
        Ok(Self::spawn_driver(rc, runtime))
    }

    /// Construct an endpoint that receives and sends on several sockets
    ///
    /// Each socket comes with its routes, which pick the socket for each transmit, see
    /// [`SocketRoute`]. `sockets` may be empty, and [`Endpoint::add_socket`] adds more later.
    /// [`Runtime::wrap_udp_socket_next`] wraps a [`std::net::UdpSocket`].
    ///
    /// The endpoint keeps remote addresses as IPv6: receiving on an IPv4 socket reports
    /// IPv4-mapped addresses, and the senders of IPv4 sockets map them back. MTU discovery is
    /// allowed for new connections while no socket may fragment.
    ///
    /// Each receiver of a socket is polled by its own task. When a receiver's `poll_recv`
    /// returns an error, the endpoint stops receiving from it. It does not stop itself: close
    /// it when it can no longer reach anything.
    ///
    /// # Errors
    ///
    /// Fails if a socket cannot report its local address.
    #[allow(clippy::type_complexity)] // A public type for the pair would only add API surface.
    pub fn new_with_multiple_sockets(
        config: EndpointConfig,
        server_config: Option<ServerConfig>,
        sockets: Vec<(Box<dyn AsyncUdpSocketNext>, Vec<SocketRoute>)>,
        runtime: Arc<dyn Runtime>,
    ) -> io::Result<Self> {
        // Check all sockets first, so that no receive task starts for an endpoint that fails.
        let sockets = sockets
            .into_iter()
            .map(|(socket, routes)| Ok((socket.local_addr()?, socket, routes)))
            .collect::<io::Result<Vec<_>>>()?;
        let inner = proto::Endpoint::new(Arc::new(config), server_config.map(Arc::new), true);
        let multi = Sockets::Multi {
            routes: Arc::default(),
            receivers: Vec::new(),
        };
        let rc = EndpointRef::new(multi, inner, true, runtime.clone());
        {
            let mut state = rc.state.lock().unwrap();
            for (local_addr, socket, routes) in sockets {
                state.add_socket(&rc.0, socket.into(), local_addr, routes);
            }
        }
        Ok(Self::spawn_driver(rc, runtime))
    }

    /// Add a socket to an endpoint created with [`Endpoint::new_with_multiple_sockets`]
    ///
    /// The socket's routes apply to new and existing connections.
    ///
    /// # Errors
    ///
    /// Fails with [`io::ErrorKind::Unsupported`] if the endpoint has a single socket, and fails
    /// if the socket cannot report its local address or the endpoint has stopped.
    pub fn add_socket(
        &self,
        socket: Box<dyn AsyncUdpSocketNext>,
        routes: Vec<SocketRoute>,
    ) -> io::Result<()> {
        let local_addr = socket.local_addr()?;
        let mut state = self.inner.state.lock().unwrap();
        if let Sockets::Single { .. } = state.sockets {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "adding a socket is only supported on an endpoint with multiple sockets",
            ));
        }
        if state.driver_lost {
            return Err(io::Error::other("the endpoint has stopped"));
        }
        state.add_socket(&self.inner.0, socket.into(), local_addr, routes);
        Ok(())
    }

    fn spawn_driver(rc: EndpointRef, runtime: Arc<dyn Runtime>) -> Self {
        let driver = EndpointDriver(rc.clone());
        runtime.spawn(Box::pin(
            async {
                if let Err(e) = driver.await {
                    tracing::error!("I/O error: {}", e);
                }
            }
            .instrument(Span::current()),
        ));
        Self { inner: rc, runtime }
    }

    /// Get the next incoming connection attempt from a client
    ///
    /// Yields [`Incoming`]s, or `None` if the endpoint is [`close`](Self::close)d. [`Incoming`]
    /// can be `await`ed to obtain the final [`Connection`](crate::Connection), or used to e.g.
    /// filter connection attempts or force address validation, or converted into an intermediate
    /// `Connecting` future which can be used to e.g. send 0.5-RTT data.
    pub fn accept(&self) -> Accept<'_> {
        Accept {
            endpoint: self,
            notify: self.inner.shared.incoming.notified(),
        }
    }

    /// Set the client configuration used by `connect`
    pub fn set_default_client_config(&self, config: ClientConfig) {
        self.inner.0.state.lock().unwrap().default_client_config = Some(config);
    }

    /// Connect to a remote endpoint
    ///
    /// `server_name` must be covered by the certificate presented by the server. This prevents a
    /// connection from being intercepted by an attacker with a valid certificate for some other
    /// server.
    ///
    /// May fail immediately due to configuration errors, or in the future if the connection could
    /// not be established.
    pub fn connect(&self, addr: SocketAddr, server_name: &str) -> Result<Connecting, ConnectError> {
        let Some(config) = self
            .inner
            .0
            .state
            .lock()
            .unwrap()
            .default_client_config
            .clone()
        else {
            return Err(ConnectError::NoDefaultClientConfig);
        };

        self.connect_with(config, addr, server_name)
    }

    /// Connect to a remote endpoint using a custom configuration.
    ///
    /// See [`connect()`] for details.
    ///
    /// [`connect()`]: Endpoint::connect
    pub fn connect_with(
        &self,
        config: ClientConfig,
        addr: SocketAddr,
        server_name: &str,
    ) -> Result<Connecting, ConnectError> {
        let mut endpoint = self.inner.state.lock().unwrap();
        if endpoint.driver_lost || endpoint.recv_state.connections.close.is_some() {
            return Err(ConnectError::EndpointStopping);
        }
        if addr.is_ipv6() && !endpoint.ipv6 {
            return Err(ConnectError::InvalidRemoteAddress(addr));
        }
        let addr = if endpoint.ipv6 {
            SocketAddr::V6(ensure_ipv6(addr))
        } else {
            addr
        };

        let (ch, conn) = endpoint
            .inner
            .connect(self.runtime.now(), config, addr, server_name)?;

        let sender = endpoint.sockets.create_sender();
        endpoint.stats.outgoing_handshakes += 1;
        Ok(endpoint
            .recv_state
            .connections
            .insert(ch, conn, sender, self.runtime.clone()))
    }

    /// Switch to a new UDP socket
    ///
    /// See [`Endpoint::rebind_abstract()`] for details.
    #[cfg(not(wasm_browser))]
    pub fn rebind(&self, socket: std::net::UdpSocket) -> io::Result<()> {
        self.rebind_abstract(self.runtime.wrap_udp_socket(socket)?)
    }

    /// Switch to a new UDP socket
    ///
    /// Allows the endpoint's address to be updated live, affecting all active connections. Incoming
    /// connections and connections to servers unreachable from the new address will be lost.
    ///
    /// On error, the old UDP socket is retained.
    ///
    /// # Errors
    ///
    /// Fails with [`io::ErrorKind::Unsupported`] if the endpoint was created with
    /// [`Endpoint::new_with_multiple_sockets`].
    pub fn rebind_abstract(&self, socket: Box<dyn AsyncUdpSocket>) -> io::Result<()> {
        let addr = socket.local_addr()?;
        let mut guard = self.inner.state.lock().unwrap();
        let inner = &mut *guard;
        let (current, prev_socket) = match &mut inner.sockets {
            Sockets::Single {
                socket,
                prev_socket,
                ..
            } => (socket, prev_socket),
            Sockets::Multi { .. } => {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "rebind is not supported on an endpoint with multiple sockets",
                ));
            }
        };
        *prev_socket = Some(mem::replace(current, socket));
        inner.ipv6 = addr.is_ipv6();
        inner.sender = inner.sockets.create_sender();

        // Update connection socket references
        for sender in inner.recv_state.connections.senders.values() {
            // Ignoring errors from dropped connections
            let _ = sender.send(ConnectionEvent::Rebind(inner.sockets.create_sender()));
        }
        if let Some(driver) = inner.driver.take() {
            // Ensure the driver can register for wake-ups from the new socket
            driver.wake();
        }

        Ok(())
    }

    /// Notify connections that the local network address has changed.
    ///
    /// This informs all active connections that the local address may have changed (e.g., due to a
    /// network interface change), triggering liveness checks and recovery procedures on each
    /// connection without requiring a socket rebind.
    ///
    /// Unlike [`Self::rebind`], this does not change the underlying socket. Use this when the
    /// network topology changes but the socket remains valid (e.g., if bound to the unspecified
    /// address and switching from WiFi to cellular, or when the local IP address changes).
    ///
    /// The optional `hint` allows callers to indicate which paths may still be recoverable after
    /// the network change. If `None`, all paths are assumed to be non-recoverable. For client-side
    /// multipath connections, unrecoverable paths will be closed and replaced with new paths to
    /// the same remote addresses.
    pub fn handle_network_change(&self, hint: Option<Arc<dyn NetworkChangeHint + Sync + Send>>) {
        let mut inner = self.inner.state.lock().unwrap();
        for sender in inner.recv_state.connections.senders.values() {
            // Ignoring errors from dropped connections
            let _ = sender.send(ConnectionEvent::LocalAddressChanged(hint.clone()));
        }
        if let Some(driver) = inner.driver.take() {
            driver.wake();
        }
    }

    /// Replace the server configuration, affecting new incoming connections only
    ///
    /// Useful for e.g. refreshing TLS certificates without disrupting existing connections.
    pub fn set_server_config(&self, server_config: Option<ServerConfig>) {
        self.inner
            .state
            .lock()
            .unwrap()
            .inner
            .set_server_config(server_config.map(Arc::new))
    }

    /// Get the local `SocketAddr` the underlying socket is bound to
    ///
    /// For an endpoint with multiple sockets, this is the address of the first socket added,
    /// and fails if it has none.
    pub fn local_addr(&self) -> io::Result<SocketAddr> {
        match &self.inner.state.lock().unwrap().sockets {
            Sockets::Single { socket, .. } => socket.local_addr(),
            Sockets::Multi { routes, .. } => routes.local_addr(),
        }
    }

    /// Get the number of connections that are currently open
    pub fn open_connections(&self) -> usize {
        self.inner.state.lock().unwrap().inner.open_connections()
    }

    /// Close all of this endpoint's connections immediately and cease accepting new connections.
    ///
    /// See [`Connection::close()`] for details.
    ///
    /// [`Connection::close()`]: crate::Connection::close
    pub fn close(&self, error_code: VarInt, reason: &[u8]) {
        let reason = Bytes::copy_from_slice(reason);
        let mut endpoint = self.inner.state.lock().unwrap();
        endpoint.recv_state.connections.close = Some((error_code, reason.clone()));
        for sender in endpoint.recv_state.connections.senders.values() {
            // Ignoring errors from dropped connections
            let _ = sender.send(ConnectionEvent::Close {
                error_code,
                reason: reason.clone(),
            });
        }
        self.inner.shared.incoming.notify_waiters();
        // Wake the endpoint driver so that it can shutdown if the endpoint is already drained.
        if let Some(waker) = endpoint.driver.take() {
            waker.wake();
        }
    }

    /// Waits for all connections on the endpoint to be cleanly shut down and drained.
    ///
    /// This is equivalent to [`wait_all_draining()`] with additionally waiting for the connections
    /// to be drained. Please see its documentation for more information.
    ///
    /// Use `wait_idle()` in favor of `wait_all_draining()` if you care about waiting for the
    /// [`Connection`] structs to be dropped.
    ///
    /// [`wait_all_draining()`]: Self::wait_all_draining
    /// [`Connection`]: crate::Connection
    pub async fn wait_idle(&self) {
        loop {
            {
                let endpoint = &mut *self.inner.state.lock().unwrap();
                if endpoint.recv_state.connections.is_empty() {
                    break;
                }
                // Construct future while lock is held to avoid race
                self.inner.shared.idle.notified()
            }
            .await;
        }
    }

    /// Waits for all connections on the endpoint to be ready for shutting down.
    ///
    /// Waiting for this condition before exiting ensures that a good-faith effort is made to notify
    /// peers of recent connection closes, whereas exiting immediately could force them to wait out
    /// the idle timeout period.
    ///
    /// Does not proactively close existing connections or cause incoming connections to be
    /// rejected. Consider calling [`close()`] if that is desired.
    ///
    /// Unlike [`wait_idle()`], this doesn't wait for the full draining period, so it can't be
    /// used to wait for all now-idle [`Connection`]s to be dropped.
    ///
    /// See also this section in the QUIC RFC: <https://datatracker.ietf.org/doc/html/rfc9000#section-10.2-6>
    ///
    /// [`close()`]: Self::close
    /// [`wait_idle()`]: Self::wait_idle
    /// [`Connection`]: crate::Connection
    pub async fn wait_all_draining(&self) {
        loop {
            {
                let endpoint = &mut *self.inner.state.lock().unwrap();
                if endpoint.recv_state.connections.active_connections == 0 {
                    break;
                }
                // Construct future while lock is held to avoid race
                self.inner.shared.all_draining.notified()
            }
            .await;
        }
    }
}

/// Statistics on [Endpoint] activity
#[non_exhaustive]
#[derive(Debug, Default, Copy, Clone)]
pub struct EndpointStats {
    /// Cumulative number of Quic handshakes accepted by this [Endpoint]
    pub accepted_handshakes: u64,
    /// Cumulative number of Quic handshakes sent from this [Endpoint]
    pub outgoing_handshakes: u64,
    /// Cumulative number of Quic handshakes refused on this [Endpoint]
    pub refused_handshakes: u64,
    /// Cumulative number of Quic handshakes ignored on this [Endpoint]
    pub ignored_handshakes: u64,
}

/// A future that drives IO on an endpoint
///
/// This task functions as the switch point between the UDP socket object and the
/// `Endpoint` responsible for routing datagrams to their owning `Connection`.
/// In order to do so, it also facilitates the exchange of different types of events
/// flowing between the `Endpoint` and the tasks managing `Connection`s. As such,
/// running this task is necessary to keep the endpoint's connections running.
///
/// `EndpointDriver` futures terminate when all clones of the `Endpoint` have been dropped, or when
/// an I/O error occurs.
#[must_use = "endpoint drivers must be spawned for I/O to occur"]
#[derive(Debug)]
pub(crate) struct EndpointDriver(pub(crate) EndpointRef);

impl Future for EndpointDriver {
    type Output = Result<(), io::Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut endpoint = self.0.state.lock().unwrap();
        if endpoint.driver.is_none() {
            endpoint.driver = Some(cx.waker().clone());
        }

        let now = endpoint.runtime.now();
        let mut keep_going = false;
        keep_going |= endpoint.drive_recv(cx, now)?;
        keep_going |= endpoint.handle_events(cx, &self.0.shared);

        if !endpoint.recv_state.incoming.is_empty() {
            self.0.shared.incoming.notify_waiters();
        }

        // Stop the driver if either:
        // - all `Endpoint` structs are dropped and all connections are drained,
        // - or `Endpoint::close` has been called and all connections are drained.
        if endpoint.recv_state.connections.is_empty()
            && (self.0.shared.ref_count.load(Ordering::Relaxed) == 0
                || endpoint.recv_state.connections.close.is_some())
        {
            trace!("endpoint driver stopping");
            Poll::Ready(Ok(()))
        } else {
            drop(endpoint);
            // If there is more work to do schedule the endpoint task again.
            // `wake_by_ref()` is called outside the lock to minimize
            // lock contention on a multithreaded runtime.
            if keep_going {
                cx.waker().wake_by_ref();
            }
            Poll::Pending
        }
    }
}

impl Drop for EndpointDriver {
    fn drop(&mut self) {
        let mut endpoint = self.0.state.lock().unwrap();
        endpoint.driver_lost = true;
        if let Sockets::Multi { receivers, .. } = &endpoint.sockets {
            for receiver in receivers {
                receiver.stop();
            }
        }
        self.0.shared.incoming.notify_waiters();
        // Drop all outgoing channels, signaling the termination of the endpoint to the associated
        // connections.
        endpoint.recv_state.connections.senders.clear();
        endpoint.recv_state.connections.active_connections = 0;
        // Wake callers waiting for the connection state cleared above.
        self.0.shared.all_draining.notify_waiters();
        self.0.shared.idle.notify_waiters();
    }
}

#[derive(Debug)]
pub(crate) struct EndpointInner {
    pub(crate) state: Mutex<State>,
    pub(crate) shared: Shared,
}

impl EndpointInner {
    pub(crate) fn accept(
        &self,
        incoming: proto::Incoming,
        server_config: Option<Arc<ServerConfig>>,
    ) -> Result<Connecting, ConnectionError> {
        let mut state = self.state.lock().unwrap();
        let mut response_buffer = Vec::new();
        let now = state.runtime.now();
        match state
            .inner
            .accept(incoming, now, &mut response_buffer, server_config)
        {
            Ok((handle, conn)) => {
                state.stats.accepted_handshakes += 1;
                let sender = state.sockets.create_sender();
                let runtime = state.runtime.clone();
                Ok(state
                    .recv_state
                    .connections
                    .insert(handle, conn, sender, runtime))
            }
            Err(error) => {
                if let Some(transmit) = error.response {
                    respond(transmit, &response_buffer, &mut state.sender);
                }
                Err(error.cause)
            }
        }
    }

    pub(crate) fn refuse(&self, incoming: proto::Incoming) {
        let mut state = self.state.lock().unwrap();
        state.stats.refused_handshakes += 1;
        let mut response_buffer = Vec::new();
        let transmit = state.inner.refuse(incoming, &mut response_buffer);
        respond(transmit, &response_buffer, &mut state.sender);
    }

    pub(crate) fn retry(&self, incoming: proto::Incoming) -> Result<(), proto::RetryError> {
        let mut state = self.state.lock().unwrap();
        let mut response_buffer = Vec::new();
        let transmit = state.inner.retry(incoming, &mut response_buffer)?;
        respond(transmit, &response_buffer, &mut state.sender);
        Ok(())
    }

    pub(crate) fn ignore(&self, incoming: proto::Incoming) {
        let mut state = self.state.lock().unwrap();
        state.stats.ignored_handshakes += 1;
        state.inner.ignore(incoming);
    }
}

#[derive(Debug)]
pub(crate) struct State {
    sockets: Sockets,
    /// Sends endpoint-generated responses
    sender: ConnectionSender,
    inner: proto::Endpoint,
    recv_state: RecvState,
    driver: Option<Waker>,
    ipv6: bool,
    events: mpsc::UnboundedReceiver<(ConnectionHandle, EndpointEvent)>,
    driver_lost: bool,
    runtime: Arc<dyn Runtime>,
    stats: EndpointStats,
    default_client_config: Option<ClientConfig>,
}

/// The sockets of an endpoint
#[derive(Debug)]
enum Sockets {
    /// One socket, polled by the endpoint driver under the endpoint lock
    Single {
        socket: Box<dyn AsyncUdpSocket>,
        /// The socket before a rebind, received from until the new socket gets traffic
        prev_socket: Option<Box<dyn AsyncUdpSocket>>,
        buffer: RecvBuffer,
    },
    /// Several sockets, each receiver polled by its own [`ReceiverDriver`]
    Multi {
        /// The sockets and their routes, shared with the connections
        routes: Arc<RouteTable>,
        /// Stops the receive tasks when the endpoint driver ends
        receivers: Vec<Arc<ReceiverControl>>,
    },
}

#[derive(Debug)]
pub(crate) struct Shared {
    /// Notifies subscribers of new incoming connections.
    ///
    /// This enables the `Endpoint::accept` API.
    incoming: Notify,
    /// Notifies subscribers when *all* connections have entered the draining state.
    ///
    /// This powers the `Endpoint::wait_idle` API.
    all_draining: Notify,
    /// Notifies subscribesr when *all* connections have been dropped.
    ///
    /// This powers the `Endpoint::wait_drained` API.
    idle: Notify,
    /// Number of live handles that can be used to initiate or handle I/O; excludes the driver
    ref_count: AtomicUsize,
}

impl Sockets {
    /// Creates the sender of a new connection
    fn create_sender(&self) -> ConnectionSender {
        match self {
            Self::Single { socket, .. } => ConnectionSender::single(socket.create_sender()),
            Self::Multi { routes, .. } => ConnectionSender::multi(routes.clone()),
        }
    }
}

impl State {
    /// Adds a socket to an endpoint with multiple sockets and starts its receive tasks
    fn add_socket(
        &mut self,
        endpoint: &Arc<EndpointInner>,
        socket: Arc<dyn AsyncUdpSocketNext>,
        local_addr: SocketAddr,
        routes: Vec<SocketRoute>,
    ) {
        let Sockets::Multi {
            routes: table,
            receivers,
        } = &mut self.sockets
        else {
            unreachable!("only endpoints with multiple sockets add sockets");
        };
        debug!(%local_addr, ?routes, "adding socket");
        *table = Arc::new(table.with_socket(socket.clone(), local_addr, routes).0);
        self.inner.set_allow_mtud(!table.may_fragment());
        self.sender.set_routes(table.clone());
        for sender in self.recv_state.connections.senders.values() {
            // Ignoring errors from dropped connections
            let _ = sender.send(ConnectionEvent::RoutesChanged(table.clone()));
        }

        let count = socket.max_receivers().min(MAX_RECEIVERS_PER_SOCKET);
        for _ in 0..count {
            let Some(receiver) = socket.create_receiver() else {
                break;
            };
            let control = Arc::new(ReceiverControl::default());
            receivers.push(control.clone());
            let driver = ReceiverDriver {
                endpoint: endpoint.clone(),
                buffer: RecvBuffer::new(&self.inner, receiver.max_receive_segments()),
                receiver,
                control,
                runtime: self.runtime.clone(),
            };
            self.runtime
                .spawn(Box::pin(driver.instrument(Span::current())));
        }
    }

    fn drive_recv(&mut self, cx: &mut Context<'_>, now: Instant) -> Result<bool, io::Error> {
        let Sockets::Single {
            socket: current,
            prev_socket,
            buffer,
        } = &mut self.sockets
        else {
            // Each socket has its own receive task.
            return Ok(false);
        };
        let runtime = &*self.runtime;
        let (recv_state, inner, sender) = (&mut self.recv_state, &mut self.inner, &mut self.sender);
        let get_time = || runtime.now();
        buffer.limiter.start_cycle(get_time);
        let mut poll = |socket: &mut dyn AsyncUdpSocket, received_connection_packet: &mut bool| {
            buffer.poll_recv(cx, socket, runtime, &mut |metas, datagrams| {
                *received_connection_packet |=
                    recv_state.handle_batch(inner, sender, now, metas, datagrams);
                true
            })
        };
        if let Some(socket) = prev_socket {
            // We don't care about the progress of old sockets.
            if poll(&mut **socket, &mut false).is_err() {
                *prev_socket = None;
            }
        };
        let mut received_connection_packet = false;
        let keep_going = poll(&mut **current, &mut received_connection_packet);
        buffer.limiter.finish_cycle(get_time);
        let keep_going = keep_going?;
        if received_connection_packet {
            // Traffic has arrived on self.socket, therefore there is no need for the abandoned
            // one anymore. TODO: Account for multiple outgoing connections.
            *prev_socket = None;
        }
        Ok(keep_going)
    }

    fn handle_events(&mut self, cx: &mut Context<'_>, shared: &Shared) -> bool {
        for _ in 0..IO_LOOP_BOUND {
            let (ch, event) = match self.events.poll_recv(cx) {
                Poll::Ready(Some(x)) => x,
                Poll::Ready(None) => unreachable!("EndpointInner owns one sender"),
                Poll::Pending => {
                    return false;
                }
            };

            if event.is_draining() {
                self.recv_state.connections.active_connections -= 1;
                if self.recv_state.connections.active_connections == 0 {
                    shared.all_draining.notify_waiters();
                }
            } else if event.is_drained() {
                self.recv_state.connections.senders.remove(&ch);
                if self.recv_state.connections.is_empty() {
                    shared.idle.notify_waiters();
                }
            }
            let Some(event) = self.inner.handle_event(ch, event) else {
                continue;
            };
            // Ignoring errors from dropped connections that haven't yet been cleaned up
            let _ = self
                .recv_state
                .connections
                .senders
                .get_mut(&ch)
                .unwrap()
                .send(ConnectionEvent::Proto(event));
        }

        true
    }
}

impl Drop for State {
    fn drop(&mut self) {
        for incoming in self.recv_state.incoming.drain(..) {
            self.inner.ignore(incoming);
        }
    }
}

fn respond(transmit: proto::Transmit, response_buffer: &[u8], sender: &mut ConnectionSender) {
    // Send if there's kernel buffer space; otherwise, drop it
    //
    // As an endpoint-generated packet, we know this is an
    // immediate, stateless response to an unconnected peer,
    // one of:
    //
    // - A version negotiation response due to an unknown version
    // - A `CLOSE` due to a malformed or unwanted connection attempt
    // - A stateless reset due to an unrecognized connection
    // - A `Retry` packet due to a connection attempt when `use_retry` is set
    //
    // In each case, a well-behaved peer can be trusted to retry a
    // few times, which is guaranteed to produce the same response
    // from us. Repeated failures might at worst cause a peer's new
    // connection attempt to time out, which is acceptable if we're
    // under such heavy load that there's never room for this code
    // to transmit. This is morally equivalent to the packet getting
    // lost due to congestion further along the link, which
    // similarly relies on peer retries for recovery.

    // Copied from rust 1.85's std::task::Waker::noop() implementation for backwards compatibility
    const NOOP: RawWaker = {
        const VTABLE: RawWakerVTable = RawWakerVTable::new(
            // Cloning just returns a new no-op raw waker
            |_| NOOP,
            // `wake` does nothing
            |_| {},
            // `wake_by_ref` does nothing
            |_| {},
            // Dropping does nothing as we don't allocate anything
            |_| {},
        );
        RawWaker::new(std::ptr::null(), &VTABLE)
    };
    // SAFETY: Copied from rust stdlib, the NOOP waker is thread-safe and doesn't violate the
    // RawWakerVTable contract, it doesn't access the data pointer at all.
    let waker = unsafe { Waker::from_raw(NOOP) };
    let mut cx = Context::from_waker(&waker);
    _ = sender.send_response(
        &udp_transmit(&transmit, &response_buffer[..transmit.size]),
        &mut cx,
    );
}

#[inline]
fn proto_ecn(ecn: udp::EcnCodepoint) -> proto::EcnCodepoint {
    match ecn {
        udp::EcnCodepoint::Ect0 => proto::EcnCodepoint::Ect0,
        udp::EcnCodepoint::Ect1 => proto::EcnCodepoint::Ect1,
        udp::EcnCodepoint::Ce => proto::EcnCodepoint::Ce,
    }
}

#[derive(Debug)]
struct ConnectionSet {
    /// Senders for communicating with the endpoint's connections
    senders: FxHashMap<ConnectionHandle, mpsc::UnboundedSender<ConnectionEvent>>,
    /// Stored to give out clones to new ConnectionInners
    sender: mpsc::UnboundedSender<(ConnectionHandle, EndpointEvent)>,
    /// Set if the endpoint has been manually closed
    close: Option<(VarInt, Bytes)>,
    /// Counter for all active (non-draining/drained) connections.
    ///
    /// This is directly related to the QUIC connection states "Initial", "Handshake",
    /// "Established", "Closed", "Draining" and "Drained" (see also
    /// `proto/src/connection/state.rs`).
    ///
    /// Any connection state that is not "Draining" or "Drained" is considered active.
    ///
    /// This counter is updated when new connections are added ([`ConnectionSet::insert`]) and when
    /// a connection informs us about entering the draining state ([`State::handle_events`]).
    active_connections: u64,
}

impl ConnectionSet {
    fn insert(
        &mut self,
        handle: ConnectionHandle,
        conn: proto::Connection,
        sender: ConnectionSender,
        runtime: Arc<dyn Runtime>,
    ) -> Connecting {
        let (send, recv) = mpsc::unbounded_channel();
        if let Some((error_code, ref reason)) = self.close {
            send.send(ConnectionEvent::Close {
                error_code,
                reason: reason.clone(),
            })
            .unwrap();
        }
        self.senders.insert(handle, send);
        self.active_connections += 1;
        Connecting::new(handle, conn, self.sender.clone(), recv, sender, runtime)
    }

    fn is_empty(&self) -> bool {
        self.senders.is_empty()
    }
}

pub(crate) fn ensure_ipv6(x: SocketAddr) -> SocketAddrV6 {
    match x {
        SocketAddr::V6(x) => x,
        SocketAddr::V4(x) => SocketAddrV6::new(x.ip().to_ipv6_mapped(), x.port(), 0, 0),
    }
}

pin_project! {
    /// Future produced by [`Endpoint::accept`]
    pub struct Accept<'a> {
        endpoint: &'a Endpoint,
        #[pin]
        notify: Notified<'a>,
    }
}

impl Future for Accept<'_> {
    type Output = Option<Incoming>;
    fn poll(self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        let mut endpoint = this.endpoint.inner.state.lock().unwrap();
        if endpoint.driver_lost {
            return Poll::Ready(None);
        }
        if let Some(incoming) = endpoint.recv_state.incoming.pop_front() {
            // Release the mutex lock on endpoint so cloning it doesn't deadlock
            drop(endpoint);
            let incoming = Incoming::new(incoming, this.endpoint.inner.clone());
            return Poll::Ready(Some(incoming));
        }
        if endpoint.recv_state.connections.close.is_some() {
            return Poll::Ready(None);
        }
        loop {
            match this.notify.as_mut().poll(ctx) {
                // `state` lock ensures we didn't race with readiness
                Poll::Pending => return Poll::Pending,
                // Spurious wakeup, get a new future
                Poll::Ready(()) => this
                    .notify
                    .set(this.endpoint.inner.shared.incoming.notified()),
            }
        }
    }
}

#[derive(Debug)]
pub(crate) struct EndpointRef(Arc<EndpointInner>);

impl EndpointRef {
    fn new(
        sockets: Sockets,
        inner: proto::Endpoint,
        ipv6: bool,
        runtime: Arc<dyn Runtime>,
    ) -> Self {
        let sender = sockets.create_sender();
        let (events_sender, events) = mpsc::unbounded_channel();
        let recv_state = RecvState::new(events_sender);
        Self(Arc::new(EndpointInner {
            shared: Shared {
                incoming: Notify::new(),
                all_draining: Notify::new(),
                idle: Notify::new(),
                ref_count: AtomicUsize::new(0),
            },
            state: Mutex::new(State {
                sockets,
                sender,
                inner,
                ipv6,
                events,
                driver: None,
                driver_lost: false,
                recv_state,
                runtime,
                stats: EndpointStats::default(),
                default_client_config: None,
            }),
        }))
    }
}

impl Clone for EndpointRef {
    fn clone(&self) -> Self {
        self.0.shared.ref_count.fetch_add(1, Ordering::Relaxed);
        Self(self.0.clone())
    }
}

impl Drop for EndpointRef {
    fn drop(&mut self) {
        if self.0.shared.ref_count.fetch_sub(1, Ordering::Relaxed) > 1 {
            return;
        }

        let endpoint = &mut *self.0.state.lock().unwrap();
        // If the driver is about to be on its own, ensure it can shut down if the last
        // connection is gone.
        if let Some(task) = endpoint.driver.take() {
            task.wake();
        }
    }
}

impl std::ops::Deref for EndpointRef {
    type Target = EndpointInner;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

/// State directly involved in handling incoming packets
#[derive(Debug)]
struct RecvState {
    incoming: VecDeque<proto::Incoming>,
    connections: ConnectionSet,
}

impl RecvState {
    fn new(sender: mpsc::UnboundedSender<(ConnectionHandle, EndpointEvent)>) -> Self {
        Self {
            connections: ConnectionSet {
                senders: FxHashMap::default(),
                sender,
                close: None,
                active_connections: 0,
            },
            incoming: VecDeque::new(),
        }
    }

    /// Hands a batch of received datagrams to the endpoint
    ///
    /// Returns whether a datagram was routed to an existing connection.
    fn handle_batch(
        &mut self,
        endpoint: &mut proto::Endpoint,
        sender: &mut ConnectionSender,
        now: Instant,
        metas: &[RecvMeta],
        datagrams: &mut BytesMut,
    ) -> bool {
        let mut received_connection_packet = false;
        for meta in metas {
            let mut data = datagrams.split_to(meta.len);
            while !data.is_empty() {
                let buf = data.split_to(meta.stride.min(data.len()));
                let mut response_buffer = Vec::new();
                let addresses = FourTuple::new(meta.addr, meta.dst_ip);
                match endpoint.handle(
                    now,
                    addresses,
                    meta.ecn.map(proto_ecn),
                    buf,
                    &mut response_buffer,
                ) {
                    Some(DatagramEvent::NewConnection(incoming)) => {
                        if self.connections.close.is_none() {
                            self.incoming.push_back(incoming);
                        } else {
                            let transmit = endpoint.refuse(incoming, &mut response_buffer);
                            respond(transmit, &response_buffer, sender);
                        }
                    }
                    Some(DatagramEvent::ConnectionEvent(handle, event)) => {
                        // Ignoring errors from dropped connections that haven't yet
                        // been cleaned up
                        received_connection_packet = true;
                        let _ = self
                            .connections
                            .senders
                            .get_mut(&handle)
                            .unwrap()
                            .send(ConnectionEvent::Proto(event));
                    }
                    Some(DatagramEvent::Response(transmit)) => {
                        respond(transmit, &response_buffer, sender);
                    }
                    None => {}
                }
            }
        }
        received_connection_packet
    }
}

/// The receive buffer and work limiter of one socket or receiver
struct RecvBuffer {
    buf: Box<[u8]>,
    /// Reused buffer that received datagrams are copied into and split into owned slices
    datagrams: BytesMut,
    limiter: WorkLimiter,
}

impl RecvBuffer {
    fn new(endpoint: &proto::Endpoint, max_receive_segments: NonZeroUsize) -> Self {
        let datagram_capacity = endpoint.config().get_max_udp_payload_size().min(64 * 1024)
            as usize
            * max_receive_segments.get();
        Self {
            buf: vec![0; datagram_capacity * BATCH_SIZE].into(),
            datagrams: BytesMut::with_capacity(datagram_capacity),
            limiter: WorkLimiter::new(RECV_TIME_BOUND),
        }
    }

    /// Receives from `socket` and passes each batch to `dispatch`
    ///
    /// Stops when the socket has nothing more, `dispatch` returns `false`, or the work limiter
    /// asks to yield. Returns whether there is more work to do.
    fn poll_recv<S: PollRecv + ?Sized>(
        &mut self,
        cx: &mut Context<'_>,
        socket: &mut S,
        runtime: &dyn Runtime,
        dispatch: &mut dyn FnMut(&mut [RecvMeta], &mut BytesMut) -> bool,
    ) -> io::Result<bool> {
        let mut metas = [RecvMeta::default(); BATCH_SIZE];
        let mut iovs: [IoSliceMut<'_>; BATCH_SIZE] = {
            let mut bufs = self
                .buf
                .chunks_mut(self.buf.len() / BATCH_SIZE)
                .map(IoSliceMut::new);

            // expect() safe as self.buf is chunked into BATCH_SIZE items
            // and iovs will be of size BATCH_SIZE, thus from_fn is called
            // exactly BATCH_SIZE times.
            std::array::from_fn(|_| bufs.next().expect("BATCH_SIZE elements"))
        };
        loop {
            match socket.poll_recv(cx, &mut iovs, &mut metas) {
                Poll::Ready(Ok(msgs)) => {
                    self.limiter.record_work(msgs);
                    // Copy the received batch into `datagrams`, so that each datagram can be
                    // split off as an owned slice. We can't read directly into `datagrams` and
                    // drop `buf`: once a slice has been handed to a connection the allocation is
                    // shared, so the next `reserve` must allocate fresh backing memory. Reading
                    // in place would therefore mean reserving the worst-case iovec envelope
                    // before every poll and reallocating it whenever a prior datagram is still
                    // in flight. `buf` stays an allocate-once scratch buffer, and we reserve
                    // only the bytes actually received.
                    let batch_len = metas.iter().take(msgs).map(|meta| meta.len).sum();
                    self.datagrams.reserve(batch_len);
                    for (meta, buf) in metas.iter().zip(iovs.iter()).take(msgs) {
                        self.datagrams.extend_from_slice(&buf[..meta.len]);
                    }
                    if !dispatch(&mut metas[..msgs], &mut self.datagrams) {
                        return Ok(false);
                    }
                }
                Poll::Pending => return Ok(false),
                // Ignore ECONNRESET as it's undefined in QUIC and may be injected by an
                // attacker
                Poll::Ready(Err(ref e)) if e.kind() == io::ErrorKind::ConnectionReset => {
                    continue;
                }
                // Ignore EMSGSIZE as we're currently not handling ICMPv4 Fragmentation Needed
                // and ICMPv6 Packet Too Big (PTB) messages since they cannot be authenticated,
                // and Datagram Packetization Layer Path MTU Discovery (DPLPMTUD) works without
                // it anyways.
                Poll::Ready(Err(ref e)) if is_msg_size_err(e) => {
                    continue;
                }
                Poll::Ready(Err(e)) => return Err(e),
            }
            if !self.limiter.allow_work(|| runtime.now()) {
                return Ok(true);
            }
        }
    }
}

impl fmt::Debug for RecvBuffer {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // buf too large
        f.debug_struct("RecvBuffer")
            .field("limiter", &self.limiter)
            .finish_non_exhaustive()
    }
}

/// Receives datagrams from either kind of socket
trait PollRecv {
    fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>>;
}

impl PollRecv for dyn AsyncUdpSocket {
    fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        AsyncUdpSocket::poll_recv(self, cx, bufs, meta)
    }
}

impl PollRecv for dyn UdpReceiver {
    fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
        bufs: &mut [IoSliceMut<'_>],
        meta: &mut [RecvMeta],
    ) -> Poll<io::Result<usize>> {
        UdpReceiver::poll_recv(self, cx, bufs, meta)
    }
}

/// The most receivers created per socket of an endpoint with multiple sockets
const MAX_RECEIVERS_PER_SOCKET: usize = 1;

/// Stops a [`ReceiverDriver`]
#[derive(Debug, Default)]
struct ReceiverControl {
    stopped: AtomicBool,
    /// The waker of the parked driver
    waker: AtomicWaker,
}

impl ReceiverControl {
    fn stop(&self) {
        // Set the flag before waking. A driver that parks concurrently registers its waker
        // first and checks the flag after, so it either sees the flag or gets woken.
        self.stopped.store(true, Ordering::Release);
        self.waker.wake();
    }
}

/// Receives from one receiver of a socket of an endpoint with multiple sockets
///
/// One of these runs per receiver. It owns the receiver and polls it without the endpoint
/// lock. It takes the lock only to hand a received batch to the endpoint.
struct ReceiverDriver {
    endpoint: Arc<EndpointInner>,
    receiver: Box<dyn UdpReceiver>,
    buffer: RecvBuffer,
    control: Arc<ReceiverControl>,
    runtime: Arc<dyn Runtime>,
}

impl Future for ReceiverDriver {
    type Output = ();

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        let this = &mut *self;
        if this.control.stopped.load(Ordering::Acquire) {
            return Poll::Ready(());
        }
        let (endpoint, runtime) = (&this.endpoint, &*this.runtime);
        let mut endpoint_gone = false;
        let get_time = || runtime.now();
        this.buffer.limiter.start_cycle(get_time);
        let result =
            this.buffer
                .poll_recv(cx, &mut *this.receiver, runtime, &mut |metas, datagrams| {
                    let now = runtime.now();
                    let mut guard = endpoint.state.lock().unwrap();
                    if guard.driver_lost {
                        endpoint_gone = true;
                        return false;
                    }
                    let state = &mut *guard;
                    // The endpoint keeps addresses as IPv6.
                    for meta in metas.iter_mut() {
                        meta.addr = SocketAddr::V6(ensure_ipv6(meta.addr));
                    }
                    let (inner, sender) = (&mut state.inner, &mut state.sender);
                    state
                        .recv_state
                        .handle_batch(inner, sender, now, metas, datagrams);
                    let has_incoming = !state.recv_state.incoming.is_empty();
                    drop(guard);
                    if has_incoming {
                        endpoint.shared.incoming.notify_waiters();
                    }
                    true
                });
        this.buffer.limiter.finish_cycle(get_time);
        match result {
            _ if endpoint_gone => Poll::Ready(()),
            Ok(true) => {
                // Yield for fairness and continue in the next poll.
                cx.waker().wake_by_ref();
                Poll::Pending
            }
            Ok(false) => {
                this.control.waker.register(cx.waker());
                if this.control.stopped.load(Ordering::Acquire) {
                    Poll::Ready(())
                } else {
                    Poll::Pending
                }
            }
            Err(e) => {
                warn!(error = %e, "receiver failed, no longer receiving from it");
                Poll::Ready(())
            }
        }
    }
}
