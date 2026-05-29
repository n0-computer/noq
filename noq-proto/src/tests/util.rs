use std::{
    cmp,
    collections::{HashMap, HashSet, VecDeque},
    io::{self, Write},
    mem,
    net::{Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
    num::{NonZeroU32, NonZeroUsize},
    str,
    sync::{Arc, LazyLock},
};

use assert_matches::assert_matches;
use bytes::BytesMut;
use ipnet::IpNet;
use rand::{SeedableRng, rngs::StdRng};
use rustls::{
    KeyLogFile,
    client::WebPkiServerVerifier,
    pki_types::{CertificateDer, PrivateKeyDer},
};
use tracing::{debug, error, info_span, trace};

use super::crypto::rustls::{QuicClientConfig, QuicServerConfig, configured_provider};
use super::*;
use crate::{Duration, Instant, congestion::Controller};

pub(super) const DEFAULT_MTU: usize = 1452;

/// The last octet for an IP address belonging to the server.
///
/// Consistently using this makes endpoint addresses easier to recognise. Generally an
/// endpoint only needs a single IP per subnet so this is sufficient to identify it.
const IP_LAST_OCTET_SERVER: u8 = 1;

/// The port for a socket address belonging to the server.
///
/// Consistently using this makes server addresses easier to recognise.
const PORT_SERVER: u16 = 11;

/// The last octet for an IP address belonging to the client.
///
/// Consistently using this makes endpoint addresses easier to recognise. Generally an
/// endpoint only needs a single IP per subnet so this is sufficient to identify it.
const IP_LAST_OCTET_CLIENT: u8 = 2;

/// The port for a socket address belonging to the client.
///
/// Consistently using this makes endpoint addresses easier to recognise.
const PORT_CLIENT: u16 = 22;

/// A port we use for NAT mappings to make those recognisable.
///
/// Currently we only have a single port mapping, in the future this could be extended to be
/// a range, e.g. 80-89.
const NAT_MAPPING_PORT: u16 = 80;

pub(super) struct Pair {
    pub(super) server: TestEndpoint,
    pub(super) client: TestEndpoint,
    /// Start time
    epoch: Instant,
    /// Current time
    pub(super) time: Instant,
    /// Simulates the maximum size allowed for UDP payloads by the link (packets exceeding this size will be dropped)
    pub(super) mtu: usize,
    /// Simulates explicit congestion notification
    pub(super) congestion_experienced: bool,
    // One-way
    pub(super) latency: Duration,
    /// Number of spin bit flips
    pub(super) spins: u64,
    /// The routing table used for resolving addresses observed for incoming packets
    /// and determining whether they should get lost.
    pub(super) routes: Routing,
    last_spin: bool,
}

impl Pair {
    /// The default client address of the pair.
    ///
    /// IPv6 address `::1:1`. This is a normal unicast address.
    /// - `::1:1` is for the first client address.
    /// - `::1:0/112` is the subnet used for all client addresses.
    pub(super) const CLIENT_ADDR: SocketAddr =
        SocketAddr::new(IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 1, 1)), 1);
    /// The default server address of the pair.
    ///
    /// IPv6 address `::2:1`. This is a normal unicast address.
    /// - `::2:1` is for the first server address.
    /// - `::2:0/112` is the subnet used for all server addresses.
    pub(super) const SERVER_ADDR: SocketAddr =
        SocketAddr::new(IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 2, 1)), 1);

    /// Creates an endpoint pair that'll run deterministically with hardcoded addresses.
    pub(super) fn seeded(seed: [u8; 32]) -> Self {
        let mut rng = StdRng::from_seed(seed);
        let mut client_seed = [0u8; 32];
        let mut server_seed = [0u8; 32];
        rng.fill_bytes(&mut client_seed);
        rng.fill_bytes(&mut server_seed);

        let mut cfg = server_config();
        let mut transport = TransportConfig::default();
        transport.deterministic_packet_numbers(true);
        cfg.transport = Arc::new(transport);

        let mut client_config = EndpointConfig::default();
        let mut server_config = EndpointConfig::default();
        client_config.rng_seed(Some(client_seed));
        server_config.rng_seed(Some(server_seed));

        let server = Endpoint::new(Arc::new(client_config), Some(Arc::new(cfg)), true);
        let client = Endpoint::new(Arc::new(server_config), None, true);

        let server_addr = SocketAddr::new(Ipv6Addr::LOCALHOST.into(), 4433);
        let client_addr = SocketAddr::new(Ipv6Addr::LOCALHOST.into(), 44433);
        let now = Instant::now();
        Self {
            server: TestEndpoint::new(server),
            client: TestEndpoint::new(client),
            epoch: now,
            time: now,
            mtu: DEFAULT_MTU,
            latency: Duration::from_millis(1),
            spins: 0,
            last_spin: false,
            congestion_experienced: false,
            routes: BasicRouting {
                client_addr,
                server_addr,
            }
            .into(),
        }
    }

    pub(super) fn default_with_deterministic_pns() -> Self {
        let mut cfg = server_config();
        let mut transport = TransportConfig::default();
        transport.deterministic_packet_numbers(true);
        cfg.transport = Arc::new(transport);
        Self::new(Default::default(), cfg)
    }

    pub(super) fn new(endpoint_config: Arc<EndpointConfig>, server_config: ServerConfig) -> Self {
        let server = Endpoint::new(endpoint_config.clone(), Some(Arc::new(server_config)), true);
        let client = Endpoint::new(endpoint_config, None, true);

        Self::new_from_endpoint(client, server)
    }

    pub(super) fn new_from_endpoint(client: Endpoint, server: Endpoint) -> Self {
        let now = Instant::now();
        Self {
            server: TestEndpoint::new(server),
            client: TestEndpoint::new(client),
            epoch: now,
            time: now,
            mtu: DEFAULT_MTU,
            latency: Duration::ZERO,
            spins: 0,
            last_spin: false,
            congestion_experienced: false,
            routes: BasicRouting {
                client_addr: Self::CLIENT_ADDR,
                server_addr: Self::SERVER_ADDR,
            }
            .into(),
        }
    }

    /// Returns whether the connection is not idle
    pub(super) fn step(&mut self) -> bool {
        self.blackhole_step(false, false)
    }

    /// Drive both endpoints once, optionally preventing them from receiving traffic.
    ///
    /// Returns `false` if the connection is idle after the step.
    pub(super) fn blackhole_step(
        &mut self,
        server_blackhole: bool,
        client_blackhole: bool,
    ) -> bool {
        self.drive_client();
        if server_blackhole {
            self.server.inbound.clear();
        }
        self.drive_server();
        if client_blackhole {
            self.client.inbound.clear();
        }
        if self.client.is_idle() && self.server.is_idle() {
            return false;
        }

        self.advance_time()
    }

    /// Advance time until both connections are idle
    pub(super) fn drive(&mut self) {
        while self.step() {}
    }

    /// Advance time until both connections are idle, or after 100 steps have been executed
    ///
    /// Returns true if the amount of steps exceeds the bounds, because the connections never became
    /// idle
    pub(super) fn drive_bounded(&mut self, iters: usize) -> bool {
        for _ in 0..iters {
            if !self.step() {
                return false;
            }
        }

        true
    }

    pub(super) fn drive_client(&mut self) {
        let span = info_span!("client");
        let _guard = span.enter();
        self.client.drive(self.time);
        for (packet, buffer) in self.client.outbound.drain(..) {
            let packet_size = packet_size(&packet, &buffer);
            if packet_size > self.mtu {
                info!(packet_size, "dropping packet (max size exceeded)");
                continue;
            }
            if buffer[0] & packet::LONG_HEADER_FORM == 0 {
                let spin = buffer[0] & packet::SPIN_BIT != 0;
                self.spins += (spin == self.last_spin) as u64;
                self.last_spin = spin;
            }
            match self.routes.route_client_to_server(&packet) {
                RoutingDecision::Deliver { src, dst } => {
                    let ecn = set_congestion_experienced(packet.ecn, self.congestion_experienced);
                    self.server.inbound.push_back(Inbound {
                        recv_time: self.time + self.latency,
                        ecn,
                        packet: buffer.as_ref().into(),
                        remote: src,
                        dst_ip: dst,
                    });
                }
                RoutingDecision::Drop => {
                    debug!(?packet.destination, "no route from client to server for packet");
                }
            }
        }
    }

    pub(super) fn drive_server(&mut self) {
        let span = info_span!("server");
        let _guard = span.enter();
        self.server.drive(self.time);
        for (packet, buffer) in self.server.outbound.drain(..) {
            let packet_size = packet_size(&packet, &buffer);
            if packet_size > self.mtu {
                info!(packet_size, "dropping packet (max size exceeded)");
                continue;
            }
            match self.routes.route_server_to_client(&packet) {
                RoutingDecision::Deliver { src, dst } => {
                    let ecn = set_congestion_experienced(packet.ecn, self.congestion_experienced);
                    self.client.inbound.push_back(Inbound {
                        recv_time: self.time + self.latency,
                        ecn,
                        packet: buffer.as_ref().into(),
                        remote: src,
                        dst_ip: dst,
                    });
                }
                RoutingDecision::Drop => {
                    debug!(?packet.destination, "no route from server to client for packet");
                }
            }
        }
    }

    pub(super) fn advance_time(&mut self) -> bool {
        let client_t = self.client.next_wakeup();
        let server_t = self.server.next_wakeup();
        match min_opt(client_t, server_t) {
            Some(t) if Some(t) == client_t => {
                if t != self.time {
                    self.time = self.time.max(t);
                    trace!("advancing to {:?} for client", self.time - self.epoch);
                }
                true
            }
            Some(t) if Some(t) == server_t => {
                if t != self.time {
                    self.time = self.time.max(t);
                    trace!("advancing to {:?} for server", self.time - self.epoch);
                }
                true
            }
            Some(_) => unreachable!(),
            None => false,
        }
    }

    pub(super) fn connect(&mut self) -> (ConnectionHandle, ConnectionHandle) {
        self.connect_with(client_config())
    }

    pub(super) fn connect_with(
        &mut self,
        config: ClientConfig,
    ) -> (ConnectionHandle, ConnectionHandle) {
        info!("connecting");
        let client_ch = self.begin_connect(config);
        self.drive();
        let server_ch = self.server.assert_accept();
        self.finish_connect(client_ch, server_ch);
        (client_ch, server_ch)
    }

    /// Just start connecting the client
    pub(super) fn begin_connect(&mut self, config: ClientConfig) -> ConnectionHandle {
        let span = info_span!("client");
        let _guard = span.enter();
        let (client_ch, client_conn) = self
            .client
            .connect(
                self.time,
                config,
                self.routes.public_server_addr(),
                "localhost",
            )
            .unwrap();
        self.client.connections.insert(client_ch, client_conn);
        client_ch
    }

    fn finish_connect(&mut self, client_ch: ConnectionHandle, server_ch: ConnectionHandle) {
        assert_matches!(
            self.client_conn_mut(client_ch).poll(),
            Some(Event::HandshakeDataReady)
        );
        assert_matches!(
            self.client_conn_mut(client_ch).poll(),
            Some(Event::Connected)
        );
        assert_matches!(
            self.server_conn_mut(server_ch).poll(),
            Some(Event::HandshakeDataReady)
        );
        assert_matches!(
            self.server_conn_mut(server_ch).poll(),
            Some(Event::HandshakeConfirmed)
        );
        assert_matches!(
            self.server_conn_mut(server_ch).poll(),
            Some(Event::Connected)
        );
        assert_matches!(
            self.client_conn_mut(client_ch).poll(),
            Some(Event::HandshakeConfirmed)
        );
        info!("connected");
    }

    pub(super) fn client_conn_mut(&mut self, ch: ConnectionHandle) -> &mut Connection {
        self.client.connections.get_mut(&ch).unwrap()
    }

    pub(super) fn client_streams(&mut self, ch: ConnectionHandle) -> Streams<'_> {
        self.client_conn_mut(ch).streams()
    }

    pub(super) fn client_send(&mut self, ch: ConnectionHandle, s: StreamId) -> SendStream<'_> {
        self.client_conn_mut(ch).send_stream(s)
    }

    pub(super) fn client_recv(&mut self, ch: ConnectionHandle, s: StreamId) -> RecvStream<'_> {
        self.client_conn_mut(ch).recv_stream(s)
    }

    pub(super) fn client_datagrams(&mut self, ch: ConnectionHandle) -> Datagrams<'_> {
        self.client_conn_mut(ch).datagrams()
    }

    pub(super) fn server_conn_mut(&mut self, ch: ConnectionHandle) -> &mut Connection {
        self.server.connections.get_mut(&ch).unwrap()
    }

    pub(super) fn server_streams(&mut self, ch: ConnectionHandle) -> Streams<'_> {
        self.server_conn_mut(ch).streams()
    }

    pub(super) fn server_send(&mut self, ch: ConnectionHandle, s: StreamId) -> SendStream<'_> {
        self.server_conn_mut(ch).send_stream(s)
    }

    pub(super) fn server_recv(&mut self, ch: ConnectionHandle, s: StreamId) -> RecvStream<'_> {
        self.server_conn_mut(ch).recv_stream(s)
    }

    pub(super) fn server_datagrams(&mut self, ch: ConnectionHandle) -> Datagrams<'_> {
        self.server_conn_mut(ch).datagrams()
    }
}

/// A builder for [`ConnPair`], because there are too many with_* methods.
///
/// Long-term we should probably aim to remove all the other constructors.
#[derive(Debug, Default)]
pub(super) struct ConnPairBuilder {
    server_transport: Option<TransportConfig>,
    client_transport: Option<TransportConfig>,
    routes: Option<Routing>,
}

impl ConnPairBuilder {
    /// Sets a [`TransportConfig`] for both the client and server.
    pub(super) fn with_transport_cfg(mut self, cfg: TransportConfig) -> Self {
        self.server_transport = Some(cfg.clone());
        self.client_transport = Some(cfg);
        self
    }

    /// Sets the [`Routing`] to use.
    pub(super) fn with_routes(mut self, routes: Routing) -> Self {
        self.routes = Some(routes);
        self
    }

    /// Builds the [`ConnPair`] and connects the two endpoints.
    pub(super) fn connect(self) -> ConnPair {
        let Self {
            server_transport,
            client_transport,
            routes,
        } = self;
        let server_cfg = ServerConfig {
            transport: Arc::new(server_transport.unwrap_or_default()),
            ..server_config()
        };
        let client_cfg = ClientConfig {
            transport: Arc::new(client_transport.unwrap_or_default()),
            ..client_config()
        };
        let mut pair = Pair::new(Default::default(), server_cfg);
        if let Some(routes) = routes {
            pair.routes = routes;
        }
        ConnPair::connect_with(pair, client_cfg)
    }
}

/// Wrapper to a [`Pair`] which keeps handles to the client and server connections.
#[derive(derive_more::Deref, derive_more::DerefMut)]
pub(super) struct ConnPair {
    #[deref]
    #[deref_mut]
    pair: Pair,
    client_ch: ConnectionHandle,
    server_ch: ConnectionHandle,
}

impl Default for ConnPair {
    /// Uses the defaults from [`server_config`] and [`client_config`].
    fn default() -> Self {
        let server_cfg = server_config();
        let client_cfg = client_config();
        Self::with_default_endpoint(server_cfg, client_cfg)
    }
}

impl ConnPair {
    pub(super) fn connect_with(mut pair: Pair, client_cfg: ClientConfig) -> Self {
        let (client_ch, server_ch) = pair.connect_with(client_cfg);
        Self {
            pair,
            client_ch,
            server_ch,
        }
    }

    /// Creates a [`ConnPair`] with the default [`EndpointConfig`] and given `server_cfg` and
    /// `client_cfg`.
    pub(super) fn with_default_endpoint(
        server_cfg: ServerConfig,
        client_cfg: ClientConfig,
    ) -> Self {
        let pair = Pair::new(Default::default(), server_cfg);
        Self::connect_with(pair, client_cfg)
    }

    /// Creates a [`ConnPair`] using the default [`EndpointConfig`] and configurations for the
    /// server and client as defined by [`server_config`] and [`client_config`], setting the
    /// [`TransportConfig`] given for each.
    pub(super) fn with_transport_cfg(
        server_transport: TransportConfig,
        client_transport: TransportConfig,
    ) -> Self {
        let server_cfg = ServerConfig {
            transport: Arc::new(server_transport),
            ..server_config()
        };
        let client_cfg = ClientConfig {
            transport: Arc::new(client_transport),
            ..client_config()
        };
        Self::with_default_endpoint(server_cfg, client_cfg)
    }

    pub(super) fn conn(&self, side: Side) -> &Connection {
        match side {
            Side::Client => self.pair.client.connections.get(&self.client_ch).unwrap(),
            Side::Server => self.pair.server.connections.get(&self.server_ch).unwrap(),
        }
    }

    pub(super) fn conn_mut(&mut self, side: Side) -> &mut Connection {
        match side {
            Side::Client => self
                .pair
                .client
                .connections
                .get_mut(&self.client_ch)
                .unwrap(),
            Side::Server => self
                .pair
                .server
                .connections
                .get_mut(&self.server_ch)
                .unwrap(),
        }
    }

    pub(super) fn poll_timeout(&mut self, side: Side) -> Option<Instant> {
        self.conn_mut(side).poll_timeout()
    }

    pub(super) fn poll(&mut self, side: Side) -> Option<Event> {
        self.conn_mut(side).poll()
    }

    pub(super) fn poll_endpoint_events(&mut self, side: Side) -> Option<EndpointEvent> {
        self.conn_mut(side).poll_endpoint_events()
    }

    pub(super) fn streams(&mut self, side: Side) -> Streams<'_> {
        self.conn_mut(side).streams()
    }

    pub(super) fn recv_stream(&mut self, side: Side, id: StreamId) -> RecvStream<'_> {
        self.conn_mut(side).recv_stream(id)
    }

    pub(super) fn send_stream(&mut self, side: Side, id: StreamId) -> SendStream<'_> {
        self.conn_mut(side).send_stream(id)
    }

    pub(super) fn open_path_ensure(
        &mut self,
        side: Side,
        network_path: FourTuple,
        initial_status: PathStatus,
    ) -> Result<(PathId, bool), PathError> {
        let now = self.pair.time;
        self.conn_mut(side)
            .open_path_ensure(network_path, initial_status, now)
    }

    pub(super) fn open_path(
        &mut self,
        side: Side,
        network_path: FourTuple,
        initial_status: PathStatus,
    ) -> Result<PathId, PathError> {
        let now = self.pair.time;
        self.conn_mut(side)
            .open_path(network_path, initial_status, now)
    }

    pub(super) fn close_path(
        &mut self,
        side: Side,
        path_id: PathId,
        error_code: VarInt,
    ) -> Result<(), ClosePathError> {
        let now = self.pair.time;
        self.conn_mut(side).close_path(now, path_id, error_code)
    }

    /// Simulate receiving a remote PATH_ABANDON for the last path.
    ///
    /// This bypasses the local LastOpenPath guard. In real usage this happens
    /// when a remote peer (possibly a different implementation) sends
    /// PATH_ABANDON for the last shared path.
    #[track_caller]
    pub(super) fn force_remote_abandon(&mut self, side: Side, path_id: PathId) {
        let now = self.pair.time;
        self.conn_mut(side)
            .close_path_inner(
                now,
                path_id,
                PathAbandonReason::RemoteAbandoned {
                    error_code: 0u8.into(),
                },
            )
            .expect("remote abandon should succeed for last path");
    }

    pub(super) fn paths(&self, side: Side) -> Vec<PathId> {
        self.conn(side).paths()
    }

    pub(super) fn path_status(
        &self,
        side: Side,
        path_id: PathId,
    ) -> Result<PathStatus, ClosedPath> {
        self.conn(side).path_status(path_id)
    }

    pub(super) fn network_path(
        &self,
        side: Side,
        path_id: PathId,
    ) -> Result<FourTuple, ClosedPath> {
        self.conn(side).network_path(path_id)
    }

    pub(super) fn set_path_status(
        &mut self,
        side: Side,
        path_id: PathId,
        status: PathStatus,
    ) -> Result<PathStatus, SetPathStatusError> {
        self.conn_mut(side).set_path_status(path_id, status)
    }

    pub(super) fn remote_path_status(&self, side: Side, path_id: PathId) -> Option<PathStatus> {
        self.conn(side).remote_path_status(path_id)
    }

    pub(super) fn set_path_max_idle_timeout(
        &mut self,
        side: Side,
        path_id: PathId,
        timeout: Option<Duration>,
    ) -> Result<Option<Duration>, ClosedPath> {
        let now = self.pair.time;
        self.conn_mut(side)
            .set_path_max_idle_timeout(now, path_id, timeout)
    }

    pub(super) fn set_path_keep_alive_interval(
        &mut self,
        side: Side,
        path_id: PathId,
        interval: Option<Duration>,
    ) -> Result<Option<Duration>, ClosedPath> {
        self.conn_mut(side)
            .set_path_keep_alive_interval(path_id, interval)
    }

    pub(super) fn poll_transmit(
        &mut self,
        side: Side,
        max_datagrams: NonZeroUsize,
        buf: &mut Vec<u8>,
    ) -> Option<Transmit> {
        let now = self.pair.time;
        self.conn_mut(side).poll_transmit(now, max_datagrams, buf)
    }

    pub(super) fn handle_event(&mut self, side: Side, event: ConnectionEvent) {
        self.conn_mut(side).handle_event(event)
    }

    pub(super) fn handle_timeout(&mut self, side: Side, now: Instant) {
        self.conn_mut(side).handle_timeout(now)
    }

    pub(super) fn close(&mut self, side: Side, error_code: u32, reason: &[u8]) {
        let now = self.pair.time;
        self.conn_mut(side)
            .close(now, error_code.into(), Bytes::copy_from_slice(reason))
    }

    pub(super) fn datagrams(&mut self, side: Side) -> Datagrams<'_> {
        self.conn_mut(side).datagrams()
    }

    pub(super) fn stats(&mut self, side: Side) -> ConnectionStats {
        self.conn_mut(side).stats()
    }

    pub(super) fn path_stats(&mut self, side: Side, path_id: PathId) -> Option<PathStats> {
        self.conn_mut(side).path_stats(path_id)
    }

    pub(super) fn ping(&mut self, side: Side) {
        self.conn_mut(side).ping()
    }

    pub(super) fn ping_path(&mut self, side: Side, path: PathId) -> Result<(), ClosedPath> {
        self.conn_mut(side).ping_path(path)
    }

    pub(super) fn force_key_update(&mut self, side: Side) {
        self.conn_mut(side).force_key_update()
    }

    pub(super) fn crypto_session(&self, side: Side) -> &dyn crypto::Session {
        self.conn(side).crypto_session()
    }

    pub(super) fn is_handshaking(&self, side: Side) -> bool {
        self.conn(side).is_handshaking()
    }

    pub(super) fn is_closed(&self, side: Side) -> bool {
        self.conn(side).is_closed()
    }

    pub(super) fn is_drained(&self, side: Side) -> bool {
        self.conn(side).is_drained()
    }

    pub(super) fn accepted_0rtt(&self, side: Side) -> bool {
        self.conn(side).accepted_0rtt()
    }

    pub(super) fn has_0rtt(&self, side: Side) -> bool {
        self.conn(side).has_0rtt()
    }

    pub(super) fn has_pending_retransmits(&self, side: Side) -> bool {
        self.conn(side).has_pending_retransmits()
    }

    pub(super) fn path_observed_address(
        &self,
        side: Side,
        path_id: PathId,
    ) -> Result<Option<SocketAddr>, ClosedPath> {
        self.conn(side).path_observed_address(path_id)
    }

    pub(super) fn rtt(&self, side: Side, path_id: PathId) -> Option<Duration> {
        self.conn(side).rtt(path_id)
    }

    pub(super) fn congestion_state(&self, side: Side, path_id: PathId) -> Option<&dyn Controller> {
        self.conn(side).congestion_state(path_id)
    }

    pub(super) fn set_max_concurrent_streams(&mut self, side: Side, dir: Dir, count: VarInt) {
        self.conn_mut(side).set_max_concurrent_streams(dir, count)
    }

    #[track_caller]
    pub(super) fn set_max_concurrent_paths(
        &mut self,
        side: Side,
        count: u32,
    ) -> Result<(), MultipathNotNegotiated> {
        let now = self.pair.time;
        let count = NonZeroU32::new(count).unwrap();
        self.conn_mut(side).set_max_concurrent_paths(now, count)
    }

    pub(super) fn max_concurrent_streams(&self, side: Side, dir: Dir) -> u64 {
        self.conn(side).max_concurrent_streams(dir)
    }

    pub(super) fn set_send_window(&mut self, side: Side, send_window: u64) {
        self.conn_mut(side).set_send_window(send_window)
    }

    pub(super) fn set_receive_window(&mut self, side: Side, receive_window: VarInt) {
        self.conn_mut(side).set_receive_window(receive_window)
    }

    #[track_caller]
    pub(super) fn reorder_inbound(&mut self, side: Side) {
        let inbound = match side {
            Side::Client => &mut self.pair.client.inbound,
            Side::Server => &mut self.pair.server.inbound,
        };
        let p = inbound.pop_front().unwrap();
        inbound.push_back(p);
    }

    pub(super) fn is_multipath_negotiated(&self, side: Side) -> bool {
        self.conn(side).is_multipath_negotiated()
    }

    pub(super) fn current_mtu(&self, side: Side) -> u16 {
        self.conn(side).current_mtu()
    }

    pub(super) fn add_nat_traversal_address(
        &mut self,
        side: Side,
        address: SocketAddr,
    ) -> Result<(), n0_nat_traversal::Error> {
        self.conn_mut(side).add_nat_traversal_address(address)
    }

    pub(super) fn remove_nat_traversal_address(
        &mut self,
        side: Side,
        address: SocketAddr,
    ) -> Result<(), n0_nat_traversal::Error> {
        self.conn_mut(side).remove_nat_traversal_address(address)
    }

    pub(super) fn get_local_nat_traversal_addresses(
        &self,
        side: Side,
    ) -> Result<Vec<SocketAddr>, n0_nat_traversal::Error> {
        self.conn(side).get_local_nat_traversal_addresses()
    }

    pub(super) fn get_remote_nat_traversal_addresses(
        &self,
        side: Side,
    ) -> Result<Vec<SocketAddr>, n0_nat_traversal::Error> {
        self.conn(side).get_remote_nat_traversal_addresses()
    }

    pub(super) fn initiate_nat_traversal_round(
        &mut self,
        side: Side,
    ) -> Result<Vec<SocketAddr>, n0_nat_traversal::Error> {
        let now = self.pair.time;
        self.conn_mut(side).initiate_nat_traversal_round(now)
    }

    pub(crate) fn handle_network_change(
        &mut self,
        side: Side,
        hint: Option<&dyn NetworkChangeHint>,
    ) {
        let now = self.pair.time;
        self.conn_mut(side).handle_network_change(hint, now);
    }

    pub(super) fn is_draining(&self, side: Side) -> bool {
        match side {
            Client => self.client.draining_connections.contains(&self.client_ch),
            Server => self.server.draining_connections.contains(&self.server_ch),
        }
    }
}

impl Default for Pair {
    fn default() -> Self {
        Self::new(Default::default(), server_config())
    }
}

pub(super) struct TestEndpoint {
    pub(super) endpoint: Endpoint,
    timeout: Option<Instant>,
    pub(super) outbound: VecDeque<(Transmit, Bytes)>,
    delayed: VecDeque<(Transmit, Bytes)>,
    pub(super) inbound: VecDeque<Inbound>,
    pub(super) accepted: Option<Result<ConnectionHandle, ConnectionError>>,
    pub(super) connections: HashMap<ConnectionHandle, Connection>,
    pub(super) draining_connections: HashSet<ConnectionHandle>,
    conn_events: HashMap<ConnectionHandle, VecDeque<ConnectionEvent>>,
    pub(super) captured_packets: Vec<Vec<u8>>,
    pub(super) capture_inbound_packets: bool,
    pub(super) handle_incoming: Box<dyn FnMut(&Incoming) -> IncomingConnectionBehavior>,
    pub(super) waiting_incoming: Vec<Incoming>,
}

pub(super) struct Inbound {
    pub(super) recv_time: Instant,
    pub(super) ecn: Option<EcnCodepoint>,
    pub(super) packet: BytesMut,
    pub(super) remote: SocketAddr,
    pub(super) dst_ip: Option<IpAddr>,
}

#[derive(Debug, Copy, Clone)]
pub(super) enum IncomingConnectionBehavior {
    Accept,
    Reject,
    Retry,
    Wait,
}

pub(super) fn validate_incoming(incoming: &Incoming) -> IncomingConnectionBehavior {
    if incoming.remote_address_validated() {
        IncomingConnectionBehavior::Accept
    } else {
        IncomingConnectionBehavior::Retry
    }
}

impl TestEndpoint {
    fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            timeout: None,
            outbound: VecDeque::new(),
            delayed: VecDeque::new(),
            inbound: VecDeque::new(),
            accepted: None,
            connections: HashMap::default(),
            draining_connections: HashSet::default(),
            conn_events: HashMap::default(),
            captured_packets: Vec::new(),
            capture_inbound_packets: false,
            handle_incoming: Box::new(|_| IncomingConnectionBehavior::Accept),
            waiting_incoming: Vec::new(),
        }
    }

    pub(super) fn drive(&mut self, now: Instant) {
        self.drive_incoming(now);
        self.drive_outgoing(now);
    }

    pub(super) fn drive_incoming(&mut self, now: Instant) {
        let buffer_size = self.endpoint.config().get_max_udp_payload_size() as usize;
        let mut buf = Vec::with_capacity(buffer_size);

        while self.inbound.front().is_some_and(|x| x.recv_time <= now) {
            let Inbound {
                recv_time,
                ecn,
                packet,
                remote,
                dst_ip,
            } = self.inbound.pop_front().unwrap();
            let network_path = FourTuple {
                remote,
                local_ip: dst_ip,
            };
            if let Some(event) =
                self.endpoint
                    .handle(recv_time, network_path, ecn, packet, &mut buf)
            {
                match event {
                    DatagramEvent::NewConnection(incoming) => {
                        match (self.handle_incoming)(&incoming) {
                            IncomingConnectionBehavior::Accept => {
                                let _ = self.try_accept(incoming, now);
                            }
                            IncomingConnectionBehavior::Reject => {
                                self.reject(incoming);
                            }
                            IncomingConnectionBehavior::Retry => {
                                self.retry(incoming);
                            }
                            IncomingConnectionBehavior::Wait => {
                                self.waiting_incoming.push(incoming);
                            }
                        }
                    }
                    DatagramEvent::ConnectionEvent(ch, event) => {
                        if self.capture_inbound_packets {
                            let packet = self.connections[&ch].decode_packet(&event);
                            self.captured_packets.extend(packet);
                        }

                        self.conn_events.entry(ch).or_default().push_back(event);
                    }
                    DatagramEvent::Response(transmit) => {
                        let size = transmit.size;
                        self.outbound.extend(split_transmit(transmit, &buf[..size]));
                        buf.clear();
                    }
                }
            }
        }
    }

    pub(super) fn drive_outgoing(&mut self, now: Instant) {
        let buffer_size = self.endpoint.config().get_max_udp_payload_size() as usize;
        let mut buf = Vec::with_capacity(buffer_size);

        loop {
            let mut endpoint_events: Vec<(ConnectionHandle, EndpointEvent)> = vec![];
            for (ch, conn) in self.connections.iter_mut() {
                if self.timeout.is_some_and(|x| x <= now) {
                    self.timeout = None;
                    conn.handle_timeout(now);
                }

                for (_, mut events) in self.conn_events.drain() {
                    for event in events.drain(..) {
                        conn.handle_event(event);
                    }
                }

                while let Some(event) = conn.poll_endpoint_events() {
                    endpoint_events.push((*ch, event));
                }
                while let Some(transmit) = conn.poll_transmit(now, MAX_DATAGRAMS, &mut buf) {
                    let size = transmit.size;
                    self.outbound.extend(split_transmit(transmit, &buf[..size]));
                    buf.clear();
                }
                self.timeout = conn.poll_timeout();
            }

            if endpoint_events.is_empty() {
                break;
            }

            for (ch, event) in endpoint_events {
                if event.is_draining() {
                    self.draining_connections.insert(ch);
                }
                if let Some(event) = self.handle_event(ch, event)
                    && let Some(conn) = self.connections.get_mut(&ch)
                {
                    conn.handle_event(event);
                }
            }
        }
    }

    pub(super) fn next_wakeup(&self) -> Option<Instant> {
        let next_inbound = self.inbound.front().map(|x| x.recv_time);
        min_opt(self.timeout, next_inbound)
    }

    pub(super) fn is_idle(&self) -> bool {
        self.connections.values().all(|x| x.is_idle())
    }

    pub(super) fn delay_outbound(&mut self) {
        assert!(self.delayed.is_empty());
        mem::swap(&mut self.delayed, &mut self.outbound);
    }

    pub(super) fn finish_delay(&mut self) {
        self.outbound.extend(self.delayed.drain(..));
    }

    pub(super) fn try_accept(
        &mut self,
        incoming: Incoming,
        now: Instant,
    ) -> Result<ConnectionHandle, ConnectionError> {
        let mut buf = Vec::new();
        match self.endpoint.accept(incoming, now, &mut buf, None) {
            Ok((ch, conn)) => {
                self.connections.insert(ch, conn);
                self.accepted = Some(Ok(ch));
                Ok(ch)
            }
            Err(error) => {
                if let Some(transmit) = error.response {
                    let size = transmit.size;
                    self.outbound.extend(split_transmit(transmit, &buf[..size]));
                }
                self.accepted = Some(Err(error.cause.clone()));
                Err(error.cause)
            }
        }
    }

    pub(super) fn retry(&mut self, incoming: Incoming) {
        let mut buf = Vec::new();
        let transmit = self.endpoint.retry(incoming, &mut buf).unwrap();
        let size = transmit.size;
        self.outbound.extend(split_transmit(transmit, &buf[..size]));
    }

    pub(super) fn reject(&mut self, incoming: Incoming) {
        let mut buf = Vec::new();
        let transmit = self.endpoint.refuse(incoming, &mut buf);
        let size = transmit.size;
        self.outbound.extend(split_transmit(transmit, &buf[..size]));
    }

    #[track_caller]
    pub(super) fn assert_accept(&mut self) -> ConnectionHandle {
        self.accepted
            .take()
            .expect("server didn't try connecting")
            .expect("server experienced error connecting")
    }

    #[track_caller]
    pub(super) fn assert_accept_error(&mut self) -> ConnectionError {
        self.accepted
            .take()
            .expect("server didn't try connecting")
            .expect_err("server did unexpectedly connect without error")
    }

    #[track_caller]
    pub(super) fn assert_no_accept(&self) {
        assert!(self.accepted.is_none(), "server did unexpectedly connect")
    }
}

impl ::std::ops::Deref for TestEndpoint {
    type Target = Endpoint;
    fn deref(&self) -> &Endpoint {
        &self.endpoint
    }
}

impl ::std::ops::DerefMut for TestEndpoint {
    fn deref_mut(&mut self) -> &mut Endpoint {
        &mut self.endpoint
    }
}

pub(crate) fn subscribe() -> tracing::subscriber::DefaultGuard {
    let builder = tracing_subscriber::FmtSubscriber::builder()
        .with_env_filter(
            tracing_subscriber::EnvFilter::builder()
                .with_default_directive(tracing::Level::TRACE.into())
                .from_env_lossy(),
        )
        .without_time()
        .with_line_number(true)
        .with_writer(|| TestWriter);
    tracing::subscriber::set_default(builder.finish())
}

struct TestWriter;

impl Write for TestWriter {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        print!(
            "{}",
            str::from_utf8(buf).expect("tried to log invalid UTF-8")
        );
        Ok(buf.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        io::stdout().flush()
    }
}

pub(super) fn server_config() -> ServerConfig {
    let mut config = ServerConfig::with_crypto(Arc::new(server_crypto()));
    if !cfg!(feature = "bloom") {
        config
            .validation_token
            .sent(2)
            .log(Arc::new(SimpleTokenLog::default()));
    }
    config
}

pub(super) fn server_config_with_cert(
    cert: CertificateDer<'static>,
    key: PrivateKeyDer<'static>,
) -> ServerConfig {
    let mut config = ServerConfig::with_crypto(Arc::new(server_crypto_with_cert(cert, key)));
    config
        .validation_token
        .sent(2)
        .log(Arc::new(SimpleTokenLog::default()));
    config
}

pub(super) fn server_crypto() -> QuicServerConfig {
    server_crypto_inner(None, None)
}

pub(super) fn server_crypto_with_alpn(alpn: Vec<Vec<u8>>) -> QuicServerConfig {
    server_crypto_inner(None, Some(alpn))
}

pub(super) fn server_crypto_with_cert(
    cert: CertificateDer<'static>,
    key: PrivateKeyDer<'static>,
) -> QuicServerConfig {
    server_crypto_inner(Some((cert, key)), None)
}

fn server_crypto_inner(
    identity: Option<(CertificateDer<'static>, PrivateKeyDer<'static>)>,
    alpn: Option<Vec<Vec<u8>>>,
) -> QuicServerConfig {
    let (cert, key) = identity.unwrap_or_else(|| {
        (
            CERTIFIED_KEY.cert.der().clone(),
            PrivateKeyDer::Pkcs8(CERTIFIED_KEY.signing_key.serialize_der().into()),
        )
    });

    let mut config = QuicServerConfig::inner(vec![cert], key).unwrap();
    if let Some(alpn) = alpn {
        config.alpn_protocols = alpn;
    }

    config.try_into().unwrap()
}

pub(super) fn client_config() -> ClientConfig {
    ClientConfig::new(Arc::new(client_crypto()))
}

pub(super) fn client_config_with_deterministic_pns() -> ClientConfig {
    let mut cfg = ClientConfig::new(Arc::new(client_crypto()));
    let mut transport = TransportConfig::default();
    transport.deterministic_packet_numbers(true);
    cfg.transport = Arc::new(transport);
    cfg
}

pub(super) fn client_config_with_certs(certs: Vec<CertificateDer<'static>>) -> ClientConfig {
    ClientConfig::new(Arc::new(client_crypto_inner(Some(certs), None)))
}

pub(super) fn client_crypto() -> QuicClientConfig {
    client_crypto_inner(None, None)
}

pub(super) fn client_crypto_with_alpn(protocols: Vec<Vec<u8>>) -> QuicClientConfig {
    client_crypto_inner(None, Some(protocols))
}

fn client_crypto_inner(
    certs: Option<Vec<CertificateDer<'static>>>,
    alpn: Option<Vec<Vec<u8>>>,
) -> QuicClientConfig {
    let mut roots = rustls::RootCertStore::empty();
    for cert in certs.unwrap_or_else(|| vec![CERTIFIED_KEY.cert.der().clone()]) {
        roots.add(cert).unwrap();
    }

    let mut inner = QuicClientConfig::inner(
        WebPkiServerVerifier::builder_with_provider(Arc::new(roots), configured_provider())
            .build()
            .unwrap(),
    );
    inner.key_log = Arc::new(KeyLogFile::new());
    if let Some(alpn) = alpn {
        inner.alpn_protocols = alpn;
    }

    inner.try_into().unwrap()
}

pub(super) fn min_opt<T: Ord>(x: Option<T>, y: Option<T>) -> Option<T> {
    match (x, y) {
        (Some(x), Some(y)) => Some(cmp::min(x, y)),
        (Some(x), _) => Some(x),
        (_, Some(y)) => Some(y),
        _ => None,
    }
}

/// The maximum of datagrams TestEndpoint will produce via `poll_transmit`
const MAX_DATAGRAMS: NonZeroUsize = NonZeroUsize::new(10).expect("known");

fn split_transmit(transmit: Transmit, buffer: &[u8]) -> Vec<(Transmit, Bytes)> {
    let mut buffer = Bytes::copy_from_slice(buffer);
    let Some(segment_size) = transmit.segment_size else {
        return vec![(transmit, buffer)];
    };

    let mut transmits = Vec::new();
    while !buffer.is_empty() {
        let end = segment_size.min(buffer.len());

        let contents = buffer.split_to(end);
        transmits.push((
            Transmit {
                destination: transmit.destination,
                size: contents.len(),
                ecn: transmit.ecn,
                segment_size: None,
                src_ip: transmit.src_ip,
            },
            contents,
        ));
    }

    transmits
}

fn packet_size(transmit: &Transmit, buffer: &Bytes) -> usize {
    if transmit.segment_size.is_some() {
        panic!("This transmit is meant to be split into multiple packets!");
    }

    buffer.len()
}

fn set_congestion_experienced(
    x: Option<EcnCodepoint>,
    congestion_experienced: bool,
) -> Option<EcnCodepoint> {
    x.map(|codepoint| match congestion_experienced {
        true => EcnCodepoint::Ce,
        false => codepoint,
    })
}

pub(crate) static CERTIFIED_KEY: LazyLock<rcgen::CertifiedKey<rcgen::KeyPair>> =
    LazyLock::new(|| rcgen::generate_simple_self_signed(vec!["localhost".into()]).unwrap());

#[derive(Default)]
struct SimpleTokenLog(Mutex<HashSet<u128>>);

impl TokenLog for SimpleTokenLog {
    fn check_and_insert(
        &self,
        nonce: u128,
        _issued: SystemTime,
        _lifetime: Duration,
    ) -> Result<(), TokenReuseError> {
        if self.0.lock().unwrap().insert(nonce) {
            Ok(())
        } else {
            Err(TokenReuseError)
        }
    }
}

#[derive(Debug)]
pub(super) enum Routing {
    Basic(BasicRouting),
    SimpleFirewall(SimpleFirewallRouting),
    ManyToMany(ManyToManyRouting),
    TwoHop(TwoHopRouting),
}

impl Routing {
    /// Returns the current public server address.
    ///
    /// This is the address that can be used to establish a connection with the server.
    pub(super) fn public_server_addr(&self) -> SocketAddr {
        match self {
            Self::Basic(inner) => inner.public_server_addr(),
            Self::SimpleFirewall(inner) => inner.public_server_addr(),
            Self::ManyToMany(inner) => inner.public_server_addr(),
            Self::TwoHop(inner) => inner.public_server_addr(),
        }
    }

    /// Routes a datagram from client to server.
    fn route_client_to_server(&mut self, transmit: &Transmit) -> RoutingDecision {
        match self {
            Self::Basic(inner) => inner.route_client_to_server(transmit),
            Self::SimpleFirewall(inner) => inner.route_client_to_server(transmit),
            Self::ManyToMany(inner) => inner.route_client_to_server(transmit),
            Self::TwoHop(inner) => inner.route_client_to_server(transmit),
        }
    }
    /// Routes a datagram from server to client.
    fn route_server_to_client(&mut self, transmit: &Transmit) -> RoutingDecision {
        match self {
            Self::Basic(inner) => inner.route_server_to_client(transmit),
            Self::SimpleFirewall(inner) => inner.route_server_to_client(transmit),
            Self::ManyToMany(inner) => inner.route_server_to_client(transmit),
            Self::TwoHop(inner) => inner.route_server_to_client(transmit),
        }
    }

    pub(super) fn as_basic(&self) -> &BasicRouting {
        match self {
            Self::Basic(inner) => inner,
            _ => panic!("cast to BasicRouting failed, a different routing table is set"),
        }
    }

    pub(super) fn as_basic_mut(&mut self) -> &mut BasicRouting {
        match self {
            Self::Basic(inner) => inner,
            _ => panic!("cast to BasicRouting failed, a different routing table is set"),
        }
    }

    pub(super) fn as_many_to_many(&self) -> &ManyToManyRouting {
        match self {
            Self::ManyToMany(inner) => inner,
            _ => panic!("cast to ManyToManyRouting failed, a different routing table is set"),
        }
    }

    pub(super) fn as_many_to_many_mut(&mut self) -> &mut ManyToManyRouting {
        match self {
            Self::ManyToMany(inner) => inner,
            _ => panic!("cast to ManyToManyRouting failed, a different routing table is set"),
        }
    }

    pub(super) fn as_two_hop(&self) -> &TwoHopRouting {
        match self {
            Self::TwoHop(inner) => inner,
            _ => panic!("cast to TwoHopRouting failed, a different routing table is set"),
        }
    }

    pub(super) fn as_two_hop_mut(&mut self) -> &mut TwoHopRouting {
        match self {
            Self::TwoHop(inner) => inner,
            _ => panic!("cast to TwoHopRouting failed, a different routing table is set"),
        }
    }
}

#[derive(Debug)]
pub(super) enum RoutingDecision {
    Deliver {
        /// The source address of the delivered packet.
        ///
        /// In other words this becomes the [`FourTuple::remote`] for the receiver.
        src: SocketAddr,
        /// The destination IP address of the delivered packet.
        ///
        /// In other words this becomes the [`FourTuple::local_ip`] for the receiver. For
        /// all normal IP transports this is always known by the routing. This would only be
        /// None to emulate old kernels or special transports like the iroh relay transport.
        dst: Option<IpAddr>,
    },
    Drop,
}

/// Routing that is essentially a direct link between the client and server.
///
/// Packets set to the wrong destination are still dropped however. But the source IP they
/// are sent from is ignored, so it is only a primitive kind of routing.
///
/// You may change the addresses of either to make it look like they migrated to a new
/// address.
#[derive(Debug)]
pub(super) struct BasicRouting {
    pub(super) client_addr: SocketAddr,
    pub(super) server_addr: SocketAddr,
}

impl From<BasicRouting> for Routing {
    fn from(value: BasicRouting) -> Self {
        Self::Basic(value)
    }
}

impl BasicRouting {
    fn public_server_addr(&self) -> SocketAddr {
        self.server_addr
    }

    fn route_client_to_server(&mut self, transmit: &Transmit) -> RoutingDecision {
        if transmit.destination == self.server_addr {
            RoutingDecision::Deliver {
                src: self.client_addr,
                dst: Some(transmit.destination.ip()),
            }
        } else {
            RoutingDecision::Drop
        }
    }

    fn route_server_to_client(&mut self, transmit: &Transmit) -> RoutingDecision {
        if transmit.destination == self.client_addr {
            RoutingDecision::Deliver {
                src: self.server_addr,
                dst: Some(transmit.destination.ip()),
            }
        } else {
            RoutingDecision::Drop
        }
    }

    /// Simulate a passive migration, remaining in the same subnet.
    ///
    /// This increments the last octet of the IP and the port
    pub(super) fn passive_migration(&mut self, side: Side) -> SocketAddr {
        let address = match side {
            Side::Client => &mut self.client_addr,
            Side::Server => &mut self.server_addr,
        };
        let prev_addr = *address;
        match address {
            SocketAddr::V4(socket_addr_v4) => {
                let [a, b, c, d] = socket_addr_v4.ip().octets();
                let mut d = d.wrapping_add(1);
                if d == 0 {
                    // skip the (potential) broadcast address
                    d += 1;
                }
                socket_addr_v4.set_ip(Ipv4Addr::new(a, b, c, d));
            }
            SocketAddr::V6(socket_addr_v6) => {
                let [a, b, c, d, e, f, g, h] = socket_addr_v6.ip().segments();
                socket_addr_v6.set_ip(Ipv6Addr::new(a, b, c, d, e, f, g, h.wrapping_add(1)));
            }
        }
        let new_port = address.port().checked_add(1).unwrap();
        address.set_port(new_port);
        info!(?side, ?prev_addr, new_addr = ?address, "passive migration");
        *address
    }
}

/// Set of uni-directional links between interfaces of a client and server.
///
/// Each entry on the client or server side represents a single interface in a /32
/// subnet. Each interface has exactly one uni-directional outgoing link to a peer
/// interface. The destination interface is identified by the `usize` index into the peer's
/// interfaces `Vec`.
///
/// An interface may only appear once for a peer, so each interface only has a single
/// outgoing link. However interfaces can have multiple incoming links if multiple
/// interfaces of the peer have an outgoing link to it.
#[derive(Debug, Clone)]
pub(super) struct ManyToManyRouting {
    client_routes: Vec<(SocketAddr, usize)>,
    server_routes: Vec<(SocketAddr, usize)>,
}

impl From<ManyToManyRouting> for Routing {
    fn from(value: ManyToManyRouting) -> Self {
        Self::ManyToMany(value)
    }
}

impl ManyToManyRouting {
    fn public_server_addr(&self) -> SocketAddr {
        // Return the address that the first client address can send to.
        self.server_routes[0].0
    }

    fn route_client_to_server(&mut self, transmit: &Transmit) -> RoutingDecision {
        if let Some(client_interface_ip) = transmit.src_ip {
            // If we have a client interface IP, then we use that to build the packet coming in on the other side.
            // But we need to check if that is even connected to the server.
            let Some(&(client_addr, _)) = self
                .server_routes
                .iter()
                .filter(|(addr, _)| *addr == transmit.destination)
                .filter_map(|&(_, idx)| self.client_routes.get(idx))
                .find(|&(addr, _)| addr.ip() == client_interface_ip)
            else {
                // There's no route for given four-tuple.
                return RoutingDecision::Drop;
            };
            RoutingDecision::Deliver {
                src: client_addr,
                dst: Some(transmit.destination.ip()),
            }
        } else {
            let Some((_, client_addr_idx)) = self
                .server_routes
                .iter()
                .find(|(addr, _)| *addr == transmit.destination)
            else {
                return RoutingDecision::Drop;
            };
            let Some((client_addr, _)) = self.client_routes.get(*client_addr_idx) else {
                return RoutingDecision::Drop;
            };
            RoutingDecision::Deliver {
                src: *client_addr,
                dst: Some(transmit.destination.ip()),
            }
        }
    }

    fn route_server_to_client(&mut self, transmit: &Transmit) -> RoutingDecision {
        if let Some(server_interface_ip) = transmit.src_ip {
            // If we have a server interface IP, then we use that to build the packet coming in on the other side.
            // But we need to check if that is even connected to the client.
            let Some(&(server_addr, _)) = self
                .client_routes
                .iter()
                .filter(|(addr, _)| *addr == transmit.destination)
                .filter_map(|&(_, idx)| self.server_routes.get(idx))
                .find(|&(addr, _)| addr.ip() == server_interface_ip)
            else {
                // There's no route for given four-tuple.
                return RoutingDecision::Drop;
            };
            RoutingDecision::Deliver {
                src: server_addr,
                dst: Some(transmit.destination.ip()),
            }
        } else {
            let Some((_, server_addr_idx)) = self
                .client_routes
                .iter()
                .find(|(addr, _)| *addr == transmit.destination)
            else {
                return RoutingDecision::Drop;
            };
            let Some((server_addr, _)) = self.server_routes.get(*server_addr_idx) else {
                return RoutingDecision::Drop;
            };
            RoutingDecision::Deliver {
                src: *server_addr,
                dst: Some(transmit.destination.ip()),
            }
        }
    }

    pub(super) fn from_routes(
        client_routes: Vec<(SocketAddr, usize)>,
        server_routes: Vec<(SocketAddr, usize)>,
    ) -> Self {
        for (_, idx) in client_routes.iter() {
            assert!(*idx < server_routes.len(), "routing table corrupt");
        }
        for (_, idx) in server_routes.iter() {
            assert!(*idx < client_routes.len(), "routing table corrupt");
        }
        Self {
            client_routes,
            server_routes,
        }
    }

    /// Each interface has an outgoing link to the peer's interface with the same index.
    ///
    /// This produces a routing table where each link is bi-directional (or symmetric) and
    /// connected to the corresponding index of the peer interfaces.
    ///
    /// Client and server have the same number of interfaces.
    pub(super) fn simple_symmetric(
        client_addrs: impl IntoIterator<Item = SocketAddr>,
        server_addrs: impl IntoIterator<Item = SocketAddr>,
    ) -> Self {
        let mut client_routes = Vec::new();
        let mut server_routes = Vec::new();

        for (idx, (client_addr, server_addr)) in
            client_addrs.into_iter().zip(server_addrs).enumerate()
        {
            client_routes.push((client_addr, idx));
            server_routes.push((server_addr, idx));
        }

        Self {
            client_routes,
            server_routes,
        }
    }

    /// Adds a new route from an existing server address (identified by index) to a new client address.
    pub(super) fn add_client_route(&mut self, client_addr: SocketAddr, server_addr_idx: usize) {
        assert!(server_addr_idx < self.server_routes.len());
        self.client_routes.push((client_addr, server_addr_idx));
    }

    /// Adds a new route from an existing client address (identified by index) to a new server address.
    pub(super) fn add_server_route(&mut self, server_addr: SocketAddr, client_addr_idx: usize) {
        assert!(client_addr_idx < self.client_routes.len());
        self.server_routes.push((server_addr, client_addr_idx));
    }

    pub(super) fn client_addr(&self, idx: usize) -> Option<SocketAddr> {
        let (addr, _) = self.client_routes.get(idx)?;
        Some(*addr)
    }

    pub(super) fn server_addr(&self, idx: usize) -> Option<SocketAddr> {
        let (addr, _) = self.server_routes.get(idx)?;
        Some(*addr)
    }

    pub(super) fn sim_client_migration(
        &mut self,
        route_idx: usize,
        modify_fn: impl Fn(SocketAddr) -> SocketAddr,
    ) -> Option<SocketAddr> {
        let route = self.client_routes.get_mut(route_idx)?;
        route.0 = modify_fn(route.0);
        Some(route.0)
    }

    pub(super) fn sim_server_migration(
        &mut self,
        route_idx: usize,
        modify_fn: impl Fn(SocketAddr) -> SocketAddr,
    ) -> Option<SocketAddr> {
        let route = self.server_routes.get_mut(route_idx)?;
        route.0 = modify_fn(route.0);
        Some(route.0)
    }
}

/// A routing table with one open and one firewalled interface.
///
/// This essentially behaves like a Destination Endpoint Independent NAT with address and
/// port filtering. But without having to simulate the public side of the network.
///
/// This is pretty simplistic, but tests the basics.
///
/// The client and server both have 2 interfaces:
///
/// 1. One "direct" interface, which has a link to the peer's "direct" interface.
/// 2. One "nat" interface, which has a link to the peer's "nat" interface. This link
///    however does not allow an incoming packet unless it has seen an outgoing packet
///    first.
///
/// When an outgoing transmit has no `src_ip` is set, the source IP is set based on the
/// destination. This is the same as the kernel selecting the correct outbound interface. If
/// an outgoing transmit has an `src_ip` of an interface that is not linked to the interface
/// of the destination IP then a transmit is dropped.
#[derive(Debug)]
pub(super) struct SimpleFirewallRouting {
    /// Whether the client has sent a packet from `client_nat` to `server_nat`.
    ///
    /// If so packets from `server_nat` to `client_nat` will be allowed. If not they will be
    /// dropped.
    client_firewall_open: bool,
    /// Whether the server has sent a packet from `server_nat` to `client_nat`.
    server_firewall_open: bool,
}

impl From<SimpleFirewallRouting> for Routing {
    fn from(value: SimpleFirewallRouting) -> Self {
        Self::SimpleFirewall(value)
    }
}

impl SimpleFirewallRouting {
    /// The address of the client's non-firewalled interface.
    ///
    /// IPv6 address `::1:1`. This is a normal unicast address.
    /// - `::1:1` is for the first client address.
    /// - `::1:0/112` is the subnet used for all client addresses.
    pub(super) const CLIENT_DIRECT_ADDR: SocketAddr = Pair::CLIENT_ADDR;
    /// The address of the server's non-firewalled interface.
    ///
    /// IPv6 address `::2:1`. This is a normal unicast address.
    /// - `::2:1` is for the first server address.
    /// - `::2:0/112` is the subnet used for all server addresses.
    pub(super) const SERVER_DIRECT_ADDR: SocketAddr = Pair::SERVER_ADDR;
    /// The address of the client's firewalled interface.
    ///
    /// IPv6 address `::1:2`. This is a normal IPv6 unicast address.
    /// - `::1:2` is for the second client address.
    /// - `::1:0/112` is the subnet used for all client addresses.
    pub(super) const CLIENT_FW_ADDR: SocketAddr =
        SocketAddr::new(IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 1, 2)), 1);
    /// The address of the server's firewalled interface.
    ///
    /// IPv6 address `::2:2`. This is a normal IPv6 unicast address.
    /// - `::2:1` is for the second server address.
    /// - `::2:0/112` is the subnet used for all server addresses.
    pub(super) const SERVER_FW_ADDR: SocketAddr =
        SocketAddr::new(IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 2, 2)), 1);

    pub(super) fn new() -> Self {
        Self {
            client_firewall_open: false,
            server_firewall_open: false,
        }
    }

    fn public_server_addr(&self) -> SocketAddr {
        Self::SERVER_DIRECT_ADDR
    }

    /// Routes a datagram from client to server.
    ///
    /// Returns the address the server will observe as the sender of the datagram. Or `None`
    /// if there is no open link.
    ///
    /// If there is no [`Transmit::src_ip`] then this routing table selects one based on the
    /// destination, if reachable. Otherwise if the `src_ip` does not match an open link the
    /// datagram is blocked.
    fn route_client_to_server(&mut self, transmit: &Transmit) -> RoutingDecision {
        // Find the address this datagram SHOULD have been sent on to be able to reach the
        // destination.
        let link_src = if transmit.destination == Self::SERVER_DIRECT_ADDR {
            Self::CLIENT_DIRECT_ADDR
        } else if transmit.destination == Self::SERVER_FW_ADDR {
            Self::CLIENT_FW_ADDR
        } else {
            debug!(
                ?transmit.src_ip,
                ?transmit.destination,
                "transmit dropped: unknown destination (network unreachable)",
            );
            return RoutingDecision::Drop;
        };

        // If the datagram is NOT sent from this source then it can't be sent.
        if transmit.src_ip.unwrap_or_else(|| link_src.ip()) != link_src.ip() {
            debug!(
                ?transmit.src_ip,
                ?transmit.destination,
                "transmit dropped: sent from wrong source (network unreachable)",
            );
            return RoutingDecision::Drop;
        }

        // Open the local firewall for outgoing packet.
        if link_src == Self::CLIENT_FW_ADDR && !self.client_firewall_open {
            info!("client firewall opened");
            self.client_firewall_open = true;
        }

        if transmit.destination == Self::SERVER_FW_ADDR && !self.server_firewall_open {
            debug!(
                ?transmit.src_ip,
                ?transmit.destination,
                "transmit dropped: blocked by server firewall",
            );
            return RoutingDecision::Drop;
        }

        // Allow the datagram to be delivered.
        RoutingDecision::Deliver {
            src: link_src,
            dst: Some(transmit.destination.ip()),
        }
    }

    /// Routes a datagram from server to client.
    ///
    /// Returns the address the client will observe as the sender of the datagram. Or `None`
    /// if there is no open link.
    ///
    /// If there is no [`Transmit::src_ip`] then this routing table selects one based on the
    /// destination, if reachable. Otherwise if the `src_ip` does not match an open link the
    /// datagram is blocked.
    fn route_server_to_client(&mut self, transmit: &Transmit) -> RoutingDecision {
        // Find the address this datagram SHOULD have been sent on to be able to reach the
        // destination.
        let link_src = if transmit.destination == Self::CLIENT_DIRECT_ADDR {
            Self::SERVER_DIRECT_ADDR
        } else if transmit.destination == Self::CLIENT_FW_ADDR {
            Self::SERVER_FW_ADDR
        } else {
            debug!(
                ?transmit.src_ip,
                ?transmit.destination,
                "transmit dropped: unknown destination (network unreachable)",
            );
            return RoutingDecision::Drop;
        };

        // If the datagram is NOT sent from this source then it can't be sent.
        if transmit.src_ip.unwrap_or_else(|| link_src.ip()) != link_src.ip() {
            debug!(
                ?transmit.src_ip,
                ?transmit.destination,
                "transmit dropped: sent from wrong source (network unreachable)",
            );
            return RoutingDecision::Drop;
        }

        // Open the local firewall for outgoing packet.
        if link_src == Self::SERVER_FW_ADDR && !self.server_firewall_open {
            info!("server firewall opened");
            self.server_firewall_open = true;
        }

        if transmit.destination == Self::CLIENT_FW_ADDR && !self.client_firewall_open {
            debug!(
                ?transmit.src_ip,
                ?transmit.destination,
                "transmit dropped: blocked by client firewall",
            );
            return RoutingDecision::Drop;
        }

        // Allow the datagram to be delivered.
        RoutingDecision::Deliver {
            src: link_src,
            dst: Some(transmit.destination.ip()),
        }
    }
}

/// Composable routing with each endpoint having a custom hop.
///
/// The test setup always has exactly two endpoints. Both endpoints can have multiple
/// interfaces however, each represented by a [`SubNetRouter`]. Logically each interface can
/// be attached to a local network first, in which case the [`SubNetRouter`] emulates the
/// local network the endpoint iterface sees and behaves like a router.
///
/// Each pair of [`SubNetRouter`]s is connected to a [`TwoHopNetwork`]. This toplevel
/// [`TwoHopRouting`] can hold many networks, but does not perform any routing between
/// them. The [`TwoHopNetworks`] are best kept distinct sibling networks for virtually all
/// scenarios.
///
/// The server must always have at least one [`PublicInterface`] attached to one of the
/// networks, to be able to establish a connection.
///
/// Use [`FromIterator::from_iter`] to construct this.
#[derive(Debug)]
pub(super) struct TwoHopRouting {
    /// All the networks, always at least one.
    networks: Vec<TwoHopNetwork>,
}

impl FromIterator<TwoHopNetwork> for TwoHopRouting {
    fn from_iter<T: IntoIterator<Item = TwoHopNetwork>>(iter: T) -> Self {
        let mut this = Self {
            networks: iter.into_iter().collect(),
        };
        // Sort the networks, we always need to look through these from the most specific
        // network (the longest prefix_len) to the most generic network.
        this.networks.sort_by_key(|n| n.network.prefix_len());
        this.networks.reverse();
        this
    }
}

impl From<TwoHopRouting> for Routing {
    fn from(source: TwoHopRouting) -> Self {
        Self::TwoHop(source)
    }
}

impl TwoHopRouting {
    /// [`Routing::public_server_addr`] impl.
    ///
    /// Returns the smallest server subnet that says it is publicly reachable.
    fn public_server_addr(&self) -> SocketAddr {
        for network in self.networks.iter() {
            if network.server.endpoint_is_public() {
                return network.server.endpoint_addr();
            }
        }
        panic!("no public server address");
    }

    /// [`Routing::route_client_to_server`] impl.
    fn route_client_to_server(&mut self, transmit: &Transmit) -> RoutingDecision {
        self.route_transmit(Side::Client, transmit)
    }

    /// [`Routing::route_server_to_client`] impl.
    fn route_server_to_client(&mut self, transmit: &Transmit) -> RoutingDecision {
        self.route_transmit(Side::Server, transmit)
    }

    /// Implementation for [`Self::route_client_to_server`] and [`Self::route_server_to_client`].
    ///
    /// Finds the network this transmit should be sent in. If there is a `src_ip` it needs
    /// to be sent on the network that contains said src_ip or not at all. If there is only
    /// a destination IP it needs to be sent on the network that contains said destination
    /// IP or not at all.
    fn route_transmit(&mut self, source: Side, transmit: &Transmit) -> RoutingDecision {
        let network = match transmit.src_ip {
            Some(src_ip) => {
                let Some(network) = self.networks.iter_mut().find(|n| {
                    if source == Side::Client {
                        n.client.endpoint_addr().ip() == src_ip
                    } else {
                        n.server.endpoint_addr().ip() == src_ip
                    }
                }) else {
                    error!(%src_ip, "no matching source network for src_ip");
                    return RoutingDecision::Drop;
                };
                network
            }
            None => {
                let Some(network) = self
                    .networks
                    .iter_mut()
                    .find(|n| n.network.contains(&transmit.destination.ip()))
                else {
                    error!(
                        dst_ip = %transmit.destination.ip(),
                        "no matching destination network for destination"
                    );
                    return RoutingDecision::Drop;
                };
                network
            }
        };
        trace!(sender = ?source, net = ?network.network, "selected interface/network to send on");
        // Select the source address and ask the network to route the transmit.
        let src = if source == Side::Client {
            network.client.endpoint_addr()
        } else {
            network.server.endpoint_addr()
        };
        network.send_transmit(src, transmit)
    }

    /// Returns the QAD address of the server-side NAT, if the first network uses one.
    pub(super) fn server_nat_qad_addr(&self) -> Option<SocketAddr> {
        self.networks
            .first()
            .and_then(|n| match &n.server {
                SubNetRouter::EimAdfNat(nat) => Some(nat.qad_addr()),
                _ => None,
            })
    }

    /// Returns the QAD address of the client-side NAT, if the first network uses one.
    pub(super) fn client_nat_qad_addr(&self) -> Option<SocketAddr> {
        self.networks
            .first()
            .and_then(|n| match &n.client {
                SubNetRouter::EimAdfNat(nat) => Some(nat.qad_addr()),
                _ => None,
            })
    }
}

/// A single network with no firewalls.
///
/// Packets are routed from client to server and from server to client subnet. Packets with
/// a destination outside of those subnets are dropped.
#[derive(Debug)]
pub(super) struct TwoHopNetwork {
    network: IpNet,
    server: SubNetRouter,
    server_router: IpAddr,
    client: SubNetRouter,
    client_router: IpAddr,
}

impl TwoHopNetwork {
    /// Creates a new network with the server and client connected.
    ///
    /// The IP addresses of the routers (or interfaces) of the server and client will
    /// respect [`IP_LAST_OCTET_SERVER`] and [`IP_LAST_OCTET_CLIENT`] so they are
    /// recognisable as usual. Though for this the network needs to start at a 0-octet,
    /// which is customary but not strictly required (e.g. an IPv4 /30 network can start at
    /// other values).
    pub(super) fn new(
        network: IpNet,
        mut server: SubNetRouter,
        mut client: SubNetRouter,
    ) -> Self {
        let mut hosts = network.hosts();
        let server_router = match network {
            IpNet::V4(_) => hosts.next().expect("subnet has hosts"),
            IpNet::V6(_) => {
                // Skip the first host so that our server uses 1 as last octet.
                hosts.next().expect("subnet has hosts");
                hosts.next().expect("subnet has hosts")
            }
        };
        // TODO: would love the assert the IP_LAST_OCTET_* values, but IpAddr::as_octets is
        //    nightly-only at the time of writing.
        let client_router = hosts.next().expect("subnet has hosts");
        server.assign_ip(server_router);
        client.assign_ip(client_router);
        Self {
            network,
            server: server,
            server_router,
            client: client,
            client_router,
        }
    }

    /// Requests to send a [`Transmit`] to its destination on this network.
    ///
    /// If the destination is unreachable [`RoutingDecision::Drop`] will be
    /// returned. If the transmit can be delivered to the peer
    /// [`RoutingDecision::Deliver`] will be returned with the addresses the destination
    /// endpoint should observe.
    ///
    /// This will pass the transmit though both the sender and receiver [`SubNetRouter`]s.
    fn send_transmit(&mut self, src: SocketAddr, transmit: &Transmit) -> RoutingDecision {
        let (sender, receiver) = if self.server.endpoint_addr() == src {
            (&mut self.server, &mut self.client)
        } else if self.client.endpoint_addr() == src {
            (&mut self.client, &mut self.server)
        } else {
            // This should be impossible, the caller should never have chosen this
            // network for the transmit to be sent on.
            panic!("src matches neither client or server");
        };

        // First hop: ask the sender's router if it will send this packet out and what the
        // new src_ip will be.
        let HopDecision::Forward { src } = sender.send_transmit(transmit) else {
            return RoutingDecision::Drop;
        };

        // Second hop: ask the receiver's router if the packet can be delivered.
        receiver.recv_transmit(src, transmit)
    }
}

/// A router that connects an endpoint interface to a [`TwoHopNetwork`].
///
/// This enum represents either a [`PublicInterface`] (no NAT/firewall) or an
/// [`EimAdfNat`] (NAT with address-dependent filtering).
#[derive(Debug)]
pub(super) enum SubNetRouter {
    PublicInterface(PublicInterface),
    EimAdfNat(EimAdfNat),
}

impl SubNetRouter {
    fn assign_ip(&mut self, ip: IpAddr) {
        match self {
            Self::PublicInterface(inner) => inner.assign_ip(ip),
            Self::EimAdfNat(inner) => inner.assign_ip(ip),
        }
    }

    fn endpoint_addr(&self) -> SocketAddr {
        match self {
            Self::PublicInterface(inner) => inner.endpoint_addr(),
            Self::EimAdfNat(inner) => inner.endpoint_addr(),
        }
    }

    fn endpoint_is_public(&self) -> bool {
        match self {
            Self::PublicInterface(inner) => inner.endpoint_is_public(),
            Self::EimAdfNat(inner) => inner.endpoint_is_public(),
        }
    }

    fn send_transmit(&mut self, transmit: &Transmit) -> HopDecision {
        match self {
            Self::PublicInterface(inner) => inner.send_transmit(transmit),
            Self::EimAdfNat(inner) => inner.send_transmit(transmit),
        }
    }

    fn recv_transmit(&mut self, src: SocketAddr, transmit: &Transmit) -> RoutingDecision {
        match self {
            Self::PublicInterface(inner) => inner.recv_transmit(src, transmit),
            Self::EimAdfNat(inner) => inner.recv_transmit(src, transmit),
        }
    }
}

#[derive(Debug)]
pub(super) enum HopDecision {
    /// Forward this transmit, the next hop will see `src` as the source address.
    Forward {
        src: SocketAddr,
    },
    Drop,
}

#[derive(Debug)]
pub(super) struct PublicInterface {
    ip: Option<IpAddr>,
    port: u16,
}

impl PublicInterface {
    fn new(port: u16) -> Self {
        Self { ip: None, port }
    }

    pub(super) fn new_server() -> Self {
        Self::new(PORT_SERVER)
    }

    pub(super) fn new_client() -> Self {
        Self::new(PORT_CLIENT)
    }

    fn assign_ip(&mut self, ip: IpAddr) {
        self.ip = Some(ip);
    }

    fn endpoint_addr(&self) -> SocketAddr {
        SocketAddr::new(self.ip.expect("ip not assigned"), self.port)
    }

    fn endpoint_is_public(&self) -> bool {
        true
    }

    fn send_transmit(&mut self, _transmit: &Transmit) -> HopDecision {
        HopDecision::Forward {
            src: self.endpoint_addr(),
        }
    }

    fn recv_transmit(&mut self, src: SocketAddr, transmit: &Transmit) -> RoutingDecision {
        if transmit.destination == self.endpoint_addr() {
            RoutingDecision::Deliver {
                src,
                dst: Some(transmit.destination.ip()),
            }
        } else {
            RoutingDecision::Drop
        }
    }
}

/// Destination-Endpoint Independent Mapping with Address-Dependent Filtering NAT emulation.
///
/// This is essentially EIM + ADF from [RFC4787]: destination Endpoint-Independent Mapping +
/// destination Endpoint-Dependent Filtering. This means the mapping of an internal IP +
/// port is made to the same public IP + port for all destinations, but incoming datagrams
/// from a remote IP address are only allowed once there have been outgoing datagrams to
/// that remote IP address, regardless of port.
///
/// This is an "easy NAT", just like EIM+EIF would be. Emulating EIF (Endpoint Independent
/// Filtering) however is a bit academic since in reality it'd also be open right from start
/// and essentially be a [`PublicInterface`]: either opened by an earlier QAD probe that we
/// need to pretend has happened to get the QNT address candidates, or because the client
/// opened the connection from behind the NAT to start with. ADF at least requires us to
/// send probes before the mapping is made.
///
/// [RFC4787]: https://www.rfc-editor.org/info/rfc4787/#section-4.1
#[derive(Debug)]
pub(super) struct EimAdfNat {
    /// The IP address of the router's uplink.
    ///
    /// This is the IP address the remote peer will see.
    uplink: Option<IpAddr>,
    /// The socket address of the endpoint in the internal network.
    ///
    /// This must be within the LAN network.
    endpoint: SocketAddr,
    /// The prefix length of the LAN network.
    lan_prefix_len: u8,
    /// The external port of the mapping.
    ///
    /// Because we only support a single internal endpoint this only needs to store a single
    /// external port number. We hardcode the mapping so that [`Self::qad_addr`] can return
    /// a value before an outgoing packet was sent.
    mapping_port: u16,
    /// Remote addresses for which the Address-Dependent Filtering is opened.
    filter_allowed: Vec<IpAddr>,
}

impl EimAdfNat {
    fn new(endpoint: SocketAddr, lan_prefix_len: u8) -> Self {
        IpNet::new(endpoint.ip(), lan_prefix_len).expect("invalid lan_prefix_len");
        Self {
            uplink: None,
            endpoint,
            lan_prefix_len,
            mapping_port: NAT_MAPPING_PORT,
            filter_allowed: vec![],
        }
    }

    /// Creates a new EIN router with an internal IPv4 network for the server.
    ///
    /// This uses numbers reserved for the server for the last octet of the IP address and
    /// for the port. This makes things easier to recognise in logs.
    pub(super) fn new_v4_server() -> Self {
        let endpoint: SocketAddr = SocketAddrV4::new(
            Ipv4Addr::new(192, 168, 0, IP_LAST_OCTET_SERVER),
            PORT_SERVER,
        )
        .into();
        Self::new(endpoint, 24)
    }

    /// Creates a new EIN router with an internal IPv4 network for the client.
    ///
    /// This uses numbers reserved for the client for the last octet of the IP address and
    /// for the port. This makes things easier to recognise in logs.
    pub(super) fn new_v4_client() -> Self {
        // 192.168.0.0/16 is reserved for private networks. 198.168.0.0/24 is very commonly
        // used for home LANs.
        let endpoint: SocketAddr = SocketAddrV4::new(
            Ipv4Addr::new(192, 168, 0, IP_LAST_OCTET_CLIENT),
            PORT_CLIENT,
        )
        .into();
        Self::new(endpoint, 24)
    }

    pub(super) fn new_v6_server() -> Self {
        // fc00::/7 is the Unique Local Address range for private networks. We do not
        // properly generate a Global ID, simply use 0 instead: ffc00::/64.
        let endpoint: SocketAddr = SocketAddrV6::new(
            Ipv6Addr::new(0xfc00, 0, 0, 0, 0, 0, 0, IP_LAST_OCTET_SERVER as u16),
            PORT_SERVER,
            0,
            0,
        )
        .into();
        Self::new(endpoint, 64)
    }

    pub(super) fn new_v6_client() -> Self {
        // fc00::/7 is the Unique Local Address range for private networks. We do not
        // properly generate a Global ID, simply use 0 instead: ffc00::/64.
        let endpoint: SocketAddr = SocketAddrV6::new(
            Ipv6Addr::new(0xfc00, 0, 0, 0, 0, 0, 0, IP_LAST_OCTET_CLIENT as u16),
            PORT_CLIENT,
            0,
            0,
        )
        .into();
        Self::new(endpoint, 64)
    }

    /// Returns the QNT candidate address to be used, pretend-QAD.
    ///
    /// Essentially this behaves as if the endpoint already had performed a QAD request to a
    /// 3rd party server and this was the mapping returned. Due to the Address-Dependent
    /// Filtering this does not yet mean the peer can send datagrams to this address.
    pub(super) fn qad_addr(&self) -> SocketAddr {
        SocketAddr::new(self.uplink.expect("not initialised"), self.mapping_port)
    }

    fn assign_ip(&mut self, ip: IpAddr) {
        debug!("assign_ip for {}", self.endpoint);
        let lan =
            IpNet::new(self.endpoint.ip(), self.lan_prefix_len).expect("invalid lan_prefix_len");
        assert!(
            !lan.contains(&ip),
            "invalid config: uplink IP contained in LAN network"
        );
        self.uplink = Some(ip);
    }

    fn endpoint_addr(&self) -> SocketAddr {
        self.endpoint
    }

    fn endpoint_is_public(&self) -> bool {
        false
    }

    fn send_transmit(&mut self, transmit: &Transmit) -> HopDecision {
        let lan =
            IpNet::new(self.endpoint.ip(), self.lan_prefix_len).expect("invalid lan_prefix_len");
        if lan.contains(&transmit.destination.ip()) {
            debug!(dst = %transmit.destination, "transmit destination to same local network");
            return HopDecision::Drop;
        }

        // Create the NAT mapping.
        let remote_ip = transmit.destination.ip();
        if !self.filter_allowed.contains(&remote_ip) {
            debug!(%remote_ip, "EimAfdNat: filter opened for remote");
            self.filter_allowed.push(remote_ip);
        }
        HopDecision::Forward {
            src: SocketAddr::new(self.uplink.expect("not initialised"), self.mapping_port),
        }
    }

    fn recv_transmit(&mut self, src: SocketAddr, transmit: &Transmit) -> RoutingDecision {
        let mapped_addr = self.qad_addr();
        if transmit.destination != mapped_addr {
            debug!(
                dst = %transmit.destination,
                %mapped_addr,
                "EimAdfNat: recvd transmit dropped, incorrect destination"
            );
            return RoutingDecision::Drop;
        }
        if !self.filter_allowed.contains(&src.ip()) {
            debug!(
                %src,
                dst = %transmit.destination,
                "EimAdfNat recvd transmit filtered, source IP not yet allowed"
            );
            return RoutingDecision::Drop;
        }
        RoutingDecision::Deliver {
            src,
            dst: Some(self.endpoint.ip()),
        }
    }
}
