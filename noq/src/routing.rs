//! Routing for endpoints with multiple sockets: picks the socket for each transmit.

use std::{
    io,
    net::{IpAddr, SocketAddr},
    num::NonZeroUsize,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
};

use ipnet::IpNet;
use udp::Transmit;

use crate::runtime::{AsyncUdpSocketNext, UdpSender};

/// A route to a socket of an endpoint with multiple sockets
///
/// Passed with a socket to [`Endpoint::new_with_multiple_sockets`] or [`Endpoint::add_socket`].
/// For each transmit, the endpoint picks a socket from all routes of all its sockets:
///
/// 1. If the transmit has a source address, the first route whose source is that address, or which
///    has no source and contains the destination.
/// 2. Without a source address, the first route that contains the destination. An IPv6 link-local
///    destination also matches a socket bound with the same, non-zero scope id.
/// 3. Otherwise, the first default route of the destination's address family, or of the source's if
///    the transmit has one.
///
/// Routes are ordered by prefix length, longest first, and then by the order they were added
/// in. The transmit is dropped if no route matches.
///
/// IPv4 ranges also match IPv4-mapped addresses. IPv6 ranges match IPv4-mapped addresses only
/// inside `::ffff:0:0/96`, so a dual-stack socket that should carry IPv4 lists that range too.
///
/// [`Endpoint::new_with_multiple_sockets`]: crate::Endpoint::new_with_multiple_sockets
/// [`Endpoint::add_socket`]: crate::Endpoint::add_socket
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SocketRoute {
    destinations: IpNet,
    source: Option<IpAddr>,
    is_default: bool,
}

impl SocketRoute {
    /// Creates a route for the destinations in `destinations`
    pub fn new(destinations: IpNet) -> Self {
        Self {
            destinations,
            source: None,
            is_default: false,
        }
    }

    /// Sets the source address of the socket, usually the address it is bound to
    ///
    /// A transmit from `source` uses this route whatever its destination.
    pub fn with_source(mut self, source: IpAddr) -> Self {
        self.source = Some(source);
        self
    }

    /// Makes this the default route of its address family
    ///
    /// A default route is used if no route contains the destination.
    pub fn with_default(mut self, is_default: bool) -> Self {
        self.is_default = is_default;
        self
    }

    /// The destinations of this route
    pub fn destinations(&self) -> IpNet {
        self.destinations
    }

    /// The source address of this route
    pub fn source(&self) -> Option<IpAddr> {
        self.source
    }

    /// Whether this is the default route of its address family
    pub fn is_default(&self) -> bool {
        self.is_default
    }

    /// Whether this route carries IPv4, including IPv4-mapped IPv6
    fn is_ipv4(&self) -> bool {
        match self.destinations {
            IpNet::V4(_) => true,
            IpNet::V6(net) => net.prefix_len() >= 96 && net.network().to_ipv4_mapped().is_some(),
        }
    }
}

/// Identifies one socket of an endpoint
///
/// Ids are never reused. A connection caches one sender per id, so a new socket always gets a
/// new sender.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct SocketId(u64);

impl SocketId {
    /// The id of the socket of a single-socket endpoint, never returned by `new`
    pub(crate) const SINGLE: Self = Self(0);

    /// Creates a new id, different from every id created before
    fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(1);
        Self(NEXT.fetch_add(1, Ordering::Relaxed))
    }
}

/// The sockets of an endpoint with multiple sockets and their routes
///
/// Never changed once shared: adding a socket creates a new table, which the endpoint hands to
/// its connections.
#[derive(Debug, Default, Clone)]
pub(crate) struct RouteTable {
    /// Sorted by prefix length, longest first, then by insertion order
    routes: Vec<RouteEntry>,
    /// In insertion order
    sockets: Vec<RoutedSocket>,
}

/// One route of a socket
#[derive(Debug, Clone, Copy)]
struct RouteEntry {
    route: SocketRoute,
    /// The scope id of the socket's local address, or 0
    scope_id: u32,
    socket: SocketId,
}

/// A socket of the table, for creating its senders
#[derive(Debug, Clone)]
struct RoutedSocket {
    id: SocketId,
    socket: Arc<dyn AsyncUdpSocketNext>,
    local_addr: SocketAddr,
}

impl RouteTable {
    /// Returns a copy of this table with `socket` and its routes added
    ///
    /// Returns the id of the new socket.
    pub(crate) fn with_socket(
        &self,
        socket: Arc<dyn AsyncUdpSocketNext>,
        local_addr: SocketAddr,
        routes: Vec<SocketRoute>,
    ) -> (Self, SocketId) {
        let id = SocketId::new();
        let scope_id = match local_addr {
            SocketAddr::V6(addr) => addr.scope_id(),
            SocketAddr::V4(_) => 0,
        };
        let mut table = self.clone();
        table
            .routes
            .extend(routes.into_iter().map(|route| RouteEntry {
                route,
                scope_id,
                socket: id,
            }));
        // Stable sort keeps the insertion order for equal prefix lengths.
        table
            .routes
            .sort_by_key(|entry| std::cmp::Reverse(entry.route.destinations.prefix_len()));
        table.sockets.push(RoutedSocket {
            id,
            socket,
            local_addr,
        });
        (table, id)
    }

    /// Returns the socket for a transmit to `destination` from `src_ip`
    pub(crate) fn route(
        &self,
        destination: SocketAddr,
        src_ip: Option<IpAddr>,
    ) -> Option<SocketId> {
        self.routes
            .iter()
            .find(|entry| entry.is_valid_send_addr(destination, src_ip))
            .or_else(|| {
                self.routes
                    .iter()
                    .find(|entry| entry.is_valid_default_addr(destination, src_ip))
            })
            .map(|entry| entry.socket)
    }

    /// Creates a new sender for `id`, or `None` if the socket is not in this table
    ///
    /// Senders of IPv4 sockets map the endpoint's IPv4-mapped addresses back to IPv4.
    pub(crate) fn create_sender(&self, id: SocketId) -> Option<Pin<Box<dyn UdpSender>>> {
        let socket = self.sockets.iter().find(|socket| socket.id == id)?;
        let sender = socket.socket.create_sender();
        Some(match socket.local_addr {
            SocketAddr::V4(_) => Box::pin(Ipv4Sender(sender)),
            SocketAddr::V6(_) => sender,
        })
    }

    /// The local address of the first socket added
    pub(crate) fn local_addr(&self) -> io::Result<SocketAddr> {
        match self.sockets.first() {
            Some(socket) => Ok(socket.local_addr),
            None => Err(io::Error::new(
                io::ErrorKind::AddrNotAvailable,
                "the endpoint has no sockets",
            )),
        }
    }

    /// Whether any socket may fragment datagrams
    pub(crate) fn may_fragment(&self) -> bool {
        self.sockets
            .iter()
            .any(|socket| socket.socket.may_fragment())
    }
}

impl RouteEntry {
    /// Whether this route matches the destination, or the source if there is one
    fn is_valid_send_addr(&self, dst: SocketAddr, src: Option<IpAddr>) -> bool {
        if let (Some(src), Some(source)) = (src, self.route.source) {
            return source.to_canonical() == src.to_canonical()
                && self.route.is_ipv4() == dst.ip().to_canonical().is_ipv4();
        }
        if route_contains(&self.route.destinations, dst.ip()) {
            return true;
        }
        // A link-local destination is reachable through a socket bound on its interface.
        match dst {
            SocketAddr::V6(dst) => {
                self.scope_id != 0
                    && dst.ip().is_unicast_link_local()
                    && dst.scope_id() == self.scope_id
            }
            SocketAddr::V4(_) => false,
        }
    }

    /// Whether this is a default route for the source's family, or the destination's
    fn is_valid_default_addr(&self, dst: SocketAddr, src: Option<IpAddr>) -> bool {
        let ip = src.unwrap_or_else(|| dst.ip()).to_canonical();
        self.route.is_default && self.route.is_ipv4() == ip.is_ipv4()
    }
}

/// Returns whether `ip` is in `range`, treating IPv4-mapped addresses as IPv4
///
/// An IPv4-mapped address matches IPv4 ranges, and IPv6 ranges only if they lie within
/// `::ffff:0:0/96`. A plain `::/0` socket is therefore never asked to send IPv4, which only a
/// dual-stack socket can do, and such a socket lists that range.
fn route_contains(range: &IpNet, ip: IpAddr) -> bool {
    match (range, ip) {
        (IpNet::V4(range), IpAddr::V6(ip)) => {
            ip.to_ipv4_mapped().is_some_and(|ip| range.contains(&ip))
        }
        (IpNet::V6(range), IpAddr::V6(ip)) if ip.to_ipv4_mapped().is_some() => {
            range.prefix_len() >= 96 && range.contains(&ip)
        }
        (range, ip) => range.contains(&ip),
    }
}

/// Sends on an IPv4 socket, mapping IPv4-mapped addresses back to IPv4
#[derive(Debug)]
struct Ipv4Sender(Pin<Box<dyn UdpSender>>);

impl UdpSender for Ipv4Sender {
    fn poll_send(
        self: Pin<&mut Self>,
        transmit: &Transmit<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        let unmap = |ip: IpAddr| ip.to_canonical();
        let transmit = Transmit {
            destination: SocketAddr::new(
                unmap(transmit.destination.ip()),
                transmit.destination.port(),
            ),
            src_ip: transmit.src_ip.map(unmap),
            ..transmit.clone()
        };
        self.get_mut().0.as_mut().poll_send(&transmit, cx)
    }

    fn max_transmit_segments(&self) -> NonZeroUsize {
        self.0.max_transmit_segments()
    }
}

#[cfg(test)]
mod tests {
    use crate::runtime::UdpReceiver;

    use super::*;

    /// A socket that only has an address
    #[derive(Debug)]
    struct NullSocket(SocketAddr);

    impl AsyncUdpSocketNext for NullSocket {
        fn create_sender(&self) -> Pin<Box<dyn UdpSender>> {
            unimplemented!()
        }

        fn create_receiver(&self) -> Option<Box<dyn UdpReceiver>> {
            None
        }

        fn local_addr(&self) -> io::Result<SocketAddr> {
            Ok(self.0)
        }
    }

    /// A table with one socket per entry of `sockets`, and the ids of the sockets
    fn table(sockets: &[(&str, &[SocketRoute])]) -> (RouteTable, Vec<SocketId>) {
        let mut table = RouteTable::default();
        let mut ids = Vec::new();
        for (addr, routes) in sockets {
            let addr: SocketAddr = addr.parse().unwrap();
            let (next, id) = table.with_socket(Arc::new(NullSocket(addr)), addr, routes.to_vec());
            table = next;
            ids.push(id);
        }
        (table, ids)
    }

    fn route(net: &str) -> SocketRoute {
        SocketRoute::new(net.parse().unwrap())
    }

    #[test]
    fn route_contains_maps_ipv4() {
        let v4: IpNet = "10.0.0.0/8".parse().unwrap();
        let v6: IpNet = "fd15:70a:510b:1::/64".parse().unwrap();
        assert!(route_contains(&v4, "10.1.2.3".parse().unwrap()));
        assert!(route_contains(&v4, "::ffff:10.1.2.3".parse().unwrap()));
        assert!(!route_contains(&v4, "11.1.2.3".parse().unwrap()));
        assert!(!route_contains(&v4, "fd15:70a:510b:1::1".parse().unwrap()));
        assert!(route_contains(&v6, "fd15:70a:510b:1::1".parse().unwrap()));
        assert!(!route_contains(&v6, "fd15:70a:510b:2::1".parse().unwrap()));
        assert!(!route_contains(&v6, "10.1.2.3".parse().unwrap()));
        // Only a dual-stack range covers IPv4-mapped addresses.
        let all_v6: IpNet = "::/0".parse().unwrap();
        let dual_stack: IpNet = "::ffff:0:0/96".parse().unwrap();
        assert!(!route_contains(&all_v6, "::ffff:10.1.2.3".parse().unwrap()));
        assert!(route_contains(
            &dual_stack,
            "::ffff:10.1.2.3".parse().unwrap()
        ));
        assert!(route_contains(&all_v6, "2001:db8::1".parse().unwrap()));
        assert!(!route_contains(&dual_stack, "2001:db8::1".parse().unwrap()));
    }

    #[test]
    fn longest_prefix_then_order() {
        let (table, ids) = table(&[
            ("0.0.0.0:1", &[route("0.0.0.0/0")]),
            ("0.0.0.0:2", &[route("10.1.0.0/16")]),
            ("0.0.0.0:3", &[route("0.0.0.0/0")]),
            ("[::]:4", &[route("::/0")]),
        ]);
        let select = |dst: &str| table.route(dst.parse().unwrap(), None);
        assert_eq!(select("10.1.2.3:443"), Some(ids[1]));
        assert_eq!(select("10.2.2.3:443"), Some(ids[0]));
        assert_eq!(select("[::ffff:10.1.2.3]:443"), Some(ids[1]));
        assert_eq!(select("[2001:db8::1]:443"), Some(ids[3]));
        let (v4_only, _) = self::table(&[("0.0.0.0:1", &[route("0.0.0.0/0")])]);
        assert_eq!(
            v4_only.route("[2001:db8::1]:443".parse().unwrap(), None),
            None
        );
    }

    /// The cases of iroh's IP transport selection, which this routing is ported from
    #[test]
    fn selection() {
        let (table, ids) = table(&[
            ("0.0.0.0:1", &[route("0.0.0.0/0")]),
            (
                "10.1.0.5:1",
                &[route("10.1.0.0/16").with_source("10.1.0.5".parse().unwrap())],
            ),
            (
                "192.168.0.7:1",
                &[route("192.168.0.7/32")
                    .with_source("192.168.0.7".parse().unwrap())
                    .with_default(true)],
            ),
            ("[::]:1", &[route("::/0")]),
            (
                "[fe80::1%7]:1",
                &[route("fe80::1/128").with_source("fe80::1".parse().unwrap())],
            ),
        ]);
        let select = |dst: &str, src: Option<&str>| {
            table.route(dst.parse().unwrap(), src.map(|s| s.parse().unwrap()))
        };
        // Without a source: longest prefix.
        assert_eq!(select("10.1.2.3:1", None), Some(ids[1]));
        assert_eq!(select("8.8.8.8:1", None), Some(ids[0]));
        // IPv4-mapped destinations select IPv4 sockets.
        assert_eq!(select("[::ffff:10.1.2.3]:1", None), Some(ids[1]));
        // With a source: a socket bound to it, or one without a source.
        assert_eq!(select("10.1.2.3:1", Some("192.168.0.7")), Some(ids[2]));
        assert_eq!(
            select("10.1.2.3:1", Some("::ffff:192.168.0.7")),
            Some(ids[2])
        );
        assert_eq!(select("10.1.2.3:1", Some("172.16.0.1")), Some(ids[0]));
        // Link-local destinations select by scope id.
        assert_eq!(select("[fe80::9%7]:1", None), Some(ids[4]));
        assert_eq!(select("[2001:db8::1]:1", None), Some(ids[3]));
    }

    #[test]
    fn default_route() {
        let (table, ids) = table(&[
            ("0.0.0.0:1", &[route("10.0.0.0/8")]),
            ("0.0.0.0:2", &[route("192.168.0.0/16").with_default(true)]),
            ("[::]:3", &[route("fd00::/8")]),
        ]);
        let select = |dst: &str| table.route(dst.parse().unwrap(), None);
        assert_eq!(select("10.0.0.1:1"), Some(ids[0]));
        assert_eq!(select("8.8.8.8:1"), Some(ids[1]));
        assert_eq!(select("[::ffff:8.8.8.8]:1"), Some(ids[1]));
        // No IPv6 default route.
        assert_eq!(select("[2001:db8::1]:1"), None);
    }

    #[test]
    fn socket_ids_are_unique() {
        let ids: Vec<_> = (0..100).map(|_| SocketId::new()).collect();
        let unique: std::collections::HashSet<_> = ids.iter().collect();
        assert_eq!(unique.len(), ids.len());
        assert!(!ids.contains(&SocketId::SINGLE));
    }
}
