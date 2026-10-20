//! Routing for endpoints with multiple sockets: picks the socket for each transmit.

use std::{
    io,
    net::{IpAddr, SocketAddr},
    num::NonZeroUsize,
    pin::Pin,
    sync::Arc,
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
/// in. An IPv4 prefix counts as its IPv4-mapped IPv6 prefix, so `10.0.0.0/8` is a /104. The
/// transmit is dropped if no route matches.
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

    /// Returns the destinations of this route
    pub fn destinations(&self) -> IpNet {
        self.destinations
    }

    /// Returns the source address of this route
    pub fn source(&self) -> Option<IpAddr> {
        self.source
    }

    /// Returns whether this is the default route of its address family
    pub fn is_default(&self) -> bool {
        self.is_default
    }
}

/// Identifies one socket of an endpoint, by its index in the route table
///
/// Tables only gain sockets, so an id stays valid for the lifetime of the endpoint.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct SocketId(usize);

impl SocketId {
    /// The id of the socket of a single-socket endpoint
    pub(crate) const SINGLE: Self = Self(0);

    /// The index of the socket in the route table
    pub(crate) fn index(self) -> usize {
        self.0
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
    /// Indexed by [`SocketId`]
    sockets: Vec<RoutedSocket>,
}

/// One route of a socket, prepared for matching
///
/// Addresses are kept as the bits of an IPv6 address, with IPv4 as IPv4-mapped IPv6. Thus a
/// match is a mask and a compare, for both address families.
#[derive(Debug, Clone, Copy)]
struct RouteEntry {
    /// The network of the destinations
    network: u128,
    /// The prefix mask of the destinations
    mask: u128,
    /// Whether the destinations are IPv4, including IPv4-mapped IPv6
    is_ipv4: bool,
    /// The source address, as the bits of an IPv6 address
    source: Option<u128>,
    is_default: bool,
    /// The scope id of the socket's local address, or 0
    scope_id: u32,
    socket: SocketId,
}

/// A socket of the table, for creating its senders
#[derive(Debug, Clone)]
struct RoutedSocket {
    socket: Arc<dyn AsyncUdpSocketNext>,
    local_addr: SocketAddr,
}

impl RouteTable {
    /// Returns a copy of this table with `socket` and its routes added
    ///
    /// The new socket gets the next [`SocketId`], its index.
    pub(crate) fn with_socket(
        &self,
        socket: Arc<dyn AsyncUdpSocketNext>,
        local_addr: SocketAddr,
        routes: Vec<SocketRoute>,
    ) -> Self {
        let id = SocketId(self.sockets.len());
        let scope_id = match local_addr {
            SocketAddr::V6(addr) => addr.scope_id(),
            SocketAddr::V4(_) => 0,
        };
        let mut table = self.clone();
        table.routes.extend(
            routes
                .iter()
                .map(|route| RouteEntry::new(route, scope_id, id)),
        );
        // Stable sort keeps the insertion order for equal prefix lengths. The prefix length
        // of an IPv4 route counts as IPv4-mapped, so `::ffff:0:0/96` and `0.0.0.0/0` tie.
        table
            .routes
            .sort_by_key(|entry| std::cmp::Reverse(entry.mask.count_ones()));
        table.sockets.push(RoutedSocket { socket, local_addr });
        table
    }

    /// Returns the socket for a transmit to `destination` from `src_ip`
    ///
    /// Called for every path on each transmit, so this is one pass over the routes with a mask
    /// and a compare each.
    pub(crate) fn route(
        &self,
        destination: SocketAddr,
        src_ip: Option<IpAddr>,
    ) -> Option<SocketId> {
        let dst = ip_bits(destination.ip());
        let dst_is_ipv4 = is_ipv4_mapped(dst);
        let src = src_ip.map(ip_bits);
        // A link-local destination is reachable through a socket bound on its interface.
        let link_local_scope = match destination {
            SocketAddr::V6(addr) if is_unicast_link_local(dst) => addr.scope_id(),
            _ => 0,
        };
        let default_is_ipv4 = src.map_or(dst_is_ipv4, is_ipv4_mapped);
        let mut default = None;
        for entry in &self.routes {
            let matches = match (src, entry.source) {
                (Some(src), Some(source)) => source == src && entry.is_ipv4 == dst_is_ipv4,
                _ => {
                    entry.contains(dst)
                        || (link_local_scope != 0 && link_local_scope == entry.scope_id)
                }
            };
            if matches {
                return Some(entry.socket);
            }
            if default.is_none() && entry.is_default && entry.is_ipv4 == default_is_ipv4 {
                default = Some(entry.socket);
            }
        }
        default
    }

    /// Creates a new sender for `id`, or `None` if the socket is not in this table
    ///
    /// Senders of IPv4 sockets map the endpoint's IPv4-mapped addresses back to IPv4.
    pub(crate) fn create_sender(&self, id: SocketId) -> Option<Pin<Box<dyn UdpSender>>> {
        let socket = self.sockets.get(id.index())?;
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
    fn new(route: &SocketRoute, scope_id: u32, socket: SocketId) -> Self {
        let (network, prefix_len) = match route.destinations {
            IpNet::V4(net) => (
                u128::from(net.network().to_ipv6_mapped()),
                96 + u32::from(net.prefix_len()),
            ),
            IpNet::V6(net) => (u128::from(net.network()), u32::from(net.prefix_len())),
        };
        Self {
            network,
            mask: u128::MAX.checked_shl(128 - prefix_len).unwrap_or(0),
            // An IPv6 range shorter than /96 does not carry IPv4: only a dual-stack socket
            // can send IPv4, and such a socket lists `::ffff:0:0/96`.
            is_ipv4: prefix_len >= 96 && is_ipv4_mapped(network),
            source: route.source.map(ip_bits),
            is_default: route.is_default,
            scope_id,
            socket,
        }
    }

    /// Whether the destinations contain `ip`, given as [`ip_bits`]
    fn contains(&self, ip: u128) -> bool {
        self.is_ipv4 == is_ipv4_mapped(ip) && ip & self.mask == self.network
    }
}

/// The bits of `ip` as an IPv6 address, with IPv4 as IPv4-mapped IPv6
fn ip_bits(ip: IpAddr) -> u128 {
    match ip {
        IpAddr::V4(ip) => u128::from(ip.to_ipv6_mapped()),
        IpAddr::V6(ip) => u128::from(ip),
    }
}

/// Whether `bits` are an IPv4-mapped IPv6 address, in `::ffff:0:0/96`
fn is_ipv4_mapped(bits: u128) -> bool {
    bits >> 32 == 0xffff
}

/// Whether `bits` are an IPv6 unicast link-local address, in `fe80::/10`
fn is_unicast_link_local(bits: u128) -> bool {
    bits >> 118 == 0xfe80 >> 6
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
            ids.push(SocketId(ids.len()));
            table = table.with_socket(Arc::new(NullSocket(addr)), addr, routes.to_vec());
        }
        (table, ids)
    }

    fn route(net: &str) -> SocketRoute {
        SocketRoute::new(net.parse().unwrap())
    }

    /// Whether a route for `net` contains `ip`
    fn contains(net: &str, ip: &str) -> bool {
        let entry = RouteEntry::new(&route(net), 0, SocketId(0));
        entry.contains(ip_bits(ip.parse().unwrap()))
    }

    #[test]
    fn contains_maps_ipv4() {
        assert!(contains("10.0.0.0/8", "10.1.2.3"));
        assert!(contains("10.0.0.0/8", "::ffff:10.1.2.3"));
        assert!(!contains("10.0.0.0/8", "11.1.2.3"));
        assert!(!contains("10.0.0.0/8", "fd15:70a:510b:1::1"));
        assert!(contains("0.0.0.0/0", "192.0.2.1"));
        assert!(!contains("0.0.0.0/0", "2001:db8::1"));
        assert!(contains("fd15:70a:510b:1::/64", "fd15:70a:510b:1::1"));
        assert!(!contains("fd15:70a:510b:1::/64", "fd15:70a:510b:2::1"));
        assert!(!contains("fd15:70a:510b:1::/64", "10.1.2.3"));
        // Only a dual-stack range covers IPv4-mapped addresses.
        assert!(!contains("::/0", "::ffff:10.1.2.3"));
        assert!(!contains("::/0", "10.1.2.3"));
        assert!(contains("::ffff:0:0/96", "::ffff:10.1.2.3"));
        assert!(contains("::ffff:0:0/96", "10.1.2.3"));
        assert!(contains("::/0", "2001:db8::1"));
        assert!(!contains("::ffff:0:0/96", "2001:db8::1"));
        assert!(contains("::1/128", "::1"));
        assert!(!contains("::1/128", "::2"));
    }

    #[test]
    fn link_local_bits() {
        let bits = |ip: &str| ip_bits(ip.parse().unwrap());
        assert!(is_unicast_link_local(bits("fe80::1")));
        assert!(is_unicast_link_local(bits("febf:ffff::1")));
        assert!(!is_unicast_link_local(bits("fec0::1")));
        assert!(!is_unicast_link_local(bits("fe7f::1")));
        assert!(!is_unicast_link_local(bits("169.254.0.1")));
    }

    /// IPv4 prefix lengths count as IPv4-mapped, so a longer IPv4 route beats `::ffff:0:0/96`
    #[test]
    fn ipv4_prefix_counts_as_mapped() {
        let (table, ids) = table(&[
            ("[::]:1", &[route("::ffff:0:0/96")]),
            ("0.0.0.0:2", &[route("10.0.0.0/8")]),
        ]);
        let select = |dst: &str| table.route(dst.parse().unwrap(), None);
        assert_eq!(select("[::ffff:10.1.2.3]:1"), Some(ids[1]));
        assert_eq!(select("[::ffff:192.0.2.1]:1"), Some(ids[0]));
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
}
