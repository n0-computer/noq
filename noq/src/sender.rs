//! The sender of a connection: one socket, or several behind a route table.

use std::{
    fmt, io,
    net::{IpAddr, SocketAddr},
    num::NonZeroUsize,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use tracing::trace;
use udp::Transmit;

use crate::{
    routing::{RouteTable, SocketId},
    runtime::UdpSender,
};

/// Sends on the socket a transmit is routed to
///
/// A multi-socket endpoint creates a sender per socket from its route table, the first time a
/// transmit is routed to that socket. A connection therefore only holds senders for sockets it
/// uses.
pub(crate) enum ConnectionSender {
    /// The sender of a single-socket endpoint, under [`SocketId::SINGLE`]
    Single(Pin<Box<dyn UdpSender>>),
    /// The senders of a multi-socket endpoint
    Multi {
        routes: Arc<RouteTable>,
        senders: Vec<(SocketId, Pin<Box<dyn UdpSender>>)>,
    },
}

impl ConnectionSender {
    /// Creates the sender of a single-socket endpoint
    pub(crate) fn single(sender: Pin<Box<dyn UdpSender>>) -> Self {
        Self::Single(sender)
    }

    /// Creates a sender that routes with `routes`
    pub(crate) fn multi(routes: Arc<RouteTable>) -> Self {
        Self::Multi {
            routes,
            senders: Vec::new(),
        }
    }

    /// Replaces the route table, keeping the senders already created
    ///
    /// Tables only ever gain sockets, so every existing sender still has its socket.
    pub(crate) fn set_routes(&mut self, new_routes: Arc<RouteTable>) {
        match self {
            Self::Multi { routes, .. } => *routes = new_routes,
            Self::Single(_) => debug_assert!(false, "a single-socket endpoint has no routes"),
        }
    }

    /// Returns the socket for a destination and the segment limit of its sender
    ///
    /// Creates the socket's sender on first use. Returns `None` if no route matches.
    pub(crate) fn route(
        &mut self,
        destination: SocketAddr,
        src_ip: Option<IpAddr>,
    ) -> Option<(SocketId, NonZeroUsize)> {
        let (routes, senders) = match self {
            Self::Single(sender) => {
                return Some((SocketId::SINGLE, sender.max_transmit_segments()));
            }
            Self::Multi { routes, senders } => (routes, senders),
        };
        let socket = routes.route(destination, src_ip)?;
        let sender = match senders.iter().position(|(id, _)| *id == socket) {
            Some(index) => &senders[index].1,
            None => {
                let sender = routes
                    .create_sender(socket)
                    .expect("routed sockets are in the table");
                senders.push((socket, sender));
                &senders.last().expect("just pushed").1
            }
        };
        Some((socket, sender.max_transmit_segments()))
    }

    /// Routes and sends one of the endpoint's own responses, outside any connection
    pub(crate) fn send_response(
        &mut self,
        transmit: &Transmit<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        let Some((socket, _)) = self.route(transmit.destination, transmit.src_ip) else {
            // Like a failed sendmsg, this counts as loss and is left to QUIC to recover from.
            trace!(destination = %transmit.destination, "no socket for destination, dropping transmit");
            return Poll::Ready(Ok(()));
        };
        self.poll_send_on(socket, transmit, cx)
    }

    /// Sends `transmit` on the sender of `socket`, which [`route`] returned
    ///
    /// [`route`]: Self::route
    pub(crate) fn poll_send_on(
        &mut self,
        socket: SocketId,
        transmit: &Transmit<'_>,
        cx: &mut Context<'_>,
    ) -> Poll<io::Result<()>> {
        let sender = match self {
            Self::Single(sender) => Some(sender),
            Self::Multi { senders, .. } => senders
                .iter_mut()
                .find(|(id, _)| *id == socket)
                .map(|(_, sender)| sender),
        };
        match sender {
            Some(sender) => sender.as_mut().poll_send(transmit, cx),
            // Not expected: `route` created the sender, and senders are never removed.
            None => {
                trace!(destination = %transmit.destination, "no sender for socket, dropping transmit");
                Poll::Ready(Ok(()))
            }
        }
    }
}

impl fmt::Debug for ConnectionSender {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Single(sender) => f.debug_tuple("Single").field(sender).finish(),
            Self::Multi { senders, .. } => f
                .debug_struct("Multi")
                .field("senders", &senders.len())
                .finish_non_exhaustive(),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;

    use crate::runtime::{AsyncUdpSocketNext, UdpReceiver};

    use super::*;

    /// A sender with a fixed segment limit that never sends
    #[derive(Debug)]
    struct LimitSender(NonZeroUsize);

    impl UdpSender for LimitSender {
        fn poll_send(
            self: Pin<&mut Self>,
            _transmit: &Transmit<'_>,
            _cx: &mut Context<'_>,
        ) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn max_transmit_segments(&self) -> NonZeroUsize {
            self.0
        }
    }

    /// A socket whose senders have a fixed segment limit, counting the senders created
    #[derive(Debug)]
    struct LimitSocket {
        addr: SocketAddr,
        segments: NonZeroUsize,
        created: Arc<Mutex<Vec<SocketAddr>>>,
    }

    impl AsyncUdpSocketNext for LimitSocket {
        fn create_sender(&self) -> Pin<Box<dyn UdpSender>> {
            self.created.lock().expect("poisoned").push(self.addr);
            Box::pin(LimitSender(self.segments))
        }

        fn create_receiver(&self) -> Option<Box<dyn UdpReceiver>> {
            None
        }

        fn local_addr(&self) -> io::Result<SocketAddr> {
            Ok(self.addr)
        }
    }

    #[test]
    fn route_uses_each_sockets_segment_limit() {
        let created = Arc::new(Mutex::new(Vec::new()));
        let mut table = RouteTable::default();
        let mut ids = Vec::new();
        for (addr, net, segments) in [("0.0.0.0:1", "0.0.0.0/0", 1), ("[::]:1", "::/0", 10)] {
            let addr: SocketAddr = addr.parse().unwrap();
            let socket = LimitSocket {
                addr,
                segments: NonZeroUsize::new(segments).unwrap(),
                created: created.clone(),
            };
            let route = crate::SocketRoute::new(net.parse().unwrap());
            let (next, id) = table.with_socket(Arc::new(socket), addr, vec![route]);
            table = next;
            ids.push(id);
        }
        let mut sender = ConnectionSender::multi(Arc::new(table));
        let v4 = "127.0.0.1:1".parse().unwrap();
        let v6 = "[::1]:1".parse().unwrap();

        assert_eq!(sender.route(v4, None), Some((ids[0], NonZeroUsize::MIN)));
        assert_eq!(
            sender.route(v6, None),
            Some((ids[1], NonZeroUsize::new(10).unwrap()))
        );
        // Senders are created once per socket, on first use.
        sender.route(v4, None);
        sender.route(v6, None);
        assert_eq!(
            *created.lock().expect("poisoned"),
            ["0.0.0.0:1".parse().unwrap(), "[::]:1".parse().unwrap()]
        );
    }
}
