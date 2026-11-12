use std::{
    fmt::Debug,
    future::Future,
    io,
    num::NonZeroUsize,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll, ready},
    time::Instant,
};

use tokio::{
    io::Interest,
    time::{Sleep, sleep_until},
};

use super::{
    AsyncTimer, AsyncUdpSocket, AsyncUdpSocketNext, Runtime, UdpReceiver, UdpSenderHelper,
    UdpSenderHelperSocket,
};

/// A noq runtime for Tokio
#[derive(Debug)]
pub struct TokioRuntime;

impl Runtime for TokioRuntime {
    fn new_timer(&self, t: Instant) -> Pin<Box<dyn AsyncTimer>> {
        Box::pin(sleep_until(t.into()))
    }

    fn spawn(&self, future: Pin<Box<dyn Future<Output = ()> + Send>>) {
        tokio::spawn(future);
    }

    fn wrap_udp_socket(&self, sock: std::net::UdpSocket) -> io::Result<Box<dyn AsyncUdpSocket>> {
        Ok(Box::new(UdpSocket::new(sock)?))
    }

    fn wrap_udp_socket_next(
        &self,
        sock: std::net::UdpSocket,
    ) -> io::Result<Box<dyn AsyncUdpSocketNext>> {
        // Clones share the socket, so senders and the receiver need no second file descriptor.
        Ok(Box::new(UdpSocket::new(sock)?))
    }

    fn now(&self) -> Instant {
        tokio::time::Instant::now().into_std()
    }
}

impl AsyncTimer for Sleep {
    fn reset(self: Pin<&mut Self>, t: Instant) {
        Self::reset(self, t.into())
    }
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        Future::poll(self, cx)
    }
}

#[derive(Debug, Clone)]
struct UdpSocket {
    io: Arc<tokio::net::UdpSocket>,
    inner: Arc<udp::UdpSocketState>,
}

impl UdpSocket {
    fn new(sock: std::net::UdpSocket) -> io::Result<Self> {
        Ok(Self {
            inner: Arc::new(udp::UdpSocketState::new((&sock).into())?),
            io: Arc::new(tokio::net::UdpSocket::from_std(sock)?),
        })
    }
}

impl UdpSenderHelperSocket for UdpSocket {
    fn max_transmit_segments(&self) -> NonZeroUsize {
        self.inner.max_gso_segments()
    }

    fn try_send(&self, transmit: &udp::Transmit<'_>) -> io::Result<()> {
        self.io.try_io(Interest::WRITABLE, || {
            self.inner.try_send((&self.io).into(), transmit)
        })?;

        Ok(())
    }
}

impl AsyncUdpSocket for UdpSocket {
    fn create_sender(&self) -> Pin<Box<dyn super::UdpSender>> {
        Box::pin(UdpSenderHelper::new(self.clone(), |socket: &Self| {
            let socket = socket.clone();
            async move { socket.io.writable().await }
        }))
    }

    fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
        bufs: &mut [io::IoSliceMut<'_>],
        meta: &mut [udp::RecvMeta],
    ) -> Poll<io::Result<usize>> {
        loop {
            ready!(self.io.poll_recv_ready(cx))?;
            match self.io.try_io(Interest::READABLE, || {
                self.inner.recv((&self.io).into(), bufs, meta)
            }) {
                Ok(res) => return Poll::Ready(Ok(res)),
                // `try_io` clears readiness only for `WouldBlock`. Looping on any other
                // error would spin: readiness stays asserted, so `poll_recv_ready`
                // completes at once and the recv fails again, without ever yielding.
                Err(e) if e.kind() == io::ErrorKind::WouldBlock => {}
                Err(e) => return Poll::Ready(Err(e)),
            }
        }
    }

    fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
        self.io.local_addr()
    }

    fn may_fragment(&self) -> bool {
        self.inner.may_fragment()
    }

    fn max_receive_segments(&self) -> NonZeroUsize {
        self.inner.gro_segments()
    }
}

impl AsyncUdpSocketNext for UdpSocket {
    fn create_sender(&self) -> Pin<Box<dyn super::UdpSender>> {
        AsyncUdpSocket::create_sender(self)
    }

    fn create_receiver(&self) -> Option<Box<dyn UdpReceiver>> {
        Some(Box::new(self.clone()))
    }

    fn local_addr(&self) -> io::Result<std::net::SocketAddr> {
        AsyncUdpSocket::local_addr(self)
    }

    fn may_fragment(&self) -> bool {
        AsyncUdpSocket::may_fragment(self)
    }
}

impl UdpReceiver for UdpSocket {
    fn poll_recv(
        &mut self,
        cx: &mut Context<'_>,
        bufs: &mut [io::IoSliceMut<'_>],
        meta: &mut [udp::RecvMeta],
    ) -> Poll<io::Result<usize>> {
        AsyncUdpSocket::poll_recv(self, cx, bufs, meta)
    }

    fn max_receive_segments(&self) -> NonZeroUsize {
        AsyncUdpSocket::max_receive_segments(self)
    }
}
