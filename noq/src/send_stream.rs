use std::{
    future::{Future, poll_fn},
    io,
    pin::{Pin, pin},
    sync::Arc,
    task::{Context, Poll},
};

use bytes::Bytes;
use pin_project_lite::pin_project;
use proto::{ClosedStream, ConnectionError, FinishError, StreamId};
use thiserror::Error;
use tokio::sync::futures::OwnedNotified;

use crate::{
    VarInt,
    connection::{ConnectionRef, State},
};

/// A stream that can only be used to send data
///
/// If dropped, streams that haven't been explicitly [`reset()`] will be implicitly [`finish()`]ed,
/// continuing to (re)transmit previously written data until it has been fully acknowledged or the
/// connection is closed.
///
/// # Cancellation
///
/// A `write` method is said to be *cancel-safe* when dropping its future before the future becomes
/// ready will always result in no data being written to the stream. This is true of methods which
/// succeed immediately when any progress is made, and is not true of methods which might need to
/// perform multiple writes internally before succeeding. Each `write` method documents whether it
/// is cancel-safe.
///
/// [`reset()`]: SendStream::reset
/// [`finish()`]: SendStream::finish
#[derive(Debug)]
pub struct SendStream {
    conn: ConnectionRef,
    stream: StreamId,
    is_0rtt: bool,
}

impl SendStream {
    pub(crate) fn new(conn: ConnectionRef, stream: StreamId, is_0rtt: bool) -> Self {
        Self {
            conn,
            stream,
            is_0rtt,
        }
    }

    /// Write a buffer into this stream, returning how many bytes were written
    ///
    /// Unless this method errors, it waits until some amount of `buf` can be written into this
    /// stream, and then writes as much as it can without waiting again. Due to congestion and flow
    /// control, this may be shorter than `buf.len()`. On success this yields the length of the
    /// prefix that was written.
    ///
    /// # Cancel safety
    ///
    /// This method is cancellation safe. If this does not resolve, no bytes were written.
    pub async fn write(&mut self, buf: &[u8]) -> Result<usize, WriteError> {
        poll_fn(|cx| self.execute_poll(cx, |s| s.write(buf))).await
    }

    /// Write a buffer into this stream in its entirety
    ///
    /// This method repeatedly calls [`write`](Self::write) until all bytes are written, or an
    /// error occurs.
    ///
    /// # Cancel safety
    ///
    /// This method is *not* cancellation safe. Even if this does not resolve, some prefix of `buf`
    /// may have been written when previously polled.
    pub async fn write_all(&mut self, mut buf: &[u8]) -> Result<(), WriteError> {
        while !buf.is_empty() {
            let written = self.write(buf).await?;
            buf = &buf[written..];
        }
        Ok(())
    }

    /// Writes [`Bytes`] from a slice of buffers into this stream.
    ///
    /// Returns how many bytes were written.
    ///
    /// Bytes to try to write are provided to this method as an array of cheaply cloneable chunks.
    /// Unless this method errors, it waits until some amount of those bytes can be written into
    /// this stream, and then writes as much as it can without waiting again. Due to congestion and
    /// flow control, this may be less than the total number of bytes.
    ///
    /// On success, this method both mutates `bufs` and returns the number of bytes written:
    ///
    /// - `bufs` is advanced past chunks that were fully written.
    /// - If a [`Bytes`] chunk was partially written, the chunk at the new front of `bufs` is [split
    ///   to](Bytes::split_to) contain only the suffix of bytes that were not written.
    ///
    /// # Cancel safety
    ///
    /// This method is cancellation safe. If this does not resolve, no bytes were written.
    pub async fn write_many_chunks(
        &mut self,
        bufs: &mut &mut [Bytes],
    ) -> Result<usize, WriteError> {
        poll_fn(|cx| self.execute_poll(cx, |s| s.write_chunks(bufs))).await
    }

    /// Writes a single [`Bytes`] into this stream in its entirety.
    ///
    /// Bytes to write are provided to this method as a single cheaply cloneable chunk. This
    /// method repeatedly calls [`write_many_chunks`](Self::write_many_chunks) until all bytes
    /// are written, or an error occurs.
    ///
    /// # Cancel safety
    ///
    /// This method is *not* cancellation safe. Even if this does not resolve, some bytes may have
    /// been written when previously polled.
    pub async fn write_chunk(&mut self, buf: Bytes) -> Result<(), WriteError> {
        self.write_all_chunks(&mut [buf]).await
    }

    /// Writes a slice of [`Bytes`] into this stream in its entirety.
    ///
    /// Bytes to write are provided to this method as an array of cheaply cloneable chunks. This
    /// method repeatedly calls [`write_many_chunks`](Self::write_many_chunks) until all bytes are
    /// written, or an error occurs.
    ///
    /// # Cancel safety
    ///
    /// This method is *not* cancellation safe. Even if this does not resolve, some bytes may have
    /// been written when previously polled.
    pub async fn write_all_chunks(&mut self, bufs: &mut [Bytes]) -> Result<(), WriteError> {
        let mut bufs = &mut bufs[..];
        while !bufs.is_empty() {
            self.write_many_chunks(&mut bufs).await?;
        }
        Ok(())
    }

    fn execute_poll<F, R>(
        &mut self,
        cx: &mut Context<'_>,
        write_fn: F,
    ) -> Poll<Result<R, WriteError>>
    where
        F: FnOnce(&mut proto::SendStream<'_>) -> Result<R, proto::WriteError>,
    {
        use proto::WriteError::*;
        let mut conn = self.conn.lock_and_wake("SendStream::poll_write");
        if self.is_0rtt && conn.check_0rtt().is_err() {
            conn.skip_waking();
            return Poll::Ready(Err(WriteError::ZeroRttRejected));
        }
        if let Some(conn_err) = conn.error.clone() {
            conn.skip_waking();
            return Poll::Ready(Err(WriteError::ConnectionLost(conn_err)));
        }

        let result = match write_fn(&mut conn.inner.send_stream(self.stream)) {
            Ok(result) => result,
            Err(Blocked) => {
                conn.blocked_writers.insert(self.stream, cx.waker().clone());
                conn.skip_waking();
                return Poll::Pending;
            }
            Err(Stopped(error_code)) => {
                conn.skip_waking();
                return Poll::Ready(Err(WriteError::Stopped(error_code)));
            }
            Err(ClosedStream) => {
                conn.skip_waking();
                return Poll::Ready(Err(WriteError::ClosedStream));
            }
        };

        Poll::Ready(Ok(result))
    }

    /// Notify the peer that no more data will ever be written to this stream
    ///
    /// It is an error to write to a [`SendStream`] after `finish()`ing it. [`reset()`](Self::reset)
    /// may still be called after `finish` to abandon transmission of any stream data that might
    /// still be buffered.
    ///
    /// To wait for the peer to receive all buffered stream data, see [`stopped()`](Self::stopped).
    ///
    /// May fail if [`finish()`](Self::finish) or [`reset()`](Self::reset) was previously
    /// called. This error is harmless and serves only to indicate that the caller may have
    /// incorrect assumptions about the stream's state.
    pub fn finish(&mut self) -> Result<(), ClosedStream> {
        let mut conn = self.conn.lock_and_wake("finish");
        if let Err(e) = conn.inner.send_stream(self.stream).finish() {
            conn.skip_waking();
            match e {
                FinishError::ClosedStream => Err(ClosedStream::default()),
                // Harmless. If the application needs to know about stopped streams at this point,
                // it should call `stopped`.
                FinishError::Stopped(_) => Ok(()),
            }
        } else {
            Ok(())
        }
    }

    /// Close the send stream immediately.
    ///
    /// No new data can be written after calling this method. Locally buffered data is dropped, and
    /// previously transmitted data will no longer be retransmitted if lost. If an attempt has
    /// already been made to finish the stream, the peer may still receive all written data.
    ///
    /// May fail if [`finish()`](Self::finish) or [`reset()`](Self::reset) was previously
    /// called. This error is harmless and serves only to indicate that the caller may have
    /// incorrect assumptions about the stream's state.
    pub fn reset(&mut self, error_code: VarInt) -> Result<(), ClosedStream> {
        let mut conn = self.conn.lock_and_wake("SendStream::reset");
        if self.is_0rtt && conn.check_0rtt().is_err() {
            conn.skip_waking();
            return Ok(());
        }
        conn.inner.send_stream(self.stream).reset(error_code)?;
        Ok(())
    }

    /// Set the priority of the send stream
    ///
    /// Every send stream has an initial priority of 0. Locally buffered data from streams with
    /// higher priority will be transmitted before data from streams with lower priority. Changing
    /// the priority of a stream with pending data may only take effect after that data has been
    /// transmitted. Using many different priority levels per connection may have a negative
    /// impact on performance.
    pub fn set_priority(&self, priority: i32) -> Result<(), ClosedStream> {
        let mut conn = self.conn.lock_without_waking("SendStream::set_priority");
        conn.inner.send_stream(self.stream).set_priority(priority)?;
        Ok(())
    }

    /// Get the priority of the send stream
    pub fn priority(&self) -> Result<i32, ClosedStream> {
        let mut conn = self.conn.lock_without_waking("SendStream::priority");
        conn.inner.send_stream(self.stream).priority()
    }

    /// Sets a new callback to set the [`StreamAffinity`].
    ///
    /// The stream affinity allows to restrict over which network paths the *application
    /// data* for a stream is sent, i.e. it only affects STREAM frames. The callback is
    /// invoked immediately upon setting it, as well as any time that the available network
    /// paths or their properties change.
    ///
    /// Conditions where this callback will be invoked:
    /// - A path is *opened*.
    /// - A path is *established*.
    /// - Packet scheduling stops using a path.
    /// - Packet scheduling starts using a path.
    /// - A path's status is changed (PATH_STATUS_AVAILABLE, PATH_STATUS_BACKUP).
    /// - A path is *closed*.
    ///
    /// A path is *opened* whenever either the local side opens the path, or a packet was
    /// received from the peer on the path. In the former case there is no implication yet
    /// that the path will be able to carry any datagrams between the two peers. A path is
    /// *established* when their is indication that it is able to carry datagrams between
    /// the peers.
    ///
    /// Packet scheduling affects all QUIC frames and not only application data. It
    /// functions entirely independent of stream affinity. It may decide to stop scheduling
    /// packets on a path depending on many circumstances, e.g. because of increased packet
    /// loss on a path, while the path might still be able to carry packets. When packet
    /// scheduling stops using a path **no** packets are sent on that path, regardless of
    /// what stream affinity indicates.
    ///
    /// Setting stream affinity to be sticky to a path on which no packets are sent means no
    /// application data will be sent for the stream. This will quickly fill up the send
    /// window and result in further writes being blocking.
    ///
    /// To react to gradual changes, e.g. a certain RTT threshold, paths must be monitored
    /// outside of the callback and a new callback can be installed any time to change the
    /// stream affinity.
    pub fn set_affinity(&self, affinity_cb: Arc<dyn StreamAffinityCallback>) {
        todo!();
    }

    /// Completes when the peer stops the stream or reads the stream to completion
    ///
    /// Yields `Some` with the stop error code if the peer stops the stream. Yields `None` if the
    /// local side [`finish()`](Self::finish)es the stream and then the peer acknowledges receipt
    /// of all stream data (although not necessarily the processing of it), after which the peer
    /// closing the stream is no longer meaningful.
    ///
    /// For a variety of reasons, the peer may not send acknowledgements immediately upon receiving
    /// data. As such, relying on `stopped` to know when the peer has read a stream to completion
    /// may introduce more latency than using an application-level response of some sort.
    ///
    /// Clients may wish to await this after finishing a unidirectional 0-RTT stream to reliably
    /// determine whether the stream was rejected.
    pub fn stopped(&self) -> Stopped {
        let notified = {
            // Create an `OwnedNotified` to move into the future. By creating it before the first
            // poll, we make sure that we don't miss any notifications.
            let mut conn = self.conn.lock_without_waking("SendStream::stopped");
            conn.stopped
                .entry(self.stream)
                .or_default()
                .clone()
                .notified_owned()
        };
        Stopped {
            conn: self.conn.clone(),
            stream: self.stream,
            is_0rtt: self.is_0rtt,
            notified,
        }
    }

    /// Get the identity of this stream
    pub fn id(&self) -> StreamId {
        self.stream
    }

    /// Attempt to write bytes from buf into the stream.
    ///
    /// On success, returns Poll::Ready(Ok(num_bytes_written)).
    ///
    /// If the stream is not ready for writing, the method returns Poll::Pending and arranges
    /// for the current task (via cx.waker().wake_by_ref()) to receive a notification when the
    /// stream becomes writable or is closed.
    pub fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<Result<usize, WriteError>> {
        pin!(self.get_mut().write(buf)).as_mut().poll(cx)
    }
}

/// Check if a send stream is stopped.
///
/// Returns `Some` if the stream is stopped or the connection is closed.
/// Returns `None` if the stream is not stopped.
fn send_stream_stopped(
    conn: &mut State,
    stream: StreamId,
    is_0rtt: bool,
) -> Option<Result<Option<VarInt>, StoppedError>> {
    if is_0rtt && conn.check_0rtt().is_err() {
        return Some(Err(StoppedError::ZeroRttRejected));
    }
    match conn.inner.send_stream(stream).stopped() {
        Err(ClosedStream { .. }) => Some(Ok(None)),
        Ok(Some(error_code)) => Some(Ok(Some(error_code))),
        Ok(None) => conn.error.clone().map(|error| Err(error.into())),
    }
}

#[cfg(feature = "futures-io")]
impl futures_io::AsyncWrite for SendStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_write(cx, buf).map_err(Into::into)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(self.get_mut().finish().map_err(Into::into))
    }
}

impl tokio::io::AsyncWrite for SendStream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.poll_write(cx, buf).map_err(Into::into)
    }

    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(self.get_mut().finish().map_err(Into::into))
    }
}

impl Drop for SendStream {
    fn drop(&mut self) {
        let mut conn = self.conn.lock_and_wake("SendStream::drop");

        // clean up any previously registered wakers
        conn.blocked_writers.remove(&self.stream);

        if conn.error.is_some() || (self.is_0rtt && conn.check_0rtt().is_err()) {
            conn.skip_waking();
            return;
        }
        match conn.inner.send_stream(self.stream).finish() {
            Ok(()) => {}
            Err(FinishError::Stopped(reason)) => {
                if conn.inner.send_stream(self.stream).reset(reason).is_err() {
                    conn.skip_waking()
                }
            }
            // Already finished or reset, which is fine.
            Err(FinishError::ClosedStream) => {
                conn.skip_waking();
            }
        }
    }
}

/// Errors that arise from writing to a stream
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum WriteError {
    /// The peer is no longer accepting data on this stream
    ///
    /// Carries an application-defined error code.
    #[error("sending stopped by peer: error {0}")]
    Stopped(VarInt),
    /// The connection was lost
    #[error("connection lost")]
    ConnectionLost(#[from] ConnectionError),
    /// The stream has already been finished or reset
    #[error("closed stream")]
    ClosedStream,
    /// This was a 0-RTT stream and the server rejected it
    ///
    /// Can only occur on clients for 0-RTT streams, which can be opened using
    /// [`Connecting::into_0rtt()`].
    ///
    /// [`Connecting::into_0rtt()`]: crate::Connecting::into_0rtt()
    #[error("0-RTT rejected")]
    ZeroRttRejected,
}

impl From<ClosedStream> for WriteError {
    #[inline]
    fn from(_: ClosedStream) -> Self {
        Self::ClosedStream
    }
}

impl From<StoppedError> for WriteError {
    fn from(x: StoppedError) -> Self {
        match x {
            StoppedError::ConnectionLost(e) => Self::ConnectionLost(e),
            StoppedError::ZeroRttRejected => Self::ZeroRttRejected,
        }
    }
}

impl From<WriteError> for io::Error {
    fn from(x: WriteError) -> Self {
        use WriteError::*;
        let kind = match x {
            Stopped(_) | ZeroRttRejected => io::ErrorKind::ConnectionReset,
            ConnectionLost(_) | ClosedStream => io::ErrorKind::NotConnected,
        };
        Self::new(kind, x)
    }
}

/// Errors that arise while monitoring for a send stream stop from the peer
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum StoppedError {
    /// The connection was lost
    #[error("connection lost")]
    ConnectionLost(#[from] ConnectionError),
    /// This was a 0-RTT stream and the server rejected it
    ///
    /// Can only occur on clients for 0-RTT streams, which can be opened using
    /// [`Connecting::into_0rtt()`].
    ///
    /// [`Connecting::into_0rtt()`]: crate::Connecting::into_0rtt()
    #[error("0-RTT rejected")]
    ZeroRttRejected,
}

impl From<StoppedError> for io::Error {
    fn from(x: StoppedError) -> Self {
        use StoppedError::*;
        let kind = match x {
            ZeroRttRejected => io::ErrorKind::ConnectionReset,
            ConnectionLost(_) => io::ErrorKind::NotConnected,
        };
        Self::new(kind, x)
    }
}

pin_project! {
    /// Future returned from [`SendStream::stopped`].
    #[derive(Debug)]
    pub struct Stopped {
        conn: ConnectionRef,
        stream: StreamId,
        is_0rtt: bool,
        #[pin]
        notified: OwnedNotified,
    }
}

impl Future for Stopped {
    type Output = Result<Option<VarInt>, StoppedError>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let mut this = self.project();
        loop {
            let mut conn = this.conn.lock_without_waking("SendStream::stopped");
            // Check if the stream is stopped before polling the notify. This makes sure that
            // no wakeups are missed.
            if let Some(output) = send_stream_stopped(&mut conn, *this.stream, *this.is_0rtt) {
                return Poll::Ready(output);
            }
            std::task::ready!(this.notified.as_mut().poll(cx));
        }
    }
}

pub trait StreamAffinityCallback {
    fn affinity(&mut self, ctx: StreamAffinityContext) -> StreamAffinity;
}

/// Context for the [`StreamAffinityCallback`] being called.
///
/// This context gives access to a number of items, they are intended for inspection
/// only. Manipulating them will complicate understanding what is going on with your
/// connection.
#[derive(Debug)]
pub struct StreamAffinityContext;

impl StreamAffinityContext {
    /// The connection the callback is invoked for.
    ///
    /// It is advised to not manipulate the connection during the callback. Restrict calls
    /// to e.g. [`Connection::stable_id`] or [`Connection::side`].
    ///
    /// [`Connection::stable_id`]: crate::Connection::stable_id
    /// [`Connection::side`]
    // TODO(flub): maybe provide just accessors for those two pieces?
    pub fn connection(&self) -> crate::Connection {
        todo!()
    }

    /// Returns the identity of the stream.
    pub fn stream_id(&self) -> StreamId {
        todo!()
    }

    /// Returns the priority of the stream.
    pub fn stream_priority(&self) -> i32 {
        todo!()
    }

    /// Iterator over all the currently known paths.
    ///
    /// This is but a snapshot in time, this should not be stored. Current paths are anyway
    /// available from the [`Connection`] itself. Likewise the returned [`Path`] objects
    /// allow manipulation, it is encouraged to refrain from this and only inspect the
    /// paths.
    ///
    /// [`Connection`]: crate::Connection
    /// [`Path`]: crate::Path
    // TODO(flub): I have Connection::iter_paths() in another branch. Maybe it makes more
    //    sense to add that here? OTOH maybe the shortcut is nice anyway. Plus that version
    //    had to be with a detached lifespan, while this version can have an attached
    //    lifespan and save an allocation.
    pub fn paths(&self) -> impl Iterator<Item = crate::Path> + '_ {
        vec![].into_iter()
    }
}

/// Structure to allow choosing an affinity for a stream.
///
/// An instance of this needs to be returned in [`StreamAffinityCallback::affinity`].
///
/// The default affinity is [`StreamAffinity::any`], which is the implied affinity if no
/// explicit stream affinity is set.
#[derive(Debug, Default)]
pub struct StreamAffinity {
    inner: InnerStreamAffinity,
}

impl StreamAffinity {
    /// Creates a new struct to return from [`StreamAffinityCallback::affinity`].
    ///
    /// Will use the default affinity, [`Self::any`].
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets not specific affinity for this stream, the default behaviour.
    ///
    /// STREAM frames for this stream may be sent on any path that the packet scheduling
    /// decides could carry the data to reach the peer.
    pub fn any(&mut self) {
        self.inner = InnerStreamAffinity::Any;
    }

    /// Marks this stream to be only transmitted on a particular network path.
    pub fn sticky(&mut self, path: proto::PathId) {
        self.inner = InnerStreamAffinity::Sticky(path);
    }

    /// Stops transmitting data for this stream.
    ///
    /// If no network path is suitable to carry this stream's data it is possible to not
    /// send it at all. This will result in write calls on the [`SendStream`] blocking once
    /// the send window is filled up.
    pub fn block(&mut self) {
        self.inner = InnerStreamAffinity::Block;
    }
}

#[derive(Debug, Default)]
enum InnerStreamAffinity {
    #[default]
    Any,
    Sticky(proto::PathId),
    Block,
}

#[cfg(test)]
mod tests {
    fn check_is_send_sync<A: Send + Sync>() {}

    #[allow(dead_code)]
    fn test_bounds() {
        check_is_send_sync::<super::Stopped>();
    }
}
