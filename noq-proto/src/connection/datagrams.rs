use std::collections::VecDeque;

use bytes::Bytes;
use thiserror::Error;
use tracing::{debug, trace};

use super::Connection;
use crate::{
    FrameStats, TransportError,
    connection::PacketBuilder,
    frame::{Datagram, FrameStruct},
};

/// API to control datagram traffic
pub struct Datagrams<'a> {
    pub(super) conn: &'a mut Connection,
}

impl Datagrams<'_> {
    /// Queue an unreliable, unordered datagram for immediate transmission
    ///
    /// If `drop` is true, previously queued datagrams which are still unsent may be discarded to
    /// make space for this datagram, in order of oldest to newest. If `drop` is false, and there
    /// isn't enough space due to previously queued datagrams, this function will return
    /// `SendDatagramError::Blocked`. `Event::DatagramsUnblocked` will be emitted once datagrams
    /// have been sent.
    ///
    /// Returns `Err` iff a `len`-byte datagram cannot currently be sent.
    pub fn send(&mut self, data: Bytes, drop: bool) -> Result<(), SendDatagramError> {
        if self.conn.config.datagram_receive_buffer_size.is_none() {
            return Err(SendDatagramError::Disabled);
        }
        let max = self
            .max_size()
            .ok_or(SendDatagramError::UnsupportedByPeer)?;
        if data.len() > max {
            return Err(SendDatagramError::TooLarge);
        }
        if drop {
            while self.conn.datagrams.outgoing_total > self.conn.config.datagram_send_buffer_size {
                let prev = self
                    .conn
                    .datagrams
                    .outgoing
                    .pop_front()
                    .expect("datagrams.outgoing_total desynchronized");
                trace!(len = prev.data.len(), "dropping outgoing datagram");
                self.conn.datagrams.outgoing_total -= prev.data.len();
            }
        } else if self.conn.datagrams.outgoing_total + data.len()
            > self.conn.config.datagram_send_buffer_size
        {
            self.conn.datagrams.send_blocked = true;
            return Err(SendDatagramError::Blocked(data));
        }
        self.conn.datagrams.outgoing_total += data.len();
        self.conn.datagrams.outgoing.push_back(Datagram { data });
        Ok(())
    }

    /// Queue many unreliable, unordered datagrams for transmission in a single call.
    ///
    /// This is the batch analogue of [`send`](Self::send) with `drop = true`: it
    /// amortizes the per-datagram bookkeeping (size checks, buffer accounting) and
    /// lets a caller that already has a batch of `Bytes` push them under one logical
    /// operation. Semantics match calling `send(data, true)` for each element in
    /// order, with two differences:
    ///
    /// - the peer-support / disabled / `max_size` checks are performed once, up
    ///   front, using the current MTU (so a batch is rejected atomically if the
    ///   *first* datagram is too large, rather than partially sent);
    /// - drop-oldest-to-make-space is applied once after enqueueing the whole batch
    ///   rather than per element, which is cheaper and avoids re-entering the drop
    ///   loop N times.
    ///
    /// Returns `Ok(n)` where `n` is the number of datagrams actually queued (may be
    /// less than the iterator yielded if an individual datagram exceeded `max_size`
    /// and was skipped). Returns `Err` iff datagrams are unsupported/disabled, or
    /// if *all* yielded datagrams are larger than the current max size.
    pub fn send_many<I: IntoIterator<Item = Bytes>>(
        &mut self,
        datagrams: I,
    ) -> Result<usize, SendDatagramError> {
        if self.conn.config.datagram_receive_buffer_size.is_none() {
            return Err(SendDatagramError::Disabled);
        }
        let max = self
            .max_size()
            .ok_or(SendDatagramError::UnsupportedByPeer)?;

        let mut queued = 0usize;
        let mut saw_any = false;
        for data in datagrams {
            saw_any = true;
            if data.len() > max {
                // Skip oversized datagrams individually rather than failing the
                // whole batch: a caller streaming a mix of sizes shouldn't lose the
                // small ones because one big one was malformed.
                trace!(len = data.len(), max, "skipping oversized datagram in batch");
                continue;
            }
            self.conn.datagrams.outgoing_total += data.len();
            self.conn.datagrams.outgoing.push_back(Datagram { data });
            queued += 1;
        }

        if queued == 0 && saw_any {
            // Iterator was non-empty but every element was too large.
            return Err(SendDatagramError::TooLarge);
        }

        // Drop oldest until we're back under the send buffer budget. Applied once
        // for the whole batch.
        while self.conn.datagrams.outgoing_total
            > self.conn.config.datagram_send_buffer_size
        {
            let prev = self
                .conn
                .datagrams
                .outgoing
                .pop_front()
                .expect("datagrams.outgoing_total desynchronized");
            trace!(len = prev.data.len(), "dropping outgoing datagram (batch)");
            self.conn.datagrams.outgoing_total -= prev.data.len();
        }

        Ok(queued)
    }

    /// Compute the maximum size of datagrams that may be passed to `send_datagram`
    ///
    /// Returns `None` if datagrams are unsupported by the peer or disabled locally.
    ///
    /// This may change over the lifetime of a connection according to variation in the path MTU
    /// estimate. The peer can also enforce an arbitrarily small fixed limit, but if the peer's
    /// limit is large this is guaranteed to be a little over a kilobyte at minimum.
    ///
    /// Not necessarily the maximum size of received datagrams.
    ///
    /// When multipath is enabled, this is calculated using the smallest MTU across all
    /// available paths.
    pub fn max_size(&self) -> Option<usize> {
        // We use the conservative overhead bound for any packet number, reducing the budget by at
        // most 3 bytes, so that PN size fluctuations don't cause users sending maximum-size
        // datagrams to suffer avoidable packet loss.
        let max_size = self.conn.current_mtu() as usize
            - self.conn.predict_1rtt_overhead_no_pn()
            - Datagram::SIZE_BOUND;
        let limit = self
            .conn
            .peer_params
            .max_datagram_frame_size?
            .into_inner()
            .saturating_sub(Datagram::SIZE_BOUND as u64);
        Some(limit.min(max_size as u64) as usize)
    }

    /// Receive an unreliable, unordered datagram
    pub fn recv(&mut self) -> Option<Bytes> {
        self.conn.datagrams.recv()
    }

    /// Drain up to `out.capacity()` buffered datagrams into `out`, in arrival order.
    ///
    /// This is the batch analogue of [`recv`](Self::recv): a single call drains
    /// many datagrams, so a caller holding the connection lock once can forward a
    /// batch without re-acquiring it per datagram. Returns the number of datagrams
    /// appended.
    ///
    /// `out` is not cleared; datagrams are `extend`ed onto whatever is present.
    /// Pass an empty `Vec` (with a reserved capacity matching your batch budget)
    /// for the common case.
    pub fn recv_many(&mut self, out: &mut Vec<Bytes>) -> usize {
        self.conn.datagrams.recv_many(out)
    }

    /// Bytes available in the outgoing datagram buffer
    ///
    /// When greater than zero, [`send`](Self::send)ing a datagram of at most this size is
    /// guaranteed not to cause older datagrams to be dropped.
    pub fn send_buffer_space(&self) -> usize {
        self.conn
            .config
            .datagram_send_buffer_size
            .saturating_sub(self.conn.datagrams.outgoing_total)
    }
}

#[derive(Default)]
pub(super) struct DatagramState {
    /// Number of bytes of datagrams that have been received by the local transport but not
    /// delivered to the application
    pub(super) recv_buffered: usize,
    pub(super) incoming: VecDeque<Datagram>,
    pub(super) outgoing: VecDeque<Datagram>,
    pub(super) outgoing_total: usize,
    pub(super) send_blocked: bool,
}

impl DatagramState {
    pub(super) fn received(
        &mut self,
        datagram: Datagram,
        window: &Option<usize>,
    ) -> Result<bool, TransportError> {
        let window = match window {
            None => {
                return Err(TransportError::PROTOCOL_VIOLATION(
                    "unexpected DATAGRAM frame",
                ));
            }
            Some(x) => *x,
        };

        if datagram.data.len() > window {
            return Err(TransportError::PROTOCOL_VIOLATION("oversized datagram"));
        }

        let was_empty = self.recv_buffered == 0;
        while datagram.data.len() + self.recv_buffered > window {
            debug!("dropping stale datagram");
            self.recv();
        }

        self.recv_buffered += datagram.data.len();
        self.incoming.push_back(datagram);
        Ok(was_empty)
    }

    /// Discard outgoing datagrams with a payload larger than `max_payload` bytes
    ///
    /// Returns whether any datagrams were dropped.
    ///
    /// Used to ensure that reductions in MTU don't get us stuck in a state where we have a datagram
    /// queued but can't send it.
    pub(super) fn drop_oversized(&mut self, max_payload: usize) -> bool {
        let mut dropped_any = false;
        self.outgoing.retain(|datagram| {
            let result = datagram.data.len() < max_payload;
            if !result {
                trace!(
                    "dropping {} byte datagram violating {} byte limit",
                    datagram.data.len(),
                    max_payload
                );
                self.outgoing_total -= datagram.data.len();
                dropped_any = true;
            }
            result
        });
        dropped_any
    }

    /// Attempt to write a datagram frame into `buf`, consuming it from `self.outgoing`
    ///
    /// Returns whether a frame was written. At most `max_size` bytes will be written, including
    /// framing.
    pub(super) fn write<'a, 'b>(
        &mut self,
        buf: &mut PacketBuilder<'a, 'b>,
        stat: &mut FrameStats,
    ) -> bool {
        let Some(datagram) = self.outgoing.pop_front() else {
            return false;
        };

        if buf.frame_space_remaining() < datagram.size(true) {
            // Future work: we could be more clever about cramming small datagrams into
            // mostly-full packets when a larger one is queued first
            self.outgoing.push_front(datagram);
            return false;
        }

        self.outgoing_total -= datagram.data.len();
        buf.write_frame(datagram, stat);
        true
    }

    pub(super) fn recv(&mut self) -> Option<Bytes> {
        let x = self.incoming.pop_front()?.data;
        self.recv_buffered -= x.len();
        Some(x)
    }

    /// Drain all currently-buffered incoming datagrams into `out`, in arrival order.
    ///
    /// Cheaper than calling [`recv`](Self::recv) in a loop: one `VecDeque` drain
    /// instead of N `pop_front`s, and the caller amortizes the connection mutex.
    /// Returns the number of datagrams appended.
    pub(super) fn recv_many(&mut self, out: &mut Vec<Bytes>) -> usize {
        let n = self.incoming.len();
        if n == 0 {
            return 0;
        }
        out.reserve(n);
        while let Some(d) = self.incoming.pop_front() {
            self.recv_buffered -= d.data.len();
            out.push(d.data);
        }
        n
    }
}

/// Errors that can arise when sending a datagram
#[derive(Debug, Error, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum SendDatagramError {
    /// The peer does not support receiving datagram frames
    #[error("datagrams not supported by peer")]
    UnsupportedByPeer,
    /// Datagram support is disabled locally
    #[error("datagram support disabled")]
    Disabled,
    /// The datagram is larger than the connection can currently accommodate
    ///
    /// Indicates that the path MTU minus overhead or the limit advertised by the peer has been
    /// exceeded.
    #[error("datagram too large")]
    TooLarge,
    /// Send would block
    #[error("datagram send blocked")]
    Blocked(Bytes),
}
