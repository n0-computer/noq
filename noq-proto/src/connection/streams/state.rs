use alloc::{boxed::Box, collections::VecDeque, vec::Vec};
use core::{convert::TryFrom, mem};
use std::collections::hash_map;

use rustc_hash::FxHashMap;
use tracing::{debug, trace};

use super::{
    PendingStreamsQueue, Recv, Retransmits, Send, SendState, ShouldTransmit, StreamEvent,
    StreamHalf,
};
use crate::{
    Dir, MAX_STREAM_COUNT, Side, StreamId, TransportError, VarInt,
    connection::{PacketBuilder, stats::FrameStats},
    frame::{self, FrameStruct},
    transport_parameters::TransportParameters,
};

/// Wrapper around `Recv` that facilitates reusing `Recv` instances
#[derive(Debug)]
pub(super) enum StreamRecv {
    /// A `Recv` that is ready to be opened
    Free(Box<Recv>),
    /// A `Recv` that has been opened
    Open(Box<Recv>),
}

impl StreamRecv {
    /// Returns a reference to the inner `Recv` if the stream is open
    pub(super) fn as_open_recv(&self) -> Option<&Recv> {
        match self {
            Self::Open(r) => Some(r),
            _ => None,
        }
    }

    // Returns a mutable reference to the inner `Recv` if the stream is open
    pub(super) fn as_open_recv_mut(&mut self) -> Option<&mut Recv> {
        match self {
            Self::Open(r) => Some(r),
            _ => None,
        }
    }

    // Returns the inner `Recv`
    pub(super) fn into_inner(self) -> Box<Recv> {
        match self {
            Self::Free(r) | Self::Open(r) => r,
        }
    }

    // Reinitialize the stream so the inner `Recv` can be reused
    pub(super) fn free(self, initial_max_data: u64) -> Self {
        match self {
            Self::Free(_) => unreachable!("Self::Free on reinit()"),
            Self::Open(mut recv) => {
                recv.reinit(initial_max_data);
                Self::Free(recv)
            }
        }
    }
}

#[allow(unreachable_pub)] // fuzzing only
pub struct StreamsState {
    pub(super) side: Side,
    // Set of streams that are currently open, or could be immediately opened by the peer
    pub(super) send: FxHashMap<StreamId, Option<Box<Send>>>,
    pub(super) recv: FxHashMap<StreamId, Option<StreamRecv>>,
    pub(super) free_recv: Vec<StreamRecv>,
    pub(super) next: [u64; 2],
    /// Maximum number of locally-initiated streams that may be opened over the lifetime of the
    /// connection so far, per direction
    pub(super) max: [u64; 2],
    /// Maximum number of remotely-initiated streams that may be opened.
    ///
    /// This is over the lifetime of the connection so far, per direction.
    pub(super) max_remote: [u64; 2],
    /// Value of `max_remote` most recently transmitted to the peer in a `MAX_STREAMS` frame
    sent_max_remote: [u64; 2],
    /// Number of streams the peer may open and which aren't fully closed.
    pub(super) allocated_remote_count: [u64; 2],
    /// Size of the desired stream flow control window. May be smaller than
    /// `allocated_remote_count` due to `set_max_concurrent` calls.
    max_concurrent_remote_count: [u64; 2],
    /// Whether `max_concurrent_remote_count` has ever changed
    flow_control_adjusted: bool,
    /// Lowest remotely-initiated stream index that haven't actually been opened by the peer
    pub(super) next_remote: [u64; 2],
    /// Whether the remote opened streams which are not yet reported to the application.
    ///
    /// Per directionality.
    opened: [bool; 2],
    // Next to report to the application, once opened
    pub(super) next_reported_remote: [u64; 2],
    /// Number of outbound streams
    ///
    /// This differs from `self.send.len()` in that it does not include streams that the peer is
    /// permitted to open but which have not yet been opened.
    pub(super) send_streams: usize,
    /// Streams with outgoing data queued, sorted by priority
    pub(super) pending: PendingStreamsQueue,

    events: VecDeque<StreamEvent>,
    /// Streams blocked on connection-level flow control or stream window space
    ///
    /// Streams are only added to this list when a write fails.
    pub(super) connection_blocked: Vec<StreamId>,
    /// Connection-level flow control budget dictated by the peer
    pub(super) max_data: u64,
    /// The initial receive window
    receive_window: u64,
    /// Limit on incoming data, which is transmitted through `MAX_DATA` frames
    local_max_data: u64,
    /// The last value of `MAX_DATA` which had been queued for transmission in
    /// an outgoing `MAX_DATA` frame
    sent_max_data: VarInt,
    /// Sum of current offsets of all send streams.
    pub(super) data_sent: u64,
    /// Sum of end offsets of all receive streams. Includes gaps, so it's an upper bound.
    data_recvd: u64,
    /// Total quantity of unacknowledged outgoing data
    pub(super) unacked_data: u64,
    /// Configured upper bound for `unacked_data`.
    ///
    /// Note this may be less than `unacked_data` if the user has set a new value.
    pub(super) send_window: u64,
    /// Configured upper bound for how much unacked data the peer can send us per stream
    pub(super) stream_receive_window: u64,

    // Pertinent state from the TransportParameters supplied by the peer
    initial_max_stream_data_uni: VarInt,
    initial_max_stream_data_bidi_local: VarInt,
    initial_max_stream_data_bidi_remote: VarInt,

    /// The shrink to be applied to local_max_data when receive_window is shrunk
    receive_window_shrink_debt: u64,
    /// Whether the locally-initiated stream limit has been hit, per direction
    pub(super) streams_blocked: [bool; 2],
    /// Whether the peer accepts RESET_STREAM_AT frames
    pub(super) peer_reset_stream_at: bool,
}

impl StreamsState {
    #[allow(unreachable_pub)] // fuzzing only
    pub fn new(
        side: Side,
        max_remote_uni: VarInt,
        max_remote_bi: VarInt,
        send_window: u64,
        receive_window: VarInt,
        stream_receive_window: VarInt,
    ) -> Self {
        Self {
            side,
            send: FxHashMap::default(),
            recv: FxHashMap::default(),
            free_recv: Vec::new(),
            next: [0, 0],
            max: [0, 0],
            max_remote: [max_remote_bi.into(), max_remote_uni.into()],
            sent_max_remote: [max_remote_bi.into(), max_remote_uni.into()],
            allocated_remote_count: [max_remote_bi.into(), max_remote_uni.into()],
            max_concurrent_remote_count: [max_remote_bi.into(), max_remote_uni.into()],
            flow_control_adjusted: false,
            next_remote: [0, 0],
            opened: [false, false],
            next_reported_remote: [0, 0],
            send_streams: 0,
            pending: PendingStreamsQueue::new(),
            events: VecDeque::new(),
            connection_blocked: Vec::new(),
            max_data: 0,
            receive_window: receive_window.into(),
            local_max_data: receive_window.into(),
            sent_max_data: receive_window,
            data_sent: 0,
            data_recvd: 0,
            unacked_data: 0,
            send_window,
            stream_receive_window: stream_receive_window.into(),
            initial_max_stream_data_uni: 0u32.into(),
            initial_max_stream_data_bidi_local: 0u32.into(),
            initial_max_stream_data_bidi_remote: 0u32.into(),
            receive_window_shrink_debt: 0,
            streams_blocked: [false, false],
            peer_reset_stream_at: false,
        }
    }

    /// Set whether the peer accepts RESET_STREAM_AT frames
    #[cfg(fuzzing)]
    pub fn set_peer_reset_stream_at(&mut self, value: bool) {
        self.peer_reset_stream_at = value;
    }

    pub(crate) fn set_params(&mut self, params: &TransportParameters) {
        self.initial_max_stream_data_uni = params.initial_max_stream_data_uni;
        self.initial_max_stream_data_bidi_local = params.initial_max_stream_data_bidi_local;
        self.initial_max_stream_data_bidi_remote = params.initial_max_stream_data_bidi_remote;
        self.max[Dir::Bi as usize] = params.initial_max_streams_bidi.into();
        self.max[Dir::Uni as usize] = params.initial_max_streams_uni.into();
        self.peer_reset_stream_at = params.reset_stream_at;
        self.received_max_data(params.initial_max_data);
        for (&id, slot) in self.send.iter_mut() {
            if id.initiator() != self.side
                && id.dir() == Dir::Bi
                && let Some(s) = slot.as_mut()
            {
                s.max_data = params.initial_max_stream_data_bidi_local.into();
            }
        }
    }

    /// Ensure we have space for at least a full flow control window of remotely-initiated streams
    /// to be open, and notify the peer if the window has moved
    fn ensure_remote_streams(&mut self, dir: Dir) {
        let new_count = self.max_concurrent_remote_count[dir as usize]
            .saturating_sub(self.allocated_remote_count[dir as usize]);
        self.allocated_remote_count[dir as usize] += new_count;
        self.max_remote[dir as usize] += new_count;
    }

    pub(crate) fn zero_rtt_rejected(&mut self) {
        // Revert to initial state for outgoing streams
        for dir in Dir::iter() {
            for i in 0..self.next[dir as usize] {
                // We don't bother calling `stream_freed` here because we explicitly reset affected
                // counters below.
                let id = StreamId::new(self.side, dir, i);
                self.send.remove(&id).unwrap();
                if let Dir::Bi = dir {
                    self.recv.remove(&id).unwrap();
                }
            }
            self.next[dir as usize] = 0;

            // If 0-RTT was rejected, any flow control frames we sent were lost.
            if self.flow_control_adjusted {
                // Conservative approximation of whatever we sent in transport parameters
                self.sent_max_remote[dir as usize] = 0;
            }
        }

        self.pending.clear();
        self.send_streams = 0;
        self.data_sent = 0;
        self.connection_blocked.clear();
    }

    /// Process incoming stream frame
    ///
    /// If successful, returns whether a `MAX_DATA` frame needs to be transmitted
    pub(crate) fn received(
        &mut self,
        frame: frame::Stream,
        payload_len: usize,
    ) -> Result<ShouldTransmit, TransportError> {
        let id = frame.id;
        self.validate_receive_id(id).inspect_err(|_e| {
            debug!("received illegal STREAM frame");
        })?;

        // Create state for this stream if the remote peer created it.
        let newly_created = self.ensure_remote(id);

        let Some(rs) = self
            .recv
            .get_mut(&id)
            .map(get_or_insert_recv(self.stream_receive_window))
        else {
            trace!("dropping frame for closed stream");
            return Ok(ShouldTransmit(false));
        };

        if !rs.accepts_data_at(frame.offset) {
            trace!("dropping frame for finished stream");
            return Ok(ShouldTransmit(false));
        }

        let (new_bytes, closed) =
            rs.ingest(frame, payload_len, self.data_recvd, self.local_max_data)?;
        self.data_recvd = self.data_recvd.saturating_add(new_bytes);

        if !rs.stopped {
            // Newly opened streams are inherently readable; the app discovers them via
            // `StreamEvent::Opened` and a separate `Readable` would be redundant.
            if !newly_created {
                self.events.push_back(StreamEvent::Readable { id });
            }
            return Ok(ShouldTransmit(false));
        }

        // We don't buffer data on stopped streams, so issue flow control credit immediately
        let credit = rs.take_unread_credit();

        // Stopped streams become closed instantly on FIN, so check whether we need to clean up
        if closed {
            let rs = self.recv.remove(&id).flatten().unwrap();
            self.stream_recv_freed(id, rs);
        }

        Ok(self.add_read_credits(credit))
    }

    /// Process incoming RESET_STREAM frame
    ///
    /// If successful, returns whether a `MAX_DATA` frame needs to be transmitted
    #[allow(unreachable_pub)] // fuzzing only
    pub fn received_reset(
        &mut self,
        frame: frame::ResetStream,
    ) -> Result<ShouldTransmit, TransportError> {
        let frame::ResetStream {
            id,
            error_code,
            final_offset,
        } = frame;
        self.validate_receive_id(id).inspect_err(|_e| {
            debug!("received illegal RESET_STREAM frame");
        })?;
        self.received_reset_inner(id, error_code, final_offset, VarInt(0))
    }

    /// Process incoming RESET_STREAM_AT frame
    ///
    /// If successful, returns whether a `MAX_DATA` frame needs to be transmitted
    #[allow(unreachable_pub)] // fuzzing only
    pub fn received_reset_at(
        &mut self,
        frame: frame::ResetStreamAt,
    ) -> Result<ShouldTransmit, TransportError> {
        let frame::ResetStreamAt {
            id,
            error_code,
            final_offset,
            reliable_size,
        } = frame;
        self.validate_receive_id(id).inspect_err(|_e| {
            debug!("received illegal RESET_STREAM_AT frame");
        })?;
        if reliable_size > final_offset {
            return Err(TransportError::FRAME_ENCODING_ERROR(
                "reliable size exceeds final size",
            ));
        }
        self.received_reset_inner(id, error_code, final_offset, reliable_size)
    }

    fn received_reset_inner(
        &mut self,
        id: StreamId,
        error_code: VarInt,
        final_offset: VarInt,
        reliable_size: VarInt,
    ) -> Result<ShouldTransmit, TransportError> {
        // Create state for this stream if the remote peer created it.
        let newly_created = self.ensure_remote(id);

        let Some(rs) = self
            .recv
            .get_mut(&id)
            .map(get_or_insert_recv(self.stream_receive_window))
        else {
            trace!("received reset on closed stream");
            return Ok(ShouldTransmit(false));
        };

        // State transition
        let end = rs.end;
        if !rs.reset(
            error_code,
            final_offset,
            reliable_size,
            self.data_recvd,
            self.local_max_data,
        )? {
            // Redundant reset
            return Ok(ShouldTransmit(false));
        }
        let credit = rs.take_unread_credit();
        let stopped = rs.stopped;
        if stopped {
            // Stopped streams should be disposed immediately on reset
            let rs = self.recv.remove(&id).flatten().unwrap();
            self.stream_recv_freed(id, rs);
        } else if !newly_created {
            // Newly opened streams are inherently readable; the app discovers them via
            // `StreamEvent::Opened` and a separate `Readable` would be redundant.
            self.events.push_back(StreamEvent::Readable { id });
        }

        // Update connection-level flow control. `reset` checked that end <= final_offset.
        self.data_recvd = self
            .data_recvd
            .saturating_add(u64::from(final_offset) - end);
        Ok(match credit {
            0 => ShouldTransmit(false),
            _ => self.add_read_credits(credit),
        })
    }

    /// Process incoming `STOP_SENDING` frame
    ///
    /// If a RESET_STREAM frame now needs to be sent, returns its error code.
    #[allow(unreachable_pub)] // fuzzing only
    pub fn received_stop_sending(&mut self, id: StreamId, error_code: VarInt) -> Option<VarInt> {
        // Create state for this stream if the remote peer created it.
        self.ensure_remote(id);

        let max_send_data = self.max_send_data(id);
        let stream = self
            .send
            .get_mut(&id)
            .map(get_or_insert_send(max_send_data))?;

        if stream.try_stop(error_code) {
            self.events
                .push_back(StreamEvent::Stopped { id, error_code });
        }

        // The peer doesn't want the data still to be delivered after RESET_STREAM_AT, so reset
        // the stream completely, keeping the original error code.
        if !stream.abandon() {
            return None;
        }
        self.unacked_data -= stream.pending.unacked();
        stream.reset_frame(id).map(|frame| frame.error_code)
    }

    pub(crate) fn reset_acked(&mut self, id: StreamId) {
        match self.send.entry(id) {
            hash_map::Entry::Vacant(_) => {}
            hash_map::Entry::Occupied(mut e) => {
                if e.get_mut().as_mut().is_some_and(|s| s.reset_acked()) {
                    e.remove_entry();
                    self.stream_freed(id, StreamHalf::Send);
                }
            }
        }
    }

    /// Whether any stream data is queued, regardless of control frames
    pub(crate) fn can_send_stream_data(&self) -> bool {
        // Reset streams may linger in the pending stream list, but will only produce stream frames
        // for data still to be delivered after RESET_STREAM_AT
        self.pending.iter().any(|stream| {
            self.send
                .get(&stream.id)
                .and_then(|s| s.as_ref())
                .is_some_and(|s| !s.is_reset() || s.is_pending())
        })
    }

    /// Whether MAX_STREAM_DATA frames could be sent for stream `id`
    pub(crate) fn can_send_flow_control(&self, id: StreamId) -> bool {
        self.recv
            .get(&id)
            .and_then(|s| s.as_ref())
            .and_then(|s| s.as_open_recv())
            .is_some_and(|s| s.can_send_flow_control())
    }

    pub(in crate::connection) fn write_control_frames<'a, 'b>(
        &mut self,
        builder: &mut PacketBuilder<'a, 'b>,
        pending: &mut Retransmits,
        stats: &mut FrameStats,
    ) {
        // RESET_STREAM and RESET_STREAM_AT
        while builder.frame_space_remaining() > frame::ResetStreamAt::SIZE_BOUND {
            let Some((id, _)) = pending.reset_stream.pop() else {
                break;
            };
            // Always send the stream's current reset, which has the smallest reliable size
            let Some(frame) = self
                .send
                .get(&id)
                .and_then(|s| s.as_ref())
                .and_then(|s| s.reset_frame(id))
            else {
                continue;
            };
            match frame.reliable_size.into_inner() {
                0 => builder.write_frame(
                    frame::ResetStream {
                        id,
                        error_code: frame.error_code,
                        final_offset: frame.final_offset,
                    },
                    stats,
                ),
                _ => builder.write_frame(frame, stats),
            }
        }

        // STOP_SENDING
        while builder.frame_space_remaining() > frame::StopSending::SIZE_BOUND {
            let Some(frame) = pending.stop_sending.pop() else {
                break;
            };
            // We may need to transmit STOP_SENDING even for streams whose state we have discarded,
            // because we are able to discard local state for stopped streams immediately upon
            // receiving FIN, even if the peer still has arbitrarily large amounts of data to
            // (re)transmit due to loss or unconventional sending strategy. We could fine-tune this
            // a little by dropping the frame if we specifically know the stream's been reset by the
            // peer, but we discard that information as soon as the application consumes it, so it
            // can't be relied upon regardless.
            builder.write_frame(frame, stats);
        }

        // MAX_DATA
        if pending.max_data && builder.frame_space_remaining() > 9 {
            pending.max_data = false;

            // `local_max_data` can grow bigger than `VarInt`.
            // For transmission inside QUIC frames we need to clamp it to the
            // maximum allowed `VarInt` size.
            let max = VarInt::try_from(self.local_max_data).unwrap_or(VarInt::MAX);

            if max > self.sent_max_data {
                // Record that a `MAX_DATA` announcing a certain window was sent. This will
                // suppress enqueuing further `MAX_DATA` frames unless either the previous
                // transmission was not acknowledged or the window further increased.
                self.sent_max_data = max;
            }

            builder.write_frame(frame::MaxData(max), stats);
        }

        // MAX_STREAM_DATA
        while builder.frame_space_remaining() > 17 {
            let id = match pending.max_stream_data.iter().next() {
                Some(x) => *x,
                None => break,
            };
            pending.max_stream_data.remove(&id);
            let Some(rs) = self
                .recv
                .get_mut(&id)
                .and_then(|s| s.as_mut())
                .and_then(|s| s.as_open_recv_mut())
            else {
                continue;
            };
            if !rs.can_send_flow_control() {
                continue;
            }

            let (max, _) = rs.max_stream_data(self.stream_receive_window);
            rs.record_sent_max_stream_data(max);
            builder.write_frame(frame::MaxStreamData { id, offset: max }, stats);
        }

        // MAX_STREAMS
        for dir in Dir::iter() {
            if !pending.max_stream_id[dir as usize] || builder.frame_space_remaining() <= 9 {
                continue;
            }

            pending.max_stream_id[dir as usize] = false;
            self.sent_max_remote[dir as usize] = self.max_remote[dir as usize];
            let count = self.max_remote[dir as usize];
            builder.write_frame(frame::MaxStreams { dir, count }, stats);
        }

        // STREAMS_BLOCKED
        for dir in Dir::iter() {
            if self.streams_blocked[dir as usize] {
                pending.streams_blocked[dir as usize] = true;
                self.streams_blocked[dir as usize] = false;
            }

            if !pending.streams_blocked[dir as usize] || builder.frame_space_remaining() <= 9 {
                continue;
            }

            pending.streams_blocked[dir as usize] = false;
            let limit = self.max[dir as usize];
            builder.write_frame(frame::StreamsBlocked { dir, limit }, stats);
        }
    }

    pub(in crate::connection) fn write_stream_frames<'a, 'b>(
        &mut self,
        builder: &mut PacketBuilder<'a, 'b>,
        fair: bool,
        stats: &mut FrameStats,
    ) {
        while builder.frame_space_remaining() > frame::Stream::SIZE_BOUND {
            // Pop the stream of the highest priority that currently has pending data. If
            // the stream still has some pending data left after writing, it will be
            // reinserted, otherwise not
            let Some(stream) = self.pending.pop() else {
                break;
            };

            let id = stream.id;

            let Some(stream) = self.send.get_mut(&id).and_then(|s| s.as_mut()) else {
                // Stream was reset with pending data and the reset was acknowledged
                continue;
            };

            // Reset streams aren't removed from the pending list and still exist while the peer
            // hasn't acknowledged the reset, but should only generate STREAM frames for data still
            // to be delivered after RESET_STREAM_AT, so we need to check for them explicitly.
            if stream.is_reset() && !stream.is_pending() {
                continue;
            }

            // Now that we know the `StreamId`, we can better account for how many bytes
            // are required to encode it.
            let max_buf_size = builder.frame_space_remaining() - 1 - VarInt::size(id.into());
            let (offsets, encode_length) = stream.pending.poll_transmit(max_buf_size);
            let fin = offsets.end == stream.pending.offset()
                && matches!(stream.state, SendState::DataSent { .. });
            if fin {
                stream.fin_pending = false;
            }

            if stream.is_pending() {
                // If the stream still has pending data, reinsert it, possibly with an updated
                // priority value Fairness with other streams is achieved by
                // implementing round-robin scheduling, so that the other streams
                // will have a chance to write data before we touch this stream
                // again.
                if fair {
                    self.pending.push_pending(id, stream.priority);
                } else {
                    self.pending.reinsert_pending(id, stream.priority);
                }
            }

            let range = offsets.clone();
            let meta = frame::StreamMeta { id, offsets, fin };
            builder.write_frame(meta.encoder(encode_length), stats);
            stream.pending.get_into(range, builder.buf);
        }
    }

    #[cfg(test)]
    fn write_frames_for_test(&mut self, capacity: usize, fair: bool) -> frame::StreamMetaVec {
        let buf = &mut Vec::with_capacity(capacity);
        let mut tbuf =
            crate::connection::TransmitBuf::new(buf, core::num::NonZeroUsize::MIN, 1_200);
        tbuf.start_new_datagram_with_size(capacity);
        let builder = &mut PacketBuilder::simple_data_buf(&mut tbuf);
        let stats = &mut FrameStats::default();
        self.write_stream_frames(builder, fair, stats);
        builder.sent_frames().stream_frames.clone()
    }

    pub(crate) fn received_ack_of(&mut self, mut frame: frame::StreamMeta) {
        let mut entry = match self.send.entry(frame.id) {
            hash_map::Entry::Vacant(_) => return,
            hash_map::Entry::Occupied(e) => e,
        };

        let Some(stream) = entry.get_mut().as_mut() else {
            // Because we only call this after sending data on this stream,
            // this closure should be unreachable. If we did somehow screw that up,
            // then we might hit an underflow below with unpredictable effects down
            // the line. Best to short-circuit.
            return;
        };

        if stream.is_abandoned() {
            // We account for outstanding data on reset streams at time of reset
            return;
        }
        let reset = stream.is_reset();
        if reset {
            // Data after RESET_STREAM_AT's reliable size was accounted for at time of reset
            let end = frame.offsets.end.min(stream.pending.offset());
            frame.offsets = frame.offsets.start.min(end)..end;
        }
        let id = frame.id;
        self.unacked_data -= frame.offsets.end - frame.offsets.start;
        if !stream.ack(frame) {
            // The stream is unfinished or may still need retransmits
            return;
        }

        entry.remove_entry();
        self.stream_freed(id, StreamHalf::Send);
        if !reset {
            self.events.push_back(StreamEvent::Finished { id });
        }
    }

    pub(crate) fn retransmit(&mut self, frame: frame::StreamMeta) {
        let Some(stream) = self.send.get_mut(&frame.id).and_then(|s| s.as_mut()) else {
            // Loss of data on a closed stream is a noop
            return;
        };
        if stream.is_abandoned() {
            // Abandoned data is never retransmitted
            return;
        }
        let mut offsets = frame.offsets;
        if stream.is_reset() {
            // Only data before RESET_STREAM_AT's reliable size needs to be delivered
            offsets.end = offsets.end.min(stream.pending.offset());
            if offsets.is_empty() {
                return;
            }
        }
        if !stream.is_pending() {
            self.pending.push_pending(frame.id, stream.priority);
        }
        stream.fin_pending |= frame.fin && !stream.is_reset();
        stream.pending.retransmit(offsets);
    }

    pub(crate) fn retransmit_all_for_0rtt(&mut self) {
        for dir in Dir::iter() {
            for index in 0..self.next[dir as usize] {
                let id = StreamId::new(Side::Client, dir, index);
                let Some(stream) = self.send.get_mut(&id).and_then(|s| s.as_mut()) else {
                    continue;
                };
                if stream.pending.is_fully_acked() && !stream.fin_pending {
                    // Stream data can't be acked in 0-RTT, so we must not have sent anything on
                    // this stream
                    continue;
                }
                if !stream.is_pending() {
                    self.pending.push_pending(id, stream.priority);
                }
                stream.pending.retransmit_all_for_0rtt();
            }
        }
    }

    pub(crate) fn received_max_streams(
        &mut self,
        dir: Dir,
        count: u64,
    ) -> Result<(), TransportError> {
        if count > MAX_STREAM_COUNT {
            return Err(TransportError::FRAME_ENCODING_ERROR(
                "unrepresentable stream limit",
            ));
        }

        let current = &mut self.max[dir as usize];
        if count > *current {
            *current = count;
            self.streams_blocked[dir as usize] = false;
            self.events.push_back(StreamEvent::Available { dir });
        }

        Ok(())
    }

    /// Handle increase to connection-level flow control limit
    pub(crate) fn received_max_data(&mut self, n: VarInt) {
        self.max_data = self.max_data.max(n.into());
    }

    pub(crate) fn received_max_stream_data(
        &mut self,
        id: StreamId,
        offset: u64,
    ) -> Result<(), TransportError> {
        if id.initiator() != self.side && id.dir() == Dir::Uni {
            debug!("got MAX_STREAM_DATA on recv-only {}", id);
            return Err(TransportError::STREAM_STATE_ERROR(
                "MAX_STREAM_DATA on recv-only stream",
            ));
        }

        // Create state for this stream if the remote peer created it.
        self.ensure_remote(id);

        let write_limit = self.write_limit();
        let max_send_data = self.max_send_data(id);
        if let Some(ss) = self
            .send
            .get_mut(&id)
            .map(get_or_insert_send(max_send_data))
        {
            if ss.increase_max_data(offset) {
                if write_limit > 0 {
                    self.events.push_back(StreamEvent::Writable { id });
                } else if !ss.connection_blocked {
                    // The stream is still blocked on the connection flow control
                    // window. In order to get unblocked when the window relaxes
                    // it needs to be in the connection blocked list.
                    ss.connection_blocked = true;
                    self.connection_blocked.push(id);
                }
            }
        } else if id.initiator() == self.side && self.is_local_unopened(id) {
            debug!("got MAX_STREAM_DATA on unopened {}", id);
            return Err(TransportError::STREAM_STATE_ERROR(
                "MAX_STREAM_DATA on unopened stream",
            ));
        }

        Ok(())
    }

    /// Returns the maximum amount of data this is allowed to be written on the connection
    pub(crate) fn write_limit(&self) -> u64 {
        (self.max_data - self.data_sent)
            // `send_window` can be set after construction to something *less* than `unacked_data`
            .min(self.send_window.saturating_sub(self.unacked_data))
    }

    /// Yield stream events
    pub(crate) fn poll(&mut self) -> Option<StreamEvent> {
        if let Some(dir) = Dir::iter().find(|&i| mem::replace(&mut self.opened[i as usize], false))
        {
            return Some(StreamEvent::Opened { dir });
        }

        if self.write_limit() > 0 {
            while let Some(id) = self.connection_blocked.pop() {
                let Some(stream) = self.send.get_mut(&id).and_then(|s| s.as_mut()) else {
                    continue;
                };

                debug_assert!(stream.connection_blocked);
                stream.connection_blocked = false;

                // If it's no longer sensible to write to a stream (even to detect an error) then
                // don't report it.
                if stream.is_writable() && stream.max_data > stream.offset() {
                    return Some(StreamEvent::Writable { id });
                }
            }
        }

        self.events.pop_front()
    }

    /// Queues MAX_STREAM_ID frames in `pending` if needed
    ///
    /// Returns whether any frames were queued.
    pub(crate) fn queue_max_stream_id(&mut self, pending: &mut Retransmits) -> bool {
        let mut queued = false;
        for dir in Dir::iter() {
            let diff = self.max_remote[dir as usize] - self.sent_max_remote[dir as usize];
            // To reduce traffic, only announce updates if at least 1/8 of the flow control window
            // has been consumed.
            if diff > self.max_concurrent_remote_count[dir as usize] / 8 {
                pending.max_stream_id[dir as usize] = true;
                queued = true;
            }
        }
        queued
    }

    /// Check for errors entailed by the peer's use of `id` as a send stream
    fn validate_receive_id(&mut self, id: StreamId) -> Result<(), TransportError> {
        if self.side == id.initiator() {
            match id.dir() {
                Dir::Uni => {
                    return Err(TransportError::STREAM_STATE_ERROR(
                        "illegal operation on send-only stream",
                    ));
                }
                Dir::Bi if id.index() >= self.next[Dir::Bi as usize] => {
                    return Err(TransportError::STREAM_STATE_ERROR(
                        "operation on unopened stream",
                    ));
                }
                Dir::Bi => {}
            };
        } else {
            let limit = self.max_remote[id.dir() as usize];
            if id.index() >= limit {
                return Err(TransportError::STREAM_LIMIT_ERROR(""));
            }
        }
        Ok(())
    }

    /// Whether a locally initiated stream has never been open
    pub(crate) fn is_local_unopened(&self, id: StreamId) -> bool {
        id.index() >= self.next[id.dir() as usize]
    }

    pub(crate) fn set_max_concurrent(&mut self, dir: Dir, count: VarInt) {
        self.flow_control_adjusted = true;
        self.max_concurrent_remote_count[dir as usize] = count.into();
        self.ensure_remote_streams(dir);
    }

    pub(crate) fn max_concurrent(&self, dir: Dir) -> u64 {
        self.allocated_remote_count[dir as usize]
    }

    pub(crate) fn set_send_window(&mut self, send_window: u64) {
        self.send_window = send_window;
    }

    /// Set the receive_window and returns whether the receive_window has been
    /// expanded or shrunk: true if expanded, false if shrunk.
    pub(crate) fn set_receive_window(&mut self, receive_window: VarInt) -> bool {
        let receive_window = receive_window.into();
        let mut expanded = false;
        if receive_window > self.receive_window {
            self.local_max_data = self
                .local_max_data
                .saturating_add(receive_window - self.receive_window);
            expanded = true;
        } else {
            let diff = self.receive_window - receive_window;
            self.receive_window_shrink_debt = self.receive_window_shrink_debt.saturating_add(diff);
        }
        self.receive_window = receive_window;
        expanded
    }

    /// Insert `(id, None)` placeholders for a locally-initiated stream into `send` (and `recv`
    /// for bidi). Called from `Streams::open`; the caller guarantees the id is fresh.
    pub(super) fn insert_local(&mut self, id: StreamId) {
        debug_assert_eq!(id.initiator(), self.side);
        assert!(self.send.insert(id, None).is_none());
        if id.dir() == Dir::Bi {
            let recv = self.free_recv.pop();
            assert!(self.recv.insert(id, recv).is_none());
        }
    }

    /// Allocate any new remote streams when a packet arrives for a stream id.
    /// Any streams above `next_remote` are considered in the default state, avoiding allocations.
    /// Once we receive a packet for a new remote stream, we advance `next_remote` and allocate
    /// actual state. If there's a gap, we insert `None` placeholders for the missing streams.
    /// Returns `true` if `id` was a newly allocated remote stream.
    fn ensure_remote(&mut self, id: StreamId) -> bool {
        let dir = id.dir();
        let dir_idx = dir as usize;

        // If we initiated this stream, nothing to do
        if id.initiator() == self.side
            // If this stream is larger than the max allowed, return.
            // NOTE: STREAM/RESET_STREAM already enforces this, however STOP_SENDING/MAX_STREAM_DATA do not
            || id.index() >= self.max_remote[dir_idx]
            // If this stream has already been opened, nothing to do
            || id.index() < self.next_remote[dir_idx]
        {
            return false;
        }

        // Create all of the streams between the largest opened and this stream.
        for i in self.next_remote[dir_idx]..=id.index() {
            let id = StreamId::new(!self.side, dir, i);
            let recv = self.free_recv.pop();
            assert!(self.recv.insert(id, recv).is_none());
            if dir == Dir::Bi {
                assert!(self.send.insert(id, None).is_none());
            }
        }
        self.next_remote[dir_idx] = id.index() + 1;
        self.opened[dir_idx] = true;
        true
    }

    /// Adds credits to the connection flow control window
    ///
    /// Returns whether a `MAX_DATA` frame should be enqueued as soon as possible.
    /// This will only be the case if the window update would is significant
    /// enough. As soon as a window update with a `MAX_DATA` frame has been
    /// queued, the [`Recv::record_sent_max_stream_data`] function should be called to
    /// suppress sending further updates until the window increases significantly
    /// again.
    pub(super) fn add_read_credits(&mut self, credits: u64) -> ShouldTransmit {
        if credits > self.receive_window_shrink_debt {
            let net_credits = credits - self.receive_window_shrink_debt;
            self.local_max_data = self.local_max_data.saturating_add(net_credits);
            self.receive_window_shrink_debt = 0;
        } else {
            self.receive_window_shrink_debt -= credits;
        }

        if self.local_max_data > VarInt::MAX.into_inner() {
            return ShouldTransmit(false);
        }

        // Only announce a window update if it's significant enough
        // to make it worthwhile sending a MAX_DATA frame.
        // We use a fraction of the configured connection receive window to make
        // the decision, to accommodate for connection using bigger windows requiring
        // less updates.
        let diff = self.local_max_data - self.sent_max_data.into_inner();
        ShouldTransmit(diff >= (self.receive_window / 8))
    }

    /// Update counters for removal of a stream
    pub(super) fn stream_freed(&mut self, id: StreamId, half: StreamHalf) {
        if id.initiator() != self.side {
            let fully_free = id.dir() == Dir::Uni
                || match half {
                    StreamHalf::Send => !self.recv.contains_key(&id),
                    StreamHalf::Recv => !self.send.contains_key(&id),
                };
            if fully_free {
                self.allocated_remote_count[id.dir() as usize] -= 1;
                self.ensure_remote_streams(id.dir());
            }
        }
        if half == StreamHalf::Send {
            self.send_streams -= 1;
        }
    }

    pub(super) fn stream_recv_freed(&mut self, id: StreamId, recv: StreamRecv) {
        self.free_recv.push(recv.free(self.stream_receive_window));
        self.stream_freed(id, StreamHalf::Recv);
    }

    pub(super) fn max_send_data(&self, id: StreamId) -> VarInt {
        let remote = self.side != id.initiator();
        match id.dir() {
            Dir::Uni => self.initial_max_stream_data_uni,
            // Remote/local appear reversed here because the transport parameters are named from
            // the perspective of the peer.
            Dir::Bi if remote => self.initial_max_stream_data_bidi_local,
            Dir::Bi => self.initial_max_stream_data_bidi_remote,
        }
    }
}

#[inline]
pub(super) fn get_or_insert_send(
    max_data: VarInt,
) -> impl Fn(&mut Option<Box<Send>>) -> &mut Box<Send> {
    move |opt| opt.get_or_insert_with(|| Send::new(max_data))
}

#[inline]
pub(super) fn get_or_insert_recv(
    initial_max_data: u64,
) -> impl FnMut(&mut Option<StreamRecv>) -> &mut Recv {
    move |opt| {
        *opt = opt.take().map(|s| match s {
            StreamRecv::Free(recv) => StreamRecv::Open(recv),
            s => s,
        });
        opt.get_or_insert_with(|| StreamRecv::Open(Recv::new(initial_max_data)))
            .as_open_recv_mut()
            .unwrap()
    }
}

#[cfg(test)]
mod tests {
    use alloc::vec;

    use super::*;
    use crate::{
        FinishError, ReadableError, RecvStream, ResetStreamAtError, SendStream, TransportErrorCode,
        WriteError, connection::State as ConnState, connection::Streams,
    };
    use bytes::Bytes;

    fn make(side: Side) -> StreamsState {
        StreamsState::new(
            side,
            128u32.into(),
            128u32.into(),
            1024 * 1024,
            (1024 * 1024u32).into(),
            (1024 * 1024u32).into(),
        )
    }

    #[test]
    fn trivial_flow_control() {
        let mut client = StreamsState::new(
            Side::Client,
            1u32.into(),
            1u32.into(),
            1024 * 1024,
            (1024 * 1024u32).into(),
            (1024 * 1024u32).into(),
        );
        let id = StreamId::new(Side::Server, Dir::Uni, 0);
        let initial_max = client.local_max_data;
        const MESSAGE_SIZE: usize = 2048;
        assert_eq!(
            client
                .received(
                    frame::Stream {
                        id,
                        offset: 0,
                        fin: true,
                        data: Bytes::from_static(&[0; MESSAGE_SIZE]),
                    },
                    2048
                )
                .unwrap(),
            ShouldTransmit(false)
        );
        assert_eq!(client.data_recvd, 2048);
        assert_eq!(client.local_max_data - initial_max, 0);

        let mut pending = Retransmits::default();
        let mut recv = RecvStream {
            id,
            state: &mut client,
            pending: &mut pending,
        };

        let mut chunks = recv.read(true).unwrap();
        assert_eq!(
            chunks.next(MESSAGE_SIZE).unwrap().unwrap().bytes.len(),
            MESSAGE_SIZE
        );
        assert!(chunks.next(0).unwrap().is_none());
        let should_transmit = chunks.finalize();
        assert!(should_transmit.0);
        assert!(pending.max_stream_id[Dir::Uni as usize]);
        assert_eq!(client.local_max_data - initial_max, MESSAGE_SIZE as u64);
    }

    #[test]
    fn reset_flow_control() {
        let mut client = make(Side::Client);
        let id = StreamId::new(Side::Server, Dir::Uni, 0);
        let initial_max = client.local_max_data;
        assert_eq!(
            client
                .received(
                    frame::Stream {
                        id,
                        offset: 0,
                        fin: false,
                        data: Bytes::from_static(&[0; 2048]),
                    },
                    2048
                )
                .unwrap(),
            ShouldTransmit(false)
        );
        assert_eq!(client.data_recvd, 2048);
        assert_eq!(client.local_max_data - initial_max, 0);

        let mut pending = Retransmits::default();
        let mut recv = RecvStream {
            id,
            state: &mut client,
            pending: &mut pending,
        };

        let mut chunks = recv.read(true).unwrap();
        chunks.next(1024).unwrap();
        let _ = chunks.finalize();
        assert_eq!(client.local_max_data - initial_max, 1024);
        assert_eq!(
            client
                .received_reset(frame::ResetStream {
                    id,
                    error_code: 0u32.into(),
                    final_offset: 4096u32.into(),
                })
                .unwrap(),
            ShouldTransmit(false)
        );

        assert_eq!(client.data_recvd, 4096);
        assert_eq!(client.local_max_data - initial_max, 4096);

        // Ensure reading after a reset doesn't issue redundant credit
        let mut recv = RecvStream {
            id,
            state: &mut client,
            pending: &mut pending,
        };
        let mut chunks = recv.read(true).unwrap();
        assert_eq!(
            chunks.next(1024).unwrap_err(),
            crate::ReadError::Reset(0u32.into())
        );
        let _ = chunks.finalize();
        assert_eq!(client.data_recvd, 4096);
        assert_eq!(client.local_max_data - initial_max, 4096);
    }

    #[test]
    fn reset_after_empty_frame_flow_control() {
        let mut client = make(Side::Client);
        let id = StreamId::new(Side::Server, Dir::Uni, 0);
        let initial_max = client.local_max_data;
        assert_eq!(
            client
                .received(
                    frame::Stream {
                        id,
                        offset: 4096,
                        fin: false,
                        data: Bytes::from_static(&[0; 0]),
                    },
                    0
                )
                .unwrap(),
            ShouldTransmit(false)
        );
        assert_eq!(client.data_recvd, 4096);
        assert_eq!(client.local_max_data - initial_max, 0);
        assert_eq!(
            client
                .received_reset(frame::ResetStream {
                    id,
                    error_code: 0u32.into(),
                    final_offset: 4096u32.into(),
                })
                .unwrap(),
            ShouldTransmit(false)
        );
        assert_eq!(client.data_recvd, 4096);
        assert_eq!(client.local_max_data - initial_max, 4096);
    }

    #[test]
    fn duplicate_reset_flow_control() {
        let mut client = make(Side::Client);
        let id = StreamId::new(Side::Server, Dir::Uni, 0);
        assert_eq!(
            client
                .received_reset(frame::ResetStream {
                    id,
                    error_code: 0u32.into(),
                    final_offset: 4096u32.into(),
                })
                .unwrap(),
            ShouldTransmit(false)
        );
        assert_eq!(client.data_recvd, 4096);
        assert_eq!(
            client
                .received_reset(frame::ResetStream {
                    id,
                    error_code: 0u32.into(),
                    final_offset: 4096u32.into(),
                })
                .unwrap(),
            ShouldTransmit(false)
        );
        assert_eq!(client.data_recvd, 4096);
    }

    #[test]
    fn recv_stopped() {
        let mut client = make(Side::Client);
        let id = StreamId::new(Side::Server, Dir::Uni, 0);
        let initial_max = client.local_max_data;
        assert_eq!(
            client
                .received(
                    frame::Stream {
                        id,
                        offset: 0,
                        fin: false,
                        data: Bytes::from_static(&[0; 32]),
                    },
                    32
                )
                .unwrap(),
            ShouldTransmit(false)
        );
        assert_eq!(client.local_max_data, initial_max);

        let mut pending = Retransmits::default();
        let mut recv = RecvStream {
            id,
            state: &mut client,
            pending: &mut pending,
        };

        recv.stop(0u32.into()).unwrap();
        assert_eq!(recv.pending.stop_sending.len(), 1);
        assert!(!recv.pending.max_data);

        assert!(recv.stop(0u32.into()).is_err());
        assert_eq!(recv.read(true).err(), Some(ReadableError::ClosedStream));
        assert_eq!(recv.read(false).err(), Some(ReadableError::ClosedStream));
        assert!(recv.is_ordered().is_err());

        assert_eq!(client.local_max_data - initial_max, 32);
        assert_eq!(
            client
                .received(
                    frame::Stream {
                        id,
                        offset: 32,
                        fin: true,
                        data: Bytes::from_static(&[0; 16]),
                    },
                    16
                )
                .unwrap(),
            ShouldTransmit(false)
        );
        assert_eq!(client.local_max_data - initial_max, 48);
        assert!(!client.recv.contains_key(&id));
    }

    #[test]
    fn recv_stream_ordering_mode() {
        let mut client = make(Side::Client);
        let id = StreamId::new(Side::Server, Dir::Uni, 0);
        let _ = client
            .received(
                frame::Stream {
                    id,
                    offset: 0,
                    fin: false,
                    data: Bytes::from_static(b"hello"),
                },
                5,
            )
            .unwrap();

        let mut pending = Retransmits::default();
        let mut recv = RecvStream {
            id,
            state: &mut client,
            pending: &mut pending,
        };
        assert_eq!(recv.is_ordered(), Ok(true));
        let _ = recv.read(false).unwrap().finalize();
        assert_eq!(recv.is_ordered(), Ok(false));
    }

    #[test]
    fn stopped_reset() {
        let mut client = make(Side::Client);
        let id = StreamId::new(Side::Server, Dir::Uni, 0);
        // Server opens stream
        assert_eq!(
            client
                .received(
                    frame::Stream {
                        id,
                        offset: 0,
                        fin: false,
                        data: Bytes::from_static(&[0; 32])
                    },
                    32
                )
                .unwrap(),
            ShouldTransmit(false)
        );

        let mut pending = Retransmits::default();
        let mut recv = RecvStream {
            id,
            state: &mut client,
            pending: &mut pending,
        };

        recv.stop(0u32.into()).unwrap();
        assert_eq!(pending.stop_sending.len(), 1);
        assert!(!pending.max_data);

        // Server complies
        let prev_max = client.max_remote[Dir::Uni as usize];
        assert_eq!(
            client
                .received_reset(frame::ResetStream {
                    id,
                    error_code: 0u32.into(),
                    final_offset: 32u32.into(),
                })
                .unwrap(),
            ShouldTransmit(false)
        );
        assert!(!client.recv.contains_key(&id), "stream state is freed");
        assert_eq!(client.max_remote[Dir::Uni as usize], prev_max + 1);
    }

    /// Connection-level credit for a stream that is both stopped and reset must be issued once
    #[test]
    fn stopped_reset_flow_control() {
        let mut client = make(Side::Client);
        let id = StreamId::new(Side::Server, Dir::Uni, 0);
        let initial_max = client.local_max_data;
        let _ = client
            .received(
                frame::Stream {
                    id,
                    offset: 0,
                    fin: false,
                    data: Bytes::from_static(&[0; 32]),
                },
                32,
            )
            .unwrap();

        let mut pending = Retransmits::default();
        let mut recv = RecvStream {
            id,
            state: &mut client,
            pending: &mut pending,
        };
        recv.stop(0u32.into()).unwrap();
        assert_eq!(client.local_max_data - initial_max, 32);

        let _ = client
            .received_reset(frame::ResetStream {
                id,
                error_code: 0u32.into(),
                final_offset: 64u32.into(),
            })
            .unwrap();
        assert_eq!(client.data_recvd, 64);
        assert_eq!(client.local_max_data - initial_max, 64);
    }

    /// Connection-level credit for a stream that is both reset and stopped must be issued once
    #[test]
    fn reset_stopped_flow_control() {
        let mut client = make(Side::Client);
        let id = StreamId::new(Side::Server, Dir::Uni, 0);
        let initial_max = client.local_max_data;
        let _ = client
            .received(
                frame::Stream {
                    id,
                    offset: 0,
                    fin: false,
                    data: Bytes::from_static(&[0; 32]),
                },
                32,
            )
            .unwrap();
        let _ = client
            .received_reset(frame::ResetStream {
                id,
                error_code: 0u32.into(),
                final_offset: 64u32.into(),
            })
            .unwrap();
        assert_eq!(client.local_max_data - initial_max, 64);

        let mut pending = Retransmits::default();
        let mut recv = RecvStream {
            id,
            state: &mut client,
            pending: &mut pending,
        };
        recv.stop(0u32.into()).unwrap();
        assert_eq!(client.data_recvd, 64);
        assert_eq!(client.local_max_data - initial_max, 64);
    }

    #[test]
    fn send_stopped() {
        let mut server = make(Side::Server);
        server.set_params(&TransportParameters {
            initial_max_streams_uni: 1u32.into(),
            initial_max_data: 42u32.into(),
            initial_max_stream_data_uni: 42u32.into(),
            ..TransportParameters::default()
        });

        let (mut pending, state) = (Retransmits::default(), ConnState::established());
        let id = Streams {
            state: &mut server,
            conn_state: &state,
        }
        .open(Dir::Uni)
        .unwrap();

        let mut stream = SendStream {
            id,
            state: &mut server,
            pending: &mut pending,
            conn_state: &state,
        };

        let error_code = 0u32.into();
        stream.state.received_stop_sending(id, error_code);
        assert!(
            stream
                .state
                .events
                .contains(&StreamEvent::Stopped { id, error_code })
        );
        stream.state.events.clear();

        assert_eq!(stream.write(&[]), Err(WriteError::Stopped(error_code)));

        stream.reset(0u32.into()).unwrap();
        assert_eq!(stream.write(&[]), Err(WriteError::ClosedStream));

        // A duplicate frame is a no-op
        stream.state.received_stop_sending(id, error_code);
        assert!(stream.state.events.is_empty());
    }

    #[test]
    fn final_offset_flow_control() {
        let mut client = make(Side::Client);
        assert_eq!(
            client
                .received_reset(frame::ResetStream {
                    id: StreamId::new(Side::Server, Dir::Uni, 0),
                    error_code: 0u32.into(),
                    final_offset: VarInt::MAX,
                })
                .unwrap_err()
                .code,
            TransportErrorCode::FLOW_CONTROL_ERROR
        );
    }

    #[test]
    fn stream_priority() {
        let mut server = make(Side::Server);
        server.set_params(&TransportParameters {
            initial_max_streams_bidi: 3u32.into(),
            initial_max_data: 10u32.into(),
            initial_max_stream_data_bidi_remote: 10u32.into(),
            ..TransportParameters::default()
        });

        let (mut pending, state) = (Retransmits::default(), ConnState::established());
        let mut streams = Streams {
            state: &mut server,
            conn_state: &state,
        };

        let id_high = streams.open(Dir::Bi).unwrap();
        let id_mid = streams.open(Dir::Bi).unwrap();
        let id_low = streams.open(Dir::Bi).unwrap();

        let mut mid = SendStream {
            id: id_mid,
            state: &mut server,
            pending: &mut pending,
            conn_state: &state,
        };
        mid.write(b"mid").unwrap();

        let mut low = SendStream {
            id: id_low,
            state: &mut server,
            pending: &mut pending,
            conn_state: &state,
        };
        low.set_priority(-1).unwrap();
        low.write(b"low").unwrap();

        let mut high = SendStream {
            id: id_high,
            state: &mut server,
            pending: &mut pending,
            conn_state: &state,
        };
        high.set_priority(1).unwrap();
        high.write(b"high").unwrap();

        let meta = server.write_frames_for_test(40, true);
        assert_eq!(meta[0].id, id_high);
        assert_eq!(meta[1].id, id_mid);
        assert_eq!(meta[2].id, id_low);

        assert!(!server.can_send_stream_data());
        assert_eq!(server.pending.len(), 0);
    }

    #[test]
    fn requeue_stream_priority() {
        let mut server = make(Side::Server);
        server.set_params(&TransportParameters {
            initial_max_streams_bidi: 3u32.into(),
            initial_max_data: 1000u32.into(),
            initial_max_stream_data_bidi_remote: 1000u32.into(),
            ..TransportParameters::default()
        });

        let (mut pending, state) = (Retransmits::default(), ConnState::established());
        let mut streams = Streams {
            state: &mut server,
            conn_state: &state,
        };

        let id_high = streams.open(Dir::Bi).unwrap();
        let id_mid = streams.open(Dir::Bi).unwrap();

        let mut mid = SendStream {
            id: id_mid,
            state: &mut server,
            pending: &mut pending,
            conn_state: &state,
        };
        assert_eq!(mid.write(b"mid").unwrap(), 3);
        assert_eq!(server.pending.len(), 1);

        let mut high = SendStream {
            id: id_high,
            state: &mut server,
            pending: &mut pending,
            conn_state: &state,
        };
        high.set_priority(1).unwrap();
        assert_eq!(high.write(&[0; 200]).unwrap(), 200);
        assert_eq!(server.pending.len(), 2);

        // Requeue the high priority stream to lowest priority. The initial send
        // still uses high priority since it's queued that way. After that it will
        // switch to low priority
        let mut high = SendStream {
            id: id_high,
            state: &mut server,
            pending: &mut pending,
            conn_state: &state,
        };
        high.set_priority(-1).unwrap();

        let meta = server.write_frames_for_test(40, true);
        assert_eq!(meta.len(), 1);
        assert_eq!(meta[0].id, id_high);

        // After requeuing we should end up with 2 priorities - not 3
        assert_eq!(server.pending.len(), 2);

        // Send the remaining data. The initial mid priority one should go first now
        let meta = server.write_frames_for_test(1000 - 40, true);
        assert_eq!(meta.len(), 2);
        assert_eq!(meta[0].id, id_mid);
        assert_eq!(meta[1].id, id_high);

        assert!(!server.can_send_stream_data());
        assert_eq!(server.pending.len(), 0);
    }

    #[test]
    fn same_stream_priority() {
        for fair in [true, false] {
            let mut server = make(Side::Server);
            server.set_params(&TransportParameters {
                initial_max_streams_bidi: 3u32.into(),
                initial_max_data: 300u32.into(),
                initial_max_stream_data_bidi_remote: 300u32.into(),
                ..TransportParameters::default()
            });

            let (mut pending, state) = (Retransmits::default(), ConnState::established());
            let mut streams = Streams {
                state: &mut server,
                conn_state: &state,
            };

            // a, b and c all have the same priority
            let id_a = streams.open(Dir::Bi).unwrap();
            let id_b = streams.open(Dir::Bi).unwrap();
            let id_c = streams.open(Dir::Bi).unwrap();

            let mut stream_a = SendStream {
                id: id_a,
                state: &mut server,
                pending: &mut pending,
                conn_state: &state,
            };
            stream_a.write(&[b'a'; 100]).unwrap();

            let mut stream_b = SendStream {
                id: id_b,
                state: &mut server,
                pending: &mut pending,
                conn_state: &state,
            };
            stream_b.write(&[b'b'; 100]).unwrap();

            let mut stream_c = SendStream {
                id: id_c,
                state: &mut server,
                pending: &mut pending,
                conn_state: &state,
            };
            stream_c.write(&[b'c'; 100]).unwrap();

            let mut metas = vec![];

            // loop until all the streams are written
            loop {
                let meta = server.write_frames_for_test(40, fair);
                if meta.is_empty() {
                    break;
                }
                metas.extend(meta);
            }

            assert!(!server.can_send_stream_data());
            assert_eq!(server.pending.len(), 0);

            let stream_ids = metas.iter().map(|m| m.id).collect::<Vec<_>>();
            if fair {
                // When fairness is enabled, if we run out of buffer space to write out a stream,
                // the stream is re-queued after all the streams with the same priority.
                assert_eq!(
                    stream_ids,
                    vec![id_a, id_b, id_c, id_a, id_b, id_c, id_a, id_b, id_c]
                );
            } else {
                // When fairness is disabled the stream is re-queued before all the other streams
                // with the same priority.
                assert_eq!(
                    stream_ids,
                    vec![id_a, id_a, id_a, id_b, id_b, id_b, id_c, id_c, id_c]
                );
            }
        }
    }

    #[test]
    fn unfair_priority_bump() {
        let mut server = make(Side::Server);
        server.set_params(&TransportParameters {
            initial_max_streams_bidi: 3u32.into(),
            initial_max_data: 300u32.into(),
            initial_max_stream_data_bidi_remote: 300u32.into(),
            ..TransportParameters::default()
        });

        let (mut pending, state) = (Retransmits::default(), ConnState::established());
        let mut streams = Streams {
            state: &mut server,
            conn_state: &state,
        };

        // a, and b have the same priority, c has higher priority
        let id_a = streams.open(Dir::Bi).unwrap();
        let id_b = streams.open(Dir::Bi).unwrap();
        let id_c = streams.open(Dir::Bi).unwrap();

        let mut stream_a = SendStream {
            id: id_a,
            state: &mut server,
            pending: &mut pending,
            conn_state: &state,
        };
        stream_a.write(&[b'a'; 100]).unwrap();

        let mut stream_b = SendStream {
            id: id_b,
            state: &mut server,
            pending: &mut pending,
            conn_state: &state,
        };
        stream_b.write(&[b'b'; 100]).unwrap();

        let mut metas = vec![];

        // Write the first chunk of stream_a
        let meta = server.write_frames_for_test(40, false);
        assert!(!meta.is_empty());
        metas.extend(meta);

        // Queue stream_c which has higher priority
        let mut stream_c = SendStream {
            id: id_c,
            state: &mut server,
            pending: &mut pending,
            conn_state: &state,
        };
        stream_c.set_priority(1).unwrap();
        stream_c.write(&[b'b'; 100]).unwrap();

        // loop until all the streams are written
        loop {
            let meta = server.write_frames_for_test(40, false);
            if meta.is_empty() {
                break;
            }
            metas.extend(meta);
        }

        assert!(!server.can_send_stream_data());
        assert_eq!(server.pending.len(), 0);

        let stream_ids = metas.iter().map(|m| m.id).collect::<Vec<_>>();
        assert_eq!(
            stream_ids,
            // stream_c bumps stream_b but doesn't bump stream_a which had already been partly
            // written out
            vec![id_a, id_a, id_a, id_c, id_c, id_c, id_b, id_b, id_b]
        );
    }

    #[test]
    fn stop_finished() {
        let mut client = make(Side::Client);
        let id = StreamId::new(Side::Server, Dir::Uni, 0);
        // Server finishes stream
        let _ = client
            .received(
                frame::Stream {
                    id,
                    offset: 0,
                    fin: true,
                    data: Bytes::from_static(&[0; 32]),
                },
                32,
            )
            .unwrap();
        let mut pending = Retransmits::default();
        let mut stream = RecvStream {
            id,
            state: &mut client,
            pending: &mut pending,
        };
        stream.stop(0u32.into()).unwrap();
        assert!(client.recv.get_mut(&id).is_none(), "stream is freed");
    }

    // Verify that a stream that's been reset doesn't cause the appearance of pending data
    #[test]
    fn reset_stream_cannot_send() {
        let mut server = make(Side::Server);
        server.set_params(&TransportParameters {
            initial_max_streams_uni: 1u32.into(),
            initial_max_data: 42u32.into(),
            initial_max_stream_data_uni: 42u32.into(),
            ..TransportParameters::default()
        });
        let (mut pending, state) = (Retransmits::default(), ConnState::established());
        let mut streams = Streams {
            state: &mut server,
            conn_state: &state,
        };

        let id = streams.open(Dir::Uni).unwrap();
        let mut stream = SendStream {
            id,
            state: &mut server,
            pending: &mut pending,
            conn_state: &state,
        };
        stream.write(b"hello").unwrap();
        stream.reset(0u32.into()).unwrap();

        assert_eq!(pending.reset_stream, &[(id, 0u32.into())]);
        assert!(!server.can_send_stream_data());
    }

    #[test]
    fn stream_limit_fixed() {
        let mut client = make(Side::Client);
        // Open streams 0-127
        assert_eq!(
            client.received(
                frame::Stream {
                    id: StreamId::new(Side::Server, Dir::Uni, 127),
                    offset: 0,
                    fin: true,
                    data: Bytes::from_static(&[]),
                },
                0
            ),
            Ok(ShouldTransmit(false))
        );
        // Try to open stream 128, exceeding limit
        assert_eq!(
            client
                .received(
                    frame::Stream {
                        id: StreamId::new(Side::Server, Dir::Uni, 128),
                        offset: 0,
                        fin: true,
                        data: Bytes::from_static(&[]),
                    },
                    0
                )
                .unwrap_err()
                .code,
            TransportErrorCode::STREAM_LIMIT_ERROR
        );

        // Free stream 127
        let mut pending = Retransmits::default();
        let mut stream = RecvStream {
            id: StreamId::new(Side::Server, Dir::Uni, 127),
            state: &mut client,
            pending: &mut pending,
        };
        stream.stop(0u32.into()).unwrap();

        // Open stream 128
        assert_eq!(
            client.received(
                frame::Stream {
                    id: StreamId::new(Side::Server, Dir::Uni, 128),
                    offset: 0,
                    fin: true,
                    data: Bytes::from_static(&[]),
                },
                0
            ),
            Ok(ShouldTransmit(false))
        );
    }

    #[test]
    fn stream_limit_grows() {
        let mut client = make(Side::Client);
        // Open streams 0-127
        assert_eq!(
            client.received(
                frame::Stream {
                    id: StreamId::new(Side::Server, Dir::Uni, 127),
                    offset: 0,
                    fin: true,
                    data: Bytes::from_static(&[]),
                },
                0
            ),
            Ok(ShouldTransmit(false))
        );
        // Try to open stream 128, exceeding limit
        assert_eq!(
            client
                .received(
                    frame::Stream {
                        id: StreamId::new(Side::Server, Dir::Uni, 128),
                        offset: 0,
                        fin: true,
                        data: Bytes::from_static(&[]),
                    },
                    0
                )
                .unwrap_err()
                .code,
            TransportErrorCode::STREAM_LIMIT_ERROR
        );

        // Relax limit by one
        client.set_max_concurrent(Dir::Uni, 129u32.into());

        // Open stream 128
        assert_eq!(
            client.received(
                frame::Stream {
                    id: StreamId::new(Side::Server, Dir::Uni, 128),
                    offset: 0,
                    fin: true,
                    data: Bytes::from_static(&[]),
                },
                0
            ),
            Ok(ShouldTransmit(false))
        );
    }

    #[test]
    fn stream_limit_shrinks() {
        let mut client = make(Side::Client);
        // Open streams 0-127
        assert_eq!(
            client.received(
                frame::Stream {
                    id: StreamId::new(Side::Server, Dir::Uni, 127),
                    offset: 0,
                    fin: true,
                    data: Bytes::from_static(&[]),
                },
                0
            ),
            Ok(ShouldTransmit(false))
        );

        // Tighten limit by one
        client.set_max_concurrent(Dir::Uni, 127u32.into());

        // Free stream 127
        let mut pending = Retransmits::default();
        let mut stream = RecvStream {
            id: StreamId::new(Side::Server, Dir::Uni, 127),
            state: &mut client,
            pending: &mut pending,
        };
        stream.stop(0u32.into()).unwrap();

        // Try to open stream 128, still exceeding limit
        assert_eq!(
            client
                .received(
                    frame::Stream {
                        id: StreamId::new(Side::Server, Dir::Uni, 128),
                        offset: 0,
                        fin: true,
                        data: Bytes::from_static(&[]),
                    },
                    0
                )
                .unwrap_err()
                .code,
            TransportErrorCode::STREAM_LIMIT_ERROR
        );

        // Free stream 126
        assert_eq!(
            client.received_reset(frame::ResetStream {
                id: StreamId::new(Side::Server, Dir::Uni, 126),
                error_code: 0u32.into(),
                final_offset: 0u32.into(),
            }),
            Ok(ShouldTransmit(false))
        );
        let mut pending = Retransmits::default();
        let mut stream = RecvStream {
            id: StreamId::new(Side::Server, Dir::Uni, 126),
            state: &mut client,
            pending: &mut pending,
        };
        stream.stop(0u32.into()).unwrap();

        // Open stream 128
        assert_eq!(
            client.received(
                frame::Stream {
                    id: StreamId::new(Side::Server, Dir::Uni, 128),
                    offset: 0,
                    fin: true,
                    data: Bytes::from_static(&[]),
                },
                0
            ),
            Ok(ShouldTransmit(false))
        );
    }

    #[test]
    fn remote_stream_capacity() {
        let mut client = make(Side::Client);
        for _ in 0..2 {
            client.set_max_concurrent(Dir::Uni, 200u32.into());
            client.set_max_concurrent(Dir::Bi, 201u32.into());
            assert_eq!(client.max_remote[Dir::Uni as usize], 200);
            assert_eq!(client.max_remote[Dir::Bi as usize], 201);
            assert_eq!(client.allocated_remote_count[Dir::Uni as usize], 200);
            assert_eq!(client.allocated_remote_count[Dir::Bi as usize], 201);
            // Slots are materialized lazily: no remote stream has been touched yet.
            assert!(client.recv.is_empty());
            assert!(client.send.is_empty());
        }
    }

    #[test]
    fn lazy_remote_allocation_starts_empty() {
        // `StreamsState::new` must not pre-populate `send`/`recv` with placeholder slots.
        let client = StreamsState::new(
            Side::Client,
            10_000u32.into(),
            10_000u32.into(),
            1024 * 1024,
            (1024 * 1024u32).into(),
            (1024 * 1024u32).into(),
        );
        // No slots allocated until a stream is actually received.
        assert!(client.recv.is_empty());
        assert!(client.send.is_empty());
        assert_eq!(client.recv.capacity(), 0);
        assert_eq!(client.send.capacity(), 0);
    }

    #[test]
    fn out_of_order_implicit_open() {
        // Receiving idx=5 implicitly opens idx 0..=4. A later frame for idx=3 must be
        // processed normally, not dropped as "closed".
        const STREAM_5_PAYLOAD: &[u8] = &[0xAA; 8];
        const STREAM_3_PAYLOAD: &[u8] = &[0xBB; 4];

        let mut client = make(Side::Client);
        assert_eq!(
            client.received(
                frame::Stream {
                    id: StreamId::new(Side::Server, Dir::Uni, 5),
                    offset: 0,
                    fin: true,
                    data: Bytes::from_static(STREAM_5_PAYLOAD),
                },
                STREAM_5_PAYLOAD.len(),
            ),
            Ok(ShouldTransmit(false))
        );
        assert_eq!(client.next_remote[Dir::Uni as usize], 6);
        assert_eq!(
            client.received(
                frame::Stream {
                    id: StreamId::new(Side::Server, Dir::Uni, 3),
                    offset: 0,
                    fin: true,
                    data: Bytes::from_static(STREAM_3_PAYLOAD),
                },
                STREAM_3_PAYLOAD.len(),
            ),
            Ok(ShouldTransmit(false))
        );

        let id = StreamId::new(Side::Server, Dir::Uni, 3);
        let mut pending = Retransmits::default();
        let mut recv = RecvStream {
            id,
            state: &mut client,
            pending: &mut pending,
        };
        let mut chunks = recv.read(true).unwrap();
        assert_eq!(
            chunks.next(STREAM_3_PAYLOAD.len()).unwrap().unwrap().bytes,
            STREAM_3_PAYLOAD
        );
        let _ = chunks.finalize();
    }

    #[test]
    fn frame_for_closed_stream_is_dropped() {
        // After a remote stream is fully freed, a subsequent frame for the same id must be
        // dropped — absence from the map unambiguously means "closed" for ids below the
        // frontier.
        const PAYLOAD: &[u8] = &[0; 4];

        let mut client = make(Side::Client);
        let id = StreamId::new(Side::Server, Dir::Uni, 0);
        assert_eq!(
            client.received(
                frame::Stream {
                    id,
                    offset: 0,
                    fin: true,
                    data: Bytes::from_static(PAYLOAD),
                },
                PAYLOAD.len(),
            ),
            Ok(ShouldTransmit(false))
        );
        // Stop the stream so it's fully freed.
        let mut pending = Retransmits::default();
        RecvStream {
            id,
            state: &mut client,
            pending: &mut pending,
        }
        .stop(0u32.into())
        .unwrap();
        assert!(!client.recv.contains_key(&id));

        // A stray retransmit for the freed stream must be dropped without resurrecting state.
        assert_eq!(
            client.received(
                frame::Stream {
                    id,
                    offset: 0,
                    fin: true,
                    data: Bytes::from_static(PAYLOAD),
                },
                PAYLOAD.len(),
            ),
            Ok(ShouldTransmit(false))
        );
        assert!(!client.recv.contains_key(&id));
    }

    #[test]
    fn churn_keeps_maps_bounded() {
        // Rapidly open + fully close a long sequence of remote streams. The maps must stay
        // bounded (only active streams are materialized) even though thousands of ids have
        // been used over the connection's lifetime.
        const N: u64 = 5_000;

        let mut client = make(Side::Client);
        for i in 0..N {
            let id = StreamId::new(Side::Server, Dir::Uni, i);
            assert_eq!(
                client.received(
                    frame::Stream {
                        id,
                        offset: 0,
                        fin: true,
                        data: Bytes::from_static(&[0; 1]),
                    },
                    1,
                ),
                Ok(ShouldTransmit(false))
            );
            let mut pending = Retransmits::default();
            let mut recv = RecvStream {
                id,
                state: &mut client,
                pending: &mut pending,
            };
            let mut chunks = recv.read(true).unwrap();
            let _ = chunks.next(1).unwrap();
            assert!(chunks.next(1).unwrap().is_none());
            let _ = chunks.finalize();
        }
        // Every stream was fully drained; the map must be empty.
        assert_eq!(client.recv.len(), 0);
        assert_eq!(client.send.len(), 0);
    }

    #[test]
    fn expand_receive_window() {
        let mut server = make(Side::Server);
        let new_receive_window = 2 * server.receive_window as u32;
        let expanded = server.set_receive_window(new_receive_window.into());
        assert!(expanded);
        assert_eq!(server.receive_window, new_receive_window as u64);
        assert_eq!(server.local_max_data, new_receive_window as u64);
        assert_eq!(server.receive_window_shrink_debt, 0);
        let prev_local_max_data = server.local_max_data;

        // credit, expecting all of them added to local_max_data
        let credits = 1024u64;
        let should_transmit = server.add_read_credits(credits);
        assert_eq!(server.receive_window_shrink_debt, 0);
        assert_eq!(server.local_max_data, prev_local_max_data + credits);
        assert!(should_transmit.should_transmit());
    }

    #[test]
    fn shrink_receive_window() {
        let mut server = make(Side::Server);
        let new_receive_window = server.receive_window as u32 / 2;
        let prev_local_max_data = server.local_max_data;

        // shrink the receive_winbow, local_max_data is not expected to be changed
        let shrink_diff = server.receive_window - new_receive_window as u64;
        let expanded = server.set_receive_window(new_receive_window.into());
        assert!(!expanded);
        assert_eq!(server.receive_window, new_receive_window as u64);
        assert_eq!(server.local_max_data, prev_local_max_data);
        assert_eq!(server.receive_window_shrink_debt, shrink_diff);
        let prev_local_max_data = server.local_max_data;

        // credit twice, local_max_data does not change as it is absorbed by
        // receive_window_shrink_debt
        let credits = 1024u64;
        for _ in 0..2 {
            let expected_receive_window_shrink_debt = server.receive_window_shrink_debt - credits;
            let should_transmit = server.add_read_credits(credits);
            assert_eq!(
                server.receive_window_shrink_debt,
                expected_receive_window_shrink_debt
            );
            assert_eq!(server.local_max_data, prev_local_max_data);
            assert!(!should_transmit.should_transmit());
        }

        // credit again which exceeds all remaining expected_receive_window_shrink_debt
        let credits = 1024 * 512;
        let prev_local_max_data = server.local_max_data;
        let expected_local_max_data =
            server.local_max_data + (credits - server.receive_window_shrink_debt);
        let _should_transmit = server.add_read_credits(credits);
        assert_eq!(server.receive_window_shrink_debt, 0);
        assert_eq!(server.local_max_data, expected_local_max_data);
        assert!(server.local_max_data > prev_local_max_data);

        // credit again, all should be added to local_max_data
        let credits = 1024 * 512;
        let expected_local_max_data = server.local_max_data + credits;
        let should_transmit = server.add_read_credits(credits);
        assert_eq!(server.receive_window_shrink_debt, 0);
        assert_eq!(server.local_max_data, expected_local_max_data);
        assert!(should_transmit.should_transmit());
    }

    #[test]
    fn expand_send_window() {
        let mut server = make(Side::Server);

        let initial_send_window = server.send_window;
        let larger_send_window = initial_send_window * 2;

        // Set `initial_max_data` larger than `send_window` so we're limited by local flow control
        server.set_params(&TransportParameters {
            initial_max_data: VarInt::MAX,
            initial_max_stream_data_uni: VarInt::MAX,
            initial_max_streams_uni: VarInt::from_u32(100),
            ..TransportParameters::default()
        });

        assert_eq!(server.write_limit(), initial_send_window);
        assert_eq!(server.poll(), None);

        let mut retransmits = Retransmits::default();
        let conn_state = ConnState::established();

        let stream_id = Streams {
            state: &mut server,
            conn_state: &conn_state,
        }
        .open(Dir::Uni)
        .expect("should be able to open a stream");

        let mut stream = SendStream {
            id: stream_id,
            state: &mut server,
            pending: &mut retransmits,
            conn_state: &conn_state,
        };

        // Check that the stream accepts `initial_send_window` bytes
        let initial_send_len = initial_send_window as usize;
        let data = vec![0xFFu8; initial_send_len];

        assert_eq!(stream.write(&data), Ok(initial_send_len));

        // Try to write the same data again, observe that it's blocked
        assert_eq!(stream.write(&data), Err(WriteError::Blocked));

        // Check that we get a `Writable` event after increasing the send window
        stream.state.set_send_window(larger_send_window);
        assert_eq!(
            stream.state.poll(),
            Some(StreamEvent::Writable { id: stream_id })
        );

        // Check that the stream accepts the exact same amount of data again
        assert_eq!(stream.write(&data), Ok(initial_send_len));
        assert_eq!(stream.write(&data), Err(WriteError::Blocked));

        assert_eq!(stream.state.poll(), None);

        // Ack the data
        stream.state.received_ack_of(frame::StreamMeta {
            id: stream_id,
            offsets: 0..larger_send_window,
            fin: false,
        });

        assert_eq!(
            stream.state.poll(),
            Some(StreamEvent::Writable { id: stream_id })
        );

        // Check that our full send window is available again
        assert_eq!(stream.write(&data), Ok(initial_send_len));
        assert_eq!(stream.write(&data), Ok(initial_send_len));
        assert_eq!(stream.write(&data), Err(WriteError::Blocked));
    }

    #[test]
    fn shrink_send_window() {
        let mut server = make(Side::Server);

        let initial_send_window = server.send_window;
        let smaller_send_window = server.send_window / 2;

        // Set `initial_max_data` larger than `send_window` so we're limited by local flow control
        server.set_params(&TransportParameters {
            initial_max_data: VarInt::MAX,
            initial_max_stream_data_uni: VarInt::MAX,
            initial_max_streams_uni: VarInt::from_u32(100),
            ..TransportParameters::default()
        });

        assert_eq!(server.write_limit(), initial_send_window);
        assert_eq!(server.poll(), None);

        let mut retransmits = Retransmits::default();
        let conn_state = ConnState::established();

        let stream_id = Streams {
            state: &mut server,
            conn_state: &conn_state,
        }
        .open(Dir::Uni)
        .expect("should be able to open a stream");

        let mut stream = SendStream {
            id: stream_id,
            state: &mut server,
            pending: &mut retransmits,
            conn_state: &conn_state,
        };

        let initial_send_len = initial_send_window as usize;

        let data = vec![0xFFu8; initial_send_len];

        // Assert that the full send window is accepted
        assert_eq!(stream.write(&data), Ok(initial_send_len));
        assert_eq!(stream.write(&data), Err(WriteError::Blocked));

        assert_eq!(stream.state.write_limit(), 0);
        assert_eq!(stream.state.poll(), None);

        // Shrink our send window, assert that it's still not writable
        stream.state.set_send_window(smaller_send_window);
        assert_eq!(stream.state.write_limit(), 0);
        assert_eq!(stream.state.poll(), None);

        // Assert that data is still not accepted
        assert_eq!(stream.write(&data), Err(WriteError::Blocked));

        // Ack some data, assert that writes are still not accepted due to outstanding sends
        stream.state.received_ack_of(frame::StreamMeta {
            id: stream_id,
            offsets: 0..smaller_send_window,
            fin: false,
        });

        assert_eq!(stream.write(&data), Err(WriteError::Blocked));

        // Ack the rest of the data
        stream.state.received_ack_of(frame::StreamMeta {
            id: stream_id,
            offsets: smaller_send_window..initial_send_window,
            fin: false,
        });

        // This should generate a `Writable` event
        assert_eq!(
            stream.state.poll(),
            Some(StreamEvent::Writable { id: stream_id })
        );
        assert_eq!(stream.state.write_limit(), smaller_send_window);

        // Assert that only `smaller_send_window` bytes are accepted
        assert_eq!(stream.write(&data), Ok(smaller_send_window as usize));
        assert_eq!(stream.write(&data), Err(WriteError::Blocked));
    }

    fn stream_frame(id: StreamId, offset: u64, data: &'static [u8]) -> frame::Stream {
        frame::Stream {
            id,
            offset,
            fin: false,
            data: Bytes::from_static(data),
        }
    }

    fn reset_at_frame(id: StreamId, final_offset: u32, reliable_size: u32) -> frame::ResetStreamAt {
        frame::ResetStreamAt {
            id,
            error_code: 7u32.into(),
            final_offset: final_offset.into(),
            reliable_size: reliable_size.into(),
        }
    }

    /// Read everything currently readable from a stream, and the error that ended the read
    fn read_stream(
        state: &mut StreamsState,
        pending: &mut Retransmits,
        id: StreamId,
        ordered: bool,
    ) -> (Vec<u8>, Option<crate::ReadError>) {
        let mut recv = RecvStream { id, state, pending };
        let mut chunks = recv.read(ordered).unwrap();
        let mut data = Vec::new();
        let err = loop {
            match chunks.next(usize::MAX) {
                Ok(Some(chunk)) => data.extend_from_slice(&chunk.bytes),
                Ok(None) => break None,
                Err(e) => break Some(e),
            }
        };
        let _ = chunks.finalize();
        (data, err)
    }

    #[test]
    fn reset_at_delivers_reliable_data() {
        let mut client = make(Side::Client);
        let mut pending = Retransmits::default();
        let id = StreamId::new(Side::Server, Dir::Uni, 0);
        let initial_max = client.local_max_data;

        // Data after the reliable size, which must be discarded
        let _ = client.received(stream_frame(id, 0, b"0123"), 4).unwrap();
        let _ = client.received(stream_frame(id, 8, b"89"), 2).unwrap();
        let _ = client.received_reset_at(reset_at_frame(id, 10, 6)).unwrap();
        assert_eq!(client.data_recvd, 10);
        // Data that will never be read is credited immediately
        assert_eq!(client.local_max_data - initial_max, 4);

        let (data, err) = read_stream(&mut client, &mut pending, id, true);
        assert_eq!(data, b"0123");
        assert_eq!(err, Some(crate::ReadError::Blocked));
        assert_eq!(client.local_max_data - initial_max, 8);

        // The reset is withheld until the reliable data has been read
        let mut recv = RecvStream {
            id,
            state: &mut client,
            pending: &mut pending,
        };
        assert_eq!(recv.received_reset(), Ok(None));

        // Retransmitted data is truncated at the reliable size
        let _ = client.received(stream_frame(id, 4, b"456789"), 6).unwrap();
        assert_eq!(client.data_recvd, 10);
        let (data, err) = read_stream(&mut client, &mut pending, id, true);
        assert_eq!(data, b"45");
        assert_eq!(err, Some(crate::ReadError::Reset(7u32.into())));
        assert_eq!(client.local_max_data - initial_max, 10);
        assert!(!client.recv.contains_key(&id), "stream state is freed");
    }

    #[test]
    fn reset_at_unordered() {
        let mut client = make(Side::Client);
        let mut pending = Retransmits::default();
        let id = StreamId::new(Side::Server, Dir::Uni, 0);
        let initial_max = client.local_max_data;

        let _ = client.received(stream_frame(id, 2, b"2345"), 4).unwrap();
        let (data, err) = read_stream(&mut client, &mut pending, id, false);
        assert_eq!(data, b"2345");
        assert_eq!(err, Some(crate::ReadError::Blocked));

        // Bytes 4 and 5 were already read, so only 0..2 remain to be delivered
        let _ = client.received_reset_at(reset_at_frame(id, 8, 4)).unwrap();
        assert_eq!(client.local_max_data - initial_max, 6);
        let _ = client.received(stream_frame(id, 0, b"01"), 2).unwrap();
        let (data, err) = read_stream(&mut client, &mut pending, id, false);
        assert_eq!(data, b"01");
        assert_eq!(err, Some(crate::ReadError::Reset(7u32.into())));
        assert_eq!(client.local_max_data - initial_max, 8);
    }

    #[test]
    fn reset_at_reliable_size_changes() {
        let mut client = make(Side::Client);
        let mut pending = Retransmits::default();
        let id = StreamId::new(Side::Server, Dir::Uni, 0);
        let initial_max = client.local_max_data;

        let _ = client.received(stream_frame(id, 0, b"0123"), 4).unwrap();
        let _ = client.received_reset_at(reset_at_frame(id, 10, 6)).unwrap();
        assert_eq!(client.local_max_data - initial_max, 4);

        // Increasing the reliable size is ignored
        let _ = client.received_reset_at(reset_at_frame(id, 10, 8)).unwrap();
        assert_eq!(client.local_max_data - initial_max, 4);

        // Reducing it releases flow control credit for the data no longer delivered
        let _ = client.received_reset_at(reset_at_frame(id, 10, 3)).unwrap();
        assert_eq!(client.local_max_data - initial_max, 7);
        let (data, err) = read_stream(&mut client, &mut pending, id, true);
        assert_eq!(data, b"012");
        assert_eq!(err, Some(crate::ReadError::Reset(7u32.into())));
        assert_eq!(client.local_max_data - initial_max, 10);
    }

    #[test]
    fn reset_at_reduced_by_reset_stream() {
        let mut client = make(Side::Client);
        let mut pending = Retransmits::default();
        let id = StreamId::new(Side::Server, Dir::Uni, 0);
        let initial_max = client.local_max_data;

        let _ = client.received(stream_frame(id, 0, b"0123"), 4).unwrap();
        let _ = client.received_reset_at(reset_at_frame(id, 10, 6)).unwrap();
        let _ = client
            .received_reset(frame::ResetStream {
                id,
                error_code: 7u32.into(),
                final_offset: 10u32.into(),
            })
            .unwrap();
        assert_eq!(client.local_max_data - initial_max, 10);
        let (data, err) = read_stream(&mut client, &mut pending, id, true);
        assert_eq!(data, b"");
        assert_eq!(err, Some(crate::ReadError::Reset(7u32.into())));
        assert_eq!(client.local_max_data - initial_max, 10);
    }

    #[test]
    fn reset_at_errors() {
        let mut client = make(Side::Client);
        let id = StreamId::new(Side::Server, Dir::Uni, 0);
        assert_eq!(
            client
                .received_reset_at(reset_at_frame(id, 4, 5))
                .unwrap_err()
                .code,
            TransportErrorCode::FRAME_ENCODING_ERROR
        );

        let _ = client.received_reset_at(reset_at_frame(id, 10, 6)).unwrap();
        assert_eq!(
            client
                .received_reset_at(reset_at_frame(id, 11, 6))
                .unwrap_err()
                .code,
            TransportErrorCode::FINAL_SIZE_ERROR
        );
        assert_eq!(
            client
                .received_reset_at(frame::ResetStreamAt {
                    error_code: 8u32.into(),
                    ..reset_at_frame(id, 10, 6)
                })
                .unwrap_err()
                .code,
            TransportErrorCode::STREAM_STATE_ERROR
        );
        // Data after the final size
        assert_eq!(
            client
                .received(stream_frame(id, 4, b"0123456"), 7)
                .unwrap_err()
                .code,
            TransportErrorCode::FINAL_SIZE_ERROR
        );
    }

    #[test]
    fn reset_at_stopped_flow_control() {
        let mut client = make(Side::Client);
        let mut pending = Retransmits::default();
        let id = StreamId::new(Side::Server, Dir::Uni, 0);
        let initial_max = client.local_max_data;

        let _ = client.received(stream_frame(id, 0, b"0123"), 4).unwrap();
        let _ = client.received_reset_at(reset_at_frame(id, 10, 6)).unwrap();
        let (data, _) = read_stream(&mut client, &mut pending, id, true);
        assert_eq!(data, b"0123");
        assert_eq!(client.local_max_data - initial_max, 8);

        let mut recv = RecvStream {
            id,
            state: &mut client,
            pending: &mut pending,
        };
        recv.stop(0u32.into()).unwrap();
        assert_eq!(client.local_max_data - initial_max, 10);
        assert!(!client.recv.contains_key(&id), "stream state is freed");
        // Late data for the stopped stream is ignored
        let _ = client.received(stream_frame(id, 4, b"45"), 2).unwrap();
        assert_eq!(client.data_recvd, 10);
        assert_eq!(client.local_max_data - initial_max, 10);
    }

    /// Opens a uni stream on a server whose peer supports RESET_STREAM_AT, and writes `data`
    fn reset_at_sender(data: &[u8]) -> (StreamsState, StreamId) {
        let mut server = make(Side::Server);
        server.set_params(&TransportParameters {
            initial_max_streams_uni: 1u32.into(),
            initial_max_data: 42u32.into(),
            initial_max_stream_data_uni: 42u32.into(),
            reset_stream_at: true,
            ..TransportParameters::default()
        });
        let (mut pending, state) = (Retransmits::default(), ConnState::established());
        let id = Streams {
            state: &mut server,
            conn_state: &state,
        }
        .open(Dir::Uni)
        .unwrap();
        SendStream {
            id,
            state: &mut server,
            pending: &mut pending,
            conn_state: &state,
        }
        .write(data)
        .unwrap();
        (server, id)
    }

    #[test]
    fn reset_at_not_negotiated() {
        let mut server = make(Side::Server);
        server.set_params(&TransportParameters {
            initial_max_streams_uni: 1u32.into(),
            initial_max_data: 42u32.into(),
            initial_max_stream_data_uni: 42u32.into(),
            ..TransportParameters::default()
        });
        let (mut pending, state) = (Retransmits::default(), ConnState::established());
        let id = Streams {
            state: &mut server,
            conn_state: &state,
        }
        .open(Dir::Uni)
        .unwrap();
        let mut stream = SendStream {
            id,
            state: &mut server,
            pending: &mut pending,
            conn_state: &state,
        };
        stream.write(b"hello").unwrap();
        assert_eq!(
            stream.reset_at(0u32.into(), 2),
            Err(ResetStreamAtError::NotNegotiated)
        );
        // The stream is unaffected
        stream.write(b"world").unwrap();
        assert!(pending.reset_stream.is_empty());
    }

    #[test]
    fn reset_at_send_errors() {
        let (mut server, id) = reset_at_sender(b"hello");
        let (mut pending, state) = (Retransmits::default(), ConnState::established());
        let mut stream = SendStream {
            id,
            state: &mut server,
            pending: &mut pending,
            conn_state: &state,
        };
        assert_eq!(
            stream.reset_at(0u32.into(), 6),
            Err(ResetStreamAtError::InvalidReliableSize)
        );
        stream.reset_at(0u32.into(), 5).unwrap();
        assert_eq!(
            stream.reset_at(0u32.into(), 2),
            Err(ResetStreamAtError::ClosedStream)
        );
        assert!(stream.reset(0u32.into()).is_err());
        assert_eq!(stream.write(b"!"), Err(WriteError::ClosedStream));
        assert_eq!(stream.finish(), Err(FinishError::ClosedStream));

        let (mut server, id) = reset_at_sender(b"hello");
        server.received_stop_sending(id, 3u32.into());
        let mut stream = SendStream {
            id,
            state: &mut server,
            pending: &mut pending,
            conn_state: &state,
        };
        assert_eq!(
            stream.reset_at(0u32.into(), 2),
            Err(ResetStreamAtError::Stopped(3u32.into()))
        );
    }

    #[test]
    fn reset_at_sends_reliable_data() {
        let (mut server, id) = reset_at_sender(b"0123456789");
        let (mut pending, state) = (Retransmits::default(), ConnState::established());

        // Send everything, then reset
        let frames = server.write_frames_for_test(100, true);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].offsets, 0..10);
        SendStream {
            id,
            state: &mut server,
            pending: &mut pending,
            conn_state: &state,
        }
        .reset_at(7u32.into(), 4)
        .unwrap();
        assert_eq!(pending.reset_stream, &[(id, 7u32.into())]);
        assert_eq!(server.unacked_data, 4);
        let reset = server.send[&id].as_ref().unwrap().reset_frame(id).unwrap();
        assert_eq!(reset.final_offset, 10u32.into());
        assert_eq!(reset.reliable_size, 4u32.into());
        assert!(!server.can_send_stream_data());

        // Only lost data before the reliable size is retransmitted, without FIN
        server.retransmit(frames[0].clone());
        assert!(server.can_send_stream_data());
        let retransmitted = server.write_frames_for_test(100, true);
        assert_eq!(retransmitted.len(), 1);
        assert_eq!(retransmitted[0].offsets, 0..4);
        assert!(!retransmitted[0].fin);
        assert!(!server.can_send_stream_data());

        // The stream is freed once both the data and the reset are acknowledged
        server.received_ack_of(retransmitted[0].clone());
        assert_eq!(server.unacked_data, 0);
        assert!(server.send.contains_key(&id));
        server.reset_acked(id);
        assert!(!server.send.contains_key(&id));
        assert!(
            server
                .events
                .iter()
                .all(|e| !matches!(e, StreamEvent::Finished { .. }))
        );
    }

    #[test]
    fn reset_at_reset_acked_first() {
        let (mut server, id) = reset_at_sender(b"0123456789");
        let (mut pending, state) = (Retransmits::default(), ConnState::established());
        SendStream {
            id,
            state: &mut server,
            pending: &mut pending,
            conn_state: &state,
        }
        .reset_at(7u32.into(), 4)
        .unwrap();
        // Data written but never sent is only sent up to the reliable size
        let frames = server.write_frames_for_test(100, true);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].offsets, 0..4);
        server.reset_acked(id);
        assert!(server.send.contains_key(&id));
        server.received_ack_of(frames[0].clone());
        assert!(!server.send.contains_key(&id));
        assert_eq!(server.unacked_data, 0);
    }

    #[test]
    fn reset_at_stop_sending() {
        let (mut server, id) = reset_at_sender(b"0123456789");
        let (mut pending, state) = (Retransmits::default(), ConnState::established());
        SendStream {
            id,
            state: &mut server,
            pending: &mut pending,
            conn_state: &state,
        }
        .reset_at(7u32.into(), 4)
        .unwrap();
        assert_eq!(server.unacked_data, 4);

        // Abandons the reliable data, keeping the original error code
        assert_eq!(
            server.received_stop_sending(id, 3u32.into()),
            Some(7u32.into())
        );
        assert_eq!(server.unacked_data, 0);
        assert!(!server.can_send_stream_data());
        let reset = server.send[&id].as_ref().unwrap().reset_frame(id).unwrap();
        assert_eq!(reset.reliable_size, 0u32.into());
        assert_eq!(reset.error_code, 7u32.into());
        assert_eq!(server.received_stop_sending(id, 3u32.into()), None);

        server.reset_acked(id);
        assert!(!server.send.contains_key(&id));
    }
}
