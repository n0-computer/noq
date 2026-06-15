use std::collections::hash_map::Entry;
use std::mem;

use thiserror::Error;
use tracing::debug;

use super::state::get_or_insert_recv;
use super::{ClosedStream, Retransmits, ShouldTransmit, StreamId, StreamsState};
use crate::connection::assembler::{Assembler, Chunk, IllegalOrderedRead};
use crate::connection::streams::state::StreamRecv;
use crate::{TransportError, VarInt, frame};

#[derive(Debug, Default)]
pub(super) struct Recv {
    // NB: when adding or removing fields, remember to update `reinit`.
    state: RecvState,
    pub(super) assembler: Assembler,
    sent_max_stream_data: u64,
    pub(super) end: u64,
    pub(super) stopped: bool,
}

impl Recv {
    pub(super) fn new(initial_max_data: u64) -> Box<Self> {
        Box::new(Self {
            state: RecvState::default(),
            assembler: Assembler::new(),
            sent_max_stream_data: initial_max_data,
            end: 0,
            stopped: false,
        })
    }

    /// Reset to the initial state
    pub(super) fn reinit(&mut self, initial_max_data: u64) {
        self.state = RecvState::default();
        self.assembler.reinit();
        self.sent_max_stream_data = initial_max_data;
        self.end = 0;
        self.stopped = false;
    }

    /// Process a STREAM frame
    ///
    /// Return value is `(number_of_new_bytes_ingested, stream_is_closed)`
    pub(super) fn ingest(
        &mut self,
        frame: frame::Stream,
        payload_len: usize,
        received: u64,
        max_data: u64,
    ) -> Result<(u64, bool), TransportError> {
        let end = frame.offset + frame.data.len() as u64;
        if end >= 2u64.pow(62) {
            return Err(TransportError::FLOW_CONTROL_ERROR(
                "maximum stream offset too large",
            ));
        }

        if let Some(final_offset) = self.final_offset()
            && (end > final_offset || (frame.fin && end != final_offset))
        {
            debug!(end, final_offset, "final size error");
            return Err(TransportError::FINAL_SIZE_ERROR(""));
        }

        let new_bytes = self.credit_consumed_by(end, received, max_data)?;

        // Stopped streams don't need to wait for the actual data, they just need to know
        // how much there was.
        if frame.fin
            && !self.stopped
            && let RecvState::Recv { ref mut size } = self.state
        {
            *size = Some(end);
        }

        self.end = self.end.max(end);
        // Don't bother storing data or releasing stream-level flow control credit if the stream's
        // already stopped
        if !self.stopped {
            self.assembler.insert(frame.offset, frame.data, payload_len);
        }

        Ok((new_bytes, frame.fin && self.stopped))
    }

    pub(super) fn stop(&mut self) -> Result<(u64, ShouldTransmit), ClosedStream> {
        if self.stopped {
            return Err(ClosedStream { _private: () });
        }

        self.stopped = true;
        self.assembler.clear();
        // Issue flow control credit for unread data
        let read_credits = self.end - self.assembler.bytes_read();
        // This may send a spurious STOP_SENDING if we've already received all data, but it's a bit
        // fiddly to distinguish that from the case where we've received a FIN but are missing some
        // data that the peer might still be trying to retransmit, in which case a STOP_SENDING is
        // still useful.
        Ok((read_credits, ShouldTransmit(self.is_receiving())))
    }

    /// Returns the window that should be advertised in a `MAX_STREAM_DATA` frame
    ///
    /// The method returns a tuple which consists of the window that should be
    /// announced, as well as a boolean parameter which indicates if a new
    /// transmission of the value is recommended. If the boolean value is
    /// `false` the new window should only be transmitted if a previous transmission
    /// had failed.
    pub(super) fn max_stream_data(&mut self, stream_receive_window: u64) -> (u64, ShouldTransmit) {
        let max_stream_data = self.assembler.bytes_read() + stream_receive_window;

        // Only announce a window update if it's significant enough
        // to make it worthwhile sending a MAX_STREAM_DATA frame.
        // We use here a fraction of the configured stream receive window to make
        // the decision, and accommodate for streams using bigger windows requiring
        // less updates. A fixed size would also work - but it would need to be
        // smaller than `stream_receive_window` in order to make sure the stream
        // does not get stuck.
        let diff = max_stream_data - self.sent_max_stream_data;
        let transmit = self.can_send_flow_control() && diff >= (stream_receive_window / 8);
        (max_stream_data, ShouldTransmit(transmit))
    }

    /// Records that a `MAX_STREAM_DATA` announcing a certain window was sent
    ///
    /// This will suppress enqueuing further `MAX_STREAM_DATA` frames unless
    /// either the previous transmission was not acknowledged or the window
    /// further increased.
    pub(super) fn record_sent_max_stream_data(&mut self, sent_value: u64) {
        if sent_value > self.sent_max_stream_data {
            self.sent_max_stream_data = sent_value;
        }
    }

    /// Whether the total amount of data that the peer will send on this stream is unknown
    ///
    /// True until we've received either a reset or the final frame.
    ///
    /// Implies that the sender might benefit from stream-level flow control updates, and we might
    /// need to issue connection-level flow control updates due to flow control budget use by this
    /// stream in the future, even if it's been stopped.
    pub(super) fn final_offset_unknown(&self) -> bool {
        matches!(self.state, RecvState::Recv { size: None })
    }

    /// Whether stream-level flow control updates should be sent for this stream
    pub(super) fn can_send_flow_control(&self) -> bool {
        // Stream-level flow control is redundant if the sender has already sent the whole stream,
        // and moot if we no longer want data on this stream.
        self.final_offset_unknown() && !self.stopped
    }

    /// Whether data is still being accepted from the peer
    ///
    /// Remains true after a RESET_STREAM_AT frame is received, since the sender may still
    /// retransmit reliable data needed to fill gaps below the reliable size.
    pub(super) fn is_receiving(&self) -> bool {
        matches!(
            self.state,
            RecvState::Recv { .. } | RecvState::ResetRecvdAt { .. }
        )
    }

    fn final_offset(&self) -> Option<u64> {
        match self.state {
            RecvState::Recv { size } => size,
            RecvState::ResetRecvd { size, .. } | RecvState::ResetRecvdAt { size, .. } => Some(size),
        }
    }

    /// Returns `false` iff the reset was redundant
    pub(super) fn reset(
        &mut self,
        error_code: VarInt,
        final_offset: VarInt,
        received: u64,
        max_data: u64,
    ) -> Result<bool, TransportError> {
        // Validate final_offset
        if let Some(offset) = self.final_offset() {
            if offset != final_offset.into_inner() {
                return Err(TransportError::FINAL_SIZE_ERROR("inconsistent value"));
            }
        } else if self.end > u64::from(final_offset) {
            return Err(TransportError::FINAL_SIZE_ERROR(
                "lower than high water mark",
            ));
        }
        self.credit_consumed_by(final_offset.into(), received, max_data)?;

        if matches!(self.state, RecvState::ResetRecvd { .. }) {
            return Ok(false);
        }
        self.state = RecvState::ResetRecvd {
            size: final_offset.into(),
            error_code,
        };
        // Nuke buffers so that future reads fail immediately, which ensures future reads don't
        // issue flow control credit redundant to that already issued. We could instead special-case
        // reset streams during read, but it's unclear if there's any benefit to retaining data for
        // reset streams.
        self.assembler.clear();
        Ok(true)
    }

    /// Process a RESET_STREAM_AT (reliable reset) frame.
    ///
    /// `final_size` is the stream's final size and `reliable_size` (`<= final_size`) the offset up
    /// to which data must still be delivered to the application. The error code is surfaced to the
    /// application only after it has read all data up to the reliable size.
    ///
    /// Establishes the final size for flow control like [`Self::reset`], but instead of discarding
    /// buffered data it retains it for delivery up to the reliable size. To avoid double-counting
    /// connection flow control as retransmitted reliable data arrives, the receive high-water mark
    /// is advanced to the final size here, so subsequent [`Self::ingest`] calls consume no further
    /// credit.
    pub(super) fn reset_at(
        &mut self,
        error_code: VarInt,
        final_size: VarInt,
        reliable_size: VarInt,
        received: u64,
        max_data: u64,
    ) -> Result<ResetAtOutcome, TransportError> {
        let final_size = final_size.into_inner();
        // The wire decoder already rejects `reliable > final`; clamp defensively for callers that
        // construct frames directly (e.g. fuzzing).
        let reliable_size = reliable_size.into_inner().min(final_size);

        // The final size is immutable once known and may not be below already-received data.
        if let Some(known) = self.final_offset() {
            if known != final_size {
                return Err(TransportError::FINAL_SIZE_ERROR("inconsistent value"));
            }
        } else if self.end > final_size {
            return Err(TransportError::FINAL_SIZE_ERROR(
                "lower than high water mark",
            ));
        }

        // We cannot un-deliver data already read beyond the reliable size.
        let deliver_cap = reliable_size.max(self.assembler.bytes_read());

        match self.state {
            RecvState::ResetRecvd {
                error_code: prev_code,
                ..
            } => {
                // The error code is immutable across frames for the same stream (RFC §5.2).
                if error_code != prev_code {
                    return Err(TransportError::STREAM_STATE_ERROR(
                        "RESET_STREAM_AT error code changed",
                    ));
                }
                // An ordinary reset already fully terminated the stream (reliable size 0); a
                // reliable reset cannot un-terminate it or reduce the reliable size below zero.
                Ok(ResetAtOutcome::Ignored)
            }
            RecvState::ResetRecvdAt {
                reliable_size: prev_cap,
                error_code: prev_code,
                ..
            } => {
                // The error code is immutable across frames for the same stream (RFC §5.2).
                if error_code != prev_code {
                    return Err(TransportError::STREAM_STATE_ERROR(
                        "RESET_STREAM_AT error code changed",
                    ));
                }
                if deliver_cap >= prev_cap {
                    // The reliable size did not decrease; the spec requires ignoring increases.
                    return Ok(ResetAtOutcome::Ignored);
                }
                self.state = RecvState::ResetRecvdAt {
                    size: final_size,
                    reliable_size: deliver_cap,
                    error_code,
                };
                // Release connection credit for data between the old and new caps that will no
                // longer be delivered. The final size, and thus `data_recvd`, is unchanged.
                Ok(ResetAtOutcome::Applied {
                    received_delta: 0,
                    credit: prev_cap - deliver_cap,
                })
            }
            RecvState::Recv { .. } => {
                // Bounds-check the final size against flow control. The returned value is the
                // number of so-far-unaccounted bytes up to the final size (`final_size - end`).
                let received_delta = self.credit_consumed_by(final_size, received, max_data)?;
                // Account every byte up to the final size as received, so retransmitted reliable
                // data ingested later adds no further credit (see method docs).
                self.end = final_size;
                self.state = RecvState::ResetRecvdAt {
                    size: final_size,
                    reliable_size: deliver_cap,
                    error_code,
                };
                // Release connection credit for the tail beyond the reliable size, which will never
                // be delivered. Data up to the reliable size releases its credit as it is read.
                Ok(ResetAtOutcome::Applied {
                    received_delta,
                    credit: final_size - deliver_cap,
                })
            }
        }
    }

    pub(super) fn reset_code(&self) -> Option<VarInt> {
        match self.state {
            RecvState::ResetRecvd { error_code, .. } => Some(error_code),
            _ => None,
        }
    }

    /// If a reliable reset (RESET_STREAM_AT) is in progress, the offset up to which data is still to
    /// be delivered to the application.
    ///
    /// Used when a plain RESET_STREAM follows a RESET_STREAM_AT: only the credit for the
    /// still-deliverable region `[bytes_read, cap)` must be released, because the tail beyond the
    /// reliable size already had its credit released when the RESET_STREAM_AT was processed.
    pub(super) fn reliable_reset_deliver_cap(&self) -> Option<u64> {
        match self.state {
            RecvState::ResetRecvdAt { reliable_size, .. } => Some(reliable_size),
            _ => None,
        }
    }

    /// Compute the amount of flow control credit consumed, or return an error if more was consumed
    /// than issued
    fn credit_consumed_by(
        &self,
        offset: u64,
        received: u64,
        max_data: u64,
    ) -> Result<u64, TransportError> {
        let prev_end = self.end;
        let new_bytes = offset.saturating_sub(prev_end);
        if offset > self.sent_max_stream_data || received + new_bytes > max_data {
            debug!(
                received,
                new_bytes,
                max_data,
                offset,
                stream_max_data = self.sent_max_stream_data,
                "flow control error"
            );
            return Err(TransportError::FLOW_CONTROL_ERROR(""));
        }

        Ok(new_bytes)
    }
}

/// Chunks returned from [`RecvStream::read()`][crate::RecvStream::read].
///
/// ### Note: Finalization Needed
/// Bytes read from the stream are not released from the congestion window until
/// either [`Self::finalize()`] is called, or this type is dropped.
///
/// It is recommended that you call [`Self::finalize()`] because it returns a flag
/// telling you whether reading from the stream has resulted in the need to transmit a packet.
///
/// If this type is leaked, the stream will remain blocked on the remote peer until
/// another read from the stream is done.
pub struct Chunks<'a> {
    id: StreamId,
    ordered: bool,
    streams: &'a mut StreamsState,
    pending: &'a mut Retransmits,
    state: ChunksState,
    read: u64,
}

impl<'a> Chunks<'a> {
    pub(super) fn new(
        id: StreamId,
        ordered: bool,
        streams: &'a mut StreamsState,
        pending: &'a mut Retransmits,
    ) -> Result<Self, ReadableError> {
        let mut entry = match streams.recv.entry(id) {
            Entry::Occupied(entry) => entry,
            Entry::Vacant(_) => return Err(ReadableError::ClosedStream),
        };

        let mut recv =
            match get_or_insert_recv(streams.stream_receive_window)(entry.get_mut()).stopped {
                true => return Err(ReadableError::ClosedStream),
                false => entry.remove().unwrap().into_inner(), // this can't fail due to the previous get_or_insert_with
            };

        recv.assembler.ensure_ordering(ordered)?;
        Ok(Self {
            id,
            ordered,
            streams,
            pending,
            state: ChunksState::Readable(recv),
            read: 0,
        })
    }

    /// Next
    ///
    /// Should call finalize() when done calling this.
    pub fn next(&mut self, max_length: usize) -> Result<Option<Chunk>, ReadError> {
        let rs = match self.state {
            ChunksState::Readable(ref mut rs) => rs,
            ChunksState::Reset(error_code) => {
                return Err(ReadError::Reset(error_code));
            }
            ChunksState::Finished => {
                return Ok(None);
            }
            ChunksState::Finalized => panic!("must not call next() after finalize()"),
        };

        // For a reliable reset, never deliver data beyond the reliable size. The cap is by stream
        // offset (not the read cursor) so it is correct for unordered reads too.
        let chunk = match rs.state {
            RecvState::ResetRecvdAt { reliable_size, .. } => {
                rs.assembler
                    .read_capped(max_length, self.ordered, reliable_size)
            }
            _ => rs.assembler.read(max_length, self.ordered),
        };
        if let Some(chunk) = chunk {
            self.read += chunk.bytes.len() as u64;
            return Ok(Some(chunk));
        }

        match rs.state {
            RecvState::ResetRecvd { error_code, .. } => {
                debug_assert_eq!(self.read, 0, "reset streams have empty buffers");
                let state = mem::replace(&mut self.state, ChunksState::Reset(error_code));
                // At this point if we have `rs` self.state must be `ChunksState::Readable`
                let recv = match state {
                    ChunksState::Readable(recv) => StreamRecv::Open(recv),
                    _ => unreachable!("state must be ChunkState::Readable"),
                };
                self.streams.stream_recv_freed(self.id, recv);
                Err(ReadError::Reset(error_code))
            }
            RecvState::ResetRecvdAt {
                reliable_size,
                error_code,
                ..
            } => {
                if rs.assembler.bytes_read() >= reliable_size {
                    // All reliable data has been delivered; surface the reset and dispose of the
                    // stream. Any data buffered beyond the reliable size is dropped undelivered.
                    let state = mem::replace(&mut self.state, ChunksState::Reset(error_code));
                    let recv = match state {
                        ChunksState::Readable(recv) => StreamRecv::Open(recv),
                        _ => unreachable!("state must be ChunkState::Readable"),
                    };
                    self.streams.stream_recv_freed(self.id, recv);
                    Err(ReadError::Reset(error_code))
                } else {
                    // A gap remains below the reliable size; wait for the sender to retransmit it.
                    Err(ReadError::Blocked)
                }
            }
            RecvState::Recv { size } => {
                if size == Some(rs.end) && rs.assembler.bytes_read() == rs.end {
                    let state = mem::replace(&mut self.state, ChunksState::Finished);
                    // At this point if we have `rs` self.state must be `ChunksState::Readable`
                    let recv = match state {
                        ChunksState::Readable(recv) => StreamRecv::Open(recv),
                        _ => unreachable!("state must be ChunkState::Readable"),
                    };
                    self.streams.stream_recv_freed(self.id, recv);
                    Ok(None)
                } else {
                    // We don't need a distinct `ChunksState` variant for a blocked stream because
                    // retrying a read harmlessly re-traces our steps back to returning
                    // `Err(Blocked)` again. The buffers can't refill and the stream's own state
                    // can't change so long as this `Chunks` exists.
                    Err(ReadError::Blocked)
                }
            }
        }
    }

    /// Mark the read data as consumed from the stream.
    ///
    /// The number of read bytes will be released from the congestion window,
    /// allowing the remote peer to send more data if it was previously blocked.
    ///
    /// If [`ShouldTransmit::should_transmit()`] returns `true`,
    /// a packet needs to be sent to the peer informing them that the stream is unblocked.
    /// This means that you should call [`Connection::poll_transmit()`][crate::Connection::poll_transmit]
    /// and send the returned packet as soon as is reasonable, to unblock the remote peer.
    pub fn finalize(mut self) -> ShouldTransmit {
        self.finalize_inner()
    }

    fn finalize_inner(&mut self) -> ShouldTransmit {
        let state = mem::replace(&mut self.state, ChunksState::Finalized);
        if let ChunksState::Finalized = state {
            // Noop on repeated calls
            return ShouldTransmit(false);
        }

        // We issue additional stream ID credit after the application is notified that a previously
        // open stream has finished or been reset and we've therefore disposed of its state, as
        // recorded by `stream_freed` calls in `next`.
        let mut should_transmit = self.streams.queue_max_stream_id(self.pending);

        // If the stream hasn't finished, we may need to issue stream-level flow control credit
        if let ChunksState::Readable(mut rs) = state {
            let (_, max_stream_data) = rs.max_stream_data(self.streams.stream_receive_window);
            should_transmit |= max_stream_data.0;
            if max_stream_data.0 {
                self.pending.max_stream_data.insert(self.id);
            }
            // Return the stream to storage for future use
            self.streams
                .recv
                .insert(self.id, Some(StreamRecv::Open(rs)));
        }

        // Issue connection-level flow control credit for any data we read regardless of state
        let max_data = self.streams.add_read_credits(self.read);
        self.pending.max_data |= max_data.0;
        should_transmit |= max_data.0;
        ShouldTransmit(should_transmit)
    }
}

impl Drop for Chunks<'_> {
    fn drop(&mut self) {
        let _ = self.finalize_inner();
    }
}

enum ChunksState {
    Readable(Box<Recv>),
    Reset(VarInt),
    Finished,
    Finalized,
}

/// Errors triggered when reading from a recv stream
#[derive(Debug, Error, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ReadError {
    /// No more data is currently available on this stream.
    ///
    /// If more data on this stream is received from the peer, an `Event::StreamReadable` will be
    /// generated for this stream, indicating that retrying the read might succeed.
    #[error("blocked")]
    Blocked,
    /// The peer abandoned transmitting data on this stream.
    ///
    /// Carries an application-defined error code.
    #[error("reset by peer: code {0}")]
    Reset(VarInt),
}

/// Errors triggered when opening a recv stream for reading
#[derive(Debug, Error, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum ReadableError {
    /// The stream has not been opened or was already stopped, finished, or reset
    #[error("closed stream")]
    ClosedStream,
    /// Attempted an ordered read following an unordered read
    ///
    /// Performing an unordered read allows discontinuities to arise in the receive buffer of a
    /// stream which cannot be recovered, making further ordered reads impossible.
    #[error("ordered read after unordered read")]
    IllegalOrderedRead,
}

impl From<IllegalOrderedRead> for ReadableError {
    fn from(_: IllegalOrderedRead) -> Self {
        Self::IllegalOrderedRead
    }
}

/// The effect of a RESET_STREAM_AT frame on connection-level flow control, returned by
/// [`Recv::reset_at`] for the caller to apply.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum ResetAtOutcome {
    /// The frame was redundant, or attempted to increase the reliable size, and was ignored.
    Ignored,
    /// A reliable reset was newly established or its reliable size reduced.
    Applied {
        /// Amount to add to the connection-level received-bytes counter (`data_recvd`): the
        /// never-arriving tail up to the final size. Zero when only reducing the reliable size.
        received_delta: u64,
        /// Connection flow-control credit to release immediately for data beyond the reliable size
        /// that will never be delivered to the application.
        credit: u64,
    },
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
enum RecvState {
    Recv {
        size: Option<u64>,
    },
    ResetRecvd {
        size: u64,
        error_code: VarInt,
    },
    /// A RESET_STREAM_AT (reliable reset) was received. Stream data up to `reliable_size` is still
    /// delivered to the application; once it has all been read, the application observes the reset
    /// carrying `error_code`. `size` is the stream's final size (`>= reliable_size`), used for
    /// flow-control accounting and final-size consistency checks.
    ResetRecvdAt {
        size: u64,
        reliable_size: u64,
        error_code: VarInt,
    },
}

impl Default for RecvState {
    fn default() -> Self {
        Self::Recv { size: None }
    }
}

#[cfg(test)]
mod tests {
    use bytes::Bytes;

    use crate::{Dir, Side};

    use super::*;

    #[test]
    fn reordered_frames_while_stopped() {
        const INITIAL_BYTES: u64 = 3;
        const INITIAL_OFFSET: u64 = 3;
        const RECV_WINDOW: u64 = 8;
        let mut s = Recv::new(RECV_WINDOW);
        let mut data_recvd = 0;
        // Receive bytes 3..6
        let (new_bytes, is_closed) = s
            .ingest(
                frame::Stream {
                    id: StreamId::new(Side::Client, Dir::Uni, 0),
                    offset: INITIAL_OFFSET,
                    fin: false,
                    data: Bytes::from_static(&[0; INITIAL_BYTES as usize]),
                },
                123,
                data_recvd,
                data_recvd + 1024,
            )
            .unwrap();
        data_recvd += new_bytes;
        assert_eq!(new_bytes, INITIAL_OFFSET + INITIAL_BYTES);
        assert!(!is_closed);

        let (credits, transmit) = s.stop().unwrap();
        assert!(transmit.should_transmit());
        assert_eq!(
            credits,
            INITIAL_OFFSET + INITIAL_BYTES,
            "full connection flow control credit is issued by stop"
        );

        let (max_stream_data, transmit) = s.max_stream_data(RECV_WINDOW);
        assert!(!transmit.should_transmit());
        assert_eq!(
            max_stream_data, RECV_WINDOW,
            "stream flow control credit isn't issued by stop"
        );

        // Receive byte 7
        let (new_bytes, is_closed) = s
            .ingest(
                frame::Stream {
                    id: StreamId::new(Side::Client, Dir::Uni, 0),
                    offset: RECV_WINDOW - 1,
                    fin: false,
                    data: Bytes::from_static(&[0; 1]),
                },
                123,
                data_recvd,
                data_recvd + 1024,
            )
            .unwrap();
        data_recvd += new_bytes;
        assert_eq!(new_bytes, RECV_WINDOW - (INITIAL_OFFSET + INITIAL_BYTES));
        assert!(!is_closed);

        let (max_stream_data, transmit) = s.max_stream_data(RECV_WINDOW);
        assert!(!transmit.should_transmit());
        assert_eq!(
            max_stream_data, RECV_WINDOW,
            "stream flow control credit isn't issued after stop"
        );

        // Receive bytes 0..3
        let (new_bytes, is_closed) = s
            .ingest(
                frame::Stream {
                    id: StreamId::new(Side::Client, Dir::Uni, 0),
                    offset: 0,
                    fin: false,
                    data: Bytes::from_static(&[0; INITIAL_OFFSET as usize]),
                },
                123,
                data_recvd,
                data_recvd + 1024,
            )
            .unwrap();
        assert_eq!(
            new_bytes, 0,
            "reordered frames don't issue connection-level flow control for stopped streams"
        );
        assert!(!is_closed);

        let (max_stream_data, transmit) = s.max_stream_data(RECV_WINDOW);
        assert!(!transmit.should_transmit());
        assert_eq!(
            max_stream_data, RECV_WINDOW,
            "stream flow control credit isn't issued after stop"
        );
    }

    const WINDOW: u64 = 1024;

    fn stream_id() -> StreamId {
        StreamId::new(Side::Client, Dir::Uni, 0)
    }

    /// Ingest `len` bytes at `offset` into a receive stream, returning the new connection bytes.
    fn ingest_at(r: &mut Recv, offset: u64, len: usize, received: u64) -> u64 {
        let data = Bytes::from(vec![0u8; len]);
        let (new_bytes, _) = r
            .ingest(
                frame::Stream {
                    id: stream_id(),
                    offset,
                    fin: false,
                    data,
                },
                len,
                received,
                WINDOW,
            )
            .unwrap();
        new_bytes
    }

    #[test]
    fn reset_at_establishes_reliable_reset() {
        let mut r = Recv::new(WINDOW);
        assert_eq!(ingest_at(&mut r, 0, 30, 0), 30);

        // Final size 100, deliver up to 40.
        let outcome = r
            .reset_at(7u32.into(), 100u32.into(), 40u32.into(), 30, WINDOW)
            .unwrap();
        // The never-arriving tail [30, 100) is accounted as received (70 bytes); the never-delivered
        // tail [40, 100) releases its connection credit immediately (60 bytes).
        assert_eq!(
            outcome,
            ResetAtOutcome::Applied {
                received_delta: 70,
                credit: 60,
            }
        );
        assert!(
            r.is_receiving(),
            "must keep accepting retransmitted reliable data"
        );
        assert_eq!(r.reset_code(), None, "reset surfaces only after delivery");
    }

    #[test]
    fn reset_at_does_not_double_count_retransmits() {
        let mut r = Recv::new(WINDOW);
        assert_eq!(ingest_at(&mut r, 0, 20, 0), 20);
        r.reset_at(0u32.into(), 100u32.into(), 40u32.into(), 20, WINDOW)
            .unwrap();

        // Retransmitted reliable data [20, 40) arriving after the reset must not consume any more
        // connection credit: the final size was already fully accounted.
        assert_eq!(ingest_at(&mut r, 20, 20, 100), 0);
    }

    #[test]
    fn reset_at_ignores_reliable_size_increase() {
        let mut r = Recv::new(WINDOW);
        r.reset_at(0u32.into(), 100u32.into(), 40u32.into(), 0, WINDOW)
            .unwrap();
        // A later (reordered) frame raising the reliable size must be ignored.
        let outcome = r
            .reset_at(0u32.into(), 100u32.into(), 60u32.into(), 100, WINDOW)
            .unwrap();
        assert_eq!(outcome, ResetAtOutcome::Ignored);
    }

    #[test]
    fn reset_at_reduces_reliable_size() {
        let mut r = Recv::new(WINDOW);
        r.reset_at(0u32.into(), 100u32.into(), 40u32.into(), 0, WINDOW)
            .unwrap();
        // Reducing 40 -> 25 releases the 15 bytes that will no longer be delivered.
        let outcome = r
            .reset_at(0u32.into(), 100u32.into(), 25u32.into(), 100, WINDOW)
            .unwrap();
        assert_eq!(
            outcome,
            ResetAtOutcome::Applied {
                received_delta: 0,
                credit: 15,
            }
        );
    }

    #[test]
    fn reset_at_final_size_must_be_consistent() {
        let mut r = Recv::new(WINDOW);
        r.reset_at(0u32.into(), 100u32.into(), 40u32.into(), 0, WINDOW)
            .unwrap();
        let err = r
            .reset_at(0u32.into(), 90u32.into(), 40u32.into(), 100, WINDOW)
            .unwrap_err();
        assert_eq!(err.code, crate::TransportErrorCode::FINAL_SIZE_ERROR);
    }

    #[test]
    fn reset_at_final_size_below_high_water_is_error() {
        let mut r = Recv::new(WINDOW);
        assert_eq!(ingest_at(&mut r, 0, 50, 0), 50);
        // A final size below the data already received is illegal.
        let err = r
            .reset_at(0u32.into(), 40u32.into(), 30u32.into(), 50, WINDOW)
            .unwrap_err();
        assert_eq!(err.code, crate::TransportErrorCode::FINAL_SIZE_ERROR);
    }

    #[test]
    fn reset_at_respects_connection_flow_control() {
        let mut r = Recv::new(WINDOW);
        // Final size 50 would push consumption (20 already + 50) past the 40-byte budget.
        let err = r
            .reset_at(0u32.into(), 50u32.into(), 30u32.into(), 20, 40)
            .unwrap_err();
        assert_eq!(err.code, crate::TransportErrorCode::FLOW_CONTROL_ERROR);
    }

    #[test]
    fn reset_at_error_code_is_immutable() {
        let mut r = Recv::new(WINDOW);
        r.reset_at(7u32.into(), 100u32.into(), 40u32.into(), 0, WINDOW)
            .unwrap();
        // A later frame for the same stream that changes the error code is a STREAM_STATE_ERROR
        // (RFC §5.2), even when it does not reduce the reliable size.
        let err = r
            .reset_at(8u32.into(), 100u32.into(), 40u32.into(), 100, WINDOW)
            .unwrap_err();
        assert_eq!(err.code, crate::TransportErrorCode::STREAM_STATE_ERROR);
    }
}
