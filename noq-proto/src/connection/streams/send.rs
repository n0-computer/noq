use bytes::Bytes;
use thiserror::Error;

use crate::{
    VarInt,
    connection::{send_buffer::SendBuffer, streams::BytesOrSlice},
    frame,
};

use super::ResetStreamAtError;

#[derive(Debug)]
pub(super) struct Send {
    pub(super) max_data: u64,
    pub(super) state: SendState,
    pub(super) pending: SendBuffer,
    pub(super) priority: i32,
    /// Whether a frame containing a FIN bit must be transmitted, even if we don't have any new data
    pub(super) fin_pending: bool,
    /// Whether this stream is in the `connection_blocked` list of `Streams`
    pub(super) connection_blocked: bool,
    /// The reason the peer wants us to stop, if `STOP_SENDING` was received
    pub(super) stop_reason: Option<VarInt>,
    /// State of an in-progress reliable reset (RESET_STREAM_AT), if any.
    ///
    /// When set, the stream is in [`SendState::DataSent`] and keeps (re)transmitting buffered data
    /// up to the reliable size (the send buffer is truncated to it) before being closed with the
    /// reset's error code. Distinguishes a reliable reset from an ordinary FIN-based finish.
    pub(super) reset_at: Option<ResetAt>,
}

/// State for an in-progress reliable reset, see [`Send::reset_at`].
#[derive(Debug)]
pub(super) struct ResetAt {
    /// The stream's final size: the send offset captured at the first `reset_at` call, reported in
    /// every RESET_STREAM_AT frame for the stream. Immutable, even as the reliable size shrinks.
    pub(super) final_size: u64,
    /// The application error code carried by the reset. Immutable across frames.
    pub(super) error_code: VarInt,
    /// Whether the most recently transmitted RESET_STREAM_AT frame (carrying the current reliable
    /// size) has been acknowledged. Reset to `false` whenever the reliable size is reduced.
    pub(super) frame_acked: bool,
}

impl Send {
    pub(super) fn new(max_data: VarInt) -> Box<Self> {
        Box::new(Self {
            max_data: max_data.into(),
            state: SendState::Ready,
            pending: SendBuffer::new(),
            priority: 0,
            fin_pending: false,
            connection_blocked: false,
            stop_reason: None,
            reset_at: None,
        })
    }

    /// Whether the stream has been reset
    pub(super) fn is_reset(&self) -> bool {
        matches!(self.state, SendState::ResetSent)
    }

    pub(super) fn finish(&mut self) -> Result<(), FinishError> {
        if let Some(error_code) = self.stop_reason {
            Err(FinishError::Stopped(error_code))
        } else if self.state == SendState::Ready {
            self.state = SendState::DataSent {
                finish_acked: false,
            };
            self.fin_pending = true;
            Ok(())
        } else {
            Err(FinishError::ClosedStream)
        }
    }

    /// Begin or tighten a reliable reset (RESET_STREAM_AT), committing to delivering stream data up
    /// to `reliable_size` before the stream is reset with `error_code`.
    ///
    /// The send buffer is truncated to the reliable size so data beyond it is no longer
    /// (re)transmitted. The committed reliable size is read back from the buffer (it cannot drop
    /// below already-acknowledged data). Returns whether a RESET_STREAM_AT frame needs to be
    /// (re)queued for transmission.
    ///
    /// May be called repeatedly to *reduce* the reliable size; increasing it, changing the error
    /// code, or calling it after a FIN-based finish or an ordinary reset is rejected.
    pub(super) fn reset_at(
        &mut self,
        reliable_size: VarInt,
        error_code: VarInt,
    ) -> Result<bool, ResetStreamAtError> {
        if let Some(code) = self.stop_reason {
            return Err(ResetStreamAtError::Stopped(code));
        }
        let reliable_size = reliable_size.into_inner();
        match self.state {
            SendState::Ready => {
                // The reliable size cannot exceed the data the application has written so far.
                if reliable_size > self.pending.offset() {
                    return Err(ResetStreamAtError::InvalidReliableSize);
                }
                self.state = SendState::DataSent {
                    finish_acked: false,
                };
                self.reset_at = Some(ResetAt {
                    final_size: self.pending.offset(),
                    error_code,
                    frame_acked: false,
                });
                self.pending.truncate(reliable_size);
                Ok(true)
            }
            SendState::DataSent { .. } => {
                let current_reliable = self.pending.offset();
                let Some(reset_at) = self.reset_at.as_mut() else {
                    // The stream was finished with a FIN; reliable reset after FIN is unsupported.
                    return Err(ResetStreamAtError::ClosedStream);
                };
                // The error code and final size are immutable; the reliable size may only shrink.
                if error_code != reset_at.error_code || reliable_size > current_reliable {
                    return Err(ResetStreamAtError::InvalidReliableSize);
                }
                self.pending.truncate(reliable_size);
                if self.pending.offset() < current_reliable {
                    // The reliable size genuinely shrank, so a new RESET_STREAM_AT carrying it must
                    // be transmitted and acknowledged afresh.
                    reset_at.frame_acked = false;
                    Ok(true)
                } else {
                    Ok(false)
                }
            }
            SendState::ResetSent => Err(ResetStreamAtError::ClosedStream),
        }
    }

    pub(super) fn write<'a, S: BytesSource<'a>>(
        &mut self,
        source: &'a mut S,
        limit: u64,
    ) -> Result<Written, WriteError> {
        if !self.is_writable() {
            return Err(WriteError::ClosedStream);
        }
        if let Some(error_code) = self.stop_reason {
            return Err(WriteError::Stopped(error_code));
        }
        let budget = self.max_data - self.pending.offset();
        if budget == 0 {
            return Err(WriteError::Blocked);
        }
        let mut limit = limit.min(budget) as usize;

        let mut result = Written::default();
        loop {
            let (chunk, chunks_consumed) = source.pop_chunk(limit);
            result.chunks += chunks_consumed;
            result.bytes += chunk.len();

            if chunk.is_empty() {
                break;
            }

            limit -= chunk.len();
            self.pending.write(chunk);
        }

        Ok(result)
    }

    /// Update stream state due to a reset sent by the local application
    pub(super) fn reset(&mut self) {
        use SendState::*;
        if let DataSent { .. } | Ready = self.state {
            self.state = ResetSent;
        }
    }

    /// Handle STOP_SENDING
    ///
    /// Returns true if the stream was stopped due to this frame, and false
    /// if it had been stopped before
    pub(super) fn try_stop(&mut self, error_code: VarInt) -> bool {
        if self.stop_reason.is_none() {
            self.stop_reason = Some(error_code);
            true
        } else {
            false
        }
    }

    /// Returns whether the stream is fully closed and all data has been acknowledged by the peer
    ///
    /// For a FIN-based finish this means the FIN and all data were acknowledged; for a reliable
    /// reset it means the RESET_STREAM_AT frame and all data up to the reliable size were
    /// acknowledged.
    pub(super) fn ack(&mut self, frame: frame::StreamMeta) -> bool {
        self.pending.ack(frame.offsets);
        match self.state {
            SendState::DataSent {
                ref mut finish_acked,
            } => {
                if let Some(reset_at) = &self.reset_at {
                    // A reliable reset completes once the RESET_STREAM_AT frame and all data up to
                    // the reliable size have been acknowledged. The FIN bit is never set on these
                    // streams, so `finish_acked` is irrelevant here.
                    reset_at.frame_acked && self.pending.is_fully_acked()
                } else {
                    *finish_acked |= frame.fin;
                    *finish_acked && self.pending.is_fully_acked()
                }
            }
            _ => false,
        }
    }

    /// Records acknowledgement of a RESET_STREAM_AT frame carrying the current reliable size.
    ///
    /// Returns whether the reliable reset is now complete (the frame and all reliable data have
    /// been acknowledged), in which case the stream may be freed.
    pub(super) fn reset_at_acked(&mut self) -> bool {
        match &mut self.reset_at {
            Some(reset_at) => {
                reset_at.frame_acked = true;
                self.pending.is_fully_acked()
            }
            None => false,
        }
    }

    /// Handle increase to stream-level flow control limit
    ///
    /// Returns whether the stream was unblocked
    pub(super) fn increase_max_data(&mut self, offset: u64) -> bool {
        if offset <= self.max_data || self.state != SendState::Ready {
            return false;
        }
        let was_blocked = self.pending.offset() == self.max_data;
        self.max_data = offset;
        was_blocked
    }

    pub(super) fn offset(&self) -> u64 {
        self.pending.offset()
    }

    pub(super) fn is_pending(&self) -> bool {
        self.pending.has_unsent_data() || self.fin_pending
    }

    pub(super) fn is_writable(&self) -> bool {
        matches!(self.state, SendState::Ready)
    }
}

/// A [`BytesSource`] implementation for `&'a mut [Bytes]`
///
/// The type allows to dequeue [`Bytes`] chunks from an array of chunks, up to
/// a configured limit.
pub(crate) struct BytesArray<'a> {
    /// The wrapped slice of `Bytes`
    chunks: &'a mut [Bytes],
    /// The amount of chunks consumed from this source
    consumed: usize,
}

impl<'a> BytesArray<'a> {
    pub(crate) fn from_chunks(chunks: &'a mut [Bytes]) -> Self {
        Self {
            chunks,
            consumed: 0,
        }
    }
}

impl<'a> BytesSource<'a> for BytesArray<'a> {
    fn pop_chunk<'b>(&'b mut self, limit: usize) -> (impl BytesOrSlice<'b>, usize)
    where
        'a: 'b,
    {
        // The loop exists to skip empty chunks while still marking them as
        // consumed
        let mut chunks_consumed = 0;

        while self.consumed < self.chunks.len() {
            let chunk = &mut self.chunks[self.consumed];

            if chunk.len() <= limit {
                let chunk = std::mem::take(chunk);
                self.consumed += 1;
                chunks_consumed += 1;
                if chunk.is_empty() {
                    continue;
                }
                return (chunk, chunks_consumed);
            } else if limit > 0 {
                let chunk = chunk.split_to(limit);
                return (chunk, chunks_consumed);
            } else {
                break;
            }
        }

        (Bytes::new(), chunks_consumed)
    }
}

/// A [`BytesSource`] implementation for `&[u8]`
///
/// The type allows to dequeue a single [`Bytes`] chunk, which will be lazily
/// created from a reference. This allows to defer the allocation until it is
/// known how much data needs to be copied.
pub(crate) struct ByteSlice<'a> {
    /// The wrapped byte slice
    data: &'a [u8],
}

impl<'a> ByteSlice<'a> {
    pub(crate) fn from_slice(data: &'a [u8]) -> Self {
        Self { data }
    }
}

impl<'a> BytesSource<'a> for ByteSlice<'a> {
    fn pop_chunk<'b>(&'b mut self, limit: usize) -> (impl BytesOrSlice<'b>, usize)
    where
        'a: 'b,
    {
        let limit = limit.min(self.data.len());
        if limit == 0 {
            return (&[][..], 0);
        }

        let chunk = &self.data[..limit];
        self.data = &self.data[chunk.len()..];

        let chunks_consumed = usize::from(self.data.is_empty());
        (chunk, chunks_consumed)
    }
}

/// A source of one or more buffers which can be converted into `Bytes` buffers on demand
///
/// The purpose of this data type is to defer conversion as long as possible,
/// so that no heap allocation is required in case no data is writable.
pub(super) trait BytesSource<'a> {
    /// Returns the next chunk from the source of owned chunks.
    ///
    /// This method will consume parts of the source.
    /// Calling it will yield `Bytes` elements up to the configured `limit`.
    ///
    /// The method returns a tuple:
    /// - The first item is the yielded `Bytes` element. The element will be
    ///   empty if the limit is zero or no more data is available.
    /// - The second item returns how many complete chunks inside the source had
    ///   had been consumed. This can be less than 1, if a chunk inside the
    ///   source had been truncated in order to adhere to the limit. It can also
    ///   be more than 1, if zero-length chunks had been skipped.
    fn pop_chunk<'b>(&'b mut self, limit: usize) -> (impl BytesOrSlice<'b>, usize)
    where
        'a: 'b;
}

/// Indicates how many bytes and chunks had been transferred in a write operation
#[derive(Debug, Default, PartialEq, Eq, Clone, Copy)]
pub(crate) struct Written {
    /// The amount of bytes which had been written
    pub(crate) bytes: usize,
    /// The amount of full chunks which had been written
    ///
    /// If a chunk was only partially written, it will not be counted by this field.
    pub(crate) chunks: usize,
}

/// Errors triggered while writing to a send stream
#[derive(Debug, Error, Clone, Eq, PartialEq, Ord, PartialOrd, Hash)]
pub enum WriteError {
    /// The peer is not able to accept additional data, or the connection is congested.
    ///
    /// If the peer issues additional flow control credit, a [`StreamEvent::Writable`] event will
    /// be generated, indicating that retrying the write might succeed.
    ///
    /// [`StreamEvent::Writable`]: crate::StreamEvent::Writable
    #[error("unable to accept further writes")]
    Blocked,
    /// The peer is no longer accepting data on this stream, and it has been implicitly reset. The
    /// stream cannot be finished or further written to.
    ///
    /// Carries an application-defined error code.
    ///
    /// [`StreamEvent::Finished`]: crate::StreamEvent::Finished
    #[error("stopped by peer: code {0}")]
    Stopped(VarInt),
    /// The stream has not been opened or has already been finished or reset
    #[error("closed stream")]
    ClosedStream,
}

#[derive(Debug, Copy, Clone, Eq, PartialEq)]
pub(super) enum SendState {
    /// Sending new data
    Ready,
    /// Stream was finished; now sending retransmits only
    DataSent { finish_acked: bool },
    /// Sent RESET
    ResetSent,
}

/// Reasons why attempting to finish a stream might fail
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum FinishError {
    /// The peer is no longer accepting data on this stream. No
    /// [`StreamEvent::Finished`] event will be emitted for this stream.
    ///
    /// Carries an application-defined error code.
    ///
    /// [`StreamEvent::Finished`]: crate::StreamEvent::Finished
    #[error("stopped by peer: code {0}")]
    Stopped(VarInt),
    /// The stream has not been opened or was already finished or reset
    #[error("closed stream")]
    ClosedStream,
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A `Send` with `data` bytes written and ready to be reset.
    fn writer(data: &[u8]) -> Box<Send> {
        let mut send = Send::new(VarInt::MAX);
        let mut source = ByteSlice::from_slice(data);
        send.write(&mut source, data.len() as u64).unwrap();
        assert_eq!(send.offset(), data.len() as u64);
        send
    }

    #[test]
    fn reset_at_from_ready_truncates_and_keeps_final_size() {
        let mut send = writer(b"0123456789"); // 10 bytes
        assert!(send.reset_at(4u32.into(), 7u32.into()).unwrap());

        // The buffer is truncated to the reliable size, but the final size is remembered.
        assert_eq!(
            send.offset(),
            4,
            "reliable size becomes the send-buffer end"
        );
        let reset_at = send.reset_at.as_ref().expect("reliable reset in progress");
        assert_eq!(reset_at.final_size, 10);
        assert_eq!(reset_at.error_code, VarInt::from_u32(7));
        assert!(!reset_at.frame_acked);
        // The stream is closing: not writable, but not a full reset either (data still flows).
        assert!(!send.is_writable());
        assert!(!send.is_reset());
    }

    #[test]
    fn reset_at_reliable_size_may_equal_final_size() {
        let mut send = writer(b"0123456789");
        // Reliable size == bytes written is valid (deliver everything, then signal the reset).
        assert!(send.reset_at(10u32.into(), 0u32.into()).unwrap());
        assert_eq!(send.offset(), 10);
        assert_eq!(send.reset_at.as_ref().unwrap().final_size, 10);
    }

    #[test]
    fn reset_at_reliable_size_beyond_written_is_rejected() {
        let mut send = writer(b"0123456789");
        assert_eq!(
            send.reset_at(11u32.into(), 0u32.into()),
            Err(ResetStreamAtError::InvalidReliableSize)
        );
        // The stream is untouched and still writable.
        assert!(send.is_writable());
        assert!(send.reset_at.is_none());
    }

    #[test]
    fn reset_at_on_stopped_stream_is_rejected() {
        let mut send = writer(b"0123456789");
        assert!(send.try_stop(9u32.into()));
        assert_eq!(
            send.reset_at(4u32.into(), 0u32.into()),
            Err(ResetStreamAtError::Stopped(9u32.into()))
        );
    }

    #[test]
    fn reset_at_after_full_reset_is_rejected() {
        let mut send = writer(b"0123456789");
        send.reset();
        assert_eq!(
            send.reset_at(4u32.into(), 0u32.into()),
            Err(ResetStreamAtError::ClosedStream)
        );
    }

    #[test]
    fn reset_at_may_only_reduce_reliable_size() {
        let mut send = writer(b"0123456789");
        assert!(send.reset_at(6u32.into(), 7u32.into()).unwrap());

        // Reducing genuinely shrinks the buffer and requires re-acknowledging the frame.
        assert!(send.reset_at(3u32.into(), 7u32.into()).unwrap());
        assert_eq!(send.offset(), 3);
        assert!(!send.reset_at.as_ref().unwrap().frame_acked);

        // Re-requesting the same size is a no-op (no new frame needed).
        assert!(!send.reset_at(3u32.into(), 7u32.into()).unwrap());
        assert_eq!(send.offset(), 3);

        // Increasing the reliable size, or changing the error code, is rejected.
        assert_eq!(
            send.reset_at(5u32.into(), 7u32.into()),
            Err(ResetStreamAtError::InvalidReliableSize)
        );
        assert_eq!(
            send.reset_at(2u32.into(), 8u32.into()),
            Err(ResetStreamAtError::InvalidReliableSize)
        );
        // The final size never changes across reductions.
        assert_eq!(send.reset_at.as_ref().unwrap().final_size, 10);
    }

    #[test]
    fn reset_at_completes_only_when_frame_and_data_acknowledged() {
        // Data acknowledged first, then the frame.
        let mut send = writer(b"0123456789");
        assert!(send.reset_at(4u32.into(), 0u32.into()).unwrap());
        let meta = frame::StreamMeta {
            id: crate::StreamId::new(crate::Side::Client, crate::Dir::Uni, 0),
            offsets: 0..4,
            fin: false,
        };
        assert!(!send.ack(meta), "data acked but RESET_STREAM_AT not yet");
        assert!(
            send.reset_at_acked(),
            "frame ack now completes the reliable reset"
        );

        // Frame acknowledged first, then the data.
        let mut send = writer(b"0123456789");
        assert!(send.reset_at(4u32.into(), 0u32.into()).unwrap());
        assert!(!send.reset_at_acked(), "frame acked but data not yet");
        let meta = frame::StreamMeta {
            id: crate::StreamId::new(crate::Side::Client, crate::Dir::Uni, 0),
            offsets: 0..4,
            fin: false,
        };
        assert!(send.ack(meta), "data ack now completes the reliable reset");
    }

    #[test]
    fn bytes_array() {
        let full = b"Hello World 123456789 ABCDEFGHJIJKLMNOPQRSTUVWXYZ".to_owned();
        for limit in 0..full.len() {
            let mut chunks = [
                Bytes::from_static(b""),
                Bytes::from_static(b"Hello "),
                Bytes::from_static(b"Wo"),
                Bytes::from_static(b""),
                Bytes::from_static(b"r"),
                Bytes::from_static(b"ld"),
                Bytes::from_static(b""),
                Bytes::from_static(b" 12345678"),
                Bytes::from_static(b"9 ABCDE"),
                Bytes::from_static(b"F"),
                Bytes::from_static(b"GHJIJKLMNOPQRSTUVWXYZ"),
            ];
            let num_chunks = chunks.len();
            let last_chunk_len = chunks[chunks.len() - 1].len();

            let mut array = BytesArray::from_chunks(&mut chunks);

            let mut buf = Vec::new();
            let mut chunks_popped = 0;
            let mut chunks_consumed = 0;
            let mut remaining = limit;
            loop {
                let (chunk, consumed) = array.pop_chunk(remaining);
                chunks_consumed += consumed;

                if !chunk.is_empty() {
                    buf.extend_from_slice(chunk.as_ref());
                    remaining -= chunk.len();
                    chunks_popped += 1;
                } else {
                    break;
                }
            }

            assert_eq!(&buf[..], &full[..limit]);

            if limit == full.len() {
                // Full consumption of the last chunk
                assert_eq!(chunks_consumed, num_chunks);
                // Since there are empty chunks, we consume more than there are popped
                assert_eq!(chunks_consumed, chunks_popped + 3);
            } else if limit > full.len() - last_chunk_len {
                // Partial consumption of the last chunk
                assert_eq!(chunks_consumed, num_chunks - 1);
                assert_eq!(chunks_consumed, chunks_popped + 2);
            }
        }
    }

    #[test]
    fn byte_slice() {
        let full = b"Hello World 123456789 ABCDEFGHJIJKLMNOPQRSTUVWXYZ".to_owned();
        for limit in 0..full.len() {
            let mut array = ByteSlice::from_slice(&full[..]);

            let mut buf = Vec::new();
            let mut chunks_popped = 0;
            let mut chunks_consumed = 0;
            let mut remaining = limit;
            loop {
                let (chunk, consumed) = array.pop_chunk(remaining);
                chunks_consumed += consumed;

                if !chunk.is_empty() {
                    buf.extend_from_slice(chunk.as_ref());
                    remaining -= chunk.len();
                    chunks_popped += 1;
                } else {
                    break;
                }
            }

            assert_eq!(&buf[..], &full[..limit]);
            if limit != 0 {
                assert_eq!(chunks_popped, 1);
            } else {
                assert_eq!(chunks_popped, 0);
            }

            if limit == full.len() {
                assert_eq!(chunks_consumed, 1);
            } else {
                assert_eq!(chunks_consumed, 0);
            }
        }
    }
}
