//! Logic for controlling the rate at which data is sent

use crate::connection::RttEstimator;
use crate::{Duration, Instant};
use alloc::{boxed::Box, sync::Arc};
use core::any::Any;

mod bbr3;
mod cubic;
mod new_reno;

pub use bbr3::{Bbr3, Bbr3Config};
pub use cubic::{Cubic, CubicConfig};
pub use new_reno::{NewReno, NewRenoConfig};

/// The packet number space a packet was sent in
pub use crate::connection::SpaceKind as Space;

/// A sent packet: its packet number space and its number within that space
///
/// Packet numbers restart from zero in each space, so the number alone is ambiguous while the
/// handshake spaces are live. Each path has its own controller, so this is unique per controller.
#[derive(Debug, Copy, Clone, Eq, PartialEq, Hash)]
pub struct PacketId {
    /// The packet number space
    pub space: Space,
    /// The packet number within `space`
    pub number: u64,
}

impl PacketId {
    /// Returns the Data-space packet `number`, the space the pn-only callbacks assume.
    pub(crate) fn data(number: u64) -> Self {
        Self {
            space: Space::Data,
            number,
        }
    }
}

/// Common interface for different congestion controllers
///
/// The transport calls the space-aware per-packet callbacks, such as
/// [`Self::on_packet_space_acked`]. A controller that wraps another must forward those. The
/// deprecated pn-only callbacks remain for older wrappers, and the built-in controllers still
/// answer them, assuming the Data space.
pub trait Controller: Send + Sync + core::fmt::Debug {
    /// One or more packets were just sent
    #[allow(unused_variables)]
    fn on_sent(&mut self, now: Instant, bytes: u64, largest_pn: u64) {}

    /// One packet was just sent
    #[deprecated(note = "implement `on_packet_space_sent`, which also names the packet's space")]
    #[allow(unused_variables)]
    fn on_packet_sent(&mut self, now: Instant, bytes: u16, pn: u64) {}

    /// One packet was just sent, identified by its space and number
    ///
    /// Defaults to the deprecated [`Self::on_packet_sent`], dropping the space.
    fn on_packet_space_sent(&mut self, now: Instant, bytes: u16, packet: PacketId) {
        #[allow(deprecated)]
        self.on_packet_sent(now, bytes, packet.number);
    }

    /// The connection had data to send but was blocked by the congestion window
    ///
    /// Reports the spec's `C.is_cwnd_limited` signal to the controller: the sender fully utilized
    /// the congestion window at this point in the current round trip.
    fn on_cwnd_limited(&mut self) {}

    /// The connection had nothing to send, and neither the controller nor the path held it back
    ///
    /// Reported on every such transmit poll, so a controller learns of starvation before the next
    /// send rather than at the next ACK. `in_flight` is the path's bytes in flight at that point.
    /// Data blocked on the peer's flow control credit counts as nothing to send. A controller
    /// should ignore the report when `in_flight` fills its window, since the connection is then
    /// window-limited rather than starved.
    #[allow(unused_variables)]
    fn on_app_limited(&mut self, in_flight: u64) {}

    /// Packet deliveries were confirmed
    #[deprecated(note = "implement `on_packet_space_acked`, which also names the packet's space")]
    #[allow(unused_variables)]
    fn on_ack(
        &mut self,
        now: Instant,
        sent: Instant,
        bytes: u64,
        pn: u64,
        app_limited: bool,
        rtt: &RttEstimator,
    ) {
    }

    /// One packet's delivery was confirmed, identified by its space and number
    ///
    /// `app_limited` indicates whether the connection was blocked on outgoing
    /// application data prior to receiving these acknowledgements.
    ///
    /// Defaults to the deprecated [`Self::on_ack`], dropping the space.
    fn on_packet_space_acked(
        &mut self,
        now: Instant,
        sent: Instant,
        bytes: u64,
        packet: PacketId,
        app_limited: bool,
        rtt: &RttEstimator,
    ) {
        #[allow(deprecated)]
        self.on_ack(now, sent, bytes, packet.number, app_limited, rtt);
    }

    /// Packets are acked in batches, all with the same `now` argument. This indicates one of those
    /// batches has completed.
    ///
    /// It is called after the loss detection that the ACK triggered, so the losses the ACK
    /// revealed have already been reported through [`Self::on_packet_space_lost`] and
    /// [`Self::on_congestion_event_space`], and `in_flight` no longer counts the lost packets.
    /// CE marks the ACK reported arrive after it.
    #[allow(unused_variables)]
    fn on_end_acks(
        &mut self,
        now: Instant,
        in_flight: u64,
        app_limited: bool,
        largest_packet_num_acked: Option<u64>,
    ) {
    }

    /// Packets were deemed lost or marked congested
    #[deprecated(
        note = "implement `on_congestion_event_space`, which also names the packet's space"
    )]
    #[allow(unused_variables)]
    fn on_congestion_event(
        &mut self,
        now: Instant,
        sent: Instant,
        is_persistent_congestion: bool,
        is_ecn: bool,
        lost_bytes: u64,
        largest_lost_pn: u64,
    ) {
    }

    /// Packets were deemed lost or marked congested
    ///
    /// `is_persistent_congestion` indicates whether all packets sent within the persistent
    /// congestion threshold period ending when the most recent packet in this batch was sent were
    /// lost.
    /// `lost_bytes` indicates how many bytes were lost. This value will be 0 for ECN triggers.
    /// `largest_lost` identifies the packet with the highest packet number in the congestion
    /// event, and `sent` is its send time. For ECN, that is the largest packet acknowledged by
    /// the ACK whose CE count increased, and the event fires only on such an increase.
    ///
    /// Defaults to the deprecated [`Self::on_congestion_event`], dropping the space.
    fn on_congestion_event_space(
        &mut self,
        now: Instant,
        sent: Instant,
        is_persistent_congestion: bool,
        is_ecn: bool,
        lost_bytes: u64,
        largest_lost: PacketId,
    ) {
        #[allow(deprecated)]
        self.on_congestion_event(
            now,
            sent,
            is_persistent_congestion,
            is_ecn,
            lost_bytes,
            largest_lost.number,
        );
    }

    /// One packet was just lost
    #[deprecated(note = "implement `on_packet_space_lost`, which also names the packet's space")]
    #[allow(unused_variables)]
    fn on_packet_lost(&mut self, lost_bytes: u16, pn: u64, now: Instant) {}

    /// One packet was just lost, identified by its space and number
    ///
    /// Defaults to the deprecated [`Self::on_packet_lost`], dropping the space.
    fn on_packet_space_lost(&mut self, lost_bytes: u16, packet: PacketId, now: Instant) {
        #[allow(deprecated)]
        self.on_packet_lost(lost_bytes, packet.number, now);
    }

    /// Packets were incorrectly deemed lost
    ///
    /// This function is called when all packets that were deemed lost (for instance because
    /// of packet reordering) are acknowledged after the congestion event was raised.
    fn on_spurious_congestion_event(&mut self) {}

    /// The known MTU for the current network path has been updated
    fn on_mtu_update(&mut self, new_mtu: u16);

    /// The peer's ACK-frequency parameters have changed
    ///
    /// `ack_eliciting_threshold` is the number of ack-eliciting packets the peer may receive
    /// before being required to send an immediate ACK (per the QUIC ACK frequency extension).
    /// `requested_max_ack_delay` is the maximum delay we asked the peer to wait before sending
    /// an ACK when the threshold hasn't been reached.
    ///
    /// Controllers can use this to refine estimates that depend on peer ACK behavior (e.g.
    /// BBR's offload budget).
    #[allow(unused_variables)]
    fn on_ack_frequency_update(
        &mut self,
        ack_eliciting_threshold: u64,
        requested_max_ack_delay: Duration,
    ) {
    }

    /// Number of ack-eliciting bytes that may be in flight
    fn window(&self) -> u64;

    /// Retrieve implementation-specific metrics used to populate connection statistics and `qlog`
    /// traces when they are enabled. This is also used to alter the pacing of the connection with
    /// `pacing_rate` and `send_quantum`.
    fn metrics(&self) -> ControllerMetrics {
        ControllerMetrics {
            congestion_window: self.window(),
            ssthresh: None,
            pacing_rate: None,
            send_quantum: None,
            bandwidth_estimate: None,
        }
    }

    /// Duplicate the controller's state
    fn clone_box(&self) -> Box<dyn Controller>;

    /// Initial congestion window
    fn initial_window(&self) -> u64;

    /// Returns Self for use in down-casting to extract implementation details
    fn into_any(self: Box<Self>) -> Box<dyn Any>;
}

/// Common congestion controller metrics used both for logging purposes
/// but also to alter the pacing of the connection with
/// `pacing_rate` and `send_quantum`
#[derive(Default)]
#[non_exhaustive]
pub struct ControllerMetrics {
    /// Congestion window (bytes)
    pub congestion_window: u64,
    /// Slow start threshold (bytes)
    pub ssthresh: Option<u64>,
    /// Pacing rate (bytes/s)
    pub pacing_rate: Option<u64>,
    /// Send Quantum (bytes) used to control the size of packet bursts
    pub send_quantum: Option<u64>,
    /// Estimated sustainable path bandwidth (bits/s).
    pub bandwidth_estimate: Option<u64>,
}

/// Constructs controllers on demand
pub trait ControllerFactory {
    /// Construct a fresh `Controller`
    fn build(self: Arc<Self>, now: Instant, current_mtu: u16) -> Box<dyn Controller>;
}

const BASE_DATAGRAM_SIZE: u64 = 1200;
