use alloc::{boxed::Box, sync::Arc};
use core::any::Any;

use super::{BASE_DATAGRAM_SIZE, Controller, ControllerFactory, PacketId};
use crate::Instant;
use crate::connection::RttEstimator;

/// A simple, standard congestion controller
#[derive(Debug, Clone)]
pub struct NewReno {
    config: Arc<NewRenoConfig>,
    current_mtu: u64,
    /// Maximum number of bytes in flight that may be sent.
    window: u64,
    /// Slow start threshold in bytes. When the congestion window is below ssthresh, the mode is
    /// slow start and the window grows by the number of bytes acknowledged.
    ssthresh: u64,
    /// The time when QUIC first detects a loss, causing it to enter recovery. When a packet sent
    /// after this time is acknowledged, QUIC exits recovery.
    recovery_start_time: Instant,
    /// Bytes which had been acked by the peer since leaving slow start
    bytes_acked: u64,
}

impl NewReno {
    /// Construct a state using the given `config` and current time `now`
    ///
    /// The window starts at the configured initial window, or at the minimum window of two
    /// datagrams of `current_mtu` if that is larger, so the first datagram always fits.
    pub fn new(config: Arc<NewRenoConfig>, now: Instant, current_mtu: u16) -> Self {
        Self {
            window: config.initial_window.max(2 * u64::from(current_mtu)),
            ssthresh: u64::MAX,
            recovery_start_time: now,
            current_mtu: current_mtu as u64,
            config,
            bytes_acked: 0,
        }
    }

    fn minimum_window(&self) -> u64 {
        2 * self.current_mtu
    }
}

impl Controller for NewReno {
    fn on_packet_space_acked(
        &mut self,
        _now: Instant,
        sent: Instant,
        bytes: u64,
        _packet: PacketId,
        app_limited: bool,
        _rtt: &RttEstimator,
    ) {
        if app_limited || sent <= self.recovery_start_time {
            return;
        }

        if self.window < self.ssthresh {
            // Slow start
            self.window += bytes;

            if self.window >= self.ssthresh {
                // Exiting slow start
                // Initialize `bytes_acked` for congestion avoidance. The idea
                // here is that any bytes over `sshthresh` will already be counted
                // towards the congestion avoidance phase - independent of when
                // how close to `sshthresh` the `window` was when switching states,
                // and independent of datagram sizes.
                self.bytes_acked = self.window - self.ssthresh;
            }
        } else {
            // Congestion avoidance
            // This implementation uses the method which does not require
            // floating point math, which also increases the window by 1 datagram
            // for every round trip.
            // This mechanism is called Appropriate Byte Counting in
            // https://tools.ietf.org/html/rfc3465
            self.bytes_acked += bytes;

            if self.bytes_acked >= self.window {
                self.bytes_acked -= self.window;
                self.window += self.current_mtu;
            }
        }
    }

    fn on_congestion_event_space(
        &mut self,
        now: Instant,
        sent: Instant,
        is_persistent_congestion: bool,
        _is_ecn: bool,
        _lost_bytes: u64,
        _largest_lost: PacketId,
    ) {
        if sent <= self.recovery_start_time {
            return;
        }

        self.recovery_start_time = now;
        self.window = (self.window as f32 * self.config.loss_reduction_factor) as u64;
        self.window = self.window.max(self.minimum_window());
        self.ssthresh = self.window;

        if is_persistent_congestion {
            self.window = self.minimum_window();
        }
    }

    // The pn-only callbacks are deprecated, but a controller wrapping this one may still forward
    // them. They keep working, with packets numbered in the Data space as before.
    fn on_ack(
        &mut self,
        now: Instant,
        sent: Instant,
        bytes: u64,
        pn: u64,
        app_limited: bool,
        rtt: &RttEstimator,
    ) {
        self.on_packet_space_acked(now, sent, bytes, PacketId::data(pn), app_limited, rtt);
    }

    fn on_congestion_event(
        &mut self,
        now: Instant,
        sent: Instant,
        is_persistent_congestion: bool,
        is_ecn: bool,
        lost_bytes: u64,
        largest_lost_pn: u64,
    ) {
        self.on_congestion_event_space(
            now,
            sent,
            is_persistent_congestion,
            is_ecn,
            lost_bytes,
            PacketId::data(largest_lost_pn),
        );
    }

    fn on_mtu_update(&mut self, new_mtu: u16) {
        self.current_mtu = new_mtu as u64;
        self.window = self.window.max(self.minimum_window());
    }

    fn window(&self) -> u64 {
        self.window
    }

    fn metrics(&self) -> super::ControllerMetrics {
        super::ControllerMetrics {
            congestion_window: self.window(),
            ssthresh: Some(self.ssthresh),
            pacing_rate: None,
            send_quantum: None,
            bandwidth_estimate: None,
        }
    }

    fn clone_box(&self) -> Box<dyn Controller> {
        Box::new(self.clone())
    }

    fn initial_window(&self) -> u64 {
        self.config.initial_window.max(self.minimum_window())
    }

    fn into_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

/// Configuration for the `NewReno` congestion controller
#[derive(Debug, Clone)]
pub struct NewRenoConfig {
    initial_window: u64,
    loss_reduction_factor: f32,
}

impl NewRenoConfig {
    /// Default limit on the amount of outstanding data in bytes.
    ///
    /// Recommended value: `min(10 * max_datagram_size, max(2 * max_datagram_size, 14720))`
    ///
    /// The controller starts at no less than two datagrams of the current MTU, so a large initial MTU cannot leave the
    /// first datagram blocked.
    pub fn initial_window(&mut self, value: u64) -> &mut Self {
        self.initial_window = value;
        self
    }

    /// Reduction in congestion window when a new loss event is detected.
    pub fn loss_reduction_factor(&mut self, value: f32) -> &mut Self {
        self.loss_reduction_factor = value;
        self
    }
}

impl Default for NewRenoConfig {
    fn default() -> Self {
        Self {
            initial_window: 14720.clamp(2 * BASE_DATAGRAM_SIZE, 10 * BASE_DATAGRAM_SIZE),
            loss_reduction_factor: 0.5,
        }
    }
}

impl ControllerFactory for NewRenoConfig {
    fn build(self: Arc<Self>, now: Instant, current_mtu: u16) -> Box<dyn Controller> {
        Box::new(NewReno::new(self, now, current_mtu))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Duration;

    /// A controller that wraps NewReno and forwards the deprecated pn-only callbacks still drives
    /// it: an ACK grows the window in slow start and a loss cuts it.
    #[test]
    #[allow(deprecated)]
    fn deprecated_callbacks_still_drive_new_reno() {
        let now = Instant::now();
        let rtt = RttEstimator::new(Duration::from_millis(100));
        let mut reno = NewReno::new(Arc::new(NewRenoConfig::default()), now, 1200);
        let window = reno.window();

        let sent = now + Duration::from_millis(1);
        let later = now + Duration::from_millis(100);
        reno.on_ack(later, sent, 1200, 0, false, &rtt);
        assert_eq!(reno.window(), window + 1200);

        reno.on_congestion_event(later, later, false, false, 1200, 1);
        assert!(reno.window() < window);
    }
}
