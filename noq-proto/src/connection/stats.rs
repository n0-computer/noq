//! Connection statistics

use rustc_hash::FxHashMap;

use crate::Duration;
use crate::FrameType;

use super::PathId;

/// Statistics about UDP datagrams transmitted or received on a connection.
///
/// All QUIC packets are carried by UDP datagrams. Hence, these statistics cover all traffic
/// on a connection.
#[derive(Default, Debug, Copy, Clone, PartialEq, Eq, derive_more::Add, derive_more::AddAssign)]
#[non_exhaustive]
pub struct UdpStats {
    /// The number of UDP datagrams observed.
    pub datagrams: u64,
    /// The total amount of bytes which have been transferred inside UDP datagrams.
    pub bytes: u64,
    /// The number of I/O operations executed.
    ///
    /// Can be less than `datagrams` when GSO, GRO, and/or batched system calls are in use.
    pub ios: u64,
}

impl UdpStats {
    pub(crate) fn on_sent(&mut self, datagrams: u64, bytes: usize) {
        self.datagrams += datagrams;
        self.bytes += bytes as u64;
        self.ios += 1;
    }
}

/// Number of frames transmitted or received of each frame type.
#[derive(
    Default,
    Copy,
    Clone,
    PartialEq,
    Eq,
    derive_more::Debug,
    derive_more::Add,
    derive_more::AddAssign,
)]
#[non_exhaustive]
#[allow(missing_docs)]
pub struct FrameStats {
    #[debug("ACK")]
    pub acks: u64,
    #[debug("PATH_ACK")]
    pub path_acks: u64,
    #[debug("ACK_FREQUENCY")]
    pub ack_frequency: u64,
    #[debug("CRYPTO")]
    pub crypto: u64,
    #[debug("CONNECTION_CLOSE")]
    pub connection_close: u64,
    #[debug("DATA_BLOCKED")]
    pub data_blocked: u64,
    #[debug("DATAGRAM")]
    pub datagram: u64,
    #[debug("HANDSHAKE_DONE")]
    pub handshake_done: u8,
    #[debug("IMMEDIATE_ACK")]
    pub immediate_ack: u64,
    #[debug("MAX_DATA")]
    pub max_data: u64,
    #[debug("MAX_STREAM_DATA")]
    pub max_stream_data: u64,
    #[debug("MAX_STREAMS_BIDI")]
    pub max_streams_bidi: u64,
    #[debug("MAX_STREAMS_UNI")]
    pub max_streams_uni: u64,
    #[debug("NEW_CONNECTION_ID")]
    pub new_connection_id: u64,
    #[debug("PATH_NEW_CONNECTION_ID")]
    pub path_new_connection_id: u64,
    #[debug("NEW_TOKEN")]
    pub new_token: u64,
    #[debug("PATH_CHALLENGE")]
    pub path_challenge: u64,
    #[debug("PATH_RESPONSE")]
    pub path_response: u64,
    #[debug("PING")]
    pub ping: u64,
    #[debug("RESET_STREAM")]
    pub reset_stream: u64,
    #[debug("RETIRE_CONNECTION_ID")]
    pub retire_connection_id: u64,
    #[debug("PATH_RETIRE_CONNECTION_ID")]
    pub path_retire_connection_id: u64,
    #[debug("STREAM_DATA_BLOCKED")]
    pub stream_data_blocked: u64,
    #[debug("STREAMS_BLOCKED_BIDI")]
    pub streams_blocked_bidi: u64,
    #[debug("STREAMS_BLOCKED_UNI")]
    pub streams_blocked_uni: u64,
    #[debug("STOP_SENDING")]
    pub stop_sending: u64,
    #[debug("STREAM")]
    pub stream: u64,
    #[debug("OBSERVED_ADDR")]
    pub observed_addr: u64,
    #[debug("PATH_ABANDON")]
    pub path_abandon: u64,
    #[debug("PATH_STATUS_AVAILABLE")]
    pub path_status_available: u64,
    #[debug("PATH_STATUS_BACKUP")]
    pub path_status_backup: u64,
    #[debug("MAX_PATH_ID")]
    pub max_path_id: u64,
    #[debug("PATHS_BLOCKED")]
    pub paths_blocked: u64,
    #[debug("PATH_CIDS_BLOCKED")]
    pub path_cids_blocked: u64,
    #[debug("ADD_ADDRESS")]
    pub add_address: u64,
    #[debug("REACH_OUT")]
    pub reach_out: u64,
    #[debug("REMOVE_ADDRESS")]
    pub remove_address: u64,
    #[debug("RESET_STREAM_AT")]
    pub reset_stream_at: u64,
}

impl FrameStats {
    pub(crate) fn record(&mut self, frame_type: FrameType) {
        use FrameType::*;
        // Increments the field. Added for readability
        macro_rules! inc {
            ($field_name: ident) => {{ self.$field_name = self.$field_name.saturating_add(1) }};
        }
        match frame_type {
            Padding => {}
            Ping => inc!(ping),
            Ack | AckEcn => inc!(acks),
            PathAck | PathAckEcn => inc!(path_acks),
            ResetStream => inc!(reset_stream),
            StopSending => inc!(stop_sending),
            Crypto => inc!(crypto),
            Datagram(_) => inc!(datagram),
            NewToken => inc!(new_token),
            MaxData => inc!(max_data),
            MaxStreamData => inc!(max_stream_data),
            MaxStreamsBidi => inc!(max_streams_bidi),
            MaxStreamsUni => inc!(max_streams_uni),
            DataBlocked => inc!(data_blocked),
            Stream(_) => inc!(stream),
            StreamDataBlocked => inc!(stream_data_blocked),
            StreamsBlockedUni => inc!(streams_blocked_uni),
            StreamsBlockedBidi => inc!(streams_blocked_bidi),
            NewConnectionId => inc!(new_connection_id),
            PathNewConnectionId => inc!(path_new_connection_id),
            RetireConnectionId => inc!(retire_connection_id),
            PathRetireConnectionId => inc!(path_retire_connection_id),
            PathChallenge => inc!(path_challenge),
            PathResponse => inc!(path_response),
            ConnectionClose | ApplicationClose => inc!(connection_close),
            AckFrequency => inc!(ack_frequency),
            ImmediateAck => inc!(immediate_ack),
            HandshakeDone => inc!(handshake_done),
            ObservedIpv4Addr | ObservedIpv6Addr => inc!(observed_addr),
            PathAbandon => inc!(path_abandon),
            PathStatusAvailable => inc!(path_status_available),
            PathStatusBackup => inc!(path_status_backup),
            MaxPathId => inc!(max_path_id),
            PathsBlocked => inc!(paths_blocked),
            PathCidsBlocked => inc!(path_cids_blocked),
            AddIpv4Address | AddIpv6Address => inc!(add_address),
            ReachOutAtIpv4 | ReachOutAtIpv6 => inc!(reach_out),
            RemoveAddress => inc!(remove_address),
            ResetStreamAt => inc!(reset_stream_at),
        };
    }
}

/// Statistics related to a transmission path.
#[derive(Debug, Default, Copy, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct PathStats {
    /// Current best estimate of this connection's latency (round-trip-time).
    pub rtt: Duration,
    /// Statistics about datagrams and bytes sent on this path.
    pub udp_tx: UdpStats,
    /// Statistics about datagrams and bytes received on this path.
    pub udp_rx: UdpStats,
    /// Statistics about frames transmitted on this path.
    pub frame_tx: FrameStats,
    /// Statistics about frames received on this path.
    pub frame_rx: FrameStats,
    /// Current congestion window of the connection.
    pub cwnd: u64,
    /// Congestion events on the connection.
    pub congestion_events: u64,
    /// Spurious congestion events on the connection.
    pub spurious_congestion_events: u64,
    /// The number of packets lost on this path.
    pub lost_packets: u64,
    /// The number of bytes lost on this path.
    pub lost_bytes: u64,
    /// The number of PLPMTUD probe packets sent on this path.
    ///
    /// These are also counted by [`UdpStats::datagrams`].
    pub sent_plpmtud_probes: u64,
    /// The number of PLPMTUD probe packets lost on this path.
    ///
    /// These are not included in [`Self::lost_packets`] and [`Self::lost_bytes`].
    pub lost_plpmtud_probes: u64,
    /// The number of times a black hole was detected in the path.
    pub black_holes_detected: u64,
    /// Largest UDP payload size the path currently supports.
    pub current_mtu: u16,
}

/// Connection statistics.
///
/// The fields here are a sum of the respective fields in the [`PathStats`] for all the
/// paths that exist as well as all paths that previously existed.
#[derive(Debug, Default, Clone)]
#[non_exhaustive]
pub struct ConnectionStats {
    /// Statistics about UDP datagrams transmitted on the connection.
    pub udp_tx: UdpStats,
    /// Statistics about UDP datagrams received on the connection.
    pub udp_rx: UdpStats,
    /// Statistics about frames transmitted on the connection.
    pub frame_tx: FrameStats,
    /// Statistics about frames received on the connection.
    pub frame_rx: FrameStats,
    /// The number of packets lost on the connection.
    pub lost_packets: u64,
    /// The number of bytes lost on the connection.
    pub lost_bytes: u64,
}

impl std::ops::Add<PathStats> for ConnectionStats {
    type Output = Self;

    fn add(self, rhs: PathStats) -> Self::Output {
        // Be aware that Connection::stats() relies on the fact this function ignores the
        // rtt, cwnd and current_mtu fields.
        let PathStats {
            rtt: _,
            udp_tx,
            udp_rx,
            frame_tx,
            frame_rx,
            cwnd: _,
            congestion_events: _,
            spurious_congestion_events: _,
            lost_packets,
            lost_bytes,
            sent_plpmtud_probes: _,
            lost_plpmtud_probes: _,
            black_holes_detected: _,
            current_mtu: _,
        } = rhs;
        Self {
            udp_tx: self.udp_tx + udp_tx,
            udp_rx: self.udp_rx + udp_rx,
            frame_tx: self.frame_tx + frame_tx,
            frame_rx: self.frame_rx + frame_rx,
            lost_packets: self.lost_packets + lost_packets,
            lost_bytes: self.lost_bytes + lost_bytes,
        }
    }
}

impl std::ops::AddAssign<PathStats> for ConnectionStats {
    fn add_assign(&mut self, rhs: PathStats) {
        // Be aware that Connection::stats() relies on the fact this function ignores the
        // rtt, cwnd and current_mtu fields.
        let PathStats {
            rtt: _,
            udp_tx: path_udp_tx,
            udp_rx: path_udp_rx,
            frame_tx: path_frame_tx,
            frame_rx: path_frame_rx,
            cwnd: _,
            congestion_events: _,
            spurious_congestion_events: _,
            lost_packets: path_lost_packets,
            lost_bytes: path_lost_bytes,
            sent_plpmtud_probes: _,
            lost_plpmtud_probes: _,
            black_holes_detected: _,
            current_mtu: _,
        } = rhs;
        let Self {
            udp_tx,
            udp_rx,
            frame_tx,
            frame_rx,
            lost_packets,
            lost_bytes,
        } = self;
        *udp_tx += path_udp_tx;
        *udp_rx += path_udp_rx;
        *frame_tx += path_frame_tx;
        *frame_rx += path_frame_rx;
        *lost_packets += path_lost_packets;
        *lost_bytes += path_lost_bytes;
    }
}

/// Helper to make [`PathStats`] infallibly available.
///
/// This helper also helps with borrowing issues compared to having the [`Self::for_path`]
/// function as a helper directly on [`Connection`].
///
/// [`Connection`]: super::Connection
#[derive(Debug, Default)]
pub(super) struct PathStatsMap(FxHashMap<PathId, PathStats>);

impl PathStatsMap {
    /// Returns the [`PathStats`] for the path.
    pub(super) fn for_path(&mut self, path_id: PathId) -> &mut PathStats {
        self.0.entry(path_id).or_default()
    }

    /// An iterator over all contained [`PathStats`].
    pub(super) fn iter_stats(&self) -> impl Iterator<Item = &PathStats> {
        self.0.values()
    }

    /// Removes the stats for a given path.
    ///
    /// Only do this once you are discarding the path.
    pub(super) fn discard(&mut self, path_id: &PathId) -> PathStats {
        self.0.remove(path_id).unwrap_or_default()
    }
}
