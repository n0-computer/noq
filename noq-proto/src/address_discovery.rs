//! Address discovery types from
//! <https://datatracker.ietf.org/doc/draft-seemann-quic-address-discovery/>

use crate::VarInt;

/// The role of each participant.
///
/// When enabled, this is reported as a transport parameter.
#[derive(PartialEq, Eq, Clone, Copy, Debug, Default)]
pub(crate) struct Role {
    /// Whether this peer reports observed addresses to other peers.
    pub(crate) send: bool,
    /// Whether this peer wants to receive observed address reports from other peers.
    pub(crate) receive: bool,
}

impl Role {
    pub(crate) const fn send_only() -> Self {
        Self {
            send: true,
            receive: false,
        }
    }

    pub(crate) const fn receive_only() -> Self {
        Self {
            send: false,
            receive: true,
        }
    }

    pub(crate) const fn both() -> Self {
        Self {
            send: true,
            receive: true,
        }
    }
}

impl TryFrom<VarInt> for Role {
    type Error = crate::transport_parameters::Error;

    fn try_from(value: VarInt) -> Result<Self, Self::Error> {
        match value.0 {
            0 => Ok(Self::send_only()),
            1 => Ok(Self::receive_only()),
            2 => Ok(Self::both()),
            _ => Err(crate::transport_parameters::Error::IllegalValue),
        }
    }
}

impl Role {
    /// Whether address discovery is disabled.
    pub(crate) fn is_disabled(&self) -> bool {
        !self.send && !self.receive
    }

    /// Whether this peer should report observed addresses to the other peer.
    pub(crate) fn should_report(&self, other: &Self) -> bool {
        self.send && other.receive
    }

    /// Gives the [`VarInt`] representing this [`Role`] as a transport parameter.
    pub(crate) fn as_transport_parameter(&self) -> Option<VarInt> {
        match (self.send, self.receive) {
            (false, false) => None,
            (true, false) => Some(VarInt(0)),
            (false, true) => Some(VarInt(1)),
            (true, true) => Some(VarInt(2)),
        }
    }
}
