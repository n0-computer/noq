//! Ethernet, IPv4 and UDP headers of AF_XDP frames.
//!
//! AF_XDP hands whole Ethernet frames to and from user space, so the socket builds the headers
//! the kernel's UDP stack would otherwise build, including both checksums.

use std::{
    fmt, io,
    net::{Ipv4Addr, SocketAddrV4},
    str::FromStr,
};

/// Length of an Ethernet header without VLAN tags
const ETH_HDR_LEN: usize = 14;
/// Length of an IPv4 header without options
const IPV4_HDR_LEN: usize = 20;
/// Length of a UDP header
const UDP_HDR_LEN: usize = 8;
/// Length of the headers in front of the UDP payload of a frame this module builds
pub(super) const HEADERS_LEN: usize = ETH_HDR_LEN + IPV4_HDR_LEN + UDP_HDR_LEN;
/// Length of the IPv4 and UDP headers, which count against the link MTU
pub(super) const IP_UDP_HDR_LEN: usize = IPV4_HDR_LEN + UDP_HDR_LEN;

const ETHERTYPE_IPV4: u16 = 0x0800;
const IPPROTO_UDP: u8 = 17;
/// The IPv4 "don't fragment" flag, set on every packet so that MTU discovery works
const IPV4_DONT_FRAGMENT: u16 = 0x4000;
/// The "more fragments" flag and the fragment offset of an IPv4 header
const IPV4_FRAGMENT_MASK: u16 = 0x3fff;
const DEFAULT_TTL: u8 = 64;

/// An Ethernet MAC address
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct MacAddr(pub [u8; 6]);

impl fmt::Display for MacAddr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let [a, b, c, d, e, g] = self.0;
        write!(f, "{a:02x}:{b:02x}:{c:02x}:{d:02x}:{e:02x}:{g:02x}")
    }
}

impl FromStr for MacAddr {
    type Err = io::Error;

    /// Parses the colon-separated hexadecimal form, as in `02:00:00:00:00:01`.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let invalid = || io::Error::new(io::ErrorKind::InvalidInput, format!("invalid MAC {s:?}"));
        let mut mac = [0; 6];
        let mut parts = s.trim().split(':');
        for byte in &mut mac {
            let part = parts.next().ok_or_else(invalid)?;
            *byte = u8::from_str_radix(part, 16).map_err(|_| invalid())?;
        }
        match parts.next() {
            Some(_) => Err(invalid()),
            None => Ok(Self(mac)),
        }
    }
}

/// The addresses and ECN bits of one outgoing frame
#[derive(Debug, Clone, Copy)]
pub(super) struct FrameHeader {
    pub(super) src_mac: MacAddr,
    pub(super) dst_mac: MacAddr,
    pub(super) src: SocketAddrV4,
    pub(super) dst: SocketAddrV4,
    /// The two ECN bits of the IPv4 TOS byte
    pub(super) ecn: u8,
}

/// Writes the headers for a UDP payload of `payload_len` bytes at `frame[HEADERS_LEN..]`
///
/// The payload must already be in place, because the UDP checksum covers it. Returns the
/// length of the whole frame.
///
/// # Panics
///
/// Panics if `frame` is shorter than [`HEADERS_LEN`] plus `payload_len`, or the payload does
/// not fit an IPv4 packet.
pub(super) fn write_headers(frame: &mut [u8], header: &FrameHeader, payload_len: usize) -> usize {
    let frame_len = HEADERS_LEN + payload_len;
    let ip_len = u16::try_from(IP_UDP_HDR_LEN + payload_len).expect("payload fits IPv4");
    let udp_len = ip_len - IPV4_HDR_LEN as u16;
    let (eth, rest) = frame[..frame_len].split_at_mut(ETH_HDR_LEN);
    let (ip, udp) = rest.split_at_mut(IPV4_HDR_LEN);

    eth[0..6].copy_from_slice(&header.dst_mac.0);
    eth[6..12].copy_from_slice(&header.src_mac.0);
    eth[12..14].copy_from_slice(&ETHERTYPE_IPV4.to_be_bytes());

    ip[0] = 0x45; // version 4, header length 5 words
    ip[1] = header.ecn & 0b11; // DSCP 0
    ip[2..4].copy_from_slice(&ip_len.to_be_bytes());
    ip[4..6].copy_from_slice(&[0, 0]); // identification, unused with DF set
    ip[6..8].copy_from_slice(&IPV4_DONT_FRAGMENT.to_be_bytes());
    ip[8] = DEFAULT_TTL;
    ip[9] = IPPROTO_UDP;
    ip[10..12].copy_from_slice(&[0, 0]);
    ip[12..16].copy_from_slice(&header.src.ip().octets());
    ip[16..20].copy_from_slice(&header.dst.ip().octets());
    let ip_checksum = finish_checksum(checksum_add(0, ip));
    ip[10..12].copy_from_slice(&ip_checksum.to_be_bytes());

    udp[0..2].copy_from_slice(&header.src.port().to_be_bytes());
    udp[2..4].copy_from_slice(&header.dst.port().to_be_bytes());
    udp[4..6].copy_from_slice(&udp_len.to_be_bytes());
    udp[6..8].copy_from_slice(&[0, 0]);
    let pseudo = pseudo_header_sum(*header.src.ip(), *header.dst.ip(), udp_len);
    let udp_checksum = match finish_checksum(checksum_add(pseudo, udp)) {
        // Zero means "no checksum" in UDP over IPv4, so a computed zero is sent as all ones.
        0 => 0xffff,
        sum => sum,
    };
    udp[6..8].copy_from_slice(&udp_checksum.to_be_bytes());
    frame_len
}

/// A UDP datagram parsed out of a received frame
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ParsedDatagram<'a> {
    pub(super) src_mac: MacAddr,
    pub(super) src: SocketAddrV4,
    pub(super) dst: SocketAddrV4,
    /// The two ECN bits of the IPv4 TOS byte
    pub(super) ecn: u8,
    pub(super) payload: &'a [u8],
}

/// Parses an Ethernet frame carrying an unfragmented UDP over IPv4 datagram
///
/// Returns `None` for anything else. Checksums are not verified: the XDP program only passes
/// UDP to the socket, and QUIC authenticates every packet it accepts.
pub(super) fn parse_frame(frame: &[u8]) -> Option<ParsedDatagram<'_>> {
    let eth = frame.get(..ETH_HDR_LEN)?;
    if u16::from_be_bytes([eth[12], eth[13]]) != ETHERTYPE_IPV4 {
        return None;
    }
    let ip = &frame[ETH_HDR_LEN..];
    let fixed = ip.get(..IPV4_HDR_LEN)?;
    if fixed[0] >> 4 != 4 || fixed[9] != IPPROTO_UDP {
        return None;
    }
    let ihl = usize::from(fixed[0] & 0x0f) * 4;
    let total_len = usize::from(u16::from_be_bytes([fixed[2], fixed[3]]));
    let flags = u16::from_be_bytes([fixed[6], fixed[7]]);
    if ihl < IPV4_HDR_LEN || total_len < ihl + UDP_HDR_LEN || flags & IPV4_FRAGMENT_MASK != 0 {
        return None;
    }
    // Ethernet pads short frames, so the IP total length bounds the packet, not the frame.
    let ip = ip.get(..total_len)?;
    let udp = &ip[ihl..];
    let udp_len = usize::from(u16::from_be_bytes([udp[4], udp[5]]));
    let payload = udp.get(UDP_HDR_LEN..udp_len)?;
    let src_ip = Ipv4Addr::new(fixed[12], fixed[13], fixed[14], fixed[15]);
    let dst_ip = Ipv4Addr::new(fixed[16], fixed[17], fixed[18], fixed[19]);
    Some(ParsedDatagram {
        src_mac: MacAddr(eth[6..12].try_into().expect("6 bytes")),
        src: SocketAddrV4::new(src_ip, u16::from_be_bytes([udp[0], udp[1]])),
        dst: SocketAddrV4::new(dst_ip, u16::from_be_bytes([udp[2], udp[3]])),
        ecn: fixed[1] & 0b11,
        payload,
    })
}

/// Returns the unfolded one's complement sum of the UDP pseudo header
fn pseudo_header_sum(src: Ipv4Addr, dst: Ipv4Addr, udp_len: u16) -> u64 {
    let sum = checksum_add(0, &src.octets());
    let sum = checksum_add(sum, &dst.octets());
    sum + u64::from(IPPROTO_UDP) + u64::from(udp_len)
}

/// Adds `data`, as big-endian 16-bit words, to an unfolded one's complement sum
///
/// Sums 32-bit words into a 64-bit accumulator, which gives the same folded result as 16-bit
/// words with fewer additions. An odd trailing byte is padded with zero, so only the last
/// chunk of a checksummed range may have odd length.
fn checksum_add(mut sum: u64, data: &[u8]) -> u64 {
    let (words, rest) = data.as_chunks::<4>();
    for word in words {
        sum += u64::from(u32::from_be_bytes(*word));
    }
    for half in rest.chunks(2) {
        let hi = half[0];
        let lo = half.get(1).copied().unwrap_or(0);
        sum += u64::from(u16::from_be_bytes([hi, lo]));
    }
    sum
}

/// Folds an unfolded one's complement sum to 16 bits and complements it
fn finish_checksum(mut sum: u64) -> u16 {
    while sum >> 16 != 0 {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    !(sum as u16)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The textbook checksum over 16-bit words, to check the optimized one against
    fn naive_checksum(data: &[u8]) -> u16 {
        let mut sum = 0u32;
        for pair in data.chunks(2) {
            let word = u16::from_be_bytes([pair[0], pair.get(1).copied().unwrap_or(0)]);
            sum += u32::from(word);
        }
        while sum >> 16 != 0 {
            sum = (sum & 0xffff) + (sum >> 16);
        }
        !(sum as u16)
    }

    fn header() -> FrameHeader {
        FrameHeader {
            src_mac: MacAddr([0x02, 0, 0, 0, 0, 1]),
            dst_mac: MacAddr([0x02, 0, 0, 0, 0, 2]),
            src: "10.0.0.1:4433".parse().unwrap(),
            dst: "10.0.0.2:50000".parse().unwrap(),
            ecn: 0b10,
        }
    }

    fn build(payload: &[u8]) -> Vec<u8> {
        let mut frame = vec![0; HEADERS_LEN + payload.len()];
        frame[HEADERS_LEN..].copy_from_slice(payload);
        let len = write_headers(&mut frame, &header(), payload.len());
        assert_eq!(len, frame.len());
        frame
    }

    #[test]
    fn checksum_matches_naive_for_all_lengths() {
        let data: Vec<u8> = (0..300u32).map(|i| (i * 7 + 3) as u8).collect();
        for len in 0..data.len() {
            let data = &data[..len];
            assert_eq!(
                finish_checksum(checksum_add(0, data)),
                naive_checksum(data),
                "len {len}"
            );
        }
    }

    #[test]
    fn ipv4_header_checksum_matches_known_vector() {
        // The example header from https://en.wikipedia.org/wiki/IPv4_header_checksum
        let header = [
            0x45, 0x00, 0x00, 0x73, 0x00, 0x00, 0x40, 0x00, 0x40, 0x11, 0x00, 0x00, 0xc0, 0xa8,
            0x00, 0x01, 0xc0, 0xa8, 0x00, 0xc7,
        ];
        assert_eq!(finish_checksum(checksum_add(0, &header)), 0xb861);
    }

    #[test]
    fn written_checksums_verify() {
        for payload_len in [0, 1, 2, 3, 1200, 1201] {
            let payload: Vec<u8> = (0..payload_len).map(|i| i as u8).collect();
            let frame = build(&payload);
            let ip = &frame[ETH_HDR_LEN..ETH_HDR_LEN + IPV4_HDR_LEN];
            assert_eq!(finish_checksum(checksum_add(0, ip)), 0, "IPv4 header");
            let udp = &frame[ETH_HDR_LEN + IPV4_HDR_LEN..];
            let pseudo = pseudo_header_sum(
                "10.0.0.1".parse().unwrap(),
                "10.0.0.2".parse().unwrap(),
                udp.len() as u16,
            );
            assert_eq!(finish_checksum(checksum_add(pseudo, udp)), 0, "UDP");
        }
    }

    #[test]
    fn parse_returns_what_write_headers_wrote() {
        let payload = b"hello quic";
        let frame = build(payload);
        let parsed = parse_frame(&frame).unwrap();
        let header = header();
        assert_eq!(parsed.src_mac, header.src_mac);
        assert_eq!(parsed.src, header.src);
        assert_eq!(parsed.dst, header.dst);
        assert_eq!(parsed.ecn, 0b10);
        assert_eq!(parsed.payload, payload);
    }

    #[test]
    fn parse_ignores_ethernet_padding() {
        let mut frame = build(b"x");
        frame.resize(60, 0);
        assert_eq!(parse_frame(&frame).unwrap().payload, b"x");
    }

    #[test]
    fn parse_skips_ip_options() {
        let frame = build(b"payload");
        // Insert one 4-byte option word (NOPs) after the fixed IPv4 header.
        let mut with_options = frame[..ETH_HDR_LEN + IPV4_HDR_LEN].to_vec();
        with_options.extend_from_slice(&[1, 1, 1, 1]);
        with_options.extend_from_slice(&frame[ETH_HDR_LEN + IPV4_HDR_LEN..]);
        with_options[ETH_HDR_LEN] = 0x46;
        let total = u16::from_be_bytes([with_options[16], with_options[17]]) + 4;
        with_options[16..18].copy_from_slice(&total.to_be_bytes());
        assert_eq!(parse_frame(&with_options).unwrap().payload, b"payload");
    }

    #[test]
    fn parse_rejects_non_udp_fragments_and_truncation() {
        let frame = build(b"payload");

        let mut arp = frame.clone();
        arp[12..14].copy_from_slice(&0x0806u16.to_be_bytes());
        assert!(parse_frame(&arp).is_none());

        let mut tcp = frame.clone();
        tcp[ETH_HDR_LEN + 9] = 6;
        assert!(parse_frame(&tcp).is_none());

        let mut fragment = frame.clone();
        fragment[ETH_HDR_LEN + 6] = 0x20; // more fragments
        assert!(parse_frame(&fragment).is_none());

        assert!(parse_frame(&frame[..frame.len() - 1]).is_none());
        assert!(parse_frame(&frame[..HEADERS_LEN - 1]).is_none());
    }

    #[test]
    fn mac_round_trips_through_display_and_parse() {
        let mac: MacAddr = "02:0a:ff:00:10:01".parse().unwrap();
        assert_eq!(mac, MacAddr([0x02, 0x0a, 0xff, 0x00, 0x10, 0x01]));
        assert_eq!(mac.to_string(), "02:0a:ff:00:10:01");
        assert!("02:0a:ff:00:10".parse::<MacAddr>().is_err());
        assert!("02:0a:ff:00:10:01:02".parse::<MacAddr>().is_err());
        assert!("zz:0a:ff:00:10:01".parse::<MacAddr>().is_err());
    }
}
