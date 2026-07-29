use std::{
    ffi::{c_int, c_uchar},
    mem::MaybeUninit,
};

use super::{CMsgHdr, Encoder, MsgHdr};
// netbsd sends no IP_TOS control message, so it has no payload type for one.
#[cfg(not(target_os = "netbsd"))]
use crate::imp::IpTosTy;

/// Every payload we put into, or read out of, a control message on this platform.
///
/// A payload slot holds any one of these, so the largest of them sizes a message.
#[derive(Copy, Clone)]
#[repr(C)]
#[allow(dead_code)] // the fields are here for their size, nothing reads them
pub(crate) union Payload {
    hdr: libc::cmsghdr,
    #[cfg(not(target_os = "netbsd"))]
    ecn_v4: IpTosTy,
    ecn_v6: c_int,
    /// `IP_TOS` and, on Darwin, `IPV6_TCLASS` come back as a single byte.
    ecn_byte: u8,
    segment_size: u16,
    #[cfg(not(target_os = "redox"))]
    pktinfo_v6: libc::in6_pktinfo,
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pktinfo_v4: libc::in_pktinfo,
    #[cfg(any(bsd, apple, solarish))]
    dst_addr_v4: libc::in_addr,
    #[cfg(any(target_os = "linux", target_os = "android"))]
    timestamp: libc::timespec,
}

/// Set in `msg_flags` when control messages did not fit in the buffer.
pub(crate) const MSG_CTRUNC: c_int = libc::MSG_CTRUNC;

/// The buffer space one control message with a payload of this size takes up.
// https://man7.org/linux/man-pages/man3/cmsg.3.html
const fn cmsg_space(payload_len: usize) -> usize {
    unsafe { libc::CMSG_SPACE(payload_len as _) as usize }
}

/// The weaker of two alignments, i.e. the largest power of two dividing both.
const fn common_align(a: usize, b: usize) -> usize {
    // The lower of the two lowest set bits decides the trailing zeros of the OR.
    1 << (a | b).trailing_zeros()
}

/// The alignment a control message payload is guaranteed to have.
///
/// Payloads sit `CMSG_LEN(0)` into their message and messages a sum of `CMSG_SPACE`s into
/// the buffer, so it is what those offsets and [`ControlBuf`]'s alignment share.
pub(crate) const PAYLOAD_ALIGN: usize = common_align(
    common_align(unsafe { libc::CMSG_LEN(0) } as usize, cmsg_space(1)),
    align_of::<ControlBuf<0>>(),
);

/// Space for one control message carrying any of our payloads.
const MESSAGE_LEN: usize = cmsg_space(size_of::<Payload>());

/// Space for the control messages one `sendmsg` can carry.
///
/// ECN, GSO segment size and source address, one each; the v4 and v6 forms are exclusive.
pub(crate) const SEND_LEN: usize = 3 * MESSAGE_LEN;

/// Space for the control messages the kernel can attach to one received datagram.
///
/// TOS or traffic class, packet info, GRO segment size and receive timestamp, one each,
/// matching the options `UdpSocketState::new` enables.
pub(crate) const RECV_LEN: usize = 4 * MESSAGE_LEN;

/// A control message buffer of `N` bytes.
#[derive(Copy, Clone)]
#[repr(C)]
pub(crate) struct ControlBuf<const N: usize> {
    /// Aligns the buffer like the `size_t` the `CMSG_*` macros round offsets to.
    /// Zero sized: `repr(align)` takes a literal, not an expression.
    _align: [usize; 0],
    bytes: [MaybeUninit<u8>; N],
}

/// Control message buffer for one `sendmsg`.
pub(crate) type SendBuf = ControlBuf<SEND_LEN>;

/// Control message buffer for one `recvmsg`.
pub(crate) type RecvBuf = ControlBuf<RECV_LEN>;

impl<const N: usize> ControlBuf<N> {
    /// A zeroed buffer, for sending.
    pub(crate) const fn zeroed() -> Self {
        Self {
            _align: [],
            bytes: [MaybeUninit::new(0); N],
        }
    }

    /// An uninitialised buffer, for receiving: the kernel initialises what it uses.
    pub(crate) const fn uninit() -> Self {
        Self {
            _align: [],
            bytes: [MaybeUninit::uninit(); N],
        }
    }

    pub(crate) fn as_mut_ptr(&mut self) -> *mut u8 {
        self.bytes.as_mut_ptr().cast()
    }

    /// The size of the buffer, for `msg_controllen`.
    pub(crate) const fn len(&self) -> usize {
        N
    }
}

/// The control messages we send.
///
/// One method each rather than a generic `push`, keeping the set next to the [`SEND_LEN`]
/// covering it.
impl<M: MsgHdr<ControlMessage = libc::cmsghdr>> Encoder<'_, M> {
    /// Sets the ECN codepoint of an IPv4 or IPv4-mapped datagram.
    #[cfg(not(target_os = "netbsd"))]
    pub(crate) fn push_ecn_v4(&mut self, ecn: IpTosTy) {
        self.push(libc::IPPROTO_IP, libc::IP_TOS, ecn);
    }

    /// Sets the IPv6 traffic class, which carries the ECN codepoint.
    #[cfg(not(target_os = "redox"))]
    pub(crate) fn push_ecn_v6(&mut self, ecn: c_int) {
        self.push(libc::IPPROTO_IPV6, libc::IPV6_TCLASS, ecn);
    }

    /// Sets the GSO segment size the kernel splits an oversized datagram into.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub(crate) fn push_segment_size(&mut self, segment_size: u16) {
        self.push(libc::SOL_UDP, libc::UDP_SEGMENT, segment_size);
    }

    /// Sets the source address of an IPv4 datagram.
    #[cfg(any(target_os = "linux", target_os = "android"))]
    pub(crate) fn push_pktinfo_v4(&mut self, pktinfo: libc::in_pktinfo) {
        self.push(libc::IPPROTO_IP, libc::IP_PKTINFO, pktinfo);
    }

    /// Sets the source address of an IPv4 datagram.
    ///
    /// `IP_RECVDSTADDR` is `IP_SENDSRCADDR` on FreeBSD, the two have the same value.
    #[cfg(any(bsd, apple, solarish))]
    pub(crate) fn push_src_addr_v4(&mut self, addr: libc::in_addr) {
        self.push(libc::IPPROTO_IP, libc::IP_RECVDSTADDR, addr);
    }

    /// Sets the source address of an IPv6 datagram.
    #[cfg(not(target_os = "redox"))]
    pub(crate) fn push_pktinfo_v6(&mut self, pktinfo: libc::in6_pktinfo) {
        self.push(libc::IPPROTO_IPV6, libc::IPV6_PKTINFO, pktinfo);
    }
}

/// Helpers for [`libc::msghdr`]
impl MsgHdr for libc::msghdr {
    type ControlMessage = libc::cmsghdr;

    fn cmsg_first_hdr(&self) -> *mut Self::ControlMessage {
        unsafe { libc::CMSG_FIRSTHDR(self) }
    }

    fn cmsg_nxt_hdr(&self, cmsg: &Self::ControlMessage) -> *mut Self::ControlMessage {
        unsafe { libc::CMSG_NXTHDR(self, cmsg) }
    }

    fn set_control_len(&mut self, len: usize) {
        self.msg_controllen = len as _;
        if len == 0 {
            // netbsd is particular about this being a NULL pointer if there are no control
            // messages.
            self.msg_control = std::ptr::null_mut();
        }
    }

    fn control_len(&self) -> usize {
        self.msg_controllen as _
    }

    fn recv_flags(&self) -> c_int {
        self.msg_flags
    }
}

/// Helpers for [`libc::cmsghdr`]
impl CMsgHdr for libc::cmsghdr {
    fn cmsg_len(length: usize) -> usize {
        unsafe { libc::CMSG_LEN(length as _) as usize }
    }

    fn cmsg_space(length: usize) -> usize {
        unsafe { libc::CMSG_SPACE(length as _) as usize }
    }

    fn cmsg_data(&self) -> *mut c_uchar {
        unsafe { libc::CMSG_DATA(self) }
    }

    fn set(&mut self, level: c_int, ty: c_int, len: usize) {
        self.cmsg_level = level as _;
        self.cmsg_type = ty as _;
        self.cmsg_len = len as _;
    }

    fn len(&self) -> usize {
        self.cmsg_len as _
    }
}

#[cfg(test)]
mod tests {
    use std::mem;

    use super::*;

    /// The payload of every control message we can send in one `sendmsg`.
    ///
    /// `IpTosTy` is `c_int` or smaller everywhere it exists, so `c_int` stands in for it.
    fn sent_payload_lens() -> Vec<usize> {
        vec![
            size_of::<c_int>(), // IP_TOS or IPV6_TCLASS
            size_of::<u16>(),   // UDP_SEGMENT
            // IP_PKTINFO, IP_RECVDSTADDR or IPV6_PKTINFO
            size_of::<Payload>(),
        ]
    }

    /// The payload of every control message the kernel can attach to one datagram.
    fn received_payload_lens() -> Vec<usize> {
        vec![
            size_of::<c_int>(),   // IP_TOS or IPV6_TCLASS
            size_of::<Payload>(), // IP_PKTINFO or IPV6_PKTINFO
            size_of::<c_int>(),   // UDP_GRO
            #[cfg(any(target_os = "linux", target_os = "android"))]
            size_of::<libc::timespec>(), // SCM_TIMESTAMPNS
        ]
    }

    fn libc_cmsg_space(payload_lens: &[usize]) -> usize {
        payload_lens
            .iter()
            .map(|len| unsafe { libc::CMSG_SPACE(*len as _) as usize })
            .sum()
    }

    /// The buffers hold every control message they have to.
    ///
    /// The constants count messages and assume the largest payload; this adds up the real
    /// ones, so a message we failed to count shows up here, not as a truncated datagram.
    #[test]
    fn control_len_covers_libc() {
        let sent = libc_cmsg_space(&sent_payload_lens());
        assert!(SEND_LEN >= sent, "SEND_LEN is {SEND_LEN}, need {sent}");

        let received = libc_cmsg_space(&received_payload_lens());
        assert!(
            RECV_LEN >= received,
            "RECV_LEN is {RECV_LEN}, need {received}"
        );
    }

    /// Every payload in a full buffer is aligned for the type read out of it.
    ///
    /// What `cmsg::decode` relies on. musl aligns `cmsghdr` to 4 where glibc aligns it to
    /// 8, so aligning the buffer for it rather than for the macros breaks there.
    // https://github.com/kraj/musl/blob/master/include/sys/socket.h#L44
    // https://github.com/bminor/glibc/blob/master/sysdeps/unix/sysv/linux/bits/socket.h#L283
    #[test]
    fn payloads_are_aligned() {
        let mut buf = RecvBuf::zeroed();
        let mut hdr: libc::msghdr = unsafe { mem::zeroed() };
        hdr.msg_control = buf.as_mut_ptr().cast();
        hdr.msg_controllen = buf.len() as _;

        // The largest payload we use, so the messages after the first sit where a real
        // receive would put them.
        let mut encoder = unsafe { Encoder::new(&mut hdr) };
        for _ in 0..received_payload_lens().len() {
            encoder.push(libc::SOL_SOCKET, 0, Payload { ecn_v6: 0 });
        }
        encoder.finish();

        let mut count = 0;
        for cmsg in unsafe { super::super::Iter::new(&hdr) } {
            assert_eq!(
                cmsg.cmsg_data() as usize % PAYLOAD_ALIGN,
                0,
                "payload {count} is not aligned to {PAYLOAD_ALIGN}",
            );
            count += 1;
        }
        assert_eq!(count, received_payload_lens().len());
    }
}
