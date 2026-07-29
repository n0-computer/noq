use std::{
    ffi::{c_int, c_uchar},
    mem::{self, MaybeUninit},
    ptr,
};

use windows_sys::Win32::Networking::WinSock;

use super::{CMsgHdr, Encoder, MsgHdr};

/// Every payload we put into, or read out of, a control message on this platform.
///
/// A payload slot holds any one of these, so the largest of them sizes a message.
///
/// <https://learn.microsoft.com/en-us/windows/win32/api/ws2ipdef/ns-ws2ipdef-in_pktinfo>
/// <https://learn.microsoft.com/en-us/windows/win32/api/ws2ipdef/ns-ws2ipdef-in6_pktinfo>
#[derive(Copy, Clone)]
#[repr(C)]
#[allow(dead_code)] // the fields are here for their size, nothing reads them
pub(crate) union Payload {
    ecn: c_int,
    segment_size: u32,
    pktinfo_v4: WinSock::IN_PKTINFO,
    pktinfo_v6: WinSock::IN6_PKTINFO,
}

/// The alignment a control message payload is guaranteed to have.
///
/// `WSA_CMSG_DATA` rounds the header size up to it and `WSA_CMSG_SPACE` keeps every
/// following header at a multiple of it, [`ControlBuf`] having at least as much.
pub(crate) const PAYLOAD_ALIGN: usize = mem::align_of::<usize>();

/// Set in `dwFlags` when control messages did not fit in the buffer.
pub(crate) const MSG_CTRUNC: c_int = WinSock::MSG_CTRUNC as c_int;

// The four functions below follow the C macros in
// <https://github.com/microsoft/win32metadata/blob/main/generation/WinSDK/RecompiledIdlHeaders/shared/ws2def.h#L741>

/// `WSA_CMSG_ALIGN`, which control message headers are aligned to.
const fn cmsghdr_align(len: usize) -> usize {
    (len + mem::align_of::<WinSock::CMSGHDR>() - 1) & !(mem::align_of::<WinSock::CMSGHDR>() - 1)
}

/// `WSA_CMSGDATA_ALIGN`, which control message payloads are aligned to.
const fn cmsgdata_align(len: usize) -> usize {
    (len + PAYLOAD_ALIGN - 1) & !(PAYLOAD_ALIGN - 1)
}

/// `WSA_CMSG_LEN`, the value of `cmsg_len` for a payload of `payload_len` bytes.
const fn cmsg_len(payload_len: usize) -> usize {
    cmsgdata_align(mem::size_of::<WinSock::CMSGHDR>()) + payload_len
}

/// `WSA_CMSG_SPACE`, the buffer space one control message with this payload takes up.
const fn cmsg_space(payload_len: usize) -> usize {
    cmsgdata_align(mem::size_of::<WinSock::CMSGHDR>() + cmsghdr_align(payload_len))
}

/// Space for one control message carrying any of our payloads.
const MESSAGE_LEN: usize = cmsg_space(mem::size_of::<Payload>());

/// Space for the control messages one `WSASendMsg` can carry.
///
/// The ECN codepoint, the source address and the segment size, one each: the IPv4 and
/// IPv6 forms are mutually exclusive.
pub(crate) const SEND_LEN: usize = 3 * MESSAGE_LEN;

/// Space for the control messages `WSARecvMsg` can return for one datagram.
///
/// The ECN codepoint, the packet info and the URO coalesced size, one each.
pub(crate) const RECV_LEN: usize = 3 * MESSAGE_LEN;

/// A control message buffer of `N` bytes.
#[derive(Copy, Clone)]
#[repr(C)]
pub(crate) struct ControlBuf<const N: usize> {
    /// Aligns the buffer like the `usize` `WSA_CMSGDATA_ALIGN` rounds to, which covers
    /// the headers too: `CMSGHDR` is a `SIZE_T` and two `INT`s.
    /// Zero sized: `repr(align)` takes a literal, not an expression.
    _align: [usize; 0],
    bytes: [MaybeUninit<u8>; N],
}

/// Control message buffer for one `WSASendMsg`.
pub(crate) type SendBuf = ControlBuf<SEND_LEN>;

/// Control message buffer for one `WSARecvMsg`.
pub(crate) type RecvBuf = ControlBuf<RECV_LEN>;

impl<const N: usize> ControlBuf<N> {
    /// A zeroed buffer.
    pub(crate) const fn zeroed() -> Self {
        Self {
            _align: [],
            bytes: [MaybeUninit::new(0); N],
        }
    }

    pub(crate) fn as_mut_ptr(&mut self) -> *mut u8 {
        self.bytes.as_mut_ptr().cast()
    }

    /// The size of the buffer, for `Control.len`.
    pub(crate) const fn len(&self) -> usize {
        N
    }
}

/// The control messages we send.
///
/// One method each rather than a generic `push`, keeping the set next to the [`SEND_LEN`]
/// covering it.
impl<M: MsgHdr<ControlMessage = WinSock::CMSGHDR>> Encoder<'_, M> {
    /// Sets the ECN codepoint of an IPv4 datagram.
    pub(crate) fn push_ecn_v4(&mut self, ecn: c_int) {
        self.push(WinSock::IPPROTO_IP, WinSock::IP_ECN, ecn);
    }

    /// Sets the ECN codepoint of an IPv6 datagram.
    pub(crate) fn push_ecn_v6(&mut self, ecn: c_int) {
        self.push(WinSock::IPPROTO_IPV6, WinSock::IPV6_ECN, ecn);
    }

    /// Sets the segment size the stack splits an oversized datagram into.
    ///
    /// <https://learn.microsoft.com/en-us/windows/win32/api/ws2tcpip/nf-ws2tcpip-wsasetudpsendmessagesize>
    pub(crate) fn push_segment_size(&mut self, segment_size: u32) {
        self.push(
            WinSock::IPPROTO_UDP,
            WinSock::UDP_SEND_MSG_SIZE,
            segment_size,
        );
    }

    /// Sets the source address of an IPv4 datagram.
    pub(crate) fn push_pktinfo_v4(&mut self, pktinfo: WinSock::IN_PKTINFO) {
        self.push(WinSock::IPPROTO_IP, WinSock::IP_PKTINFO, pktinfo);
    }

    /// Sets the source address of an IPv6 datagram.
    pub(crate) fn push_pktinfo_v6(&mut self, pktinfo: WinSock::IN6_PKTINFO) {
        self.push(WinSock::IPPROTO_IPV6, WinSock::IPV6_PKTINFO, pktinfo);
    }
}

/// Helpers for [`WinSock::WSAMSG`]
///
/// <https://learn.microsoft.com/en-us/windows/win32/api/ws2def/ns-ws2def-wsamsg>
/// <https://microsoft.github.io/windows-docs-rs/doc/windows/Win32/Networking/WinSock/struct.WSAMSG.html>
impl MsgHdr for WinSock::WSAMSG {
    type ControlMessage = WinSock::CMSGHDR;

    fn cmsg_first_hdr(&self) -> *mut Self::ControlMessage {
        if self.Control.len as usize >= mem::size_of::<WinSock::CMSGHDR>() {
            self.Control.buf as *mut WinSock::CMSGHDR
        } else {
            ptr::null_mut::<WinSock::CMSGHDR>()
        }
    }

    fn cmsg_nxt_hdr(&self, cmsg: &Self::ControlMessage) -> *mut Self::ControlMessage {
        let next =
            (cmsg as *const _ as usize + cmsghdr_align(cmsg.cmsg_len)) as *mut WinSock::CMSGHDR;
        let max = self.Control.buf as usize + self.Control.len as usize;
        if unsafe { next.offset(1) } as usize > max {
            ptr::null_mut()
        } else {
            next
        }
    }

    fn set_control_len(&mut self, len: usize) {
        self.Control.len = len as _;
    }

    fn control_len(&self) -> usize {
        self.Control.len as _
    }

    fn recv_flags(&self) -> c_int {
        self.dwFlags as _
    }
}

/// Helpers for [`WinSock::CMSGHDR`]
///
/// <https://learn.microsoft.com/en-us/windows/win32/api/ws2def/ns-ws2def-wsacmsghdr>
/// <https://microsoft.github.io/windows-docs-rs/doc/windows/Win32/Networking/WinSock/struct.CMSGHDR.html>
impl CMsgHdr for WinSock::CMSGHDR {
    fn cmsg_len(length: usize) -> usize {
        cmsg_len(length)
    }

    fn cmsg_space(length: usize) -> usize {
        cmsg_space(length)
    }

    fn cmsg_data(&self) -> *mut c_uchar {
        (self as *const _ as usize + cmsgdata_align(mem::size_of::<Self>())) as *mut c_uchar
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
    use super::*;

    /// Every payload in a full buffer is aligned for the type read out of it.
    ///
    /// Encoding the whole send set also proves [`SEND_LEN`] covers it, `push` panicking
    /// rather than overrunning.
    #[test]
    fn payloads_are_aligned() {
        let mut buf = SendBuf::zeroed();
        let mut msg: WinSock::WSAMSG = unsafe { mem::zeroed() };
        msg.Control = WinSock::WSABUF {
            buf: buf.as_mut_ptr(),
            len: buf.len() as _,
        };

        let mut encoder = unsafe { Encoder::new(&mut msg) };
        encoder.push_pktinfo_v6(unsafe { mem::zeroed() });
        encoder.push_ecn_v6(0);
        encoder.push_segment_size(1200);
        encoder.finish();

        let mut count = 0;
        for cmsg in unsafe { super::super::Iter::new(&msg) } {
            assert_eq!(
                cmsg.cmsg_data() as usize % PAYLOAD_ALIGN,
                0,
                "payload {count} is not aligned to {PAYLOAD_ALIGN}",
            );
            count += 1;
        }
        assert_eq!(count, 3);
    }
}
