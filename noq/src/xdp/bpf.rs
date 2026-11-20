//! The XDP program that steers QUIC traffic into AF_XDP sockets.
//!
//! The program is a few dozen hand-assembled eBPF instructions, loaded with the `bpf(2)`
//! syscall and attached through a BPF link, so neither libbpf nor a BPF compiler is needed.
//! Closing the link detaches the program, which also happens when the process dies.

use std::{
    ffi::{CStr, c_void},
    io,
    mem::size_of,
    net::SocketAddrV4,
    os::fd::{AsRawFd, BorrowedFd, FromRawFd, OwnedFd},
};

use super::{AttachMode, context};

const BPF_MAP_CREATE: libc::c_int = 0;
const BPF_MAP_UPDATE_ELEM: libc::c_int = 2;
const BPF_PROG_LOAD: libc::c_int = 5;
const BPF_LINK_CREATE: libc::c_int = 28;

const BPF_MAP_TYPE_XSKMAP: u32 = 17;
const BPF_PROG_TYPE_XDP: u32 = 6;
const BPF_XDP: u32 = 37;

const XDP_FLAGS_SKB_MODE: u32 = 1 << 1;
const XDP_FLAGS_DRV_MODE: u32 = 1 << 2;

const XDP_PASS: i32 = 2;
const BPF_FUNC_REDIRECT_MAP: i32 = 51;
/// `src_reg` of a 64-bit immediate load whose immediate is a map file descriptor
const BPF_PSEUDO_MAP_FD: u8 = 1;

/// The program license, which must be GPL compatible for the kernel to accept it
const LICENSE: &CStr = c"Dual MIT/GPL";

/// An XDP program attached to an interface, with the map of sockets it redirects to
#[derive(Debug)]
pub(super) struct XdpProgram {
    /// Detaches the program when closed, so it is dropped first
    _link: OwnedFd,
    _program: OwnedFd,
    xsk_map: OwnedFd,
}

impl XdpProgram {
    /// Loads the program for `local_addr` and attaches it to the interface `ifindex`
    ///
    /// `queues` is the number of entries of the socket map, one per receive queue.
    pub(super) fn attach(
        ifindex: u32,
        local_addr: SocketAddrV4,
        queues: u32,
        mode: AttachMode,
    ) -> io::Result<Self> {
        let xsk_map = create_xsk_map(queues)?;
        let insns = program(local_addr, xsk_map.as_raw_fd());
        let program = load_program(&insns)?;
        let link = attach_program(program.as_raw_fd(), ifindex, mode)?;
        Ok(Self {
            _link: link,
            _program: program,
            xsk_map,
        })
    }

    /// Makes the program redirect matching packets from `queue` to the AF_XDP socket `xsk`
    ///
    /// The kernel removes the entry when the socket is closed.
    pub(super) fn register(&self, queue: u32, xsk: BorrowedFd<'_>) -> io::Result<()> {
        let value = xsk.as_raw_fd() as u32;
        let mut attr = MapUpdateAttr {
            map_fd: self.xsk_map.as_raw_fd() as u32,
            _pad: 0,
            key: &queue as *const u32 as u64,
            value: &value as *const u32 as u64,
            flags: 0,
        };
        bpf(BPF_MAP_UPDATE_ELEM, &mut attr).map_err(|e| context(e, "updating XSKMAP"))?;
        Ok(())
    }
}

/// `union bpf_attr` for `BPF_MAP_CREATE`, up to `map_flags`
#[repr(C)]
struct MapCreateAttr {
    map_type: u32,
    key_size: u32,
    value_size: u32,
    max_entries: u32,
    map_flags: u32,
}

/// `union bpf_attr` for `BPF_MAP_UPDATE_ELEM`
#[repr(C)]
struct MapUpdateAttr {
    map_fd: u32,
    _pad: u32,
    key: u64,
    value: u64,
    flags: u64,
}

/// `union bpf_attr` for `BPF_PROG_LOAD`, up to `expected_attach_type`
#[repr(C)]
struct ProgLoadAttr {
    prog_type: u32,
    insn_cnt: u32,
    insns: u64,
    license: u64,
    log_level: u32,
    log_size: u32,
    log_buf: u64,
    kern_version: u32,
    prog_flags: u32,
    prog_name: [u8; 16],
    prog_ifindex: u32,
    expected_attach_type: u32,
}

/// `union bpf_attr` for `BPF_LINK_CREATE`, up to `flags`
#[repr(C)]
struct LinkCreateAttr {
    prog_fd: u32,
    target_ifindex: u32,
    attach_type: u32,
    flags: u32,
}

/// Calls `bpf(2)` with the attributes in `attr` and returns the result as a file descriptor
fn bpf<T>(cmd: libc::c_int, attr: &mut T) -> io::Result<libc::c_long> {
    // SAFETY: `attr` is a `repr(C)` prefix of `union bpf_attr` for `cmd`, valid for
    // `size_of::<T>()` bytes; the kernel treats the bytes past that as zero. Pointers inside
    // `attr` point to memory the caller keeps alive for the duration of the call.
    let ret = unsafe {
        libc::syscall(
            libc::SYS_bpf,
            cmd,
            attr as *mut T as *mut c_void,
            size_of::<T>() as libc::c_uint,
        )
    };
    match ret {
        ..0 => Err(io::Error::last_os_error()),
        fd => Ok(fd),
    }
}

/// Like [`bpf`], for commands that return a new file descriptor
fn bpf_fd<T>(cmd: libc::c_int, attr: &mut T) -> io::Result<OwnedFd> {
    let fd = bpf(cmd, attr)?;
    // SAFETY: on success these commands return a new file descriptor that nothing else owns.
    Ok(unsafe { OwnedFd::from_raw_fd(fd as libc::c_int) })
}

fn create_xsk_map(queues: u32) -> io::Result<OwnedFd> {
    let mut attr = MapCreateAttr {
        map_type: BPF_MAP_TYPE_XSKMAP,
        key_size: 4,
        value_size: 4,
        max_entries: queues.max(1),
        map_flags: 0,
    };
    bpf_fd(BPF_MAP_CREATE, &mut attr).map_err(|e| context(e, "creating XSKMAP"))
}

fn load_program(insns: &[Insn]) -> io::Result<OwnedFd> {
    let mut attr = ProgLoadAttr {
        prog_type: BPF_PROG_TYPE_XDP,
        insn_cnt: insns.len() as u32,
        insns: insns.as_ptr() as u64,
        license: LICENSE.as_ptr() as u64,
        log_level: 0,
        log_size: 0,
        log_buf: 0,
        kern_version: 0,
        prog_flags: 0,
        prog_name: *b"noq_xdp\0\0\0\0\0\0\0\0\0",
        prog_ifindex: 0,
        expected_attach_type: BPF_XDP,
    };
    match bpf_fd(BPF_PROG_LOAD, &mut attr) {
        Ok(fd) => Ok(fd),
        Err(e) if e.raw_os_error() == Some(libc::EPERM) => Err(context(
            e,
            "loading XDP program (needs CAP_BPF and CAP_NET_ADMIN, or root)",
        )),
        Err(e) => {
            // Load again with the verifier log, so that the error says what the verifier
            // rejected.
            let mut log = vec![0u8; 64 * 1024];
            attr.log_level = 1;
            attr.log_size = log.len() as u32;
            attr.log_buf = log.as_mut_ptr() as u64;
            let _ = bpf_fd(BPF_PROG_LOAD, &mut attr);
            let log = CStr::from_bytes_until_nul(&log)
                .map(|log| log.to_string_lossy().into_owned())
                .unwrap_or_default();
            Err(context(
                e,
                &format!("loading XDP program, verifier log:\n{log}"),
            ))
        }
    }
}

fn attach_program(program: libc::c_int, ifindex: u32, mode: AttachMode) -> io::Result<OwnedFd> {
    let flags = match mode {
        AttachMode::Auto => 0,
        AttachMode::Generic => XDP_FLAGS_SKB_MODE,
        AttachMode::Native => XDP_FLAGS_DRV_MODE,
    };
    let mut attr = LinkCreateAttr {
        prog_fd: program as u32,
        target_ifindex: ifindex,
        attach_type: BPF_XDP,
        flags,
    };
    bpf_fd(BPF_LINK_CREATE, &mut attr).map_err(|e| {
        let hint = match e.raw_os_error() {
            Some(libc::EBUSY) | Some(libc::EEXIST) => {
                " (another XDP program is attached, see `ip link show`)"
            }
            Some(libc::EOPNOTSUPP) => " (the driver does not support this XDP mode)",
            _ => "",
        };
        context(e, &format!("attaching XDP program{hint}"))
    })
}

/// One eBPF instruction, `struct bpf_insn`
#[repr(C)]
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Insn {
    code: u8,
    /// The `dst_reg:4, src_reg:4` bitfield: destination register in the low nibble on
    /// little-endian hosts, in the high nibble on big-endian hosts
    regs: u8,
    off: i16,
    imm: i32,
}

impl Insn {
    const fn new(code: u8, dst: u8, src: u8, off: i16, imm: i32) -> Self {
        // C bitfields are laid out from the least significant bit on little-endian targets and
        // from the most significant bit on big-endian ones.
        let regs = match cfg!(target_endian = "little") {
            true => (src << 4) | dst,
            false => (dst << 4) | src,
        };
        Self {
            code,
            regs,
            off,
            imm,
        }
    }
}

// Opcodes, see https://docs.kernel.org/bpf/standardization/instruction-set.html
const LDX_W: u8 = 0x61;
const LDX_H: u8 = 0x69;
const LDX_B: u8 = 0x71;
const MOV64_IMM: u8 = 0xb7;
const MOV64_REG: u8 = 0xbf;
const ADD64_IMM: u8 = 0x07;
const AND64_IMM: u8 = 0x57;
const JGT_REG: u8 = 0x2d;
/// `JNE` on the lower 32 bits, so that the immediate is not sign-extended
const JNE32_IMM: u8 = 0x56;
const LD_IMM64: u8 = 0x18;
const CALL: u8 = 0x85;
const EXIT: u8 = 0x95;

/// Jump offset placeholder for "jump to the pass label", patched by [`program`]
const TO_PASS: i16 = i16::MIN;

/// Assembles the XDP program for a socket bound to `local_addr`
///
/// In C, the program is:
///
/// ```c
/// if (data + 42 > data_end) return XDP_PASS;
/// if (eth->h_proto != htons(ETH_P_IP)) return XDP_PASS;
/// if (ip->version_ihl != 0x45 || ip->protocol != IPPROTO_UDP) return XDP_PASS;
/// if (ip->frag_off & htons(0x3fff)) return XDP_PASS;
/// if (udp->dest != htons(port) || ip->daddr != addr) return XDP_PASS;
/// return bpf_redirect_map(&xsk_map, ctx->rx_queue_index, XDP_PASS);
/// ```
///
/// Everything not for `local_addr`, including ARP, reaches the kernel stack as usual. A packet
/// on a queue without a registered socket also goes to the kernel, through the `XDP_PASS`
/// fallback of `bpf_redirect_map`. IPv4 options make the program pass the packet too.
fn program(local_addr: SocketAddrV4, xsk_map: libc::c_int) -> Vec<Insn> {
    // Packet loads interpret bytes in host order, so the immediates are the wire bytes read in
    // host order: byte-swapped on little-endian hosts, unchanged on big-endian ones such as
    // s390x.
    let ethertype = i32::from(u16::from_ne_bytes([0x08, 0x00]));
    let port = i32::from(u16::from_ne_bytes(local_addr.port().to_be_bytes()));
    let addr = i32::from_ne_bytes(local_addr.ip().octets());
    let fragment_mask = i32::from(u16::from_ne_bytes([0x3f, 0xff]));

    let mut insns = vec![
        Insn::new(MOV64_REG, 6, 1, 0, 0),     // r6 = ctx
        Insn::new(LDX_W, 2, 6, 0, 0),         // r2 = ctx->data
        Insn::new(LDX_W, 3, 6, 4, 0),         // r3 = ctx->data_end
        Insn::new(MOV64_REG, 4, 2, 0, 0),     // r4 = r2
        Insn::new(ADD64_IMM, 4, 0, 0, 42),    // r4 += eth + ip + udp
        Insn::new(JGT_REG, 4, 3, TO_PASS, 0), // if r4 > data_end
        Insn::new(LDX_H, 5, 2, 12, 0),        // r5 = eth->h_proto
        Insn::new(JNE32_IMM, 5, 0, TO_PASS, ethertype),
        Insn::new(LDX_B, 5, 2, 14, 0), // r5 = version and IHL
        Insn::new(JNE32_IMM, 5, 0, TO_PASS, 0x45),
        Insn::new(LDX_B, 5, 2, 23, 0), // r5 = ip->protocol
        Insn::new(JNE32_IMM, 5, 0, TO_PASS, 17),
        Insn::new(LDX_H, 5, 2, 20, 0), // r5 = ip->frag_off
        Insn::new(AND64_IMM, 5, 0, 0, fragment_mask),
        Insn::new(JNE32_IMM, 5, 0, TO_PASS, 0),
        Insn::new(LDX_H, 5, 2, 36, 0), // r5 = udp->dest
        Insn::new(JNE32_IMM, 5, 0, TO_PASS, port),
        Insn::new(LDX_W, 5, 2, 30, 0), // r5 = ip->daddr
        Insn::new(JNE32_IMM, 5, 0, TO_PASS, addr),
        Insn::new(LDX_W, 2, 6, 16, 0), // r2 = ctx->rx_queue_index
        Insn::new(LD_IMM64, 1, BPF_PSEUDO_MAP_FD, 0, xsk_map), // r1 = xsk_map
        Insn::new(0, 0, 0, 0, 0),      // upper half of the 64-bit immediate
        Insn::new(MOV64_IMM, 3, 0, 0, XDP_PASS), // r3 = fallback action
        Insn::new(CALL, 0, 0, 0, BPF_FUNC_REDIRECT_MAP),
        Insn::new(EXIT, 0, 0, 0, 0),
    ];
    let pass = insns.len();
    insns.push(Insn::new(MOV64_IMM, 0, 0, 0, XDP_PASS));
    insns.push(Insn::new(EXIT, 0, 0, 0, 0));
    for (i, insn) in insns.iter_mut().enumerate() {
        if insn.off == TO_PASS {
            // Jump offsets count from the next instruction.
            insn.off = (pass - i - 1) as i16;
        }
    }
    insns
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn program_jumps_land_on_pass_label() {
        let insns = program("10.0.0.1:4433".parse().unwrap(), 7);
        let pass = insns.len() - 2;
        assert_eq!(insns[pass], Insn::new(MOV64_IMM, 0, 0, 0, XDP_PASS));
        let jumps: Vec<_> = insns
            .iter()
            .enumerate()
            .filter(|(_, insn)| matches!(insn.code, JGT_REG | JNE32_IMM))
            .collect();
        assert_eq!(jumps.len(), 7);
        for (i, insn) in jumps {
            assert_eq!(
                i as i64 + 1 + i64::from(insn.off),
                pass as i64,
                "jump at {i}"
            );
        }
    }

    #[test]
    fn program_compares_against_wire_order_values() {
        let insns = program("192.168.1.200:4433".parse().unwrap(), 7);
        let imm_of = |offset: i16| {
            let load = insns
                .iter()
                .position(|insn| insn.regs == 0x25 && insn.off == offset)
                .unwrap();
            insns[load + 1].imm
        };
        // The bytes of the immediate in memory order must equal the bytes on the wire.
        assert_eq!(&imm_of(36).to_ne_bytes()[..2], &4433u16.to_be_bytes());
        assert_eq!(imm_of(30).to_ne_bytes(), [192, 168, 1, 200]);
        assert_eq!(&imm_of(12).to_ne_bytes()[..2], &[0x08, 0x00]);
    }

    #[test]
    fn insn_encodes_registers_in_nibbles() {
        let insn = Insn::new(LD_IMM64, 1, BPF_PSEUDO_MAP_FD, 0, 3);
        assert_eq!(insn.regs, 0x11);
        let insn = Insn::new(MOV64_REG, 6, 1, 0, 0);
        let expected = match cfg!(target_endian = "little") {
            true => 0x16,
            false => 0x61,
        };
        assert_eq!(insn.regs, expected);
        assert_eq!(size_of::<Insn>(), 8);
    }
}
