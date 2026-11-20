//! AF_XDP sockets, their UMEM and their four rings.
//!
//! See <https://docs.kernel.org/networking/af_xdp.html> for the kernel interface. Each ring is a
//! single-producer single-consumer queue in memory shared with the kernel. User space produces
//! into the fill and TX rings and consumes from the RX and completion rings.

use std::{
    io,
    marker::PhantomData,
    mem::{MaybeUninit, size_of},
    os::fd::{AsFd, AsRawFd, BorrowedFd, FromRawFd, OwnedFd},
    ptr::NonNull,
    sync::atomic::{AtomicU32, Ordering},
};

use super::{BindMode, context};

/// A memory mapping, unmapped on drop
#[derive(Debug)]
struct Mmap {
    ptr: NonNull<u8>,
    len: usize,
}

// SAFETY: the mapping is plain memory; `Mmap` hands out only raw pointers, and the types
// built on it synchronize access to the bytes they read and write.
unsafe impl Send for Mmap {}
// SAFETY: see `Send`.
unsafe impl Sync for Mmap {}

impl Mmap {
    /// Maps `len` bytes of `fd` at `offset`, or anonymous memory if `fd` is `None`
    fn new(len: usize, fd: Option<BorrowedFd<'_>>, offset: libc::off_t) -> io::Result<Self> {
        let (flags, fd) = match fd {
            Some(fd) => (libc::MAP_SHARED, fd.as_raw_fd()),
            None => (libc::MAP_PRIVATE | libc::MAP_ANONYMOUS, -1),
        };
        // SAFETY: a fresh mapping at an address of the kernel's choice aliases no Rust object.
        let ptr = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                flags | libc::MAP_POPULATE,
                fd,
                offset,
            )
        };
        if ptr == libc::MAP_FAILED {
            return Err(io::Error::last_os_error());
        }
        Ok(Self {
            ptr: NonNull::new(ptr.cast()).expect("mmap does not return null on success"),
            len,
        })
    }
}

impl Drop for Mmap {
    fn drop(&mut self) {
        // SAFETY: `ptr` and `len` describe a mapping this value owns, and nothing else points
        // into it once it is dropped.
        unsafe { libc::munmap(self.ptr.as_ptr().cast(), self.len) };
    }
}

/// One side of a ring shared with the kernel
///
/// `T` is `u64` (a UMEM address) for the fill and completion rings, and [`libc::xdp_desc`]
/// for the RX and TX rings.
#[derive(Debug)]
pub(super) struct Ring<T> {
    /// Keeps the pointers below valid
    _mmap: Mmap,
    producer: NonNull<AtomicU32>,
    consumer: NonNull<AtomicU32>,
    flags: NonNull<AtomicU32>,
    descs: NonNull<T>,
    mask: u32,
    /// Our copy of the index we advance: the producer index for a producer ring
    local: u32,
    /// The last seen index the kernel advances: the consumer index for a producer ring
    remote: u32,
    _marker: PhantomData<T>,
}

// SAFETY: the ring is only used through `&mut self`, and the shared indices are atomics.
unsafe impl<T: Send> Send for Ring<T> {}
// SAFETY: see `Send`; `&Ring` gives no access to the ring memory.
unsafe impl<T: Send> Sync for Ring<T> {}

impl<T: Copy> Ring<T> {
    /// Maps the ring of `fd` at `pgoff` with the offsets `off` and `size` entries
    fn map(
        fd: BorrowedFd<'_>,
        pgoff: libc::off_t,
        off: &libc::xdp_ring_offset,
        size: u32,
        is_producer: bool,
    ) -> io::Result<Self> {
        let len = off.desc as usize + size as usize * size_of::<T>();
        let mmap = Mmap::new(len, Some(fd), pgoff)?;
        let at = |offset: u64| {
            // SAFETY: the kernel guarantees that its offsets lie inside the mapping.
            unsafe { NonNull::new_unchecked(mmap.ptr.as_ptr().add(offset as usize)) }
        };
        let producer = at(off.producer).cast::<AtomicU32>();
        let consumer = at(off.consumer).cast::<AtomicU32>();
        // SAFETY: the indices are aligned `u32`s that the kernel only changes atomically.
        let (prod, cons) = unsafe {
            (
                producer.as_ref().load(Ordering::Acquire),
                consumer.as_ref().load(Ordering::Acquire),
            )
        };
        let (local, remote) = match is_producer {
            true => (prod, cons),
            false => (cons, prod),
        };
        Ok(Self {
            producer,
            consumer,
            flags: at(off.flags).cast(),
            descs: at(off.desc).cast(),
            mask: size - 1,
            local,
            remote,
            _mmap: mmap,
            _marker: PhantomData,
        })
    }

    fn producer(&self) -> &AtomicU32 {
        // SAFETY: points into `self.mmap`, aligned, accessed atomically by both sides.
        unsafe { self.producer.as_ref() }
    }

    fn consumer(&self) -> &AtomicU32 {
        // SAFETY: as in `producer`.
        unsafe { self.consumer.as_ref() }
    }

    /// Whether the kernel asks for a syscall to resume processing this ring
    pub(super) fn needs_wakeup(&self) -> bool {
        // SAFETY: as in `producer`.
        let flags = unsafe { self.flags.as_ref() }.load(Ordering::Relaxed);
        flags & libc::XDP_RING_NEED_WAKEUP != 0
    }

    // Producer side

    /// Returns how many entries can be pushed before the ring is full
    pub(super) fn free(&mut self) -> u32 {
        self.remote = self.consumer().load(Ordering::Acquire);
        self.mask + 1 - self.local.wrapping_sub(self.remote)
    }

    /// Returns how many pushed entries the kernel has not consumed yet
    pub(super) fn pending(&mut self) -> u32 {
        self.mask + 1 - self.free()
    }

    /// Writes `value` into the next free slot; [`Ring::submit`] publishes it
    ///
    /// The caller checks [`Ring::free`] first.
    pub(super) fn push(&mut self, value: T) {
        debug_assert!(self.local.wrapping_sub(self.remote) <= self.mask);
        let index = (self.local & self.mask) as usize;
        // SAFETY: `index` is within the ring, and the slot is ours until we publish it.
        unsafe { self.descs.as_ptr().add(index).write(value) };
        self.local = self.local.wrapping_add(1);
    }

    /// Publishes all pushed entries to the kernel
    pub(super) fn submit(&mut self) {
        self.producer().store(self.local, Ordering::Release);
    }

    // Consumer side

    /// Takes the next entry the kernel produced, if any; [`Ring::release`] returns the slots
    pub(super) fn pop(&mut self) -> Option<T> {
        if self.local == self.remote {
            self.remote = self.producer().load(Ordering::Acquire);
            if self.local == self.remote {
                return None;
            }
        }
        let index = (self.local & self.mask) as usize;
        // SAFETY: `index` is within the ring, and the kernel does not reuse the slot before we
        // release it.
        let value = unsafe { self.descs.as_ptr().add(index).read() };
        self.local = self.local.wrapping_add(1);
        Some(value)
    }

    /// Hands all popped slots back to the kernel
    pub(super) fn release(&mut self) {
        self.consumer().store(self.local, Ordering::Release);
    }
}

/// The packet buffer memory shared with the kernel, split into equal frames
#[derive(Debug)]
pub(super) struct Umem {
    mmap: Mmap,
    frame_size: u32,
}

impl Umem {
    /// Returns the bytes of the frame at UMEM address `addr`, `len` bytes from `addr`
    ///
    /// # Safety
    ///
    /// The caller must own the frame, i.e. it must not be in any ring the kernel reads or
    /// writes, and must not hold another reference into it.
    #[allow(clippy::mut_from_ref)] // Frames are owned through the rings, not through `&self`.
    pub(super) unsafe fn frame(&self, addr: u64, len: usize) -> &mut [u8] {
        assert!(addr as usize + len <= self.mmap.len, "frame out of bounds");
        // SAFETY: in bounds per the assert; exclusive per the caller.
        unsafe { std::slice::from_raw_parts_mut(self.mmap.ptr.as_ptr().add(addr as usize), len) }
    }

    /// Returns the address of the frame that contains `addr`
    pub(super) fn frame_base(&self, addr: u64) -> u64 {
        addr - addr % u64::from(self.frame_size)
    }

    pub(super) fn frame_size(&self) -> usize {
        self.frame_size as usize
    }
}

/// Sizes of an [`Xsk`]
#[derive(Debug, Clone, Copy)]
pub(super) struct XskSizes {
    /// Number of UMEM frames, half for receiving and half for sending
    pub(super) frame_count: u32,
    /// Size of each frame, 2048 or 4096
    pub(super) frame_size: u32,
    /// Number of entries of each ring, a power of two
    pub(super) ring_size: u32,
}

/// An AF_XDP socket bound to one queue of an interface, with its own UMEM
#[derive(Debug)]
pub(super) struct Xsk {
    // Closed before the rings and UMEM are unmapped.
    pub(super) fd: OwnedFd,
    pub(super) umem: Umem,
    pub(super) fill: Ring<u64>,
    pub(super) completion: Ring<u64>,
    pub(super) rx: Ring<libc::xdp_desc>,
    pub(super) tx: Ring<libc::xdp_desc>,
    /// Frames reserved for sending, the rest are for receiving
    pub(super) tx_frames: Vec<u64>,
    /// Whether the socket ended up in zero-copy mode
    pub(super) zero_copy: bool,
}

impl Xsk {
    /// Creates a socket on `queue` of interface `ifindex` and fills its fill ring
    pub(super) fn new(
        ifindex: u32,
        queue: u32,
        sizes: XskSizes,
        mode: BindMode,
    ) -> io::Result<Self> {
        let XskSizes {
            frame_count,
            frame_size,
            ring_size,
        } = sizes;
        // SAFETY: plain syscall without pointers.
        let fd = unsafe { libc::socket(libc::AF_XDP, libc::SOCK_RAW | libc::SOCK_CLOEXEC, 0) };
        if fd < 0 {
            let e = io::Error::last_os_error();
            return Err(context(e, "creating AF_XDP socket (needs CAP_NET_RAW)"));
        }
        // SAFETY: `socket` returned a new file descriptor that nothing else owns.
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };

        let umem_len = frame_count as usize * frame_size as usize;
        let umem = Umem {
            mmap: Mmap::new(umem_len, None, 0).map_err(|e| context(e, "allocating UMEM"))?,
            frame_size,
        };
        let reg = libc::xdp_umem_reg {
            addr: umem.mmap.ptr.as_ptr() as u64,
            len: umem_len as u64,
            chunk_size: frame_size,
            headroom: 0,
            flags: 0,
            tx_metadata_len: 0,
        };
        setsockopt(&fd, libc::XDP_UMEM_REG, &reg).map_err(|e| context(e, "registering UMEM"))?;
        for opt in [
            libc::XDP_UMEM_FILL_RING,
            libc::XDP_UMEM_COMPLETION_RING,
            libc::XDP_RX_RING,
            libc::XDP_TX_RING,
        ] {
            setsockopt(&fd, opt, &ring_size).map_err(|e| context(e, "sizing rings"))?;
        }
        let off = mmap_offsets(&fd)?;
        let borrowed = fd.as_fd();
        let fill = Ring::map(
            borrowed,
            libc::XDP_UMEM_PGOFF_FILL_RING as libc::off_t,
            &off.fr,
            ring_size,
            true,
        )?;
        let completion = Ring::map(
            borrowed,
            libc::XDP_UMEM_PGOFF_COMPLETION_RING as libc::off_t,
            &off.cr,
            ring_size,
            false,
        )?;
        let rx = Ring::map(borrowed, libc::XDP_PGOFF_RX_RING, &off.rx, ring_size, false)?;
        let tx = Ring::map(borrowed, libc::XDP_PGOFF_TX_RING, &off.tx, ring_size, true)?;

        let copy_flags = match mode {
            BindMode::Auto => 0,
            BindMode::Copy => libc::XDP_COPY,
            BindMode::ZeroCopy => libc::XDP_ZEROCOPY,
        };
        let addr = libc::sockaddr_xdp {
            sxdp_family: libc::AF_XDP as u16,
            sxdp_flags: libc::XDP_USE_NEED_WAKEUP | copy_flags,
            sxdp_ifindex: ifindex,
            sxdp_queue_id: queue,
            sxdp_shared_umem_fd: 0,
        };
        // SAFETY: `addr` is a valid `sockaddr_xdp` of the given length.
        let ret = unsafe {
            libc::bind(
                fd.as_raw_fd(),
                &addr as *const libc::sockaddr_xdp as *const libc::sockaddr,
                size_of::<libc::sockaddr_xdp>() as libc::socklen_t,
            )
        };
        if ret != 0 {
            let e = io::Error::last_os_error();
            return Err(context(
                e,
                &format!("binding AF_XDP socket to queue {queue}"),
            ));
        }
        let zero_copy = xdp_options(&fd)? & libc::XDP_OPTIONS_ZEROCOPY != 0;

        let mut xsk = Self {
            fd,
            umem,
            fill,
            completion,
            rx,
            tx,
            tx_frames: Vec::new(),
            zero_copy,
        };
        let rx_frames = (frame_count / 2).min(ring_size);
        for frame in 0..frame_count {
            let addr = u64::from(frame) * u64::from(frame_size);
            if frame < rx_frames {
                xsk.fill.push(addr);
            } else {
                xsk.tx_frames.push(addr);
            }
        }
        xsk.fill.submit();
        Ok(xsk)
    }

    /// Reads the kernel's statistics of this socket
    pub(super) fn statistics(fd: BorrowedFd<'_>) -> io::Result<libc::xdp_statistics> {
        getsockopt(fd, libc::XDP_STATISTICS)
    }
}

/// Kicks the kernel to process the TX ring, the `sendto` of the need-wakeup protocol
pub(super) fn wake_tx(fd: BorrowedFd<'_>) -> io::Result<()> {
    // SAFETY: a zero-length send without buffers or address.
    let ret = unsafe {
        libc::sendto(
            fd.as_raw_fd(),
            std::ptr::null(),
            0,
            libc::MSG_DONTWAIT,
            std::ptr::null(),
            0,
        )
    };
    match ret {
        0.. => Ok(()),
        _ => Err(io::Error::last_os_error()),
    }
}

/// Kicks the kernel to process the fill ring, the `recvfrom` of the need-wakeup protocol
pub(super) fn wake_rx(fd: BorrowedFd<'_>) {
    // SAFETY: a zero-length receive without buffers or address.
    unsafe {
        libc::recvfrom(
            fd.as_raw_fd(),
            std::ptr::null_mut(),
            0,
            libc::MSG_DONTWAIT,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    };
}

fn setsockopt<T>(fd: &OwnedFd, opt: libc::c_int, value: &T) -> io::Result<()> {
    // SAFETY: `value` is valid for reads of `size_of::<T>()` bytes.
    let ret = unsafe {
        libc::setsockopt(
            fd.as_raw_fd(),
            libc::SOL_XDP,
            opt,
            value as *const T as *const libc::c_void,
            size_of::<T>() as libc::socklen_t,
        )
    };
    match ret {
        0 => Ok(()),
        _ => Err(io::Error::last_os_error()),
    }
}

/// Reads a socket option of type `T`, which must be valid for any bit pattern
fn getsockopt<T: Copy>(fd: BorrowedFd<'_>, opt: libc::c_int) -> io::Result<T> {
    let mut value = MaybeUninit::<T>::zeroed();
    let mut len = size_of::<T>() as libc::socklen_t;
    // SAFETY: `value` is valid for writes of `len` bytes.
    let ret = unsafe {
        libc::getsockopt(
            fd.as_raw_fd(),
            libc::SOL_XDP,
            opt,
            value.as_mut_ptr().cast(),
            &mut len,
        )
    };
    if ret != 0 {
        return Err(io::Error::last_os_error());
    }
    // SAFETY: zero-initialized, and the callers only use plain integer structs.
    Ok(unsafe { value.assume_init() })
}

fn mmap_offsets(fd: &OwnedFd) -> io::Result<libc::xdp_mmap_offsets> {
    // Kernels before 5.4 return offsets without the `flags` field, and thus no need-wakeup
    // support, which binding with `XDP_USE_NEED_WAKEUP` would reject anyway.
    let off: libc::xdp_mmap_offsets = getsockopt(fd.as_fd(), libc::XDP_MMAP_OFFSETS)
        .map_err(|e| context(e, "reading ring offsets"))?;
    Ok(off)
}

fn xdp_options(fd: &OwnedFd) -> io::Result<u32> {
    let options: libc::xdp_options =
        getsockopt(fd.as_fd(), libc::XDP_OPTIONS).map_err(|e| context(e, "reading options"))?;
    Ok(options.flags)
}
