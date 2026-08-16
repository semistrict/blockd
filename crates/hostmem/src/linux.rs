//! The Linux memory machinery: memfd regions, dual views, userfaultfd.
//!
//! ABI constants below are checked against `<linux/userfaultfd.h>` by the
//! `abi_constants_match_kernel_headers` step of the Lima validation run;
//! the `api()` handshake additionally fails loudly at runtime if the
//! kernel rejects them.

use std::io;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};

use blockd_platform::page_size;

// ── userfaultfd ABI ─────────────────────────────────────────────────────

const UFFD_API: u64 = 0xAA;
const UFFD_USER_MODE_ONLY: libc::c_int = 1;

// _IOWR('U' family 0xAA): dir<<30 | size<<16 | 0xAA<<8 | nr
const fn iowr(nr: u64, size: u64) -> u64 {
    (3 << 30) | (size << 16) | (0xAA << 8) | nr
}
const fn ior(nr: u64, size: u64) -> u64 {
    (2 << 30) | (size << 16) | (0xAA << 8) | nr
}

const UFFDIO_API_IOCTL: u64 = iowr(0x3F, 24); // uffdio_api: 3×u64
const UFFDIO_COPY: u64 = iowr(0x03, 40); // dst+src+len+mode+copy
const UFFDIO_REGISTER: u64 = iowr(0x00, 32); // range + mode + ioctls
const UFFDIO_WAKE: u64 = ior(0x02, 16); // uffdio_range
const UFFDIO_WRITEPROTECT: u64 = iowr(0x06, 24); // range + mode
const UFFDIO_CONTINUE: u64 = iowr(0x07, 32); // range + mode + mapped

pub const UFFDIO_REGISTER_MODE_MISSING: u64 = 1 << 0;
pub const UFFDIO_REGISTER_MODE_WP: u64 = 1 << 1;
pub const UFFDIO_REGISTER_MODE_MINOR: u64 = 1 << 2;

const UFFDIO_WRITEPROTECT_MODE_WP: u64 = 1 << 0;

const UFFD_EVENT_PAGEFAULT: u8 = 0x12;
pub const UFFD_PAGEFAULT_FLAG_WRITE: u64 = 1 << 0;
pub const UFFD_PAGEFAULT_FLAG_WP: u64 = 1 << 1;
pub const UFFD_PAGEFAULT_FLAG_MINOR: u64 = 1 << 2;

/// The feature bits this runtime depends on (R9.1's kernel contract).
#[derive(Clone, Copy, Debug)]
pub struct UffdFeatures(pub u64);

impl UffdFeatures {
    pub const PAGEFAULT_FLAG_WP: u64 = 1 << 0;
    pub const MINOR_SHMEM: u64 = 1 << 10;
    pub const WP_HUGETLBFS_SHMEM: u64 = 1 << 12;
    pub const WP_UNPOPULATED: u64 = 1 << 13;

    pub fn has(self, bit: u64) -> bool {
        self.0 & bit != 0
    }
}

#[repr(C)]
struct UffdioApi {
    api: u64,
    features: u64,
    ioctls: u64,
}

#[repr(C)]
struct UffdioRange {
    start: u64,
    len: u64,
}

#[repr(C)]
struct UffdioRegister {
    range: UffdioRange,
    mode: u64,
    ioctls: u64,
}

#[repr(C)]
struct UffdioWriteprotect {
    range: UffdioRange,
    mode: u64,
}

#[repr(C)]
struct UffdioContinue {
    range: UffdioRange,
    mode: u64,
    mapped: i64,
}

/// One page fault delivered by the kernel.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FaultEvent {
    pub address: usize,
    pub write: bool,
    /// Write to a write-protected page (the capture boundary, R2.4/R3).
    pub wp: bool,
    /// Page-cache page present but not mapped (an evicted page refaulting,
    /// R2.4).
    pub minor: bool,
}

impl FaultEvent {
    /// No page-cache page at all (first touch, or after a hole punch):
    /// the fill boundary proper (R2.1). Verified against the live kernel:
    /// MINOR mode alone silently zero-allocates absent shmem pages —
    /// MISSING registration is what makes first touches trap.
    pub fn missing(self) -> bool {
        !self.wp && !self.minor
    }
}

fn errno(context: &str) -> io::Error {
    let e = io::Error::last_os_error();
    io::Error::new(e.kind(), format!("{context}: {e}"))
}

// ── memfd region + views ────────────────────────────────────────────────

/// A region of guest memory: one memfd, mapped once as the daemon view.
/// The daemon view is how fills populate the page cache and how captures
/// read bytes back (`HostMap`) — the same physical pages every guest view
/// maps.
pub struct HostRegion {
    fd: OwnedFd,
    daemon: *mut u8,
    len: usize,
    pages: usize,
}

// The raw pointer is to a shared file mapping owned by this struct.
unsafe impl Send for HostRegion {}
unsafe impl Sync for HostRegion {}

impl HostRegion {
    pub fn new(pages: usize) -> io::Result<HostRegion> {
        let len = pages.checked_mul(page_size()).expect("region size fits");
        // SAFETY: plain syscalls; the fd and mapping are owned below.
        let fd = unsafe { libc::memfd_create(c"blockd-region".as_ptr(), libc::MFD_CLOEXEC) };
        if fd < 0 {
            return Err(errno("memfd_create"));
        }
        let fd = unsafe { OwnedFd::from_raw_fd(fd) };
        if unsafe { libc::ftruncate(fd.as_raw_fd(), libc::off_t::try_from(len).expect("fits")) }
            != 0
        {
            return Err(errno("ftruncate"));
        }
        let daemon = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                fd.as_raw_fd(),
                0,
            )
        };
        if daemon == libc::MAP_FAILED {
            return Err(errno("mmap daemon view"));
        }
        Ok(HostRegion {
            fd,
            daemon: daemon.cast(),
            len,
            pages,
        })
    }

    pub fn pages(&self) -> usize {
        self.pages
    }

    /// Fill's first half: write bytes through the daemon view, populating
    /// the shared page-cache page a `UFFDIO_CONTINUE` will then map.
    pub fn write_page(&self, page: usize, bytes: &[u8]) {
        assert_eq!(bytes.len(), page_size());
        assert!(page < self.pages());
        // SAFETY: in-bounds write to our own shared mapping.
        unsafe {
            std::ptr::copy_nonoverlapping(
                bytes.as_ptr(),
                self.daemon.add(page * page_size()),
                page_size(),
            );
        }
    }

    /// Capture's read (`HostMap::read_page`): the daemon view observes the
    /// exact bytes the guest sees, including writes still in its TLB-
    /// coherent shared mapping.
    pub fn read_page(&self, page: usize) -> Vec<u8> {
        assert!(page < self.pages());
        let mut bytes = vec![0u8; page_size()];
        // SAFETY: in-bounds read from our own shared mapping.
        unsafe {
            std::ptr::copy_nonoverlapping(
                self.daemon.add(page * page_size()),
                bytes.as_mut_ptr(),
                page_size(),
            );
        }
        bytes
    }

    /// Reclaim the backing itself (R2.7's droppable class): the page-cache
    /// copy is freed; the data is gone from this host.
    pub fn punch_hole(&self, page: usize, count: usize) -> io::Result<()> {
        let r = unsafe {
            libc::fallocate(
                self.fd.as_raw_fd(),
                libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
                libc::off_t::try_from(page * page_size()).expect("fits"),
                libc::off_t::try_from(count * page_size()).expect("fits"),
            )
        };
        if r != 0 {
            return Err(errno("fallocate(PUNCH_HOLE)"));
        }
        Ok(())
    }

    /// Bytes of this region resident in the page cache right now.
    pub fn resident_bytes(&self) -> io::Result<usize> {
        let mut st: libc::stat = unsafe { std::mem::zeroed() };
        if unsafe { libc::fstat(self.fd.as_raw_fd(), &raw mut st) } != 0 {
            return Err(errno("fstat"));
        }
        Ok(usize::try_from(st.st_blocks).expect("fits") * 512)
    }
}

impl Drop for HostRegion {
    fn drop(&mut self) {
        // SAFETY: unmapping the mapping created in `new`.
        unsafe {
            libc::munmap(self.daemon.cast(), self.len);
        }
    }
}

/// A VM's mapping of (part of) a region: the faulting side. Real guests
/// hand this range to the VMM; the tests touch it directly.
pub struct GuestView {
    base: *mut u8,
    len: usize,
}

unsafe impl Send for GuestView {}
unsafe impl Sync for GuestView {}

impl GuestView {
    /// Map `pages` pages of `region` starting at `first_page`. Many views
    /// of the same range share the same physical pages (R5.3).
    pub fn map(region: &HostRegion, first_page: usize, pages: usize) -> io::Result<GuestView> {
        let len = pages.checked_mul(page_size()).expect("view size fits");
        assert!(first_page + pages <= region.pages);
        // SAFETY: fresh shared mapping of the region's fd; owned below.
        let base = unsafe {
            libc::mmap(
                std::ptr::null_mut(),
                len,
                libc::PROT_READ | libc::PROT_WRITE,
                libc::MAP_SHARED,
                region.fd.as_raw_fd(),
                libc::off_t::try_from(first_page * page_size()).expect("fits"),
            )
        };
        if base == libc::MAP_FAILED {
            return Err(errno("mmap guest view"));
        }
        Ok(GuestView {
            base: base.cast(),
            len,
        })
    }

    pub fn addr_of(&self, page: usize) -> usize {
        assert!(page * page_size() < self.len);
        self.base as usize + page * page_size()
    }

    /// A guest load: blocks in the kernel if the page faults, until the
    /// handler resolves it.
    pub fn read_word(&self, page: usize) -> u64 {
        // SAFETY: in-bounds volatile read; may fault (that is the point).
        unsafe { std::ptr::read_volatile(self.addr_of(page) as *const u64) }
    }

    pub fn read_page(&self, page: usize) -> Vec<u8> {
        let mut bytes = vec![0u8; page_size()];
        // SAFETY: in-bounds read; may fault.
        unsafe {
            std::ptr::copy_nonoverlapping(
                self.addr_of(page) as *const u8,
                bytes.as_mut_ptr(),
                page_size(),
            );
        }
        bytes
    }

    /// A guest store: blocks on WP or missing-mapping faults.
    pub fn write_word(&self, page: usize, value: u64) {
        // SAFETY: in-bounds volatile write; may fault.
        unsafe { std::ptr::write_volatile(self.addr_of(page) as *mut u64, value) }
    }

    /// Which of this view's pages have a resident backing page right now.
    /// Kernel-verified semantics: for a `MAP_SHARED` file mapping, `mincore`
    /// reports PAGE-CACHE residency of the backed range — NOT this view's
    /// PTEs. `MADV_DONTNEED` therefore changes nothing here (the backing
    /// survives; only a refault proves the PTE went away), while a hole
    /// punch flips pages to false (the backing itself is gone).
    pub fn resident(&self) -> io::Result<Vec<bool>> {
        let pages = self.len / page_size();
        let mut vec = vec![0u8; pages];
        // SAFETY: querying our own mapping with a correctly sized buffer.
        let r = unsafe {
            libc::mincore(
                self.base.cast::<libc::c_void>(),
                self.len,
                vec.as_mut_ptr().cast(),
            )
        };
        if r != 0 {
            return Err(errno("mincore"));
        }
        Ok(vec.into_iter().map(|b| b & 1 != 0).collect())
    }

    /// Evict: drop this view's PTEs (`MADV_DONTNEED`). For a shared
    /// mapping this can never lose data — the page cache keeps the page;
    /// the next touch is a minor fault.
    pub fn evict(&self, page: usize, count: usize) -> io::Result<()> {
        let r = unsafe {
            libc::madvise(
                self.addr_of(page) as *mut libc::c_void,
                count * page_size(),
                libc::MADV_DONTNEED,
            )
        };
        if r != 0 {
            return Err(errno("madvise(DONTNEED)"));
        }
        Ok(())
    }
}

impl Drop for GuestView {
    fn drop(&mut self) {
        // SAFETY: unmapping the mapping created in `map`.
        unsafe {
            libc::munmap(self.base.cast(), self.len);
        }
    }
}

// ── userfaultfd ─────────────────────────────────────────────────────────

pub struct Uffd {
    fd: OwnedFd,
}

impl Uffd {
    /// Adopt a userfaultfd created (and registered) by another process —
    /// e.g. the one Firecracker hands its page-fault handler over a unix
    /// socket at snapshot restore. Foreign uffds often arrive `O_NONBLOCK`
    /// (Firecracker's does); this runtime serves with blocking reads, so
    /// clear it.
    pub fn from_fd(fd: OwnedFd) -> Uffd {
        let uffd = Uffd { fd };
        uffd.set_nonblocking(false).expect("configure userfaultfd");
        uffd
    }

    /// Adopt a foreign userfaultfd for a readiness-driven event loop.
    /// Reads return `WouldBlock` after the queued events have drained.
    pub fn from_fd_nonblocking(fd: OwnedFd) -> Uffd {
        let uffd = Uffd { fd };
        uffd.set_nonblocking(true).expect("configure userfaultfd");
        uffd
    }

    pub fn set_nonblocking(&self, nonblocking: bool) -> io::Result<()> {
        // SAFETY: fcntl on our owned fd.
        let flags = unsafe { libc::fcntl(self.fd.as_raw_fd(), libc::F_GETFL) };
        if flags < 0 {
            return Err(errno("fcntl(F_GETFL)"));
        }
        let flags = if nonblocking {
            flags | libc::O_NONBLOCK
        } else {
            flags & !libc::O_NONBLOCK
        };
        if unsafe { libc::fcntl(self.fd.as_raw_fd(), libc::F_SETFL, flags) } < 0 {
            return Err(errno("fcntl(F_SETFL)"));
        }
        Ok(())
    }

    /// `UFFDIO_COPY`: allocate-and-copy `bytes` into the faulting range.
    /// This is the fill door for ANONYMOUS registered memory (Firecracker
    /// guests); shmem regions use populate + `continue_range` instead.
    pub fn copy(&self, dst: usize, bytes: &[u8]) -> io::Result<()> {
        #[repr(C)]
        struct UffdioCopy {
            dst: u64,
            src: u64,
            len: u64,
            mode: u64,
            copy: i64,
        }
        let mut arg = UffdioCopy {
            dst: dst as u64,
            src: bytes.as_ptr() as u64,
            len: bytes.len() as u64,
            mode: 0,
            copy: 0,
        };
        // SAFETY: ioctl with a properly-sized repr(C) struct; src points
        // at `bytes`, which outlives the call.
        if unsafe {
            libc::ioctl(
                self.fd.as_raw_fd(),
                UFFDIO_COPY as libc::c_ulong,
                &raw mut arg,
            )
        } != 0
        {
            return Err(errno("UFFDIO_COPY"));
        }
        Ok(())
    }

    /// Open and handshake. `UFFD_USER_MODE_ONLY` keeps this usable without
    /// privilege (guest faults are user-mode faults). Returns the kernel's
    /// feature set.
    pub fn new(requested_features: u64) -> io::Result<(Uffd, UffdFeatures)> {
        // SAFETY: raw syscall; fd owned below.
        let fd =
            unsafe { libc::syscall(libc::SYS_userfaultfd, libc::O_CLOEXEC | UFFD_USER_MODE_ONLY) };
        if fd < 0 {
            return Err(errno("userfaultfd"));
        }
        let fd = unsafe { OwnedFd::from_raw_fd(i32::try_from(fd).expect("fd fits")) };
        let mut api = UffdioApi {
            api: UFFD_API,
            features: requested_features,
            ioctls: 0,
        };
        // SAFETY: ioctl with a properly-sized repr(C) struct.
        if unsafe {
            libc::ioctl(
                fd.as_raw_fd(),
                UFFDIO_API_IOCTL as libc::c_ulong,
                &raw mut api,
            )
        } != 0
        {
            return Err(errno("UFFDIO_API"));
        }
        Ok((Uffd { fd }, UffdFeatures(api.features)))
    }

    /// Register a guest view for the full fault surface: MISSING (absent
    /// pages — first touches and post-punch refills), MINOR (present but
    /// unmapped — evicted-PTE refaults), and WP (capture arming). All
    /// three resolve through the same door: populate the page cache via
    /// the daemon view, then `UFFDIO_CONTINUE` (for shmem this lands in
    /// the SHARED page cache — one copy for every view).
    pub fn register_all(&self, view: &GuestView) -> io::Result<()> {
        self.register(
            view.base as usize,
            view.len,
            UFFDIO_REGISTER_MODE_MISSING | UFFDIO_REGISTER_MODE_MINOR | UFFDIO_REGISTER_MODE_WP,
        )
    }

    pub fn register(&self, start: usize, len: usize, mode: u64) -> io::Result<()> {
        let mut reg = UffdioRegister {
            range: UffdioRange {
                start: start as u64,
                len: len as u64,
            },
            mode,
            ioctls: 0,
        };
        // SAFETY: ioctl with a properly-sized repr(C) struct.
        if unsafe {
            libc::ioctl(
                self.fd.as_raw_fd(),
                UFFDIO_REGISTER as libc::c_ulong,
                &raw mut reg,
            )
        } != 0
        {
            return Err(errno("UFFDIO_REGISTER"));
        }
        Ok(())
    }

    /// Fill's second half — and the whole of a prefetch (R6.2): map the
    /// already-populated page-cache page into the registered range.
    /// `protect` installs it write-protected (capture-armed).
    pub fn continue_range(&self, start: usize, len: usize, protect: bool) -> io::Result<()> {
        let mut arg = UffdioContinue {
            range: UffdioRange {
                start: start as u64,
                len: len as u64,
            },
            mode: if protect { 2 } else { 0 }, // UFFDIO_CONTINUE_MODE_WP
            mapped: 0,
        };
        // SAFETY: ioctl with a properly-sized repr(C) struct.
        if unsafe {
            libc::ioctl(
                self.fd.as_raw_fd(),
                UFFDIO_CONTINUE as libc::c_ulong,
                &raw mut arg,
            )
        } != 0
        {
            return Err(errno("UFFDIO_CONTINUE"));
        }
        Ok(())
    }

    /// Arm (`protect`) or clear write protection. Clearing wakes the
    /// blocked writer (Unprotect).
    pub fn writeprotect(&self, start: usize, len: usize, protect: bool) -> io::Result<()> {
        let mut arg = UffdioWriteprotect {
            range: UffdioRange {
                start: start as u64,
                len: len as u64,
            },
            mode: if protect {
                UFFDIO_WRITEPROTECT_MODE_WP
            } else {
                0
            },
        };
        // SAFETY: ioctl with a properly-sized repr(C) struct.
        if unsafe {
            libc::ioctl(
                self.fd.as_raw_fd(),
                UFFDIO_WRITEPROTECT as libc::c_ulong,
                &raw mut arg,
            )
        } != 0
        {
            return Err(errno("UFFDIO_WRITEPROTECT"));
        }
        Ok(())
    }

    pub fn wake(&self, start: usize, len: usize) -> io::Result<()> {
        let mut range = UffdioRange {
            start: start as u64,
            len: len as u64,
        };
        // SAFETY: ioctl with a properly-sized repr(C) struct.
        if unsafe {
            libc::ioctl(
                self.fd.as_raw_fd(),
                UFFDIO_WAKE as libc::c_ulong,
                &raw mut range,
            )
        } != 0
        {
            return Err(errno("UFFDIO_WAKE"));
        }
        Ok(())
    }

    /// Read EVERY currently queued event. A blocking descriptor waits for
    /// the first event; a nonblocking descriptor returns `WouldBlock` when
    /// empty. A uffd read fills the buffer with as many queued `uffd_msg`s
    /// as fit, so a
    /// reader that parsed just one would consume and silently drop the
    /// rest — leaving those faulters parked in the kernel forever.
    /// (Message layout checked against the uapi header by the ABI probe:
    /// 32-byte stride, event at offset 0, pagefault flags at 8, address
    /// at 16.)
    pub fn read_events(&self) -> io::Result<Vec<FaultEvent>> {
        const MSG_SIZE: usize = 32; // sizeof(struct uffd_msg)
        let mut buf = [0u8; 32 * MSG_SIZE];
        // SAFETY: reading into a local buffer.
        let n = unsafe { libc::read(self.fd.as_raw_fd(), buf.as_mut_ptr().cast(), buf.len()) };
        if n < 0 {
            return Err(errno("read(uffd)"));
        }
        let n = usize::try_from(n).expect("fits");
        if n == 0 || n % MSG_SIZE != 0 {
            return Err(io::Error::other(format!("torn uffd read: {n} bytes")));
        }
        let mut events = Vec::with_capacity(n / MSG_SIZE);
        for msg in buf[..n].chunks_exact(MSG_SIZE) {
            let event = msg[0];
            if event != UFFD_EVENT_PAGEFAULT {
                return Err(io::Error::other(format!(
                    "unexpected uffd event 0x{event:x}"
                )));
            }
            let flags = u64::from_le_bytes(msg[8..16].try_into().expect("sized"));
            let address = u64::from_le_bytes(msg[16..24].try_into().expect("sized"));
            events.push(FaultEvent {
                address: usize::try_from(address).expect("fits"),
                write: flags & UFFD_PAGEFAULT_FLAG_WRITE != 0,
                wp: flags & UFFD_PAGEFAULT_FLAG_WP != 0,
                minor: flags & UFFD_PAGEFAULT_FLAG_MINOR != 0,
            });
        }
        Ok(events)
    }
}

impl AsRawFd for Uffd {
    fn as_raw_fd(&self) -> std::os::fd::RawFd {
        self.fd.as_raw_fd()
    }
}

/// Punch a hole in any file (`FALLOC_FL_PUNCH_HOLE`): frees the page-cache
/// pages and disk blocks of the range. For a shared-memory file backing
/// guest mappings this is the backing-reclaim primitive — private
/// copy-on-write pages in the mappings survive; clean pages refault.
pub fn punch_hole_file(file: &std::fs::File, offset: u64, len: u64) -> io::Result<()> {
    // SAFETY: fallocate on the caller's fd with plain integer arguments.
    let r = unsafe {
        libc::fallocate(
            file.as_raw_fd(),
            libc::FALLOC_FL_PUNCH_HOLE | libc::FALLOC_FL_KEEP_SIZE,
            libc::off_t::try_from(offset).expect("fits"),
            libc::off_t::try_from(len).expect("fits"),
        )
    };
    if r != 0 {
        return Err(errno("fallocate(PUNCH_HOLE)"));
    }
    Ok(())
}

/// Bytes of a file resident/allocated right now (`st_blocks`): for shmem
/// files this is exactly the physical memory the file holds.
pub fn file_resident_bytes(file: &std::fs::File) -> io::Result<usize> {
    let mut st: libc::stat = unsafe { std::mem::zeroed() };
    // SAFETY: fstat into a zeroed local.
    if unsafe { libc::fstat(file.as_raw_fd(), &raw mut st) } != 0 {
        return Err(errno("fstat"));
    }
    Ok(usize::try_from(st.st_blocks).expect("fits") * 512)
}

/// Receive one message and (optionally) one file descriptor over a unix
/// stream — `recvmsg` with `SCM_RIGHTS`. This is how Firecracker hands its
/// page-fault handler the guest-memory uffd at snapshot restore.
pub fn recv_with_fd(stream: &impl AsRawFd, buf: &mut [u8]) -> io::Result<(usize, Option<OwnedFd>)> {
    const CMSG_SPACE: usize = 64;
    let mut iov = libc::iovec {
        iov_base: buf.as_mut_ptr().cast(),
        iov_len: buf.len(),
    };
    let mut cmsg = [0u8; CMSG_SPACE];
    // SAFETY: zeroed msghdr filled with valid pointers to locals.
    let mut msg: libc::msghdr = unsafe { std::mem::zeroed() };
    msg.msg_iov = &raw mut iov;
    msg.msg_iovlen = 1;
    msg.msg_control = cmsg.as_mut_ptr().cast();
    msg.msg_controllen = CMSG_SPACE;
    // SAFETY: recvmsg on our own connected stream with the msghdr above.
    let n = unsafe { libc::recvmsg(stream.as_raw_fd(), &raw mut msg, 0) };
    if n < 0 {
        return Err(errno("recvmsg"));
    }
    let mut fd = None;
    // SAFETY: walking the control buffer the kernel just filled, using the
    // kernel's own CMSG accessors.
    unsafe {
        let mut c = libc::CMSG_FIRSTHDR(&raw const msg);
        while !c.is_null() {
            if (*c).cmsg_level == libc::SOL_SOCKET && (*c).cmsg_type == libc::SCM_RIGHTS {
                let mut raw: libc::c_int = 0;
                std::ptr::copy_nonoverlapping(
                    libc::CMSG_DATA(c),
                    (&raw mut raw).cast::<u8>(),
                    std::mem::size_of::<libc::c_int>(),
                );
                fd = Some(OwnedFd::from_raw_fd(raw));
            }
            c = libc::CMSG_NXTHDR(&raw const msg, c);
        }
    }
    Ok((usize::try_from(n).expect("fits"), fd))
}
