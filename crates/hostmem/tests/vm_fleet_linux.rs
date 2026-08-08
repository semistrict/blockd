//! A fleet of simulated VMs doing what real blockd guests do — boot from a
//! shared base, read it, diverge by writing, get evicted, survive backing
//! reclaim, shut down — with the ACTUAL physical memory measured and
//! asserted after every phase.
//!
//! Physical truth comes from the kernel, two ways:
//! - `fstat().st_blocks` on each memfd: the page-cache pages the backing
//!   actually holds. For shmem this IS the physical footprint of the data.
//! - `/proc/self/smaps` Pss over the fleet's mappings: the same total,
//!   arrived at independently through the page tables.
//!
//! `mincore` separates PTE presence from residency (eviction drops the
//! former, never the latter).
//!
//! The R1.3/R5.3 economics this proves on real hardware: N VMs sharing a
//! base cost ONE base — plus exactly what each VM writes.

#![cfg(target_os = "linux")]
// Threads and index-to-tag truncation are deliberate here — this is the
// nondeterministic side of the seam.
#![allow(clippy::cast_possible_truncation, clippy::disallowed_methods)]

use std::io;
use std::os::fd::AsRawFd;
use std::os::unix::fs::OpenOptionsExt;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;

use blockd_hostmem::{GuestView, HostRegion, Uffd, UffdFeatures, page_size};

const BASE_PAGES: usize = 256; // 1 MiB base image
const VMS: usize = 32;
const PRIVATE_PAGES: usize = 64;
const DIVERGE: usize = 8; // pages each VM writes

struct PageBuf {
    ptr: *mut u8,
}

impl PageBuf {
    fn new() -> PageBuf {
        let layout = std::alloc::Layout::from_size_align(page_size(), page_size()).expect("layout");
        // SAFETY: nonzero size; deallocated with the same layout in Drop.
        let ptr = unsafe { std::alloc::alloc_zeroed(layout) };
        assert!(!ptr.is_null(), "aligned alloc failed");
        PageBuf { ptr }
    }

    fn as_slice(&self) -> &[u8] {
        // SAFETY: one system page is owned by this buffer.
        unsafe { std::slice::from_raw_parts(self.ptr, page_size()) }
    }

    fn as_mut_slice(&mut self) -> &mut [u8] {
        // SAFETY: one system page is owned by this buffer.
        unsafe { std::slice::from_raw_parts_mut(self.ptr, page_size()) }
    }
}

impl Drop for PageBuf {
    fn drop(&mut self) {
        let layout = std::alloc::Layout::from_size_align(page_size(), page_size()).expect("layout");
        // SAFETY: allocated in `new` with this exact layout.
        unsafe { std::alloc::dealloc(self.ptr, layout) }
    }
}

struct DirectFile {
    file: std::fs::File,
}

impl DirectFile {
    fn create(path: &str) -> io::Result<DirectFile> {
        let file = std::fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create_new(true)
            .custom_flags(libc::O_DIRECT)
            .open(path)?;
        Ok(DirectFile { file })
    }

    fn write_page(&self, page: usize, buf: &PageBuf) -> io::Result<()> {
        // SAFETY: aligned page-sized buffer; pwrite on our own fd.
        let n = unsafe {
            libc::pwrite(
                self.file.as_raw_fd(),
                buf.ptr.cast(),
                page_size(),
                libc::off_t::try_from(page * page_size()).expect("fits"),
            )
        };
        if n != page_size().cast_signed() {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn read_page(&self, page: usize, buf: &mut PageBuf) -> io::Result<()> {
        // SAFETY: aligned page-sized buffer; pread on our own fd.
        let n = unsafe {
            libc::pread(
                self.file.as_raw_fd(),
                buf.ptr.cast(),
                page_size(),
                libc::off_t::try_from(page * page_size()).expect("fits"),
            )
        };
        if n != page_size().cast_signed() {
            return Err(io::Error::last_os_error());
        }
        Ok(())
    }

    fn sync(&self) -> io::Result<()> {
        self.file.sync_all()
    }
}

fn base_pattern(page: usize) -> Vec<u8> {
    let mut bytes = vec![0u8; page_size()];
    for (i, b) in bytes.iter_mut().enumerate() {
        *b = (page as u8) ^ (i as u8) ^ 0xB5;
    }
    bytes
}

fn required_features() -> u64 {
    UffdFeatures::PAGEFAULT_FLAG_WP | UffdFeatures::MINOR_SHMEM | UffdFeatures::WP_HUGETLBFS_SHMEM
}

/// One simulated VM: a window onto the shared base plus its own private
/// region, with a daemon-side fault handler thread — the same split the
/// production daemon has.
struct Vm {
    base_win: Arc<GuestView>,
    own: Arc<HostRegion>,
    own_win: Arc<GuestView>,
    /// Faults this VM's handler resolved — the observable proof of which
    /// touches actually trapped.
    faults: Arc<AtomicU64>,
}

impl Vm {
    fn boot(base: &Arc<HostRegion>) -> Vm {
        let base_win = Arc::new(GuestView::map(base, 0, BASE_PAGES).expect("base window"));
        let own = Arc::new(HostRegion::new(PRIVATE_PAGES).expect("own region"));
        let own_win = Arc::new(GuestView::map(&own, 0, PRIVATE_PAGES).expect("own window"));
        let (uffd, _) = Uffd::new(required_features()).expect("uffd");
        uffd.register_all(&base_win).expect("register base window");
        uffd.register_all(&own_win).expect("register own window");
        let uffd = Arc::new(uffd);
        let faults = Arc::new(AtomicU64::new(0));

        // The daemon-side handler: resolves this VM's faults for as long
        // as the process lives (each nextest test is its own process).
        {
            let (base, base_win, own, own_win, uffd, faults) = (
                base.clone(),
                base_win.clone(),
                own.clone(),
                own_win.clone(),
                uffd.clone(),
                faults.clone(),
            );
            thread::spawn(move || {
                while let Ok(events) = uffd.read_events() {
                    for event in events {
                        faults.fetch_add(1, Ordering::SeqCst);
                        let addr = event.address & !(page_size() - 1);
                        if event.wp {
                            uffd.writeprotect(addr, page_size(), false)
                                .expect("unprotect");
                            continue;
                        }
                        if (base_win.addr_of(0)..base_win.addr_of(0) + BASE_PAGES * page_size())
                            .contains(&addr)
                        {
                            let page = (addr - base_win.addr_of(0)) / page_size();
                            if event.missing() {
                                // The backing was reclaimed: "refetch from the
                                // store" (the base image is the store here).
                                base.write_page(page, &base_pattern(page));
                            }
                            uffd.continue_range(addr, page_size(), false)
                                .expect("continue base");
                        } else {
                            let page = (addr - own_win.addr_of(0)) / page_size();
                            // Divergence: copy-on-write from the base image
                            // into this VM's own storage (the sim's CoW path:
                            // the page's location moves to the fork's own
                            // namespace).
                            own.write_page(page, &base_pattern(page));
                            uffd.continue_range(addr, page_size(), false)
                                .expect("continue own");
                        }
                    }
                }
            });
        }
        Vm {
            base_win,
            own,
            own_win,
            faults,
        }
    }
}

/// Sum of Pss (proportionally-shared physical KB) over the given address
/// ranges, straight from /proc/self/smaps.
fn pss_bytes_of(ranges: &[(usize, usize)]) -> usize {
    let smaps = std::fs::read_to_string("/proc/self/smaps").expect("smaps");
    let mut total_kb = 0usize;
    let mut in_range = false;
    for line in smaps.lines() {
        if let Some((span, _)) = line.split_once(' ')
            && let Some((lo, hi)) = span.split_once('-')
            && let (Ok(lo), Ok(hi)) = (usize::from_str_radix(lo, 16), usize::from_str_radix(hi, 16))
        {
            in_range = ranges
                .iter()
                .any(|&(start, len)| lo >= start && hi <= start + len);
            continue;
        }
        if in_range && let Some(rest) = line.strip_prefix("Pss:") {
            let kb: usize = rest
                .trim()
                .trim_end_matches(" kB")
                .trim()
                .parse()
                .expect("Pss kB");
            total_kb += kb;
        }
    }
    total_kb * 1024
}

#[test]
#[allow(clippy::too_many_lines)] // one phased scenario, asserted per phase
fn fleet_pays_for_the_base_once_and_for_divergence_only() {
    // ── Phase 1: build the base image (a kept checkpoint, R5.2) ─────────
    let base = Arc::new(HostRegion::new(BASE_PAGES).expect("base"));
    for page in 0..BASE_PAGES {
        base.write_page(page, &base_pattern(page));
    }
    assert_eq!(
        base.resident_bytes().expect("resident"),
        BASE_PAGES * page_size(),
        "base image not fully physical"
    );

    // ── Phase 2: boot the fleet; every VM reads the ENTIRE base ─────────
    let vms: Vec<Vm> = (0..VMS).map(|_| Vm::boot(&base)).collect();
    for vm in &vms {
        for page in 0..BASE_PAGES {
            assert_eq!(vm.base_win.read_page(page), base_pattern(page));
        }
    }
    // 32 VMs × 1 MiB of reads: physical memory is STILL one base (R5.3).
    assert_eq!(
        base.resident_bytes().expect("resident"),
        BASE_PAGES * page_size(),
        "sharing broke: fleet reads duplicated base pages"
    );
    for vm in &vms {
        assert_eq!(
            vm.own.resident_bytes().expect("resident"),
            0,
            "a read-only VM allocated private pages"
        );
    }
    // Independent check through the page tables: the Pss of all 32 base
    // windows plus the daemon view sums to ~one base, not 32.
    let ranges: Vec<(usize, usize)> = vms
        .iter()
        .map(|vm| (vm.base_win.addr_of(0), BASE_PAGES * page_size()))
        .collect();
    let pss = pss_bytes_of(&ranges);
    let one_base = BASE_PAGES * page_size();
    assert!(
        pss <= one_base + one_base / 10,
        "Pss says the fleet holds {pss} bytes of base — more than one copy ({one_base})"
    );
    assert!(
        pss >= one_base - one_base / 10,
        "Pss says only {pss} bytes are resident — the base is not mapped ({one_base})"
    );

    // ── Phase 3: divergence — each VM writes its own pages ──────────────
    for (n, vm) in vms.iter().enumerate() {
        for k in 0..DIVERGE {
            // Write faults MISSING on the private window; the handler CoWs
            // base content into the VM's own region, then the store lands.
            vm.own_win.write_word(k, 0xD1BE_0000 + n as u64);
        }
    }
    for (n, vm) in vms.iter().enumerate() {
        assert_eq!(
            vm.own.resident_bytes().expect("resident"),
            DIVERGE * page_size(),
            "vm {n}: divergence cost is not exactly its writes"
        );
        // Content: the guest's word over the CoW'd base bytes.
        let got = vm.own_win.read_page(0);
        let mut want = base_pattern(0);
        want[0..8].copy_from_slice(&(0xD1BE_0000 + n as u64).to_le_bytes());
        assert_eq!(got, want, "vm {n}: CoW content wrong");
    }
    // Fleet total: one base + exactly the divergence. (32 VMs × 8 pages —
    // NOT 32 × 256.)
    let total: usize = base.resident_bytes().expect("resident")
        + vms
            .iter()
            .map(|vm| vm.own.resident_bytes().expect("resident"))
            .sum::<usize>();
    assert_eq!(total, (BASE_PAGES + VMS * DIVERGE) * page_size());

    // ── Phase 4: memory pressure — evict every VM's base PTEs ───────────
    // Every VM demand-faulted the whole base during boot.
    assert!(vms[7].faults.load(Ordering::SeqCst) >= BASE_PAGES as u64);
    for vm in &vms {
        vm.base_win.evict(0, BASE_PAGES).expect("evict");
    }
    // Kernel-verified: mincore still reports the range resident — the
    // page CACHE survives eviction; only this view's PTEs dropped.
    assert!(
        vms[0]
            .base_win
            .resident()
            .expect("mincore")
            .iter()
            .all(|m| *m),
        "eviction freed backing pages — it must only drop PTEs"
    );
    // Eviction freed NOTHING physical (the pages are the base's, R5.3) —
    // and lost nothing: the PROOF the PTE dropped is the refault itself,
    // a MINOR fault served zero-copy.
    assert_eq!(
        base.resident_bytes().expect("resident"),
        BASE_PAGES * page_size()
    );
    let before = vms[7].faults.load(Ordering::SeqCst);
    assert_eq!(vms[7].base_win.read_page(9), base_pattern(9));
    assert!(
        vms[7].faults.load(Ordering::SeqCst) > before,
        "eviction did not drop the PTE: the re-touch never refaulted"
    );

    // ── Phase 5: backing reclaim (R2.7) — punch half the base ───────────
    base.punch_hole(BASE_PAGES / 2, BASE_PAGES / 2)
        .expect("punch");
    assert_eq!(
        base.resident_bytes().expect("resident"),
        BASE_PAGES / 2 * page_size(),
        "hole punch did not free physical pages"
    );
    // A punched page is a MISSING fault: the handler "refetches" it and
    // the guest sees the same bytes — one page returns to residency.
    assert_eq!(
        vms[3].base_win.read_page(BASE_PAGES / 2 + 1),
        base_pattern(BASE_PAGES / 2 + 1)
    );
    assert_eq!(
        base.resident_bytes().expect("resident"),
        (BASE_PAGES / 2 + 1) * page_size()
    );

    // ── Phase 6: shut down half the fleet ───────────────────────────────
    // Explicit reclaim first (R4.5: deletion is explicit), then unmap.
    for vm in &vms[VMS / 2..] {
        vm.own.punch_hole(0, PRIVATE_PAGES).expect("punch own");
        assert_eq!(vm.own.resident_bytes().expect("resident"), 0);
    }
    let survivors_total: usize = base.resident_bytes().expect("resident")
        + vms[..VMS / 2]
            .iter()
            .map(|vm| vm.own.resident_bytes().expect("resident"))
            .sum::<usize>();
    assert_eq!(
        survivors_total,
        (BASE_PAGES / 2 + 1 + (VMS / 2) * DIVERGE) * page_size(),
        "post-shutdown fleet holds unexpected physical memory"
    );
    // Survivors are untouched by their neighbors' teardown.
    let got = vms[0].own_win.read_page(0);
    let mut want = base_pattern(0);
    want[0..8].copy_from_slice(&0xD1BE_0000u64.to_le_bytes());
    assert_eq!(got, want);
}

/// A memory-reclaim-by-"swap" experiment: dirty guest pages are
/// written back to a real disk file, the shmem backing is punched free —
/// PHYSICAL MEMORY GOES TO ZERO — and later guest touches demand-page the
/// bytes back in from disk, one page per touch. The guest's own written
/// content round-trips exactly.
#[test]
fn reclaim_by_writeback_to_disk_swaps_out_and_demand_pages_back_in() {
    const PAGES: usize = 64;
    let region = Arc::new(HostRegion::new(PAGES).expect("region"));
    let view = Arc::new(GuestView::map(&region, 0, PAGES).expect("view"));
    let (uffd, _) = Uffd::new(required_features()).expect("uffd");
    uffd.register_all(&view).expect("register");
    let uffd = Arc::new(uffd);

    // The "NVMe segment": a real file on persistent disk (/var/tmp, never
    // tmpfs), opened O_DIRECT to keep this test's disk copy out of cache.
    // Page I/O moves straight between our aligned buffers and the device;
    // no page-cache copy of the data exists anywhere to begin with.
    let path = format!("/var/tmp/blockd-swap-test-{}", std::process::id());
    let file = Arc::new(DirectFile::create(&path).expect("segment file"));
    let swapped_out = Arc::new(AtomicBool::new(false));

    // Handler: before writeback, first touches are zero-fills; after the
    // swap-out, refills read the disk segment (the store tier of R2.3).
    {
        let (region, view, uffd, file, swapped_out) = (
            region.clone(),
            view.clone(),
            uffd.clone(),
            file.clone(),
            swapped_out.clone(),
        );
        thread::spawn(move || {
            while let Ok(events) = uffd.read_events() {
                for event in events {
                    let addr = event.address & !(page_size() - 1);
                    let page = (addr - view.addr_of(0)) / page_size();
                    assert!(event.missing(), "unexpected fault kind: {event:?}");
                    let mut buf = PageBuf::new();
                    if swapped_out.load(Ordering::SeqCst) {
                        file.read_page(page, &mut buf).expect("swap-in read");
                    }
                    region.write_page(page, buf.as_slice());
                    uffd.continue_range(addr, page_size(), false)
                        .expect("continue");
                }
            }
        });
    }

    // The guest writes every page: 64 pages of genuinely dirty state.
    for page in 0..PAGES {
        view.write_word(page, 0x5AB0_0000 + page as u64);
    }
    assert_eq!(
        region.resident_bytes().expect("resident"),
        PAGES * page_size(),
        "dirty working set not physical"
    );

    // ── Swap out ────────────────────────────────────────────────────────
    // Writeback: capture every page through the daemon view onto disk,
    // durably, via O_DIRECT. Direct I/O
    // means the ONLY RAM copy of the data is the shmem page we are about
    // to reclaim.
    let mut buf = PageBuf::new();
    for page in 0..PAGES {
        buf.as_mut_slice().copy_from_slice(&region.read_page(page));
        file.write_page(page, &buf).expect("writeback");
    }
    file.sync().expect("fsync");
    swapped_out.store(true, Ordering::SeqCst);

    // Reclaim: drop the guest's PTEs, then free the shmem backing itself.
    view.evict(0, PAGES).expect("evict");
    region.punch_hole(0, PAGES).expect("punch");
    assert_eq!(
        region.resident_bytes().expect("resident"),
        0,
        "swap-out left physical pages behind"
    );
    assert!(
        view.resident().expect("mincore").iter().all(|m| !m),
        "swap-out left the backing in memory"
    );

    // ── Swap in, on demand ──────────────────────────────────────────────
    // Touch only the even pages: exactly those — and only those — come
    // back, with the guest's own bytes intact.
    for page in (0..PAGES).step_by(2) {
        assert_eq!(
            view.read_word(page),
            0x5AB0_0000 + page as u64,
            "page {page} lost its dirty content across the swap"
        );
    }
    assert_eq!(
        region.resident_bytes().expect("resident"),
        PAGES / 2 * page_size(),
        "demand paging brought back more than was touched"
    );

    // The rest follows when touched; the region converges to fully
    // resident with every byte the guest wrote.
    for page in (1..PAGES).step_by(2) {
        assert_eq!(view.read_word(page), 0x5AB0_0000 + page as u64);
    }
    assert_eq!(
        region.resident_bytes().expect("resident"),
        PAGES * page_size()
    );
    std::fs::remove_file(&path).expect("cleanup");
}
