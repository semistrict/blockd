//! The real-kernel proof of the guest-memory machinery (R9.1): every
//! memory-manipulation primitive the deterministic core's seam models,
//! exercised against live Linux — memfd dual views, minor faults,
//! `UFFDIO_CONTINUE` fills (demand and prefetch), shared base pages,
//! write-protect capture ordering, and both eviction flavors.
//!
//! Faults are taken by a touching thread and resolved by a handler thread,
//! exactly the split the production daemon has.

#![cfg(target_os = "linux")]
// Truncating page indexes into pattern tags is deliberate; threads are the
// whole point here — this is the nondeterministic side of the seam, where
// the core's single-thread rule does not apply.
#![allow(clippy::cast_possible_truncation, clippy::disallowed_methods)]

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;

use blockd_hostmem::{FaultEvent, GuestView, HostRegion, PAGE_SIZE, Uffd, UffdFeatures};

fn pattern(tag: u8) -> Vec<u8> {
    let mut bytes = vec![0u8; PAGE_SIZE];
    for (i, b) in bytes.iter_mut().enumerate() {
        *b = tag ^ (i as u8);
    }
    bytes
}

fn required_features() -> u64 {
    UffdFeatures::PAGEFAULT_FLAG_WP | UffdFeatures::MINOR_SHMEM | UffdFeatures::WP_HUGETLBFS_SHMEM
}

#[test]
fn kernel_grants_the_features_this_design_needs() {
    let (_uffd, features) = Uffd::new(required_features()).expect("userfaultfd handshake");
    assert!(
        features.has(UffdFeatures::MINOR_SHMEM),
        "kernel lacks shmem minor faults: {features:?}"
    );
    assert!(
        features.has(UffdFeatures::PAGEFAULT_FLAG_WP),
        "kernel lacks uffd write-protect: {features:?}"
    );
    assert!(
        features.has(UffdFeatures::WP_HUGETLBFS_SHMEM),
        "kernel lacks shmem write-protect: {features:?}"
    );
    eprintln!("uffd features: {features:?}");
}

/// A demand fill: first guest touch → MISSING fault (kernel-verified:
/// with MINOR registration alone, absent shmem pages silently
/// zero-allocate — MISSING is what makes first touches trap) → daemon
/// writes bytes through its view → CONTINUE → guest observes exactly
/// those bytes. Refaults don't happen while the mapping stays.
#[test]
fn first_touch_faults_missing_and_resolves_via_continue() {
    let region = Arc::new(HostRegion::new(4).expect("region"));
    let view = Arc::new(GuestView::map(&region, 0, 4).expect("view"));
    let (uffd, _) = Uffd::new(required_features()).expect("uffd");
    uffd.register_all(&view).expect("register");
    let uffd = Arc::new(uffd);
    let faults = Arc::new(AtomicU64::new(0));

    let handler = {
        let (region, view, uffd, faults) =
            (region.clone(), view.clone(), uffd.clone(), faults.clone());
        thread::spawn(move || {
            let event = uffd.read_event().expect("event");
            assert!(event.missing(), "expected a missing fault: {event:?}");
            assert_eq!(event.address & !(PAGE_SIZE - 1), view.addr_of(2));
            faults.fetch_add(1, Ordering::SeqCst);
            // Fill = populate via the daemon view, then CONTINUE.
            region.write_page(2, &pattern(0x5A));
            uffd.continue_range(view.addr_of(2), PAGE_SIZE, false)
                .expect("continue");
        })
    };

    let got = view.read_page(2); // blocks until the handler resolves
    handler.join().expect("handler");
    assert_eq!(got, pattern(0x5A));
    // Second read: still mapped, no second fault.
    assert_eq!(view.read_page(2), pattern(0x5A));
    assert_eq!(faults.load(Ordering::SeqCst), 1);
}

/// Prefetch (R6.2): populate + eager CONTINUE before any guest touch.
/// The guest's later read takes NO fault at all.
#[test]
fn proactive_continue_prefetches_without_any_fault() {
    let region = HostRegion::new(4).expect("region");
    let view = GuestView::map(&region, 0, 4).expect("view");
    let (uffd, _) = Uffd::new(required_features()).expect("uffd");
    uffd.register_all(&view).expect("register");

    region.write_page(1, &pattern(0x7B));
    uffd.continue_range(view.addr_of(1), PAGE_SIZE, false)
        .expect("eager continue");

    // No handler thread exists: if this faulted, the read would hang and
    // the test harness would time out. It must not fault.
    assert_eq!(view.read_page(1), pattern(0x7B));
}

/// R5.3 in hardware: two guest views of the same memfd range resolve to
/// the SAME physical page — a daemon-side write is visible through both,
/// and the kernel reports one resident copy, not two.
#[test]
fn shared_base_views_map_one_physical_copy() {
    let region = Arc::new(HostRegion::new(8).expect("region"));
    let fork_a = Arc::new(GuestView::map(&region, 0, 8).expect("fork a"));
    let fork_b = Arc::new(GuestView::map(&region, 0, 8).expect("fork b"));
    let (uffd_a, _) = Uffd::new(required_features()).expect("uffd a");
    let (uffd_b, _) = Uffd::new(required_features()).expect("uffd b");
    uffd_a.register_all(&fork_a).expect("register a");
    uffd_b.register_all(&fork_b).expect("register b");

    // One populate; both forks get eager CONTINUEs (the shared-tier hit:
    // no second copy, no second I/O).
    for page in 0..8 {
        region.write_page(page, &pattern(page as u8));
        uffd_a
            .continue_range(fork_a.addr_of(page), PAGE_SIZE, false)
            .expect("continue a");
        uffd_b
            .continue_range(fork_b.addr_of(page), PAGE_SIZE, false)
            .expect("continue b");
    }
    for page in 0..8 {
        assert_eq!(fork_a.read_page(page), pattern(page as u8));
        assert_eq!(fork_b.read_page(page), pattern(page as u8));
    }

    // Same physical page: a write through the daemon view appears in both
    // forks' mappings instantly.
    let mut changed = pattern(0xEE);
    changed[0] = 0xFF;
    region.write_page(3, &changed);
    assert_eq!(fork_a.read_page(3), changed);
    assert_eq!(fork_b.read_page(3), changed);

    // And the backing holds exactly 8 pages — not 8 per fork.
    let resident = region.resident_bytes().expect("resident");
    assert_eq!(resident, 8 * PAGE_SIZE, "forks duplicated pages");
}

/// The capture boundary (R2.4/R3.8): a write-protected page traps the
/// writer BEFORE the store lands; the capture reads the pre-write bytes
/// through the daemon view while the writer is blocked; clearing WP
/// retires the store.
#[test]
fn write_protect_traps_before_the_store_and_capture_reads_old_bytes() {
    let region = Arc::new(HostRegion::new(2).expect("region"));
    let view = Arc::new(GuestView::map(&region, 0, 2).expect("view"));
    let (uffd, _) = Uffd::new(required_features()).expect("uffd");
    uffd.register_all(&view).expect("register");
    let uffd = Arc::new(uffd);

    // Fill page 0 writable, then arm write protection (as a capture does).
    region.write_page(0, &pattern(0x11));
    uffd.continue_range(view.addr_of(0), PAGE_SIZE, false)
        .expect("continue");
    assert_eq!(view.read_page(0), pattern(0x11));
    uffd.writeprotect(view.addr_of(0), PAGE_SIZE, true)
        .expect("arm wp");

    let (captured_tx, captured_rx) = mpsc::channel::<Vec<u8>>();
    let handler = {
        let (region, view, uffd) = (region.clone(), view.clone(), uffd.clone());
        thread::spawn(move || {
            let event = uffd.read_event().expect("event");
            assert!(
                event.wp && event.write,
                "expected a write-protect fault: {event:?}"
            );
            assert_eq!(event.address & !(PAGE_SIZE - 1), view.addr_of(0));
            // The writer is blocked: what the capture reads NOW is the
            // pre-write state — the R3.8-critical ordering.
            captured_tx.send(region.read_page(0)).expect("send");
            uffd.writeprotect(view.addr_of(0), PAGE_SIZE, false)
                .expect("clear wp");
        })
    };

    view.write_word(0, 0xDEAD_BEEF_CAFE_F00D); // blocks on the WP fault
    handler.join().expect("handler");
    let captured = captured_rx.recv().expect("captured");
    assert_eq!(captured, pattern(0x11), "capture saw the in-flight store");
    // After unprotect the store retired; both views see it.
    assert_eq!(view.read_word(0), 0xDEAD_BEEF_CAFE_F00D);
    let now = region.read_page(0);
    assert_eq!(now[0..8], 0xDEAD_BEEF_CAFE_F00Du64.to_le_bytes());
}

/// CONTINUE can install the mapping already write-protected — the
/// restore/prefetch path arms captures without a separate WRITEPROTECT
/// round-trip.
#[test]
fn continue_can_install_write_protected() {
    let region = Arc::new(HostRegion::new(1).expect("region"));
    let view = Arc::new(GuestView::map(&region, 0, 1).expect("view"));
    let (uffd, _) = Uffd::new(required_features()).expect("uffd");
    uffd.register_all(&view).expect("register");
    let uffd = Arc::new(uffd);

    region.write_page(0, &pattern(0x33));
    uffd.continue_range(view.addr_of(0), PAGE_SIZE, true)
        .expect("continue wp");
    // Reads pass through...
    assert_eq!(view.read_page(0), pattern(0x33));

    // ...writes trap.
    let handler = {
        let (view, uffd) = (view.clone(), uffd.clone());
        thread::spawn(move || {
            let event = uffd.read_event().expect("event");
            assert!(event.wp && event.write, "not a wp fault: {event:?}");
            uffd.writeprotect(view.addr_of(0), PAGE_SIZE, false)
                .expect("clear");
        })
    };
    view.write_word(0, 7);
    handler.join().expect("handler");
    assert_eq!(view.read_word(0), 7);
}

/// Evict (R2.4): `MADV_DONTNEED` on a shared view drops PTEs only. The
/// page-cache copy survives — the refault is a MINOR fault and CONTINUE
/// restores the very same bytes, dirty writes included. Eviction of a
/// shared mapping can never lose data.
#[test]
fn evict_drops_ptes_keeps_data_and_refaults_minor() {
    let region = Arc::new(HostRegion::new(2).expect("region"));
    let view = Arc::new(GuestView::map(&region, 0, 2).expect("view"));
    let (uffd, _) = Uffd::new(required_features()).expect("uffd");
    uffd.register_all(&view).expect("register");
    let uffd = Arc::new(uffd);

    region.write_page(0, &pattern(0x44));
    uffd.continue_range(view.addr_of(0), PAGE_SIZE, false)
        .expect("continue");
    view.write_word(0, 0xA11C_E000_0000_0001); // guest dirties the page

    view.evict(0, 1).expect("evict");

    let faults = Arc::new(AtomicU64::new(0));
    let handler = {
        let (view, uffd, faults) = (view.clone(), uffd.clone(), faults.clone());
        thread::spawn(move || {
            let event = uffd.read_event().expect("event");
            assert!(event.minor, "refault after evict must be minor: {event:?}");
            faults.fetch_add(1, Ordering::SeqCst);
            // No repopulate needed: the page cache still has the bytes.
            uffd.continue_range(view.addr_of(0), PAGE_SIZE, false)
                .expect("continue");
        })
    };
    let word = view.read_word(0);
    handler.join().expect("handler");
    assert_eq!(word, 0xA11C_E000_0000_0001, "evict lost a dirty write");
    assert_eq!(faults.load(Ordering::SeqCst), 1);
}

/// Reclaiming the backing (R2.7): punching a hole frees the page-cache
/// copy. The next guest touch must TRAP (never silently zero-fill) so the
/// daemon can refetch from durable storage — the fill door stays the only
/// door (R8.1).
#[test]
fn hole_punch_frees_the_backing_and_the_next_touch_traps_for_refill() {
    let region = Arc::new(HostRegion::new(4).expect("region"));
    let view = Arc::new(GuestView::map(&region, 0, 4).expect("view"));
    let (uffd, _) = Uffd::new(required_features()).expect("uffd");
    uffd.register_all(&view).expect("register");
    let uffd = Arc::new(uffd);

    region.write_page(2, &pattern(0x66));
    uffd.continue_range(view.addr_of(2), PAGE_SIZE, false)
        .expect("continue");
    assert_eq!(view.read_page(2), pattern(0x66));
    assert_eq!(region.resident_bytes().expect("resident"), PAGE_SIZE);

    // Drop the PTE, then free the backing itself.
    view.evict(2, 1).expect("evict");
    region.punch_hole(2, 1).expect("punch");
    assert_eq!(
        region.resident_bytes().expect("resident"),
        0,
        "hole punch did not free the page"
    );

    // The refault: the daemon "refetches from the store" (writes fresh
    // bytes) and CONTINUEs — the guest sees the refetched content, not
    // silent zeros.
    let handler = {
        let (region, view, uffd) = (region.clone(), view.clone(), uffd.clone());
        thread::spawn(move || {
            let event = uffd.read_event().expect("event");
            // Kernel-verified: the backing is gone, so this is a MISSING
            // fault (a present-but-unmapped page would be MINOR).
            assert!(
                event.missing(),
                "post-punch touch must trap as missing: {event:?}"
            );
            region.write_page(2, &pattern(0x77));
            uffd.continue_range(view.addr_of(2), PAGE_SIZE, false)
                .expect("continue");
        })
    };
    let got = view.read_page(2);
    handler.join().expect("handler");
    assert_eq!(got, pattern(0x77), "refill bytes did not reach the guest");
}

/// The full capture cycle end to end, exactly as the daemon runs it:
/// fill → guest dirties → capture (WP-arm + read via daemon view) →
/// guest's next write refaults WP → unprotect → next capture sees only
/// the new delta. This is R2.4 + R3.3 + R3.8 machinery in one loop.
#[test]
fn capture_cycle_write_protect_read_rearm() {
    let region = Arc::new(HostRegion::new(1).expect("region"));
    let view = Arc::new(GuestView::map(&region, 0, 1).expect("view"));
    let (uffd, _) = Uffd::new(required_features()).expect("uffd");
    uffd.register_all(&view).expect("register");
    let uffd = Arc::new(uffd);

    // Fill + first guest write (installs writable via minor-fault path).
    region.write_page(0, &pattern(0x00));
    uffd.continue_range(view.addr_of(0), PAGE_SIZE, false)
        .expect("continue");
    view.write_word(0, 1);

    let (events_tx, events_rx) = mpsc::channel::<FaultEvent>();
    let handler = {
        let (view, uffd) = (view.clone(), uffd.clone());
        thread::spawn(move || {
            for _ in 0..2 {
                let event = uffd.read_event().expect("event");
                events_tx.send(event).expect("send");
                uffd.writeprotect(view.addr_of(0), PAGE_SIZE, false)
                    .expect("clear");
            }
        })
    };

    for round in 2..4u64 {
        // Capture: arm WP, read the stable bytes through the daemon view.
        uffd.writeprotect(view.addr_of(0), PAGE_SIZE, true)
            .expect("arm");
        let captured = region.read_page(0);
        assert_eq!(captured[0..8], (round - 1).to_le_bytes());
        // Guest writes again: traps, handler unprotects, store retires.
        view.write_word(0, round);
        let event = events_rx.recv().expect("event");
        assert!(event.wp && event.write, "round {round}: {event:?}");
        assert_eq!(view.read_word(0), round);
    }
    handler.join().expect("handler");
}
