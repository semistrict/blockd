//! blockd-hostmem: the real Linux implementation of the guest-memory
//! boundary the deterministic core models (R9.1, R10.1).
//!
//! One memfd is the truth for a region of guest memory. The daemon maps it
//! once (the *daemon view* — the `HostMap` side captures read through) and
//! each VM maps it again (a *guest view*) registered with userfaultfd in
//! MISSING+MINOR+WP mode (kernel-verified: MINOR alone silently
//! zero-allocates absent shmem pages — MISSING is what makes first
//! touches and post-punch refills trap):
//!
//! - **Fill** = write the bytes into the daemon view (populating the shared
//!   page cache) and resolve the guest's fault — MISSING for first
//!   touches, MINOR for evicted-PTE refaults — with `UFFDIO_CONTINUE`:
//!   zero copy, the guest maps the very page the daemon wrote, and every
//!   fault kind resolves through this one door.
//! - **Prefetch** (R6.2) = the same two steps issued eagerly, before any
//!   fault.
//! - **Shared bases** (R5.3) = many guest views mapping the same memfd
//!   range; every view resolves to the one physical page.
//! - **`WriteProtect` / `Unprotect`** = `UFFDIO_WRITEPROTECT` arming and the
//!   WP-flagged fault it produces; the capture reads the pre-write bytes
//!   through the daemon view while the writer is still blocked (R3.8's
//!   ordering, in hardware).
//! - **Evict** = `MADV_DONTNEED` on a guest view (drops PTEs, the page
//!   cache copy survives — refaults are minor faults), and
//!   `FALLOC_FL_PUNCH_HOLE` on the memfd when the *backing* itself is
//!   reclaimed.
//!
//! This crate is the audited home of `unsafe`; the deterministic core
//! (`blockd-core`) stays `#![forbid(unsafe_code)]` and drives these same
//! semantics through its `Effect` seam. The integration tests
//! (`tests/uffd_linux.rs`) prove the machinery against a live kernel.

#[cfg(target_os = "linux")]
mod linux;

pub use blockd_platform::page_size;

#[cfg(target_os = "linux")]
pub use linux::{
    DirectFile, FaultEvent, GuestView, HostRegion, PageBuf, Uffd, UffdFeatures,
    file_resident_bytes, punch_hole_file, recv_with_fd,
};
