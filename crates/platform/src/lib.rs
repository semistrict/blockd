//! Process-wide platform properties shared by the deterministic core and
//! operating-system boundary.

use std::sync::OnceLock;

/// The system's native base-page size.
///
/// Every blockd page uses this size: memory, cache, capture, segments, wire
/// transfers, and simulation. Persisted records carry the value and reject a
/// restore on a host with an incompatible page size.
pub fn page_size() -> usize {
    static PAGE_SIZE: OnceLock<usize> = OnceLock::new();
    *PAGE_SIZE.get_or_init(detect_page_size)
}

#[cfg(unix)]
fn detect_page_size() -> usize {
    #[cfg(feature = "test-page-size")]
    if let Some(size) = std::env::var("BLOCKD_TEST_PAGE_SIZE")
        .ok()
        .and_then(|value| value.parse().ok())
    {
        return validate_page_size(size);
    }
    // SAFETY: sysconf has no pointer arguments or side effects.
    let size = unsafe { libc::sysconf(libc::_SC_PAGESIZE) };
    validate_page_size(usize::try_from(size).expect("positive system page size"))
}

#[cfg(unix)]
fn validate_page_size(size: usize) -> usize {
    assert!(size.is_power_of_two(), "page size is not a power of two");
    assert!(size >= 4096, "page size is smaller than 4 KiB");
    assert!(u32::try_from(size).is_ok(), "page size does not fit u32");
    size
}

#[cfg(not(unix))]
fn detect_page_size() -> usize {
    // All currently supported non-Unix targets use this base-page size.
    4096
}
