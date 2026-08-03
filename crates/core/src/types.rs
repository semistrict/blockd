//! Identifiers and time. All fixed-width, float-free (R10.2 spirit).

use std::fmt;

/// Simulated (or, in production, monotonic) time in nanoseconds since start.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct SimTime(pub u64);

impl SimTime {
    pub const ZERO: SimTime = SimTime(0);

    #[must_use]
    pub const fn after(self, nanos: u64) -> SimTime {
        SimTime(self.0 + nanos)
    }

    pub const fn nanos(self) -> u64 {
        self.0
    }
}

impl fmt::Debug for SimTime {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}ns", self.0)
    }
}

pub const fn micros(n: u64) -> u64 {
    n * 1_000
}

pub const fn millis(n: u64) -> u64 {
    n * 1_000_000
}

pub const fn secs(n: u64) -> u64 {
    n * 1_000_000_000
}

macro_rules! id_type {
    ($(#[$doc:meta])* $name:ident($inner:ty)) => {
        $(#[$doc])*
        #[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        pub struct $name(pub $inner);

        impl fmt::Debug for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, concat!(stringify!($name), "({})"), self.0)
            }
        }
    };
}

id_type!(
    /// A host in the cluster; one daemon per host (R9.1).
    HostId(u16)
);
id_type!(
    /// A volume set: one memory volume plus disk volumes (R1.1). Never reused (R6.5).
    VsetId(u64)
);
id_type!(
    /// Volume index within a vset. Index 0 is the memory volume.
    VolumeIdx(u8)
);
id_type!(
    /// Page number within a volume.
    PageNo(u32)
);
id_type!(
    /// A whole-vset consistency epoch (R1.2).
    Epoch(u64)
);
id_type!(
    /// A page-object generation: page contents are write-once per
    /// `(page, gen)`, so overwriting can never tear the previous copy.
    Gen(u64)
);
id_type!(
    /// Per-vset journal record sequence number.
    JournalSeq(u64)
);
id_type!(
    /// A segment: one write-once blob of compressed page entries, identical
    /// bytes on local disk and in the object store (R8.4).
    SegId(u64)
);

/// Size of a guest-visible page in bytes.
pub const PAGE_SIZE: usize = 4096;

impl VolumeIdx {
    pub const MEMORY: VolumeIdx = VolumeIdx(0);

    pub fn is_memory(self) -> bool {
        self == VolumeIdx::MEMORY
    }
}

/// A volume within a vset.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VolumeId {
    pub vset: VsetId,
    pub idx: VolumeIdx,
}

impl fmt::Debug for VolumeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v{}/{}", self.vset.0, self.idx.0)
    }
}

/// A page within a volume — the unit of caching, faulting and transfer.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PageId {
    pub volume: VolumeId,
    pub page: PageNo,
}

impl fmt::Debug for PageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}p{}", self.volume, self.page.0)
    }
}
