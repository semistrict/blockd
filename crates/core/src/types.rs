//! Identifiers and time. All fixed-width, float-free (R10.2 spirit).

use std::fmt;

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

/// A host in the cluster; one daemon per host (R9.1).
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HostId(u32);

impl HostId {
    /// Constructs an identifier from its durable numeric representation.
    pub const fn new(value: u32) -> Self {
        Self(value)
    }

    /// Returns the durable numeric representation.
    pub const fn get(self) -> u32 {
        self.0
    }
}

impl From<u32> for HostId {
    fn from(value: u32) -> Self {
        Self::new(value)
    }
}

impl From<HostId> for u32 {
    fn from(value: HostId) -> Self {
        value.get()
    }
}

impl fmt::Debug for HostId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "HostId({})", self.0)
    }
}

impl fmt::Display for HostId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

id_type!(
    /// One independently managed memory or block volume. Never reused (R6.5).
    VolumeId(u64)
);
id_type!(
    /// A VM identity assigned by the host control plane, never trusted from
    /// guest-provided protocol bytes.
    VmId(u64)
);
id_type!(
    /// Page number within a volume.
    PageNo(u32)
);
id_type!(
    /// A memory-volume snapshot epoch.
    Epoch(u64)
);
id_type!(
    /// A page-object generation: page contents are write-once per
    /// `(page, gen)`, so overwriting can never tear the previous copy.
    Gen(u64)
);
id_type!(
    /// Per-volume journal record sequence number.
    JournalSeq(u64)
);
id_type!(
    /// A blx: one write-once blob of compressed page entries, identical
    /// bytes on local disk and in the object store (R8.4).
    ObjectId(u64)
);

/// Size of every memory, cache, storage, and wire page in this process.
pub use blockd_platform::page_size;

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
