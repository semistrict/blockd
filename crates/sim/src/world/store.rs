//! The object store, holding exactly the contract R4.6 demands and nothing
//! more: strong read-after-write consistency, conditional writes
//! (compare-and-swap by version), and objects up to 64 MiB — the limit is
//! enforced, because production code must never depend on more. Keys are the
//! real production keys (`blockd_core::layout`); values are raw bytes, and
//! bit rot can damage them — the daemon verifies every payload it reads
//! (R8.1), the store gives it no help. Outages (R8.3) fail every operation
//! loudly; nothing is ever weakly consistent.
//!
//! Operations linearize at submission; the returned time is when the harness
//! should deliver the outcome.

use std::collections::{BTreeMap, BTreeSet};
use std::fmt;

use blockd_core::head::HeadRecord;
use blockd_core::journal::JournalRecord;
use blockd_core::layout;
use blockd_core::mapleaf::MapLeaf;
use blockd_core::types::SimTime;
use blockd_core::types::{Gen, PageId, VolumeId, VsetId, page_size};

use crate::rng::Pcg64;

/// R4.6: the largest object the contract guarantees.
pub const MAX_OBJECT_BYTES: usize = 64 << 20;

#[derive(Clone, Debug)]
pub struct StoreConfig {
    pub latency_min: u64,
    pub latency_max: u64,
    /// Throughput term: added nanoseconds per payload byte.
    pub ns_per_byte: u64,
}

impl StoreConfig {
    /// Warm-object-store-class latencies (R2.3's tens of milliseconds,
    /// ~200 MB/s per stream).
    pub fn s3() -> StoreConfig {
        StoreConfig {
            latency_min: 5_000_000,
            latency_max: 60_000_000,
            ns_per_byte: 5,
        }
    }
}

/// Object version for CAS. Versions start at 1 and only grow.
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Version(pub u64);

impl fmt::Debug for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v{}", self.0)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum StoreError {
    /// CAS expectation not met; carries the actual current version.
    CasConflict { actual: Option<Version> },
    /// Outage: the operation reached nothing (R8.3).
    Unavailable,
    /// The object exceeds the 64 MiB contract (R4.6). The daemon must never
    /// trigger this; the oracle asserts the counter stays zero.
    TooLarge,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct PutCounters {
    pub attempts: u64,
    pub successes: u64,
    pub attempted_bytes: u64,
    pub successful_bytes: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(usize)]
pub enum StoreObjectKind {
    Head = 0,
    Manifest = 1,
    Segment = 2,
    Leaf = 3,
    ResumeSet = 4,
    Base = 5,
    Other = 6,
}

impl StoreObjectKind {
    pub const COUNT: usize = 7;
}

/// Successful object sizes in the upper-bound buckets
/// 4 KiB, 64 KiB, 1 MiB, 8 MiB, 32 MiB, 64 MiB, and oversized.
pub const OBJECT_SIZE_BUCKETS: usize = 7;

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StoreCounters {
    pub gets: u64,
    /// Successful unconditional and conditional writes retained for older
    /// reports. The detailed attempt/success split below is authoritative.
    pub puts: u64,
    pub deletes: u64,
    pub put_attempts: u64,
    pub put_successes: u64,
    pub cas_attempts: u64,
    pub cas_successes: u64,
    pub cas_conflicts: u64,
    pub unavailable: u64,
    pub too_large: u64,
    pub bytes_put: u64,
    /// Bytes belonging to a key+payload combination that succeeded for the
    /// first time during this store lifetime.
    pub unique_bytes: u64,
    /// Bytes in attempts whose exact key+payload combination had already
    /// succeeded. This includes successful idempotent retries and retries
    /// that encounter a later outage.
    pub retry_bytes: u64,
    pub bytes_got: u64,
    pub bitflips: u64,
    pub puts_by_kind: [PutCounters; StoreObjectKind::COUNT],
    pub object_size_histogram: [u64; OBJECT_SIZE_BUCKETS],
    /// Raw logical page bytes whose generation changed between successive
    /// successfully published archive heads. This is the explicit baseline
    /// used by hot-working-set amplification tests.
    pub logical_changed_bytes: u64,
}

pub struct ObjectStore {
    config: StoreConfig,
    objects: BTreeMap<String, (Version, SimTime, Vec<u8>)>,
    out: bool,
    attempted_payloads: BTreeSet<(String, usize, u32)>,
    seen_payloads: BTreeSet<(String, usize, u32)>,
    archived_generations: BTreeMap<VsetId, BTreeMap<PageId, Gen>>,
    pub counters: StoreCounters,
}

impl ObjectStore {
    pub fn new(config: StoreConfig) -> ObjectStore {
        ObjectStore {
            config,
            objects: BTreeMap::new(),
            out: false,
            attempted_payloads: BTreeSet::new(),
            seen_payloads: BTreeSet::new(),
            archived_generations: BTreeMap::new(),
            counters: StoreCounters::default(),
        }
    }

    fn object_kind(key: &str) -> StoreObjectKind {
        if key.ends_with("/head") {
            StoreObjectKind::Head
        } else if key.contains("/m/") {
            StoreObjectKind::Manifest
        } else if key.starts_with("b/") {
            StoreObjectKind::Base
        } else if key.ends_with("/rs") {
            StoreObjectKind::ResumeSet
        } else if key.contains("/s/") {
            StoreObjectKind::Segment
        } else if key.contains("/l/") || key.contains("/lb/") {
            StoreObjectKind::Leaf
        } else {
            StoreObjectKind::Other
        }
    }

    fn write_attempt(&mut self, key: &str, bytes: &[u8], cas: bool) -> StoreObjectKind {
        let kind = Self::object_kind(key);
        self.counters.put_attempts += 1;
        self.counters.puts_by_kind[kind as usize].attempts += 1;
        self.counters.puts_by_kind[kind as usize].attempted_bytes += bytes.len() as u64;
        if cas {
            self.counters.cas_attempts += 1;
        }
        let identity = (
            key.to_owned(),
            bytes.len(),
            blockd_core::format::crc32c(bytes),
        );
        if !self.attempted_payloads.insert(identity) {
            self.counters.retry_bytes += bytes.len() as u64;
        }
        kind
    }

    fn write_success(&mut self, key: &str, bytes: &[u8], kind: StoreObjectKind, cas: bool) {
        self.counters.puts += 1;
        self.counters.put_successes += 1;
        self.counters.bytes_put += bytes.len() as u64;
        self.counters.puts_by_kind[kind as usize].successes += 1;
        self.counters.puts_by_kind[kind as usize].successful_bytes += bytes.len() as u64;
        if cas {
            self.counters.cas_successes += 1;
        }
        let identity = (
            key.to_owned(),
            bytes.len(),
            blockd_core::format::crc32c(bytes),
        );
        if self.seen_payloads.insert(identity) {
            self.counters.unique_bytes += bytes.len() as u64;
        }
        let bucket = match bytes.len() {
            0..=4096 => 0,
            4097..=65_536 => 1,
            65_537..=1_048_576 => 2,
            1_048_577..=8_388_608 => 3,
            8_388_609..=33_554_432 => 4,
            33_554_433..=67_108_864 => 5,
            _ => 6,
        };
        self.counters.object_size_histogram[bucket] += 1;
    }

    fn observe_archive_head(&mut self, key: &str, bytes: &[u8]) {
        let Some(encoded_vset) = key
            .strip_prefix("v/")
            .and_then(|rest| rest.split('/').next())
        else {
            return;
        };
        let Ok(raw_vset) = u64::from_str_radix(encoded_vset, 16) else {
            return;
        };
        let vset = VsetId(raw_vset);
        let Ok(head) = HeadRecord::decode(vset, bytes) else {
            return;
        };
        let Some(ptr) = head.manifest else {
            return;
        };
        let Some((_, _, record_bytes)) = self
            .objects
            .get(&layout::manifest_key(vset, ptr.fence, ptr.seq))
        else {
            return;
        };
        let Ok(record) = JournalRecord::decode(vset, record_bytes) else {
            return;
        };
        let mut current = BTreeMap::new();
        for leaf_ptr in record.leaves.values() {
            let (owner, leaf_key) = if leaf_ptr.base == 0 {
                (vset, layout::leaf_key(vset, leaf_ptr.fence, leaf_ptr.id))
            } else {
                (
                    VsetId(leaf_ptr.base),
                    layout::base_leaf_key(leaf_ptr.base, leaf_ptr.fence, leaf_ptr.id),
                )
            };
            let Some((_, _, leaf_bytes)) = self.objects.get(&leaf_key) else {
                continue;
            };
            let Ok(leaf) = MapLeaf::decode(owner, leaf_ptr.fence, leaf_ptr.id, leaf_bytes) else {
                continue;
            };
            for (idx, page, generation, _) in leaf.entries {
                current.insert(
                    PageId {
                        volume: VolumeId { vset, idx },
                        page,
                    },
                    generation,
                );
            }
        }
        current.extend(
            record
                .overlay
                .iter()
                .map(|(&page, &(generation, _))| (page, generation)),
        );
        let previous = self.archived_generations.entry(vset).or_default();
        let changed = current
            .iter()
            .filter(|(page, generation)| previous.get(page) != Some(generation))
            .count() as u64;
        self.counters.logical_changed_bytes += changed * page_size() as u64;
        *previous = current;
    }

    fn latency(&self, now: SimTime, rng: &mut Pcg64, bytes: usize) -> SimTime {
        now.after(
            rng.range(self.config.latency_min, self.config.latency_max)
                + self.config.ns_per_byte * bytes as u64,
        )
    }

    pub fn set_outage(&mut self, out: bool) {
        self.out = out;
    }

    pub fn is_out(&self) -> bool {
        self.out
    }

    /// Read an object: version plus payload bytes, verbatim, damage included.
    #[allow(clippy::type_complexity)]
    pub fn get(
        &mut self,
        now: SimTime,
        rng: &mut Pcg64,
        key: &str,
    ) -> (SimTime, Result<Option<(Version, Vec<u8>)>, StoreError>) {
        let found = self.objects.get(key).map(|(v, _, b)| (*v, b.clone()));
        let len = found.as_ref().map_or(0, |(_, b)| b.len());
        let at = self.latency(now, rng, len);
        if self.out {
            self.counters.unavailable += 1;
            return (at, Err(StoreError::Unavailable));
        }
        self.counters.gets += 1;
        self.counters.bytes_got += len as u64;
        (at, Ok(found))
    }

    /// Conditional write: `expected` is the version the caller believes is
    /// current (`None` = create only if absent). This is the CAS of R6.3.
    pub fn put_cas(
        &mut self,
        now: SimTime,
        rng: &mut Pcg64,
        key: &str,
        expected: Option<Version>,
        bytes: Vec<u8>,
    ) -> (SimTime, Result<Version, StoreError>) {
        let at = self.latency(now, rng, bytes.len());
        let kind = self.write_attempt(key, &bytes, true);
        if self.out {
            self.counters.unavailable += 1;
            return (at, Err(StoreError::Unavailable));
        }
        if bytes.len() > MAX_OBJECT_BYTES {
            self.counters.too_large += 1;
            return (at, Err(StoreError::TooLarge));
        }
        let actual = self.objects.get(key).map(|(v, _, _)| *v);
        if actual != expected {
            self.counters.cas_conflicts += 1;
            return (at, Err(StoreError::CasConflict { actual }));
        }
        let next = Version(actual.map_or(1, |v| v.0 + 1));
        self.write_success(key, &bytes, kind, true);
        if kind == StoreObjectKind::Head {
            self.observe_archive_head(key, &bytes);
        }
        self.objects.insert(key.to_owned(), (next, now, bytes));
        (at, Ok(next))
    }

    /// Unconditional write.
    pub fn put(
        &mut self,
        now: SimTime,
        rng: &mut Pcg64,
        key: &str,
        bytes: Vec<u8>,
    ) -> (SimTime, Result<Version, StoreError>) {
        let at = self.latency(now, rng, bytes.len());
        let kind = self.write_attempt(key, &bytes, false);
        if self.out {
            self.counters.unavailable += 1;
            return (at, Err(StoreError::Unavailable));
        }
        if bytes.len() > MAX_OBJECT_BYTES {
            self.counters.too_large += 1;
            return (at, Err(StoreError::TooLarge));
        }
        let next = Version(self.objects.get(key).map_or(1, |(v, _, _)| v.0 + 1));
        self.write_success(key, &bytes, kind, false);
        self.objects.insert(key.to_owned(), (next, now, bytes));
        (at, Ok(next))
    }

    /// Ranged read (S3 ranged GET): the store-tier fill path (R2.3). Ranges
    /// beyond the object's length are clamped; damage comes back verbatim.
    #[allow(clippy::type_complexity)]
    pub fn get_range(
        &mut self,
        now: SimTime,
        rng: &mut Pcg64,
        key: &str,
        offset: u64,
        len: u64,
    ) -> (SimTime, Result<Option<(Version, Vec<u8>)>, StoreError>) {
        let found = self.objects.get(key).map(|(v, _, b)| {
            let start = usize::try_from(offset.min(b.len() as u64)).expect("fits");
            let end = usize::try_from((offset + len).min(b.len() as u64)).expect("fits");
            (*v, b[start..end].to_vec())
        });
        let got = found.as_ref().map_or(0, |(_, b)| b.len());
        let at = self.latency(now, rng, got);
        if self.out {
            self.counters.unavailable += 1;
            return (at, Err(StoreError::Unavailable));
        }
        self.counters.gets += 1;
        self.counters.bytes_got += got as u64;
        (at, Ok(found))
    }

    /// Delete an object (GC and explicit discard only — R4.5).
    pub fn delete(
        &mut self,
        now: SimTime,
        rng: &mut Pcg64,
        key: &str,
    ) -> (SimTime, Result<bool, StoreError>) {
        let at = self.latency(now, rng, 0);
        if self.out {
            self.counters.unavailable += 1;
            return (at, Err(StoreError::Unavailable));
        }
        self.counters.deletes += 1;
        (at, Ok(self.objects.remove(key).is_some()))
    }

    /// List keys under a prefix (S3 LIST; GC and restore planning).
    pub fn list_prefix(
        &mut self,
        now: SimTime,
        rng: &mut Pcg64,
        prefix: &str,
    ) -> (SimTime, Result<Vec<String>, StoreError>) {
        let at = self.latency(now, rng, 0);
        if self.out {
            self.counters.unavailable += 1;
            return (at, Err(StoreError::Unavailable));
        }
        let keys = self
            .objects
            .range(prefix.to_owned()..)
            .take_while(|(k, _)| k.starts_with(prefix))
            .map(|(k, _)| k.clone())
            .collect();
        (at, Ok(keys))
    }

    /// Bit rot: flip one random bit of one random object's payload. Returns
    /// the victim key for the oracle.
    pub fn flip_random_bit(&mut self, rng: &mut Pcg64) -> Option<String> {
        self.flip_random_bit_where(rng, |_| true)
    }

    /// Flip one random bit in a random object whose key satisfies the
    /// predicate — targeted rot injection.
    pub fn flip_random_bit_where(
        &mut self,
        rng: &mut Pcg64,
        pred: impl Fn(&str) -> bool,
    ) -> Option<String> {
        let candidates: Vec<&String> = self
            .objects
            .iter()
            .filter(|(k, (_, _, b))| !b.is_empty() && pred(k))
            .map(|(k, _)| k)
            .collect();
        if candidates.is_empty() {
            return None;
        }
        let key = (*rng.pick(&candidates)).clone();
        let (_, _, blob) = self.objects.get_mut(&key).expect("picked");
        let bit = rng.below(blob.len() as u64 * 8);
        blob[usize::try_from(bit / 8).expect("fits")] ^= 1 << (bit % 8);
        self.counters.bitflips += 1;
        Some(key)
    }

    /// Number of stored objects (oracle use — not a store API).
    pub fn object_count(&self) -> usize {
        self.objects.len()
    }

    /// Non-mutating peek at an object's bytes (oracle/harness bookkeeping —
    /// not a store API; no latency, no counters, no trace).
    pub fn peek(&self, key: &str) -> Option<&[u8]> {
        self.objects.get(key).map(|(_, _, b)| b.as_slice())
    }

    /// Non-mutating versioned peek for control-plane recovery simulation.
    pub fn peek_versioned(&self, key: &str) -> Option<(Version, &[u8])> {
        self.objects
            .get(key)
            .map(|(version, _, bytes)| (*version, bytes.as_slice()))
    }

    /// Full listing with last-write times and bytes: the GC process's view
    /// of the bucket (LIST + GET, R9.3).
    pub fn snapshot(&self) -> Vec<(String, SimTime, Vec<u8>)> {
        self.objects
            .iter()
            .map(|(k, (_, at, b))| (k.clone(), *at, b.clone()))
            .collect()
    }
}

#[cfg(test)]
mod assignment_tests {
    use blockd_core::head::{HeadRecord, StashAssignment};
    use blockd_core::layout;
    use blockd_core::types::{HostId, VsetId};

    use super::*;
    use crate::rng::Pcg64;

    #[test]
    fn competing_assignment_cas_writes_linearize_to_exactly_one_winner() {
        let vset = VsetId(9);
        let key = layout::head_key(vset);
        let base = HeadRecord {
            vset,
            holder: HostId(0),
            fence: 4,
            manifest: None,
            stash: Some(StashAssignment {
                assignment_epoch: 1,
                active_peer: HostId(1),
                active_assignment_epoch: 1,
                transition_peer: None,
                membership_epoch: 7,
            }),
            retired_stashes: Vec::new(),
        };
        let mut store = ObjectStore::new(StoreConfig::s3());
        let mut rng = Pcg64::new(71, 0);
        let (_, created) = store.put_cas(SimTime::ZERO, &mut rng, &key, None, base.encode());
        let version = created.expect("create head");

        let proposal = |transition_peer| HeadRecord {
            stash: Some(StashAssignment {
                assignment_epoch: 2,
                active_peer: HostId(1),
                active_assignment_epoch: 1,
                transition_peer: Some(transition_peer),
                membership_epoch: 7,
            }),
            ..base.clone()
        };
        let (_, first) = store.put_cas(
            SimTime::ZERO,
            &mut rng,
            &key,
            Some(version),
            proposal(HostId(2)).encode(),
        );
        let (_, second) = store.put_cas(
            SimTime::ZERO,
            &mut rng,
            &key,
            Some(version),
            proposal(HostId(3)).encode(),
        );
        assert!(first.is_ok());
        assert!(matches!(second, Err(StoreError::CasConflict { .. })));

        let (_, stored) = store.get(SimTime::ZERO, &mut rng, &key);
        let (_, bytes) = stored.expect("read").expect("head");
        let head = HeadRecord::decode(vset, &bytes).expect("valid head");
        let assignment = head.stash.expect("assignment");
        assert_eq!(assignment.assignment_epoch, 2);
        assert_eq!(assignment.transition_peer, Some(HostId(2)));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use blockd_core::format::{open_frame, seal_frame};

    const T0: SimTime = SimTime::ZERO;
    const MAGIC: u32 = 0x0B10_C0D1;

    fn rng() -> Pcg64 {
        Pcg64::new(0x000b_10cd, 0)
    }

    #[test]
    fn read_after_write_is_strong() {
        let mut store = ObjectStore::new(StoreConfig::s3());
        let mut rng = rng();
        let (_, put) = store.put(T0, &mut rng, "v/1/head", b"h1".to_vec());
        assert_eq!(put, Ok(Version(1)));
        let (_, got) = store.get(T0, &mut rng, "v/1/head");
        assert_eq!(got, Ok(Some((Version(1), b"h1".to_vec()))));
    }

    #[test]
    fn writes_are_accounted_by_kind_outcome_identity_and_size() {
        let mut store = ObjectStore::new(StoreConfig::s3());
        let mut rng = rng();
        let manifest = "v/0000000000000001/m/0000000000000001-0000000000000001";
        let bytes = vec![7; 5000];

        store.set_outage(true);
        let (_, unavailable) = store.put(T0, &mut rng, manifest, bytes.clone());
        assert_eq!(unavailable, Err(StoreError::Unavailable));
        store.set_outage(false);
        let (_, first) = store.put(T0, &mut rng, manifest, bytes.clone());
        let (_, retry) = store.put(T0, &mut rng, manifest, bytes.clone());
        assert!(first.is_ok() && retry.is_ok());

        let head = "v/0000000000000001/head";
        let (_, created) = store.put_cas(T0, &mut rng, head, None, b"head".to_vec());
        let (_, conflict) = store.put_cas(T0, &mut rng, head, None, b"other".to_vec());
        assert!(created.is_ok());
        assert!(matches!(conflict, Err(StoreError::CasConflict { .. })));

        let manifest_counts = store.counters.puts_by_kind[StoreObjectKind::Manifest as usize];
        assert_eq!(manifest_counts.attempts, 3);
        assert_eq!(manifest_counts.successes, 2);
        assert_eq!(manifest_counts.attempted_bytes, 15_000);
        assert_eq!(manifest_counts.successful_bytes, 10_000);
        assert_eq!(store.counters.put_attempts, 5);
        assert_eq!(store.counters.put_successes, 3);
        assert_eq!(store.counters.cas_attempts, 2);
        assert_eq!(store.counters.cas_successes, 1);
        assert_eq!(store.counters.cas_conflicts, 1);
        assert_eq!(store.counters.unique_bytes, 5004);
        assert_eq!(store.counters.retry_bytes, 10_000);
        assert_eq!(store.counters.object_size_histogram[0], 1);
        assert_eq!(store.counters.object_size_histogram[1], 2);
    }

    #[test]
    fn cas_races_resolve_to_exactly_one_winner() {
        let mut store = ObjectStore::new(StoreConfig::s3());
        let mut rng = rng();
        // Two hosts race to create the same head record (R6.3).
        let (_, first) = store.put_cas(T0, &mut rng, "v/9/head", None, b"host0".to_vec());
        let (_, second) = store.put_cas(T0, &mut rng, "v/9/head", None, b"host1".to_vec());
        assert_eq!(first, Ok(Version(1)));
        assert_eq!(
            second,
            Err(StoreError::CasConflict {
                actual: Some(Version(1))
            })
        );
        // The loser refreshes and takes over explicitly.
        let (_, takeover) = store.put_cas(
            T0,
            &mut rng,
            "v/9/head",
            Some(Version(1)),
            b"host1".to_vec(),
        );
        assert_eq!(takeover, Ok(Version(2)));
        assert_eq!(store.counters.cas_conflicts, 1);
    }

    #[test]
    fn outage_fails_everything_loudly_and_lifts() {
        let mut store = ObjectStore::new(StoreConfig::s3());
        let mut rng = rng();
        store.set_outage(true);
        let (_, put) = store.put(T0, &mut rng, "k", vec![5]);
        let (_, get) = store.get(T0, &mut rng, "k");
        let (_, cas) = store.put_cas(T0, &mut rng, "k", None, vec![5]);
        let (_, del) = store.delete(T0, &mut rng, "k");
        let (_, list) = store.list_prefix(T0, &mut rng, "");
        assert_eq!(put, Err(StoreError::Unavailable));
        assert_eq!(get, Err(StoreError::Unavailable));
        assert_eq!(cas, Err(StoreError::Unavailable));
        assert_eq!(del, Err(StoreError::Unavailable));
        assert_eq!(list, Err(StoreError::Unavailable));
        assert_eq!(store.counters.unavailable, 5);
        assert_eq!(store.object_count(), 0, "nothing landed during the outage");

        store.set_outage(false);
        let (_, put) = store.put(T0, &mut rng, "k", vec![5]);
        assert_eq!(put, Ok(Version(1)));
    }

    #[test]
    fn oversized_objects_are_rejected() {
        let mut store = ObjectStore::new(StoreConfig::s3());
        let mut rng = rng();
        let (_, exact) = store.put(T0, &mut rng, "big", vec![0; MAX_OBJECT_BYTES]);
        assert_eq!(exact, Ok(Version(1)));
        let (_, over) = store.put(T0, &mut rng, "bigger", vec![0; MAX_OBJECT_BYTES + 1]);
        assert_eq!(over, Err(StoreError::TooLarge));
        let (_, cas_over) =
            store.put_cas(T0, &mut rng, "bigger", None, vec![0; MAX_OBJECT_BYTES + 1]);
        assert_eq!(cas_over, Err(StoreError::TooLarge));
        assert_eq!(store.counters.too_large, 2);
    }

    #[test]
    fn list_returns_keys_under_prefix() {
        let mut store = ObjectStore::new(StoreConfig::s3());
        let mut rng = rng();
        for key in ["v/1/head", "v/1/m/00", "v/1/s/00", "v/2/head"] {
            assert!(store.put(T0, &mut rng, key, vec![1]).1.is_ok());
        }
        let (_, listed) = store.list_prefix(T0, &mut rng, "v/1/");
        assert_eq!(
            listed,
            Ok(vec![
                "v/1/head".to_owned(),
                "v/1/m/00".to_owned(),
                "v/1/s/00".to_owned()
            ])
        );
    }

    #[test]
    fn bit_rot_defeats_frame_checks() {
        let mut store = ObjectStore::new(StoreConfig::s3());
        let mut rng = rng();
        let framed = seal_frame(MAGIC, b"backed up");
        assert!(store.put(T0, &mut rng, "v/1/m/01", framed).1.is_ok());
        assert_eq!(store.flip_random_bit(&mut rng), Some("v/1/m/01".to_owned()));
        let (_, got) = store.get(T0, &mut rng, "v/1/m/01");
        let (_, bytes) = got.unwrap().unwrap();
        assert!(open_frame(MAGIC, &bytes).is_err());
    }
}
