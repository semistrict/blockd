//! A host's local NVMe, modeled as a write-once named-blob device — the
//! exact contract the daemon's write path is built on:
//!
//! - a **completed** write is durable across any crash;
//! - an **in-flight** write at crash time independently lands whole, not at
//!   all, or torn (a prefix of the bytes);
//! - bit rot can flip any stored bit at any time.
//!
//! The device never verifies anything. Reads return the stored bytes
//! verbatim — damaged or not — and range reads of a short (torn) blob return
//! fewer bytes than asked. Catching all of that is the daemon's job, via its
//! own frame checksums (R8.1); the device gives it no help.

use std::collections::BTreeMap;
use std::fmt;

use blockd_core::types::SimTime;

use crate::rng::Pcg64;

#[derive(Clone, Copy, Debug)]
pub struct BlobDevConfig {
    pub read_latency_min: u64,
    pub read_latency_max: u64,
    pub write_latency_min: u64,
    pub write_latency_max: u64,
    /// Throughput term: added nanoseconds per byte moved.
    pub ns_per_byte: u64,
}

impl BlobDevConfig {
    /// Local-NVMe-class latencies (R2.3's ~100 µs class, ~1 GB/s).
    pub fn nvme() -> BlobDevConfig {
        BlobDevConfig {
            read_latency_min: 20_000,
            read_latency_max: 150_000,
            write_latency_min: 30_000,
            write_latency_max: 400_000,
            ns_per_byte: 1,
        }
    }
}

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct BdevIo(pub u64);

impl fmt::Debug for BdevIo {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "bdev{}", self.0)
    }
}

/// What a crash did to one in-flight write.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum CrashFate {
    Applied,
    Dropped,
    /// Only the first `kept` bytes landed.
    Torn {
        kept: usize,
    },
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct BlobDevCounters {
    pub writes_completed: u64,
    pub bytes_written: u64,
    pub reads: u64,
    pub bytes_read: u64,
    pub crash_applied: u64,
    pub crash_dropped: u64,
    pub crash_torn: u64,
    pub bitflips: u64,
}

pub struct BlobDev {
    config: BlobDevConfig,
    blobs: BTreeMap<String, Vec<u8>>,
    inflight: BTreeMap<BdevIo, (String, Vec<u8>, WriteKind)>,
    next_io: u64,
    pub counters: BlobDevCounters,
}

#[derive(Clone, Copy)]
enum WriteKind {
    New,
    Append,
}

impl BlobDev {
    pub fn new(config: BlobDevConfig) -> BlobDev {
        BlobDev {
            config,
            blobs: BTreeMap::new(),
            inflight: BTreeMap::new(),
            next_io: 0,
            counters: BlobDevCounters::default(),
        }
    }

    fn latency(&self, rng: &mut Pcg64, min: u64, max: u64, bytes: usize) -> u64 {
        rng.range(min, max) + self.config.ns_per_byte * bytes as u64
    }

    /// Submit a write of a new blob. Names are write-once: reusing one is a
    /// daemon bug and panics. Returns the io and its completion time.
    pub fn submit_write(
        &mut self,
        now: SimTime,
        rng: &mut Pcg64,
        name: String,
        bytes: Vec<u8>,
    ) -> (BdevIo, SimTime) {
        assert!(
            !self.blobs.contains_key(&name) && !self.inflight.values().any(|(n, _, _)| *n == name),
            "blob name reused: {name}"
        );
        let latency = self.latency(
            rng,
            self.config.write_latency_min,
            self.config.write_latency_max,
            bytes.len(),
        );
        let io = BdevIo(self.next_io);
        self.next_io += 1;
        self.inflight.insert(io, (name, bytes, WriteKind::New));
        (io, now.after(latency))
    }

    /// Submit one append to an existing or new spool blob. Only one append
    /// per name may be in flight, matching the ordered runtime lane.
    pub fn submit_append(
        &mut self,
        now: SimTime,
        rng: &mut Pcg64,
        name: String,
        bytes: Vec<u8>,
    ) -> (BdevIo, SimTime) {
        assert!(
            !self.inflight.values().any(|(n, _, _)| *n == name),
            "concurrent append to blob: {name}"
        );
        let latency = self.latency(
            rng,
            self.config.write_latency_min,
            self.config.write_latency_max,
            bytes.len(),
        );
        let io = BdevIo(self.next_io);
        self.next_io += 1;
        self.inflight.insert(io, (name, bytes, WriteKind::Append));
        (io, now.after(latency))
    }

    /// Make a submitted write durable. Panics on unknown io — a crash clears
    /// in-flight ios, and completing one twice is a harness bug.
    pub fn complete_write(&mut self, io: BdevIo) {
        let (name, bytes, kind) = self.inflight.remove(&io).expect("completing unknown io");
        self.counters.writes_completed += 1;
        self.counters.bytes_written += bytes.len() as u64;
        match kind {
            WriteKind::New => {
                self.blobs.insert(name, bytes);
            }
            WriteKind::Append => self.blobs.entry(name).or_default().extend(bytes),
        }
    }

    /// Read a whole blob: the stored bytes, verbatim, damage included.
    pub fn read(
        &mut self,
        now: SimTime,
        rng: &mut Pcg64,
        name: &str,
    ) -> (SimTime, Option<Vec<u8>>) {
        let bytes = self.blobs.get(name).cloned();
        let len = bytes.as_ref().map_or(0, Vec::len);
        self.counters.reads += 1;
        self.counters.bytes_read += len as u64;
        let latency = self.latency(
            rng,
            self.config.read_latency_min,
            self.config.read_latency_max,
            len,
        );
        (now.after(latency), bytes)
    }

    /// Read a byte range. A range beyond the blob's (possibly torn-short)
    /// length is clamped: the caller gets fewer bytes than asked and its
    /// frame checks must catch it.
    pub fn read_range(
        &mut self,
        now: SimTime,
        rng: &mut Pcg64,
        name: &str,
        offset: u64,
        len: u64,
    ) -> (SimTime, Option<Vec<u8>>) {
        let bytes = self.blobs.get(name).map(|blob| {
            let start = usize::try_from(offset.min(blob.len() as u64)).expect("fits");
            let end = usize::try_from((offset + len).min(blob.len() as u64)).expect("fits");
            blob[start..end].to_vec()
        });
        let got = bytes.as_ref().map_or(0, Vec::len);
        self.counters.reads += 1;
        self.counters.bytes_read += got as u64;
        let latency = self.latency(
            rng,
            self.config.read_latency_min,
            self.config.read_latency_max,
            got,
        );
        (now.after(latency), bytes)
    }

    /// Reclaim a blob (R4.5: always explicit).
    pub fn delete(&mut self, name: &str) -> bool {
        self.blobs.remove(name).is_some()
    }

    pub fn truncate(&mut self, name: &str, len: usize) -> bool {
        let Some(bytes) = self.blobs.get_mut(name) else {
            return false;
        };
        bytes.truncate(len);
        true
    }

    /// Power loss / daemon death: every in-flight write independently lands
    /// whole, vanishes, or lands torn to a random prefix. Completed writes
    /// are untouched. Returns each blob's fate for the oracle.
    pub fn crash(&mut self, rng: &mut Pcg64) -> Vec<(String, CrashFate)> {
        let inflight = std::mem::take(&mut self.inflight);
        let mut fates = Vec::new();
        for (_, (name, bytes, kind)) in inflight {
            let fate = match rng.below(3) {
                0 => {
                    self.counters.crash_applied += 1;
                    match kind {
                        WriteKind::New => {
                            self.blobs.insert(name.clone(), bytes);
                        }
                        WriteKind::Append => {
                            self.blobs.entry(name.clone()).or_default().extend(bytes);
                        }
                    }
                    CrashFate::Applied
                }
                1 => {
                    self.counters.crash_dropped += 1;
                    CrashFate::Dropped
                }
                _ => {
                    let kept = usize::try_from(rng.below(bytes.len() as u64 + 1)).expect("fits");
                    self.counters.crash_torn += 1;
                    match kind {
                        WriteKind::New => {
                            self.blobs.insert(name.clone(), bytes[..kept].to_vec());
                        }
                        WriteKind::Append => self
                            .blobs
                            .entry(name.clone())
                            .or_default()
                            .extend_from_slice(&bytes[..kept]),
                    }
                    CrashFate::Torn { kept }
                }
            };
            fates.push((name, fate));
        }
        fates
    }

    /// Bit rot: flip one random bit of one random stored blob. Returns the
    /// victim's name for the oracle's sanctioned-failure bookkeeping.
    pub fn flip_random_bit(&mut self, rng: &mut Pcg64) -> Option<String> {
        self.flip_random_bit_where(rng, |_| true)
    }

    /// Bit rot restricted to blobs matching a predicate (test targeting).
    pub fn flip_random_bit_where(
        &mut self,
        rng: &mut Pcg64,
        pred: impl Fn(&str) -> bool,
    ) -> Option<String> {
        let candidates: Vec<&String> = self
            .blobs
            .iter()
            .filter(|(n, b)| !b.is_empty() && pred(n))
            .map(|(n, _)| n)
            .collect();
        if candidates.is_empty() {
            return None;
        }
        let name = (*rng.pick(&candidates)).clone();
        let blob = self.blobs.get_mut(&name).expect("picked");
        let bit = rng.below(blob.len() as u64 * 8);
        blob[usize::try_from(bit / 8).expect("fits")] ^= 1 << (bit % 8);
        self.counters.bitflips += 1;
        Some(name)
    }

    /// Recovery scan: every stored blob, verbatim.
    pub fn scan(&self) -> impl Iterator<Item = (&String, &Vec<u8>)> {
        self.blobs.iter()
    }

    pub fn contains(&self, name: &str) -> bool {
        self.blobs.contains_key(name)
    }

    pub fn blob_count(&self) -> usize {
        self.blobs.len()
    }

    pub fn inflight(&self) -> usize {
        self.inflight.len()
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
    fn completed_writes_survive_crash_verbatim() {
        let mut dev = BlobDev::new(BlobDevConfig::nvme());
        let mut rng = rng();
        let framed = seal_frame(MAGIC, b"durable payload");
        let (io, done) = dev.submit_write(T0, &mut rng, "v/0/a.rec".into(), framed.clone());
        assert!(done > T0);
        dev.complete_write(io);
        assert_eq!(dev.crash(&mut rng), vec![]);
        let (_, bytes) = dev.read(T0, &mut rng, "v/0/a.rec");
        let bytes = bytes.expect("blob exists");
        assert_eq!(bytes, framed);
        assert_eq!(open_frame(MAGIC, &bytes), Ok(&b"durable payload"[..]));
    }

    #[test]
    fn inflight_writes_resolve_three_ways_and_torn_blobs_fail_frame_checks() {
        let mut dev = BlobDev::new(BlobDevConfig::nvme());
        let mut rng = rng();
        let framed = seal_frame(MAGIC, &[0x5A; 256]);
        for n in 0..300 {
            dev.submit_write(T0, &mut rng, format!("blob/{n}"), framed.clone());
        }
        assert_eq!(dev.inflight(), 300);
        let fates = dev.crash(&mut rng);
        assert_eq!(dev.inflight(), 0);
        let c = dev.counters;
        assert_eq!(
            (c.crash_applied, c.crash_dropped, c.crash_torn),
            (88, 107, 105)
        );

        // The daemon's frame check must catch every torn blob that isn't
        // accidentally whole; count exactly what survives verification.
        let mut verified = 0;
        let mut rejected = 0;
        let mut missing = 0;
        for (name, _) in &fates {
            let (_, bytes) = dev.read(T0, &mut rng, name);
            match bytes {
                None => missing += 1,
                Some(b) if open_frame(MAGIC, &b) == Ok(&[0x5A; 256][..]) => verified += 1,
                Some(_) => rejected += 1,
            }
        }
        let accidental_whole = fates
            .iter()
            .filter(|(_, f)| *f == CrashFate::Torn { kept: framed.len() })
            .count();
        assert_eq!(missing, 107);
        assert_eq!(verified, 88 + accidental_whole);
        assert_eq!(rejected, 105 - accidental_whole);
        assert_eq!(accidental_whole, 0);
    }

    #[test]
    fn range_reads_of_short_blobs_come_back_short() {
        let mut dev = BlobDev::new(BlobDevConfig::nvme());
        let mut rng = rng();
        let (io, _) = dev.submit_write(T0, &mut rng, "short".into(), vec![7; 100]);
        dev.complete_write(io);
        let (_, got) = dev.read_range(T0, &mut rng, "short", 60, 80);
        assert_eq!(got, Some(vec![7; 40]));
        let (_, absent) = dev.read_range(T0, &mut rng, "nope", 0, 10);
        assert_eq!(absent, None);
    }

    #[test]
    fn bit_rot_defeats_the_frame_check() {
        let mut dev = BlobDev::new(BlobDevConfig::nvme());
        let mut rng = rng();
        let framed = seal_frame(MAGIC, b"about to rot");
        let (io, _) = dev.submit_write(T0, &mut rng, "rotting".into(), framed);
        dev.complete_write(io);
        assert_eq!(dev.flip_random_bit(&mut rng), Some("rotting".to_owned()));
        let (_, bytes) = dev.read(T0, &mut rng, "rotting");
        assert!(open_frame(MAGIC, &bytes.expect("still present")).is_err());
        assert_eq!(dev.counters.bitflips, 1);
    }

    #[test]
    #[should_panic(expected = "blob name reused")]
    fn blob_names_are_write_once() {
        let mut dev = BlobDev::new(BlobDevConfig::nvme());
        let mut rng = rng();
        let (io, _) = dev.submit_write(T0, &mut rng, "once".into(), vec![1]);
        dev.complete_write(io);
        dev.submit_write(T0, &mut rng, "once".into(), vec![2]);
    }

    #[test]
    fn deleted_blobs_are_gone() {
        let mut dev = BlobDev::new(BlobDevConfig::nvme());
        let mut rng = rng();
        let (io, _) = dev.submit_write(T0, &mut rng, "gone".into(), vec![9]);
        dev.complete_write(io);
        assert!(dev.delete("gone"));
        assert!(!dev.delete("gone"));
        let (_, bytes) = dev.read(T0, &mut rng, "gone");
        assert_eq!(bytes, None);
    }
}
