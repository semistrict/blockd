//! Residency accounting for the host page cache — the only cache in the
//! system (R2.2). The page *bytes* live in the shared guest mapping; what
//! the daemon owns is the bookkeeping: which pages are resident, dirty
//! (write-unprotected), or mid-flush, and which to evict under pressure.
//!
//! Eviction mirrors the kernel's multi-generational LRU (R2.6) over the
//! daemon's own tier accounting — the same structure, translated:
//!
//! - Pages live in **generations** — coarse age buckets between `min_seq`
//!   and `max_seq`, at most [`MAX_NR_GENS`] wide, exactly the kernel's
//!   shape. Fault-ins join the youngest generation; readahead (resume-set
//!   prefetch, R6.2) joins the oldest, just as the kernel places
//!   speculative page-cache reads.
//! - **Aging** advances `max_seq` and promotes pages the accessed-bit
//!   harvest ([`crate::seam::HostMap::harvest_accessed`]) reports touched —
//!   the mirror of the kernel walking page tables for young bits; blockd's
//!   uffd boundary cannot see resident-page accesses any other way.
//!   Write-protect faults promote inline (they ARE observed accesses).
//! - The kernel's anon/file **type split** maps to blockd's memory/disk
//!   volume split, with the balancing policy fixed by R2.4 instead of a
//!   PID controller: disk pages always evict first (a guest tolerates
//!   device latency; a RAM miss stalls a vCPU). This bias — and the tier
//!   accounting itself — is exactly the "what the kernel cannot know" that
//!   R2.6 permits on top of the borrowed algorithm. The kernel's
//!   fd-access tiers and refault feedback exist to tune that balance
//!   dynamically and are deliberately replaced by this fixed rule.
//! - **Eviction** takes the oldest generation of the preferred type;
//!   `min_seq` advances per type as generations empty.
//!
//! A page is evictable only when its newest content is durable: never
//! while dirty, never while a flush is in flight.

use std::collections::{BTreeMap, BTreeSet};

use crate::types::{PageId, SegId, VsetId};

/// Generations resident pages spread across, as in the kernel's MGLRU.
pub const MAX_NR_GENS: u64 = 4;

#[derive(Clone, Copy, Debug)]
pub struct Entry {
    /// Write-unprotected: the guest is mutating it invisibly.
    dirty: bool,
    /// Number of in-flight flushes of this page.
    flushing: u8,
    /// The generation this page belongs to (its coarse age).
    generation: u64,
    /// Arrival order within the generation (eviction tie-break).
    touched: u64,
}

impl Entry {
    fn evictable(&self) -> bool {
        !self.dirty && self.flushing == 0
    }
}

/// Eviction class: clean disk pages go before clean memory pages (R2.4).
fn class(page: PageId) -> u8 {
    u8::from(page.volume.idx.is_memory())
}

/// Identity of a shared base page: its immutable location.
pub type BaseKey = (u64, u64, SegId, u32);

pub struct Cache {
    capacity: usize,
    /// Slots promised to in-flight fills but not yet installed.
    reserved: usize,
    entries: BTreeMap<PageId, Entry>,
    /// Evictable pages ordered by (class, generation, arrival): the first
    /// element is the best victim — oldest generation of the preferred
    /// (disk) type.
    victims: BTreeSet<(u8, u64, u64, PageId)>,
    /// Resident pages per (class, generation) — what lets `min_seq`
    /// advance when a generation empties.
    gen_counts: BTreeMap<(u8, u64), usize>,
    /// The youngest generation (shared by both types, like the kernel).
    max_seq: u64,
    /// The oldest non-empty generation, per type (disk, memory).
    min_seq: [u64; 2],
    /// The shared base tier (R5.3): one physical page per base location,
    /// no matter how many forks map it.
    base_resident: BTreeSet<BaseKey>,
    clock: u64,
}

impl Cache {
    pub fn new(capacity: usize) -> Cache {
        assert!(capacity >= 1);
        Cache {
            capacity,
            reserved: 0,
            entries: BTreeMap::new(),
            victims: BTreeSet::new(),
            gen_counts: BTreeMap::new(),
            max_seq: 0,
            min_seq: [0, 0],
            base_resident: BTreeSet::new(),
            clock: 0,
        }
    }

    pub fn base_is_resident(&self, key: BaseKey) -> bool {
        self.base_resident.contains(&key)
    }

    pub fn base_resident_count(&self) -> usize {
        self.base_resident.len()
    }

    /// Admit a shared base page (consumes one slot, once, for every fork).
    pub fn base_insert(&mut self, key: BaseKey) {
        assert!(self.reserved > 0, "base fill without reserve");
        self.reserved -= 1;
        self.base_resident.insert(key);
    }

    pub fn is_resident(&self, page: PageId) -> bool {
        self.entries.contains_key(&page)
    }

    pub fn is_dirty(&self, page: PageId) -> bool {
        self.entries.get(&page).is_some_and(|e| e.dirty)
    }

    pub fn resident_count(&self) -> usize {
        self.entries.len()
    }

    fn unlink(&mut self, page: PageId, entry: &Entry) {
        self.victims
            .remove(&(class(page), entry.generation, entry.touched, page));
    }

    fn link(&mut self, page: PageId, entry: &Entry) {
        if entry.evictable() {
            self.victims
                .insert((class(page), entry.generation, entry.touched, page));
        }
    }

    fn count_add(&mut self, page: PageId, generation: u64) {
        *self
            .gen_counts
            .entry((class(page), generation))
            .or_insert(0) += 1;
    }

    fn count_remove(&mut self, page: PageId, generation: u64) {
        let key = (class(page), generation);
        let count = self.gen_counts.get_mut(&key).expect("counted");
        *count -= 1;
        if *count == 0 {
            self.gen_counts.remove(&key);
        }
        self.advance_min_seq(usize::from(class(page)));
    }

    /// `min_seq` advances past emptied generations (the kernel's rule).
    fn advance_min_seq(&mut self, ty: usize) {
        let class = u8::try_from(ty).expect("two types");
        while self.min_seq[ty] < self.max_seq
            && !self.gen_counts.contains_key(&(class, self.min_seq[ty]))
        {
            self.min_seq[ty] += 1;
        }
    }

    fn span(&self) -> u64 {
        self.max_seq - self.min_seq.iter().min().expect("two types") + 1
    }

    /// Observable generation span (R9.2): 1..=[`MAX_NR_GENS`].
    pub fn gen_span(&self) -> u64 {
        self.span()
    }

    /// One aging pass (R2.6), mirroring the kernel's: promote every page
    /// the accessed-bit harvest reports touched into a new youngest
    /// generation; everything untouched keeps aging toward eviction. The
    /// generation span stays within [`MAX_NR_GENS`].
    pub fn age(&mut self, harvest: impl FnOnce(&[PageId]) -> Vec<PageId>) {
        let resident: Vec<PageId> = self.entries.keys().copied().collect();
        if resident.is_empty() {
            return;
        }
        for ty in 0..2 {
            self.advance_min_seq(ty);
        }
        // A new youngest generation opens only while the span allows
        // (the kernel's MAX_NR_GENS bound); accessed pages promote to the
        // youngest either way.
        if self.span() < MAX_NR_GENS {
            self.max_seq += 1;
        }
        let accessed = harvest(&resident);
        for page in accessed {
            let Some(entry) = self.entries.get(&page).copied() else {
                continue;
            };
            self.unlink(page, &entry);
            self.count_remove(page, entry.generation);
            self.clock += 1;
            let entry = Entry {
                generation: self.max_seq,
                touched: self.clock,
                ..entry
            };
            self.count_add(page, self.max_seq);
            self.link(page, &entry);
            self.entries.insert(page, entry);
        }
    }

    /// Make room for one more page: spare capacity if there is any, else
    /// evict the best victim (returned — the caller must emit the `Evict`
    /// effect). `None` means genuine pressure (R2.5): every resident page is
    /// dirty or mid-flush, so the caller must wait for writeback — never
    /// kill.
    /// Reserve a slot only if free capacity exists — prefetch (R6.2) must
    /// never evict live pages to make room for guesses.
    pub fn reserve_if_free(&mut self) -> bool {
        if self.entries.len() + self.base_resident.len() + self.reserved < self.capacity {
            self.reserved += 1;
            return true;
        }
        false
    }

    pub fn reserve_slot(&mut self) -> Option<Option<PageId>> {
        if self.entries.len() + self.base_resident.len() + self.reserved < self.capacity {
            self.reserved += 1;
            return Some(None);
        }
        let &(_, _, _, victim) = self.victims.iter().next()?;
        let entry = self.entries.remove(&victim).expect("victim resident");
        self.unlink(victim, &entry);
        self.count_remove(victim, entry.generation);
        self.reserved += 1;
        Some(Some(victim))
    }

    /// A fill resolved into a slot obtained from [`Cache::reserve_slot`]:
    /// the page is resident — protected-clean, or dirty if the faulting
    /// access was a write.
    pub fn fill_slot(&mut self, page: PageId, dirty: bool) {
        self.fill_slot_in(page, dirty, self.max_seq);
    }

    /// Install a readahead page (resume-set prefetch, R6.2): it joins the
    /// OLDEST generation, exactly where the kernel puts speculative
    /// page-cache reads — a guess that is never used ages out first.
    pub fn fill_slot_cold(&mut self, page: PageId) {
        self.fill_slot_in(page, false, self.min_seq[usize::from(class(page))]);
    }

    fn fill_slot_in(&mut self, page: PageId, dirty: bool, generation: u64) {
        assert!(self.reserved > 0, "fill without reserve");
        self.reserved -= 1;
        self.clock += 1;
        let entry = Entry {
            dirty,
            flushing: 0,
            generation,
            touched: self.clock,
        };
        self.count_add(page, generation);
        self.link(page, &entry);
        let previous = self.entries.insert(page, entry);
        assert!(previous.is_none(), "page already resident");
    }

    /// Release a slot reserved for a fill that failed.
    pub fn release_slot(&mut self) {
        assert!(self.reserved > 0, "release without reserve");
        self.reserved -= 1;
    }

    /// A write-protect fault: the page is being written; it stays
    /// unevictable until captured and flushed again.
    pub fn mark_dirty(&mut self, page: PageId) {
        self.clock += 1;
        let clock = self.clock;
        let entry = *self.entries.get(&page).expect("fault on non-resident page");
        self.unlink(page, &entry);
        // A write-protect fault is an OBSERVED access: promote to the
        // youngest generation inline (the aging harvest would only learn
        // of it later).
        self.count_remove(page, entry.generation);
        self.count_add(page, self.max_seq);
        let entry = self.entries.get_mut(&page).expect("just observed");
        entry.dirty = true;
        entry.generation = self.max_seq;
        entry.touched = clock;
    }

    /// A capture flushed this page's current content: no longer dirty (it
    /// was re-write-protected), unevictable until the flush completes.
    pub fn begin_flush(&mut self, page: PageId) {
        let entry = self
            .entries
            .get_mut(&page)
            .expect("flush of non-resident page");
        entry.dirty = false;
        entry.flushing += 1;
    }

    /// One flush of this page completed (its bytes are durable).
    pub fn end_flush(&mut self, page: PageId) {
        let entry = self
            .entries
            .get_mut(&page)
            .expect("flush completion for non-resident page");
        assert!(entry.flushing > 0);
        entry.flushing -= 1;
        let entry = *self.entries.get(&page).expect("just observed");
        self.link(page, &entry);
    }

    /// Resident pages of one vset whose current content is not yet durable:
    /// dirty, or still mid-flush. A capture must persist exactly these — a
    /// mid-flush page's durable location still holds *stale* content, so
    /// referencing it would capture a state that never existed.
    pub fn unstable_pages_of(&self, vset: VsetId) -> Vec<PageId> {
        self.entries
            .iter()
            .filter(|(p, e)| p.volume.vset == vset && (e.dirty || e.flushing > 0))
            .map(|(p, _)| *p)
            .collect()
    }

    /// Dirty pages of one vset (these get re-write-protected at capture).
    pub fn dirty_pages_of(&self, vset: VsetId) -> Vec<PageId> {
        self.entries
            .iter()
            .filter(|(p, e)| p.volume.vset == vset && e.dirty)
            .map(|(p, _)| *p)
            .collect()
    }

    /// Does the vset have resident dirty pages? (Writeback trigger — pages
    /// merely mid-flush are already on their way and need no new capture.)
    pub fn has_dirty_of(&self, vset: VsetId) -> bool {
        self.entries
            .iter()
            .any(|(p, e)| p.volume.vset == vset && e.dirty)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{PageNo, VolumeId, VolumeIdx, VsetId};

    fn page(idx: u8, n: u32) -> PageId {
        PageId {
            volume: VolumeId {
                vset: VsetId(1),
                idx: VolumeIdx(idx),
            },
            page: PageNo(n),
        }
    }

    /// R2.4/R2.6: memory-volume pages have strictly higher residency
    /// affinity — under pressure, disk pages go first, even when the disk
    /// page is the most recently touched clean page.
    #[test]
    fn eviction_prefers_disk_pages_over_memory_pages() {
        let mut cache = Cache::new(2);
        assert_eq!(cache.reserve_slot(), Some(None));
        cache.fill_slot(page(0, 0), false); // memory page, touched FIRST
        assert_eq!(cache.reserve_slot(), Some(None));
        cache.fill_slot(page(1, 0), false); // disk page, touched LAST
        // Full: the victim must be the DISK page despite its recency.
        assert_eq!(cache.reserve_slot(), Some(Some(page(1, 0))));
        cache.fill_slot(page(1, 1), false);
        // Both remaining are (memory, disk): disk goes first again…
        assert_eq!(cache.reserve_slot(), Some(Some(page(1, 1))));
        cache.fill_slot(page(0, 1), false);
        // …and only with no disk pages left do memory pages evict, in
        // least-recently-touched order.
        assert_eq!(cache.reserve_slot(), Some(Some(page(0, 0))));
    }

    /// Dirty pages are never eviction victims (R2.4: durable-first).
    #[test]
    fn dirty_pages_are_not_victims() {
        let mut cache = Cache::new(1);
        assert_eq!(cache.reserve_slot(), Some(None));
        cache.fill_slot(page(1, 0), true); // dirty
        assert_eq!(cache.reserve_slot(), None, "a dirty page was offered");
    }

    /// R2.6, the heart of MGLRU: aging promotes pages the accessed-bit
    /// harvest reports touched; what nobody touched ages out first — even
    /// when it arrived more recently (generation beats recency).
    #[test]
    fn aging_evicts_the_unaccessed_before_the_accessed() {
        let mut cache = Cache::new(2);
        assert_eq!(cache.reserve_slot(), Some(None));
        cache.fill_slot(page(1, 0), false); // A, arrived first
        assert_eq!(cache.reserve_slot(), Some(None));
        cache.fill_slot(page(1, 1), false); // B, arrived last
        // The harvest saw A touched, B idle.
        cache.age(|resident| {
            assert_eq!(resident.len(), 2);
            vec![page(1, 0)]
        });
        // B is in the older generation now: it goes first, recency be
        // damned; a pure-LRU cache would have evicted A.
        assert_eq!(cache.reserve_slot(), Some(Some(page(1, 1))));
    }

    /// R2.6 readahead placement: prefetched pages join the OLDEST
    /// generation — an unused guess ages out before any demand fill,
    /// regardless of insertion order.
    #[test]
    fn readahead_ages_out_before_demand_fills() {
        let mut cache = Cache::new(3);
        assert_eq!(cache.reserve_slot(), Some(None));
        cache.fill_slot(page(1, 0), false); // A: demand, gen 0
        cache.age(|_| Vec::new()); // opens gen 1; A stays old
        assert_eq!(cache.reserve_slot(), Some(None));
        cache.fill_slot(page(1, 1), false); // B: demand, gen 1
        assert!(cache.reserve_if_free());
        cache.fill_slot_cold(page(1, 2)); // C: readahead → gen 0
        // Oldest generation drains first: A then C — the prefetched C dies
        // before B even though C arrived last.
        assert_eq!(cache.reserve_slot(), Some(Some(page(1, 0))));
        cache.fill_slot(page(1, 3), false);
        assert_eq!(cache.reserve_slot(), Some(Some(page(1, 2))));
        cache.fill_slot(page(1, 4), false);
        assert_eq!(cache.reserve_slot(), Some(Some(page(1, 1))));
    }

    /// The generation span is bounded by [`MAX_NR_GENS`], like the
    /// kernel's: aging cannot run away from an undrained oldest gen.
    #[test]
    fn generation_span_stays_bounded() {
        let mut cache = Cache::new(2);
        assert_eq!(cache.reserve_slot(), Some(None));
        cache.fill_slot(page(1, 0), false);
        for _ in 0..10 {
            cache.age(|_| Vec::new());
        }
        assert_eq!(cache.gen_span(), MAX_NR_GENS);
    }

    /// A write-protect fault is an observed access: the page promotes to
    /// the youngest generation inline and outlives untouched peers.
    #[test]
    fn write_faults_promote_inline() {
        let mut cache = Cache::new(2);
        assert_eq!(cache.reserve_slot(), Some(None));
        cache.fill_slot(page(1, 0), false); // A
        assert_eq!(cache.reserve_slot(), Some(None));
        cache.fill_slot(page(1, 1), false); // B
        cache.age(|_| Vec::new()); // both old now, gen 1 open
        cache.mark_dirty(page(1, 0)); // guest wrote A → promoted + dirty
        cache.begin_flush(page(1, 0)); // captured…
        cache.end_flush(page(1, 0)); // …durable: clean again, young
        assert_eq!(cache.reserve_slot(), Some(Some(page(1, 1))));
    }
}
