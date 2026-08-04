//! The simulated guest: the workload actor behind one vset's Firecracker
//! sandbox. Strictly sequential — one vCPU that either runs, blocks on a
//! fault, or waits on a pmem sync. Reads and writes of resident, writable
//! pages touch guest memory directly and never reach the daemon; only
//! faults, syncs and pauses cross the boundary, exactly like userfaultfd.
//!
//! Page contents are deterministic patterns carrying the volume write
//! sequence, which is what lets the oracle validate every byte the daemon
//! ever fills (R8.1) and reconstruct recovered states after crashes (R3.8).

use std::collections::VecDeque;

use blockd_core::journal::VsetConfig;
use blockd_core::seam::ReqId;
use blockd_core::types::{PAGE_SIZE, PageId, PageNo, VolumeId, VolumeIdx, VsetId};

use crate::rng::{Pcg64, Ppm};

/// Deterministic content of a page after its volume's write number
/// `vol_seq` touched it. `vol_seq` 0 is the never-written zero page. The
/// first word carries `vol_seq` in the clear (recovery inference); the rest
/// mixes identity so any cross-page or cross-epoch confusion breaks the
/// comparison; three of four words stay zero so pages stay compressible
/// (R8.4 is real compression).
pub fn page_pattern(page: PageId, vol_seq: u64) -> Vec<u8> {
    let mut bytes = vec![0u8; PAGE_SIZE];
    if vol_seq == 0 {
        return bytes;
    }
    bytes[0..8].copy_from_slice(&vol_seq.to_le_bytes());
    for word in (1..PAGE_SIZE / 8).step_by(4) {
        let mut mix = 0xcbf2_9ce4_8422_2325u64;
        for v in [
            page.volume.vset.0,
            u64::from(page.volume.idx.0),
            u64::from(page.page.0),
            vol_seq,
            word as u64,
        ] {
            mix ^= v;
            mix = mix.wrapping_mul(0x0000_0100_0000_01b3);
        }
        bytes[word * 8..word * 8 + 8].copy_from_slice(&mix.to_le_bytes());
    }
    bytes
}

/// The volume write sequence a stored page claims to be, read back out of
/// the pattern (0 for the zero page).
pub fn claimed_vol_seq(bytes: &[u8]) -> u64 {
    u64::from_le_bytes(bytes[0..8].try_into().expect("page has 8 bytes"))
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PendingOp {
    Read {
        page: PageId,
    },
    Write {
        page: PageId,
        vol_seq: u64,
    },
    /// Post-recovery verification read (boot fsck).
    Fsck {
        page: PageId,
    },
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum GuestState {
    Idle,
    /// The vCPU is blocked on a fault raised by this operation.
    Faulted {
        op: PendingOp,
    },
    /// Waiting for a pmem sync acknowledgment.
    Syncing {
        req: ReqId,
        volume: VolumeId,
    },
    /// The blocking fill arrived while the vCPU was paused: memory is
    /// resolved, but the instruction retires only on resume. A paused vCPU
    /// never advances — this is what makes the captured vmstate and the
    /// captured memory the same instant.
    Parked {
        op: PendingOp,
    },
    /// A sync acknowledged while the vCPU was paused: retires on resume.
    SyncParked {
        volume: VolumeId,
    },
    /// Failed loudly (R8.1) — dead until the next daemon incarnation.
    Dead,
}

pub struct Guest {
    pub vset: VsetId,
    pub config: VsetConfig,
    pub state: GuestState,
    /// vCPUs paused for a checkpoint capture (R3.1).
    pub paused: bool,
    /// Applied operations — the op numbering vmstate refers to (R1.2).
    pub applied: u64,
    /// Completed operations, cumulative across daemon incarnations.
    pub completed: u64,
    /// Pages still to verify after a recovery, in order.
    pub fsck: VecDeque<PageId>,
    /// True while verifying a cold boot (fsck fills infer recovered state).
    pub cold_booting: bool,
    /// Override for the sync share of the op mix (`None` = the default
    /// sync-heavy mix). Each sync buys a durable consistency point, so
    /// workload-cost tests tune this to what they mean to measure.
    pub sync_share: Option<Ppm>,
    /// Access skew override (`None` = uniform): this share of page picks
    /// lands in the first N pages of the volume (the hot set), the rest
    /// spread over the cold remainder. Hot churn plus cold survivors is
    /// what makes space amplification measurable.
    pub hot_pages: Option<(Ppm, u32)>,
}

impl Guest {
    pub fn new(vset: VsetId, config: VsetConfig) -> Guest {
        Guest {
            vset,
            config,
            state: GuestState::Idle,
            paused: false,
            applied: 0,
            completed: 0,
            fsck: VecDeque::new(),
            cold_booting: false,
            sync_share: None,
            hot_pages: None,
        }
    }

    /// Rebirth after recovery: resumed guests continue at the checkpoint's
    /// vmstate; cold-booted guests boot fresh. Either way every page gets
    /// one verification read before normal load resumes.
    pub fn reborn(&mut self, applied: u64, cold: bool) {
        self.state = GuestState::Idle;
        self.paused = false;
        self.applied = applied;
        self.cold_booting = cold;
        self.fsck = self
            .config
            .volumes(self.vset)
            .flat_map(|volume| {
                (0..self.config.pages_per_volume).map(move |n| PageId {
                    volume,
                    page: PageNo(n),
                })
            })
            .collect();
    }

    fn random_page(&self, rng: &mut Pcg64) -> PageId {
        let idx = VolumeIdx(
            u8::try_from(rng.below(u64::from(self.config.disk_volumes) + 1)).expect("fits"),
        );
        let page = match self.hot_pages {
            Some((share, hot)) if rng.hit(share) => rng.below(u64::from(hot)),
            Some((_, hot)) => {
                u64::from(hot) + rng.below(u64::from(self.config.pages_per_volume - hot))
            }
            None => rng.below(u64::from(self.config.pages_per_volume)),
        };
        PageId {
            volume: VolumeId {
                vset: self.vset,
                idx,
            },
            page: PageNo(u32::try_from(page).expect("fits")),
        }
    }

    /// Decide the next operation: an fsck read if booting, else a random
    /// write / read / sync. `next_vol_seq` is supplied by the oracle.
    /// Returns `Ok(op)` for memory operations and `Err(volume)` for a sync.
    pub fn next_op(
        &mut self,
        rng: &mut Pcg64,
        next_vol_seq: impl Fn(VolumeId) -> u64,
    ) -> Result<PendingOp, VolumeId> {
        assert_eq!(self.state, GuestState::Idle, "one outstanding op only");
        if let Some(page) = self.fsck.pop_front() {
            return Ok(PendingOp::Fsck { page });
        }
        if let Some(share) = self.sync_share {
            if rng.hit(share) {
                let idx = VolumeIdx(
                    u8::try_from(rng.range(1, u64::from(self.config.disk_volumes))).expect("fits"),
                );
                return Err(VolumeId {
                    vset: self.vset,
                    idx,
                });
            }
            let page = self.random_page(rng);
            if rng.hit(Ppm::percent(60)) {
                return Ok(PendingOp::Write {
                    page,
                    vol_seq: next_vol_seq(page.volume),
                });
            }
            return Ok(PendingOp::Read { page });
        }
        if rng.hit(Ppm::percent(50)) {
            let page = self.random_page(rng);
            return Ok(PendingOp::Write {
                page,
                vol_seq: next_vol_seq(page.volume),
            });
        }
        if rng.hit(Ppm::percent(70)) {
            return Ok(PendingOp::Read {
                page: self.random_page(rng),
            });
        }
        // Sync a random disk volume.
        let idx = VolumeIdx(
            u8::try_from(rng.range(1, u64::from(self.config.disk_volumes))).expect("fits"),
        );
        Err(VolumeId {
            vset: self.vset,
            idx,
        })
    }
}
