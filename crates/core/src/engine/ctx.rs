//! Bound-free actor contexts for host- and volume-scoped protocol work.
//!
//! Trait bounds belong on method impls, never these structs. Keeping the
//! stored contexts bound-free preserves narrow world mocks in module tests.
//! Inherent method names must also remain unique per context type across the
//! crate so independently migrated modules cannot create duplicate methods.

use std::rc::Rc;

use super::SharedHost;
use crate::types::VolumeId;

pub(super) struct HostCtx<W> {
    state: SharedHost,
    world: Rc<W>,
}

impl<W> HostCtx<W> {
    pub(super) fn new(state: SharedHost, world: Rc<W>) -> Self {
        Self { state, world }
    }

    pub(super) fn state(&self) -> &SharedHost {
        &self.state
    }

    pub(super) fn world(&self) -> &Rc<W> {
        &self.world
    }

    pub(super) fn volume(&self, volume: VolumeId) -> VolumeCtx<W> {
        VolumeCtx {
            host: self.clone(),
            volume,
        }
    }
}

impl<W> Clone for HostCtx<W> {
    fn clone(&self) -> Self {
        Self {
            state: Rc::clone(&self.state),
            world: Rc::clone(&self.world),
        }
    }
}

pub(super) struct VolumeCtx<W> {
    host: HostCtx<W>,
    volume: VolumeId,
}

impl<W> VolumeCtx<W> {
    pub(super) fn host(&self) -> &HostCtx<W> {
        &self.host
    }

    pub(super) fn id(&self) -> VolumeId {
        self.volume
    }

    pub(super) fn pin(&self, run_generation: u64) -> VolumeRun<W> {
        VolumeRun {
            volume: self.clone(),
            run_generation,
        }
    }
}

impl<W> Clone for VolumeCtx<W> {
    fn clone(&self) -> Self {
        Self {
            host: self.host.clone(),
            volume: self.volume,
        }
    }
}

pub(super) struct VolumeRun<W> {
    volume: VolumeCtx<W>,
    run_generation: u64,
}

impl<W> VolumeRun<W> {
    pub(super) fn volume(&self) -> &VolumeCtx<W> {
        &self.volume
    }

    pub(super) fn run_generation(&self) -> u64 {
        self.run_generation
    }

    pub(super) fn interrupted(&self) -> bool {
        self.volume
            .host
            .state
            .borrow()
            .volume_at(self.volume.volume, self.run_generation)
            .is_none_or(|state| state.operations.migration_running() || state.outbound.is_some())
    }
}

impl<W> Clone for VolumeRun<W> {
    fn clone(&self) -> Self {
        Self {
            volume: self.volume.clone(),
            run_generation: self.run_generation,
        }
    }
}
