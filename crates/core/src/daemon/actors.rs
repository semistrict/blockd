//! Straight-line actors for one vset.
//!
//! This module is the replacement path for the reified continuations in the
//! parent module. Durable operations are awaited where the protocol needs
//! them, so local state contains protocol facts rather than I/O identifiers.

use std::collections::BTreeMap;

use blockd_exec::join2;

use crate::journal::{DatabaseMeta, JournalRecord, RecordKind, VsetConfig};
use crate::layout;
use crate::protocol::{AdminReply, ReqId};
use crate::types::{JournalSeq, VsetId};
use crate::world::{AdminIo, Blobs};

/// The durable facts established by a fresh local-vset creation actor.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LocalVset {
    pub record: JournalRecord,
    pub next_seq: u64,
}

/// Create a fresh non-backed vset and publish it only after both journal
/// copies are durable.
///
/// The two writes are submitted together and then joined in declaration
/// order. A failed local write is fatal, matching the existing durability
/// contract: replying with only one newly-created copy would silently reduce
/// the record's rot tolerance.
pub async fn create_fresh_local<W>(
    world: &W,
    req: ReqId,
    vset: VsetId,
    config: VsetConfig,
) -> Option<LocalVset>
where
    W: Blobs + AdminIo,
{
    let record = JournalRecord {
        config,
        seq: JournalSeq(0),
        fence: 1,
        kind: RecordKind::Commit,
        capture_seq: 0,
        sync_covered_through: 0,
        database: DatabaseMeta::default(),
        overlay: BTreeMap::new(),
        leaves: BTreeMap::new(),
        migrated_from: None,
    };
    let bytes = record.encode(vset);
    let (primary, mirror) = join2(
        Blobs::write(
            world,
            layout::journal_blob(vset, record.fence, record.seq),
            bytes.clone(),
        ),
        Blobs::write(
            world,
            layout::journal_mirror_blob(vset, record.fence, record.seq),
            bytes,
        ),
    )
    .await;
    if primary.is_err() || mirror.is_err() {
        AdminIo::abort(world, "local journal write failed").await;
        return None;
    }
    AdminIo::reply_admin(world, AdminReply::VsetCreated { req, vset }).await;
    Some(LocalVset {
        record,
        next_seq: 1,
    })
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::BTreeMap;
    use std::rc::Rc;

    use async_trait::async_trait;
    use blockd_exec::{Executor, delay};

    use super::create_fresh_local;
    use crate::database::{DatabaseReply, DatabaseRequest};
    use crate::journal::{JournalRecord, VsetConfig};
    use crate::layout;
    use crate::protocol::{AdminCmd, AdminReply, ReqId};
    use crate::types::VsetId;
    use crate::world::{AdminIo, BlobError, Blobs};

    #[derive(Default)]
    struct ModelWorld {
        durable: RefCell<BTreeMap<String, Vec<u8>>>,
        replies: RefCell<Vec<AdminReply>>,
    }

    #[async_trait(?Send)]
    impl Blobs for ModelWorld {
        async fn write(&self, name: String, bytes: Vec<u8>) -> Result<(), BlobError> {
            let latency = if std::path::Path::new(&name)
                .extension()
                .is_some_and(|extension| extension.eq_ignore_ascii_case("recm"))
            {
                5
            } else {
                1
            };
            delay(latency).await;
            self.durable.borrow_mut().insert(name, bytes);
            Ok(())
        }

        async fn append(&self, _name: String, _bytes: Vec<u8>) -> Result<(), BlobError> {
            unreachable!()
        }

        async fn truncate(&self, _name: &str, _len: u64) -> Result<(), BlobError> {
            unreachable!()
        }

        async fn read(&self, name: &str) -> Result<Option<Vec<u8>>, BlobError> {
            Ok(self.durable.borrow().get(name).cloned())
        }

        async fn read_range(
            &self,
            _name: &str,
            _offset: u64,
            _len: u64,
        ) -> Result<Option<Vec<u8>>, BlobError> {
            unreachable!()
        }

        async fn delete(&self, _name: &str) -> Result<(), BlobError> {
            unreachable!()
        }
    }

    #[async_trait(?Send)]
    impl AdminIo for ModelWorld {
        async fn next_admin(&self) -> Option<AdminCmd> {
            unreachable!()
        }

        async fn reply_admin(&self, reply: AdminReply) {
            self.replies.borrow_mut().push(reply);
        }

        async fn next_database(&self) -> Option<DatabaseRequest> {
            unreachable!()
        }

        async fn reply_database(&self, _reply: DatabaseReply) {
            unreachable!()
        }

        async fn abort(&self, reason: &'static str) {
            panic!("actor aborted: {reason}")
        }
    }

    #[test]
    fn fresh_local_creation_waits_for_both_exact_record_copies() {
        let world = Rc::new(ModelWorld::default());
        let vset = VsetId(7);
        let req = ReqId(4);
        let config = VsetConfig::compute(1, 8, false);
        let mut executor = Executor::simulation(9);
        let actor_world = Rc::clone(&world);
        let actor = executor.spawn(async move {
            create_fresh_local(actor_world.as_ref(), req, vset, config).await
        });

        executor.run_until(4);
        assert!(world.replies.borrow().is_empty());
        assert_eq!(world.durable.borrow().len(), 1);

        let created = executor
            .block_on(actor)
            .expect("creation actor was not cancelled");
        let created = created.expect("local writes succeeded");
        assert_eq!(created.next_seq, 1);
        assert_eq!(
            *world.replies.borrow(),
            [AdminReply::VsetCreated { req, vset }]
        );

        let durable = world.durable.borrow();
        let primary = &durable[&layout::journal_blob(vset, 1, created.record.seq)];
        let mirror = &durable[&layout::journal_mirror_blob(vset, 1, created.record.seq)];
        assert_eq!(primary, mirror);
        assert_eq!(JournalRecord::decode(vset, primary), Ok(created.record));
    }
}
