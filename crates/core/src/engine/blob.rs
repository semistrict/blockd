use blockd_exec::delay;

use crate::world::{BlobError, Blobs};

use super::SharedHost;

async fn full_backoff(state: &SharedHost) {
    let after = {
        let mut host = state.borrow_mut();
        host.note_blob_full();
        host.config.writeback_interval.max(1)
    };
    delay(after).await;
}

pub(super) async fn write<W: Blobs>(
    state: &SharedHost,
    world: &W,
    name: String,
    bytes: Vec<u8>,
) -> Result<(), BlobError> {
    loop {
        match Blobs::write(world, name.clone(), bytes.clone()).await {
            Ok(()) => return Ok(()),
            Err(BlobError::Full) => full_backoff(state).await,
            Err(BlobError::Io) => return Err(BlobError::Io),
        }
    }
}

pub(super) async fn truncate<W: Blobs>(
    state: &SharedHost,
    world: &W,
    name: &str,
    len: u64,
) -> Result<(), BlobError> {
    loop {
        match Blobs::truncate(world, name, len).await {
            Ok(()) => return Ok(()),
            Err(BlobError::Full) => full_backoff(state).await,
            Err(BlobError::Io) => return Err(BlobError::Io),
        }
    }
}
