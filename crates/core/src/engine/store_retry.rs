use blockd_exec::delay;

use super::SharedHost;
use crate::world::{Store, StoreError};

async fn retry(state: &SharedHost, delay_ns: u64) {
    state.borrow_mut().counters.store_retries += 1;
    delay(delay_ns).await;
}

pub async fn get<W: Store>(
    state: &SharedHost,
    world: &W,
    key: &str,
    delay_ns: u64,
) -> Result<Option<(u64, Vec<u8>)>, StoreError> {
    loop {
        match Store::get(world, key).await {
            Err(StoreError::Fault(crate::protocol::StoreFault::Unavailable)) => {
                retry(state, delay_ns).await;
            }
            result => return result,
        }
    }
}

pub async fn get_range<W: Store>(
    state: &SharedHost,
    world: &W,
    key: &str,
    offset: u64,
    len: u64,
    delay_ns: u64,
) -> Result<Option<(u64, Vec<u8>)>, StoreError> {
    loop {
        match Store::get_range(world, key, offset, len).await {
            Err(StoreError::Fault(crate::protocol::StoreFault::Unavailable)) => {
                retry(state, delay_ns).await;
            }
            result => return result,
        }
    }
}

pub async fn put<W: Store>(
    state: &SharedHost,
    world: &W,
    key: String,
    bytes: Vec<u8>,
    delay_ns: u64,
) -> Result<u64, StoreError> {
    loop {
        match Store::put(world, key.clone(), bytes.clone()).await {
            Err(StoreError::Fault(crate::protocol::StoreFault::Unavailable)) => {
                retry(state, delay_ns).await;
            }
            result => return result,
        }
    }
}

pub async fn put_immutable<W: Store>(
    state: &SharedHost,
    world: &W,
    key: String,
    bytes: Vec<u8>,
    delay_ns: u64,
) -> Result<u64, StoreError> {
    loop {
        match Store::put_cas(world, key.clone(), None, bytes.clone()).await {
            Ok(version) => return Ok(version),
            Err(StoreError::Fault(crate::protocol::StoreFault::Unavailable)) => {
                retry(state, delay_ns).await;
            }
            Err(StoreError::Fault(crate::protocol::StoreFault::CasConflict { actual })) => {
                let found = get(state, world, &key, delay_ns).await?;
                return match found {
                    Some((version, found)) if found == bytes => Ok(version),
                    _ => Err(StoreError::Fault(
                        crate::protocol::StoreFault::CasConflict { actual },
                    )),
                };
            }
            Err(StoreError::TooLarge) => return Err(StoreError::TooLarge),
        }
    }
}

pub async fn read<W: Store>(
    state: &SharedHost,
    world: &W,
    key: &str,
    delay_ns: u64,
) -> Option<Vec<u8>> {
    get(state, world, key, delay_ns)
        .await
        .ok()
        .flatten()
        .map(|(_, bytes)| bytes)
}

pub async fn write<W: Store>(
    state: &SharedHost,
    world: &W,
    key: String,
    bytes: Vec<u8>,
    delay_ns: u64,
) -> Option<u64> {
    put(state, world, key, bytes, delay_ns).await.ok()
}

pub async fn write_immutable<W: Store>(
    state: &SharedHost,
    world: &W,
    key: String,
    bytes: Vec<u8>,
    delay_ns: u64,
) -> Option<u64> {
    put_immutable(state, world, key, bytes, delay_ns).await.ok()
}
