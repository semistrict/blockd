//! The single page-data format used locally, on the passive, and in object storage.

use std::collections::BTreeMap;

use crate::format::{
    Dec, DecodeError, Enc, FRAME_HEADER, checksum64, crc32c, open_frame, seal_frame,
};
use crate::journal::VsetKind;
use crate::types::{Gen, PageId, PageNo, VolumeId, VolumeIdx, VsetId, page_size};

pub const MAGIC_HEADER: u32 = u32::from_le_bytes(*b"BLXH");
pub const MAGIC_ENTRY: u32 = u32::from_le_bytes(*b"BLXE");
pub const MAGIC_FOOTER: u32 = u32::from_le_bytes(*b"BLXF");
pub const MAGIC_TRAILER: u32 = u32::from_le_bytes(*b"BLXT");
pub const TARGET_FILE_BYTES: usize = 32 * 1024 * 1024;
pub const MAX_FILE_BYTES: usize = 64 * 1024 * 1024;
pub const MAX_OVERLAPPING_FILES: usize = 8;
pub const TRAILER_BYTES: usize = 16;
const COMPACTION_OBJECT_IDS_PER_BATCH: u64 = u32::MAX as u64 + 1;

const HEADER_VERSION: u16 = 1;
const HEADER_PAYLOAD_BYTES: usize = 2 + 4 + 1 + 8 * 6 + 4 * 3 + 8 * 2 + 8 * 2;
pub const HEADER_BYTES: usize = FRAME_HEADER + HEADER_PAYLOAD_BYTES;
const FOOTER_PREFIX_BYTES: usize = 2 + 4;
const FOOTER_ENTRY_BYTES: usize = 8 + 4 + 4 + 8 + 1 + 7 + 8;

/// Reserve one non-overlapping object-id range large enough for every chunk a
/// single BLX batch can encode.
pub fn compaction_object_id_start(slot: u64) -> Option<u64> {
    let ranges = slot.checked_add(1)?;
    let end = ranges.checked_mul(COMPACTION_OBJECT_IDS_PER_BATCH)?;
    u64::MAX.checked_sub(end)?.checked_add(1)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum NamespaceKind {
    Vset = 0,
    ImportedBase = 1,
}

impl NamespaceKind {
    fn decode(value: u8) -> Result<Self, DecodeError> {
        match value {
            0 => Ok(Self::Vset),
            1 => Ok(Self::ImportedBase),
            _ => Err(DecodeError),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum BlockSpace {
    Memory = 0,
    Data = 1,
    Vmm = 2,
}

impl BlockSpace {
    fn decode(value: u8) -> Result<Self, DecodeError> {
        match value {
            0 => Ok(Self::Memory),
            1 => Ok(Self::Data),
            2 => Ok(Self::Vmm),
            _ => Err(DecodeError),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlockKey {
    pub space: BlockSpace,
    pub volume: u8,
    pub block: u32,
}

/// Canonical page-sized BLX blocks for one logical VMM snapshot.
pub fn vmm_snapshot_blocks(bytes: &[u8]) -> impl Iterator<Item = (BlockKey, Vec<u8>)> + '_ {
    bytes.chunks(page_size()).enumerate().map(|(block, chunk)| {
        let mut padded = vec![0; page_size()];
        padded[..chunk.len()].copy_from_slice(chunk);
        (
            BlockKey {
                space: BlockSpace::Vmm,
                volume: 0,
                block: u32::try_from(block).expect("VMM snapshot block fits u32"),
            },
            padded,
        )
    })
}

impl BlockKey {
    pub fn file_partition(self) -> (u32, u16) {
        (
            self.block / blocks_per_file_partition(),
            self.namespace_rank() / NAMESPACES_PER_FILE_PARTITION,
        )
    }

    pub fn from_page(kind: VsetKind, page: PageId) -> Self {
        match kind {
            VsetKind::Compute if page.volume.idx.0 == 0 => Self {
                space: BlockSpace::Memory,
                volume: 0,
                block: page.page.0,
            },
            VsetKind::Compute => Self {
                space: BlockSpace::Data,
                volume: page.volume.idx.0,
                block: page.page.0,
            },
        }
    }

    pub fn to_page(self, kind: VsetKind, vset: VsetId) -> Option<PageId> {
        let idx = match (kind, self.space) {
            (VsetKind::Compute, BlockSpace::Memory) if self.volume == 0 => 0,
            (VsetKind::Compute, BlockSpace::Data) => self.volume,
            _ => return None,
        };
        Some(PageId {
            volume: VolumeId {
                vset,
                idx: VolumeIdx(idx),
            },
            page: PageNo(self.block),
        })
    }

    /// Convert a stored page key without needing the manifest's vset kind.
    /// Memory and data occupy distinct spaces, while the volume number is the
    /// same number used by the live page identifier.
    pub fn to_page_id(self, vset: VsetId) -> Option<PageId> {
        match self.space {
            BlockSpace::Memory if self.volume == 0 => {}
            BlockSpace::Data => {}
            BlockSpace::Vmm | BlockSpace::Memory => return None,
        }
        Some(PageId {
            volume: VolumeId {
                vset,
                idx: VolumeIdx(self.volume),
            },
            page: PageNo(self.block),
        })
    }

    fn encode(self, e: &mut Enc) {
        e.u8(self.space as u8);
        e.u8(self.volume);
        e.u16(0);
        e.u32(self.block);
    }

    fn decode(d: &mut Dec<'_>) -> Result<Self, DecodeError> {
        let key = Self {
            space: BlockSpace::decode(d.u8()?)?,
            volume: d.u8()?,
            block: {
                if d.u16()? != 0 {
                    return Err(DecodeError);
                }
                d.u32()?
            },
        };
        if key.space == BlockSpace::Memory && key.volume != 0 {
            return Err(DecodeError);
        }
        Ok(key)
    }

    fn namespace_rank(self) -> u16 {
        match self.space {
            BlockSpace::Memory => 0,
            BlockSpace::Vmm => 1,
            BlockSpace::Data => u16::from(self.volume) + 2,
        }
    }
}

impl Ord for BlockKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        let blocks = blocks_per_file_partition();
        (
            self.block / blocks,
            self.namespace_rank() / NAMESPACES_PER_FILE_PARTITION,
            self.namespace_rank(),
            self.block % blocks,
        )
            .cmp(&(
                other.block / blocks,
                other.namespace_rank() / NAMESPACES_PER_FILE_PARTITION,
                other.namespace_rank(),
                other.block % blocks,
            ))
    }
}

impl PartialOrd for BlockKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

fn blocks_per_file_partition() -> u32 {
    u32::try_from(
        (TARGET_FILE_BYTES / page_size() / usize::from(NAMESPACES_PER_FILE_PARTITION)).max(1),
    )
    .expect("BLX target block count fits u32")
}

const NAMESPACES_PER_FILE_PARTITION: u16 = 8;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlxHeader {
    pub block_size: u32,
    pub namespace_kind: NamespaceKind,
    pub namespace_id: u64,
    pub writer_fence: u64,
    pub object_id: u64,
    pub min_seq: u64,
    pub max_seq: u64,
    pub batch_id: u64,
    pub chunk_index: u32,
    pub chunk_count: u32,
    pub entry_count: u32,
    pub first_key: BlockKey,
    pub last_key: BlockKey,
    pub pre_state_checksum: u64,
    pub post_state_checksum: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum EntryKind {
    Data = 0,
    Tombstone = 1,
}

impl EntryKind {
    fn decode(value: u8) -> Result<Self, DecodeError> {
        match value {
            0 => Ok(Self::Data),
            1 => Ok(Self::Tombstone),
            _ => Err(DecodeError),
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FooterEntry {
    pub key: BlockKey,
    pub offset: u32,
    pub length: u32,
    pub generation: Gen,
    pub kind: EntryKind,
    /// Checksum of the uncompressed block. Tombstones use zero.
    pub value_checksum: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlxFooter {
    pub entries: Vec<FooterEntry>,
}

impl BlxFooter {
    pub fn find(&self, key: BlockKey) -> Option<FooterEntry> {
        self.entries
            .binary_search_by_key(&key, |entry| entry.key)
            .ok()
            .map(|index| self.entries[index])
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum BlxEntry {
    Data {
        key: BlockKey,
        generation: Gen,
        bytes: Vec<u8>,
    },
    Tombstone {
        key: BlockKey,
        generation: Gen,
    },
}

impl BlxEntry {
    pub fn key(&self) -> BlockKey {
        match self {
            Self::Data { key, .. } | Self::Tombstone { key, .. } => *key,
        }
    }

    pub fn generation(&self) -> Gen {
        match self {
            Self::Data { generation, .. } | Self::Tombstone { generation, .. } => *generation,
        }
    }

    pub fn kind(&self) -> EntryKind {
        match self {
            Self::Data { .. } => EntryKind::Data,
            Self::Tombstone { .. } => EntryKind::Tombstone,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BatchMeta {
    pub namespace_kind: NamespaceKind,
    pub namespace_id: u64,
    pub writer_fence: u64,
    pub first_object_id: u64,
    pub min_seq: u64,
    pub max_seq: u64,
    pub batch_id: u64,
    pub pre_state_checksum: u64,
    pub post_state_checksum: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BlxObject {
    pub header: BlxHeader,
    pub footer: BlxFooter,
    pub footer_offset: u32,
    pub footer_length: u32,
    pub checksum: u64,
    pub bytes: Vec<u8>,
}

#[derive(Debug)]
pub struct BlxBatchBuilder {
    meta: BatchMeta,
    entries: BTreeMap<BlockKey, BlxEntry>,
    split_at_file_partitions: bool,
}

impl BlxBatchBuilder {
    pub fn new(meta: BatchMeta) -> Self {
        assert!(meta.min_seq <= meta.max_seq, "invalid sequence interval");
        Self {
            meta,
            entries: BTreeMap::new(),
            split_at_file_partitions: false,
        }
    }

    /// Build objects whose key ranges never cross a storage-file partition.
    ///
    /// Capture and compaction use this so a later compaction can process one
    /// bounded key range at a time. The ordinary constructor remains useful
    /// for small, self-contained objects that must stay together.
    pub fn new_partitioned(meta: BatchMeta) -> Self {
        let mut builder = Self::new(meta);
        builder.split_at_file_partitions = true;
        builder
    }

    pub fn add_data(&mut self, key: BlockKey, generation: Gen, bytes: Vec<u8>) {
        assert_eq!(bytes.len(), page_size(), "BLX data entries store one block");
        self.insert(BlxEntry::Data {
            key,
            generation,
            bytes,
        });
    }

    pub fn add_tombstone(&mut self, key: BlockKey, generation: Gen) {
        self.insert(BlxEntry::Tombstone { key, generation });
    }

    fn insert(&mut self, entry: BlxEntry) {
        let key = entry.key();
        if self
            .entries
            .get(&key)
            .is_none_or(|old| old.generation() <= entry.generation())
        {
            self.entries.insert(key, entry);
        }
    }

    pub fn object_ids_fit(&self) -> bool {
        let mut chunks = usize::from(!self.entries.is_empty());
        let mut current_empty = true;
        let mut estimated = HEADER_BYTES + TRAILER_BYTES;
        let mut partition = None;
        for entry in self.entries.values() {
            if should_split(
                current_empty,
                estimated,
                partition,
                entry,
                self.split_at_file_partitions,
            ) {
                chunks += 1;
                estimated = HEADER_BYTES + TRAILER_BYTES;
            }
            current_empty = false;
            partition = Some(entry.key().file_partition());
            estimated += encoded_entry_upper_bound(entry) + FOOTER_ENTRY_BYTES;
        }
        chunks == 0
            || self
                .meta
                .first_object_id
                .checked_add(u64::try_from(chunks - 1).expect("chunk count fits u64"))
                .is_some()
    }

    pub fn finish(self) -> Vec<BlxObject> {
        if self.entries.is_empty() {
            return Vec::new();
        }
        let mut chunks = Vec::<Vec<BlxEntry>>::new();
        let mut current = Vec::new();
        let mut estimated = HEADER_BYTES + TRAILER_BYTES;
        let mut partition = None;
        for entry in self.entries.into_values() {
            let entry_size = encoded_entry_upper_bound(&entry);
            let entry_partition = entry.key().file_partition();
            if should_split(
                current.is_empty(),
                estimated,
                partition,
                &entry,
                self.split_at_file_partitions,
            ) {
                chunks.push(std::mem::take(&mut current));
                estimated = HEADER_BYTES + TRAILER_BYTES;
            }
            partition = Some(entry_partition);
            estimated += entry_size + FOOTER_ENTRY_BYTES;
            current.push(entry);
        }
        if !current.is_empty() {
            chunks.push(current);
        }
        let chunk_count = u32::try_from(chunks.len()).expect("BLX chunk count fits u32");
        chunks
            .into_iter()
            .enumerate()
            .map(|(index, entries)| {
                let index = u32::try_from(index).expect("BLX chunk index fits u32");
                let object_id = self
                    .meta
                    .first_object_id
                    .checked_add(u64::from(index))
                    .expect("BLX object id overflow");
                encode_object(self.meta, object_id, index, chunk_count, entries)
            })
            .collect()
    }
}

fn should_split(
    current_empty: bool,
    estimated: usize,
    partition: Option<(u32, u16)>,
    entry: &BlxEntry,
    split_at_file_partitions: bool,
) -> bool {
    !current_empty
        && ((split_at_file_partitions && partition != Some(entry.key().file_partition()))
            || estimated
                + encoded_entry_upper_bound(entry)
                + FOOTER_ENTRY_BYTES
                + FRAME_HEADER
                + FOOTER_PREFIX_BYTES
                > TARGET_FILE_BYTES)
}

fn encoded_entry_upper_bound(entry: &BlxEntry) -> usize {
    match entry {
        BlxEntry::Data { bytes, .. } => {
            FRAME_HEADER + 1 + 8 + 8 + 4 + 4 + lz4_flex::block::get_maximum_output_size(bytes.len())
        }
        BlxEntry::Tombstone { .. } => FRAME_HEADER + 1 + 8 + 8,
    }
}

fn encode_object(
    meta: BatchMeta,
    object_id: u64,
    chunk_index: u32,
    chunk_count: u32,
    entries: Vec<BlxEntry>,
) -> BlxObject {
    let first_key = entries.first().expect("nonempty BLX chunk").key();
    let last_key = entries.last().expect("nonempty BLX chunk").key();
    let header = BlxHeader {
        block_size: u32::try_from(page_size()).expect("page size fits u32"),
        namespace_kind: meta.namespace_kind,
        namespace_id: meta.namespace_id,
        writer_fence: meta.writer_fence,
        object_id,
        min_seq: meta.min_seq,
        max_seq: meta.max_seq,
        batch_id: meta.batch_id,
        chunk_index,
        chunk_count,
        entry_count: u32::try_from(entries.len()).expect("entry count fits u32"),
        first_key,
        last_key,
        pre_state_checksum: meta.pre_state_checksum,
        post_state_checksum: meta.post_state_checksum,
    };
    let mut bytes = encode_header(header);
    let mut footer_entries = Vec::with_capacity(entries.len());
    for entry in entries {
        let offset = u32::try_from(bytes.len()).expect("BLX offset fits u32");
        let generation = entry.generation();
        let key = entry.key();
        let kind = entry.kind();
        let encoded = encode_entry(&entry);
        let length = u32::try_from(encoded.len()).expect("BLX entry length fits u32");
        bytes.extend_from_slice(&encoded);
        footer_entries.push(FooterEntry {
            key,
            offset,
            length,
            generation,
            kind,
            value_checksum: match &entry {
                BlxEntry::Data { bytes, .. } => checksum64(bytes),
                BlxEntry::Tombstone { .. } => 0,
            },
        });
    }
    let footer = BlxFooter {
        entries: footer_entries,
    };
    let footer_offset = u32::try_from(bytes.len()).expect("BLX footer offset fits u32");
    let encoded_footer = encode_footer(&footer);
    let footer_length = u32::try_from(encoded_footer.len()).expect("BLX footer length fits u32");
    bytes.extend_from_slice(&encoded_footer);
    bytes.extend_from_slice(&encode_trailer(footer_offset, footer_length));
    assert!(bytes.len() <= MAX_FILE_BYTES, "BLX object exceeds 64 MiB");
    let checksum = checksum64(&bytes);
    BlxObject {
        header,
        footer,
        footer_offset,
        footer_length,
        checksum,
        bytes,
    }
}

fn encode_header(header: BlxHeader) -> Vec<u8> {
    let mut e = Enc::new();
    e.u16(HEADER_VERSION);
    e.u32(header.block_size);
    e.u8(header.namespace_kind as u8);
    e.u64(header.namespace_id);
    e.u64(header.writer_fence);
    e.u64(header.object_id);
    e.u64(header.min_seq);
    e.u64(header.max_seq);
    e.u64(header.batch_id);
    e.u32(header.chunk_index);
    e.u32(header.chunk_count);
    e.u32(header.entry_count);
    header.first_key.encode(&mut e);
    header.last_key.encode(&mut e);
    e.u64(header.pre_state_checksum);
    e.u64(header.post_state_checksum);
    debug_assert_eq!(e.len(), HEADER_PAYLOAD_BYTES);
    seal_frame(MAGIC_HEADER, &e.finish())
}

pub fn open_header(bytes: &[u8]) -> Result<(BlxHeader, usize), DecodeError> {
    if bytes.len() < HEADER_BYTES {
        return Err(DecodeError);
    }
    let end = HEADER_BYTES;
    let payload = open_frame(MAGIC_HEADER, &bytes[..end])?;
    let mut d = Dec::new(payload);
    if d.u16()? != HEADER_VERSION {
        return Err(DecodeError);
    }
    let header = BlxHeader {
        block_size: d.u32()?,
        namespace_kind: NamespaceKind::decode(d.u8()?)?,
        namespace_id: d.u64()?,
        writer_fence: d.u64()?,
        object_id: d.u64()?,
        min_seq: d.u64()?,
        max_seq: d.u64()?,
        batch_id: d.u64()?,
        chunk_index: d.u32()?,
        chunk_count: d.u32()?,
        entry_count: d.u32()?,
        first_key: BlockKey::decode(&mut d)?,
        last_key: BlockKey::decode(&mut d)?,
        pre_state_checksum: d.u64()?,
        post_state_checksum: d.u64()?,
    };
    d.finish()?;
    if header.block_size != u32::try_from(page_size()).expect("page size fits u32")
        || header.min_seq > header.max_seq
        || header.chunk_count == 0
        || header.chunk_index >= header.chunk_count
        || header.entry_count == 0
        || header.first_key > header.last_key
    {
        return Err(DecodeError);
    }
    Ok((header, end))
}

fn encode_entry(entry: &BlxEntry) -> Vec<u8> {
    let mut e = Enc::new();
    e.u8(entry.kind() as u8);
    entry.key().encode(&mut e);
    e.u64(entry.generation().0);
    if let BlxEntry::Data { bytes, .. } = entry {
        let compressed = lz4_flex::block::compress(bytes);
        e.u32(u32::try_from(bytes.len()).expect("raw block length fits u32"));
        e.u32(u32::try_from(compressed.len()).expect("stored block length fits u32"));
        e.bytes(&compressed);
    }
    seal_frame(MAGIC_ENTRY, &e.finish())
}

pub fn open_entry(bytes: &[u8]) -> Result<BlxEntry, DecodeError> {
    let payload = open_frame(MAGIC_ENTRY, bytes)?;
    let mut d = Dec::new(payload);
    let kind = EntryKind::decode(d.u8()?)?;
    let key = BlockKey::decode(&mut d)?;
    let generation = Gen(d.u64()?);
    let entry = match kind {
        EntryKind::Data => {
            let raw_len = usize::try_from(d.u32()?).expect("u32 fits usize");
            let stored_len = usize::try_from(d.u32()?).expect("u32 fits usize");
            if raw_len != page_size() {
                return Err(DecodeError);
            }
            let compressed = d.bytes(stored_len)?;
            let bytes =
                lz4_flex::block::decompress(compressed, raw_len).map_err(|_| DecodeError)?;
            BlxEntry::Data {
                key,
                generation,
                bytes,
            }
        }
        EntryKind::Tombstone => BlxEntry::Tombstone { key, generation },
    };
    d.finish()?;
    Ok(entry)
}

fn encode_footer(footer: &BlxFooter) -> Vec<u8> {
    let mut e = Enc::new();
    e.u16(1);
    e.u32(u32::try_from(footer.entries.len()).expect("footer count fits u32"));
    for entry in &footer.entries {
        entry.key.encode(&mut e);
        e.u32(entry.offset);
        e.u32(entry.length);
        e.u64(entry.generation.0);
        e.u8(entry.kind as u8);
        e.bytes(&[0; 7]);
        e.u64(entry.value_checksum);
    }
    seal_frame(MAGIC_FOOTER, &e.finish())
}

pub fn open_footer(bytes: &[u8]) -> Result<BlxFooter, DecodeError> {
    let payload = open_frame(MAGIC_FOOTER, bytes)?;
    let mut d = Dec::new(payload);
    if d.u16()? != 1 {
        return Err(DecodeError);
    }
    let count = usize::try_from(d.u32()?).expect("u32 fits usize");
    if d.remaining() != count.checked_mul(FOOTER_ENTRY_BYTES).ok_or(DecodeError)? {
        return Err(DecodeError);
    }
    let mut entries = Vec::with_capacity(count);
    let mut previous = None;
    for _ in 0..count {
        let entry = FooterEntry {
            key: BlockKey::decode(&mut d)?,
            offset: d.u32()?,
            length: d.u32()?,
            generation: Gen(d.u64()?),
            kind: EntryKind::decode(d.u8()?)?,
            value_checksum: {
                let reserved = d.bytes(7)?;
                if reserved != [0; 7] {
                    return Err(DecodeError);
                }
                d.u64()?
            },
        };
        if (entry.kind == EntryKind::Tombstone && entry.value_checksum != 0)
            || previous.is_some_and(|key| key >= entry.key)
        {
            return Err(DecodeError);
        }
        previous = Some(entry.key);
        entries.push(entry);
    }
    d.finish()?;
    Ok(BlxFooter { entries })
}

fn encode_trailer(footer_offset: u32, footer_length: u32) -> [u8; TRAILER_BYTES] {
    let mut prefix = [0u8; 12];
    prefix[..4].copy_from_slice(&MAGIC_TRAILER.to_le_bytes());
    prefix[4..8].copy_from_slice(&footer_offset.to_le_bytes());
    prefix[8..12].copy_from_slice(&footer_length.to_le_bytes());
    let mut trailer = [0u8; TRAILER_BYTES];
    trailer[..12].copy_from_slice(&prefix);
    trailer[12..].copy_from_slice(&crc32c(&prefix).to_le_bytes());
    trailer
}

pub fn open_trailer(bytes: &[u8]) -> Result<(u32, u32), DecodeError> {
    if bytes.len() != TRAILER_BYTES {
        return Err(DecodeError);
    }
    let mut d = Dec::new(bytes);
    if d.u32()? != MAGIC_TRAILER {
        return Err(DecodeError);
    }
    let offset = d.u32()?;
    let length = d.u32()?;
    let checksum = d.u32()?;
    d.finish()?;
    if crc32c(&bytes[..12]) != checksum {
        return Err(DecodeError);
    }
    Ok((offset, length))
}

pub fn scan_object(bytes: &[u8]) -> Result<(BlxHeader, BlxFooter), DecodeError> {
    let (header, header_end) = open_header(bytes)?;
    if bytes.len() < header_end + TRAILER_BYTES {
        return Err(DecodeError);
    }
    let (footer_offset, footer_length) = open_trailer(&bytes[bytes.len() - TRAILER_BYTES..])?;
    let footer_offset = usize::try_from(footer_offset).expect("u32 fits usize");
    let footer_length = usize::try_from(footer_length).expect("u32 fits usize");
    let footer_end = footer_offset
        .checked_add(footer_length)
        .ok_or(DecodeError)?;
    if footer_offset < header_end || footer_end + TRAILER_BYTES != bytes.len() {
        return Err(DecodeError);
    }
    let footer = open_footer(&bytes[footer_offset..footer_end])?;
    if footer.entries.len() != usize::try_from(header.entry_count).expect("u32 fits usize")
        || footer.entries.first().map(|entry| entry.key) != Some(header.first_key)
        || footer.entries.last().map(|entry| entry.key) != Some(header.last_key)
    {
        return Err(DecodeError);
    }
    let mut expected_offset = header_end;
    for footer_entry in &footer.entries {
        let offset = usize::try_from(footer_entry.offset).expect("u32 fits usize");
        let length = usize::try_from(footer_entry.length).expect("u32 fits usize");
        let end = offset.checked_add(length).ok_or(DecodeError)?;
        if offset != expected_offset || end > footer_offset {
            return Err(DecodeError);
        }
        let entry = open_entry(&bytes[offset..end])?;
        let value_checksum = match &entry {
            BlxEntry::Data { bytes, .. } => checksum64(bytes),
            BlxEntry::Tombstone { .. } => 0,
        };
        if (
            entry.key(),
            entry.generation(),
            entry.kind(),
            value_checksum,
        ) != (
            footer_entry.key,
            footer_entry.generation,
            footer_entry.kind,
            footer_entry.value_checksum,
        ) {
            return Err(DecodeError);
        }
        expected_offset = end;
    }
    if expected_offset != footer_offset {
        return Err(DecodeError);
    }
    Ok((header, footer))
}

/// One visible data block's layout-independent contribution to the state
/// checksum. XOR makes replacements incremental and compaction-independent.
pub fn state_contribution(key: BlockKey, generation: Gen, value_checksum: u64) -> u64 {
    let mut e = Enc::new();
    key.encode(&mut e);
    e.u64(generation.0);
    e.u64(value_checksum);
    checksum64(&e.finish())
}

pub fn replace_state_block(
    checksum: &mut u64,
    blocks: &mut BTreeMap<BlockKey, (Gen, u64)>,
    key: BlockKey,
    value: Option<(Gen, u64)>,
) {
    if let Some((generation, value_checksum)) = blocks.remove(&key) {
        *checksum ^= state_contribution(key, generation, value_checksum);
    }
    if let Some((generation, value_checksum)) = value {
        *checksum ^= state_contribution(key, generation, value_checksum);
        blocks.insert(key, (generation, value_checksum));
    }
}

/// Open a complete object and retain the verified index and reference fields.
pub fn open_object(bytes: &[u8]) -> Result<BlxObject, DecodeError> {
    let (header, footer) = scan_object(bytes)?;
    let (footer_offset, footer_length) = open_trailer(
        bytes
            .get(bytes.len().checked_sub(TRAILER_BYTES).ok_or(DecodeError)?..)
            .ok_or(DecodeError)?,
    )?;
    Ok(BlxObject {
        header,
        footer,
        footer_offset,
        footer_length,
        checksum: checksum64(bytes),
        bytes: bytes.to_vec(),
    })
}

/// Merge complete input objects into a new batch. The newest generation for
/// each block wins. Callers may discard tombstones only after proving that no
/// remaining file or base can contain the hidden value.
#[derive(Debug, Default)]
pub struct BlxCompactor {
    newest: BTreeMap<BlockKey, BlxEntry>,
}

impl BlxCompactor {
    pub fn add_object(&mut self, bytes: &[u8]) -> Result<(), DecodeError> {
        let (_, footer) = scan_object(bytes)?;
        for indexed in footer.entries {
            let start = usize::try_from(indexed.offset).expect("u32 fits usize");
            let end = start
                .checked_add(usize::try_from(indexed.length).expect("u32 fits usize"))
                .ok_or(DecodeError)?;
            let entry = open_entry(bytes.get(start..end).ok_or(DecodeError)?)?;
            if self
                .newest
                .get(&entry.key())
                .is_none_or(|old| old.generation() <= entry.generation())
            {
                self.newest.insert(entry.key(), entry);
            }
        }
        Ok(())
    }

    pub fn finish(self, meta: BatchMeta, retain_tombstones: bool) -> Vec<BlxObject> {
        let mut builder = BlxBatchBuilder::new_partitioned(meta);
        for entry in self.newest.into_values() {
            match entry {
                BlxEntry::Data {
                    key,
                    generation,
                    bytes,
                } => builder.add_data(key, generation, bytes),
                BlxEntry::Tombstone { key, generation } if retain_tombstones => {
                    builder.add_tombstone(key, generation);
                }
                BlxEntry::Tombstone { .. } => {}
            }
        }
        builder.finish()
    }
}

pub fn compact_objects(
    meta: BatchMeta,
    inputs: &[BlxObject],
    retain_tombstones: bool,
) -> Result<Vec<BlxObject>, DecodeError> {
    let mut compactor = BlxCompactor::default();
    for input in inputs {
        compactor.add_object(&input.bytes)?;
    }
    Ok(compactor.finish(meta, retain_tombstones))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn meta() -> BatchMeta {
        BatchMeta {
            namespace_kind: NamespaceKind::Vset,
            namespace_id: 7,
            writer_fence: 12,
            first_object_id: 30,
            min_seq: 4,
            max_seq: 9,
            batch_id: 5,
            pre_state_checksum: 100,
            post_state_checksum: 200,
        }
    }

    #[test]
    fn compaction_object_ranges_cover_every_u32_chunk_without_overlap() {
        let first = compaction_object_id_start(0).expect("first range");
        let second = compaction_object_id_start(1).expect("second range");
        assert_eq!(first, u64::MAX - u64::from(u32::MAX));
        assert_eq!(second + COMPACTION_OBJECT_IDS_PER_BATCH, first);
        assert_eq!(first + u64::from(u32::MAX), u64::MAX);
    }

    fn key(block: u32) -> BlockKey {
        BlockKey {
            space: BlockSpace::Memory,
            volume: 0,
            block,
        }
    }

    #[test]
    fn partitioned_batches_keep_small_vm_cuts_together_and_bound_large_ranges() {
        let mut builder = BlxBatchBuilder::new_partitioned(meta());
        for key in [
            BlockKey {
                space: BlockSpace::Memory,
                volume: 0,
                block: 0,
            },
            BlockKey {
                space: BlockSpace::Data,
                volume: 1,
                block: 0,
            },
            BlockKey {
                space: BlockSpace::Vmm,
                volume: 0,
                block: 0,
            },
            BlockKey {
                space: BlockSpace::Memory,
                volume: 0,
                block: blocks_per_file_partition(),
            },
        ] {
            builder.add_data(key, Gen(1), vec![1; page_size()]);
        }
        let objects = builder.finish();
        assert_eq!(objects.len(), 2);
        assert_eq!(objects[0].header.entry_count, 3);
        assert!(objects.iter().all(|object| {
            object.header.first_key.file_partition() == object.header.last_key.file_partition()
        }));
    }

    #[test]
    fn object_round_trips_with_exact_footer_locations() {
        let mut builder = BlxBatchBuilder::new(meta());
        builder.add_data(key(2), Gen(4), vec![0x55; page_size()]);
        builder.add_tombstone(key(8), Gen(7));
        let objects = builder.finish();
        assert_eq!(objects.len(), 1);
        let object = &objects[0];
        let (header, footer) = scan_object(&object.bytes).expect("valid BLX object");
        assert_eq!(header, object.header);
        assert_eq!(footer, object.footer);
        let found = footer.find(key(2)).expect("indexed block");
        let entry = open_entry(
            &object.bytes[found.offset as usize..(found.offset + found.length) as usize],
        )
        .expect("entry");
        assert!(matches!(entry, BlxEntry::Data { bytes, .. } if bytes == vec![0x55; page_size()]));
    }

    #[test]
    fn newest_duplicate_wins_and_keys_are_sorted() {
        let mut builder = BlxBatchBuilder::new(meta());
        builder.add_data(key(9), Gen(2), vec![2; page_size()]);
        builder.add_data(key(1), Gen(1), vec![1; page_size()]);
        builder.add_tombstone(key(9), Gen(3));
        let object = builder.finish().pop().expect("one object");
        assert_eq!(object.footer.entries[0].key, key(1));
        assert_eq!(object.footer.entries[1].key, key(9));
        assert_eq!(object.footer.entries[1].kind, EntryKind::Tombstone);
    }

    #[test]
    fn every_prefix_truncation_is_rejected() {
        let mut builder = BlxBatchBuilder::new(meta());
        builder.add_data(key(1), Gen(1), vec![9; page_size()]);
        let bytes = builder.finish().pop().expect("object").bytes;
        for keep in 0..bytes.len() {
            assert!(
                scan_object(&bytes[..keep]).is_err(),
                "accepted {keep} bytes"
            );
        }
    }

    #[test]
    fn block_keys_map_compute_volumes() {
        let page = |idx| PageId {
            volume: VolumeId {
                vset: VsetId(7),
                idx: VolumeIdx(idx),
            },
            page: PageNo(3),
        };
        assert_eq!(
            BlockKey::from_page(VsetKind::Compute, page(0)).space,
            BlockSpace::Memory
        );
        assert_eq!(BlockKey::from_page(VsetKind::Compute, page(2)).volume, 2);
    }

    #[test]
    fn compaction_keeps_only_the_newest_generation() {
        let mut first = BlxBatchBuilder::new(meta());
        first.add_data(key(1), Gen(1), vec![1; page_size()]);
        first.add_data(key(2), Gen(2), vec![2; page_size()]);
        let first = first.finish();
        let mut later_meta = meta();
        later_meta.first_object_id = 40;
        later_meta.batch_id = 6;
        let mut later = BlxBatchBuilder::new(later_meta);
        later.add_data(key(1), Gen(3), vec![3; page_size()]);
        later.add_tombstone(key(2), Gen(4));
        let later = later.finish();
        let mut output_meta = meta();
        output_meta.first_object_id = 50;
        output_meta.batch_id = 7;
        let compacted = compact_objects(output_meta, &[first[0].clone(), later[0].clone()], true)
            .expect("compact");
        let (_, footer) = scan_object(&compacted[0].bytes).expect("output");
        assert_eq!(footer.find(key(1)).expect("key 1").generation, Gen(3));
        assert_eq!(
            footer.find(key(2)).expect("key 2").kind,
            EntryKind::Tombstone
        );
    }
}
