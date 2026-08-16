//! Current archive metadata: one complete file list plus bounded changes.

use std::collections::{BTreeMap, BTreeSet};

use crate::blx::{BlockKey, BlxObject, MAX_OVERLAPPING_FILES, NamespaceKind};
use crate::format::{Dec, DecodeError, Enc, checksum64, open_frame, seal_frame};
use crate::head::ManifestPtr;
use crate::journal::{VsetConfig, VsetKind};
use crate::types::{Epoch, VsetId, page_size};

pub const MAGIC_FILE_LIST: u32 = u32::from_le_bytes(*b"BLFL");
pub const MAGIC_MANIFEST: u32 = u32::from_le_bytes(*b"BLMF");
pub const MAGIC_BASE_MANIFEST: u32 = u32::from_le_bytes(*b"BLBM");
pub const MAGIC_BASE_ROOT: u32 = u32::from_le_bytes(*b"BLBR");
pub const MAX_MANIFEST_BYTES: usize = 256 * 1024;
pub const MAX_METADATA_OBJECT_BYTES: usize = 64 * 1024 * 1024;
pub const OBJECT_IDENTITY_BYTES: usize = 25;
pub const OBJECT_REF_BYTES: usize = 109;

const FORMAT_VERSION: u16 = 1;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct ObjectIdentity {
    pub namespace_kind: NamespaceKind,
    pub namespace_id: u64,
    pub writer_fence: u64,
    pub object_id: u64,
}

impl ObjectIdentity {
    pub fn store_key(self) -> String {
        match self.namespace_kind {
            NamespaceKind::Vset => {
                crate::layout::blx_key(VsetId(self.namespace_id), self.writer_fence, self.object_id)
            }
            NamespaceKind::ImportedBase => format!(
                "b/{:016x}/o/{:016x}-{:016x}.blx",
                self.namespace_id, self.writer_fence, self.object_id
            ),
        }
    }

    fn encode(self, e: &mut Enc) {
        e.u8(self.namespace_kind as u8);
        e.u64(self.namespace_id);
        e.u64(self.writer_fence);
        e.u64(self.object_id);
    }

    fn decode(d: &mut Dec<'_>) -> Result<Self, DecodeError> {
        Ok(Self {
            namespace_kind: decode_namespace(d.u8()?)?,
            namespace_id: d.u64()?,
            writer_fence: d.u64()?,
            object_id: d.u64()?,
        })
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ObjectRef {
    pub identity: ObjectIdentity,
    pub min_seq: u64,
    pub max_seq: u64,
    pub batch_id: u64,
    pub chunk_index: u32,
    pub chunk_count: u32,
    pub first_key: BlockKey,
    pub last_key: BlockKey,
    pub pre_state_checksum: u64,
    pub post_state_checksum: u64,
    pub size: u32,
    pub footer_offset: u32,
    pub footer_length: u32,
    pub object_checksum: u64,
}

impl ObjectRef {
    pub fn from_blx(object: &BlxObject) -> Self {
        let header = object.header;
        Self {
            identity: ObjectIdentity {
                namespace_kind: header.namespace_kind,
                namespace_id: header.namespace_id,
                writer_fence: header.writer_fence,
                object_id: header.object_id,
            },
            min_seq: header.min_seq,
            max_seq: header.max_seq,
            batch_id: header.batch_id,
            chunk_index: header.chunk_index,
            chunk_count: header.chunk_count,
            first_key: header.first_key,
            last_key: header.last_key,
            pre_state_checksum: header.pre_state_checksum,
            post_state_checksum: header.post_state_checksum,
            size: u32::try_from(object.bytes.len()).expect("BLX size fits u32"),
            footer_offset: object.footer_offset,
            footer_length: object.footer_length,
            object_checksum: object.checksum,
        }
    }

    pub fn encode_into(self, e: &mut Enc) {
        self.identity.encode(e);
        e.u64(self.min_seq);
        e.u64(self.max_seq);
        e.u64(self.batch_id);
        e.u32(self.chunk_index);
        e.u32(self.chunk_count);
        encode_key(self.first_key, e);
        encode_key(self.last_key, e);
        e.u64(self.pre_state_checksum);
        e.u64(self.post_state_checksum);
        e.u32(self.size);
        e.u32(self.footer_offset);
        e.u32(self.footer_length);
        e.u64(self.object_checksum);
    }

    pub fn decode_from(d: &mut Dec<'_>) -> Result<Self, DecodeError> {
        let object_ref = Self {
            identity: ObjectIdentity::decode(d)?,
            min_seq: d.u64()?,
            max_seq: d.u64()?,
            batch_id: d.u64()?,
            chunk_index: d.u32()?,
            chunk_count: d.u32()?,
            first_key: decode_key(d)?,
            last_key: decode_key(d)?,
            pre_state_checksum: d.u64()?,
            post_state_checksum: d.u64()?,
            size: d.u32()?,
            footer_offset: d.u32()?,
            footer_length: d.u32()?,
            object_checksum: d.u64()?,
        };
        if object_ref.min_seq > object_ref.max_seq
            || object_ref.chunk_count == 0
            || object_ref.chunk_index >= object_ref.chunk_count
            || object_ref.first_key > object_ref.last_key
            || object_ref.size == 0
            || object_ref.footer_length == 0
            || object_ref
                .footer_offset
                .checked_add(object_ref.footer_length)
                .is_none_or(|end| end > object_ref.size)
        {
            return Err(DecodeError);
        }
        Ok(object_ref)
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BaseRef {
    pub base_id: u64,
    pub manifest_id: u64,
    pub manifest_checksum: u64,
    pub post_state_checksum: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FileListRef {
    pub writer_fence: u64,
    pub list_id: u64,
    pub checksum: u64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u8)]
pub enum RecoveryKind {
    Whole = 0,
    DiskOnly = 1,
}

impl RecoveryKind {
    fn decode(value: u8) -> Result<Self, DecodeError> {
        match value {
            0 => Ok(Self::Whole),
            1 => Ok(Self::DiskOnly),
            _ => Err(DecodeError),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CompleteFileList {
    pub vset: VsetId,
    pub writer_fence: u64,
    pub list_id: u64,
    pub objects: Vec<ObjectRef>,
}

impl CompleteFileList {
    pub fn encode(&self) -> Vec<u8> {
        assert_valid_objects(&self.objects);
        let mut e = Enc::new();
        e.u16(FORMAT_VERSION);
        e.u64(self.vset.0);
        e.u64(self.writer_fence);
        e.u64(self.list_id);
        e.u32(u32::try_from(self.objects.len()).expect("file count fits u32"));
        for object in &self.objects {
            object.encode_into(&mut e);
        }
        let content_checksum = checksum64(&e.finish());
        let mut e = Enc::new();
        e.u16(FORMAT_VERSION);
        e.u64(self.vset.0);
        e.u64(self.writer_fence);
        e.u64(self.list_id);
        e.u32(u32::try_from(self.objects.len()).expect("file count fits u32"));
        for object in &self.objects {
            object.encode_into(&mut e);
        }
        e.u64(content_checksum);
        let bytes = seal_frame(MAGIC_FILE_LIST, &e.finish());
        assert!(
            bytes.len() <= MAX_METADATA_OBJECT_BYTES,
            "file list exceeds 64 MiB"
        );
        bytes
    }

    pub fn decode(expected: FileListRef, vset: VsetId, bytes: &[u8]) -> Result<Self, DecodeError> {
        if bytes.len() > MAX_METADATA_OBJECT_BYTES || checksum64(bytes) != expected.checksum {
            return Err(DecodeError);
        }
        let payload = open_frame(MAGIC_FILE_LIST, bytes)?;
        let mut d = Dec::new(payload);
        if d.u16()? != FORMAT_VERSION || d.u64()? != vset.0 {
            return Err(DecodeError);
        }
        let writer_fence = d.u64()?;
        let list_id = d.u64()?;
        if (writer_fence, list_id) != (expected.writer_fence, expected.list_id) {
            return Err(DecodeError);
        }
        let count = usize::try_from(d.u32()?).expect("u32 fits usize");
        let mut objects = Vec::with_capacity(count);
        for _ in 0..count {
            objects.push(ObjectRef::decode_from(&mut d)?);
        }
        let stored_content_checksum = d.u64()?;
        d.finish()?;
        let mut content = Enc::new();
        content.u16(FORMAT_VERSION);
        content.u64(vset.0);
        content.u64(writer_fence);
        content.u64(list_id);
        content.u32(u32::try_from(objects.len()).expect("count fits u32"));
        for object in &objects {
            object.encode_into(&mut content);
        }
        if checksum64(&content.finish()) != stored_content_checksum {
            return Err(DecodeError);
        }
        validate_objects(&objects, Some(MAX_OVERLAPPING_FILES))?;
        Ok(Self {
            vset,
            writer_fence,
            list_id,
            objects,
        })
    }

    pub fn reference(&self) -> FileListRef {
        let bytes = self.encode();
        FileListRef {
            writer_fence: self.writer_fence,
            list_id: self.list_id,
            checksum: checksum64(&bytes),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Manifest {
    pub vset: VsetId,
    pub writer_fence: u64,
    pub journal_seq: u64,
    pub archive_seq: u64,
    pub capture_seq: u64,
    pub sync_covered_through: u64,
    pub recovery_kind: RecoveryKind,
    pub checkpoint_epoch: Epoch,
    pub config: VsetConfig,
    pub vmstate_logical_length: u64,
    pub base: Option<BaseRef>,
    pub complete_list: Option<FileListRef>,
    pub post_state_checksum: u64,
    pub metadata_checksum: u64,
    pub added: Vec<ObjectRef>,
    pub removed: Vec<ObjectIdentity>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ManifestClosure {
    pub manifest: Manifest,
    pub complete_list: Option<CompleteFileList>,
    pub files: Vec<ObjectRef>,
}

pub fn decode_manifest_closure(
    vset: VsetId,
    pointer: ManifestPtr,
    manifest_bytes: &[u8],
    complete_list_bytes: Option<&[u8]>,
) -> Result<ManifestClosure, DecodeError> {
    if checksum64(manifest_bytes) != pointer.checksum {
        return Err(DecodeError);
    }
    let manifest = Manifest::decode(vset, manifest_bytes)?;
    if (
        manifest.writer_fence,
        manifest.journal_seq,
        manifest.archive_seq,
        manifest.capture_seq,
    ) != (
        pointer.fence,
        pointer.journal_seq.0,
        pointer.seq.0,
        pointer.capture_seq,
    ) {
        return Err(DecodeError);
    }
    let complete_list = match (manifest.complete_list, complete_list_bytes) {
        (None, None) => None,
        (Some(reference), Some(bytes)) => Some(CompleteFileList::decode(reference, vset, bytes)?),
        _ => return Err(DecodeError),
    };
    let files = manifest.current_files(complete_list.as_ref())?;
    Ok(ManifestClosure {
        manifest,
        complete_list,
        files,
    })
}

impl Manifest {
    pub fn encode(&self) -> Result<Vec<u8>, ManifestTooLarge> {
        assert_manifest_shape(self);
        let mut e = Enc::new();
        encode_manifest_prefix(self, &mut e);
        e.u32(u32::try_from(self.added.len()).expect("added count fits u32"));
        for object in &self.added {
            object.encode_into(&mut e);
        }
        e.u32(u32::try_from(self.removed.len()).expect("removed count fits u32"));
        for identity in &self.removed {
            identity.encode(&mut e);
        }
        let bytes = seal_frame(MAGIC_MANIFEST, &e.finish());
        if bytes.len() > MAX_MANIFEST_BYTES {
            Err(ManifestTooLarge)
        } else {
            Ok(bytes)
        }
    }

    pub fn decode(vset: VsetId, bytes: &[u8]) -> Result<Self, DecodeError> {
        if bytes.len() > MAX_MANIFEST_BYTES {
            return Err(DecodeError);
        }
        let payload = open_frame(MAGIC_MANIFEST, bytes)?;
        let mut d = Dec::new(payload);
        let mut manifest = decode_manifest_prefix(vset, &mut d)?;
        let added_count = usize::try_from(d.u32()?).expect("u32 fits usize");
        for _ in 0..added_count {
            manifest.added.push(ObjectRef::decode_from(&mut d)?);
        }
        let removed_count = usize::try_from(d.u32()?).expect("u32 fits usize");
        for _ in 0..removed_count {
            manifest.removed.push(ObjectIdentity::decode(&mut d)?);
        }
        d.finish()?;
        validate_manifest_shape(&manifest)?;
        Ok(manifest)
    }

    pub fn current_files(
        &self,
        complete_list: Option<&CompleteFileList>,
    ) -> Result<Vec<ObjectRef>, DecodeError> {
        match (self.complete_list, complete_list) {
            (None, None) => {}
            (Some(expected), Some(list)) if list.reference() == expected => {}
            _ => return Err(DecodeError),
        }
        let mut files = complete_list
            .into_iter()
            .flat_map(|list| list.objects.iter().copied())
            .map(|object| (object.identity, object))
            .collect::<BTreeMap<_, _>>();
        for identity in &self.removed {
            if files.remove(identity).is_none() {
                return Err(DecodeError);
            }
        }
        for object in &self.added {
            if files.insert(object.identity, *object).is_some() {
                return Err(DecodeError);
            }
        }
        let files = files.into_values().collect::<Vec<_>>();
        validate_objects(&files, Some(MAX_OVERLAPPING_FILES))?;
        validate_state_chain(self, &files)?;
        Ok(files)
    }
}

fn validate_state_chain(manifest: &Manifest, files: &[ObjectRef]) -> Result<(), DecodeError> {
    validate_file_state_chain(
        manifest.base.map_or(0, |base| base.post_state_checksum),
        manifest.post_state_checksum,
        files,
    )
}

pub fn validate_file_state_chain(
    base_checksum: u64,
    post_state_checksum: u64,
    files: &[ObjectRef],
) -> Result<(), DecodeError> {
    let mut batches = BTreeMap::<(u64, u64), (u64, u64)>::new();
    for file in files {
        let state = (file.pre_state_checksum, file.post_state_checksum);
        if batches
            .insert((file.max_seq, file.batch_id), state)
            .is_some_and(|old| old != state)
        {
            return Err(DecodeError);
        }
    }
    if batches.is_empty() {
        return (post_state_checksum == base_checksum)
            .then_some(())
            .ok_or(DecodeError);
    }
    if batches.len() == 1 {
        let (pre, post) = *batches.values().next().expect("one batch");
        if pre == post && post == post_state_checksum {
            return Ok(());
        }
    }
    let mut batches = batches.into_values();
    let (first_pre, first_post) = batches.next().expect("non-empty batches");
    let mut checksum = if first_pre == first_post {
        // Compaction writes a complete materialized state. It replaces the
        // earlier change history, so it becomes the starting checksum for
        // any later changes in this manifest.
        first_post
    } else {
        if first_pre != base_checksum {
            return Err(DecodeError);
        }
        first_post
    };
    for (pre, post) in batches {
        if pre != checksum {
            return Err(DecodeError);
        }
        checksum = post;
    }
    (checksum == post_state_checksum)
        .then_some(())
        .ok_or(DecodeError)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ManifestTooLarge;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BoundedManifest {
    pub manifest: Manifest,
    /// Present only when the change arrays had to be reset.
    pub new_complete_list: Option<CompleteFileList>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BaseRoot {
    pub base_id: u64,
    pub manifest_id: u64,
    pub manifest_checksum: u64,
    pub post_state_checksum: u64,
}

impl BaseRoot {
    pub fn encode(self) -> Vec<u8> {
        let mut e = Enc::new();
        e.u16(FORMAT_VERSION);
        e.u64(self.base_id);
        e.u64(self.manifest_id);
        e.u64(self.manifest_checksum);
        e.u64(self.post_state_checksum);
        seal_frame(MAGIC_BASE_ROOT, &e.finish())
    }

    pub fn decode(base_id: u64, bytes: &[u8]) -> Result<Self, DecodeError> {
        let payload = open_frame(MAGIC_BASE_ROOT, bytes)?;
        let mut d = Dec::new(payload);
        if d.u16()? != FORMAT_VERSION || d.u64()? != base_id {
            return Err(DecodeError);
        }
        let root = Self {
            base_id,
            manifest_id: d.u64()?,
            manifest_checksum: d.u64()?,
            post_state_checksum: d.u64()?,
        };
        d.finish()?;
        Ok(root)
    }

    pub fn as_base_ref(self) -> BaseRef {
        BaseRef {
            base_id: self.base_id,
            manifest_id: self.manifest_id,
            manifest_checksum: self.manifest_checksum,
            post_state_checksum: self.post_state_checksum,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BaseManifest {
    pub base_id: u64,
    pub manifest_id: u64,
    pub capture_seq: u64,
    pub sync_covered_through: u64,
    pub recovery_kind: RecoveryKind,
    pub checkpoint_epoch: Epoch,
    pub config: VsetConfig,
    pub vmstate_logical_length: u64,
    pub post_state_checksum: u64,
    pub metadata_checksum: u64,
    pub objects: Vec<ObjectRef>,
}

impl BaseManifest {
    pub fn encode(&self) -> Vec<u8> {
        assert_valid_objects(&self.objects);
        assert!(self.sync_covered_through <= self.capture_seq);
        let mut e = Enc::new();
        e.u16(FORMAT_VERSION);
        e.u64(self.base_id);
        e.u64(self.manifest_id);
        e.u64(self.capture_seq);
        e.u64(self.sync_covered_through);
        e.u8(self.recovery_kind as u8);
        e.u64(self.checkpoint_epoch.0);
        e.u32(u32::try_from(page_size()).expect("page size fits u32"));
        encode_config(self.config, &mut e);
        e.u64(self.vmstate_logical_length);
        e.u64(self.post_state_checksum);
        e.u64(self.metadata_checksum);
        e.u32(u32::try_from(self.objects.len()).expect("base file count fits u32"));
        for object in &self.objects {
            object.encode_into(&mut e);
        }
        let bytes = seal_frame(MAGIC_BASE_MANIFEST, &e.finish());
        assert!(
            bytes.len() <= MAX_METADATA_OBJECT_BYTES,
            "base manifest exceeds 64 MiB"
        );
        bytes
    }

    pub fn decode(root: BaseRoot, bytes: &[u8]) -> Result<Self, DecodeError> {
        if bytes.len() > MAX_METADATA_OBJECT_BYTES || checksum64(bytes) != root.manifest_checksum {
            return Err(DecodeError);
        }
        let payload = open_frame(MAGIC_BASE_MANIFEST, bytes)?;
        let mut d = Dec::new(payload);
        if d.u16()? != FORMAT_VERSION || d.u64()? != root.base_id || d.u64()? != root.manifest_id {
            return Err(DecodeError);
        }
        let capture_seq = d.u64()?;
        let sync_covered_through = d.u64()?;
        let recovery_kind = RecoveryKind::decode(d.u8()?)?;
        let checkpoint_epoch = Epoch(d.u64()?);
        if d.u32()? != u32::try_from(page_size()).expect("page size fits u32") {
            return Err(DecodeError);
        }
        let config = decode_config(&mut d)?;
        let vmstate_logical_length = d.u64()?;
        let post_state_checksum = d.u64()?;
        let metadata_checksum = d.u64()?;
        let count = usize::try_from(d.u32()?).expect("u32 fits usize");
        let mut objects = Vec::with_capacity(count);
        for _ in 0..count {
            objects.push(ObjectRef::decode_from(&mut d)?);
        }
        d.finish()?;
        if sync_covered_through > capture_seq || post_state_checksum != root.post_state_checksum {
            return Err(DecodeError);
        }
        validate_objects(&objects, Some(MAX_OVERLAPPING_FILES))?;
        Ok(Self {
            base_id: root.base_id,
            manifest_id: root.manifest_id,
            capture_seq,
            sync_covered_through,
            recovery_kind,
            checkpoint_epoch,
            config,
            vmstate_logical_length,
            post_state_checksum,
            metadata_checksum,
            objects,
        })
    }

    pub fn root(&self) -> BaseRoot {
        let bytes = self.encode();
        BaseRoot {
            base_id: self.base_id,
            manifest_id: self.manifest_id,
            manifest_checksum: checksum64(&bytes),
            post_state_checksum: self.post_state_checksum,
        }
    }
}

/// Enforce the 256 KiB manifest limit. A manifest that already fits is left
/// alone. Otherwise its resulting current file set becomes a new complete
/// list and both change arrays are cleared.
pub fn bound_manifest(
    mut manifest: Manifest,
    current_complete_list: Option<&CompleteFileList>,
    new_list_id: u64,
) -> Result<BoundedManifest, DecodeError> {
    if manifest.encode().is_ok() {
        return Ok(BoundedManifest {
            manifest,
            new_complete_list: None,
        });
    }
    let objects = manifest.current_files(current_complete_list)?;
    let list = CompleteFileList {
        vset: manifest.vset,
        writer_fence: manifest.writer_fence,
        list_id: new_list_id,
        objects,
    };
    manifest.complete_list = Some(list.reference());
    manifest.added.clear();
    manifest.removed.clear();
    manifest.encode().map_err(|_| DecodeError)?;
    Ok(BoundedManifest {
        manifest,
        new_complete_list: Some(list),
    })
}

fn encode_manifest_prefix(manifest: &Manifest, e: &mut Enc) {
    e.u16(FORMAT_VERSION);
    e.u64(manifest.vset.0);
    e.u64(manifest.writer_fence);
    e.u64(manifest.journal_seq);
    e.u64(manifest.archive_seq);
    e.u64(manifest.capture_seq);
    e.u64(manifest.sync_covered_through);
    e.u8(manifest.recovery_kind as u8);
    e.u64(manifest.checkpoint_epoch.0);
    e.u32(u32::try_from(page_size()).expect("page size fits u32"));
    encode_config(manifest.config, e);
    e.u64(manifest.vmstate_logical_length);
    encode_base_ref(manifest.base, e);
    encode_list_ref(manifest.complete_list, e);
    e.u64(manifest.post_state_checksum);
    e.u64(manifest.metadata_checksum);
}

fn decode_manifest_prefix(vset: VsetId, d: &mut Dec<'_>) -> Result<Manifest, DecodeError> {
    if d.u16()? != FORMAT_VERSION || d.u64()? != vset.0 {
        return Err(DecodeError);
    }
    let writer_fence = d.u64()?;
    let journal_seq = d.u64()?;
    let archive_seq = d.u64()?;
    let capture_seq = d.u64()?;
    let sync_covered_through = d.u64()?;
    let recovery_kind = RecoveryKind::decode(d.u8()?)?;
    let checkpoint_epoch = Epoch(d.u64()?);
    if d.u32()? != u32::try_from(page_size()).expect("page size fits u32") {
        return Err(DecodeError);
    }
    let config = decode_config(d)?;
    Ok(Manifest {
        vset,
        writer_fence,
        journal_seq,
        archive_seq,
        capture_seq,
        sync_covered_through,
        recovery_kind,
        checkpoint_epoch,
        config,
        vmstate_logical_length: d.u64()?,
        base: decode_base_ref(d)?,
        complete_list: decode_list_ref(d)?,
        post_state_checksum: d.u64()?,
        metadata_checksum: d.u64()?,
        added: Vec::new(),
        removed: Vec::new(),
    })
}

fn encode_config(config: VsetConfig, e: &mut Enc) {
    e.u8(config.kind as u8);
    e.u8(config.disk_volumes);
    e.u32(config.pages_per_volume);
}

fn decode_config(d: &mut Dec<'_>) -> Result<VsetConfig, DecodeError> {
    let kind = match d.u8()? {
        0 => VsetKind::Compute,
        _ => return Err(DecodeError),
    };
    let config = VsetConfig {
        kind,
        disk_volumes: d.u8()?,
        pages_per_volume: d.u32()?,
    };
    if config.pages_per_volume == 0 {
        return Err(DecodeError);
    }
    Ok(config)
}

fn encode_base_ref(value: Option<BaseRef>, e: &mut Enc) {
    match value {
        None => {
            e.u8(0);
            for _ in 0..4 {
                e.u64(0);
            }
        }
        Some(value) => {
            e.u8(1);
            e.u64(value.base_id);
            e.u64(value.manifest_id);
            e.u64(value.manifest_checksum);
            e.u64(value.post_state_checksum);
        }
    }
}

fn decode_base_ref(d: &mut Dec<'_>) -> Result<Option<BaseRef>, DecodeError> {
    let present = d.u8()?;
    let value = BaseRef {
        base_id: d.u64()?,
        manifest_id: d.u64()?,
        manifest_checksum: d.u64()?,
        post_state_checksum: d.u64()?,
    };
    match present {
        0 if value
            == BaseRef {
                base_id: 0,
                manifest_id: 0,
                manifest_checksum: 0,
                post_state_checksum: 0,
            } =>
        {
            Ok(None)
        }
        1 => Ok(Some(value)),
        _ => Err(DecodeError),
    }
}

fn encode_list_ref(value: Option<FileListRef>, e: &mut Enc) {
    match value {
        None => {
            e.u8(0);
            for _ in 0..3 {
                e.u64(0);
            }
        }
        Some(value) => {
            e.u8(1);
            e.u64(value.writer_fence);
            e.u64(value.list_id);
            e.u64(value.checksum);
        }
    }
}

fn decode_list_ref(d: &mut Dec<'_>) -> Result<Option<FileListRef>, DecodeError> {
    let present = d.u8()?;
    let value = FileListRef {
        writer_fence: d.u64()?,
        list_id: d.u64()?,
        checksum: d.u64()?,
    };
    match present {
        0 if value
            == FileListRef {
                writer_fence: 0,
                list_id: 0,
                checksum: 0,
            } =>
        {
            Ok(None)
        }
        1 => Ok(Some(value)),
        _ => Err(DecodeError),
    }
}

fn decode_namespace(value: u8) -> Result<NamespaceKind, DecodeError> {
    match value {
        0 => Ok(NamespaceKind::Vset),
        1 => Ok(NamespaceKind::ImportedBase),
        _ => Err(DecodeError),
    }
}

fn encode_key(key: BlockKey, e: &mut Enc) {
    e.u8(key.space as u8);
    e.u8(key.volume);
    e.u16(0);
    e.u32(key.block);
}

fn decode_key(d: &mut Dec<'_>) -> Result<BlockKey, DecodeError> {
    let space = match d.u8()? {
        0 => crate::blx::BlockSpace::Memory,
        1 => crate::blx::BlockSpace::Data,
        2 => crate::blx::BlockSpace::Vmm,
        _ => return Err(DecodeError),
    };
    let volume = d.u8()?;
    if d.u16()? != 0 || (space == crate::blx::BlockSpace::Memory && volume != 0) {
        return Err(DecodeError);
    }
    Ok(BlockKey {
        space,
        volume,
        block: d.u32()?,
    })
}

fn assert_manifest_shape(manifest: &Manifest) {
    assert!(
        validate_manifest_shape(manifest).is_ok(),
        "invalid manifest"
    );
}

fn validate_manifest_shape(manifest: &Manifest) -> Result<(), DecodeError> {
    if manifest.sync_covered_through > manifest.capture_seq
        || (manifest.complete_list.is_none() && !manifest.removed.is_empty())
    {
        return Err(DecodeError);
    }
    validate_objects(&manifest.added, Some(MAX_OVERLAPPING_FILES))?;
    let added = manifest
        .added
        .iter()
        .map(|object| object.identity)
        .collect::<BTreeSet<_>>();
    let removed = manifest.removed.iter().copied().collect::<BTreeSet<_>>();
    if added.len() != manifest.added.len()
        || removed.len() != manifest.removed.len()
        || added.iter().any(|identity| removed.contains(identity))
    {
        return Err(DecodeError);
    }
    Ok(())
}

fn assert_valid_objects(objects: &[ObjectRef]) {
    assert!(
        validate_objects(objects, Some(MAX_OVERLAPPING_FILES)).is_ok(),
        "invalid object references"
    );
}

fn validate_objects(
    objects: &[ObjectRef],
    maximum_overlap: Option<usize>,
) -> Result<(), DecodeError> {
    struct Batch {
        chunk_count: u32,
        min_seq: u64,
        max_seq: u64,
        pre_state_checksum: u64,
        post_state_checksum: u64,
        chunks: BTreeMap<u32, (BlockKey, BlockKey)>,
    }

    let mut identities = BTreeSet::new();
    let mut batches = BTreeMap::<(ObjectIdentity, u64), Batch>::new();
    for object in objects {
        if !identities.insert(object.identity) {
            return Err(DecodeError);
        }
        let batch_namespace = ObjectIdentity {
            object_id: 0,
            ..object.identity
        };
        let batch = batches
            .entry((batch_namespace, object.batch_id))
            .or_insert_with(|| Batch {
                chunk_count: object.chunk_count,
                min_seq: object.min_seq,
                max_seq: object.max_seq,
                pre_state_checksum: object.pre_state_checksum,
                post_state_checksum: object.post_state_checksum,
                chunks: BTreeMap::new(),
            });
        if batch.chunk_count != object.chunk_count
            || batch.min_seq != object.min_seq
            || batch.max_seq != object.max_seq
            || batch.pre_state_checksum != object.pre_state_checksum
            || batch.post_state_checksum != object.post_state_checksum
            || batch
                .chunks
                .insert(object.chunk_index, (object.first_key, object.last_key))
                .is_some()
        {
            return Err(DecodeError);
        }
    }
    for batch in batches.values() {
        if batch.chunks.len() != usize::try_from(batch.chunk_count).expect("u32 fits usize")
            || !(0..batch.chunk_count).all(|index| batch.chunks.contains_key(&index))
        {
            return Err(DecodeError);
        }
        let mut previous_last = None;
        for &(first, last) in batch.chunks.values() {
            if previous_last.is_some_and(|previous| previous >= first) {
                return Err(DecodeError);
            }
            previous_last = Some(last);
        }
    }
    let mut events = BTreeMap::<BlockKey, (usize, usize)>::new();
    for object in objects {
        events.entry(object.first_key).or_default().0 += 1;
        events.entry(object.last_key).or_default().1 += 1;
    }
    let mut overlapping = 0usize;
    for (_, (starts, ends)) in events {
        overlapping = overlapping.checked_add(starts).ok_or(DecodeError)?;
        if maximum_overlap.is_some_and(|maximum| overlapping > maximum) || ends > overlapping {
            return Err(DecodeError);
        }
        overlapping -= ends;
    }
    if overlapping != 0 {
        return Err(DecodeError);
    }
    Ok(())
}

pub fn validate_object_refs(objects: &[ObjectRef]) -> Result<(), DecodeError> {
    validate_objects(objects, Some(MAX_OVERLAPPING_FILES))
}

/// Validate references kept in a local journal. Unlike an archival manifest,
/// a journal can name all uncompacted files that are still needed locally, so
/// it has no archive overlap limit.
pub fn validate_journal_object_refs(objects: &[ObjectRef]) -> Result<(), DecodeError> {
    validate_objects(objects, None)
}

pub fn max_object_overlap(objects: &[ObjectRef]) -> usize {
    let mut events = BTreeMap::<BlockKey, (usize, usize)>::new();
    for object in objects {
        events.entry(object.first_key).or_default().0 += 1;
        events.entry(object.last_key).or_default().1 += 1;
    }
    let mut overlapping = 0usize;
    let mut maximum = 0usize;
    for (_, (starts, ends)) in events {
        overlapping = overlapping.saturating_add(starts);
        maximum = maximum.max(overlapping);
        overlapping = overlapping.saturating_sub(ends);
    }
    maximum
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::blx::{BlockSpace, NamespaceKind};

    fn object(id: u64) -> ObjectRef {
        ObjectRef {
            identity: ObjectIdentity {
                namespace_kind: NamespaceKind::Vset,
                namespace_id: 7,
                writer_fence: 3,
                object_id: id,
            },
            min_seq: id,
            max_seq: id,
            batch_id: id,
            chunk_index: 0,
            chunk_count: 1,
            first_key: BlockKey {
                space: BlockSpace::Data,
                volume: 0,
                block: u32::try_from(id).expect("test object id fits u32"),
            },
            last_key: BlockKey {
                space: BlockSpace::Data,
                volume: 0,
                block: u32::try_from(id).expect("test object id fits u32"),
            },
            pre_state_checksum: id - 1,
            post_state_checksum: id,
            size: 1000,
            footer_offset: 800,
            footer_length: 100,
            object_checksum: id * 10,
        }
    }

    fn manifest() -> Manifest {
        Manifest {
            vset: VsetId(7),
            writer_fence: 3,
            journal_seq: 6,
            archive_seq: 8,
            capture_seq: 10,
            sync_covered_through: 9,
            recovery_kind: RecoveryKind::DiskOnly,
            checkpoint_epoch: Epoch(0),
            config: VsetConfig::compute(1, 1024),
            vmstate_logical_length: 0,
            base: None,
            complete_list: None,
            post_state_checksum: 1,
            metadata_checksum: 12,
            added: vec![object(1)],
            removed: Vec::new(),
        }
    }

    #[test]
    fn object_ref_has_the_documented_size() {
        let mut e = Enc::new();
        object(1).encode_into(&mut e);
        assert_eq!(e.len(), OBJECT_REF_BYTES);
        let mut e = Enc::new();
        object(1).identity.encode(&mut e);
        assert_eq!(e.len(), OBJECT_IDENTITY_BYTES);
    }

    #[test]
    fn manifest_round_trips_and_is_bounded() {
        let manifest = manifest();
        let bytes = manifest.encode().expect("bounded manifest");
        assert_eq!(Manifest::decode(VsetId(7), &bytes), Ok(manifest));
        assert!(bytes.len() <= MAX_MANIFEST_BYTES);
    }

    #[test]
    fn complete_list_round_trips_and_changes_produce_current_files() {
        let list = CompleteFileList {
            vset: VsetId(7),
            writer_fence: 3,
            list_id: 2,
            objects: vec![object(1), object(2)],
        };
        let bytes = list.encode();
        let reference = list.reference();
        let decoded = CompleteFileList::decode(reference, VsetId(7), &bytes).expect("file list");
        let mut manifest = manifest();
        manifest.complete_list = Some(reference);
        let mut third = object(3);
        third.pre_state_checksum = 1;
        manifest.added = vec![third];
        manifest.removed = vec![object(2).identity];
        manifest.post_state_checksum = 3;
        assert_eq!(
            manifest
                .current_files(Some(&decoded))
                .expect("current files")
                .iter()
                .map(|file| file.identity.object_id)
                .collect::<Vec<_>>(),
            [1, 3]
        );
    }

    #[test]
    fn a_compacted_state_can_be_followed_by_later_changes() {
        let mut compacted = object(10);
        compacted.min_seq = 5;
        compacted.max_seq = 5;
        compacted.batch_id = 5;
        compacted.pre_state_checksum = 7;
        compacted.post_state_checksum = 7;
        let mut later = object(11);
        later.min_seq = 6;
        later.max_seq = 6;
        later.batch_id = 6;
        later.pre_state_checksum = 7;
        later.post_state_checksum = 9;
        let list = CompleteFileList {
            vset: VsetId(7),
            writer_fence: 3,
            list_id: 6,
            objects: vec![compacted, later],
        };
        let mut manifest = manifest();
        manifest.complete_list = Some(list.reference());
        manifest.added.clear();
        manifest.post_state_checksum = 9;

        assert_eq!(
            manifest.current_files(Some(&list)),
            Ok(vec![compacted, later])
        );
    }

    #[test]
    fn manifest_rejects_invalid_removal_without_a_complete_list() {
        let mut manifest = manifest();
        manifest.removed.push(object(9).identity);
        let bytes = {
            let mut e = Enc::new();
            encode_manifest_prefix(&manifest, &mut e);
            e.u32(1);
            manifest.added[0].encode_into(&mut e);
            e.u32(1);
            manifest.removed[0].encode(&mut e);
            seal_frame(MAGIC_MANIFEST, &e.finish())
        };
        assert!(Manifest::decode(VsetId(7), &bytes).is_err());
    }

    #[test]
    fn oversized_changes_roll_into_a_new_complete_list() {
        let mut manifest = manifest();
        manifest.added = (1..=2_500).map(object).collect();
        manifest.post_state_checksum = 2_500;
        assert!(manifest.encode().is_err());
        let bounded = bound_manifest(manifest, None, 44).expect("roll complete list");
        let list = bounded.new_complete_list.expect("new list");
        assert_eq!(list.objects.len(), 2_500);
        assert_eq!(bounded.manifest.complete_list, Some(list.reference()));
        assert!(bounded.manifest.added.is_empty());
        assert!(bounded.manifest.removed.is_empty());
        assert!(bounded.manifest.encode().is_ok());
    }

    #[test]
    fn base_root_and_flat_manifest_round_trip() {
        let base = BaseManifest {
            base_id: 90,
            manifest_id: 2,
            capture_seq: 10,
            sync_covered_through: 9,
            recovery_kind: RecoveryKind::DiskOnly,
            checkpoint_epoch: Epoch(0),
            config: VsetConfig::compute(1, 1024),
            vmstate_logical_length: 0,
            post_state_checksum: 11,
            metadata_checksum: 12,
            objects: vec![object(1), object(2)],
        };
        let bytes = base.encode();
        let root = base.root();
        assert_eq!(BaseRoot::decode(90, &root.encode()), Ok(root));
        assert_eq!(BaseManifest::decode(root, &bytes), Ok(base));
    }
}
