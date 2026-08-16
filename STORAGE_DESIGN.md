# Unified vset storage design

Status: draft

This document defines how blockd stores VM memory, VM disks, VMM state, and
database files. It replaces the current page maps, map leaves, and segments.

## Glossary

The document uses the following storage-specific terms:

| Term | Meaning |
|---|---|
| **Archive** | The durable copy of a vset in object storage. |
| **Archive batch** | One group of recent changes that the primary combines and uploads together. |
| **Archive sequence** | A number that increases every time a manifest is published, including publications that only change file layout. |
| **Attach** | Open an archived vset so it can be read or restored. Attach reads metadata but does not download all page data. |
| **Base** | A saved state that one or more forks share without copying its page data. |
| **Base root** | A small, fixed-size record that makes a kept base discoverable and points to its base manifest. |
| **Block** | The smallest piece of data stored and fetched independently. It is normally one operating-system memory page. |
| **Block key** | The fixed-size address of a block: which kind of data it belongs to, which disk or file it belongs to, and its block number. |
| **`.blx` file** | A read-only file containing compressed block values, deletion markers, and an index. The document sometimes calls it a data file. |
| **Capture sequence** | The number of the guest or database state represented by a manifest. Compaction does not change it. |
| **Checkpoint epoch** | The VM checkpoint number returned to callers and used when resuming an exact whole-VM state. |
| **Checksum** | A value computed from stored bytes or visible vset state. A mismatch means the data is corrupt or the wrong pieces were combined. CRC32C checks stored bytes; the state checksum checks the assembled vset state. |
| **Compare-and-swap (CAS)** | Change a small record only if it still has the value or object-store version that the writer previously read. This prevents an old owner from replacing a newer owner's state. |
| **Complete file list** | A reusable snapshot listing every `.blx` file owned by a vset when the snapshot was written. The manifest records later additions and removals. |
| **Compaction** | Read several existing `.blx` files, keep the newest value for each block, write fewer replacement files, and then stop referencing the old files. |
| **Conditional create** | Create an object only if its key does not already exist. Uploaded files are never overwritten. |
| **Durable** | Confirmed written to storage that is expected to survive a process or host crash. |
| **File-list changes** | The files added and removed since the current complete file list was written. These changes live in the manifest and are capped at 256 KiB. |
| **File partition** | A fixed range of block addresses compacted independently. It limits how much block data one compaction worker holds in memory. |
| **Fixed-size / fixed-width** | A field or record that always occupies the same number of bytes. |
| **Footer** | The index at the end of a `.blx` file. It says which blocks are in the file and where each block's bytes are. |
| **Flush to durable storage** | Ask the operating system to write buffered bytes to the storage device and wait for confirmation before continuing. |
| **Fork** | A new vset that reads unchanged blocks from a base and stores only its own later changes. |
| **Frame** | One encoded record with a type marker, byte length, checksum, and payload. |
| **Garbage collection (GC)** | Delete uploaded objects that no current vset, kept base, or in-progress publication needs. |
| **Generation** | The write number stored with a block value. The highest generation is the newest value. |
| **Guest sync** | A guest request that requires all earlier accepted disk or database writes to be durable before the request is acknowledged. |
| **Head** | The small per-vset record that names the current manifest and the host allowed to publish the next one. |
| **Journal** | The ordered local record of recent changes on the primary and passive before those changes are archived. |
| **Journal sequence** | A number that identifies one exact local journal record. It does not change when compaction republishes the same state in a different file layout. |
| **KiB / MiB** | 1,024 bytes / 1,048,576 bytes. |
| **Local checkpoint** | A recovery point saved on the primary's local storage. It is not protected until copied to the passive and not archived until published to object storage. |
| **Lazy restore** | Start a vset from metadata and fetch each data block only when it is first needed. |
| **Litestream/LTX** | The existing SQLite replication file design that inspired the sorted, read-only data files used here. |
| **LZ4** | The compression method used independently for each stored block. |
| **Manifest** | The current description of an archived vset: recovery information, an optional base, a complete-file-list pointer, and file-list changes. It contains no page data. |
| **Metadata** | Descriptive information such as file references, sizes, checksums, and recovery mode. It is not VM memory, disk contents, or database contents. |
| **Namespace** | The vset or imported base that originally created an object. It is part of the object's permanent identity. |
| **Object identity / object reference** | An identity names one uploaded object. A reference also records enough size, range, index, and checksum information to read and verify it. |
| **Object storage** | The bucket holding named, read-only objects. `GET` reads an object or byte range; `PUT` uploads one. |
| **Passive** | The one other host that durably stores recent changes replicated by the primary. It does not upload the archive. |
| **Primary** | The host currently running or serving the vset. It replicates recent changes to the passive and uploads archive batches. |
| **Protected cut / protected frontier** | A point in the journal that is durable on both the primary and passive. The frontier is the newest such point. |
| **Publish** | Make an uploaded manifest current by changing the vset's head with CAS. Uploading files alone does not publish them. |
| **Range read** | Read only a selected byte range from an object instead of downloading the whole object. |
| **Recovery kind** | Whether an archived state can resume a VM exactly, start it normally from saved disks, or open a database. |
| **Recovery point** | One complete saved state that recovery is allowed to use. |
| **Resume set** | A best-effort hint listing blocks likely to be needed immediately when a VM resumes. Correctness does not depend on it. |
| **Stash** | The passive's durable storage for replicated recent changes. The head records which passive owns the active stash and a bounded list of old stashes still being retired. |
| **Sync watermark** | The newest guest sync included in an archived recovery point. |
| **Tombstone** | A stored deletion marker. It prevents an older value of the same block from becoming visible again. |
| **Trailer** | The fixed-size final bytes of a `.blx` file. They say where the footer is. |
| **Virtual machine (VM)** | A guest computer run by blockd. |
| **VMM state** | The saved state of the virtual machine monitor: virtual CPUs and emulated devices, separate from guest memory and disks. |
| **vset** | Everything saved and restored together: either a VM's memory, disks, and VMM state, or a database's files. |
| **Write-ahead log (WAL)** | A database file containing committed changes that have not yet been copied into the main database file. |
| **Wire integer (`u8`, `u16`, `u32`, `u64`)** | An unsigned integer stored in exactly 1, 2, 4, or 8 bytes. |
| **Writer fence** | A number assigned when a host becomes primary. It is included in uploaded object names so a former primary cannot collide with the current primary's objects. |

## Overview

The basic idea is simple:

- Changed blocks are packed into read-only `.blx` files.
- The primary keeps recent changes safe on itself and one passive.
- From time to time, the primary combines those recent changes and uploads a
  batch of `.blx` files to object storage.
- A small manifest describes the saved state. It points to a reusable complete
  file list and records only the files added or removed since that list was
  written.
- A tiny head record points to the current manifest.
- A fork points to an existing saved base. It does not copy the base's data.

This is close to the Litestream/LTX file model: sorted read-only files, an
index at the end of each file, checksums, and background merging. The main
difference is that blockd does not upload a file for every protected write.
Replication to the passive already protects those writes until the primary
combines them into an archive batch.

The requirements document still controls. In plain terms:

- a whole vset is saved or restored together;
- guest sync is protected on the primary and exactly one active passive before
  acknowledgement;
- upload to object storage happens later and does not delay guest writes;
- restore first reads a fixed number of metadata objects, then fetches blocks
  only when they are needed;
- every durable byte is verified before use;
- a fork copies no page data;
- data is shared only when a fork explicitly points to a base; and
- object-store objects are at most 64 MiB.

The project has not been deployed, so there is only one format. There is no
old-format reader, conversion path, or format negotiation.

## 1. Decisions

1. **Use one data-file format everywhere.** A page is stored the same way on
   the primary, on the passive, and in object storage. Compaction, bases, and
   forks also use that format.
2. **Do not upload every protected write.** The primary and passive keep a
   journal of recent changes. The primary periodically combines all changes
   not yet archived, keeps only the newest value for each block, and uploads
   the result as `.blx` files.
3. **Reuse the complete file list.** An archive write does not rewrite the
   full list of files. It writes a manifest containing the files added and
   removed since the last complete list. The manifest may grow only to
   256 KiB. Before it would exceed that size, the primary writes a new complete
   list and starts the additions and removals at empty again. There is no chain
   of old manifests to follow.
4. **A fork copies no data.** A fork's manifest points to the base's manifest.
   The fork initially owns no `.blx` files. New writes create files owned by
   the fork; unchanged blocks continue to come from the base.
5. **A read follows at most one base pointer.** A normal manifest may point to
   one base manifest. A base manifest contains all file references directly
   and never points to another base. Creating a base from a fork copies file
   references, not page data.
6. **Only the current owner may publish.** Files and manifests are read-only
   after upload. The head is changed with a compare-and-swap: the update works
   only if nobody changed the head since it was read. A former owner may upload
   unused files, but it cannot make them current.
7. **Keep only named states.** The current head and explicitly kept bases keep
   data alive. Old manifests and files that nothing points to can be deleted
   after enough time has passed for an in-progress upload to finish.
8. **The primary uploads.** The passive only stores and serves the replicated
   recent changes. The primary chooses a fully replicated point, builds the
   archive files, uploads them, and makes the new manifest current.

## 2. How blocks are addressed

Every stored block has the same kind of address, whether it came from VM
memory, a disk, VMM state, or a database file:

```text
BlockKey {
    space:  u8,
    volume: u8,
    reserved: u16,
    block:  u32,
}
```

`space` says what kind of data this is. `volume` selects a particular disk or
database file. `block` is the block number within that space and volume.
`reserved` is unused and must be zero.

Keys are sorted into fixed file partitions. The block-number range comes
first, followed by a small group of nearby memory, disk, database, and VMM
namespaces. This means a small VM cut normally remains one `.blx` file, while a
large cut splits at stable boundaries. Files from later cuts use the same
boundaries, so compaction can handle one bounded partition at a time instead
of loading the whole vset.

The spaces are:

| Space | Contents |
|---|---|
| `0` | compute memory |
| `1` | compute disk or database file pages; `volume` selects the disk/file |
| `2` | VMM state split into blocks; the manifest records its visible byte length |

For a database, fixed `volume` values identify the main database, write-ahead
log (WAL), and rollback journal. The manifest records whether each file exists
and its visible length. Those facts and the file's blocks always belong to the
same recovery point. When a file shrinks or is deleted, deletion markers stop
old blocks from reappearing if the file later grows.

The block size is the host process's operating-system page size. Every data
file and manifest records it. A host using a different page size cannot open
the saved state. If the final VMM-state block is only partly used, the unused
bytes are stored as zero and the manifest records how many bytes are actually
visible.

This one address format lets all vset types use the same data-file reader and
writer. Manifests and other control records remain separate from page data.

## 3. Read-only `.blx` files

### 3.1 Purpose

A `.blx` file contains block changes sorted by block key. One file contains at
most one value for a given key. If the input contains several writes to the
same block, the writer keeps only the newest one before creating the file.

An uploaded `.blx` file is never changed. Upload uses conditional create, so it
fails if the name already exists. A retry may treat an existing file as
success only after checking that its identity and checksum exactly match the
file it intended to upload.

The writer starts a new file at about 32 MiB. This leaves room below the
64 MiB hard limit. A single block entry is never split between files.

### 3.2 Layout

```text
header frame
entry frame 0
entry frame 1
...
entry frame N-1
footer frame
trailer
```

The header, each entry, and the footer are each stored as a frame:

```text
magic u32 | payload_len u32 | crc32c u32 | payload
```

Integers use the fixed sizes shown in the layouts and store the lowest byte
first. There is exactly one valid byte encoding. A reader rejects extra bytes,
unknown record types, duplicate or unsorted keys, incorrect lengths, and
nonzero reserved fields.

The trailer says where the footer begins and how long it is. The manifest also
stores that location. A normal block read can therefore fetch the footer
directly. Reading the trailer provides a second way to verify the complete
file.

### 3.3 Header

The header identifies the file and the state change it belongs to:

```text
BlxHeader {
    format:             u16,
    block_size:         u32,
    namespace_kind:     u8,   // vset or imported-base origin
    namespace_id:       u64,
    writer_fence:       u64,
    object_id:          u64,
    min_seq:            u64,
    max_seq:            u64,
    batch_id:           u64,
    chunk_index:        u32,
    chunk_count:        u32,
    entry_count:        u32,
    first_key:          BlockKey,
    last_key:           BlockKey,
    pre_state_checksum: u64,
    post_state_checksum:u64,
}
```

One archive batch or compaction may be too large for one file. In that case it
is split into numbered files. Every file records the total file count and its
own number. Their key ranges may not overlap. A manifest either includes every
file in the batch or rejects the batch as incomplete.

### 3.4 Entries

An entry is one of:

```text
Data {
    key: BlockKey,
    generation: u64,
    raw_len: u32,
    stored_len: u32,
    lz4_block: [u8; stored_len],
}

Tombstone {
    key: BlockKey,
    generation: u64,
}
```

Each entry has its own frame and checksum, so reading one block requires
reading and decompressing only that entry. A deletion marker is a real newest
value: it stops the search and says the older block is gone. Corrupt or missing
data is never treated as a deletion. A block outside the vset's valid size
reads as zero; a block that should exist but cannot be verified is an error.

The first format stores one deletion marker per affected block. A future format
may add one marker covering a whole range, but only if measurements show that
large file truncations make the individual markers too expensive.

### 3.5 Footer

The footer is a sorted exact index:

```text
FooterEntry {
    key:           BlockKey,
    offset:        u32,
    length:        u32,
    generation:    u64,
    kind:          u8,  // data or tombstone
    value_checksum:u64, // checksum of the uncompressed data; zero for a tombstone
}
```

The footer lets the reader quickly find one block and the exact byte range to
fetch. Because a file never changes, its footer may remain cached. Each file
reference also records the first and last block key, so the reader can skip
files that cannot contain the requested block without fetching their footers.
The uncompressed-value checksum lets metadata-only recovery reconstruct and
verify the logical state checksum without reading every data entry.

### 3.6 Checksums

The CRC32C in each frame detects damaged stored bytes. The state checksum
represents the complete visible block state, regardless of how that state is
split among `.blx` files. It is calculated from each block key, generation,
and uncompressed block checksum. Replacing or deleting a block updates that
one block's contribution.

Every archive batch records the state checksum before and after applying the
batch. The first value must equal the previous manifest's final checksum. The
second value must equal the new manifest's final checksum. Compaction changes
only the file layout, so it must leave the state checksum unchanged. A separate
checksum covers manifest information such as file sizes and recovery kind.

## 4. Archive files and compaction

For each archive batch, the primary chooses a protected journal point. It
combines every not-yet-archived change through that point and keeps only the
newest value or deletion marker for each block. The result may need several
`.blx` files. Each file covers a different range of block keys.

All archive files are in one set. There are no levels. To read a block, the
reader considers only files whose first and last keys include that block. It
checks their footers and uses the matching entry with the highest generation.
The complete file list plus the manifest's additions and removals say exactly
which files are current.

Over time there may be too many small files or too many files that could hold
the same block. Compaction reads such files, keeps the newest entry for each
block, and writes fewer replacement `.blx` files. The next manifest adds the
replacement files and removes the input files.

Compaction starts only when the file-overlap limit is reached, not after every
new archive write. It processes one fixed file partition at a time and drops
each input file after merging it into that partition's in-memory block map.
Memory use therefore depends on one partition's maximum number of blocks, not
on the vset's size or the number of archive writes accumulated during an
object-store outage.

A deletion marker can be removed only when no remaining file and no base can
still contain the older block that it hides. A fork that still uses a base must
therefore keep its deletion markers.

The system has a configured maximum number of files that may contain one block
key. Before publishing a batch that would exceed that maximum, the primary
must compact. This keeps the number of footer reads bounded. If object storage
is unavailable or compaction is behind, archive publication waits; replication
between the primary and passive continues to protect new writes.

## 5. Complete file lists and bounded manifests

### 5.1 No manifest history on attach

The head points directly to the current manifest. The manifest points to one
read-only complete file list and contains the net file-list changes since that
list was written. It never points to an older manifest.

For example:

```text
complete file list: A B C
manifest additions: D
manifest removals:  B
current files:      A C D
```

If the next archive adds `E`, the new manifest contains additions `D E` and
removal `B`. It does not point to the previous manifest. When the additions and
removals are about to make the manifest larger than 256 KiB, the primary writes
a new complete list containing `A C D E`; the new manifest then has empty
additions and removals.

Attaching an ordinary vset requires:

```text
head GET -> current manifest GET -> complete file list GET
```

A fork additionally fetches its one read-only base manifest. The child's
complete list and base manifest can be fetched in parallel:

```text
head GET -> child manifest GET -> child complete list GET
                                -> base manifest GET
```

The number of reads does not grow as more archive batches are written. A new
fork with no files of its own does not need a complete-list read. Reading the
optional resume set can improve startup speed but is never required for a
correct restore.

### 5.2 Manifest contents

The following layouts define the exact bytes. The prose after each layout
explains what they mean.

The manifest stores recovery information, pointers to the base and complete
file list, and the changes to that list:

```text
Manifest {
    format:                 u16,
    vset:                   u64,
    writer_fence:           u64,
    journal_seq:            u64,
    archive_seq:            u64,
    capture_seq:            u64,
    sync_covered_through:   u64,
    recovery_kind:          u8,   // whole, disk-only, database
    checkpoint_epoch:       u64,
    block_size:             u32,
    vset_config:            VsetConfig,
    database_metadata:      DatabaseMetadata,
    vmstate_logical_length: u64,
    base:                   OptionalBaseRef,
    complete_list:          OptionalFileListRef,
    post_state_checksum:    u64,
    metadata_checksum:      u64,
    added_count:            u32,
    added:                  [ObjectRef; added_count],
    removed_count:          u32,
    removed:                [ObjectIdentity; removed_count],
}
```

`vset_config` describes the VM or database shape needed to interpret its block
keys. `database_metadata` records database-file existence and visible lengths.
`journal_seq` identifies the exact local record whose state is published.
`archive_seq` independently identifies this particular metadata publication.
The remaining names correspond to terms in the glossary.

The complete file list is a separate read-only record:

```text
CompleteFileList {
    format:       u16,
    vset:         u64,
    writer_fence: u64,
    list_id:      u64,
    object_count: u32,
    objects:      [ObjectRef; object_count],
    checksum:     u64,
}
```

`objects` lists every data file owned by this vset when the list was written,
sorted by identity. It does not repeat files supplied by the base. `added` and
`removed` describe the difference between that list and the current state. If
a file was added and later removed, it disappears from both arrays. In plain
terms, start with the complete list, take out `removed`, and put in `added`.

This record points to the complete file list. A new fork may have no such list:

```text
OptionalFileListRef {
    present:      u8,
    writer_fence: u64,
    list_id:      u64,
    checksum:     u64,
}
```

When `present` is zero, every other field is zero. `removed` must then be empty,
and `added` is the complete set of files owned by the vset.

`OptionalBaseRef` is fixed-width even when absent:

```text
OptionalBaseRef {
    present:                    u8,
    base_id:                    u64,
    base_manifest_id:           u64,
    base_manifest_checksum:     u64,
    base_post_state_checksum:   u64,
}
```

When `present` is zero, every other field is zero. When it is one, these fields
name and verify the base manifest. That base manifest directly lists all of
its files; it does not point to another base.

An `ObjectRef` is the information needed to locate, filter, and verify one
`.blx` file. The object-store key is built from its identity fields:

```text
ObjectRef {
    namespace_kind: u8,
    namespace_id:   u64,
    writer_fence:   u64,
    object_id:      u64,
    min_seq:        u64,
    max_seq:        u64,
    batch_id:       u64,
    chunk_index:    u32,
    chunk_count:    u32,
    first_key:      BlockKey,
    last_key:       BlockKey,
    pre_state_checksum:u64,
    post_state_checksum:u64,
    size:           u32,
    footer_offset:  u32,
    footer_length:  u32,
    object_checksum:u64,
}
```

An `ObjectIdentity` contains only `namespace_kind`, `namespace_id`,
`writer_fence`, and `object_id`. It is therefore smaller than a full reference.
A removal must name a file in the complete list. The same identity cannot be
both added and removed.

The entire encoded manifest, including its frame, must fit in 256 KiB. Before
an update would exceed that limit, the primary calculates the new current file
set and writes it as a new complete file list. The new manifest points to that
list and has empty change arrays. If one unusually large archive or compaction
update would exceed the limit by itself, the primary writes a new complete list
immediately. This copies file references only; it does not copy `.blx` data.

A reader rejects the manifest if any part is inconsistent. In particular: the
complete-list checksum must match; file identities must be unique; every
removal must be valid; every multi-file batch must be complete; sequence
ranges must make sense; the per-block file limit must hold; and applying the
listed batches must lead from the prior state checksum to the final checksum
stored in this manifest. A publisher must also prove that it started from the
manifest currently named by the head.

### 5.3 Recovery kinds

- A **whole** VM manifest contains matching memory, disks, and VMM state from
  one capture. The VM can continue from that exact state.
- A **disk-only** VM manifest contains disks through the recorded guest sync,
  but no usable memory or VMM state. The VM starts normally from those disks.
- A **database** manifest contains the main database, WAL, and rollback journal
  through the recorded guest sync, including whether each file exists and its
  visible length.

A disk-only archive may still reference older memory files because a running
VM or later compaction may need them. Those files do not make that recovery
point resumable. The `recovery_kind` field alone decides which recovery action
is allowed.

### 5.4 Manifest generation

A new manifest is written only when the visible archive description changes:

- an archival batch publishes a newer recovery point;
- compaction replaces input objects with outputs;
- a base is kept or imported; or
- a fork publishes its initial state.

Ordinary writes, copying dirty pages to local storage, guest sync, and local
checkpoints do not write object-store manifests.

`archive_seq` counts manifest publications. It increases even when compaction
changes only the file layout. `capture_seq` identifies the saved guest or
database state and therefore does not increase for compaction alone.
`journal_seq` also stays unchanged for compaction alone. Recovery never
compares `archive_seq` with `journal_seq`; they count different things.

Publication order is:

1. upload each new `.blx` file without allowing overwrite;
2. verify the new files and, when resetting the change list, upload and verify the
   new complete file list;
3. upload the new manifest without allowing overwrite;
4. change the vset head with CAS so it points to the new manifest; and
5. allow the older archive inputs to be deleted only after GC can see the new
   head.

A crash before step 4 may leave uploaded files that no head uses; GC later
deletes them. If step 4 loses the CAS race, another writer changed the head,
so this publication is discarded and can never become current.

## 6. The head and protection against former primaries

Each vset has one small head record. It says which host is primary and which
manifest is current. It can be changed only with CAS:

```text
Head {
    vset:                 u64,
    holder:               u16, // current primary host
    writer_fence:         u64, // identity of the current ownership period
    manifest_fence:       u64, // writer fence used by the current manifest
    manifest_journal_seq: u64, // exact journal state represented
    manifest_seq:         u64, // archive sequence of the current manifest
    manifest_checksum:    u64, // verifies the current manifest
    stash_assignment:     StashAssignment,
    bounded_retired_stash:RetiredStash,
}
```

When a host successfully becomes primary, the object store gives the updated
head a new version. That version becomes the new writer fence. The primary
keeps using that fence in every uploaded name until ownership changes again.

Object-store names are built from numeric identities:

```text
v/<vset>/head
v/<vset>/m/<writer-fence>-<archive-seq>.manifest
v/<vset>/f/<writer-fence>-<list-id>.files
v/<origin-vset>/o/<writer-fence>-<object-id>.blx
b/<base>/root
b/<base>/m/<manifest-id>.manifest
```

Every data file, complete file list, and manifest is created without allowing
overwrite. The writer fence gives each primary different names. The head CAS
ensures that only the current primary can make a manifest visible. A manifest
may safely point to a file created by another vset because uploaded files never
change.

## 7. Primary and passive journals

The journal keeps each recent change safe before the primary creates a larger
archive batch. Individual journal records are not uploaded as individual
objects.

### 7.1 Saving recent changes locally

In the background, the primary writes changed pages into bounded, read-only
local `.blx` files. It then appends a journal record naming the complete set of
files required for that recovery point, the captured state, the recovery kind,
and the final logical-state checksum. The checksum belongs to that immutable
journal record; archival work must not substitute the primary's newer live
checksum. The journal record is committed only after every file it names has
been flushed to durable local storage.

The amount of work depends on the number of changed pages, not the total vset
size. The host keeps an in-memory list of local files and their first and last
block keys. Cached footers provide exact block locations. There is no durable
entry mapping every page to a file.

The journal retains only a bounded number of recovery records. Local
compaction replaces old named files with a bounded replacement set before the
old files are deleted. Recovery reads file headers, trailers, and footers to
rebuild its in-memory block lookup; it does not read every data block and it
does not load a separate durable page map.

A cold boot discards memory and VMM state but keeps disk state. If a retained
`.blx` batch also contains one of those discarded blocks, the primary writes a
newer deletion marker in its next local cut. Otherwise retaining the batch for
an unrelated live disk block could make the discarded value appear again after
a later restart. These deletion markers change no current logical bytes; they
only make the file history agree with the cold-boot state.

### 7.2 Sync protection

For a guest sync:

1. finish a local journal record containing every accepted write through the
   sync request;
2. flush its `.blx` files and journal commit record to durable local storage;
3. send those exact compressed files and the journal record to the passive;
4. have the passive append them and flush its own commit record to durable
   storage; and
5. acknowledge the guest only after the passive confirms that the whole record
   is durable.

There is exactly one passive. Step 4 advances the protected frontier. A local
checkpoint may finish using only the primary's storage, but it is not protected
until replicated and is not archived until object-store publication succeeds.

### 7.3 Primary archival

The primary chooses a protected journal point and combines every change since
the last archived point:

1. for each block key, keep only the newest change;
2. keep every deletion marker still needed to hide an older value;
3. sort the result;
4. write bounded `.blx` files;
5. upload and verify those files;
6. update the manifest's additions and removals, first writing a new complete
   file list if the manifest would exceed 256 KiB; and
7. publish the manifest by changing the head with CAS.

The primary may combine any number of journal records into one archive batch.
Object storage can restore only the resulting archive point, not each journal
record inside it. An explicitly kept checkpoint must get its own published
recovery point and cannot be combined away.

The passive needs no permission to write object storage. It may compact its
local journal files while object storage is unavailable, but it must preserve
the newest protected point and every state currently being archived. The
primary prevents its selected files from being deleted until publication
finishes or is abandoned. The passive does the same until the archive includes
that protected point.

## 8. Reads and lazy restore

Attach reads and verifies the head, current manifest, and complete file list.
A fork also reads its base manifest. A new fork with no files of its own skips
the complete file list. Attach downloads no page data.

Local restart also reloads the archive file references named by the head. A
later local journal contains only its local changes, so those archive
references are what keep older, untouched disk blocks discoverable.

For a block read:

1. check memory and the list of files on local storage;
2. calculate the current archive file set by applying the manifest's additions
   and removals to the complete file list;
3. discard files whose first and last block keys exclude the requested key;
4. fetch or reuse the remaining files' footers and choose the matching entry
   with the greatest generation;
5. read and verify only that entry's byte range;
6. return the block, or report it absent if the entry is a deletion marker; and
7. if a fork has no entry of its own, repeat the search in its base manifest.

The reader prefers a local copy, then a copy on the other recovery host, then
object storage. Uploaded files never change, so a verified footer or block may
stay cached until the cache removes it.

The per-block file limit bounds the number of footers checked. A fork adds at
most one base search. Before resuming a VM, the host may read the resume-set
blocks and footers in advance, but correctness does not depend on that hint.

## 9. Bases and forks

### 9.1 Keeping a base

A base consists of a read-only base manifest and a small base root that points
to it. The base manifest describes one complete recovery point and directly
lists every data file needed for that point. It never points to another base.

If the selected point is already archived, keeping it writes only metadata:
the primary writes a base manifest containing direct references to the
existing vset and base files, then creates the base root without allowing
overwrite.

If the selected point contains unarchived changes, the primary first archives
only those changed pages as a normal archive batch. Already archived objects
are referenced in place and are never copied. Base creation may therefore
upload new bytes that have never reached the archive, but it never duplicates
or renames existing archived page data.

The base manifest's size grows with the number of live `.blx` files, not with
the number of live pages. Creating it copies file references only. It does not
read or rewrite the page bytes in those files.

### 9.2 Creating a fork

Creating a fork writes only fixed-size metadata:

1. read and verify the base root;
2. create the new vset head and assign its passive;
3. create the fork's manifest containing the base-manifest pointer, recovery
   metadata, no complete file list, and empty change arrays; and
4. publish that manifest by changing the fork's head with CAS.

Fork creation reads the fixed-size base root and writes the new head and
manifest. It does not upload, copy, rename, rewrite, or download a `.blx` file,
regardless of the base's size. A test records every object-store operation and
fails if fork creation performs any `.blx` write.

The fork's first change creates a `.blx` file owned by the fork. If both the
fork and base contain a block, the fork's entry wins. Unchanged blocks continue
to exist only once in storage.

### 9.3 Keeping a fork as another base

Keeping a fork as a new base writes one base manifest containing references to
both the old base's files and the fork's files. It copies neither set of
`.blx` files. A later fork points directly to this new base manifest, so reads
still follow only one base pointer.

### 9.4 Local memory sharing

Forks also share clean base pages in local memory. Every fork initially maps
the same in-memory page. When one fork writes, the operating system gives only
that fork a private copy. The memory cache uses the base identity as its key,
so simultaneous first reads by many forks load one shared page instead of one
copy per fork.

## 10. Garbage collection

GC considers only objects in the bucket. It begins with:

- every vset head and its current manifest;
- every kept base root and its base manifest; and
- every durable record describing a publication that is still in progress.

From each vset manifest, GC keeps its complete file list, every current `.blx`
file, and its base manifest if it has one. From each base manifest, GC keeps
every `.blx` file named there. GC never follows old manifests and never follows
more than one base pointer.

Deleting a base removes its base root, so no new fork can find it. Existing
forks point directly to the base manifest and continue to work. The base
manifest and files can be deleted only after the last existing fork stops
using them.

GC deletes an unused object only if it is also older than the maximum time an
upload may remain in progress. Age alone never causes deletion. If GC cannot
read a head, base root, or manifest, it refuses to delete anything that record
might protect.

Only an explicit base-and-fork relationship shares files. Equal data in
unrelated vsets is not combined, because doing so could reveal that two
different users store the same content.

## 11. Recovery and failure rules

Recovery uses a saved state only if all of the following are true:

1. the head and manifest identify each other and their checksums match;
2. applying the additions and removals to the complete file list produces one
   unambiguous current file set, and every multi-file batch is complete;
3. each object's name, header, and manifest reference agree;
4. sequence ranges are valid and no block can occur in too many files;
5. the before-and-after state checksums connect correctly;
6. every required local or passive journal record was fully committed to
   durable storage;
7. the state includes the newest guest sync that was acknowledged by any
   allowed recovery source; and
8. the recovery kind explicitly permits VM resume, VM boot from disk, or
   database open.

Takeover and migration still use the head CAS and writer fence. A new primary
may reference read-only files uploaded by an older primary, but it can publish
only by successfully changing the current head.

For VM migration, the source pauses the VM only to drain local guest work and
write one durable local cut. It performs no peer calls and no object-store
reads or writes while the VM is paused. As soon as that local write completes,
the source commits the VM as stopped and ends pause-time accounting. It then
copies that exact cut to the passive, writes the local handoff marker, and
offers the cut to the destination. The offer carries the captured VMM bytes
when they fit in one peer message. If they do not, or a recovered source is
re-offering the cut, the destination reads and verifies the VMM blocks from the
source's local `.blx` files before it resumes. If this later work is cancelled
before the handoff marker is durable, the source can still resume from the
local cut.

If a footer or block entry is corrupt, the reader tries another permitted copy.
If no copy provides verified bytes, recovery fails. Missing data that should
exist is never silently returned as zero.

## 12. Object layout example

For vset `7`, writer fence `12`, and base `90`:

```text
cluster/placement
hosts/0007/session

v/0000000000000007/head
v/0000000000000007/m/000000000000000c-0000000000000042.manifest
v/0000000000000007/f/000000000000000c-0000000000000003.files
v/0000000000000007/o/000000000000000c-0000000000000011.blx
v/0000000000000007/o/000000000000000c-0000000000000012.blx
v/0000000000000007/rs

b/000000000000005a/root
b/000000000000005a/m/0000000000000001.manifest
```

The base manifest may reference the two vset files above directly. A fork of
base `90` stores the base-manifest identity in its own manifest; it does not
create `b/90/o/...` copies.

## 13. Removed current-format concepts

This design removes:

- the durable map from every page to its exact storage location;
- splitting that map into separate leaf records;
- storing small map updates inside other records;
- copying the whole page map into the archive journal;
- copying existing page data when creating a base;
- compaction rules tied to the old segment format;
- archive packs that require every page location to be rewritten; and
- the rule that a vset may reference files only under its own object-store
  prefix.

It keeps these useful properties:

- every record has one exact byte encoding and its own checksum;
- each block is compressed and verified independently;
- a reader can fetch only the bytes for one block;
- both primary and passive explicitly commit journal records;
- exactly one passive protects recent changes;
- CAS and writer fences prevent former primaries from publishing;
- the newest protected and archived points only move forward; and
- restore fetches blocks only when needed and may read likely startup blocks
  in advance.

## 14. Required tests

The replacement is complete only when all of these tests exist and pass:

1. Tests that compare the exact expected bytes for every stored record and
   every part of a `.blx` file.
2. Tests that flip each bit and cut records at every byte position, proving
   that all damaged records are rejected.
3. Tests that generate many write and deletion histories and prove compaction
   never changes the visible state.
4. Tests that recover the same state from the primary journal, passive journal,
   and object storage and compare the results.
5. Tests that simulate a crash before and after every upload and head update.
6. A fork test that rejects every `.blx` write during fork creation,
   independent of base size.
7. Fork isolation tests proving child writes never alter parent or sibling
   reads and that unmodified blocks remain shared.
8. Base-deletion tests proving existing children retain data while new forks
   are rejected.
9. Attach tests proving that an ordinary vset needs three required object-store
   reads and a fork needs four; a new fork with no files of its own needs three.
10. Measurements of worst-case block-read cost with the maximum permitted
    number of candidate files, both with empty and populated caches.
11. Tests of manifest size, complete-file-list reset, and attach time at the
    largest supported vset size and with widely scattered writes.
12. Simulations of primary loss, passive loss, object-storage outage,
    interrupted compaction, uploads by a former primary, GC races, migration,
    and repeated fork, keep, and delete operations.

## 15. Costs and trade-offs

The main read cost is clear: the archive no longer has an exact stored map from
every block to one file. On the first read of a block, the reader may have to
fetch several file footers before it knows which entry is newest. The hard
per-block file limit keeps this bounded, and caching makes later reads cheaper.

The archive-write cost is also explicit. Every publication uploads one new
manifest, never larger than 256 KiB, and performs one head CAS. The manifest
grows as file-list changes accumulate. If it grows at a roughly even rate from
empty to 256 KiB, the average manifest upload is about 128 KiB.

When the manifest would exceed 256 KiB, the primary also uploads a new complete
file list. Each live file costs 109 bytes in that list, plus a small fixed
header. A vset with 10,000 live files therefore has a complete list of about
1.04 MiB. The primary reuses that list until another 256 KiB of file-list
changes accumulates.

This is the trade: bounded metadata upload and a fixed number of attach reads
in exchange for occasional complete-file-list uploads and bounded footer reads
on a cold block lookup. Fork creation still copies no page data.
