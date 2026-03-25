//! Durable `.kjm` artifact helpers for the `Pezhai` engine.
//!
//! This module owns file mechanics for metadata checkpoints and shared data
//! files. Publication and replay decisions remain engine-owned.

use std::cmp::Ordering;
use std::fs::{self, File};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::error::KalanjiyamError;
use crate::pezhai::codec::{
    CurrentFile, current_relative_path, data_relative_path, decode_file_meta_wire,
    decode_logical_shard_entry_wire, encode_current, encode_file_meta_wire,
    encode_logical_shard_entry_wire, metadata_relative_path, resolve_store_path,
};
use crate::pezhai::types::{
    Bound, FileMeta, InternalRecord, KeyRange, LogicalShardEntry, MemtableRecord, NONE_U64,
    RecordKind, SnapshotHandle, compare_internal_records, validate_page_size_bytes,
    visible_rows_from_records,
};

const KJM_MAGIC: &[u8; 8] = b"KJKJM001";
const KJM_FOOTER_MAGIC: &[u8; 8] = b"KJKJMF01";
const DATA_FILE_KIND: u16 = 1;
const METADATA_FILE_KIND: u16 = 2;
const DATA_LEAF_BLOCK_KIND: u8 = 2;
const FILE_MANIFEST_BLOCK_KIND: u8 = 3;
const LOGICAL_SHARD_BLOCK_KIND: u8 = 4;

/// In-memory view of one metadata checkpoint file.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CheckpointState {
    /// Highest committed seqno represented by the checkpoint.
    pub checkpoint_max_seqno: u64,
    /// Next seqno allocator value captured by the checkpoint.
    pub next_seqno: u64,
    /// Next file-id allocator value captured by the checkpoint.
    pub next_file_id: u64,
    /// Shared-data generation represented by the checkpoint.
    pub checkpoint_data_generation: u64,
    /// Published shared-data manifest files.
    pub manifest_files: Vec<FileMeta>,
    /// Current latest-only logical-shard map.
    pub logical_shards: Vec<LogicalShardEntry>,
}

/// Parsed immutable shared data file used by point reads and scans.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ParsedDataFile {
    /// File metadata carried by the manifest.
    pub meta: FileMeta,
    /// Internal records stored in full internal-key order.
    pub records: Vec<InternalRecord>,
}

/// Minimal validated layout needed to seek individual records inside one file.
#[derive(Debug)]
struct DataFileLeafLayout {
    block_offset: u64,
    block_bytes: usize,
    entry_count: usize,
}

/// Decoded fixed-width slot metadata for one internal record in a leaf block.
struct DataLeafSlot {
    key_offset: usize,
    key_len: usize,
    value_offset: usize,
    value_len: usize,
    seqno: u64,
    kind: RecordKind,
}

/// Forward-only lazy iterator over one immutable data file's internal records.
///
/// The iterator validates file structure and checksums once at open, then reads
/// one slot at a time as callers advance. This keeps scan merge inputs lazy and
/// avoids materializing all records into memory up front.
#[derive(Debug)]
pub(crate) struct DataFileRecordIterator {
    file: File,
    layout: DataFileLeafLayout,
    next_index: usize,
    end_key_exclusive: Option<Vec<u8>>,
}

/// Builds one deterministic `FileMeta` from sorted internal records.
///
/// # Complexity
/// Time: O(r log r), where `r` is the number of input records, because the
/// helper clones and sorts the slice before aggregating the metadata.
/// Space: O(r) for the sorted clone of the input records.
#[must_use]
pub fn build_file_meta(file_id: u64, level_no: u16, records: &[InternalRecord]) -> FileMeta {
    let mut sorted = records.to_vec();
    sorted.sort_by(compare_internal_records);

    let (min_user_key, max_user_key) = match (sorted.first(), sorted.last()) {
        (Some(first), Some(last)) => (first.user_key.clone(), last.user_key.clone()),
        _ => (Vec::new(), Vec::new()),
    };

    FileMeta {
        file_id,
        level_no,
        min_user_key,
        max_user_key,
        min_seqno: sorted
            .iter()
            .map(|record| record.seqno)
            .min()
            .unwrap_or(NONE_U64),
        max_seqno: sorted
            .iter()
            .map(|record| record.seqno)
            .max()
            .unwrap_or(NONE_U64),
        entry_count: sorted.len() as u64,
        logical_bytes: sorted.iter().map(InternalRecord::logical_bytes).sum(),
        physical_bytes: 0,
    }
}

/// Writes one immutable shared data file from sorted internal records.
///
/// # Complexity
/// Time: O(r log r + b), where `r` is the number of records and `b` is the
/// encoded file size in bytes. The `r log r` term comes from cloning and
/// sorting the records before encoding. Filesystem latency for the temp-file
/// write, rename, and required `fsync` calls is additional.
/// Space: O(b) for the encoded file buffer built before it is written.
///
/// The helper follows the spec's crash-safe publish order: write one temporary
/// file, `fsync` that temporary file, rename it to the canonical data-file
/// path, then `fsync` the `data/` directory so the filename publication is
/// durable. Any I/O failure aborts publication and leaves the caller to decide
/// whether the orphaned temp path should be cleaned up.
pub fn write_data_file(
    store_dir: &Path,
    meta: &FileMeta,
    records: &[MemtableRecord],
    page_size_bytes: usize,
) -> Result<PathBuf, KalanjiyamError> {
    let path = resolve_store_path(store_dir, &data_relative_path(meta.file_id));
    let temp_path = path.with_extension("tmp");
    let bytes = build_data_file_bytes(meta, records, page_size_bytes)?;
    write_and_sync_file(&temp_path, &bytes, "write data file")?;
    rename_and_sync_parent(&temp_path, &path, "rename data file")?;
    Ok(path)
}

/// Loads one immutable shared data file for point reads or scans.
///
/// # Complexity
/// Time: O(b + r log r), where `b` is the file size in bytes and `r` is the
/// number of decoded records. The extra `r log r` term comes from restoring
/// spec-defined internal-key order after decoding the leaf block.
/// Space: O(b) for the file bytes and decoded `ParsedDataFile`.
pub fn load_data_file(
    store_dir: &Path,
    meta: &FileMeta,
) -> Result<ParsedDataFile, KalanjiyamError> {
    let path = resolve_store_path(store_dir, &data_relative_path(meta.file_id));
    let bytes = fs::read(&path).map_err(|error| KalanjiyamError::io("read data file", error))?;
    parse_data_file_bytes(meta, &bytes)
}

/// Validates that one published data file exists and matches the `.kjm` layout.
pub(crate) fn validate_data_file(store_dir: &Path, meta: &FileMeta) -> Result<(), KalanjiyamError> {
    let path = resolve_store_path(store_dir, &data_relative_path(meta.file_id));
    let _ = open_validated_data_file(&path)?;
    let _ = meta;
    Ok(())
}

/// Finds the first visible record for one user key at one snapshot in one file.
///
/// # Complexity
/// Time: O(r) worst case, where `r` is the number of decoded records in the
/// file, because the search walks the in-memory vector until it finds a visible
/// version or exhausts the file.
/// Space: O(1) additional space.
pub fn find_visible_record_in_file(
    file: &ParsedDataFile,
    user_key: &[u8],
    snapshot_seqno: u64,
) -> Result<Option<MemtableRecord>, KalanjiyamError> {
    Ok(file
        .records
        .iter()
        .find(|record| record.user_key == user_key && record.seqno <= snapshot_seqno)
        .cloned())
}

/// Finds the first visible record for one user key without materializing the whole file.
pub(crate) fn find_visible_record_in_data_file(
    store_dir: &Path,
    meta: &FileMeta,
    user_key: &[u8],
    snapshot_seqno: u64,
) -> Result<Option<MemtableRecord>, KalanjiyamError> {
    let path = resolve_store_path(store_dir, &data_relative_path(meta.file_id));
    let (mut file, layout) = open_validated_data_file(&path)?;
    let mut index = find_first_slot_with_user_key_ge(&mut file, &layout, user_key)?;
    while index < layout.entry_count {
        let slot = read_data_leaf_slot(&mut file, &layout, index)?;
        let slot_key = read_slot_key(&mut file, &layout, &slot)?;
        match slot_key.as_slice().cmp(user_key) {
            Ordering::Less => {
                index += 1;
                continue;
            }
            Ordering::Greater => return Ok(None),
            Ordering::Equal => {
                if slot.seqno <= snapshot_seqno {
                    return read_slot_record(&mut file, &layout, slot).map(Some);
                }
                index += 1;
            }
        }
    }
    Ok(None)
}

/// Scans one immutable shared data file across one user-key range.
///
/// # Complexity
/// Time: O(r + n), where `r` is the number of decoded records in the file and
/// `n` is the number of visible rows returned in the page.
/// Space: O(n) for the returned rows.
pub fn scan_visible_rows_in_file(
    file: &ParsedDataFile,
    start_key: Option<&[u8]>,
    end_key: Option<&[u8]>,
    resume_after_key: Option<&[u8]>,
    snapshot_seqno: u64,
    max_records_per_page: u32,
    max_bytes_per_page: u32,
) -> Result<(Vec<crate::pezhai::types::ScanRow>, bool, Option<Vec<u8>>), KalanjiyamError> {
    let range = KeyRange {
        start_bound: start_key.map_or(Bound::NegInf, |key| Bound::Finite(key.to_vec())),
        end_bound: end_key.map_or(Bound::PosInf, |key| Bound::Finite(key.to_vec())),
    };
    let rows = visible_rows_from_records(&file.records, &range, snapshot_seqno, resume_after_key);
    Ok(page_rows(rows, max_records_per_page, max_bytes_per_page))
}

impl DataFileRecordIterator {
    /// Opens one lazy iterator pinned to the provided half-open user-key range.
    ///
    /// `resume_after_key` advances the start position strictly past that key.
    pub(crate) fn open(
        store_dir: &Path,
        meta: &FileMeta,
        start_key: Option<&[u8]>,
        end_key_exclusive: Option<&[u8]>,
        resume_after_key: Option<&[u8]>,
    ) -> Result<Self, KalanjiyamError> {
        let path = resolve_store_path(store_dir, &data_relative_path(meta.file_id));
        let (mut file, layout) = open_validated_data_file(&path)?;
        let mut next_index = 0;
        if let Some(start_key) = start_key {
            next_index = find_first_slot_with_user_key_ge(&mut file, &layout, start_key)?;
        }
        if let Some(resume_after) = resume_after_key {
            let resume_index = find_first_slot_with_user_key_ge(&mut file, &layout, resume_after)?;
            next_index = next_index.max(skip_same_user_key_forward(
                &mut file,
                &layout,
                resume_index,
                resume_after,
            )?);
        }
        Ok(Self {
            file,
            layout,
            next_index,
            end_key_exclusive: end_key_exclusive.map(ToOwned::to_owned),
        })
    }

    /// Returns the next internal record in file order, or `None` at EOF/range end.
    pub(crate) fn next_record(&mut self) -> Result<Option<InternalRecord>, KalanjiyamError> {
        if self.next_index >= self.layout.entry_count {
            return Ok(None);
        }
        let slot = read_data_leaf_slot(&mut self.file, &self.layout, self.next_index)?;
        let slot_key = read_slot_key(&mut self.file, &self.layout, &slot)?;
        if self
            .end_key_exclusive
            .as_ref()
            .is_some_and(|end_key| slot_key.as_slice() >= end_key.as_slice())
        {
            self.next_index = self.layout.entry_count;
            return Ok(None);
        }
        self.next_index += 1;
        read_slot_record(&mut self.file, &self.layout, slot).map(Some)
    }
}

/// Writes one metadata checkpoint artifact and returns its durable path.
///
/// # Complexity
/// Time: O(f + s + b), where `f` is the number of manifest files, `s` is the
/// number of logical shards, and `b` is the encoded checkpoint size in bytes.
/// Filesystem latency for the temp-file write, rename, and directory `fsync`
/// calls is additional.
/// Space: O(b) for the encoded checkpoint payload.
///
/// The checkpoint file is written to one temporary path, `fsync`ed, renamed to
/// its canonical metadata filename, and then published by syncing the `meta/`
/// directory. Installing `CURRENT` remains a separate step owned by the engine.
pub fn write_checkpoint_file(
    store_dir: &Path,
    checkpoint_generation: u64,
    checkpoint_state: &CheckpointState,
    page_size_bytes: usize,
) -> Result<PathBuf, KalanjiyamError> {
    let path = resolve_store_path(store_dir, &metadata_relative_path(checkpoint_generation));
    let temp_path = path.with_extension("tmp");
    let bytes = build_checkpoint_bytes(checkpoint_generation, checkpoint_state, page_size_bytes)?;
    write_and_sync_file(&temp_path, &bytes, "write checkpoint file")?;
    rename_and_sync_parent(&temp_path, &path, "rename checkpoint file")?;
    Ok(path)
}

/// Loads one metadata checkpoint artifact named by `CURRENT`.
///
/// # Complexity
/// Time: O(b), where `b` is the checkpoint size in bytes, to read and decode
/// the artifact.
/// Space: O(b) for the checkpoint bytes and decoded state.
pub fn load_checkpoint_file(
    store_dir: &Path,
    current: &CurrentFile,
) -> Result<CheckpointState, KalanjiyamError> {
    let path = resolve_store_path(
        store_dir,
        &metadata_relative_path(current.checkpoint_generation),
    );
    let bytes =
        fs::read(&path).map_err(|error| KalanjiyamError::io("read checkpoint file", error))?;
    parse_checkpoint_bytes(&bytes)
}

/// Installs the `CURRENT` pointer for one already written checkpoint.
///
/// # Complexity
/// Time: O(1) for the fixed-size 72-byte encoding and bounded rename/write
/// steps, plus filesystem latency for the file and directory `fsync` calls.
/// Space: O(1).
///
/// This helper writes `CURRENT` to one temporary path, `fsync`es it, renames
/// it into place, `fsync`es the final `CURRENT` file, and then `fsync`es the
/// store root so recovery observes either the old pointer or the full new one.
pub fn install_current(store_dir: &Path, current: &CurrentFile) -> Result<(), KalanjiyamError> {
    let path = resolve_store_path(store_dir, &current_relative_path());
    let temp_path = path.with_extension("tmp");
    let bytes = encode_current(current);
    write_and_sync_file(&temp_path, &bytes, "write CURRENT")?;
    fs::rename(&temp_path, &path).map_err(|error| KalanjiyamError::io("rename CURRENT", error))?;
    sync_path(&path, "sync CURRENT")?;
    sync_directory(
        path.parent()
            .expect("CURRENT path should have a store parent"),
        "sync store directory after CURRENT install",
    )?;
    Ok(())
}

/// Returns whether one internal record is visible as a live user row.
///
/// # Complexity
/// Time: O(1).
/// Space: O(1).
#[must_use]
pub fn is_live_put(record: &MemtableRecord) -> bool {
    matches!(record.kind, RecordKind::Put) && record.value.is_some()
}

/// Returns whether one snapshot handle matches a given engine instance id.
///
/// # Complexity
/// Time: O(1).
/// Space: O(1).
#[must_use]
pub fn snapshot_matches_engine(handle: &SnapshotHandle, engine_instance_id: u64) -> bool {
    handle.engine_instance_id == engine_instance_id
}

fn build_data_file_bytes(
    meta: &FileMeta,
    records: &[InternalRecord],
    page_size_bytes: usize,
) -> Result<Vec<u8>, KalanjiyamError> {
    let mut sorted = records.to_vec();
    sorted.sort_by(compare_internal_records);
    let leaf_block = build_data_leaf_block(&sorted, page_size_bytes)?;
    let physical_bytes = 128 + leaf_block.len() + 128;
    let logical_bytes: u64 = sorted.iter().map(InternalRecord::logical_bytes).sum();
    let min_seqno = sorted
        .iter()
        .map(|record| record.seqno)
        .min()
        .unwrap_or(NONE_U64);
    let max_seqno = sorted
        .iter()
        .map(|record| record.seqno)
        .max()
        .unwrap_or(NONE_U64);

    let mut bytes = Vec::with_capacity(physical_bytes);
    bytes.extend_from_slice(&build_kjm_header(
        DATA_FILE_KIND,
        128,
        page_size_bytes,
        1,
        1,
        sorted.len() as u64,
        min_seqno,
        max_seqno,
        0,
        logical_bytes,
        physical_bytes as u64,
    ));
    bytes.extend_from_slice(&leaf_block);
    bytes.extend_from_slice(&build_kjm_footer(
        1,
        1,
        sorted.len() as u64,
        min_seqno,
        max_seqno,
        0,
        logical_bytes,
        physical_bytes as u64,
    ));

    let parsed = parse_data_file_bytes(meta, &bytes)?;
    if parsed.records != sorted {
        return Err(KalanjiyamError::Corruption(
            "written data file failed round-trip validation".to_string(),
        ));
    }
    Ok(bytes)
}

fn parse_data_file_bytes(meta: &FileMeta, bytes: &[u8]) -> Result<ParsedDataFile, KalanjiyamError> {
    if bytes.len() < 256 {
        return Err(KalanjiyamError::Corruption(
            "data file is too short".to_string(),
        ));
    }
    let page_size_bytes = validate_kjm_header(bytes, DATA_FILE_KIND)?;
    validate_kjm_footer(bytes)?;
    let block_count = u64::from_le_bytes(bytes[32..40].try_into().expect("header slice length"));
    if block_count != 1 {
        return Err(KalanjiyamError::Corruption(
            "v1 data files must contain exactly one block".to_string(),
        ));
    }
    let records = parse_data_leaf_block(&bytes[128..bytes.len() - 128], page_size_bytes)?;
    Ok(ParsedDataFile {
        meta: FileMeta {
            physical_bytes: bytes.len() as u64,
            logical_bytes: records.iter().map(InternalRecord::logical_bytes).sum(),
            entry_count: records.len() as u64,
            min_seqno: records
                .iter()
                .map(|record| record.seqno)
                .min()
                .unwrap_or(NONE_U64),
            max_seqno: records
                .iter()
                .map(|record| record.seqno)
                .max()
                .unwrap_or(NONE_U64),
            ..meta.clone()
        },
        records,
    })
}

fn build_data_leaf_block(
    records: &[InternalRecord],
    page_size_bytes: usize,
) -> Result<Vec<u8>, KalanjiyamError> {
    validate_page_size_bytes(page_size_bytes)?;
    let variable_bytes_total: usize = records
        .iter()
        .map(|record| record.user_key.len() + record.value.as_ref().map_or(0, Vec::len))
        .sum();
    let required_bytes = 64 + records.len() * 24 + variable_bytes_total;
    let block_span_pages = required_bytes.div_ceil(page_size_bytes) as u32;
    let block_bytes = block_span_pages as usize * page_size_bytes;
    let variable_begin_offset = block_bytes - variable_bytes_total;

    let mut block = vec![0_u8; block_bytes];
    block[0] = DATA_LEAF_BLOCK_KIND;
    block[4..8].copy_from_slice(&(records.len() as u32).to_le_bytes());
    block[8..12].copy_from_slice(&block_span_pages.to_le_bytes());
    let variable_begin_offset = u16::try_from(variable_begin_offset).map_err(|_| {
        KalanjiyamError::InvalidArgument(
            "data block variable region does not fit the v1 offset fields".to_string(),
        )
    })?;
    block[16..18].copy_from_slice(&variable_begin_offset.to_le_bytes());
    block[20..24].copy_from_slice(&(variable_bytes_total as u32).to_le_bytes());
    block[24..32].copy_from_slice(&NONE_U64.to_le_bytes());

    let mut next_variable_offset = block_bytes;
    for (reverse_index, record) in records.iter().rev().enumerate() {
        let slot_index = records.len() - 1 - reverse_index;

        let value_offset = if let Some(value) = &record.value {
            next_variable_offset -= value.len();
            block[next_variable_offset..next_variable_offset + value.len()].copy_from_slice(value);
            next_variable_offset as u32
        } else {
            0
        };

        next_variable_offset -= record.user_key.len();
        block[next_variable_offset..next_variable_offset + record.user_key.len()]
            .copy_from_slice(&record.user_key);
        let key_offset = u16::try_from(next_variable_offset).map_err(|_| {
            KalanjiyamError::InvalidArgument(
                "data block key offset does not fit the v1 slot layout".to_string(),
            )
        })?;

        let slot_offset = 64 + slot_index * 24;
        block[slot_offset..slot_offset + 2].copy_from_slice(&key_offset.to_le_bytes());
        block[slot_offset + 2..slot_offset + 4]
            .copy_from_slice(&(record.user_key.len() as u16).to_le_bytes());
        block[slot_offset + 4..slot_offset + 8].copy_from_slice(&value_offset.to_le_bytes());
        block[slot_offset + 8..slot_offset + 12].copy_from_slice(
            &(record.value.as_ref().map_or(0_usize, Vec::len) as u32).to_le_bytes(),
        );
        block[slot_offset + 12..slot_offset + 20].copy_from_slice(&record.seqno.to_le_bytes());
        block[slot_offset + 20] = record.kind as u8;
    }

    let crc = crc32c(&zeroed_block_crc(block.clone()));
    block[32..36].copy_from_slice(&crc.to_le_bytes());
    Ok(block)
}

fn parse_data_leaf_block(
    bytes: &[u8],
    page_size_bytes: usize,
) -> Result<Vec<InternalRecord>, KalanjiyamError> {
    if bytes.len() < 64 {
        return Err(KalanjiyamError::Corruption(
            "data leaf block too short".to_string(),
        ));
    }
    if bytes[0] != DATA_LEAF_BLOCK_KIND {
        return Err(KalanjiyamError::Corruption(
            "data block kind mismatch".to_string(),
        ));
    }
    let expected_crc =
        u32::from_le_bytes(bytes[32..36].try_into().expect("slice has exact length"));
    if expected_crc != crc32c(&zeroed_block_crc(bytes.to_vec())) {
        return Err(KalanjiyamError::Checksum(
            "data block checksum mismatch".to_string(),
        ));
    }
    let entry_count =
        u32::from_le_bytes(bytes[4..8].try_into().expect("slice has exact length")) as usize;
    let block_span_pages =
        u32::from_le_bytes(bytes[8..12].try_into().expect("slice has exact length")) as usize;
    let block_bytes = block_span_pages * page_size_bytes;
    if bytes.len() != block_bytes {
        return Err(KalanjiyamError::Corruption(
            "data block span does not match actual bytes".to_string(),
        ));
    }
    let variable_begin_offset =
        u16::from_le_bytes(bytes[16..18].try_into().expect("slice has exact length")) as usize;
    let variable_total =
        u32::from_le_bytes(bytes[20..24].try_into().expect("slice has exact length")) as usize;
    if variable_begin_offset + variable_total != bytes.len() {
        return Err(KalanjiyamError::Corruption(
            "data block variable region mismatch".to_string(),
        ));
    }

    let mut records = Vec::with_capacity(entry_count);
    for slot_bytes in bytes[64..64 + entry_count * 24].chunks_exact(24) {
        let key_offset =
            u16::from_le_bytes(slot_bytes[0..2].try_into().expect("slice has exact length"))
                as usize;
        let key_len =
            u16::from_le_bytes(slot_bytes[2..4].try_into().expect("slice has exact length"))
                as usize;
        let value_offset =
            u32::from_le_bytes(slot_bytes[4..8].try_into().expect("slice has exact length"))
                as usize;
        let value_len = u32::from_le_bytes(
            slot_bytes[8..12]
                .try_into()
                .expect("slice has exact length"),
        ) as usize;
        let seqno = u64::from_le_bytes(
            slot_bytes[12..20]
                .try_into()
                .expect("slice has exact length"),
        );
        let kind = match slot_bytes[20] {
            0 => RecordKind::Delete,
            1 => RecordKind::Put,
            other => {
                return Err(KalanjiyamError::Corruption(format!(
                    "unknown record kind {other}"
                )));
            }
        };
        let user_key = bytes
            .get(key_offset..key_offset + key_len)
            .ok_or_else(|| KalanjiyamError::Corruption("key slot out of range".to_string()))?
            .to_vec();
        let value = if kind == RecordKind::Delete {
            if value_offset != 0 || value_len != 0 {
                return Err(KalanjiyamError::Corruption(
                    "delete slot must store zero value offset and length".to_string(),
                ));
            }
            None
        } else {
            Some(
                bytes
                    .get(value_offset..value_offset + value_len)
                    .ok_or_else(|| {
                        KalanjiyamError::Corruption("value slot out of range".to_string())
                    })?
                    .to_vec(),
            )
        };
        records.push(InternalRecord {
            user_key,
            seqno,
            kind,
            value,
        });
    }
    records.sort_by(compare_internal_records);
    Ok(records)
}

fn open_validated_data_file(path: &Path) -> Result<(File, DataFileLeafLayout), KalanjiyamError> {
    let mut file =
        File::open(path).map_err(|error| KalanjiyamError::io("open data file", error))?;
    let file_len = file
        .metadata()
        .map_err(|error| KalanjiyamError::io("stat data file", error))?
        .len();
    if file_len < 256 {
        return Err(KalanjiyamError::Corruption(
            "data file is too short".to_string(),
        ));
    }

    let mut header = [0_u8; 128];
    file.read_exact(&mut header)
        .map_err(|error| KalanjiyamError::io("read data file header", error))?;
    let page_size_bytes = validate_kjm_header(&header, DATA_FILE_KIND)?;
    let block_count =
        u64::from_le_bytes(header[32..40].try_into().expect("slice has exact length"));
    if block_count != 1 {
        return Err(KalanjiyamError::Corruption(
            "v1 data files must contain exactly one block".to_string(),
        ));
    }

    let mut footer = [0_u8; 128];
    file.seek(SeekFrom::End(-128))
        .map_err(|error| KalanjiyamError::io("seek data file footer", error))?;
    file.read_exact(&mut footer)
        .map_err(|error| KalanjiyamError::io("read data file footer", error))?;
    validate_kjm_footer_bytes(&footer)?;

    let footer_block_count =
        u64::from_le_bytes(footer[24..32].try_into().expect("slice has exact length"));
    if footer_block_count != block_count {
        return Err(KalanjiyamError::Corruption(
            "data file header/footer block_count mismatch".to_string(),
        ));
    }

    let mut leaf_prefix = [0_u8; 64];
    file.seek(SeekFrom::Start(128))
        .map_err(|error| KalanjiyamError::io("seek data leaf", error))?;
    file.read_exact(&mut leaf_prefix)
        .map_err(|error| KalanjiyamError::io("read data leaf prefix", error))?;
    if leaf_prefix[0] != DATA_LEAF_BLOCK_KIND {
        return Err(KalanjiyamError::Corruption(
            "data block kind mismatch".to_string(),
        ));
    }
    let entry_count = u32::from_le_bytes(
        leaf_prefix[4..8]
            .try_into()
            .expect("slice has exact length"),
    ) as usize;
    let span_pages = u32::from_le_bytes(
        leaf_prefix[8..12]
            .try_into()
            .expect("slice has exact length"),
    ) as usize;
    let block_bytes = span_pages * page_size_bytes;
    if block_bytes < 64 {
        return Err(KalanjiyamError::Corruption(
            "data block span does not include a full prefix".to_string(),
        ));
    }
    let expected_file_len = 128_u64 + block_bytes as u64 + 128_u64;
    if file_len != expected_file_len {
        return Err(KalanjiyamError::Corruption(
            "data file physical length does not match header and block span".to_string(),
        ));
    }

    let variable_begin_offset = u16::from_le_bytes(
        leaf_prefix[16..18]
            .try_into()
            .expect("slice has exact length"),
    ) as usize;
    let variable_total = u32::from_le_bytes(
        leaf_prefix[20..24]
            .try_into()
            .expect("slice has exact length"),
    ) as usize;
    if variable_begin_offset + variable_total != block_bytes {
        return Err(KalanjiyamError::Corruption(
            "data block variable region mismatch".to_string(),
        ));
    }
    if 64 + entry_count * 24 > variable_begin_offset {
        return Err(KalanjiyamError::Corruption(
            "data block slot area overlaps variable payload region".to_string(),
        ));
    }

    validate_data_leaf_block_crc(&mut file, 128, block_bytes)?;

    file.seek(SeekFrom::Start(0))
        .map_err(|error| KalanjiyamError::io("rewind data file", error))?;
    Ok((
        file,
        DataFileLeafLayout {
            block_offset: 128,
            block_bytes,
            entry_count,
        },
    ))
}

fn validate_data_leaf_block_crc(
    file: &mut File,
    block_offset: u64,
    block_bytes: usize,
) -> Result<(), KalanjiyamError> {
    let mut prefix = [0_u8; 36];
    read_exact_at(
        file,
        block_offset,
        &mut prefix,
        "read data block crc prefix",
    )?;
    let expected_crc =
        u32::from_le_bytes(prefix[32..36].try_into().expect("slice has exact length"));
    file.seek(SeekFrom::Start(block_offset))
        .map_err(|error| KalanjiyamError::io("seek data block for crc", error))?;

    let mut remaining = block_bytes;
    let mut block_pos = 0_usize;
    let mut crc = 0_u32;
    let mut buffer = vec![0_u8; 4096];
    while remaining > 0 {
        let read_len = remaining.min(buffer.len());
        file.read_exact(&mut buffer[..read_len])
            .map_err(|error| KalanjiyamError::io("read data block for crc", error))?;
        if block_pos <= 35 && block_pos + read_len > 32 {
            let start = 32_usize.saturating_sub(block_pos);
            let end = (36_usize.saturating_sub(block_pos)).min(read_len);
            for byte in &mut buffer[start..end] {
                *byte = 0;
            }
        }
        crc = crc32c_append(crc, &buffer[..read_len]);
        remaining -= read_len;
        block_pos += read_len;
    }
    if crc != expected_crc {
        return Err(KalanjiyamError::Checksum(
            "data block checksum mismatch".to_string(),
        ));
    }
    Ok(())
}

fn read_data_leaf_slot(
    file: &mut File,
    layout: &DataFileLeafLayout,
    index: usize,
) -> Result<DataLeafSlot, KalanjiyamError> {
    if index >= layout.entry_count {
        return Err(KalanjiyamError::Corruption(
            "data slot index out of range".to_string(),
        ));
    }
    let slot_offset = layout.block_offset + 64 + (index as u64) * 24;
    let mut slot_bytes = [0_u8; 24];
    read_exact_at(file, slot_offset, &mut slot_bytes, "read data slot")?;

    let key_offset =
        u16::from_le_bytes(slot_bytes[0..2].try_into().expect("slice has exact length")) as usize;
    let key_len =
        u16::from_le_bytes(slot_bytes[2..4].try_into().expect("slice has exact length")) as usize;
    let value_offset =
        u32::from_le_bytes(slot_bytes[4..8].try_into().expect("slice has exact length")) as usize;
    let value_len = u32::from_le_bytes(
        slot_bytes[8..12]
            .try_into()
            .expect("slice has exact length"),
    ) as usize;
    let seqno = u64::from_le_bytes(
        slot_bytes[12..20]
            .try_into()
            .expect("slice has exact length"),
    );
    let kind = match slot_bytes[20] {
        0 => RecordKind::Delete,
        1 => RecordKind::Put,
        other => {
            return Err(KalanjiyamError::Corruption(format!(
                "unknown record kind {other}"
            )));
        }
    };

    let block_end = layout.block_bytes;
    let key_end = key_offset.saturating_add(key_len);
    if key_end > block_end {
        return Err(KalanjiyamError::Corruption(
            "key slot out of range".to_string(),
        ));
    }
    if kind == RecordKind::Delete {
        if value_offset != 0 || value_len != 0 {
            return Err(KalanjiyamError::Corruption(
                "delete slot must store zero value offset and length".to_string(),
            ));
        }
    } else {
        let value_end = value_offset.saturating_add(value_len);
        if value_end > block_end {
            return Err(KalanjiyamError::Corruption(
                "value slot out of range".to_string(),
            ));
        }
    }

    Ok(DataLeafSlot {
        key_offset,
        key_len,
        value_offset,
        value_len,
        seqno,
        kind,
    })
}

fn read_slot_key(
    file: &mut File,
    layout: &DataFileLeafLayout,
    slot: &DataLeafSlot,
) -> Result<Vec<u8>, KalanjiyamError> {
    let mut key = vec![0_u8; slot.key_len];
    read_exact_at(
        file,
        layout.block_offset + slot.key_offset as u64,
        &mut key,
        "read data slot key bytes",
    )?;
    Ok(key)
}

fn read_slot_record(
    file: &mut File,
    layout: &DataFileLeafLayout,
    slot: DataLeafSlot,
) -> Result<InternalRecord, KalanjiyamError> {
    let key = read_slot_key(file, layout, &slot)?;
    let value = if slot.kind == RecordKind::Delete {
        None
    } else {
        let mut value = vec![0_u8; slot.value_len];
        read_exact_at(
            file,
            layout.block_offset + slot.value_offset as u64,
            &mut value,
            "read data slot value bytes",
        )?;
        Some(value)
    };
    Ok(InternalRecord {
        user_key: key,
        seqno: slot.seqno,
        kind: slot.kind,
        value,
    })
}

fn compare_slot_user_key(
    file: &mut File,
    layout: &DataFileLeafLayout,
    index: usize,
    user_key: &[u8],
) -> Result<Ordering, KalanjiyamError> {
    let slot = read_data_leaf_slot(file, layout, index)?;
    let slot_key = read_slot_key(file, layout, &slot)?;
    Ok(slot_key.as_slice().cmp(user_key))
}

fn find_first_slot_with_user_key_ge(
    file: &mut File,
    layout: &DataFileLeafLayout,
    user_key: &[u8],
) -> Result<usize, KalanjiyamError> {
    let mut low = 0_usize;
    let mut high = layout.entry_count;
    while low < high {
        let mid = low + (high - low) / 2;
        match compare_slot_user_key(file, layout, mid, user_key)? {
            Ordering::Less => low = mid + 1,
            Ordering::Equal | Ordering::Greater => high = mid,
        }
    }
    Ok(low)
}

fn skip_same_user_key_forward(
    file: &mut File,
    layout: &DataFileLeafLayout,
    start_index: usize,
    user_key: &[u8],
) -> Result<usize, KalanjiyamError> {
    let mut index = start_index;
    while index < layout.entry_count {
        if compare_slot_user_key(file, layout, index, user_key)? != Ordering::Equal {
            break;
        }
        index += 1;
    }
    Ok(index)
}

fn read_exact_at(
    file: &mut File,
    offset: u64,
    bytes: &mut [u8],
    context: &str,
) -> Result<(), KalanjiyamError> {
    file.seek(SeekFrom::Start(offset))
        .map_err(|error| KalanjiyamError::io(context, error))?;
    file.read_exact(bytes)
        .map_err(|error| KalanjiyamError::io(context, error))
}

fn build_checkpoint_bytes(
    checkpoint_generation: u64,
    checkpoint_state: &CheckpointState,
    page_size_bytes: usize,
) -> Result<Vec<u8>, KalanjiyamError> {
    let manifest_values = checkpoint_state
        .manifest_files
        .iter()
        .map(encode_file_meta_wire)
        .collect::<Result<Vec<_>, _>>()?;
    let manifest_block =
        build_metadata_block(FILE_MANIFEST_BLOCK_KIND, &manifest_values, page_size_bytes)?;
    let logical_values = checkpoint_state
        .logical_shards
        .iter()
        .map(encode_logical_shard_entry_wire)
        .collect::<Result<Vec<_>, _>>()?;
    let logical_block =
        build_metadata_block(LOGICAL_SHARD_BLOCK_KIND, &logical_values, page_size_bytes)?;

    let block_count = if checkpoint_state.manifest_files.is_empty() {
        1
    } else {
        2
    };
    let entry_count =
        checkpoint_state.manifest_files.len() as u64 + checkpoint_state.logical_shards.len() as u64;
    let physical_bytes =
        192 + if block_count == 2 {
            manifest_block.len()
        } else {
            0
        } + logical_block.len()
            + 128;

    let mut bytes = Vec::with_capacity(physical_bytes);
    bytes.extend_from_slice(&build_metadata_header(
        checkpoint_generation,
        checkpoint_state,
        page_size_bytes,
        block_count as u64,
        entry_count,
        physical_bytes as u64,
    ));
    if block_count == 2 {
        bytes.extend_from_slice(&manifest_block);
    }
    bytes.extend_from_slice(&logical_block);
    bytes.extend_from_slice(&build_kjm_footer(
        NONE_U64,
        block_count as u64,
        entry_count,
        NONE_U64,
        NONE_U64,
        checkpoint_generation,
        0,
        physical_bytes as u64,
    ));
    Ok(bytes)
}

fn parse_checkpoint_bytes(bytes: &[u8]) -> Result<CheckpointState, KalanjiyamError> {
    if bytes.len() < 320 {
        return Err(KalanjiyamError::Corruption(
            "checkpoint file is too short".to_string(),
        ));
    }
    let page_size_bytes = validate_kjm_header(bytes, METADATA_FILE_KIND)?;
    validate_kjm_footer(bytes)?;

    let checkpoint_max_seqno =
        u64::from_le_bytes(bytes[128..136].try_into().expect("slice has exact length"));
    let next_seqno =
        u64::from_le_bytes(bytes[136..144].try_into().expect("slice has exact length"));
    let next_file_id =
        u64::from_le_bytes(bytes[144..152].try_into().expect("slice has exact length"));
    let manifest_entry_count =
        u32::from_le_bytes(bytes[152..156].try_into().expect("slice has exact length")) as usize;
    let logical_shard_count =
        u32::from_le_bytes(bytes[156..160].try_into().expect("slice has exact length")) as usize;
    let checkpoint_data_generation =
        u64::from_le_bytes(bytes[160..168].try_into().expect("slice has exact length"));
    if bytes[168..192] != [0_u8; 24] {
        return Err(KalanjiyamError::Corruption(
            "checkpoint reserved extension bytes must be zero".to_string(),
        ));
    }

    let mut offset = 192;
    let mut manifest_files = Vec::new();
    let mut logical_shards = Vec::new();
    while offset < bytes.len() - 128 {
        let block_kind = bytes[offset];
        let span_pages = u32::from_le_bytes(
            bytes[offset + 8..offset + 12]
                .try_into()
                .expect("slice has exact length"),
        ) as usize;
        let block_bytes = span_pages * page_size_bytes;
        let block = bytes
            .get(offset..offset + block_bytes)
            .ok_or_else(|| KalanjiyamError::Corruption("checkpoint block truncated".to_string()))?;
        let values = parse_metadata_block(block, page_size_bytes)?;
        match block_kind {
            FILE_MANIFEST_BLOCK_KIND => {
                for value in values {
                    let (meta, consumed) = decode_file_meta_wire(&value)?;
                    if consumed != value.len() {
                        return Err(KalanjiyamError::Corruption(
                            "checkpoint file meta block entry has trailing bytes".to_string(),
                        ));
                    }
                    manifest_files.push(meta);
                }
            }
            LOGICAL_SHARD_BLOCK_KIND => {
                for value in values {
                    let (entry, consumed) = decode_logical_shard_entry_wire(&value)?;
                    if consumed != value.len() {
                        return Err(KalanjiyamError::Corruption(
                            "checkpoint logical shard block entry has trailing bytes".to_string(),
                        ));
                    }
                    logical_shards.push(entry);
                }
            }
            other => {
                return Err(KalanjiyamError::Corruption(format!(
                    "unknown checkpoint block kind {other}"
                )));
            }
        }
        offset += block_bytes;
    }

    if manifest_files.len() != manifest_entry_count || logical_shards.len() != logical_shard_count {
        return Err(KalanjiyamError::Corruption(
            "checkpoint entry counts do not match header".to_string(),
        ));
    }

    Ok(CheckpointState {
        checkpoint_max_seqno,
        next_seqno,
        next_file_id,
        checkpoint_data_generation,
        manifest_files,
        logical_shards,
    })
}

fn write_and_sync_file(path: &Path, bytes: &[u8], context: &str) -> Result<(), KalanjiyamError> {
    // Writers fsync temporary artifacts before rename so crashes expose either
    // the old file or one fully durable replacement.
    let mut file = File::create(path)
        .map_err(|error| KalanjiyamError::io(format!("{context}: create"), error))?;
    file.write_all(bytes)
        .map_err(|error| KalanjiyamError::io(context, error))?;
    file.sync_all()
        .map_err(|error| KalanjiyamError::io(format!("{context}: sync"), error))
}

fn rename_and_sync_parent(from: &Path, to: &Path, context: &str) -> Result<(), KalanjiyamError> {
    // Publishing one durable artifact is not complete until the parent
    // directory records the new filename.
    fs::rename(from, to).map_err(|error| KalanjiyamError::io(context, error))?;
    sync_directory(
        to.parent()
            .expect("published durable file should have a parent directory"),
        format!("{context}: sync parent directory"),
    )
}

fn sync_path(path: &Path, context: &str) -> Result<(), KalanjiyamError> {
    File::open(path)
        .map_err(|error| KalanjiyamError::io(context, error))?
        .sync_all()
        .map_err(|error| KalanjiyamError::io(context, error))
}

fn sync_directory(directory: &Path, context: impl Into<String>) -> Result<(), KalanjiyamError> {
    // Directory sync closes the rename crash window for spec-published files.
    let context = context.into();
    File::open(directory)
        .map_err(|error| KalanjiyamError::io(context.clone(), error))?
        .sync_all()
        .map_err(|error| KalanjiyamError::io(context, error))
}

fn build_metadata_block(
    block_kind: u8,
    values: &[Vec<u8>],
    page_size_bytes: usize,
) -> Result<Vec<u8>, KalanjiyamError> {
    validate_page_size_bytes(page_size_bytes)?;
    let variable_bytes_total: usize = values.iter().map(Vec::len).sum();
    let required_bytes = 64 + values.len() * 8 + variable_bytes_total;
    let block_span_pages = required_bytes.div_ceil(page_size_bytes) as u32;
    let block_bytes = block_span_pages as usize * page_size_bytes;
    let variable_begin_offset = block_bytes - variable_bytes_total;
    let mut block = vec![0_u8; block_bytes];

    block[0] = block_kind;
    block[4..8].copy_from_slice(&(values.len() as u32).to_le_bytes());
    block[8..12].copy_from_slice(&block_span_pages.to_le_bytes());
    let variable_begin_offset = u16::try_from(variable_begin_offset).map_err(|_| {
        KalanjiyamError::InvalidArgument(
            "metadata block variable region does not fit the v1 offset fields".to_string(),
        )
    })?;
    block[16..18].copy_from_slice(&variable_begin_offset.to_le_bytes());
    block[20..24].copy_from_slice(&(variable_bytes_total as u32).to_le_bytes());
    block[24..32].copy_from_slice(&NONE_U64.to_le_bytes());

    let mut next_variable_offset = block_bytes;
    for (reverse_index, value) in values.iter().rev().enumerate() {
        let slot_index = values.len() - 1 - reverse_index;
        next_variable_offset -= value.len();
        block[next_variable_offset..next_variable_offset + value.len()].copy_from_slice(value);
        let slot_offset = 64 + slot_index * 8;
        block[slot_offset..slot_offset + 4]
            .copy_from_slice(&(next_variable_offset as u32).to_le_bytes());
        block[slot_offset + 4..slot_offset + 8]
            .copy_from_slice(&(value.len() as u32).to_le_bytes());
    }

    let crc = crc32c(&zeroed_block_crc(block.clone()));
    block[32..36].copy_from_slice(&crc.to_le_bytes());
    Ok(block)
}

fn parse_metadata_block(
    block: &[u8],
    page_size_bytes: usize,
) -> Result<Vec<Vec<u8>>, KalanjiyamError> {
    if block.len() < 64 {
        return Err(KalanjiyamError::Corruption(
            "metadata block too short".to_string(),
        ));
    }
    let expected_crc =
        u32::from_le_bytes(block[32..36].try_into().expect("slice has exact length"));
    if expected_crc != crc32c(&zeroed_block_crc(block.to_vec())) {
        return Err(KalanjiyamError::Checksum(
            "metadata block checksum mismatch".to_string(),
        ));
    }
    let entry_count =
        u32::from_le_bytes(block[4..8].try_into().expect("slice has exact length")) as usize;
    let span_pages =
        u32::from_le_bytes(block[8..12].try_into().expect("slice has exact length")) as usize;
    if span_pages * page_size_bytes != block.len() {
        return Err(KalanjiyamError::Corruption(
            "metadata block span mismatch".to_string(),
        ));
    }
    let mut values = Vec::with_capacity(entry_count);
    for slot in block[64..64 + entry_count * 8].chunks_exact(8) {
        let value_offset =
            u32::from_le_bytes(slot[0..4].try_into().expect("slice has exact length")) as usize;
        let value_len =
            u32::from_le_bytes(slot[4..8].try_into().expect("slice has exact length")) as usize;
        values.push(
            block
                .get(value_offset..value_offset + value_len)
                .ok_or_else(|| {
                    KalanjiyamError::Corruption("metadata slot out of range".to_string())
                })?
                .to_vec(),
        );
    }
    Ok(values)
}

fn build_kjm_header(
    file_kind: u16,
    header_bytes: u32,
    page_size_bytes: usize,
    root_block_id_or_none: u64,
    block_count: u64,
    entry_count: u64,
    min_seqno_or_none: u64,
    max_seqno_or_none: u64,
    metadata_generation_or_zero: u64,
    logical_bytes_total: u64,
    physical_bytes_total: u64,
) -> [u8; 128] {
    let mut bytes = [0_u8; 128];
    bytes[0..8].copy_from_slice(KJM_MAGIC);
    bytes[8..10].copy_from_slice(&1_u16.to_le_bytes());
    bytes[12..16].copy_from_slice(&header_bytes.to_le_bytes());
    bytes[16..18].copy_from_slice(&file_kind.to_le_bytes());
    bytes[20..24].copy_from_slice(
        &(u32::try_from(page_size_bytes).expect("supported page size fits u32")).to_le_bytes(),
    );
    bytes[24..32].copy_from_slice(&root_block_id_or_none.to_le_bytes());
    bytes[32..40].copy_from_slice(&block_count.to_le_bytes());
    bytes[40..48].copy_from_slice(&entry_count.to_le_bytes());
    bytes[48..56].copy_from_slice(&min_seqno_or_none.to_le_bytes());
    bytes[56..64].copy_from_slice(&max_seqno_or_none.to_le_bytes());
    bytes[64..72].copy_from_slice(&metadata_generation_or_zero.to_le_bytes());
    bytes[72..80].copy_from_slice(&logical_bytes_total.to_le_bytes());
    bytes[80..88].copy_from_slice(&physical_bytes_total.to_le_bytes());
    let crc = crc32c(&zeroed_header_crc(bytes));
    bytes[88..92].copy_from_slice(&crc.to_le_bytes());
    bytes
}

fn build_metadata_header(
    checkpoint_generation: u64,
    checkpoint_state: &CheckpointState,
    page_size_bytes: usize,
    block_count: u64,
    entry_count: u64,
    physical_bytes_total: u64,
) -> [u8; 192] {
    let mut bytes = [0_u8; 192];
    let base = build_kjm_header(
        METADATA_FILE_KIND,
        192,
        page_size_bytes,
        NONE_U64,
        block_count,
        entry_count,
        NONE_U64,
        NONE_U64,
        checkpoint_generation,
        0,
        physical_bytes_total,
    );
    bytes[..128].copy_from_slice(&base);
    bytes[128..136].copy_from_slice(&checkpoint_state.checkpoint_max_seqno.to_le_bytes());
    bytes[136..144].copy_from_slice(&checkpoint_state.next_seqno.to_le_bytes());
    bytes[144..152].copy_from_slice(&checkpoint_state.next_file_id.to_le_bytes());
    bytes[152..156].copy_from_slice(&(checkpoint_state.manifest_files.len() as u32).to_le_bytes());
    bytes[156..160].copy_from_slice(&(checkpoint_state.logical_shards.len() as u32).to_le_bytes());
    bytes[160..168].copy_from_slice(&checkpoint_state.checkpoint_data_generation.to_le_bytes());
    let crc = crc32c(&zeroed_header_crc(bytes));
    bytes[88..92].copy_from_slice(&crc.to_le_bytes());
    bytes
}

fn build_kjm_footer(
    root_block_id_or_none: u64,
    block_count: u64,
    entry_count: u64,
    min_seqno_or_none: u64,
    max_seqno_or_none: u64,
    metadata_generation_or_zero: u64,
    logical_bytes_total: u64,
    physical_bytes_total: u64,
) -> [u8; 128] {
    let mut bytes = [0_u8; 128];
    bytes[0..8].copy_from_slice(KJM_FOOTER_MAGIC);
    bytes[8..10].copy_from_slice(&1_u16.to_le_bytes());
    bytes[12..16].copy_from_slice(&128_u32.to_le_bytes());
    bytes[16..24].copy_from_slice(&root_block_id_or_none.to_le_bytes());
    bytes[24..32].copy_from_slice(&block_count.to_le_bytes());
    bytes[32..40].copy_from_slice(&entry_count.to_le_bytes());
    bytes[40..48].copy_from_slice(&min_seqno_or_none.to_le_bytes());
    bytes[48..56].copy_from_slice(&max_seqno_or_none.to_le_bytes());
    bytes[56..64].copy_from_slice(&metadata_generation_or_zero.to_le_bytes());
    bytes[64..72].copy_from_slice(&logical_bytes_total.to_le_bytes());
    bytes[72..80].copy_from_slice(&physical_bytes_total.to_le_bytes());
    let crc = crc32c(&zeroed_footer_crc(bytes));
    bytes[80..84].copy_from_slice(&crc.to_le_bytes());
    bytes
}

fn validate_kjm_header(bytes: &[u8], expected_file_kind: u16) -> Result<usize, KalanjiyamError> {
    if &bytes[0..8] != KJM_MAGIC {
        return Err(KalanjiyamError::Corruption(
            "kjm header magic mismatch".to_string(),
        ));
    }
    let header_len = header_bytes(bytes)?;
    let stored_crc = u32::from_le_bytes(bytes[88..92].try_into().expect("slice has exact length"));
    if stored_crc != crc32c(&zeroed_header_crc_vec(bytes[0..header_len].to_vec())) {
        return Err(KalanjiyamError::Checksum(
            "kjm header checksum mismatch".to_string(),
        ));
    }
    let file_kind = u16::from_le_bytes(bytes[16..18].try_into().expect("slice has exact length"));
    if file_kind != expected_file_kind {
        return Err(KalanjiyamError::Corruption(
            "kjm file kind mismatch".to_string(),
        ));
    }
    page_size_bytes_from_header(bytes)
}

fn validate_kjm_footer(bytes: &[u8]) -> Result<(), KalanjiyamError> {
    let footer = &bytes[bytes.len() - 128..];
    let footer: [u8; 128] = footer.try_into().expect("footer length fixed");
    validate_kjm_footer_bytes(&footer)
}

fn validate_kjm_footer_bytes(footer: &[u8; 128]) -> Result<(), KalanjiyamError> {
    if &footer[0..8] != KJM_FOOTER_MAGIC {
        return Err(KalanjiyamError::Corruption(
            "kjm footer magic mismatch".to_string(),
        ));
    }
    let stored_crc = u32::from_le_bytes(footer[80..84].try_into().expect("slice has exact length"));
    if stored_crc != crc32c(&zeroed_footer_crc(*footer)) {
        return Err(KalanjiyamError::Checksum(
            "kjm footer checksum mismatch".to_string(),
        ));
    }
    Ok(())
}

fn header_bytes(bytes: &[u8]) -> Result<usize, KalanjiyamError> {
    let header_bytes =
        u32::from_le_bytes(bytes[12..16].try_into().expect("slice has exact length")) as usize;
    match header_bytes {
        128 | 192 => Ok(header_bytes),
        _ => Err(KalanjiyamError::Corruption(
            "invalid kjm header_bytes value".to_string(),
        )),
    }
}

// `.kjm` artifacts carry their own page size in the common header, so readers
// must validate that value before using it for block-span math.
fn page_size_bytes_from_header(bytes: &[u8]) -> Result<usize, KalanjiyamError> {
    let page_size_bytes =
        u32::from_le_bytes(bytes[20..24].try_into().expect("slice has exact length")) as usize;
    validate_page_size_bytes(page_size_bytes).map_err(|_| {
        KalanjiyamError::Corruption(format!("unsupported kjm page_size_bytes {page_size_bytes}"))
    })?;
    Ok(page_size_bytes)
}

fn page_rows(
    rows: Vec<crate::pezhai::types::ScanRow>,
    max_records_per_page: u32,
    max_bytes_per_page: u32,
) -> (Vec<crate::pezhai::types::ScanRow>, bool, Option<Vec<u8>>) {
    if rows.is_empty() {
        return (Vec::new(), true, None);
    }

    let total_rows = rows.len();
    let first_row = rows.first().cloned();
    let mut page = Vec::new();
    let mut used_bytes = 0_u32;
    for row in rows {
        let row_bytes = (row.key.len() + row.value.len()) as u32;
        let would_exceed_records = page.len() as u32 >= max_records_per_page;
        let would_exceed_bytes = !page.is_empty() && used_bytes + row_bytes > max_bytes_per_page;
        if would_exceed_records || would_exceed_bytes {
            break;
        }
        used_bytes += row_bytes;
        page.push(row);
    }

    if page.is_empty() {
        let first = first_row.expect("non-empty rows has first element");
        let next_resume = Some(first.key.clone());
        return (vec![first], false, next_resume);
    }

    let eof = page.len() == total_rows;
    let next_resume = if eof {
        None
    } else {
        Some(
            page.last()
                .expect("non-empty page has last row")
                .key
                .clone(),
        )
    };
    (page, eof, next_resume)
}

fn zeroed_header_crc<const N: usize>(mut bytes: [u8; N]) -> [u8; N] {
    for byte in &mut bytes[88..92] {
        *byte = 0;
    }
    bytes
}

fn zeroed_header_crc_vec(mut bytes: Vec<u8>) -> Vec<u8> {
    for byte in &mut bytes[88..92] {
        *byte = 0;
    }
    bytes
}

fn zeroed_footer_crc(mut bytes: [u8; 128]) -> [u8; 128] {
    for byte in &mut bytes[80..84] {
        *byte = 0;
    }
    bytes
}

fn zeroed_block_crc(mut bytes: Vec<u8>) -> Vec<u8> {
    for byte in &mut bytes[32..36] {
        *byte = 0;
    }
    bytes
}

fn crc32c(bytes: &[u8]) -> u32 {
    crc32c::crc32c(bytes)
}

fn crc32c_append(crc: u32, bytes: &[u8]) -> u32 {
    crc32c::crc32c_append(crc, bytes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pezhai::types::ScanRow;

    fn put(seqno: u64, key: &[u8], value: &[u8]) -> InternalRecord {
        InternalRecord {
            user_key: key.to_vec(),
            seqno,
            kind: RecordKind::Put,
            value: Some(value.to_vec()),
        }
    }

    fn delete(seqno: u64, key: &[u8]) -> InternalRecord {
        InternalRecord {
            user_key: key.to_vec(),
            seqno,
            kind: RecordKind::Delete,
            value: None,
        }
    }

    fn sample_file_meta() -> FileMeta {
        FileMeta {
            file_id: 1,
            level_no: 0,
            min_user_key: b"ant".to_vec(),
            max_user_key: b"yak".to_vec(),
            min_seqno: 1,
            max_seqno: 9,
            entry_count: 1,
            logical_bytes: 8,
            physical_bytes: 4096,
        }
    }

    fn sample_checkpoint_state() -> CheckpointState {
        CheckpointState {
            checkpoint_max_seqno: 11,
            next_seqno: 12,
            next_file_id: 5,
            checkpoint_data_generation: 7,
            manifest_files: vec![sample_file_meta()],
            logical_shards: vec![LogicalShardEntry {
                start_bound: Bound::NegInf,
                end_bound: Bound::PosInf,
                live_size_bytes: 3,
            }],
        }
    }

    fn rewrite_block_crc(block: &mut [u8]) {
        let crc = crc32c(&zeroed_block_crc(block.to_vec()));
        block[32..36].copy_from_slice(&crc.to_le_bytes());
    }

    fn rewrite_header_crc(bytes: &mut [u8]) {
        let header_len =
            u32::from_le_bytes(bytes[12..16].try_into().expect("slice has exact length")) as usize;
        let crc = crc32c(&zeroed_header_crc_vec(bytes[0..header_len].to_vec()));
        bytes[88..92].copy_from_slice(&crc.to_le_bytes());
    }

    #[test]
    fn build_file_meta_handles_empty_records_and_live_put_helper_variants() {
        let meta = build_file_meta(9, 3, &[]);
        assert_eq!(meta.file_id, 9);
        assert_eq!(meta.level_no, 3);
        assert_eq!(meta.min_user_key, Vec::<u8>::new());
        assert_eq!(meta.max_user_key, Vec::<u8>::new());
        assert_eq!(meta.min_seqno, NONE_U64);
        assert_eq!(meta.max_seqno, NONE_U64);
        assert_eq!(meta.entry_count, 0);
        assert_eq!(meta.logical_bytes, 0);
        assert_eq!(meta.physical_bytes, 0);

        assert!(is_live_put(&put(1, b"ant", b"v1")));
        assert!(!is_live_put(&delete(2, b"ant")));
        assert!(!is_live_put(&InternalRecord {
            user_key: b"ant".to_vec(),
            seqno: 3,
            kind: RecordKind::Put,
            value: None,
        }));
    }

    #[test]
    fn page_rows_covers_empty_non_eof_and_resume_paths() {
        let empty = page_rows(Vec::new(), 10, 10);
        assert_eq!(empty, (Vec::new(), true, None));

        let rows = vec![
            ScanRow {
                key: b"ant".to_vec(),
                value: b"value-one".to_vec(),
            },
            ScanRow {
                key: b"bee".to_vec(),
                value: b"value-two".to_vec(),
            },
        ];
        let forced_single = page_rows(rows.clone(), 0, 1024);
        assert_eq!(forced_single.0.len(), 1);
        assert!(!forced_single.1);
        assert_eq!(forced_single.2, Some(b"ant".to_vec()));

        let partial = page_rows(rows.clone(), 1, 1024);
        assert_eq!(partial.0.len(), 1);
        assert!(!partial.1);
        assert_eq!(partial.2, Some(b"ant".to_vec()));

        let full = page_rows(rows, 10, 10_000);
        assert_eq!(full.0.len(), 2);
        assert!(full.1);
        assert_eq!(full.2, None);
    }

    #[test]
    fn data_leaf_block_parser_rejects_corrupt_variants() {
        let put_block = build_data_leaf_block(&[put(9, b"ant", b"value-one")], 4096)
            .expect("valid block should build");

        assert!(matches!(
            parse_data_leaf_block(&put_block[..63], 4096),
            Err(KalanjiyamError::Corruption(_))
        ));

        let mut bad_kind = put_block.clone();
        bad_kind[0] = 0;
        assert!(matches!(
            parse_data_leaf_block(&bad_kind, 4096),
            Err(KalanjiyamError::Corruption(_))
        ));

        let mut bad_crc = put_block.clone();
        bad_crc[32] ^= 1;
        assert!(matches!(
            parse_data_leaf_block(&bad_crc, 4096),
            Err(KalanjiyamError::Checksum(_))
        ));

        let mut bad_span = put_block.clone();
        bad_span[8..12].copy_from_slice(&2_u32.to_le_bytes());
        rewrite_block_crc(&mut bad_span);
        assert!(matches!(
            parse_data_leaf_block(&bad_span, 4096),
            Err(KalanjiyamError::Corruption(_))
        ));

        let mut bad_variable = put_block.clone();
        bad_variable[20..24].copy_from_slice(&0_u32.to_le_bytes());
        rewrite_block_crc(&mut bad_variable);
        assert!(matches!(
            parse_data_leaf_block(&bad_variable, 4096),
            Err(KalanjiyamError::Corruption(_))
        ));

        let mut bad_record_kind = put_block.clone();
        bad_record_kind[64 + 20] = 9;
        rewrite_block_crc(&mut bad_record_kind);
        assert!(matches!(
            parse_data_leaf_block(&bad_record_kind, 4096),
            Err(KalanjiyamError::Corruption(_))
        ));

        let mut bad_key_slot = put_block.clone();
        bad_key_slot[64..66].copy_from_slice(&4095_u16.to_le_bytes());
        bad_key_slot[64 + 2..64 + 4].copy_from_slice(&32_u16.to_le_bytes());
        rewrite_block_crc(&mut bad_key_slot);
        assert!(matches!(
            parse_data_leaf_block(&bad_key_slot, 4096),
            Err(KalanjiyamError::Corruption(_))
        ));

        let mut bad_value_slot = put_block;
        bad_value_slot[64 + 4..64 + 8].copy_from_slice(&4095_u32.to_le_bytes());
        bad_value_slot[64 + 8..64 + 12].copy_from_slice(&16_u32.to_le_bytes());
        rewrite_block_crc(&mut bad_value_slot);
        assert!(matches!(
            parse_data_leaf_block(&bad_value_slot, 4096),
            Err(KalanjiyamError::Corruption(_))
        ));

        let delete_block =
            build_data_leaf_block(&[delete(10, b"ant")], 4096).expect("delete block should build");
        let mut bad_delete_slot = delete_block;
        bad_delete_slot[64 + 4..64 + 8].copy_from_slice(&1_u32.to_le_bytes());
        rewrite_block_crc(&mut bad_delete_slot);
        assert!(matches!(
            parse_data_leaf_block(&bad_delete_slot, 4096),
            Err(KalanjiyamError::Corruption(_))
        ));
    }

    #[test]
    fn data_and_metadata_parsers_reject_truncation_and_structure_mismatches() {
        let meta = sample_file_meta();
        let data_bytes = build_data_file_bytes(&meta, &[put(7, b"ant", b"value-one")], 4096)
            .expect("data bytes should build");
        assert!(matches!(
            parse_data_file_bytes(&meta, &data_bytes[..255]),
            Err(KalanjiyamError::Corruption(_))
        ));

        let mut bad_block_count = data_bytes.clone();
        bad_block_count[32..40].copy_from_slice(&2_u64.to_le_bytes());
        rewrite_header_crc(&mut bad_block_count);
        assert!(matches!(
            parse_data_file_bytes(&meta, &bad_block_count),
            Err(KalanjiyamError::Corruption(_))
        ));

        let checkpoint = sample_checkpoint_state();
        let bytes =
            build_checkpoint_bytes(3, &checkpoint, 4096).expect("checkpoint bytes should build");
        let parsed = parse_checkpoint_bytes(&bytes).expect("checkpoint bytes should parse");
        assert_eq!(parsed, checkpoint);

        assert!(matches!(
            parse_checkpoint_bytes(&bytes[..319]),
            Err(KalanjiyamError::Corruption(_))
        ));

        let mut bad_reserved = bytes.clone();
        bad_reserved[168] = 1;
        rewrite_header_crc(&mut bad_reserved);
        assert!(matches!(
            parse_checkpoint_bytes(&bad_reserved),
            Err(KalanjiyamError::Corruption(_))
        ));

        let mut bad_counts = bytes.clone();
        bad_counts[152..156].copy_from_slice(&2_u32.to_le_bytes());
        rewrite_header_crc(&mut bad_counts);
        assert!(matches!(
            parse_checkpoint_bytes(&bad_counts),
            Err(KalanjiyamError::Corruption(_))
        ));

        let mut bad_truncated_block = bytes.clone();
        bad_truncated_block[200..204].copy_from_slice(&99_u32.to_le_bytes());
        assert!(matches!(
            parse_checkpoint_bytes(&bad_truncated_block),
            Err(KalanjiyamError::Corruption(_))
        ));

        let mut unknown_kind = bytes.clone();
        let page_size = u32::from_le_bytes(
            unknown_kind[20..24]
                .try_into()
                .expect("slice has exact length"),
        ) as usize;
        let block_span = u32::from_le_bytes(
            unknown_kind[200..204]
                .try_into()
                .expect("slice has exact length"),
        ) as usize;
        let block_bytes = block_span * page_size;
        unknown_kind[192] = 9;
        rewrite_block_crc(&mut unknown_kind[192..192 + block_bytes]);
        assert!(matches!(
            parse_checkpoint_bytes(&unknown_kind),
            Err(KalanjiyamError::Corruption(_))
        ));
    }

    #[test]
    fn metadata_block_and_header_footer_validators_cover_failure_paths() {
        let values = vec![b"a".to_vec(), b"bb".to_vec()];
        let block =
            build_metadata_block(FILE_MANIFEST_BLOCK_KIND, &values, 4096).expect("block builds");
        assert_eq!(
            parse_metadata_block(&block, 4096).expect("block parses"),
            values
        );

        assert!(matches!(
            parse_metadata_block(&block[..63], 4096),
            Err(KalanjiyamError::Corruption(_))
        ));

        let mut bad_crc = block.clone();
        bad_crc[32] ^= 1;
        assert!(matches!(
            parse_metadata_block(&bad_crc, 4096),
            Err(KalanjiyamError::Checksum(_))
        ));

        let mut bad_span = block.clone();
        bad_span[8..12].copy_from_slice(&2_u32.to_le_bytes());
        rewrite_block_crc(&mut bad_span);
        assert!(matches!(
            parse_metadata_block(&bad_span, 4096),
            Err(KalanjiyamError::Corruption(_))
        ));

        let mut bad_slot = block;
        bad_slot[64..68].copy_from_slice(&4095_u32.to_le_bytes());
        bad_slot[68..72].copy_from_slice(&32_u32.to_le_bytes());
        rewrite_block_crc(&mut bad_slot);
        assert!(matches!(
            parse_metadata_block(&bad_slot, 4096),
            Err(KalanjiyamError::Corruption(_))
        ));

        let header = build_kjm_header(DATA_FILE_KIND, 128, 4096, 1, 1, 1, 1, 1, 0, 8, 256);
        assert_eq!(
            validate_kjm_header(&header, DATA_FILE_KIND).expect("header should validate"),
            4096
        );

        let mut bad_header_magic = header;
        bad_header_magic[0] = 0;
        assert!(matches!(
            validate_kjm_header(&bad_header_magic, DATA_FILE_KIND),
            Err(KalanjiyamError::Corruption(_))
        ));

        let mut bad_header_bytes = header;
        bad_header_bytes[12..16].copy_from_slice(&129_u32.to_le_bytes());
        assert!(matches!(
            validate_kjm_header(&bad_header_bytes, DATA_FILE_KIND),
            Err(KalanjiyamError::Corruption(_))
        ));

        let mut bad_header_crc = header;
        bad_header_crc[88] ^= 1;
        assert!(matches!(
            validate_kjm_header(&bad_header_crc, DATA_FILE_KIND),
            Err(KalanjiyamError::Checksum(_))
        ));

        let mut wrong_kind = header;
        wrong_kind[16..18].copy_from_slice(&METADATA_FILE_KIND.to_le_bytes());
        let crc = crc32c(&zeroed_header_crc(wrong_kind));
        wrong_kind[88..92].copy_from_slice(&crc.to_le_bytes());
        assert!(matches!(
            validate_kjm_header(&wrong_kind, DATA_FILE_KIND),
            Err(KalanjiyamError::Corruption(_))
        ));

        let mut bad_page_size = header;
        bad_page_size[20..24].copy_from_slice(&12345_u32.to_le_bytes());
        let crc = crc32c(&zeroed_header_crc(bad_page_size));
        bad_page_size[88..92].copy_from_slice(&crc.to_le_bytes());
        assert!(matches!(
            validate_kjm_header(&bad_page_size, DATA_FILE_KIND),
            Err(KalanjiyamError::Corruption(_))
        ));

        let footer = build_kjm_footer(1, 1, 1, 1, 1, 0, 8, 256);
        assert!(validate_kjm_footer(&footer).is_ok());

        let mut bad_footer_magic = footer;
        bad_footer_magic[0] = 0;
        assert!(matches!(
            validate_kjm_footer(&bad_footer_magic),
            Err(KalanjiyamError::Corruption(_))
        ));

        let mut bad_footer_crc = footer;
        bad_footer_crc[80] ^= 1;
        assert!(matches!(
            validate_kjm_footer(&bad_footer_crc),
            Err(KalanjiyamError::Checksum(_))
        ));
    }

    #[test]
    fn point_lookup_streaming_path_finds_visible_versions() {
        let tempdir = tempfile::tempdir().expect("tempdir should be creatable");
        let store_dir = tempdir.path().join("store");
        fs::create_dir_all(store_dir.join("data")).expect("data dir should be creatable");
        let records = vec![
            put(9, b"yak", b"v9"),
            put(8, b"yak", b"v8"),
            delete(7, b"yak"),
            put(6, b"bee", b"b6"),
        ];
        let meta = build_file_meta(1, 0, &records);
        let _ = write_data_file(&store_dir, &meta, &records, 4096)
            .expect("test data file should be writable");

        let at_8 = find_visible_record_in_data_file(&store_dir, &meta, b"yak", 8)
            .expect("streaming lookup at snapshot 8 should succeed")
            .expect("snapshot 8 should find one visible record");
        assert_eq!(at_8.kind, RecordKind::Put);
        assert_eq!(at_8.value, Some(b"v8".to_vec()));

        let at_9 = find_visible_record_in_data_file(&store_dir, &meta, b"yak", 9)
            .expect("streaming lookup at snapshot 9 should succeed")
            .expect("snapshot 9 should find one visible record");
        assert_eq!(at_9.kind, RecordKind::Put);
        assert_eq!(at_9.value, Some(b"v9".to_vec()));

        let at_7 = find_visible_record_in_data_file(&store_dir, &meta, b"yak", 7)
            .expect("streaming lookup at snapshot 7 should succeed")
            .expect("snapshot 7 should find one visible record");
        assert_eq!(at_7.kind, RecordKind::Delete);
        assert_eq!(at_7.value, None);

        assert_eq!(
            find_visible_record_in_data_file(&store_dir, &meta, b"yak", 5)
                .expect("streaming lookup before first yak record should succeed"),
            None
        );
    }

    #[test]
    fn point_lookup_streaming_path_prefers_delete_over_put_at_same_seqno() {
        let tempdir = tempfile::tempdir().expect("tempdir should be creatable");
        let store_dir = tempdir.path().join("store");
        fs::create_dir_all(store_dir.join("data")).expect("data dir should be creatable");
        let records = vec![
            delete(9, b"yak"),
            put(9, b"yak", b"v9"),
            put(8, b"yak", b"v8"),
        ];
        let meta = build_file_meta(11, 0, &records);
        let _ = write_data_file(&store_dir, &meta, &records, 4096)
            .expect("test data file should be writable");

        let visible = find_visible_record_in_data_file(&store_dir, &meta, b"yak", 9)
            .expect("streaming lookup at snapshot 9 should succeed")
            .expect("snapshot 9 should find one visible record");
        assert_eq!(visible.kind, RecordKind::Delete);
        assert_eq!(visible.value, None);
    }

    #[test]
    fn data_file_record_iterator_is_lazy_and_respects_resume_and_end_bounds() {
        let tempdir = tempfile::tempdir().expect("tempdir should be creatable");
        let store_dir = tempdir.path().join("store");
        fs::create_dir_all(store_dir.join("data")).expect("data dir should be creatable");
        let records = vec![
            put(9, b"ant", b"a9"),
            put(8, b"ant", b"a8"),
            delete(7, b"bee"),
            put(6, b"cat", b"c6"),
            put(5, b"yak", b"y5"),
        ];
        let meta = build_file_meta(2, 0, &records);
        let _ =
            write_data_file(&store_dir, &meta, &records, 4096).expect("test data file is writable");

        let mut iter = DataFileRecordIterator::open(
            &store_dir,
            &meta,
            Some(b"ant"),
            Some(b"yak"),
            Some(b"ant"),
        )
        .expect("iterator should open");
        let first = iter
            .next_record()
            .expect("iterator read should succeed")
            .expect("iterator should have one first record");
        assert_eq!(first.user_key, b"bee".to_vec());
        let second = iter
            .next_record()
            .expect("iterator read should succeed")
            .expect("iterator should have one second record");
        assert_eq!(second.user_key, b"cat".to_vec());
        assert_eq!(
            iter.next_record()
                .expect("iterator terminal read should succeed"),
            None
        );
    }

    #[test]
    fn streaming_slot_reader_matches_internal_key_order() {
        let tempdir = tempfile::tempdir().expect("tempdir should be creatable");
        let store_dir = tempdir.path().join("store");
        fs::create_dir_all(store_dir.join("data")).expect("data dir should be creatable");
        let records = vec![
            put(9, b"yak", b"v9"),
            put(8, b"yak", b"v8"),
            delete(7, b"yak"),
            put(6, b"bee", b"b6"),
        ];
        let meta = build_file_meta(3, 0, &records);
        let path =
            write_data_file(&store_dir, &meta, &records, 4096).expect("test data file is writable");

        let (mut file, layout) = open_validated_data_file(&path).expect("streaming open succeeds");
        let mut seen = Vec::new();
        for index in 0..layout.entry_count {
            let slot = read_data_leaf_slot(&mut file, &layout, index).expect("slot read succeeds");
            let key = read_slot_key(&mut file, &layout, &slot).expect("key read succeeds");
            seen.push((key, slot.seqno, slot.kind));
        }
        assert_eq!(
            seen,
            vec![
                (b"bee".to_vec(), 6, RecordKind::Put),
                (b"yak".to_vec(), 9, RecordKind::Put),
                (b"yak".to_vec(), 8, RecordKind::Put),
                (b"yak".to_vec(), 7, RecordKind::Delete),
            ]
        );
    }
}
