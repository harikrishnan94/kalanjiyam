//! WAL, shared-data file, and recovery helpers used by the engine runtime.

use std::collections::BTreeSet;
use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::config::SyncMode;
use crate::error::Error;
use crate::idam::StoreLayout;
use crate::nilaimai::{
    BuiltDataFileSummary, CompactPublishPayload, FileMeta, FlushPublishPayload, InternalRecord,
    ManifestLevelView, Mutation, RecordKind, compare_internal, compare_internal_to_parts,
    key_in_range,
};
use crate::sevai::Bound;

/// Maximum accepted key size for public engine and server operations.
pub(crate) const MAX_KEY_BYTES: usize = 1024;

/// Maximum accepted value size for public engine and server operations.
pub(crate) const MAX_VALUE_BYTES: usize = 268_435_455;

/// Upper bound used by config validation for one encoded WAL record.
pub(crate) const MAX_SINGLE_WAL_RECORD_BYTES: u64 =
    64 + 8 + MAX_KEY_BYTES as u64 + MAX_VALUE_BYTES as u64 + 8;

const NONE_U64: u64 = u64::MAX;
const FORMAT_MAJOR: u16 = 1;

const CURRENT_BYTES: usize = 72;
const SEGMENT_HEADER_BYTES: usize = 128;
const SEGMENT_FOOTER_BYTES: usize = 128;
const RECORD_HEADER_BYTES: usize = 64;

const CURRENT_MAGIC: &[u8; 8] = b"KJCURR1\0";
const SEGMENT_HEADER_MAGIC: &[u8; 8] = b"KJWALSE1";
const SEGMENT_FOOTER_MAGIC: &[u8; 8] = b"KJWALF1\0";
const RECORD_MAGIC: &[u8; 4] = b"KJWR";

const KJM_HEADER_BYTES: usize = 128;
const KJM_FOOTER_BYTES: usize = 128;
const KJM_HEADER_MAGIC: &[u8; 8] = b"KJKJM001";
const KJM_FOOTER_MAGIC: &[u8; 8] = b"KJKJMF01";
const FILE_KIND_DATA: u16 = 1;
const BLOCK_KIND_DATA_LEAF: u8 = 2;
const FILE_META_FIXED_BYTES: usize = 60;

const WAL_RECORD_PUT: u8 = 1;
const WAL_RECORD_DELETE: u8 = 2;
const WAL_RECORD_FLUSH_PUBLISH: u8 = 3;
const WAL_RECORD_COMPACT_PUBLISH: u8 = 4;

/// One parsed `CURRENT` pointer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CurrentFile {
    pub(crate) checkpoint_generation: u64,
    pub(crate) checkpoint_max_seqno: u64,
    pub(crate) checkpoint_data_generation: u64,
}

/// One recovered mutation that survived WAL replay and was not covered by a publish record.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RecoveredMutation {
    pub(crate) seqno: u64,
    pub(crate) mutation: Mutation,
}

/// The active segment that remains open after recovery.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RecoveredActiveSegment {
    pub(crate) path: PathBuf,
    pub(crate) first_seqno: u64,
    pub(crate) last_seqno: Option<u64>,
    pub(crate) record_count: u64,
    pub(crate) payload_bytes_used: u64,
    pub(crate) bytes_used: u64,
}

/// Replay results used to seed the manifest-backed engine state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct WalRecovery {
    pub(crate) pending_mutations: Vec<RecoveredMutation>,
    pub(crate) levels: Vec<ManifestLevelView>,
    pub(crate) last_committed_seqno: u64,
    pub(crate) data_generation: u64,
    pub(crate) next_file_id: u64,
    pub(crate) active_segment: Option<RecoveredActiveSegment>,
}

/// Test-only failure injection for the WAL writer.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct WalWriterTestOptions {
    pub(crate) fail_at_seqno: Option<u64>,
}

/// One append result returned by the WAL writer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct AppendOutcome {
    pub(crate) durably_synced: bool,
}

/// Sequential WAL writer owned by the engine worker thread.
pub(crate) struct WalWriter {
    layout: StoreLayout,
    segment_bytes: u64,
    active: Option<ActiveSegment>,
    test_options: WalWriterTestOptions,
}

struct ActiveSegment {
    path: PathBuf,
    file: File,
    first_seqno: u64,
    last_seqno: Option<u64>,
    record_count: u64,
    payload_bytes_used: u64,
    bytes_used: u64,
}

enum WalRecordBody {
    Mutation(Mutation),
    FlushPublish(FlushPublishPayload),
    CompactPublish(CompactPublishPayload),
}

struct DecodedWalRecord {
    seqno: u64,
    body: WalRecordBody,
}

struct DecodedDataFile {
    summary: BuiltDataFileSummary,
    records: Vec<InternalRecord>,
}

/// Computes the CRC32C checksum mandated by the storage-engine specification.
#[must_use]
pub(crate) fn crc32c(bytes: &[u8]) -> u32 {
    crc::Crc::<u32>::new(&crc::CRC_32_ISCSI).checksum(bytes)
}

/// Reads and validates the store's `CURRENT` pointer if it exists.
pub(crate) fn read_current(layout: &StoreLayout) -> Result<Option<CurrentFile>, Error> {
    let bytes = match fs::read(layout.current_path()) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(Error::Io(error)),
    };

    if bytes.len() != CURRENT_BYTES {
        return Err(Error::Corruption(format!(
            "CURRENT must be exactly {CURRENT_BYTES} bytes"
        )));
    }
    if &bytes[0..8] != CURRENT_MAGIC {
        return Err(Error::Corruption("CURRENT magic was invalid".into()));
    }
    if read_u16(&bytes, 8) != FORMAT_MAJOR {
        return Err(Error::Corruption("CURRENT format_major was invalid".into()));
    }
    if read_u16(&bytes, 10) != 0
        || read_u64(&bytes, 40) != 0
        || read_u32(&bytes, 48) != 0
        || !bytes[56..72].iter().all(|byte| *byte == 0)
    {
        return Err(Error::Corruption(
            "CURRENT reserved bytes must be zero".into(),
        ));
    }
    if read_u32(&bytes, 12) != CURRENT_BYTES as u32 {
        return Err(Error::Corruption("CURRENT file_bytes was invalid".into()));
    }

    let expected_crc = crc32c(&zeroed_crc_field(&bytes, 52..56));
    if expected_crc != read_u32(&bytes, 52) {
        return Err(Error::Checksum("CURRENT body CRC32C did not match".into()));
    }

    Ok(Some(CurrentFile {
        checkpoint_generation: read_u64(&bytes, 16),
        checkpoint_max_seqno: read_u64(&bytes, 24),
        checkpoint_data_generation: read_u64(&bytes, 32),
    }))
}

/// Builds one valid `CURRENT` file body.
#[cfg_attr(not(test), allow(dead_code))]
#[must_use]
pub(crate) fn build_current_bytes(current: CurrentFile) -> [u8; CURRENT_BYTES] {
    let mut bytes = [0_u8; CURRENT_BYTES];
    bytes[0..8].copy_from_slice(CURRENT_MAGIC);
    bytes[8..10].copy_from_slice(&FORMAT_MAJOR.to_le_bytes());
    bytes[12..16].copy_from_slice(&(CURRENT_BYTES as u32).to_le_bytes());
    bytes[16..24].copy_from_slice(&current.checkpoint_generation.to_le_bytes());
    bytes[24..32].copy_from_slice(&current.checkpoint_max_seqno.to_le_bytes());
    bytes[32..40].copy_from_slice(&current.checkpoint_data_generation.to_le_bytes());
    let crc = crc32c(&zeroed_crc_field(&bytes, 52..56));
    bytes[52..56].copy_from_slice(&crc.to_le_bytes());
    bytes
}

/// Discovers WAL files, replays publish records, and reconstructs the latest manifest state.
pub(crate) fn recover_wal(layout: &StoreLayout, segment_bytes: u64) -> Result<WalRecovery, Error> {
    fs::create_dir_all(layout.wal_dir()).map_err(Error::Io)?;
    let mut discovered = Vec::new();

    for entry in fs::read_dir(layout.wal_dir()).map_err(Error::Io)? {
        let entry = entry.map_err(Error::Io)?;
        let file_name = entry.file_name();
        let Some(file_name) = file_name.to_str() else {
            continue;
        };
        let Some(parsed_name) = parse_wal_file_name(file_name) else {
            continue;
        };
        discovered.push((parsed_name, entry.path()));
    }

    discovered.sort_by(|left, right| {
        left.0
            .first_seqno
            .cmp(&right.0.first_seqno)
            .then(left.0.is_open.cmp(&right.0.is_open))
    });

    let mut active_segment = None;
    let mut records = Vec::new();
    let mut expected_seqno = 1_u64;

    for (name, path) in discovered {
        if name.is_open {
            if active_segment.is_some() {
                return Err(Error::Corruption(
                    "multiple active WAL segments were discovered".into(),
                ));
            }
            let (file_records, recovered_active) =
                parse_active_segment(&path, segment_bytes, expected_seqno)?;
            expected_seqno = expected_seqno.saturating_add(file_records.len() as u64);
            records.extend(file_records);
            active_segment = Some(recovered_active);
        } else {
            let file_records = parse_closed_segment(&path, name, segment_bytes, expected_seqno)?;
            expected_seqno = expected_seqno.saturating_add(file_records.len() as u64);
            records.extend(file_records);
        }
    }

    let mut pending_mutations = Vec::new();
    let mut levels = Vec::<ManifestLevelView>::new();
    let mut used_file_ids = BTreeSet::<u64>::new();
    let mut last_committed_seqno = 0;
    let mut data_generation = 0;
    let mut next_file_id = 1;

    for record in records {
        last_committed_seqno = record.seqno;
        match record.body {
            WalRecordBody::Mutation(mutation) => {
                pending_mutations.push(RecoveredMutation {
                    seqno: record.seqno,
                    mutation,
                });
            }
            WalRecordBody::FlushPublish(payload) => {
                validate_output_file_metas(layout, &payload.output_file_metas, &mut used_file_ids)?;
                remove_oldest_matching_pending_prefix(
                    &mut pending_mutations,
                    payload.source_first_seqno,
                    payload.source_last_seqno,
                    payload.source_record_count,
                )?;
                push_l0_outputs(&mut levels, payload.output_file_metas.clone());
                data_generation += 1;
                next_file_id = next_file_id.max(max_output_file_id(&payload.output_file_metas) + 1);
            }
            WalRecordBody::CompactPublish(payload) => {
                validate_output_file_metas(layout, &payload.output_file_metas, &mut used_file_ids)?;
                remove_input_files(&mut levels, &payload.input_file_ids)?;
                add_output_files(&mut levels, payload.output_file_metas.clone());
                data_generation += 1;
                next_file_id = next_file_id.max(max_output_file_id(&payload.output_file_metas) + 1);
            }
        }
    }

    Ok(WalRecovery {
        pending_mutations,
        levels,
        last_committed_seqno,
        data_generation,
        next_file_id,
        active_segment,
    })
}

impl WalWriter {
    /// Opens a sequential WAL writer after recovery.
    pub(crate) fn open(
        layout: StoreLayout,
        segment_bytes: u64,
        recovered_active: Option<RecoveredActiveSegment>,
        test_options: WalWriterTestOptions,
    ) -> Result<Self, Error> {
        fs::create_dir_all(layout.wal_dir()).map_err(Error::Io)?;
        let active = match recovered_active {
            Some(recovered) => Some(open_recovered_active_segment(&recovered)?),
            None => None,
        };

        Ok(Self {
            layout,
            segment_bytes,
            active,
            test_options,
        })
    }

    /// Appends one mutation to the WAL and optionally synchronizes it immediately.
    pub(crate) fn append_mutation(
        &mut self,
        seqno: u64,
        mutation: &Mutation,
        sync_mode: SyncMode,
    ) -> Result<AppendOutcome, Error> {
        self.append_record(seqno, WalRecordBody::Mutation(mutation.clone()), sync_mode)
    }

    /// Appends one `FlushPublish` record to the WAL.
    pub(crate) fn append_flush_publish(
        &mut self,
        seqno: u64,
        payload: &FlushPublishPayload,
        sync_mode: SyncMode,
    ) -> Result<AppendOutcome, Error> {
        self.append_record(
            seqno,
            WalRecordBody::FlushPublish(payload.clone()),
            sync_mode,
        )
    }

    /// Appends one `CompactPublish` record to the WAL.
    pub(crate) fn append_compact_publish(
        &mut self,
        seqno: u64,
        payload: &CompactPublishPayload,
        sync_mode: SyncMode,
    ) -> Result<AppendOutcome, Error> {
        self.append_record(
            seqno,
            WalRecordBody::CompactPublish(payload.clone()),
            sync_mode,
        )
    }

    /// Synchronizes the current active segment and returns the durable frontier it now covers.
    pub(crate) fn sync_to_current_frontier(&mut self, target_seqno: u64) -> Result<u64, Error> {
        if target_seqno == 0 {
            return Ok(0);
        }
        let Some(active) = self.active.as_mut() else {
            return Ok(target_seqno);
        };

        active.file.sync_all().map_err(Error::Io)?;
        sync_directory(self.layout.wal_dir())?;
        Ok(active.last_seqno.unwrap_or(target_seqno))
    }

    /// Shuts down the writer and leaves the active segment available for the next open.
    pub(crate) fn shutdown(self) {}

    fn append_record(
        &mut self,
        seqno: u64,
        record: WalRecordBody,
        sync_mode: SyncMode,
    ) -> Result<AppendOutcome, Error> {
        let record_bytes = encode_wal_record(seqno, &record)?;
        self.ensure_active_segment(seqno, record_bytes.len() as u64)?;

        if self.test_options.fail_at_seqno == Some(seqno) {
            return Err(Error::Io(std::io::Error::other(format!(
                "injected WAL append failure at seqno {seqno}"
            ))));
        }

        let active = self
            .active
            .as_mut()
            .expect("active WAL segment should exist before writes");
        active.file.write_all(&record_bytes).map_err(Error::Io)?;
        active.bytes_used += record_bytes.len() as u64;
        active.payload_bytes_used += record_bytes.len() as u64;
        active.last_seqno = Some(seqno);
        active.record_count += 1;

        let durably_synced = sync_mode == SyncMode::PerWrite;
        if durably_synced {
            active.file.sync_all().map_err(Error::Io)?;
            sync_directory(self.layout.wal_dir())?;
        }

        Ok(AppendOutcome { durably_synced })
    }

    fn ensure_active_segment(
        &mut self,
        first_seqno_for_new_segment: u64,
        next_record_bytes: u64,
    ) -> Result<(), Error> {
        match self.active.as_ref() {
            Some(active)
                if active.bytes_used + next_record_bytes + SEGMENT_FOOTER_BYTES as u64
                    <= self.segment_bytes =>
            {
                return Ok(());
            }
            Some(_) => self.close_active_segment()?,
            None => {}
        }

        self.active = Some(create_active_segment(
            &self.layout,
            self.segment_bytes,
            first_seqno_for_new_segment,
        )?);
        Ok(())
    }

    fn close_active_segment(&mut self) -> Result<(), Error> {
        let Some(active) = self.active.take() else {
            return Ok(());
        };
        let Some(last_seqno) = active.last_seqno else {
            return Ok(());
        };

        let footer = build_segment_footer(
            active.first_seqno,
            last_seqno,
            active.record_count,
            active.payload_bytes_used,
        );
        let closed_path = self
            .layout
            .wal_dir()
            .join(format_closed_segment_name(active.first_seqno, last_seqno));
        let mut file = active.file;
        file.write_all(&footer).map_err(Error::Io)?;
        file.sync_all().map_err(Error::Io)?;
        drop(file);
        fs::rename(active.path, &closed_path).map_err(Error::Io)?;
        sync_directory(self.layout.wal_dir())?;
        Ok(())
    }
}

/// Builds and fsyncs one temporary shared-data file.
pub(crate) fn build_temp_data_file(
    temp_path: &Path,
    page_size_bytes: u32,
    records: &[InternalRecord],
) -> Result<BuiltDataFileSummary, Error> {
    if records.is_empty() {
        return Err(Error::Corruption(
            "shared data files must contain at least one record".into(),
        ));
    }

    if !page_size_bytes.is_power_of_two() || !(4096..=32768).contains(&page_size_bytes) {
        return Err(Error::InvalidArgument(
            "page_size_bytes must be a power of two in 4096..=32768".into(),
        ));
    }

    let mut sorted_records = records.to_vec();
    sorted_records.sort_by(compare_internal);

    let block = build_data_leaf_block(&sorted_records, page_size_bytes as usize)?;
    let physical_bytes = page_size_bytes as usize + block.len() + KJM_FOOTER_BYTES;
    let summary = BuiltDataFileSummary {
        min_seqno: sorted_records
            .iter()
            .map(|record| record.seqno)
            .min()
            .unwrap_or(0),
        max_seqno: sorted_records
            .iter()
            .map(|record| record.seqno)
            .max()
            .unwrap_or(0),
        entry_count: sorted_records.len() as u64,
        logical_bytes: sorted_records
            .iter()
            .map(InternalRecord::logical_bytes)
            .sum(),
        physical_bytes: physical_bytes as u64,
        min_user_key: sorted_records.first().unwrap().user_key.clone(),
        max_user_key: sorted_records.last().unwrap().user_key.clone(),
    };

    let mut bytes = vec![0_u8; physical_bytes];
    let header = build_data_file_header(page_size_bytes, &summary);
    bytes[0..KJM_HEADER_BYTES].copy_from_slice(&header);
    let block_offset = page_size_bytes as usize;
    bytes[block_offset..block_offset + block.len()].copy_from_slice(&block);
    let footer = build_data_file_footer(&summary);
    let footer_offset = physical_bytes - KJM_FOOTER_BYTES;
    bytes[footer_offset..].copy_from_slice(&footer);

    if let Some(parent) = temp_path.parent() {
        fs::create_dir_all(parent).map_err(Error::Io)?;
    }
    let mut file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(temp_path)
        .map_err(Error::Io)?;
    file.write_all(&bytes).map_err(Error::Io)?;
    file.sync_all().map_err(Error::Io)?;

    Ok(summary)
}

/// Loads one shared-data file and returns only the records that overlap the requested range.
pub(crate) fn load_data_file_records(
    layout: &StoreLayout,
    file_meta: &FileMeta,
    start_bound: &Bound,
    end_bound: &Bound,
) -> Result<Vec<InternalRecord>, Error> {
    let decoded = read_data_file(&layout.data_file_path(file_meta.file_id))?;
    Ok(decoded
        .records
        .into_iter()
        .filter(|record| key_in_range(&record.user_key, start_bound, end_bound))
        .collect())
}

/// Executes one in-file point lookup for the requested user key and snapshot seqno.
pub(crate) fn find_visible_record_in_data_file(
    layout: &StoreLayout,
    file_meta: &FileMeta,
    user_key: &[u8],
    snapshot_seqno: u64,
) -> Result<Option<InternalRecord>, Error> {
    let decoded = read_data_file(&layout.data_file_path(file_meta.file_id))?;
    let search_index = decoded
        .records
        .binary_search_by(|record| {
            compare_internal_to_parts(record, user_key, snapshot_seqno, RecordKind::Delete)
        })
        .unwrap_or_else(|index| index);

    for record in decoded.records.into_iter().skip(search_index) {
        match record.user_key.as_slice().cmp(user_key) {
            std::cmp::Ordering::Less => continue,
            std::cmp::Ordering::Greater => return Ok(None),
            std::cmp::Ordering::Equal => {
                if record.seqno <= snapshot_seqno {
                    return Ok(Some(record));
                }
            }
        }
    }

    Ok(None)
}

fn create_active_segment(
    layout: &StoreLayout,
    segment_bytes: u64,
    first_seqno: u64,
) -> Result<ActiveSegment, Error> {
    let path = layout
        .wal_dir()
        .join(format_active_segment_name(first_seqno));
    let mut file = OpenOptions::new()
        .create_new(true)
        .read(true)
        .write(true)
        .open(&path)
        .map_err(Error::Io)?;
    let header = build_segment_header(first_seqno, segment_bytes);
    file.write_all(&header).map_err(Error::Io)?;
    file.sync_all().map_err(Error::Io)?;
    sync_directory(layout.wal_dir())?;

    Ok(ActiveSegment {
        path,
        file,
        first_seqno,
        last_seqno: None,
        record_count: 0,
        payload_bytes_used: 0,
        bytes_used: SEGMENT_HEADER_BYTES as u64,
    })
}

fn open_recovered_active_segment(
    recovered: &RecoveredActiveSegment,
) -> Result<ActiveSegment, Error> {
    let mut file = OpenOptions::new()
        .read(true)
        .write(true)
        .open(&recovered.path)
        .map_err(Error::Io)?;
    file.set_len(recovered.bytes_used).map_err(Error::Io)?;
    file.seek(SeekFrom::Start(recovered.bytes_used))
        .map_err(Error::Io)?;

    Ok(ActiveSegment {
        path: recovered.path.clone(),
        file,
        first_seqno: recovered.first_seqno,
        last_seqno: recovered.last_seqno,
        record_count: recovered.record_count,
        payload_bytes_used: recovered.payload_bytes_used,
        bytes_used: recovered.bytes_used,
    })
}

fn parse_closed_segment(
    path: &Path,
    name: WalFileName,
    segment_bytes: u64,
    expected_seqno: u64,
) -> Result<Vec<DecodedWalRecord>, Error> {
    let bytes = fs::read(path).map_err(Error::Io)?;
    let header = parse_segment_header(&bytes, segment_bytes)?;
    let footer = parse_segment_footer(path, &bytes, name, &header)?;
    let end_of_records = bytes.len() - SEGMENT_FOOTER_BYTES;
    let (records, stats) = parse_records(path, &bytes, end_of_records, false, expected_seqno)?;

    if footer.record_count != stats.record_count {
        return Err(Error::Corruption(format!(
            "closed WAL segment `{}` record_count did not match",
            path.display()
        )));
    }

    Ok(records)
}

fn parse_active_segment(
    path: &Path,
    segment_bytes: u64,
    expected_seqno: u64,
) -> Result<(Vec<DecodedWalRecord>, RecoveredActiveSegment), Error> {
    let bytes = fs::read(path).map_err(Error::Io)?;
    let header = parse_segment_header(&bytes, segment_bytes)?;
    let (records, stats) = parse_records(path, &bytes, bytes.len(), true, expected_seqno)?;

    Ok((
        records,
        RecoveredActiveSegment {
            path: path.to_path_buf(),
            first_seqno: header.first_seqno,
            last_seqno: stats.last_seqno,
            record_count: stats.record_count,
            payload_bytes_used: stats.payload_bytes_used,
            bytes_used: stats.end_offset,
        },
    ))
}

fn parse_segment_header(bytes: &[u8], segment_bytes: u64) -> Result<ParsedHeader, Error> {
    if bytes.len() < SEGMENT_HEADER_BYTES {
        return Err(Error::Corruption(
            "WAL segment was shorter than its fixed header".into(),
        ));
    }
    if &bytes[0..8] != SEGMENT_HEADER_MAGIC {
        return Err(Error::Corruption(
            "WAL segment header magic was invalid".into(),
        ));
    }
    if read_u16(bytes, 8) != FORMAT_MAJOR {
        return Err(Error::Corruption(
            "WAL segment format_major was invalid".into(),
        ));
    }
    if read_u16(bytes, 10) != 0 || !bytes[36..128].iter().all(|byte| *byte == 0) {
        return Err(Error::Corruption(
            "WAL segment reserved header bytes must be zero".into(),
        ));
    }
    if read_u32(bytes, 12) != SEGMENT_HEADER_BYTES as u32 {
        return Err(Error::Corruption(
            "WAL segment header_bytes was invalid".into(),
        ));
    }
    if read_u64(bytes, 24) != segment_bytes {
        return Err(Error::Corruption(
            "WAL segment_bytes did not match the open config".into(),
        ));
    }

    let expected_crc = crc32c(&zeroed_crc_field(&bytes[0..SEGMENT_HEADER_BYTES], 32..36));
    if expected_crc != read_u32(bytes, 32) {
        return Err(Error::Checksum(
            "WAL segment header CRC32C did not match".into(),
        ));
    }

    Ok(ParsedHeader {
        first_seqno: read_u64(bytes, 16),
    })
}

fn parse_segment_footer(
    path: &Path,
    bytes: &[u8],
    name: WalFileName,
    header: &ParsedHeader,
) -> Result<ParsedFooter, Error> {
    if bytes.len() < SEGMENT_HEADER_BYTES + SEGMENT_FOOTER_BYTES {
        return Err(Error::Corruption(format!(
            "closed WAL segment `{}` was too short",
            path.display()
        )));
    }
    let footer_offset = bytes.len() - SEGMENT_FOOTER_BYTES;
    let footer = &bytes[footer_offset..];
    if &footer[0..8] != SEGMENT_FOOTER_MAGIC {
        return Err(Error::Corruption(
            "WAL segment footer magic was invalid".into(),
        ));
    }
    if !footer[44..128].iter().all(|byte| *byte == 0) {
        return Err(Error::Corruption(
            "WAL segment footer reserved bytes must be zero".into(),
        ));
    }

    let expected_crc = crc32c(&zeroed_crc_field(footer, 40..44));
    if expected_crc != read_u32(footer, 40) {
        return Err(Error::Checksum(
            "WAL segment footer CRC32C did not match".into(),
        ));
    }

    let first_seqno = read_u64(footer, 8);
    let last_seqno = read_u64(footer, 16);
    if name.first_seqno != first_seqno || name.last_seqno != Some(last_seqno) {
        return Err(Error::Corruption(format!(
            "closed WAL segment `{}` did not match its filename range",
            path.display()
        )));
    }
    if header.first_seqno != first_seqno {
        return Err(Error::Corruption(format!(
            "closed WAL segment `{}` header and footer first_seqno disagreed",
            path.display()
        )));
    }

    Ok(ParsedFooter {
        record_count: read_u64(footer, 24),
    })
}

fn parse_records(
    path: &Path,
    bytes: &[u8],
    end_of_records: usize,
    allow_truncated_tail: bool,
    mut expected_seqno: u64,
) -> Result<(Vec<DecodedWalRecord>, RecordStats), Error> {
    let mut offset = SEGMENT_HEADER_BYTES;
    let mut records = Vec::new();
    let mut record_count = 0_u64;
    let mut payload_bytes_used = 0_u64;
    let mut last_seqno = None;

    while offset < end_of_records {
        match try_parse_record(bytes, offset, end_of_records) {
            Ok(Some((record, next_offset))) => {
                if record.seqno != expected_seqno {
                    return Err(Error::Corruption(format!(
                        "WAL record seqno {} in `{}` was out of order; expected {}",
                        record.seqno,
                        path.display(),
                        expected_seqno
                    )));
                }
                expected_seqno += 1;
                record_count += 1;
                payload_bytes_used += (next_offset - offset) as u64;
                last_seqno = Some(record.seqno);
                records.push(record);
                offset = next_offset;
            }
            Ok(None) => break,
            Err(error) if allow_truncated_tail && is_truncation_error(&error) => break,
            Err(error) => return Err(error),
        }
    }

    Ok((
        records,
        RecordStats {
            record_count,
            payload_bytes_used,
            last_seqno,
            end_offset: offset as u64,
        },
    ))
}

fn try_parse_record(
    bytes: &[u8],
    offset: usize,
    end_of_records: usize,
) -> Result<Option<(DecodedWalRecord, usize)>, Error> {
    if offset == end_of_records {
        return Ok(None);
    }
    if offset + RECORD_HEADER_BYTES > end_of_records {
        return Err(Error::Corruption(
            "trailing WAL bytes did not contain a full record header".into(),
        ));
    }
    if &bytes[offset..offset + 4] != RECORD_MAGIC {
        return Err(Error::Corruption("WAL record magic was invalid".into()));
    }
    if bytes[offset + 5..offset + 8].iter().any(|byte| *byte != 0)
        || bytes[offset + 32..offset + 64]
            .iter()
            .any(|byte| *byte != 0)
    {
        return Err(Error::Corruption(
            "WAL record reserved header bytes must be zero".into(),
        ));
    }

    let total_bytes = read_u32(bytes, offset + 8) as usize;
    let payload_bytes = read_u32(bytes, offset + 12) as usize;
    if !total_bytes.is_multiple_of(8) || total_bytes < RECORD_HEADER_BYTES + payload_bytes {
        return Err(Error::Corruption(
            "WAL record size fields were invalid".into(),
        ));
    }
    if offset + total_bytes > end_of_records {
        return Err(Error::Corruption(
            "WAL record extended past the durable bytes in the segment".into(),
        ));
    }

    let header_bytes = &bytes[offset..offset + RECORD_HEADER_BYTES];
    let expected_header_crc = crc32c(&zeroed_crc_field(header_bytes, 24..28));
    if expected_header_crc != read_u32(bytes, offset + 24) {
        return Err(Error::Checksum(
            "WAL record header CRC32C did not match".into(),
        ));
    }

    let payload_offset = offset + RECORD_HEADER_BYTES;
    let payload_end = payload_offset + payload_bytes;
    let payload = &bytes[payload_offset..payload_end];
    let expected_payload_crc = crc32c(payload);
    if expected_payload_crc != read_u32(bytes, offset + 28) {
        return Err(Error::Checksum(
            "WAL record payload CRC32C did not match".into(),
        ));
    }
    if bytes[payload_end..offset + total_bytes]
        .iter()
        .any(|byte| *byte != 0)
    {
        return Err(Error::Corruption(
            "WAL record padding bytes must be zero".into(),
        ));
    }

    let seqno = read_u64(bytes, offset + 16);
    let body = match bytes[offset + 4] {
        WAL_RECORD_PUT => WalRecordBody::Mutation(decode_put_payload(payload)?),
        WAL_RECORD_DELETE => WalRecordBody::Mutation(decode_delete_payload(payload)?),
        WAL_RECORD_FLUSH_PUBLISH => {
            WalRecordBody::FlushPublish(decode_flush_publish_payload(payload)?)
        }
        WAL_RECORD_COMPACT_PUBLISH => {
            WalRecordBody::CompactPublish(decode_compact_publish_payload(payload)?)
        }
        record_type => {
            return Err(Error::Corruption(format!(
                "WAL record type {record_type} is not supported by milestone 3 replay"
            )));
        }
    };

    Ok(Some((
        DecodedWalRecord { seqno, body },
        offset + total_bytes,
    )))
}

fn decode_put_payload(payload: &[u8]) -> Result<Mutation, Error> {
    if payload.len() < 8 {
        return Err(Error::Corruption("Put payload was too short".into()));
    }
    let key_len = read_u16(payload, 0) as usize;
    if read_u16(payload, 2) != 0 {
        return Err(Error::Corruption(
            "Put payload reserved bytes were non-zero".into(),
        ));
    }
    let value_len = read_u32(payload, 4) as usize;
    if payload.len() != 8 + key_len + value_len {
        return Err(Error::Corruption("Put payload lengths were invalid".into()));
    }

    Ok(Mutation::Put {
        key: payload[8..8 + key_len].to_vec(),
        value: payload[8 + key_len..].to_vec(),
    })
}

fn decode_delete_payload(payload: &[u8]) -> Result<Mutation, Error> {
    if payload.len() < 8 {
        return Err(Error::Corruption("Delete payload was too short".into()));
    }
    let key_len = read_u16(payload, 0) as usize;
    if payload[2..8].iter().any(|byte| *byte != 0) {
        return Err(Error::Corruption(
            "Delete payload reserved bytes were non-zero".into(),
        ));
    }
    if payload.len() != 8 + key_len {
        return Err(Error::Corruption(
            "Delete payload length was invalid".into(),
        ));
    }

    Ok(Mutation::Delete {
        key: payload[8..].to_vec(),
    })
}

fn decode_flush_publish_payload(payload: &[u8]) -> Result<FlushPublishPayload, Error> {
    if payload.len() < 40 {
        return Err(Error::Corruption(
            "FlushPublish payload was too short".into(),
        ));
    }

    let output_file_count = read_u32(payload, 32) as usize;
    if read_u32(payload, 36) != 40 {
        return Err(Error::Corruption(
            "FlushPublish fixed_bytes was invalid".into(),
        ));
    }
    if output_file_count != 1 {
        return Err(Error::Corruption(
            "FlushPublish output_file_count must be 1 in v1".into(),
        ));
    }

    let mut offset = 40;
    let mut output_file_metas = Vec::with_capacity(output_file_count);
    for _ in 0..output_file_count {
        let (file_meta, next_offset) = decode_file_meta_wire(payload, offset)?;
        output_file_metas.push(file_meta);
        offset = next_offset;
    }
    if offset != payload.len() {
        return Err(Error::Corruption(
            "FlushPublish payload contained trailing bytes".into(),
        ));
    }

    let source_first_seqno = read_u64(payload, 8);
    let source_last_seqno = read_u64(payload, 16);
    let source_record_count = read_u64(payload, 24);
    if source_first_seqno > source_last_seqno || source_record_count == 0 {
        return Err(Error::Corruption(
            "FlushPublish source fields were invalid".into(),
        ));
    }

    Ok(FlushPublishPayload {
        data_generation_expected: read_u64(payload, 0),
        source_first_seqno,
        source_last_seqno,
        source_record_count,
        output_file_metas,
    })
}

fn decode_compact_publish_payload(payload: &[u8]) -> Result<CompactPublishPayload, Error> {
    if payload.len() < 24 {
        return Err(Error::Corruption(
            "CompactPublish payload was too short".into(),
        ));
    }

    let input_file_count = read_u32(payload, 8) as usize;
    let output_file_count = read_u32(payload, 12) as usize;
    if read_u32(payload, 16) != 24 || read_u32(payload, 20) != 0 {
        return Err(Error::Corruption(
            "CompactPublish fixed bytes were invalid".into(),
        ));
    }
    if input_file_count == 0 {
        return Err(Error::Corruption(
            "CompactPublish input_file_count must be at least one".into(),
        ));
    }

    let mut offset = 24;
    let mut input_file_ids = Vec::with_capacity(input_file_count);
    for _ in 0..input_file_count {
        input_file_ids.push(read_u64(payload, offset));
        offset += 8;
    }
    if !is_strictly_sorted_unique(&input_file_ids) {
        return Err(Error::Corruption(
            "CompactPublish input_file_ids must be sorted and unique".into(),
        ));
    }

    let mut output_file_metas = Vec::with_capacity(output_file_count);
    for _ in 0..output_file_count {
        let (file_meta, next_offset) = decode_file_meta_wire(payload, offset)?;
        output_file_metas.push(file_meta);
        offset = next_offset;
    }
    if offset != payload.len() {
        return Err(Error::Corruption(
            "CompactPublish payload contained trailing bytes".into(),
        ));
    }

    Ok(CompactPublishPayload {
        data_generation_expected: read_u64(payload, 0),
        input_file_ids,
        output_file_metas,
    })
}

fn encode_wal_record(seqno: u64, record: &WalRecordBody) -> Result<Vec<u8>, Error> {
    let (record_type, payload) = match record {
        WalRecordBody::Mutation(mutation) => encode_mutation_payload(mutation)?,
        WalRecordBody::FlushPublish(payload) => (
            WAL_RECORD_FLUSH_PUBLISH,
            encode_flush_publish_payload(payload)?,
        ),
        WalRecordBody::CompactPublish(payload) => (
            WAL_RECORD_COMPACT_PUBLISH,
            encode_compact_publish_payload(payload)?,
        ),
    };

    let total_bytes = align_up(RECORD_HEADER_BYTES + payload.len(), 8);
    let mut bytes = vec![0_u8; total_bytes];
    bytes[0..4].copy_from_slice(RECORD_MAGIC);
    bytes[4] = record_type;
    bytes[8..12].copy_from_slice(&(total_bytes as u32).to_le_bytes());
    bytes[12..16].copy_from_slice(&(payload.len() as u32).to_le_bytes());
    bytes[16..24].copy_from_slice(&seqno.to_le_bytes());
    bytes[RECORD_HEADER_BYTES..RECORD_HEADER_BYTES + payload.len()].copy_from_slice(&payload);
    let payload_crc = crc32c(&payload);
    bytes[28..32].copy_from_slice(&payload_crc.to_le_bytes());
    let header_crc = crc32c(&zeroed_crc_field(&bytes[0..RECORD_HEADER_BYTES], 24..28));
    bytes[24..28].copy_from_slice(&header_crc.to_le_bytes());
    Ok(bytes)
}

fn encode_mutation_payload(mutation: &Mutation) -> Result<(u8, Vec<u8>), Error> {
    match mutation {
        Mutation::Put { key, value } => {
            let payload_len = 8 + key.len() + value.len();
            let mut payload = vec![0_u8; payload_len];
            payload[0..2].copy_from_slice(&(key.len() as u16).to_le_bytes());
            payload[4..8].copy_from_slice(&(value.len() as u32).to_le_bytes());
            payload[8..8 + key.len()].copy_from_slice(key);
            payload[8 + key.len()..].copy_from_slice(value);
            Ok((WAL_RECORD_PUT, payload))
        }
        Mutation::Delete { key } => {
            let payload_len = 8 + key.len();
            let mut payload = vec![0_u8; payload_len];
            payload[0..2].copy_from_slice(&(key.len() as u16).to_le_bytes());
            payload[8..].copy_from_slice(key);
            Ok((WAL_RECORD_DELETE, payload))
        }
    }
}

fn encode_flush_publish_payload(payload: &FlushPublishPayload) -> Result<Vec<u8>, Error> {
    if payload.source_first_seqno > payload.source_last_seqno || payload.source_record_count == 0 {
        return Err(Error::Corruption(
            "FlushPublish source fields were invalid".into(),
        ));
    }
    if payload.output_file_metas.len() != 1 {
        return Err(Error::Corruption(
            "FlushPublish output_file_count must be 1 in v1".into(),
        ));
    }

    let mut bytes = vec![0_u8; 40];
    bytes[0..8].copy_from_slice(&payload.data_generation_expected.to_le_bytes());
    bytes[8..16].copy_from_slice(&payload.source_first_seqno.to_le_bytes());
    bytes[16..24].copy_from_slice(&payload.source_last_seqno.to_le_bytes());
    bytes[24..32].copy_from_slice(&payload.source_record_count.to_le_bytes());
    bytes[32..36].copy_from_slice(&(payload.output_file_metas.len() as u32).to_le_bytes());
    bytes[36..40].copy_from_slice(&40_u32.to_le_bytes());
    for file_meta in &payload.output_file_metas {
        bytes.extend_from_slice(&encode_file_meta_wire(file_meta)?);
    }
    Ok(bytes)
}

fn encode_compact_publish_payload(payload: &CompactPublishPayload) -> Result<Vec<u8>, Error> {
    if payload.input_file_ids.is_empty() || !is_strictly_sorted_unique(&payload.input_file_ids) {
        return Err(Error::Corruption(
            "CompactPublish input_file_ids must be sorted and unique".into(),
        ));
    }

    let mut bytes = vec![0_u8; 24];
    bytes[0..8].copy_from_slice(&payload.data_generation_expected.to_le_bytes());
    bytes[8..12].copy_from_slice(&(payload.input_file_ids.len() as u32).to_le_bytes());
    bytes[12..16].copy_from_slice(&(payload.output_file_metas.len() as u32).to_le_bytes());
    bytes[16..20].copy_from_slice(&24_u32.to_le_bytes());
    for file_id in &payload.input_file_ids {
        bytes.extend_from_slice(&file_id.to_le_bytes());
    }
    for file_meta in &payload.output_file_metas {
        bytes.extend_from_slice(&encode_file_meta_wire(file_meta)?);
    }
    Ok(bytes)
}

fn encode_file_meta_wire(file_meta: &FileMeta) -> Result<Vec<u8>, Error> {
    if file_meta.file_id == 0
        || file_meta.min_user_key.is_empty()
        || file_meta.max_user_key.is_empty()
        || file_meta.min_user_key > file_meta.max_user_key
    {
        return Err(Error::Corruption("FileMetaWire fields were invalid".into()));
    }

    let total_bytes =
        FILE_META_FIXED_BYTES + file_meta.min_user_key.len() + file_meta.max_user_key.len();
    let mut bytes = vec![0_u8; total_bytes];
    bytes[0..8].copy_from_slice(&file_meta.file_id.to_le_bytes());
    bytes[8..16].copy_from_slice(&file_meta.min_seqno.to_le_bytes());
    bytes[16..24].copy_from_slice(&file_meta.max_seqno.to_le_bytes());
    bytes[24..32].copy_from_slice(&file_meta.entry_count.to_le_bytes());
    bytes[32..40].copy_from_slice(&file_meta.logical_bytes.to_le_bytes());
    bytes[40..48].copy_from_slice(&file_meta.physical_bytes.to_le_bytes());
    bytes[48..50].copy_from_slice(&file_meta.level_no.to_le_bytes());
    bytes[50..52].copy_from_slice(&(file_meta.min_user_key.len() as u16).to_le_bytes());
    bytes[52..54].copy_from_slice(&(file_meta.max_user_key.len() as u16).to_le_bytes());
    bytes[56..60].copy_from_slice(&(FILE_META_FIXED_BYTES as u32).to_le_bytes());
    bytes[60..60 + file_meta.min_user_key.len()].copy_from_slice(&file_meta.min_user_key);
    bytes[60 + file_meta.min_user_key.len()..].copy_from_slice(&file_meta.max_user_key);
    Ok(bytes)
}

fn decode_file_meta_wire(bytes: &[u8], offset: usize) -> Result<(FileMeta, usize), Error> {
    if offset + FILE_META_FIXED_BYTES > bytes.len() {
        return Err(Error::Corruption("FileMetaWire was truncated".into()));
    }

    let min_key_len = read_u16(bytes, offset + 50) as usize;
    let max_key_len = read_u16(bytes, offset + 52) as usize;
    if read_u16(bytes, offset + 54) != 0
        || read_u32(bytes, offset + 56) != FILE_META_FIXED_BYTES as u32
    {
        return Err(Error::Corruption(
            "FileMetaWire fixed bytes were invalid".into(),
        ));
    }
    let total_bytes = FILE_META_FIXED_BYTES + min_key_len + max_key_len;
    if offset + total_bytes > bytes.len() || min_key_len == 0 || max_key_len == 0 {
        return Err(Error::Corruption(
            "FileMetaWire lengths were invalid".into(),
        ));
    }

    let min_user_key = bytes[offset + 60..offset + 60 + min_key_len].to_vec();
    let max_user_key = bytes[offset + 60 + min_key_len..offset + total_bytes].to_vec();
    if min_user_key > max_user_key {
        return Err(Error::Corruption(
            "FileMetaWire key bounds were invalid".into(),
        ));
    }

    let file_meta = FileMeta {
        file_id: read_u64(bytes, offset),
        min_seqno: read_u64(bytes, offset + 8),
        max_seqno: read_u64(bytes, offset + 16),
        entry_count: read_u64(bytes, offset + 24),
        logical_bytes: read_u64(bytes, offset + 32),
        physical_bytes: read_u64(bytes, offset + 40),
        level_no: read_u16(bytes, offset + 48),
        min_user_key,
        max_user_key,
    };
    if file_meta.file_id == 0 {
        return Err(Error::Corruption(
            "FileMetaWire file_id must be at least one".into(),
        ));
    }

    Ok((file_meta, offset + total_bytes))
}

fn build_segment_header(first_seqno: u64, segment_bytes: u64) -> [u8; SEGMENT_HEADER_BYTES] {
    let mut bytes = [0_u8; SEGMENT_HEADER_BYTES];
    bytes[0..8].copy_from_slice(SEGMENT_HEADER_MAGIC);
    bytes[8..10].copy_from_slice(&FORMAT_MAJOR.to_le_bytes());
    bytes[12..16].copy_from_slice(&(SEGMENT_HEADER_BYTES as u32).to_le_bytes());
    bytes[16..24].copy_from_slice(&first_seqno.to_le_bytes());
    bytes[24..32].copy_from_slice(&segment_bytes.to_le_bytes());
    let crc = crc32c(&zeroed_crc_field(&bytes, 32..36));
    bytes[32..36].copy_from_slice(&crc.to_le_bytes());
    bytes
}

fn build_segment_footer(
    first_seqno: u64,
    last_seqno: u64,
    record_count: u64,
    payload_bytes_used: u64,
) -> [u8; SEGMENT_FOOTER_BYTES] {
    let mut bytes = [0_u8; SEGMENT_FOOTER_BYTES];
    bytes[0..8].copy_from_slice(SEGMENT_FOOTER_MAGIC);
    bytes[8..16].copy_from_slice(&first_seqno.to_le_bytes());
    bytes[16..24].copy_from_slice(&last_seqno.to_le_bytes());
    bytes[24..32].copy_from_slice(&record_count.to_le_bytes());
    bytes[32..40].copy_from_slice(&payload_bytes_used.to_le_bytes());
    let crc = crc32c(&zeroed_crc_field(&bytes, 40..44));
    bytes[40..44].copy_from_slice(&crc.to_le_bytes());
    bytes
}

fn build_data_file_header(
    page_size_bytes: u32,
    summary: &BuiltDataFileSummary,
) -> [u8; KJM_HEADER_BYTES] {
    let mut bytes = [0_u8; KJM_HEADER_BYTES];
    bytes[0..8].copy_from_slice(KJM_HEADER_MAGIC);
    bytes[8..10].copy_from_slice(&FORMAT_MAJOR.to_le_bytes());
    bytes[12..16].copy_from_slice(&(KJM_HEADER_BYTES as u32).to_le_bytes());
    bytes[16..18].copy_from_slice(&FILE_KIND_DATA.to_le_bytes());
    bytes[20..24].copy_from_slice(&page_size_bytes.to_le_bytes());
    bytes[24..32].copy_from_slice(&1_u64.to_le_bytes());
    bytes[32..40].copy_from_slice(&1_u64.to_le_bytes());
    bytes[40..48].copy_from_slice(&summary.entry_count.to_le_bytes());
    bytes[48..56].copy_from_slice(&summary.min_seqno.to_le_bytes());
    bytes[56..64].copy_from_slice(&summary.max_seqno.to_le_bytes());
    bytes[72..80].copy_from_slice(&summary.logical_bytes.to_le_bytes());
    bytes[80..88].copy_from_slice(&summary.physical_bytes.to_le_bytes());
    let crc = crc32c(&zeroed_crc_field(&bytes, 88..92));
    bytes[88..92].copy_from_slice(&crc.to_le_bytes());
    bytes
}

fn build_data_file_footer(summary: &BuiltDataFileSummary) -> [u8; KJM_FOOTER_BYTES] {
    let mut bytes = [0_u8; KJM_FOOTER_BYTES];
    bytes[0..8].copy_from_slice(KJM_FOOTER_MAGIC);
    bytes[8..10].copy_from_slice(&FORMAT_MAJOR.to_le_bytes());
    bytes[12..16].copy_from_slice(&(KJM_FOOTER_BYTES as u32).to_le_bytes());
    bytes[16..24].copy_from_slice(&1_u64.to_le_bytes());
    bytes[24..32].copy_from_slice(&1_u64.to_le_bytes());
    bytes[32..40].copy_from_slice(&summary.entry_count.to_le_bytes());
    bytes[40..48].copy_from_slice(&summary.min_seqno.to_le_bytes());
    bytes[48..56].copy_from_slice(&summary.max_seqno.to_le_bytes());
    bytes[64..72].copy_from_slice(&summary.logical_bytes.to_le_bytes());
    bytes[72..80].copy_from_slice(&summary.physical_bytes.to_le_bytes());
    let crc = crc32c(&zeroed_crc_field(&bytes, 80..84));
    bytes[80..84].copy_from_slice(&crc.to_le_bytes());
    bytes
}

fn build_data_leaf_block(
    records: &[InternalRecord],
    page_size_bytes: usize,
) -> Result<Vec<u8>, Error> {
    let slot_bytes = 64 + (24 * records.len());
    let variable_bytes_total: usize = records
        .iter()
        .map(|record| record.user_key.len() + record.value.as_ref().map_or(0, Vec::len))
        .sum();
    let block_bytes = align_up(slot_bytes + variable_bytes_total, page_size_bytes);
    let block_span_pages = u32::try_from(block_bytes / page_size_bytes).map_err(|_| {
        Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "data leaf block exceeded the u32 page-span limit",
        ))
    })?;
    let mut bytes = vec![0_u8; block_bytes];

    let mut variable_offset = block_bytes;
    let mut slots = vec![(0_u16, 0_u16, 0_u32, 0_u32, 0_u64, 0_u8); records.len()];
    for (index, record) in records.iter().enumerate().rev() {
        if let Some(value) = &record.value {
            variable_offset = variable_offset.saturating_sub(value.len());
            bytes[variable_offset..variable_offset + value.len()].copy_from_slice(value);
            slots[index].2 = variable_offset as u32;
            slots[index].3 = value.len() as u32;
        }

        variable_offset = variable_offset.saturating_sub(record.user_key.len());
        if variable_offset > u16::MAX as usize {
            return Err(Error::Io(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "data leaf block exceeded the supported key-offset range",
            )));
        }
        bytes[variable_offset..variable_offset + record.user_key.len()]
            .copy_from_slice(&record.user_key);
        slots[index].0 = variable_offset as u16;
        slots[index].1 = record.user_key.len() as u16;
        slots[index].4 = record.seqno;
        slots[index].5 = record.kind as u8;
    }

    if variable_offset < slot_bytes || variable_offset > u16::MAX as usize {
        return Err(Error::Io(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "data leaf block layout exceeded the supported limits",
        )));
    }

    bytes[0] = BLOCK_KIND_DATA_LEAF;
    bytes[4..8].copy_from_slice(&(records.len() as u32).to_le_bytes());
    bytes[8..12].copy_from_slice(&block_span_pages.to_le_bytes());
    bytes[16..18].copy_from_slice(&(variable_offset as u16).to_le_bytes());
    bytes[20..24].copy_from_slice(&((block_bytes - variable_offset) as u32).to_le_bytes());
    bytes[24..32].copy_from_slice(&NONE_U64.to_le_bytes());

    let mut slot_offset = 64;
    for slot in slots {
        bytes[slot_offset..slot_offset + 2].copy_from_slice(&slot.0.to_le_bytes());
        bytes[slot_offset + 2..slot_offset + 4].copy_from_slice(&slot.1.to_le_bytes());
        bytes[slot_offset + 4..slot_offset + 8].copy_from_slice(&slot.2.to_le_bytes());
        bytes[slot_offset + 8..slot_offset + 12].copy_from_slice(&slot.3.to_le_bytes());
        bytes[slot_offset + 12..slot_offset + 20].copy_from_slice(&slot.4.to_le_bytes());
        bytes[slot_offset + 20] = slot.5;
        slot_offset += 24;
    }

    let crc = crc32c(&zeroed_crc_field(&bytes, 32..36));
    bytes[32..36].copy_from_slice(&crc.to_le_bytes());
    Ok(bytes)
}

fn read_data_file(path: &Path) -> Result<DecodedDataFile, Error> {
    let bytes = fs::read(path).map_err(Error::Io)?;
    if bytes.len() < KJM_HEADER_BYTES + KJM_FOOTER_BYTES {
        return Err(Error::Corruption(format!(
            "data file `{}` was too short",
            path.display()
        )));
    }

    let header = &bytes[..KJM_HEADER_BYTES];
    let footer = &bytes[bytes.len() - KJM_FOOTER_BYTES..];
    if &header[0..8] != KJM_HEADER_MAGIC || &footer[0..8] != KJM_FOOTER_MAGIC {
        return Err(Error::Corruption(
            "shared data file header or footer magic was invalid".into(),
        ));
    }
    if read_u16(header, 8) != FORMAT_MAJOR || read_u16(footer, 8) != FORMAT_MAJOR {
        return Err(Error::Corruption(
            "shared data file format_major was invalid".into(),
        ));
    }
    if read_u32(header, 12) != KJM_HEADER_BYTES as u32
        || read_u32(footer, 12) != KJM_FOOTER_BYTES as u32
    {
        return Err(Error::Corruption(
            "shared data file header or footer byte count was invalid".into(),
        ));
    }
    if read_u16(header, 16) != FILE_KIND_DATA {
        return Err(Error::Corruption(
            "shared data file_kind was invalid".into(),
        ));
    }

    let expected_header_crc = crc32c(&zeroed_crc_field(header, 88..92));
    if expected_header_crc != read_u32(header, 88) {
        return Err(Error::Checksum(
            "shared data file header CRC32C did not match".into(),
        ));
    }
    let expected_footer_crc = crc32c(&zeroed_crc_field(footer, 80..84));
    if expected_footer_crc != read_u32(footer, 80) {
        return Err(Error::Checksum(
            "shared data file footer CRC32C did not match".into(),
        ));
    }

    let page_size_bytes = read_u32(header, 20) as usize;
    if !page_size_bytes.is_power_of_two() || !(4096..=32768).contains(&page_size_bytes) {
        return Err(Error::Corruption(
            "shared data file page_size_bytes was invalid".into(),
        ));
    }
    if read_u64(header, 24) != 1 || read_u64(header, 32) != 1 {
        return Err(Error::Corruption(
            "shared data file root_block_id_or_none or block_count was invalid".into(),
        ));
    }
    if read_u64(header, 64) != 0 || read_u64(footer, 56) != 0 {
        return Err(Error::Corruption(
            "shared data file metadata_generation_or_zero must be zero".into(),
        ));
    }

    let footer_physical_bytes = read_u64(footer, 72);
    let header_physical_bytes = read_u64(header, 80);
    if header_physical_bytes != bytes.len() as u64
        || footer_physical_bytes != bytes.len() as u64
        || header_physical_bytes != footer_physical_bytes
    {
        return Err(Error::Corruption(
            "shared data file physical_bytes_total did not match the file length".into(),
        ));
    }

    let block_offset = page_size_bytes;
    if block_offset + 64 + KJM_FOOTER_BYTES > bytes.len() {
        return Err(Error::Corruption(
            "shared data file root block offset was invalid".into(),
        ));
    }
    let block_span_pages = read_u32(&bytes, block_offset + 8) as usize;
    if bytes[block_offset] != BLOCK_KIND_DATA_LEAF || block_span_pages == 0 {
        return Err(Error::Corruption(
            "shared data file root block was not a valid data leaf".into(),
        ));
    }
    let block_bytes = block_span_pages * page_size_bytes;
    let footer_offset = bytes.len() - KJM_FOOTER_BYTES;
    if block_offset + block_bytes != footer_offset {
        return Err(Error::Corruption(
            "shared data file block layout did not reach the footer boundary".into(),
        ));
    }
    let block = &bytes[block_offset..footer_offset];
    let expected_block_crc = crc32c(&zeroed_crc_field(block, 32..36));
    if expected_block_crc != read_u32(block, 32) {
        return Err(Error::Checksum(
            "shared data file block CRC32C did not match".into(),
        ));
    }
    if read_u64(block, 24) != NONE_U64 {
        return Err(Error::Corruption(
            "shared data file next_leaf_block_id_or_none must be NONE in milestone 3".into(),
        ));
    }

    let entry_count = read_u32(block, 4) as usize;
    let variable_begin = read_u16(block, 16) as usize;
    let variable_total = read_u32(block, 20) as usize;
    let slot_end = 64 + 24 * entry_count;
    if variable_begin < slot_end || variable_begin + variable_total > block.len() {
        return Err(Error::Corruption(
            "shared data file variable bytes were invalid".into(),
        ));
    }

    let mut records = Vec::with_capacity(entry_count);
    let mut logical_bytes = 0_u64;
    for index in 0..entry_count {
        let offset = 64 + index * 24;
        let key_offset = read_u16(block, offset) as usize;
        let key_length = read_u16(block, offset + 2) as usize;
        let value_offset = read_u32(block, offset + 4) as usize;
        let value_length = read_u32(block, offset + 8) as usize;
        let seqno = read_u64(block, offset + 12);
        if block[offset + 21..offset + 24]
            .iter()
            .any(|byte| *byte != 0)
        {
            return Err(Error::Corruption(
                "shared data file slot reserved bytes must be zero".into(),
            ));
        }
        if key_offset + key_length > block.len() {
            return Err(Error::Corruption(
                "shared data file key bounds were invalid".into(),
            ));
        }

        let user_key = block[key_offset..key_offset + key_length].to_vec();
        let (kind, value) = match block[offset + 20] {
            1 => {
                if value_offset + value_length > block.len() {
                    return Err(Error::Corruption(
                        "shared data file value bounds were invalid".into(),
                    ));
                }
                (
                    RecordKind::Put,
                    Some(block[value_offset..value_offset + value_length].to_vec()),
                )
            }
            2 => {
                if value_offset != 0 || value_length != 0 {
                    return Err(Error::Corruption(
                        "Delete records must store zero value offsets and lengths".into(),
                    ));
                }
                (RecordKind::Delete, None)
            }
            other => {
                return Err(Error::Corruption(format!(
                    "shared data file record_kind {other} was invalid"
                )));
            }
        };

        let record = InternalRecord {
            user_key,
            seqno,
            kind,
            value,
        };
        logical_bytes += record.logical_bytes();
        records.push(record);
    }
    if !records
        .windows(2)
        .all(|window| compare_internal(&window[0], &window[1]).is_le())
    {
        return Err(Error::Corruption(
            "shared data file records were not sorted in internal-key order".into(),
        ));
    }

    let min_seqno = records.iter().map(|record| record.seqno).min().unwrap_or(0);
    let max_seqno = records.iter().map(|record| record.seqno).max().unwrap_or(0);
    let summary = BuiltDataFileSummary {
        min_seqno,
        max_seqno,
        entry_count: records.len() as u64,
        logical_bytes,
        physical_bytes: bytes.len() as u64,
        min_user_key: records.first().unwrap().user_key.clone(),
        max_user_key: records.last().unwrap().user_key.clone(),
    };

    if read_u64(header, 40) != summary.entry_count
        || read_u64(footer, 32) != summary.entry_count
        || read_u64(header, 48) != summary.min_seqno
        || read_u64(footer, 40) != summary.min_seqno
        || read_u64(header, 56) != summary.max_seqno
        || read_u64(footer, 48) != summary.max_seqno
        || read_u64(header, 72) != summary.logical_bytes
        || read_u64(footer, 64) != summary.logical_bytes
    {
        return Err(Error::Corruption(
            "shared data file header and footer totals did not match the decoded records".into(),
        ));
    }

    Ok(DecodedDataFile { summary, records })
}

fn validate_output_file_metas(
    layout: &StoreLayout,
    output_file_metas: &[FileMeta],
    used_file_ids: &mut BTreeSet<u64>,
) -> Result<(), Error> {
    let mut seen_this_publish = BTreeSet::new();
    for file_meta in output_file_metas {
        if !seen_this_publish.insert(file_meta.file_id)
            || used_file_ids.contains(&file_meta.file_id)
        {
            return Err(Error::Corruption(
                "publish output_file_metas reused a FileId".into(),
            ));
        }
        let decoded = read_data_file(&layout.data_file_path(file_meta.file_id))?;
        let expected = decoded
            .summary
            .to_file_meta(file_meta.file_id, file_meta.level_no);
        if expected != *file_meta {
            return Err(Error::Corruption(format!(
                "shared data file metadata for file_id {} did not match the WAL payload",
                file_meta.file_id
            )));
        }
        used_file_ids.insert(file_meta.file_id);
    }

    Ok(())
}

fn remove_oldest_matching_pending_prefix(
    pending_mutations: &mut Vec<RecoveredMutation>,
    source_first_seqno: u64,
    source_last_seqno: u64,
    source_record_count: u64,
) -> Result<(), Error> {
    if pending_mutations.len() < source_record_count as usize || source_record_count == 0 {
        return Err(Error::Corruption(
            "FlushPublish referenced a pending-mutation prefix that did not exist".into(),
        ));
    }

    let first = pending_mutations
        .first()
        .expect("pending prefix should exist");
    let last = &pending_mutations[source_record_count as usize - 1];
    if first.seqno != source_first_seqno
        || last.seqno != source_last_seqno
        || source_last_seqno < source_first_seqno
    {
        return Err(Error::Corruption(
            "FlushPublish did not match the oldest replay pending prefix".into(),
        ));
    }

    pending_mutations.drain(..source_record_count as usize);
    Ok(())
}

fn push_l0_outputs(levels: &mut Vec<ManifestLevelView>, outputs: Vec<FileMeta>) {
    let level_zero = ensure_level(levels, 0);
    level_zero.files.extend(outputs);
    level_zero.files.sort_by_key(|file| file.file_id);
}

fn add_output_files(levels: &mut Vec<ManifestLevelView>, outputs: Vec<FileMeta>) {
    for output in outputs {
        let level = ensure_level(levels, output.level_no);
        level.files.push(output);
    }

    levels.sort_by_key(|level| level.level_no);
    for level in levels.iter_mut() {
        if level.level_no == 0 {
            level.files.sort_by_key(|file| file.file_id);
        } else {
            level
                .files
                .sort_by(|left, right| left.min_user_key.cmp(&right.min_user_key));
        }
    }
    levels.retain(|level| !level.files.is_empty());
}

fn remove_input_files(
    levels: &mut Vec<ManifestLevelView>,
    input_file_ids: &[u64],
) -> Result<(), Error> {
    for input_file_id in input_file_ids {
        let mut removed = false;
        for level in levels.iter_mut() {
            if let Some(index) = level
                .files
                .iter()
                .position(|file| file.file_id == *input_file_id)
            {
                level.files.remove(index);
                removed = true;
                break;
            }
        }
        if !removed {
            return Err(Error::Corruption(format!(
                "CompactPublish input_file_id {input_file_id} was not present during replay"
            )));
        }
    }
    levels.retain(|level| !level.files.is_empty());
    Ok(())
}

fn ensure_level(levels: &mut Vec<ManifestLevelView>, level_no: u16) -> &mut ManifestLevelView {
    if let Some(index) = levels.iter().position(|level| level.level_no == level_no) {
        return &mut levels[index];
    }

    levels.push(ManifestLevelView {
        level_no,
        files: Vec::new(),
    });
    levels.sort_by_key(|level| level.level_no);
    let index = levels
        .iter()
        .position(|level| level.level_no == level_no)
        .expect("level should exist after insertion");
    &mut levels[index]
}

fn max_output_file_id(output_file_metas: &[FileMeta]) -> u64 {
    output_file_metas
        .iter()
        .map(|file| file.file_id)
        .max()
        .unwrap_or(0)
}

fn format_active_segment_name(first_seqno: u64) -> String {
    format!("wal-{first_seqno:020}-open.log")
}

fn format_closed_segment_name(first_seqno: u64, last_seqno: u64) -> String {
    format!("wal-{first_seqno:020}-{last_seqno:020}.log")
}

fn parse_wal_file_name(file_name: &str) -> Option<WalFileName> {
    let prefix = "wal-";
    let suffix = ".log";
    if !file_name.starts_with(prefix) || !file_name.ends_with(suffix) {
        return None;
    }

    let body = &file_name[prefix.len()..file_name.len() - suffix.len()];
    let mut parts = body.split('-');
    let first = parts.next()?;
    let second = parts.next()?;
    if parts.next().is_some() || first.len() != 20 {
        return None;
    }

    let first_seqno = first.parse().ok()?;
    if second == "open" {
        return Some(WalFileName {
            first_seqno,
            last_seqno: None,
            is_open: true,
        });
    }
    if second.len() != 20 {
        return None;
    }

    Some(WalFileName {
        first_seqno,
        last_seqno: Some(second.parse().ok()?),
        is_open: false,
    })
}

fn sync_directory(path: &Path) -> Result<(), Error> {
    File::open(path)
        .map_err(Error::Io)?
        .sync_all()
        .map_err(Error::Io)
}

fn read_u16(bytes: &[u8], offset: usize) -> u16 {
    u16::from_le_bytes(bytes[offset..offset + 2].try_into().expect("u16 slice"))
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_le_bytes(bytes[offset..offset + 4].try_into().expect("u32 slice"))
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_le_bytes(bytes[offset..offset + 8].try_into().expect("u64 slice"))
}

fn zeroed_crc_field(bytes: &[u8], crc_range: std::ops::Range<usize>) -> Vec<u8> {
    let mut copy = bytes.to_vec();
    copy[crc_range].fill(0);
    copy
}

const fn align_up(value: usize, alignment: usize) -> usize {
    value.div_ceil(alignment) * alignment
}

fn is_truncation_error(error: &Error) -> bool {
    matches!(error, Error::Corruption(message) if message.contains("trailing WAL bytes") || message.contains("extended past"))
}

fn is_strictly_sorted_unique(values: &[u64]) -> bool {
    values.windows(2).all(|window| window[0] < window[1])
}

struct ParsedHeader {
    first_seqno: u64,
}

struct ParsedFooter {
    record_count: u64,
}

struct RecordStats {
    record_count: u64,
    payload_bytes_used: u64,
    last_seqno: Option<u64>,
    end_offset: u64,
}

#[derive(Clone, Copy)]
struct WalFileName {
    first_seqno: u64,
    last_seqno: Option<u64>,
    is_open: bool,
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use crate::idam::StoreLayout;
    use crate::nilaimai::{
        CompactPublishPayload, FileMeta, FlushPublishPayload, InternalRecord, Mutation, RecordKind,
    };

    use super::{
        AppendOutcome, CurrentFile, SyncMode, WalWriter, WalWriterTestOptions, build_current_bytes,
        build_segment_header, build_temp_data_file, crc32c, encode_wal_record,
        format_active_segment_name, parse_wal_file_name, read_current, recover_wal,
    };

    static NEXT_PATH_ID: AtomicU64 = AtomicU64::new(0);

    #[test]
    fn crc32c_matches_the_standard_test_vector() {
        assert_eq!(crc32c(b"123456789"), 0xe306_9283);
    }

    #[test]
    fn current_round_trips_and_rejects_bad_crc() {
        let layout = test_layout();
        fs::create_dir_all(layout.store_root()).unwrap();
        let bytes = build_current_bytes(CurrentFile {
            checkpoint_generation: 7,
            checkpoint_max_seqno: 8,
            checkpoint_data_generation: 9,
        });
        fs::write(layout.current_path(), bytes).unwrap();

        let current = read_current(&layout).unwrap().unwrap();
        assert_eq!(current.checkpoint_generation, 7);
        assert_eq!(current.checkpoint_max_seqno, 8);
        assert_eq!(current.checkpoint_data_generation, 9);

        let mut corrupt = bytes;
        corrupt[52] ^= 0x01;
        fs::write(layout.current_path(), corrupt).unwrap();
        let error = read_current(&layout).unwrap_err();
        assert!(error.to_string().contains("CURRENT body CRC32C"));
    }

    #[test]
    fn wal_file_name_parsing_accepts_active_and_closed_files() {
        let active = parse_wal_file_name("wal-00000000000000000001-open.log").unwrap();
        assert!(active.is_open);

        let closed =
            parse_wal_file_name("wal-00000000000000000001-00000000000000000002.log").unwrap();
        assert_eq!(closed.last_seqno, Some(2));
    }

    #[test]
    fn encodes_aligned_mutation_and_publish_records() {
        let put = encode_wal_record(
            1,
            &super::WalRecordBody::Mutation(Mutation::Put {
                key: b"ant".to_vec(),
                value: b"v".to_vec(),
            }),
        )
        .unwrap();
        assert_eq!(put.len() % 8, 0);
        assert_eq!(&put[0..4], b"KJWR");

        let flush = encode_wal_record(
            2,
            &super::WalRecordBody::FlushPublish(FlushPublishPayload {
                data_generation_expected: 3,
                source_first_seqno: 1,
                source_last_seqno: 1,
                source_record_count: 1,
                output_file_metas: vec![FileMeta {
                    file_id: 7,
                    min_seqno: 1,
                    max_seqno: 1,
                    entry_count: 1,
                    logical_bytes: 4,
                    physical_bytes: 4224,
                    level_no: 0,
                    min_user_key: b"ant".to_vec(),
                    max_user_key: b"ant".to_vec(),
                }],
            }),
        )
        .unwrap();
        assert_eq!(flush.len() % 8, 0);

        let compact = encode_wal_record(
            3,
            &super::WalRecordBody::CompactPublish(CompactPublishPayload {
                data_generation_expected: 4,
                input_file_ids: vec![7],
                output_file_metas: vec![FileMeta {
                    file_id: 8,
                    min_seqno: 1,
                    max_seqno: 1,
                    entry_count: 1,
                    logical_bytes: 4,
                    physical_bytes: 4224,
                    level_no: 1,
                    min_user_key: b"ant".to_vec(),
                    max_user_key: b"ant".to_vec(),
                }],
            }),
        )
        .unwrap();
        assert_eq!(compact.len() % 8, 0);
    }

    #[test]
    fn data_files_round_trip_records_and_metadata() {
        let layout = test_layout();
        fs::create_dir_all(layout.data_dir()).unwrap();
        let temp_path = layout.temp_data_file_path("round-trip");
        let summary = build_temp_data_file(
            &temp_path,
            4096,
            &[
                InternalRecord {
                    user_key: b"ant".to_vec(),
                    seqno: 2,
                    kind: RecordKind::Delete,
                    value: None,
                },
                InternalRecord {
                    user_key: b"ant".to_vec(),
                    seqno: 1,
                    kind: RecordKind::Put,
                    value: Some(b"a".to_vec()),
                },
                InternalRecord {
                    user_key: b"bee".to_vec(),
                    seqno: 3,
                    kind: RecordKind::Put,
                    value: Some(b"b".to_vec()),
                },
            ],
        )
        .unwrap();
        let file_path = layout.data_file_path(1);
        fs::rename(&temp_path, &file_path).unwrap();

        let decoded = super::read_data_file(&file_path).unwrap();
        assert_eq!(decoded.summary, summary);
        assert_eq!(decoded.records.len(), 3);
        assert_eq!(decoded.records[0].user_key, b"ant".to_vec());
        assert_eq!(decoded.records[2].user_key, b"bee".to_vec());
    }

    #[test]
    fn recovers_mutations_and_publish_records_from_an_active_segment() {
        let layout = test_layout();
        fs::create_dir_all(layout.wal_dir()).unwrap();
        fs::create_dir_all(layout.data_dir()).unwrap();
        let temp_path = layout.temp_data_file_path("recover");
        let summary = build_temp_data_file(
            &temp_path,
            4096,
            &[InternalRecord {
                user_key: b"ant".to_vec(),
                seqno: 1,
                kind: RecordKind::Put,
                value: Some(b"a".to_vec()),
            }],
        )
        .unwrap();
        fs::rename(&temp_path, layout.data_file_path(1)).unwrap();

        let path = layout.wal_dir().join(format_active_segment_name(1));
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&build_segment_header(1, 1_073_741_824));
        bytes.extend_from_slice(
            &encode_wal_record(
                1,
                &super::WalRecordBody::Mutation(Mutation::Put {
                    key: b"ant".to_vec(),
                    value: b"a".to_vec(),
                }),
            )
            .unwrap(),
        );
        bytes.extend_from_slice(
            &encode_wal_record(
                2,
                &super::WalRecordBody::FlushPublish(FlushPublishPayload {
                    data_generation_expected: 1,
                    source_first_seqno: 1,
                    source_last_seqno: 1,
                    source_record_count: 1,
                    output_file_metas: vec![summary.to_file_meta(1, 0)],
                }),
            )
            .unwrap(),
        );
        fs::write(path, bytes).unwrap();

        let recovered = recover_wal(&layout, 1_073_741_824).unwrap();
        assert_eq!(recovered.last_committed_seqno, 2);
        assert_eq!(recovered.pending_mutations.len(), 0);
        assert_eq!(recovered.data_generation, 1);
        assert_eq!(recovered.levels[0].files.len(), 1);
    }

    #[test]
    fn writer_appends_and_syncs_mutations() {
        let layout = test_layout();
        let mut writer = WalWriter::open(
            layout.clone(),
            1_073_741_824,
            None,
            WalWriterTestOptions::default(),
        )
        .unwrap();

        let append = writer
            .append_mutation(
                1,
                &Mutation::Put {
                    key: b"ant".to_vec(),
                    value: b"a".to_vec(),
                },
                SyncMode::Manual,
            )
            .unwrap();
        assert_eq!(
            append,
            AppendOutcome {
                durably_synced: false
            }
        );
        assert_eq!(writer.sync_to_current_frontier(1).unwrap(), 1);
        writer.shutdown();

        let recovered = recover_wal(&layout, 1_073_741_824).unwrap();
        assert_eq!(recovered.last_committed_seqno, 1);
        assert_eq!(recovered.pending_mutations.len(), 1);
    }

    fn test_layout() -> StoreLayout {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let path_id = NEXT_PATH_ID.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!("pezhai-pathivu-{unique}-{path_id}"));
        fs::create_dir_all(&root).unwrap();
        let config_path = root.join("config.toml");
        fs::write(&config_path, "[sevai]\nlisten_addr = \"127.0.0.1:0\"\n").unwrap();
        StoreLayout::from_config_path(&config_path)
    }
}
