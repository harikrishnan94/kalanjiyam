//! WAL, checksum, and recovery helpers shared by the direct engine and later durable work.

use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::config::SyncMode;
use crate::error::Error;
use crate::idam::StoreLayout;
use crate::nilaimai::Mutation;

/// Maximum accepted key size for public engine and server operations.
pub(crate) const MAX_KEY_BYTES: usize = 1024;

/// Maximum accepted value size for public engine and server operations.
pub(crate) const MAX_VALUE_BYTES: usize = 268_435_455;

/// Upper bound used by config validation for one encoded WAL record.
pub(crate) const MAX_SINGLE_WAL_RECORD_BYTES: u64 =
    64 + 8 + MAX_KEY_BYTES as u64 + MAX_VALUE_BYTES as u64 + 8;

const CURRENT_BYTES: usize = 72;
const SEGMENT_HEADER_BYTES: usize = 128;
const SEGMENT_FOOTER_BYTES: usize = 128;
const RECORD_HEADER_BYTES: usize = 64;
const FORMAT_MAJOR: u16 = 1;
const CURRENT_MAGIC: &[u8; 8] = b"KJCURR1\0";
const SEGMENT_HEADER_MAGIC: &[u8; 8] = b"KJWALSE1";
const SEGMENT_FOOTER_MAGIC: &[u8; 8] = b"KJWALF1\0";
const RECORD_MAGIC: &[u8; 4] = b"KJWR";
const WAL_RECORD_PUT: u8 = 1;
const WAL_RECORD_DELETE: u8 = 2;

/// One parsed `CURRENT` pointer.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct CurrentFile {
    pub(crate) checkpoint_generation: u64,
    pub(crate) checkpoint_max_seqno: u64,
    pub(crate) checkpoint_data_generation: u64,
}

/// One recovered mutation that survived WAL replay.
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

/// Replay results used to seed the in-memory engine state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct WalRecovery {
    pub(crate) mutations: Vec<RecoveredMutation>,
    pub(crate) last_committed_seqno: u64,
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

/// Sequential WAL writer owned by the direct engine runtime.
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

/// Computes the CRC32C checksum mandated by the storage-engine specification.
#[must_use]
pub(crate) fn crc32c(bytes: &[u8]) -> u32 {
    crc::Crc::<u32>::new(&crc::CRC_32_ISCSI).checksum(bytes)
}

/// Reads and validates the store's `CURRENT` pointer if it exists.
pub(crate) fn read_current(layout: &StoreLayout) -> Result<Option<CurrentFile>, Error> {
    let path = layout.current_path();
    let bytes = match fs::read(&path) {
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

/// Builds one valid `CURRENT` file body. Used by tests and later checkpoint installs.
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

/// Discovers WAL files and replays durable `Put` and `Delete` records from physical order.
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
    let mut mutations = Vec::new();
    let mut expected_seqno = 1_u64;

    for (name, path) in discovered {
        if name.is_open {
            if active_segment.is_some() {
                return Err(Error::Corruption(
                    "multiple active WAL segments were discovered".into(),
                ));
            }
            let (file_mutations, recovered_active) =
                parse_active_segment(&path, segment_bytes, expected_seqno)?;
            expected_seqno = expected_seqno.saturating_add(file_mutations.len() as u64);
            mutations.extend(file_mutations);
            active_segment = Some(recovered_active);
        } else {
            let file_mutations = parse_closed_segment(&path, name, segment_bytes, expected_seqno)?;
            expected_seqno = expected_seqno.saturating_add(file_mutations.len() as u64);
            mutations.extend(file_mutations);
        }
    }

    let last_committed_seqno = mutations.last().map(|mutation| mutation.seqno).unwrap_or(0);
    Ok(WalRecovery {
        mutations,
        last_committed_seqno,
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
        let record_bytes = encode_mutation_record(seqno, mutation)?;
        self.ensure_active_segment(seqno, record_bytes.len() as u64)?;

        if self.test_options.fail_at_seqno == Some(seqno) {
            return Err(Error::Io(std::io::Error::other(format!(
                "injected WAL append failure at seqno {seqno}"
            ))));
        }

        let active = self
            .active
            .as_mut()
            .expect("active segment should exist before writes");
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
            Some(_) => {
                self.close_active_segment()?;
            }
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
) -> Result<Vec<RecoveredMutation>, Error> {
    let bytes = fs::read(path).map_err(Error::Io)?;
    let header = parse_segment_header(&bytes, segment_bytes)?;
    let footer = parse_segment_footer(path, &bytes, name, &header)?;
    let end_of_records = bytes.len() - SEGMENT_FOOTER_BYTES;
    let (records, _stats) = parse_records(path, &bytes, end_of_records, false, expected_seqno)?;

    if footer.record_count != records.len() as u64 {
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
) -> Result<(Vec<RecoveredMutation>, RecoveredActiveSegment), Error> {
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
) -> Result<(Vec<RecoveredMutation>, RecordStats), Error> {
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
                records.push(RecoveredMutation {
                    seqno: record.seqno,
                    mutation: record.mutation,
                });
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
) -> Result<Option<(DecodedRecord, usize)>, Error> {
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
    let mutation = match bytes[offset + 4] {
        WAL_RECORD_PUT => decode_put_payload(payload)?,
        WAL_RECORD_DELETE => decode_delete_payload(payload)?,
        record_type => {
            return Err(Error::Corruption(format!(
                "WAL record type {record_type} is not supported by milestone 2 replay"
            )));
        }
    };

    Ok(Some((
        DecodedRecord { seqno, mutation },
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
    let key = payload[8..8 + key_len].to_vec();
    let value = payload[8 + key_len..].to_vec();

    Ok(Mutation::Put { key, value })
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

fn encode_mutation_record(seqno: u64, mutation: &Mutation) -> Result<Vec<u8>, Error> {
    let (record_type, payload) = match mutation {
        Mutation::Put { key, value } => {
            let payload_len = 8 + key.len() + value.len();
            let mut payload = vec![0_u8; payload_len];
            payload[0..2].copy_from_slice(&(key.len() as u16).to_le_bytes());
            payload[4..8].copy_from_slice(&(value.len() as u32).to_le_bytes());
            payload[8..8 + key.len()].copy_from_slice(key);
            payload[8 + key.len()..].copy_from_slice(value);
            (WAL_RECORD_PUT, payload)
        }
        Mutation::Delete { key } => {
            let payload_len = 8 + key.len();
            let mut payload = vec![0_u8; payload_len];
            payload[0..2].copy_from_slice(&(key.len() as u16).to_le_bytes());
            payload[8..].copy_from_slice(key);
            (WAL_RECORD_DELETE, payload)
        }
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

fn sync_directory(path: &Path) -> Result<(), Error> {
    File::open(path)
        .map_err(Error::Io)?
        .sync_all()
        .map_err(Error::Io)
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

struct DecodedRecord {
    seqno: u64,
    mutation: Mutation,
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

    use super::{
        AppendOutcome, CurrentFile, Mutation, SyncMode, WalWriter, WalWriterTestOptions,
        build_current_bytes, build_segment_header, crc32c, encode_mutation_record,
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
    fn encodes_records_with_alignment_and_detectable_payloads() {
        let put = encode_mutation_record(
            1,
            &Mutation::Put {
                key: b"ant".to_vec(),
                value: b"v".to_vec(),
            },
        )
        .unwrap();
        assert_eq!(put.len() % 8, 0);
        assert_eq!(&put[0..4], b"KJWR");

        let delete = encode_mutation_record(
            2,
            &Mutation::Delete {
                key: b"ant".to_vec(),
            },
        )
        .unwrap();
        assert_eq!(delete.len() % 8, 0);
        assert_eq!(&delete[0..4], b"KJWR");
    }

    #[test]
    fn recovers_mutations_from_an_active_segment() {
        let layout = test_layout();
        fs::create_dir_all(layout.wal_dir()).unwrap();
        let path = layout.wal_dir().join(format_active_segment_name(1));
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&build_segment_header(1, 1_073_741_824));
        bytes.extend_from_slice(
            &encode_mutation_record(
                1,
                &Mutation::Put {
                    key: b"ant".to_vec(),
                    value: b"a".to_vec(),
                },
            )
            .unwrap(),
        );
        bytes.extend_from_slice(
            &encode_mutation_record(
                2,
                &Mutation::Delete {
                    key: b"ant".to_vec(),
                },
            )
            .unwrap(),
        );
        fs::write(path, bytes).unwrap();

        let recovered = recover_wal(&layout, 1_073_741_824).unwrap();
        assert_eq!(recovered.last_committed_seqno, 2);
        assert_eq!(recovered.mutations.len(), 2);
        assert_eq!(
            recovered.mutations[0].mutation,
            Mutation::Put {
                key: b"ant".to_vec(),
                value: b"a".to_vec(),
            }
        );
        assert_eq!(
            recovered.mutations[1].mutation,
            Mutation::Delete {
                key: b"ant".to_vec(),
            }
        );
        assert!(recovered.active_segment.is_some());
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
