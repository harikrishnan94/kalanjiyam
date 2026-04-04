//! Durable byte encoders, decoders, and canonical path helpers for pezhai.

use std::path::{Path, PathBuf};

use crate::error::KalanjiyamError;
use crate::pezhai::types::{
    Bound, FileMeta, LogicalShardEntry, NONE_U64, RecordKind, WalRecordType, validate_key,
    validate_logical_shards,
};

const CURRENT_MAGIC: &[u8; 8] = b"KJCURR1\0";
const WAL_HEADER_MAGIC: &[u8; 8] = b"KJWALSE1";
const WAL_FOOTER_MAGIC: &[u8; 8] = b"KJWALF1\0";
const WAL_RECORD_MAGIC: &[u8; 4] = b"KJWR";

/// Parsed contents of the fixed-size `CURRENT` file.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CurrentFile {
    /// Referenced checkpoint generation.
    pub checkpoint_generation: u64,
    /// Highest seqno fully represented by the checkpoint.
    pub checkpoint_max_seqno: u64,
    /// Shared-data generation represented by the checkpoint.
    pub checkpoint_data_generation: u64,
}

/// Parsed fixed-size WAL segment header.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WalSegmentHeader {
    /// First seqno in the segment or `NONE_U64` for a new empty active segment.
    pub first_seqno_or_none: u64,
    /// Configured segment size.
    pub segment_bytes: u64,
}

/// Parsed fixed-size WAL segment footer.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct WalSegmentFooter {
    /// First seqno in the closed segment.
    pub first_seqno: u64,
    /// Last seqno in the closed segment.
    pub last_seqno: u64,
    /// Count of durable records in the segment.
    pub record_count: u64,
    /// Bytes used between the segment header and footer.
    pub payload_bytes_used: u64,
}

/// Shared payload for one flush publication record.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FlushPublishPayload {
    /// Generation expected at publish time.
    pub data_generation_expected: u64,
    /// First seqno of the flushed memtable.
    pub source_first_seqno: u64,
    /// Last seqno of the flushed memtable.
    pub source_last_seqno: u64,
    /// Record count of the flushed memtable.
    pub source_record_count: u64,
    /// Output file metadata, exactly one file in v1.
    pub output_file_metas: Vec<FileMeta>,
}

/// Shared payload for one compaction publication record.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CompactPublishPayload {
    /// Generation expected at publish time.
    pub data_generation_expected: u64,
    /// Exact input file ids being replaced.
    pub input_file_ids: Vec<u64>,
    /// Replacement output files.
    pub output_file_metas: Vec<FileMeta>,
}

/// Shared payload for one logical-shard installation record.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LogicalShardInstallPayload {
    /// Exact current entries expected at install time.
    pub source_entries: Vec<LogicalShardEntry>,
    /// Full replacement entry set.
    pub output_entries: Vec<LogicalShardEntry>,
}

/// One parsed or to-be-encoded WAL record.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WalRecord {
    /// Global visibility seqno.
    pub seqno: u64,
    /// Logical payload.
    pub payload: WalRecordPayload,
}

/// One parsed or to-be-encoded WAL payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WalRecordPayload {
    /// User value insertion or replacement.
    Put { key: Vec<u8>, value: Vec<u8> },
    /// User tombstone.
    Delete { key: Vec<u8> },
    /// Flush publication.
    FlushPublish(FlushPublishPayload),
    /// Compaction publication.
    CompactPublish(CompactPublishPayload),
    /// Latest-only logical-shard map update.
    LogicalShardInstall(LogicalShardInstallPayload),
}

/// Returns the canonical relative path for one open or closed WAL segment.
///
/// # Complexity
/// Time: O(1)
/// Space: O(1)
#[must_use]
pub fn wal_relative_path(first_seqno: u64, last_seqno: Option<u64>) -> PathBuf {
    match last_seqno {
        Some(last_seqno) => {
            PathBuf::from(format!("wal/wal-{first_seqno:020}-{last_seqno:020}.log"))
        }
        None => PathBuf::from(format!("wal/wal-{first_seqno:020}-open.log")),
    }
}

/// Returns the canonical relative path for one shared data file.
///
/// # Complexity
/// Time: O(1)
/// Space: O(1)
#[must_use]
pub fn data_relative_path(file_id: u64) -> PathBuf {
    PathBuf::from(format!("data/{file_id:020}.kjm"))
}

/// Returns the canonical relative path for one metadata checkpoint file.
///
/// # Complexity
/// Time: O(1)
/// Space: O(1)
#[must_use]
pub fn metadata_relative_path(checkpoint_generation: u64) -> PathBuf {
    PathBuf::from(format!("meta/meta-{checkpoint_generation:020}.kjm"))
}

/// Returns the canonical relative path for the store-root `CURRENT` file.
///
/// # Complexity
/// Time: O(1)
/// Space: O(1)
#[must_use]
pub fn current_relative_path() -> PathBuf {
    PathBuf::from("CURRENT")
}

/// Resolves one canonical relative durable path under the store root.
///
/// # Complexity
/// Time: O(|store_dir| + |relative_path|)
/// Space: O(|store_dir| + |relative_path|) for the returned `PathBuf`.
#[must_use]
pub fn resolve_store_path(store_dir: &Path, relative_path: &Path) -> PathBuf {
    store_dir.join(relative_path)
}

/// Returns the CRC32C checksum for one byte slice.
///
/// # Complexity
/// Time: O(n)
/// Space: O(1)
#[must_use]
pub fn crc32c_bytes(bytes: &[u8]) -> u32 {
    crc32c::crc32c(bytes)
}

/// Aligns one byte count upward to the next multiple of eight.
///
/// # Complexity
/// Time: O(1)
/// Space: O(1)
#[must_use]
pub fn align_to_8(value: usize) -> usize {
    (value + 7) & !7
}

/// Encodes the fixed-size 72-byte `CURRENT` file.
///
/// # Examples
///
/// ```
/// use kalanjiyam::pezhai::codec::{CurrentFile, decode_current, encode_current};
///
/// let current = CurrentFile {
///     checkpoint_generation: 9,
///     checkpoint_max_seqno: 140,
///     checkpoint_data_generation: 12,
/// };
///
/// let bytes = encode_current(&current);
/// assert_eq!(decode_current(&bytes).unwrap(), current);
/// ```
///
/// # Complexity
/// Time: O(1)
/// Space: O(1)
pub fn encode_current(current: &CurrentFile) -> [u8; 72] {
    let mut bytes = [0_u8; 72];
    bytes[0..8].copy_from_slice(CURRENT_MAGIC);
    bytes[8..10].copy_from_slice(&1_u16.to_le_bytes());
    bytes[12..16].copy_from_slice(&72_u32.to_le_bytes());
    bytes[16..24].copy_from_slice(&current.checkpoint_generation.to_le_bytes());
    bytes[24..32].copy_from_slice(&current.checkpoint_max_seqno.to_le_bytes());
    bytes[32..40].copy_from_slice(&current.checkpoint_data_generation.to_le_bytes());
    let crc = crc32c_bytes(&zeroed_array_range(bytes, 52, 4));
    bytes[52..56].copy_from_slice(&crc.to_le_bytes());
    bytes
}

/// Decodes the fixed-size 72-byte `CURRENT` file.
///
/// # Complexity
/// Time: O(1)
/// Space: O(1)
pub fn decode_current(bytes: &[u8]) -> Result<CurrentFile, KalanjiyamError> {
    if bytes.len() != 72 {
        return Err(KalanjiyamError::Corruption(
            "CURRENT must be exactly 72 bytes".to_string(),
        ));
    }
    if &bytes[0..8] != CURRENT_MAGIC {
        return Err(KalanjiyamError::Corruption(
            "CURRENT magic mismatch".to_string(),
        ));
    }
    if u16::from_le_bytes(bytes[8..10].try_into().expect("slice has exact length")) != 1 {
        return Err(KalanjiyamError::Corruption(
            "CURRENT format_major mismatch".to_string(),
        ));
    }
    if bytes[10..12] != [0_u8; 2]
        || bytes[40..48] != [0_u8; 8]
        || bytes[48..52] != [0_u8; 4]
        || bytes[56..72] != [0_u8; 16]
    {
        return Err(KalanjiyamError::Corruption(
            "CURRENT reserved bytes must be zero".to_string(),
        ));
    }
    if u32::from_le_bytes(bytes[12..16].try_into().expect("slice has exact length")) != 72 {
        return Err(KalanjiyamError::Corruption(
            "CURRENT file_bytes mismatch".to_string(),
        ));
    }
    let expected_crc =
        u32::from_le_bytes(bytes[52..56].try_into().expect("slice has exact length"));
    let actual_crc = crc32c_bytes(&zeroed_vec_range(bytes.to_vec(), 52, 4));
    if expected_crc != actual_crc {
        return Err(KalanjiyamError::Checksum(
            "CURRENT checksum mismatch".to_string(),
        ));
    }
    Ok(CurrentFile {
        checkpoint_generation: u64::from_le_bytes(
            bytes[16..24].try_into().expect("slice has exact length"),
        ),
        checkpoint_max_seqno: u64::from_le_bytes(
            bytes[24..32].try_into().expect("slice has exact length"),
        ),
        checkpoint_data_generation: u64::from_le_bytes(
            bytes[32..40].try_into().expect("slice has exact length"),
        ),
    })
}

/// Encodes a fixed 128-byte WAL segment header.
///
/// # Complexity
/// Time: O(1)
/// Space: O(1)
pub fn encode_wal_segment_header(header: &WalSegmentHeader) -> [u8; 128] {
    let mut bytes = [0_u8; 128];
    bytes[0..8].copy_from_slice(WAL_HEADER_MAGIC);
    bytes[8..10].copy_from_slice(&1_u16.to_le_bytes());
    bytes[12..16].copy_from_slice(&128_u32.to_le_bytes());
    bytes[16..24].copy_from_slice(&header.first_seqno_or_none.to_le_bytes());
    bytes[24..32].copy_from_slice(&header.segment_bytes.to_le_bytes());
    let crc = crc32c_bytes(&zeroed_array_range(bytes, 32, 4));
    bytes[32..36].copy_from_slice(&crc.to_le_bytes());
    bytes
}

/// Decodes a fixed 128-byte WAL segment header.
///
/// # Complexity
/// Time: O(1)
/// Space: O(1)
pub fn decode_wal_segment_header(bytes: &[u8]) -> Result<WalSegmentHeader, KalanjiyamError> {
    if bytes.len() != 128 {
        return Err(KalanjiyamError::Corruption(
            "WAL segment header must be 128 bytes".to_string(),
        ));
    }
    if &bytes[0..8] != WAL_HEADER_MAGIC {
        return Err(KalanjiyamError::Corruption(
            "WAL header magic mismatch".to_string(),
        ));
    }
    if bytes[10..12] != [0_u8; 2] || bytes[36..128] != [0_u8; 92] {
        return Err(KalanjiyamError::Corruption(
            "WAL header reserved bytes must be zero".to_string(),
        ));
    }
    let expected_crc =
        u32::from_le_bytes(bytes[32..36].try_into().expect("slice has exact length"));
    let actual_crc = crc32c_bytes(&zeroed_vec_range(bytes.to_vec(), 32, 4));
    if expected_crc != actual_crc {
        return Err(KalanjiyamError::Checksum(
            "WAL header checksum mismatch".to_string(),
        ));
    }
    Ok(WalSegmentHeader {
        first_seqno_or_none: u64::from_le_bytes(
            bytes[16..24].try_into().expect("slice has exact length"),
        ),
        segment_bytes: u64::from_le_bytes(
            bytes[24..32].try_into().expect("slice has exact length"),
        ),
    })
}

/// Encodes a fixed 128-byte WAL segment footer.
///
/// # Complexity
/// Time: O(1)
/// Space: O(1)
pub fn encode_wal_segment_footer(footer: &WalSegmentFooter) -> [u8; 128] {
    let mut bytes = [0_u8; 128];
    bytes[0..8].copy_from_slice(WAL_FOOTER_MAGIC);
    bytes[8..16].copy_from_slice(&footer.first_seqno.to_le_bytes());
    bytes[16..24].copy_from_slice(&footer.last_seqno.to_le_bytes());
    bytes[24..32].copy_from_slice(&footer.record_count.to_le_bytes());
    bytes[32..40].copy_from_slice(&footer.payload_bytes_used.to_le_bytes());
    let crc = crc32c_bytes(&zeroed_array_range(bytes, 40, 4));
    bytes[40..44].copy_from_slice(&crc.to_le_bytes());
    bytes
}

/// Encodes one aligned WAL record.
///
/// # Complexity
/// Time: O(p), where `p` is the encoded payload size before 8-byte padding.
/// Space: O(p) for the returned record buffer.
pub fn encode_wal_record(record: &WalRecord) -> Result<Vec<u8>, KalanjiyamError> {
    let (record_type, payload) = encode_wal_payload(&record.payload)?;
    let total_bytes = align_to_8(64 + payload.len());
    let payload_crc = crc32c_bytes(&payload);

    let mut bytes = vec![0_u8; total_bytes];
    bytes[0..4].copy_from_slice(WAL_RECORD_MAGIC);
    bytes[4] = record_type as u8;
    bytes[8..12].copy_from_slice(&(total_bytes as u32).to_le_bytes());
    bytes[12..16].copy_from_slice(&(payload.len() as u32).to_le_bytes());
    bytes[16..24].copy_from_slice(&record.seqno.to_le_bytes());
    bytes[28..32].copy_from_slice(&payload_crc.to_le_bytes());
    bytes[64..64 + payload.len()].copy_from_slice(&payload);
    let header_crc = crc32c_bytes(&zeroed_vec_range(bytes[0..64].to_vec(), 24, 4));
    bytes[24..28].copy_from_slice(&header_crc.to_le_bytes());
    Ok(bytes)
}

/// Decodes one aligned WAL record.
///
/// # Complexity
/// Time: O(p), where `p` is the encoded payload size before 8-byte padding.
/// Space: O(p) for the decoded key/value or publication payload owned by the
/// returned `WalRecord`.
pub fn decode_wal_record(bytes: &[u8]) -> Result<WalRecord, KalanjiyamError> {
    if bytes.len() < 64 || !bytes.len().is_multiple_of(8) {
        return Err(KalanjiyamError::Corruption(
            "WAL record must be at least 64 bytes and 8-byte aligned".to_string(),
        ));
    }
    if &bytes[0..4] != WAL_RECORD_MAGIC {
        return Err(KalanjiyamError::Corruption(
            "WAL record magic mismatch".to_string(),
        ));
    }
    if bytes[5..8] != [0_u8; 3] || bytes[32..64] != [0_u8; 32] {
        return Err(KalanjiyamError::Corruption(
            "WAL record reserved bytes must be zero".to_string(),
        ));
    }
    let total_bytes =
        u32::from_le_bytes(bytes[8..12].try_into().expect("slice has exact length")) as usize;
    let payload_bytes =
        u32::from_le_bytes(bytes[12..16].try_into().expect("slice has exact length")) as usize;
    if total_bytes != bytes.len() || total_bytes < 64 + payload_bytes {
        return Err(KalanjiyamError::Corruption(
            "WAL record total_bytes is invalid".to_string(),
        ));
    }
    if bytes[64 + payload_bytes..total_bytes]
        .iter()
        .any(|byte| *byte != 0)
    {
        return Err(KalanjiyamError::Corruption(
            "WAL padding bytes must be zero".to_string(),
        ));
    }
    let expected_header_crc =
        u32::from_le_bytes(bytes[24..28].try_into().expect("slice has exact length"));
    let actual_header_crc = crc32c_bytes(&zeroed_vec_range(bytes[0..64].to_vec(), 24, 4));
    if expected_header_crc != actual_header_crc {
        return Err(KalanjiyamError::Checksum(
            "WAL header checksum mismatch".to_string(),
        ));
    }
    let expected_payload_crc =
        u32::from_le_bytes(bytes[28..32].try_into().expect("slice has exact length"));
    let payload = &bytes[64..64 + payload_bytes];
    if expected_payload_crc != crc32c_bytes(payload) {
        return Err(KalanjiyamError::Checksum(
            "WAL payload checksum mismatch".to_string(),
        ));
    }
    let record_type = WalRecordType::from_u8(bytes[4])?;
    let seqno = u64::from_le_bytes(bytes[16..24].try_into().expect("slice has exact length"));
    let payload = decode_wal_payload(record_type, payload)?;
    Ok(WalRecord { seqno, payload })
}

fn encode_wal_payload(
    payload: &WalRecordPayload,
) -> Result<(WalRecordType, Vec<u8>), KalanjiyamError> {
    match payload {
        WalRecordPayload::Put { key, value } => {
            let mut bytes = Vec::with_capacity(8 + key.len() + value.len());
            bytes.extend_from_slice(
                &(u16::try_from(key.len()).map_err(|_| {
                    KalanjiyamError::InvalidArgument("Put key exceeds u16".to_string())
                })?)
                .to_le_bytes(),
            );
            bytes.extend_from_slice(&0_u16.to_le_bytes());
            bytes.extend_from_slice(
                &(u32::try_from(value.len()).map_err(|_| {
                    KalanjiyamError::InvalidArgument("Put value exceeds u32".to_string())
                })?)
                .to_le_bytes(),
            );
            bytes.extend_from_slice(key);
            bytes.extend_from_slice(value);
            Ok((WalRecordType::Put, bytes))
        }
        WalRecordPayload::Delete { key } => {
            let mut bytes = Vec::with_capacity(8 + key.len());
            bytes.extend_from_slice(
                &(u16::try_from(key.len()).map_err(|_| {
                    KalanjiyamError::InvalidArgument("Delete key exceeds u16".to_string())
                })?)
                .to_le_bytes(),
            );
            bytes.extend_from_slice(&[0_u8; 6]);
            bytes.extend_from_slice(key);
            Ok((WalRecordType::Delete, bytes))
        }
        WalRecordPayload::FlushPublish(payload) => {
            let metas = encode_file_meta_vec(&payload.output_file_metas)?;
            let mut bytes = Vec::with_capacity(40 + metas.len());
            bytes.extend_from_slice(&payload.data_generation_expected.to_le_bytes());
            bytes.extend_from_slice(&payload.source_first_seqno.to_le_bytes());
            bytes.extend_from_slice(&payload.source_last_seqno.to_le_bytes());
            bytes.extend_from_slice(&payload.source_record_count.to_le_bytes());
            bytes.extend_from_slice(
                &(u32::try_from(payload.output_file_metas.len()).map_err(|_| {
                    KalanjiyamError::InvalidArgument("too many flush output file metas".to_string())
                })?)
                .to_le_bytes(),
            );
            bytes.extend_from_slice(&40_u32.to_le_bytes());
            bytes.extend_from_slice(&metas);
            Ok((WalRecordType::FlushPublish, bytes))
        }
        WalRecordPayload::CompactPublish(payload) => {
            let metas = encode_file_meta_vec(&payload.output_file_metas)?;
            let mut bytes = Vec::with_capacity(24 + 8 * payload.input_file_ids.len() + metas.len());
            bytes.extend_from_slice(&payload.data_generation_expected.to_le_bytes());
            bytes.extend_from_slice(
                &(u32::try_from(payload.input_file_ids.len()).map_err(|_| {
                    KalanjiyamError::InvalidArgument("too many compaction inputs".to_string())
                })?)
                .to_le_bytes(),
            );
            bytes.extend_from_slice(
                &(u32::try_from(payload.output_file_metas.len()).map_err(|_| {
                    KalanjiyamError::InvalidArgument("too many compaction outputs".to_string())
                })?)
                .to_le_bytes(),
            );
            bytes.extend_from_slice(&24_u32.to_le_bytes());
            bytes.extend_from_slice(&0_u32.to_le_bytes());
            for file_id in &payload.input_file_ids {
                bytes.extend_from_slice(&file_id.to_le_bytes());
            }
            bytes.extend_from_slice(&metas);
            Ok((WalRecordType::CompactPublish, bytes))
        }
        WalRecordPayload::LogicalShardInstall(payload) => {
            if !(1..=2).contains(&payload.source_entries.len()) {
                return Err(KalanjiyamError::InvalidArgument(
                    "LogicalShardInstall source entries must contain 1 or 2 entries".to_string(),
                ));
            }
            if !(1..=2).contains(&payload.output_entries.len()) {
                return Err(KalanjiyamError::InvalidArgument(
                    "LogicalShardInstall output entries must contain 1 or 2 entries".to_string(),
                ));
            }
            let source_bytes = encode_logical_shard_vec(&payload.source_entries)?;
            let output_bytes = encode_logical_shard_vec(&payload.output_entries)?;
            let mut bytes = Vec::with_capacity(8 + source_bytes.len() + output_bytes.len());
            bytes.extend_from_slice(
                &(u16::try_from(payload.source_entries.len()).map_err(|_| {
                    KalanjiyamError::InvalidArgument("too many logical source entries".to_string())
                })?)
                .to_le_bytes(),
            );
            bytes.extend_from_slice(
                &(u16::try_from(payload.output_entries.len()).map_err(|_| {
                    KalanjiyamError::InvalidArgument("too many logical output entries".to_string())
                })?)
                .to_le_bytes(),
            );
            bytes.extend_from_slice(&8_u32.to_le_bytes());
            bytes.extend_from_slice(&source_bytes);
            bytes.extend_from_slice(&output_bytes);
            Ok((WalRecordType::LogicalShardInstall, bytes))
        }
    }
}

fn decode_wal_payload(
    record_type: WalRecordType,
    bytes: &[u8],
) -> Result<WalRecordPayload, KalanjiyamError> {
    match record_type {
        WalRecordType::Put => {
            if bytes.len() < 8 {
                return Err(KalanjiyamError::Corruption(
                    "Put payload too short".to_string(),
                ));
            }
            let key_len =
                u16::from_le_bytes(bytes[0..2].try_into().expect("slice has exact length"))
                    as usize;
            let value_len =
                u32::from_le_bytes(bytes[4..8].try_into().expect("slice has exact length"))
                    as usize;
            if bytes[2..4] != [0_u8; 2] || bytes.len() != 8 + key_len + value_len {
                return Err(KalanjiyamError::Corruption(
                    "Put payload layout mismatch".to_string(),
                ));
            }
            Ok(WalRecordPayload::Put {
                key: bytes[8..8 + key_len].to_vec(),
                value: bytes[8 + key_len..].to_vec(),
            })
        }
        WalRecordType::Delete => {
            if bytes.len() < 8 {
                return Err(KalanjiyamError::Corruption(
                    "Delete payload too short".to_string(),
                ));
            }
            let key_len =
                u16::from_le_bytes(bytes[0..2].try_into().expect("slice has exact length"))
                    as usize;
            if bytes[2..8] != [0_u8; 6] || bytes.len() != 8 + key_len {
                return Err(KalanjiyamError::Corruption(
                    "Delete payload layout mismatch".to_string(),
                ));
            }
            Ok(WalRecordPayload::Delete {
                key: bytes[8..].to_vec(),
            })
        }
        WalRecordType::FlushPublish => decode_flush_publish_payload(bytes),
        WalRecordType::CompactPublish => decode_compact_publish_payload(bytes),
        WalRecordType::LogicalShardInstall => decode_logical_install_payload(bytes),
    }
}

fn decode_flush_publish_payload(bytes: &[u8]) -> Result<WalRecordPayload, KalanjiyamError> {
    if bytes.len() < 40 {
        return Err(KalanjiyamError::Corruption(
            "FlushPublish payload too short".to_string(),
        ));
    }
    let fixed_bytes = u32::from_le_bytes(bytes[36..40].try_into().expect("slice has exact length"));
    if fixed_bytes != 40 {
        return Err(KalanjiyamError::Corruption(
            "FlushPublish fixed_bytes mismatch".to_string(),
        ));
    }
    let output_count =
        u32::from_le_bytes(bytes[32..36].try_into().expect("slice has exact length")) as usize;
    let output_file_metas = decode_file_meta_vec(&bytes[40..], output_count)?;
    Ok(WalRecordPayload::FlushPublish(FlushPublishPayload {
        data_generation_expected: u64::from_le_bytes(
            bytes[0..8].try_into().expect("slice has exact length"),
        ),
        source_first_seqno: u64::from_le_bytes(
            bytes[8..16].try_into().expect("slice has exact length"),
        ),
        source_last_seqno: u64::from_le_bytes(
            bytes[16..24].try_into().expect("slice has exact length"),
        ),
        source_record_count: u64::from_le_bytes(
            bytes[24..32].try_into().expect("slice has exact length"),
        ),
        output_file_metas,
    }))
}

fn decode_compact_publish_payload(bytes: &[u8]) -> Result<WalRecordPayload, KalanjiyamError> {
    if bytes.len() < 24 {
        return Err(KalanjiyamError::Corruption(
            "CompactPublish payload too short".to_string(),
        ));
    }
    let input_count =
        u32::from_le_bytes(bytes[8..12].try_into().expect("slice has exact length")) as usize;
    let output_count =
        u32::from_le_bytes(bytes[12..16].try_into().expect("slice has exact length")) as usize;
    let fixed_bytes = u32::from_le_bytes(bytes[16..20].try_into().expect("slice has exact length"));
    if fixed_bytes != 24 || bytes[20..24] != [0_u8; 4] {
        return Err(KalanjiyamError::Corruption(
            "CompactPublish fixed bytes mismatch".to_string(),
        ));
    }
    let inputs_end = 24 + input_count * 8;
    if bytes.len() < inputs_end {
        return Err(KalanjiyamError::Corruption(
            "CompactPublish input ids truncated".to_string(),
        ));
    }
    let mut input_file_ids = Vec::with_capacity(input_count);
    for chunk in bytes[24..inputs_end].chunks_exact(8) {
        input_file_ids.push(u64::from_le_bytes(
            chunk.try_into().expect("slice has exact length"),
        ));
    }
    let output_file_metas = decode_file_meta_vec(&bytes[inputs_end..], output_count)?;
    Ok(WalRecordPayload::CompactPublish(CompactPublishPayload {
        data_generation_expected: u64::from_le_bytes(
            bytes[0..8].try_into().expect("slice has exact length"),
        ),
        input_file_ids,
        output_file_metas,
    }))
}

fn decode_logical_install_payload(bytes: &[u8]) -> Result<WalRecordPayload, KalanjiyamError> {
    if bytes.len() < 8 {
        return Err(KalanjiyamError::Corruption(
            "LogicalShardInstall payload too short".to_string(),
        ));
    }
    let source_count =
        u16::from_le_bytes(bytes[0..2].try_into().expect("slice has exact length")) as usize;
    let output_count =
        u16::from_le_bytes(bytes[2..4].try_into().expect("slice has exact length")) as usize;
    if !(1..=2).contains(&source_count) || !(1..=2).contains(&output_count) {
        return Err(KalanjiyamError::Corruption(
            "LogicalShardInstall entry counts must both be 1 or 2".to_string(),
        ));
    }
    let fixed_bytes = u32::from_le_bytes(bytes[4..8].try_into().expect("slice has exact length"));
    if fixed_bytes != 8 {
        return Err(KalanjiyamError::Corruption(
            "LogicalShardInstall fixed_bytes mismatch".to_string(),
        ));
    }
    let (source_entries, consumed) = decode_logical_shard_vec(&bytes[8..], source_count)?;
    let (output_entries, output_consumed) =
        decode_logical_shard_vec(&bytes[8 + consumed..], output_count)?;
    if 8 + consumed + output_consumed != bytes.len() {
        return Err(KalanjiyamError::Corruption(
            "LogicalShardInstall payload has trailing bytes".to_string(),
        ));
    }
    validate_logical_shards(&source_entries)?;
    validate_logical_shards(&output_entries)?;
    if source_entries == output_entries {
        return Err(KalanjiyamError::Corruption(
            "LogicalShardInstall payload must not be a no-op".to_string(),
        ));
    }
    if source_entries.first().map(|entry| &entry.start_bound)
        != output_entries.first().map(|entry| &entry.start_bound)
        || source_entries.last().map(|entry| &entry.end_bound)
            != output_entries.last().map(|entry| &entry.end_bound)
    {
        return Err(KalanjiyamError::Corruption(
            "LogicalShardInstall payload must preserve outer bounds".to_string(),
        ));
    }
    Ok(WalRecordPayload::LogicalShardInstall(
        LogicalShardInstallPayload {
            source_entries,
            output_entries,
        },
    ))
}

fn encode_file_meta_vec(metas: &[FileMeta]) -> Result<Vec<u8>, KalanjiyamError> {
    let mut bytes = Vec::new();
    for meta in metas {
        bytes.extend_from_slice(&encode_file_meta_wire(meta)?);
    }
    Ok(bytes)
}

fn decode_file_meta_vec(bytes: &[u8], count: usize) -> Result<Vec<FileMeta>, KalanjiyamError> {
    let mut offset = 0;
    let mut metas = Vec::with_capacity(count);
    for _ in 0..count {
        let (meta, consumed) = decode_file_meta_wire(&bytes[offset..])?;
        metas.push(meta);
        offset += consumed;
    }
    if offset != bytes.len() {
        return Err(KalanjiyamError::Corruption(
            "file meta payload has trailing bytes".to_string(),
        ));
    }
    Ok(metas)
}

/// Encodes one canonical `FileMetaWire` value used by WAL and checkpoint payloads.
pub(crate) fn encode_file_meta_wire(meta: &FileMeta) -> Result<Vec<u8>, KalanjiyamError> {
    if meta.file_id == 0 {
        return Err(KalanjiyamError::InvalidArgument(
            "file_id must be at least 1".to_string(),
        ));
    }
    validate_key(&meta.min_user_key)?;
    validate_key(&meta.max_user_key)?;
    if meta.min_user_key > meta.max_user_key {
        return Err(KalanjiyamError::InvalidArgument(
            "min_user_key must be <= max_user_key".to_string(),
        ));
    }
    let min_len = u16::try_from(meta.min_user_key.len())
        .map_err(|_| KalanjiyamError::InvalidArgument("min_user_key exceeds u16".to_string()))?;
    let max_len = u16::try_from(meta.max_user_key.len())
        .map_err(|_| KalanjiyamError::InvalidArgument("max_user_key exceeds u16".to_string()))?;
    let mut bytes = Vec::with_capacity(60 + meta.min_user_key.len() + meta.max_user_key.len());
    bytes.extend_from_slice(&meta.file_id.to_le_bytes());
    bytes.extend_from_slice(&meta.min_seqno.to_le_bytes());
    bytes.extend_from_slice(&meta.max_seqno.to_le_bytes());
    bytes.extend_from_slice(&meta.entry_count.to_le_bytes());
    bytes.extend_from_slice(&meta.logical_bytes.to_le_bytes());
    bytes.extend_from_slice(&meta.physical_bytes.to_le_bytes());
    bytes.extend_from_slice(&meta.level_no.to_le_bytes());
    bytes.extend_from_slice(&min_len.to_le_bytes());
    bytes.extend_from_slice(&max_len.to_le_bytes());
    bytes.extend_from_slice(&0_u16.to_le_bytes());
    bytes.extend_from_slice(&60_u32.to_le_bytes());
    bytes.extend_from_slice(&meta.min_user_key);
    bytes.extend_from_slice(&meta.max_user_key);
    Ok(bytes)
}

/// Decodes one canonical `FileMetaWire` value used by WAL and checkpoint payloads.
pub(crate) fn decode_file_meta_wire(bytes: &[u8]) -> Result<(FileMeta, usize), KalanjiyamError> {
    if bytes.len() < 60 {
        return Err(KalanjiyamError::Corruption(
            "FileMeta payload too short".to_string(),
        ));
    }
    let min_len =
        u16::from_le_bytes(bytes[50..52].try_into().expect("slice has exact length")) as usize;
    let max_len =
        u16::from_le_bytes(bytes[52..54].try_into().expect("slice has exact length")) as usize;
    if bytes[54..56] != [0_u8; 2] {
        return Err(KalanjiyamError::Corruption(
            "FileMeta reserved bytes must be zero".to_string(),
        ));
    }
    if u32::from_le_bytes(bytes[56..60].try_into().expect("slice has exact length")) != 60 {
        return Err(KalanjiyamError::Corruption(
            "FileMeta fixed_bytes mismatch".to_string(),
        ));
    }
    let total = 60 + min_len + max_len;
    if bytes.len() < total {
        return Err(KalanjiyamError::Corruption(
            "FileMeta payload truncated".to_string(),
        ));
    }
    let meta = FileMeta {
        file_id: u64::from_le_bytes(bytes[0..8].try_into().expect("slice has exact length")),
        min_seqno: u64::from_le_bytes(bytes[8..16].try_into().expect("slice has exact length")),
        max_seqno: u64::from_le_bytes(bytes[16..24].try_into().expect("slice has exact length")),
        entry_count: u64::from_le_bytes(bytes[24..32].try_into().expect("slice has exact length")),
        logical_bytes: u64::from_le_bytes(
            bytes[32..40].try_into().expect("slice has exact length"),
        ),
        physical_bytes: u64::from_le_bytes(
            bytes[40..48].try_into().expect("slice has exact length"),
        ),
        level_no: u16::from_le_bytes(bytes[48..50].try_into().expect("slice has exact length")),
        min_user_key: bytes[60..60 + min_len].to_vec(),
        max_user_key: bytes[60 + min_len..total].to_vec(),
    };
    if meta.file_id == 0 {
        return Err(KalanjiyamError::Corruption(
            "FileMeta file_id must be at least 1".to_string(),
        ));
    }
    validate_key(&meta.min_user_key)
        .map_err(|_| KalanjiyamError::Corruption("FileMeta min_user_key is invalid".to_string()))?;
    validate_key(&meta.max_user_key)
        .map_err(|_| KalanjiyamError::Corruption("FileMeta max_user_key is invalid".to_string()))?;
    if meta.min_user_key > meta.max_user_key {
        return Err(KalanjiyamError::Corruption(
            "FileMeta min_user_key must be <= max_user_key".to_string(),
        ));
    }
    Ok((meta, total))
}

fn encode_logical_shard_vec(entries: &[LogicalShardEntry]) -> Result<Vec<u8>, KalanjiyamError> {
    let mut bytes = Vec::new();
    for entry in entries {
        bytes.extend_from_slice(&encode_logical_shard_entry_wire(entry)?);
    }
    Ok(bytes)
}

fn decode_logical_shard_vec(
    bytes: &[u8],
    count: usize,
) -> Result<(Vec<LogicalShardEntry>, usize), KalanjiyamError> {
    let mut offset = 0;
    let mut entries = Vec::with_capacity(count);
    for _ in 0..count {
        let (entry, consumed) = decode_logical_shard_entry_wire(&bytes[offset..])?;
        entries.push(entry);
        offset += consumed;
    }
    Ok((entries, offset))
}

/// Encodes one canonical `LogicalShardEntryWire` value used by WAL and checkpoints.
pub(crate) fn encode_logical_shard_entry_wire(
    entry: &LogicalShardEntry,
) -> Result<Vec<u8>, KalanjiyamError> {
    entry.key_range().validate()?;
    let start = encode_bound_wire(&entry.start_bound)?;
    let end = encode_bound_wire(&entry.end_bound)?;
    let mut bytes = Vec::with_capacity(24 + start.len() + end.len());
    bytes.extend_from_slice(&entry.live_size_bytes.to_le_bytes());
    bytes.extend_from_slice(
        &(u32::try_from(start.len()).map_err(|_| {
            KalanjiyamError::InvalidArgument("start bound encoding too large".to_string())
        })?)
        .to_le_bytes(),
    );
    bytes.extend_from_slice(
        &(u32::try_from(end.len()).map_err(|_| {
            KalanjiyamError::InvalidArgument("end bound encoding too large".to_string())
        })?)
        .to_le_bytes(),
    );
    bytes.extend_from_slice(&24_u32.to_le_bytes());
    bytes.extend_from_slice(&0_u32.to_le_bytes());
    bytes.extend_from_slice(&start);
    bytes.extend_from_slice(&end);
    Ok(bytes)
}

/// Decodes one canonical `LogicalShardEntryWire` value used by WAL and checkpoints.
pub(crate) fn decode_logical_shard_entry_wire(
    bytes: &[u8],
) -> Result<(LogicalShardEntry, usize), KalanjiyamError> {
    if bytes.len() < 24 {
        return Err(KalanjiyamError::Corruption(
            "LogicalShardEntry payload too short".to_string(),
        ));
    }
    let start_len =
        u32::from_le_bytes(bytes[8..12].try_into().expect("slice has exact length")) as usize;
    let end_len =
        u32::from_le_bytes(bytes[12..16].try_into().expect("slice has exact length")) as usize;
    if u32::from_le_bytes(bytes[16..20].try_into().expect("slice has exact length")) != 24 {
        return Err(KalanjiyamError::Corruption(
            "LogicalShardEntry fixed_bytes mismatch".to_string(),
        ));
    }
    if bytes[20..24] != [0_u8; 4] {
        return Err(KalanjiyamError::Corruption(
            "LogicalShardEntry reserved bytes must be zero".to_string(),
        ));
    }
    let total = 24 + start_len + end_len;
    if bytes.len() < total {
        return Err(KalanjiyamError::Corruption(
            "LogicalShardEntry payload truncated".to_string(),
        ));
    }
    let entry = LogicalShardEntry {
        start_bound: decode_bound_wire(&bytes[24..24 + start_len])?,
        end_bound: decode_bound_wire(&bytes[24 + start_len..total])?,
        live_size_bytes: u64::from_le_bytes(
            bytes[0..8].try_into().expect("slice has exact length"),
        ),
    };
    entry.key_range().validate().map_err(|_| {
        KalanjiyamError::Corruption("LogicalShardEntry range must be non-empty".to_string())
    })?;
    Ok((entry, total))
}

/// Encodes one canonical `BoundWire` value used by WAL and checkpoints.
pub(crate) fn encode_bound_wire(bound: &Bound) -> Result<Vec<u8>, KalanjiyamError> {
    let mut bytes = Vec::new();
    match bound {
        Bound::Finite(key) => {
            validate_key(key)?;
            bytes.push(0);
            bytes.extend_from_slice(
                &(u16::try_from(key.len()).map_err(|_| {
                    KalanjiyamError::InvalidArgument("bound key exceeds u16".to_string())
                })?)
                .to_le_bytes(),
            );
            bytes.extend_from_slice(key);
        }
        Bound::NegInf => bytes.push(1),
        Bound::PosInf => bytes.push(2),
    }
    Ok(bytes)
}

/// Decodes one canonical `BoundWire` value used by WAL and checkpoints.
pub(crate) fn decode_bound_wire(bytes: &[u8]) -> Result<Bound, KalanjiyamError> {
    let Some(tag) = bytes.first() else {
        return Err(KalanjiyamError::Corruption("missing bound tag".to_string()));
    };
    match tag {
        0 => {
            if bytes.len() < 3 {
                return Err(KalanjiyamError::Corruption(
                    "finite bound payload too short".to_string(),
                ));
            }
            let key_len =
                u16::from_le_bytes(bytes[1..3].try_into().expect("slice has exact length"))
                    as usize;
            if bytes.len() != 3 + key_len {
                return Err(KalanjiyamError::Corruption(
                    "finite bound payload truncated".to_string(),
                ));
            }
            let key = bytes[3..].to_vec();
            validate_key(&key).map_err(|_| {
                KalanjiyamError::Corruption("finite bound key is invalid".to_string())
            })?;
            Ok(Bound::Finite(key))
        }
        1 if bytes.len() == 1 => Ok(Bound::NegInf),
        2 if bytes.len() == 1 => Ok(Bound::PosInf),
        _ => Err(KalanjiyamError::Corruption(
            "invalid bound encoding".to_string(),
        )),
    }
}

fn zeroed_array_range<const N: usize>(mut bytes: [u8; N], offset: usize, len: usize) -> [u8; N] {
    for byte in &mut bytes[offset..offset + len] {
        *byte = 0;
    }
    bytes
}

fn zeroed_vec_range(mut bytes: Vec<u8>, offset: usize, len: usize) -> Vec<u8> {
    for byte in &mut bytes[offset..offset + len] {
        *byte = 0;
    }
    bytes
}

/// Returns the first seqno value used by a brand-new open segment filename.
///
/// # Complexity
/// Time: O(1)
/// Space: O(1)
#[must_use]
pub fn empty_open_segment_first_seqno() -> u64 {
    NONE_U64
}

/// Maps one payload to its storage record kind when applicable.
///
/// # Complexity
/// Time: O(1)
/// Space: O(1)
#[must_use]
pub fn payload_record_kind(payload: &WalRecordPayload) -> Option<RecordKind> {
    match payload {
        WalRecordPayload::Put { .. } => Some(RecordKind::Put),
        WalRecordPayload::Delete { .. } => Some(RecordKind::Delete),
        WalRecordPayload::FlushPublish(_)
        | WalRecordPayload::CompactPublish(_)
        | WalRecordPayload::LogicalShardInstall(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bound_wire_uses_spec_tag_order() {
        assert_eq!(encode_bound_wire(&Bound::NegInf).unwrap(), vec![1]);
        assert_eq!(encode_bound_wire(&Bound::PosInf).unwrap(), vec![2]);
        assert_eq!(
            encode_bound_wire(&Bound::Finite(b"ant".to_vec())).unwrap(),
            vec![0, 3, 0, b'a', b'n', b't']
        );
        assert!(matches!(
            decode_bound_wire(&[0, 0, 0]),
            Err(KalanjiyamError::Corruption(_))
        ));
    }

    #[test]
    fn file_meta_wire_matches_spec_fixed_prefix() {
        let meta = FileMeta {
            file_id: 42,
            level_no: 1,
            min_user_key: b"ant".to_vec(),
            max_user_key: b"yak".to_vec(),
            min_seqno: 4,
            max_seqno: 9,
            entry_count: 3,
            logical_bytes: 18,
            physical_bytes: 4096,
        };

        let bytes = encode_file_meta_wire(&meta).unwrap();
        assert_eq!(bytes.len(), 66);
        assert_eq!(
            u64::from_le_bytes(bytes[0..8].try_into().unwrap()),
            meta.file_id
        );
        assert_eq!(
            u16::from_le_bytes(bytes[48..50].try_into().unwrap()),
            meta.level_no
        );
        assert_eq!(u32::from_le_bytes(bytes[56..60].try_into().unwrap()), 60);

        let (decoded, consumed) = decode_file_meta_wire(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, meta);
    }

    #[test]
    fn logical_shard_wire_matches_spec_fixed_prefix() {
        let entry = LogicalShardEntry {
            start_bound: Bound::NegInf,
            end_bound: Bound::Finite(b"m".to_vec()),
            live_size_bytes: 77,
        };

        let bytes = encode_logical_shard_entry_wire(&entry).unwrap();
        assert_eq!(u64::from_le_bytes(bytes[0..8].try_into().unwrap()), 77);
        assert_eq!(u32::from_le_bytes(bytes[16..20].try_into().unwrap()), 24);
        assert_eq!(bytes[20..24], [0_u8; 4]);

        let (decoded, consumed) = decode_logical_shard_entry_wire(&bytes).unwrap();
        assert_eq!(consumed, bytes.len());
        assert_eq!(decoded, entry);
    }

    #[test]
    fn logical_install_payload_rejects_oversized_entry_sets() {
        let entry = LogicalShardEntry {
            start_bound: Bound::NegInf,
            end_bound: Bound::PosInf,
            live_size_bytes: 1,
        };
        let oversized = WalRecord {
            seqno: 9,
            payload: WalRecordPayload::LogicalShardInstall(LogicalShardInstallPayload {
                source_entries: vec![entry.clone(), entry.clone(), entry.clone()],
                output_entries: vec![entry],
            }),
        };
        assert!(matches!(
            encode_wal_record(&oversized),
            Err(KalanjiyamError::InvalidArgument(_))
        ));
        assert!(matches!(
            decode_logical_install_payload(&[3, 0, 1, 0, 8, 0, 0, 0]),
            Err(KalanjiyamError::Corruption(_))
        ));
    }

    fn file_meta(file_id: u64) -> FileMeta {
        FileMeta {
            file_id,
            level_no: 0,
            min_user_key: b"ant".to_vec(),
            max_user_key: b"yak".to_vec(),
            min_seqno: 1,
            max_seqno: 9,
            entry_count: 2,
            logical_bytes: 12,
            physical_bytes: 4096,
        }
    }

    fn shard(start_bound: Bound, end_bound: Bound) -> LogicalShardEntry {
        LogicalShardEntry {
            start_bound,
            end_bound,
            live_size_bytes: 7,
        }
    }

    #[test]
    fn current_and_wal_header_decoders_reject_structural_corruption() {
        let current = encode_current(&CurrentFile {
            checkpoint_generation: 9,
            checkpoint_max_seqno: 140,
            checkpoint_data_generation: 12,
        });
        assert!(matches!(
            decode_current(&current[..71]),
            Err(KalanjiyamError::Corruption(_))
        ));

        let mut bad_major = current;
        bad_major[8..10].copy_from_slice(&2_u16.to_le_bytes());
        assert!(matches!(
            decode_current(&bad_major),
            Err(KalanjiyamError::Corruption(_))
        ));

        let mut bad_reserved = current;
        bad_reserved[10] = 1;
        assert!(matches!(
            decode_current(&bad_reserved),
            Err(KalanjiyamError::Corruption(_))
        ));

        let mut bad_size = current;
        bad_size[12..16].copy_from_slice(&71_u32.to_le_bytes());
        assert!(matches!(
            decode_current(&bad_size),
            Err(KalanjiyamError::Corruption(_))
        ));

        let mut bad_crc = current;
        bad_crc[52] ^= 1;
        assert!(matches!(
            decode_current(&bad_crc),
            Err(KalanjiyamError::Checksum(_))
        ));

        let header = encode_wal_segment_header(&WalSegmentHeader {
            first_seqno_or_none: 1,
            segment_bytes: 8192,
        });
        assert!(matches!(
            decode_wal_segment_header(&header[..127]),
            Err(KalanjiyamError::Corruption(_))
        ));

        let mut bad_reserved = header;
        bad_reserved[10] = 1;
        assert!(matches!(
            decode_wal_segment_header(&bad_reserved),
            Err(KalanjiyamError::Corruption(_))
        ));

        let mut bad_crc = header;
        bad_crc[32] ^= 1;
        assert!(matches!(
            decode_wal_segment_header(&bad_crc),
            Err(KalanjiyamError::Checksum(_))
        ));
    }

    #[test]
    fn wal_footer_and_payload_kind_helpers_cover_all_variants() {
        let footer = encode_wal_segment_footer(&WalSegmentFooter {
            first_seqno: 11,
            last_seqno: 19,
            record_count: 4,
            payload_bytes_used: 96,
        });
        assert_eq!(&footer[0..8], WAL_FOOTER_MAGIC);
        assert_eq!(u64::from_le_bytes(footer[8..16].try_into().unwrap()), 11);
        assert_eq!(u64::from_le_bytes(footer[16..24].try_into().unwrap()), 19);

        assert_eq!(
            payload_record_kind(&WalRecordPayload::Put {
                key: b"ant".to_vec(),
                value: b"value".to_vec(),
            }),
            Some(RecordKind::Put)
        );
        assert_eq!(
            payload_record_kind(&WalRecordPayload::Delete {
                key: b"ant".to_vec(),
            }),
            Some(RecordKind::Delete)
        );
        assert_eq!(
            payload_record_kind(&WalRecordPayload::FlushPublish(FlushPublishPayload {
                data_generation_expected: 1,
                source_first_seqno: 2,
                source_last_seqno: 3,
                source_record_count: 4,
                output_file_metas: vec![file_meta(1)],
            })),
            None
        );
        assert_eq!(
            payload_record_kind(&WalRecordPayload::CompactPublish(CompactPublishPayload {
                data_generation_expected: 1,
                input_file_ids: vec![1],
                output_file_metas: vec![file_meta(2)],
            })),
            None
        );
        assert_eq!(
            payload_record_kind(&WalRecordPayload::LogicalShardInstall(
                LogicalShardInstallPayload {
                    source_entries: vec![shard(Bound::NegInf, Bound::PosInf)],
                    output_entries: vec![
                        shard(Bound::NegInf, Bound::Finite(b"m".to_vec())),
                        shard(Bound::Finite(b"m".to_vec()), Bound::PosInf),
                    ],
                }
            )),
            None
        );
    }

    #[test]
    fn wal_record_decoder_rejects_corrupt_envelope_fields() {
        let record = WalRecord {
            seqno: 42,
            payload: WalRecordPayload::Put {
                key: b"ant".to_vec(),
                value: b"v".to_vec(),
            },
        };
        let bytes = encode_wal_record(&record).unwrap();
        assert!(matches!(
            decode_wal_record(&bytes[..63]),
            Err(KalanjiyamError::Corruption(_))
        ));

        let mut bad_magic = bytes.clone();
        bad_magic[0] = 0;
        assert!(matches!(
            decode_wal_record(&bad_magic),
            Err(KalanjiyamError::Corruption(_))
        ));

        let mut bad_reserved = bytes.clone();
        bad_reserved[5] = 1;
        assert!(matches!(
            decode_wal_record(&bad_reserved),
            Err(KalanjiyamError::Corruption(_))
        ));

        let mut bad_total = bytes.clone();
        bad_total[8..12].copy_from_slice(&72_u32.to_le_bytes());
        assert!(matches!(
            decode_wal_record(&bad_total),
            Err(KalanjiyamError::Corruption(_))
        ));

        let mut bad_padding = bytes.clone();
        let last = bad_padding.len() - 1;
        bad_padding[last] = 1;
        assert!(matches!(
            decode_wal_record(&bad_padding),
            Err(KalanjiyamError::Corruption(_))
        ));

        let mut bad_header_crc = bytes.clone();
        bad_header_crc[24] ^= 1;
        assert!(matches!(
            decode_wal_record(&bad_header_crc),
            Err(KalanjiyamError::Checksum(_))
        ));

        let mut bad_payload_crc = bytes;
        bad_payload_crc[28] ^= 1;
        assert!(matches!(
            decode_wal_record(&bad_payload_crc),
            Err(KalanjiyamError::Checksum(_))
        ));
    }

    #[test]
    fn publish_payload_decoders_cover_valid_and_corrupt_layouts() {
        let flush = WalRecordPayload::FlushPublish(FlushPublishPayload {
            data_generation_expected: 9,
            source_first_seqno: 10,
            source_last_seqno: 11,
            source_record_count: 2,
            output_file_metas: vec![file_meta(1)],
        });
        let (_, flush_bytes) = encode_wal_payload(&flush).unwrap();
        assert_eq!(decode_flush_publish_payload(&flush_bytes).unwrap(), flush);
        assert!(matches!(
            decode_flush_publish_payload(&flush_bytes[..39]),
            Err(KalanjiyamError::Corruption(_))
        ));

        let mut bad_flush = flush_bytes.clone();
        bad_flush[36..40].copy_from_slice(&41_u32.to_le_bytes());
        assert!(matches!(
            decode_flush_publish_payload(&bad_flush),
            Err(KalanjiyamError::Corruption(_))
        ));

        let mut trailing_flush = flush_bytes;
        trailing_flush.push(0);
        assert!(matches!(
            decode_flush_publish_payload(&trailing_flush),
            Err(KalanjiyamError::Corruption(_))
        ));

        let compact = WalRecordPayload::CompactPublish(CompactPublishPayload {
            data_generation_expected: 9,
            input_file_ids: vec![1],
            output_file_metas: vec![file_meta(2)],
        });
        let (_, compact_bytes) = encode_wal_payload(&compact).unwrap();
        assert_eq!(
            decode_compact_publish_payload(&compact_bytes).unwrap(),
            compact
        );
        assert!(matches!(
            decode_compact_publish_payload(&compact_bytes[..23]),
            Err(KalanjiyamError::Corruption(_))
        ));

        let mut bad_fixed = compact_bytes.clone();
        bad_fixed[16..20].copy_from_slice(&25_u32.to_le_bytes());
        assert!(matches!(
            decode_compact_publish_payload(&bad_fixed),
            Err(KalanjiyamError::Corruption(_))
        ));

        let mut bad_inputs = compact_bytes.clone();
        bad_inputs[8..12].copy_from_slice(&2_u32.to_le_bytes());
        assert!(matches!(
            decode_compact_publish_payload(&bad_inputs),
            Err(KalanjiyamError::Corruption(_))
        ));

        let logical = WalRecordPayload::LogicalShardInstall(LogicalShardInstallPayload {
            source_entries: vec![shard(Bound::NegInf, Bound::PosInf)],
            output_entries: vec![
                shard(Bound::NegInf, Bound::Finite(b"m".to_vec())),
                shard(Bound::Finite(b"m".to_vec()), Bound::PosInf),
            ],
        });
        let (_, logical_bytes) = encode_wal_payload(&logical).unwrap();
        assert_eq!(
            decode_logical_install_payload(&logical_bytes).unwrap(),
            logical
        );
        assert!(matches!(
            decode_logical_install_payload(&logical_bytes[..7]),
            Err(KalanjiyamError::Corruption(_))
        ));

        let mut bad_fixed = logical_bytes.clone();
        bad_fixed[4..8].copy_from_slice(&9_u32.to_le_bytes());
        assert!(matches!(
            decode_logical_install_payload(&bad_fixed),
            Err(KalanjiyamError::Corruption(_))
        ));

        let mut trailing = logical_bytes.clone();
        trailing.push(0);
        assert!(matches!(
            decode_logical_install_payload(&trailing),
            Err(KalanjiyamError::Corruption(_))
        ));

        let (_, noop_bytes) = encode_wal_payload(&WalRecordPayload::LogicalShardInstall(
            LogicalShardInstallPayload {
                source_entries: vec![shard(Bound::NegInf, Bound::PosInf)],
                output_entries: vec![shard(Bound::NegInf, Bound::PosInf)],
            },
        ))
        .unwrap();
        assert!(matches!(
            decode_logical_install_payload(&noop_bytes),
            Err(KalanjiyamError::Corruption(_))
        ));

        let (_, outer_bounds_bytes) = encode_wal_payload(&WalRecordPayload::LogicalShardInstall(
            LogicalShardInstallPayload {
                source_entries: vec![shard(Bound::NegInf, Bound::PosInf)],
                output_entries: vec![shard(Bound::Finite(b"a".to_vec()), Bound::PosInf)],
            },
        ))
        .unwrap();
        assert!(matches!(
            decode_logical_install_payload(&outer_bounds_bytes),
            Err(KalanjiyamError::Corruption(_))
        ));
    }

    #[test]
    fn file_meta_and_logical_shard_wires_reject_invalid_fields() {
        let meta = file_meta(7);
        assert!(matches!(
            encode_file_meta_wire(&FileMeta {
                file_id: 0,
                ..meta.clone()
            }),
            Err(KalanjiyamError::InvalidArgument(_))
        ));
        assert!(matches!(
            encode_file_meta_wire(&FileMeta {
                min_user_key: b"yak".to_vec(),
                max_user_key: b"ant".to_vec(),
                ..meta.clone()
            }),
            Err(KalanjiyamError::InvalidArgument(_))
        ));

        let bytes = encode_file_meta_wire(&meta).unwrap();
        assert!(matches!(
            decode_file_meta_wire(&bytes[..59]),
            Err(KalanjiyamError::Corruption(_))
        ));

        let mut bad_reserved = bytes.clone();
        bad_reserved[54] = 1;
        assert!(matches!(
            decode_file_meta_wire(&bad_reserved),
            Err(KalanjiyamError::Corruption(_))
        ));

        let mut bad_fixed = bytes.clone();
        bad_fixed[56..60].copy_from_slice(&61_u32.to_le_bytes());
        assert!(matches!(
            decode_file_meta_wire(&bad_fixed),
            Err(KalanjiyamError::Corruption(_))
        ));

        let mut truncated = bytes.clone();
        truncated.truncate(65);
        assert!(matches!(
            decode_file_meta_wire(&truncated),
            Err(KalanjiyamError::Corruption(_))
        ));

        let mut zero_id = bytes.clone();
        zero_id[0..8].copy_from_slice(&0_u64.to_le_bytes());
        assert!(matches!(
            decode_file_meta_wire(&zero_id),
            Err(KalanjiyamError::Corruption(_))
        ));

        let mut reversed_keys = bytes.clone();
        reversed_keys[60..63].copy_from_slice(b"yak");
        reversed_keys[63..66].copy_from_slice(b"ant");
        assert!(matches!(
            decode_file_meta_wire(&reversed_keys),
            Err(KalanjiyamError::Corruption(_))
        ));

        let entry = shard(Bound::NegInf, Bound::Finite(b"m".to_vec()));
        let entry_bytes = encode_logical_shard_entry_wire(&entry).unwrap();
        assert!(matches!(
            decode_logical_shard_entry_wire(&entry_bytes[..23]),
            Err(KalanjiyamError::Corruption(_))
        ));

        let mut bad_fixed = entry_bytes.clone();
        bad_fixed[16..20].copy_from_slice(&25_u32.to_le_bytes());
        assert!(matches!(
            decode_logical_shard_entry_wire(&bad_fixed),
            Err(KalanjiyamError::Corruption(_))
        ));

        let mut bad_reserved = entry_bytes.clone();
        bad_reserved[20] = 1;
        assert!(matches!(
            decode_logical_shard_entry_wire(&bad_reserved),
            Err(KalanjiyamError::Corruption(_))
        ));

        let mut truncated = entry_bytes.clone();
        truncated.truncate(entry_bytes.len() - 1);
        assert!(matches!(
            decode_logical_shard_entry_wire(&truncated),
            Err(KalanjiyamError::Corruption(_))
        ));

        let mut empty_range = entry_bytes.clone();
        empty_range[12..16].copy_from_slice(&1_u32.to_le_bytes());
        empty_range.truncate(25);
        empty_range[24] = 1;
        assert!(matches!(
            decode_logical_shard_entry_wire(&empty_range),
            Err(KalanjiyamError::Corruption(_))
        ));
    }

    #[test]
    fn bound_decoder_rejects_invalid_encodings() {
        assert!(matches!(
            decode_bound_wire(&[]),
            Err(KalanjiyamError::Corruption(_))
        ));
        assert!(matches!(
            decode_bound_wire(&[0, 1]),
            Err(KalanjiyamError::Corruption(_))
        ));
        assert!(matches!(
            decode_bound_wire(&[0, 3, 0, b'a', b'n']),
            Err(KalanjiyamError::Corruption(_))
        ));
        assert!(matches!(
            decode_bound_wire(&[9]),
            Err(KalanjiyamError::Corruption(_))
        ));
    }
}
