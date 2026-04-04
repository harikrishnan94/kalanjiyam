//! Shared in-memory types and validation helpers used by the pezhai engine.

use std::cmp::Ordering;
use std::collections::{BTreeMap, btree_map};
use std::fmt::{Display, Formatter};
use std::ops::Bound as RangeBound;
use std::path::PathBuf;
use std::sync::Arc;

use crate::error::KalanjiyamError;

/// Stable storage-format major version defined by the pezhai specs.
pub const FORMAT_MAJOR: u16 = 1;
/// Smallest supported `.kjm` page size.
pub const MIN_PAGE_SIZE_BYTES: usize = 4096;
/// Default `.kjm` page size used when `config.toml` omits `engine.page_size_bytes`.
pub const DEFAULT_PAGE_SIZE_BYTES: usize = MIN_PAGE_SIZE_BYTES;
/// Largest supported `.kjm` page size in format-major v1.
///
/// The current `.kjm` block layouts store some intra-block offsets as `u16`,
/// so v1 caps page size at 32 KiB until the durable format widens those fields.
pub const MAX_PAGE_SIZE_BYTES: usize = 32 * 1024;
/// Maximum accepted user-key length.
pub const MAX_KEY_BYTES: usize = 1024;
/// Maximum accepted value length.
pub const MAX_VALUE_BYTES: usize = 268_435_455;
/// Sentinel `u64` used by durable formats to mean "none".
pub const NONE_U64: u64 = u64::MAX;
/// Maximum single-record WAL size from the spec-defined formula.
pub const MAX_SINGLE_WAL_RECORD_BYTES: usize = 64 + 8 + MAX_KEY_BYTES + MAX_VALUE_BYTES + 8;

/// The configured durability policy for ordinary writes and publish records.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SyncMode {
    /// Acknowledge only after the WAL durable frontier covers the record seqno.
    PerWrite,
    /// Acknowledge after append and require explicit `sync()` for crash durability.
    Manual,
}

impl Display for SyncMode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PerWrite => f.write_str("per_write"),
            Self::Manual => f.write_str("manual"),
        }
    }
}

/// The value kind stored in memtables and data files.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq, Ord, PartialOrd)]
pub enum RecordKind {
    /// Delete sorts before `Put` in internal-key order.
    #[default]
    Delete = 0,
    /// Put stores a visible value payload.
    Put = 1,
}

/// Durable WAL record kinds defined by the storage-engine spec.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum WalRecordType {
    /// User `Put`.
    Put = 1,
    /// User `Delete`.
    Delete = 2,
    /// Flush publication.
    FlushPublish = 3,
    /// Compaction publication.
    CompactPublish = 4,
    /// Logical-shard installation.
    LogicalShardInstall = 5,
}

impl WalRecordType {
    /// Parses a durable WAL record-type tag.
    ///
    /// # Complexity
    /// Time: O(1)
    /// Space: O(1)
    pub fn from_u8(value: u8) -> Result<Self, KalanjiyamError> {
        match value {
            1 => Ok(Self::Put),
            2 => Ok(Self::Delete),
            3 => Ok(Self::FlushPublish),
            4 => Ok(Self::CompactPublish),
            5 => Ok(Self::LogicalShardInstall),
            _ => Err(KalanjiyamError::Corruption(format!(
                "unknown WAL record type {value}"
            ))),
        }
    }
}

/// Bound encoding used for logical shards and scan ranges.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub enum Bound {
    /// Inclusive finite key bound.
    Finite(Vec<u8>),
    /// Negative infinity.
    #[default]
    NegInf,
    /// Positive infinity.
    PosInf,
}

impl Bound {
    /// Validates a start bound against the spec.
    ///
    /// # Complexity
    /// Time: O(1).
    /// Space: O(1).
    pub fn validate_start(&self) -> Result<(), KalanjiyamError> {
        match self {
            Self::Finite(key) => validate_key(key),
            Self::NegInf => Ok(()),
            Self::PosInf => Err(KalanjiyamError::InvalidArgument(
                "start bound must be NegInf or Finite".to_string(),
            )),
        }
    }

    /// Validates an end bound against the spec.
    ///
    /// # Complexity
    /// Time: O(1).
    /// Space: O(1).
    pub fn validate_end(&self) -> Result<(), KalanjiyamError> {
        match self {
            Self::Finite(key) => validate_key(key),
            Self::PosInf => Ok(()),
            Self::NegInf => Err(KalanjiyamError::InvalidArgument(
                "end bound must be Finite or PosInf".to_string(),
            )),
        }
    }
}

impl Ord for Bound {
    fn cmp(&self, other: &Self) -> Ordering {
        use Bound::{Finite, NegInf, PosInf};
        match (self, other) {
            (NegInf, NegInf) | (PosInf, PosInf) => Ordering::Equal,
            (NegInf, _) => Ordering::Less,
            (_, NegInf) => Ordering::Greater,
            (PosInf, _) => Ordering::Greater,
            (_, PosInf) => Ordering::Less,
            (Finite(left), Finite(right)) => left.cmp(right),
        }
    }
}

impl PartialOrd for Bound {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Half-open key range used by scans and logical-shard entries.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct KeyRange {
    /// Inclusive start bound.
    pub start_bound: Bound,
    /// Exclusive end bound.
    pub end_bound: Bound,
}

impl Default for KeyRange {
    fn default() -> Self {
        Self {
            start_bound: Bound::NegInf,
            end_bound: Bound::PosInf,
        }
    }
}

impl KeyRange {
    /// Validates a scan or logical-shard range against the spec.
    ///
    /// # Complexity
    /// Time: O(k), where `k` is the boundary-key length compared when checking
    /// `start_bound < end_bound`.
    /// Space: O(1).
    pub fn validate(&self) -> Result<(), KalanjiyamError> {
        self.start_bound.validate_start()?;
        self.end_bound.validate_end()?;
        if self.start_bound >= self.end_bound {
            return Err(KalanjiyamError::InvalidArgument(
                "range must be non-empty".to_string(),
            ));
        }
        Ok(())
    }

    /// Returns whether the half-open range contains the given user key.
    ///
    /// # Complexity
    /// Time: O(k), where `k` is the key length, because the implementation
    /// materializes a temporary finite bound before comparing it.
    /// Space: O(k) for that temporary bound.
    #[must_use]
    pub fn contains_key(&self, key: &[u8]) -> bool {
        let bound = Bound::Finite(key.to_vec());
        self.start_bound <= bound && bound < self.end_bound
    }

    /// Returns whether the range overlaps one file's inclusive user-key span.
    ///
    /// # Complexity
    /// Time: O(k), where `k` is the combined length of the compared boundary
    /// keys.
    /// Space: O(k) for the temporary finite bounds used in the comparison.
    #[must_use]
    pub fn overlaps_file(&self, min_user_key: &[u8], max_user_key: &[u8]) -> bool {
        let file_start = Bound::Finite(min_user_key.to_vec());
        let file_end = Bound::Finite(max_user_key.to_vec());
        self.start_bound < file_end && file_start < self.end_bound
    }
}

/// One internal-key record in memtables or data files.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct InternalRecord {
    /// User key bytes.
    pub user_key: Vec<u8>,
    /// Monotonic global sequence number.
    pub seqno: u64,
    /// Visible record kind.
    pub kind: RecordKind,
    /// Value payload for `Put`.
    pub value: Option<Vec<u8>>,
}

impl InternalRecord {
    /// Returns the logical byte contribution defined by the spec.
    ///
    /// # Complexity
    /// Time: O(1)
    /// Space: O(1)
    #[must_use]
    pub fn logical_bytes(&self) -> u64 {
        let value_len = self
            .value
            .as_ref()
            .map_or(0_u64, |value| value.len() as u64);
        self.user_key.len() as u64 + value_len
    }
}

/// Compatibility alias used by durable helpers.
pub type MemtableRecord = InternalRecord;

type MemtableVersions = BTreeMap<(u64, u8), usize>;

/// Immutable memtable snapshot used by current and historical manifest generations.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Memtable {
    /// Optional frozen identifier once the memtable leaves the active slot.
    pub frozen_memtable_id: Option<u64>,
    /// Internal records in append order.
    ///
    /// The storage engine still uses this field for durable flush payload
    /// construction and plan capture. Visibility lookups are handled by the
    /// private `key_index` to avoid whole-vector resorting on each insert.
    pub records: Vec<InternalRecord>,
    /// Per-key visibility index keyed by `(seqno, visibility_rank)`.
    ///
    /// `visibility_rank` is `0` for `Put` and `1` for `Delete` so a
    /// descending predecessor search prefers `Delete` when both kinds exist
    /// at one seqno, matching internal-key visibility rules. Each leaf points
    /// back into `records` so the ordered index does not duplicate whole
    /// payload-bearing records.
    key_index: BTreeMap<Vec<u8>, MemtableVersions>,
    logical_bytes_total: u64,
    min_seqno: Option<u64>,
    max_seqno: Option<u64>,
}

/// Lazy internal-record iterator over one memtable range.
///
/// Records are yielded in the spec-defined internal-key order without
/// rebuilding or resorting the full memtable on each scan.
pub struct MemtableRecordIterator<'a> {
    records: &'a [InternalRecord],
    keys: btree_map::Range<'a, Vec<u8>, MemtableVersions>,
    versions: Option<std::iter::Rev<btree_map::Values<'a, (u64, u8), usize>>>,
}

impl Memtable {
    /// Creates one empty memtable.
    ///
    /// # Complexity
    /// Time: O(1)
    /// Space: O(1)
    #[must_use]
    pub fn empty() -> Self {
        Self::default()
    }

    /// Returns whether the memtable has no internal records.
    ///
    /// # Complexity
    /// Time: O(1)
    /// Space: O(1)
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Returns the number of stored internal records.
    ///
    /// # Complexity
    /// Time: O(1)
    /// Space: O(1)
    #[must_use]
    pub fn entry_count(&self) -> u64 {
        self.records.len() as u64
    }

    /// Returns the smallest seqno in the memtable if it is non-empty.
    ///
    /// # Complexity
    /// Time: O(1)
    /// Space: O(1)
    #[must_use]
    pub fn min_seqno(&self) -> Option<u64> {
        self.min_seqno
    }

    /// Returns the largest seqno in the memtable if it is non-empty.
    ///
    /// # Complexity
    /// Time: O(1)
    /// Space: O(1)
    #[must_use]
    pub fn max_seqno(&self) -> Option<u64> {
        self.max_seqno
    }

    /// Returns the exact logical-byte total for stored internal records.
    ///
    /// # Complexity
    /// Time: O(1)
    /// Space: O(1)
    #[must_use]
    pub fn logical_bytes(&self) -> u64 {
        self.logical_bytes_total
    }

    /// Inserts one visible `Put` version while preserving internal-key order.
    ///
    /// # Complexity
    /// Time: O(log u + log n + k + v) due to BTreeMap updates and cloning the key/value.
    /// Space: O(k + v)
    pub fn insert_put(&mut self, seqno: u64, key: &[u8], value: &[u8]) {
        self.insert(InternalRecord {
            user_key: key.to_vec(),
            seqno,
            kind: RecordKind::Put,
            value: Some(value.to_vec()),
        });
    }

    /// Inserts one visible `Delete` tombstone while preserving internal-key order.
    ///
    /// # Complexity
    /// Time: O(log u + log n + k) due to BTreeMap updates and cloning the key.
    /// Space: O(k)
    pub fn insert_delete(&mut self, seqno: u64, key: &[u8]) {
        self.insert(InternalRecord {
            user_key: key.to_vec(),
            seqno,
            kind: RecordKind::Delete,
            value: None,
        });
    }

    /// Finds the first visible record for one key at the given snapshot seqno.
    ///
    /// # Complexity
    /// Time: O(log u + log n + k), where `u` is the number of distinct keys in
    /// the memtable, `n` is the number of versions stored for the selected key,
    /// and `k` is the key length used by the `BTreeMap` comparison path.
    /// Space: O(k + v) for the cloned record returned to the caller.
    #[must_use]
    pub fn get_visible(&self, key: &[u8], snapshot_seqno: u64) -> Option<InternalRecord> {
        self.key_index
            .get(key)
            .and_then(|versions| visible_version(&self.records, versions, snapshot_seqno))
            .cloned()
    }

    /// Produces visible scan rows in ascending user-key order for one range.
    ///
    /// # Complexity
    /// Time: O(log u + u_r * log n + r * (k + v)), where `u` is the total
    /// number of distinct keys in the memtable, `u_r` is the number of keys
    /// visited in the selected range, `n` is the number of versions stored for
    /// a visited key, and `r` is the number of returned rows.
    /// Space: O(r * (k + v)) for the returned rows.
    #[must_use]
    pub fn scan_visible(
        &self,
        range: &KeyRange,
        snapshot_seqno: u64,
        resume_after_key: Option<&[u8]>,
    ) -> Vec<ScanRow> {
        let (start, end) = key_range_bounds(range);
        let mut rows = Vec::new();
        for (user_key, versions) in self.key_index.range::<[u8], _>((start, end)) {
            if resume_after_key.is_some_and(|resume| user_key.as_slice() <= resume) {
                continue;
            }
            let Some(record) = visible_version(&self.records, versions, snapshot_seqno) else {
                continue;
            };
            if record.kind == RecordKind::Put {
                rows.push(ScanRow {
                    key: user_key.clone(),
                    value: record.value.clone().unwrap_or_default(),
                });
            }
        }
        rows
    }

    /// Returns a lazy iterator over internal records in one scan range.
    ///
    /// # Complexity
    /// Time: O(log u)
    /// Space: O(1)
    #[must_use]
    pub fn iter_internal_records<'a>(
        &'a self,
        range: &KeyRange,
        resume_after_key: Option<&[u8]>,
    ) -> MemtableRecordIterator<'a> {
        let (start, end) = key_range_bounds(range);
        let start = effective_start_bound(start, resume_after_key);
        MemtableRecordIterator {
            records: &self.records,
            keys: self.key_index.range::<[u8], _>((start, end)),
            versions: None,
        }
    }

    fn insert(&mut self, record: InternalRecord) {
        self.logical_bytes_total += record.logical_bytes();
        self.min_seqno = Some(
            self.min_seqno
                .map_or(record.seqno, |seq| seq.min(record.seqno)),
        );
        self.max_seqno = Some(
            self.max_seqno
                .map_or(record.seqno, |seq| seq.max(record.seqno)),
        );

        let version_key = (record.seqno, visibility_rank(record.kind));
        let record_index = self.records.len();
        if let Some(versions) = self.key_index.get_mut(record.user_key.as_slice()) {
            versions.insert(version_key, record_index);
        } else {
            let mut versions = MemtableVersions::new();
            versions.insert(version_key, record_index);
            self.key_index.insert(record.user_key.clone(), versions);
        }
        self.records.push(record);
    }
}

impl MemtableRecordIterator<'_> {
    /// Returns the next internal record in user-key then descending-version order.
    ///
    /// # Complexity
    /// Time: Amortized O(1) iterator advancement plus O(k + v) to clone the
    /// returned record's key and value.
    /// Space: O(k + v) for the returned clone.
    pub fn next_record(&mut self) -> Option<InternalRecord> {
        loop {
            if let Some(versions) = self.versions.as_mut()
                && let Some(index) = versions.next()
            {
                return self.records.get(*index).cloned();
            }
            let (_, versions) = self.keys.next()?;
            self.versions = Some(versions.values().rev());
        }
    }
}

fn visible_version<'a>(
    records: &'a [InternalRecord],
    versions: &MemtableVersions,
    snapshot_seqno: u64,
) -> Option<&'a InternalRecord> {
    versions
        .range(..=(snapshot_seqno, visibility_rank(RecordKind::Delete)))
        .next_back()
        .and_then(|(_, index)| records.get(*index))
}

fn visibility_rank(kind: RecordKind) -> u8 {
    match kind {
        RecordKind::Put => 0,
        RecordKind::Delete => 1,
    }
}

fn key_range_bounds(range: &KeyRange) -> (RangeBound<&[u8]>, RangeBound<&[u8]>) {
    let start = match &range.start_bound {
        Bound::NegInf => RangeBound::Unbounded,
        Bound::Finite(key) => RangeBound::Included(key.as_slice()),
        Bound::PosInf => RangeBound::Unbounded,
    };
    let end = match &range.end_bound {
        Bound::NegInf => RangeBound::Unbounded,
        Bound::Finite(key) => RangeBound::Excluded(key.as_slice()),
        Bound::PosInf => RangeBound::Unbounded,
    };
    (start, end)
}

// Resume-after pagination may need to advance the starting key past the scan
// range's inclusive lower bound, but never widen the range below that bound.
fn effective_start_bound<'a>(
    start: RangeBound<&'a [u8]>,
    resume_after_key: Option<&'a [u8]>,
) -> RangeBound<&'a [u8]> {
    match (start, resume_after_key) {
        (RangeBound::Included(start_key), Some(resume)) if resume >= start_key => {
            RangeBound::Excluded(resume)
        }
        (RangeBound::Excluded(start_key), Some(resume)) if resume >= start_key => {
            RangeBound::Excluded(resume)
        }
        (RangeBound::Unbounded, Some(resume)) => RangeBound::Excluded(resume),
        (start, _) => start,
    }
}

/// Compatibility aliases used by engine planning types.
pub type ActiveMemtable = Memtable;
/// Compatibility aliases used by engine planning types.
pub type FrozenMemtable = Memtable;

/// One published immutable data file in the shared manifest.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FileMeta {
    /// Monotonic, never-reused file identifier.
    pub file_id: u64,
    /// Level that owns the file in the manifest.
    pub level_no: u16,
    /// Inclusive minimum user key stored in the file.
    pub min_user_key: Vec<u8>,
    /// Inclusive maximum user key stored in the file.
    pub max_user_key: Vec<u8>,
    /// Minimum stored seqno.
    pub min_seqno: u64,
    /// Maximum stored seqno.
    pub max_seqno: u64,
    /// Stored internal-record count.
    pub entry_count: u64,
    /// Exact logical bytes across all stored internal records.
    pub logical_bytes: u64,
    /// Exact durable file length.
    pub physical_bytes: u64,
}

/// Immutable snapshot of one historical shared-data source set.
#[derive(Clone, Debug)]
pub struct DataManifestSnapshot {
    /// Shared-data generation identifier.
    pub data_generation: u64,
    /// Published shared data files grouped by level.
    pub levels: Vec<Vec<FileMeta>>,
    /// Active memtable snapshot for that generation.
    pub active_memtable_ref: Arc<Memtable>,
    /// Frozen memtables from oldest to newest freeze.
    pub frozen_memtable_refs: Vec<Arc<Memtable>>,
}

impl Default for DataManifestSnapshot {
    fn default() -> Self {
        Self {
            data_generation: 0,
            levels: Vec::new(),
            active_memtable_ref: Arc::new(Memtable::empty()),
            frozen_memtable_refs: Vec::new(),
        }
    }
}

/// Historical manifest snapshots indexed by shared-data generation.
#[derive(Clone, Debug, Default)]
pub struct DataManifestIndex {
    /// Manifest snapshots keyed by `data_generation`.
    pub by_generation: BTreeMap<u64, DataManifestSnapshot>,
}

/// Current latest-only logical-shard entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalShardEntry {
    /// Inclusive start bound.
    pub start_bound: Bound,
    /// Exclusive end bound.
    pub end_bound: Bound,
    /// Advisory live-byte total for the current range.
    pub live_size_bytes: u64,
}

impl LogicalShardEntry {
    /// Returns the half-open range covered by the entry.
    ///
    /// # Complexity
    /// Time: O(k)
    /// Space: O(k)
    #[must_use]
    pub fn key_range(&self) -> KeyRange {
        KeyRange {
            start_bound: self.start_bound.clone(),
            end_bound: self.end_bound.clone(),
        }
    }
}

/// Snapshot handle returned by `create_snapshot()`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SnapshotHandle {
    /// Engine instance that created the handle.
    pub engine_instance_id: u64,
    /// Monotonic snapshot identifier for that engine instance.
    pub snapshot_id: u64,
    /// Pinned committed seqno.
    pub snapshot_seqno: u64,
    /// Pinned shared-data generation.
    pub data_generation: u64,
}

/// Point-read response returned by the engine and latest-only server `Get`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GetResponse {
    /// Whether the key was found.
    pub found: bool,
    /// Value payload when `found = true`.
    pub value: Option<Vec<u8>>,
    /// Seqno observed when the read snapshot was resolved.
    pub observation_seqno: u64,
    /// Shared-data generation used by the read.
    pub data_generation: u64,
}

/// One user-visible scan row.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ScanRow {
    /// User key bytes.
    pub key: Vec<u8>,
    /// Visible value bytes.
    pub value: Vec<u8>,
}

/// One scan page result used by the engine and server host.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ScanPage {
    /// Returned rows in ascending user-key order.
    pub rows: Vec<ScanRow>,
    /// Whether the page reached EOF.
    pub eof: bool,
    /// Greatest emitted key when `eof = false`.
    pub next_resume_after_key: Option<Vec<u8>>,
}

/// Sync response used by the engine and latest-only server `Sync`.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct SyncResponse {
    /// Durable WAL frontier observed at completion.
    pub durable_seqno: u64,
}

/// Aggregated shared-data totals for one level.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct LevelStats {
    /// Level number.
    pub level_no: u16,
    /// File count in the level.
    pub file_count: u64,
    /// Exact logical bytes across the level.
    pub logical_bytes: u64,
    /// Exact physical bytes across the level.
    pub physical_bytes: u64,
}

/// Latest-only logical-shard statistics entry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LogicalShardStats {
    /// Inclusive start bound.
    pub start_bound: Bound,
    /// Exclusive end bound.
    pub end_bound: Bound,
    /// Current tracked live-byte value for the range.
    pub live_size_bytes: u64,
}

/// Engine-wide stats response.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct StatsResponse {
    /// Latest committed seqno when stats were produced.
    pub observation_seqno: u64,
    /// Current shared-data generation.
    pub data_generation: u64,
    /// Shared-data totals by level.
    pub levels: Vec<LevelStats>,
    /// Latest logical-shard map.
    pub logical_shards: Vec<LogicalShardStats>,
}

/// Shared file reference captured into immutable worker plans.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct DataFileRef {
    /// Durable metadata for the referenced file.
    pub meta: FileMeta,
    /// Canonical file path on disk.
    pub path: PathBuf,
}

/// Returns the spec-defined internal-key ordering for data records.
#[must_use]
/// Compares two internal records in the spec-defined internal-key order.
///
/// Ordering is by ascending user key, then descending sequence number, with
/// `Delete` sorting before `Put` when both key and seqno are equal.
///
/// # Complexity
/// Time: O(k), where `k` is the length of the common key prefix inspected by
/// the byte-slice comparison.
/// Space: O(1).
///
/// # Examples
///
/// ```
/// use std::cmp::Ordering;
///
/// use kalanjiyam::pezhai::types::{InternalRecord, RecordKind, compare_internal_records};
///
/// let newer_delete = InternalRecord {
///     user_key: b"yak".to_vec(),
///     seqno: 9,
///     kind: RecordKind::Delete,
///     value: None,
/// };
/// let older_put = InternalRecord {
///     user_key: b"yak".to_vec(),
///     seqno: 8,
///     kind: RecordKind::Put,
///     value: Some(b"value".to_vec()),
/// };
/// let same_seqno_put = InternalRecord {
///     user_key: b"yak".to_vec(),
///     seqno: 9,
///     kind: RecordKind::Put,
///     value: Some(b"value".to_vec()),
/// };
///
/// assert_eq!(compare_internal_records(&newer_delete, &older_put), Ordering::Less);
/// assert_eq!(compare_internal_records(&newer_delete, &same_seqno_put), Ordering::Less);
/// ```
pub fn compare_internal_records(left: &InternalRecord, right: &InternalRecord) -> Ordering {
    match left.user_key.cmp(&right.user_key) {
        Ordering::Equal => match right.seqno.cmp(&left.seqno) {
            Ordering::Equal => left.kind.cmp(&right.kind),
            other => other,
        },
        other => other,
    }
}

/// Validates one user key against the engine rules.
///
/// # Complexity
/// Time: O(1).
/// Space: O(1).
pub fn validate_key(key: &[u8]) -> Result<(), KalanjiyamError> {
    if key.is_empty() || key.len() > MAX_KEY_BYTES {
        return Err(KalanjiyamError::InvalidArgument(format!(
            "key length {} is outside 1..={MAX_KEY_BYTES}",
            key.len()
        )));
    }
    Ok(())
}

/// Validates one user value against the engine rules.
///
/// # Complexity
/// Time: O(1).
/// Space: O(1).
pub fn validate_value(value: &[u8]) -> Result<(), KalanjiyamError> {
    if value.len() > MAX_VALUE_BYTES {
        return Err(KalanjiyamError::InvalidArgument(format!(
            "value length {} exceeds {MAX_VALUE_BYTES}",
            value.len()
        )));
    }
    Ok(())
}

/// Validates one configured or decoded `.kjm` page size against the v1 rules.
///
/// # Complexity
/// Time: O(1).
/// Space: O(1).
pub fn validate_page_size_bytes(page_size_bytes: usize) -> Result<(), KalanjiyamError> {
    if !(MIN_PAGE_SIZE_BYTES..=MAX_PAGE_SIZE_BYTES).contains(&page_size_bytes) {
        return Err(KalanjiyamError::InvalidArgument(format!(
            "page_size_bytes must be in {}..={}",
            MIN_PAGE_SIZE_BYTES, MAX_PAGE_SIZE_BYTES
        )));
    }
    if !page_size_bytes.is_power_of_two() {
        return Err(KalanjiyamError::InvalidArgument(
            "page_size_bytes must be a power of two".to_string(),
        ));
    }
    Ok(())
}

/// Validates scan page limits defined by the server spec.
///
/// # Complexity
/// Time: O(1)
/// Space: O(1)
pub fn validate_page_limits(
    max_records_per_page: u32,
    max_bytes_per_page: u32,
) -> Result<(), KalanjiyamError> {
    if max_records_per_page == 0 || max_bytes_per_page == 0 {
        return Err(KalanjiyamError::InvalidArgument(
            "page limits must both be greater than zero".to_string(),
        ));
    }
    Ok(())
}

/// Validates one latest-only logical-shard map.
///
/// # Complexity
/// Time: O(s * k), where `s` is the number of shard entries and `k` is the
/// boundary-key length touched by validation and adjacency checks.
/// Space: O(k) peak temporary space for cloned bounds inside `key_range()`.
///
/// This helper treats malformed shard maps as corruption because callers use
/// it for replayed or engine-owned state that should already satisfy the spec.
pub fn validate_logical_shards(entries: &[LogicalShardEntry]) -> Result<(), KalanjiyamError> {
    if entries.is_empty() {
        return Err(KalanjiyamError::Corruption(
            "logical shard map must not be empty".to_string(),
        ));
    }
    for entry in entries {
        entry.key_range().validate()?;
    }
    for pair in entries.windows(2) {
        if pair[0].end_bound != pair[1].start_bound {
            return Err(KalanjiyamError::Corruption(
                "logical shard ranges must be contiguous".to_string(),
            ));
        }
        if pair[0].start_bound >= pair[1].start_bound {
            return Err(KalanjiyamError::Corruption(
                "logical shard ranges must be sorted".to_string(),
            ));
        }
    }
    Ok(())
}

/// Returns visible scan rows from one internal-record stream.
///
/// # Complexity
/// Time: O(n * k + r * (k + v)), where `n` is the number of input records and
/// `r` is the number of returned rows.
/// Space: O(r * (k + v)) for the returned rows.
///
/// The input is expected to already be in full internal-key order. The helper
/// suppresses shadowed older versions, skips tombstones, applies the pinned
/// snapshot sequence number, and skips any key at or before `resume_after_key`.
///
/// # Examples
///
/// ```
/// use kalanjiyam::pezhai::types::{
///     Bound, InternalRecord, KeyRange, RecordKind, visible_rows_from_records,
/// };
///
/// let records = vec![
///     InternalRecord {
///         user_key: b"ant".to_vec(),
///         seqno: 5,
///         kind: RecordKind::Put,
///         value: Some(b"v2".to_vec()),
///     },
///     InternalRecord {
///         user_key: b"ant".to_vec(),
///         seqno: 4,
///         kind: RecordKind::Put,
///         value: Some(b"v1".to_vec()),
///     },
///     InternalRecord {
///         user_key: b"bee".to_vec(),
///         seqno: 6,
///         kind: RecordKind::Delete,
///         value: None,
///     },
///     InternalRecord {
///         user_key: b"bee".to_vec(),
///         seqno: 3,
///         kind: RecordKind::Put,
///         value: Some(b"hidden".to_vec()),
///     },
///     InternalRecord {
///         user_key: b"cat".to_vec(),
///         seqno: 2,
///         kind: RecordKind::Put,
///         value: Some(b"v-cat".to_vec()),
///     },
/// ];
/// let range = KeyRange {
///     start_bound: Bound::NegInf,
///     end_bound: Bound::PosInf,
/// };
///
/// let rows = visible_rows_from_records(&records, &range, 10, Some(b"ant"));
/// assert_eq!(rows.len(), 1);
/// assert_eq!(rows[0].key, b"cat".to_vec());
/// assert_eq!(rows[0].value, b"v-cat".to_vec());
/// ```
#[must_use]
pub fn visible_rows_from_records(
    records: &[InternalRecord],
    range: &KeyRange,
    snapshot_seqno: u64,
    resume_after_key: Option<&[u8]>,
) -> Vec<ScanRow> {
    let mut rows = Vec::new();
    let mut last_key: Option<&[u8]> = None;
    for record in records {
        if !range.contains_key(&record.user_key) || record.seqno > snapshot_seqno {
            continue;
        }
        if let Some(resume_key) = resume_after_key
            && record.user_key.as_slice() <= resume_key
        {
            continue;
        }
        if last_key == Some(record.user_key.as_slice()) {
            continue;
        }
        last_key = Some(record.user_key.as_slice());
        if record.kind == RecordKind::Put {
            rows.push(ScanRow {
                key: record.user_key.clone(),
                value: record.value.clone().unwrap_or_default(),
            });
        }
    }
    rows
}

/// Returns the tracked current logical shard that covers one user key.
///
/// # Complexity
/// Time: O(log s * k), where `s` is the number of shard entries and `k` is the
/// key length compared during the binary search.
/// Space: O(1)
///
/// This helper first validates the shard map and therefore reports malformed
/// inputs as corruption rather than guessing one fallback range.
///
/// # Examples
///
/// ```
/// use kalanjiyam::pezhai::types::{
///     Bound, LogicalShardEntry, find_logical_shard_covering_key, validate_logical_shards,
/// };
///
/// let shards = vec![
///     LogicalShardEntry {
///         start_bound: Bound::NegInf,
///         end_bound: Bound::Finite(b"m".to_vec()),
///         live_size_bytes: 10,
///     },
///     LogicalShardEntry {
///         start_bound: Bound::Finite(b"m".to_vec()),
///         end_bound: Bound::PosInf,
///         live_size_bytes: 20,
///     },
/// ];
///
/// validate_logical_shards(&shards).unwrap();
/// assert_eq!(find_logical_shard_covering_key(&shards, b"yak").unwrap(), 1);
/// ```
pub fn find_logical_shard_covering_key(
    entries: &[LogicalShardEntry],
    user_key: &[u8],
) -> Result<usize, KalanjiyamError> {
    if entries.is_empty() {
        return Err(KalanjiyamError::Corruption(
            "logical shard map must not be empty".to_string(),
        ));
    }
    let index = entries
        .partition_point(|entry| bound_starts_at_or_before_key(&entry.start_bound, user_key));
    let candidate = index.checked_sub(1).ok_or_else(|| {
        KalanjiyamError::Corruption("logical shard lookup missed every range".to_string())
    })?;
    let entry = &entries[candidate];
    if !bound_starts_at_or_before_key(&entry.start_bound, user_key)
        || !key_is_before_end_bound(user_key, &entry.end_bound)
    {
        return Err(KalanjiyamError::Corruption(
            "logical shard lookup missed every range".to_string(),
        ));
    }
    Ok(candidate)
}

fn bound_starts_at_or_before_key(bound: &Bound, user_key: &[u8]) -> bool {
    match bound {
        Bound::NegInf => true,
        Bound::Finite(key) => key.as_slice() <= user_key,
        Bound::PosInf => false,
    }
}

fn key_is_before_end_bound(user_key: &[u8], bound: &Bound) -> bool {
    match bound {
        Bound::NegInf => false,
        Bound::Finite(key) => user_key < key.as_slice(),
        Bound::PosInf => true,
    }
}

/// Builds level totals from one manifest.
///
/// # Complexity
/// Time: O(l + f), where `l` is the number of levels and `f` is the total
/// number of files across all levels.
/// Space: O(l) for the returned `LevelStats` vector.
#[must_use]
pub fn build_level_stats(levels: &[Vec<FileMeta>]) -> Vec<LevelStats> {
    levels
        .iter()
        .enumerate()
        .map(|(level_no, files)| LevelStats {
            level_no: level_no as u16,
            file_count: files.len() as u64,
            logical_bytes: files.iter().map(|file| file.logical_bytes).sum(),
            physical_bytes: files.iter().map(|file| file.physical_bytes).sum(),
        })
        .collect()
}

/// Builds logical-shard stats from the current latest-only map.
///
/// # Complexity
/// Time: O(s * k), where `s` is the number of shard entries and `k` is the
/// boundary-key length cloned into the output.
/// Space: O(s * k) for the returned stats entries.
#[must_use]
pub fn build_logical_shard_stats(entries: &[LogicalShardEntry]) -> Vec<LogicalShardStats> {
    entries
        .iter()
        .map(|entry| LogicalShardStats {
            start_bound: entry.start_bound.clone(),
            end_bound: entry.end_bound.clone(),
            live_size_bytes: entry.live_size_bytes,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn record(key: &[u8], seqno: u64, kind: RecordKind, value: Option<&[u8]>) -> InternalRecord {
        InternalRecord {
            user_key: key.to_vec(),
            seqno,
            kind,
            value: value.map(|bytes| bytes.to_vec()),
        }
    }

    #[test]
    fn sync_mode_display_bound_validation_and_wal_record_types_cover_all_variants() {
        assert_eq!(SyncMode::PerWrite.to_string(), "per_write");
        assert_eq!(SyncMode::Manual.to_string(), "manual");
        assert!(matches!(
            Bound::PosInf.validate_start(),
            Err(KalanjiyamError::InvalidArgument(_))
        ));
        assert!(matches!(
            Bound::NegInf.validate_end(),
            Err(KalanjiyamError::InvalidArgument(_))
        ));
        assert_eq!(Bound::NegInf.cmp(&Bound::NegInf), Ordering::Equal);
        assert_eq!(
            Bound::Finite(b"ant".to_vec()).cmp(&Bound::NegInf),
            Ordering::Greater
        );
        assert_eq!(
            Bound::PosInf.cmp(&Bound::Finite(b"yak".to_vec())),
            Ordering::Greater
        );

        for (tag, expected) in [
            (1_u8, WalRecordType::Put),
            (2_u8, WalRecordType::Delete),
            (3_u8, WalRecordType::FlushPublish),
            (4_u8, WalRecordType::CompactPublish),
            (5_u8, WalRecordType::LogicalShardInstall),
        ] {
            assert_eq!(WalRecordType::from_u8(tag).unwrap(), expected);
        }
    }

    #[test]
    fn memtable_and_visibility_helpers_cover_resume_and_lookup_miss() {
        let mut memtable = Memtable::empty();
        assert!(memtable.is_empty());
        assert_eq!(memtable.entry_count(), 0);
        assert_eq!(memtable.min_seqno(), None);
        assert_eq!(memtable.max_seqno(), None);

        memtable.insert_put(4, b"ant", b"v1");
        memtable.insert_put(6, b"ant", b"v2");
        memtable.insert_delete(5, b"bee");
        memtable.insert_put(3, b"cat", b"v3");

        assert_eq!(
            memtable.records,
            vec![
                record(b"ant", 4, RecordKind::Put, Some(b"v1")),
                record(b"ant", 6, RecordKind::Put, Some(b"v2")),
                record(b"bee", 5, RecordKind::Delete, None),
                record(b"cat", 3, RecordKind::Put, Some(b"v3")),
            ]
        );
        assert_eq!(memtable.entry_count(), 4);
        assert_eq!(memtable.min_seqno(), Some(3));
        assert_eq!(memtable.max_seqno(), Some(6));
        assert_eq!(
            memtable.logical_bytes(),
            record(b"ant", 6, RecordKind::Put, Some(b"v2")).logical_bytes()
                + record(b"ant", 4, RecordKind::Put, Some(b"v1")).logical_bytes()
                + record(b"bee", 5, RecordKind::Delete, None).logical_bytes()
                + record(b"cat", 3, RecordKind::Put, Some(b"v3")).logical_bytes()
        );
        assert_eq!(
            memtable.get_visible(b"ant", 10).unwrap().value,
            Some(b"v2".to_vec())
        );
        assert_eq!(
            memtable.get_visible(b"ant", 4).unwrap().value,
            Some(b"v1".to_vec())
        );
        assert_eq!(
            memtable.get_visible(b"bee", 10).unwrap().kind,
            RecordKind::Delete
        );
        assert_eq!(memtable.get_visible(b"yak", 10), None);

        let range = KeyRange::default();
        let rows = memtable.scan_visible(&range, 10, None);
        assert_eq!(
            rows,
            vec![
                ScanRow {
                    key: b"ant".to_vec(),
                    value: b"v2".to_vec(),
                },
                ScanRow {
                    key: b"cat".to_vec(),
                    value: b"v3".to_vec(),
                },
            ]
        );
        let resumed = memtable.scan_visible(&range, 10, Some(b"ant"));
        assert_eq!(
            resumed,
            vec![ScanRow {
                key: b"cat".to_vec(),
                value: b"v3".to_vec(),
            }]
        );

        let middle = KeyRange {
            start_bound: Bound::Finite(b"bee".to_vec()),
            end_bound: Bound::PosInf,
        };
        assert_eq!(
            memtable.scan_visible(&middle, 10, None),
            vec![ScanRow {
                key: b"cat".to_vec(),
                value: b"v3".to_vec(),
            }]
        );
    }

    #[test]
    fn memtable_index_keeps_visibility_when_seqnos_arrive_out_of_order() {
        let mut memtable = Memtable::empty();
        memtable.insert_put(9, b"yak", b"v9");
        memtable.insert_delete(11, b"yak");
        memtable.insert_put(10, b"yak", b"v10");

        assert_eq!(
            memtable.get_visible(b"yak", 9).unwrap().value,
            Some(b"v9".to_vec())
        );
        assert_eq!(
            memtable.get_visible(b"yak", 10).unwrap().value,
            Some(b"v10".to_vec())
        );
        assert_eq!(
            memtable.get_visible(b"yak", 11).unwrap().kind,
            RecordKind::Delete
        );
    }

    #[test]
    fn compare_and_shard_helpers_cover_equal_seqno_and_lookup_miss() {
        assert_eq!(
            compare_internal_records(
                &record(b"yak", 9, RecordKind::Delete, None),
                &record(b"yak", 9, RecordKind::Put, Some(b"value"))
            ),
            Ordering::Less
        );

        let range = KeyRange {
            start_bound: Bound::Finite(b"ant".to_vec()),
            end_bound: Bound::Finite(b"yak".to_vec()),
        };
        assert!(range.contains_key(b"bee"));
        assert!(!range.contains_key(b"yak"));
        assert!(range.overlaps_file(b"bee", b"cat"));
        assert!(!range.overlaps_file(b"yak", b"zebra"));

        let shards = vec![
            LogicalShardEntry {
                start_bound: Bound::NegInf,
                end_bound: Bound::Finite(b"m".to_vec()),
                live_size_bytes: 1,
            },
            LogicalShardEntry {
                start_bound: Bound::Finite(b"m".to_vec()),
                end_bound: Bound::PosInf,
                live_size_bytes: 2,
            },
        ];
        assert_eq!(find_logical_shard_covering_key(&shards, b"yak").unwrap(), 1);

        let partial = vec![LogicalShardEntry {
            start_bound: Bound::Finite(b"a".to_vec()),
            end_bound: Bound::Finite(b"m".to_vec()),
            live_size_bytes: 3,
        }];
        assert!(matches!(
            find_logical_shard_covering_key(&partial, b"yak"),
            Err(KalanjiyamError::Corruption(_))
        ));
    }

    #[test]
    fn validation_helpers_cover_empty_shard_maps_and_invalid_limits() {
        assert!(matches!(
            validate_logical_shards(&[]),
            Err(KalanjiyamError::Corruption(_))
        ));
        assert!(matches!(
            validate_page_limits(0, 1),
            Err(KalanjiyamError::InvalidArgument(_))
        ));
        assert!(matches!(
            validate_page_size_bytes(2048),
            Err(KalanjiyamError::InvalidArgument(_))
        ));
    }
}
