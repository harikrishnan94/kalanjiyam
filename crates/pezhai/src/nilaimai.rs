//! Mutable engine state, manifest generations, and immutable read/maintenance plans.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};

use crate::config::{RuntimeConfig, SyncMode};
use crate::error::Error;
use crate::sevai::{Bound, LevelStats, LogicalShardStats, ScanRow};

/// One visible or tombstoned version for one user key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ValueVersion {
    pub(crate) seqno: u64,
    pub(crate) value: Option<Vec<u8>>,
}

/// One direct-engine mutation that is durable only after its WAL record survives.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Mutation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

impl Mutation {
    /// Returns the user key owned by this mutation.
    #[must_use]
    pub(crate) fn key(&self) -> &[u8] {
        match self {
            Self::Put { key, .. } | Self::Delete { key } => key,
        }
    }

    /// Returns the logical bytes contributed by this mutation as one stored record.
    #[must_use]
    pub(crate) fn logical_record_bytes(&self) -> u64 {
        match self {
            Self::Put { key, value } => (key.len() + value.len()) as u64,
            Self::Delete { key } => key.len() as u64,
        }
    }
}

/// The durable record kind used by the shared-data format.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub(crate) enum RecordKind {
    Put = 1,
    Delete = 2,
}

/// One fully materialized internal record used for flushes, compaction, and scans.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct InternalRecord {
    pub(crate) user_key: Vec<u8>,
    pub(crate) seqno: u64,
    pub(crate) kind: RecordKind,
    pub(crate) value: Option<Vec<u8>>,
}

impl InternalRecord {
    /// Returns the logical bytes contributed by this stored record.
    #[must_use]
    pub(crate) fn logical_bytes(&self) -> u64 {
        match &self.value {
            Some(value) => (self.user_key.len() + value.len()) as u64,
            None => self.user_key.len() as u64,
        }
    }
}

impl From<(&[u8], &ValueVersion)> for InternalRecord {
    fn from((user_key, version): (&[u8], &ValueVersion)) -> Self {
        Self {
            user_key: user_key.to_vec(),
            seqno: version.seqno,
            kind: if version.value.is_some() {
                RecordKind::Put
            } else {
                RecordKind::Delete
            },
            value: version.value.clone(),
        }
    }
}

/// One mutable memtable object that may later be frozen and retained by older generations.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Memtable {
    versions: BTreeMap<Vec<u8>, Vec<ValueVersion>>,
    entry_count: u64,
    logical_bytes: u64,
    min_seqno: Option<u64>,
    max_seqno: Option<u64>,
}

impl Memtable {
    /// Returns `true` when the memtable contains no committed records.
    #[must_use]
    pub(crate) fn is_empty(&self) -> bool {
        self.entry_count == 0
    }

    /// Returns the stored record count.
    #[must_use]
    pub(crate) fn entry_count(&self) -> u64 {
        self.entry_count
    }

    /// Returns the accumulated logical bytes of every stored record.
    #[must_use]
    pub(crate) fn logical_bytes(&self) -> u64 {
        self.logical_bytes
    }

    /// Returns the earliest seqno stored in this memtable.
    #[must_use]
    pub(crate) fn min_seqno(&self) -> Option<u64> {
        self.min_seqno
    }

    /// Returns the latest seqno stored in this memtable.
    #[must_use]
    pub(crate) fn max_seqno(&self) -> Option<u64> {
        self.max_seqno
    }

    /// Applies one committed mutation in sequence order.
    pub(crate) fn insert(&mut self, seqno: u64, mutation: &Mutation) {
        let key = mutation.key().to_vec();
        let value = match mutation {
            Mutation::Put { value, .. } => Some(value.clone()),
            Mutation::Delete { .. } => None,
        };

        self.versions
            .entry(key)
            .or_default()
            .insert(0, ValueVersion { seqno, value });
        self.entry_count += 1;
        self.logical_bytes += mutation.logical_record_bytes();
        self.min_seqno = Some(self.min_seqno.map_or(seqno, |current| current.min(seqno)));
        self.max_seqno = Some(self.max_seqno.map_or(seqno, |current| current.max(seqno)));
    }

    /// Returns the first visible version for one key at one snapshot sequence number.
    #[must_use]
    pub(crate) fn visible_version(&self, key: &[u8], snapshot_seqno: u64) -> Option<&ValueVersion> {
        self.versions
            .get(key)
            .and_then(|versions| visible_version(versions, snapshot_seqno))
    }

    /// Clones every internal record in the requested half-open range.
    #[must_use]
    pub(crate) fn collect_internal_records(
        &self,
        start_bound: &Bound,
        end_bound: &Bound,
    ) -> Vec<InternalRecord> {
        let mut records = Vec::new();
        for (key, versions) in &self.versions {
            if !key_in_range(key, start_bound, end_bound) {
                continue;
            }
            for version in versions {
                records.push(InternalRecord::from((key.as_slice(), version)));
            }
        }
        records
    }
}

/// Shared reference to one active or frozen memtable object.
pub(crate) type MemtableRef = Arc<Mutex<Memtable>>;

/// Locks one memtable reference and normalizes poisoning into the engine error type.
pub(crate) fn lock_memtable(memtable: &MemtableRef) -> Result<MutexGuard<'_, Memtable>, Error> {
    memtable
        .lock()
        .map_err(|_| Error::Corruption("memtable mutex was poisoned".into()))
}

/// One immutable frozen memtable reference installed into manifest generations.
#[derive(Clone, Debug)]
pub(crate) struct FrozenMemtableRef {
    pub(crate) frozen_memtable_id: u64,
    pub(crate) memtable: MemtableRef,
    pub(crate) source_first_seqno: u64,
    pub(crate) source_last_seqno: u64,
    pub(crate) source_record_count: u64,
}

impl FrozenMemtableRef {
    /// Returns `true` when the stored source identity matches the expected flush payload fields.
    #[must_use]
    pub(crate) fn matches_source_fields(
        &self,
        first_seqno: u64,
        last_seqno: u64,
        record_count: u64,
    ) -> bool {
        self.source_first_seqno == first_seqno
            && self.source_last_seqno == last_seqno
            && self.source_record_count == record_count
    }
}

/// One manifest entry describing one published shared-data file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FileMeta {
    pub(crate) file_id: u64,
    pub(crate) min_seqno: u64,
    pub(crate) max_seqno: u64,
    pub(crate) entry_count: u64,
    pub(crate) logical_bytes: u64,
    pub(crate) physical_bytes: u64,
    pub(crate) level_no: u16,
    pub(crate) min_user_key: Vec<u8>,
    pub(crate) max_user_key: Vec<u8>,
}

impl FileMeta {
    /// Returns `true` when the file can contain the requested key.
    #[must_use]
    pub(crate) fn covers_key(&self, key: &[u8]) -> bool {
        self.min_user_key.as_slice() <= key && key <= self.max_user_key.as_slice()
    }

    /// Returns `true` when the file overlaps the requested half-open scan range.
    #[must_use]
    pub(crate) fn overlaps_range(&self, start_bound: &Bound, end_bound: &Bound) -> bool {
        bound_allows_start(start_bound, &self.max_user_key)
            && bound_allows_end(end_bound, &self.min_user_key)
    }
}

/// One ordered view of one manifest level.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ManifestLevelView {
    pub(crate) level_no: u16,
    pub(crate) files: Vec<FileMeta>,
}

/// One immutable shared-data source set pinned by `data_generation`.
#[derive(Clone, Debug)]
pub(crate) struct DataManifestSnapshot {
    pub(crate) data_generation: u64,
    pub(crate) active_memtable: MemtableRef,
    pub(crate) frozen_memtables: Vec<Arc<FrozenMemtableRef>>,
    pub(crate) levels: Vec<ManifestLevelView>,
}

impl DataManifestSnapshot {
    /// Returns the current published level view for one level number.
    #[must_use]
    pub(crate) fn level(&self, level_no: u16) -> Option<&ManifestLevelView> {
        self.levels.iter().find(|level| level.level_no == level_no)
    }

    /// Returns every file that can contribute to one point lookup, in read order.
    #[must_use]
    pub(crate) fn files_covering_key(&self, user_key: &[u8]) -> Vec<FileMeta> {
        let mut files = Vec::new();

        if let Some(level_zero) = self.level(0) {
            for file in level_zero.files.iter().rev() {
                if file.covers_key(user_key) {
                    files.push(file.clone());
                }
            }
        }

        for level in self.levels.iter().filter(|level| level.level_no >= 1) {
            if let Some(file) = level.files.iter().find(|file| file.covers_key(user_key)) {
                files.push(file.clone());
            }
        }

        files
    }

    /// Returns every file that overlaps one scan range.
    #[must_use]
    pub(crate) fn files_overlapping_range(
        &self,
        start_bound: &Bound,
        end_bound: &Bound,
    ) -> Vec<FileMeta> {
        let mut files = Vec::new();

        for level in &self.levels {
            for file in &level.files {
                if file.overlaps_range(start_bound, end_bound) {
                    files.push(file.clone());
                }
            }
        }

        files
    }
}

/// Snapshot metadata retained by the engine while a handle is active.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SnapshotEntry {
    pub(crate) snapshot_seqno: u64,
    pub(crate) data_generation: u64,
}

/// One fully captured point read over immutable sources beyond the active memtable fast path.
#[derive(Clone, Debug)]
pub(crate) struct PointReadPlan {
    pub(crate) key: Vec<u8>,
    pub(crate) snapshot_seqno: u64,
    pub(crate) frozen_memtables: Vec<Arc<FrozenMemtableRef>>,
    pub(crate) candidate_files: Vec<FileMeta>,
}

/// One scan plan that owns the immutable source set needed for later cursor reads.
#[derive(Clone, Debug)]
pub(crate) struct ScanPlan {
    pub(crate) start_bound: Bound,
    pub(crate) end_bound: Bound,
    pub(crate) snapshot_seqno: u64,
    pub(crate) active_memtable: MemtableRef,
    pub(crate) frozen_memtables: Vec<Arc<FrozenMemtableRef>>,
    pub(crate) candidate_files: Vec<FileMeta>,
}

impl ScanPlan {
    /// Returns `true` when the scan can finish from the active memtable alone.
    #[must_use]
    pub(crate) fn is_active_only(&self) -> bool {
        self.frozen_memtables.is_empty() && self.candidate_files.is_empty()
    }
}

/// One immutable summary of one built temporary shared-data file.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct BuiltDataFileSummary {
    pub(crate) min_seqno: u64,
    pub(crate) max_seqno: u64,
    pub(crate) entry_count: u64,
    pub(crate) logical_bytes: u64,
    pub(crate) physical_bytes: u64,
    pub(crate) min_user_key: Vec<u8>,
    pub(crate) max_user_key: Vec<u8>,
}

impl BuiltDataFileSummary {
    /// Converts the summary into one manifest-visible file entry.
    #[must_use]
    pub(crate) fn to_file_meta(&self, file_id: u64, level_no: u16) -> FileMeta {
        FileMeta {
            file_id,
            min_seqno: self.min_seqno,
            max_seqno: self.max_seqno,
            entry_count: self.entry_count,
            logical_bytes: self.logical_bytes,
            physical_bytes: self.physical_bytes,
            level_no,
            min_user_key: self.min_user_key.clone(),
            max_user_key: self.max_user_key.clone(),
        }
    }
}

/// One captured flush plan over the oldest frozen memtable.
#[derive(Clone, Debug)]
pub(crate) struct FlushPlan {
    pub(crate) data_generation_expected: u64,
    pub(crate) source: Arc<FrozenMemtableRef>,
    pub(crate) temp_tag: String,
}

/// One temporary flush output built before rename and WAL publication.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FlushBuildResult {
    pub(crate) temp_path: PathBuf,
    pub(crate) summary: BuiltDataFileSummary,
}

/// The durable WAL payload for one accepted flush publication.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct FlushPublishPayload {
    pub(crate) data_generation_expected: u64,
    pub(crate) source_first_seqno: u64,
    pub(crate) source_last_seqno: u64,
    pub(crate) source_record_count: u64,
    pub(crate) output_file_metas: Vec<FileMeta>,
}

/// One captured compaction plan over one oldest L0 file and overlapping L1 files.
#[derive(Clone, Debug)]
pub(crate) struct CompactionPlan {
    pub(crate) data_generation_expected: u64,
    pub(crate) input_files: Vec<FileMeta>,
    pub(crate) output_level_no: u16,
    pub(crate) temp_tag: String,
}

/// One temporary compaction output built before rename and WAL publication.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CompactionBuildResult {
    pub(crate) temp_path: PathBuf,
    pub(crate) summary: BuiltDataFileSummary,
}

/// The durable WAL payload for one accepted compaction publication.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CompactPublishPayload {
    pub(crate) data_generation_expected: u64,
    pub(crate) input_file_ids: Vec<u64>,
    pub(crate) output_file_metas: Vec<FileMeta>,
}

/// Shared mutable state for the manifest-backed shared-data engine.
pub(crate) struct EngineState {
    config: RuntimeConfig,
    engine_instance_id: u64,
    pub(crate) last_committed_seqno: u64,
    pub(crate) durable_seqno: u64,
    pub(crate) data_generation: u64,
    next_seqno: u64,
    pub(crate) next_file_id: u64,
    next_snapshot_id: u64,
    next_frozen_memtable_id: u64,
    snapshots: HashMap<u64, SnapshotEntry>,
    pub(crate) current_manifest: Arc<DataManifestSnapshot>,
    manifests_by_generation: BTreeMap<u64, Arc<DataManifestSnapshot>>,
    current_visible_logical_bytes: HashMap<Vec<u8>, u64>,
    current_live_size_bytes: u64,
    write_stop_message: Option<String>,
}

impl EngineState {
    /// Creates a fresh engine state from one validated config file.
    #[cfg_attr(not(test), allow(dead_code))]
    #[must_use]
    pub(crate) fn new(config: RuntimeConfig, engine_instance_id: u64) -> Self {
        let active_memtable = Arc::new(Mutex::new(Memtable::default()));
        let manifest = Arc::new(DataManifestSnapshot {
            data_generation: 0,
            active_memtable,
            frozen_memtables: Vec::new(),
            levels: Vec::new(),
        });

        let mut manifests_by_generation = BTreeMap::new();
        manifests_by_generation.insert(0, Arc::clone(&manifest));

        Self {
            config,
            engine_instance_id,
            last_committed_seqno: 0,
            durable_seqno: 0,
            data_generation: 0,
            next_seqno: 1,
            next_file_id: 1,
            next_snapshot_id: 1,
            next_frozen_memtable_id: 1,
            snapshots: HashMap::new(),
            current_manifest: manifest,
            manifests_by_generation,
            current_visible_logical_bytes: HashMap::new(),
            current_live_size_bytes: 0,
            write_stop_message: None,
        }
    }

    /// Restores one engine state from replayed files plus uncovered pending mutations.
    pub(crate) fn from_recovery(
        config: RuntimeConfig,
        engine_instance_id: u64,
        data_generation: u64,
        next_file_id: u64,
        last_committed_seqno: u64,
        active_memtable: MemtableRef,
        levels: Vec<ManifestLevelView>,
    ) -> Self {
        let manifest = Arc::new(DataManifestSnapshot {
            data_generation,
            active_memtable,
            frozen_memtables: Vec::new(),
            levels,
        });

        let mut manifests_by_generation = BTreeMap::new();
        manifests_by_generation.insert(data_generation, Arc::clone(&manifest));

        Self {
            config,
            engine_instance_id,
            last_committed_seqno,
            durable_seqno: last_committed_seqno,
            data_generation,
            next_seqno: last_committed_seqno + 1,
            next_file_id,
            next_snapshot_id: 1,
            next_frozen_memtable_id: 1,
            snapshots: HashMap::new(),
            current_manifest: manifest,
            manifests_by_generation,
            current_visible_logical_bytes: HashMap::new(),
            current_live_size_bytes: 0,
            write_stop_message: None,
        }
    }

    /// Returns the configured sync mode for live writes and publish records.
    #[must_use]
    pub(crate) fn sync_mode(&self) -> SyncMode {
        self.config.engine.sync_mode
    }

    /// Returns the configured page size used for new shared-data files.
    #[must_use]
    pub(crate) fn page_size_bytes(&self) -> u32 {
        self.config.engine.page_size_bytes
    }

    /// Returns the engine-instance identifier used to reject stale snapshot handles across reopen.
    #[must_use]
    pub(crate) fn engine_instance_id(&self) -> u64 {
        self.engine_instance_id
    }

    /// Returns the active write-stop error, if a prior WAL append or sync failed.
    #[must_use]
    pub(crate) fn write_stop_error(&self) -> Option<Error> {
        self.write_stop_message
            .as_ref()
            .map(|message| Error::Io(std::io::Error::other(message.clone())))
    }

    /// Reserves the next seqno before the durable bytes for that record are written.
    pub(crate) fn reserve_seqno(&mut self) -> Result<u64, Error> {
        if let Some(error) = self.write_stop_error() {
            return Err(error);
        }

        let seqno = self.next_seqno;
        self.next_seqno += 1;
        Ok(seqno)
    }

    /// Marks the open instance unsafe for further writes after a WAL append or sync failure.
    pub(crate) fn mark_write_failed(&mut self, message: impl Into<String>) {
        self.write_stop_message = Some(message.into());
    }

    /// Allocates and records one snapshot at the current visible frontier.
    pub(crate) fn create_snapshot_entry(&mut self) -> (u64, SnapshotEntry) {
        let snapshot_id = self.next_snapshot_id;
        self.next_snapshot_id += 1;
        let entry = SnapshotEntry {
            snapshot_seqno: self.last_committed_seqno,
            data_generation: self.data_generation,
        };
        self.snapshots.insert(snapshot_id, entry);
        (snapshot_id, entry)
    }

    /// Returns the tracked metadata for one active snapshot identifier.
    #[must_use]
    pub(crate) fn snapshot_entry(&self, snapshot_id: u64) -> Option<SnapshotEntry> {
        self.snapshots.get(&snapshot_id).copied()
    }

    /// Releases one active snapshot entry.
    pub(crate) fn release_snapshot_entry(&mut self, snapshot_id: u64) -> Option<SnapshotEntry> {
        self.snapshots.remove(&snapshot_id)
    }

    /// Returns the manifest pinned by one current or historical generation.
    pub(crate) fn manifest_for_generation(
        &self,
        data_generation: u64,
    ) -> Result<Arc<DataManifestSnapshot>, Error> {
        self.manifests_by_generation
            .get(&data_generation)
            .cloned()
            .ok_or_else(|| {
                Error::Corruption(format!(
                    "manifest generation {data_generation} was not retained"
                ))
            })
    }

    /// Installs the cached current visible logical-byte map used by `stats()`.
    pub(crate) fn install_visible_bytes_cache(
        &mut self,
        visible_logical_bytes: HashMap<Vec<u8>, u64>,
    ) {
        self.current_live_size_bytes = visible_logical_bytes.values().sum();
        self.current_visible_logical_bytes = visible_logical_bytes;
    }

    /// Returns the latest whole-keyspace live logical bytes for `stats()`.
    #[must_use]
    pub(crate) fn current_live_size_bytes(&self) -> u64 {
        self.current_live_size_bytes
    }

    /// Returns the current `levels[]` stats view without performing file I/O.
    #[must_use]
    pub(crate) fn current_level_stats(&self) -> Vec<LevelStats> {
        self.current_manifest
            .levels
            .iter()
            .map(|level| LevelStats {
                level_no: level.level_no as u32,
                file_count: level.files.len() as u64,
                logical_bytes: level.files.iter().map(|file| file.logical_bytes).sum(),
                physical_bytes: level.files.iter().map(|file| file.physical_bytes).sum(),
            })
            .collect()
    }

    /// Returns the milestone-3 whole-keyspace logical-shard stats view.
    #[must_use]
    pub(crate) fn current_logical_shard_stats(&self) -> Vec<LogicalShardStats> {
        vec![LogicalShardStats {
            start_bound: Bound::NegInf,
            end_bound: Bound::PosInf,
            live_size_bytes: self.current_live_size_bytes(),
        }]
    }

    /// Advances the durable frontier after one successful explicit `sync()`.
    pub(crate) fn mark_synced(&mut self, durable_seqno: u64) {
        self.durable_seqno = self.durable_seqno.max(durable_seqno);
    }

    /// Returns the current committed frontier that `sync()` must cover.
    pub(crate) fn sync_target_seqno(&self) -> Result<u64, Error> {
        if let Some(error) = self.write_stop_error() {
            return Err(error);
        }

        Ok(self.last_committed_seqno)
    }

    /// Applies one replayed pending mutation into the recovered active memtable.
    pub(crate) fn apply_recovered_mutation(
        memtable: &MemtableRef,
        seqno: u64,
        mutation: &Mutation,
    ) -> Result<(), Error> {
        lock_memtable(memtable)?.insert(seqno, mutation);
        Ok(())
    }

    /// Applies one live mutation in seqno order and freezes the active memtable if needed.
    pub(crate) fn apply_live_mutation(
        &mut self,
        seqno: u64,
        mutation: &Mutation,
        durably_synced: bool,
    ) -> Result<(), Error> {
        self.advance_committed_seqno(seqno, durably_synced)?;
        lock_memtable(&self.current_manifest.active_memtable)?.insert(seqno, mutation);
        self.apply_visible_logical_bytes(mutation);
        self.maybe_freeze_active_memtable()?;
        Ok(())
    }

    /// Captures the immutable plan for one point read after the active-memtable fast path misses.
    pub(crate) fn plan_point_read(
        &self,
        key: &[u8],
        snapshot_seqno: u64,
        data_generation: u64,
    ) -> Result<PointReadPlan, Error> {
        let manifest = self.manifest_for_generation(data_generation)?;
        debug_assert_eq!(manifest.data_generation, data_generation);
        Ok(PointReadPlan {
            key: key.to_vec(),
            snapshot_seqno,
            frozen_memtables: manifest.frozen_memtables.iter().rev().cloned().collect(),
            candidate_files: manifest.files_covering_key(key),
        })
    }

    /// Captures the immutable source set for one forward scan.
    pub(crate) fn plan_scan(
        &self,
        start_bound: &Bound,
        end_bound: &Bound,
        snapshot_seqno: u64,
        data_generation: u64,
    ) -> Result<ScanPlan, Error> {
        let manifest = self.manifest_for_generation(data_generation)?;
        debug_assert_eq!(manifest.data_generation, data_generation);
        Ok(ScanPlan {
            start_bound: start_bound.clone(),
            end_bound: end_bound.clone(),
            snapshot_seqno,
            active_memtable: Arc::clone(&manifest.active_memtable),
            frozen_memtables: manifest.frozen_memtables.clone(),
            candidate_files: manifest.files_overlapping_range(start_bound, end_bound),
        })
    }

    /// Returns the current active-memtable result, if one visible record exists there.
    pub(crate) fn active_memtable_result(
        &self,
        data_generation: u64,
        key: &[u8],
        snapshot_seqno: u64,
    ) -> Result<Option<Option<Vec<u8>>>, Error> {
        let manifest = self.manifest_for_generation(data_generation)?;
        let memtable = lock_memtable(&manifest.active_memtable)?;
        Ok(memtable
            .visible_version(key, snapshot_seqno)
            .map(|version| version.value.clone()))
    }

    /// Chooses the current oldest frozen memtable as the next flush source, if any exist.
    #[must_use]
    pub(crate) fn plan_flush(&self) -> Option<FlushPlan> {
        let source = self.current_manifest.frozen_memtables.first()?.clone();
        Some(FlushPlan {
            data_generation_expected: self.data_generation,
            source: source.clone(),
            temp_tag: format!(
                "flush-{}-{}-{}-{}",
                self.data_generation,
                source.frozen_memtable_id,
                source.source_first_seqno,
                source.source_last_seqno
            ),
        })
    }

    /// Chooses the next deterministic v1 L0 compaction input set, if any are needed.
    #[must_use]
    pub(crate) fn plan_compaction(&self) -> Option<CompactionPlan> {
        let level_zero = self.current_manifest.level(0)?;
        if level_zero.files.len() < self.config.lsm.l0_file_threshold as usize {
            return None;
        }

        let oldest_l0 = level_zero.files.first()?.clone();
        let mut input_files = vec![oldest_l0.clone()];
        if let Some(level_one) = self.current_manifest.level(1) {
            for file in &level_one.files {
                if ranges_overlap(
                    &oldest_l0.min_user_key,
                    &oldest_l0.max_user_key,
                    &file.min_user_key,
                    &file.max_user_key,
                ) {
                    input_files.push(file.clone());
                }
            }
        }

        Some(CompactionPlan {
            data_generation_expected: self.data_generation,
            input_files,
            output_level_no: 1,
            temp_tag: format!("compact-{}-{}", self.data_generation, oldest_l0.file_id),
        })
    }

    /// Publishes one flush result after the WAL record was accepted.
    pub(crate) fn apply_flush_publish(
        &mut self,
        seqno: u64,
        durably_synced: bool,
        payload: &FlushPublishPayload,
    ) -> Result<(), Error> {
        self.advance_committed_seqno(seqno, durably_synced)?;
        self.validate_flush_publish(payload)?;

        let mut frozen = self.current_manifest.frozen_memtables.clone();
        frozen.remove(0);

        let mut levels = self.current_manifest.levels.clone();
        push_l0_outputs(&mut levels, payload.output_file_metas.clone());
        self.next_file_id = self.next_file_id.max(
            payload
                .output_file_metas
                .iter()
                .map(|file| file.file_id)
                .max()
                .unwrap_or(0)
                + 1,
        );
        self.install_next_manifest_generation(
            Arc::clone(&self.current_manifest.active_memtable),
            frozen,
            levels,
        );
        Ok(())
    }

    /// Publishes one compaction result after the WAL record was accepted.
    pub(crate) fn apply_compact_publish(
        &mut self,
        seqno: u64,
        durably_synced: bool,
        payload: &CompactPublishPayload,
    ) -> Result<(), Error> {
        self.advance_committed_seqno(seqno, durably_synced)?;
        self.validate_compact_publish(payload)?;

        let mut levels = self.current_manifest.levels.clone();
        remove_input_files(&mut levels, &payload.input_file_ids)?;
        add_output_files(&mut levels, payload.output_file_metas.clone());
        self.next_file_id = self.next_file_id.max(
            payload
                .output_file_metas
                .iter()
                .map(|file| file.file_id)
                .max()
                .unwrap_or(0)
                + 1,
        );
        self.install_next_manifest_generation(
            Arc::clone(&self.current_manifest.active_memtable),
            self.current_manifest.frozen_memtables.clone(),
            levels,
        );
        Ok(())
    }

    /// Validates that the current flush source still matches the captured plan.
    pub(crate) fn validate_flush_publish(
        &self,
        payload: &FlushPublishPayload,
    ) -> Result<(), Error> {
        if self.data_generation != payload.data_generation_expected {
            return Err(Error::Stale(format!(
                "flush plan expected data_generation {} but current generation is {}",
                payload.data_generation_expected, self.data_generation
            )));
        }

        let Some(oldest_frozen) = self.current_manifest.frozen_memtables.first() else {
            return Err(Error::Stale(
                "flush source vanished before publication".into(),
            ));
        };
        if !oldest_frozen.matches_source_fields(
            payload.source_first_seqno,
            payload.source_last_seqno,
            payload.source_record_count,
        ) {
            return Err(Error::Stale(
                "flush source no longer matches the oldest frozen memtable".into(),
            ));
        }

        Ok(())
    }

    /// Validates that the exact compaction input set is still present in the current generation.
    pub(crate) fn validate_compact_publish(
        &self,
        payload: &CompactPublishPayload,
    ) -> Result<(), Error> {
        if self.data_generation != payload.data_generation_expected {
            return Err(Error::Stale(format!(
                "compaction plan expected data_generation {} but current generation is {}",
                payload.data_generation_expected, self.data_generation
            )));
        }

        let current_ids: Vec<u64> = self
            .current_manifest
            .levels
            .iter()
            .flat_map(|level| level.files.iter().map(|file| file.file_id))
            .collect();
        if payload
            .input_file_ids
            .iter()
            .any(|file_id| !current_ids.contains(file_id))
        {
            return Err(Error::Stale(
                "compaction input set no longer matches the current manifest".into(),
            ));
        }

        Ok(())
    }

    fn advance_committed_seqno(&mut self, seqno: u64, durably_synced: bool) -> Result<(), Error> {
        let expected_seqno = self.last_committed_seqno + 1;
        if seqno != expected_seqno {
            return Err(Error::Corruption(format!(
                "record seqno {seqno} was committed out of order; expected {expected_seqno}"
            )));
        }

        self.last_committed_seqno = seqno;
        if durably_synced {
            self.durable_seqno = seqno;
        }
        if self.next_seqno <= seqno {
            self.next_seqno = seqno + 1;
        }

        Ok(())
    }

    fn apply_visible_logical_bytes(&mut self, mutation: &Mutation) {
        let key = mutation.key().to_vec();
        let previous = self.current_visible_logical_bytes.remove(&key).unwrap_or(0);
        self.current_live_size_bytes = self.current_live_size_bytes.saturating_sub(previous);

        if let Mutation::Put { value, .. } = mutation {
            let logical_bytes = (key.len() + value.len()) as u64;
            self.current_live_size_bytes += logical_bytes;
            self.current_visible_logical_bytes
                .insert(key, logical_bytes);
        }
    }

    fn maybe_freeze_active_memtable(&mut self) -> Result<(), Error> {
        let needs_freeze = {
            let active = lock_memtable(&self.current_manifest.active_memtable)?;
            !active.is_empty() && active.logical_bytes() >= self.config.lsm.memtable_flush_bytes
        };
        if !needs_freeze {
            return Ok(());
        }

        let frozen_stats = {
            let active = lock_memtable(&self.current_manifest.active_memtable)?;
            if active.is_empty() {
                return Ok(());
            }
            (
                active.min_seqno().expect("non-empty memtable min_seqno"),
                active.max_seqno().expect("non-empty memtable max_seqno"),
                active.entry_count(),
            )
        };
        let frozen = Arc::new(FrozenMemtableRef {
            frozen_memtable_id: self.next_frozen_memtable_id,
            memtable: Arc::clone(&self.current_manifest.active_memtable),
            source_first_seqno: frozen_stats.0,
            source_last_seqno: frozen_stats.1,
            source_record_count: frozen_stats.2,
        });
        self.next_frozen_memtable_id += 1;

        let mut next_frozen = self.current_manifest.frozen_memtables.clone();
        next_frozen.push(frozen);
        self.install_next_manifest_generation(
            Arc::new(Mutex::new(Memtable::default())),
            next_frozen,
            self.current_manifest.levels.clone(),
        );
        Ok(())
    }

    fn install_next_manifest_generation(
        &mut self,
        active_memtable: MemtableRef,
        frozen_memtables: Vec<Arc<FrozenMemtableRef>>,
        levels: Vec<ManifestLevelView>,
    ) {
        let next_generation = self.data_generation + 1;
        let next_manifest = Arc::new(DataManifestSnapshot {
            data_generation: next_generation,
            active_memtable,
            frozen_memtables,
            levels,
        });
        self.manifests_by_generation
            .insert(next_generation, Arc::clone(&next_manifest));
        self.current_manifest = next_manifest;
        self.data_generation = next_generation;
    }
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
    for file_id in input_file_ids {
        let mut removed = false;
        for level in levels.iter_mut() {
            if let Some(index) = level.files.iter().position(|file| file.file_id == *file_id) {
                level.files.remove(index);
                removed = true;
                break;
            }
        }
        if !removed {
            return Err(Error::Stale(format!(
                "input file {file_id} no longer exists in the current manifest"
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
        .expect("new level should exist");
    &mut levels[index]
}

/// Compares two internal records using the spec's full internal-key order.
#[must_use]
pub(crate) fn compare_internal(left: &InternalRecord, right: &InternalRecord) -> Ordering {
    compare_internal_parts(
        left.user_key.as_slice(),
        left.seqno,
        left.kind,
        right.user_key.as_slice(),
        right.seqno,
        right.kind,
    )
}

/// Compares one internal record to a `(user_key, seqno, kind)` search tuple.
#[must_use]
pub(crate) fn compare_internal_to_parts(
    left: &InternalRecord,
    right_user_key: &[u8],
    right_seqno: u64,
    right_kind: RecordKind,
) -> Ordering {
    compare_internal_parts(
        left.user_key.as_slice(),
        left.seqno,
        left.kind,
        right_user_key,
        right_seqno,
        right_kind,
    )
}

fn compare_internal_parts(
    left_user_key: &[u8],
    left_seqno: u64,
    left_kind: RecordKind,
    right_user_key: &[u8],
    right_seqno: u64,
    right_kind: RecordKind,
) -> Ordering {
    match left_user_key.cmp(right_user_key) {
        Ordering::Equal => {}
        non_equal => return non_equal,
    }

    match right_seqno.cmp(&left_seqno) {
        Ordering::Equal => {}
        non_equal => return non_equal,
    }

    left_kind.cmp(&right_kind)
}

fn visible_version(versions: &[ValueVersion], snapshot_seqno: u64) -> Option<&ValueVersion> {
    versions
        .iter()
        .find(|version| version.seqno <= snapshot_seqno)
}

/// Returns `true` when one user key is inside the requested half-open range.
#[must_use]
pub(crate) fn key_in_range(key: &[u8], start_bound: &Bound, end_bound: &Bound) -> bool {
    bound_allows_start(start_bound, key) && bound_allows_end(end_bound, key)
}

fn ranges_overlap(left_min: &[u8], left_max: &[u8], right_min: &[u8], right_max: &[u8]) -> bool {
    left_min <= right_max && right_min <= left_max
}

fn bound_allows_start(bound: &Bound, key: &[u8]) -> bool {
    match bound {
        Bound::NegInf => true,
        Bound::Finite(start) => matches!(
            key.cmp(start.as_slice()),
            Ordering::Equal | Ordering::Greater
        ),
        Bound::PosInf => false,
    }
}

fn bound_allows_end(bound: &Bound, key: &[u8]) -> bool {
    match bound {
        Bound::NegInf => false,
        Bound::Finite(end) => matches!(key.cmp(end.as_slice()), Ordering::Less),
        Bound::PosInf => true,
    }
}

/// Merges one sorted internal-record set into visible ascending scan rows.
#[must_use]
pub(crate) fn visible_scan_rows(
    records: &mut [InternalRecord],
    snapshot_seqno: u64,
) -> Vec<ScanRow> {
    records.sort_by(compare_internal);

    let mut rows = Vec::new();
    let mut index = 0;
    while index < records.len() {
        let key = records[index].user_key.clone();
        let mut visible = None;
        while index < records.len() && records[index].user_key == key {
            if records[index].seqno <= snapshot_seqno {
                visible = Some(records[index].clone());
                while index < records.len() && records[index].user_key == key {
                    index += 1;
                }
                break;
            }
            index += 1;
        }
        while index < records.len() && records[index].user_key == key {
            index += 1;
        }
        if let Some(record) = visible
            && let Some(value) = record.value
        {
            rows.push(ScanRow { key, value });
        }
    }

    rows
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};

    use crate::config::parse_runtime_config;
    use crate::error::Error;

    use super::{
        Bound, CompactPublishPayload, EngineState, FileMeta, FlushPublishPayload, InternalRecord,
        ManifestLevelView, Memtable, Mutation, RecordKind, compare_internal_to_parts,
        visible_scan_rows,
    };

    fn test_config() -> crate::config::RuntimeConfig {
        parse_runtime_config("[sevai]\nlisten_addr = \"127.0.0.1:0\"\n").unwrap()
    }

    #[test]
    fn memtable_collects_internal_records_in_internal_order() {
        let mut memtable = Memtable::default();
        memtable.insert(
            1,
            &Mutation::Put {
                key: b"ant".to_vec(),
                value: b"v1".to_vec(),
            },
        );
        memtable.insert(
            2,
            &Mutation::Delete {
                key: b"ant".to_vec(),
            },
        );
        memtable.insert(
            3,
            &Mutation::Put {
                key: b"bee".to_vec(),
                value: b"v2".to_vec(),
            },
        );

        let records = memtable.collect_internal_records(&Bound::NegInf, &Bound::PosInf);
        assert_eq!(records.len(), 3);
        assert_eq!(records[0].user_key, b"ant".to_vec());
        assert_eq!(records[0].seqno, 2);
        assert_eq!(records[1].user_key, b"ant".to_vec());
        assert_eq!(records[1].seqno, 1);
        assert_eq!(records[2].user_key, b"bee".to_vec());
    }

    #[test]
    fn visible_scan_rows_keep_first_visible_put_per_key() {
        let mut records = vec![
            InternalRecord {
                user_key: b"ant".to_vec(),
                seqno: 4,
                kind: RecordKind::Delete,
                value: None,
            },
            InternalRecord {
                user_key: b"ant".to_vec(),
                seqno: 3,
                kind: RecordKind::Put,
                value: Some(b"v1".to_vec()),
            },
            InternalRecord {
                user_key: b"bee".to_vec(),
                seqno: 2,
                kind: RecordKind::Put,
                value: Some(b"v2".to_vec()),
            },
        ];

        let rows = visible_scan_rows(&mut records, 3);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].key, b"ant".to_vec());
        assert_eq!(rows[1].key, b"bee".to_vec());

        let rows_after_delete = visible_scan_rows(&mut records, 4);
        assert_eq!(rows_after_delete.len(), 1);
        assert_eq!(rows_after_delete[0].key, b"bee".to_vec());
    }

    #[test]
    fn compare_internal_to_parts_matches_search_key_ordering() {
        let record = InternalRecord {
            user_key: b"yak".to_vec(),
            seqno: 8,
            kind: RecordKind::Put,
            value: Some(b"v2".to_vec()),
        };

        assert_eq!(
            compare_internal_to_parts(&record, b"yak", 8, RecordKind::Delete),
            std::cmp::Ordering::Less
        );
    }

    #[test]
    fn flush_publish_rejects_generation_and_source_mismatches() {
        let mut state = EngineState::new(test_config(), 41);
        state.install_visible_bytes_cache(HashMap::new());
        state
            .apply_live_mutation(
                1,
                &Mutation::Put {
                    key: b"ant".to_vec(),
                    value: b"a".to_vec(),
                },
                true,
            )
            .unwrap();
        state.config.lsm.memtable_flush_bytes = 1;
        state
            .apply_live_mutation(
                2,
                &Mutation::Put {
                    key: b"bee".to_vec(),
                    value: b"b".to_vec(),
                },
                true,
            )
            .unwrap();

        let stale_generation = FlushPublishPayload {
            data_generation_expected: state.data_generation + 1,
            source_first_seqno: 1,
            source_last_seqno: 2,
            source_record_count: 2,
            output_file_metas: vec![FileMeta {
                file_id: 1,
                min_seqno: 1,
                max_seqno: 2,
                entry_count: 2,
                logical_bytes: 4,
                physical_bytes: 4096,
                level_no: 0,
                min_user_key: b"ant".to_vec(),
                max_user_key: b"bee".to_vec(),
            }],
        };
        assert!(matches!(
            state.validate_flush_publish(&stale_generation),
            Err(Error::Stale(_))
        ));

        let stale_source = FlushPublishPayload {
            data_generation_expected: state.data_generation,
            source_first_seqno: 99,
            source_last_seqno: 100,
            source_record_count: 2,
            output_file_metas: stale_generation.output_file_metas.clone(),
        };
        assert!(matches!(
            state.validate_flush_publish(&stale_source),
            Err(Error::Stale(_))
        ));
    }

    #[test]
    fn compaction_publish_rejects_generation_and_input_mismatches() {
        let state = EngineState::from_recovery(
            test_config(),
            52,
            1,
            4,
            3,
            Arc::new(Mutex::new(Memtable::default())),
            vec![
                ManifestLevelView {
                    level_no: 0,
                    files: vec![FileMeta {
                        file_id: 1,
                        min_seqno: 1,
                        max_seqno: 1,
                        entry_count: 1,
                        logical_bytes: 2,
                        physical_bytes: 4096,
                        level_no: 0,
                        min_user_key: b"ant".to_vec(),
                        max_user_key: b"ant".to_vec(),
                    }],
                },
                ManifestLevelView {
                    level_no: 1,
                    files: vec![FileMeta {
                        file_id: 2,
                        min_seqno: 2,
                        max_seqno: 2,
                        entry_count: 1,
                        logical_bytes: 2,
                        physical_bytes: 4096,
                        level_no: 1,
                        min_user_key: b"ant".to_vec(),
                        max_user_key: b"yak".to_vec(),
                    }],
                },
            ],
        );

        let stale_generation = CompactPublishPayload {
            data_generation_expected: 9,
            input_file_ids: vec![1, 2],
            output_file_metas: vec![FileMeta {
                file_id: 3,
                min_seqno: 1,
                max_seqno: 2,
                entry_count: 2,
                logical_bytes: 4,
                physical_bytes: 4096,
                level_no: 1,
                min_user_key: b"ant".to_vec(),
                max_user_key: b"yak".to_vec(),
            }],
        };
        assert!(matches!(
            state.validate_compact_publish(&stale_generation),
            Err(Error::Stale(_))
        ));

        let stale_inputs = CompactPublishPayload {
            data_generation_expected: state.data_generation,
            input_file_ids: vec![1, 9],
            output_file_metas: stale_generation.output_file_metas,
        };
        assert!(matches!(
            state.validate_compact_publish(&stale_inputs),
            Err(Error::Stale(_))
        ));
    }
}
