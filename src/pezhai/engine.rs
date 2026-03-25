//! Host-agnostic storage-engine state and async API for `Pezhai`.
//!
//! This module owns storage semantics, recovery, snapshot lifecycle, durable
//! publication, and immutable plan capture. The in-process server host adds
//! request admission and runtime policy around this engine surface.

use std::cmp::Ordering;
use std::collections::{BTreeSet, BinaryHeap};
use std::fs::{self, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::error::KalanjiyamError;
use crate::pezhai::codec::{
    CompactPublishPayload, CurrentFile, FlushPublishPayload, LogicalShardInstallPayload, WalRecord,
    WalRecordPayload, WalSegmentFooter, WalSegmentHeader, current_relative_path, decode_current,
    decode_wal_record, decode_wal_segment_header, empty_open_segment_first_seqno,
    encode_wal_record, encode_wal_segment_footer, encode_wal_segment_header, resolve_store_path,
    wal_relative_path,
};
use crate::pezhai::config::PezhaiConfig;
use crate::pezhai::durable::{
    CheckpointState, DataFileRecordIterator, build_file_meta, find_visible_record_in_data_file,
    install_current, load_checkpoint_file, load_data_file, validate_data_file,
    write_checkpoint_file, write_data_file,
};
use crate::pezhai::types::{
    ActiveMemtable, Bound, DataManifestIndex, DataManifestSnapshot, FileMeta, FrozenMemtable,
    GetResponse, InternalRecord, KeyRange, LogicalShardEntry, Memtable, RecordKind, ScanPage,
    ScanRow, SnapshotHandle, StatsResponse, SyncMode, SyncResponse, build_level_stats,
    build_logical_shard_stats, compare_internal_records, validate_key, validate_logical_shards,
    validate_page_limits, validate_value,
};

/// Immutable point-read plan captured by the engine for a host to execute.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GetPlan {
    /// Requested user key.
    pub key: Vec<u8>,
    /// Pinned read snapshot seqno.
    pub snapshot_seqno: u64,
    /// Pinned shared-data generation.
    pub data_generation: u64,
    /// Immutable frozen memtables for the plan.
    pub frozen_memtables: Vec<Arc<FrozenMemtable>>,
    /// Candidate files in engine-defined search order.
    pub candidate_files: Vec<FileMeta>,
}

/// Immutable range-scan plan captured by the engine for a host to execute.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ScanPlan {
    /// Session or task-local scan identifier.
    pub scan_id: u64,
    /// Snapshot pinned for the whole scan session.
    pub snapshot_handle: SnapshotHandle,
    /// Requested half-open key range.
    pub range: KeyRange,
    /// Greatest emitted key from a prior page, if any.
    pub resume_after_key: Option<Vec<u8>>,
    /// Page record cap.
    pub max_records_per_page: u32,
    /// Page byte cap.
    pub max_bytes_per_page: u32,
    /// Immutable active memtable snapshot for the pinned generation.
    pub active_memtable: ActiveMemtable,
    /// Immutable frozen memtables for the pinned generation.
    pub frozen_memtables: Vec<Arc<FrozenMemtable>>,
    /// Candidate files for the pinned generation.
    pub candidate_files: Vec<FileMeta>,
}

/// Engine-owned durability waiter target.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct WalWait {
    /// Seqno that must be covered by the durable frontier.
    pub target_seqno: u64,
}

/// One engine write decision that a host may complete immediately or wait on.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum WriteDecision {
    /// The write is fully acknowledged after owner-thread mutation.
    Immediate,
    /// The host must wait for the WAL durable frontier to cover the write.
    Wait(WalWait),
}

/// Immutable flush task input captured by the engine.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FlushTaskPlan {
    /// Generation expected at publish time.
    pub source_generation: u64,
    /// Oldest frozen memtable id expected at publish time.
    pub source_frozen_memtable_id: u64,
    /// Exact immutable source memtable reference captured into the plan.
    pub source_memtable: Arc<FrozenMemtable>,
    /// Reserved output file ids for worker output.
    pub output_file_ids: Vec<u64>,
}

/// Worker-prepared flush output validated by the owner before publication.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct FlushTaskResult {
    /// Generation captured when the worker started.
    pub source_generation: u64,
    /// Oldest frozen memtable id captured by the worker.
    pub source_frozen_memtable_id: u64,
    /// Prepared output files in stable order.
    pub outputs: Vec<FileMeta>,
}

/// Immutable compaction task input captured by the engine.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CompactTaskPlan {
    /// Generation expected at publish time.
    pub source_generation: u64,
    /// Exact input file ids expected at publish time.
    pub input_file_ids: Vec<u64>,
    /// Exact input file metadata.
    pub input_files: Vec<FileMeta>,
    /// Reserved output file ids for worker output.
    pub output_file_ids: Vec<u64>,
}

/// Worker-prepared compaction output validated by the owner before publication.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CompactTaskResult {
    /// Generation captured when the worker started.
    pub source_generation: u64,
    /// Exact input file ids captured by the worker.
    pub input_file_ids: Vec<u64>,
    /// Prepared replacement files in stable order.
    pub outputs: Vec<FileMeta>,
}

/// Immutable checkpoint task input captured by the engine.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CheckpointTaskPlan {
    /// Highest committed seqno to persist in the checkpoint.
    pub checkpoint_max_seqno: u64,
    /// Current manifest generation to persist.
    pub manifest_generation: u64,
    /// Checkpoint state to materialize durably.
    pub checkpoint_state: CheckpointState,
}

/// Worker-prepared checkpoint artifact validated before `CURRENT` publication.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct CheckpointTaskResult {
    /// Highest committed seqno represented by the artifact.
    pub checkpoint_max_seqno: u64,
    /// Shared-data generation represented by the artifact.
    pub manifest_generation: u64,
    /// Prepared `CURRENT` payload naming the durable checkpoint file.
    pub current: CurrentFile,
}

/// One deletable durable artifact selected by engine-side GC capture.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum DurableFileRef {
    /// One shared data file.
    Data {
        /// Stable file identifier.
        file_id: u64,
        /// Canonical absolute path.
        path: PathBuf,
    },
    /// One metadata checkpoint file.
    MetadataCheckpoint {
        /// Stable checkpoint generation.
        checkpoint_generation: u64,
        /// Canonical absolute path.
        path: PathBuf,
    },
}

impl DurableFileRef {
    /// Returns the canonical durable path for worker-side deletion.
    ///
    /// # Complexity
    /// Time: O(1) constant-time match and reference.
    /// Space: O(1) reuses existing path storage.
    #[must_use]
    pub fn path(&self) -> &Path {
        match self {
            Self::Data { path, .. } | Self::MetadataCheckpoint { path, .. } => path,
        }
    }
}

/// Owner-visible extra pins that the engine cannot infer from its own state.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GcOwnerPins {
    /// Manifest generations referenced by in-flight worker plans.
    pub manifest_generations: BTreeSet<u64>,
    /// Data files referenced by in-flight worker plans or unpublished outputs.
    pub data_file_ids: BTreeSet<u64>,
    /// Metadata checkpoint generations referenced by in-flight worker plans.
    pub checkpoint_generations: BTreeSet<u64>,
}

/// Immutable garbage-collection task selected by the engine.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GcTaskPlan {
    /// Durable artifacts already known to be safe for deletion.
    pub deletable_files: Vec<DurableFileRef>,
    /// Historical manifest generations safe to prune after deletion.
    pub prunable_manifest_generations: Vec<u64>,
}

/// Worker-reported garbage-collection completion validated by the owner.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct GcTaskResult {
    /// Durable artifacts successfully deleted by the worker.
    pub deleted_files: Vec<DurableFileRef>,
    /// Historical manifest generations the owner may now drop.
    pub pruned_manifest_generations: Vec<u64>,
}

/// One engine read decision that a host may complete immediately or delegate.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ReadDecision {
    /// The engine already knows the final point-read result.
    ImmediateGet(GetResponse),
    /// The engine already knows the final scan-page result.
    ImmediateScan(ScanPage),
    /// Immutable point-read work for a host worker.
    GetPlan(GetPlan),
    /// Immutable scan-page work for a host worker.
    ScanPlan(ScanPlan),
}

/// One engine durability decision that a host may complete immediately or wait on.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SyncDecision {
    /// The current durable frontier already covers the requested seqno.
    Immediate(SyncResponse),
    /// The host must wait for the durable frontier to advance.
    Wait(WalWait),
}

#[derive(Clone, Debug, Eq, PartialEq)]
struct WalSegmentDescriptor {
    path: PathBuf,
    first_seqno: u64,
    last_seqno: Option<u64>,
    is_open: bool,
}

/// Mutable storage-engine state for one open engine instance.
#[derive(Debug)]
pub struct EngineState {
    /// Validated configuration used to open the engine.
    pub config: PezhaiConfig,
    /// Store root directory resolved from the config path.
    pub store_dir: PathBuf,
    /// Monotonic id for this open engine instance.
    pub engine_instance_id: u64,
    /// Last committed seqno visible to latest-only reads.
    pub last_committed_seqno: u64,
    /// Durable seqno frontier already covered by WAL sync.
    pub durable_seqno: u64,
    /// Current shared-data generation.
    pub data_generation: u64,
    /// Next seqno allocator value.
    pub next_seqno: u64,
    /// Next file-id allocator value.
    pub next_file_id: u64,
    /// Next snapshot-handle id.
    pub next_snapshot_id: u64,
    /// Next frozen-memtable id.
    pub next_frozen_memtable_id: u64,
    /// Next metadata-checkpoint generation.
    pub next_checkpoint_generation: u64,
    /// Current active snapshot table indexed by `snapshot_id - 1`.
    ///
    /// Slots become `None` after release so create/release/validate stay
    /// constant-time without renumbering existing handles.
    pub active_snapshots: Vec<Option<SnapshotHandle>>,
    /// Current latest-only logical-shard map.
    pub current_logical_shards: Vec<LogicalShardEntry>,
    /// Retained manifest history keyed by generation.
    pub manifest_index: DataManifestIndex,
    /// Current shared-data manifest snapshot.
    pub current_manifest: DataManifestSnapshot,
    /// Whether the open instance may still accept writes.
    pub write_admission_open: bool,
    /// Active open WAL segment path.
    pub active_wal_path: PathBuf,
    /// First seqno in the active open WAL segment or `NONE_U64` for empty.
    pub active_wal_first_seqno: u64,
    /// Count of committed records already appended to the active segment.
    pub active_wal_record_count: u64,
    /// Payload bytes already appended to the active segment.
    pub active_wal_payload_bytes_used: u64,
    /// Record bytes appended since the last published sync batch.
    pub unsynced_wal_bytes: u64,
    /// Most recently installed `CURRENT` pointer.
    pub durable_current: Option<CurrentFile>,
}

impl EngineState {
    fn empty(config: PezhaiConfig) -> Self {
        let mut manifest_index = DataManifestIndex::default();
        let current_manifest = DataManifestSnapshot::default();
        manifest_index
            .by_generation
            .insert(current_manifest.data_generation, current_manifest.clone());
        Self {
            store_dir: config.store_dir.clone(),
            config,
            engine_instance_id: 1,
            last_committed_seqno: 0,
            durable_seqno: 0,
            data_generation: 0,
            next_seqno: 1,
            next_file_id: 1,
            next_snapshot_id: 1,
            next_frozen_memtable_id: 1,
            next_checkpoint_generation: 1,
            active_snapshots: Vec::new(),
            current_logical_shards: vec![LogicalShardEntry {
                start_bound: Bound::NegInf,
                end_bound: Bound::PosInf,
                live_size_bytes: 0,
            }],
            manifest_index,
            current_manifest,
            write_admission_open: true,
            active_wal_path: PathBuf::new(),
            active_wal_first_seqno: empty_open_segment_first_seqno(),
            active_wal_record_count: 0,
            active_wal_payload_bytes_used: 0,
            unsynced_wal_bytes: 0,
            durable_current: None,
        }
    }
}

/// Host-agnostic storage-engine handle used by direct embeddings and tests.
#[derive(Debug)]
pub struct PezhaiEngine {
    state: EngineState,
}

impl PezhaiEngine {
    /// Opens one engine instance from the provided `store/config.toml` path.
    ///
    /// # Complexity
    /// Time: O(p + c + w + r * log n), where `p` is the config-path length,
    /// `c` is the size of `config.toml`, `w` is the total durable bytes read
    /// during checkpoint and WAL recovery, `r` is the number of replayed WAL
    /// records, and `n` is the number of records retained in the recovered
    /// memtable. Filesystem directory-scan and read latency is additional.
    /// Space: O(r + f + s), where `f` is the number of recovered manifest files
    /// and `s` is the number of logical shard entries retained in memory.
    pub async fn open(config_path: impl AsRef<Path>) -> std::result::Result<Self, KalanjiyamError> {
        let config = PezhaiConfig::load(config_path)?;
        let state = recover_engine_state(config)?;
        Ok(Self { state })
    }

    /// Returns an immutable view of the current engine state.
    ///
    /// # Complexity
    /// Time: O(1).
    /// Space: O(1).
    #[must_use]
    pub fn state(&self) -> &EngineState {
        &self.state
    }

    /// Creates one snapshot handle pinned by `(snapshot_seqno, data_generation)`.
    ///
    /// # Complexity
    /// Time: Amortized O(1).
    /// Space: Amortized O(1) for the new snapshot slot.
    pub async fn create_snapshot(
        &mut self,
    ) -> std::result::Result<SnapshotHandle, KalanjiyamError> {
        let handle = SnapshotHandle {
            engine_instance_id: self.state.engine_instance_id,
            snapshot_id: self.state.next_snapshot_id,
            snapshot_seqno: self.state.last_committed_seqno,
            data_generation: self.state.data_generation,
        };
        self.state.next_snapshot_id += 1;
        self.state.active_snapshots.push(Some(handle.clone()));
        Ok(handle)
    }

    /// Releases one previously allocated snapshot handle.
    ///
    /// # Complexity
    /// Time: O(1).
    /// Space: O(1).
    pub async fn release_snapshot(
        &mut self,
        handle: &SnapshotHandle,
    ) -> std::result::Result<(), KalanjiyamError> {
        validate_snapshot_handle(&self.state, handle)?;
        let slot_index = snapshot_slot_index(handle.snapshot_id)?;
        self.state.active_snapshots[slot_index] = None;
        Ok(())
    }

    /// Appends one `Put` mutation, updates the active memtable, and obeys sync mode.
    ///
    /// # Complexity
    /// Time: O(log u + log n + p), where `u` is the number of distinct keys in
    /// the active memtable, `n` is the number of versions stored for the
    /// written key, and `p` is the encoded WAL record size. WAL append latency
    /// is additional, and `PerWrite` mode also adds WAL `fsync` latency. A
    /// segment rollover adds one bounded footer/write/rename sequence.
    /// Space: O(p) for the encoded WAL record and cloned key/value bytes.
    pub async fn put(
        &mut self,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> std::result::Result<(), KalanjiyamError> {
        match self.decide_put(key, value)? {
            WriteDecision::Immediate => Ok(()),
            WriteDecision::Wait(wait) => {
                sync_active_wal(&self.state.active_wal_path)?;
                let _ = self.complete_inline_wal_sync(wait.target_seqno)?;
                Ok(())
            }
        }
    }

    /// Appends one `Delete` mutation, updates the active memtable, and obeys sync mode.
    ///
    /// # Complexity
    /// Time: O(log u + log n + p), where `u` is the number of distinct keys in
    /// the active memtable, `n` is the number of versions stored for the
    /// written key, and `p` is the encoded WAL record size. WAL append latency
    /// is additional, and `PerWrite` mode also adds WAL `fsync` latency.
    /// Space: O(p) for the encoded WAL record and cloned key bytes.
    pub async fn delete(&mut self, key: Vec<u8>) -> std::result::Result<(), KalanjiyamError> {
        match self.decide_delete(key)? {
            WriteDecision::Immediate => Ok(()),
            WriteDecision::Wait(wait) => {
                sync_active_wal(&self.state.active_wal_path)?;
                let _ = self.complete_inline_wal_sync(wait.target_seqno)?;
                Ok(())
            }
        }
    }

    /// Reads one user key at either the latest view or one supplied snapshot.
    ///
    /// # Complexity
    /// Time: O(log g + m * log n + f + b) worst case, where `g` is the number
    /// of retained manifest generations, `m` is the number of frozen memtables
    /// searched, `n` is the number of versions checked per memtable lookup,
    /// `f` is the number of candidate files selected from the manifest, and
    /// `b` is the total number of file bytes read while validating and probing
    /// those files. Filesystem read latency is additional.
    /// Space: O(f + k + v) for the candidate-file list and any returned record.
    pub async fn get(
        &self,
        key: Vec<u8>,
        snapshot: Option<&SnapshotHandle>,
    ) -> std::result::Result<GetResponse, KalanjiyamError> {
        validate_key(&key)?;
        let read_snapshot = resolve_snapshot(&self.state, snapshot)?;
        let manifest = manifest_for_generation(&self.state, read_snapshot.data_generation)?;
        let record = find_visible_record_in_manifest(
            &self.state.store_dir,
            manifest,
            &key,
            read_snapshot.snapshot_seqno,
        )?;
        Ok(GetResponse {
            found: record
                .as_ref()
                .is_some_and(|record| record.kind == RecordKind::Put),
            value: record.and_then(|record| {
                if record.kind == RecordKind::Put {
                    record.value
                } else {
                    None
                }
            }),
            observation_seqno: read_snapshot.snapshot_seqno,
            data_generation: read_snapshot.data_generation,
        })
    }

    /// Scans one half-open range at either the latest view or one supplied snapshot.
    ///
    /// # Complexity
    /// Time: O(log g + f + b + n * log(m + f + 1)) worst case, where `g` is
    /// the number of retained manifest generations, `m` is the number of
    /// frozen memtables, `f` is the number of overlapping files, `b` is the
    /// total number of file bytes read while opening those files, and `n` is
    /// the number of records consumed by the merge. Filesystem read latency is
    /// additional.
    /// Space: O(r * (k + v) + m + f) for the returned rows, merge heap, and
    /// source iterators.
    pub async fn scan(
        &self,
        range: KeyRange,
        snapshot: Option<&SnapshotHandle>,
    ) -> std::result::Result<Vec<ScanRow>, KalanjiyamError> {
        range.validate()?;
        let read_snapshot = resolve_snapshot(&self.state, snapshot)?;
        let manifest = manifest_for_generation(&self.state, read_snapshot.data_generation)?;
        scan_manifest(
            &self.state.store_dir,
            manifest,
            &range,
            None,
            read_snapshot.snapshot_seqno,
            u32::MAX,
            u32::MAX,
        )
        .map(|page| page.rows)
    }

    /// Waits until the durable frontier covers the current committed WAL frontier.
    ///
    /// # Complexity
    /// Time: O(1) algorithmic work, plus WAL `fsync` latency when the durable
    /// frontier still trails the committed frontier.
    /// Space: O(1).
    pub async fn sync(&mut self) -> std::result::Result<SyncResponse, KalanjiyamError> {
        if self.state.durable_seqno >= self.state.last_committed_seqno {
            return Ok(SyncResponse {
                durable_seqno: self.state.durable_seqno,
            });
        }
        sync_active_wal(&self.state.active_wal_path)?;
        self.state.durable_seqno = self.state.last_committed_seqno;
        self.state.unsynced_wal_bytes = 0;
        Ok(SyncResponse {
            durable_seqno: self.state.durable_seqno,
        })
    }

    /// Returns the latest global shared-data stats and logical-shard map.
    ///
    /// # Complexity
    /// Time: O(l + f + s * k), where `l` is the number of levels, `f` is the
    /// total number of manifest files, `s` is the number of logical shards, and
    /// `k` is the boundary-key length cloned into the shard stats.
    /// Space: O(l + s * k) for the returned stats vectors.
    pub async fn stats(&self) -> std::result::Result<StatsResponse, KalanjiyamError> {
        Ok(StatsResponse {
            observation_seqno: self.state.last_committed_seqno,
            data_generation: self.state.data_generation,
            levels: build_level_stats(&self.state.current_manifest.levels),
            logical_shards: build_logical_shard_stats(&self.state.current_logical_shards),
        })
    }

    /// Captures the engine-owned decision for one latest-only external `Get`.
    ///
    /// # Complexity
    /// Time: O(log u + m + f) worst case, where `u` is the number of distinct
    /// keys in the active memtable, `m` is the number of frozen memtables
    /// inspected, and `f` is the number of manifest files inspected while
    /// building the candidate-file list.
    /// Space: O(m + f + k) when returning a `GetPlan`; the immediate path is
    /// O(1) additional space.
    pub fn decide_current_read(
        &self,
        key: &[u8],
    ) -> std::result::Result<ReadDecision, KalanjiyamError> {
        validate_key(key)?;
        let snapshot = SnapshotHandle {
            engine_instance_id: self.state.engine_instance_id,
            snapshot_id: 0,
            snapshot_seqno: self.state.last_committed_seqno,
            data_generation: self.state.data_generation,
        };
        let manifest = &self.state.current_manifest;
        if let Some(record) = manifest
            .active_memtable_ref
            .get_visible(key, snapshot.snapshot_seqno)
        {
            return Ok(ReadDecision::ImmediateGet(build_get_response(
                record,
                snapshot.snapshot_seqno,
                snapshot.data_generation,
            )));
        }

        let candidate_files = files_covering_key(manifest, key);
        if manifest.frozen_memtable_refs.is_empty() && candidate_files.is_empty() {
            return Ok(ReadDecision::ImmediateGet(GetResponse {
                found: false,
                value: None,
                observation_seqno: snapshot.snapshot_seqno,
                data_generation: snapshot.data_generation,
            }));
        }

        Ok(ReadDecision::GetPlan(GetPlan {
            key: key.to_vec(),
            snapshot_seqno: snapshot.snapshot_seqno,
            data_generation: snapshot.data_generation,
            frozen_memtables: manifest
                .frozen_memtable_refs
                .iter()
                .rev()
                .cloned()
                .collect(),
            candidate_files,
        }))
    }

    /// Captures the engine-owned decision for one paged scan fetch.
    ///
    /// # Complexity
    /// Time: O(log g + a + m + f + r * (k + v)) worst case, where `g` is the
    /// number of retained manifest generations, `a` is the number of active
    /// memtable records cloned into a `ScanPlan`, `m` is the number of frozen
    /// memtables, `f` is the number of overlapping files, and `r` is the
    /// number of rows produced by the immediate active-memtable-only path.
    /// Space: O(a + m + f + k) when returning a `ScanPlan`, or
    /// O(r * (k + v)) on the immediate path.
    pub fn decide_scan_page(
        &self,
        scan_id: u64,
        snapshot_handle: &SnapshotHandle,
        range: &KeyRange,
        resume_after_key: Option<&[u8]>,
        max_records_per_page: u32,
        max_bytes_per_page: u32,
    ) -> std::result::Result<ReadDecision, KalanjiyamError> {
        validate_snapshot_handle(&self.state, snapshot_handle)?;
        range.validate()?;
        validate_page_limits(max_records_per_page, max_bytes_per_page)?;
        let manifest = manifest_for_generation(&self.state, snapshot_handle.data_generation)?;
        if manifest.frozen_memtable_refs.is_empty() && manifest.levels.iter().all(Vec::is_empty) {
            let page = page_rows(
                manifest.active_memtable_ref.scan_visible(
                    range,
                    snapshot_handle.snapshot_seqno,
                    resume_after_key,
                ),
                max_records_per_page,
                max_bytes_per_page,
            );
            return Ok(ReadDecision::ImmediateScan(page));
        }

        Ok(ReadDecision::ScanPlan(ScanPlan {
            scan_id,
            snapshot_handle: snapshot_handle.clone(),
            range: range.clone(),
            resume_after_key: resume_after_key.map(ToOwned::to_owned),
            max_records_per_page,
            max_bytes_per_page,
            active_memtable: (*manifest.active_memtable_ref).clone(),
            frozen_memtables: manifest.frozen_memtable_refs.clone(),
            candidate_files: files_overlapping_range(manifest, range),
        }))
    }

    /// Captures one durability wait decision for the current WAL frontier.
    ///
    /// # Complexity
    /// Time: O(1).
    /// Space: O(1).
    pub fn decide_sync_wait(&self) -> std::result::Result<SyncDecision, KalanjiyamError> {
        if self.state.durable_seqno >= self.state.last_committed_seqno {
            Ok(SyncDecision::Immediate(SyncResponse {
                durable_seqno: self.state.durable_seqno,
            }))
        } else {
            Ok(SyncDecision::Wait(WalWait {
                target_seqno: self.state.last_committed_seqno,
            }))
        }
    }

    /// Applies one `Put` mutation and returns the durability acknowledgement route.
    pub(crate) fn decide_put(
        &mut self,
        key: Vec<u8>,
        value: Vec<u8>,
    ) -> std::result::Result<WriteDecision, KalanjiyamError> {
        validate_key(&key)?;
        validate_value(&value)?;
        let seqno = self.append_live_record(WalRecordPayload::Put {
            key: key.clone(),
            value: value.clone(),
        })?;
        self.mutate_current_active_memtable(|active| {
            active.insert_put(seqno, &key, &value);
        });
        self.state.last_committed_seqno = seqno;
        self.freeze_if_needed()?;
        Ok(self.decide_write_ack(seqno))
    }

    /// Applies one `Delete` mutation and returns the durability acknowledgement route.
    pub(crate) fn decide_delete(
        &mut self,
        key: Vec<u8>,
    ) -> std::result::Result<WriteDecision, KalanjiyamError> {
        validate_key(&key)?;
        let seqno = self.append_live_record(WalRecordPayload::Delete { key: key.clone() })?;
        self.mutate_current_active_memtable(|active| {
            active.insert_delete(seqno, &key);
        });
        self.state.last_committed_seqno = seqno;
        self.freeze_if_needed()?;
        Ok(self.decide_write_ack(seqno))
    }

    /// Returns the active WAL segment path for dedicated sync-thread execution.
    ///
    /// # Complexity
    /// Time: O(1).
    /// Space: O(1).
    #[must_use]
    pub fn active_wal_path(&self) -> &Path {
        &self.state.active_wal_path
    }

    /// Returns the number of appended record bytes not yet covered by one published sync batch.
    #[must_use]
    pub(crate) fn unsynced_wal_bytes(&self) -> u64 {
        self.state.unsynced_wal_bytes
    }

    /// Publishes one completed WAL sync result into the durable frontier.
    ///
    /// Hosts call this on the owner thread after the dedicated sync service
    /// confirms durability up to `durable_seqno`.
    ///
    /// # Complexity
    /// Time: O(1).
    /// Space: O(1).
    pub fn publish_wal_sync_result(
        &mut self,
        durable_seqno: u64,
    ) -> std::result::Result<SyncResponse, KalanjiyamError> {
        if durable_seqno > self.state.last_committed_seqno {
            return Err(KalanjiyamError::Corruption(format!(
                "published durable_seqno {durable_seqno} exceeds last committed seqno {}",
                self.state.last_committed_seqno
            )));
        }
        self.state.durable_seqno = self.state.durable_seqno.max(durable_seqno);
        Ok(SyncResponse {
            durable_seqno: self.state.durable_seqno,
        })
    }

    /// Closes write admission after WAL sync failure and returns one mapped error.
    ///
    /// # Complexity
    /// Time: O(c), where `c` is the length of the formatted context/error
    /// message stored in the returned `KalanjiyamError`.
    /// Space: O(c).
    pub fn fail_wal_sync(&mut self, context: &str, error: std::io::Error) -> KalanjiyamError {
        self.state.write_admission_open = false;
        KalanjiyamError::io(context, error)
    }

    /// Drops one completed sync batch's byte budget after the owner publishes its frontier.
    pub(crate) fn finish_wal_sync_batch(&mut self, synced_bytes: u64) {
        self.state.unsynced_wal_bytes = self.state.unsynced_wal_bytes.saturating_sub(synced_bytes);
    }

    /// Applies one already computed point-read worker result to a response.
    ///
    /// # Complexity
    /// Time: O(1).
    /// Space: O(1).
    pub fn finish_get(
        &self,
        plan: &GetPlan,
        response: GetResponse,
    ) -> std::result::Result<GetResponse, KalanjiyamError> {
        if response.observation_seqno != plan.snapshot_seqno
            || response.data_generation != plan.data_generation
        {
            return Err(KalanjiyamError::Corruption(
                "get completion no longer matches the captured plan".to_string(),
            ));
        }
        Ok(response)
    }

    /// Applies one already computed scan-page result to session-visible state.
    ///
    /// # Complexity
    /// Time: O(1).
    /// Space: O(1).
    pub fn finish_scan_page(
        &self,
        _plan: &ScanPlan,
        page: ScanPage,
    ) -> std::result::Result<ScanPage, KalanjiyamError> {
        if !page.eof && page.rows.is_empty() {
            return Err(KalanjiyamError::Corruption(
                "non-EOF scan page must contain at least one row".to_string(),
            ));
        }
        Ok(page)
    }

    /// Freezes the active memtable if it is non-empty and returns one owned snapshot.
    ///
    /// # Complexity
    /// Time: O(a + f + log g), where `a` is the size of the active memtable
    /// being cloned, `f` is the number of frozen-memtable/file references
    /// cloned with the manifest snapshot, and `g` is the number of retained
    /// manifest generations updated in the history index.
    /// Space: O(a + f) for the returned frozen memtable clone and manifest
    /// snapshot clones created during publication.
    pub fn force_freeze(&mut self) -> std::result::Result<Option<FrozenMemtable>, KalanjiyamError> {
        if self.state.current_manifest.active_memtable_ref.is_empty() {
            return Ok(None);
        }
        self.detach_current_manifest_generation();
        let frozen_id = self.state.next_frozen_memtable_id;
        self.state.next_frozen_memtable_id += 1;
        let mut historical_manifest = std::mem::take(&mut self.state.current_manifest);
        // The retained historical generation and the new frozen queue can share
        // one immutable Arc; only the public return value still needs one owned
        // clone of the memtable contents.
        Arc::make_mut(&mut historical_manifest.active_memtable_ref).frozen_memtable_id =
            Some(frozen_id);
        let frozen = (*historical_manifest.active_memtable_ref).clone();

        let mut next_manifest = historical_manifest.clone();
        next_manifest.data_generation += 1;
        next_manifest.active_memtable_ref = Arc::new(Memtable::empty());
        next_manifest
            .frozen_memtable_refs
            .push(historical_manifest.active_memtable_ref.clone());
        self.state
            .manifest_index
            .by_generation
            .insert(historical_manifest.data_generation, historical_manifest);
        self.install_manifest(next_manifest);
        Ok(Some(frozen))
    }

    /// Captures a flush plan for the oldest frozen memtable, if one exists.
    ///
    /// The returned plan shares one immutable memtable reference so inline and
    /// worker execution can proceed without borrowing live engine state.
    ///
    /// # Complexity
    /// Time: O(1).
    /// Space: O(1).
    pub fn capture_flush_plan(
        &self,
    ) -> std::result::Result<Option<FlushTaskPlan>, KalanjiyamError> {
        let Some(oldest) = self.state.current_manifest.frozen_memtable_refs.first() else {
            return Ok(None);
        };
        Ok(Some(FlushTaskPlan {
            source_generation: self.state.data_generation,
            source_frozen_memtable_id: oldest.frozen_memtable_id.unwrap_or(0),
            source_memtable: oldest.clone(),
            output_file_ids: vec![self.state.next_file_id],
        }))
    }

    /// Publishes one previously captured flush plan inline for direct embeddings.
    ///
    /// # Complexity
    /// Time: O(r log r + b), where `r` is the number of records in the source
    /// frozen memtable and `b` is the total number of bytes written for the
    /// output data file and publication WAL record. Filesystem write and `fsync`
    /// latency is additional.
    /// Space: O(r + b) for the sorted file build and encoded output buffers.
    pub fn publish_flush_plan(
        &mut self,
        plan: &FlushTaskPlan,
    ) -> std::result::Result<Option<FileMeta>, KalanjiyamError> {
        let result = execute_flush_task(
            &self.state.store_dir,
            self.state.config.engine.page_size_bytes,
            plan,
        )?;
        self.publish_flush_task_result(plan, &result)
    }

    /// Publishes one worker-prepared flush result after stale checks.
    pub(crate) fn publish_flush_task_result(
        &mut self,
        plan: &FlushTaskPlan,
        result: &FlushTaskResult,
    ) -> std::result::Result<Option<FileMeta>, KalanjiyamError> {
        self.validate_flush_publication(plan, result)?;

        let payload = WalRecordPayload::FlushPublish(FlushPublishPayload {
            data_generation_expected: plan.source_generation,
            source_first_seqno: plan.source_memtable.min_seqno().unwrap_or(0),
            source_last_seqno: plan.source_memtable.max_seqno().unwrap_or(0),
            source_record_count: plan.source_memtable.entry_count(),
            output_file_metas: result.outputs.clone(),
        });
        let seqno = self.append_live_record(payload)?;
        self.state.last_committed_seqno = seqno;
        advance_next_file_id(&mut self.state.next_file_id, &result.outputs);

        let mut next_manifest = self.state.current_manifest.clone();
        next_manifest.data_generation += 1;
        next_manifest.frozen_memtable_refs.remove(0);
        for meta in &result.outputs {
            ensure_level_exists(&mut next_manifest.levels, meta.level_no as usize);
            next_manifest.levels[meta.level_no as usize].push(meta.clone());
        }
        self.install_manifest(next_manifest);
        self.sync_if_per_write()?;
        Ok(result.outputs.first().cloned())
    }

    /// Captures one compaction plan over all current L0 files.
    ///
    /// # Complexity
    /// Time: O(f0), where `f0` is the number of current L0 files.
    /// Space: O(f0) for the cloned file metadata and reserved output ids.
    pub fn capture_compact_plan(
        &self,
    ) -> std::result::Result<Option<CompactTaskPlan>, KalanjiyamError> {
        let Some(l0_files) = self.state.current_manifest.levels.first() else {
            return Ok(None);
        };
        if l0_files.is_empty() {
            return Ok(None);
        }
        Ok(Some(CompactTaskPlan {
            source_generation: self.state.data_generation,
            input_file_ids: l0_files.iter().map(|file| file.file_id).collect(),
            input_files: l0_files.clone(),
            output_file_ids: vec![self.state.next_file_id],
        }))
    }

    /// Publishes one previously captured compaction plan inline for direct embeddings.
    ///
    /// # Complexity
    /// Time: O(n log n + b + f0), where `n` is the total number of internal
    /// records loaded from the input files, `b` is the total number of bytes
    /// read and written during compaction, and `f0` is the number of input
    /// files. Filesystem read/write and `fsync` latency is additional.
    /// Space: O(n + f0) for the merged records and cloned file metadata.
    pub fn publish_compact_plan(
        &mut self,
        plan: &CompactTaskPlan,
    ) -> std::result::Result<Option<FileMeta>, KalanjiyamError> {
        let result = execute_compact_task(
            &self.state.store_dir,
            self.state.config.engine.page_size_bytes,
            plan,
        )?;
        self.publish_compact_task_result(plan, &result)
    }

    /// Publishes one worker-prepared compaction result after stale checks.
    pub(crate) fn publish_compact_task_result(
        &mut self,
        plan: &CompactTaskPlan,
        result: &CompactTaskResult,
    ) -> std::result::Result<Option<FileMeta>, KalanjiyamError> {
        self.validate_compact_publication(plan, result)?;

        let payload = WalRecordPayload::CompactPublish(CompactPublishPayload {
            data_generation_expected: plan.source_generation,
            input_file_ids: plan.input_file_ids.clone(),
            output_file_metas: result.outputs.clone(),
        });
        let seqno = self.append_live_record(payload)?;
        self.state.last_committed_seqno = seqno;
        advance_next_file_id(&mut self.state.next_file_id, &result.outputs);

        let mut next_manifest = self.state.current_manifest.clone();
        next_manifest.data_generation += 1;
        remove_input_files(&mut next_manifest.levels, &plan.input_file_ids)?;
        for meta in &result.outputs {
            ensure_level_exists(&mut next_manifest.levels, meta.level_no as usize);
            next_manifest.levels[meta.level_no as usize].push(meta.clone());
        }
        for level in next_manifest.levels.iter_mut().skip(1) {
            level.sort_by(|left, right| left.min_user_key.cmp(&right.min_user_key));
        }
        self.install_manifest(next_manifest);
        self.sync_if_per_write()?;
        Ok(result.outputs.first().cloned())
    }

    /// Installs one metadata-only logical-shard map update.
    ///
    /// # Complexity
    /// Time: O(s * k + p), where `s` is the number of current logical shard
    /// entries, `k` is the boundary-key length compared while validating and
    /// matching the source slice, and `p` is the encoded WAL record size.
    /// `PerWrite` mode adds WAL `fsync` latency.
    /// Space: O(p + k) for the encoded WAL record and cloned shard bounds.
    pub fn install_logical_shards(
        &mut self,
        source_entries: Vec<LogicalShardEntry>,
        output_entries: Vec<LogicalShardEntry>,
    ) -> std::result::Result<(), KalanjiyamError> {
        if !(1..=2).contains(&source_entries.len()) || !(1..=2).contains(&output_entries.len()) {
            return Err(KalanjiyamError::InvalidArgument(
                "logical-shard install requires 1 or 2 source entries and 1 or 2 output entries"
                    .to_string(),
            ));
        }
        validate_logical_shards(&source_entries)?;
        validate_logical_shards(&output_entries)?;
        if source_entries == output_entries {
            return Err(KalanjiyamError::InvalidArgument(
                "logical-shard install must not be a no-op".to_string(),
            ));
        }
        let source_start = source_entries
            .first()
            .map(|entry| entry.start_bound.clone());
        let source_end = source_entries.last().map(|entry| entry.end_bound.clone());
        let output_start = output_entries
            .first()
            .map(|entry| entry.start_bound.clone());
        let output_end = output_entries.last().map(|entry| entry.end_bound.clone());
        if source_start != output_start || source_end != output_end {
            return Err(KalanjiyamError::InvalidArgument(
                "logical-shard install must preserve outer bounds".to_string(),
            ));
        }

        let Some((start, end)) =
            find_matching_logical_slice(&self.state.current_logical_shards, &source_entries)
        else {
            return Err(KalanjiyamError::Stale(
                "logical-shard source entries no longer match current state".to_string(),
            ));
        };

        let seqno = self.append_live_record(WalRecordPayload::LogicalShardInstall(
            LogicalShardInstallPayload {
                source_entries: source_entries.clone(),
                output_entries: output_entries.clone(),
            },
        ))?;
        self.state
            .current_logical_shards
            .splice(start..end, output_entries);
        self.state.last_committed_seqno = seqno;
        self.sync_if_per_write()?;
        Ok(())
    }

    /// Captures and installs one checkpoint after draining mutable shared-data state.
    ///
    /// # Complexity
    /// Time: O(r log r + f + s * k + b), where `r` is the total number of
    /// records flushed before the checkpoint, `f` is the number of manifest
    /// files captured into the checkpoint, `s` is the number of logical shard
    /// entries, and `b` is the total number of bytes written for flushed data
    /// files, the checkpoint artifact, and the `CURRENT` update. Filesystem
    /// write and `fsync` latency is additional.
    /// Space: O(max(r, f + s, b)) for the largest in-memory flush or checkpoint
    /// buffer built during the operation.
    pub fn checkpoint(&mut self) -> std::result::Result<Option<CurrentFile>, KalanjiyamError> {
        if !self.state.current_manifest.active_memtable_ref.is_empty() {
            self.force_freeze()?;
        }
        while let Some(plan) = self.capture_flush_plan()? {
            let result = execute_flush_task(
                &self.state.store_dir,
                self.state.config.engine.page_size_bytes,
                &plan,
            )?;
            self.publish_flush_task_result(&plan, &result)?;
        }
        if !self.state.current_manifest.active_memtable_ref.is_empty()
            || !self.state.current_manifest.frozen_memtable_refs.is_empty()
        {
            return Err(KalanjiyamError::Corruption(
                "checkpoint capture requires empty active and frozen memtables".to_string(),
            ));
        }
        let plan = self.capture_checkpoint_plan()?;
        let result = execute_checkpoint_task(
            &self.state.store_dir,
            self.state.config.engine.page_size_bytes,
            self.state.next_checkpoint_generation,
            &plan,
        )?;
        self.publish_checkpoint_task_result(&plan, &result)
    }

    /// Captures one immutable checkpoint plan for worker-side checkpoint build.
    ///
    /// # Complexity
    /// Time: O(f + s * k), where `f` is the number of manifest files cloned
    /// into the plan and `s` is the number of logical shard entries.
    /// Space: O(f + s * k) for the cloned checkpoint state.
    pub fn capture_checkpoint_plan(
        &self,
    ) -> std::result::Result<CheckpointTaskPlan, KalanjiyamError> {
        if !self.state.current_manifest.active_memtable_ref.is_empty()
            || !self.state.current_manifest.frozen_memtable_refs.is_empty()
        {
            return Err(KalanjiyamError::Corruption(
                "checkpoint capture requires empty active and frozen memtables".to_string(),
            ));
        }
        Ok(CheckpointTaskPlan {
            checkpoint_max_seqno: self.state.last_committed_seqno,
            manifest_generation: self.state.data_generation,
            checkpoint_state: CheckpointState {
                checkpoint_max_seqno: self.state.last_committed_seqno,
                next_seqno: self.state.next_seqno,
                next_file_id: self.state.next_file_id,
                checkpoint_data_generation: self.state.data_generation,
                manifest_files: self
                    .state
                    .current_manifest
                    .levels
                    .iter()
                    .flat_map(|files| files.clone())
                    .collect(),
                logical_shards: self.state.current_logical_shards.clone(),
            },
        })
    }

    /// Publishes one worker-built checkpoint `CURRENT` pointer after stale checks.
    ///
    /// # Complexity
    /// Time: O(1) algorithmic work plus the fixed-size `CURRENT` install I/O.
    /// Space: O(1).
    pub fn publish_checkpoint_current(
        &mut self,
        plan: &CheckpointTaskPlan,
        current: &CurrentFile,
    ) -> std::result::Result<Option<CurrentFile>, KalanjiyamError> {
        let result = CheckpointTaskResult {
            checkpoint_max_seqno: current.checkpoint_max_seqno,
            manifest_generation: current.checkpoint_data_generation,
            current: current.clone(),
        };
        self.publish_checkpoint_task_result(plan, &result)
    }

    /// Publishes one worker-built checkpoint result after stale checks.
    pub(crate) fn publish_checkpoint_task_result(
        &mut self,
        plan: &CheckpointTaskPlan,
        result: &CheckpointTaskResult,
    ) -> std::result::Result<Option<CurrentFile>, KalanjiyamError> {
        self.validate_checkpoint_publication(plan, result)?;
        install_current(&self.state.store_dir, &result.current)?;
        self.state.durable_current = Some(result.current.clone());
        self.state.next_checkpoint_generation += 1;
        Ok(Some(result.current.clone()))
    }

    /// Captures one garbage-collection task from engine state plus owner-visible pins.
    pub(crate) fn capture_gc_task(
        &self,
        owner_pins: &GcOwnerPins,
    ) -> std::result::Result<Option<GcTaskPlan>, KalanjiyamError> {
        let mut pinned_manifest_generations = BTreeSet::from([0, self.state.data_generation]);
        pinned_manifest_generations.extend(
            self.state
                .active_snapshots
                .iter()
                .flatten()
                .map(|handle| handle.data_generation),
        );
        pinned_manifest_generations.extend(owner_pins.manifest_generations.iter().copied());

        let mut pinned_data_file_ids = current_manifest_file_ids(&self.state.current_manifest);
        for handle in self.state.active_snapshots.iter().flatten() {
            if let Ok(manifest) = manifest_for_generation(&self.state, handle.data_generation) {
                pinned_data_file_ids.extend(current_manifest_file_ids(manifest));
            }
        }
        pinned_data_file_ids.extend(owner_pins.data_file_ids.iter().copied());

        let mut pinned_checkpoint_generations = owner_pins.checkpoint_generations.clone();
        if let Some(current) = &self.state.durable_current {
            pinned_manifest_generations.insert(current.checkpoint_data_generation);
            pinned_checkpoint_generations.insert(current.checkpoint_generation);
            let checkpoint = load_checkpoint_file(&self.state.store_dir, current)?;
            pinned_data_file_ids.extend(
                checkpoint
                    .manifest_files
                    .into_iter()
                    .map(|meta| meta.file_id),
            );
        }

        let deletable_files = capture_deletable_files(
            &self.state.store_dir,
            &pinned_data_file_ids,
            &pinned_checkpoint_generations,
        )?;
        let prunable_manifest_generations = self
            .state
            .manifest_index
            .by_generation
            .keys()
            .copied()
            .filter(|generation| !pinned_manifest_generations.contains(generation))
            .collect::<Vec<_>>();

        if deletable_files.is_empty() && prunable_manifest_generations.is_empty() {
            return Ok(None);
        }

        Ok(Some(GcTaskPlan {
            deletable_files,
            prunable_manifest_generations,
        }))
    }

    /// Publishes one completed GC task by dropping pruned manifest snapshots.
    pub(crate) fn publish_gc_task_result(
        &mut self,
        plan: &GcTaskPlan,
        result: &GcTaskResult,
    ) -> std::result::Result<(), KalanjiyamError> {
        validate_gc_publication(plan, result)?;
        for generation in &result.pruned_manifest_generations {
            self.state.manifest_index.by_generation.remove(generation);
        }
        Ok(())
    }

    // Reuse one validation step for inline and worker-driven flush publish so
    // stale and corruption checks stay identical across both paths.
    fn validate_flush_publication(
        &self,
        plan: &FlushTaskPlan,
        result: &FlushTaskResult,
    ) -> std::result::Result<(), KalanjiyamError> {
        if self.state.data_generation != plan.source_generation {
            return Err(KalanjiyamError::Stale(
                "flush plan data_generation no longer matches current state".to_string(),
            ));
        }
        let Some(oldest) = self.state.current_manifest.frozen_memtable_refs.first() else {
            return Err(KalanjiyamError::Stale(
                "no frozen memtable remains for flush publication".to_string(),
            ));
        };
        if oldest.frozen_memtable_id != Some(plan.source_frozen_memtable_id)
            || oldest.min_seqno() != plan.source_memtable.min_seqno()
            || oldest.max_seqno() != plan.source_memtable.max_seqno()
            || oldest.entry_count() != plan.source_memtable.entry_count()
        {
            return Err(KalanjiyamError::Stale(
                "oldest frozen memtable no longer matches flush plan".to_string(),
            ));
        }
        if result.source_generation != plan.source_generation
            || result.source_frozen_memtable_id != plan.source_frozen_memtable_id
        {
            return Err(KalanjiyamError::Corruption(
                "flush worker result does not match the captured plan".to_string(),
            ));
        }
        let output_ids: Vec<_> = result.outputs.iter().map(|meta| meta.file_id).collect();
        if output_ids != plan.output_file_ids {
            return Err(KalanjiyamError::Corruption(
                "flush worker outputs do not match reserved output ids".to_string(),
            ));
        }
        Ok(())
    }

    // Reuse one validation step for inline and worker-driven compaction publish
    // so the current L0 shape is checked consistently.
    fn validate_compact_publication(
        &self,
        plan: &CompactTaskPlan,
        result: &CompactTaskResult,
    ) -> std::result::Result<(), KalanjiyamError> {
        if self.state.data_generation != plan.source_generation {
            return Err(KalanjiyamError::Stale(
                "compaction plan data_generation no longer matches current state".to_string(),
            ));
        }
        let current_input_ids: BTreeSet<u64> = self
            .state
            .current_manifest
            .levels
            .first()
            .into_iter()
            .flat_map(|files| files.iter().map(|file| file.file_id))
            .collect();
        let expected_input_ids: BTreeSet<u64> = plan.input_file_ids.iter().copied().collect();
        if current_input_ids != expected_input_ids {
            return Err(KalanjiyamError::Stale(
                "compaction input set no longer matches current L0 state".to_string(),
            ));
        }
        if result.source_generation != plan.source_generation
            || result.input_file_ids != plan.input_file_ids
        {
            return Err(KalanjiyamError::Corruption(
                "compaction worker result does not match the captured plan".to_string(),
            ));
        }
        let output_ids: Vec<_> = result.outputs.iter().map(|meta| meta.file_id).collect();
        if output_ids != plan.output_file_ids {
            return Err(KalanjiyamError::Corruption(
                "compaction worker outputs do not match reserved output ids".to_string(),
            ));
        }
        Ok(())
    }

    // Keep checkpoint publication checks separate from CURRENT install so tests
    // can cover stale and corruption guards without filesystem side effects.
    fn validate_checkpoint_publication(
        &self,
        plan: &CheckpointTaskPlan,
        result: &CheckpointTaskResult,
    ) -> std::result::Result<(), KalanjiyamError> {
        if plan.manifest_generation != self.state.data_generation
            || plan.checkpoint_max_seqno != self.state.last_committed_seqno
        {
            return Err(KalanjiyamError::Stale(
                "checkpoint plan no longer matches current state".to_string(),
            ));
        }
        if result.checkpoint_max_seqno != plan.checkpoint_max_seqno
            || result.manifest_generation != plan.manifest_generation
        {
            return Err(KalanjiyamError::Corruption(
                "checkpoint worker result does not match the captured plan".to_string(),
            ));
        }
        if result.current.checkpoint_generation != self.state.next_checkpoint_generation
            || result.current.checkpoint_max_seqno != plan.checkpoint_max_seqno
            || result.current.checkpoint_data_generation != plan.manifest_generation
        {
            return Err(KalanjiyamError::Corruption(
                "checkpoint publish metadata does not match capture plan".to_string(),
            ));
        }
        Ok(())
    }

    fn append_live_record(
        &mut self,
        payload: WalRecordPayload,
    ) -> std::result::Result<u64, KalanjiyamError> {
        if !self.state.write_admission_open {
            return Err(KalanjiyamError::Io(std::io::Error::other(
                "engine is no longer accepting writes after append failure",
            )));
        }
        let seqno = self.state.next_seqno;
        self.state.next_seqno += 1;
        let record = WalRecord { seqno, payload };
        let record_bytes = encode_wal_record(&record)?;
        self.roll_wal_if_needed(record_bytes.len() as u64)
            .map_err(|error| self.fail_append_after_seqno(error))?;
        self.ensure_active_segment_named_for_first_seqno(seqno)
            .map_err(|error| self.fail_append_after_seqno(error))?;
        append_record_bytes(&self.state.active_wal_path, &record_bytes).map_err(|error| {
            self.fail_append_after_seqno(KalanjiyamError::io("append WAL record", error))
        })?;
        self.state.active_wal_record_count += 1;
        self.state.active_wal_payload_bytes_used += record_bytes.len() as u64;
        self.state.unsynced_wal_bytes += record_bytes.len() as u64;
        Ok(seqno)
    }

    fn install_manifest(&mut self, manifest: DataManifestSnapshot) {
        let mut manifest = manifest;
        sort_manifest_levels(&mut manifest.levels);
        self.state.data_generation = manifest.data_generation;
        self.state.current_manifest = manifest.clone();
        self.state
            .manifest_index
            .by_generation
            .insert(manifest.data_generation, manifest);
    }

    fn mutate_current_active_memtable(&mut self, mutate: impl FnOnce(&mut Memtable)) {
        self.detach_current_manifest_generation();
        // Removing the current generation from `manifest_index` leaves the
        // engine's active memtable uniquely owned in the steady-state write
        // path, so `Arc::make_mut` normally mutates in place instead of
        // cloning the whole memtable before the BTreeMap updates below.
        mutate(Arc::make_mut(
            &mut self.state.current_manifest.active_memtable_ref,
        ));
        self.restore_current_manifest_generation();
    }

    fn detach_current_manifest_generation(&mut self) {
        let generation = self.state.current_manifest.data_generation;
        let _ = self.state.manifest_index.by_generation.remove(&generation);
    }

    fn restore_current_manifest_generation(&mut self) {
        let generation = self.state.current_manifest.data_generation;
        self.state
            .manifest_index
            .by_generation
            .insert(generation, self.state.current_manifest.clone());
    }

    fn decide_write_ack(&self, target_seqno: u64) -> WriteDecision {
        if self.state.config.engine.sync_mode == SyncMode::PerWrite {
            WriteDecision::Wait(WalWait { target_seqno })
        } else {
            WriteDecision::Immediate
        }
    }

    fn freeze_if_needed(&mut self) -> std::result::Result<(), KalanjiyamError> {
        if self
            .state
            .current_manifest
            .active_memtable_ref
            .logical_bytes()
            >= self.state.config.lsm.memtable_flush_bytes
        {
            let _ = self.force_freeze()?;
        }
        Ok(())
    }

    fn sync_if_per_write(&mut self) -> std::result::Result<(), KalanjiyamError> {
        if self.state.config.engine.sync_mode == SyncMode::PerWrite {
            sync_active_wal(&self.state.active_wal_path)?;
            self.state.durable_seqno = self.state.last_committed_seqno;
            self.state.unsynced_wal_bytes = 0;
        }
        Ok(())
    }

    fn fail_append_after_seqno(&mut self, error: KalanjiyamError) -> KalanjiyamError {
        // After seqno allocation, any append-path I/O failure leaves the open
        // instance in read-only mode until the next successful restart.
        self.state.write_admission_open = false;
        error
    }

    fn roll_wal_if_needed(
        &mut self,
        next_record_bytes: u64,
    ) -> std::result::Result<(), KalanjiyamError> {
        let current_len = fs::metadata(&self.state.active_wal_path)
            .map_err(|error| KalanjiyamError::io("stat active WAL", error))?
            .len();
        if !wal_rollover_required(
            current_len,
            next_record_bytes,
            self.state.config.wal.segment_bytes,
        ) {
            return Ok(());
        }
        if self.state.active_wal_record_count > 0 {
            let footer = WalSegmentFooter {
                first_seqno: self.state.active_wal_first_seqno,
                last_seqno: self.state.last_committed_seqno,
                record_count: self.state.active_wal_record_count,
                payload_bytes_used: self.state.active_wal_payload_bytes_used,
            };
            append_record_bytes(
                &self.state.active_wal_path,
                &encode_wal_segment_footer(&footer),
            )
            .map_err(|error| KalanjiyamError::io("append WAL footer", error))?;
            sync_active_wal(&self.state.active_wal_path)?;
            let closed_path = resolve_store_path(
                &self.state.store_dir,
                &wal_relative_path(footer.first_seqno, Some(footer.last_seqno)),
            );
            fs::rename(&self.state.active_wal_path, &closed_path)
                .map_err(|error| KalanjiyamError::io("rename closed WAL segment", error))?;
            sync_directory(
                self.state.store_dir.join("wal").as_path(),
                "sync wal directory after closed segment rename",
            )?;
        }
        create_empty_active_segment(&self.state.store_dir, self.state.config.wal.segment_bytes)?;
        self.state.active_wal_path = resolve_store_path(
            &self.state.store_dir,
            &wal_relative_path(empty_open_segment_first_seqno(), None),
        );
        self.state.active_wal_first_seqno = empty_open_segment_first_seqno();
        self.state.active_wal_record_count = 0;
        self.state.active_wal_payload_bytes_used = 0;
        Ok(())
    }

    fn ensure_active_segment_named_for_first_seqno(
        &mut self,
        seqno: u64,
    ) -> std::result::Result<(), KalanjiyamError> {
        if self.state.active_wal_first_seqno != empty_open_segment_first_seqno() {
            return Ok(());
        }
        let new_path = resolve_store_path(&self.state.store_dir, &wal_relative_path(seqno, None));
        fs::rename(&self.state.active_wal_path, &new_path)
            .map_err(|error| KalanjiyamError::io("rename active WAL segment", error))?;
        let header = WalSegmentHeader {
            first_seqno_or_none: seqno,
            segment_bytes: self.state.config.wal.segment_bytes,
        };
        rewrite_segment_header(&new_path, &header)?;
        sync_active_wal(&new_path)?;
        sync_directory(
            self.state.store_dir.join("wal").as_path(),
            "sync wal directory after active segment rename",
        )?;
        self.state.active_wal_path = new_path;
        self.state.active_wal_first_seqno = seqno;
        Ok(())
    }

    fn complete_inline_wal_sync(
        &mut self,
        durable_seqno: u64,
    ) -> std::result::Result<SyncResponse, KalanjiyamError> {
        let synced_bytes = self.state.unsynced_wal_bytes;
        let response = self.publish_wal_sync_result(durable_seqno)?;
        self.finish_wal_sync_batch(synced_bytes);
        Ok(response)
    }
}

/// Executes one WAL fsync against the provided active segment path.
#[allow(dead_code)]
pub(crate) fn execute_wal_sync(path: &Path) -> Result<(), KalanjiyamError> {
    sync_active_wal(path)
}

/// Builds flush output files from an immutable flush task plan.
#[allow(dead_code)]
pub(crate) fn execute_flush_task(
    store_dir: &Path,
    page_size_bytes: usize,
    plan: &FlushTaskPlan,
) -> Result<FlushTaskResult, KalanjiyamError> {
    let mut outputs = Vec::with_capacity(plan.output_file_ids.len());
    for file_id in &plan.output_file_ids {
        let mut meta = build_file_meta(*file_id, 0, &plan.source_memtable.records);
        let path = write_data_file(
            store_dir,
            &meta,
            &plan.source_memtable.records,
            page_size_bytes,
        )?;
        meta.physical_bytes = fs::metadata(&path)
            .map_err(|error| KalanjiyamError::io("stat flushed data file", error))?
            .len();
        outputs.push(meta);
    }
    Ok(FlushTaskResult {
        source_generation: plan.source_generation,
        source_frozen_memtable_id: plan.source_frozen_memtable_id,
        outputs,
    })
}

/// Builds compaction output files from an immutable compaction task plan.
#[allow(dead_code)]
pub(crate) fn execute_compact_task(
    store_dir: &Path,
    page_size_bytes: usize,
    plan: &CompactTaskPlan,
) -> Result<CompactTaskResult, KalanjiyamError> {
    let mut merged_records = Vec::new();
    for file in &plan.input_files {
        let parsed = load_data_file(store_dir, file)?;
        merged_records.extend(parsed.records);
    }
    merged_records.sort_by(compare_internal_records);

    let mut outputs = Vec::with_capacity(plan.output_file_ids.len());
    for file_id in &plan.output_file_ids {
        let mut meta = build_file_meta(*file_id, 1, &merged_records);
        let path = write_data_file(store_dir, &meta, &merged_records, page_size_bytes)?;
        meta.physical_bytes = fs::metadata(&path)
            .map_err(|error| KalanjiyamError::io("stat compacted data file", error))?
            .len();
        outputs.push(meta);
    }

    Ok(CompactTaskResult {
        source_generation: plan.source_generation,
        input_file_ids: plan.input_file_ids.clone(),
        outputs,
    })
}

/// Builds one checkpoint artifact from an immutable checkpoint task plan.
#[allow(dead_code)]
pub(crate) fn execute_checkpoint_task(
    store_dir: &Path,
    page_size_bytes: usize,
    checkpoint_generation: u64,
    plan: &CheckpointTaskPlan,
) -> Result<CheckpointTaskResult, KalanjiyamError> {
    write_checkpoint_file(
        store_dir,
        checkpoint_generation,
        &plan.checkpoint_state,
        page_size_bytes,
    )?;
    Ok(CheckpointTaskResult {
        checkpoint_max_seqno: plan.checkpoint_max_seqno,
        manifest_generation: plan.manifest_generation,
        current: CurrentFile {
            checkpoint_generation,
            checkpoint_max_seqno: plan.checkpoint_max_seqno,
            checkpoint_data_generation: plan.manifest_generation,
        },
    })
}

/// Compatibility wrapper that returns only the prepared `CURRENT` payload.
#[allow(dead_code)]
pub(crate) fn execute_checkpoint_plan(
    store_dir: &Path,
    page_size_bytes: usize,
    checkpoint_generation: u64,
    plan: &CheckpointTaskPlan,
) -> Result<CurrentFile, KalanjiyamError> {
    execute_checkpoint_task(store_dir, page_size_bytes, checkpoint_generation, plan)
        .map(|result| result.current)
}

/// Deletes already-safe durable files and reports matching manifest pruning.
#[allow(dead_code)]
pub(crate) fn execute_gc_task(plan: &GcTaskPlan) -> Result<GcTaskResult, KalanjiyamError> {
    for file in &plan.deletable_files {
        match fs::remove_file(file.path()) {
            Ok(()) => {}
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(error) => return Err(KalanjiyamError::io("delete gc artifact", error)),
        }
    }
    Ok(GcTaskResult {
        deleted_files: plan.deletable_files.clone(),
        pruned_manifest_generations: plan.prunable_manifest_generations.clone(),
    })
}

/// Executes one immutable `GetPlan` against durable inputs without engine mutation.
#[allow(dead_code)]
pub(crate) fn execute_get_plan(
    store_dir: &Path,
    plan: &GetPlan,
) -> std::result::Result<GetResponse, KalanjiyamError> {
    lookup_get_plan_response(store_dir, plan).map(|response| {
        response.unwrap_or(GetResponse {
            found: false,
            value: None,
            observation_seqno: plan.snapshot_seqno,
            data_generation: plan.data_generation,
        })
    })
}

/// One scan input source used by the lazy merge path.
enum ScanSourceIterator<'a> {
    Memtable(crate::pezhai::types::MemtableRecordIterator<'a>),
    File(DataFileRecordIterator),
}

impl ScanSourceIterator<'_> {
    fn next_record(&mut self) -> Result<Option<InternalRecord>, KalanjiyamError> {
        match self {
            Self::Memtable(iterator) => Ok(iterator.next_record()),
            Self::File(iterator) => iterator.next_record(),
        }
    }
}

/// Heap entry that keeps one next-record candidate per scan source.
#[derive(Eq, PartialEq)]
struct HeapRecord {
    record: InternalRecord,
    source_index: usize,
}

impl Ord for HeapRecord {
    fn cmp(&self, other: &Self) -> Ordering {
        compare_internal_records(&other.record, &self.record)
            .then_with(|| other.source_index.cmp(&self.source_index))
    }
}

impl PartialOrd for HeapRecord {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Executes one immutable `ScanPlan` page against durable inputs without engine mutation.
#[allow(dead_code)]
pub(crate) fn execute_scan_plan(
    store_dir: &Path,
    plan: &ScanPlan,
) -> std::result::Result<ScanPage, KalanjiyamError> {
    execute_scan_sources(
        collect_scan_source_iterators_from_plan(store_dir, plan)?,
        plan.snapshot_handle.snapshot_seqno,
        plan.max_records_per_page,
        plan.max_bytes_per_page,
    )
}

// Walk `GetPlan` sources in production order so helper tests and worker
// execution share the same frozen-then-file fallback behavior.
fn lookup_get_plan_response(
    store_dir: &Path,
    plan: &GetPlan,
) -> Result<Option<GetResponse>, KalanjiyamError> {
    for frozen in &plan.frozen_memtables {
        if let Some(record) = frozen.get_visible(&plan.key, plan.snapshot_seqno) {
            return Ok(Some(build_get_response(
                record,
                plan.snapshot_seqno,
                plan.data_generation,
            )));
        }
    }
    for file in &plan.candidate_files {
        if let Some(record) =
            find_visible_record_in_data_file(store_dir, file, &plan.key, plan.snapshot_seqno)?
        {
            return Ok(Some(build_get_response(
                record,
                plan.snapshot_seqno,
                plan.data_generation,
            )));
        }
    }
    Ok(None)
}

fn range_start_key(range: &KeyRange) -> Option<&[u8]> {
    match &range.start_bound {
        Bound::Finite(key) => Some(key),
        Bound::NegInf => None,
        Bound::PosInf => None,
    }
}

fn range_end_key(range: &KeyRange) -> Option<&[u8]> {
    match &range.end_bound {
        Bound::Finite(key) => Some(key),
        Bound::PosInf => None,
        Bound::NegInf => None,
    }
}

fn collect_scan_source_iterators_from_plan<'a>(
    store_dir: &Path,
    plan: &'a ScanPlan,
) -> Result<Vec<ScanSourceIterator<'a>>, KalanjiyamError> {
    let mut iterators = vec![ScanSourceIterator::Memtable(
        plan.active_memtable
            .iter_internal_records(&plan.range, plan.resume_after_key.as_deref()),
    )];
    for frozen in &plan.frozen_memtables {
        iterators.push(ScanSourceIterator::Memtable(
            frozen.iter_internal_records(&plan.range, plan.resume_after_key.as_deref()),
        ));
    }
    for file in &plan.candidate_files {
        iterators.push(ScanSourceIterator::File(DataFileRecordIterator::open(
            store_dir,
            file,
            range_start_key(&plan.range),
            range_end_key(&plan.range),
            plan.resume_after_key.as_deref(),
        )?));
    }
    Ok(iterators)
}

fn collect_scan_source_iterators_from_manifest<'a>(
    store_dir: &Path,
    manifest: &'a DataManifestSnapshot,
    range: &'a KeyRange,
    resume_after_key: Option<&[u8]>,
) -> Result<Vec<ScanSourceIterator<'a>>, KalanjiyamError> {
    let mut iterators = vec![ScanSourceIterator::Memtable(
        manifest
            .active_memtable_ref
            .iter_internal_records(range, resume_after_key),
    )];
    for frozen in &manifest.frozen_memtable_refs {
        iterators.push(ScanSourceIterator::Memtable(
            frozen.iter_internal_records(range, resume_after_key),
        ));
    }
    for file in files_overlapping_range(manifest, range) {
        iterators.push(ScanSourceIterator::File(DataFileRecordIterator::open(
            store_dir,
            &file,
            range_start_key(range),
            range_end_key(range),
            resume_after_key,
        )?));
    }
    Ok(iterators)
}

fn execute_scan_sources(
    mut sources: Vec<ScanSourceIterator<'_>>,
    snapshot_seqno: u64,
    max_records_per_page: u32,
    max_bytes_per_page: u32,
) -> Result<ScanPage, KalanjiyamError> {
    let mut heap = BinaryHeap::new();
    for (source_index, source) in sources.iter_mut().enumerate() {
        if let Some(record) = source.next_record()? {
            heap.push(HeapRecord {
                record,
                source_index,
            });
        }
    }

    let mut rows = Vec::new();
    let mut used_bytes = 0_u32;
    let mut last_visible_key: Option<Vec<u8>> = None;
    let mut has_more = false;
    while let Some(entry) = heap.pop() {
        if let Some(next_record) = sources[entry.source_index].next_record()? {
            heap.push(HeapRecord {
                record: next_record,
                source_index: entry.source_index,
            });
        }

        if last_visible_key
            .as_ref()
            .is_some_and(|key| key.as_slice() == entry.record.user_key.as_slice())
        {
            continue;
        }
        if entry.record.seqno > snapshot_seqno {
            continue;
        }

        last_visible_key = Some(entry.record.user_key.clone());
        if entry.record.kind == RecordKind::Delete {
            continue;
        }

        let row = ScanRow {
            key: entry.record.user_key.clone(),
            value: entry.record.value.clone().unwrap_or_default(),
        };
        let row_bytes = (row.key.len() + row.value.len()) as u32;
        let hit_record_limit = rows.len() as u32 >= max_records_per_page;
        let hit_byte_limit = !rows.is_empty() && used_bytes + row_bytes > max_bytes_per_page;
        if hit_record_limit || hit_byte_limit {
            has_more = true;
            break;
        }

        used_bytes += row_bytes;
        rows.push(row);
    }

    if rows.is_empty() {
        return Ok(ScanPage {
            rows,
            eof: !has_more && heap.is_empty(),
            next_resume_after_key: None,
        });
    }

    Ok(ScanPage {
        next_resume_after_key: if has_more || !heap.is_empty() {
            rows.last().map(|row| row.key.clone())
        } else {
            None
        },
        eof: !has_more && heap.is_empty(),
        rows,
    })
}

// Materialize the immutable scan sources for helper tests that want to assert
// source capture without depending on page-size limits or merge ordering.
#[cfg(test)]
fn collect_scan_plan_records(
    store_dir: &Path,
    plan: &ScanPlan,
) -> Result<Vec<InternalRecord>, KalanjiyamError> {
    let mut records = Vec::new();
    for mut source in collect_scan_source_iterators_from_plan(store_dir, plan)? {
        while let Some(record) = source.next_record()? {
            records.push(record);
        }
    }
    Ok(records)
}

// Keep GC publication validation separate from manifest pruning so mismatched
// worker results can be rejected without mutating engine state.
fn validate_gc_publication(
    plan: &GcTaskPlan,
    result: &GcTaskResult,
) -> Result<(), KalanjiyamError> {
    if result.deleted_files != plan.deletable_files
        || result.pruned_manifest_generations != plan.prunable_manifest_generations
    {
        return Err(KalanjiyamError::Corruption(
            "gc worker result does not match the captured plan".to_string(),
        ));
    }
    Ok(())
}

// The rollover rule is a pure size check, which makes the append-path boundary
// easier to test without forcing real filesystem rollover every time.
fn wal_rollover_required(current_len: u64, next_record_bytes: u64, segment_bytes: u64) -> bool {
    current_len + next_record_bytes + 128 > segment_bytes
}

fn recover_engine_state(config: PezhaiConfig) -> Result<EngineState, KalanjiyamError> {
    ensure_store_layout(&config.store_dir)?;
    let current = maybe_load_current(&config.store_dir)?;
    let checkpoint = match &current {
        Some(current) => Some(load_checkpoint_file(&config.store_dir, current)?),
        None => None,
    };
    let mut state = state_from_checkpoint(config, current.clone(), checkpoint)?;
    let replay_start_seqno = current
        .as_ref()
        .map_or(1, |current| current.checkpoint_max_seqno.saturating_add(1));
    let wal_segments = list_wal_segments(&state.store_dir)?;

    let mut pending_records = Vec::new();
    let mut levels = state.current_manifest.levels.clone();
    for descriptor in &wal_segments {
        stream_wal_records(descriptor, |record, _record_bytes| {
            if record.seqno < replay_start_seqno {
                return Ok(());
            }
            apply_replayed_record(
                &state.store_dir,
                &mut levels,
                &mut pending_records,
                &mut state.current_logical_shards,
                &mut state.data_generation,
                &mut state.next_file_id,
                record,
            )?;
            state.last_committed_seqno = record.seqno;
            Ok(())
        })?;
    }

    let active_memtable = memtable_from_records(&pending_records);
    state.current_manifest = DataManifestSnapshot {
        data_generation: state.data_generation,
        levels,
        active_memtable_ref: Arc::new(active_memtable),
        frozen_memtable_refs: Vec::new(),
    };
    state.manifest_index = DataManifestIndex::default();
    state
        .manifest_index
        .by_generation
        .insert(0, DataManifestSnapshot::default());
    state
        .manifest_index
        .by_generation
        .insert(state.data_generation, state.current_manifest.clone());
    state.durable_seqno = state.last_committed_seqno;
    adopt_or_create_active_segment(&mut state, wal_segments)?;
    Ok(state)
}

fn state_from_checkpoint(
    config: PezhaiConfig,
    current: Option<CurrentFile>,
    checkpoint: Option<CheckpointState>,
) -> Result<EngineState, KalanjiyamError> {
    let mut state = EngineState::empty(config);
    state.durable_current = current.clone();
    if let Some(current) = current {
        state.next_checkpoint_generation = current.checkpoint_generation + 1;
    }
    if let Some(checkpoint) = checkpoint {
        state.last_committed_seqno = checkpoint.checkpoint_max_seqno;
        state.durable_seqno = checkpoint.checkpoint_max_seqno;
        state.data_generation = checkpoint.checkpoint_data_generation;
        state.next_seqno = checkpoint.next_seqno;
        state.next_file_id = checkpoint.next_file_id;
        state.current_logical_shards = if checkpoint.logical_shards.is_empty() {
            vec![LogicalShardEntry {
                start_bound: Bound::NegInf,
                end_bound: Bound::PosInf,
                live_size_bytes: 0,
            }]
        } else {
            checkpoint.logical_shards
        };
        state.current_manifest = DataManifestSnapshot {
            data_generation: state.data_generation,
            levels: group_manifest_files_by_level(checkpoint.manifest_files),
            active_memtable_ref: Arc::new(Memtable::empty()),
            frozen_memtable_refs: Vec::new(),
        };
    }
    Ok(state)
}

fn ensure_store_layout(store_dir: &Path) -> Result<(), KalanjiyamError> {
    fs::create_dir_all(store_dir.join("wal"))
        .map_err(|error| KalanjiyamError::io("create wal directory", error))?;
    fs::create_dir_all(store_dir.join("meta"))
        .map_err(|error| KalanjiyamError::io("create meta directory", error))?;
    fs::create_dir_all(store_dir.join("data"))
        .map_err(|error| KalanjiyamError::io("create data directory", error))?;
    Ok(())
}

fn maybe_load_current(store_dir: &Path) -> Result<Option<CurrentFile>, KalanjiyamError> {
    let path = resolve_store_path(store_dir, &current_relative_path());
    if !path.exists() {
        return Ok(None);
    }
    let bytes = fs::read(&path).map_err(|error| KalanjiyamError::io("read CURRENT", error))?;
    decode_current(&bytes).map(Some)
}

fn current_manifest_file_ids(manifest: &DataManifestSnapshot) -> BTreeSet<u64> {
    manifest
        .levels
        .iter()
        .flat_map(|files| files.iter().map(|meta| meta.file_id))
        .collect()
}

fn capture_deletable_files(
    store_dir: &Path,
    pinned_data_file_ids: &BTreeSet<u64>,
    pinned_checkpoint_generations: &BTreeSet<u64>,
) -> Result<Vec<DurableFileRef>, KalanjiyamError> {
    let mut files = Vec::new();
    files.extend(
        list_data_files(store_dir)?
            .into_iter()
            .filter_map(|(file_id, path)| {
                (!pinned_data_file_ids.contains(&file_id))
                    .then_some(DurableFileRef::Data { file_id, path })
            }),
    );
    files.extend(
        list_metadata_checkpoints(store_dir)?
            .into_iter()
            .filter_map(|(checkpoint_generation, path)| {
                (!pinned_checkpoint_generations.contains(&checkpoint_generation)).then_some(
                    DurableFileRef::MetadataCheckpoint {
                        checkpoint_generation,
                        path,
                    },
                )
            }),
    );
    Ok(files)
}

fn list_data_files(store_dir: &Path) -> Result<Vec<(u64, PathBuf)>, KalanjiyamError> {
    let mut files = Vec::new();
    let data_dir = store_dir.join("data");
    for entry in
        fs::read_dir(&data_dir).map_err(|error| KalanjiyamError::io("read data dir", error))?
    {
        let entry = entry.map_err(|error| KalanjiyamError::io("read data dir entry", error))?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let Some(stem) = name.strip_suffix(".kjm") else {
            continue;
        };
        let Ok(file_id) = stem.parse::<u64>() else {
            continue;
        };
        files.push((file_id, entry.path()));
    }
    files.sort_by_key(|(file_id, _)| *file_id);
    Ok(files)
}

fn list_metadata_checkpoints(store_dir: &Path) -> Result<Vec<(u64, PathBuf)>, KalanjiyamError> {
    let mut files = Vec::new();
    let meta_dir = store_dir.join("meta");
    for entry in
        fs::read_dir(&meta_dir).map_err(|error| KalanjiyamError::io("read meta dir", error))?
    {
        let entry = entry.map_err(|error| KalanjiyamError::io("read meta dir entry", error))?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        let Some(rest) = name.strip_prefix("meta-") else {
            continue;
        };
        let Some(stem) = rest.strip_suffix(".kjm") else {
            continue;
        };
        let Ok(checkpoint_generation) = stem.parse::<u64>() else {
            continue;
        };
        files.push((checkpoint_generation, entry.path()));
    }
    files.sort_by_key(|(checkpoint_generation, _)| *checkpoint_generation);
    Ok(files)
}

fn list_wal_segments(store_dir: &Path) -> Result<Vec<WalSegmentDescriptor>, KalanjiyamError> {
    let wal_dir = store_dir.join("wal");
    let mut segments = Vec::new();
    for entry in
        fs::read_dir(&wal_dir).map_err(|error| KalanjiyamError::io("read wal dir", error))?
    {
        let entry = entry.map_err(|error| KalanjiyamError::io("read wal dir entry", error))?;
        let Some(name) = entry.file_name().to_str().map(str::to_owned) else {
            continue;
        };
        if let Some(descriptor) = parse_wal_descriptor(entry.path(), &name) {
            segments.push(descriptor);
        }
    }
    segments.sort_by_key(|segment| (segment.first_seqno, segment.last_seqno.unwrap_or(u64::MAX)));
    Ok(segments)
}

fn parse_wal_descriptor(path: PathBuf, file_name: &str) -> Option<WalSegmentDescriptor> {
    let rest = file_name.strip_prefix("wal-")?;
    if let Some(first) = rest.strip_suffix("-open.log") {
        let first_seqno = first.parse().ok()?;
        return Some(WalSegmentDescriptor {
            path,
            first_seqno,
            last_seqno: None,
            is_open: true,
        });
    }

    let closed = rest.strip_suffix(".log")?;
    let (first, last) = closed.split_once('-')?;
    Some(WalSegmentDescriptor {
        path,
        first_seqno: first.parse().ok()?,
        last_seqno: Some(last.parse().ok()?),
        is_open: false,
    })
}

// Replay and active-segment adoption only need one decoded record at a time, so
// walk the WAL as a byte stream instead of materializing the whole segment.
fn stream_wal_records(
    descriptor: &WalSegmentDescriptor,
    mut visit: impl FnMut(&WalRecord, u64) -> Result<(), KalanjiyamError>,
) -> Result<(), KalanjiyamError> {
    let mut file = OpenOptions::new()
        .read(true)
        .open(&descriptor.path)
        .map_err(|error| KalanjiyamError::io("open WAL segment", error))?;
    let file_len = file
        .metadata()
        .map_err(|error| KalanjiyamError::io("stat WAL segment", error))?
        .len();
    if file_len < 128 {
        return Err(KalanjiyamError::Corruption(
            "WAL segment is shorter than its header".to_string(),
        ));
    }

    let mut header = [0_u8; 128];
    file.read_exact(&mut header)
        .map_err(|error| KalanjiyamError::io("read WAL segment header", error))?;
    let _header = decode_wal_segment_header(&header)?;
    let end = if descriptor.is_open {
        file_len
    } else {
        if file_len < 256 {
            return Err(KalanjiyamError::Corruption(
                "closed WAL segment is shorter than header plus footer".to_string(),
            ));
        }
        file_len - 128
    };

    let mut offset = 128_u64;
    while offset + 64 <= end {
        file.seek(SeekFrom::Start(offset))
            .map_err(|error| KalanjiyamError::io("seek WAL record prefix", error))?;
        let mut prefix = [0_u8; 64];
        file.read_exact(&mut prefix)
            .map_err(|error| KalanjiyamError::io("read WAL record prefix", error))?;
        let total_bytes =
            u32::from_le_bytes(prefix[8..12].try_into().expect("slice has exact length")) as u64;
        if total_bytes == 0 || offset + total_bytes > end {
            break;
        }
        file.seek(SeekFrom::Start(offset))
            .map_err(|error| KalanjiyamError::io("seek WAL record", error))?;
        let mut record_bytes = vec![0_u8; total_bytes as usize];
        file.read_exact(&mut record_bytes)
            .map_err(|error| KalanjiyamError::io("read WAL record", error))?;
        let record = decode_wal_record(&record_bytes)?;
        visit(&record, total_bytes)?;
        offset += total_bytes;
    }
    Ok(())
}

fn adopt_or_create_active_segment(
    state: &mut EngineState,
    wal_segments: Vec<WalSegmentDescriptor>,
) -> Result<(), KalanjiyamError> {
    if let Some(active) = wal_segments.into_iter().find(|segment| segment.is_open) {
        let mut saw_record = false;
        let mut record_count = 0_u64;
        let mut payload_bytes_used = 0_u64;
        stream_wal_records(&active, |_record, record_bytes| {
            saw_record = true;
            record_count += 1;
            payload_bytes_used += record_bytes;
            Ok(())
        })?;
        state.active_wal_path = active.path;
        state.active_wal_first_seqno = if saw_record {
            active.first_seqno
        } else {
            empty_open_segment_first_seqno()
        };
        state.active_wal_record_count = record_count;
        state.active_wal_payload_bytes_used = payload_bytes_used;
        state.unsynced_wal_bytes = 0;
        return Ok(());
    }

    create_empty_active_segment(&state.store_dir, state.config.wal.segment_bytes)?;
    state.active_wal_path = resolve_store_path(
        &state.store_dir,
        &wal_relative_path(empty_open_segment_first_seqno(), None),
    );
    state.active_wal_first_seqno = empty_open_segment_first_seqno();
    state.active_wal_record_count = 0;
    state.active_wal_payload_bytes_used = 0;
    state.unsynced_wal_bytes = 0;
    Ok(())
}

fn create_empty_active_segment(
    store_dir: &Path,
    segment_bytes: u64,
) -> Result<(), KalanjiyamError> {
    let path = resolve_store_path(
        store_dir,
        &wal_relative_path(empty_open_segment_first_seqno(), None),
    );
    let header = WalSegmentHeader {
        first_seqno_or_none: empty_open_segment_first_seqno(),
        segment_bytes,
    };
    fs::write(&path, encode_wal_segment_header(&header))
        .map_err(|error| KalanjiyamError::io("create active WAL header", error))?;
    sync_active_wal(&path)?;
    sync_directory(
        store_dir.join("wal").as_path(),
        "sync wal directory after active segment create",
    )?;
    Ok(())
}

fn rewrite_segment_header(path: &Path, header: &WalSegmentHeader) -> Result<(), KalanjiyamError> {
    let mut file = OpenOptions::new()
        .write(true)
        .open(path)
        .map_err(|error| KalanjiyamError::io("open WAL header for rewrite", error))?;
    file.seek(SeekFrom::Start(0))
        .map_err(|error| KalanjiyamError::io("seek WAL header", error))?;
    file.write_all(&encode_wal_segment_header(header))
        .map_err(|error| KalanjiyamError::io("rewrite WAL header", error))?;
    Ok(())
}

fn append_record_bytes(path: &Path, bytes: &[u8]) -> Result<(), std::io::Error> {
    let mut file = OpenOptions::new().append(true).open(path)?;
    file.write_all(bytes)?;
    Ok(())
}

fn sync_active_wal(path: &Path) -> Result<(), KalanjiyamError> {
    let file = OpenOptions::new()
        .read(true)
        .open(path)
        .map_err(|error| KalanjiyamError::io("open WAL for sync", error))?;
    file.sync_all()
        .map_err(|error| KalanjiyamError::io("sync WAL", error))
}

fn sync_directory(path: &Path, context: &str) -> Result<(), KalanjiyamError> {
    // WAL rollover is only durable after the directory records the rename.
    OpenOptions::new()
        .read(true)
        .open(path)
        .map_err(|error| KalanjiyamError::io(context, error))?
        .sync_all()
        .map_err(|error| KalanjiyamError::io(context, error))
}

fn apply_replayed_record(
    store_dir: &Path,
    levels: &mut Vec<Vec<FileMeta>>,
    pending_records: &mut Vec<InternalRecord>,
    logical_shards: &mut Vec<LogicalShardEntry>,
    data_generation: &mut u64,
    next_file_id: &mut u64,
    record: &WalRecord,
) -> Result<(), KalanjiyamError> {
    match &record.payload {
        WalRecordPayload::Put { key, value } => pending_records.push(InternalRecord {
            user_key: key.clone(),
            seqno: record.seqno,
            kind: RecordKind::Put,
            value: Some(value.clone()),
        }),
        WalRecordPayload::Delete { key } => pending_records.push(InternalRecord {
            user_key: key.clone(),
            seqno: record.seqno,
            kind: RecordKind::Delete,
            value: None,
        }),
        WalRecordPayload::FlushPublish(payload) => {
            for meta in &payload.output_file_metas {
                validate_data_file(store_dir, meta)?;
            }
            remove_pending_prefix(
                pending_records,
                payload.source_first_seqno,
                payload.source_last_seqno,
                payload.source_record_count,
            )?;
            ensure_level_exists(levels, 0);
            levels[0].extend(payload.output_file_metas.clone());
            *data_generation += 1;
            advance_next_file_id(next_file_id, &payload.output_file_metas);
        }
        WalRecordPayload::CompactPublish(payload) => {
            for meta in &payload.output_file_metas {
                validate_data_file(store_dir, meta)?;
            }
            remove_input_files(levels, &payload.input_file_ids)?;
            for meta in &payload.output_file_metas {
                ensure_level_exists(levels, meta.level_no as usize);
                levels[meta.level_no as usize].push(meta.clone());
            }
            *data_generation += 1;
            advance_next_file_id(next_file_id, &payload.output_file_metas);
        }
        WalRecordPayload::LogicalShardInstall(payload) => {
            let Some((start, end)) =
                find_matching_logical_slice(logical_shards, &payload.source_entries)
            else {
                return Err(KalanjiyamError::Corruption(
                    "replayed LogicalShardInstall source entries do not match current map"
                        .to_string(),
                ));
            };
            logical_shards.splice(start..end, payload.output_entries.clone());
        }
    }
    Ok(())
}

fn remove_pending_prefix(
    pending_records: &mut Vec<InternalRecord>,
    first_seqno: u64,
    last_seqno: u64,
    record_count: u64,
) -> Result<(), KalanjiyamError> {
    if pending_records.len() < record_count as usize {
        return Err(KalanjiyamError::Corruption(
            "FlushPublish names more replayed records than currently pending".to_string(),
        ));
    }
    let prefix = &pending_records[..record_count as usize];
    let actual_first = prefix.first().map(|record| record.seqno).unwrap_or(0);
    let actual_last = prefix.last().map(|record| record.seqno).unwrap_or(0);
    if actual_first != first_seqno || actual_last != last_seqno {
        return Err(KalanjiyamError::Corruption(
            "FlushPublish seqno interval does not match replay pending prefix".to_string(),
        ));
    }
    pending_records.drain(..record_count as usize);
    Ok(())
}

fn remove_input_files(
    levels: &mut [Vec<FileMeta>],
    input_file_ids: &[u64],
) -> Result<(), KalanjiyamError> {
    let input_ids: BTreeSet<u64> = input_file_ids.iter().copied().collect();
    let mut removed = 0;
    for level in levels {
        let before = level.len();
        level.retain(|file| !input_ids.contains(&file.file_id));
        removed += before - level.len();
    }
    if removed != input_ids.len() {
        return Err(KalanjiyamError::Corruption(
            "CompactPublish input ids did not exactly match current manifest".to_string(),
        ));
    }
    Ok(())
}

fn advance_next_file_id(next_file_id: &mut u64, outputs: &[FileMeta]) {
    if let Some(max_file_id) = outputs.iter().map(|meta| meta.file_id).max() {
        *next_file_id = (*next_file_id).max(max_file_id + 1);
    }
}

fn group_manifest_files_by_level(files: Vec<FileMeta>) -> Vec<Vec<FileMeta>> {
    let mut levels = Vec::new();
    for file in files {
        ensure_level_exists(&mut levels, file.level_no as usize);
        levels[file.level_no as usize].push(file);
    }
    sort_manifest_levels(&mut levels);
    levels
}

// Higher levels rely on non-overlapping range lookup, so keep them sorted by
// user-key span whenever a manifest snapshot is installed or rebuilt.
fn sort_manifest_levels(levels: &mut [Vec<FileMeta>]) {
    for (level_no, files) in levels.iter_mut().enumerate() {
        if level_no == 0 {
            files.sort_by_key(|file| file.file_id);
            continue;
        }
        files.sort_by(|left, right| {
            left.min_user_key
                .cmp(&right.min_user_key)
                .then_with(|| left.max_user_key.cmp(&right.max_user_key))
                .then_with(|| left.file_id.cmp(&right.file_id))
        });
    }
}

fn ensure_level_exists(levels: &mut Vec<Vec<FileMeta>>, level_no: usize) {
    while levels.len() <= level_no {
        levels.push(Vec::new());
    }
}

fn memtable_from_records(records: &[InternalRecord]) -> Memtable {
    let mut memtable = Memtable::empty();
    for record in records {
        match record.kind {
            RecordKind::Put => memtable.insert_put(
                record.seqno,
                &record.user_key,
                record.value.as_deref().unwrap_or_default(),
            ),
            RecordKind::Delete => memtable.insert_delete(record.seqno, &record.user_key),
        }
    }
    memtable
}

fn resolve_snapshot(
    state: &EngineState,
    snapshot: Option<&SnapshotHandle>,
) -> Result<SnapshotHandle, KalanjiyamError> {
    match snapshot {
        Some(handle) => {
            validate_snapshot_handle(state, handle)?;
            Ok(handle.clone())
        }
        None => Ok(SnapshotHandle {
            engine_instance_id: state.engine_instance_id,
            snapshot_id: 0,
            snapshot_seqno: state.last_committed_seqno,
            data_generation: state.data_generation,
        }),
    }
}

fn validate_snapshot_handle(
    state: &EngineState,
    handle: &SnapshotHandle,
) -> Result<(), KalanjiyamError> {
    if handle.engine_instance_id != state.engine_instance_id {
        return Err(KalanjiyamError::InvalidArgument(
            "snapshot handle belongs to a different engine instance".to_string(),
        ));
    }
    let slot_index = snapshot_slot_index(handle.snapshot_id)?;
    match state
        .active_snapshots
        .get(slot_index)
        .and_then(Option::as_ref)
    {
        Some(active) if active == handle => Ok(()),
        _ => Err(KalanjiyamError::InvalidArgument(
            "snapshot handle is not active".to_string(),
        )),
    }
}

// Snapshot ids are monotonic and never reused within one open engine instance,
// so `snapshot_id - 1` maps directly to the retained slot in `active_snapshots`.
fn snapshot_slot_index(snapshot_id: u64) -> Result<usize, KalanjiyamError> {
    snapshot_id
        .checked_sub(1)
        .and_then(|index| usize::try_from(index).ok())
        .ok_or_else(|| {
            KalanjiyamError::InvalidArgument("snapshot handle is not active".to_string())
        })
}

fn manifest_for_generation(
    state: &EngineState,
    generation: u64,
) -> Result<&DataManifestSnapshot, KalanjiyamError> {
    state
        .manifest_index
        .by_generation
        .get(&generation)
        .ok_or_else(|| {
            KalanjiyamError::InvalidArgument(format!(
                "data_generation {generation} is not retained"
            ))
        })
}

fn find_visible_record_in_manifest(
    store_dir: &Path,
    manifest: &DataManifestSnapshot,
    key: &[u8],
    snapshot_seqno: u64,
) -> Result<Option<InternalRecord>, KalanjiyamError> {
    if let Some(record) = manifest
        .active_memtable_ref
        .get_visible(key, snapshot_seqno)
    {
        return Ok(Some(record));
    }
    for frozen in manifest.frozen_memtable_refs.iter().rev() {
        if let Some(record) = frozen.get_visible(key, snapshot_seqno) {
            return Ok(Some(record));
        }
    }
    for file in files_covering_key(manifest, key) {
        if let Some(record) =
            find_visible_record_in_data_file(store_dir, &file, key, snapshot_seqno)?
        {
            return Ok(Some(record));
        }
    }
    Ok(None)
}

fn files_covering_key(manifest: &DataManifestSnapshot, key: &[u8]) -> Vec<FileMeta> {
    let mut files = Vec::new();
    if let Some(level0) = manifest.levels.first() {
        files.extend(
            level0
                .iter()
                .rev()
                .filter(|file| {
                    file.min_user_key.as_slice() <= key && key <= file.max_user_key.as_slice()
                })
                .cloned(),
        );
    }
    for level in manifest.levels.iter().skip(1) {
        let index = level.partition_point(|file| file.max_user_key.as_slice() < key);
        if let Some(file) = level.get(index)
            && file.min_user_key.as_slice() <= key
        {
            files.push(file.clone());
        }
    }
    files
}

fn files_overlapping_range(manifest: &DataManifestSnapshot, range: &KeyRange) -> Vec<FileMeta> {
    manifest
        .levels
        .iter()
        .flat_map(|files| {
            files
                .iter()
                .filter(|file| range.overlaps_file(&file.min_user_key, &file.max_user_key))
        })
        .cloned()
        .collect()
}

fn scan_manifest(
    store_dir: &Path,
    manifest: &DataManifestSnapshot,
    range: &KeyRange,
    resume_after_key: Option<&[u8]>,
    snapshot_seqno: u64,
    max_records_per_page: u32,
    max_bytes_per_page: u32,
) -> Result<ScanPage, KalanjiyamError> {
    execute_scan_sources(
        collect_scan_source_iterators_from_manifest(store_dir, manifest, range, resume_after_key)?,
        snapshot_seqno,
        max_records_per_page,
        max_bytes_per_page,
    )
}

fn page_rows(rows: Vec<ScanRow>, max_records_per_page: u32, max_bytes_per_page: u32) -> ScanPage {
    if rows.is_empty() {
        return ScanPage {
            rows: Vec::new(),
            eof: true,
            next_resume_after_key: None,
        };
    }

    let total_rows = rows.len();
    let first_row = rows.first().cloned();
    let mut page_rows = Vec::new();
    let mut used_bytes = 0_u32;
    for row in rows {
        let row_bytes = (row.key.len() + row.value.len()) as u32;
        let would_exceed_records = page_rows.len() as u32 >= max_records_per_page;
        let would_exceed_bytes =
            !page_rows.is_empty() && used_bytes + row_bytes > max_bytes_per_page;
        if would_exceed_records || would_exceed_bytes {
            break;
        }
        used_bytes += row_bytes;
        page_rows.push(row);
    }
    if page_rows.is_empty() {
        let first = first_row.expect("rows is non-empty");
        return ScanPage {
            rows: vec![first.clone()],
            eof: false,
            next_resume_after_key: Some(first.key),
        };
    }

    let eof = page_rows.len() == total_rows;
    let next_resume_after_key = if eof {
        None
    } else {
        Some(page_rows.last().expect("non-empty page").key.clone())
    };
    ScanPage {
        rows: page_rows,
        eof,
        next_resume_after_key,
    }
}

fn build_get_response(
    record: InternalRecord,
    observation_seqno: u64,
    data_generation: u64,
) -> GetResponse {
    GetResponse {
        found: record.kind == RecordKind::Put,
        value: if record.kind == RecordKind::Put {
            record.value
        } else {
            None
        },
        observation_seqno,
        data_generation,
    }
}

fn find_matching_logical_slice(
    current: &[LogicalShardEntry],
    source: &[LogicalShardEntry],
) -> Option<(usize, usize)> {
    if source.is_empty() || source.len() > current.len() {
        return None;
    }
    current
        .windows(source.len())
        .position(|window| window == source)
        .map(|start| (start, start + source.len()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;

    fn run_async<T>(future: impl Future<Output = T>) -> T {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("test runtime should build");
        runtime.block_on(future)
    }

    fn create_store(sync_mode: &str) -> (tempfile::TempDir, PathBuf) {
        let tempdir = tempfile::tempdir().expect("tempdir should be creatable for engine tests");
        let store_dir = tempdir.path().join("store");
        fs::create_dir_all(store_dir.join("wal")).expect("wal dir should be creatable");
        fs::create_dir_all(store_dir.join("meta")).expect("meta dir should be creatable");
        fs::create_dir_all(store_dir.join("data")).expect("data dir should be creatable");
        let config_path = store_dir.join("config.toml");
        let config = format!(
            r#"
[engine]
sync_mode = "{sync_mode}"
page_size_bytes = 4096

[wal]
segment_bytes = 1073741824
group_commit_bytes = 65536
group_commit_max_delay_ms = 50

[lsm]
memtable_flush_bytes = 67108864
base_level_bytes = 268435456
level_fanout = 10
l0_file_threshold = 8
max_levels = 7

[maintenance]
max_external_requests = 64
max_scan_fetch_queue = 8
max_scan_sessions = 32
max_worker_tasks = 64
max_waiting_wal_syncs = 64
scan_expiry_ms = 30000
retry_delay_ms = 25
worker_threads = 2
gc_interval_secs = 300
checkpoint_interval_secs = 300
"#
        );
        fs::write(&config_path, config).expect("config fixture should be writable");
        (tempdir, config_path)
    }

    fn record(seqno: u64, key: &[u8], kind: RecordKind, value: Option<&[u8]>) -> InternalRecord {
        InternalRecord {
            user_key: key.to_vec(),
            seqno,
            kind,
            value: value.map(|bytes| bytes.to_vec()),
        }
    }

    fn write_single_data_file(
        store_dir: &Path,
        file_id: u64,
        level_no: u16,
        key: &[u8],
        seqno: u64,
        value: &[u8],
    ) -> FileMeta {
        let records = vec![record(seqno, key, RecordKind::Put, Some(value))];
        let mut meta = build_file_meta(file_id, level_no, &records);
        let path = write_data_file(store_dir, &meta, &records, 4096)
            .expect("data file fixture should be writable");
        meta.physical_bytes = fs::metadata(path)
            .expect("data file fixture should be stat-able")
            .len();
        meta
    }

    #[test]
    fn finish_get_rejects_mismatched_observation_and_generation() {
        let (_tempdir, config_path) = create_store("manual");
        let engine = run_async(PezhaiEngine::open(&config_path)).expect("open should succeed");
        let plan = GetPlan {
            key: b"ant".to_vec(),
            snapshot_seqno: 7,
            data_generation: 3,
            frozen_memtables: Vec::new(),
            candidate_files: Vec::new(),
        };
        let mismatched = GetResponse {
            found: true,
            value: Some(b"value".to_vec()),
            observation_seqno: 8,
            data_generation: 3,
        };
        assert!(matches!(
            engine.finish_get(&plan, mismatched),
            Err(KalanjiyamError::Corruption(_))
        ));

        let matched = GetResponse {
            found: true,
            value: Some(b"value".to_vec()),
            observation_seqno: 7,
            data_generation: 3,
        };
        assert_eq!(
            engine
                .finish_get(&plan, matched.clone())
                .expect("matching get result should succeed"),
            matched
        );
    }

    #[test]
    fn finish_scan_page_rejects_empty_non_eof_page() {
        let (_tempdir, config_path) = create_store("manual");
        let engine = run_async(PezhaiEngine::open(&config_path)).expect("open should succeed");
        let plan = ScanPlan::default();
        assert!(matches!(
            engine.finish_scan_page(
                &plan,
                ScanPage {
                    rows: Vec::new(),
                    eof: false,
                    next_resume_after_key: Some(b"ant".to_vec()),
                }
            ),
            Err(KalanjiyamError::Corruption(_))
        ));

        let eof_page = ScanPage {
            rows: Vec::new(),
            eof: true,
            next_resume_after_key: None,
        };
        assert_eq!(
            engine
                .finish_scan_page(&plan, eof_page.clone())
                .expect("EOF empty page should be accepted"),
            eof_page
        );
    }

    #[test]
    fn publish_gc_task_result_rejects_mismatch_and_prunes_generations() {
        let (_tempdir, config_path) = create_store("manual");
        let mut engine = run_async(PezhaiEngine::open(&config_path)).expect("open should succeed");
        engine
            .state
            .manifest_index
            .by_generation
            .insert(7, DataManifestSnapshot::default());
        engine
            .state
            .manifest_index
            .by_generation
            .insert(8, DataManifestSnapshot::default());

        let plan = GcTaskPlan {
            deletable_files: Vec::new(),
            prunable_manifest_generations: vec![7],
        };
        let mismatched = GcTaskResult {
            deleted_files: Vec::new(),
            pruned_manifest_generations: vec![8],
        };
        assert!(matches!(
            engine.publish_gc_task_result(&plan, &mismatched),
            Err(KalanjiyamError::Corruption(_))
        ));

        let matching = GcTaskResult {
            deleted_files: Vec::new(),
            pruned_manifest_generations: vec![7],
        };
        engine
            .publish_gc_task_result(&plan, &matching)
            .expect("matching gc publish should succeed");
        assert!(
            !engine.state.manifest_index.by_generation.contains_key(&7),
            "matching publish should prune requested manifest generations"
        );
        assert!(
            engine.state.manifest_index.by_generation.contains_key(&8),
            "unrelated generations should remain after gc publish"
        );
    }

    #[test]
    fn force_freeze_reuses_one_arc_for_historical_and_frozen_sources() {
        let (_tempdir, config_path) = create_store("manual");
        let mut engine = run_async(PezhaiEngine::open(&config_path)).expect("open should succeed");
        run_async(engine.put(b"ant".to_vec(), b"value-one".to_vec())).expect("put should succeed");
        let generation_before = engine.state.data_generation;

        let frozen = engine
            .force_freeze()
            .expect("freeze should succeed")
            .expect("freeze should return one memtable");
        let historical = engine
            .state
            .manifest_index
            .by_generation
            .get(&generation_before)
            .expect("freeze should retain the historical generation");
        let frozen_ref = engine
            .state
            .current_manifest
            .frozen_memtable_refs
            .first()
            .expect("freeze should retain one frozen memtable");

        assert!(
            Arc::ptr_eq(&historical.active_memtable_ref, frozen_ref),
            "freeze should share one immutable Arc between the historical active slot and the new \
             frozen queue"
        );
        assert_eq!(
            frozen_ref.frozen_memtable_id, frozen.frozen_memtable_id,
            "the retained frozen Arc and returned snapshot should agree on the frozen id"
        );
    }

    #[test]
    fn capture_flush_plan_shares_the_oldest_frozen_memtable_arc() {
        let (_tempdir, config_path) = create_store("manual");
        let mut engine = run_async(PezhaiEngine::open(&config_path)).expect("open should succeed");
        run_async(engine.put(b"ant".to_vec(), b"value-one".to_vec())).expect("put should succeed");
        engine.force_freeze().expect("freeze should succeed");

        let plan = engine
            .capture_flush_plan()
            .expect("flush plan capture should succeed")
            .expect("flush plan should exist");
        let oldest = engine
            .state
            .current_manifest
            .frozen_memtable_refs
            .first()
            .expect("freeze should retain one frozen memtable");

        assert!(
            Arc::ptr_eq(&plan.source_memtable, oldest),
            "flush plan capture should share the oldest frozen memtable instead of cloning it"
        );
    }

    #[test]
    fn validation_helpers_cover_flush_checkpoint_gc_and_rollover_edges() {
        let (_tempdir, config_path) = create_store("manual");
        let mut engine = run_async(PezhaiEngine::open(&config_path)).expect("open should succeed");
        run_async(engine.put(b"ant".to_vec(), b"value-one".to_vec())).expect("put should succeed");
        let frozen = engine
            .force_freeze()
            .expect("freeze should succeed")
            .expect("freeze should return one memtable");
        let flush_plan = FlushTaskPlan {
            source_generation: engine.state.data_generation,
            source_frozen_memtable_id: frozen.frozen_memtable_id.expect("frozen id should exist"),
            source_memtable: Arc::new(frozen.clone()),
            output_file_ids: vec![engine.state.next_file_id],
        };
        let flush_result = FlushTaskResult {
            source_generation: flush_plan.source_generation,
            source_frozen_memtable_id: flush_plan.source_frozen_memtable_id,
            outputs: vec![FileMeta {
                file_id: engine.state.next_file_id + 1,
                ..FileMeta::default()
            }],
        };
        assert!(matches!(
            engine.validate_flush_publication(&flush_plan, &flush_result),
            Err(KalanjiyamError::Corruption(_))
        ));

        engine.state.current_manifest.frozen_memtable_refs.clear();
        assert!(matches!(
            engine.validate_flush_publication(
                &flush_plan,
                &FlushTaskResult {
                    source_generation: flush_plan.source_generation,
                    source_frozen_memtable_id: flush_plan.source_frozen_memtable_id,
                    outputs: Vec::new(),
                }
            ),
            Err(KalanjiyamError::Stale(_))
        ));

        let checkpoint_plan = CheckpointTaskPlan {
            checkpoint_max_seqno: engine.state.last_committed_seqno,
            manifest_generation: engine.state.data_generation,
            checkpoint_state: CheckpointState::default(),
        };
        let checkpoint_result = CheckpointTaskResult {
            checkpoint_max_seqno: checkpoint_plan.checkpoint_max_seqno,
            manifest_generation: checkpoint_plan.manifest_generation,
            current: CurrentFile {
                checkpoint_generation: engine.state.next_checkpoint_generation + 1,
                checkpoint_max_seqno: checkpoint_plan.checkpoint_max_seqno,
                checkpoint_data_generation: checkpoint_plan.manifest_generation,
            },
        };
        assert!(matches!(
            engine.validate_checkpoint_publication(&checkpoint_plan, &checkpoint_result),
            Err(KalanjiyamError::Corruption(_))
        ));

        let gc_plan = GcTaskPlan {
            deletable_files: Vec::new(),
            prunable_manifest_generations: vec![1],
        };
        let gc_result = GcTaskResult {
            deleted_files: Vec::new(),
            pruned_manifest_generations: vec![2],
        };
        assert!(matches!(
            validate_gc_publication(&gc_plan, &gc_result),
            Err(KalanjiyamError::Corruption(_))
        ));

        assert!(!wal_rollover_required(10, 10, 512));
        assert!(wal_rollover_required(400, 32, 512));
    }

    #[test]
    fn execute_gc_task_ignores_missing_file_and_deletes_existing_file() {
        let tempdir = tempfile::tempdir().expect("tempdir should be creatable for gc tests");
        let existing = tempdir.path().join("existing.kjm");
        fs::write(&existing, b"obsolete").expect("fixture file should be writable");
        let missing = tempdir.path().join("missing.kjm");

        let plan = GcTaskPlan {
            deletable_files: vec![
                DurableFileRef::Data {
                    file_id: 1,
                    path: existing.clone(),
                },
                DurableFileRef::MetadataCheckpoint {
                    checkpoint_generation: 2,
                    path: missing.clone(),
                },
            ],
            prunable_manifest_generations: vec![3],
        };

        let result = execute_gc_task(&plan).expect("gc task should tolerate missing files");
        assert!(!existing.exists(), "gc task should delete existing files");
        assert_eq!(result.deleted_files, plan.deletable_files);
        assert_eq!(
            result.pruned_manifest_generations,
            plan.prunable_manifest_generations
        );
    }

    #[test]
    fn execute_get_plan_prefers_frozen_then_falls_back_to_file() {
        let (_tempdir, config_path) = create_store("manual");
        let store_dir = config_path
            .parent()
            .expect("config path should have store dir parent");
        let file_meta = write_single_data_file(store_dir, 1, 0, b"ant", 5, b"file");

        let mut frozen = Memtable::empty();
        frozen.insert_put(6, b"ant", b"frozen");
        let frozen_first = GetPlan {
            key: b"ant".to_vec(),
            snapshot_seqno: 6,
            data_generation: 2,
            frozen_memtables: vec![Arc::new(frozen)],
            candidate_files: vec![file_meta.clone()],
        };
        let from_frozen =
            execute_get_plan(store_dir, &frozen_first).expect("frozen path should succeed");
        assert_eq!(from_frozen.value, Some(b"frozen".to_vec()));

        let file_only = GetPlan {
            key: b"ant".to_vec(),
            snapshot_seqno: 6,
            data_generation: 2,
            frozen_memtables: Vec::new(),
            candidate_files: vec![file_meta],
        };
        let from_file = execute_get_plan(store_dir, &file_only).expect("file path should succeed");
        assert_eq!(from_file.value, Some(b"file".to_vec()));
    }

    #[test]
    fn lookup_get_plan_response_and_collect_scan_plan_records_cover_helper_fallbacks() {
        let (_tempdir, config_path) = create_store("manual");
        let store_dir = config_path
            .parent()
            .expect("config path should have store dir parent");
        let file_meta = write_single_data_file(store_dir, 9, 0, b"bee", 7, b"from-file");

        let missing = lookup_get_plan_response(
            store_dir,
            &GetPlan {
                key: b"none".to_vec(),
                snapshot_seqno: 7,
                data_generation: 0,
                frozen_memtables: Vec::new(),
                candidate_files: vec![file_meta.clone()],
            },
        )
        .expect("lookup helper should succeed");
        assert_eq!(missing, None);

        let mut active = Memtable::empty();
        active.insert_put(9, b"ant", b"from-active");
        let mut frozen = Memtable::empty();
        frozen.insert_put(8, b"cat", b"from-frozen");
        let records = collect_scan_plan_records(
            store_dir,
            &ScanPlan {
                scan_id: 1,
                snapshot_handle: SnapshotHandle {
                    engine_instance_id: 1,
                    snapshot_id: 1,
                    snapshot_seqno: 9,
                    data_generation: 0,
                },
                range: KeyRange::default(),
                resume_after_key: None,
                max_records_per_page: 10,
                max_bytes_per_page: 4096,
                active_memtable: active,
                frozen_memtables: vec![Arc::new(frozen)],
                candidate_files: vec![file_meta],
            },
        )
        .expect("scan record helper should collect records");
        assert_eq!(records.len(), 3);
    }

    #[test]
    fn execute_scan_plan_merges_active_frozen_and_file_sources() {
        let (_tempdir, config_path) = create_store("manual");
        let store_dir = config_path
            .parent()
            .expect("config path should have store dir parent");
        let file_meta = write_single_data_file(store_dir, 2, 0, b"bee", 7, b"from-file");

        let mut active = Memtable::empty();
        active.insert_put(9, b"ant", b"from-active");
        let mut frozen = Memtable::empty();
        frozen.insert_put(8, b"cat", b"from-frozen");
        let plan = ScanPlan {
            scan_id: 1,
            snapshot_handle: SnapshotHandle {
                engine_instance_id: 1,
                snapshot_id: 1,
                snapshot_seqno: 9,
                data_generation: 0,
            },
            range: KeyRange::default(),
            resume_after_key: None,
            max_records_per_page: 10,
            max_bytes_per_page: 4096,
            active_memtable: active,
            frozen_memtables: vec![Arc::new(frozen)],
            candidate_files: vec![file_meta],
        };

        let page = execute_scan_plan(store_dir, &plan).expect("scan plan should execute");
        assert!(page.eof);
        assert_eq!(
            page.rows
                .iter()
                .map(|row| row.key.clone())
                .collect::<Vec<_>>(),
            vec![b"ant".to_vec(), b"bee".to_vec(), b"cat".to_vec()]
        );
    }

    #[test]
    fn execute_scan_plan_skips_newer_versions_and_respects_cross_source_deletes() {
        let (_tempdir, config_path) = create_store("manual");
        let store_dir = config_path
            .parent()
            .expect("config path should have store dir parent");
        let file_records = vec![
            record(9, b"ant", RecordKind::Put, Some(b"from-file")),
            record(7, b"bee", RecordKind::Put, Some(b"from-file-bee")),
        ];
        let mut file_meta = build_file_meta(3, 0, &file_records);
        let path = write_data_file(store_dir, &file_meta, &file_records, 4096)
            .expect("data file fixture should be writable");
        file_meta.physical_bytes = fs::metadata(path)
            .expect("data file fixture should be stat-able")
            .len();

        let mut active = Memtable::empty();
        active.insert_put(12, b"ant", b"too-new");
        let mut frozen = Memtable::empty();
        frozen.insert_delete(10, b"ant");
        let plan = ScanPlan {
            scan_id: 2,
            snapshot_handle: SnapshotHandle {
                engine_instance_id: 1,
                snapshot_id: 1,
                snapshot_seqno: 10,
                data_generation: 0,
            },
            range: KeyRange::default(),
            resume_after_key: None,
            max_records_per_page: 10,
            max_bytes_per_page: 4096,
            active_memtable: active,
            frozen_memtables: vec![Arc::new(frozen)],
            candidate_files: vec![file_meta],
        };

        let page = execute_scan_plan(store_dir, &plan).expect("scan plan should execute");
        assert!(page.eof);
        assert_eq!(
            page.rows,
            vec![ScanRow {
                key: b"bee".to_vec(),
                value: b"from-file-bee".to_vec(),
            }]
        );
    }

    #[test]
    fn execute_scan_plan_forces_forward_progress_when_first_row_exceeds_byte_limit() {
        let (_tempdir, config_path) = create_store("manual");
        let store_dir = config_path
            .parent()
            .expect("config path should have store dir parent");
        let mut active = Memtable::empty();
        active.insert_put(9, b"ant", b"payload-too-large");
        let plan = ScanPlan {
            scan_id: 3,
            snapshot_handle: SnapshotHandle {
                engine_instance_id: 1,
                snapshot_id: 1,
                snapshot_seqno: 9,
                data_generation: 0,
            },
            range: KeyRange::default(),
            resume_after_key: None,
            max_records_per_page: 10,
            max_bytes_per_page: 1,
            active_memtable: active,
            frozen_memtables: Vec::new(),
            candidate_files: Vec::new(),
        };

        let page = execute_scan_plan(store_dir, &plan).expect("scan plan should execute");
        assert_eq!(page.rows.len(), 1);
        assert_eq!(page.rows[0].key, b"ant".to_vec());
        assert!(page.eof);
    }

    #[test]
    fn roll_wal_if_needed_closes_non_empty_active_segment_and_recreates_open_segment() {
        let (_tempdir, config_path) = create_store("manual");
        let mut engine = run_async(PezhaiEngine::open(&config_path)).expect("open should succeed");
        // Force rollover with tiny test-local limit; production validation still
        // enforces large segment sizes through config loading.
        engine.state.config.wal.segment_bytes = 320;

        run_async(engine.put(b"ant".to_vec(), b"value-one".to_vec())).expect("first put succeeds");
        run_async(engine.put(b"bee".to_vec(), b"value-two".to_vec()))
            .expect("second put triggers rollover");

        let wal_dir = engine.state.store_dir.join("wal");
        let closed = wal_dir.join("wal-00000000000000000001-00000000000000000001.log");
        assert!(
            closed.exists(),
            "second write should close and rename the previous active segment"
        );
        assert!(
            engine
                .active_wal_path()
                .ends_with("wal-00000000000000000002-open.log"),
            "append after rollover should rename the new open segment for the current seqno"
        );
        assert_eq!(
            engine.state.active_wal_record_count, 1,
            "new active segment should count only records appended after rollover"
        );
    }
}
