//! Public engine shell and shared operation foundations for the `Pezhai` storage engine.

use std::collections::HashMap;
use std::fs::{self, File};
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, mpsc};
use std::thread::JoinHandle;

use tokio::sync::oneshot;
use tokio::task;

use crate::config::load_runtime_config;
use crate::error::Error;
use crate::idam::StoreLayout;
use crate::nilaimai::{
    CompactPublishPayload, EngineState, FlushPublishPayload, Memtable, MemtableRef, Mutation,
};
use crate::pani::{
    build_compaction_output, build_flush_output, execute_point_read, execute_scan_plan,
};
use crate::pathivu::{
    MAX_KEY_BYTES, MAX_VALUE_BYTES, WalRecovery, WalWriter, WalWriterTestOptions, read_current,
    recover_wal,
};

use crate::sevai::{Bound, GetResponse, ScanRow, StatsResponse};

static NEXT_ENGINE_INSTANCE_ID: AtomicU64 = AtomicU64::new(1);

/// One stable read snapshot pinned by the engine until released.
#[derive(Clone, Debug, PartialEq, Eq)]
#[must_use = "snapshot handles keep retained read state until they are released"]
pub struct SnapshotHandle {
    engine_instance_id: u64,
    snapshot_id: u64,
    snapshot_seqno: u64,
    data_generation: u64,
}

impl SnapshotHandle {
    /// Returns the snapshot sequence number pinned by this handle.
    #[must_use]
    pub fn snapshot_seqno(&self) -> u64 {
        self.snapshot_seqno
    }

    /// Returns the data generation pinned by this handle.
    #[must_use]
    pub fn data_generation(&self) -> u64 {
        self.data_generation
    }
}

/// One forward scan range over the user-key space.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanRange {
    start_bound: Bound,
    end_bound: Bound,
}

impl ScanRange {
    /// Creates one validated half-open scan range.
    ///
    /// Returns `InvalidArgument` when the bounds are unordered or use the disallowed
    /// `PosInf` start / `NegInf` end combination.
    pub fn new(start_bound: Bound, end_bound: Bound) -> Result<Self, Error> {
        validate_scan_range(&start_bound, &end_bound)?;
        Ok(Self {
            start_bound,
            end_bound,
        })
    }

    /// Returns the full keyspace range `[-inf, +inf)`.
    #[must_use]
    pub fn full() -> Self {
        Self {
            start_bound: Bound::NegInf,
            end_bound: Bound::PosInf,
        }
    }

    /// Returns the inclusive lower bound.
    #[must_use]
    pub fn start_bound(&self) -> &Bound {
        &self.start_bound
    }

    /// Returns the exclusive upper bound.
    #[must_use]
    pub fn end_bound(&self) -> &Bound {
        &self.end_bound
    }
}

/// One owned async cursor over a scan result.
#[must_use = "scan cursors own retained rows until the caller drains or drops them"]
pub struct ScanCursor {
    observation_seqno: u64,
    data_generation: u64,
    rows: Vec<ScanRow>,
    next_row_index: usize,
    deferred: Option<DeferredScanExecution>,
}

impl Clone for ScanCursor {
    fn clone(&self) -> Self {
        Self {
            observation_seqno: self.observation_seqno,
            data_generation: self.data_generation,
            rows: self.rows.clone(),
            next_row_index: self.next_row_index,
            deferred: self.deferred.clone(),
        }
    }
}

impl std::fmt::Debug for ScanCursor {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("ScanCursor")
            .field("observation_seqno", &self.observation_seqno)
            .field("data_generation", &self.data_generation)
            .field("buffered_rows", &self.rows.len())
            .field("next_row_index", &self.next_row_index)
            .field("deferred", &self.deferred.is_some())
            .finish()
    }
}

impl PartialEq for ScanCursor {
    fn eq(&self, other: &Self) -> bool {
        self.observation_seqno == other.observation_seqno
            && self.data_generation == other.data_generation
            && self.rows == other.rows
            && self.next_row_index == other.next_row_index
            && self.deferred.is_some() == other.deferred.is_some()
    }
}

impl Eq for ScanCursor {}

impl ScanCursor {
    /// Returns the snapshot sequence number pinned by this cursor.
    #[must_use]
    pub fn observation_seqno(&self) -> u64 {
        self.observation_seqno
    }

    /// Returns the data generation pinned by this cursor.
    #[must_use]
    pub fn data_generation(&self) -> u64 {
        self.data_generation
    }

    /// Yields the next visible row for this cursor, or `None` after EOF.
    pub async fn next(&mut self) -> Result<Option<ScanRow>, Error> {
        loop {
            if self.next_row_index < self.rows.len() {
                let row = self.rows[self.next_row_index].clone();
                self.next_row_index += 1;
                return Ok(Some(row));
            }

            let Some(deferred) = self.deferred.take() else {
                return Ok(None);
            };

            self.rows = if deferred.plan.is_active_only() {
                execute_scan_plan(&deferred.layout, &deferred.plan)?
            } else {
                let layout = deferred.layout;
                let plan = deferred.plan;
                task::spawn_blocking(move || execute_scan_plan(&layout, &plan))
                    .await
                    .map_err(spawn_blocking_error)??
            };
            self.next_row_index = 0;
        }
    }
}

#[derive(Clone, Debug)]
struct DeferredScanExecution {
    layout: StoreLayout,
    plan: crate::nilaimai::ScanPlan,
}

/// Result payload returned by `sync()`.
#[derive(Clone, Debug, PartialEq, Eq)]
#[must_use = "the durable frontier result explains what `sync()` covered"]
pub struct SyncResponse {
    /// Latest durable sequence number published by the engine.
    pub durable_seqno: u64,
}

/// Direct storage-engine handle used by in-process callers.
pub struct PezhaiEngine {
    core: Arc<EngineCore>,
    wal_runtime: WalRuntime,
}

impl PezhaiEngine {
    /// Opens one engine instance from a validated config path.
    ///
    /// This is synchronous because config parsing, store discovery, replay, and worker bootstrap
    /// complete before the async data-plane methods are used.
    pub fn open(config_path: &Path) -> Result<Self, Error> {
        Self::open_with_options(config_path, EngineTestOptions::default())
    }

    /// Closes one open engine instance and waits for the internal worker to stop.
    pub fn close(mut self) -> Result<(), Error> {
        self.shutdown_worker()
    }

    /// Creates one stable snapshot handle pinned to the current committed frontier.
    pub async fn create_snapshot(&self) -> Result<SnapshotHandle, Error> {
        let mut state = self.core.lock_state()?;
        let engine_instance_id = state.engine_instance_id();
        let (snapshot_id, entry) = state.create_snapshot_entry();
        Ok(SnapshotHandle {
            engine_instance_id,
            snapshot_id,
            snapshot_seqno: entry.snapshot_seqno,
            data_generation: entry.data_generation,
        })
    }

    /// Releases one previously created snapshot handle.
    ///
    /// Returns `InvalidArgument` when the handle was already released or did not belong to this
    /// live engine instance.
    pub async fn release_snapshot(&self, handle: SnapshotHandle) -> Result<(), Error> {
        let mut state = self.core.lock_state()?;
        if handle.engine_instance_id != state.engine_instance_id() {
            return Err(Error::InvalidArgument(
                "snapshot handle does not belong to this open engine instance".into(),
            ));
        }

        match state.release_snapshot_entry(handle.snapshot_id) {
            Some(entry)
                if entry.snapshot_seqno == handle.snapshot_seqno
                    && entry.data_generation == handle.data_generation =>
            {
                Ok(())
            }
            Some(_) | None => Err(Error::InvalidArgument(
                "snapshot handle is unknown or already released".into(),
            )),
        }
    }

    /// Inserts or replaces one key/value pair.
    ///
    /// Returns `InvalidArgument` when the key or value exceeds the documented limits.
    pub async fn put(&self, key: &[u8], value: &[u8]) -> Result<(), Error> {
        validate_key(key)?;
        validate_value(value)?;
        let mutation = Mutation::Put {
            key: key.to_vec(),
            value: value.to_vec(),
        };

        let (reply_tx, reply_rx) = oneshot::channel();
        self.wal_runtime
            .command_tx
            .send(WalCommand::AppendMutation {
                mutation,
                reply: reply_tx,
            })
            .map_err(wal_worker_send_error)?;

        reply_rx.await.map_err(wal_worker_reply_error)?
    }

    /// Appends one tombstone for one key.
    ///
    /// Returns `InvalidArgument` when the key exceeds the documented limit.
    pub async fn delete(&self, key: &[u8]) -> Result<(), Error> {
        validate_key(key)?;
        let mutation = Mutation::Delete { key: key.to_vec() };

        let (reply_tx, reply_rx) = oneshot::channel();
        self.wal_runtime
            .command_tx
            .send(WalCommand::AppendMutation {
                mutation,
                reply: reply_tx,
            })
            .map_err(wal_worker_send_error)?;

        reply_rx.await.map_err(wal_worker_reply_error)?
    }

    /// Reads one key at either the latest view or one explicit snapshot.
    ///
    /// Returns `InvalidArgument` when the key is invalid or the supplied snapshot handle is no
    /// longer active.
    pub async fn get(
        &self,
        key: &[u8],
        snapshot: Option<&SnapshotHandle>,
    ) -> Result<GetResponse, Error> {
        validate_key(key)?;
        let (snapshot_seqno, data_generation, plan) = {
            let state = self.core.lock_state()?;
            let (snapshot_seqno, data_generation) = resolve_snapshot(&state, snapshot)?;
            if let Some(active_result) =
                state.active_memtable_result(data_generation, key, snapshot_seqno)?
            {
                return Ok(GetResponse {
                    found: active_result.is_some(),
                    value: active_result,
                    observation_seqno: snapshot_seqno,
                    data_generation,
                });
            }

            (
                snapshot_seqno,
                data_generation,
                state.plan_point_read(key, snapshot_seqno, data_generation)?,
            )
        };

        let layout = self.core.layout.clone();
        let value = task::spawn_blocking(move || execute_point_read(&layout, &plan))
            .await
            .map_err(spawn_blocking_error)??;

        Ok(GetResponse {
            found: value.is_some(),
            value,
            observation_seqno: snapshot_seqno,
            data_generation,
        })
    }

    /// Creates one owned forward scan cursor for either the latest view or one explicit snapshot.
    ///
    /// Returns `InvalidArgument` when the range or snapshot handle is invalid.
    pub async fn scan(
        &self,
        range: ScanRange,
        snapshot: Option<&SnapshotHandle>,
    ) -> Result<ScanCursor, Error> {
        let (snapshot_seqno, data_generation, plan) = {
            let state = self.core.lock_state()?;
            let (snapshot_seqno, data_generation) = resolve_snapshot(&state, snapshot)?;
            (
                snapshot_seqno,
                data_generation,
                state.plan_scan(
                    range.start_bound(),
                    range.end_bound(),
                    snapshot_seqno,
                    data_generation,
                )?,
            )
        };

        Ok(ScanCursor {
            observation_seqno: snapshot_seqno,
            data_generation,
            rows: Vec::new(),
            next_row_index: 0,
            deferred: Some(DeferredScanExecution {
                layout: self.core.layout.clone(),
                plan,
            }),
        })
    }

    /// Advances and reports the current durable frontier.
    pub async fn sync(&self) -> Result<SyncResponse, Error> {
        let target_seqno = {
            let state = self.core.lock_state()?;
            let target_seqno = state.sync_target_seqno()?;
            if state.durable_seqno >= target_seqno {
                return Ok(SyncResponse {
                    durable_seqno: state.durable_seqno,
                });
            }
            target_seqno
        };

        let (reply_tx, reply_rx) = oneshot::channel();
        self.wal_runtime
            .command_tx
            .send(WalCommand::Sync {
                target_seqno,
                reply: reply_tx,
            })
            .map_err(wal_worker_send_error)?;

        let durable_seqno = reply_rx.await.map_err(wal_worker_reply_error)??;
        Ok(SyncResponse { durable_seqno })
    }

    /// Returns the current engine stats view without performing external I/O.
    pub async fn stats(&self) -> Result<StatsResponse, Error> {
        let state = self.core.lock_state()?;
        Ok(StatsResponse {
            observation_seqno: state.last_committed_seqno,
            data_generation: state.data_generation,
            levels: state.current_level_stats(),
            logical_shards: state.current_logical_shard_stats(),
        })
    }

    fn open_with_options(config_path: &Path, options: EngineTestOptions) -> Result<Self, Error> {
        let config = load_runtime_config(config_path).map_err(Error::from)?;
        let layout = StoreLayout::from_config_path(config_path);
        fs::create_dir_all(layout.wal_dir()).map_err(Error::Io)?;
        fs::create_dir_all(layout.meta_dir()).map_err(Error::Io)?;
        fs::create_dir_all(layout.data_dir()).map_err(Error::Io)?;

        if let Some(current) = read_current(&layout)?
            && (current.checkpoint_generation != 0
                || current.checkpoint_max_seqno != 0
                || current.checkpoint_data_generation != 0)
        {
            return Err(Error::Corruption(
                "CURRENT references metadata checkpoints that are not supported until milestone 4"
                    .into(),
            ));
        }

        let WalRecovery {
            pending_mutations,
            levels,
            last_committed_seqno,
            data_generation,
            next_file_id,
            active_segment,
        } = recover_wal(&layout, config.wal.segment_bytes)?;
        let engine_instance_id = NEXT_ENGINE_INSTANCE_ID.fetch_add(1, Ordering::Relaxed);
        let active_memtable: MemtableRef = Arc::new(Mutex::new(Memtable::default()));
        for recovered in &pending_mutations {
            EngineState::apply_recovered_mutation(
                &active_memtable,
                recovered.seqno,
                &recovered.mutation,
            )?;
        }
        let mut state = EngineState::from_recovery(
            config.clone(),
            engine_instance_id,
            data_generation,
            next_file_id,
            last_committed_seqno,
            active_memtable,
            levels,
        );
        let visible_cache = rebuild_visible_bytes_cache(&layout, &state)?;
        state.install_visible_bytes_cache(visible_cache);

        let writer = WalWriter::open(
            layout.clone(),
            config.wal.segment_bytes,
            active_segment,
            options.wal_writer,
        )?;
        let core = Arc::new(EngineCore {
            state: Mutex::new(state),
            layout,
        });
        let (command_tx, command_rx) = mpsc::channel();
        let worker_core = Arc::clone(&core);
        let join_handle = std::thread::Builder::new()
            .name("pezhai-wal".into())
            .spawn(move || wal_worker_loop(worker_core, writer, command_rx))
            .map_err(Error::Io)?;

        Ok(Self {
            core,
            wal_runtime: WalRuntime {
                command_tx,
                join_handle: Some(join_handle),
            },
        })
    }

    fn shutdown_worker(&mut self) -> Result<(), Error> {
        let _ = self.wal_runtime.command_tx.send(WalCommand::Shutdown);
        if let Some(join_handle) = self.wal_runtime.join_handle.take() {
            join_handle.join().map_err(|_| {
                Error::Io(std::io::Error::other(
                    "engine worker thread panicked during shutdown",
                ))
            })?;
        }

        Ok(())
    }
}

impl Drop for PezhaiEngine {
    fn drop(&mut self) {
        let _ = self.shutdown_worker();
    }
}

struct EngineCore {
    state: Mutex<EngineState>,
    layout: StoreLayout,
}

impl EngineCore {
    // The public engine keeps snapshot validation and plan capture inside a short critical section
    // and delegates all blocking file work to the worker thread or Tokio's blocking pool.
    fn lock_state(&self) -> Result<MutexGuard<'_, EngineState>, Error> {
        self.state
            .lock()
            .map_err(|_| Error::Corruption("engine state mutex was poisoned".into()))
    }
}

struct WalRuntime {
    command_tx: mpsc::Sender<WalCommand>,
    join_handle: Option<JoinHandle<()>>,
}

enum WalCommand {
    AppendMutation {
        mutation: Mutation,
        reply: oneshot::Sender<Result<(), Error>>,
    },
    Sync {
        target_seqno: u64,
        reply: oneshot::Sender<Result<u64, Error>>,
    },
    Shutdown,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct EngineTestOptions {
    wal_writer: WalWriterTestOptions,
}

fn wal_worker_loop(
    core: Arc<EngineCore>,
    mut writer: WalWriter,
    command_rx: mpsc::Receiver<WalCommand>,
) {
    while let Ok(command) = command_rx.recv() {
        match command {
            WalCommand::AppendMutation { mutation, reply } => {
                let result = match core.lock_state() {
                    Ok(mut state) => {
                        let seqno = match state.reserve_seqno() {
                            Ok(seqno) => seqno,
                            Err(error) => {
                                let _ = reply.send(Err(error));
                                continue;
                            }
                        };
                        let sync_mode = state.sync_mode();
                        drop(state);

                        match writer.append_mutation(seqno, &mutation, sync_mode) {
                            Ok(outcome) => match core.lock_state() {
                                Ok(mut state) => state.apply_live_mutation(
                                    seqno,
                                    &mutation,
                                    outcome.durably_synced,
                                ),
                                Err(error) => Err(error),
                            },
                            Err(error) => {
                                if let Ok(mut state) = core.lock_state() {
                                    state.mark_write_failed(error.to_string());
                                }
                                Err(error)
                            }
                        }
                    }
                    Err(error) => Err(error),
                };
                if result.is_ok() {
                    run_maintenance_cycle(&core, &mut writer);
                }
                let _ = reply.send(result);
            }
            WalCommand::Sync {
                target_seqno,
                reply,
            } => {
                let result = match writer.sync_to_current_frontier(target_seqno) {
                    Ok(durable_seqno) => match core.lock_state() {
                        Ok(mut state) => {
                            state.mark_synced(durable_seqno);
                            Ok(durable_seqno)
                        }
                        Err(error) => Err(error),
                    },
                    Err(error) => {
                        if let Ok(mut state) = core.lock_state() {
                            state.mark_write_failed(error.to_string());
                        }
                        Err(error)
                    }
                };
                let _ = reply.send(result);
                run_maintenance_cycle(&core, &mut writer);
            }
            WalCommand::Shutdown => break,
        }
    }

    writer.shutdown();
}

fn run_maintenance_cycle(core: &EngineCore, writer: &mut WalWriter) {
    loop {
        let (page_size_bytes, flush_plan, compaction_plan) = match core.lock_state() {
            Ok(state) => {
                let flush_plan = state.plan_flush();
                let compaction_plan = if flush_plan.is_none() {
                    state.plan_compaction()
                } else {
                    None
                };
                (state.page_size_bytes(), flush_plan, compaction_plan)
            }
            Err(_) => return,
        };

        if let Some(plan) = flush_plan {
            if publish_flush_plan(core, writer, plan, page_size_bytes).is_err() {
                return;
            }
            continue;
        }
        if let Some(plan) = compaction_plan {
            if publish_compaction_plan(core, writer, plan, page_size_bytes).is_err() {
                return;
            }
            continue;
        }
        return;
    }
}

fn publish_flush_plan(
    core: &EngineCore,
    writer: &mut WalWriter,
    plan: crate::nilaimai::FlushPlan,
    page_size_bytes: u32,
) -> Result<(), Error> {
    let build = build_flush_output(&core.layout, page_size_bytes, &plan)?;
    let mut state = core.lock_state()?;
    let file_id = allocate_unused_file_id(&core.layout, state.next_file_id);
    let output_file_meta = build.summary.to_file_meta(file_id, 0);
    let payload = FlushPublishPayload {
        data_generation_expected: plan.data_generation_expected,
        source_first_seqno: plan.source.source_first_seqno,
        source_last_seqno: plan.source.source_last_seqno,
        source_record_count: plan.source.source_record_count,
        output_file_metas: vec![output_file_meta.clone()],
    };
    state.validate_flush_publish(&payload)?;

    fs::rename(&build.temp_path, core.layout.data_file_path(file_id)).map_err(Error::Io)?;
    sync_directory(core.layout.data_dir())?;

    let seqno = state.reserve_seqno()?;
    let sync_mode = state.sync_mode();
    match writer.append_flush_publish(seqno, &payload, sync_mode) {
        Ok(outcome) => state.apply_flush_publish(seqno, outcome.durably_synced, &payload),
        Err(error) => {
            state.mark_write_failed(error.to_string());
            Err(error)
        }
    }
}

fn publish_compaction_plan(
    core: &EngineCore,
    writer: &mut WalWriter,
    plan: crate::nilaimai::CompactionPlan,
    page_size_bytes: u32,
) -> Result<(), Error> {
    let build = build_compaction_output(&core.layout, page_size_bytes, &plan)?;
    let mut state = core.lock_state()?;
    let file_id = allocate_unused_file_id(&core.layout, state.next_file_id);
    let output_file_meta = build.summary.to_file_meta(file_id, plan.output_level_no);
    let payload = CompactPublishPayload {
        data_generation_expected: plan.data_generation_expected,
        input_file_ids: {
            let mut ids = plan
                .input_files
                .iter()
                .map(|file| file.file_id)
                .collect::<Vec<_>>();
            ids.sort_unstable();
            ids
        },
        output_file_metas: vec![output_file_meta.clone()],
    };
    state.validate_compact_publish(&payload)?;

    fs::rename(&build.temp_path, core.layout.data_file_path(file_id)).map_err(Error::Io)?;
    sync_directory(core.layout.data_dir())?;

    let seqno = state.reserve_seqno()?;
    let sync_mode = state.sync_mode();
    match writer.append_compact_publish(seqno, &payload, sync_mode) {
        Ok(outcome) => state.apply_compact_publish(seqno, outcome.durably_synced, &payload),
        Err(error) => {
            state.mark_write_failed(error.to_string());
            Err(error)
        }
    }
}

fn rebuild_visible_bytes_cache(
    layout: &StoreLayout,
    state: &EngineState,
) -> Result<HashMap<Vec<u8>, u64>, Error> {
    let plan = state.plan_scan(
        &Bound::NegInf,
        &Bound::PosInf,
        state.last_committed_seqno,
        state.data_generation,
    )?;
    let rows = execute_scan_plan(layout, &plan)?;
    let mut visible = HashMap::new();
    for row in rows {
        visible.insert(row.key.clone(), (row.key.len() + row.value.len()) as u64);
    }
    Ok(visible)
}

fn resolve_snapshot(
    state: &EngineState,
    snapshot: Option<&SnapshotHandle>,
) -> Result<(u64, u64), Error> {
    match snapshot {
        None => Ok((state.last_committed_seqno, state.data_generation)),
        Some(handle) => {
            if handle.engine_instance_id != state.engine_instance_id() {
                return Err(Error::InvalidArgument(
                    "snapshot handle does not belong to this open engine instance".into(),
                ));
            }

            let Some(entry) = state.snapshot_entry(handle.snapshot_id) else {
                return Err(Error::InvalidArgument(
                    "snapshot handle is unknown or already released".into(),
                ));
            };
            if entry.snapshot_seqno != handle.snapshot_seqno
                || entry.data_generation != handle.data_generation
            {
                return Err(Error::InvalidArgument(
                    "snapshot handle does not match the active snapshot table".into(),
                ));
            }

            Ok((entry.snapshot_seqno, entry.data_generation))
        }
    }
}

fn validate_key(key: &[u8]) -> Result<(), Error> {
    if key.is_empty() {
        return Err(Error::InvalidArgument("key must not be empty".into()));
    }
    validate_max_length(key.len(), MAX_KEY_BYTES, "key")
}

fn validate_value(value: &[u8]) -> Result<(), Error> {
    validate_max_length(value.len(), MAX_VALUE_BYTES, "value")
}

fn validate_max_length(length: usize, limit: usize, subject: &str) -> Result<(), Error> {
    if length > limit {
        return Err(Error::InvalidArgument(format!(
            "{subject} exceeds the {limit}-byte limit"
        )));
    }

    Ok(())
}

fn validate_scan_range(start_bound: &Bound, end_bound: &Bound) -> Result<(), Error> {
    if matches!(start_bound, Bound::PosInf) || matches!(end_bound, Bound::NegInf) {
        return Err(Error::InvalidArgument(
            "scan range must use NegInf/Finite for start and Finite/PosInf for end".into(),
        ));
    }
    if compare_bound(start_bound, end_bound) >= 0 {
        return Err(Error::InvalidArgument(
            "scan range must be non-empty".into(),
        ));
    }

    Ok(())
}

fn compare_bound(left: &Bound, right: &Bound) -> i8 {
    match (left, right) {
        (Bound::NegInf, Bound::NegInf) | (Bound::PosInf, Bound::PosInf) => 0,
        (Bound::NegInf, _) | (_, Bound::PosInf) => -1,
        (Bound::PosInf, _) | (_, Bound::NegInf) => 1,
        (Bound::Finite(left), Bound::Finite(right)) => match left.cmp(right) {
            std::cmp::Ordering::Less => -1,
            std::cmp::Ordering::Equal => 0,
            std::cmp::Ordering::Greater => 1,
        },
    }
}

fn wal_worker_send_error(error: mpsc::SendError<WalCommand>) -> Error {
    Error::Io(std::io::Error::new(
        std::io::ErrorKind::BrokenPipe,
        format!("engine worker is unavailable: {error}"),
    ))
}

fn wal_worker_reply_error(_error: oneshot::error::RecvError) -> Error {
    Error::Io(std::io::Error::new(
        std::io::ErrorKind::BrokenPipe,
        "engine worker dropped the reply channel",
    ))
}

fn spawn_blocking_error(error: task::JoinError) -> Error {
    Error::Io(std::io::Error::other(format!(
        "blocking engine task failed: {error}"
    )))
}

fn sync_directory(path: &Path) -> Result<(), Error> {
    File::open(path)
        .map_err(Error::Io)?
        .sync_all()
        .map_err(Error::Io)
}

fn allocate_unused_file_id(layout: &StoreLayout, start: u64) -> u64 {
    let mut file_id = start.max(1);
    while layout.data_file_path(file_id).exists() {
        file_id += 1;
    }
    file_id
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{
        Bound, EngineTestOptions, MAX_KEY_BYTES, MAX_VALUE_BYTES, PezhaiEngine, ScanRange,
    };
    use crate::pathivu::{CurrentFile, WalWriterTestOptions, build_current_bytes};
    use crate::sevai::{LogicalShardStats, ScanRow, StatsResponse};

    static NEXT_CONFIG_ID: AtomicU64 = AtomicU64::new(0);

    #[tokio::test]
    async fn open_stats_and_close_cover_the_empty_store() {
        let config_path = write_test_config("per_write", None);
        let engine = PezhaiEngine::open(&config_path).unwrap();

        let stats = engine.stats().await.unwrap();
        assert_eq!(
            stats,
            StatsResponse {
                observation_seqno: 0,
                data_generation: 0,
                levels: Vec::new(),
                logical_shards: vec![LogicalShardStats {
                    start_bound: Bound::NegInf,
                    end_bound: Bound::PosInf,
                    live_size_bytes: 0,
                }],
            }
        );

        engine.close().unwrap();
    }

    #[tokio::test]
    async fn per_write_mode_persists_and_recovers_mutations() {
        let config_path = write_test_config("per_write", None);
        let engine = PezhaiEngine::open(&config_path).unwrap();
        engine.put(b"ant", b"a").await.unwrap();
        engine.put(b"bee", b"b").await.unwrap();
        engine.delete(b"bee").await.unwrap();
        engine.close().unwrap();

        let reopened = PezhaiEngine::open(&config_path).unwrap();
        let ant = reopened.get(b"ant", None).await.unwrap();
        assert_eq!(ant.value, Some(b"a".to_vec()));

        let bee = reopened.get(b"bee", None).await.unwrap();
        assert!(!bee.found);

        let stats = reopened.stats().await.unwrap();
        assert_eq!(stats.observation_seqno, 3);
        assert_eq!(stats.data_generation, 0);
        reopened.close().unwrap();
    }

    #[tokio::test]
    async fn manual_mode_batches_durability_until_sync() {
        let config_path = write_test_config("manual", None);
        let engine = PezhaiEngine::open(&config_path).unwrap();

        engine.put(b"ant", b"a").await.unwrap();
        let before_sync = engine.stats().await.unwrap();
        assert_eq!(before_sync.observation_seqno, 1);

        let sync = engine.sync().await.unwrap();
        assert_eq!(sync.durable_seqno, 1);
        engine.close().unwrap();
    }

    #[tokio::test]
    async fn snapshots_keep_reads_and_scans_stable() {
        let config_path = write_test_config("per_write", None);
        let engine = PezhaiEngine::open(&config_path).unwrap();

        engine.put(b"ant", b"v1").await.unwrap();
        engine.put(b"bee", b"b").await.unwrap();
        let snapshot = engine.create_snapshot().await.unwrap();

        engine.put(b"ant", b"v2").await.unwrap();
        engine.delete(b"bee").await.unwrap();
        engine.put(b"cat", b"c").await.unwrap();

        let get = engine.get(b"ant", Some(&snapshot)).await.unwrap();
        assert_eq!(get.value, Some(b"v1".to_vec()));

        let mut scan = engine
            .scan(
                ScanRange::new(Bound::NegInf, Bound::PosInf).unwrap(),
                Some(&snapshot),
            )
            .await
            .unwrap();
        assert_eq!(
            scan.next().await.unwrap(),
            Some(ScanRow {
                key: b"ant".to_vec(),
                value: b"v1".to_vec(),
            })
        );
        assert_eq!(
            scan.next().await.unwrap(),
            Some(ScanRow {
                key: b"bee".to_vec(),
                value: b"b".to_vec(),
            })
        );
        assert_eq!(scan.next().await.unwrap(), None);
        engine.close().unwrap();
    }

    #[tokio::test]
    async fn released_snapshots_and_invalid_keys_are_rejected() {
        let config_path = write_test_config("per_write", None);
        let engine = PezhaiEngine::open(&config_path).unwrap();

        let snapshot = engine.create_snapshot().await.unwrap();
        engine.release_snapshot(snapshot.clone()).await.unwrap();
        let stale_read = engine.get(b"ant", Some(&snapshot)).await.unwrap_err();
        assert!(
            stale_read
                .to_string()
                .contains("unknown or already released")
        );

        let empty_key = engine.put(b"", b"value").await.unwrap_err();
        assert!(empty_key.to_string().contains("key must not be empty"));
        engine.close().unwrap();
    }

    #[tokio::test]
    async fn latest_reads_follow_current_state() {
        let config_path = write_test_config("per_write", None);
        let engine = PezhaiEngine::open(&config_path).unwrap();

        engine.put(b"ant", b"a").await.unwrap();
        let latest_get = engine.get(b"ant", None).await.unwrap();
        assert_eq!(latest_get.value, Some(b"a".to_vec()));

        let mut full_scan = engine.scan(ScanRange::full(), None).await.unwrap();
        assert_eq!(full_scan.observation_seqno(), 1);
        assert_eq!(full_scan.data_generation(), 0);
        assert_eq!(
            full_scan.next().await.unwrap(),
            Some(ScanRow {
                key: b"ant".to_vec(),
                value: b"a".to_vec(),
            })
        );
        assert_eq!(full_scan.next().await.unwrap(), None);

        engine.delete(b"ant").await.unwrap();
        let deleted = engine.get(b"ant", None).await.unwrap();
        assert!(!deleted.found);
        assert_eq!(deleted.value, None);
        engine.close().unwrap();
    }

    #[tokio::test]
    async fn write_failures_stop_future_writes_but_not_reads() {
        let config_path = write_test_config("per_write", None);
        let options = EngineTestOptions {
            wal_writer: WalWriterTestOptions {
                fail_at_seqno: Some(1),
            },
        };
        let engine = PezhaiEngine::open_with_options(&config_path, options).unwrap();

        let error = engine.put(b"ant", b"a").await.unwrap_err();
        assert!(error.to_string().contains("injected WAL append failure"));

        let second_write = engine.put(b"bee", b"b").await.unwrap_err();
        assert!(
            second_write
                .to_string()
                .contains("injected WAL append failure")
        );

        let sync_error = engine.sync().await.unwrap_err();
        assert!(
            sync_error
                .to_string()
                .contains("injected WAL append failure")
        );

        let stats = engine.stats().await.unwrap();
        assert_eq!(stats.observation_seqno, 0);

        let snapshot = engine.create_snapshot().await.unwrap();
        assert_eq!(snapshot.snapshot_seqno(), 0);
        engine.close().unwrap();
    }

    #[tokio::test]
    async fn flush_publishes_one_l0_file_and_preserves_snapshot_visibility() {
        let config_path = write_test_config(
            "per_write",
            Some(
                r#"
[lsm]
memtable_flush_bytes = 16384
l0_file_threshold = 2
"#,
            ),
        );
        let engine = PezhaiEngine::open(&config_path).unwrap();

        engine.put(b"ant", &big_value(b'a')).await.unwrap();
        let snapshot = engine.create_snapshot().await.unwrap();
        engine.put(b"ant", &big_value(b'b')).await.unwrap();

        let latest = engine.get(b"ant", None).await.unwrap();
        assert_eq!(latest.value, Some(big_value(b'b')));

        let snap = engine.get(b"ant", Some(&snapshot)).await.unwrap();
        assert_eq!(snap.value, Some(big_value(b'a')));

        let stats = engine.stats().await.unwrap();
        assert_eq!(stats.data_generation, 2);
        assert_eq!(stats.levels.len(), 1);
        assert_eq!(stats.levels[0].level_no, 0);
        assert_eq!(stats.levels[0].file_count, 1);
        engine.close().unwrap();
    }

    #[tokio::test]
    async fn compaction_updates_level_stats_and_preserves_reads() {
        let config_path = write_test_config(
            "per_write",
            Some(
                r#"
[lsm]
memtable_flush_bytes = 16384
l0_file_threshold = 2
"#,
            ),
        );
        let engine = PezhaiEngine::open(&config_path).unwrap();

        engine.put(b"ant", &big_value(b'a')).await.unwrap();
        engine.put(b"bee", &big_value(b'b')).await.unwrap();
        engine.put(b"cat", &big_value(b'c')).await.unwrap();
        engine.put(b"dog", &big_value(b'd')).await.unwrap();

        let stats = engine.stats().await.unwrap();
        assert_eq!(stats.levels.len(), 2);
        assert_eq!(stats.levels[0].level_no, 0);
        assert_eq!(stats.levels[1].level_no, 1);

        let ant = engine.get(b"ant", None).await.unwrap();
        assert_eq!(ant.value, Some(big_value(b'a')));
        let dog = engine.get(b"dog", None).await.unwrap();
        assert_eq!(dog.value, Some(big_value(b'd')));
        engine.close().unwrap();
    }

    #[tokio::test]
    async fn orphan_data_files_remain_invisible_after_reopen() {
        let config_path = write_test_config(
            "per_write",
            Some(
                r#"
[lsm]
memtable_flush_bytes = 16384
l0_file_threshold = 2
"#,
            ),
        );
        let options = EngineTestOptions {
            wal_writer: WalWriterTestOptions {
                fail_at_seqno: Some(3),
            },
        };
        let engine = PezhaiEngine::open_with_options(&config_path, options).unwrap();

        engine.put(b"ant", &big_value(b'a')).await.unwrap();
        engine.put(b"bee", &big_value(b'b')).await.unwrap();

        let write_stopped = engine.put(b"cat", b"c").await.unwrap_err();
        assert!(
            write_stopped
                .to_string()
                .contains("injected WAL append failure")
                || write_stopped.to_string().contains("I/O error")
        );
        engine.close().unwrap();

        let reopened = PezhaiEngine::open(&config_path).unwrap();
        let stats = reopened.stats().await.unwrap();
        assert!(stats.levels.is_empty());
        let ant = reopened.get(b"ant", None).await.unwrap();
        assert_eq!(ant.value, Some(big_value(b'a')));
        reopened.close().unwrap();
    }

    #[test]
    fn open_rejects_current_pointers_to_future_checkpoint_milestones() {
        let config_path = write_test_config("per_write", None);
        let current_path = config_path.parent().unwrap().join("CURRENT");
        fs::write(
            &current_path,
            build_current_bytes(CurrentFile {
                checkpoint_generation: 1,
                checkpoint_max_seqno: 9,
                checkpoint_data_generation: 3,
            }),
        )
        .unwrap();

        let error = match PezhaiEngine::open(&config_path) {
            Ok(_) => panic!("CURRENT-backed opens should be rejected before checkpoint support"),
            Err(error) => error,
        };
        assert!(
            error
                .to_string()
                .contains("CURRENT references metadata checkpoints")
        );
    }

    #[test]
    fn max_length_validation_covers_key_and_value_limits() {
        let key_error =
            super::validate_max_length(MAX_KEY_BYTES + 1, MAX_KEY_BYTES, "key").unwrap_err();
        assert!(key_error.to_string().contains("key exceeds"));

        let value_error =
            super::validate_max_length(MAX_VALUE_BYTES + 1, MAX_VALUE_BYTES, "value").unwrap_err();
        assert!(value_error.to_string().contains("value exceeds"));
    }

    #[test]
    fn scan_range_rejects_invalid_bounds() {
        let error = ScanRange::new(Bound::PosInf, Bound::PosInf).unwrap_err();
        assert!(error.to_string().contains("scan range"));
    }

    fn write_test_config(sync_mode: &str, extra: Option<&str>) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let config_id = NEXT_CONFIG_ID.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!("pezhai-engine-{unique}-{config_id}"));
        fs::create_dir_all(&root).unwrap();
        let path = root.join("config.toml");
        let mut contents = format!(
            r#"
[engine]
sync_mode = "{sync_mode}"

[sevai]
listen_addr = "127.0.0.1:0"
"#
        );
        if let Some(extra) = extra {
            contents.push_str(extra);
        }
        fs::write(&path, contents).unwrap();
        path
    }

    fn big_value(fill: u8) -> Vec<u8> {
        vec![fill; 9000]
    }
}
