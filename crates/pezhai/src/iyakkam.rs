//! Public engine shell and shared operation foundations for the `Pezhai` storage engine.

use std::fs;
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard, mpsc};
use std::thread::JoinHandle;

use tokio::sync::oneshot;

use crate::config::{SyncMode, load_runtime_config};
use crate::error::Error;
use crate::idam::StoreLayout;
use crate::nilaimai::{EngineState, Mutation};
use crate::pathivu::{
    MAX_KEY_BYTES, MAX_VALUE_BYTES, WalRecovery, WalWriter, WalWriterTestOptions, read_current,
    recover_wal,
};

pub use crate::sevai::{Bound, GetResponse, LevelStats, LogicalShardStats, ScanRow, StatsResponse};

/// One stable read snapshot pinned by the engine until released.
#[derive(Clone, Debug, PartialEq, Eq)]
#[must_use = "snapshot handles keep retained read state until they are released"]
pub struct SnapshotHandle {
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
#[derive(Clone, Debug, PartialEq, Eq)]
#[must_use = "scan cursors own retained rows until the caller drains or drops them"]
pub struct ScanCursor {
    observation_seqno: u64,
    data_generation: u64,
    rows: Vec<ScanRow>,
    next_row_index: usize,
}

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
        if self.next_row_index >= self.rows.len() {
            return Ok(None);
        }

        let row = self.rows[self.next_row_index].clone();
        self.next_row_index += 1;
        Ok(Some(row))
    }
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
    /// This is synchronous because config parsing, store discovery, and WAL replay complete before
    /// the async data-plane methods are used.
    pub fn open(config_path: &Path) -> Result<Self, Error> {
        Self::open_with_options(config_path, EngineTestOptions::default())
    }

    /// Closes one open engine instance and waits for the internal WAL worker to stop.
    pub fn close(mut self) -> Result<(), Error> {
        self.shutdown_worker()
    }

    /// Creates one stable snapshot handle pinned to the current committed frontier.
    pub async fn create_snapshot(&self) -> Result<SnapshotHandle, Error> {
        let mut state = self.core.lock_state()?;
        let (snapshot_id, entry) = state.create_snapshot_entry();
        Ok(SnapshotHandle {
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
        let (seqno, sync_mode) = {
            let mut state = self.core.lock_state()?;
            (state.reserve_seqno()?, state.sync_mode())
        };

        let (reply_tx, reply_rx) = oneshot::channel();
        self.wal_runtime
            .command_tx
            .send(WalCommand::AppendMutation {
                seqno,
                mutation,
                sync_mode,
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
        let (seqno, sync_mode) = {
            let mut state = self.core.lock_state()?;
            (state.reserve_seqno()?, state.sync_mode())
        };

        let (reply_tx, reply_rx) = oneshot::channel();
        self.wal_runtime
            .command_tx
            .send(WalCommand::AppendMutation {
                seqno,
                mutation,
                sync_mode,
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
        let state = self.core.lock_state()?;
        let (snapshot_seqno, data_generation) = resolve_snapshot(&state, snapshot)?;
        let value = state.visible_value(key, snapshot_seqno);

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
        let state = self.core.lock_state()?;
        let (snapshot_seqno, data_generation) = resolve_snapshot(&state, snapshot)?;
        let rows = state.scan_rows(range.start_bound(), range.end_bound(), snapshot_seqno);

        Ok(ScanCursor {
            observation_seqno: snapshot_seqno,
            data_generation,
            rows,
            next_row_index: 0,
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
            levels: Vec::<LevelStats>::new(),
            logical_shards: vec![LogicalShardStats {
                start_bound: Bound::NegInf,
                end_bound: Bound::PosInf,
                live_size_bytes: state.current_live_size_bytes(),
            }],
        })
    }

    fn open_with_options(config_path: &Path, options: EngineTestOptions) -> Result<Self, Error> {
        let config = load_runtime_config(config_path).map_err(Error::from)?;
        let layout = StoreLayout::from_config_path(config_path);
        let _ = (layout.config_path(), layout.store_root());
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
            mutations,
            last_committed_seqno: _,
            active_segment,
        } = recover_wal(&layout, config.wal.segment_bytes)?;
        let mut state = EngineState::new(config.clone());
        for recovered in &mutations {
            state.apply_recovered_mutation(recovered.seqno, &recovered.mutation)?;
        }

        let writer = WalWriter::open(
            layout,
            config.wal.segment_bytes,
            active_segment,
            options.wal_writer,
        )?;
        let core = Arc::new(EngineCore {
            state: Mutex::new(state),
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
                    "WAL worker thread panicked during shutdown",
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
}

impl EngineCore {
    // The direct engine keeps mutation planning inside a short critical section and delegates all
    // WAL file I/O to the background worker so no durable work happens before suspension.
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
        seqno: u64,
        mutation: Mutation,
        sync_mode: SyncMode,
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
            WalCommand::AppendMutation {
                seqno,
                mutation,
                sync_mode,
                reply,
            } => {
                let result = match writer.append_mutation(seqno, &mutation, sync_mode) {
                    Ok(outcome) => match core.lock_state() {
                        Ok(mut state) => {
                            state.apply_live_mutation(seqno, &mutation, outcome.durably_synced)
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
            }
            WalCommand::Shutdown => break,
        }
    }

    writer.shutdown();
}

fn resolve_snapshot(
    state: &EngineState,
    snapshot: Option<&SnapshotHandle>,
) -> Result<(u64, u64), Error> {
    match snapshot {
        None => Ok((state.last_committed_seqno, state.data_generation)),
        Some(handle) => {
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
        format!("WAL worker is unavailable: {error}"),
    ))
}

fn wal_worker_reply_error(_error: oneshot::error::RecvError) -> Error {
    Error::Io(std::io::Error::new(
        std::io::ErrorKind::BrokenPipe,
        "WAL worker dropped the reply channel",
    ))
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
        assert_eq!(stats.data_generation, 3);
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
        assert_eq!(full_scan.data_generation(), 1);
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
}
