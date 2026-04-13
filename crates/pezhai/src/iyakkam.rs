//! Public engine shell and shared operation foundations for the `Pezhai` storage engine.

use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use crate::config::load_runtime_config;
use crate::error::Error;
use crate::idam::StoreLayout;
use crate::nilaimai::EngineState;
use crate::pathivu::{MAX_KEY_BYTES, MAX_VALUE_BYTES};

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
}

impl Clone for PezhaiEngine {
    fn clone(&self) -> Self {
        Self {
            core: Arc::clone(&self.core),
        }
    }
}

impl PezhaiEngine {
    /// Opens one engine instance from a validated config path.
    ///
    /// This is synchronous because bootstrap config and directory planning happen before the
    /// async data-plane methods are used.
    pub fn open(config_path: &Path) -> Result<Self, Error> {
        let config = load_runtime_config(config_path).map_err(Error::from)?;
        let layout = StoreLayout::from_config_path(config_path);
        let _ = (
            layout.config_path(),
            layout.store_root(),
            layout.wal_dir(),
            layout.meta_dir(),
            layout.data_dir(),
        );

        Ok(Self {
            core: Arc::new(EngineCore {
                state: Mutex::new(EngineState::new(config)),
            }),
        })
    }

    /// Closes one open engine instance.
    ///
    /// The milestone-1 foundation does not hold external durable resources yet, so close is a
    /// successful no-op.
    pub fn close(self) -> Result<(), Error> {
        drop(self);
        Ok(())
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
        self.core.lock_state()?.put(key, value);
        Ok(())
    }

    /// Appends one tombstone for one key.
    ///
    /// Returns `InvalidArgument` when the key exceeds the documented limit.
    pub async fn delete(&self, key: &[u8]) -> Result<(), Error> {
        validate_key(key)?;
        self.core.lock_state()?.delete(key);
        Ok(())
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
        let durable_seqno = self.core.lock_state()?.sync();
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
}

struct EngineCore {
    state: Mutex<EngineState>,
}

impl EngineCore {
    // The milestone-1 engine foundation keeps mutation inside a small synchronous critical
    // section so later durable work can separate admission from delegated I/O.
    fn lock_state(&self) -> Result<MutexGuard<'_, EngineState>, Error> {
        self.state
            .lock()
            .map_err(|_| Error::Corruption("engine state mutex was poisoned".into()))
    }
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

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::{Bound, MAX_KEY_BYTES, MAX_VALUE_BYTES, PezhaiEngine, ScanRange};
    use crate::sevai::{LogicalShardStats, ScanRow, StatsResponse};

    static NEXT_CONFIG_ID: AtomicU64 = AtomicU64::new(0);

    #[tokio::test]
    async fn open_stats_and_close_cover_the_empty_store() {
        let config_path = write_test_config("per_write");
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
    async fn per_write_mode_makes_puts_immediately_durable() {
        let config_path = write_test_config("per_write");
        let engine = PezhaiEngine::open(&config_path).unwrap();

        engine.put(b"ant", b"a").await.unwrap();
        let sync = engine.sync().await.unwrap();
        assert_eq!(sync.durable_seqno, 1);
    }

    #[tokio::test]
    async fn manual_mode_batches_durability_until_sync() {
        let config_path = write_test_config("manual");
        let engine = PezhaiEngine::open(&config_path).unwrap();

        engine.put(b"ant", b"a").await.unwrap();
        let before_sync = engine.stats().await.unwrap();
        assert_eq!(before_sync.observation_seqno, 1);

        let sync = engine.sync().await.unwrap();
        assert_eq!(sync.durable_seqno, 1);
    }

    #[tokio::test]
    async fn snapshots_keep_reads_and_scans_stable() {
        let config_path = write_test_config("per_write");
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
    }

    #[tokio::test]
    async fn latest_reads_follow_current_state() {
        let config_path = write_test_config("per_write");
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
    }

    #[tokio::test]
    async fn release_snapshot_rejects_double_release() {
        let config_path = write_test_config("per_write");
        let engine = PezhaiEngine::open(&config_path).unwrap();

        let snapshot = engine.create_snapshot().await.unwrap();
        engine.release_snapshot(snapshot.clone()).await.unwrap();
        let error = engine.release_snapshot(snapshot).await.unwrap_err();
        assert!(error.to_string().contains("unknown or already released"));
    }

    #[tokio::test]
    async fn released_snapshots_and_invalid_keys_are_rejected() {
        let config_path = write_test_config("per_write");
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
    }

    #[test]
    fn scan_range_rejects_invalid_bounds() {
        let error = ScanRange::new(Bound::PosInf, Bound::PosInf).unwrap_err();
        assert!(error.to_string().contains("scan range"));
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

    fn write_test_config(sync_mode: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let config_id = NEXT_CONFIG_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("pezhai-engine-{unique}-{config_id}.toml"));
        fs::write(
            &path,
            format!(
                r#"
[engine]
sync_mode = "{sync_mode}"

[sevai]
listen_addr = "127.0.0.1:0"
"#
            ),
        )
        .unwrap();
        path
    }
}
