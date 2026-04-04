//! Shared integration-test fixtures for engine and server workflows.

use std::fs;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use kalanjiyam::{ExternalMethod, ExternalRequest};
use tempfile::TempDir;
use tokio::task::yield_now;

/// Store configuration knobs used by integration fixtures.
#[derive(Clone, Debug)]
pub struct StoreOptions {
    /// Engine sync mode string accepted by `config.toml`.
    pub sync_mode: &'static str,
    /// Page size for new `.kjm` artifacts.
    pub page_size_bytes: usize,
    /// Active memtable flush threshold.
    pub memtable_flush_bytes: u64,
    /// External request queue bound.
    pub max_external_requests: usize,
    /// Per-scan queued fetch bound.
    pub max_scan_fetch_queue: usize,
    /// Maximum active scan sessions.
    pub max_scan_sessions: usize,
    /// Generic worker queue bound.
    pub max_worker_tasks: usize,
    /// Waiting WAL sync bound.
    pub max_waiting_wal_syncs: usize,
    /// Scan session expiry in milliseconds.
    pub scan_expiry_ms: u64,
    /// Maintenance retry delay in milliseconds.
    pub retry_delay_ms: u64,
    /// Worker thread count.
    pub worker_threads: usize,
    /// GC interval in seconds.
    pub gc_interval_secs: u64,
    /// Checkpoint interval in seconds.
    pub checkpoint_interval_secs: u64,
    /// WAL segment size in bytes.
    pub segment_bytes: u64,
    /// WAL group commit byte threshold.
    pub group_commit_bytes: u64,
    /// WAL group commit max delay.
    pub group_commit_max_delay_ms: u64,
    /// Base level target bytes.
    pub base_level_bytes: u64,
    /// Level fanout.
    pub level_fanout: u32,
    /// L0 file threshold.
    pub l0_file_threshold: u32,
    /// Max tracked levels.
    pub max_levels: u32,
}

impl Default for StoreOptions {
    fn default() -> Self {
        Self {
            sync_mode: "manual",
            page_size_bytes: 4096,
            memtable_flush_bytes: 67_108_864,
            max_external_requests: 64,
            max_scan_fetch_queue: 8,
            max_scan_sessions: 32,
            max_worker_tasks: 64,
            max_waiting_wal_syncs: 64,
            scan_expiry_ms: 30_000,
            retry_delay_ms: 25,
            worker_threads: 2,
            gc_interval_secs: 300,
            checkpoint_interval_secs: 300,
            segment_bytes: 1_073_741_824,
            group_commit_bytes: 65_536,
            group_commit_max_delay_ms: 50,
            base_level_bytes: 268_435_456,
            level_fanout: 10,
            l0_file_threshold: 8,
            max_levels: 7,
        }
    }
}

pub fn run_async<T>(future: impl Future<Output = T>) -> T {
    let runtime = tokio::runtime::Builder::new_current_thread()
        // Shared integration fixtures now cover loopback TCP, so the helper
        // enables Tokio's I/O driver in addition to timer support.
        .enable_io()
        .enable_time()
        .build()
        .expect("integration test runtime should build");
    runtime.block_on(future)
}

pub fn create_store(options: &StoreOptions) -> (TempDir, PathBuf) {
    let tempdir = tempfile::tempdir().expect("tempdir should be creatable for integration tests");
    let store_dir = tempdir.path().join("store");
    fs::create_dir_all(store_dir.join("wal")).expect("fixture should create wal directory");
    fs::create_dir_all(store_dir.join("meta")).expect("fixture should create meta directory");
    fs::create_dir_all(store_dir.join("data")).expect("fixture should create data directory");
    let config_path = store_dir.join("config.toml");
    fs::write(&config_path, render_config(options)).expect("fixture should write config.toml");
    (tempdir, config_path)
}

pub fn store_dir(config_path: &Path) -> PathBuf {
    config_path
        .parent()
        .expect("config path should have a store parent")
        .to_path_buf()
}

pub fn request(client_id: &str, request_id: u64, method: ExternalMethod) -> ExternalRequest {
    ExternalRequest {
        client_id: client_id.to_string(),
        request_id,
        method,
        cancel_token: None,
    }
}

pub async fn wait_for_path_state(path: &Path, should_exist: bool, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if path.exists() == should_exist {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        yield_now().await;
    }
}

fn render_config(options: &StoreOptions) -> String {
    format!(
        r#"
[engine]
sync_mode = "{sync_mode}"
page_size_bytes = {page_size_bytes}

[wal]
segment_bytes = {segment_bytes}
group_commit_bytes = {group_commit_bytes}
group_commit_max_delay_ms = {group_commit_max_delay_ms}

[lsm]
memtable_flush_bytes = {memtable_flush_bytes}
base_level_bytes = {base_level_bytes}
level_fanout = {level_fanout}
l0_file_threshold = {l0_file_threshold}
max_levels = {max_levels}

[maintenance]
max_external_requests = {max_external_requests}
max_scan_fetch_queue = {max_scan_fetch_queue}
max_scan_sessions = {max_scan_sessions}
max_worker_tasks = {max_worker_tasks}
max_waiting_wal_syncs = {max_waiting_wal_syncs}
scan_expiry_ms = {scan_expiry_ms}
retry_delay_ms = {retry_delay_ms}
worker_threads = {worker_threads}
gc_interval_secs = {gc_interval_secs}
checkpoint_interval_secs = {checkpoint_interval_secs}
"#,
        sync_mode = options.sync_mode,
        page_size_bytes = options.page_size_bytes,
        segment_bytes = options.segment_bytes,
        group_commit_bytes = options.group_commit_bytes,
        group_commit_max_delay_ms = options.group_commit_max_delay_ms,
        memtable_flush_bytes = options.memtable_flush_bytes,
        base_level_bytes = options.base_level_bytes,
        level_fanout = options.level_fanout,
        l0_file_threshold = options.l0_file_threshold,
        max_levels = options.max_levels,
        max_external_requests = options.max_external_requests,
        max_scan_fetch_queue = options.max_scan_fetch_queue,
        max_scan_sessions = options.max_scan_sessions,
        max_worker_tasks = options.max_worker_tasks,
        max_waiting_wal_syncs = options.max_waiting_wal_syncs,
        scan_expiry_ms = options.scan_expiry_ms,
        retry_delay_ms = options.retry_delay_ms,
        worker_threads = options.worker_threads,
        gc_interval_secs = options.gc_interval_secs,
        checkpoint_interval_secs = options.checkpoint_interval_secs,
    )
}
