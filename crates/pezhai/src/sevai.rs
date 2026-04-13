//! Transport-agnostic `PezhaiServer` runtime for the sevai control plane.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{Mutex as AsyncMutex, Notify, mpsc, oneshot};
use tokio::task::{self, JoinHandle};
use tokio::time::Instant;

use crate::config::{RuntimeConfig, SyncMode, load_runtime_config};
use crate::error::Error;
use crate::idam::StoreLayout;
use crate::iyakkam::{
    CapturedGet, EngineTestOptions, OpenedEngineRuntime, ResolvedSnapshot,
    append_live_mutation_unsynced, capture_get_request, capture_scan_page_request,
    collect_stats_response, create_snapshot_handle, open_engine_runtime, publish_compaction_build,
    publish_flush_build, publish_logical_install_payload, release_snapshot_handle,
    resolve_latest_snapshot, validate_key, validate_scan_range, validate_value,
};
use crate::nilaimai::{
    CompactionBuildResult, CompactionPlan, EngineState, FlushBuildResult, FlushPlan,
    LogicalMaintenancePlan, LogicalShardInstallPayload, MetadataCheckpointCapture, Mutation,
};
use crate::pani::{
    ScanPagePlan, ScanPageResult, build_compaction_output, build_flush_output, execute_gc,
    execute_logical_maintenance_plan, execute_point_read, execute_scan_page_plan,
};
use crate::pathivu::{
    CurrentFile, WalAppendState, WalSyncPlan, WalSyncResult, build_temp_metadata_checkpoint_file,
    execute_wal_sync_plan, install_current, install_metadata_checkpoint,
    list_canonical_data_file_ids, retained_wal_bytes_after, truncate_covered_closed_wal_segments,
};

/// Startup arguments for `PezhaiServer`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ServerBootstrapArgs {
    /// Path to the shared `config.toml` file used during bootstrap.
    pub config_path: PathBuf,
}

/// A cloneable async handle to the owner-task-backed sevai runtime.
pub struct PezhaiServer {
    shared: Arc<SharedServerHandle>,
}

impl Clone for PezhaiServer {
    fn clone(&self) -> Self {
        Self {
            shared: Arc::clone(&self.shared),
        }
    }
}

impl Drop for PezhaiServer {
    fn drop(&mut self) {
        if Arc::strong_count(&self.shared) == 1 {
            self.shared.set_lifecycle(SharedLifecycle::Stopping);
            let _ = self.shared.control_tx.send(ControlCommand::Shutdown);
        }
    }
}

/// One transport-agnostic request admitted by the server.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExternalRequest {
    /// Opaque client identifier used for ordering and cancellation.
    pub client_id: String,
    /// Monotonic request identifier within a client.
    pub request_id: u64,
    /// Optional transport-provided cancellation token.
    pub cancel_token: Option<String>,
    /// The logical operation the caller wants to run.
    pub method: ExternalMethod,
}

/// The transport-agnostic method set exposed by `PezhaiServer`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExternalMethod {
    /// Insert or replace one key/value pair.
    Put(PutRequest),
    /// Delete one key.
    Delete(DeleteRequest),
    /// Read the latest visible value for one key.
    Get(GetRequest),
    /// Start one stable paged scan snapshot.
    ScanStart(ScanStartRequest),
    /// Fetch the next page for an existing scan.
    ScanFetchNext(ScanFetchNextRequest),
    /// Read the server's current statistics view.
    Stats(StatsRequest),
}

/// Logical `Put` request payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PutRequest {
    /// User key bytes.
    pub key: Vec<u8>,
    /// User value bytes.
    pub value: Vec<u8>,
}

/// Logical `Delete` request payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DeleteRequest {
    /// User key bytes.
    pub key: Vec<u8>,
}

/// Logical `Get` request payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetRequest {
    /// User key bytes.
    pub key: Vec<u8>,
}

/// Logical `ScanStart` request payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanStartRequest {
    /// Inclusive lower bound, or negative infinity.
    pub start_bound: Bound,
    /// Exclusive upper bound, or positive infinity.
    pub end_bound: Bound,
    /// Maximum number of rows the server should emit per page.
    pub max_records_per_page: u32,
    /// Maximum logical bytes the server should target per page.
    pub max_bytes_per_page: u32,
}

/// Logical `ScanFetchNext` request payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanFetchNextRequest {
    /// Existing scan session identifier.
    pub scan_id: u64,
}

/// Logical `Stats` request payload.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct StatsRequest;

/// One transport-agnostic response emitted by the server.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExternalResponse {
    /// Opaque client identifier echoed from the request.
    pub client_id: String,
    /// Request identifier echoed from the request.
    pub request_id: u64,
    /// Terminal logical status for the request.
    pub status: Status,
    /// Method-specific success payload when `status.code` is `Ok`.
    pub payload: Option<ExternalResponsePayload>,
}

/// Method-specific success payloads for successful logical requests.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ExternalResponsePayload {
    Put(PutResponse),
    Delete(DeleteResponse),
    Get(GetResponse),
    ScanStart(ScanStartResponse),
    ScanFetchNext(ScanFetchNextResponse),
    Stats(StatsResponse),
}

/// Logical status shared by every external response.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Status {
    /// External status code.
    pub code: StatusCode,
    /// Whether retrying later can succeed without changing the request.
    pub retryable: bool,
    /// Optional diagnostic detail intended for operators and tests.
    pub message: Option<String>,
}

/// External status codes mirrored by the TCP transport envelope.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StatusCode {
    Ok,
    Busy,
    InvalidArgument,
    Io,
    Checksum,
    Corruption,
    Stale,
    Cancelled,
}

/// Inclusive or infinite bound for the logical scan API.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Bound {
    NegInf,
    Finite(Vec<u8>),
    PosInf,
}

/// Empty `Put` success payload.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PutResponse;

/// Empty `Delete` success payload.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DeleteResponse;

/// `Get` success payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GetResponse {
    /// Whether a value was present.
    pub found: bool,
    /// Current value when `found` is true.
    pub value: Option<Vec<u8>>,
    /// Latest committed seqno observed at admission time.
    pub observation_seqno: u64,
    /// Latest data generation observed at admission time.
    pub data_generation: u64,
}

/// `ScanStart` success payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanStartResponse {
    /// New scan session identifier.
    pub scan_id: u64,
    /// Latest committed seqno pinned for the scan snapshot.
    pub observation_seqno: u64,
    /// Data generation pinned for the scan snapshot.
    pub data_generation: u64,
}

/// One row returned by a paged scan.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanRow {
    /// Row key bytes.
    pub key: Vec<u8>,
    /// Row value bytes.
    pub value: Vec<u8>,
}

/// `ScanFetchNext` success payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScanFetchNextResponse {
    /// Scan rows returned for the page.
    pub rows: Vec<ScanRow>,
    /// Whether this page reached the end of the scan snapshot.
    pub eof: bool,
}

/// One level-wise stats entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LevelStats {
    /// Level number within the storage hierarchy.
    pub level_no: u32,
    /// Number of files in the level.
    pub file_count: u64,
    /// Logical bytes represented by the level.
    pub logical_bytes: u64,
    /// Physical bytes consumed by the level.
    pub physical_bytes: u64,
}

/// One logical-shard stats entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogicalShardStats {
    /// Inclusive lower bound for the shard.
    pub start_bound: Bound,
    /// Exclusive upper bound for the shard.
    pub end_bound: Bound,
    /// Current live logical bytes tracked for the shard.
    pub live_size_bytes: u64,
}

/// `Stats` success payload.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StatsResponse {
    /// Latest committed seqno visible at reply time.
    pub observation_seqno: u64,
    /// Latest data generation visible at reply time.
    pub data_generation: u64,
    /// Shared-level stats entries.
    pub levels: Vec<LevelStats>,
    /// Current logical-shard stats entries.
    pub logical_shards: Vec<LogicalShardStats>,
}

/// Shared lifecycle state mirrored outside the owner actor for admission checks.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum SharedLifecycle {
    Booting,
    Ready,
    Stopping,
    StartFailed,
    Stopped,
}

impl SharedLifecycle {
    fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Booting,
            1 => Self::Ready,
            2 => Self::Stopping,
            3 => Self::StartFailed,
            _ => Self::Stopped,
        }
    }

    fn as_u8(self) -> u8 {
        match self {
            Self::Booting => 0,
            Self::Ready => 1,
            Self::Stopping => 2,
            Self::StartFailed => 3,
            Self::Stopped => 4,
        }
    }
}

/// One external request envelope parked in the bounded admission channel.
struct ExternalEnvelope {
    request: ExternalRequest,
    reply: oneshot::Sender<ExternalResponse>,
}

/// Completion bookkeeping shared between the handle and owner task.
#[derive(Clone, Default)]
struct CompletionState {
    owner_result: Option<Result<(), String>>,
    active_runtime_tasks: usize,
}

/// Shared state exposed by the cloneable public handle.
struct SharedServerHandle {
    external_tx: mpsc::Sender<ExternalEnvelope>,
    control_tx: mpsc::UnboundedSender<ControlCommand>,
    completion_state: Mutex<CompletionState>,
    stopped: Notify,
    listen_addr: SocketAddr,
    lifecycle: AtomicU8,
}

impl SharedServerHandle {
    fn new(
        external_tx: mpsc::Sender<ExternalEnvelope>,
        control_tx: mpsc::UnboundedSender<ControlCommand>,
        listen_addr: SocketAddr,
    ) -> Self {
        Self {
            external_tx,
            control_tx,
            completion_state: Mutex::new(CompletionState::default()),
            stopped: Notify::new(),
            listen_addr,
            lifecycle: AtomicU8::new(SharedLifecycle::Booting.as_u8()),
        }
    }

    fn lifecycle(&self) -> SharedLifecycle {
        SharedLifecycle::from_u8(self.lifecycle.load(Ordering::Relaxed))
    }

    fn set_lifecycle(&self, lifecycle: SharedLifecycle) {
        self.lifecycle.store(lifecycle.as_u8(), Ordering::Relaxed);
        self.stopped.notify_waiters();
    }

    fn store_completion_result(&self, result: Result<(), Error>) {
        let stored = result.map_err(|error| error.to_string());
        self.completion_state
            .lock()
            .expect("completion mutex poisoned")
            .owner_result = Some(stored);
        self.set_lifecycle(SharedLifecycle::Stopped);
    }

    fn take_completion_result(&self) -> Option<Result<(), Error>> {
        let state = self
            .completion_state
            .lock()
            .expect("completion mutex poisoned");
        if state.owner_result.is_some() && state.active_runtime_tasks == 0 {
            return state
                .owner_result
                .clone()
                .map(|result| result.map_err(Error::ServerUnavailable));
        }
        None
    }

    fn owner_completion_result(&self) -> Option<Result<(), Error>> {
        self.completion_state
            .lock()
            .expect("completion mutex poisoned")
            .owner_result
            .clone()
            .map(|result| result.map_err(Error::ServerUnavailable))
    }

    fn register_runtime_task(self: &Arc<Self>) -> ServerRuntimeTaskGuard {
        self.completion_state
            .lock()
            .expect("completion mutex poisoned")
            .active_runtime_tasks += 1;
        ServerRuntimeTaskGuard {
            shared: Arc::clone(self),
        }
    }

    fn finish_runtime_task(&self) {
        let mut state = self
            .completion_state
            .lock()
            .expect("completion mutex poisoned");
        state.active_runtime_tasks = state
            .active_runtime_tasks
            .checked_sub(1)
            .expect("runtime task guard underflow");
        drop(state);
        self.stopped.notify_waiters();
    }
}

/// A registered runtime task that must complete before `PezhaiServer::wait_stopped` resolves.
#[must_use = "dropping the guard marks the runtime task as complete"]
pub struct ServerRuntimeTaskGuard {
    shared: Arc<SharedServerHandle>,
}

impl Drop for ServerRuntimeTaskGuard {
    fn drop(&mut self) {
        self.shared.finish_runtime_task();
    }
}

/// Internal runtime options used by tests to inject failures or timing edges.
#[derive(Clone, Copy, Debug, Default)]
struct OwnerRuntimeOptions {
    engine: EngineTestOptions,
    get_task_delay: Duration,
    scan_task_delay: Duration,
    wal_sync_delay: Duration,
}

/// One owner-only control command routed through the unbounded control channel.
enum ControlCommand {
    Cancel {
        client_id: String,
        cancel_token: String,
        reply: oneshot::Sender<bool>,
    },
    Shutdown,
    WorkerResult {
        task_id: u64,
        result: WorkerResult,
    },
    WalSyncSucceeded(WalSyncResult),
    WalSyncFailed {
        target_seqno: u64,
        message: String,
    },
    SweepExpiredSessions,
    RetryMaintenance {
        task_id: u64,
    },
}

/// One queued external request identified by `(client_id, request_id)`.
type RequestKey = (String, u64);

/// One tracked asynchronous request phase used by cancellation and completion routing.
struct RequestRecord {
    cancel_token: String,
    kind: AsyncRequestKind,
    phase: RequestPhase,
}

/// The async request kinds that remain tracked after owner admission.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AsyncRequestKind {
    Put,
    Delete,
    Get,
    ScanFetchNext,
}

/// One server-owned async phase for a tracked request.
enum RequestPhase {
    WaitingWorker {
        cancel_flag: Arc<AtomicBool>,
    },
    QueuedScanFetch {
        scan_id: u64,
    },
    ActiveScanFetch {
        scan_id: u64,
        cancel_flag: Arc<AtomicBool>,
    },
    WaitingDurability {
        target_seqno: u64,
    },
}

/// One per-client ordering table that emits replies strictly by `request_id`.
struct ClientState {
    last_admitted_request_id: Option<u64>,
    next_response_request_id: Option<u64>,
    waiters: BTreeMap<u64, oneshot::Sender<ExternalResponse>>,
    ready_responses: BTreeMap<u64, ExternalResponse>,
}

impl ClientState {
    fn new(first_request_id: u64) -> Self {
        Self {
            last_admitted_request_id: None,
            next_response_request_id: Some(first_request_id),
            waiters: BTreeMap::new(),
            ready_responses: BTreeMap::new(),
        }
    }
}

/// One terminal marker for a scan session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ScanTerminalState {
    Open,
    EofReached,
    Expired,
    Cancelled,
}

/// One server-owned scan session pinned to a single snapshot handle.
struct ScanSession {
    scan_id: u64,
    snapshot_handle: crate::SnapshotHandle,
    snapshot_seqno: u64,
    data_generation: u64,
    start_bound: Bound,
    end_bound: Bound,
    max_records_per_page: u32,
    max_bytes_per_page: u32,
    resume_after_key: Option<Vec<u8>>,
    queued_fetches: VecDeque<RequestKey>,
    active_fetch: Option<RequestKey>,
    in_flight_task_id: Option<u64>,
    terminal_state: ScanTerminalState,
    expires_at: Instant,
}

/// One maintenance task plan that can be retried without changing the public API.
#[derive(Clone, Debug)]
enum MaintenancePlan {
    Flush(FlushPlan),
    Compact(CompactionPlan),
    Logical(LogicalMaintenancePlan),
    Checkpoint(CheckpointTaskPlan),
    Gc(Vec<u64>),
}

/// One checkpoint task captured from the current engine state.
#[derive(Clone, Debug)]
struct CheckpointTaskPlan {
    capture: MetadataCheckpointCapture,
    page_size_bytes: u32,
    temp_tag: String,
}

/// One temporary checkpoint build returned by the worker pool.
struct CheckpointBuildResult {
    capture: MetadataCheckpointCapture,
    temp_tag: String,
    temp_path: PathBuf,
}

/// One worker-pool task sent through the bounded task queue.
struct WorkerTask {
    task_id: u64,
    cancel_flag: Arc<AtomicBool>,
    payload: WorkerTaskPayload,
}

/// The stateless worker payloads used by reads and background maintenance.
enum WorkerTaskPayload {
    Get(crate::nilaimai::PointReadPlan),
    ScanPage(ScanPagePlan),
    Flush {
        plan: FlushPlan,
        page_size_bytes: u32,
    },
    Compact {
        plan: CompactionPlan,
        page_size_bytes: u32,
    },
    Logical(LogicalMaintenancePlan),
    Checkpoint(CheckpointTaskPlan),
    Gc(Vec<u64>),
}

/// One worker result delivered back onto the owner control channel.
enum WorkerResult {
    Get(Result<Option<Vec<u8>>, Error>),
    ScanPage(Result<ScanPageResult, Error>),
    Flush(Result<FlushBuildResult, Error>),
    Compact(Result<CompactionBuildResult, Error>),
    Logical(Result<Option<LogicalShardInstallPayload>, Error>),
    Checkpoint(Result<CheckpointBuildResult, Error>),
    Gc(Result<Vec<u64>, Error>),
}

/// One tracked worker task entry keyed by `task_id`.
enum TaskEntry {
    Get {
        request_key: RequestKey,
        snapshot: ResolvedSnapshot,
    },
    ScanPage {
        request_key: RequestKey,
        scan_id: u64,
    },
    Maintenance {
        plan: MaintenancePlan,
        attempt: u8,
    },
}

/// One owner-side WAL sync request that has not yet reached the sync actor.
struct PendingWalSyncPlan {
    plan: WalSyncPlan,
}

/// One cached pending sync request inside the dedicated WAL sync actor.
struct WalSyncPendingEntry {
    plan: WalSyncPlan,
    oldest_requested_at: Instant,
}

/// One owner actor that embeds the recovered engine state directly.
struct OwnerState {
    _args: ServerBootstrapArgs,
    config: RuntimeConfig,
    shared: Arc<SharedServerHandle>,
    control_tx: mpsc::UnboundedSender<ControlCommand>,
    lifecycle: SharedLifecycle,
    engine: EngineState,
    layout: StoreLayout,
    wal_append_state: WalAppendState,
    next_scan_id: u64,
    next_task_id: u64,
    next_hidden_cancel_id: u64,
    clients: BTreeMap<String, ClientState>,
    requests: BTreeMap<RequestKey, RequestRecord>,
    cancel_index: BTreeMap<(String, String), BTreeSet<u64>>,
    scan_sessions: BTreeMap<u64, ScanSession>,
    durability_waiters: BTreeMap<u64, Vec<RequestKey>>,
    waiting_durability_waiters: usize,
    task_registry: BTreeMap<u64, TaskEntry>,
    in_flight_scan_tasks: usize,
    pending_wal_sync_plans: BTreeMap<u64, PendingWalSyncPlan>,
    active_flushes: usize,
    active_compactions: usize,
    checkpoint_in_flight: bool,
    logical_in_flight: bool,
    gc_in_flight: bool,
    worker_tx: Option<mpsc::Sender<WorkerTask>>,
    wal_sync_tx: Option<mpsc::Sender<WalSyncPlan>>,
    worker_handles: Vec<JoinHandle<()>>,
    wal_sync_handle: Option<JoinHandle<()>>,
    expiry_handle: Option<JoinHandle<()>>,
}

impl PezhaiServer {
    /// Starts the owner task, parses the shared config, and returns a handle.
    pub async fn start(args: ServerBootstrapArgs) -> Result<Self, Error> {
        Self::start_with_options(args, OwnerRuntimeOptions::default()).await
    }

    /// Submits one logical request and waits for the terminal response.
    pub async fn call(&self, request: ExternalRequest) -> Result<ExternalResponse, Error> {
        match self.shared.lifecycle() {
            SharedLifecycle::Ready => {}
            SharedLifecycle::Booting => {
                return Ok(io_response(
                    request.client_id,
                    request.request_id,
                    true,
                    "server is booting",
                ));
            }
            SharedLifecycle::Stopping => {
                return Ok(io_response(
                    request.client_id,
                    request.request_id,
                    true,
                    "server is stopping",
                ));
            }
            SharedLifecycle::StartFailed => {
                return Ok(io_response(
                    request.client_id,
                    request.request_id,
                    false,
                    "server startup failed",
                ));
            }
            SharedLifecycle::Stopped => {}
        }

        let (reply_tx, reply_rx) = oneshot::channel();
        match self.shared.external_tx.try_send(ExternalEnvelope {
            request: request.clone(),
            reply: reply_tx,
        }) {
            Ok(()) => reply_rx.await.map_err(|_| {
                Error::ServerUnavailable("owner task dropped the response channel".into())
            }),
            Err(mpsc::error::TrySendError::Full(envelope)) => Ok(busy_response(
                envelope.request.client_id,
                envelope.request.request_id,
                "server request queue is full",
            )),
            Err(mpsc::error::TrySendError::Closed(_)) => Err(Error::ServerUnavailable(
                "owner task is no longer accepting work".into(),
            )),
        }
    }

    /// Attempts to cancel one queued or pending request by transport token.
    pub async fn cancel(&self, client_id: String, cancel_token: String) -> Result<bool, Error> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.shared
            .control_tx
            .send(ControlCommand::Cancel {
                client_id,
                cancel_token,
                reply: reply_tx,
            })
            .map_err(|_| {
                Error::ServerUnavailable("owner task is no longer accepting work".into())
            })?;
        reply_rx.await.map_err(|_| {
            Error::ServerUnavailable("owner task dropped the cancellation reply".into())
        })
    }

    /// Requests the same owner-task shutdown path used by the final handle drop.
    pub async fn shutdown(&self) -> Result<(), Error> {
        self.shared.set_lifecycle(SharedLifecycle::Stopping);
        self.shared
            .control_tx
            .send(ControlCommand::Shutdown)
            .map_err(|_| Error::ServerUnavailable("owner task is already stopped".into()))
    }

    /// Waits until the owner task has finished shutdown and all registered runtime tasks have unwound.
    pub async fn wait_stopped(&self) -> Result<(), Error> {
        loop {
            if let Some(result) = self.shared.take_completion_result() {
                return result;
            }
            self.shared.stopped.notified().await;
        }
    }

    /// Returns the configured TCP listen address parsed during bootstrap.
    #[must_use]
    pub fn listen_addr(&self) -> SocketAddr {
        self.shared.listen_addr
    }

    /// Registers one externally owned runtime task that must finish before `wait_stopped` resolves.
    pub fn register_runtime_task(&self) -> ServerRuntimeTaskGuard {
        self.shared.register_runtime_task()
    }

    async fn start_with_options(
        args: ServerBootstrapArgs,
        options: OwnerRuntimeOptions,
    ) -> Result<Self, Error> {
        let config = load_runtime_config(&args.config_path).map_err(Error::from)?;
        let (external_tx, external_rx) =
            mpsc::channel(config.server_limits.max_pending_requests as usize);
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        let shared = Arc::new(SharedServerHandle::new(
            external_tx.clone(),
            control_tx.clone(),
            config.sevai.listen_addr,
        ));

        let config_path = args.config_path.clone();
        let config_for_open = config.clone();
        let engine_options = options.engine;
        let opened = task::spawn_blocking(move || {
            open_engine_runtime(&config_path, config_for_open, engine_options)
        })
        .await
        .map_err(spawn_blocking_join_error)??;

        let (worker_tx, worker_handles) = spawn_worker_pool(
            opened.layout.clone(),
            control_tx.clone(),
            config.server_limits.worker_parallelism,
            config.server_limits.max_worker_tasks,
            options,
        );
        let (wal_sync_tx, wal_sync_rx) =
            mpsc::channel(config.server_limits.max_waiting_durability_waiters as usize);
        let wal_sync_handle = tokio::spawn(wal_sync_actor(
            wal_sync_rx,
            control_tx.clone(),
            config.wal.group_commit_bytes,
            Duration::from_millis(config.wal.group_commit_max_delay_ms),
            options.wal_sync_delay,
        ));
        let expiry_handle = tokio::spawn(scan_expiry_tick(control_tx.clone()));

        let owner = OwnerState::new(
            args,
            config,
            opened,
            Arc::clone(&shared),
            control_tx,
            worker_tx,
            wal_sync_tx,
            worker_handles,
            wal_sync_handle,
            expiry_handle,
        );
        shared.set_lifecycle(SharedLifecycle::Ready);

        let join_handle = tokio::spawn(async move { owner.run(external_rx, control_rx).await });
        let completion_handle = Arc::clone(&shared);
        tokio::spawn(async move {
            let result = match join_handle.await {
                Ok(result) => result,
                Err(error) => Err(Error::ServerUnavailable(format!(
                    "owner task failed to join: {error}"
                ))),
            };
            completion_handle.store_completion_result(result);
        });

        Ok(Self { shared })
    }

    /// Waits until the owner task has stopped, without waiting for externally registered runtime tasks.
    pub async fn wait_owner_stopped(&self) -> Result<(), Error> {
        loop {
            if let Some(result) = self.shared.owner_completion_result() {
                return result;
            }
            self.shared.stopped.notified().await;
        }
    }
}

impl OwnerState {
    fn new(
        args: ServerBootstrapArgs,
        config: RuntimeConfig,
        opened: OpenedEngineRuntime,
        shared: Arc<SharedServerHandle>,
        control_tx: mpsc::UnboundedSender<ControlCommand>,
        worker_tx: mpsc::Sender<WorkerTask>,
        wal_sync_tx: mpsc::Sender<WalSyncPlan>,
        worker_handles: Vec<JoinHandle<()>>,
        wal_sync_handle: JoinHandle<()>,
        expiry_handle: JoinHandle<()>,
    ) -> Self {
        Self {
            _args: args,
            config,
            shared,
            control_tx,
            lifecycle: SharedLifecycle::Ready,
            engine: opened.state,
            layout: opened.layout,
            wal_append_state: opened.wal_append_state,
            next_scan_id: 1,
            next_task_id: 1,
            next_hidden_cancel_id: 1,
            clients: BTreeMap::new(),
            requests: BTreeMap::new(),
            cancel_index: BTreeMap::new(),
            scan_sessions: BTreeMap::new(),
            durability_waiters: BTreeMap::new(),
            waiting_durability_waiters: 0,
            task_registry: BTreeMap::new(),
            in_flight_scan_tasks: 0,
            pending_wal_sync_plans: BTreeMap::new(),
            active_flushes: 0,
            active_compactions: 0,
            checkpoint_in_flight: false,
            logical_in_flight: false,
            gc_in_flight: false,
            worker_tx: Some(worker_tx),
            wal_sync_tx: Some(wal_sync_tx),
            worker_handles,
            wal_sync_handle: Some(wal_sync_handle),
            expiry_handle: Some(expiry_handle),
        }
    }

    async fn run(
        mut self,
        mut external_rx: mpsc::Receiver<ExternalEnvelope>,
        mut control_rx: mpsc::UnboundedReceiver<ControlCommand>,
    ) -> Result<(), Error> {
        loop {
            tokio::select! {
                biased;
                maybe_control = control_rx.recv() => {
                    match maybe_control {
                        Some(ControlCommand::Shutdown) => {
                            self.begin_shutdown(&mut external_rx);
                            break;
                        }
                        Some(command) => self.handle_control(command),
                        None => break,
                    }
                }
                maybe_external = external_rx.recv() => {
                    match maybe_external {
                        Some(envelope) => self.handle_external(envelope),
                        None => break,
                    }
                }
            }
        }

        self.lifecycle = SharedLifecycle::Stopped;
        self.shared.set_lifecycle(SharedLifecycle::Stopped);
        self.shutdown_internal_tasks().await;
        Ok(())
    }

    fn handle_control(&mut self, command: ControlCommand) {
        match command {
            ControlCommand::Cancel {
                client_id,
                cancel_token,
                reply,
            } => {
                let cancelled = self.handle_cancel(&client_id, &cancel_token);
                let _ = reply.send(cancelled);
            }
            ControlCommand::Shutdown => {}
            ControlCommand::WorkerResult { task_id, result } => {
                self.handle_worker_result(task_id, result);
            }
            ControlCommand::WalSyncSucceeded(result) => {
                self.handle_wal_sync_success(result);
            }
            ControlCommand::WalSyncFailed {
                target_seqno,
                message,
            } => {
                self.handle_wal_sync_failure(target_seqno, message);
            }
            ControlCommand::SweepExpiredSessions => {
                self.expire_idle_sessions();
            }
            ControlCommand::RetryMaintenance { task_id } => {
                self.retry_maintenance(task_id);
            }
        }
    }

    fn handle_external(&mut self, envelope: ExternalEnvelope) {
        let request = envelope.request;
        let reply = envelope.reply;
        if self.lifecycle != SharedLifecycle::Ready {
            let _ = reply.send(lifecycle_response(
                request.client_id,
                request.request_id,
                self.lifecycle,
            ));
            return;
        }

        if let Some(rejected) = self.register_request(&request, reply) {
            let _ = rejected.reply.send(rejected.response);
            return;
        }

        let request_key = (request.client_id.clone(), request.request_id);
        let effective_cancel_token = request
            .cancel_token
            .clone()
            .unwrap_or_else(|| self.hidden_cancel_token());
        match &request.method {
            ExternalMethod::Put(body) => {
                if let Some(response) =
                    self.handle_put(&request_key, &request, body, &effective_cancel_token)
                {
                    self.finish_request(response);
                }
            }
            ExternalMethod::Delete(body) => {
                if let Some(response) =
                    self.handle_delete(&request_key, &request, body, &effective_cancel_token)
                {
                    self.finish_request(response);
                }
            }
            ExternalMethod::Get(body) => {
                if let Some(response) =
                    self.handle_get(&request_key, &request, body, &effective_cancel_token)
                {
                    self.finish_request(response);
                }
            }
            ExternalMethod::ScanStart(body) => {
                let response = self.handle_scan_start(&request, body);
                self.finish_request(response);
            }
            ExternalMethod::ScanFetchNext(body) => {
                self.handle_scan_fetch_next(&request_key, &request, body, &effective_cancel_token);
            }
            ExternalMethod::Stats(_body) => {
                let response = self.handle_stats(&request);
                self.finish_request(response);
            }
        }
    }

    fn handle_put(
        &mut self,
        request_key: &RequestKey,
        request: &ExternalRequest,
        body: &PutRequest,
        cancel_token: &str,
    ) -> Option<ExternalResponse> {
        // Writes become visible as soon as the owner appends and updates the active memtable.
        // In `PerWrite` mode the response stays parked on a durability waiter until the WAL
        // sync actor publishes a covering frontier.
        if let Err(error) = validate_key(&body.key) {
            return Some(error_response(request, error, false));
        }
        if let Err(error) = validate_value(&body.value) {
            return Some(error_response(request, error, false));
        }
        if let Some(error) = self.engine.write_stop_error() {
            return Some(error_response(request, error, true));
        }

        let sync_mode = self.config.engine.sync_mode;
        if sync_mode == SyncMode::PerWrite
            && self.waiting_durability_waiters
                >= self.config.server_limits.max_waiting_durability_waiters as usize
        {
            return Some(busy_response(
                request.client_id.clone(),
                request.request_id,
                "write durability waiter table is full",
            ));
        }

        let outcome = match append_live_mutation_unsynced(
            &mut self.engine,
            &mut self.wal_append_state,
            &Mutation::Put {
                key: body.key.clone(),
                value: body.value.clone(),
            },
        ) {
            Ok(outcome) => outcome,
            Err(error) => return Some(error_response(request, error, true)),
        };
        self.drive_maintenance();

        if sync_mode == SyncMode::Manual || outcome.durable_frontier_covered {
            return Some(ok_response(
                request.client_id.clone(),
                request.request_id,
                ExternalResponsePayload::Put(PutResponse),
            ));
        }

        self.track_request(
            request_key.clone(),
            cancel_token.to_string(),
            AsyncRequestKind::Put,
            RequestPhase::WaitingDurability {
                target_seqno: outcome.target_seqno,
            },
        );
        self.durability_waiters
            .entry(outcome.target_seqno)
            .or_default()
            .push(request_key.clone());
        self.waiting_durability_waiters += 1;
        match self.wal_append_state.sync_plan_for_outcome(&outcome) {
            Ok(plan) => self.enqueue_wal_sync_plan(plan),
            Err(error) => self.handle_wal_sync_failure(outcome.target_seqno, error.to_string()),
        }
        None
    }

    fn handle_delete(
        &mut self,
        request_key: &RequestKey,
        request: &ExternalRequest,
        body: &DeleteRequest,
        cancel_token: &str,
    ) -> Option<ExternalResponse> {
        if let Err(error) = validate_key(&body.key) {
            return Some(error_response(request, error, false));
        }
        if let Some(error) = self.engine.write_stop_error() {
            return Some(error_response(request, error, true));
        }

        let sync_mode = self.config.engine.sync_mode;
        if sync_mode == SyncMode::PerWrite
            && self.waiting_durability_waiters
                >= self.config.server_limits.max_waiting_durability_waiters as usize
        {
            return Some(busy_response(
                request.client_id.clone(),
                request.request_id,
                "write durability waiter table is full",
            ));
        }

        let outcome = match append_live_mutation_unsynced(
            &mut self.engine,
            &mut self.wal_append_state,
            &Mutation::Delete {
                key: body.key.clone(),
            },
        ) {
            Ok(outcome) => outcome,
            Err(error) => return Some(error_response(request, error, true)),
        };
        self.drive_maintenance();

        if sync_mode == SyncMode::Manual || outcome.durable_frontier_covered {
            return Some(ok_response(
                request.client_id.clone(),
                request.request_id,
                ExternalResponsePayload::Delete(DeleteResponse),
            ));
        }

        self.track_request(
            request_key.clone(),
            cancel_token.to_string(),
            AsyncRequestKind::Delete,
            RequestPhase::WaitingDurability {
                target_seqno: outcome.target_seqno,
            },
        );
        self.durability_waiters
            .entry(outcome.target_seqno)
            .or_default()
            .push(request_key.clone());
        self.waiting_durability_waiters += 1;
        match self.wal_append_state.sync_plan_for_outcome(&outcome) {
            Ok(plan) => self.enqueue_wal_sync_plan(plan),
            Err(error) => self.handle_wal_sync_failure(outcome.target_seqno, error.to_string()),
        }
        None
    }

    fn handle_get(
        &mut self,
        request_key: &RequestKey,
        request: &ExternalRequest,
        body: &GetRequest,
        cancel_token: &str,
    ) -> Option<ExternalResponse> {
        // The owner resolves the latest read snapshot once, answers active-memtable hits inline,
        // and only hands immutable frozen/file work to the worker pool after the fast path misses.
        if let Err(error) = validate_key(&body.key) {
            return Some(error_response(request, error, false));
        }

        let snapshot = resolve_latest_snapshot(&self.engine);
        let captured = match capture_get_request(&self.engine, &body.key, snapshot) {
            Ok(captured) => captured,
            Err(error) => return Some(error_response(request, error, false)),
        };
        match captured {
            CapturedGet::Inline(value) => Some(ok_response(
                request.client_id.clone(),
                request.request_id,
                ExternalResponsePayload::Get(GetResponse {
                    found: value.is_some(),
                    value,
                    observation_seqno: snapshot.snapshot_seqno,
                    data_generation: snapshot.data_generation,
                }),
            )),
            CapturedGet::Deferred(plan) => {
                let cancel_flag = Arc::new(AtomicBool::new(false));
                let task_id = self.next_task_id();
                let Some(worker_tx) = self.worker_tx.as_ref() else {
                    return Some(io_response(
                        request.client_id.clone(),
                        request.request_id,
                        true,
                        "worker pool is stopping",
                    ));
                };
                match worker_tx.try_send(WorkerTask {
                    task_id,
                    cancel_flag: Arc::clone(&cancel_flag),
                    payload: WorkerTaskPayload::Get(plan),
                }) {
                    Ok(()) => {
                        self.track_request(
                            request_key.clone(),
                            cancel_token.to_string(),
                            AsyncRequestKind::Get,
                            RequestPhase::WaitingWorker {
                                cancel_flag: Arc::clone(&cancel_flag),
                            },
                        );
                        self.task_registry.insert(
                            task_id,
                            TaskEntry::Get {
                                request_key: request_key.clone(),
                                snapshot,
                            },
                        );
                        None
                    }
                    Err(mpsc::error::TrySendError::Full(_task)) => Some(busy_response(
                        request.client_id.clone(),
                        request.request_id,
                        "worker task queue is full",
                    )),
                    Err(mpsc::error::TrySendError::Closed(_task)) => Some(io_response(
                        request.client_id.clone(),
                        request.request_id,
                        true,
                        "worker pool is unavailable",
                    )),
                }
            }
        }
    }

    fn handle_scan_start(
        &mut self,
        request: &ExternalRequest,
        body: &ScanStartRequest,
    ) -> ExternalResponse {
        if self.scan_sessions.len() >= self.config.server_limits.max_scan_sessions as usize {
            return busy_response(
                request.client_id.clone(),
                request.request_id,
                "scan session table is full",
            );
        }
        if body.max_records_per_page == 0 {
            return invalid_argument_response(
                request.client_id.clone(),
                request.request_id,
                "max_records_per_page must be greater than zero",
            );
        }
        if body.max_bytes_per_page == 0 {
            return invalid_argument_response(
                request.client_id.clone(),
                request.request_id,
                "max_bytes_per_page must be greater than zero",
            );
        }
        if let Err(error) = validate_scan_range(&body.start_bound, &body.end_bound) {
            return error_response(request, error, false);
        }

        let snapshot_handle = create_snapshot_handle(&mut self.engine);
        let scan_id = self.next_scan_id;
        self.next_scan_id += 1;
        let expires_at = self.scan_expiry_deadline();
        self.scan_sessions.insert(
            scan_id,
            ScanSession {
                scan_id,
                snapshot_seqno: snapshot_handle.snapshot_seqno(),
                data_generation: snapshot_handle.data_generation(),
                snapshot_handle,
                start_bound: body.start_bound.clone(),
                end_bound: body.end_bound.clone(),
                max_records_per_page: body.max_records_per_page,
                max_bytes_per_page: body.max_bytes_per_page,
                resume_after_key: None,
                queued_fetches: VecDeque::new(),
                active_fetch: None,
                in_flight_task_id: None,
                terminal_state: ScanTerminalState::Open,
                expires_at,
            },
        );

        ok_response(
            request.client_id.clone(),
            request.request_id,
            ExternalResponsePayload::ScanStart(ScanStartResponse {
                scan_id,
                observation_seqno: self
                    .scan_sessions
                    .get(&scan_id)
                    .expect("new scan session should exist")
                    .snapshot_seqno,
                data_generation: self
                    .scan_sessions
                    .get(&scan_id)
                    .expect("new scan session should exist")
                    .data_generation,
            }),
        )
    }

    fn handle_scan_fetch_next(
        &mut self,
        request_key: &RequestKey,
        request: &ExternalRequest,
        body: &ScanFetchNextRequest,
        cancel_token: &str,
    ) {
        let now = Instant::now();
        let Some(session) = self.scan_sessions.get(&body.scan_id) else {
            self.finish_request(invalid_argument_response(
                request.client_id.clone(),
                request.request_id,
                "scan_id is unknown or expired",
            ));
            return;
        };
        if session.expires_at <= now {
            self.expire_scan_session(body.scan_id, "scan session expired");
            self.finish_request(invalid_argument_response(
                request.client_id.clone(),
                request.request_id,
                "scan_id is unknown or expired",
            ));
            return;
        }
        if session.active_fetch.is_some()
            && session.queued_fetches.len()
                >= self.config.server_limits.max_scan_fetch_queue_per_session as usize
        {
            self.finish_request(busy_response(
                request.client_id.clone(),
                request.request_id,
                "scan fetch queue is full",
            ));
            return;
        }

        let expires_at = self.scan_expiry_deadline();
        let session = self
            .scan_sessions
            .get_mut(&body.scan_id)
            .expect("scan session should still exist");
        session.queued_fetches.push_back(request_key.clone());
        session.expires_at = expires_at;
        self.track_request(
            request_key.clone(),
            cancel_token.to_string(),
            AsyncRequestKind::ScanFetchNext,
            RequestPhase::QueuedScanFetch {
                scan_id: body.scan_id,
            },
        );
        self.maybe_start_next_scan_fetch(body.scan_id);
    }

    fn handle_stats(&self, request: &ExternalRequest) -> ExternalResponse {
        ok_response(
            request.client_id.clone(),
            request.request_id,
            ExternalResponsePayload::Stats(collect_stats_response(&self.engine)),
        )
    }

    fn handle_cancel(&mut self, client_id: &str, cancel_token: &str) -> bool {
        enum CancelAction {
            QueuedScanFetch(u64),
            WaitingWorker(Arc<AtomicBool>),
            ActiveScanFetch(u64, Arc<AtomicBool>),
            Uncancellable,
        }

        let key = (client_id.to_string(), cancel_token.to_string());
        let Some(request_ids) = self.cancel_index.get(&key).cloned() else {
            return false;
        };

        let mut cancelled_any = false;
        for request_id in request_ids {
            let request_key = (client_id.to_string(), request_id);
            let action = match self.requests.get(&request_key) {
                Some(RequestRecord {
                    phase: RequestPhase::QueuedScanFetch { scan_id },
                    ..
                }) => CancelAction::QueuedScanFetch(*scan_id),
                Some(RequestRecord {
                    phase: RequestPhase::WaitingWorker { cancel_flag, .. },
                    ..
                }) => CancelAction::WaitingWorker(Arc::clone(cancel_flag)),
                Some(RequestRecord {
                    phase:
                        RequestPhase::ActiveScanFetch {
                            scan_id,
                            cancel_flag,
                            ..
                        },
                    ..
                }) => CancelAction::ActiveScanFetch(*scan_id, Arc::clone(cancel_flag)),
                Some(RequestRecord {
                    phase: RequestPhase::WaitingDurability { .. },
                    ..
                }) => CancelAction::Uncancellable,
                None => continue,
            };
            match action {
                CancelAction::QueuedScanFetch(scan_id) => {
                    self.remove_queued_scan_fetch(scan_id, &request_key);
                    self.complete_request(
                        &request_key,
                        cancelled_response(
                            request_key.0.clone(),
                            request_key.1,
                            "queued fetch was cancelled",
                        ),
                    );
                    cancelled_any = true;
                }
                CancelAction::WaitingWorker(cancel_flag) => {
                    cancel_flag.store(true, Ordering::Relaxed);
                    self.complete_request(
                        &request_key,
                        cancelled_response(
                            request_key.0.clone(),
                            request_key.1,
                            "request was cancelled",
                        ),
                    );
                    cancelled_any = true;
                }
                CancelAction::ActiveScanFetch(scan_id, cancel_flag) => {
                    cancel_flag.store(true, Ordering::Relaxed);
                    self.clear_active_scan_fetch(scan_id);
                    self.complete_request(
                        &request_key,
                        cancelled_response(
                            request_key.0.clone(),
                            request_key.1,
                            "active scan fetch was cancelled",
                        ),
                    );
                    self.maybe_start_next_scan_fetch(scan_id);
                    cancelled_any = true;
                }
                CancelAction::Uncancellable => {}
            }
        }

        cancelled_any
    }

    fn handle_worker_result(&mut self, task_id: u64, result: WorkerResult) {
        let Some(entry) = self.task_registry.remove(&task_id) else {
            return;
        };
        match entry {
            TaskEntry::Get {
                request_key,
                snapshot,
            } => {
                if !self.requests.contains_key(&request_key) {
                    return;
                }
                if let WorkerResult::Get(result) = result {
                    match result {
                        Ok(value) => self.complete_request(
                            &request_key,
                            ok_response(
                                request_key.0.clone(),
                                request_key.1,
                                ExternalResponsePayload::Get(GetResponse {
                                    found: value.is_some(),
                                    value,
                                    observation_seqno: snapshot.snapshot_seqno,
                                    data_generation: snapshot.data_generation,
                                }),
                            ),
                        ),
                        Err(error) => self.complete_request(
                            &request_key,
                            error_response_for_key(&request_key, error, true),
                        ),
                    }
                }
            }
            TaskEntry::ScanPage {
                request_key,
                scan_id,
            } => {
                self.in_flight_scan_tasks = self.in_flight_scan_tasks.saturating_sub(1);
                if !self.requests.contains_key(&request_key) {
                    self.clear_active_scan_fetch(scan_id);
                    self.maybe_start_next_scan_fetch(scan_id);
                    return;
                }
                if let WorkerResult::ScanPage(result) = result {
                    match result {
                        Ok(page) => self.finish_scan_page(scan_id, &request_key, page),
                        Err(error) => {
                            self.clear_active_scan_fetch(scan_id);
                            if self.requests.contains_key(&request_key) {
                                self.complete_request(
                                    &request_key,
                                    error_response_for_key(&request_key, error, true),
                                );
                            }
                            self.maybe_start_next_scan_fetch(scan_id);
                        }
                    }
                }
            }
            TaskEntry::Maintenance { plan, attempt } => {
                self.handle_maintenance_result(task_id, plan, attempt, result);
            }
        }
    }

    fn handle_maintenance_result(
        &mut self,
        task_id: u64,
        plan: MaintenancePlan,
        attempt: u8,
        result: WorkerResult,
    ) {
        let io_retry = |owner: &mut Self| {
            if owner.engine.write_stop_error().is_some() {
                owner.finish_maintenance_task(task_id, &plan);
                return;
            }
            if let Some(delay) = maintenance_retry_delay(attempt) {
                owner.task_registry.insert(
                    task_id,
                    TaskEntry::Maintenance {
                        plan: plan.clone(),
                        attempt: attempt + 1,
                    },
                );
                spawn_retry_timer(owner.control_tx.clone(), task_id, delay);
            } else {
                owner.finish_maintenance_task(task_id, &plan);
            }
        };

        match (plan.clone(), result) {
            (MaintenancePlan::Flush(plan), WorkerResult::Flush(result)) => match result {
                Ok(build) => match publish_flush_build(
                    &mut self.engine,
                    &mut self.wal_append_state,
                    &self.layout,
                    &plan,
                    &build,
                    self.config.engine.sync_mode,
                ) {
                    Ok(_outcome) => {
                        self.finish_maintenance_task(task_id, &MaintenancePlan::Flush(plan));
                        self.drive_maintenance();
                    }
                    Err(Error::Stale(_)) => {
                        self.finish_maintenance_task(task_id, &MaintenancePlan::Flush(plan));
                    }
                    Err(Error::Io(_)) => io_retry(self),
                    Err(_) => {
                        self.finish_maintenance_task(task_id, &MaintenancePlan::Flush(plan));
                    }
                },
                Err(Error::Io(_)) => io_retry(self),
                Err(_) => self.finish_maintenance_task(task_id, &MaintenancePlan::Flush(plan)),
            },
            (MaintenancePlan::Compact(plan), WorkerResult::Compact(result)) => match result {
                Ok(build) => match publish_compaction_build(
                    &mut self.engine,
                    &mut self.wal_append_state,
                    &self.layout,
                    &plan,
                    &build,
                    self.config.engine.sync_mode,
                ) {
                    Ok(_outcome) => {
                        self.finish_maintenance_task(task_id, &MaintenancePlan::Compact(plan));
                        self.drive_maintenance();
                    }
                    Err(Error::Stale(_)) => {
                        self.finish_maintenance_task(task_id, &MaintenancePlan::Compact(plan));
                    }
                    Err(Error::Io(_)) => io_retry(self),
                    Err(_) => {
                        self.finish_maintenance_task(task_id, &MaintenancePlan::Compact(plan));
                    }
                },
                Err(Error::Io(_)) => io_retry(self),
                Err(_) => self.finish_maintenance_task(task_id, &MaintenancePlan::Compact(plan)),
            },
            (MaintenancePlan::Logical(plan), WorkerResult::Logical(result)) => match result {
                Ok(Some(payload)) => match publish_logical_install_payload(
                    &mut self.engine,
                    &mut self.wal_append_state,
                    &payload,
                    self.config.engine.sync_mode,
                ) {
                    Ok(_outcome) => {
                        self.finish_maintenance_task(task_id, &MaintenancePlan::Logical(plan));
                    }
                    Err(Error::Stale(_)) => {
                        self.finish_maintenance_task(task_id, &MaintenancePlan::Logical(plan));
                    }
                    Err(Error::Io(_)) => io_retry(self),
                    Err(_) => {
                        self.finish_maintenance_task(task_id, &MaintenancePlan::Logical(plan));
                    }
                },
                Ok(None) => self.finish_maintenance_task(task_id, &MaintenancePlan::Logical(plan)),
                Err(Error::Io(_)) => io_retry(self),
                Err(_) => self.finish_maintenance_task(task_id, &MaintenancePlan::Logical(plan)),
            },
            (MaintenancePlan::Checkpoint(plan), WorkerResult::Checkpoint(result)) => match result {
                Ok(build) => match self.install_checkpoint_build(&build) {
                    Ok(()) => {
                        self.finish_maintenance_task(task_id, &MaintenancePlan::Checkpoint(plan));
                    }
                    Err(Error::Io(_)) => io_retry(self),
                    Err(_) => {
                        self.finish_maintenance_task(task_id, &MaintenancePlan::Checkpoint(plan));
                    }
                },
                Err(Error::Io(_)) => io_retry(self),
                Err(_) => self.finish_maintenance_task(task_id, &MaintenancePlan::Checkpoint(plan)),
            },
            (MaintenancePlan::Gc(file_ids), WorkerResult::Gc(result)) => match result {
                Ok(_deleted) => {
                    self.finish_maintenance_task(task_id, &MaintenancePlan::Gc(file_ids));
                }
                Err(Error::Io(_)) => io_retry(self),
                Err(_) => self.finish_maintenance_task(task_id, &MaintenancePlan::Gc(file_ids)),
            },
            (plan, _) => {
                self.finish_maintenance_task(task_id, &plan);
            }
        }
    }

    fn handle_wal_sync_success(&mut self, result: WalSyncResult) {
        self.engine.mark_synced(result.durable_seqno_target);
        let satisfied = self
            .durability_waiters
            .range(..=result.durable_seqno_target)
            .map(|(target, _)| *target)
            .collect::<Vec<_>>();
        for target_seqno in satisfied {
            let Some(waiters) = self.durability_waiters.remove(&target_seqno) else {
                continue;
            };
            for request_key in waiters {
                self.waiting_durability_waiters = self.waiting_durability_waiters.saturating_sub(1);
                let Some(record) = self.requests.get(&request_key) else {
                    continue;
                };
                let payload = match record.kind {
                    AsyncRequestKind::Put => ExternalResponsePayload::Put(PutResponse),
                    AsyncRequestKind::Delete => ExternalResponsePayload::Delete(DeleteResponse),
                    _ => continue,
                };
                self.complete_request(
                    &request_key,
                    ok_response(request_key.0.clone(), request_key.1, payload),
                );
            }
        }
        self.flush_pending_wal_sync_plans();
        self.drive_maintenance();
    }

    fn handle_wal_sync_failure(&mut self, _target_seqno: u64, message: String) {
        // Once WAL durability fails, later writes stay blocked until restart. The visible writes
        // that were waiting on this batch are failed with retryable `IO`, but reads continue.
        self.engine.mark_write_failed(message.clone());
        self.pending_wal_sync_plans.clear();

        let waiting_targets = self.durability_waiters.keys().copied().collect::<Vec<_>>();
        for target_seqno in waiting_targets {
            let Some(waiters) = self.durability_waiters.remove(&target_seqno) else {
                continue;
            };
            for request_key in waiters {
                self.waiting_durability_waiters = self.waiting_durability_waiters.saturating_sub(1);
                self.complete_request(
                    &request_key,
                    io_response(
                        request_key.0.clone(),
                        request_key.1,
                        true,
                        format!("WAL durability failed: {message}"),
                    ),
                );
            }
        }
    }

    fn expire_idle_sessions(&mut self) {
        let now = Instant::now();
        let expired = self
            .scan_sessions
            .iter()
            .filter_map(|(scan_id, session)| {
                if session.expires_at <= now {
                    Some(*scan_id)
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();
        for scan_id in expired {
            self.expire_scan_session(scan_id, "scan session expired");
        }
    }

    fn retry_maintenance(&mut self, task_id: u64) {
        let Some(TaskEntry::Maintenance { plan, attempt }) = self.task_registry.remove(&task_id)
        else {
            return;
        };
        if !self.dispatch_maintenance_task(task_id, &plan, attempt) {
            self.task_registry.insert(
                task_id,
                TaskEntry::Maintenance {
                    plan: plan.clone(),
                    attempt,
                },
            );
            spawn_retry_timer(self.control_tx.clone(), task_id, Duration::from_millis(50));
        }
    }

    fn begin_shutdown(&mut self, external_rx: &mut mpsc::Receiver<ExternalEnvelope>) {
        if self.lifecycle == SharedLifecycle::Stopping || self.lifecycle == SharedLifecycle::Stopped
        {
            return;
        }
        self.lifecycle = SharedLifecycle::Stopping;
        self.shared.set_lifecycle(SharedLifecycle::Stopping);

        let open_scans = self.scan_sessions.keys().copied().collect::<Vec<_>>();
        for scan_id in open_scans {
            self.cancel_scan_session(scan_id, "server shutdown requested");
        }

        let pending_requests = self.requests.keys().cloned().collect::<Vec<_>>();
        for request_key in pending_requests {
            if !self.requests.contains_key(&request_key) {
                continue;
            }
            self.complete_request(
                &request_key,
                io_response(
                    request_key.0.clone(),
                    request_key.1,
                    true,
                    "server is stopping",
                ),
            );
        }

        while let Ok(envelope) = external_rx.try_recv() {
            let _ = envelope.reply.send(io_response(
                envelope.request.client_id,
                envelope.request.request_id,
                true,
                "server is stopping",
            ));
        }
    }

    async fn shutdown_internal_tasks(&mut self) {
        self.worker_tx = None;
        self.wal_sync_tx = None;

        if let Some(handle) = self.expiry_handle.take() {
            handle.abort();
            let _ = handle.await;
        }
        if let Some(handle) = self.wal_sync_handle.take() {
            let _ = handle.await;
        }
        for handle in self.worker_handles.drain(..) {
            let _ = handle.await;
        }
    }

    fn register_request(
        &mut self,
        request: &ExternalRequest,
        reply: oneshot::Sender<ExternalResponse>,
    ) -> Option<RejectedRequest> {
        let client_state = self
            .clients
            .entry(request.client_id.clone())
            .or_insert_with(|| ClientState::new(request.request_id));

        if let Some(last_admitted_request_id) = client_state.last_admitted_request_id {
            let Some(expected_request_id) = last_admitted_request_id.checked_add(1) else {
                return Some(RejectedRequest {
                    reply,
                    response: invalid_argument_response(
                        request.client_id.clone(),
                        request.request_id,
                        "request_id overflowed the per-client sequence",
                    ),
                });
            };
            if request.request_id != expected_request_id {
                return Some(RejectedRequest {
                    reply,
                    response: invalid_argument_response(
                        request.client_id.clone(),
                        request.request_id,
                        format!(
                            "expected request_id {expected_request_id} for client {}",
                            request.client_id
                        ),
                    ),
                });
            }
            client_state.last_admitted_request_id = Some(request.request_id);
        } else {
            client_state.last_admitted_request_id = Some(request.request_id);
            client_state.next_response_request_id = Some(request.request_id);
        }

        client_state.waiters.insert(request.request_id, reply);
        None
    }

    fn finish_request(&mut self, response: ExternalResponse) {
        let Some(client_state) = self.clients.get_mut(&response.client_id) else {
            return;
        };
        client_state
            .ready_responses
            .insert(response.request_id, response);
        flush_client_responses(client_state);
    }

    fn complete_request(&mut self, request_key: &RequestKey, response: ExternalResponse) {
        self.clear_request_state(request_key);
        self.finish_request(response);
    }

    fn track_request(
        &mut self,
        request_key: RequestKey,
        cancel_token: String,
        kind: AsyncRequestKind,
        phase: RequestPhase,
    ) {
        self.cancel_index
            .entry((request_key.0.clone(), cancel_token.clone()))
            .or_default()
            .insert(request_key.1);
        self.requests.insert(
            request_key,
            RequestRecord {
                cancel_token,
                kind,
                phase,
            },
        );
    }

    fn clear_request_state(&mut self, request_key: &RequestKey) {
        let Some(record) = self.requests.remove(request_key) else {
            return;
        };
        if let Some(request_ids) = self
            .cancel_index
            .get_mut(&(request_key.0.clone(), record.cancel_token.clone()))
        {
            request_ids.remove(&request_key.1);
            if request_ids.is_empty() {
                self.cancel_index
                    .remove(&(request_key.0.clone(), record.cancel_token));
            }
        }

        if let RequestPhase::WaitingDurability { target_seqno } = record.phase
            && let Some(waiters) = self.durability_waiters.get_mut(&target_seqno)
        {
            if let Some(position) = waiters.iter().position(|waiter| waiter == request_key) {
                waiters.remove(position);
                self.waiting_durability_waiters = self.waiting_durability_waiters.saturating_sub(1);
            }
            if waiters.is_empty() {
                self.durability_waiters.remove(&target_seqno);
            }
        }
    }

    fn maybe_start_next_scan_fetch(&mut self, scan_id: u64) {
        // Same-scan fetches stay serialized here so queued requests across clients all advance the
        // shared `resume_after_key` in FIFO order, regardless of which worker eventually runs them.
        let Some(session) = self.scan_sessions.get(&scan_id) else {
            return;
        };
        if session.active_fetch.is_some() || session.queued_fetches.is_empty() {
            return;
        }

        let request_key = session
            .queued_fetches
            .front()
            .expect("queued fetch should exist")
            .clone();
        let page_plan = match capture_scan_page_request(
            &self.engine,
            scan_id,
            &session.snapshot_handle,
            &session.start_bound,
            &session.end_bound,
            session.resume_after_key.clone(),
            session.max_records_per_page,
            session.max_bytes_per_page,
        ) {
            Ok(plan) => plan,
            Err(error) => {
                self.remove_queued_scan_fetch(scan_id, &request_key);
                self.complete_request(
                    &request_key,
                    error_response_for_key(&request_key, error, true),
                );
                return;
            }
        };

        if page_plan.is_active_only() {
            let expires_at = self.scan_expiry_deadline();
            let session = self
                .scan_sessions
                .get_mut(&scan_id)
                .expect("scan session should exist");
            let request_key = session
                .queued_fetches
                .pop_front()
                .expect("queued fetch should exist");
            session.active_fetch = Some(request_key.clone());
            session.in_flight_task_id = None;
            session.expires_at = expires_at;
            let result = execute_scan_page_plan(&self.layout, &page_plan);
            match result {
                Ok(page) => self.finish_scan_page(scan_id, &request_key, page),
                Err(error) => {
                    self.clear_active_scan_fetch(scan_id);
                    if self.requests.contains_key(&request_key) {
                        self.complete_request(
                            &request_key,
                            error_response_for_key(&request_key, error, true),
                        );
                    }
                    self.maybe_start_next_scan_fetch(scan_id);
                }
            }
            return;
        }

        if self.in_flight_scan_tasks >= self.config.server_limits.max_in_flight_scan_tasks as usize
        {
            return;
        }
        let Some(worker_tx) = self.worker_tx.as_ref().cloned() else {
            return;
        };

        let cancel_flag = Arc::new(AtomicBool::new(false));
        let task_id = self.next_task_id();
        match worker_tx.try_send(WorkerTask {
            task_id,
            cancel_flag: Arc::clone(&cancel_flag),
            payload: WorkerTaskPayload::ScanPage(page_plan),
        }) {
            Ok(()) => {
                let expires_at = self.scan_expiry_deadline();
                let session = self
                    .scan_sessions
                    .get_mut(&scan_id)
                    .expect("scan session should exist");
                let request_key = session
                    .queued_fetches
                    .pop_front()
                    .expect("queued fetch should exist");
                session.active_fetch = Some(request_key.clone());
                session.in_flight_task_id = Some(task_id);
                session.expires_at = expires_at;
                if let Some(record) = self.requests.get_mut(&request_key) {
                    record.phase = RequestPhase::ActiveScanFetch {
                        scan_id,
                        cancel_flag: Arc::clone(&cancel_flag),
                    };
                }
                self.task_registry.insert(
                    task_id,
                    TaskEntry::ScanPage {
                        request_key,
                        scan_id,
                    },
                );
                self.in_flight_scan_tasks += 1;
            }
            Err(mpsc::error::TrySendError::Full(_task))
            | Err(mpsc::error::TrySendError::Closed(_task)) => {}
        }
    }

    fn finish_scan_page(&mut self, scan_id: u64, request_key: &RequestKey, page: ScanPageResult) {
        let expires_at = self.scan_expiry_deadline();
        let Some(session) = self.scan_sessions.get_mut(&scan_id) else {
            return;
        };
        session.active_fetch = None;
        session.in_flight_task_id = None;
        session.expires_at = expires_at;

        if page.eof {
            self.complete_request(
                request_key,
                ok_response(
                    request_key.0.clone(),
                    request_key.1,
                    ExternalResponsePayload::ScanFetchNext(ScanFetchNextResponse {
                        rows: page.rows,
                        eof: true,
                    }),
                ),
            );
            self.finish_scan_session_with_eof(scan_id);
            return;
        }

        if !page.rows.is_empty()
            && let Some(session) = self.scan_sessions.get_mut(&scan_id)
        {
            session.resume_after_key = page.next_resume_after_key;
        }
        self.complete_request(
            request_key,
            ok_response(
                request_key.0.clone(),
                request_key.1,
                ExternalResponsePayload::ScanFetchNext(ScanFetchNextResponse {
                    rows: page.rows,
                    eof: false,
                }),
            ),
        );
        self.maybe_start_next_scan_fetch(scan_id);
    }

    fn finish_scan_session_with_eof(&mut self, scan_id: u64) {
        if let Some(session) = self.scan_sessions.get_mut(&scan_id) {
            session.terminal_state = ScanTerminalState::EofReached;
        }
        let Some(session) = self.scan_sessions.remove(&scan_id) else {
            return;
        };
        debug_assert_eq!(session.scan_id, scan_id);
        let _ = release_snapshot_handle(&mut self.engine, &session.snapshot_handle);
        for request_key in session.queued_fetches {
            self.complete_request(
                &request_key,
                invalid_argument_response(
                    request_key.0.clone(),
                    request_key.1,
                    "scan_id is unknown or expired",
                ),
            );
        }
    }

    fn cancel_scan_session(&mut self, scan_id: u64, message: &str) {
        if let Some(session) = self.scan_sessions.get_mut(&scan_id) {
            session.terminal_state = ScanTerminalState::Cancelled;
        }
        let Some(session) = self.scan_sessions.remove(&scan_id) else {
            return;
        };
        debug_assert_eq!(session.scan_id, scan_id);
        let _ = release_snapshot_handle(&mut self.engine, &session.snapshot_handle);

        if let Some(active_request) = session.active_fetch {
            if let Some(record) = self.requests.get(&active_request)
                && let RequestPhase::ActiveScanFetch { cancel_flag, .. } = &record.phase
            {
                cancel_flag.store(true, Ordering::Relaxed);
            }
            self.complete_request(
                &active_request,
                cancelled_response(active_request.0.clone(), active_request.1, message),
            );
        }
        for request_key in session.queued_fetches {
            self.complete_request(
                &request_key,
                cancelled_response(request_key.0.clone(), request_key.1, message),
            );
        }
    }

    fn expire_scan_session(&mut self, scan_id: u64, message: &str) {
        if let Some(session) = self.scan_sessions.get_mut(&scan_id) {
            session.terminal_state = ScanTerminalState::Expired;
        }
        self.cancel_scan_session(scan_id, message);
    }

    fn clear_active_scan_fetch(&mut self, scan_id: u64) {
        if let Some(session) = self.scan_sessions.get_mut(&scan_id) {
            session.active_fetch = None;
            session.in_flight_task_id = None;
        }
    }

    fn remove_queued_scan_fetch(&mut self, scan_id: u64, request_key: &RequestKey) {
        let Some(session) = self.scan_sessions.get_mut(&scan_id) else {
            return;
        };
        if let Some(position) = session
            .queued_fetches
            .iter()
            .position(|queued| queued == request_key)
        {
            session.queued_fetches.remove(position);
        }
    }

    fn enqueue_wal_sync_plan(&mut self, plan: WalSyncPlan) {
        self.pending_wal_sync_plans
            .insert(plan.wal_segment_id, PendingWalSyncPlan { plan });
        self.flush_pending_wal_sync_plans();
    }

    fn flush_pending_wal_sync_plans(&mut self) {
        let Some(wal_sync_tx) = self.wal_sync_tx.as_ref() else {
            self.handle_wal_sync_failure(0, "WAL sync actor is unavailable".into());
            return;
        };

        let segment_ids = self
            .pending_wal_sync_plans
            .keys()
            .copied()
            .collect::<Vec<_>>();
        for segment_id in segment_ids {
            let Some(pending) = self.pending_wal_sync_plans.remove(&segment_id) else {
                continue;
            };
            match wal_sync_tx.try_send(pending.plan) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(plan)) => {
                    self.pending_wal_sync_plans
                        .insert(segment_id, PendingWalSyncPlan { plan });
                    break;
                }
                Err(mpsc::error::TrySendError::Closed(plan)) => {
                    self.pending_wal_sync_plans
                        .insert(segment_id, PendingWalSyncPlan { plan });
                    self.handle_wal_sync_failure(0, "WAL sync actor is unavailable".into());
                    break;
                }
            }
        }
    }

    fn drive_maintenance(&mut self) {
        // The owner reuses the direct engine's maintenance ordering: flush first, then checkpoint,
        // then compaction, logical maintenance, and finally best-effort GC.
        if self.lifecycle != SharedLifecycle::Ready {
            return;
        }

        if self.active_flushes < self.config.maintenance.max_concurrent_flushes as usize
            && let Some(plan) = self.engine.plan_flush()
            && self.dispatch_new_maintenance(MaintenancePlan::Flush(plan))
        {
            return;
        }

        let checkpoint_frontier = self
            .engine
            .durable_current()
            .map(|current| current.checkpoint_max_seqno)
            .unwrap_or(0);
        let retained_wal_bytes =
            retained_wal_bytes_after(&self.layout, checkpoint_frontier).unwrap_or_default();
        if self.engine.should_checkpoint(retained_wal_bytes) && !self.checkpoint_in_flight {
            match self.engine.freeze_active_memtable_if_non_empty() {
                Ok(true) => {
                    self.drive_maintenance();
                    return;
                }
                Ok(false) => {}
                Err(_) => return,
            }
            if self.engine.current_manifest.frozen_memtables.is_empty() {
                let capture = match self.capture_checkpoint_plan() {
                    Ok(plan) => plan,
                    Err(_) => return,
                };
                if self.dispatch_new_maintenance(MaintenancePlan::Checkpoint(capture)) {
                    return;
                }
            }
        }

        if self.active_compactions < self.config.maintenance.max_concurrent_compactions as usize
            && let Some(plan) = self.engine.plan_compaction()
            && self.dispatch_new_maintenance(MaintenancePlan::Compact(plan))
        {
            return;
        }

        if !self.logical_in_flight {
            let plan = match self.engine.plan_logical_maintenance() {
                Ok(plan) => plan,
                Err(_) => return,
            };
            if self.dispatch_new_maintenance(MaintenancePlan::Logical(plan)) {
                return;
            }
        }

        if !self.gc_in_flight {
            let mut candidates = match list_canonical_data_file_ids(&self.layout) {
                Ok(candidates) => candidates,
                Err(_) => return,
            };
            let protected = match self.engine.gc_protected_file_ids() {
                Ok(protected) => protected,
                Err(_) => return,
            };
            candidates.retain(|file_id| !protected.contains(file_id));
            if !candidates.is_empty() {
                let _ = self.dispatch_new_maintenance(MaintenancePlan::Gc(candidates));
            }
        }
    }

    fn dispatch_new_maintenance(&mut self, plan: MaintenancePlan) -> bool {
        let task_id = self.next_task_id();
        self.task_registry.insert(
            task_id,
            TaskEntry::Maintenance {
                plan: plan.clone(),
                attempt: 0,
            },
        );
        self.mark_maintenance_started(&plan);
        if self.dispatch_maintenance_task(task_id, &plan, 0) {
            true
        } else {
            self.task_registry.remove(&task_id);
            self.clear_maintenance_flags(&plan);
            false
        }
    }

    fn dispatch_maintenance_task(
        &mut self,
        task_id: u64,
        plan: &MaintenancePlan,
        _attempt: u8,
    ) -> bool {
        let Some(worker_tx) = self.worker_tx.as_ref() else {
            return false;
        };
        let payload = match plan {
            MaintenancePlan::Flush(plan) => WorkerTaskPayload::Flush {
                plan: plan.clone(),
                page_size_bytes: self.config.engine.page_size_bytes,
            },
            MaintenancePlan::Compact(plan) => WorkerTaskPayload::Compact {
                plan: plan.clone(),
                page_size_bytes: self.config.engine.page_size_bytes,
            },
            MaintenancePlan::Logical(plan) => WorkerTaskPayload::Logical(plan.clone()),
            MaintenancePlan::Checkpoint(plan) => WorkerTaskPayload::Checkpoint(plan.clone()),
            MaintenancePlan::Gc(file_ids) => WorkerTaskPayload::Gc(file_ids.clone()),
        };
        let cancel_flag = Arc::new(AtomicBool::new(false));
        match worker_tx.try_send(WorkerTask {
            task_id,
            cancel_flag,
            payload,
        }) {
            Ok(()) => true,
            Err(mpsc::error::TrySendError::Full(_task))
            | Err(mpsc::error::TrySendError::Closed(_task)) => false,
        }
    }

    fn capture_checkpoint_plan(&mut self) -> Result<CheckpointTaskPlan, Error> {
        self.engine.enter_checkpoint_capture_pause()?;
        let capture = self.engine.capture_metadata_checkpoint();
        self.engine.leave_checkpoint_capture_pause();
        Ok(CheckpointTaskPlan {
            capture: capture?,
            page_size_bytes: self.config.engine.page_size_bytes,
            temp_tag: format!(
                "checkpoint-{}-{}-{}",
                self.engine.next_file_id, self.engine.last_committed_seqno, self.next_task_id
            ),
        })
    }

    fn install_checkpoint_build(&mut self, build: &CheckpointBuildResult) -> Result<(), Error> {
        install_metadata_checkpoint(
            &self.layout,
            &build.temp_path,
            build.capture.checkpoint_generation,
        )?;
        install_current(
            &self.layout,
            CurrentFile {
                checkpoint_generation: build.capture.checkpoint_generation,
                checkpoint_max_seqno: build.capture.checkpoint_max_seqno,
                checkpoint_data_generation: build.capture.checkpoint_data_generation,
            },
            &build.temp_tag,
        )?;
        self.engine.install_durable_current(
            build.capture.checkpoint_generation,
            build.capture.checkpoint_max_seqno,
            build.capture.checkpoint_data_generation,
            &build.capture.levels,
        );
        truncate_covered_closed_wal_segments(&self.layout, build.capture.checkpoint_max_seqno)?;
        Ok(())
    }

    fn mark_maintenance_started(&mut self, plan: &MaintenancePlan) {
        match plan {
            MaintenancePlan::Flush(_) => self.active_flushes += 1,
            MaintenancePlan::Compact(_) => self.active_compactions += 1,
            MaintenancePlan::Logical(_) => self.logical_in_flight = true,
            MaintenancePlan::Checkpoint(_) => self.checkpoint_in_flight = true,
            MaintenancePlan::Gc(_) => self.gc_in_flight = true,
        }
    }

    fn finish_maintenance_task(&mut self, task_id: u64, plan: &MaintenancePlan) {
        self.task_registry.remove(&task_id);
        self.clear_maintenance_flags(plan);
        self.drive_maintenance();
    }

    fn clear_maintenance_flags(&mut self, plan: &MaintenancePlan) {
        match plan {
            MaintenancePlan::Flush(_) => {
                self.active_flushes = self.active_flushes.saturating_sub(1);
            }
            MaintenancePlan::Compact(_) => {
                self.active_compactions = self.active_compactions.saturating_sub(1);
            }
            MaintenancePlan::Logical(_) => self.logical_in_flight = false,
            MaintenancePlan::Checkpoint(_) => self.checkpoint_in_flight = false,
            MaintenancePlan::Gc(_) => self.gc_in_flight = false,
        }
    }

    fn hidden_cancel_token(&mut self) -> String {
        let token = format!("__sevai_hidden_{}", self.next_hidden_cancel_id);
        self.next_hidden_cancel_id += 1;
        token
    }

    fn next_task_id(&mut self) -> u64 {
        let task_id = self.next_task_id;
        self.next_task_id += 1;
        task_id
    }

    fn scan_expiry_deadline(&self) -> Instant {
        Instant::now() + Duration::from_millis(self.config.server_limits.scan_idle_timeout_ms)
    }
}

/// One rejected external request that never entered the owner actor's async state tables.
struct RejectedRequest {
    reply: oneshot::Sender<ExternalResponse>,
    response: ExternalResponse,
}

fn flush_client_responses(client_state: &mut ClientState) {
    while let Some(next_response_request_id) = client_state.next_response_request_id {
        let Some(response) = client_state
            .ready_responses
            .remove(&next_response_request_id)
        else {
            break;
        };
        let Some(reply) = client_state.waiters.remove(&next_response_request_id) else {
            break;
        };

        let _ = reply.send(response);
        client_state.next_response_request_id = next_response_request_id.checked_add(1);
    }
}

fn spawn_worker_pool(
    layout: StoreLayout,
    control_tx: mpsc::UnboundedSender<ControlCommand>,
    worker_parallelism: u32,
    max_worker_tasks: u32,
    options: OwnerRuntimeOptions,
) -> (mpsc::Sender<WorkerTask>, Vec<JoinHandle<()>>) {
    let (worker_tx, worker_rx) = mpsc::channel(max_worker_tasks as usize);
    let shared_rx = Arc::new(AsyncMutex::new(worker_rx));
    let mut handles = Vec::new();
    for _worker_id in 0..worker_parallelism {
        let rx = Arc::clone(&shared_rx);
        let layout = layout.clone();
        let control_tx = control_tx.clone();
        handles.push(tokio::spawn(async move {
            worker_loop(layout, rx, control_tx, options).await;
        }));
    }
    (worker_tx, handles)
}

async fn worker_loop(
    layout: StoreLayout,
    receiver: Arc<AsyncMutex<mpsc::Receiver<WorkerTask>>>,
    control_tx: mpsc::UnboundedSender<ControlCommand>,
    options: OwnerRuntimeOptions,
) {
    loop {
        let maybe_task = {
            let mut receiver = receiver.lock().await;
            receiver.recv().await
        };
        let Some(task) = maybe_task else {
            break;
        };

        let result = match task.payload {
            WorkerTaskPayload::Get(plan) => {
                if !options.get_task_delay.is_zero() {
                    tokio::time::sleep(options.get_task_delay).await;
                }
                if task.cancel_flag.load(Ordering::Relaxed) {
                    WorkerResult::Get(Err(Error::Cancelled(
                        "worker get task was cancelled before execution".into(),
                    )))
                } else {
                    let layout = layout.clone();
                    let result = task::spawn_blocking(move || execute_point_read(&layout, &plan))
                        .await
                        .map_err(spawn_blocking_join_error)
                        .and_then(|result| result);
                    WorkerResult::Get(result)
                }
            }
            WorkerTaskPayload::ScanPage(plan) => {
                if !options.scan_task_delay.is_zero() {
                    tokio::time::sleep(options.scan_task_delay).await;
                }
                if task.cancel_flag.load(Ordering::Relaxed) {
                    WorkerResult::ScanPage(Err(Error::Cancelled(
                        "worker scan task was cancelled before execution".into(),
                    )))
                } else {
                    let layout = layout.clone();
                    let result =
                        task::spawn_blocking(move || execute_scan_page_plan(&layout, &plan))
                            .await
                            .map_err(spawn_blocking_join_error)
                            .and_then(|result| result);
                    WorkerResult::ScanPage(result)
                }
            }
            WorkerTaskPayload::Flush {
                plan,
                page_size_bytes,
            } => {
                let layout = layout.clone();
                let result = task::spawn_blocking(move || {
                    build_flush_output(&layout, page_size_bytes, &plan)
                })
                .await
                .map_err(spawn_blocking_join_error)
                .and_then(|result| result);
                WorkerResult::Flush(result)
            }
            WorkerTaskPayload::Compact {
                plan,
                page_size_bytes,
            } => {
                let layout = layout.clone();
                let result = task::spawn_blocking(move || {
                    build_compaction_output(&layout, page_size_bytes, &plan)
                })
                .await
                .map_err(spawn_blocking_join_error)
                .and_then(|result| result);
                WorkerResult::Compact(result)
            }
            WorkerTaskPayload::Logical(plan) => {
                let layout = layout.clone();
                let result =
                    task::spawn_blocking(move || execute_logical_maintenance_plan(&layout, &plan))
                        .await
                        .map_err(spawn_blocking_join_error)
                        .and_then(|result| result);
                WorkerResult::Logical(result)
            }
            WorkerTaskPayload::Checkpoint(plan) => {
                let layout = layout.clone();
                let result = task::spawn_blocking(move || {
                    let replay_seed = crate::pathivu::ReplaySeed {
                        checkpoint_generation: plan.capture.checkpoint_generation,
                        checkpoint_max_seqno: plan.capture.checkpoint_max_seqno,
                        checkpoint_data_generation: plan.capture.checkpoint_data_generation,
                        next_seqno: plan.capture.next_seqno,
                        next_file_id: plan.capture.next_file_id,
                        levels: plan.capture.levels.clone(),
                        logical_shards: plan.capture.logical_shards.clone(),
                    };
                    let temp_path = build_temp_metadata_checkpoint_file(
                        &layout,
                        plan.page_size_bytes,
                        &replay_seed,
                        &plan.temp_tag,
                    )?;
                    Ok(CheckpointBuildResult {
                        capture: plan.capture,
                        temp_tag: plan.temp_tag,
                        temp_path,
                    })
                })
                .await
                .map_err(spawn_blocking_join_error)
                .and_then(|result| result);
                WorkerResult::Checkpoint(result)
            }
            WorkerTaskPayload::Gc(file_ids) => {
                let layout = layout.clone();
                let result = task::spawn_blocking(move || Ok(execute_gc(&layout, &file_ids)))
                    .await
                    .map_err(spawn_blocking_join_error)
                    .and_then(|result: Result<Vec<u64>, Error>| result);
                WorkerResult::Gc(result)
            }
        };

        let _ = control_tx.send(ControlCommand::WorkerResult {
            task_id: task.task_id,
            result,
        });
    }
}

async fn wal_sync_actor(
    mut receiver: mpsc::Receiver<WalSyncPlan>,
    control_tx: mpsc::UnboundedSender<ControlCommand>,
    group_commit_bytes: u64,
    group_commit_max_delay: Duration,
    extra_delay: Duration,
) {
    let mut pending = BTreeMap::<u64, WalSyncPendingEntry>::new();
    let mut last_synced_offsets = BTreeMap::<u64, u64>::new();

    loop {
        let deadline = pending
            .values()
            .map(|entry| entry.oldest_requested_at + group_commit_max_delay)
            .min();

        tokio::select! {
            maybe_plan = receiver.recv() => {
                let Some(plan) = maybe_plan else {
                    break;
                };
                let now = Instant::now();
                match pending.entry(plan.wal_segment_id) {
                    std::collections::btree_map::Entry::Occupied(mut entry) => {
                        let oldest = entry.get().oldest_requested_at.min(now);
                        if plan.durable_seqno_target >= entry.get().plan.durable_seqno_target {
                            entry.insert(WalSyncPendingEntry {
                                plan,
                                oldest_requested_at: oldest,
                            });
                        }
                    }
                    std::collections::btree_map::Entry::Vacant(entry) => {
                        entry.insert(WalSyncPendingEntry {
                            plan,
                            oldest_requested_at: now,
                        });
                    }
                }
            }
            _ = async {
                if let Some(deadline) = deadline {
                    tokio::time::sleep_until(deadline).await;
                }
            }, if deadline.is_some() => {}
        }

        loop {
            let due_segment = pending.iter().find_map(|(segment_id, entry)| {
                let last_synced = last_synced_offsets.get(segment_id).copied().unwrap_or(0);
                let bytes_since_sync = entry.plan.durable_offset_target.saturating_sub(last_synced);
                if bytes_since_sync >= group_commit_bytes
                    || entry.oldest_requested_at + group_commit_max_delay <= Instant::now()
                {
                    Some(*segment_id)
                } else {
                    None
                }
            });
            let Some(segment_id) = due_segment else {
                break;
            };
            let Some(entry) = pending.remove(&segment_id) else {
                continue;
            };
            let target_seqno = entry.plan.durable_seqno_target;
            if !extra_delay.is_zero() {
                tokio::time::sleep(extra_delay).await;
            }
            match task::spawn_blocking(move || execute_wal_sync_plan(entry.plan))
                .await
                .map_err(spawn_blocking_join_error)
                .and_then(|result| result)
            {
                Ok(result) => {
                    last_synced_offsets
                        .insert(result.wal_segment_id, result.durable_offset_reached);
                    let _ = control_tx.send(ControlCommand::WalSyncSucceeded(result));
                }
                Err(error) => {
                    let _ = control_tx.send(ControlCommand::WalSyncFailed {
                        target_seqno,
                        message: error.to_string(),
                    });
                }
            }
        }
    }
}

async fn scan_expiry_tick(control_tx: mpsc::UnboundedSender<ControlCommand>) {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    loop {
        interval.tick().await;
        if control_tx
            .send(ControlCommand::SweepExpiredSessions)
            .is_err()
        {
            break;
        }
    }
}

fn spawn_retry_timer(
    control_tx: mpsc::UnboundedSender<ControlCommand>,
    task_id: u64,
    delay: Duration,
) {
    tokio::spawn(async move {
        tokio::time::sleep(delay).await;
        let _ = control_tx.send(ControlCommand::RetryMaintenance { task_id });
    });
}

fn maintenance_retry_delay(attempt: u8) -> Option<Duration> {
    match attempt {
        0 => Some(Duration::from_millis(50)),
        1 => Some(Duration::from_millis(200)),
        2 => Some(Duration::from_secs(1)),
        _ => None,
    }
}

fn spawn_blocking_join_error(error: task::JoinError) -> Error {
    Error::Io(std::io::Error::other(format!(
        "blocking task failed: {error}"
    )))
}

fn error_response(request: &ExternalRequest, error: Error, io_retryable: bool) -> ExternalResponse {
    error_response_for_key(
        &(request.client_id.clone(), request.request_id),
        error,
        io_retryable,
    )
}

fn error_response_for_key(
    request_key: &RequestKey,
    error: Error,
    io_retryable: bool,
) -> ExternalResponse {
    match error {
        Error::InvalidArgument(message) => {
            invalid_argument_response(request_key.0.clone(), request_key.1, message)
        }
        Error::Busy(message) => busy_response(request_key.0.clone(), request_key.1, message),
        Error::Io(error) => io_response(
            request_key.0.clone(),
            request_key.1,
            io_retryable,
            error.to_string(),
        ),
        Error::Checksum(message) => {
            checksum_response(request_key.0.clone(), request_key.1, message)
        }
        Error::Corruption(message) => {
            corruption_response(request_key.0.clone(), request_key.1, message)
        }
        Error::Stale(message) => stale_response(request_key.0.clone(), request_key.1, message),
        Error::Cancelled(message) => {
            cancelled_response(request_key.0.clone(), request_key.1, message)
        }
        Error::Protocol(message) | Error::ServerUnavailable(message) => {
            io_response(request_key.0.clone(), request_key.1, false, message)
        }
    }
}

fn lifecycle_response(
    client_id: String,
    request_id: u64,
    lifecycle: SharedLifecycle,
) -> ExternalResponse {
    match lifecycle {
        SharedLifecycle::Booting => io_response(client_id, request_id, true, "server is booting"),
        SharedLifecycle::Ready => io_response(client_id, request_id, true, "server is ready"),
        SharedLifecycle::Stopping => io_response(client_id, request_id, true, "server is stopping"),
        SharedLifecycle::StartFailed => {
            io_response(client_id, request_id, false, "server startup failed")
        }
        SharedLifecycle::Stopped => io_response(client_id, request_id, false, "server is stopped"),
    }
}

fn ok_response(
    client_id: String,
    request_id: u64,
    payload: ExternalResponsePayload,
) -> ExternalResponse {
    ExternalResponse {
        client_id,
        request_id,
        status: Status {
            code: StatusCode::Ok,
            retryable: false,
            message: None,
        },
        payload: Some(payload),
    }
}

fn busy_response(
    client_id: String,
    request_id: u64,
    message: impl Into<String>,
) -> ExternalResponse {
    ExternalResponse {
        client_id,
        request_id,
        status: Status {
            code: StatusCode::Busy,
            retryable: true,
            message: Some(message.into()),
        },
        payload: None,
    }
}

fn invalid_argument_response(
    client_id: String,
    request_id: u64,
    message: impl Into<String>,
) -> ExternalResponse {
    ExternalResponse {
        client_id,
        request_id,
        status: Status {
            code: StatusCode::InvalidArgument,
            retryable: false,
            message: Some(message.into()),
        },
        payload: None,
    }
}

fn checksum_response(
    client_id: String,
    request_id: u64,
    message: impl Into<String>,
) -> ExternalResponse {
    ExternalResponse {
        client_id,
        request_id,
        status: Status {
            code: StatusCode::Checksum,
            retryable: false,
            message: Some(message.into()),
        },
        payload: None,
    }
}

fn corruption_response(
    client_id: String,
    request_id: u64,
    message: impl Into<String>,
) -> ExternalResponse {
    ExternalResponse {
        client_id,
        request_id,
        status: Status {
            code: StatusCode::Corruption,
            retryable: false,
            message: Some(message.into()),
        },
        payload: None,
    }
}

fn stale_response(
    client_id: String,
    request_id: u64,
    message: impl Into<String>,
) -> ExternalResponse {
    ExternalResponse {
        client_id,
        request_id,
        status: Status {
            code: StatusCode::Stale,
            retryable: false,
            message: Some(message.into()),
        },
        payload: None,
    }
}

fn cancelled_response(
    client_id: String,
    request_id: u64,
    message: impl Into<String>,
) -> ExternalResponse {
    ExternalResponse {
        client_id,
        request_id,
        status: Status {
            code: StatusCode::Cancelled,
            retryable: true,
            message: Some(message.into()),
        },
        payload: None,
    }
}

fn io_response(
    client_id: String,
    request_id: u64,
    retryable: bool,
    message: impl Into<String>,
) -> ExternalResponse {
    ExternalResponse {
        client_id,
        request_id,
        status: Status {
            code: StatusCode::Io,
            retryable,
            message: Some(message.into()),
        },
        payload: None,
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};

    use super::*;
    use crate::pathivu::WalWriterTestOptions;

    static NEXT_CONFIG_ID: AtomicU64 = AtomicU64::new(0);

    /// Small harness that drives the owner actor directly so tests can inspect private state.
    struct TestOwner {
        owner: OwnerState,
        control_rx: mpsc::UnboundedReceiver<ControlCommand>,
    }

    impl TestOwner {
        fn submit(&mut self, request: ExternalRequest) -> oneshot::Receiver<ExternalResponse> {
            let (reply_tx, reply_rx) = oneshot::channel();
            self.owner.handle_external(ExternalEnvelope {
                request,
                reply: reply_tx,
            });
            reply_rx
        }

        async fn recv(
            &mut self,
            response_rx: oneshot::Receiver<ExternalResponse>,
        ) -> ExternalResponse {
            self.recv_with_sync_count(response_rx).await.0
        }

        async fn recv_with_sync_count(
            &mut self,
            response_rx: oneshot::Receiver<ExternalResponse>,
        ) -> (ExternalResponse, usize) {
            let mut response_rx = response_rx;
            let mut wal_sync_successes = 0usize;
            loop {
                match tokio::time::timeout(Duration::from_millis(20), &mut response_rx).await {
                    Ok(Ok(response)) => {
                        wal_sync_successes += self.drain_control().await;
                        return (response, wal_sync_successes);
                    }
                    Ok(Err(error)) => panic!("response channel dropped unexpectedly: {error}"),
                    Err(_) => {
                        wal_sync_successes += self.drain_control().await;
                    }
                }
            }
        }

        async fn drain_control(&mut self) -> usize {
            let mut wal_sync_successes = 0usize;
            loop {
                let mut progressed = false;
                while let Ok(command) = self.control_rx.try_recv() {
                    if matches!(command, ControlCommand::WalSyncSucceeded(_)) {
                        wal_sync_successes += 1;
                    }
                    self.owner.handle_control(command);
                    progressed = true;
                }
                if !progressed {
                    break;
                }
                tokio::task::yield_now().await;
            }
            wal_sync_successes
        }

        async fn wait_for(&mut self, predicate: impl Fn(&OwnerState) -> bool, context: &str) {
            for _ in 0..200 {
                self.drain_control().await;
                if predicate(&self.owner) {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            panic!("timed out waiting for {context}");
        }

        fn get_task_count(&self) -> usize {
            self.owner
                .task_registry
                .values()
                .filter(|entry| matches!(entry, TaskEntry::Get { .. }))
                .count()
        }

        async fn shutdown(mut self) {
            self.owner.shutdown_internal_tasks().await;
        }
    }

    #[tokio::test]
    async fn active_memtable_get_hit_returns_without_worker_dispatch() {
        let mut harness = build_test_owner(None, OwnerRuntimeOptions::default()).await;

        let put = harness.submit(test_request(
            "client-a",
            1,
            Some("put-1"),
            ExternalMethod::Put(PutRequest {
                key: b"ant".to_vec(),
                value: b"value-1".to_vec(),
            }),
        ));
        assert_eq!(harness.recv(put).await.status.code, StatusCode::Ok);

        let get = harness.submit(test_request(
            "client-a",
            2,
            Some("get-2"),
            ExternalMethod::Get(GetRequest {
                key: b"ant".to_vec(),
            }),
        ));
        assert_eq!(harness.get_task_count(), 0);
        let response = harness.recv(get).await;
        match response.payload {
            Some(ExternalResponsePayload::Get(payload)) => {
                assert!(payload.found);
                assert_eq!(payload.value.as_deref(), Some(&b"value-1"[..]));
                assert_eq!(payload.observation_seqno, 1);
                assert_eq!(payload.data_generation, 0);
            }
            other => panic!("unexpected payload: {other:?}"),
        }

        harness.shutdown().await;
    }

    #[tokio::test]
    async fn file_backed_get_dispatches_one_worker_task() {
        let mut harness = build_test_owner(
            Some(
                r#"
[lsm]
memtable_flush_bytes = 16384
"#,
            ),
            OwnerRuntimeOptions::default(),
        )
        .await;

        let put1 = harness.submit(test_request(
            "writer-a",
            1,
            Some("put-1"),
            ExternalMethod::Put(PutRequest {
                key: b"ant".to_vec(),
                value: big_value(b'a'),
            }),
        ));
        let put2 = harness.submit(test_request(
            "writer-a",
            2,
            Some("put-2"),
            ExternalMethod::Put(PutRequest {
                key: b"bee".to_vec(),
                value: big_value(b'b'),
            }),
        ));
        assert_eq!(harness.recv(put1).await.status.code, StatusCode::Ok);
        assert_eq!(harness.recv(put2).await.status.code, StatusCode::Ok);
        harness
            .wait_for(
                |owner| {
                    collect_stats_response(&owner.engine)
                        .levels
                        .iter()
                        .any(|level| level.file_count > 0)
                },
                "flush completion",
            )
            .await;

        let get = harness.submit(test_request(
            "reader-a",
            1,
            Some("get-1"),
            ExternalMethod::Get(GetRequest {
                key: b"ant".to_vec(),
            }),
        ));
        assert_eq!(harness.get_task_count(), 1);
        let response = harness.recv(get).await;
        match response.payload {
            Some(ExternalResponsePayload::Get(payload)) => {
                assert!(payload.found);
                assert_eq!(payload.value, Some(big_value(b'a')));
            }
            other => panic!("unexpected payload: {other:?}"),
        }

        harness.shutdown().await;
    }

    #[tokio::test]
    async fn scan_start_and_eof_release_the_session() {
        let mut harness = build_test_owner(None, OwnerRuntimeOptions::default()).await;

        for (request_id, key, value) in [
            (1, b"ant".as_slice(), b"a".as_slice()),
            (2, b"bee".as_slice(), b"b".as_slice()),
        ] {
            let put = harness.submit(test_request(
                "scanner",
                request_id,
                Some("put"),
                ExternalMethod::Put(PutRequest {
                    key: key.to_vec(),
                    value: value.to_vec(),
                }),
            ));
            assert_eq!(harness.recv(put).await.status.code, StatusCode::Ok);
        }

        let start = harness.submit(test_request(
            "scanner",
            3,
            Some("scan-start"),
            ExternalMethod::ScanStart(ScanStartRequest {
                start_bound: Bound::NegInf,
                end_bound: Bound::PosInf,
                max_records_per_page: 4,
                max_bytes_per_page: 4096,
            }),
        ));
        let start_response = harness.recv(start).await;
        let scan_id = match start_response.payload {
            Some(ExternalResponsePayload::ScanStart(payload)) => payload.scan_id,
            other => panic!("unexpected payload: {other:?}"),
        };
        assert_eq!(harness.owner.scan_sessions.len(), 1);

        let fetch = harness.submit(test_request(
            "scanner",
            4,
            Some("scan-fetch"),
            ExternalMethod::ScanFetchNext(ScanFetchNextRequest { scan_id }),
        ));
        let fetch_response = harness.recv(fetch).await;
        match fetch_response.payload {
            Some(ExternalResponsePayload::ScanFetchNext(payload)) => {
                assert!(payload.eof);
                assert_eq!(payload.rows.len(), 2);
            }
            other => panic!("unexpected payload: {other:?}"),
        }
        assert!(harness.owner.scan_sessions.is_empty());

        let follow_up = harness.submit(test_request(
            "scanner",
            5,
            Some("scan-fetch-2"),
            ExternalMethod::ScanFetchNext(ScanFetchNextRequest { scan_id }),
        ));
        assert_eq!(
            harness.recv(follow_up).await.status.code,
            StatusCode::InvalidArgument
        );

        harness.shutdown().await;
    }

    #[tokio::test]
    async fn same_scan_fetch_requests_complete_fifo_across_clients() {
        let mut harness = build_test_owner(
            Some(
                r#"
[lsm]
memtable_flush_bytes = 16384
"#,
            ),
            OwnerRuntimeOptions {
                scan_task_delay: Duration::from_millis(20),
                ..OwnerRuntimeOptions::default()
            },
        )
        .await;

        for (request_id, key, fill) in [(1, b"ant".as_slice(), b'a'), (2, b"bee".as_slice(), b'b')]
        {
            let put = harness.submit(test_request(
                "writer",
                request_id,
                Some("put"),
                ExternalMethod::Put(PutRequest {
                    key: key.to_vec(),
                    value: big_value(fill),
                }),
            ));
            assert_eq!(harness.recv(put).await.status.code, StatusCode::Ok);
        }
        harness
            .wait_for(
                |owner| {
                    collect_stats_response(&owner.engine)
                        .levels
                        .iter()
                        .any(|level| level.file_count > 0)
                },
                "file-backed scan sources",
            )
            .await;

        let start = harness.submit(test_request(
            "client-a",
            1,
            Some("scan-start"),
            ExternalMethod::ScanStart(ScanStartRequest {
                start_bound: Bound::NegInf,
                end_bound: Bound::PosInf,
                max_records_per_page: 1,
                max_bytes_per_page: 8192,
            }),
        ));
        let start_response = harness.recv(start).await;
        let scan_id = match start_response.payload {
            Some(ExternalResponsePayload::ScanStart(payload)) => payload.scan_id,
            other => panic!("unexpected payload: {other:?}"),
        };

        let fetch_a = harness.submit(test_request(
            "client-a",
            2,
            Some("fetch-a"),
            ExternalMethod::ScanFetchNext(ScanFetchNextRequest { scan_id }),
        ));
        let fetch_b = harness.submit(test_request(
            "client-b",
            1,
            Some("fetch-b"),
            ExternalMethod::ScanFetchNext(ScanFetchNextRequest { scan_id }),
        ));
        assert!(matches!(
            harness
                .owner
                .requests
                .get(&(String::from("client-b"), 1))
                .map(|record| &record.phase),
            Some(RequestPhase::QueuedScanFetch { .. })
        ));

        let response_a = harness.recv(fetch_a).await;
        let response_b = harness.recv(fetch_b).await;
        let row_a = match response_a.payload {
            Some(ExternalResponsePayload::ScanFetchNext(payload)) => payload.rows[0].key.clone(),
            other => panic!("unexpected payload: {other:?}"),
        };
        let row_b = match response_b.payload {
            Some(ExternalResponsePayload::ScanFetchNext(payload)) => payload.rows[0].key.clone(),
            other => panic!("unexpected payload: {other:?}"),
        };
        assert_eq!(row_a, b"ant".to_vec());
        assert_eq!(row_b, b"bee".to_vec());

        harness.shutdown().await;
    }

    #[tokio::test]
    async fn queued_scan_fetch_cancellation_removes_only_that_request() {
        let mut harness = build_test_owner(
            Some(
                r#"
[lsm]
memtable_flush_bytes = 16384
"#,
            ),
            OwnerRuntimeOptions {
                scan_task_delay: Duration::from_millis(20),
                ..OwnerRuntimeOptions::default()
            },
        )
        .await;

        for (request_id, key, fill) in [
            (1, b"ant".as_slice(), b'a'),
            (2, b"bee".as_slice(), b'b'),
            (3, b"cat".as_slice(), b'c'),
        ] {
            let put = harness.submit(test_request(
                "writer",
                request_id,
                Some("put"),
                ExternalMethod::Put(PutRequest {
                    key: key.to_vec(),
                    value: big_value(fill),
                }),
            ));
            assert_eq!(harness.recv(put).await.status.code, StatusCode::Ok);
        }
        harness
            .wait_for(
                |owner| {
                    collect_stats_response(&owner.engine)
                        .levels
                        .iter()
                        .any(|level| level.file_count > 0)
                },
                "file-backed scan sources",
            )
            .await;

        let start = harness.submit(test_request(
            "client-a",
            1,
            Some("scan-start"),
            ExternalMethod::ScanStart(ScanStartRequest {
                start_bound: Bound::NegInf,
                end_bound: Bound::PosInf,
                max_records_per_page: 1,
                max_bytes_per_page: 8192,
            }),
        ));
        let scan_id = match harness.recv(start).await.payload {
            Some(ExternalResponsePayload::ScanStart(payload)) => payload.scan_id,
            other => panic!("unexpected payload: {other:?}"),
        };

        let fetch_a = harness.submit(test_request(
            "client-a",
            2,
            Some("fetch-a"),
            ExternalMethod::ScanFetchNext(ScanFetchNextRequest { scan_id }),
        ));
        let fetch_b = harness.submit(test_request(
            "client-b",
            1,
            Some("fetch-b"),
            ExternalMethod::ScanFetchNext(ScanFetchNextRequest { scan_id }),
        ));
        let fetch_c = harness.submit(test_request(
            "client-c",
            1,
            Some("fetch-c"),
            ExternalMethod::ScanFetchNext(ScanFetchNextRequest { scan_id }),
        ));

        assert!(harness.owner.handle_cancel("client-b", "fetch-b"));
        let cancelled = harness.recv(fetch_b).await;
        assert_eq!(cancelled.status.code, StatusCode::Cancelled);

        let first = harness.recv(fetch_a).await;
        let third = harness.recv(fetch_c).await;
        let first_key = match first.payload {
            Some(ExternalResponsePayload::ScanFetchNext(payload)) => payload.rows[0].key.clone(),
            other => panic!("unexpected payload: {other:?}"),
        };
        let third_key = match third.payload {
            Some(ExternalResponsePayload::ScanFetchNext(payload)) => payload.rows[0].key.clone(),
            other => panic!("unexpected payload: {other:?}"),
        };
        assert_eq!(first_key, b"ant".to_vec());
        assert_eq!(third_key, b"bee".to_vec());
        assert!(harness.owner.scan_sessions.contains_key(&scan_id));

        harness.shutdown().await;
    }

    #[tokio::test]
    async fn expiry_releases_session_and_future_fetches_fail_invalid_argument() {
        let mut harness = build_test_owner(None, OwnerRuntimeOptions::default()).await;

        let start = harness.submit(test_request(
            "client-a",
            1,
            Some("scan-start"),
            ExternalMethod::ScanStart(ScanStartRequest {
                start_bound: Bound::NegInf,
                end_bound: Bound::PosInf,
                max_records_per_page: 1,
                max_bytes_per_page: 1024,
            }),
        ));
        let scan_id = match harness.recv(start).await.payload {
            Some(ExternalResponsePayload::ScanStart(payload)) => payload.scan_id,
            other => panic!("unexpected payload: {other:?}"),
        };
        let snapshot_handle = harness
            .owner
            .scan_sessions
            .get(&scan_id)
            .expect("scan session should exist")
            .snapshot_handle
            .clone();

        harness
            .owner
            .scan_sessions
            .get_mut(&scan_id)
            .expect("scan session should exist")
            .expires_at = Instant::now();
        harness.owner.expire_idle_sessions();
        assert!(harness.owner.scan_sessions.is_empty());
        assert!(matches!(
            release_snapshot_handle(&mut harness.owner.engine, &snapshot_handle),
            Err(Error::InvalidArgument(_))
        ));

        let fetch = harness.submit(test_request(
            "client-a",
            2,
            Some("scan-fetch"),
            ExternalMethod::ScanFetchNext(ScanFetchNextRequest { scan_id }),
        ));
        assert_eq!(
            harness.recv(fetch).await.status.code,
            StatusCode::InvalidArgument
        );

        harness.shutdown().await;
    }

    #[tokio::test]
    async fn one_wal_sync_batch_satisfies_multiple_waiters() {
        let mut harness = build_test_owner(
            Some(
                r#"
[wal]
group_commit_max_delay_ms = 20
"#,
            ),
            OwnerRuntimeOptions::default(),
        )
        .await;

        let put_a = harness.submit(test_request(
            "client-a",
            1,
            Some("put-a"),
            ExternalMethod::Put(PutRequest {
                key: b"ant".to_vec(),
                value: b"a".to_vec(),
            }),
        ));
        let put_b = harness.submit(test_request(
            "client-b",
            1,
            Some("put-b"),
            ExternalMethod::Put(PutRequest {
                key: b"bee".to_vec(),
                value: b"b".to_vec(),
            }),
        ));

        let (response_a, syncs_a) = harness.recv_with_sync_count(put_a).await;
        let (response_b, syncs_b) = harness.recv_with_sync_count(put_b).await;
        assert_eq!(response_a.status.code, StatusCode::Ok);
        assert_eq!(response_b.status.code, StatusCode::Ok);
        assert_eq!(syncs_a + syncs_b, 1);

        harness.shutdown().await;
    }

    #[tokio::test]
    async fn waiter_backpressure_returns_busy_before_visible_mutation() {
        let mut harness = build_test_owner(
            Some(
                r#"
[wal]
group_commit_max_delay_ms = 200

[server_limits]
max_waiting_durability_waiters = 1
"#,
            ),
            OwnerRuntimeOptions::default(),
        )
        .await;

        let put_a = harness.submit(test_request(
            "client-a",
            1,
            Some("put-a"),
            ExternalMethod::Put(PutRequest {
                key: b"ant".to_vec(),
                value: b"a".to_vec(),
            }),
        ));
        let put_b = harness.submit(test_request(
            "client-b",
            1,
            Some("put-b"),
            ExternalMethod::Put(PutRequest {
                key: b"bee".to_vec(),
                value: b"b".to_vec(),
            }),
        ));
        let busy = harness.recv(put_b).await;
        assert_eq!(busy.status.code, StatusCode::Busy);

        let get_b = harness.submit(test_request(
            "client-b",
            2,
            Some("get-b"),
            ExternalMethod::Get(GetRequest {
                key: b"bee".to_vec(),
            }),
        ));
        match harness.recv(get_b).await.payload {
            Some(ExternalResponsePayload::Get(payload)) => assert!(!payload.found),
            other => panic!("unexpected payload: {other:?}"),
        }
        assert_eq!(harness.recv(put_a).await.status.code, StatusCode::Ok);

        harness.shutdown().await;
    }

    #[tokio::test]
    async fn wal_sync_failure_stops_future_writes_but_reads_still_work() {
        let mut harness = build_test_owner(
            None,
            OwnerRuntimeOptions {
                engine: EngineTestOptions {
                    wal_writer: WalWriterTestOptions {
                        fail_at_seqno: None,
                        fail_sync_at_seqno: Some(1),
                    },
                },
                ..OwnerRuntimeOptions::default()
            },
        )
        .await;

        let put = harness.submit(test_request(
            "client-a",
            1,
            Some("put-1"),
            ExternalMethod::Put(PutRequest {
                key: b"ant".to_vec(),
                value: b"a".to_vec(),
            }),
        ));
        let put_response = harness.recv(put).await;
        assert_eq!(put_response.status.code, StatusCode::Io);
        assert!(put_response.status.retryable);

        let get = harness.submit(test_request(
            "client-a",
            2,
            Some("get-2"),
            ExternalMethod::Get(GetRequest {
                key: b"ant".to_vec(),
            }),
        ));
        match harness.recv(get).await.payload {
            Some(ExternalResponsePayload::Get(payload)) => {
                assert!(payload.found);
                assert_eq!(payload.value.as_deref(), Some(&b"a"[..]));
            }
            other => panic!("unexpected payload: {other:?}"),
        }

        let second_put = harness.submit(test_request(
            "client-a",
            3,
            Some("put-3"),
            ExternalMethod::Put(PutRequest {
                key: b"bee".to_vec(),
                value: b"b".to_vec(),
            }),
        ));
        let second_put_response = harness.recv(second_put).await;
        assert_eq!(second_put_response.status.code, StatusCode::Io);
        assert!(second_put_response.status.retryable);

        harness.shutdown().await;
    }

    async fn build_test_owner(
        extra_config: Option<&str>,
        options: OwnerRuntimeOptions,
    ) -> TestOwner {
        let config_path = write_test_config(extra_config);
        let config = load_runtime_config(&config_path).unwrap();
        let opened = open_engine_runtime(&config_path, config.clone(), options.engine).unwrap();
        let (external_tx, _external_rx) =
            mpsc::channel(config.server_limits.max_pending_requests as usize);
        let (control_tx, control_rx) = mpsc::unbounded_channel();
        let shared = Arc::new(SharedServerHandle::new(
            external_tx,
            control_tx.clone(),
            config.sevai.listen_addr,
        ));
        shared.set_lifecycle(SharedLifecycle::Ready);

        let (worker_tx, worker_handles) = spawn_worker_pool(
            opened.layout.clone(),
            control_tx.clone(),
            config.server_limits.worker_parallelism,
            config.server_limits.max_worker_tasks,
            options,
        );
        let (wal_sync_tx, wal_sync_rx) =
            mpsc::channel(config.server_limits.max_waiting_durability_waiters as usize);
        let wal_sync_handle = tokio::spawn(wal_sync_actor(
            wal_sync_rx,
            control_tx.clone(),
            config.wal.group_commit_bytes,
            Duration::from_millis(config.wal.group_commit_max_delay_ms),
            options.wal_sync_delay,
        ));
        let expiry_handle = tokio::spawn(scan_expiry_tick(control_tx.clone()));

        let owner = OwnerState::new(
            ServerBootstrapArgs {
                config_path: config_path.clone(),
            },
            config,
            opened,
            shared,
            control_tx,
            worker_tx,
            wal_sync_tx,
            worker_handles,
            wal_sync_handle,
            expiry_handle,
        );
        TestOwner { owner, control_rx }
    }

    fn test_request(
        client_id: &str,
        request_id: u64,
        cancel_token: Option<&str>,
        method: ExternalMethod,
    ) -> ExternalRequest {
        ExternalRequest {
            client_id: client_id.to_string(),
            request_id,
            cancel_token: cancel_token.map(ToOwned::to_owned),
            method,
        }
    }

    fn write_test_config(extra_config: Option<&str>) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let config_id = NEXT_CONFIG_ID.fetch_add(1, Ordering::Relaxed);
        let root = std::env::temp_dir().join(format!("pezhai-sevai-owner-{unique}-{config_id}"));
        fs::create_dir_all(&root).unwrap();
        let path = root.join("config.toml");
        let mut config = r#"
[engine]
sync_mode = "per_write"

[sevai]
listen_addr = "127.0.0.1:0"
"#
        .to_string();
        if let Some(extra_config) = extra_config {
            config.push_str(extra_config);
        }
        fs::write(&path, config).unwrap();
        path
    }

    fn big_value(fill: u8) -> Vec<u8> {
        vec![fill; 9000]
    }
}
