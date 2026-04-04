//! Owner-loop runtime for the in-process `PezhaiServer`.
//!
//! This module hosts one engine instance as ordinary owner-thread state. The
//! owner loop admits external requests, serializes visible mutations, manages
//! scan sessions, dispatches immutable work to background services, and emits
//! responses in per-client order.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU8, Ordering};
use std::thread::{self, JoinHandle};

use tokio::sync::{Notify, mpsc, oneshot};
use tokio::time::{Duration, Instant};

use crate::error::KalanjiyamError;
use crate::pezhai::engine::{
    CheckpointTaskPlan, CheckpointTaskResult, CompactTaskPlan, CompactTaskResult, FlushTaskPlan,
    FlushTaskResult, GcOwnerPins, GcTaskPlan, GcTaskResult, PezhaiEngine, ReadDecision,
    SyncDecision, WriteDecision,
};
use crate::pezhai::types::{ScanPage, SnapshotHandle};
use crate::sevai::worker::{
    WorkerCompletion, WorkerPool, WorkerResultEnvelope, WorkerTask, WorkerTaskEnvelope,
    WorkerTaskKind,
};
use crate::sevai::{
    ExternalMethod, ExternalRequest, ExternalResponse, ExternalResponsePayload,
    ScanFetchNextResponse, ScanStartResponse, error_response, lifecycle_error_response,
    success_response, validate_scan_start_inputs,
};

type ReplySender = oneshot::Sender<ExternalResponse>;

/// Shared lifecycle states visible to the public handle and the owner loop.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum LifecycleState {
    /// The owner loop has not yet entered the ready state.
    Booting,
    /// The server is accepting external requests.
    Ready,
    /// Shutdown has stopped admission but cleanup is still in progress.
    Stopping,
    /// The server has fully stopped.
    Stopped,
}

impl LifecycleState {
    pub(crate) const fn as_u8(self) -> u8 {
        match self {
            Self::Booting => 0,
            Self::Ready => 1,
            Self::Stopping => 2,
            Self::Stopped => 3,
        }
    }

    pub(crate) fn from_u8(value: u8) -> Self {
        match value {
            0 => Self::Booting,
            1 => Self::Ready,
            2 => Self::Stopping,
            3 => Self::Stopped,
            _ => Self::Stopped,
        }
    }
}

/// One public request queued to the owner loop.
#[derive(Debug)]
pub(crate) struct ExternalCall {
    /// External request envelope.
    pub request: ExternalRequest,
    /// Reply channel owned by the public handle.
    pub reply: ReplySender,
}

/// One control-plane message routed to the owner loop.
#[derive(Debug)]
pub(crate) enum ControlMessage {
    /// Cancels one external request on a best-effort basis.
    Cancel {
        client_id: String,
        request_id: u64,
        ack: oneshot::Sender<Result<(), KalanjiyamError>>,
    },
    /// Marks one client disconnected and cancels that client's queued work.
    DisconnectClient {
        client_id: String,
        ack: oneshot::Sender<Result<(), KalanjiyamError>>,
    },
    /// Begins asynchronous shutdown cleanup.
    Shutdown {
        ack: oneshot::Sender<Result<(), KalanjiyamError>>,
    },
}

/// Stable key used for request-ordering and waiter tables inside the owner loop.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
struct RequestKey {
    client_id: String,
    request_id: u64,
}

impl RequestKey {
    fn from_request(request: &ExternalRequest) -> Self {
        Self {
            client_id: request.client_id.clone(),
            request_id: request.request_id,
        }
    }
}

/// Per-client sequencing and buffered-response state owned by the main thread.
#[derive(Debug)]
struct ClientState {
    next_request_id: u64,
    next_response_id: u64,
    buffered_responses: BTreeMap<u64, (ReplySender, ExternalResponse)>,
}

/// Owner-loop stage for one admitted external request.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RequestStage {
    OwnerProcessing,
    WaitingWorker { task_id: u64 },
    WaitingWal { target_seqno: u64 },
    ScanQueued { scan_id: u64 },
    ScanActive { scan_id: u64, task_id: Option<u64> },
}

/// Owner-owned tracking state for one admitted request until its reply is emitted.
#[derive(Debug)]
struct ExternalRequestState {
    request: ExternalRequest,
    reply: ReplySender,
    stage: RequestStage,
    cancelled: bool,
}

/// Terminal scan-session states used for expiry and EOF cleanup.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum ScanTerminalState {
    Open,
    EofReached,
    Expired,
}

/// Owner-owned scan-session state that serializes same-`scan_id` fetches.
#[derive(Debug)]
struct ScanSessionState {
    snapshot: SnapshotHandle,
    range: crate::pezhai::types::KeyRange,
    max_records_per_page: u32,
    max_bytes_per_page: u32,
    resume_after_key: Option<Vec<u8>>,
    fetch_request_queue: VecDeque<RequestKey>,
    active_fetch_request: Option<RequestKey>,
    in_flight_task_id: Option<u64>,
    terminal_state: ScanTerminalState,
    expires_at: Instant,
}

/// Automatic maintenance kinds dispatched by the owner runtime.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MaintenanceTaskKind {
    Flush,
    Compact,
    Checkpoint,
    Gc,
}

/// Owner-side bookkeeping for one in-flight worker task.
#[derive(Debug)]
struct WorkerTaskState {
    request_key: Option<RequestKey>,
    scan_id: Option<u64>,
    maintenance: Option<MaintenanceTaskKind>,
    get_plan: Option<crate::pezhai::engine::GetPlan>,
    scan_plan: Option<crate::pezhai::engine::ScanPlan>,
    flush_plan: Option<FlushTaskPlan>,
    compact_plan: Option<CompactTaskPlan>,
    checkpoint_plan: Option<CheckpointTaskPlan>,
    checkpoint_generation: Option<u64>,
    gc_plan: Option<GcTaskPlan>,
    cancelled: bool,
}

/// Normalized durability-admission result shared by writes and explicit sync waits.
#[derive(Debug)]
enum DurabilityWaitDecision {
    Complete(ExternalResponsePayload),
    Wait(WalWaiterState),
    Reject(KalanjiyamError),
}

/// Normalized `Get` routing decision shared by production and direct helper tests.
#[derive(Debug)]
enum GetDispatchDecision {
    Complete(crate::pezhai::types::GetResponse),
    Dispatch {
        task_id: u64,
        task: Box<WorkerTask>,
        task_state: Box<WorkerTaskState>,
    },
    Reject(KalanjiyamError),
}

/// One owner-side scan transition built before publication or worker dispatch.
#[derive(Debug)]
enum ScanFetchTransition {
    Idle,
    DrainQueued {
        queued: Vec<RequestKey>,
        error: KalanjiyamError,
    },
    Complete {
        request_key: RequestKey,
        response: ExternalResponse,
        continue_scan: bool,
    },
    ImmediatePage {
        request_key: RequestKey,
        page: ScanPage,
    },
    Dispatch {
        task_id: u64,
        task: Box<WorkerTask>,
        task_state: Box<WorkerTaskState>,
    },
}

/// Owner-thread publication plan for one finished scan fetch.
#[derive(Debug)]
struct ScanPublication {
    primary: ExternalResponse,
    queued: Vec<(RequestKey, ExternalResponse)>,
    release_snapshot: Option<SnapshotHandle>,
    continue_scan: bool,
}

/// Normalized worker completion after task-kind validation and publication shaping.
#[derive(Debug)]
enum NormalizedWorkerCompletion {
    Request {
        request_key: RequestKey,
        response: ExternalResponse,
    },
    ScanPage {
        scan_id: u64,
        request_key: RequestKey,
        page: ScanPage,
    },
    Maintenance(MaintenanceWorkerCompletion),
    Ignore,
}

/// Typed maintenance completion ready for owner-thread publication.
#[derive(Debug)]
enum MaintenanceWorkerCompletion {
    Flush {
        plan: FlushTaskPlan,
        prepared: FlushTaskResult,
    },
    Compact {
        plan: CompactTaskPlan,
        prepared: CompactTaskResult,
    },
    Checkpoint {
        plan: CheckpointTaskPlan,
        prepared: CheckpointTaskResult,
    },
    Gc {
        plan: GcTaskPlan,
        prepared: GcTaskResult,
    },
    Retry,
    Ignore,
}

/// Owner-visible maintenance side effects after result publication.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum MaintenancePublicationDecision {
    NoChange,
    Retry,
    CheckpointPublished,
    GcPublished,
}

/// Success payload shape associated with one waiting WAL acknowledgement.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum WalSuccessPayloadKind {
    Put,
    Delete,
    Sync,
}

/// Owner-visible waiter state for one request blocked on WAL durability.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct WalWaiterState {
    target_seqno: u64,
    success_kind: WalSuccessPayloadKind,
    created_at: Instant,
}

/// One sync batch currently owned by the dedicated WAL service thread.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct WalBatchState {
    task_id: u64,
    target_seqno: u64,
    synced_bytes: u64,
}

/// One request sent to the dedicated WAL sync service.
#[derive(Debug)]
struct WalSyncRequest {
    task_id: u64,
    path: PathBuf,
}

/// One completion from the dedicated WAL sync service.
#[derive(Debug)]
pub(crate) struct WalSyncResult {
    task_id: u64,
    result: Result<(), KalanjiyamError>,
}

/// Shared queue state for the dedicated WAL sync service.
#[derive(Default)]
struct WalQueueState {
    closed: bool,
    requests: VecDeque<WalSyncRequest>,
}

/// Blocking queue used by the dedicated WAL sync service thread.
struct WalQueue {
    state: std::sync::Mutex<WalQueueState>,
    cv: std::sync::Condvar,
}

impl WalQueue {
    fn new() -> Self {
        Self {
            state: std::sync::Mutex::new(WalQueueState::default()),
            cv: std::sync::Condvar::new(),
        }
    }

    fn push(&self, request: WalSyncRequest) {
        let mut state = self
            .state
            .lock()
            .expect("wal queue mutex should not poison");
        if state.closed {
            return;
        }
        state.requests.push_back(request);
        self.cv.notify_one();
    }

    fn pop(&self) -> Option<WalSyncRequest> {
        let mut state = self
            .state
            .lock()
            .expect("wal queue mutex should not poison");
        loop {
            if let Some(request) = state.requests.pop_front() {
                return Some(request);
            }
            if state.closed {
                return None;
            }
            state = self
                .cv
                .wait(state)
                .expect("wal queue mutex should not poison");
        }
    }

    fn close(&self) {
        let mut state = self
            .state
            .lock()
            .expect("wal queue mutex should not poison");
        state.closed = true;
        self.cv.notify_all();
    }
}

/// Dedicated WAL sync service used by the owner runtime.
struct WalSyncService {
    queue: Arc<WalQueue>,
    join: Option<JoinHandle<()>>,
}

impl WalSyncService {
    fn new(result_tx: mpsc::Sender<WalSyncResult>) -> Self {
        let queue = Arc::new(WalQueue::new());
        let queue_clone = Arc::clone(&queue);
        let join = thread::spawn(move || {
            while let Some(request) = queue_clone.pop() {
                let result = crate::pezhai::engine::execute_wal_sync(&request.path);
                if result_tx
                    .blocking_send(WalSyncResult {
                        task_id: request.task_id,
                        result,
                    })
                    .is_err()
                {
                    return;
                }
            }
        });
        Self {
            queue,
            join: Some(join),
        }
    }

    fn enqueue(&self, request: WalSyncRequest) {
        self.queue.push(request);
    }

    fn shutdown(&mut self) {
        self.queue.close();
        if let Some(join) = self.join.take() {
            let _ = join.join();
        }
    }
}

impl Drop for WalSyncService {
    fn drop(&mut self) {
        self.shutdown();
    }
}

/// Owner-loop runtime state hosting one engine instance and all server policy.
pub(crate) struct OwnerRuntime {
    engine: PezhaiEngine,
    external_rx: mpsc::Receiver<ExternalCall>,
    control_rx: mpsc::Receiver<ControlMessage>,
    worker_result_rx: mpsc::Receiver<WorkerResultEnvelope>,
    wal_result_rx: mpsc::Receiver<WalSyncResult>,
    worker_pool: WorkerPool,
    wal_service: WalSyncService,
    stopped: Arc<Notify>,
    lifecycle: Arc<AtomicU8>,
    drop_shutdown_requested: Arc<AtomicBool>,
    drop_shutdown_notify: Arc<Notify>,
    expected_request_ids: BTreeMap<String, ClientState>,
    active_requests: BTreeMap<RequestKey, ExternalRequestState>,
    pre_admission_cancellations: BTreeMap<String, BTreeSet<u64>>,
    disconnected_clients: BTreeSet<String>,
    scan_sessions: BTreeMap<u64, ScanSessionState>,
    worker_tasks: BTreeMap<u64, WorkerTaskState>,
    wal_waiters: BTreeMap<RequestKey, WalWaiterState>,
    wal_batch: Option<WalBatchState>,
    next_scan_id: u64,
    next_task_id: u64,
    maintenance_retry_at: Option<Instant>,
    checkpoint_due: bool,
    gc_due: bool,
    next_checkpoint_at: Instant,
    next_gc_at: Instant,
}

impl OwnerRuntime {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        engine: PezhaiEngine,
        external_rx: mpsc::Receiver<ExternalCall>,
        control_rx: mpsc::Receiver<ControlMessage>,
        worker_result_rx: mpsc::Receiver<WorkerResultEnvelope>,
        wal_result_rx: mpsc::Receiver<WalSyncResult>,
        worker_result_tx: mpsc::Sender<WorkerResultEnvelope>,
        wal_result_tx: mpsc::Sender<WalSyncResult>,
        stopped: Arc<Notify>,
        lifecycle: Arc<AtomicU8>,
        drop_shutdown_requested: Arc<AtomicBool>,
        drop_shutdown_notify: Arc<Notify>,
    ) -> Self {
        let worker_threads = engine.state().config.maintenance.worker_threads;
        let gc_interval_secs = engine.state().config.maintenance.gc_interval_secs;
        let checkpoint_interval_secs = engine.state().config.maintenance.checkpoint_interval_secs;
        Self {
            engine,
            external_rx,
            control_rx,
            worker_result_rx,
            wal_result_rx,
            worker_pool: WorkerPool::new(worker_threads, worker_result_tx),
            wal_service: WalSyncService::new(wal_result_tx),
            stopped,
            lifecycle,
            drop_shutdown_requested,
            drop_shutdown_notify,
            expected_request_ids: BTreeMap::new(),
            active_requests: BTreeMap::new(),
            pre_admission_cancellations: BTreeMap::new(),
            disconnected_clients: BTreeSet::new(),
            scan_sessions: BTreeMap::new(),
            worker_tasks: BTreeMap::new(),
            wal_waiters: BTreeMap::new(),
            wal_batch: None,
            next_scan_id: 1,
            next_task_id: 1,
            maintenance_retry_at: None,
            checkpoint_due: false,
            gc_due: false,
            next_checkpoint_at: Instant::now() + Duration::from_secs(checkpoint_interval_secs),
            next_gc_at: Instant::now() + Duration::from_secs(gc_interval_secs),
        }
    }

    pub(crate) async fn run(&mut self) {
        self.lifecycle
            .store(LifecycleState::Ready.as_u8(), Ordering::SeqCst);
        loop {
            if self.drop_shutdown_requested.load(Ordering::SeqCst) {
                self.finish_after_last_handle_drop().await;
                break;
            }
            let next_deadline = self.next_deadline();
            if let Some(deadline) = next_deadline {
                tokio::select! {
                    biased;
                    _ = self.drop_shutdown_notify.notified() => {
                        self.finish_after_last_handle_drop().await;
                        break;
                    }
                    Some(control) = self.control_rx.recv() => {
                        if self.handle_control(control).await {
                            break;
                        }
                    }
                    Some(worker_result) = self.worker_result_rx.recv() => {
                        self.handle_worker_result(worker_result).await;
                    }
                    Some(wal_result) = self.wal_result_rx.recv() => {
                        self.handle_wal_result(wal_result).await;
                    }
                    Some(call) = self.external_rx.recv() => {
                        self.handle_external(call).await;
                    }
                    _ = tokio::time::sleep_until(deadline) => {
                        self.handle_timers().await;
                    }
                }
            } else {
                tokio::select! {
                    biased;
                    _ = self.drop_shutdown_notify.notified() => {
                        self.finish_after_last_handle_drop().await;
                        break;
                    }
                    Some(control) = self.control_rx.recv() => {
                        if self.handle_control(control).await {
                            break;
                        }
                    }
                    Some(worker_result) = self.worker_result_rx.recv() => {
                        self.handle_worker_result(worker_result).await;
                    }
                    Some(wal_result) = self.wal_result_rx.recv() => {
                        self.handle_wal_result(wal_result).await;
                    }
                    Some(call) = self.external_rx.recv() => {
                        self.handle_external(call).await;
                    }
                    else => {
                        break;
                    }
                }
            }
        }
        self.finish_shutdown().await;
    }

    async fn finish_after_last_handle_drop(&mut self) {
        // Last-handle drop has no caller waiting for an acknowledgment, but it
        // still needs the same owner-side cleanup before the runtime exits.
        self.lifecycle
            .store(LifecycleState::Stopping.as_u8(), Ordering::SeqCst);
        let _ = self.begin_shutdown().await;
    }

    fn next_deadline(&self) -> Option<Instant> {
        let scan_deadline = self
            .scan_sessions
            .values()
            .map(|session| session.expires_at)
            .min();
        let wal_deadline = if self.wal_batch.is_none() {
            self.wal_waiters
                .values()
                .map(|waiter| {
                    waiter.created_at
                        + Duration::from_millis(
                            self.engine.state().config.wal.group_commit_max_delay_ms,
                        )
                })
                .min()
        } else {
            None
        };
        let maintenance_deadline = self.next_checkpoint_at.min(self.next_gc_at);
        let mut next = Some(maintenance_deadline);
        if let Some(deadline) = scan_deadline {
            next = Some(next.map_or(deadline, |current| current.min(deadline)));
        }
        if let Some(deadline) = wal_deadline {
            next = Some(next.map_or(deadline, |current| current.min(deadline)));
        }
        if let Some(deadline) = self.maintenance_retry_at {
            next = Some(next.map_or(deadline, |current| current.min(deadline)));
        }
        next
    }

    async fn handle_control(&mut self, control: ControlMessage) -> bool {
        match control {
            ControlMessage::Cancel {
                client_id,
                request_id,
                ack,
            } => {
                let result = self.cancel_request(
                    &RequestKey {
                        client_id,
                        request_id,
                    },
                    "request was cancelled",
                );
                let _ = ack.send(result);
                false
            }
            ControlMessage::DisconnectClient { client_id, ack } => {
                let result = self.disconnect_client(&client_id);
                let _ = ack.send(result);
                false
            }
            ControlMessage::Shutdown { ack } => {
                self.lifecycle
                    .store(LifecycleState::Stopping.as_u8(), Ordering::SeqCst);
                let result = self.begin_shutdown().await;
                let _ = ack.send(result);
                true
            }
        }
    }

    async fn handle_external(&mut self, call: ExternalCall) {
        let request = call.request;
        if let Some(response) = self.pre_admission_external_rejection(&request) {
            let _ = call.reply.send(response);
            return;
        }
        let request_key = RequestKey::from_request(&request);
        self.active_requests.insert(
            request_key.clone(),
            ExternalRequestState {
                request: request.clone(),
                reply: call.reply,
                stage: RequestStage::OwnerProcessing,
                cancelled: false,
            },
        );

        let response = match request.method.clone() {
            ExternalMethod::Put { key, value } => {
                let decision = self.engine.decide_put(key, value);
                self.handle_write_request(
                    &request_key,
                    &request,
                    WalSuccessPayloadKind::Put,
                    decision,
                )
            }
            ExternalMethod::Delete { key } => {
                let decision = self.engine.decide_delete(key);
                self.handle_write_request(
                    &request_key,
                    &request,
                    WalSuccessPayloadKind::Delete,
                    decision,
                )
            }
            ExternalMethod::Stats => Some(match self.engine.stats().await {
                Ok(payload) => success_response(&request, ExternalResponsePayload::Stats(payload)),
                Err(error) => error_response(&request, error),
            }),
            ExternalMethod::Sync => self.handle_sync_request(&request_key),
            ExternalMethod::Get { key: user_key } => {
                self.handle_get_request(&request_key, &request, user_key.as_slice())
            }
            ExternalMethod::ScanStart {
                start_bound,
                end_bound,
                max_records_per_page,
                max_bytes_per_page,
            } => {
                self.handle_scan_start(
                    &request_key,
                    &request,
                    start_bound,
                    end_bound,
                    max_records_per_page,
                    max_bytes_per_page,
                )
                .await
            }
            ExternalMethod::ScanFetchNext { scan_id } => {
                self.handle_scan_fetch_next(&request_key, &request, scan_id)
                    .await
            }
        };

        if let Some(response) = response {
            self.complete_request(request_key, response);
        }
    }

    // Reject requests before owner admission so public handling and helper
    // tests share the same lifecycle, disconnect, cancellation, and sequencing rules.
    fn pre_admission_external_rejection(
        &mut self,
        request: &ExternalRequest,
    ) -> Option<ExternalResponse> {
        let lifecycle = LifecycleState::from_u8(self.lifecycle.load(Ordering::SeqCst));
        if lifecycle != LifecycleState::Ready {
            return Some(lifecycle_error_response(request, lifecycle));
        }
        if self.disconnected_clients.contains(&request.client_id) {
            return Some(error_response(
                request,
                KalanjiyamError::Cancelled("client has been disconnected".to_string()),
            ));
        }
        if self
            .pre_admission_cancellations
            .get(&request.client_id)
            .is_some_and(|ids| ids.contains(&request.request_id))
        {
            return Some(error_response(
                request,
                KalanjiyamError::Cancelled("request was cancelled before admission".to_string()),
            ));
        }
        if let Err(error) = self.admit_request_id(request) {
            return Some(error_response(request, error));
        }
        None
    }

    fn admit_request_id(&mut self, request: &ExternalRequest) -> Result<(), KalanjiyamError> {
        let client = self
            .expected_request_ids
            .entry(request.client_id.clone())
            .or_insert_with(|| ClientState {
                next_request_id: request.request_id,
                next_response_id: request.request_id,
                buffered_responses: BTreeMap::new(),
            });
        if request.request_id != client.next_request_id {
            return Err(KalanjiyamError::InvalidArgument(format!(
                "request_id {} does not match the next expected request_id {}",
                request.request_id, client.next_request_id
            )));
        }
        client.next_request_id += 1;
        Ok(())
    }

    fn handle_write_request(
        &mut self,
        request_key: &RequestKey,
        request: &ExternalRequest,
        success_kind: WalSuccessPayloadKind,
        decision: Result<WriteDecision, KalanjiyamError>,
    ) -> Option<ExternalResponse> {
        let normalized = match decision {
            Ok(WriteDecision::Immediate) => {
                DurabilityWaitDecision::Complete(wal_success_payload(success_kind, None))
            }
            Ok(WriteDecision::Wait(wait)) => self.register_durability_wait(
                request_key,
                WalWaiterState {
                    target_seqno: wait.target_seqno,
                    success_kind,
                    created_at: Instant::now(),
                },
                "too many waiting WAL acknowledgements",
            ),
            Err(error) => DurabilityWaitDecision::Reject(error),
        };
        match normalized {
            DurabilityWaitDecision::Complete(payload) => {
                let _ = self.maybe_dispatch_maintenance();
                Some(success_response(request, payload))
            }
            DurabilityWaitDecision::Wait(waiter) => {
                self.register_wal_waiter(request_key.clone(), waiter);
                if let Some(state) = self.active_requests.get_mut(request_key) {
                    state.stage = RequestStage::WaitingWal {
                        target_seqno: waiter.target_seqno,
                    };
                }
                self.dispatch_wal_sync_if_needed();
                let _ = self.maybe_dispatch_maintenance();
                None
            }
            DurabilityWaitDecision::Reject(error) => Some(error_response(request, error)),
        }
    }

    // Normalize write and sync durability waits before reply creation so the
    // owner updates waiter tables and request stages in one place.
    fn register_durability_wait(
        &self,
        request_key: &RequestKey,
        waiter: WalWaiterState,
        busy_message: &str,
    ) -> DurabilityWaitDecision {
        if self.wal_waiters.len() >= self.engine.state().config.maintenance.max_waiting_wal_syncs {
            return DurabilityWaitDecision::Reject(KalanjiyamError::Busy(busy_message.to_string()));
        }
        let _ = request_key;
        DurabilityWaitDecision::Wait(waiter)
    }

    fn handle_get_request(
        &mut self,
        request_key: &RequestKey,
        request: &ExternalRequest,
        key: &[u8],
    ) -> Option<ExternalResponse> {
        match self.get_dispatch_decision(request_key, key) {
            GetDispatchDecision::Complete(payload) => Some(success_response(
                request,
                ExternalResponsePayload::Get(payload),
            )),
            GetDispatchDecision::Dispatch {
                task_id,
                task,
                task_state,
            } => {
                self.worker_tasks.insert(task_id, *task_state);
                if let Some(state) = self.active_requests.get_mut(request_key) {
                    state.stage = RequestStage::WaitingWorker { task_id };
                }
                self.worker_pool.enqueue(WorkerTaskEnvelope {
                    task_id,
                    task: *task,
                });
                None
            }
            GetDispatchDecision::Reject(error) => Some(error_response(request, error)),
        }
    }

    // Build the `Get` decision before mutating owner tables so tests can cover
    // the same immediate, busy, and worker-dispatch branches as production.
    fn get_dispatch_decision(
        &mut self,
        request_key: &RequestKey,
        key: &[u8],
    ) -> GetDispatchDecision {
        match self.engine.decide_current_read(key) {
            Ok(ReadDecision::ImmediateGet(payload)) => GetDispatchDecision::Complete(payload),
            Ok(ReadDecision::GetPlan(plan)) => {
                if self.worker_tasks.len()
                    >= self.engine.state().config.maintenance.max_worker_tasks
                {
                    return GetDispatchDecision::Reject(KalanjiyamError::Busy(
                        "worker task queue is full".to_string(),
                    ));
                }
                let task_id = self.allocate_task_id();
                let store_dir = self.engine.state().store_dir.clone();
                GetDispatchDecision::Dispatch {
                    task_id,
                    task: Box::new(WorkerTask::Get {
                        store_dir,
                        plan: plan.clone(),
                    }),
                    task_state: Box::new(WorkerTaskState {
                        request_key: Some(request_key.clone()),
                        scan_id: None,
                        maintenance: None,
                        get_plan: Some(plan),
                        scan_plan: None,
                        flush_plan: None,
                        compact_plan: None,
                        checkpoint_plan: None,
                        checkpoint_generation: None,
                        gc_plan: None,
                        cancelled: false,
                    }),
                }
            }
            Ok(other) => GetDispatchDecision::Reject(KalanjiyamError::Corruption(format!(
                "unexpected engine read decision for Get: {other:?}"
            ))),
            Err(error) => GetDispatchDecision::Reject(error),
        }
    }

    async fn handle_scan_start(
        &mut self,
        request_key: &RequestKey,
        request: &ExternalRequest,
        start_bound: crate::pezhai::types::Bound,
        end_bound: crate::pezhai::types::Bound,
        max_records_per_page: u32,
        max_bytes_per_page: u32,
    ) -> Option<ExternalResponse> {
        if self.scan_sessions.len() >= self.engine.state().config.maintenance.max_scan_sessions {
            return Some(error_response(
                request,
                KalanjiyamError::Busy("scan session table is full".to_string()),
            ));
        }
        let range = match validate_scan_start_inputs(
            &start_bound,
            &end_bound,
            max_records_per_page,
            max_bytes_per_page,
        ) {
            Ok(range) => range,
            Err(error) => return Some(error_response(request, error)),
        };
        let snapshot = match self.engine.create_snapshot().await {
            Ok(snapshot) => snapshot,
            Err(error) => return Some(error_response(request, error)),
        };
        let scan_id = self.next_scan_id;
        self.next_scan_id += 1;
        let expiry = Instant::now()
            + Duration::from_millis(self.engine.state().config.maintenance.scan_expiry_ms);
        self.scan_sessions.insert(
            scan_id,
            ScanSessionState {
                snapshot: snapshot.clone(),
                range,
                max_records_per_page,
                max_bytes_per_page,
                resume_after_key: None,
                fetch_request_queue: VecDeque::new(),
                active_fetch_request: None,
                in_flight_task_id: None,
                terminal_state: ScanTerminalState::Open,
                expires_at: expiry,
            },
        );
        let _ = request_key;
        Some(success_response(
            request,
            ExternalResponsePayload::ScanStart(ScanStartResponse {
                scan_id,
                observation_seqno: snapshot.snapshot_seqno,
                data_generation: snapshot.data_generation,
            }),
        ))
    }

    async fn handle_scan_fetch_next(
        &mut self,
        request_key: &RequestKey,
        request: &ExternalRequest,
        scan_id: u64,
    ) -> Option<ExternalResponse> {
        match self.build_scan_fetch_transition(scan_id, Some(request_key), None) {
            ScanFetchTransition::Complete { response, .. } => Some(response),
            transition => {
                self.apply_scan_fetch_transition(scan_id, transition).await;
                let _ = request;
                None
            }
        }
    }

    fn handle_sync_request(&mut self, request_key: &RequestKey) -> Option<ExternalResponse> {
        let request = self.active_requests[request_key].request.clone();
        let normalized = match self.engine.decide_sync_wait() {
            Ok(SyncDecision::Immediate(payload)) => {
                DurabilityWaitDecision::Complete(ExternalResponsePayload::Sync(payload))
            }
            Ok(SyncDecision::Wait(wait)) => self.register_durability_wait(
                request_key,
                WalWaiterState {
                    target_seqno: wait.target_seqno,
                    success_kind: WalSuccessPayloadKind::Sync,
                    created_at: Instant::now(),
                },
                "too many waiting Sync requests",
            ),
            Err(error) => DurabilityWaitDecision::Reject(error),
        };
        match normalized {
            DurabilityWaitDecision::Complete(payload) => Some(success_response(&request, payload)),
            DurabilityWaitDecision::Wait(waiter) => {
                self.register_wal_waiter(request_key.clone(), waiter);
                if let Some(state) = self.active_requests.get_mut(request_key) {
                    state.stage = RequestStage::WaitingWal {
                        target_seqno: waiter.target_seqno,
                    };
                }
                self.dispatch_wal_sync_if_needed();
                None
            }
            DurabilityWaitDecision::Reject(error) => Some(error_response(&request, error)),
        }
    }

    async fn maybe_start_next_scan_fetch(
        &mut self,
        scan_id: u64,
        terminal_error: Option<KalanjiyamError>,
    ) {
        let transition = self.build_scan_fetch_transition(scan_id, None, terminal_error);
        self.apply_scan_fetch_transition(scan_id, transition).await;
    }

    async fn finish_scan_fetch(&mut self, scan_id: u64, request_key: RequestKey, page: ScanPage) {
        let Some(publication) = self.publish_scan_fetch_result(scan_id, &request_key, page) else {
            return;
        };
        self.complete_request(request_key, publication.primary);
        for (queued_key, response) in publication.queued {
            self.complete_request(queued_key, response);
        }
        if let Some(snapshot) = publication.release_snapshot {
            let _ = self.engine.release_snapshot(&snapshot).await;
        }
        if publication.continue_scan {
            Box::pin(self.maybe_start_next_scan_fetch(scan_id, None)).await;
        }
    }

    // Advance one scan session through queueing, claiming, and dispatch setup
    // without publishing replies, keeping the side-effect boundary explicit.
    fn build_scan_fetch_transition(
        &mut self,
        scan_id: u64,
        request_key: Option<&RequestKey>,
        terminal_error: Option<KalanjiyamError>,
    ) -> ScanFetchTransition {
        if let Some(request_key) = request_key {
            let Some(session) = self.scan_sessions.get_mut(&scan_id) else {
                let request = self.active_requests[request_key].request.clone();
                return ScanFetchTransition::Complete {
                    request_key: request_key.clone(),
                    response: error_response(
                        &request,
                        KalanjiyamError::InvalidArgument("unknown scan_id".to_string()),
                    ),
                    continue_scan: false,
                };
            };
            if session.terminal_state != ScanTerminalState::Open {
                let request = self.active_requests[request_key].request.clone();
                return ScanFetchTransition::Complete {
                    request_key: request_key.clone(),
                    response: error_response(
                        &request,
                        KalanjiyamError::InvalidArgument("unknown scan_id".to_string()),
                    ),
                    continue_scan: false,
                };
            }
            if session.active_fetch_request.is_some()
                && session.fetch_request_queue.len()
                    >= self.engine.state().config.maintenance.max_scan_fetch_queue
            {
                let request = self.active_requests[request_key].request.clone();
                return ScanFetchTransition::Complete {
                    request_key: request_key.clone(),
                    response: error_response(
                        &request,
                        KalanjiyamError::Busy("scan fetch queue is full".to_string()),
                    ),
                    continue_scan: false,
                };
            }
            session.fetch_request_queue.push_back(request_key.clone());
            if let Some(state) = self.active_requests.get_mut(request_key) {
                state.stage = RequestStage::ScanQueued { scan_id };
            }
        }

        let Some(session) = self.scan_sessions.get(&scan_id) else {
            return ScanFetchTransition::Idle;
        };
        if session.active_fetch_request.is_some()
            || session.terminal_state != ScanTerminalState::Open
        {
            return ScanFetchTransition::Idle;
        }
        if let Some(error) = terminal_error {
            let queued = self
                .scan_sessions
                .get_mut(&scan_id)
                .expect("scan session should still exist")
                .fetch_request_queue
                .drain(..)
                .collect();
            return ScanFetchTransition::DrainQueued { queued, error };
        }

        let (
            request_key,
            snapshot,
            range,
            resume_after_key,
            max_records_per_page,
            max_bytes_per_page,
        ) = {
            let session = self
                .scan_sessions
                .get_mut(&scan_id)
                .expect("scan session should still exist");
            let Some(request_key) = session.fetch_request_queue.pop_front() else {
                return ScanFetchTransition::Idle;
            };
            // The owner thread is the only place that may advance a scan session,
            // so claiming the active request here preserves per-scan FIFO order.
            session.active_fetch_request = Some(request_key.clone());
            (
                request_key,
                session.snapshot.clone(),
                session.range.clone(),
                session.resume_after_key.clone(),
                session.max_records_per_page,
                session.max_bytes_per_page,
            )
        };
        if let Some(state) = self.active_requests.get_mut(&request_key) {
            state.stage = RequestStage::ScanActive {
                scan_id,
                task_id: None,
            };
        }

        match self.engine.decide_scan_page(
            scan_id,
            &snapshot,
            &range,
            resume_after_key.as_deref(),
            max_records_per_page,
            max_bytes_per_page,
        ) {
            Ok(ReadDecision::ImmediateScan(page)) => {
                ScanFetchTransition::ImmediatePage { request_key, page }
            }
            Ok(ReadDecision::ScanPlan(plan)) => {
                if self.worker_tasks.len()
                    >= self.engine.state().config.maintenance.max_worker_tasks
                {
                    self.reset_active_scan_dispatch(scan_id);
                    let request = self.active_requests[&request_key].request.clone();
                    return ScanFetchTransition::Complete {
                        request_key,
                        response: error_response(
                            &request,
                            KalanjiyamError::Busy("worker task queue is full".to_string()),
                        ),
                        continue_scan: true,
                    };
                }
                let task_id = self.allocate_task_id();
                let store_dir = self.engine.state().store_dir.clone();
                if let Some(session) = self.scan_sessions.get_mut(&scan_id) {
                    session.in_flight_task_id = Some(task_id);
                }
                if let Some(state) = self.active_requests.get_mut(&request_key) {
                    state.stage = RequestStage::ScanActive {
                        scan_id,
                        task_id: Some(task_id),
                    };
                }
                ScanFetchTransition::Dispatch {
                    task_id,
                    task: Box::new(WorkerTask::ScanPage {
                        store_dir,
                        plan: plan.clone(),
                    }),
                    task_state: Box::new(WorkerTaskState {
                        request_key: Some(request_key),
                        scan_id: Some(scan_id),
                        maintenance: None,
                        get_plan: None,
                        scan_plan: Some(plan),
                        flush_plan: None,
                        compact_plan: None,
                        checkpoint_plan: None,
                        checkpoint_generation: None,
                        gc_plan: None,
                        cancelled: false,
                    }),
                }
            }
            Ok(other) => {
                self.reset_active_scan_dispatch(scan_id);
                let request = self.active_requests[&request_key].request.clone();
                ScanFetchTransition::Complete {
                    request_key,
                    response: error_response(
                        &request,
                        KalanjiyamError::Corruption(format!(
                            "unexpected engine read decision for ScanFetchNext: {other:?}"
                        )),
                    ),
                    continue_scan: true,
                }
            }
            Err(error) => {
                self.reset_active_scan_dispatch(scan_id);
                let request = self.active_requests[&request_key].request.clone();
                ScanFetchTransition::Complete {
                    request_key,
                    response: error_response(&request, error),
                    continue_scan: true,
                }
            }
        }
    }

    // Apply one prepared scan transition after the decision step above has
    // already normalized any immediate response, page, or worker dispatch.
    async fn apply_scan_fetch_transition(&mut self, scan_id: u64, transition: ScanFetchTransition) {
        match transition {
            ScanFetchTransition::Idle => {}
            ScanFetchTransition::DrainQueued { queued, error } => {
                for request_key in queued {
                    let request = self.active_requests[&request_key].request.clone();
                    self.complete_request(
                        request_key,
                        error_response(&request, copy_error(&error)),
                    );
                }
            }
            ScanFetchTransition::Complete {
                request_key,
                response,
                continue_scan,
            } => {
                self.complete_request(request_key, response);
                if continue_scan {
                    Box::pin(self.maybe_start_next_scan_fetch(scan_id, None)).await;
                }
            }
            ScanFetchTransition::ImmediatePage { request_key, page } => {
                self.finish_scan_fetch(scan_id, request_key, page).await;
            }
            ScanFetchTransition::Dispatch {
                task_id,
                task,
                task_state,
            } => {
                self.worker_tasks.insert(task_id, *task_state);
                self.worker_pool.enqueue(WorkerTaskEnvelope {
                    task_id,
                    task: *task,
                });
            }
        }
    }

    fn reset_active_scan_dispatch(&mut self, scan_id: u64) {
        if let Some(session) = self.scan_sessions.get_mut(&scan_id) {
            session.active_fetch_request = None;
            session.in_flight_task_id = None;
        }
    }

    // Publish one completed scan page into session state so EOF cleanup and
    // queued follow-up failures can be tested without driving timer behavior.
    fn publish_scan_fetch_result(
        &mut self,
        scan_id: u64,
        request_key: &RequestKey,
        page: ScanPage,
    ) -> Option<ScanPublication> {
        let cancelled = self
            .active_requests
            .get(request_key)
            .is_some_and(|state| state.cancelled);
        let session = self.scan_sessions.get_mut(&scan_id)?;
        session.active_fetch_request = None;
        session.in_flight_task_id = None;

        if cancelled {
            return Some(ScanPublication {
                primary: error_response(
                    &self.active_requests[request_key].request,
                    KalanjiyamError::Cancelled("request was cancelled".to_string()),
                ),
                queued: Vec::new(),
                release_snapshot: None,
                continue_scan: true,
            });
        }

        let request = self.active_requests[request_key].request.clone();
        if page.eof {
            session.terminal_state = ScanTerminalState::EofReached;
            let queued_keys: Vec<_> = std::mem::take(&mut session.fetch_request_queue).into();
            let snapshot = session.snapshot.clone();
            self.scan_sessions.remove(&scan_id);
            let queued = queued_keys
                .into_iter()
                .filter_map(|queued_key| {
                    self.active_requests.get(&queued_key).map(|queued_request| {
                        (
                            queued_key,
                            error_response(
                                &queued_request.request,
                                KalanjiyamError::InvalidArgument(
                                    "scan session has already reached EOF".to_string(),
                                ),
                            ),
                        )
                    })
                })
                .collect();
            return Some(ScanPublication {
                primary: success_response(
                    &request,
                    ExternalResponsePayload::ScanFetchNext(ScanFetchNextResponse {
                        rows: page.rows,
                        eof: true,
                    }),
                ),
                queued,
                release_snapshot: Some(snapshot),
                continue_scan: false,
            });
        }

        session.resume_after_key = page.next_resume_after_key.clone();
        session.expires_at = Instant::now()
            + Duration::from_millis(self.engine.state().config.maintenance.scan_expiry_ms);
        Some(ScanPublication {
            primary: success_response(
                &request,
                ExternalResponsePayload::ScanFetchNext(ScanFetchNextResponse {
                    rows: page.rows,
                    eof: false,
                }),
            ),
            queued: Vec::new(),
            release_snapshot: None,
            continue_scan: true,
        })
    }

    async fn handle_worker_result(&mut self, worker_result: WorkerResultEnvelope) {
        let Some(task_state) = self.worker_tasks.remove(&worker_result.task_id) else {
            return;
        };
        match self.normalize_worker_completion(worker_result.kind, task_state, worker_result.result)
        {
            NormalizedWorkerCompletion::Request {
                request_key,
                response,
            } => self.complete_request(request_key, response),
            NormalizedWorkerCompletion::ScanPage {
                scan_id,
                request_key,
                page,
            } => self.finish_scan_fetch(scan_id, request_key, page).await,
            NormalizedWorkerCompletion::Maintenance(completion) => {
                self.apply_maintenance_worker_completion(completion)
            }
            NormalizedWorkerCompletion::Ignore => {}
        }
        let _ = self.maybe_dispatch_maintenance();
    }

    // Normalize every worker completion into one typed owner action so request
    // publication and maintenance publication share one validation layer.
    fn normalize_worker_completion(
        &mut self,
        kind: WorkerTaskKind,
        task_state: WorkerTaskState,
        result: Result<WorkerCompletion, KalanjiyamError>,
    ) -> NormalizedWorkerCompletion {
        match kind {
            WorkerTaskKind::Get => self.normalize_get_completion(task_state, result),
            WorkerTaskKind::ScanPage => self.normalize_scan_completion(task_state, result),
            WorkerTaskKind::Flush => NormalizedWorkerCompletion::Maintenance(
                self.normalize_maintenance_completion(task_state, result),
            ),
            WorkerTaskKind::Compact => NormalizedWorkerCompletion::Maintenance(
                self.normalize_maintenance_completion(task_state, result),
            ),
            WorkerTaskKind::Checkpoint => NormalizedWorkerCompletion::Maintenance(
                self.normalize_maintenance_completion(task_state, result),
            ),
            WorkerTaskKind::Gc => NormalizedWorkerCompletion::Maintenance(
                self.normalize_maintenance_completion(task_state, result),
            ),
        }
    }

    fn normalize_get_completion(
        &self,
        task_state: WorkerTaskState,
        result: Result<WorkerCompletion, KalanjiyamError>,
    ) -> NormalizedWorkerCompletion {
        let Some(request_key) = task_state.request_key else {
            return NormalizedWorkerCompletion::Ignore;
        };
        let Some(request_state) = self.active_requests.get(&request_key) else {
            return NormalizedWorkerCompletion::Ignore;
        };
        if request_state.cancelled || task_state.cancelled {
            return NormalizedWorkerCompletion::Request {
                request_key,
                response: error_response(
                    &request_state.request,
                    KalanjiyamError::Cancelled("request was cancelled".to_string()),
                ),
            };
        }
        match (task_state.get_plan.as_ref(), result) {
            (Some(plan), Ok(WorkerCompletion::Get(payload))) => {
                match self.engine.finish_get(plan, payload) {
                    Ok(get) => NormalizedWorkerCompletion::Request {
                        request_key,
                        response: success_response(
                            &request_state.request,
                            ExternalResponsePayload::Get(get),
                        ),
                    },
                    Err(error) => NormalizedWorkerCompletion::Request {
                        request_key,
                        response: error_response(&request_state.request, error),
                    },
                }
            }
            (_, Ok(other)) => NormalizedWorkerCompletion::Request {
                request_key,
                response: error_response(
                    &request_state.request,
                    KalanjiyamError::Corruption(format!(
                        "unexpected worker completion for Get: {other:?}"
                    )),
                ),
            },
            (_, Err(error)) => NormalizedWorkerCompletion::Request {
                request_key,
                response: error_response(&request_state.request, error),
            },
        }
    }

    // Validate scan completions before touching session state so unexpected
    // worker results fail the request instead of mutating the session.
    fn normalize_scan_completion(
        &mut self,
        task_state: WorkerTaskState,
        result: Result<WorkerCompletion, KalanjiyamError>,
    ) -> NormalizedWorkerCompletion {
        let Some(request_key) = task_state.request_key else {
            return NormalizedWorkerCompletion::Ignore;
        };
        let Some(scan_id) = task_state.scan_id else {
            return NormalizedWorkerCompletion::Ignore;
        };
        let Some(request) = self
            .active_requests
            .get(&request_key)
            .map(|request_state| request_state.request.clone())
        else {
            self.reset_active_scan_dispatch(scan_id);
            return NormalizedWorkerCompletion::Ignore;
        };
        match result {
            Ok(WorkerCompletion::ScanPage(page)) => match task_state.scan_plan.as_ref() {
                Some(plan) => match self.engine.finish_scan_page(plan, page) {
                    Ok(validated) => NormalizedWorkerCompletion::ScanPage {
                        scan_id,
                        request_key,
                        page: validated,
                    },
                    Err(error) => {
                        self.reset_active_scan_dispatch(scan_id);
                        NormalizedWorkerCompletion::Request {
                            request_key,
                            response: error_response(&request, error),
                        }
                    }
                },
                None => {
                    self.reset_active_scan_dispatch(scan_id);
                    NormalizedWorkerCompletion::Ignore
                }
            },
            Ok(other) => {
                self.reset_active_scan_dispatch(scan_id);
                NormalizedWorkerCompletion::Request {
                    request_key,
                    response: error_response(
                        &request,
                        KalanjiyamError::Corruption(format!(
                            "unexpected worker completion for ScanFetchNext: {other:?}"
                        )),
                    ),
                }
            }
            Err(error) => {
                self.reset_active_scan_dispatch(scan_id);
                NormalizedWorkerCompletion::Request {
                    request_key,
                    response: error_response(&request, error),
                }
            }
        }
    }

    fn normalize_maintenance_completion(
        &self,
        task_state: WorkerTaskState,
        result: Result<WorkerCompletion, KalanjiyamError>,
    ) -> MaintenanceWorkerCompletion {
        match result {
            Ok(WorkerCompletion::FlushPrepared(prepared)) => task_state
                .flush_plan
                .map(|plan| MaintenanceWorkerCompletion::Flush { plan, prepared })
                .unwrap_or(MaintenanceWorkerCompletion::Ignore),
            Ok(WorkerCompletion::CompactPrepared(prepared)) => task_state
                .compact_plan
                .map(|plan| MaintenanceWorkerCompletion::Compact { plan, prepared })
                .unwrap_or(MaintenanceWorkerCompletion::Ignore),
            Ok(WorkerCompletion::CheckpointPrepared(prepared)) => task_state
                .checkpoint_plan
                .map(|plan| MaintenanceWorkerCompletion::Checkpoint { plan, prepared })
                .unwrap_or(MaintenanceWorkerCompletion::Ignore),
            Ok(WorkerCompletion::GcPrepared(prepared)) => task_state
                .gc_plan
                .map(|plan| MaintenanceWorkerCompletion::Gc { plan, prepared })
                .unwrap_or(MaintenanceWorkerCompletion::Ignore),
            Err(KalanjiyamError::Io(_)) => MaintenanceWorkerCompletion::Retry,
            _ => MaintenanceWorkerCompletion::Ignore,
        }
    }

    // Publish maintenance results separately from worker normalization so retry
    // policy stays consistent across flush, compact, checkpoint, and GC.
    fn apply_maintenance_worker_completion(&mut self, completion: MaintenanceWorkerCompletion) {
        let decision = match completion {
            MaintenanceWorkerCompletion::Flush { plan, prepared } => {
                self.publish_flush_result(&plan, &prepared)
            }
            MaintenanceWorkerCompletion::Compact { plan, prepared } => {
                self.publish_compact_result(&plan, &prepared)
            }
            MaintenanceWorkerCompletion::Checkpoint { plan, prepared } => {
                self.publish_checkpoint_result(&plan, &prepared)
            }
            MaintenanceWorkerCompletion::Gc { plan, prepared } => {
                self.publish_gc_result(&plan, &prepared)
            }
            MaintenanceWorkerCompletion::Retry => MaintenancePublicationDecision::Retry,
            MaintenanceWorkerCompletion::Ignore => MaintenancePublicationDecision::NoChange,
        };
        match decision {
            MaintenancePublicationDecision::NoChange => {}
            MaintenancePublicationDecision::Retry => self.schedule_maintenance_retry(),
            MaintenancePublicationDecision::CheckpointPublished => {
                self.checkpoint_due = false;
                self.gc_due = true;
                self.next_gc_at = Instant::now();
            }
            MaintenancePublicationDecision::GcPublished => self.gc_due = false,
        }
    }

    fn publish_flush_result(
        &mut self,
        plan: &FlushTaskPlan,
        prepared: &FlushTaskResult,
    ) -> MaintenancePublicationDecision {
        match self.engine.publish_flush_task_result(plan, prepared) {
            Ok(_) | Err(KalanjiyamError::Stale(_)) => MaintenancePublicationDecision::NoChange,
            Err(KalanjiyamError::Io(_)) => MaintenancePublicationDecision::Retry,
            Err(_) => MaintenancePublicationDecision::NoChange,
        }
    }

    fn publish_compact_result(
        &mut self,
        plan: &CompactTaskPlan,
        prepared: &CompactTaskResult,
    ) -> MaintenancePublicationDecision {
        match self.engine.publish_compact_task_result(plan, prepared) {
            Ok(_) | Err(KalanjiyamError::Stale(_)) => MaintenancePublicationDecision::NoChange,
            Err(KalanjiyamError::Io(_)) => MaintenancePublicationDecision::Retry,
            Err(_) => MaintenancePublicationDecision::NoChange,
        }
    }

    fn publish_checkpoint_result(
        &mut self,
        plan: &CheckpointTaskPlan,
        prepared: &CheckpointTaskResult,
    ) -> MaintenancePublicationDecision {
        match self.engine.publish_checkpoint_task_result(plan, prepared) {
            Ok(_) => MaintenancePublicationDecision::CheckpointPublished,
            Err(KalanjiyamError::Stale(_)) => MaintenancePublicationDecision::NoChange,
            Err(KalanjiyamError::Io(_)) => MaintenancePublicationDecision::Retry,
            Err(_) => MaintenancePublicationDecision::NoChange,
        }
    }

    fn publish_gc_result(
        &mut self,
        plan: &GcTaskPlan,
        prepared: &GcTaskResult,
    ) -> MaintenancePublicationDecision {
        match self.engine.publish_gc_task_result(plan, prepared) {
            Ok(()) => MaintenancePublicationDecision::GcPublished,
            Err(KalanjiyamError::Io(_)) => MaintenancePublicationDecision::Retry,
            Err(_) => MaintenancePublicationDecision::NoChange,
        }
    }

    async fn handle_wal_result(&mut self, wal_result: WalSyncResult) {
        let Some(batch) = self.wal_batch else {
            return;
        };
        if batch.task_id != wal_result.task_id {
            return;
        }
        self.wal_batch = None;
        for (request_key, outcome) in self.complete_wal_batch(batch, wal_result.result) {
            let Some(request_state) = self.active_requests.get(&request_key) else {
                continue;
            };
            let response = match outcome {
                Ok(payload) => success_response(&request_state.request, payload),
                Err(error) => error_response(&request_state.request, error),
            };
            self.complete_request(request_key, response);
        }
        self.dispatch_wal_sync_if_needed();
    }

    // Compute WAL waiter completions independently from reply delivery so
    // success fanout and failure fanout can be exercised directly in tests.
    fn complete_wal_batch(
        &mut self,
        batch: WalBatchState,
        result: Result<(), KalanjiyamError>,
    ) -> Vec<(RequestKey, Result<ExternalResponsePayload, KalanjiyamError>)> {
        match result {
            Ok(()) => match self.engine.publish_wal_sync_result(batch.target_seqno) {
                Ok(sync_response) => {
                    self.engine.finish_wal_sync_batch(batch.synced_bytes);
                    let satisfied: Vec<_> = self
                        .wal_waiters
                        .iter()
                        .filter_map(|(key, waiter)| {
                            (waiter.target_seqno <= sync_response.durable_seqno)
                                .then_some(key.clone())
                        })
                        .collect();
                    satisfied
                        .into_iter()
                        .filter_map(|key| {
                            self.wal_waiters.remove(&key).map(|waiter| {
                                (
                                    key,
                                    Ok(wal_success_payload(
                                        waiter.success_kind,
                                        Some(sync_response.clone()),
                                    )),
                                )
                            })
                        })
                        .collect()
                }
                Err(error) => self.fail_all_waiting_syncs(error),
            },
            Err(KalanjiyamError::Io(source)) => {
                let error = self.engine.fail_wal_sync("WAL sync thread", source);
                self.fail_all_waiting_syncs(error)
            }
            Err(error) => self.fail_all_waiting_syncs(error),
        }
    }

    fn fail_all_waiting_syncs(
        &mut self,
        error: KalanjiyamError,
    ) -> Vec<(RequestKey, Result<ExternalResponsePayload, KalanjiyamError>)> {
        let waiting: Vec<_> = self.wal_waiters.keys().cloned().collect();
        self.wal_waiters.clear();
        waiting
            .into_iter()
            .map(|key| (key, Err(copy_error(&error))))
            .collect()
    }

    fn register_wal_waiter(&mut self, request_key: RequestKey, waiter: WalWaiterState) {
        self.wal_waiters.insert(request_key, waiter);
    }

    fn wal_sync_batch_ready(&self) -> bool {
        if self.wal_waiters.is_empty() {
            return false;
        }
        // The spec-defined batching rule is "bytes threshold OR oldest waiter
        // age", so the owner waits until one of those conditions becomes true
        // before it dispatches a sync request.
        if self.engine.unsynced_wal_bytes() >= self.engine.state().config.wal.group_commit_bytes {
            return true;
        }
        let Some(oldest_waiter) = self
            .wal_waiters
            .values()
            .map(|waiter| waiter.created_at)
            .min()
        else {
            return false;
        };
        Instant::now()
            >= oldest_waiter
                + Duration::from_millis(self.engine.state().config.wal.group_commit_max_delay_ms)
    }

    fn dispatch_wal_sync_if_needed(&mut self) {
        if self.wal_batch.is_some() || !self.wal_sync_batch_ready() {
            return;
        }
        let target_seqno = self
            .wal_waiters
            .values()
            .map(|waiter| waiter.target_seqno)
            .max()
            .unwrap_or_default();
        let task_id = self.allocate_task_id();
        self.wal_batch = Some(WalBatchState {
            task_id,
            target_seqno,
            synced_bytes: self.engine.unsynced_wal_bytes(),
        });
        self.wal_service.enqueue(WalSyncRequest {
            task_id,
            path: self.engine.active_wal_path().to_path_buf(),
        });
    }

    fn maybe_dispatch_maintenance(&mut self) -> Result<(), KalanjiyamError> {
        if self.maintenance_active(MaintenanceTaskKind::Flush) {
            return Ok(());
        }
        if let Some(plan) = self.engine.capture_flush_plan()? {
            if self.worker_tasks.len() >= self.engine.state().config.maintenance.max_worker_tasks {
                self.schedule_maintenance_retry();
                return Ok(());
            }
            let task_id = self.allocate_task_id();
            let store_dir = self.engine.state().store_dir.clone();
            let page_size_bytes = self.engine.state().config.engine.page_size_bytes;
            self.worker_tasks.insert(
                task_id,
                WorkerTaskState {
                    request_key: None,
                    scan_id: None,
                    maintenance: Some(MaintenanceTaskKind::Flush),
                    get_plan: None,
                    scan_plan: None,
                    flush_plan: Some(plan.clone()),
                    compact_plan: None,
                    checkpoint_plan: None,
                    checkpoint_generation: None,
                    gc_plan: None,
                    cancelled: false,
                },
            );
            self.worker_pool.enqueue(WorkerTaskEnvelope {
                task_id,
                task: WorkerTask::Flush {
                    store_dir,
                    page_size_bytes,
                    plan,
                },
            });
            return Ok(());
        }
        if self.maintenance_active(MaintenanceTaskKind::Compact) {
            return Ok(());
        }
        if let Some(plan) = self.engine.capture_compact_plan()? {
            if self.worker_tasks.len() >= self.engine.state().config.maintenance.max_worker_tasks {
                self.schedule_maintenance_retry();
                return Ok(());
            }
            let task_id = self.allocate_task_id();
            let store_dir = self.engine.state().store_dir.clone();
            let page_size_bytes = self.engine.state().config.engine.page_size_bytes;
            self.worker_tasks.insert(
                task_id,
                WorkerTaskState {
                    request_key: None,
                    scan_id: None,
                    maintenance: Some(MaintenanceTaskKind::Compact),
                    get_plan: None,
                    scan_plan: None,
                    flush_plan: None,
                    compact_plan: Some(plan.clone()),
                    checkpoint_plan: None,
                    checkpoint_generation: None,
                    gc_plan: None,
                    cancelled: false,
                },
            );
            self.worker_pool.enqueue(WorkerTaskEnvelope {
                task_id,
                task: WorkerTask::Compact {
                    store_dir,
                    page_size_bytes,
                    plan,
                },
            });
            return Ok(());
        }
        if self.maintenance_active(MaintenanceTaskKind::Checkpoint) {
            return Ok(());
        }
        if self.checkpoint_due {
            if !self
                .engine
                .state()
                .current_manifest
                .active_memtable_ref
                .is_empty()
            {
                let _ = self.engine.force_freeze()?;
            }
            if let Some(plan) = self.engine.capture_flush_plan()? {
                if self.worker_tasks.len()
                    >= self.engine.state().config.maintenance.max_worker_tasks
                {
                    self.schedule_maintenance_retry();
                    return Ok(());
                }
                let task_id = self.allocate_task_id();
                let store_dir = self.engine.state().store_dir.clone();
                let page_size_bytes = self.engine.state().config.engine.page_size_bytes;
                self.worker_tasks.insert(
                    task_id,
                    WorkerTaskState {
                        request_key: None,
                        scan_id: None,
                        maintenance: Some(MaintenanceTaskKind::Flush),
                        get_plan: None,
                        scan_plan: None,
                        flush_plan: Some(plan.clone()),
                        compact_plan: None,
                        checkpoint_plan: None,
                        checkpoint_generation: None,
                        gc_plan: None,
                        cancelled: false,
                    },
                );
                self.worker_pool.enqueue(WorkerTaskEnvelope {
                    task_id,
                    task: WorkerTask::Flush {
                        store_dir,
                        page_size_bytes,
                        plan,
                    },
                });
                return Ok(());
            }
            let plan = self.engine.capture_checkpoint_plan()?;
            if self.worker_tasks.len() >= self.engine.state().config.maintenance.max_worker_tasks {
                self.schedule_maintenance_retry();
                return Ok(());
            }
            let task_id = self.allocate_task_id();
            let store_dir = self.engine.state().store_dir.clone();
            let page_size_bytes = self.engine.state().config.engine.page_size_bytes;
            let checkpoint_generation = self.engine.state().next_checkpoint_generation;
            self.worker_tasks.insert(
                task_id,
                WorkerTaskState {
                    request_key: None,
                    scan_id: None,
                    maintenance: Some(MaintenanceTaskKind::Checkpoint),
                    get_plan: None,
                    scan_plan: None,
                    flush_plan: None,
                    compact_plan: None,
                    checkpoint_plan: Some(plan.clone()),
                    checkpoint_generation: Some(checkpoint_generation),
                    gc_plan: None,
                    cancelled: false,
                },
            );
            self.worker_pool.enqueue(WorkerTaskEnvelope {
                task_id,
                task: WorkerTask::Checkpoint {
                    store_dir,
                    page_size_bytes,
                    checkpoint_generation,
                    plan,
                },
            });
            return Ok(());
        }
        if self.maintenance_active(MaintenanceTaskKind::Gc) {
            return Ok(());
        }
        if self.gc_due {
            let owner_pins = self.current_gc_owner_pins();
            if let Some(plan) = self.engine.capture_gc_task(&owner_pins)? {
                if self.worker_tasks.len()
                    >= self.engine.state().config.maintenance.max_worker_tasks
                {
                    self.schedule_maintenance_retry();
                    return Ok(());
                }
                let task_id = self.allocate_task_id();
                self.worker_tasks.insert(
                    task_id,
                    WorkerTaskState {
                        request_key: None,
                        scan_id: None,
                        maintenance: Some(MaintenanceTaskKind::Gc),
                        get_plan: None,
                        scan_plan: None,
                        flush_plan: None,
                        compact_plan: None,
                        checkpoint_plan: None,
                        checkpoint_generation: None,
                        gc_plan: Some(plan.clone()),
                        cancelled: false,
                    },
                );
                self.worker_pool.enqueue(WorkerTaskEnvelope {
                    task_id,
                    task: WorkerTask::Gc { plan },
                });
                return Ok(());
            }
            self.gc_due = false;
        }
        Ok(())
    }

    fn schedule_maintenance_retry(&mut self) {
        self.maintenance_retry_at = Some(
            Instant::now()
                + Duration::from_millis(self.engine.state().config.maintenance.retry_delay_ms),
        );
    }

    async fn handle_timers(&mut self) {
        let now = Instant::now();
        let mut maintenance_became_due = false;
        if self.next_checkpoint_at <= now {
            self.checkpoint_due = true;
            self.next_checkpoint_at = now
                + Duration::from_secs(
                    self.engine
                        .state()
                        .config
                        .maintenance
                        .checkpoint_interval_secs,
                );
            maintenance_became_due = true;
        }
        if self.next_gc_at <= now {
            self.gc_due = true;
            self.next_gc_at =
                now + Duration::from_secs(self.engine.state().config.maintenance.gc_interval_secs);
            maintenance_became_due = true;
        }
        if self
            .maintenance_retry_at
            .is_some_and(|deadline| deadline <= now)
        {
            self.maintenance_retry_at = None;
            maintenance_became_due = true;
        }
        if maintenance_became_due {
            let _ = self.maybe_dispatch_maintenance();
        }
        self.dispatch_wal_sync_if_needed();
        let expired: Vec<_> = self
            .scan_sessions
            .iter()
            .filter_map(|(scan_id, session)| {
                if session.expires_at <= now {
                    Some(*scan_id)
                } else {
                    None
                }
            })
            .collect();
        for scan_id in expired {
            self.expire_scan_session(scan_id).await;
        }
    }

    fn maintenance_active(&self, kind: MaintenanceTaskKind) -> bool {
        self.worker_tasks
            .values()
            .any(|task| task.maintenance == Some(kind))
    }

    fn current_gc_owner_pins(&self) -> GcOwnerPins {
        let mut pins = GcOwnerPins::default();
        for task in self.worker_tasks.values() {
            if let Some(plan) = task.get_plan.as_ref() {
                pins.manifest_generations.insert(plan.data_generation);
                pins.data_file_ids
                    .extend(plan.candidate_files.iter().map(|meta| meta.file_id));
            }
            if let Some(plan) = task.scan_plan.as_ref() {
                pins.manifest_generations
                    .insert(plan.snapshot_handle.data_generation);
                pins.data_file_ids
                    .extend(plan.candidate_files.iter().map(|meta| meta.file_id));
            }
            if let Some(plan) = task.flush_plan.as_ref() {
                pins.manifest_generations.insert(plan.source_generation);
                pins.data_file_ids
                    .extend(plan.output_file_ids.iter().copied());
            }
            if let Some(plan) = task.compact_plan.as_ref() {
                pins.manifest_generations.insert(plan.source_generation);
                pins.data_file_ids
                    .extend(plan.input_file_ids.iter().copied());
                pins.data_file_ids
                    .extend(plan.output_file_ids.iter().copied());
            }
            if let Some(plan) = task.checkpoint_plan.as_ref() {
                pins.manifest_generations.insert(plan.manifest_generation);
            }
            if let Some(checkpoint_generation) = task.checkpoint_generation {
                pins.checkpoint_generations.insert(checkpoint_generation);
            }
        }
        pins
    }

    async fn expire_scan_session(&mut self, scan_id: u64) {
        let Some(mut session) = self.scan_sessions.remove(&scan_id) else {
            return;
        };
        session.terminal_state = ScanTerminalState::Expired;
        if let Some(active_key) = session.active_fetch_request.take() {
            if let Some(request_state) = self.active_requests.get_mut(&active_key) {
                request_state.cancelled = true;
            }
            self.complete_request(
                active_key.clone(),
                error_response(
                    &self.active_requests[&active_key].request,
                    KalanjiyamError::Cancelled("scan session expired".to_string()),
                ),
            );
        }
        while let Some(queued_key) = session.fetch_request_queue.pop_front() {
            if let Some(request_state) = self.active_requests.get(&queued_key) {
                self.complete_request(
                    queued_key.clone(),
                    error_response(
                        &request_state.request,
                        KalanjiyamError::Cancelled("scan session expired".to_string()),
                    ),
                );
            }
        }
        let _ = self.engine.release_snapshot(&session.snapshot).await;
    }

    fn cancel_request(
        &mut self,
        request_key: &RequestKey,
        message: &str,
    ) -> Result<(), KalanjiyamError> {
        if let Some(request_state) = self.active_requests.get_mut(request_key) {
            match request_state.stage {
                RequestStage::ScanQueued { scan_id } => {
                    if let Some(session) = self.scan_sessions.get_mut(&scan_id) {
                        session
                            .fetch_request_queue
                            .retain(|queued| queued != request_key);
                    }
                    let request = request_state.request.clone();
                    self.complete_request(
                        request_key.clone(),
                        error_response(&request, KalanjiyamError::Cancelled(message.to_string())),
                    );
                }
                RequestStage::WaitingWal { .. } => {
                    let Some(waiter) = self.wal_waiters.get(request_key) else {
                        return Ok(());
                    };
                    // `Put` and `Delete` waiters have already crossed the
                    // append-visible point, so cancellation may not tear down
                    // the durability wait that completes their acknowledgement.
                    if waiter.success_kind == WalSuccessPayloadKind::Sync {
                        self.wal_waiters.remove(request_key);
                        let request = request_state.request.clone();
                        self.complete_request(
                            request_key.clone(),
                            error_response(
                                &request,
                                KalanjiyamError::Cancelled(message.to_string()),
                            ),
                        );
                    }
                }
                RequestStage::WaitingWorker { task_id } => {
                    request_state.cancelled = true;
                    if let Some(task_state) = self.worker_tasks.get_mut(&task_id) {
                        task_state.cancelled = true;
                    }
                }
                RequestStage::ScanActive {
                    task_id: Some(task_id),
                    ..
                } => {
                    request_state.cancelled = true;
                    if let Some(task_state) = self.worker_tasks.get_mut(&task_id) {
                        task_state.cancelled = true;
                    }
                }
                RequestStage::OwnerProcessing | RequestStage::ScanActive { task_id: None, .. } => {
                    request_state.cancelled = true;
                }
            }
            return Ok(());
        }
        self.pre_admission_cancellations
            .entry(request_key.client_id.clone())
            .or_default()
            .insert(request_key.request_id);
        Ok(())
    }

    fn disconnect_client(&mut self, client_id: &str) -> Result<(), KalanjiyamError> {
        self.disconnected_clients.insert(client_id.to_string());
        let keys: Vec<_> = self
            .active_requests
            .keys()
            .filter(|key| key.client_id == client_id)
            .cloned()
            .collect();
        for key in keys {
            self.cancel_request(&key, "client has been disconnected")?;
        }
        Ok(())
    }

    async fn begin_shutdown(&mut self) -> Result<(), KalanjiyamError> {
        while let Ok(call) = self.external_rx.try_recv() {
            let _ = call.reply.send(lifecycle_error_response(
                &call.request,
                LifecycleState::Stopping,
            ));
        }

        let keys: Vec<_> = self.active_requests.keys().cloned().collect();
        for key in keys {
            if let Some(request_state) = self.active_requests.get(&key) {
                let response = error_response(
                    &request_state.request,
                    KalanjiyamError::Cancelled("server is shutting down".to_string()),
                );
                self.complete_request(key.clone(), response);
            }
        }
        self.wal_waiters.clear();
        self.worker_tasks.clear();
        self.wal_batch = None;

        let scan_ids: Vec<_> = self.scan_sessions.keys().copied().collect();
        for scan_id in scan_ids {
            self.expire_scan_session(scan_id).await;
        }
        Ok(())
    }

    async fn finish_shutdown(&mut self) {
        self.worker_pool.shutdown();
        self.wal_service.shutdown();
        self.lifecycle
            .store(LifecycleState::Stopped.as_u8(), Ordering::SeqCst);
        self.stopped.notify_waiters();
    }

    fn allocate_task_id(&mut self) -> u64 {
        let task_id = self.next_task_id;
        self.next_task_id += 1;
        task_id
    }

    fn complete_request(&mut self, request_key: RequestKey, response: ExternalResponse) {
        let Some(request_state) = self.active_requests.remove(&request_key) else {
            return;
        };
        let client_id = request_key.client_id.clone();
        let client_state = self
            .expected_request_ids
            .get_mut(&client_id)
            .expect("admitted request must have one client state");
        client_state
            .buffered_responses
            .insert(request_key.request_id, (request_state.reply, response));
        self.flush_client_responses(&client_id);
    }

    fn flush_client_responses(&mut self, client_id: &str) {
        let Some(client_state) = self.expected_request_ids.get_mut(client_id) else {
            return;
        };
        while let Some((reply, response)) = client_state
            .buffered_responses
            .remove(&client_state.next_response_id)
        {
            let _ = reply.send(response);
            client_state.next_response_id += 1;
        }
    }
}

fn wal_success_payload(
    kind: WalSuccessPayloadKind,
    sync: Option<crate::pezhai::types::SyncResponse>,
) -> ExternalResponsePayload {
    match kind {
        WalSuccessPayloadKind::Put => ExternalResponsePayload::Put,
        WalSuccessPayloadKind::Delete => ExternalResponsePayload::Delete,
        WalSuccessPayloadKind::Sync => {
            ExternalResponsePayload::Sync(sync.expect("sync waiter must include sync payload"))
        }
    }
}

fn copy_error(error: &KalanjiyamError) -> KalanjiyamError {
    match error {
        KalanjiyamError::Busy(message) => KalanjiyamError::Busy(message.clone()),
        KalanjiyamError::InvalidArgument(message) => {
            KalanjiyamError::InvalidArgument(message.clone())
        }
        KalanjiyamError::Io(source) => {
            KalanjiyamError::Io(std::io::Error::new(source.kind(), source.to_string()))
        }
        KalanjiyamError::Checksum(message) => KalanjiyamError::Checksum(message.clone()),
        KalanjiyamError::Corruption(message) => KalanjiyamError::Corruption(message.clone()),
        KalanjiyamError::Stale(message) => KalanjiyamError::Stale(message.clone()),
        KalanjiyamError::Cancelled(message) => KalanjiyamError::Cancelled(message.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use tempfile::TempDir;

    fn run_async<T>(future: impl Future<Output = T>) -> T {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("test runtime should build");
        runtime.block_on(future)
    }

    fn create_store() -> (TempDir, PathBuf) {
        let tempdir = tempfile::tempdir().expect("tempdir should be creatable for owner tests");
        let store_dir = tempdir.path().join("store");
        std::fs::create_dir_all(store_dir.join("wal"))
            .expect("wal dir should be creatable for owner tests");
        std::fs::create_dir_all(store_dir.join("meta"))
            .expect("meta dir should be creatable for owner tests");
        std::fs::create_dir_all(store_dir.join("data"))
            .expect("data dir should be creatable for owner tests");
        let config_path = store_dir.join("config.toml");
        std::fs::write(
            &config_path,
            r#"
[engine]
sync_mode = "manual"
page_size_bytes = 4096

[wal]
segment_bytes = 1073741824
group_commit_bytes = 65536
group_commit_max_delay_ms = 50

[lsm]
memtable_flush_bytes = 16384
base_level_bytes = 268435456
level_fanout = 10
l0_file_threshold = 8
max_levels = 7

[maintenance]
max_external_requests = 64
max_scan_fetch_queue = 2
max_scan_sessions = 32
max_worker_tasks = 64
max_waiting_wal_syncs = 64
scan_expiry_ms = 30000
retry_delay_ms = 25
worker_threads = 1
gc_interval_secs = 300
checkpoint_interval_secs = 300
"#,
        )
        .expect("config fixture should be writable");
        (tempdir, config_path)
    }

    fn request(client_id: &str, request_id: u64, method: ExternalMethod) -> ExternalRequest {
        ExternalRequest {
            client_id: client_id.to_string(),
            request_id,
            method,
            cancel_token: None,
        }
    }

    fn owner_runtime() -> (TempDir, OwnerRuntime) {
        let (tempdir, config_path) = create_store();
        let engine = run_async(PezhaiEngine::open(&config_path)).expect("engine should open");
        let (_external_tx, external_rx) = mpsc::channel(8);
        let (_control_tx, control_rx) = mpsc::channel(8);
        let (worker_result_tx, worker_result_rx) = mpsc::channel(8);
        let (wal_result_tx, wal_result_rx) = mpsc::channel(8);
        let stopped = Arc::new(Notify::new());
        let lifecycle = Arc::new(AtomicU8::new(LifecycleState::Ready.as_u8()));
        let drop_shutdown_requested = Arc::new(AtomicBool::new(false));
        let drop_shutdown_notify = Arc::new(Notify::new());
        (
            tempdir,
            OwnerRuntime::new(
                engine,
                external_rx,
                control_rx,
                worker_result_rx,
                wal_result_rx,
                worker_result_tx,
                wal_result_tx,
                stopped,
                lifecycle,
                drop_shutdown_requested,
                drop_shutdown_notify,
            ),
        )
    }

    fn insert_active_request(runtime: &mut OwnerRuntime, request: ExternalRequest) -> RequestKey {
        let request_key = RequestKey::from_request(&request);
        runtime
            .expected_request_ids
            .entry(request.client_id.clone())
            .or_insert_with(|| ClientState {
                next_request_id: request.request_id + 1,
                next_response_id: request.request_id,
                buffered_responses: BTreeMap::new(),
            });
        let (reply, _rx) = oneshot::channel();
        runtime.active_requests.insert(
            request_key.clone(),
            ExternalRequestState {
                request,
                reply,
                stage: RequestStage::OwnerProcessing,
                cancelled: false,
            },
        );
        request_key
    }

    #[test]
    fn pre_admission_rejection_covers_lifecycle_disconnect_cancellation_and_request_ids() {
        let (_tempdir, mut runtime) = owner_runtime();
        runtime
            .lifecycle
            .store(LifecycleState::Booting.as_u8(), Ordering::SeqCst);
        let first_request = request("client-a", 1, ExternalMethod::Stats);
        assert_eq!(
            runtime
                .pre_admission_external_rejection(&first_request)
                .expect("booting state should reject")
                .status
                .code,
            crate::sevai::StatusCode::Io
        );

        runtime
            .lifecycle
            .store(LifecycleState::Ready.as_u8(), Ordering::SeqCst);
        runtime.disconnected_clients.insert("client-a".to_string());
        assert!(matches!(
            runtime.pre_admission_external_rejection(&first_request),
            Some(response) if response.status.code == crate::sevai::StatusCode::Cancelled
        ));

        runtime.disconnected_clients.clear();
        runtime
            .pre_admission_cancellations
            .entry("client-a".to_string())
            .or_default()
            .insert(1);
        assert!(matches!(
            runtime.pre_admission_external_rejection(&first_request),
            Some(response) if response.status.code == crate::sevai::StatusCode::Cancelled
        ));

        runtime.pre_admission_cancellations.clear();
        assert!(
            runtime
                .pre_admission_external_rejection(&first_request)
                .is_none()
        );
        let wrong_request = request("client-a", 3, ExternalMethod::Stats);
        assert!(matches!(
            runtime.pre_admission_external_rejection(&wrong_request),
            Some(response) if response.status.code == crate::sevai::StatusCode::InvalidArgument
        ));

        run_async(runtime.finish_shutdown());
    }

    #[test]
    fn get_dispatch_decision_rejects_full_queue_and_builds_worker_dispatch() {
        let (_tempdir, mut runtime) = owner_runtime();
        let request = request(
            "reader",
            1,
            ExternalMethod::Get {
                key: b"yak".to_vec(),
            },
        );
        let request_key = insert_active_request(&mut runtime, request);
        runtime
            .engine
            .decide_put(b"yak".to_vec(), vec![b'v'; 2048])
            .expect("put should succeed for owner test");
        runtime
            .engine
            .force_freeze()
            .expect("freeze should succeed for owner test");

        for task_id in 0..runtime.engine.state().config.maintenance.max_worker_tasks as u64 {
            runtime.worker_tasks.insert(
                task_id,
                WorkerTaskState {
                    request_key: None,
                    scan_id: None,
                    maintenance: None,
                    get_plan: None,
                    scan_plan: None,
                    flush_plan: None,
                    compact_plan: None,
                    checkpoint_plan: None,
                    checkpoint_generation: None,
                    gc_plan: None,
                    cancelled: false,
                },
            );
        }
        assert!(matches!(
            runtime.get_dispatch_decision(&request_key, b"yak"),
            GetDispatchDecision::Reject(KalanjiyamError::Busy(_))
        ));

        runtime.worker_tasks.clear();
        assert!(matches!(
            runtime.get_dispatch_decision(&request_key, b"yak"),
            GetDispatchDecision::Dispatch { .. }
        ));

        run_async(runtime.finish_shutdown());
    }

    #[test]
    fn build_scan_fetch_transition_rejects_when_per_scan_queue_is_full() {
        let (_tempdir, mut runtime) = owner_runtime();
        let snapshot = run_async(runtime.engine.create_snapshot())
            .expect("snapshot should be creatable for owner test");
        let scan_id = 41;
        let active_request = request("reader-a", 1, ExternalMethod::ScanFetchNext { scan_id });
        let queued_request_one = request("reader-b", 1, ExternalMethod::ScanFetchNext { scan_id });
        let queued_request_two = request("reader-c", 1, ExternalMethod::ScanFetchNext { scan_id });
        let rejected_request = request("reader-d", 1, ExternalMethod::ScanFetchNext { scan_id });
        let active_key = insert_active_request(&mut runtime, active_request);
        let queued_key_one = insert_active_request(&mut runtime, queued_request_one);
        let queued_key_two = insert_active_request(&mut runtime, queued_request_two);
        let rejected_key = insert_active_request(&mut runtime, rejected_request);
        runtime.scan_sessions.insert(
            scan_id,
            ScanSessionState {
                snapshot,
                range: crate::pezhai::types::KeyRange::default(),
                max_records_per_page: 1,
                max_bytes_per_page: 1024,
                resume_after_key: None,
                fetch_request_queue: VecDeque::from([queued_key_one, queued_key_two]),
                active_fetch_request: Some(active_key),
                in_flight_task_id: Some(9),
                terminal_state: ScanTerminalState::Open,
                expires_at: Instant::now() + Duration::from_secs(1),
            },
        );

        let transition = runtime.build_scan_fetch_transition(scan_id, Some(&rejected_key), None);
        assert!(matches!(
            transition,
            ScanFetchTransition::Complete {
                request_key,
                response,
                continue_scan: false,
            } if request_key == rejected_key
                && response.status.code == crate::sevai::StatusCode::Busy
        ));
        let session = runtime
            .scan_sessions
            .get(&scan_id)
            .expect("scan session should remain after rejecting one fetch");
        assert_eq!(session.fetch_request_queue.len(), 2);

        run_async(runtime.finish_shutdown());
    }

    #[test]
    fn publish_scan_fetch_result_handles_eof_cleanup_and_queued_rejections() {
        let (_tempdir, mut runtime) = owner_runtime();
        let snapshot = run_async(runtime.engine.create_snapshot())
            .expect("snapshot should be creatable for owner test");
        let scan_id = 41;
        let first_request = request("reader-a", 1, ExternalMethod::ScanFetchNext { scan_id });
        let second_request = request("reader-b", 1, ExternalMethod::ScanFetchNext { scan_id });
        let first_key = insert_active_request(&mut runtime, first_request);
        let second_key = insert_active_request(&mut runtime, second_request);
        runtime.scan_sessions.insert(
            scan_id,
            ScanSessionState {
                snapshot: snapshot.clone(),
                range: crate::pezhai::types::KeyRange::default(),
                max_records_per_page: 1,
                max_bytes_per_page: 1024,
                resume_after_key: None,
                fetch_request_queue: VecDeque::from([second_key.clone()]),
                active_fetch_request: Some(first_key.clone()),
                in_flight_task_id: Some(9),
                terminal_state: ScanTerminalState::Open,
                expires_at: Instant::now() + Duration::from_secs(1),
            },
        );

        let publication = runtime
            .publish_scan_fetch_result(
                scan_id,
                &first_key,
                ScanPage {
                    rows: vec![crate::pezhai::types::ScanRow {
                        key: b"ant".to_vec(),
                        value: b"value".to_vec(),
                    }],
                    eof: true,
                    next_resume_after_key: None,
                },
            )
            .expect("scan publication should exist");

        assert!(!runtime.scan_sessions.contains_key(&scan_id));
        assert!(publication.release_snapshot.is_some());
        assert_eq!(publication.queued.len(), 1);
        assert_eq!(
            publication.primary.status.code,
            crate::sevai::StatusCode::Ok
        );
        assert_eq!(
            publication.queued[0].1.status.code,
            crate::sevai::StatusCode::InvalidArgument
        );

        run_async(runtime.finish_shutdown());
    }

    #[test]
    fn complete_wal_batch_returns_satisfied_waiters_and_failure_fanout() {
        let (_tempdir, mut runtime) = owner_runtime();
        runtime
            .engine
            .decide_put(b"ant".to_vec(), b"value-one".to_vec())
            .expect("first put should succeed");
        runtime
            .engine
            .decide_put(b"bee".to_vec(), b"value-two".to_vec())
            .expect("second put should succeed");

        runtime.register_wal_waiter(
            RequestKey {
                client_id: "writer".to_string(),
                request_id: 1,
            },
            WalWaiterState {
                target_seqno: 1,
                success_kind: WalSuccessPayloadKind::Put,
                created_at: Instant::now(),
            },
        );
        runtime.register_wal_waiter(
            RequestKey {
                client_id: "syncer".to_string(),
                request_id: 1,
            },
            WalWaiterState {
                target_seqno: 2,
                success_kind: WalSuccessPayloadKind::Sync,
                created_at: Instant::now(),
            },
        );

        let completions = runtime.complete_wal_batch(
            WalBatchState {
                task_id: 7,
                target_seqno: 2,
                synced_bytes: runtime.engine.unsynced_wal_bytes(),
            },
            Ok(()),
        );
        assert_eq!(completions.len(), 2);
        assert!(runtime.wal_waiters.is_empty());

        runtime.register_wal_waiter(
            RequestKey {
                client_id: "writer".to_string(),
                request_id: 2,
            },
            WalWaiterState {
                target_seqno: 2,
                success_kind: WalSuccessPayloadKind::Put,
                created_at: Instant::now(),
            },
        );
        let failures = runtime.complete_wal_batch(
            WalBatchState {
                task_id: 8,
                target_seqno: 2,
                synced_bytes: 0,
            },
            Err(KalanjiyamError::Cancelled("wal failed".to_string())),
        );
        assert!(matches!(failures[0].1, Err(KalanjiyamError::Cancelled(_))));

        run_async(runtime.finish_shutdown());
    }

    #[test]
    fn last_handle_drop_signal_stops_the_owner_runtime() {
        let (_tempdir, mut runtime) = owner_runtime();
        let drop_shutdown_requested = Arc::clone(&runtime.drop_shutdown_requested);
        let drop_shutdown_notify = Arc::clone(&runtime.drop_shutdown_notify);
        let lifecycle = Arc::clone(&runtime.lifecycle);

        run_async(async move {
            tokio::spawn(async move {
                tokio::task::yield_now().await;
                drop_shutdown_requested.store(true, Ordering::SeqCst);
                drop_shutdown_notify.notify_one();
            });

            runtime.run().await;
        });

        assert_eq!(
            LifecycleState::from_u8(lifecycle.load(Ordering::SeqCst)),
            LifecycleState::Stopped
        );
    }

    #[test]
    fn pre_set_last_handle_drop_flag_stops_the_owner_runtime() {
        let (_tempdir, mut runtime) = owner_runtime();
        runtime
            .drop_shutdown_requested
            .store(true, Ordering::SeqCst);

        run_async(runtime.run());

        assert_eq!(
            LifecycleState::from_u8(runtime.lifecycle.load(Ordering::SeqCst)),
            LifecycleState::Stopped
        );
    }
}
