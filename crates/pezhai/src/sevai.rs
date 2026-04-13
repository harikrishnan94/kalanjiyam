//! Transport-agnostic `PezhaiServer` runtime for the sevai control plane.

use std::collections::{BTreeMap, VecDeque};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio::sync::{Notify, mpsc, oneshot};

use crate::config::{RuntimeConfig, load_runtime_config};
use crate::error::Error;
use crate::pathivu::{MAX_KEY_BYTES, MAX_VALUE_BYTES};

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
            let _ = self.shared.command_tx.send(OwnerCommand::Shutdown);
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

impl PezhaiServer {
    /// Starts the owner task, parses the shared config, and returns a handle.
    pub async fn start(args: ServerBootstrapArgs) -> Result<Self, Error> {
        Self::start_with_options(args, OwnerRuntimeOptions::default()).await
    }

    /// Submits one logical request and waits for the terminal response.
    pub async fn call(&self, request: ExternalRequest) -> Result<ExternalResponse, Error> {
        let response_rx = self.dispatch_request(request)?;
        response_rx
            .await
            .map_err(|_| Error::ServerUnavailable("owner task dropped the response channel".into()))
    }

    /// Attempts to cancel one queued or pending request by transport token.
    pub async fn cancel(&self, client_id: String, cancel_token: String) -> Result<bool, Error> {
        let (reply_tx, reply_rx) = oneshot::channel();

        self.shared
            .command_tx
            .send(OwnerCommand::Cancel {
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
        self.shared
            .command_tx
            .send(OwnerCommand::Shutdown)
            .map_err(|_| Error::ServerUnavailable("owner task is already stopped".into()))
    }

    /// Waits until the owner task has finished shutdown and all registered runtime tasks have
    /// unwound.
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

    /// Registers one externally owned runtime task that must finish before `wait_stopped`
    /// resolves.
    ///
    /// Transport adapters use this guard to make their task lifetime visible to the logical
    /// server runtime without moving transport code back into `pezhai`.
    pub fn register_runtime_task(&self) -> ServerRuntimeTaskGuard {
        self.shared.register_runtime_task()
    }

    async fn start_with_options(
        args: ServerBootstrapArgs,
        options: OwnerRuntimeOptions,
    ) -> Result<Self, Error> {
        let config = load_runtime_config(&args.config_path).map_err(Error::from)?;
        let (command_tx, command_rx) = mpsc::unbounded_channel();
        let shared = Arc::new(SharedServerHandle::new(
            command_tx.clone(),
            config.sevai.listen_addr,
        ));

        let owner = OwnerState::new(args, config, command_tx, options);
        let join_handle = tokio::spawn(async move { owner.run(command_rx).await });
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

    fn dispatch_request(
        &self,
        request: ExternalRequest,
    ) -> Result<oneshot::Receiver<ExternalResponse>, Error> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.shared
            .command_tx
            .send(OwnerCommand::Call {
                request,
                reply: reply_tx,
            })
            .map_err(|_| {
                Error::ServerUnavailable("owner task is no longer accepting work".into())
            })?;
        Ok(reply_rx)
    }

    /// Waits until the owner task has stopped, without waiting for externally registered runtime
    /// tasks.
    ///
    /// Transport adapters use this signal to stop accepting new work before they drop their own
    /// runtime-task guards.
    pub async fn wait_owner_stopped(&self) -> Result<(), Error> {
        loop {
            if let Some(result) = self.shared.owner_completion_result() {
                return result;
            }

            self.shared.stopped.notified().await;
        }
    }
}

struct SharedServerHandle {
    command_tx: mpsc::UnboundedSender<OwnerCommand>,
    completion_state: Mutex<CompletionState>,
    stopped: Notify,
    listen_addr: SocketAddr,
}

impl SharedServerHandle {
    fn new(command_tx: mpsc::UnboundedSender<OwnerCommand>, listen_addr: SocketAddr) -> Self {
        Self {
            command_tx,
            completion_state: Mutex::new(CompletionState::default()),
            stopped: Notify::new(),
            listen_addr,
        }
    }

    fn store_completion_result(&self, result: Result<(), Error>) {
        let stored = result.map_err(|error| error.to_string());
        self.completion_state
            .lock()
            .expect("completion mutex poisoned")
            .owner_result = Some(stored);
        self.stopped.notify_waiters();
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

#[derive(Clone, Default)]
struct CompletionState {
    owner_result: Option<Result<(), String>>,
    active_runtime_tasks: usize,
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

#[derive(Clone, Copy, Default)]
struct OwnerRuntimeOptions {
    scan_fetch_delay: Duration,
}

enum OwnerCommand {
    Call {
        request: ExternalRequest,
        reply: oneshot::Sender<ExternalResponse>,
    },
    Cancel {
        client_id: String,
        cancel_token: String,
        reply: oneshot::Sender<bool>,
    },
    CompleteScanFetch {
        scan_id: u64,
    },
    Shutdown,
}

struct OwnerState {
    _args: ServerBootstrapArgs,
    config: RuntimeConfig,
    command_tx: mpsc::UnboundedSender<OwnerCommand>,
    options: OwnerRuntimeOptions,
    accepting_requests: bool,
    data: BTreeMap<Vec<u8>, Vec<u8>>,
    last_committed_seqno: u64,
    data_generation: u64,
    next_scan_id: u64,
    clients: BTreeMap<String, ClientState>,
    scan_sessions: BTreeMap<u64, ScanSession>,
}

impl OwnerState {
    fn new(
        args: ServerBootstrapArgs,
        config: RuntimeConfig,
        command_tx: mpsc::UnboundedSender<OwnerCommand>,
        options: OwnerRuntimeOptions,
    ) -> Self {
        Self {
            _args: args,
            config,
            command_tx,
            options,
            accepting_requests: true,
            data: BTreeMap::new(),
            last_committed_seqno: 0,
            data_generation: 0,
            next_scan_id: 1,
            clients: BTreeMap::new(),
            scan_sessions: BTreeMap::new(),
        }
    }

    async fn run(
        mut self,
        mut command_rx: mpsc::UnboundedReceiver<OwnerCommand>,
    ) -> Result<(), Error> {
        while let Some(command) = command_rx.recv().await {
            match command {
                OwnerCommand::Call { request, reply } => {
                    self.handle_call(request, reply);
                }
                OwnerCommand::Cancel {
                    client_id,
                    cancel_token,
                    reply,
                } => {
                    let cancelled = self.handle_cancel(&client_id, &cancel_token);
                    let _ = reply.send(cancelled);
                }
                OwnerCommand::CompleteScanFetch { scan_id } => {
                    self.complete_scan_fetch(scan_id);
                }
                OwnerCommand::Shutdown => {
                    self.accepting_requests = false;
                    self.fail_all_pending_requests(stopping_status("server shutdown requested"));

                    while let Ok(drained) = command_rx.try_recv() {
                        match drained {
                            OwnerCommand::Call { request, reply } => {
                                let response = io_response(
                                    request.client_id,
                                    request.request_id,
                                    true,
                                    "server is stopping",
                                );
                                let _ = reply.send(response);
                            }
                            OwnerCommand::Cancel { reply, .. } => {
                                let _ = reply.send(false);
                            }
                            OwnerCommand::CompleteScanFetch { .. } | OwnerCommand::Shutdown => {}
                        }
                    }

                    return Ok(());
                }
            }
        }

        Ok(())
    }

    fn handle_call(&mut self, request: ExternalRequest, reply: oneshot::Sender<ExternalResponse>) {
        if !self.accepting_requests {
            let _ = reply.send(io_response(
                request.client_id,
                request.request_id,
                true,
                "server is stopping",
            ));
            return;
        }

        if self.in_flight_request_count() >= self.config.server_limits.max_pending_requests as usize
        {
            let _ = reply.send(busy_response(
                request.client_id,
                request.request_id,
                "server request queue is full",
            ));
            return;
        }

        if let Some(rejected) = self.register_request(&request, reply) {
            let _ = rejected.reply.send(rejected.response);
            return;
        }

        match &request.method {
            ExternalMethod::Put(body) => {
                let response = self.handle_put(&request, body);
                self.finish_request(response);
            }
            ExternalMethod::Delete(body) => {
                let response = self.handle_delete(&request, body);
                self.finish_request(response);
            }
            ExternalMethod::Get(body) => {
                let response = self.handle_get(&request, body);
                self.finish_request(response);
            }
            ExternalMethod::ScanStart(body) => {
                let response = self.handle_scan_start(&request, body);
                self.finish_request(response);
            }
            ExternalMethod::ScanFetchNext(body) => {
                if let Some(response) = self.handle_scan_fetch_next(&request, body) {
                    self.finish_request(response);
                }
            }
            ExternalMethod::Stats(_body) => {
                let response = self.handle_stats(&request);
                self.finish_request(response);
            }
        }
    }

    fn handle_put(&mut self, request: &ExternalRequest, body: &PutRequest) -> ExternalResponse {
        if let Err(message) = validate_key(&body.key) {
            return invalid_argument_response(
                request.client_id.clone(),
                request.request_id,
                message,
            );
        }
        if let Err(message) = validate_value(&body.value) {
            return invalid_argument_response(
                request.client_id.clone(),
                request.request_id,
                message,
            );
        }

        self.last_committed_seqno += 1;
        self.data_generation += 1;
        self.data.insert(body.key.clone(), body.value.clone());

        ok_response(
            request.client_id.clone(),
            request.request_id,
            ExternalResponsePayload::Put(PutResponse),
        )
    }

    fn handle_delete(
        &mut self,
        request: &ExternalRequest,
        body: &DeleteRequest,
    ) -> ExternalResponse {
        if let Err(message) = validate_key(&body.key) {
            return invalid_argument_response(
                request.client_id.clone(),
                request.request_id,
                message,
            );
        }

        self.last_committed_seqno += 1;
        self.data_generation += 1;
        self.data.remove(&body.key);

        ok_response(
            request.client_id.clone(),
            request.request_id,
            ExternalResponsePayload::Delete(DeleteResponse),
        )
    }

    fn handle_get(&self, request: &ExternalRequest, body: &GetRequest) -> ExternalResponse {
        if let Err(message) = validate_key(&body.key) {
            return invalid_argument_response(
                request.client_id.clone(),
                request.request_id,
                message,
            );
        }

        let value = self.data.get(&body.key).cloned();

        ok_response(
            request.client_id.clone(),
            request.request_id,
            ExternalResponsePayload::Get(GetResponse {
                found: value.is_some(),
                value,
                observation_seqno: self.last_committed_seqno,
                data_generation: self.data_generation,
            }),
        )
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

        if matches!(body.start_bound, Bound::PosInf) || matches!(body.end_bound, Bound::NegInf) {
            return invalid_argument_response(
                request.client_id.clone(),
                request.request_id,
                "scan range must use NegInf/Finite for start and Finite/PosInf for end",
            );
        }

        if compare_bound(&body.start_bound, &body.end_bound) >= 0 {
            return invalid_argument_response(
                request.client_id.clone(),
                request.request_id,
                "scan range must be non-empty",
            );
        }

        let scan_id = self.next_scan_id;
        self.next_scan_id += 1;

        let rows = self
            .data
            .iter()
            .filter(|(key, _value)| key_in_range(key, &body.start_bound, &body.end_bound))
            .map(|(key, value)| ScanRow {
                key: key.clone(),
                value: value.clone(),
            })
            .collect();

        self.scan_sessions.insert(
            scan_id,
            ScanSession {
                _scan_id: scan_id,
                _snapshot_seqno: self.last_committed_seqno,
                _data_generation: self.data_generation,
                _start_bound: body.start_bound.clone(),
                _end_bound: body.end_bound.clone(),
                max_records_per_page: body.max_records_per_page,
                max_bytes_per_page: body.max_bytes_per_page,
                rows,
                next_row_index: 0,
                resume_after_key: None,
                queued_fetches: VecDeque::new(),
                active_fetch: None,
            },
        );

        ok_response(
            request.client_id.clone(),
            request.request_id,
            ExternalResponsePayload::ScanStart(ScanStartResponse {
                scan_id,
                observation_seqno: self.last_committed_seqno,
                data_generation: self.data_generation,
            }),
        )
    }

    fn handle_scan_fetch_next(
        &mut self,
        request: &ExternalRequest,
        body: &ScanFetchNextRequest,
    ) -> Option<ExternalResponse> {
        let Some(session) = self.scan_sessions.get_mut(&body.scan_id) else {
            return Some(invalid_argument_response(
                request.client_id.clone(),
                request.request_id,
                "scan_id is unknown or expired",
            ));
        };

        let fetch_request = PendingFetchRequest {
            client_id: request.client_id.clone(),
            request_id: request.request_id,
            cancel_token: request.cancel_token.clone(),
        };

        if session.active_fetch.is_some() {
            if session.queued_fetches.len()
                >= self.config.server_limits.max_scan_fetch_queue_per_session as usize
            {
                return Some(busy_response(
                    request.client_id.clone(),
                    request.request_id,
                    "scan fetch queue is full",
                ));
            }

            session.queued_fetches.push_back(fetch_request);
            return None;
        }

        self.activate_scan_fetch(body.scan_id, fetch_request);
        None
    }

    fn handle_stats(&self, request: &ExternalRequest) -> ExternalResponse {
        let live_size_bytes = self
            .data
            .iter()
            .map(|(key, value)| (key.len() + value.len()) as u64)
            .sum();

        ok_response(
            request.client_id.clone(),
            request.request_id,
            ExternalResponsePayload::Stats(StatsResponse {
                observation_seqno: self.last_committed_seqno,
                data_generation: self.data_generation,
                levels: Vec::new(),
                logical_shards: vec![LogicalShardStats {
                    start_bound: Bound::NegInf,
                    end_bound: Bound::PosInf,
                    live_size_bytes,
                }],
            }),
        )
    }

    fn handle_cancel(&mut self, client_id: &str, cancel_token: &str) -> bool {
        for scan_id in self.scan_sessions.keys().copied().collect::<Vec<_>>() {
            let Some(session) = self.scan_sessions.get_mut(&scan_id) else {
                continue;
            };

            if let Some(active_fetch) = &mut session.active_fetch
                && active_fetch.client_id == client_id
                && active_fetch.cancel_token.as_deref() == Some(cancel_token)
            {
                active_fetch.cancelled = true;
                return true;
            }

            let queued_position = session.queued_fetches.iter().position(|queued| {
                queued.client_id == client_id
                    && queued.cancel_token.as_deref() == Some(cancel_token)
            });

            if let Some(position) = queued_position {
                let cancelled = session
                    .queued_fetches
                    .remove(position)
                    .expect("queued fetch position should be valid");
                self.finish_request(cancelled_response(
                    cancelled.client_id,
                    cancelled.request_id,
                    "queued fetch was cancelled",
                ));
                return true;
            }
        }

        false
    }

    fn complete_scan_fetch(&mut self, scan_id: u64) {
        let Some(session) = self.scan_sessions.get_mut(&scan_id) else {
            return;
        };

        let Some(active_fetch) = session.active_fetch.take() else {
            return;
        };

        if active_fetch.cancelled {
            self.finish_request(cancelled_response(
                active_fetch.client_id,
                active_fetch.request_id,
                "pending fetch was cancelled",
            ));
            self.maybe_start_next_scan_fetch(scan_id);
            return;
        }

        let (response, reached_eof) = build_scan_fetch_response(session, active_fetch);
        self.finish_request(response);

        if reached_eof {
            if let Some(queued) = self
                .scan_sessions
                .remove(&scan_id)
                .map(|session| session.queued_fetches)
            {
                for pending in queued {
                    self.finish_request(invalid_argument_response(
                        pending.client_id,
                        pending.request_id,
                        "scan_id is unknown or expired",
                    ));
                }
            }
        } else {
            self.maybe_start_next_scan_fetch(scan_id);
        }
    }

    fn maybe_start_next_scan_fetch(&mut self, scan_id: u64) {
        let next_request = self
            .scan_sessions
            .get_mut(&scan_id)
            .and_then(|session| session.queued_fetches.pop_front());

        if let Some(next_request) = next_request {
            self.activate_scan_fetch(scan_id, next_request);
        }
    }

    fn activate_scan_fetch(&mut self, scan_id: u64, request: PendingFetchRequest) {
        let Some(session) = self.scan_sessions.get_mut(&scan_id) else {
            self.finish_request(invalid_argument_response(
                request.client_id,
                request.request_id,
                "scan_id is unknown or expired",
            ));
            return;
        };

        session.active_fetch = Some(ActiveFetch {
            client_id: request.client_id,
            request_id: request.request_id,
            cancel_token: request.cancel_token,
            cancelled: false,
        });

        let command_tx = self.command_tx.clone();
        let delay = self.options.scan_fetch_delay;
        tokio::spawn(async move {
            if delay.is_zero() {
                tokio::task::yield_now().await;
            } else {
                tokio::time::sleep(delay).await;
            }
            let _ = command_tx.send(OwnerCommand::CompleteScanFetch { scan_id });
        });
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

    fn in_flight_request_count(&self) -> usize {
        self.clients
            .values()
            .map(|client| client.waiters.len())
            .sum::<usize>()
    }

    fn fail_all_pending_requests(&mut self, status: Status) {
        let active_fetches = self
            .scan_sessions
            .values_mut()
            .filter_map(|session| session.active_fetch.take())
            .collect::<Vec<_>>();

        let pending_scan_requests = self
            .scan_sessions
            .values_mut()
            .flat_map(|session| {
                session
                    .queued_fetches
                    .drain(..)
                    .map(|queued| (queued.client_id, queued.request_id))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();

        for active_fetch in active_fetches {
            self.finish_request(ExternalResponse {
                client_id: active_fetch.client_id,
                request_id: active_fetch.request_id,
                status: status.clone(),
                payload: None,
            });
        }

        for (client_id, request_id) in pending_scan_requests {
            self.finish_request(ExternalResponse {
                client_id,
                request_id,
                status: status.clone(),
                payload: None,
            });
        }

        self.scan_sessions.clear();

        let outstanding_requests = self
            .clients
            .iter()
            .flat_map(|(client_id, state)| {
                state
                    .waiters
                    .keys()
                    .copied()
                    .map(move |request_id| (client_id.clone(), request_id))
            })
            .collect::<Vec<_>>();

        for (client_id, request_id) in outstanding_requests {
            self.finish_request(ExternalResponse {
                client_id,
                request_id,
                status: status.clone(),
                payload: None,
            });
        }
    }
}

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

struct ScanSession {
    _scan_id: u64,
    _snapshot_seqno: u64,
    _data_generation: u64,
    _start_bound: Bound,
    _end_bound: Bound,
    max_records_per_page: u32,
    max_bytes_per_page: u32,
    rows: Vec<ScanRow>,
    next_row_index: usize,
    resume_after_key: Option<Vec<u8>>,
    queued_fetches: VecDeque<PendingFetchRequest>,
    active_fetch: Option<ActiveFetch>,
}

struct PendingFetchRequest {
    client_id: String,
    request_id: u64,
    cancel_token: Option<String>,
}

struct ActiveFetch {
    client_id: String,
    request_id: u64,
    cancel_token: Option<String>,
    cancelled: bool,
}

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

fn build_scan_fetch_response(
    session: &mut ScanSession,
    active_fetch: ActiveFetch,
) -> (ExternalResponse, bool) {
    let mut rows = Vec::new();
    let mut accumulated_bytes = 0usize;

    while session.next_row_index < session.rows.len()
        && rows.len() < session.max_records_per_page as usize
    {
        let next_row = session.rows[session.next_row_index].clone();
        let row_bytes = next_row.key.len() + next_row.value.len();

        if !rows.is_empty() && accumulated_bytes + row_bytes > session.max_bytes_per_page as usize {
            break;
        }

        if rows.is_empty() && row_bytes > session.max_bytes_per_page as usize {
            rows.push(next_row);
            session.next_row_index += 1;
            break;
        }

        rows.push(next_row);
        session.next_row_index += 1;
        accumulated_bytes += row_bytes;
    }

    let eof = session.next_row_index >= session.rows.len();
    session.resume_after_key = rows.last().map(|row| row.key.clone());

    (
        ok_response(
            active_fetch.client_id,
            active_fetch.request_id,
            ExternalResponsePayload::ScanFetchNext(ScanFetchNextResponse { rows, eof }),
        ),
        eof,
    )
}

fn validate_key(key: &[u8]) -> Result<(), String> {
    if key.is_empty() {
        return Err("key must not be empty".into());
    }

    validate_max_length(key.len(), MAX_KEY_BYTES, "key")
}

fn validate_value(value: &[u8]) -> Result<(), String> {
    validate_max_length(value.len(), MAX_VALUE_BYTES, "value")
}

fn validate_max_length(length: usize, limit: usize, subject: &str) -> Result<(), String> {
    if length > limit {
        return Err(format!("{subject} exceeds the {limit}-byte limit"));
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

fn key_in_range(key: &[u8], start_bound: &Bound, end_bound: &Bound) -> bool {
    bound_allows_start(start_bound, key) && bound_allows_end(end_bound, key)
}

fn bound_allows_start(bound: &Bound, key: &[u8]) -> bool {
    match bound {
        Bound::NegInf => true,
        Bound::Finite(start) => key >= start.as_slice(),
        Bound::PosInf => false,
    }
}

fn bound_allows_end(bound: &Bound, key: &[u8]) -> bool {
    match bound {
        Bound::NegInf => false,
        Bound::Finite(end) => key < end.as_slice(),
        Bound::PosInf => true,
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

fn stopping_status(message: impl Into<String>) -> Status {
    Status {
        code: StatusCode::Io,
        retryable: true,
        message: Some(message.into()),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    use super::{
        Bound, ExternalMethod, ExternalResponsePayload, GetRequest, PezhaiServer, PutRequest,
        ScanFetchNextRequest, ScanStartRequest, ServerBootstrapArgs, StatsRequest,
    };

    static NEXT_CONFIG_ID: AtomicU64 = AtomicU64::new(0);

    #[tokio::test]
    async fn put_delete_and_get_follow_the_stub_model() {
        let server = start_test_server().await;

        let put_response = server
            .call(test_request(
                "client-a",
                7,
                Some("put-1"),
                ExternalMethod::Put(PutRequest {
                    key: b"ant".to_vec(),
                    value: b"value-1".to_vec(),
                }),
            ))
            .await
            .unwrap();
        assert_eq!(put_response.status.code, super::StatusCode::Ok);

        let get_response = server
            .call(test_request(
                "client-a",
                8,
                None,
                ExternalMethod::Get(GetRequest {
                    key: b"ant".to_vec(),
                }),
            ))
            .await
            .unwrap();
        assert_eq!(
            get_response.payload,
            Some(ExternalResponsePayload::Get(super::GetResponse {
                found: true,
                value: Some(b"value-1".to_vec()),
                observation_seqno: 1,
                data_generation: 1,
            }))
        );

        let delete_response = server
            .call(test_request(
                "client-a",
                9,
                None,
                ExternalMethod::Delete(super::DeleteRequest {
                    key: b"ant".to_vec(),
                }),
            ))
            .await
            .unwrap();
        assert_eq!(delete_response.status.code, super::StatusCode::Ok);

        let missing_response = server
            .call(test_request(
                "client-a",
                10,
                None,
                ExternalMethod::Get(GetRequest {
                    key: b"ant".to_vec(),
                }),
            ))
            .await
            .unwrap();
        assert_eq!(
            missing_response.payload,
            Some(ExternalResponsePayload::Get(super::GetResponse {
                found: false,
                value: None,
                observation_seqno: 2,
                data_generation: 2,
            }))
        );
    }

    #[tokio::test]
    async fn request_ordering_rejects_gaps_and_duplicates() {
        let server = start_test_server().await;

        let first = server
            .call(test_request(
                "ordered",
                4,
                None,
                ExternalMethod::Stats(StatsRequest),
            ))
            .await
            .unwrap();
        assert_eq!(first.status.code, super::StatusCode::Ok);

        let gap = server
            .call(test_request(
                "ordered",
                6,
                None,
                ExternalMethod::Stats(StatsRequest),
            ))
            .await
            .unwrap();
        assert_eq!(gap.status.code, super::StatusCode::InvalidArgument);

        let second = server
            .call(test_request(
                "ordered",
                5,
                None,
                ExternalMethod::Stats(StatsRequest),
            ))
            .await
            .unwrap();
        assert_eq!(second.status.code, super::StatusCode::Ok);

        let duplicate = server
            .call(test_request(
                "ordered",
                5,
                None,
                ExternalMethod::Stats(StatsRequest),
            ))
            .await
            .unwrap();
        assert_eq!(duplicate.status.code, super::StatusCode::InvalidArgument);
    }

    #[tokio::test]
    async fn scan_snapshot_is_stable_and_pages_forward() {
        let server = start_test_server().await;
        seed_keys(&server).await;

        let scan_start = server
            .call(test_request(
                "scanner",
                1,
                None,
                ExternalMethod::ScanStart(ScanStartRequest {
                    start_bound: Bound::Finite(b"ant".to_vec()),
                    end_bound: Bound::Finite(b"yak".to_vec()),
                    max_records_per_page: 1,
                    max_bytes_per_page: 64,
                }),
            ))
            .await
            .unwrap();
        let scan_id = match scan_start.payload.unwrap() {
            ExternalResponsePayload::ScanStart(response) => response.scan_id,
            other => panic!("unexpected payload: {other:?}"),
        };

        let first_page = server
            .call(test_request(
                "scanner",
                2,
                None,
                ExternalMethod::ScanFetchNext(ScanFetchNextRequest { scan_id }),
            ))
            .await
            .unwrap();
        assert_eq!(
            first_page.payload,
            Some(ExternalResponsePayload::ScanFetchNext(
                super::ScanFetchNextResponse {
                    rows: vec![super::ScanRow {
                        key: b"ant".to_vec(),
                        value: b"a".to_vec(),
                    }],
                    eof: false,
                }
            ))
        );

        let _ = server
            .call(test_request(
                "mutator",
                1,
                None,
                ExternalMethod::Put(PutRequest {
                    key: b"bat".to_vec(),
                    value: b"new".to_vec(),
                }),
            ))
            .await
            .unwrap();

        let second_page = server
            .call(test_request(
                "scanner",
                3,
                None,
                ExternalMethod::ScanFetchNext(ScanFetchNextRequest { scan_id }),
            ))
            .await
            .unwrap();
        assert_eq!(
            second_page.payload,
            Some(ExternalResponsePayload::ScanFetchNext(
                super::ScanFetchNextResponse {
                    rows: vec![super::ScanRow {
                        key: b"bee".to_vec(),
                        value: b"b".to_vec(),
                    }],
                    eof: true,
                }
            ))
        );
    }

    #[tokio::test]
    async fn queued_scan_fetch_can_be_cancelled_before_completion() {
        let server = start_test_server_with_delay(Duration::from_millis(50)).await;
        seed_keys(&server).await;

        let scan_start = server
            .call(test_request(
                "scanner",
                1,
                None,
                ExternalMethod::ScanStart(ScanStartRequest {
                    start_bound: Bound::NegInf,
                    end_bound: Bound::PosInf,
                    max_records_per_page: 1,
                    max_bytes_per_page: 64,
                }),
            ))
            .await
            .unwrap();
        let scan_id = match scan_start.payload.unwrap() {
            ExternalResponsePayload::ScanStart(response) => response.scan_id,
            payload => panic!("unexpected payload: {payload:?}"),
        };

        let first_server = server.clone();
        let first_fetch = tokio::spawn(async move {
            first_server
                .call(test_request(
                    "scanner",
                    2,
                    Some("fetch-1"),
                    ExternalMethod::ScanFetchNext(ScanFetchNextRequest { scan_id }),
                ))
                .await
        });
        let second_server = server.clone();
        let second_fetch = tokio::spawn(async move {
            second_server
                .call(test_request(
                    "scanner",
                    3,
                    Some("fetch-2"),
                    ExternalMethod::ScanFetchNext(ScanFetchNextRequest { scan_id }),
                ))
                .await
        });

        tokio::time::sleep(Duration::from_millis(5)).await;
        assert!(
            server
                .cancel("scanner".to_string(), "fetch-2".to_string())
                .await
                .unwrap()
        );

        let first_fetch = first_fetch.await.unwrap().unwrap();
        assert_eq!(first_fetch.status.code, super::StatusCode::Ok);

        let second_fetch = second_fetch.await.unwrap().unwrap();
        assert_eq!(second_fetch.status.code, super::StatusCode::Cancelled);
    }

    #[tokio::test]
    async fn stats_report_whole_keyspace_stub_values() {
        let server = start_test_server().await;
        seed_keys(&server).await;

        let stats = server
            .call(test_request(
                "stats",
                1,
                None,
                ExternalMethod::Stats(StatsRequest),
            ))
            .await
            .unwrap();

        assert_eq!(
            stats.payload,
            Some(ExternalResponsePayload::Stats(super::StatsResponse {
                observation_seqno: 2,
                data_generation: 2,
                levels: Vec::new(),
                logical_shards: vec![super::LogicalShardStats {
                    start_bound: Bound::NegInf,
                    end_bound: Bound::PosInf,
                    live_size_bytes: 8,
                }],
            }))
        );
    }

    #[test]
    fn length_validation_rejects_values_over_the_shared_limit() {
        let value_error =
            super::validate_max_length(super::MAX_VALUE_BYTES + 1, super::MAX_VALUE_BYTES, "value")
                .unwrap_err();
        assert!(value_error.contains("value exceeds"));
    }

    async fn seed_keys(server: &PezhaiServer) {
        let _ = server
            .call(test_request(
                "seed",
                1,
                None,
                ExternalMethod::Put(PutRequest {
                    key: b"ant".to_vec(),
                    value: b"a".to_vec(),
                }),
            ))
            .await
            .unwrap();
        let _ = server
            .call(test_request(
                "seed",
                2,
                None,
                ExternalMethod::Put(PutRequest {
                    key: b"bee".to_vec(),
                    value: b"b".to_vec(),
                }),
            ))
            .await
            .unwrap();
    }

    fn test_request(
        client_id: &str,
        request_id: u64,
        cancel_token: Option<&str>,
        method: ExternalMethod,
    ) -> super::ExternalRequest {
        super::ExternalRequest {
            client_id: client_id.to_string(),
            request_id,
            cancel_token: cancel_token.map(str::to_string),
            method,
        }
    }

    async fn start_test_server() -> PezhaiServer {
        start_test_server_with_delay(Duration::ZERO).await
    }

    async fn start_test_server_with_delay(delay: Duration) -> PezhaiServer {
        let config_path = write_test_config("127.0.0.1:0");
        PezhaiServer::start_with_options(
            ServerBootstrapArgs { config_path },
            super::OwnerRuntimeOptions {
                scan_fetch_delay: delay,
            },
        )
        .await
        .unwrap()
    }

    fn write_test_config(listen_addr: &str) -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let config_id = NEXT_CONFIG_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("pezhai-sevai-{unique}-{config_id}.toml"));
        fs::write(
            &path,
            format!(
                r#"
[engine]
sync_mode = "per_write"

[sevai]
listen_addr = "{listen_addr}"
"#
            ),
        )
        .unwrap();
        path
    }
}
