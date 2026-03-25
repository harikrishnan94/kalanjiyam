//! In-process async server host for the `Pezhai` engine.
//!
//! `sevai` keeps the public server API stable while routing requests through
//! one owner-loop task plus background services. The owner loop is the sole
//! publisher of mutable engine state. Background workers and the WAL sync
//! thread execute immutable or blocking work and report results back to the
//! owner for validation and publication.

mod owner;
mod worker;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU8, Ordering};

use tokio::sync::{Notify, mpsc, oneshot};

use crate::error::{ErrorKind, KalanjiyamError};
use crate::pezhai::engine::PezhaiEngine;
use crate::pezhai::types::{Bound, GetResponse, StatsResponse, SyncResponse, validate_page_limits};
use owner::{ControlMessage, ExternalCall, LifecycleState, OwnerRuntime};

/// Bootstrap arguments required to start the process-scoped server.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ServerBootstrapArgs {
    /// Path to `store/config.toml`.
    pub config_path: PathBuf,
}

/// External logical status code returned by the in-process server.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StatusCode {
    /// Successful completion.
    Ok,
    /// Admission-time backpressure before visible mutation.
    Busy,
    /// Invalid caller input or request shape.
    InvalidArgument,
    /// Filesystem or lifecycle failure.
    Io,
    /// Durable checksum failure.
    Checksum,
    /// Durable corruption or invariant violation.
    Corruption,
    /// Internal stale background result.
    Stale,
    /// Best-effort cancellation.
    Cancelled,
}

/// External logical response status.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Status {
    /// Stable status code.
    pub code: StatusCode,
    /// Whether retrying later may succeed.
    pub retryable: bool,
    /// Optional implementation-defined diagnostic message.
    pub message: Option<String>,
}

impl Status {
    /// Builds a success status.
    ///
    /// # Complexity
    /// Time: O(1).
    /// Space: O(1).
    ///
    /// # Examples
    ///
    /// ```
    /// use kalanjiyam::{Status, StatusCode};
    ///
    /// let status = Status::ok();
    /// assert_eq!(status.code, StatusCode::Ok);
    /// assert!(!status.retryable);
    /// assert_eq!(status.message, None);
    /// ```
    #[must_use]
    pub fn ok() -> Self {
        Self {
            code: StatusCode::Ok,
            retryable: false,
            message: None,
        }
    }
}

impl From<&KalanjiyamError> for Status {
    fn from(error: &KalanjiyamError) -> Self {
        Self {
            code: status_code_from_error_kind(error.kind()),
            retryable: error.retryable(),
            message: Some(error.message()),
        }
    }
}

/// External `ScanStart` success payload.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ScanStartResponse {
    /// Server-allocated scan session id.
    pub scan_id: u64,
    /// Snapshot seqno pinned for the session.
    pub observation_seqno: u64,
    /// Shared-data generation pinned for the session.
    pub data_generation: u64,
}

/// External `ScanFetchNext` success payload.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ScanFetchNextResponse {
    /// Returned rows in ascending user-key order.
    pub rows: Vec<crate::pezhai::types::ScanRow>,
    /// Whether the session reached EOF.
    pub eof: bool,
}

/// External logical request payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExternalMethod {
    /// `Put` request.
    Put { key: Vec<u8>, value: Vec<u8> },
    /// `Delete` request.
    Delete { key: Vec<u8> },
    /// Latest-only `Get`.
    Get { key: Vec<u8> },
    /// Latest-only paged `ScanStart`.
    ScanStart {
        start_bound: Bound,
        end_bound: Bound,
        max_records_per_page: u32,
        max_bytes_per_page: u32,
    },
    /// Paged `ScanFetchNext`.
    ScanFetchNext { scan_id: u64 },
    /// Explicit durability wait.
    Sync,
    /// Latest stats request.
    Stats,
}

/// External logical success payload.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ExternalResponsePayload {
    /// Empty put acknowledgement.
    Put,
    /// Empty delete acknowledgement.
    Delete,
    /// Latest-only point-read response.
    Get(GetResponse),
    /// Scan session creation response.
    ScanStart(ScanStartResponse),
    /// Next scan page response.
    ScanFetchNext(ScanFetchNextResponse),
    /// Explicit sync response.
    Sync(SyncResponse),
    /// Latest stats response.
    Stats(StatsResponse),
}

/// One external logical request envelope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExternalRequest {
    /// Stable client identifier.
    pub client_id: String,
    /// Monotonic request id unique within the client.
    pub request_id: u64,
    /// Method-specific payload.
    pub method: ExternalMethod,
    /// Optional transport-derived cancellation token.
    pub cancel_token: Option<String>,
}

/// One external logical response envelope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ExternalResponse {
    /// Stable client identifier echoed from the request.
    pub client_id: String,
    /// Request id echoed from the request.
    pub request_id: u64,
    /// Terminal response status.
    pub status: Status,
    /// Success payload when `status.code = Ok`.
    pub payload: Option<ExternalResponsePayload>,
}

/// Backwards-compatible alias while the request surface settles.
pub type RequestPayload = ExternalMethod;

/// Backwards-compatible alias while the response surface settles.
pub type ResponsePayload = ExternalResponsePayload;

/// Process-scoped server handle.
#[derive(Clone)]
pub struct PezhaiServer {
    external_tx: mpsc::Sender<ExternalCall>,
    control_tx: mpsc::Sender<ControlMessage>,
    stopped: Arc<Notify>,
    lifecycle: Arc<AtomicU8>,
}

impl std::fmt::Debug for PezhaiServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PezhaiServer").finish_non_exhaustive()
    }
}

impl PezhaiServer {
    /// Starts one process-scoped in-process server around one engine instance.
    ///
    /// # Complexity
    /// Time: O(T_open) + O(1) local setup, where `T_open` is the cost of
    /// `PezhaiEngine::open(args.config_path)`. Waiting for the owner runtime
    /// thread to start is asynchronous.
    /// Space: O(1) additional handle-side space beyond the engine state and
    /// bounded channel storage allocated during startup.
    pub async fn start(args: ServerBootstrapArgs) -> Result<Self, KalanjiyamError> {
        let engine = PezhaiEngine::open(args.config_path).await?;
        let maintenance = engine.state().config.maintenance.clone();
        let external_capacity = maintenance.max_external_requests;
        let control_capacity = maintenance
            .max_external_requests
            .saturating_add(maintenance.max_waiting_wal_syncs)
            .max(16);
        let worker_capacity = maintenance.max_worker_tasks;
        let (external_tx, external_rx) = mpsc::channel(external_capacity);
        let (control_tx, control_rx) = mpsc::channel(control_capacity);
        let (worker_result_tx, worker_result_rx) = mpsc::channel(worker_capacity);
        let (wal_result_tx, wal_result_rx) = mpsc::channel(maintenance.max_waiting_wal_syncs);
        let stopped = Arc::new(Notify::new());
        let lifecycle = Arc::new(AtomicU8::new(LifecycleState::Ready.as_u8()));
        let runtime = OwnerRuntime::new(
            engine,
            external_rx,
            control_rx,
            worker_result_rx,
            wal_result_rx,
            worker_result_tx,
            wal_result_tx,
            Arc::clone(&stopped),
            Arc::clone(&lifecycle),
        );
        std::thread::spawn(move || {
            let runtime_thread = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .expect("owner runtime thread should build");
            runtime_thread.block_on(async move {
                let mut runtime = runtime;
                runtime.run().await;
            });
        });
        Ok(Self {
            external_tx,
            control_tx,
            stopped,
            lifecycle,
        })
    }

    /// Submits one external logical request and awaits its terminal response.
    ///
    /// # Complexity
    /// Time: O(q), where `q` is the total byte size cloned into the queued
    /// `ExternalRequest` (including `client_id`, optional `cancel_token`, and
    /// any method payload). The await time depends on the owner loop and
    /// request execution path.
    /// Space: O(q) for the cloned request plus one reply channel.
    pub async fn call(
        &self,
        request: ExternalRequest,
    ) -> Result<ExternalResponse, KalanjiyamError> {
        if let Some(response) = lifecycle_rejection(&self.lifecycle, &request) {
            return Ok(response);
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        match self.external_tx.try_send(ExternalCall {
            request: request.clone(),
            reply: reply_tx,
        }) {
            Ok(()) => {}
            Err(mpsc::error::TrySendError::Full(_)) => {
                return Ok(error_response(
                    &request,
                    KalanjiyamError::Busy("external request queue is full".to_string()),
                ));
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                return Ok(lifecycle_error_response(
                    &request,
                    LifecycleState::from_u8(self.lifecycle.load(Ordering::SeqCst)),
                ));
            }
        }
        reply_rx.await.map_err(|_| {
            KalanjiyamError::Io(std::io::Error::other(
                "owner loop dropped request reply channel",
            ))
        })
    }

    /// Cancels one queued or in-flight external request on a best-effort basis.
    ///
    /// # Complexity
    /// Time: O(c), where `c` is the length of `client_id`, because the control
    /// message clones that identifier before queueing. The await time depends
    /// on when the owner loop processes that message.
    /// Space: O(c) for the cloned identifier plus one acknowledgment channel.
    pub async fn cancel(&self, client_id: &str, request_id: u64) -> Result<(), KalanjiyamError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.control_tx
            .send(ControlMessage::Cancel {
                client_id: client_id.to_string(),
                request_id,
                ack: ack_tx,
            })
            .await
            .map_err(|_| KalanjiyamError::Io(std::io::Error::other("owner loop has stopped")))?;
        ack_rx.await.map_err(|_| {
            KalanjiyamError::Io(std::io::Error::other(
                "owner loop dropped cancel acknowledgement",
            ))
        })?
    }

    /// Marks one client disconnected and cancels that client's queued work.
    ///
    /// # Complexity
    /// Time: O(c), where `c` is the length of `client_id`, because the control
    /// message clones that identifier before queueing. The await time depends
    /// on owner-loop progress.
    /// Space: O(c) for the cloned identifier plus one acknowledgment channel.
    pub async fn disconnect_client(&self, client_id: &str) -> Result<(), KalanjiyamError> {
        let (ack_tx, ack_rx) = oneshot::channel();
        self.control_tx
            .send(ControlMessage::DisconnectClient {
                client_id: client_id.to_string(),
                ack: ack_tx,
            })
            .await
            .map_err(|_| KalanjiyamError::Io(std::io::Error::other("owner loop has stopped")))?;
        ack_rx.await.map_err(|_| {
            KalanjiyamError::Io(std::io::Error::other(
                "owner loop dropped disconnect acknowledgement",
            ))
        })?
    }

    /// Stops new admission and begins asynchronous shutdown.
    ///
    /// # Complexity
    /// Time: O(1) to flip the lifecycle flag and enqueue one shutdown request.
    /// The await time depends on the owner loop finishing shutdown.
    /// Space: O(1) for the shutdown message and one acknowledgment channel.
    pub async fn shutdown(&self) -> Result<(), KalanjiyamError> {
        let previous = LifecycleState::from_u8(
            self.lifecycle
                .swap(LifecycleState::Stopping.as_u8(), Ordering::SeqCst),
        );
        if previous == LifecycleState::Stopped {
            return Ok(());
        }
        let (ack_tx, ack_rx) = oneshot::channel();
        self.control_tx
            .send(ControlMessage::Shutdown { ack: ack_tx })
            .await
            .map_err(|_| KalanjiyamError::Io(std::io::Error::other("owner loop has stopped")))?;
        ack_rx.await.map_err(|_| {
            KalanjiyamError::Io(std::io::Error::other(
                "owner loop dropped shutdown acknowledgement",
            ))
        })?
    }

    /// Waits until the server has fully stopped.
    ///
    /// # Complexity
    /// Time: O(1) to check the lifecycle flag. The await time depends on the
    /// owner runtime finishing shutdown and notifying waiters.
    /// Space: O(1).
    pub async fn wait_stopped(&self) -> Result<(), KalanjiyamError> {
        if LifecycleState::from_u8(self.lifecycle.load(Ordering::SeqCst)) == LifecycleState::Stopped
        {
            return Ok(());
        }
        self.stopped.notified().await;
        Ok(())
    }
}

fn lifecycle_rejection(
    lifecycle: &AtomicU8,
    request: &ExternalRequest,
) -> Option<ExternalResponse> {
    let lifecycle = LifecycleState::from_u8(lifecycle.load(Ordering::SeqCst));
    if lifecycle == LifecycleState::Ready {
        None
    } else {
        Some(lifecycle_error_response(request, lifecycle))
    }
}

pub(crate) fn lifecycle_error_response(
    request: &ExternalRequest,
    lifecycle: LifecycleState,
) -> ExternalResponse {
    ExternalResponse {
        client_id: request.client_id.clone(),
        request_id: request.request_id,
        status: lifecycle_status(lifecycle),
        payload: None,
    }
}

pub(crate) fn success_response(
    request: &ExternalRequest,
    payload: ExternalResponsePayload,
) -> ExternalResponse {
    ExternalResponse {
        client_id: request.client_id.clone(),
        request_id: request.request_id,
        status: Status::ok(),
        payload: Some(payload),
    }
}

pub(crate) fn error_response(
    request: &ExternalRequest,
    error: KalanjiyamError,
) -> ExternalResponse {
    ExternalResponse {
        client_id: request.client_id.clone(),
        request_id: request.request_id,
        status: Status::from(&error),
        payload: None,
    }
}

pub(crate) fn validate_scan_start_inputs(
    start_bound: &Bound,
    end_bound: &Bound,
    max_records_per_page: u32,
    max_bytes_per_page: u32,
) -> Result<crate::pezhai::types::KeyRange, KalanjiyamError> {
    validate_page_limits(max_records_per_page, max_bytes_per_page)?;
    let range = crate::pezhai::types::KeyRange {
        start_bound: start_bound.clone(),
        end_bound: end_bound.clone(),
    };
    range.validate()?;
    Ok(range)
}

fn status_code_from_error_kind(kind: ErrorKind) -> StatusCode {
    match kind {
        ErrorKind::Busy => StatusCode::Busy,
        ErrorKind::Cancelled => StatusCode::Cancelled,
        ErrorKind::Checksum => StatusCode::Checksum,
        ErrorKind::Corruption => StatusCode::Corruption,
        ErrorKind::InvalidArgument => StatusCode::InvalidArgument,
        ErrorKind::Io => StatusCode::Io,
        ErrorKind::Stale => StatusCode::Stale,
    }
}

// Keep lifecycle-to-status mapping in one place so handle checks and tests use
// the same externally visible retryability and message rules.
fn lifecycle_status(lifecycle: LifecycleState) -> Status {
    let (message, retryable) = match lifecycle {
        LifecycleState::Booting => ("server is booting", true),
        LifecycleState::Stopping => ("server is stopping", true),
        LifecycleState::Ready => ("server is ready", false),
        LifecycleState::Stopped => ("server has stopped", false),
    };
    Status {
        code: StatusCode::Io,
        retryable,
        message: Some(message.to_string()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::KalanjiyamError;
    use crate::pezhai::types::Bound;

    fn request() -> ExternalRequest {
        ExternalRequest {
            client_id: "client-a".to_string(),
            request_id: 7,
            method: ExternalMethod::Stats,
            cancel_token: None,
        }
    }

    #[test]
    fn lifecycle_errors_use_contextual_retryability() {
        let request = request();

        assert_eq!(
            lifecycle_status(LifecycleState::Booting).code,
            StatusCode::Io
        );
        assert_eq!(
            lifecycle_status(LifecycleState::Stopped).message.as_deref(),
            Some("server has stopped")
        );

        let booting = lifecycle_error_response(&request, LifecycleState::Booting);
        assert_eq!(booting.status.code, StatusCode::Io);
        assert!(booting.status.retryable);
        assert_eq!(booting.status.message.as_deref(), Some("server is booting"));

        let ready = lifecycle_error_response(&request, LifecycleState::Ready);
        assert_eq!(ready.status.code, StatusCode::Io);
        assert!(!ready.status.retryable);
        assert_eq!(ready.status.message.as_deref(), Some("server is ready"));

        let stopping = lifecycle_error_response(&request, LifecycleState::Stopping);
        assert_eq!(stopping.status.code, StatusCode::Io);
        assert!(stopping.status.retryable);
        assert_eq!(
            stopping.status.message.as_deref(),
            Some("server is stopping")
        );

        let stopped = lifecycle_error_response(&request, LifecycleState::Stopped);
        assert_eq!(stopped.status.code, StatusCode::Io);
        assert!(!stopped.status.retryable);
        assert_eq!(
            stopped.status.message.as_deref(),
            Some("server has stopped")
        );
    }

    #[test]
    fn status_from_error_maps_remaining_error_kinds_and_scan_inputs_validate() {
        assert_eq!(
            status_code_from_error_kind(ErrorKind::Busy),
            StatusCode::Busy
        );
        assert_eq!(
            status_code_from_error_kind(ErrorKind::Cancelled),
            StatusCode::Cancelled
        );
        for (error, expected_code, retryable) in [
            (
                KalanjiyamError::Checksum("crc".to_string()),
                StatusCode::Checksum,
                false,
            ),
            (
                KalanjiyamError::Corruption("corrupt".to_string()),
                StatusCode::Corruption,
                false,
            ),
            (
                KalanjiyamError::InvalidArgument("bad".to_string()),
                StatusCode::InvalidArgument,
                false,
            ),
            (
                KalanjiyamError::Io(std::io::Error::other("io")),
                StatusCode::Io,
                true,
            ),
            (
                KalanjiyamError::Stale("stale".to_string()),
                StatusCode::Stale,
                false,
            ),
        ] {
            let status = Status::from(&error);
            assert_eq!(status.code, expected_code);
            assert_eq!(status.retryable, retryable);
            assert!(status.message.is_some());
        }

        let valid = validate_scan_start_inputs(&Bound::NegInf, &Bound::PosInf, 1, 1024)
            .expect("full-range scan input should validate");
        assert_eq!(valid.start_bound, Bound::NegInf);
        assert_eq!(valid.end_bound, Bound::PosInf);
        assert!(matches!(
            validate_scan_start_inputs(
                &Bound::Finite(b"yak".to_vec()),
                &Bound::Finite(b"ant".to_vec()),
                1,
                1024
            ),
            Err(KalanjiyamError::InvalidArgument(_))
        ));
    }
}
