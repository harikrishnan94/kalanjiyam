//! TCP and protobuf adapter owned by the `pezhai-sevai` server package.

#[cfg(test)]
use std::future::Future;

use buffa::{EnumValue, Message};
use pezhai::Error;
use pezhai::sevai::{
    Bound, DeleteRequest, ExternalMethod, ExternalRequest, ExternalResponse,
    ExternalResponsePayload, GetRequest, PezhaiServer, PutRequest, ScanFetchNextRequest,
    ScanStartRequest, StatsRequest, Status, StatusCode, SyncRequest,
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinSet;

mod generated {
    include!(concat!(env!("OUT_DIR"), "/sevai_proto_include.rs"));
}

use generated::kalanjiyam::sevai::v1 as wire;

const MAX_FRAME_BYTES: usize = 16 * 1024 * 1024;

/// Runs the raw TCP adapter against the listen address configured in `PezhaiServer`.
pub async fn run_tcp_adapter(server: PezhaiServer) -> Result<(), Error> {
    let _adapter_guard = server.register_runtime_task();
    let listener = TcpListener::bind(server.listen_addr()).await?;
    let mut connection_tasks = JoinSet::new();

    let owner_result = loop {
        tokio::select! {
            stopped = server.wait_owner_stopped() => {
                break stopped;
            }
            Some(joined) = connection_tasks.join_next(), if !connection_tasks.is_empty() => {
                let _ = joined;
            }
            accepted = listener.accept() => {
                match accepted {
                    Ok((stream, _peer_addr)) => {
                        let server = server.clone();
                        let connection_guard = server.register_runtime_task();
                        connection_tasks.spawn(async move {
                            let _connection_guard = connection_guard;
                            let _ = serve_connection(server, stream).await;
                        });
                    }
                    Err(error) => return Err(Error::Io(error)),
                }
            }
        }
    };

    drop(listener);
    while let Some(joined) = connection_tasks.join_next().await {
        let _ = joined;
    }

    owner_result
}

async fn serve_connection(server: PezhaiServer, stream: TcpStream) -> Result<(), Error> {
    let mut stream = stream;
    let call_server = |request| {
        let server = server.clone();
        async move { dispatch_server_call(server, request).await }
    };

    loop {
        tokio::select! {
            stopped = server.wait_owner_stopped() => {
                stopped?;
                break;
            }
            request = read_request_frame(&mut stream) => {
                let request = match request? {
                    Some(request) => request,
                    None => break,
                };
                let response = match request {
                    DecodedRequest::Valid(request) => call_server(request).await,
                    DecodedRequest::Invalid(response) => response,
                };
                write_response_frame(&mut stream, &response).await?;
            }
        }
    }
    stream.shutdown().await.map_err(Error::Io)
}

// Read exactly one length-prefixed protobuf frame while enforcing the configured size limit.
async fn read_frame<R>(reader: &mut R) -> Result<Option<Vec<u8>>, Error>
where
    R: AsyncRead + Unpin,
{
    let mut length_prefix = [0_u8; 4];
    let bytes_read = read_prefix(reader, &mut length_prefix).await?;
    if bytes_read == 0 {
        return Ok(None);
    }

    let frame_len = u32::from_be_bytes(length_prefix) as usize;
    if frame_len > MAX_FRAME_BYTES {
        return Err(Error::Protocol(format!(
            "frame length {frame_len} exceeds the {MAX_FRAME_BYTES}-byte limit"
        )));
    }

    let mut frame = vec![0_u8; frame_len];
    reader.read_exact(&mut frame).await.map_err(Error::Io)?;
    Ok(Some(frame))
}

async fn read_prefix<R>(reader: &mut R, prefix: &mut [u8]) -> Result<usize, Error>
where
    R: AsyncRead + Unpin,
{
    let mut filled = 0usize;
    while filled < prefix.len() {
        let read = reader
            .read(&mut prefix[filled..])
            .await
            .map_err(Error::Io)?;
        if read == 0 {
            if filled == 0 {
                return Ok(0);
            }
            return Err(Error::Protocol(
                "connection closed while reading a frame length".into(),
            ));
        }
        filled += read;
    }

    Ok(filled)
}

// Read and decode one logical request so callers can control when the next socket read happens.
async fn read_request_frame<R>(reader: &mut R) -> Result<Option<DecodedRequest>, Error>
where
    R: AsyncRead + Unpin,
{
    let Some(frame) = read_frame(reader).await? else {
        return Ok(None);
    };

    let envelope = wire::RequestEnvelope::decode_from_slice(&frame)
        .map_err(|error| Error::Protocol(format!("failed to decode request envelope: {error}")))?;

    Ok(Some(match decode_request_envelope(envelope) {
        Ok(request) => DecodedRequest::Valid(request),
        Err(response) => DecodedRequest::Invalid(*response),
    }))
}

// Serialize exactly one response frame for one completed logical request.
async fn write_response_frame<W>(writer: &mut W, response: &ExternalResponse) -> Result<(), Error>
where
    W: AsyncWrite + Unpin,
{
    writer
        .write_all(&encode_response_frame(response))
        .await
        .map_err(Error::Io)
}

// Call into the logical server and map a stopped owner task into one transport-visible response.
async fn dispatch_server_call(server: PezhaiServer, request: ExternalRequest) -> ExternalResponse {
    match server.call(request.clone()).await {
        Ok(response) => response,
        Err(_error) => io_response(
            request.client_id,
            request.request_id,
            false,
            "server is no longer available",
        ),
    }
}

// Drive one connection serially so tests can prove that request admission never pipelines.
#[cfg(test)]
async fn serve_connection_loop<S, D, F>(stream: &mut S, mut dispatch: D) -> Result<(), Error>
where
    S: AsyncRead + AsyncWrite + Unpin,
    D: FnMut(ExternalRequest) -> F,
    F: Future<Output = ExternalResponse>,
{
    while let Some(request) = read_request_frame(stream).await? {
        let response = match request {
            DecodedRequest::Valid(request) => dispatch(request).await,
            DecodedRequest::Invalid(response) => response,
        };
        write_response_frame(stream, &response).await?;
    }

    stream.shutdown().await.map_err(Error::Io)
}

// Convert one decoded protobuf request into the transport-agnostic library request model.
fn decode_request_envelope(
    envelope: wire::RequestEnvelope,
) -> Result<ExternalRequest, Box<ExternalResponse>> {
    let client_id = envelope.client_id.clone();
    let request_id = envelope.request_id;
    let cancel_token = envelope.cancel_token.clone();
    let Some(method) = envelope.method else {
        return Err(invalid_argument_response(
            client_id,
            request_id,
            "request envelope did not contain a method",
        )
        .into());
    };

    let method = match method {
        wire::request_envelope::Method::Put(request) => ExternalMethod::Put(PutRequest {
            key: request.key.clone(),
            value: request.value.clone(),
        }),
        wire::request_envelope::Method::Delete(request) => ExternalMethod::Delete(DeleteRequest {
            key: request.key.clone(),
        }),
        wire::request_envelope::Method::Get(request) => ExternalMethod::Get(GetRequest {
            key: request.key.clone(),
        }),
        wire::request_envelope::Method::ScanStart(request) => {
            let start_bound = decode_bound(
                request.start_bound.clone().into_option(),
                &client_id,
                request_id,
            )?;
            let end_bound = decode_bound(
                request.end_bound.clone().into_option(),
                &client_id,
                request_id,
            )?;
            ExternalMethod::ScanStart(ScanStartRequest {
                start_bound,
                end_bound,
                max_records_per_page: request.max_records_per_page,
                max_bytes_per_page: request.max_bytes_per_page,
            })
        }
        wire::request_envelope::Method::ScanFetchNext(request) => {
            ExternalMethod::ScanFetchNext(ScanFetchNextRequest {
                scan_id: request.scan_id,
            })
        }
        wire::request_envelope::Method::Sync(_request) => ExternalMethod::Sync(SyncRequest),
        wire::request_envelope::Method::Stats(_request) => ExternalMethod::Stats(StatsRequest),
    };

    Ok(ExternalRequest {
        client_id,
        request_id,
        cancel_token,
        method,
    })
}

fn decode_bound(
    bound: Option<wire::Bound>,
    client_id: &str,
    request_id: u64,
) -> Result<Bound, Box<ExternalResponse>> {
    let Some(bound) = bound else {
        return Err(invalid_argument_response(
            client_id.to_string(),
            request_id,
            "bound did not contain a value",
        )
        .into());
    };

    match bound.kind {
        Some(wire::bound::Kind::Finite(bytes)) => Ok(Bound::Finite(bytes)),
        Some(wire::bound::Kind::NegInf(_empty)) => Ok(Bound::NegInf),
        Some(wire::bound::Kind::PosInf(_empty)) => Ok(Bound::PosInf),
        None => Err(invalid_argument_response(
            client_id.to_string(),
            request_id,
            "bound did not contain a kind",
        )
        .into()),
    }
}

fn encode_response_frame(response: &ExternalResponse) -> Vec<u8> {
    let envelope = encode_response_envelope(response);
    let payload = envelope.encode_to_vec();
    let mut frame = Vec::with_capacity(4 + payload.len());
    frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    frame.extend_from_slice(&payload);
    frame
}

fn encode_response_envelope(response: &ExternalResponse) -> wire::ResponseEnvelope {
    let status = wire::Status {
        code: EnumValue::from(encode_status_code(response.status.code)),
        retryable: response.status.retryable,
        message: response.status.message.clone(),
        ..Default::default()
    };

    let payload = response.payload.as_ref().map(encode_response_payload);

    wire::ResponseEnvelope {
        client_id: response.client_id.clone(),
        request_id: response.request_id,
        status: status.into(),
        payload,
        ..Default::default()
    }
}

fn encode_response_payload(payload: &ExternalResponsePayload) -> wire::response_envelope::Payload {
    match payload {
        ExternalResponsePayload::Put(_response) => wire::PutResponse::default().into(),
        ExternalResponsePayload::Delete(_response) => wire::DeleteResponse::default().into(),
        ExternalResponsePayload::Get(response) => wire::GetResponse {
            found: response.found,
            value: response.value.clone(),
            observation_seqno: response.observation_seqno,
            data_generation: response.data_generation,
            ..Default::default()
        }
        .into(),
        ExternalResponsePayload::ScanStart(response) => wire::ScanStartResponse {
            scan_id: response.scan_id,
            observation_seqno: response.observation_seqno,
            data_generation: response.data_generation,
            ..Default::default()
        }
        .into(),
        ExternalResponsePayload::ScanFetchNext(response) => wire::ScanFetchNextResponse {
            rows: response
                .rows
                .iter()
                .map(|row| wire::ScanRow {
                    key: row.key.clone(),
                    value: row.value.clone(),
                    ..Default::default()
                })
                .collect(),
            eof: response.eof,
            ..Default::default()
        }
        .into(),
        ExternalResponsePayload::Sync(response) => wire::SyncResponse {
            durable_seqno: response.durable_seqno,
            ..Default::default()
        }
        .into(),
        ExternalResponsePayload::Stats(response) => wire::StatsResponse {
            observation_seqno: response.observation_seqno,
            data_generation: response.data_generation,
            levels: response
                .levels
                .iter()
                .map(|level| wire::LevelStats {
                    level_no: level.level_no,
                    file_count: level.file_count,
                    logical_bytes: level.logical_bytes,
                    physical_bytes: level.physical_bytes,
                    ..Default::default()
                })
                .collect(),
            logical_shards: response
                .logical_shards
                .iter()
                .map(|shard| wire::LogicalShardStats {
                    start_bound: Some(encode_bound(&shard.start_bound)).into(),
                    end_bound: Some(encode_bound(&shard.end_bound)).into(),
                    live_size_bytes: shard.live_size_bytes,
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
        .into(),
    }
}

fn encode_bound(bound: &Bound) -> wire::Bound {
    let kind = match bound {
        Bound::NegInf => wire::bound::Kind::NegInf(Box::default()),
        Bound::Finite(bytes) => wire::bound::Kind::Finite(bytes.clone()),
        Bound::PosInf => wire::bound::Kind::PosInf(Box::default()),
    };

    wire::Bound {
        kind: Some(kind),
        ..Default::default()
    }
}

fn encode_status_code(code: StatusCode) -> wire::StatusCode {
    match code {
        StatusCode::Ok => wire::StatusCode::STATUS_CODE_OK,
        StatusCode::Busy => wire::StatusCode::STATUS_CODE_BUSY,
        StatusCode::InvalidArgument => wire::StatusCode::STATUS_CODE_INVALID_ARGUMENT,
        StatusCode::Io => wire::StatusCode::STATUS_CODE_IO,
        StatusCode::Checksum => wire::StatusCode::STATUS_CODE_CHECKSUM,
        StatusCode::Corruption => wire::StatusCode::STATUS_CODE_CORRUPTION,
        StatusCode::Stale => wire::StatusCode::STATUS_CODE_STALE,
        StatusCode::Cancelled => wire::StatusCode::STATUS_CODE_CANCELLED,
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

/// One decoded frame that is either ready for logical dispatch or already maps to an error reply.
enum DecodedRequest {
    Valid(ExternalRequest),
    Invalid(ExternalResponse),
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::path::PathBuf;
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::{SystemTime, UNIX_EPOCH};

    use buffa::Message;
    use pezhai::sevai::{
        LevelStats, LogicalShardStats, PezhaiServer, ScanFetchNextResponse, ScanRow,
        ServerBootstrapArgs, StatsRequest as LogicalStatsRequest, StatsResponse,
    };
    use tokio::io::{AsyncRead, AsyncWriteExt};
    use tokio::sync::{Notify, oneshot};

    use super::{
        Bound, ExternalMethod, ExternalResponse, ExternalResponsePayload, MAX_FRAME_BYTES, Status,
        StatusCode, decode_request_envelope, dispatch_server_call, encode_response_frame,
        read_frame, read_prefix, serve_connection_loop, wire,
    };

    static NEXT_CONFIG_ID: AtomicU64 = AtomicU64::new(0);

    /// Tracks which requests a test dispatcher has admitted and when it is allowed to finish.
    struct DispatchProbe {
        seen_request_ids: Mutex<Vec<u64>>,
        first_started_tx: Mutex<Option<oneshot::Sender<()>>>,
        second_started_tx: Mutex<Option<oneshot::Sender<()>>>,
        release_first: Notify,
    }

    #[test]
    fn decode_request_envelope_rejects_missing_method() {
        let error = decode_request_envelope(wire::RequestEnvelope {
            client_id: "client-a".into(),
            request_id: 9,
            ..Default::default()
        })
        .unwrap_err();

        assert_eq!(error.status.code, StatusCode::InvalidArgument);
        assert_eq!(error.request_id, 9);
    }

    #[test]
    fn decode_request_envelope_maps_scan_start_requests() {
        let request = decode_request_envelope(wire::RequestEnvelope {
            client_id: "scanner".into(),
            request_id: 2,
            cancel_token: Some("scan-2".into()),
            method: wire::ScanStartRequest {
                start_bound: Some(wire::Bound {
                    kind: Some(wire::bound::Kind::Finite(b"ant".to_vec())),
                    ..Default::default()
                })
                .into(),
                end_bound: Some(wire::Bound {
                    kind: Some(wire::bound::Kind::PosInf(Box::default())),
                    ..Default::default()
                })
                .into(),
                max_records_per_page: 32,
                max_bytes_per_page: 2048,
                ..Default::default()
            }
            .into(),
            ..Default::default()
        })
        .unwrap();

        assert_eq!(request.client_id, "scanner");
        assert_eq!(request.request_id, 2);
        assert_eq!(request.cancel_token.as_deref(), Some("scan-2"));
        assert_eq!(
            request.method,
            ExternalMethod::ScanStart(pezhai::sevai::ScanStartRequest {
                start_bound: Bound::Finite(b"ant".to_vec()),
                end_bound: Bound::PosInf,
                max_records_per_page: 32,
                max_bytes_per_page: 2048,
            })
        );
    }

    #[test]
    fn encode_response_frame_round_trips_stats_payloads() {
        let frame = encode_response_frame(&ExternalResponse {
            client_id: "stats".into(),
            request_id: 4,
            status: Status {
                code: StatusCode::Ok,
                retryable: false,
                message: None,
            },
            payload: Some(ExternalResponsePayload::Stats(
                pezhai::sevai::StatsResponse {
                    observation_seqno: 7,
                    data_generation: 8,
                    levels: vec![LevelStats {
                        level_no: 1,
                        file_count: 2,
                        logical_bytes: 3,
                        physical_bytes: 4,
                    }],
                    logical_shards: vec![LogicalShardStats {
                        start_bound: Bound::NegInf,
                        end_bound: Bound::Finite(b"yak".to_vec()),
                        live_size_bytes: 9,
                    }],
                },
            )),
        });

        let payload = &frame[4..];
        let envelope = wire::ResponseEnvelope::decode_from_slice(payload).unwrap();
        assert_eq!(envelope.client_id, "stats");
        assert_eq!(envelope.request_id, 4);
        assert_eq!(
            envelope.status.code.to_i32(),
            wire::StatusCode::STATUS_CODE_OK as i32
        );
        match envelope.payload {
            Some(wire::response_envelope::Payload::Stats(payload)) => {
                assert_eq!(payload.observation_seqno, 7);
                assert_eq!(payload.data_generation, 8);
                assert_eq!(payload.levels.len(), 1);
                assert_eq!(payload.logical_shards.len(), 1);
            }
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    #[test]
    fn encode_response_frame_round_trips_scan_fetch_rows() {
        let frame = encode_response_frame(&ExternalResponse {
            client_id: "scanner".into(),
            request_id: 5,
            status: Status {
                code: StatusCode::Ok,
                retryable: false,
                message: None,
            },
            payload: Some(ExternalResponsePayload::ScanFetchNext(
                ScanFetchNextResponse {
                    rows: vec![ScanRow {
                        key: b"ant".to_vec(),
                        value: b"value".to_vec(),
                    }],
                    eof: true,
                },
            )),
        });

        let payload = &frame[4..];
        let envelope = wire::ResponseEnvelope::decode_from_slice(payload).unwrap();
        match envelope.payload {
            Some(wire::response_envelope::Payload::ScanFetchNext(payload)) => {
                assert!(payload.eof);
                assert_eq!(payload.rows.len(), 1);
                assert_eq!(payload.rows[0].key, b"ant".to_vec());
            }
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    #[tokio::test]
    async fn read_frame_rejects_oversized_payloads() {
        let (mut writer, mut reader) = tokio::io::duplex(16);

        tokio::spawn(async move {
            writer
                .write_all(&(MAX_FRAME_BYTES as u32 + 1).to_be_bytes())
                .await
                .unwrap();
        });

        let error = read_frame(&mut reader).await.unwrap_err();
        assert!(error.to_string().contains("exceeds"));
    }

    #[tokio::test]
    async fn read_prefix_rejects_mid_prefix_eof() {
        let (mut writer, mut reader) = tokio::io::duplex(16);

        tokio::spawn(async move {
            writer.write_all(&[0, 0]).await.unwrap();
        });

        let mut prefix = [0_u8; 4];
        let error = read_prefix(&mut reader, &mut prefix).await.unwrap_err();
        assert!(error.to_string().contains("frame length"));
    }

    #[tokio::test]
    async fn serve_connection_loop_waits_for_the_current_response_before_dispatching_more() {
        let (mut client, mut server_stream) = tokio::io::duplex(1024);
        let (first_started_tx, first_started_rx) = oneshot::channel();
        let (second_started_tx, mut second_started_rx) = oneshot::channel();
        let probe = Arc::new(DispatchProbe {
            seen_request_ids: Mutex::new(Vec::new()),
            first_started_tx: Mutex::new(Some(first_started_tx)),
            second_started_tx: Mutex::new(Some(second_started_tx)),
            release_first: Notify::new(),
        });

        let serve_probe = Arc::clone(&probe);
        let serve_task = tokio::spawn(async move {
            serve_connection_loop(&mut server_stream, move |request| {
                let probe = Arc::clone(&serve_probe);
                async move {
                    probe
                        .seen_request_ids
                        .lock()
                        .unwrap()
                        .push(request.request_id);
                    match request.request_id {
                        1 => {
                            if let Some(tx) = probe.first_started_tx.lock().unwrap().take() {
                                let _ = tx.send(());
                            }
                            probe.release_first.notified().await;
                        }
                        2 => {
                            if let Some(tx) = probe.second_started_tx.lock().unwrap().take() {
                                let _ = tx.send(());
                            }
                        }
                        _ => {}
                    }

                    ok_stats_response(request.client_id, request.request_id)
                }
            })
            .await
        });

        client
            .write_all(&encode_stats_request_frame("serial", 1))
            .await
            .unwrap();
        client
            .write_all(&encode_stats_request_frame("serial", 2))
            .await
            .unwrap();
        client.shutdown().await.unwrap();

        first_started_rx.await.unwrap();
        for _ in 0..4 {
            tokio::task::yield_now().await;
        }

        assert_eq!(&*probe.seen_request_ids.lock().unwrap(), &[1]);
        assert!(matches!(
            second_started_rx.try_recv(),
            Err(oneshot::error::TryRecvError::Empty)
        ));

        probe.release_first.notify_waiters();

        let first_response = read_test_response(&mut client).await;
        assert_eq!(first_response.request_id, 1);

        let second_response = read_test_response(&mut client).await;
        assert_eq!(second_response.request_id, 2);
        assert_eq!(&*probe.seen_request_ids.lock().unwrap(), &[1, 2]);

        serve_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn serve_connection_loop_replies_to_invalid_requests_before_dispatching_more() {
        let (mut client, mut server_stream) = tokio::io::duplex(1024);
        let dispatched_request_ids = Arc::new(Mutex::new(Vec::new()));
        let serve_dispatched_request_ids = Arc::clone(&dispatched_request_ids);
        let serve_task = tokio::spawn(async move {
            serve_connection_loop(&mut server_stream, move |request| {
                let dispatched_request_ids = Arc::clone(&serve_dispatched_request_ids);
                async move {
                    dispatched_request_ids
                        .lock()
                        .unwrap()
                        .push(request.request_id);
                    ok_stats_response(request.client_id, request.request_id)
                }
            })
            .await
        });

        client
            .write_all(&encode_request_frame(
                wire::RequestEnvelope {
                    client_id: "serial".into(),
                    request_id: 1,
                    ..Default::default()
                }
                .encode_to_vec(),
            ))
            .await
            .unwrap();
        client
            .write_all(&encode_stats_request_frame("serial", 2))
            .await
            .unwrap();
        client.shutdown().await.unwrap();

        let invalid_response = read_test_response(&mut client).await;
        assert_eq!(invalid_response.request_id, 1);
        assert_eq!(
            invalid_response.status.code.to_i32(),
            wire::StatusCode::STATUS_CODE_INVALID_ARGUMENT as i32
        );

        let valid_response = read_test_response(&mut client).await;
        assert_eq!(valid_response.request_id, 2);
        assert_eq!(&*dispatched_request_ids.lock().unwrap(), &[2],);

        serve_task.await.unwrap().unwrap();
    }

    #[tokio::test]
    async fn dispatch_server_call_maps_a_stopped_owner_to_io() {
        let server = start_test_server().await;
        server.shutdown().await.unwrap();
        server.wait_stopped().await.unwrap();

        let response = dispatch_server_call(
            server,
            pezhai::sevai::ExternalRequest {
                client_id: "stopped".into(),
                request_id: 1,
                cancel_token: None,
                method: ExternalMethod::Stats(LogicalStatsRequest),
            },
        )
        .await;

        assert_eq!(response.status.code, StatusCode::Io);
        assert_eq!(
            response.status.message.as_deref(),
            Some("server is no longer available"),
        );
    }

    async fn start_test_server() -> PezhaiServer {
        PezhaiServer::start(ServerBootstrapArgs {
            config_path: write_test_config(),
        })
        .await
        .unwrap()
    }

    fn write_test_config() -> PathBuf {
        let unique = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let config_id = NEXT_CONFIG_ID.fetch_add(1, Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!("adapter-test-{unique}-{config_id}.toml"));
        fs::write(
            &path,
            r#"
[engine]
sync_mode = "per_write"

[sevai]
listen_addr = "127.0.0.1:0"
"#,
        )
        .unwrap();
        path
    }

    fn encode_stats_request_frame(client_id: &str, request_id: u64) -> Vec<u8> {
        encode_request_frame(
            wire::RequestEnvelope {
                client_id: client_id.into(),
                request_id,
                cancel_token: None,
                method: wire::StatsRequest::default().into(),
                ..Default::default()
            }
            .encode_to_vec(),
        )
    }

    fn encode_request_frame(payload: Vec<u8>) -> Vec<u8> {
        let mut frame = Vec::with_capacity(4 + payload.len());
        frame.extend_from_slice(&(payload.len() as u32).to_be_bytes());
        frame.extend_from_slice(&payload);
        frame
    }

    async fn read_test_response<R>(reader: &mut R) -> wire::ResponseEnvelope
    where
        R: AsyncRead + Unpin,
    {
        let payload = read_frame(reader).await.unwrap().unwrap();
        wire::ResponseEnvelope::decode_from_slice(&payload).unwrap()
    }

    fn ok_stats_response(client_id: String, request_id: u64) -> ExternalResponse {
        ExternalResponse {
            client_id,
            request_id,
            status: Status {
                code: StatusCode::Ok,
                retryable: false,
                message: None,
            },
            payload: Some(ExternalResponsePayload::Stats(StatsResponse {
                observation_seqno: 0,
                data_generation: 0,
                levels: Vec::new(),
                logical_shards: Vec::new(),
            })),
        }
    }
}
