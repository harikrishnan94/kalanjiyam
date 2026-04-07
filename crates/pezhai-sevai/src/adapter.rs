//! TCP and protobuf adapter owned by the `pezhai-sevai` server package.

use buffa::{EnumValue, Message};
use pezhai::Error;
use pezhai::sevai::{
    Bound, DeleteRequest, ExternalMethod, ExternalRequest, ExternalResponse,
    ExternalResponsePayload, GetRequest, PezhaiServer, PutRequest, ScanFetchNextRequest,
    ScanStartRequest, StatsRequest, Status, StatusCode, SyncRequest,
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
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

// Keep one writer task per connection so response frames preserve send order on the socket.
async fn serve_connection(server: PezhaiServer, stream: TcpStream) -> Result<(), Error> {
    let (mut reader, mut writer) = stream.into_split();
    let (frame_tx, mut frame_rx) = mpsc::unbounded_channel::<Vec<u8>>();
    let mut response_tasks = JoinSet::new();

    let writer_task = tokio::spawn(async move {
        while let Some(frame) = frame_rx.recv().await {
            writer.write_all(&frame).await?;
        }

        writer.shutdown().await
    });

    let owner_result = loop {
        tokio::select! {
            stopped = server.wait_owner_stopped() => {
                break Some(stopped);
            }
            frame = read_frame(&mut reader) => {
                let frame = match frame? {
                    Some(frame) => frame,
                    None => break None,
                };

                let envelope = wire::RequestEnvelope::decode_from_slice(&frame).map_err(|error| {
                    Error::Protocol(format!("failed to decode request envelope: {error}"))
                })?;

                let request = match decode_request_envelope(envelope) {
                    Ok(request) => request,
                    Err(response) => {
                        let _ = frame_tx.send(encode_response_frame(&response));
                        continue;
                    }
                };

                let server = server.clone();
                let frame_tx = frame_tx.clone();
                response_tasks.spawn(async move {
                    let response = match server.call(request.clone()).await {
                        Ok(response) => response,
                        Err(_error) => io_response(
                            request.client_id,
                            request.request_id,
                            false,
                            "server is no longer available",
                        ),
                    };
                    let _ = frame_tx.send(encode_response_frame(&response));
                });
            }
        }
    };

    while let Some(joined) = response_tasks.join_next().await {
        if let Err(error) = joined
            && error.is_panic()
        {
            return Err(Error::ServerUnavailable(format!(
                "TCP response task panicked: {error}"
            )));
        }
    }

    drop(frame_tx);

    let writer_result = writer_task
        .await
        .map_err(|error| Error::ServerUnavailable(format!("TCP writer task failed: {error}")))?
        .map_err(Error::Io);

    if let Some(owner_result) = owner_result {
        owner_result?;
    }

    writer_result
}

// Read exactly one length-prefixed protobuf frame while enforcing the configured size limit.
async fn read_frame<R>(reader: &mut R) -> Result<Option<Vec<u8>>, Error>
where
    R: AsyncReadExt + Unpin,
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
    R: AsyncReadExt + Unpin,
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

#[cfg(test)]
mod tests {
    use buffa::Message;
    use pezhai::sevai::{LevelStats, LogicalShardStats, ScanFetchNextResponse, ScanRow};
    use tokio::io::AsyncWriteExt;

    use super::{
        Bound, ExternalMethod, ExternalResponse, ExternalResponsePayload, MAX_FRAME_BYTES, Status,
        StatusCode, decode_request_envelope, encode_response_frame, read_frame, read_prefix, wire,
    };

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
}
