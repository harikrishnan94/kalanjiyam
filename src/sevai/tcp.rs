//! Raw TCP transport adapter for protobuf-framed `sevai` RPC traffic.
//!
//! This module keeps network framing separate from `PezhaiServer` semantics by
//! translating framed protobuf requests into existing `ExternalRequest` calls.
//! It intentionally excludes server bootstrap and shutdown from the network
//! surface; callers create and own `PezhaiServer` directly.

use std::net::SocketAddr;

use buffa::{Message, MessageView};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::oneshot;
use tokio::task::JoinHandle;

use crate::error::KalanjiyamError;
use crate::sevai::generated;
use crate::sevai::proto_map;
use crate::sevai::{ExternalRequest, ExternalResponse, PezhaiServer};

/// Default maximum frame payload size accepted by the TCP transport.
pub const DEFAULT_MAX_FRAME_BYTES: u32 = 16 * 1024 * 1024;

/// Runtime knobs for the raw TCP transport adapter.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct TcpTransportConfig {
    /// Maximum protobuf payload bytes per frame, excluding the 4-byte prefix.
    pub max_frame_bytes: u32,
}

impl Default for TcpTransportConfig {
    fn default() -> Self {
        Self {
            max_frame_bytes: DEFAULT_MAX_FRAME_BYTES,
        }
    }
}

impl TcpTransportConfig {
    fn validate(self) -> Result<Self, KalanjiyamError> {
        if self.max_frame_bytes == 0 {
            return Err(KalanjiyamError::InvalidArgument(
                "tcp max_frame_bytes must be > 0".to_string(),
            ));
        }
        Ok(self)
    }
}

/// Handle for one loopback TCP listener that adapts to `PezhaiServer`.
#[derive(Debug)]
pub struct LoopbackTcpTransport {
    local_addr: SocketAddr,
    shutdown_tx: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<()>>,
}

impl LoopbackTcpTransport {
    /// Returns the bound loopback listener address.
    #[must_use]
    pub fn local_addr(&self) -> SocketAddr {
        self.local_addr
    }

    /// Stops accepting new connections and waits for the listener task to exit.
    pub async fn shutdown(mut self) -> Result<(), KalanjiyamError> {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
        if let Some(task) = self.task.take() {
            task.await.map_err(|error| {
                KalanjiyamError::Io(std::io::Error::other(format!(
                    "tcp listener task join failed: {error}"
                )))
            })?;
        }
        Ok(())
    }
}

impl Drop for LoopbackTcpTransport {
    fn drop(&mut self) {
        if let Some(shutdown_tx) = self.shutdown_tx.take() {
            let _ = shutdown_tx.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

/// Binds one loopback TCP listener and forwards framed RPC requests.
pub async fn spawn_loopback_transport(
    server: PezhaiServer,
    config: TcpTransportConfig,
) -> Result<LoopbackTcpTransport, KalanjiyamError> {
    let config = config.validate()?;
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .await
        .map_err(|error| KalanjiyamError::io("bind loopback tcp listener", error))?;
    let local_addr = listener
        .local_addr()
        .map_err(|error| KalanjiyamError::io("read loopback listener address", error))?;
    let (shutdown_tx, mut shutdown_rx) = oneshot::channel::<()>();
    let task = tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = &mut shutdown_rx => {
                    break;
                }
                accepted = listener.accept() => {
                    let Ok((stream, _peer)) = accepted else {
                        break;
                    };
                    let server = server.clone();
                    tokio::spawn(async move {
                        let _ = serve_connection(server, stream, config).await;
                    });
                }
            }
        }
    });
    Ok(LoopbackTcpTransport {
        local_addr,
        shutdown_tx: Some(shutdown_tx),
        task: Some(task),
    })
}

/// Same-process TCP client helper used by integration tests and benchmarks.
#[derive(Debug)]
pub struct TcpRpcClient {
    stream: TcpStream,
    config: TcpTransportConfig,
}

impl TcpRpcClient {
    /// Connects to one loopback transport endpoint.
    pub async fn connect_loopback(addr: SocketAddr) -> Result<Self, KalanjiyamError> {
        Self::connect(addr, TcpTransportConfig::default()).await
    }

    /// Connects to one TCP transport endpoint with explicit framing config.
    pub async fn connect(
        addr: SocketAddr,
        config: TcpTransportConfig,
    ) -> Result<Self, KalanjiyamError> {
        let config = config.validate()?;
        let stream = TcpStream::connect(addr)
            .await
            .map_err(|error| KalanjiyamError::io("connect tcp rpc client", error))?;
        Ok(Self { stream, config })
    }

    /// Sends one request and waits for one response on the same connection.
    ///
    /// Connections are intentionally single-flight: callers must wait for the
    /// current response before issuing the next request on this client.
    pub async fn call(
        &mut self,
        request: ExternalRequest,
    ) -> Result<ExternalResponse, KalanjiyamError> {
        let request_proto = proto_map::request_to_wire(&request);
        write_framed_payload(&mut self.stream, &request_proto.encode_to_vec()).await?;
        let response_payload = read_framed_payload(&mut self.stream, self.config.max_frame_bytes)
            .await?
            .ok_or_else(|| {
                KalanjiyamError::Io(std::io::Error::new(
                    std::io::ErrorKind::UnexpectedEof,
                    "server closed tcp rpc connection while waiting for response",
                ))
            })?;
        decode_external_response(&response_payload)
    }
}

async fn serve_connection(
    server: PezhaiServer,
    mut stream: TcpStream,
    config: TcpTransportConfig,
) -> Result<(), KalanjiyamError> {
    while let Some(payload) = read_framed_payload(&mut stream, config.max_frame_bytes).await? {
        let request = decode_external_request(&payload)?;
        let response = server.call(request).await?;
        let response_proto = proto_map::response_to_wire(&response);
        write_framed_payload(&mut stream, &response_proto.encode_to_vec()).await?;
    }
    Ok(())
}

fn decode_external_request(bytes: &[u8]) -> Result<ExternalRequest, KalanjiyamError> {
    let view = generated::RequestEnvelopeView::decode_view(bytes).map_err(|error| {
        KalanjiyamError::InvalidArgument(format!(
            "malformed RequestEnvelope protobuf payload: {error}"
        ))
    })?;
    proto_map::request_from_wire_view(&view)
}

fn decode_external_response(bytes: &[u8]) -> Result<ExternalResponse, KalanjiyamError> {
    let view = generated::ResponseEnvelopeView::decode_view(bytes).map_err(|error| {
        KalanjiyamError::InvalidArgument(format!(
            "malformed ResponseEnvelope protobuf payload: {error}"
        ))
    })?;
    proto_map::response_from_wire_view(&view)
}

async fn read_framed_payload<R: AsyncRead + Unpin>(
    reader: &mut R,
    max_frame_bytes: u32,
) -> Result<Option<Vec<u8>>, KalanjiyamError> {
    let mut first = [0_u8; 1];
    let first_read = reader
        .read(&mut first)
        .await
        .map_err(|error| KalanjiyamError::io("read frame prefix byte", error))?;
    if first_read == 0 {
        return Ok(None);
    }

    let mut remaining_prefix = [0_u8; 3];
    reader
        .read_exact(&mut remaining_prefix)
        .await
        .map_err(|error| KalanjiyamError::io("read frame length prefix", error))?;

    let payload_len = u32::from_be_bytes([
        first[0],
        remaining_prefix[0],
        remaining_prefix[1],
        remaining_prefix[2],
    ]);
    if payload_len > max_frame_bytes {
        return Err(KalanjiyamError::InvalidArgument(format!(
            "frame length {payload_len} exceeds max_frame_bytes {max_frame_bytes}"
        )));
    }

    let mut payload = vec![0_u8; payload_len as usize];
    reader
        .read_exact(&mut payload)
        .await
        .map_err(|error| KalanjiyamError::io("read frame payload", error))?;
    Ok(Some(payload))
}

async fn write_framed_payload<W: AsyncWrite + Unpin>(
    writer: &mut W,
    payload: &[u8],
) -> Result<(), KalanjiyamError> {
    let payload_len = u32::try_from(payload.len()).map_err(|_| {
        KalanjiyamError::InvalidArgument(format!(
            "frame payload too large for u32 length prefix: {} bytes",
            payload.len()
        ))
    })?;

    writer
        .write_all(&payload_len.to_be_bytes())
        .await
        .map_err(|error| KalanjiyamError::io("write frame length prefix", error))?;
    writer
        .write_all(payload)
        .await
        .map_err(|error| KalanjiyamError::io("write frame payload", error))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pezhai::types::Bound;
    use crate::sevai::ExternalMethod;
    use tokio::io::duplex;
    use tokio::time::{Duration, sleep};

    fn run_async<T>(future: impl std::future::Future<Output = T>) -> T {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_time()
            .build()
            .expect("test runtime should build");
        runtime.block_on(future)
    }

    fn sample_request() -> ExternalRequest {
        ExternalRequest {
            client_id: "reader-a".to_string(),
            request_id: 42,
            method: ExternalMethod::ScanStart {
                start_bound: Bound::NegInf,
                end_bound: Bound::PosInf,
                max_records_per_page: 128,
                max_bytes_per_page: 256 * 1024,
            },
            cancel_token: Some("cancel-a".to_string()),
        }
    }

    #[test]
    fn frame_codec_round_trip_succeeds() {
        run_async(async {
            let (mut writer, mut reader) = duplex(1024);
            let payload = b"frame-bytes".to_vec();
            write_framed_payload(&mut writer, &payload)
                .await
                .expect("frame write should succeed");
            let decoded = read_framed_payload(&mut reader, 1024)
                .await
                .expect("frame read should succeed")
                .expect("payload should be present");
            assert_eq!(decoded, payload);
        });
    }

    #[test]
    fn frame_codec_handles_partial_reads() {
        run_async(async {
            let (mut writer, mut reader) = duplex(64);
            let payload = b"abcde".to_vec();
            let length = (payload.len() as u32).to_be_bytes();

            let write_task = tokio::spawn(async move {
                writer
                    .write_all(&length[..2])
                    .await
                    .expect("write first prefix chunk");
                sleep(Duration::from_millis(1)).await;
                writer
                    .write_all(&length[2..])
                    .await
                    .expect("write second prefix chunk");
                sleep(Duration::from_millis(1)).await;
                writer
                    .write_all(&payload[..2])
                    .await
                    .expect("write first payload chunk");
                sleep(Duration::from_millis(1)).await;
                writer
                    .write_all(&payload[2..])
                    .await
                    .expect("write second payload chunk");
            });

            let decoded = read_framed_payload(&mut reader, 64)
                .await
                .expect("frame read should succeed")
                .expect("payload should be present");
            assert_eq!(decoded, b"abcde".to_vec());
            write_task.await.expect("write task should complete");
        });
    }

    #[test]
    fn frame_codec_rejects_invalid_declared_length() {
        run_async(async {
            let (mut writer, mut reader) = duplex(16);
            writer
                .write_all(&2048_u32.to_be_bytes())
                .await
                .expect("prefix write should succeed");
            let error = read_framed_payload(&mut reader, 1024)
                .await
                .expect_err("oversized length must fail");
            assert!(matches!(error, KalanjiyamError::InvalidArgument(_)));
        });
    }

    #[test]
    fn frame_codec_rejects_malformed_protobuf_payload() {
        run_async(async {
            let malformed_payload = vec![0xff_u8, 0xff_u8, 0xff_u8];
            let error = decode_external_request(&malformed_payload)
                .expect_err("invalid protobuf payload must fail");
            assert!(matches!(error, KalanjiyamError::InvalidArgument(_)));
        });
    }

    #[test]
    fn protobuf_round_trip_succeeds_for_valid_request() {
        let request = sample_request();
        let proto = proto_map::request_to_wire(&request);
        let encoded = proto.encode_to_vec();
        let decoded = decode_external_request(&encoded).expect("protobuf decode must succeed");
        assert_eq!(decoded, request);
    }
}
