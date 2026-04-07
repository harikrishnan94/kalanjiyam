use std::fs;
use std::net::{SocketAddr, TcpListener as StdTcpListener};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use buffa::Message;
use pezhai::Error;
use pezhai::sevai::{PezhaiServer, ServerBootstrapArgs};
use pezhai_sevai::adapter::run_tcp_adapter;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::task::JoinHandle;

mod generated {
    include!(concat!(env!("OUT_DIR"), "/sevai_proto_include.rs"));
}

use generated::kalanjiyam::sevai::v1 as wire;

static NEXT_CONFIG_ID: AtomicU64 = AtomicU64::new(0);

#[tokio::test]
async fn framed_tcp_round_trip_returns_expected_payloads() {
    let (server, adapter_task, addr) = start_server().await;
    let mut stream = TcpStream::connect(addr).await.unwrap();

    write_frame(
        &mut stream,
        wire::RequestEnvelope {
            client_id: "tcp-client".into(),
            request_id: 1,
            cancel_token: None,
            method: wire::PutRequest {
                key: b"ant".to_vec(),
                value: b"value-1".to_vec(),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        }
        .encode_to_vec(),
    )
    .await;

    let put_response = read_response(&mut stream).await;
    assert_eq!(
        put_response.status.code.to_i32(),
        wire::StatusCode::STATUS_CODE_OK as i32
    );

    write_frame(
        &mut stream,
        wire::RequestEnvelope {
            client_id: "tcp-client".into(),
            request_id: 2,
            cancel_token: None,
            method: wire::GetRequest {
                key: b"ant".to_vec(),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        }
        .encode_to_vec(),
    )
    .await;

    let get_response = read_response(&mut stream).await;
    assert_eq!(
        get_response.status.code.to_i32(),
        wire::StatusCode::STATUS_CODE_OK as i32
    );
    match get_response.payload {
        Some(wire::response_envelope::Payload::Get(payload)) => {
            assert!(payload.found);
            assert_eq!(payload.value.as_deref(), Some(&b"value-1"[..]));
            assert_eq!(payload.observation_seqno, 1);
            assert_eq!(payload.data_generation, 1);
        }
        other => panic!("unexpected payload: {other:?}"),
    }

    stop_server(server, adapter_task).await;
}

#[tokio::test]
async fn oversized_frame_is_rejected_without_a_response() {
    let (server, adapter_task, addr) = start_server().await;
    let mut stream = TcpStream::connect(addr).await.unwrap();

    stream
        .write_all(&(16_u32 * 1024 * 1024 + 1).to_be_bytes())
        .await
        .unwrap();
    stream.shutdown().await.unwrap();

    let mut buf = [0_u8; 1];
    let bytes = stream.read(&mut buf).await.unwrap();
    assert_eq!(bytes, 0);

    stop_server(server, adapter_task).await;
}

#[tokio::test]
async fn malformed_protobuf_bytes_close_the_connection() {
    let (server, adapter_task, addr) = start_server().await;
    let mut stream = TcpStream::connect(addr).await.unwrap();

    write_frame(&mut stream, vec![0xff, 0xff, 0xff]).await;
    stream.shutdown().await.unwrap();

    let mut buf = [0_u8; 1];
    let bytes = stream.read(&mut buf).await.unwrap();
    assert_eq!(bytes, 0);

    stop_server(server, adapter_task).await;
}

#[tokio::test]
async fn duplicate_and_gap_request_ids_are_reported_as_invalid_argument() {
    let (server, adapter_task, addr) = start_server().await;
    let mut stream = TcpStream::connect(addr).await.unwrap();

    write_frame(
        &mut stream,
        wire::RequestEnvelope {
            client_id: "ordered".into(),
            request_id: 4,
            cancel_token: None,
            method: wire::StatsRequest::default().into(),
            ..Default::default()
        }
        .encode_to_vec(),
    )
    .await;
    let ok_response = read_response(&mut stream).await;
    assert_eq!(
        ok_response.status.code.to_i32(),
        wire::StatusCode::STATUS_CODE_OK as i32
    );

    write_frame(
        &mut stream,
        wire::RequestEnvelope {
            client_id: "ordered".into(),
            request_id: 6,
            cancel_token: None,
            method: wire::StatsRequest::default().into(),
            ..Default::default()
        }
        .encode_to_vec(),
    )
    .await;
    let gap_response = read_response(&mut stream).await;
    assert_eq!(
        gap_response.status.code.to_i32(),
        wire::StatusCode::STATUS_CODE_INVALID_ARGUMENT as i32
    );

    stop_server(server, adapter_task).await;
}

#[tokio::test]
async fn graceful_shutdown_stops_the_listener() {
    let (server, adapter_task, addr) = start_server().await;

    server.shutdown().await.unwrap();
    server.wait_stopped().await.unwrap();
    adapter_task.await.unwrap().unwrap();

    assert!(TcpStream::connect(addr).await.is_err());
}

#[tokio::test]
async fn wait_stopped_waits_for_idle_connection_handlers_to_close() {
    let (server, adapter_task, addr) = start_server().await;
    let mut stream = TcpStream::connect(addr).await.unwrap();

    server.shutdown().await.unwrap();
    server.wait_stopped().await.unwrap();

    let mut buf = [0_u8; 1];
    let read_result =
        tokio::time::timeout(std::time::Duration::from_secs(1), stream.read(&mut buf))
            .await
            .unwrap();
    match read_result {
        Ok(0) => {}
        Err(error) if error.kind() == std::io::ErrorKind::ConnectionReset => {}
        other => panic!("expected EOF or reset after shutdown, got {other:?}"),
    }

    adapter_task.await.unwrap().unwrap();
}

async fn start_server() -> (PezhaiServer, JoinHandle<Result<(), Error>>, SocketAddr) {
    let addr = reserve_local_addr();
    let config_path = write_test_config(addr);
    let server = PezhaiServer::start(ServerBootstrapArgs { config_path })
        .await
        .unwrap();
    let adapter_server = server.clone();
    let adapter_task = tokio::spawn(async move { run_tcp_adapter(adapter_server).await });

    wait_for_listener(addr).await;
    (server, adapter_task, addr)
}

async fn stop_server(server: PezhaiServer, adapter_task: JoinHandle<Result<(), Error>>) {
    server.shutdown().await.unwrap();
    server.wait_stopped().await.unwrap();
    adapter_task.await.unwrap().unwrap();
}

fn reserve_local_addr() -> SocketAddr {
    let listener = StdTcpListener::bind("127.0.0.1:0").unwrap();
    let addr = listener.local_addr().unwrap();
    drop(listener);
    addr
}

fn write_test_config(addr: SocketAddr) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let config_id = NEXT_CONFIG_ID.fetch_add(1, Ordering::Relaxed);
    let path = std::env::temp_dir().join(format!("pezhai-integration-{unique}-{config_id}.toml"));
    fs::write(
        &path,
        format!(
            r#"
[engine]
sync_mode = "per_write"

[sevai]
listen_addr = "{addr}"
"#
        ),
    )
    .unwrap();
    path
}

async fn wait_for_listener(addr: SocketAddr) {
    for _ in 0..50 {
        if TcpStream::connect(addr).await.is_ok() {
            return;
        }
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
    }

    panic!("listener never became ready at {addr}");
}

async fn write_frame(stream: &mut TcpStream, payload: Vec<u8>) {
    stream
        .write_all(&(payload.len() as u32).to_be_bytes())
        .await
        .unwrap();
    stream.write_all(&payload).await.unwrap();
}

async fn read_response(stream: &mut TcpStream) -> wire::ResponseEnvelope {
    let mut prefix = [0_u8; 4];
    stream.read_exact(&mut prefix).await.unwrap();
    let frame_len = u32::from_be_bytes(prefix) as usize;
    let mut payload = vec![0_u8; frame_len];
    stream.read_exact(&mut payload).await.unwrap();
    wire::ResponseEnvelope::decode_from_slice(&payload).unwrap()
}
