//! Loopback TCP integration matrix for protobuf-framed external RPC methods.

use kalanjiyam::Bound;
use kalanjiyam::sevai::tcp::{TcpRpcClient, TcpTransportConfig, spawn_loopback_transport};
use kalanjiyam::sevai::{
    ExternalMethod, ExternalResponsePayload, PezhaiServer, ServerBootstrapArgs,
};

use crate::common::{StoreOptions, create_store, request, run_async};

#[test]
fn stats_round_trip_over_tcp_transport() {
    run_async(async {
        let (_tempdir, server, transport, mut client) =
            start_fixture(&StoreOptions::default()).await;
        let response = client
            .call(request("reader-a", 1, ExternalMethod::Stats))
            .await
            .expect("stats request over tcp should return");
        assert!(matches!(
            response.payload,
            Some(ExternalResponsePayload::Stats(_))
        ));
        shutdown_fixture(server, transport).await;
    });
}

#[test]
fn put_delete_get_round_trip_over_tcp_transport() {
    run_async(async {
        let (_tempdir, server, transport, mut client) =
            start_fixture(&StoreOptions::default()).await;

        let put = client
            .call(request(
                "writer-a",
                1,
                ExternalMethod::Put {
                    key: b"ant".to_vec(),
                    value: b"value-one".to_vec(),
                },
            ))
            .await
            .expect("put request over tcp should return");
        assert!(matches!(put.payload, Some(ExternalResponsePayload::Put)));

        let delete = client
            .call(request(
                "writer-a",
                2,
                ExternalMethod::Delete {
                    key: b"ant".to_vec(),
                },
            ))
            .await
            .expect("delete request over tcp should return");
        assert!(matches!(
            delete.payload,
            Some(ExternalResponsePayload::Delete)
        ));

        let get = client
            .call(request(
                "reader-a",
                1,
                ExternalMethod::Get {
                    key: b"ant".to_vec(),
                },
            ))
            .await
            .expect("get request over tcp should return");
        let payload = match get.payload.expect("get must include a payload") {
            ExternalResponsePayload::Get(payload) => payload,
            other => panic!("expected Get payload, got {other:?}"),
        };
        assert!(!payload.found);
        assert_eq!(payload.value, None);

        shutdown_fixture(server, transport).await;
    });
}

#[test]
fn scan_start_and_fetch_next_round_trip_over_tcp_transport() {
    run_async(async {
        let (_tempdir, server, transport, mut client) =
            start_fixture(&StoreOptions::default()).await;

        for (request_id, key, value) in [
            (1_u64, b"ant".to_vec(), b"value-one".to_vec()),
            (2_u64, b"bee".to_vec(), b"value-two".to_vec()),
        ] {
            let put = client
                .call(request(
                    "writer-a",
                    request_id,
                    ExternalMethod::Put { key, value },
                ))
                .await
                .expect("seed put over tcp should return");
            assert!(matches!(put.payload, Some(ExternalResponsePayload::Put)));
        }

        let start = client
            .call(request(
                "reader-a",
                1,
                ExternalMethod::ScanStart {
                    start_bound: Bound::NegInf,
                    end_bound: Bound::PosInf,
                    max_records_per_page: 128,
                    max_bytes_per_page: 256 * 1024,
                },
            ))
            .await
            .expect("scan start over tcp should return");
        let scan_id = match start.payload.expect("scan start must include payload") {
            ExternalResponsePayload::ScanStart(payload) => payload.scan_id,
            other => panic!("expected ScanStart payload, got {other:?}"),
        };

        let fetch = client
            .call(request(
                "reader-a",
                2,
                ExternalMethod::ScanFetchNext { scan_id },
            ))
            .await
            .expect("scan fetch next over tcp should return");
        let page = match fetch.payload.expect("scan fetch next must include payload") {
            ExternalResponsePayload::ScanFetchNext(payload) => payload,
            other => panic!("expected ScanFetchNext payload, got {other:?}"),
        };
        assert!(!page.rows.is_empty());
        assert_eq!(page.rows[0].key, b"ant".to_vec());

        shutdown_fixture(server, transport).await;
    });
}

#[test]
fn sync_round_trip_over_tcp_transport() {
    run_async(async {
        let (_tempdir, server, transport, mut client) =
            start_fixture(&StoreOptions::default()).await;

        let put = client
            .call(request(
                "writer-a",
                1,
                ExternalMethod::Put {
                    key: b"yak".to_vec(),
                    value: b"value-one".to_vec(),
                },
            ))
            .await
            .expect("put over tcp should return");
        assert!(matches!(put.payload, Some(ExternalResponsePayload::Put)));

        let sync = client
            .call(request("sync-a", 1, ExternalMethod::Sync))
            .await
            .expect("sync over tcp should return");
        let payload = match sync.payload.expect("sync must include payload") {
            ExternalResponsePayload::Sync(payload) => payload,
            other => panic!("expected Sync payload, got {other:?}"),
        };
        assert!(payload.durable_seqno >= 1);

        shutdown_fixture(server, transport).await;
    });
}

async fn start_fixture(
    options: &StoreOptions,
) -> (
    tempfile::TempDir,
    PezhaiServer,
    kalanjiyam::sevai::tcp::LoopbackTcpTransport,
    TcpRpcClient,
) {
    let (tempdir, config_path) = create_store(options);
    let server = PezhaiServer::start(ServerBootstrapArgs { config_path })
        .await
        .expect("server should start on a valid config");
    let transport = spawn_loopback_transport(server.clone(), TcpTransportConfig::default())
        .await
        .expect("loopback tcp transport should bind");
    let client = TcpRpcClient::connect_loopback(transport.local_addr())
        .await
        .expect("tcp client should connect to loopback transport");
    (tempdir, server, transport, client)
}

async fn shutdown_fixture(
    server: PezhaiServer,
    transport: kalanjiyam::sevai::tcp::LoopbackTcpTransport,
) {
    transport
        .shutdown()
        .await
        .expect("transport shutdown should succeed");
    server
        .shutdown()
        .await
        .expect("server shutdown should succeed");
    server
        .wait_stopped()
        .await
        .expect("server wait_stopped should succeed");
}
