//! Integration matrix for the public `PezhaiServer` workflow.

use std::path::PathBuf;
use std::time::Duration;

use kalanjiyam::Bound;
use kalanjiyam::error::KalanjiyamError;
use kalanjiyam::sevai::{
    ExternalMethod, ExternalResponsePayload, PezhaiServer, ServerBootstrapArgs, StatusCode,
};

use crate::common::{
    StoreOptions, create_store, request, run_async, store_dir, wait_for_path_state,
};

#[test]
fn startup_rejects_serving_when_engine_open_fails() {
    let missing_path = PathBuf::from("/definitely-missing/kalanjiyam/config.toml");
    let result = run_async(PezhaiServer::start(ServerBootstrapArgs {
        config_path: missing_path,
    }));

    assert!(
        matches!(
            result,
            Err(KalanjiyamError::Io(_)) | Err(KalanjiyamError::InvalidArgument(_))
        ),
        "server start must fail when bootstrap config path cannot be opened"
    );
}

#[test]
fn startup_uses_config_path_from_bootstrap_arguments() {
    let (_tempdir, config_path) = create_store(&StoreOptions::default());
    let server = run_async(PezhaiServer::start(ServerBootstrapArgs {
        config_path: config_path.clone(),
    }))
    .expect("server should start when bootstrap supplies a valid config path");

    let stats_response = run_async(server.call(request("client-a", 1, ExternalMethod::Stats)))
        .expect("call should return one terminal response when server is ready");

    assert_eq!(stats_response.status.code, StatusCode::Ok);
    assert!(matches!(
        stats_response.payload,
        Some(ExternalResponsePayload::Stats(_))
    ));
}

#[test]
fn shutdown_stops_admission_then_wait_stopped_completes() {
    let (_tempdir, config_path) = create_store(&StoreOptions::default());
    let server = run_async(PezhaiServer::start(ServerBootstrapArgs { config_path }))
        .expect("server should start on valid config");

    run_async(server.shutdown()).expect("shutdown should succeed");
    run_async(server.wait_stopped()).expect("wait_stopped should succeed");

    let response = run_async(server.call(request("client-a", 1, ExternalMethod::Stats)))
        .expect("call should still return a terminal response after shutdown");
    assert_eq!(response.status.code, StatusCode::Io);
    assert!(!response.status.retryable);
}

#[test]
fn put_delete_get_and_sync_use_real_public_workflow() {
    let (_tempdir, config_path) = create_store(&StoreOptions::default());
    let server = run_async(PezhaiServer::start(ServerBootstrapArgs { config_path }))
        .expect("server should start on valid config");

    let put = run_async(server.call(request(
        "writer",
        1,
        ExternalMethod::Put {
            key: b"ant".to_vec(),
            value: b"value-one".to_vec(),
        },
    )))
    .expect("put should return");
    assert_eq!(put.status.code, StatusCode::Ok);
    assert!(matches!(put.payload, Some(ExternalResponsePayload::Put)));

    let delete = run_async(server.call(request(
        "writer",
        2,
        ExternalMethod::Delete {
            key: b"ant".to_vec(),
        },
    )))
    .expect("delete should return");
    assert_eq!(delete.status.code, StatusCode::Ok);
    assert!(matches!(
        delete.payload,
        Some(ExternalResponsePayload::Delete)
    ));

    let get = run_async(server.call(request(
        "reader",
        1,
        ExternalMethod::Get {
            key: b"ant".to_vec(),
        },
    )))
    .expect("get should return");
    let payload = get.payload.expect("get should return payload");
    let response = match payload {
        ExternalResponsePayload::Get(response) => response,
        other => panic!("expected Get payload, got {other:?}"),
    };
    assert_eq!(get.status.code, StatusCode::Ok);
    assert!(!response.found);
    assert_eq!(response.value, None);

    let sync = run_async(server.call(request("syncer", 1, ExternalMethod::Sync)))
        .expect("sync should return");
    let durable_seqno = match sync.payload.expect("sync should return payload") {
        ExternalResponsePayload::Sync(payload) => payload.durable_seqno,
        other => panic!("expected Sync payload, got {other:?}"),
    };
    assert_eq!(sync.status.code, StatusCode::Ok);
    assert!(durable_seqno >= 2);
}

#[test]
fn file_backed_get_uses_the_public_worker_read_workflow() {
    run_async(async {
        let options = StoreOptions {
            memtable_flush_bytes: 16_384,
            ..StoreOptions::default()
        };
        let (_tempdir, config_path) = create_store(&options);
        let store = store_dir(&config_path);
        let server = PezhaiServer::start(ServerBootstrapArgs { config_path })
            .await
            .expect("server should start");

        let large_value = vec![b'v'; 20_000];
        server
            .call(request(
                "writer",
                1,
                ExternalMethod::Put {
                    key: b"yak".to_vec(),
                    value: large_value.clone(),
                },
            ))
            .await
            .expect("put should return");

        let flushed_file = store.join("data/00000000000000000001.kjm");
        assert!(
            wait_for_path_state(&flushed_file, true, Duration::from_secs(3)).await,
            "background flush should publish the first data file"
        );

        let response = server
            .call(request(
                "reader",
                1,
                ExternalMethod::Get {
                    key: b"yak".to_vec(),
                },
            ))
            .await
            .expect("get should return");
        assert_eq!(response.status.code, StatusCode::Ok);
        let payload = match response.payload.expect("get should return a payload") {
            ExternalResponsePayload::Get(payload) => payload,
            other => panic!("expected Get payload, got {other:?}"),
        };
        assert!(payload.found);
        assert_eq!(payload.value, Some(large_value));
    });
}

#[test]
fn scan_start_rejects_zero_page_limits() {
    let (_tempdir, config_path) = create_store(&StoreOptions::default());
    let server = run_async(PezhaiServer::start(ServerBootstrapArgs { config_path }))
        .expect("server should start");

    for method in [
        ExternalMethod::ScanStart {
            start_bound: Bound::NegInf,
            end_bound: Bound::PosInf,
            max_records_per_page: 0,
            max_bytes_per_page: 1024,
        },
        ExternalMethod::ScanStart {
            start_bound: Bound::NegInf,
            end_bound: Bound::PosInf,
            max_records_per_page: 1,
            max_bytes_per_page: 0,
        },
    ] {
        let response = run_async(server.call(request("reader", 1, method)))
            .expect("scan start should return a terminal response");
        assert_eq!(response.status.code, StatusCode::InvalidArgument);
    }
}

#[test]
fn scan_start_rejects_empty_key_ranges() {
    let (_tempdir, config_path) = create_store(&StoreOptions::default());
    let server = run_async(PezhaiServer::start(ServerBootstrapArgs { config_path }))
        .expect("server should start");

    let response = run_async(server.call(request(
        "reader",
        1,
        ExternalMethod::ScanStart {
            start_bound: Bound::Finite(b"ant".to_vec()),
            end_bound: Bound::Finite(b"ant".to_vec()),
            max_records_per_page: 1,
            max_bytes_per_page: 1024,
        },
    )))
    .expect("scan start should return a terminal response");
    assert_eq!(response.status.code, StatusCode::InvalidArgument);
}

#[test]
fn scan_session_keeps_a_stable_view_across_later_writes() {
    let (_tempdir, config_path) = create_store(&StoreOptions::default());
    let server = run_async(PezhaiServer::start(ServerBootstrapArgs { config_path }))
        .expect("server should start");

    let _ = run_async(server.call(request(
        "writer",
        1,
        ExternalMethod::Put {
            key: b"ant".to_vec(),
            value: b"value-one".to_vec(),
        },
    )))
    .expect("put should return");

    let start = run_async(server.call(request(
        "reader",
        1,
        ExternalMethod::ScanStart {
            start_bound: Bound::NegInf,
            end_bound: Bound::PosInf,
            max_records_per_page: 1,
            max_bytes_per_page: 1024,
        },
    )))
    .expect("scan start should return");
    let scan_id = match start.payload.expect("scan start should return payload") {
        ExternalResponsePayload::ScanStart(payload) => payload.scan_id,
        other => panic!("expected ScanStart payload, got {other:?}"),
    };

    let _ = run_async(server.call(request(
        "writer",
        2,
        ExternalMethod::Put {
            key: b"bee".to_vec(),
            value: b"value-two".to_vec(),
        },
    )))
    .expect("second put should return");

    let page = run_async(server.call(request(
        "reader",
        2,
        ExternalMethod::ScanFetchNext { scan_id },
    )))
    .expect("scan fetch should return");
    let rows = match page.payload.expect("scan fetch should return payload") {
        ExternalResponsePayload::ScanFetchNext(payload) => payload.rows,
        other => panic!("expected ScanFetchNext payload, got {other:?}"),
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].key, b"ant".to_vec());
}

#[test]
fn concurrent_scan_fetch_requests_return_fifo_pages() {
    run_async(async {
        let options = StoreOptions {
            memtable_flush_bytes: 16_384,
            ..StoreOptions::default()
        };
        let (_tempdir, config_path) = create_store(&options);
        let server = PezhaiServer::start(ServerBootstrapArgs { config_path })
            .await
            .expect("server should start");

        for (request_id, key) in [(1_u64, "ant"), (2, "bee"), (3, "cat")] {
            server
                .call(request(
                    "writer",
                    request_id,
                    ExternalMethod::Put {
                        key: key.as_bytes().to_vec(),
                        value: vec![b'v'; 8_200],
                    },
                ))
                .await
                .expect("put should return");
        }

        let start = server
            .call(request(
                "reader",
                1,
                ExternalMethod::ScanStart {
                    start_bound: Bound::NegInf,
                    end_bound: Bound::PosInf,
                    max_records_per_page: 1,
                    max_bytes_per_page: 9_000,
                },
            ))
            .await
            .expect("scan start should return");
        let scan_id = match start.payload.expect("scan start should return payload") {
            ExternalResponsePayload::ScanStart(payload) => payload.scan_id,
            other => panic!("expected ScanStart payload, got {other:?}"),
        };

        let first = {
            let server = server.clone();
            tokio::spawn(async move {
                server
                    .call(request(
                        "reader-a",
                        1,
                        ExternalMethod::ScanFetchNext { scan_id },
                    ))
                    .await
                    .expect("first scan fetch should return")
            })
        };
        let second = {
            let server = server.clone();
            tokio::spawn(async move {
                server
                    .call(request(
                        "reader-b",
                        1,
                        ExternalMethod::ScanFetchNext { scan_id },
                    ))
                    .await
                    .expect("second scan fetch should return")
            })
        };
        let third = {
            let server = server.clone();
            tokio::spawn(async move {
                server
                    .call(request(
                        "reader-c",
                        1,
                        ExternalMethod::ScanFetchNext { scan_id },
                    ))
                    .await
                    .expect("third scan fetch should return")
            })
        };

        let rows = vec![
            match first
                .await
                .expect("first task should not panic")
                .payload
                .expect("first payload")
            {
                ExternalResponsePayload::ScanFetchNext(payload) => payload.rows[0].key.clone(),
                other => panic!("expected ScanFetchNext payload, got {other:?}"),
            },
            match second
                .await
                .expect("second task should not panic")
                .payload
                .expect("second payload")
            {
                ExternalResponsePayload::ScanFetchNext(payload) => payload.rows[0].key.clone(),
                other => panic!("expected ScanFetchNext payload, got {other:?}"),
            },
            match third
                .await
                .expect("third task should not panic")
                .payload
                .expect("third payload")
            {
                ExternalResponsePayload::ScanFetchNext(payload) => payload.rows[0].key.clone(),
                other => panic!("expected ScanFetchNext payload, got {other:?}"),
            },
        ];
        assert_eq!(
            rows,
            vec![b"ant".to_vec(), b"bee".to_vec(), b"cat".to_vec()]
        );
    });
}

#[test]
fn cancelling_waiting_per_write_put_still_returns_success() {
    run_async(async {
        let options = StoreOptions {
            sync_mode: "per_write",
            group_commit_bytes: 1_000_000,
            group_commit_max_delay_ms: 40,
            ..StoreOptions::default()
        };
        let (_tempdir, config_path) = create_store(&options);
        let server = PezhaiServer::start(ServerBootstrapArgs { config_path })
            .await
            .expect("server should start");

        let task = {
            let server = server.clone();
            tokio::spawn(async move {
                server
                    .call(request(
                        "writer",
                        1,
                        ExternalMethod::Put {
                            key: b"yak".to_vec(),
                            value: b"value-one".to_vec(),
                        },
                    ))
                    .await
                    .expect("put should return")
            })
        };

        tokio::time::sleep(Duration::from_millis(5)).await;
        server
            .cancel("writer", 1)
            .await
            .expect("cancel should be accepted");

        let response = task.await.expect("put task should join");
        assert_eq!(response.status.code, StatusCode::Ok);
        assert!(matches!(
            response.payload,
            Some(ExternalResponsePayload::Put)
        ));
    });
}

#[test]
fn cancelling_waiting_sync_returns_cancelled() {
    run_async(async {
        let options = StoreOptions {
            group_commit_bytes: 1_000_000,
            group_commit_max_delay_ms: 40,
            ..StoreOptions::default()
        };
        let (_tempdir, config_path) = create_store(&options);
        let server = PezhaiServer::start(ServerBootstrapArgs { config_path })
            .await
            .expect("server should start");

        server
            .call(request(
                "writer",
                1,
                ExternalMethod::Put {
                    key: b"yak".to_vec(),
                    value: b"value-one".to_vec(),
                },
            ))
            .await
            .expect("put should complete in manual mode");

        let task = {
            let server = server.clone();
            tokio::spawn(async move {
                server
                    .call(request("syncer", 1, ExternalMethod::Sync))
                    .await
                    .expect("sync should return")
            })
        };

        tokio::time::sleep(Duration::from_millis(5)).await;
        server
            .cancel("syncer", 1)
            .await
            .expect("cancel should be accepted");

        let response = task.await.expect("sync task should join");
        assert_eq!(response.status.code, StatusCode::Cancelled);
        assert!(response.status.retryable);
    });
}

#[test]
fn per_write_batching_flushes_immediately_when_byte_threshold_is_reached() {
    run_async(async {
        let options = StoreOptions {
            sync_mode: "per_write",
            group_commit_bytes: 1,
            group_commit_max_delay_ms: 5_000,
            ..StoreOptions::default()
        };
        let (_tempdir, config_path) = create_store(&options);
        let server = PezhaiServer::start(ServerBootstrapArgs { config_path })
            .await
            .expect("server should start");

        let response = tokio::time::timeout(
            Duration::from_millis(200),
            server.call(request(
                "writer",
                1,
                ExternalMethod::Put {
                    key: b"yak".to_vec(),
                    value: b"value-one".to_vec(),
                },
            )),
        )
        .await
        .expect("byte-threshold batching should not wait for the max delay")
        .expect("put should return");

        assert_eq!(response.status.code, StatusCode::Ok);
        assert!(matches!(
            response.payload,
            Some(ExternalResponsePayload::Put)
        ));
    });
}

#[test]
fn cancelled_requests_map_to_retryable_cancelled_status_when_applicable() {
    let (_tempdir, config_path) = create_store(&StoreOptions::default());
    let server = run_async(PezhaiServer::start(ServerBootstrapArgs { config_path }))
        .expect("server should start on valid config");

    run_async(server.cancel("client-a", 1)).expect("cancel should succeed");
    let response = run_async(server.call(request("client-a", 1, ExternalMethod::Stats)))
        .expect("call should return a terminal response");
    assert_eq!(response.status.code, StatusCode::Cancelled);
    assert!(response.status.retryable);
}

#[test]
fn per_client_ordering_rejects_duplicates_gaps_and_regressions() {
    let (_tempdir, config_path) = create_store(&StoreOptions::default());
    let server = run_async(PezhaiServer::start(ServerBootstrapArgs { config_path }))
        .expect("server should start on valid config");

    let _ = run_async(server.call(request("client-a", 3, ExternalMethod::Stats)))
        .expect("first request should be admitted");

    for request_id in [3_u64, 5_u64, 2_u64] {
        let response =
            run_async(server.call(request("client-a", request_id, ExternalMethod::Stats)))
                .expect("call should return a terminal response");
        assert_eq!(response.status.code, StatusCode::InvalidArgument);
    }
}

#[test]
fn unknown_scan_id_returns_invalid_argument_for_scan_fetch_next() {
    let (_tempdir, config_path) = create_store(&StoreOptions::default());
    let server = run_async(PezhaiServer::start(ServerBootstrapArgs { config_path }))
        .expect("server should start on valid config");

    let response = run_async(server.call(request(
        "client-a",
        1,
        ExternalMethod::ScanFetchNext { scan_id: 999 },
    )))
    .expect("call should return a terminal response");
    assert_eq!(response.status.code, StatusCode::InvalidArgument);
}

#[test]
fn scan_reaching_eof_invalidates_follow_up_fetches() {
    let (_tempdir, config_path) = create_store(&StoreOptions::default());
    let server = run_async(PezhaiServer::start(ServerBootstrapArgs { config_path }))
        .expect("server should start on valid config");

    let put = run_async(server.call(request(
        "writer",
        1,
        ExternalMethod::Put {
            key: b"ant".to_vec(),
            value: b"value-one".to_vec(),
        },
    )))
    .expect("put should return");
    assert_eq!(put.status.code, StatusCode::Ok);

    let start = run_async(server.call(request(
        "reader",
        1,
        ExternalMethod::ScanStart {
            start_bound: Bound::NegInf,
            end_bound: Bound::PosInf,
            max_records_per_page: 1,
            max_bytes_per_page: 1024,
        },
    )))
    .expect("scan start should return");
    let scan_id = match start.payload.expect("scan start should return payload") {
        ExternalResponsePayload::ScanStart(payload) => payload.scan_id,
        other => panic!("expected ScanStart payload, got {other:?}"),
    };

    let first_fetch = run_async(server.call(request(
        "reader",
        2,
        ExternalMethod::ScanFetchNext { scan_id },
    )))
    .expect("scan fetch should return");
    let payload = match first_fetch
        .payload
        .expect("scan fetch should return payload")
    {
        ExternalResponsePayload::ScanFetchNext(payload) => payload,
        other => panic!("expected ScanFetchNext payload, got {other:?}"),
    };
    assert_eq!(first_fetch.status.code, StatusCode::Ok);
    assert!(payload.eof);
    assert_eq!(payload.rows.len(), 1);
    assert_eq!(payload.rows[0].key, b"ant".to_vec());

    let second_fetch = run_async(server.call(request(
        "reader",
        3,
        ExternalMethod::ScanFetchNext { scan_id },
    )))
    .expect("follow-up scan fetch should return one terminal response");
    assert_eq!(second_fetch.status.code, StatusCode::InvalidArgument);
}

#[test]
fn scan_start_returns_busy_when_scan_session_table_is_full() {
    let options = StoreOptions {
        max_scan_sessions: 1,
        ..StoreOptions::default()
    };
    let (_tempdir, config_path) = create_store(&options);
    let server = run_async(PezhaiServer::start(ServerBootstrapArgs { config_path }))
        .expect("server should start on valid config");

    let first = run_async(server.call(request(
        "reader-a",
        1,
        ExternalMethod::ScanStart {
            start_bound: Bound::NegInf,
            end_bound: Bound::PosInf,
            max_records_per_page: 1,
            max_bytes_per_page: 1024,
        },
    )))
    .expect("first scan start should return");
    assert_eq!(first.status.code, StatusCode::Ok);

    let second = run_async(server.call(request(
        "reader-b",
        1,
        ExternalMethod::ScanStart {
            start_bound: Bound::NegInf,
            end_bound: Bound::PosInf,
            max_records_per_page: 1,
            max_bytes_per_page: 1024,
        },
    )))
    .expect("second scan start should return");
    assert_eq!(second.status.code, StatusCode::Busy);
}

#[test]
fn concurrent_scan_fetch_requests_complete_with_at_most_one_busy_response() {
    run_async(async {
        let options = StoreOptions {
            memtable_flush_bytes: 16_384,
            max_scan_fetch_queue: 1,
            ..StoreOptions::default()
        };
        let (_tempdir, config_path) = create_store(&options);
        let server = PezhaiServer::start(ServerBootstrapArgs { config_path })
            .await
            .expect("server should start");

        for (request_id, key) in [(1_u64, "ant"), (2, "bee"), (3, "cat")] {
            let put = server
                .call(request(
                    "writer",
                    request_id,
                    ExternalMethod::Put {
                        key: key.as_bytes().to_vec(),
                        value: vec![b'v'; 8_200],
                    },
                ))
                .await
                .expect("put should return");
            assert_eq!(put.status.code, StatusCode::Ok);
        }

        let start = server
            .call(request(
                "reader",
                1,
                ExternalMethod::ScanStart {
                    start_bound: Bound::NegInf,
                    end_bound: Bound::PosInf,
                    max_records_per_page: 1,
                    max_bytes_per_page: 9_000,
                },
            ))
            .await
            .expect("scan start should return");
        let scan_id = match start.payload.expect("scan start should return payload") {
            ExternalResponsePayload::ScanStart(payload) => payload.scan_id,
            other => panic!("expected ScanStart payload, got {other:?}"),
        };

        let first = {
            let server = server.clone();
            tokio::spawn(async move {
                server
                    .call(request(
                        "reader-a",
                        1,
                        ExternalMethod::ScanFetchNext { scan_id },
                    ))
                    .await
                    .expect("first scan fetch should return")
            })
        };
        let second = {
            let server = server.clone();
            tokio::spawn(async move {
                server
                    .call(request(
                        "reader-b",
                        1,
                        ExternalMethod::ScanFetchNext { scan_id },
                    ))
                    .await
                    .expect("second scan fetch should return")
            })
        };
        let third = {
            let server = server.clone();
            tokio::spawn(async move {
                server
                    .call(request(
                        "reader-c",
                        1,
                        ExternalMethod::ScanFetchNext { scan_id },
                    ))
                    .await
                    .expect("third scan fetch should return")
            })
        };

        let first = first.await.expect("first task should join");
        let second = second.await.expect("second task should join");
        let third = third.await.expect("third task should join");

        let statuses = [first.status.code, second.status.code, third.status.code];
        let busy_count = statuses
            .iter()
            .filter(|status| **status == StatusCode::Busy)
            .count();
        // The exact backpressure outcome depends on whether the first worker-backed
        // page finishes before the third request is admitted. The deterministic
        // queue-saturation check lives in `owner` unit tests.
        assert!(busy_count <= 1);
        assert_eq!(
            statuses
                .iter()
                .filter(|status| **status == StatusCode::Ok)
                .count(),
            3 - busy_count
        );
        assert!(
            statuses
                .iter()
                .all(|status| matches!(status, StatusCode::Ok | StatusCode::Busy))
        );
    });
}

#[test]
fn disconnect_client_marks_future_requests_cancelled() {
    let (_tempdir, config_path) = create_store(&StoreOptions::default());
    let server = run_async(PezhaiServer::start(ServerBootstrapArgs { config_path }))
        .expect("server should start on valid config");
    run_async(server.disconnect_client("client-a")).expect("disconnect should succeed");

    let response = run_async(server.call(request("client-a", 1, ExternalMethod::Stats)))
        .expect("call should return a terminal response");
    assert_eq!(response.status.code, StatusCode::Cancelled);
}

#[test]
fn clean_store_sync_returns_immediate_success() {
    let (_tempdir, config_path) = create_store(&StoreOptions::default());
    let server = run_async(PezhaiServer::start(ServerBootstrapArgs { config_path }))
        .expect("server should start on valid config");

    let response = run_async(server.call(request("syncer", 1, ExternalMethod::Sync)))
        .expect("sync should return immediately on a clean store");
    assert_eq!(response.status.code, StatusCode::Ok);
    let payload = match response.payload.expect("sync should return a payload") {
        ExternalResponsePayload::Sync(payload) => payload,
        other => panic!("expected Sync payload, got {other:?}"),
    };
    assert_eq!(payload.durable_seqno, 0);
}

#[test]
fn same_client_ordering_holds_back_ready_stats_behind_waiting_sync() {
    run_async(async {
        let options = StoreOptions {
            group_commit_bytes: 1_000_000,
            group_commit_max_delay_ms: 40,
            ..StoreOptions::default()
        };
        let (_tempdir, config_path) = create_store(&options);
        let server = PezhaiServer::start(ServerBootstrapArgs { config_path })
            .await
            .expect("server should start");

        server
            .call(request(
                "client-a",
                1,
                ExternalMethod::Put {
                    key: b"yak".to_vec(),
                    value: b"value-one".to_vec(),
                },
            ))
            .await
            .expect("put should complete in manual mode");

        let sync_task = {
            let server = server.clone();
            tokio::spawn(async move {
                server
                    .call(request("client-a", 2, ExternalMethod::Sync))
                    .await
                    .expect("sync should return")
            })
        };
        tokio::time::sleep(Duration::from_millis(5)).await;

        let stats_task = {
            let server = server.clone();
            tokio::spawn(async move {
                server
                    .call(request("client-a", 3, ExternalMethod::Stats))
                    .await
                    .expect("stats should return")
            })
        };
        tokio::time::sleep(Duration::from_millis(5)).await;
        assert!(
            !stats_task.is_finished(),
            "same-client request ordering should hold back Stats until Sync completes"
        );

        let sync_response = sync_task.await.expect("sync task should join");
        assert_eq!(sync_response.status.code, StatusCode::Ok);
        let stats_response = stats_task.await.expect("stats task should join");
        assert_eq!(stats_response.status.code, StatusCode::Ok);
        assert!(matches!(
            stats_response.payload,
            Some(ExternalResponsePayload::Stats(_))
        ));
    });
}

#[test]
fn disconnect_client_cancels_in_flight_sync_request() {
    run_async(async {
        let options = StoreOptions {
            group_commit_bytes: 1_000_000,
            group_commit_max_delay_ms: 40,
            ..StoreOptions::default()
        };
        let (_tempdir, config_path) = create_store(&options);
        let server = PezhaiServer::start(ServerBootstrapArgs { config_path })
            .await
            .expect("server should start");

        server
            .call(request(
                "syncer",
                1,
                ExternalMethod::Put {
                    key: b"yak".to_vec(),
                    value: b"value-one".to_vec(),
                },
            ))
            .await
            .expect("put should complete in manual mode");

        let task = {
            let server = server.clone();
            tokio::spawn(async move {
                server
                    .call(request("syncer", 2, ExternalMethod::Sync))
                    .await
                    .expect("sync should return")
            })
        };

        tokio::time::sleep(Duration::from_millis(5)).await;
        server
            .disconnect_client("syncer")
            .await
            .expect("disconnect should be accepted");

        let response = task.await.expect("sync task should join");
        assert_eq!(response.status.code, StatusCode::Cancelled);
        assert!(response.status.retryable);
    });
}

#[test]
fn second_waiting_sync_returns_busy_when_waiter_limit_is_reached() {
    run_async(async {
        let options = StoreOptions {
            max_waiting_wal_syncs: 1,
            group_commit_bytes: 1_000_000,
            group_commit_max_delay_ms: 40,
            ..StoreOptions::default()
        };
        let (_tempdir, config_path) = create_store(&options);
        let server = PezhaiServer::start(ServerBootstrapArgs { config_path })
            .await
            .expect("server should start");

        server
            .call(request(
                "writer",
                1,
                ExternalMethod::Put {
                    key: b"yak".to_vec(),
                    value: b"value-one".to_vec(),
                },
            ))
            .await
            .expect("put should complete in manual mode");

        let first = {
            let server = server.clone();
            tokio::spawn(async move {
                server
                    .call(request("sync-a", 1, ExternalMethod::Sync))
                    .await
                    .expect("first sync should return")
            })
        };
        tokio::time::sleep(Duration::from_millis(5)).await;

        let second = server
            .call(request("sync-b", 1, ExternalMethod::Sync))
            .await
            .expect("second sync should return");
        assert_eq!(second.status.code, StatusCode::Busy);
        assert!(second.status.retryable);

        let first = first.await.expect("first sync task should join");
        assert_eq!(first.status.code, StatusCode::Ok);
    });
}

#[test]
fn second_per_write_put_returns_busy_when_waiting_wal_ack_limit_is_reached() {
    run_async(async {
        let options = StoreOptions {
            sync_mode: "per_write",
            max_waiting_wal_syncs: 1,
            group_commit_bytes: 1_000_000,
            group_commit_max_delay_ms: 40,
            ..StoreOptions::default()
        };
        let (_tempdir, config_path) = create_store(&options);
        let server = PezhaiServer::start(ServerBootstrapArgs { config_path })
            .await
            .expect("server should start");

        let first = {
            let server = server.clone();
            tokio::spawn(async move {
                server
                    .call(request(
                        "writer-a",
                        1,
                        ExternalMethod::Put {
                            key: b"ant".to_vec(),
                            value: b"value-one".to_vec(),
                        },
                    ))
                    .await
                    .expect("first put should return")
            })
        };
        tokio::time::sleep(Duration::from_millis(5)).await;

        let second = server
            .call(request(
                "writer-b",
                1,
                ExternalMethod::Put {
                    key: b"bee".to_vec(),
                    value: b"value-two".to_vec(),
                },
            ))
            .await
            .expect("second put should return");
        assert_eq!(second.status.code, StatusCode::Busy);
        assert!(second.status.retryable);

        let first = first.await.expect("first put task should join");
        assert_eq!(first.status.code, StatusCode::Ok);
    });
}

#[test]
fn shutdown_rejects_new_calls_with_stopping_status_before_full_stop() {
    run_async(async {
        let options = StoreOptions {
            group_commit_bytes: 1_000_000,
            group_commit_max_delay_ms: 40,
            ..StoreOptions::default()
        };
        let (_tempdir, config_path) = create_store(&options);
        let server = PezhaiServer::start(ServerBootstrapArgs { config_path })
            .await
            .expect("server should start");

        server
            .call(request(
                "writer",
                1,
                ExternalMethod::Put {
                    key: b"yak".to_vec(),
                    value: b"value-one".to_vec(),
                },
            ))
            .await
            .expect("put should complete in manual mode");

        let sync_task = {
            let server = server.clone();
            tokio::spawn(async move {
                server
                    .call(request("syncer", 1, ExternalMethod::Sync))
                    .await
                    .expect("sync should return")
            })
        };
        tokio::time::sleep(Duration::from_millis(5)).await;

        let shutdown_task = {
            let server = server.clone();
            tokio::spawn(async move { server.shutdown().await })
        };
        tokio::time::sleep(Duration::from_millis(1)).await;

        let response = server
            .call(request("reader", 1, ExternalMethod::Stats))
            .await
            .expect("stats call should return a stopping response");
        assert_eq!(response.status.code, StatusCode::Io);
        assert!(matches!(
            response.status.message.as_deref(),
            Some("server is stopping") | Some("server has stopped")
        ));

        let sync_response = sync_task.await.expect("sync task should join");
        assert_eq!(sync_response.status.code, StatusCode::Cancelled);
        shutdown_task
            .await
            .expect("shutdown task should join")
            .expect("shutdown should succeed");
        server
            .wait_stopped()
            .await
            .expect("wait_stopped should succeed");
    });
}

#[test]
fn expired_scan_session_rejects_late_fetch() {
    run_async(async {
        let options = StoreOptions {
            scan_expiry_ms: 5,
            ..StoreOptions::default()
        };
        let (_tempdir, config_path) = create_store(&options);
        let server = PezhaiServer::start(ServerBootstrapArgs { config_path })
            .await
            .expect("server should start");

        server
            .call(request(
                "writer",
                1,
                ExternalMethod::Put {
                    key: b"ant".to_vec(),
                    value: b"value-one".to_vec(),
                },
            ))
            .await
            .expect("put should complete");

        let start = server
            .call(request(
                "reader",
                1,
                ExternalMethod::ScanStart {
                    start_bound: Bound::NegInf,
                    end_bound: Bound::PosInf,
                    max_records_per_page: 1,
                    max_bytes_per_page: 1024,
                },
            ))
            .await
            .expect("scan start should succeed");
        let scan_id = match start.payload.expect("scan start should return payload") {
            ExternalResponsePayload::ScanStart(payload) => payload.scan_id,
            other => panic!("expected ScanStart payload, got {other:?}"),
        };

        tokio::time::sleep(Duration::from_millis(20)).await;
        let response = server
            .call(request(
                "reader",
                2,
                ExternalMethod::ScanFetchNext { scan_id },
            ))
            .await
            .expect("late fetch should return");
        assert_eq!(response.status.code, StatusCode::InvalidArgument);
    });
}

#[test]
fn wait_stopped_is_immediate_after_server_has_already_stopped() {
    let (_tempdir, config_path) = create_store(&StoreOptions::default());
    let server = run_async(PezhaiServer::start(ServerBootstrapArgs { config_path }))
        .expect("server should start on valid config");

    run_async(server.shutdown()).expect("shutdown should succeed");
    run_async(server.wait_stopped()).expect("first wait_stopped should succeed");
    run_async(server.wait_stopped()).expect("second wait_stopped should return immediately");
}

#[test]
fn automatic_checkpoint_dispatch_and_gc_rotate_metadata_files() {
    run_async(async {
        let options = StoreOptions {
            checkpoint_interval_secs: 1,
            gc_interval_secs: 1,
            ..StoreOptions::default()
        };
        let (_tempdir, config_path) = create_store(&options);
        let store = store_dir(&config_path);
        let server = PezhaiServer::start(ServerBootstrapArgs { config_path })
            .await
            .expect("server should start");

        let checkpoint_one = store.join("meta/meta-00000000000000000001.kjm");
        let checkpoint_two = store.join("meta/meta-00000000000000000002.kjm");

        assert!(
            wait_for_path_state(&checkpoint_one, true, Duration::from_secs(3)).await,
            "first automatic checkpoint should create the initial metadata file"
        );
        assert!(
            wait_for_path_state(&checkpoint_two, true, Duration::from_secs(4)).await,
            "second automatic checkpoint should create a new metadata file"
        );
        assert!(
            wait_for_path_state(&checkpoint_one, false, Duration::from_secs(4)).await,
            "GC should delete the superseded checkpoint after the new CURRENT is published"
        );

        server.shutdown().await.expect("shutdown should succeed");
    });
}
