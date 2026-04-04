//! Integration coverage for foundational error, config, codec, durable, and type helpers.

use std::error::Error as _;
use std::fs;
use std::path::PathBuf;

use kalanjiyam::PezhaiEngine;
use kalanjiyam::error::{Error, ErrorKind, KalanjiyamError};
use kalanjiyam::pezhai::codec::{
    CurrentFile, WalRecord, WalRecordPayload, current_relative_path, data_relative_path,
    decode_current, decode_wal_record, decode_wal_segment_header, empty_open_segment_first_seqno,
    encode_current, encode_wal_record, metadata_relative_path, resolve_store_path,
    wal_relative_path,
};
use kalanjiyam::pezhai::durable::{
    CheckpointState, build_file_meta, find_visible_record_in_file, install_current,
    load_checkpoint_file, load_data_file, scan_visible_rows_in_file, snapshot_matches_engine,
    write_checkpoint_file, write_data_file,
};
use kalanjiyam::pezhai::types::{
    Bound, FileMeta, InternalRecord, KeyRange, LogicalShardEntry, RecordKind, SnapshotHandle,
    WalRecordType, build_level_stats, build_logical_shard_stats, validate_key,
    validate_logical_shards, validate_page_limits, validate_page_size_bytes, validate_value,
};

use crate::common::{StoreOptions, create_store, store_dir};

fn record(seqno: u64, key: &[u8], kind: RecordKind, value: Option<&[u8]>) -> InternalRecord {
    InternalRecord {
        user_key: key.to_vec(),
        seqno,
        kind,
        value: value.map(|bytes| bytes.to_vec()),
    }
}

#[test]
fn error_kinds_retryability_and_conversion_match_public_contract() {
    let legacy_cases = [
        (
            KalanjiyamError::Busy("busy".to_string()),
            ErrorKind::Busy,
            "Busy",
            "busy",
            true,
        ),
        (
            KalanjiyamError::InvalidArgument("bad".to_string()),
            ErrorKind::InvalidArgument,
            "InvalidArgument",
            "bad",
            false,
        ),
        (
            KalanjiyamError::Checksum("crc".to_string()),
            ErrorKind::Checksum,
            "Checksum",
            "crc",
            false,
        ),
        (
            KalanjiyamError::Corruption("bad file".to_string()),
            ErrorKind::Corruption,
            "Corruption",
            "bad file",
            false,
        ),
        (
            KalanjiyamError::Stale("old generation".to_string()),
            ErrorKind::Stale,
            "Stale",
            "old generation",
            false,
        ),
        (
            KalanjiyamError::Cancelled("stop".to_string()),
            ErrorKind::Cancelled,
            "Cancelled",
            "stop",
            true,
        ),
    ];
    for (legacy, expected_kind, expected_code, expected_message, retryable) in legacy_cases {
        assert_eq!(legacy.kind(), expected_kind);
        assert_eq!(legacy.code_name(), expected_code);
        assert_eq!(legacy.message(), expected_message);
        assert_eq!(legacy.retryable(), retryable);

        let converted: Error = legacy.into();
        assert_eq!(converted.kind(), expected_kind);
        assert_eq!(converted.message(), expected_message);
        assert_eq!(converted.retryable(), retryable);
    }

    let error_cases = [
        (Error::busy("busy"), ErrorKind::Busy, "busy", true),
        (
            Error::invalid_argument("bad"),
            ErrorKind::InvalidArgument,
            "bad",
            false,
        ),
        (Error::io("io"), ErrorKind::Io, "io", true),
        (Error::checksum("crc"), ErrorKind::Checksum, "crc", false),
        (
            Error::corruption("corrupt"),
            ErrorKind::Corruption,
            "corrupt",
            false,
        ),
        (Error::stale("stale"), ErrorKind::Stale, "stale", false),
        (
            Error::cancelled("cancelled"),
            ErrorKind::Cancelled,
            "cancelled",
            true,
        ),
    ];
    for (error, expected_kind, expected_message, retryable) in error_cases {
        assert_eq!(error.kind(), expected_kind);
        assert_eq!(error.message(), expected_message);
        assert_eq!(error.retryable(), retryable);
    }
}

#[test]
fn error_display_and_sources_cover_legacy_and_new_variants() {
    let legacy = KalanjiyamError::Io(std::io::Error::other("disk full"));
    assert!(legacy.source().is_some());
    assert_eq!(legacy.to_string(), "io: disk full");
    assert_eq!(legacy.kind(), ErrorKind::Io);
    assert_eq!(legacy.code_name(), "IO");
    assert_eq!(legacy.message(), "disk full");
    assert!(legacy.retryable());

    let checksum = Error::checksum("bad crc");
    assert_eq!(checksum.to_string(), "Checksum: bad crc");
    assert!(checksum.source().is_none());
    let from_io: Error = std::io::Error::other("broken").into();
    assert_eq!(from_io.kind(), ErrorKind::Io);
    assert_eq!(from_io.message(), "broken");
    assert_eq!(from_io.to_string(), "IO: broken");
}

#[test]
fn legacy_io_error_helper_preserves_context_in_message() {
    let legacy = KalanjiyamError::io("sync wal", std::io::Error::other("disk full"));
    assert_eq!(legacy.kind(), ErrorKind::Io);
    assert_eq!(legacy.code_name(), "IO");
    assert_eq!(legacy.message(), "sync wal: disk full");
    assert!(legacy.retryable());
    assert_eq!(legacy.to_string(), "io: sync wal: disk full");
}

#[test]
fn codec_current_round_trip_and_canonical_paths_match_spec_shapes() {
    let current = CurrentFile {
        checkpoint_generation: 9,
        checkpoint_max_seqno: 140,
        checkpoint_data_generation: 12,
    };
    let bytes = encode_current(&current);
    let decoded = decode_current(&bytes).expect("CURRENT round-trip should succeed");
    assert_eq!(decoded, current);

    assert_eq!(
        wal_relative_path(1, Some(2)),
        PathBuf::from("wal/wal-00000000000000000001-00000000000000000002.log")
    );
    assert_eq!(
        wal_relative_path(empty_open_segment_first_seqno(), None),
        PathBuf::from("wal/wal-18446744073709551615-open.log")
    );
    assert_eq!(
        data_relative_path(42),
        PathBuf::from("data/00000000000000000042.kjm")
    );
    assert_eq!(
        metadata_relative_path(9),
        PathBuf::from("meta/meta-00000000000000000009.kjm")
    );
    assert_eq!(current_relative_path(), PathBuf::from("CURRENT"));
}

#[test]
fn wal_put_and_delete_payloads_round_trip_without_logical_shard_identifier() {
    let put = WalRecord {
        seqno: 42,
        payload: WalRecordPayload::Put {
            key: b"yak".to_vec(),
            value: b"value-two".to_vec(),
        },
    };
    let delete = WalRecord {
        seqno: 43,
        payload: WalRecordPayload::Delete {
            key: b"yak".to_vec(),
        },
    };

    assert_eq!(
        decode_wal_record(&encode_wal_record(&put).expect("Put encode should succeed"))
            .expect("Put decode should succeed"),
        put
    );
    assert_eq!(
        decode_wal_record(&encode_wal_record(&delete).expect("Delete encode should succeed"))
            .expect("Delete decode should succeed"),
        delete
    );
    assert!(matches!(
        WalRecordType::from_u8(9),
        Err(KalanjiyamError::Corruption(_))
    ));
}

#[test]
fn type_helpers_enforce_order_limits_and_stats_shapes() {
    assert!(Bound::NegInf < Bound::Finite(b"ant".to_vec()));
    assert!(Bound::Finite(b"ant".to_vec()) < Bound::PosInf);
    assert!(validate_key(b"a").is_ok());
    assert!(validate_value(b"ok").is_ok());
    assert!(matches!(
        validate_key(&[]),
        Err(KalanjiyamError::InvalidArgument(_))
    ));
    assert!(matches!(
        KeyRange {
            start_bound: Bound::PosInf,
            end_bound: Bound::PosInf,
        }
        .validate(),
        Err(KalanjiyamError::InvalidArgument(_))
    ));

    let levels = vec![vec![FileMeta {
        file_id: 1,
        level_no: 0,
        min_user_key: b"a".to_vec(),
        max_user_key: b"z".to_vec(),
        min_seqno: 1,
        max_seqno: 1,
        entry_count: 1,
        logical_bytes: 7,
        physical_bytes: 9,
    }]];
    let level_stats = build_level_stats(&levels);
    assert_eq!(level_stats[0].file_count, 1);
    assert_eq!(level_stats[0].logical_bytes, 7);

    let logical_stats = build_logical_shard_stats(&[LogicalShardEntry {
        start_bound: Bound::NegInf,
        end_bound: Bound::PosInf,
        live_size_bytes: 11,
    }]);
    assert_eq!(logical_stats[0].live_size_bytes, 11);
}

#[test]
fn durable_data_and_checkpoint_round_trip_supports_point_reads_and_scans() {
    let (_tempdir, config_path) = create_store(&StoreOptions::default());
    let store_dir = store_dir(&config_path);
    let records = vec![
        record(9, b"yak", RecordKind::Delete, None),
        record(8, b"yak", RecordKind::Put, Some(b"v2")),
        record(3, b"yak", RecordKind::Put, Some(b"v1")),
        record(2, b"ant", RecordKind::Put, Some(b"v-ant")),
    ];
    let meta = build_file_meta(1, 0, &records);
    write_data_file(&store_dir, &meta, &records, 4096).expect("data file write should succeed");

    let parsed = load_data_file(&store_dir, &meta).expect("data file load should succeed");
    assert_eq!(
        find_visible_record_in_file(&parsed, b"yak", 8)
            .expect("point read should succeed")
            .expect("snapshot 8 should see put")
            .value,
        Some(b"v2".to_vec())
    );
    assert_eq!(
        find_visible_record_in_file(&parsed, b"yak", 9)
            .expect("point read should succeed")
            .expect("snapshot 9 should see delete")
            .kind,
        RecordKind::Delete
    );

    let (rows, eof, next_resume) =
        scan_visible_rows_in_file(&parsed, None, None, None, 8, 10, 1024)
            .expect("scan should succeed");
    assert!(eof);
    assert_eq!(next_resume, None);
    assert_eq!(rows.len(), 2);

    let state = CheckpointState {
        checkpoint_max_seqno: 17,
        next_seqno: 18,
        next_file_id: 4,
        checkpoint_data_generation: 2,
        manifest_files: vec![FileMeta {
            physical_bytes: fs::metadata(resolve_store_path(&store_dir, &data_relative_path(1)))
                .expect("flushed file should exist")
                .len(),
            ..meta
        }],
        logical_shards: vec![LogicalShardEntry {
            start_bound: Bound::NegInf,
            end_bound: Bound::PosInf,
            live_size_bytes: 12,
        }],
    };
    write_checkpoint_file(&store_dir, 1, &state, 4096).expect("checkpoint write should succeed");
    let current = CurrentFile {
        checkpoint_generation: 1,
        checkpoint_max_seqno: 17,
        checkpoint_data_generation: 2,
    };
    install_current(&store_dir, &current).expect("CURRENT install should succeed");
    let loaded =
        load_checkpoint_file(&store_dir, &current).expect("checkpoint load should succeed");
    assert_eq!(loaded, state);
}

#[test]
fn durable_snapshot_helper_matches_engine_instance_id() {
    let handle = SnapshotHandle {
        engine_instance_id: 7,
        snapshot_id: 1,
        snapshot_seqno: 9,
        data_generation: 3,
    };
    assert!(snapshot_matches_engine(&handle, 7));
    assert!(!snapshot_matches_engine(&handle, 8));
}

#[test]
fn config_paths_are_canonicalized_through_engine_open() {
    let (_tempdir, config_path) = create_store(&StoreOptions::default());
    let nested = config_path
        .parent()
        .expect("config path should have a parent")
        .join(".")
        .join("config.toml");
    let engine = crate::common::run_async(kalanjiyam::PezhaiEngine::open(&nested))
        .expect("open should canonicalize config and store paths");

    assert_eq!(
        engine.state().config.config_path,
        fs::canonicalize(&config_path).expect("config should exist")
    );
    assert_eq!(
        engine.state().config.store_dir,
        fs::canonicalize(store_dir(&config_path)).expect("store dir should exist")
    );
}

#[test]
fn config_defaults_are_applied_on_open() {
    let (_tempdir, config_path) = create_store(&StoreOptions::default());
    let engine = crate::common::run_async(PezhaiEngine::open(&config_path))
        .expect("open should succeed for default config");
    let config = &engine.state().config;

    assert_eq!(config.engine.sync_mode.to_string(), "manual");
    assert_eq!(config.engine.page_size_bytes, 4096);
    assert_eq!(config.wal.segment_bytes, 1_073_741_824);
    assert_eq!(config.wal.group_commit_bytes, 65_536);
    assert_eq!(config.wal.group_commit_max_delay_ms, 50);
    assert_eq!(config.lsm.memtable_flush_bytes, 67_108_864);
    assert_eq!(config.maintenance.max_external_requests, 64);
    assert_eq!(config.maintenance.checkpoint_interval_secs, 300);
}

#[test]
fn config_rejects_invalid_page_size_wal_lsm_and_maintenance_values() {
    let cases = [
        "[engine]\npage_size_bytes = 12288\n",
        "[wal]\nsegment_bytes = 100\ngroup_commit_bytes = 10\ngroup_commit_max_delay_ms = 1\n",
        "[wal]\nsegment_bytes = 4096\ngroup_commit_bytes = 4096\ngroup_commit_max_delay_ms = 1\n",
        "[wal]\ngroup_commit_max_delay_ms = 0\n",
        "[lsm]\nmemtable_flush_bytes = 100\nbase_level_bytes = 100\nlevel_fanout = 1\n",
        "[lsm]\nbase_level_bytes = 4095\n",
        "[lsm]\nl0_file_threshold = 1\n",
        "[lsm]\nmax_levels = 3\n",
        "[maintenance]\nmax_external_requests = 0\n",
        "[maintenance]\nmax_scan_fetch_queue = 0\n",
        "[maintenance]\nmax_scan_sessions = 0\n",
        "[maintenance]\nmax_worker_tasks = 0\n",
        "[maintenance]\nmax_waiting_wal_syncs = 0\n",
        "[maintenance]\nscan_expiry_ms = 0\n",
        "[maintenance]\nretry_delay_ms = 0\n",
        "[maintenance]\nworker_threads = 0\n",
        "[maintenance]\ngc_interval_secs = 0\n",
        "[maintenance]\ncheckpoint_interval_secs = 0\n",
    ];

    for body in cases {
        let (_tempdir, config_path) = create_store(&StoreOptions::default());
        fs::write(&config_path, body).expect("fixture should rewrite config");
        let error =
            crate::common::run_async(PezhaiEngine::open(&config_path)).expect_err("open must fail");
        assert!(matches!(error, KalanjiyamError::InvalidArgument(_)));
    }
}

#[test]
fn config_rejects_non_table_and_overflow_values() {
    for body in [
        "engine = 1\n",
        "[engine]\nsync_mode = 1\n",
        "[engine]\nsync_mode = \"fast\"\n",
        "[wal]\nsegment_bytes = -1\n",
        "[wal]\ngroup_commit_bytes = \"big\"\n",
        "[lsm]\nlevel_fanout = 4294967296\n",
        "[lsm]\nmax_levels = \"many\"\n",
        "[maintenance]\nmax_worker_tasks = 9999999999999999999\n",
        "[maintenance]\nworker_threads = \"two\"\n",
    ] {
        let (_tempdir, config_path) = create_store(&StoreOptions::default());
        fs::write(&config_path, body).expect("fixture should rewrite config");
        let error =
            crate::common::run_async(PezhaiEngine::open(&config_path)).expect_err("open must fail");
        assert!(matches!(error, KalanjiyamError::InvalidArgument(_)));
    }
}

#[test]
fn config_accepts_manual_sync_mode_and_unknown_keys() {
    let (_tempdir, config_path) = create_store(&StoreOptions::default());
    fs::write(
        &config_path,
        "[engine]\nsync_mode = \"manual\"\npage_size_bytes = 8192\nunknown_key = 1\n[maintenance]\ncheckpoint_interval_secs = 42\n[unknown]\nfoo = \"bar\"\n",
    )
    .expect("fixture should rewrite config");
    let engine =
        crate::common::run_async(PezhaiEngine::open(&config_path)).expect("open should succeed");
    assert_eq!(engine.state().config.engine.sync_mode.to_string(), "manual");
    assert_eq!(engine.state().config.engine.page_size_bytes, 8192);
    assert_eq!(
        engine.state().config.maintenance.checkpoint_interval_secs,
        42
    );
}

#[test]
fn type_validation_helpers_cover_page_limits_and_logical_shards() {
    assert!(validate_page_size_bytes(4096).is_ok());
    assert!(validate_page_size_bytes(32 * 1024).is_ok());
    assert!(matches!(
        validate_page_size_bytes(12 * 1024),
        Err(KalanjiyamError::InvalidArgument(_))
    ));
    assert!(validate_page_limits(1, 1).is_ok());
    assert!(matches!(
        validate_page_limits(0, 1),
        Err(KalanjiyamError::InvalidArgument(_))
    ));

    let shards = vec![
        LogicalShardEntry {
            start_bound: Bound::NegInf,
            end_bound: Bound::Finite(b"m".to_vec()),
            live_size_bytes: 1,
        },
        LogicalShardEntry {
            start_bound: Bound::Finite(b"n".to_vec()),
            end_bound: Bound::PosInf,
            live_size_bytes: 2,
        },
    ];
    assert!(matches!(
        validate_logical_shards(&shards),
        Err(KalanjiyamError::Corruption(_))
    ));
}

#[test]
fn wal_header_decoder_rejects_corrupt_magic() {
    let mut header = kalanjiyam::pezhai::codec::encode_wal_segment_header(
        &kalanjiyam::pezhai::codec::WalSegmentHeader {
            first_seqno_or_none: 1,
            segment_bytes: 1_073_741_824,
        },
    );
    header[0] = 0;
    let result = decode_wal_segment_header(&header);
    assert!(matches!(
        result,
        Err(KalanjiyamError::Checksum(_)) | Err(KalanjiyamError::Corruption(_))
    ));
}
