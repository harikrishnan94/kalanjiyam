//! Integration matrix for production engine workflows and recovery behavior.

use std::fs;

use kalanjiyam::PezhaiEngine;
use kalanjiyam::error::KalanjiyamError;
use kalanjiyam::pezhai::codec::{CurrentFile, encode_current, encode_wal_segment_header};
use kalanjiyam::pezhai::engine::{ReadDecision, SyncDecision};
use kalanjiyam::pezhai::types::{Bound, KeyRange, LogicalShardEntry};

use crate::common::{StoreOptions, create_store, run_async, store_dir};

#[test]
fn open_without_recovered_logical_shard_map_initializes_full_range() {
    let (_tempdir, config_path) = create_store(&StoreOptions::default());
    let engine = run_async(PezhaiEngine::open(&config_path))
        .expect("open should succeed on a valid empty store");
    let shards = &engine.state().current_logical_shards;

    assert_eq!(shards.len(), 1);
    assert_eq!(shards[0].start_bound, Bound::NegInf);
    assert_eq!(shards[0].end_bound, Bound::PosInf);
}

#[test]
fn release_snapshot_rejects_second_release_with_invalid_argument() {
    let (_tempdir, config_path) = create_store(&StoreOptions::default());
    let mut engine = run_async(PezhaiEngine::open(&config_path))
        .expect("open should succeed on a valid store fixture");

    let snapshot =
        run_async(engine.create_snapshot()).expect("create_snapshot should succeed on open engine");
    run_async(engine.release_snapshot(&snapshot)).expect("first snapshot release should succeed");
    let second_release = run_async(engine.release_snapshot(&snapshot));

    assert!(matches!(
        second_release,
        Err(KalanjiyamError::InvalidArgument(_))
    ));
}

#[test]
fn orphan_data_file_is_ignored_during_open() {
    let (_tempdir, config_path) = create_store(&StoreOptions::default());
    let orphan_path = store_dir(&config_path).join("data/00000000000000000042.kjm");
    fs::write(&orphan_path, b"orphan").expect("test fixture should write orphan data file bytes");

    let engine = run_async(PezhaiEngine::open(&config_path))
        .expect("open should ignore orphan data files that are not referenced");
    let stats = run_async(engine.stats()).expect("stats should succeed immediately after open");
    assert!(!stats.logical_shards.is_empty());
}

#[test]
fn engine_sync_waits_once_then_becomes_immediate_in_manual_mode() {
    let (_tempdir, config_path) = create_store(&StoreOptions::default());
    let mut engine = run_async(PezhaiEngine::open(&config_path)).expect("open should succeed");

    run_async(engine.put(b"ant".to_vec(), b"value-one".to_vec())).expect("put should succeed");
    let first = run_async(engine.sync()).expect("first sync should succeed");
    let second = run_async(engine.sync()).expect("second sync should succeed");

    assert_eq!(first.durable_seqno, engine.state().last_committed_seqno);
    assert_eq!(second.durable_seqno, first.durable_seqno);
}

#[test]
fn first_live_write_renames_active_wal_away_from_none_u64_filename() {
    let (_tempdir, config_path) = create_store(&StoreOptions::default());
    let mut engine = run_async(PezhaiEngine::open(&config_path)).expect("open should succeed");

    run_async(engine.put(b"ant".to_vec(), b"value-one".to_vec())).expect("put should succeed");

    let active_name = engine
        .active_wal_path()
        .file_name()
        .and_then(|name| name.to_str())
        .expect("active WAL path should have one UTF-8 filename");
    assert_eq!(active_name, "wal-00000000000000000001-open.log");
}

#[test]
fn append_path_io_after_seqno_allocation_closes_write_admission() {
    let (_tempdir, config_path) = create_store(&StoreOptions::default());
    let mut engine = run_async(PezhaiEngine::open(&config_path)).expect("open should succeed");

    fs::remove_file(engine.active_wal_path()).expect("test fixture should remove active WAL file");

    let first = run_async(engine.put(b"ant".to_vec(), b"value-one".to_vec()));
    assert!(matches!(first, Err(KalanjiyamError::Io(_))));

    let second = run_async(engine.put(b"bee".to_vec(), b"value-two".to_vec()));
    assert!(matches!(second, Err(KalanjiyamError::Io(_))));
}

#[test]
fn engine_scan_omits_tombstones_and_respects_half_open_range() {
    let (_tempdir, config_path) = create_store(&StoreOptions::default());
    let mut engine = run_async(PezhaiEngine::open(&config_path)).expect("open should succeed");

    run_async(engine.put(b"ant".to_vec(), b"value-one".to_vec())).expect("put should succeed");
    run_async(engine.put(b"bee".to_vec(), b"value-two".to_vec())).expect("put should succeed");
    run_async(engine.put(b"cat".to_vec(), b"value-three".to_vec())).expect("put should succeed");
    run_async(engine.delete(b"bee".to_vec())).expect("delete should succeed");

    let rows = run_async(engine.scan(
        KeyRange {
            start_bound: Bound::Finite(b"ant".to_vec()),
            end_bound: Bound::Finite(b"cat".to_vec()),
        },
        None,
    ))
    .expect("scan should succeed");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].key, b"ant".to_vec());
}

#[test]
fn install_logical_shards_rejects_noop_and_outer_bound_changes() {
    let (_tempdir, config_path) = create_store(&StoreOptions::default());
    let mut engine = run_async(PezhaiEngine::open(&config_path)).expect("open should succeed");
    let source = vec![LogicalShardEntry {
        start_bound: Bound::NegInf,
        end_bound: Bound::PosInf,
        live_size_bytes: 0,
    }];

    let noop = engine.install_logical_shards(source.clone(), source.clone());
    assert!(matches!(noop, Err(KalanjiyamError::InvalidArgument(_))));

    let changed_outer_bounds = engine.install_logical_shards(
        source,
        vec![LogicalShardEntry {
            start_bound: Bound::Finite(b"ant".to_vec()),
            end_bound: Bound::PosInf,
            live_size_bytes: 0,
        }],
    );
    assert!(matches!(
        changed_outer_bounds,
        Err(KalanjiyamError::InvalidArgument(_))
    ));
}

#[test]
fn install_logical_shards_rejects_non_contiguous_output_ranges() {
    let (_tempdir, config_path) = create_store(&StoreOptions::default());
    let mut engine = run_async(PezhaiEngine::open(&config_path)).expect("open should succeed");
    let result = engine.install_logical_shards(
        vec![LogicalShardEntry {
            start_bound: Bound::NegInf,
            end_bound: Bound::PosInf,
            live_size_bytes: 0,
        }],
        vec![
            LogicalShardEntry {
                start_bound: Bound::NegInf,
                end_bound: Bound::Finite(b"m".to_vec()),
                live_size_bytes: 0,
            },
            LogicalShardEntry {
                start_bound: Bound::Finite(b"n".to_vec()),
                end_bound: Bound::PosInf,
                live_size_bytes: 0,
            },
        ],
    );
    assert!(matches!(result, Err(KalanjiyamError::Corruption(_))));
}

#[test]
fn install_logical_shards_rejects_entry_sets_larger_than_two() {
    let (_tempdir, config_path) = create_store(&StoreOptions::default());
    let mut engine = run_async(PezhaiEngine::open(&config_path)).expect("open should succeed");

    let result = engine.install_logical_shards(
        vec![
            LogicalShardEntry {
                start_bound: Bound::NegInf,
                end_bound: Bound::Finite(b"g".to_vec()),
                live_size_bytes: 0,
            },
            LogicalShardEntry {
                start_bound: Bound::Finite(b"g".to_vec()),
                end_bound: Bound::Finite(b"n".to_vec()),
                live_size_bytes: 0,
            },
            LogicalShardEntry {
                start_bound: Bound::Finite(b"n".to_vec()),
                end_bound: Bound::PosInf,
                live_size_bytes: 0,
            },
        ],
        vec![LogicalShardEntry {
            start_bound: Bound::NegInf,
            end_bound: Bound::PosInf,
            live_size_bytes: 0,
        }],
    );

    assert!(matches!(result, Err(KalanjiyamError::InvalidArgument(_))));
}

#[test]
fn stale_logical_split_is_rejected_when_source_range_set_changes() {
    let (_tempdir, config_path) = create_store(&StoreOptions::default());
    let mut engine = run_async(PezhaiEngine::open(&config_path)).expect("open should succeed");

    let source = vec![LogicalShardEntry {
        start_bound: Bound::NegInf,
        end_bound: Bound::PosInf,
        live_size_bytes: 0,
    }];
    engine
        .install_logical_shards(
            source.clone(),
            vec![
                LogicalShardEntry {
                    start_bound: Bound::NegInf,
                    end_bound: Bound::Finite(b"m".to_vec()),
                    live_size_bytes: 0,
                },
                LogicalShardEntry {
                    start_bound: Bound::Finite(b"m".to_vec()),
                    end_bound: Bound::PosInf,
                    live_size_bytes: 0,
                },
            ],
        )
        .expect("first logical split should succeed");

    let stale = engine.install_logical_shards(
        source,
        vec![
            LogicalShardEntry {
                start_bound: Bound::NegInf,
                end_bound: Bound::Finite(b"n".to_vec()),
                live_size_bytes: 0,
            },
            LogicalShardEntry {
                start_bound: Bound::Finite(b"n".to_vec()),
                end_bound: Bound::PosInf,
                live_size_bytes: 0,
            },
        ],
    );
    assert!(matches!(stale, Err(KalanjiyamError::Stale(_))));
}

#[test]
fn checkpoint_round_trip_captures_manifest_and_current_logical_shards_after_drain() {
    let (_tempdir, config_path) = create_store(&StoreOptions::default());
    let mut engine = run_async(PezhaiEngine::open(&config_path)).expect("open should succeed");
    run_async(engine.put(b"ant".to_vec(), b"value-one".to_vec())).expect("put should succeed");
    engine.checkpoint().expect("checkpoint should succeed");
    drop(engine);

    let reopened = run_async(PezhaiEngine::open(&config_path))
        .expect("reopen should succeed after checkpoint");
    let rows = run_async(reopened.scan(KeyRange::default(), None)).expect("scan should succeed");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].key, b"ant".to_vec());
}

#[test]
fn force_freeze_and_plan_capture_return_none_on_empty_engine() {
    let (_tempdir, config_path) = create_store(&StoreOptions::default());
    let mut engine = run_async(PezhaiEngine::open(&config_path)).expect("open should succeed");
    assert_eq!(engine.force_freeze().expect("freeze should succeed"), None);
    assert_eq!(
        engine.capture_flush_plan().expect("capture should succeed"),
        None
    );
    assert_eq!(
        engine
            .capture_compact_plan()
            .expect("capture should succeed"),
        None
    );
}

#[test]
fn capture_checkpoint_plan_requires_empty_memtables() {
    let (_tempdir, config_path) = create_store(&StoreOptions::default());
    let mut engine = run_async(PezhaiEngine::open(&config_path)).expect("open should succeed");
    run_async(engine.put(b"ant".to_vec(), b"value-one".to_vec())).expect("put should succeed");
    let result = engine.capture_checkpoint_plan();
    assert!(matches!(result, Err(KalanjiyamError::Corruption(_))));
}

#[test]
fn scan_rejects_empty_range() {
    let (_tempdir, config_path) = create_store(&StoreOptions::default());
    let engine = run_async(PezhaiEngine::open(&config_path)).expect("open should succeed");
    let result = run_async(engine.scan(
        KeyRange {
            start_bound: Bound::Finite(b"ant".to_vec()),
            end_bound: Bound::Finite(b"ant".to_vec()),
        },
        None,
    ));
    assert!(matches!(result, Err(KalanjiyamError::InvalidArgument(_))));
}

#[test]
fn corrupt_current_or_wal_bytes_are_rejected_during_open() {
    let (_tempdir, config_path) = create_store(&StoreOptions::default());
    let store = store_dir(&config_path);

    let mut current_bytes = encode_current(&CurrentFile {
        checkpoint_generation: 1,
        checkpoint_max_seqno: 2,
        checkpoint_data_generation: 3,
    });
    current_bytes[0] = 0;
    fs::write(store.join("CURRENT"), current_bytes).expect("fixture should write corrupt CURRENT");

    let current_error = run_async(PezhaiEngine::open(&config_path));
    assert!(matches!(
        current_error,
        Err(KalanjiyamError::Checksum(_)) | Err(KalanjiyamError::Corruption(_))
    ));

    fs::remove_file(store.join("CURRENT")).expect("fixture should clear CURRENT");
    let mut wal_header = encode_wal_segment_header(&kalanjiyam::pezhai::codec::WalSegmentHeader {
        first_seqno_or_none: 999,
        segment_bytes: 1_073_741_824,
    });
    wal_header[0] = 0;
    fs::write(
        store.join("wal/wal-18446744073709551615-open.log"),
        wal_header,
    )
    .expect("fixture should overwrite WAL header");

    let wal_error = run_async(PezhaiEngine::open(&config_path));
    assert!(matches!(
        wal_error,
        Err(KalanjiyamError::Checksum(_)) | Err(KalanjiyamError::Corruption(_))
    ));
}

#[test]
fn reopen_replays_put_delete_and_logical_install_records() {
    let (_tempdir, config_path) = create_store(&StoreOptions::default());
    let mut engine = run_async(PezhaiEngine::open(&config_path)).expect("open should succeed");

    run_async(engine.put(b"ant".to_vec(), b"value-one".to_vec())).expect("put should succeed");
    run_async(engine.put(b"yak".to_vec(), b"value-two".to_vec())).expect("put should succeed");
    run_async(engine.delete(b"ant".to_vec())).expect("delete should succeed");
    engine
        .install_logical_shards(
            vec![LogicalShardEntry {
                start_bound: Bound::NegInf,
                end_bound: Bound::PosInf,
                live_size_bytes: 0,
            }],
            vec![
                LogicalShardEntry {
                    start_bound: Bound::NegInf,
                    end_bound: Bound::Finite(b"m".to_vec()),
                    live_size_bytes: 0,
                },
                LogicalShardEntry {
                    start_bound: Bound::Finite(b"m".to_vec()),
                    end_bound: Bound::PosInf,
                    live_size_bytes: 0,
                },
            ],
        )
        .expect("logical shard install should succeed");
    run_async(engine.sync()).expect("sync should succeed");
    drop(engine);

    let reopened = run_async(PezhaiEngine::open(&config_path)).expect("reopen should succeed");
    let ant = run_async(reopened.get(b"ant".to_vec(), None)).expect("get should succeed");
    let yak = run_async(reopened.get(b"yak".to_vec(), None)).expect("get should succeed");
    assert!(!ant.found);
    assert_eq!(yak.value, Some(b"value-two".to_vec()));
    assert_eq!(reopened.state().current_logical_shards.len(), 2);
}

#[test]
fn non_default_page_size_round_trips_across_checkpoint_and_reopen() {
    let options = StoreOptions {
        page_size_bytes: 8192,
        ..StoreOptions::default()
    };
    let (_tempdir, config_path) = create_store(&options);
    let mut engine = run_async(PezhaiEngine::open(&config_path)).expect("open should succeed");
    run_async(engine.put(b"ant".to_vec(), b"value-one".to_vec())).expect("put should succeed");
    engine.checkpoint().expect("checkpoint should succeed");
    drop(engine);

    let reopened = run_async(PezhaiEngine::open(&config_path)).expect("reopen should succeed");
    let get = run_async(reopened.get(b"ant".to_vec(), None)).expect("get should succeed");
    assert_eq!(reopened.state().config.engine.page_size_bytes, 8192);
    assert_eq!(get.value, Some(b"value-one".to_vec()));
}

#[test]
fn snapshot_reads_ignore_later_logical_shard_changes() {
    let (_tempdir, config_path) = create_store(&StoreOptions::default());
    let mut engine = run_async(PezhaiEngine::open(&config_path)).expect("open should succeed");
    run_async(engine.put(b"ant".to_vec(), b"value-one".to_vec())).expect("put should succeed");
    run_async(engine.put(b"yak".to_vec(), b"value-two".to_vec())).expect("put should succeed");
    let snapshot = run_async(engine.create_snapshot()).expect("snapshot should succeed");

    engine
        .install_logical_shards(
            vec![LogicalShardEntry {
                start_bound: Bound::NegInf,
                end_bound: Bound::PosInf,
                live_size_bytes: 0,
            }],
            vec![
                LogicalShardEntry {
                    start_bound: Bound::NegInf,
                    end_bound: Bound::Finite(b"m".to_vec()),
                    live_size_bytes: 0,
                },
                LogicalShardEntry {
                    start_bound: Bound::Finite(b"m".to_vec()),
                    end_bound: Bound::PosInf,
                    live_size_bytes: 0,
                },
            ],
        )
        .expect("logical shard install should succeed");

    let get = run_async(engine.get(b"yak".to_vec(), Some(&snapshot))).expect("get should succeed");
    let scan =
        run_async(engine.scan(KeyRange::default(), Some(&snapshot))).expect("scan should succeed");
    assert_eq!(get.value, Some(b"value-two".to_vec()));
    assert_eq!(scan.len(), 2);
}

#[test]
fn snapshot_before_flush_reads_pre_flush_generation() {
    let (_tempdir, config_path) = create_store(&StoreOptions::default());
    let mut engine = run_async(PezhaiEngine::open(&config_path)).expect("open should succeed");
    run_async(engine.put(b"ant".to_vec(), b"value-one".to_vec())).expect("put should succeed");
    let snapshot = run_async(engine.create_snapshot()).expect("snapshot should succeed");
    let frozen = engine.force_freeze().expect("freeze should succeed");
    assert!(frozen.is_some());
    let plan = engine
        .capture_flush_plan()
        .expect("flush plan capture should succeed")
        .expect("flush plan should exist");
    engine
        .publish_flush_plan(&plan)
        .expect("flush publish should succeed");

    let get = run_async(engine.get(b"ant".to_vec(), Some(&snapshot))).expect("get should succeed");
    assert_eq!(get.value, Some(b"value-one".to_vec()));
}

#[test]
fn flush_and_compaction_publish_advance_data_generation() {
    let (_tempdir, config_path) = create_store(&StoreOptions::default());
    let mut engine = run_async(PezhaiEngine::open(&config_path)).expect("open should succeed");
    run_async(engine.put(b"ant".to_vec(), b"value-one".to_vec())).expect("put should succeed");
    run_async(engine.put(b"bee".to_vec(), b"value-two".to_vec())).expect("put should succeed");
    engine.force_freeze().expect("freeze should succeed");
    let generation_before = engine.state().data_generation;
    let flush_a = engine
        .capture_flush_plan()
        .expect("flush plan capture should succeed")
        .expect("flush plan should exist");
    let meta = engine
        .publish_flush_plan(&flush_a)
        .expect("flush publish should succeed")
        .expect("flush publish should return metadata");
    assert_eq!(meta.level_no, 0);
    assert_eq!(engine.state().data_generation, generation_before + 1);

    run_async(engine.put(b"cat".to_vec(), b"value-three".to_vec())).expect("put should succeed");
    engine.force_freeze().expect("second freeze should succeed");
    let flush_b = engine
        .capture_flush_plan()
        .expect("flush plan capture should succeed")
        .expect("flush plan should exist");
    engine
        .publish_flush_plan(&flush_b)
        .expect("flush publish should succeed");

    let compact_plan = engine
        .capture_compact_plan()
        .expect("compact plan capture should succeed")
        .expect("compact plan should exist");
    let compact_generation_before = engine.state().data_generation;
    let compacted = engine
        .publish_compact_plan(&compact_plan)
        .expect("compaction publish should succeed")
        .expect("compaction publish should return metadata");
    assert_eq!(compacted.level_no, 1);
    assert_eq!(
        engine.state().data_generation,
        compact_generation_before + 1
    );
}

#[test]
fn stale_flush_and_compaction_plans_are_rejected_after_new_publication() {
    let (_tempdir, config_path) = create_store(&StoreOptions::default());
    let mut engine = run_async(PezhaiEngine::open(&config_path)).expect("open should succeed");
    run_async(engine.put(b"ant".to_vec(), b"value-one".to_vec())).expect("put should succeed");
    engine.force_freeze().expect("freeze should succeed");
    let stale_flush = engine
        .capture_flush_plan()
        .expect("flush plan capture should succeed")
        .expect("flush plan should exist");
    engine
        .publish_flush_plan(&stale_flush)
        .expect("flush publish should succeed");
    let stale_flush_result = engine.publish_flush_plan(&stale_flush);
    assert!(matches!(stale_flush_result, Err(KalanjiyamError::Stale(_))));

    run_async(engine.put(b"bee".to_vec(), b"value-two".to_vec())).expect("put should succeed");
    engine.force_freeze().expect("second freeze should succeed");
    let flush_b = engine
        .capture_flush_plan()
        .expect("flush plan capture should succeed")
        .expect("flush plan should exist");
    engine
        .publish_flush_plan(&flush_b)
        .expect("flush publish should succeed");
    let stale_compact = engine
        .capture_compact_plan()
        .expect("compact plan capture should succeed")
        .expect("compact plan should exist");

    run_async(engine.put(b"cat".to_vec(), b"value-three".to_vec())).expect("put should succeed");
    engine.force_freeze().expect("third freeze should succeed");
    let flush_c = engine
        .capture_flush_plan()
        .expect("flush plan capture should succeed")
        .expect("flush plan should exist");
    engine
        .publish_flush_plan(&flush_c)
        .expect("flush publish should succeed");

    let stale_compact_result = engine.publish_compact_plan(&stale_compact);
    assert!(matches!(
        stale_compact_result,
        Err(KalanjiyamError::Stale(_))
    ));
}

#[test]
fn wal_sync_result_advances_frontier_and_fail_wal_sync_closes_write_admission() {
    let (_tempdir, config_path) = create_store(&StoreOptions::default());
    let mut engine = run_async(PezhaiEngine::open(&config_path)).expect("open should succeed");
    run_async(engine.put(b"ant".to_vec(), b"value-one".to_vec())).expect("put should succeed");
    run_async(engine.put(b"bee".to_vec(), b"value-two".to_vec())).expect("put should succeed");

    let first = engine
        .publish_wal_sync_result(1)
        .expect("first durable frontier publish should succeed");
    let repeated = engine
        .publish_wal_sync_result(1)
        .expect("repeated durable frontier publish should succeed");
    let advanced = engine
        .publish_wal_sync_result(2)
        .expect("advanced durable frontier publish should succeed");
    assert_eq!(first.durable_seqno, 1);
    assert_eq!(repeated.durable_seqno, 1);
    assert_eq!(advanced.durable_seqno, 2);
    assert!(matches!(
        engine.publish_wal_sync_result(3),
        Err(KalanjiyamError::Corruption(_))
    ));

    let error = engine.fail_wal_sync("sync active wal", std::io::Error::other("disk full"));
    assert!(matches!(error, KalanjiyamError::Io(_)));
    let write_result = run_async(engine.put(b"cat".to_vec(), b"value-three".to_vec()));
    assert!(matches!(write_result, Err(KalanjiyamError::Io(_))));
}

#[test]
fn engine_host_read_and_scan_decisions_switch_between_immediate_and_planned_paths() {
    let (_tempdir, config_path) = create_store(&StoreOptions::default());
    let mut engine = run_async(PezhaiEngine::open(&config_path)).expect("open should succeed");

    match engine
        .decide_current_read(b"yak")
        .expect("empty latest read decision should succeed")
    {
        ReadDecision::ImmediateGet(response) => {
            assert!(!response.found);
            assert_eq!(response.value, None);
        }
        other => panic!("expected ImmediateGet miss, got {other:?}"),
    }

    run_async(engine.put(b"ant".to_vec(), b"value-one".to_vec())).expect("put should succeed");
    match engine
        .decide_current_read(b"ant")
        .expect("active-memtable read decision should succeed")
    {
        ReadDecision::ImmediateGet(response) => {
            assert!(response.found);
            assert_eq!(response.value, Some(b"value-one".to_vec()));
        }
        other => panic!("expected ImmediateGet hit, got {other:?}"),
    }

    let snapshot = run_async(engine.create_snapshot()).expect("snapshot should succeed");
    match engine
        .decide_scan_page(1, &snapshot, &KeyRange::default(), None, 1, 1024)
        .expect("memtable-only scan decision should succeed")
    {
        ReadDecision::ImmediateScan(page) => {
            assert_eq!(page.rows.len(), 1);
            assert_eq!(page.rows[0].key, b"ant".to_vec());
        }
        other => panic!("expected ImmediateScan, got {other:?}"),
    }

    let frozen = engine.force_freeze().expect("freeze should succeed");
    assert!(frozen.is_some());
    let plan = engine
        .capture_flush_plan()
        .expect("flush plan capture should succeed")
        .expect("flush plan should exist");
    engine
        .publish_flush_plan(&plan)
        .expect("flush publish should succeed");

    match engine
        .decide_current_read(b"ant")
        .expect("file-backed read decision should succeed")
    {
        ReadDecision::GetPlan(plan) => {
            assert!(!plan.candidate_files.is_empty());
            assert!(plan.frozen_memtables.is_empty());
        }
        other => panic!("expected GetPlan, got {other:?}"),
    }

    let snapshot = run_async(engine.create_snapshot()).expect("snapshot should succeed");
    match engine
        .decide_scan_page(2, &snapshot, &KeyRange::default(), None, 1, 1024)
        .expect("file-backed scan decision should succeed")
    {
        ReadDecision::ScanPlan(plan) => {
            assert!(!plan.candidate_files.is_empty());
            assert!(plan.frozen_memtables.is_empty());
        }
        other => panic!("expected ScanPlan, got {other:?}"),
    }
}

#[test]
fn engine_sync_decision_switches_between_immediate_and_wait_states() {
    let (_tempdir, config_path) = create_store(&StoreOptions::default());
    let mut engine = run_async(PezhaiEngine::open(&config_path)).expect("open should succeed");

    match engine
        .decide_sync_wait()
        .expect("clean sync decision should succeed")
    {
        SyncDecision::Immediate(response) => assert_eq!(response.durable_seqno, 0),
        other => panic!("expected Immediate sync decision, got {other:?}"),
    }

    run_async(engine.put(b"ant".to_vec(), b"value-one".to_vec())).expect("put should succeed");
    match engine
        .decide_sync_wait()
        .expect("dirty sync decision should succeed")
    {
        SyncDecision::Wait(wait) => assert_eq!(wait.target_seqno, 1),
        other => panic!("expected Wait sync decision, got {other:?}"),
    }

    run_async(engine.sync()).expect("sync should succeed");
    match engine
        .decide_sync_wait()
        .expect("post-sync decision should succeed")
    {
        SyncDecision::Immediate(response) => assert_eq!(response.durable_seqno, 1),
        other => panic!("expected Immediate sync decision, got {other:?}"),
    }
}

#[test]
fn checkpoint_capture_and_publish_current_round_trip() {
    let (_tempdir, config_path) = create_store(&StoreOptions::default());
    let mut engine = run_async(PezhaiEngine::open(&config_path)).expect("open should succeed");
    run_async(engine.put(b"ant".to_vec(), b"value-one".to_vec())).expect("put should succeed");
    engine.force_freeze().expect("freeze should succeed");
    let flush = engine
        .capture_flush_plan()
        .expect("flush capture should succeed")
        .expect("flush plan should exist");
    engine
        .publish_flush_plan(&flush)
        .expect("flush publish should succeed");

    let plan = engine
        .capture_checkpoint_plan()
        .expect("checkpoint capture should succeed");
    let current = kalanjiyam::pezhai::codec::CurrentFile {
        checkpoint_generation: engine.state().next_checkpoint_generation,
        checkpoint_max_seqno: plan.checkpoint_max_seqno,
        checkpoint_data_generation: plan.manifest_generation,
    };
    let published = engine
        .publish_checkpoint_current(&plan, &current)
        .expect("checkpoint publish should succeed")
        .expect("checkpoint publish should return CURRENT");
    assert_eq!(published, current);

    let stale_plan = engine
        .capture_checkpoint_plan()
        .expect("checkpoint capture should succeed");
    run_async(engine.put(b"bee".to_vec(), b"value-two".to_vec())).expect("put should succeed");
    let stale_current = kalanjiyam::pezhai::codec::CurrentFile {
        checkpoint_generation: engine.state().next_checkpoint_generation,
        checkpoint_max_seqno: stale_plan.checkpoint_max_seqno,
        checkpoint_data_generation: stale_plan.manifest_generation,
    };
    let stale_result = engine.publish_checkpoint_current(&stale_plan, &stale_current);
    assert!(matches!(stale_result, Err(KalanjiyamError::Stale(_))));
}

#[test]
fn snapshot_from_previous_engine_instance_is_rejected_after_reopen() {
    let (_tempdir, config_path) = create_store(&StoreOptions::default());
    let mut engine = run_async(PezhaiEngine::open(&config_path)).expect("open should succeed");
    run_async(engine.put(b"ant".to_vec(), b"value-one".to_vec())).expect("put should succeed");
    let snapshot = run_async(engine.create_snapshot()).expect("snapshot should succeed");
    drop(engine);

    let mut reopened = run_async(PezhaiEngine::open(&config_path)).expect("reopen should succeed");
    assert!(matches!(
        run_async(reopened.get(b"ant".to_vec(), Some(&snapshot))),
        Err(KalanjiyamError::InvalidArgument(_))
    ));
    assert!(matches!(
        run_async(reopened.scan(KeyRange::default(), Some(&snapshot))),
        Err(KalanjiyamError::InvalidArgument(_))
    ));
    assert!(matches!(
        run_async(reopened.release_snapshot(&snapshot)),
        Err(KalanjiyamError::InvalidArgument(_))
    ));
}

#[test]
fn snapshot_from_different_engine_instance_is_rejected() {
    let (_tempdir_a, config_path_a) = create_store(&StoreOptions::default());
    let (_tempdir_b, config_path_b) = create_store(&StoreOptions::default());
    let mut engine_a = run_async(PezhaiEngine::open(&config_path_a)).expect("open should succeed");
    let mut engine_b = run_async(PezhaiEngine::open(&config_path_b)).expect("open should succeed");
    let snapshot = run_async(engine_a.create_snapshot()).expect("snapshot should succeed");

    assert!(matches!(
        run_async(engine_b.get(b"ant".to_vec(), Some(&snapshot))),
        Err(KalanjiyamError::InvalidArgument(_))
    ));
    assert!(matches!(
        run_async(engine_b.scan(KeyRange::default(), Some(&snapshot))),
        Err(KalanjiyamError::InvalidArgument(_))
    ));
    assert!(matches!(
        run_async(engine_b.release_snapshot(&snapshot)),
        Err(KalanjiyamError::InvalidArgument(_))
    ));
}

#[test]
fn per_write_mode_keeps_durable_frontier_caught_up_to_committed_seqno() {
    let options = StoreOptions {
        sync_mode: "per_write",
        group_commit_bytes: 1_000_000,
        group_commit_max_delay_ms: 40,
        ..StoreOptions::default()
    };
    let (_tempdir, config_path) = create_store(&options);
    let mut engine = run_async(PezhaiEngine::open(&config_path)).expect("open should succeed");

    run_async(engine.put(b"ant".to_vec(), b"value-one".to_vec())).expect("put should succeed");
    assert_eq!(
        engine.state().durable_seqno,
        engine.state().last_committed_seqno
    );

    run_async(engine.delete(b"ant".to_vec())).expect("delete should succeed");
    assert_eq!(
        engine.state().durable_seqno,
        engine.state().last_committed_seqno
    );

    engine
        .install_logical_shards(
            vec![LogicalShardEntry {
                start_bound: Bound::NegInf,
                end_bound: Bound::PosInf,
                live_size_bytes: 0,
            }],
            vec![
                LogicalShardEntry {
                    start_bound: Bound::NegInf,
                    end_bound: Bound::Finite(b"m".to_vec()),
                    live_size_bytes: 0,
                },
                LogicalShardEntry {
                    start_bound: Bound::Finite(b"m".to_vec()),
                    end_bound: Bound::PosInf,
                    live_size_bytes: 0,
                },
            ],
        )
        .expect("logical shard install should succeed");
    assert_eq!(
        engine.state().durable_seqno,
        engine.state().last_committed_seqno
    );

    let sync = run_async(engine.sync()).expect("sync should succeed");
    assert_eq!(sync.durable_seqno, engine.state().last_committed_seqno);
}

#[test]
fn open_creates_missing_store_subdirectories() {
    let (_tempdir, config_path) = create_store(&StoreOptions::default());
    let store = store_dir(&config_path);
    fs::remove_dir_all(store.join("wal")).expect("fixture should remove wal directory");
    fs::remove_dir_all(store.join("meta")).expect("fixture should remove meta directory");
    fs::remove_dir_all(store.join("data")).expect("fixture should remove data directory");

    let engine = run_async(PezhaiEngine::open(&config_path)).expect("open should recreate layout");
    assert!(
        engine.state().active_wal_path.exists(),
        "open should create an active WAL segment in the recreated wal directory"
    );
    assert!(store.join("wal").is_dir());
    assert!(store.join("meta").is_dir());
    assert!(store.join("data").is_dir());
}

#[test]
fn reopen_ignores_one_truncated_trailing_wal_record() {
    let (_tempdir, config_path) = create_store(&StoreOptions::default());
    let mut engine = run_async(PezhaiEngine::open(&config_path)).expect("open should succeed");
    run_async(engine.put(b"ant".to_vec(), b"value-one".to_vec())).expect("put should succeed");
    let wal_path = engine.active_wal_path().to_path_buf();
    drop(engine);

    let wal_bytes = fs::read(&wal_path).expect("fixture should read active WAL bytes");
    let truncated_len = wal_bytes
        .len()
        .checked_sub(4)
        .expect("active WAL should be large enough to truncate");
    fs::write(&wal_path, &wal_bytes[..truncated_len])
        .expect("fixture should truncate trailing WAL record bytes");

    let reopened = run_async(PezhaiEngine::open(&config_path))
        .expect("open should ignore one truncated trailing record");
    let get = run_async(reopened.get(b"ant".to_vec(), None)).expect("get should succeed");
    assert!(!get.found);
}
