//! Immutable read and maintenance execution helpers for the shared-data engine.

use std::cmp::Ordering as CmpOrdering;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::Error;
use crate::idam::StoreLayout;
use crate::iyakkam::{Bound, ScanRow, SnapshotHandle};
use crate::nilaimai::{
    CompactionBuildResult, CompactionPlan, FlushBuildResult, FlushPlan, InternalRecord,
    LogicalMaintenancePlan, LogicalShardEntry, LogicalShardInstallPayload, MemtableRef,
    PointReadPlan, ScanPlan, lock_memtable, visible_scan_rows,
};
use crate::pathivu::{
    build_temp_data_file, find_visible_record_in_data_file, load_data_file_records,
};

static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);

/// One immutable page-oriented scan task captured from a session-pinned generation.
#[derive(Clone, Debug)]
pub(crate) struct ScanPagePlan {
    pub(crate) scan_id: u64,
    pub(crate) snapshot_handle: SnapshotHandle,
    pub(crate) scan_plan: ScanPlan,
    pub(crate) resume_after_key: Option<Vec<u8>>,
    pub(crate) max_records_per_page: u32,
    pub(crate) max_bytes_per_page: u32,
}

impl ScanPagePlan {
    /// Returns `true` when this page can be produced from the pinned active memtable alone.
    #[must_use]
    pub(crate) fn is_active_only(&self) -> bool {
        self.scan_plan.is_active_only()
    }
}

/// One page result materialized for `ScanFetchNext`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ScanPageResult {
    pub(crate) rows: Vec<ScanRow>,
    pub(crate) eof: bool,
    pub(crate) next_resume_after_key: Option<Vec<u8>>,
}

/// Executes one point-read plan after the active-memtable fast path misses.
pub(crate) fn execute_point_read(
    layout: &StoreLayout,
    plan: &PointReadPlan,
) -> Result<Option<Vec<u8>>, Error> {
    for frozen in &plan.frozen_memtables {
        let memtable = lock_memtable(&frozen.memtable)?;
        if let Some(version) = memtable.visible_version(&plan.key, plan.snapshot_seqno) {
            return Ok(version.value.clone());
        }
    }

    for file in &plan.candidate_files {
        let record =
            find_visible_record_in_data_file(layout, file, &plan.key, plan.snapshot_seqno)?;
        if let Some(record) = record {
            return Ok(record.value);
        }
    }

    Ok(None)
}

/// Materializes one full scan result from one immutable source plan.
pub(crate) fn execute_scan_plan(
    layout: &StoreLayout,
    plan: &ScanPlan,
) -> Result<Vec<ScanRow>, Error> {
    let mut records =
        collect_memtable_records(&plan.active_memtable, &plan.start_bound, &plan.end_bound)?;
    for frozen in &plan.frozen_memtables {
        records.extend(collect_memtable_records(
            &frozen.memtable,
            &plan.start_bound,
            &plan.end_bound,
        )?);
    }
    for file in &plan.candidate_files {
        records.extend(load_data_file_records(
            layout,
            file,
            &plan.start_bound,
            &plan.end_bound,
        )?);
    }

    Ok(visible_scan_rows(&mut records, plan.snapshot_seqno))
}

/// Materializes one bounded page from a stable scan plan and resume key.
pub(crate) fn execute_scan_page_plan(
    layout: &StoreLayout,
    plan: &ScanPagePlan,
) -> Result<ScanPageResult, Error> {
    let _ = plan.scan_id;
    let _ = &plan.snapshot_handle;
    let all_rows = execute_scan_plan(layout, &plan.scan_plan)?;
    let start_index = match &plan.resume_after_key {
        Some(resume_after_key) => all_rows.partition_point(|row| row.key <= *resume_after_key),
        None => 0,
    };
    if start_index >= all_rows.len() {
        return Ok(ScanPageResult {
            rows: Vec::new(),
            eof: true,
            next_resume_after_key: None,
        });
    }

    let mut rows = Vec::new();
    let mut accumulated_bytes = 0usize;
    for row in all_rows.iter().skip(start_index) {
        if rows.len() >= plan.max_records_per_page as usize {
            break;
        }

        let row_bytes = row.key.len() + row.value.len();
        if !rows.is_empty() && accumulated_bytes + row_bytes > plan.max_bytes_per_page as usize {
            break;
        }

        rows.push(row.clone());
        if row_bytes > plan.max_bytes_per_page as usize && rows.len() == 1 {
            break;
        }

        accumulated_bytes += row_bytes;
    }

    let consumed_rows = start_index + rows.len();
    let eof = consumed_rows >= all_rows.len();
    Ok(ScanPageResult {
        next_resume_after_key: if eof {
            None
        } else {
            rows.last().map(|row| row.key.clone())
        },
        rows,
        eof,
    })
}

/// Builds one temporary shared-data file from the oldest frozen memtable.
pub(crate) fn build_flush_output(
    layout: &StoreLayout,
    page_size_bytes: u32,
    plan: &FlushPlan,
) -> Result<FlushBuildResult, Error> {
    let records = collect_memtable_records(
        &plan.source.memtable,
        &crate::Bound::NegInf,
        &crate::Bound::PosInf,
    )?;
    let temp_path = layout.temp_data_file_path(&format!(
        "{}-{}",
        plan.temp_tag,
        NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed)
    ));
    let summary = build_temp_data_file(&temp_path, page_size_bytes, &records)?;
    Ok(FlushBuildResult { temp_path, summary })
}

/// Builds one temporary compaction output from the captured input-file set.
pub(crate) fn build_compaction_output(
    layout: &StoreLayout,
    page_size_bytes: u32,
    plan: &CompactionPlan,
) -> Result<CompactionBuildResult, Error> {
    let mut records = Vec::new();
    for input in &plan.input_files {
        records.extend(load_data_file_records(
            layout,
            input,
            &crate::Bound::NegInf,
            &crate::Bound::PosInf,
        )?);
    }

    let temp_path = layout.temp_data_file_path(&format!(
        "{}-{}",
        plan.temp_tag,
        NEXT_TEMP_ID.fetch_add(1, Ordering::Relaxed)
    ));
    let summary = build_temp_data_file(&temp_path, page_size_bytes, &records)?;
    Ok(CompactionBuildResult { temp_path, summary })
}

/// Recomputes exact live bytes for the current logical-shard map and chooses at most one install.
pub(crate) fn execute_logical_maintenance_plan(
    layout: &StoreLayout,
    plan: &LogicalMaintenancePlan,
) -> Result<Option<LogicalShardInstallPayload>, Error> {
    let rows = execute_scan_plan(layout, &plan.scan_plan)?;
    let exact_bytes = exact_live_bytes_by_shard(&plan.current_logical_shards, &rows)?;

    for (index, shard) in plan.current_logical_shards.iter().enumerate() {
        let total_bytes = exact_bytes[index];
        if total_bytes < plan.logical_split_bytes {
            continue;
        }

        let Some(boundary) = choose_split_boundary(shard, &rows, total_bytes) else {
            continue;
        };

        let left_bytes = rows
            .iter()
            .filter(|row| shard.contains_key(&row.key) && row.key.as_slice() < boundary.as_slice())
            .map(logical_row_bytes)
            .sum::<u64>();
        let right_bytes = total_bytes.saturating_sub(left_bytes);

        return Ok(Some(LogicalShardInstallPayload {
            source_entries: vec![shard.clone()],
            output_entries: vec![
                LogicalShardEntry::new(
                    shard.start_bound.clone(),
                    Bound::Finite(boundary.clone()),
                    left_bytes,
                )?,
                LogicalShardEntry::new(
                    Bound::Finite(boundary),
                    shard.end_bound.clone(),
                    right_bytes,
                )?,
            ],
        }));
    }

    let mut best_merge: Option<(usize, u64)> = None;
    for index in 0..plan.current_logical_shards.len().saturating_sub(1) {
        let combined = exact_bytes[index].saturating_add(exact_bytes[index + 1]);
        if combined > plan.logical_merge_bytes {
            continue;
        }

        match best_merge {
            None => best_merge = Some((index, combined)),
            Some((best_index, best_combined)) => {
                let ordering = combined
                    .cmp(&best_combined)
                    .then_with(|| index.cmp(&best_index));
                if ordering == CmpOrdering::Less {
                    best_merge = Some((index, combined));
                }
            }
        }
    }

    let Some((merge_index, combined_bytes)) = best_merge else {
        return Ok(None);
    };
    let left = &plan.current_logical_shards[merge_index];
    let right = &plan.current_logical_shards[merge_index + 1];
    Ok(Some(LogicalShardInstallPayload {
        source_entries: vec![left.clone(), right.clone()],
        output_entries: vec![LogicalShardEntry::new(
            left.start_bound.clone(),
            right.end_bound.clone(),
            combined_bytes,
        )?],
    }))
}

/// Deletes the requested canonical data files in stable order and keeps failures best-effort.
pub(crate) fn execute_gc(layout: &StoreLayout, file_ids: &[u64]) -> Vec<u64> {
    let mut deleted = Vec::new();
    for file_id in file_ids {
        match std::fs::remove_file(layout.data_file_path(*file_id)) {
            Ok(()) => deleted.push(*file_id),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
            Err(_) => continue,
        }
    }
    deleted
}

fn collect_memtable_records(
    memtable: &MemtableRef,
    start_bound: &crate::Bound,
    end_bound: &crate::Bound,
) -> Result<Vec<InternalRecord>, Error> {
    Ok(lock_memtable(memtable)?.collect_internal_records(start_bound, end_bound))
}

// Logical maintenance recomputes exact live bytes from the latest visible rows
// rather than tracking old-value deltas on the write path.
fn exact_live_bytes_by_shard(
    shards: &[LogicalShardEntry],
    rows: &[ScanRow],
) -> Result<Vec<u64>, Error> {
    let mut exact_bytes = vec![0_u64; shards.len()];
    let mut shard_index = 0_usize;

    for row in rows {
        while shard_index < shards.len() && !shards[shard_index].contains_key(&row.key) {
            shard_index += 1;
        }
        if shard_index == shards.len() {
            return Err(Error::Corruption(
                "latest visible rows no longer fit inside the captured logical-shard map".into(),
            ));
        }
        exact_bytes[shard_index] = exact_bytes[shard_index].saturating_add(logical_row_bytes(row));
    }

    Ok(exact_bytes)
}

// The split chooser follows the handoff default: walk one shard in key order
// and pick the first key where the running exact-byte total crosses half.
fn choose_split_boundary(
    shard: &LogicalShardEntry,
    rows: &[ScanRow],
    total_bytes: u64,
) -> Option<Vec<u8>> {
    let mut cumulative_bytes = 0_u64;
    for row in rows.iter().filter(|row| shard.contains_key(&row.key)) {
        cumulative_bytes = cumulative_bytes.saturating_add(logical_row_bytes(row));
        if cumulative_bytes.saturating_mul(2) < total_bytes {
            continue;
        }

        let boundary = row.key.clone();
        let valid_left = match &shard.start_bound {
            Bound::NegInf => true,
            Bound::Finite(start) => start.as_slice() < boundary.as_slice(),
            Bound::PosInf => false,
        };
        let valid_right = match &shard.end_bound {
            Bound::Finite(end) => boundary.as_slice() < end.as_slice(),
            Bound::PosInf => true,
            Bound::NegInf => false,
        };
        if valid_left && valid_right {
            return Some(boundary);
        }
    }

    None
}

// One visible row contributes exactly `len(key) + len(value)` logical bytes to
// the latest-only shard metadata.
fn logical_row_bytes(row: &ScanRow) -> u64 {
    (row.key.len() + row.value.len()) as u64
}
