//! Immutable read and maintenance execution helpers for the shared-data engine.

use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::Error;
use crate::idam::StoreLayout;
use crate::nilaimai::{
    CompactionBuildResult, CompactionPlan, FlushBuildResult, FlushPlan, InternalRecord,
    MemtableRef, PointReadPlan, ScanPlan, lock_memtable, visible_scan_rows,
};
use crate::pathivu::{
    build_temp_data_file, find_visible_record_in_data_file, load_data_file_records,
};
use crate::sevai::ScanRow;

static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);

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

/// Builds one temporary shared-data file from the oldest frozen memtable.
pub(crate) fn build_flush_output(
    layout: &StoreLayout,
    page_size_bytes: u32,
    plan: &FlushPlan,
) -> Result<FlushBuildResult, Error> {
    let records = collect_memtable_records(
        &plan.source.memtable,
        &crate::sevai::Bound::NegInf,
        &crate::sevai::Bound::PosInf,
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
            &crate::sevai::Bound::NegInf,
            &crate::sevai::Bound::PosInf,
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

fn collect_memtable_records(
    memtable: &MemtableRef,
    start_bound: &crate::sevai::Bound,
    end_bound: &crate::sevai::Bound,
) -> Result<Vec<InternalRecord>, Error> {
    Ok(lock_memtable(memtable)?.collect_internal_records(start_bound, end_bound))
}
