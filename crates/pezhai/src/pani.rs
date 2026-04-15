//! Immutable read and maintenance execution helpers for the shared-data engine.

use std::cmp::Ordering as CmpOrdering;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use crate::error::Error;
use crate::idam::StoreLayout;
use crate::iyakkam::{Bound, ScanRow, SnapshotHandle};
use crate::nilaimai::{
    CompactionBuildResult, CompactionPlan, FlushBuildResult, FlushPlan, InternalRecord,
    LogicalMaintenancePlan, LogicalShardEntry, LogicalShardInstallPayload, MemtableRef,
    PointReadPlan, RecordKind, ScanPlan, compare_internal, compare_internal_to_parts,
    lock_memtable,
};
use crate::pathivu::{DecodedDataFile, build_temp_data_file, load_data_file_records};

static NEXT_TEMP_ID: AtomicU64 = AtomicU64::new(1);
#[cfg(test)]
static SCAN_STREAM_BUILD_COUNT: AtomicU64 = AtomicU64::new(0);

/// One immutable page-oriented scan task captured from a session-pinned generation.
#[derive(Clone, Debug)]
pub(crate) struct ScanPagePlan {
    pub(crate) scan_id: u64,
    pub(crate) snapshot_handle: SnapshotHandle,
    pub(crate) scan_plan: ScanPlan,
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
}

/// One streaming merge cursor retained across paged scan fetches.
#[derive(Clone, Debug)]
pub(crate) struct StreamingScanCursor {
    snapshot_seqno: u64,
    sources: Vec<ScanSourceCursor>,
    pending_internal: Option<InternalRecord>,
    pending_row: Option<ScanRow>,
}

impl StreamingScanCursor {
    /// Produces one bounded page while keeping the remaining merge state alive.
    pub(crate) fn next_page(
        &mut self,
        max_records_per_page: u32,
        max_bytes_per_page: u32,
    ) -> ScanPageResult {
        let mut rows = Vec::new();
        let mut accumulated_bytes = 0usize;
        let mut next_row = self.pending_row.take().or_else(|| self.next_visible_row());
        while let Some(row) = next_row {
            if rows.len() >= max_records_per_page as usize {
                self.pending_row = Some(row);
                return ScanPageResult { rows, eof: false };
            }

            let row_bytes = row.key.len() + row.value.len();
            if !rows.is_empty() && accumulated_bytes + row_bytes > max_bytes_per_page as usize {
                self.pending_row = Some(row);
                return ScanPageResult { rows, eof: false };
            }

            rows.push(row);
            if row_bytes > max_bytes_per_page as usize && rows.len() == 1 {
                return ScanPageResult { rows, eof: false };
            }

            accumulated_bytes += row_bytes;
            next_row = self.next_visible_row();
        }

        ScanPageResult { rows, eof: true }
    }

    fn next_visible_row(&mut self) -> Option<ScanRow> {
        loop {
            let first = self.next_internal_record()?;
            let key = first.user_key.clone();
            let mut visible_record = if first.seqno <= self.snapshot_seqno {
                Some(first)
            } else {
                None
            };

            loop {
                let Some(next) = self.next_internal_record() else {
                    return visible_record.and_then(record_to_scan_row);
                };
                if next.user_key != key {
                    self.pending_internal = Some(next);
                    break;
                }
                if visible_record.is_none() && next.seqno <= self.snapshot_seqno {
                    visible_record = Some(next);
                }
            }

            if let Some(record) = visible_record.and_then(record_to_scan_row) {
                return Some(record);
            }
        }
    }

    fn next_internal_record(&mut self) -> Option<InternalRecord> {
        if let Some(record) = self.pending_internal.take() {
            return Some(record);
        }

        let source_index = self.select_source_index()?;
        let record = self.sources[source_index].current()?.clone();
        self.sources[source_index].advance();
        Some(record)
    }

    fn select_source_index(&self) -> Option<usize> {
        self.sources
            .iter()
            .enumerate()
            .filter_map(|(index, source)| source.current().map(|record| (index, record)))
            .min_by(|left, right| compare_internal(left.1, right.1))
            .map(|(index, _record)| index)
    }
}

#[derive(Clone, Debug)]
/// One source cursor over either owned memtable records or one decoded file.
struct ScanSourceCursor {
    records: ScanSourceRecords,
    next_index: usize,
    end_index: usize,
}

impl ScanSourceCursor {
    fn from_owned(records: Vec<InternalRecord>) -> Option<Self> {
        if records.is_empty() {
            return None;
        }
        let end_index = records.len();
        Some(Self {
            records: ScanSourceRecords::Owned(records),
            next_index: 0,
            end_index,
        })
    }

    fn from_decoded(
        decoded_file: Arc<DecodedDataFile>,
        start_index: usize,
        end_index: usize,
    ) -> Option<Self> {
        if start_index >= end_index {
            return None;
        }
        Some(Self {
            records: ScanSourceRecords::Decoded(decoded_file),
            next_index: start_index,
            end_index,
        })
    }

    fn current(&self) -> Option<&InternalRecord> {
        if self.next_index >= self.end_index {
            return None;
        }

        match &self.records {
            ScanSourceRecords::Owned(records) => records.get(self.next_index),
            ScanSourceRecords::Decoded(decoded_file) => decoded_file.records.get(self.next_index),
        }
    }

    fn advance(&mut self) {
        self.next_index = self.next_index.saturating_add(1);
    }
}

#[derive(Clone, Debug)]
/// One immutable backing store for a scan source cursor.
enum ScanSourceRecords {
    Owned(Vec<InternalRecord>),
    Decoded(Arc<DecodedDataFile>),
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
        let decoded = plan.decoded_data_files.get_or_load(layout, file)?;
        let record =
            find_visible_record_in_records(&decoded.records, &plan.key, plan.snapshot_seqno);
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
    let mut cursor = build_scan_stream_cursor(layout, plan)?;
    let mut rows = Vec::new();
    loop {
        let page = cursor.next_page(u32::MAX, u32::MAX);
        rows.extend(page.rows);
        if page.eof {
            break;
        }
    }

    Ok(rows)
}

/// Builds one streaming cursor and materializes the first bounded page.
pub(crate) fn execute_scan_page_plan(
    layout: &StoreLayout,
    plan: &ScanPagePlan,
) -> Result<(StreamingScanCursor, ScanPageResult), Error> {
    let _ = plan.scan_id;
    let _ = &plan.snapshot_handle;
    let mut cursor = build_scan_stream_cursor(layout, &plan.scan_plan)?;
    let page = cursor.next_page(plan.max_records_per_page, plan.max_bytes_per_page);
    Ok((cursor, page))
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

// Build one retained per-source cursor set so later page fetches can resume
// from the previous merge position instead of rescanning every source.
pub(crate) fn build_scan_stream_cursor(
    layout: &StoreLayout,
    plan: &ScanPlan,
) -> Result<StreamingScanCursor, Error> {
    #[cfg(test)]
    SCAN_STREAM_BUILD_COUNT.fetch_add(1, Ordering::Relaxed);

    let mut sources = Vec::new();

    let mut active_records =
        collect_memtable_records(&plan.active_memtable, &plan.start_bound, &plan.end_bound)?;
    active_records.sort_by(compare_internal);
    if let Some(source) = ScanSourceCursor::from_owned(active_records) {
        sources.push(source);
    }

    for frozen in &plan.frozen_memtables {
        let mut records =
            collect_memtable_records(&frozen.memtable, &plan.start_bound, &plan.end_bound)?;
        records.sort_by(compare_internal);
        if let Some(source) = ScanSourceCursor::from_owned(records) {
            sources.push(source);
        }
    }

    for file in &plan.candidate_files {
        let decoded = plan.decoded_data_files.get_or_load(layout, file)?;
        let (start_index, end_index) =
            decoded_file_range_indices(&decoded, &plan.start_bound, &plan.end_bound);
        if let Some(source) = ScanSourceCursor::from_decoded(decoded, start_index, end_index) {
            sources.push(source);
        }
    }

    Ok(StreamingScanCursor {
        snapshot_seqno: plan.snapshot_seqno,
        sources,
        pending_internal: None,
        pending_row: None,
    })
}

// Capture each source once, then keep only cursor positions between pages so
// paged scans advance incrementally instead of rebuilding the full row set.
fn collect_memtable_records(
    memtable: &MemtableRef,
    start_bound: &crate::Bound,
    end_bound: &crate::Bound,
) -> Result<Vec<InternalRecord>, Error> {
    Ok(lock_memtable(memtable)?.collect_internal_records(start_bound, end_bound))
}

// Decode each shared-data file at most once per generation, then use the
// internal-key order to find the slice that can overlap the requested range.
fn decoded_file_range_indices(
    decoded_file: &DecodedDataFile,
    start_bound: &Bound,
    end_bound: &Bound,
) -> (usize, usize) {
    let start_index = match start_bound {
        Bound::NegInf => 0,
        Bound::Finite(start_key) => decoded_file
            .records
            .partition_point(|record| record.user_key.as_slice() < start_key.as_slice()),
        Bound::PosInf => decoded_file.records.len(),
    };
    let end_index = match end_bound {
        Bound::NegInf => 0,
        Bound::Finite(end_key) => decoded_file
            .records
            .partition_point(|record| record.user_key.as_slice() < end_key.as_slice()),
        Bound::PosInf => decoded_file.records.len(),
    };
    (start_index, end_index)
}

// Point reads reuse the decoded-record vectors so repeated lookups only pay
// the binary-search cost after the first cache miss.
fn find_visible_record_in_records(
    records: &[InternalRecord],
    user_key: &[u8],
    snapshot_seqno: u64,
) -> Option<InternalRecord> {
    let search_index = records
        .binary_search_by(|record| {
            compare_internal_to_parts(record, user_key, snapshot_seqno, RecordKind::Delete)
        })
        .unwrap_or_else(|index| index);

    for record in records.iter().skip(search_index) {
        match record.user_key.as_slice().cmp(user_key) {
            std::cmp::Ordering::Less => continue,
            std::cmp::Ordering::Greater => return None,
            std::cmp::Ordering::Equal => {
                if record.seqno <= snapshot_seqno {
                    return Some(record.clone());
                }
            }
        }
    }

    None
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

// The streaming cursor already filtered shadowed versions and tombstones at
// the internal-record layer, so only visible `Put` rows become public output.
fn record_to_scan_row(record: InternalRecord) -> Option<ScanRow> {
    if record.kind != RecordKind::Put {
        return None;
    }
    Some(ScanRow {
        key: record.user_key,
        value: record
            .value
            .expect("put records should always retain their value bytes"),
    })
}

#[cfg(test)]
pub(crate) fn reset_scan_stream_build_count() {
    SCAN_STREAM_BUILD_COUNT.store(0, Ordering::Relaxed);
}

#[cfg(test)]
pub(crate) fn scan_stream_build_count() -> u64 {
    SCAN_STREAM_BUILD_COUNT.load(Ordering::Relaxed)
}
