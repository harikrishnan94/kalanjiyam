//! Mutable engine state scaffolding shared by the direct engine API and future server integration.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};

use crate::config::{RuntimeConfig, SyncMode};
use crate::sevai::{Bound, ScanRow};

/// One visible or tombstoned version for one user key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ValueVersion {
    pub(crate) seqno: u64,
    pub(crate) value: Option<Vec<u8>>,
}

/// Snapshot metadata retained by the engine while a handle is active.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct SnapshotEntry {
    pub(crate) snapshot_seqno: u64,
    pub(crate) data_generation: u64,
}

/// Shared mutable state for the current milestone-1 engine foundation.
pub(crate) struct EngineState {
    config: RuntimeConfig,
    pub(crate) last_committed_seqno: u64,
    pub(crate) durable_seqno: u64,
    pub(crate) data_generation: u64,
    next_snapshot_id: u64,
    snapshots: HashMap<u64, SnapshotEntry>,
    versions: BTreeMap<Vec<u8>, Vec<ValueVersion>>,
}

impl EngineState {
    /// Creates a fresh engine state from one validated config file.
    #[must_use]
    pub(crate) fn new(config: RuntimeConfig) -> Self {
        Self {
            config,
            last_committed_seqno: 0,
            durable_seqno: 0,
            data_generation: 0,
            next_snapshot_id: 1,
            snapshots: HashMap::new(),
            versions: BTreeMap::new(),
        }
    }

    /// Allocates and records one snapshot at the current visible frontier.
    pub(crate) fn create_snapshot_entry(&mut self) -> (u64, SnapshotEntry) {
        let snapshot_id = self.next_snapshot_id;
        self.next_snapshot_id += 1;
        let entry = SnapshotEntry {
            snapshot_seqno: self.last_committed_seqno,
            data_generation: self.data_generation,
        };
        self.snapshots.insert(snapshot_id, entry);
        (snapshot_id, entry)
    }

    /// Returns the tracked metadata for one active snapshot identifier.
    #[must_use]
    pub(crate) fn snapshot_entry(&self, snapshot_id: u64) -> Option<SnapshotEntry> {
        self.snapshots.get(&snapshot_id).copied()
    }

    /// Releases one active snapshot entry.
    pub(crate) fn release_snapshot_entry(&mut self, snapshot_id: u64) -> Option<SnapshotEntry> {
        self.snapshots.remove(&snapshot_id)
    }

    /// Appends one new visible put version to the in-memory state.
    pub(crate) fn put(&mut self, key: &[u8], value: &[u8]) -> (u64, u64) {
        let seqno = self.next_seqno();
        self.versions.entry(key.to_vec()).or_default().insert(
            0,
            ValueVersion {
                seqno,
                value: Some(value.to_vec()),
            },
        );
        self.publish_write(seqno)
    }

    /// Appends one tombstone version to the in-memory state.
    pub(crate) fn delete(&mut self, key: &[u8]) -> (u64, u64) {
        let seqno = self.next_seqno();
        self.versions
            .entry(key.to_vec())
            .or_default()
            .insert(0, ValueVersion { seqno, value: None });
        self.publish_write(seqno)
    }

    /// Returns the visible value, if any, for one key at one snapshot sequence number.
    #[must_use]
    pub(crate) fn visible_value(&self, key: &[u8], snapshot_seqno: u64) -> Option<Vec<u8>> {
        self.versions
            .get(key)
            .and_then(|versions| visible_version(versions, snapshot_seqno))
            .and_then(|version| version.value.clone())
    }

    /// Materializes one forward scan result set for the requested snapshot.
    #[must_use]
    pub(crate) fn scan_rows(
        &self,
        start_bound: &Bound,
        end_bound: &Bound,
        snapshot_seqno: u64,
    ) -> Vec<ScanRow> {
        self.versions
            .iter()
            .filter(|(key, _)| key_in_range(key, start_bound, end_bound))
            .filter_map(|(key, versions)| {
                let version = visible_version(versions, snapshot_seqno)?;
                let value = version.value.as_ref()?;
                Some(ScanRow {
                    key: key.clone(),
                    value: value.clone(),
                })
            })
            .collect()
    }

    /// Returns the latest whole-keyspace live logical bytes for `stats()`.
    #[must_use]
    pub(crate) fn current_live_size_bytes(&self) -> u64 {
        self.versions
            .iter()
            .filter_map(|(key, versions)| {
                let version = visible_version(versions, self.last_committed_seqno)?;
                let value = version.value.as_ref()?;
                Some((key.len() + value.len()) as u64)
            })
            .sum()
    }

    /// Advances the durable frontier to the current committed frontier.
    pub(crate) fn sync(&mut self) -> u64 {
        self.durable_seqno = self.last_committed_seqno;
        self.durable_seqno
    }

    fn next_seqno(&mut self) -> u64 {
        self.last_committed_seqno += 1;
        self.last_committed_seqno
    }

    fn publish_write(&mut self, seqno: u64) -> (u64, u64) {
        self.data_generation += 1;
        if self.config.engine.sync_mode == SyncMode::PerWrite {
            self.durable_seqno = seqno;
        }
        (seqno, self.data_generation)
    }
}

fn visible_version(versions: &[ValueVersion], snapshot_seqno: u64) -> Option<&ValueVersion> {
    versions
        .iter()
        .find(|version| version.seqno <= snapshot_seqno)
}

fn key_in_range(key: &[u8], start_bound: &Bound, end_bound: &Bound) -> bool {
    bound_allows_start(start_bound, key) && bound_allows_end(end_bound, key)
}

fn bound_allows_start(bound: &Bound, key: &[u8]) -> bool {
    match bound {
        Bound::NegInf => true,
        Bound::Finite(start) => match key.cmp(start.as_slice()) {
            Ordering::Less => false,
            Ordering::Equal | Ordering::Greater => true,
        },
        Bound::PosInf => false,
    }
}

fn bound_allows_end(bound: &Bound, key: &[u8]) -> bool {
    match bound {
        Bound::NegInf => false,
        Bound::Finite(end) => match key.cmp(end.as_slice()) {
            Ordering::Less => true,
            Ordering::Equal | Ordering::Greater => false,
        },
        Bound::PosInf => true,
    }
}
