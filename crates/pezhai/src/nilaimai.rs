//! Mutable engine state shared by direct engine calls and WAL-driven recovery.

use std::cmp::Ordering;
use std::collections::{BTreeMap, HashMap};

use crate::config::{RuntimeConfig, SyncMode};
use crate::error::Error;
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

/// One direct-engine mutation that is durable only after its WAL record survives.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Mutation {
    Put { key: Vec<u8>, value: Vec<u8> },
    Delete { key: Vec<u8> },
}

/// Shared mutable state for the WAL-backed memtable engine.
pub(crate) struct EngineState {
    config: RuntimeConfig,
    pub(crate) last_committed_seqno: u64,
    pub(crate) durable_seqno: u64,
    pub(crate) data_generation: u64,
    next_seqno: u64,
    next_snapshot_id: u64,
    snapshots: HashMap<u64, SnapshotEntry>,
    versions: BTreeMap<Vec<u8>, Vec<ValueVersion>>,
    write_stop_message: Option<String>,
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
            next_seqno: 1,
            next_snapshot_id: 1,
            snapshots: HashMap::new(),
            versions: BTreeMap::new(),
            write_stop_message: None,
        }
    }

    /// Returns the configured sync mode for live writes.
    #[must_use]
    pub(crate) fn sync_mode(&self) -> SyncMode {
        self.config.engine.sync_mode
    }

    /// Returns the active write-stop error, if a prior WAL failure poisoned the instance.
    #[must_use]
    pub(crate) fn write_stop_error(&self) -> Option<Error> {
        self.write_stop_message
            .as_ref()
            .map(|message| Error::Io(std::io::Error::other(message.clone())))
    }

    /// Reserves the next mutation sequence number before durable bytes are written.
    pub(crate) fn reserve_seqno(&mut self) -> Result<u64, Error> {
        if let Some(error) = self.write_stop_error() {
            return Err(error);
        }

        let seqno = self.next_seqno;
        self.next_seqno += 1;
        Ok(seqno)
    }

    /// Applies one recovered mutation that survived WAL replay.
    pub(crate) fn apply_recovered_mutation(
        &mut self,
        seqno: u64,
        mutation: &Mutation,
    ) -> Result<(), Error> {
        self.apply_committed_mutation(seqno, mutation, true)
    }

    /// Applies one newly appended mutation in commit order.
    pub(crate) fn apply_live_mutation(
        &mut self,
        seqno: u64,
        mutation: &Mutation,
        durably_synced: bool,
    ) -> Result<(), Error> {
        self.apply_committed_mutation(seqno, mutation, durably_synced)
    }

    /// Marks the open instance unsafe for further writes after an append/sync failure.
    pub(crate) fn mark_write_failed(&mut self, message: impl Into<String>) {
        self.write_stop_message = Some(message.into());
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

    /// Advances the durable frontier after a successful explicit `sync()`.
    pub(crate) fn mark_synced(&mut self, durable_seqno: u64) {
        self.durable_seqno = self.durable_seqno.max(durable_seqno);
    }

    /// Returns the current committed frontier for `sync()`.
    pub(crate) fn sync_target_seqno(&self) -> Result<u64, Error> {
        if let Some(error) = self.write_stop_error() {
            return Err(error);
        }

        Ok(self.last_committed_seqno)
    }

    fn apply_committed_mutation(
        &mut self,
        seqno: u64,
        mutation: &Mutation,
        durably_synced: bool,
    ) -> Result<(), Error> {
        let expected_seqno = self.last_committed_seqno + 1;
        if seqno != expected_seqno {
            return Err(Error::Corruption(format!(
                "mutation seqno {seqno} was committed out of order; expected {expected_seqno}"
            )));
        }

        match mutation {
            Mutation::Put { key, value } => {
                self.versions.entry(key.clone()).or_default().insert(
                    0,
                    ValueVersion {
                        seqno,
                        value: Some(value.clone()),
                    },
                );
            }
            Mutation::Delete { key } => {
                self.versions
                    .entry(key.clone())
                    .or_default()
                    .insert(0, ValueVersion { seqno, value: None });
            }
        }

        self.last_committed_seqno = seqno;
        self.data_generation += 1;
        if durably_synced {
            self.durable_seqno = seqno;
        }
        if self.next_seqno <= seqno {
            self.next_seqno = seqno + 1;
        }

        Ok(())
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
