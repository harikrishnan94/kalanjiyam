//! Generic background worker execution for `sevai`.
//!
//! The owner loop owns admission, cancellation, and publication. This module
//! only runs immutable work items on background threads and reports results
//! back to the owner for validation.

use std::collections::VecDeque;
use std::path::PathBuf;
use std::sync::{Arc, Condvar, Mutex};
use std::thread::{self, JoinHandle};

use tokio::sync::mpsc;

use crate::error::KalanjiyamError;
use crate::pezhai::engine::{
    CheckpointTaskPlan, CompactTaskPlan, CompactTaskResult, FlushTaskPlan, FlushTaskResult,
    GcTaskPlan, GcTaskResult, GetPlan, ScanPlan, execute_checkpoint_task, execute_compact_task,
    execute_flush_task, execute_gc_task, execute_get_plan, execute_scan_plan,
};
use crate::pezhai::types::{GetResponse, ScanPage};

/// One background task kind visible to the worker pool.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum WorkerTaskKind {
    /// Immutable point-read work.
    Get,
    /// Immutable paged scan work.
    ScanPage,
    /// Flush-preparation work.
    Flush,
    /// Compaction-preparation work.
    Compact,
    /// Checkpoint-preparation work.
    Checkpoint,
    /// GC work.
    Gc,
}

/// One immutable task executed by the generic background worker pool.
#[derive(Clone, Debug)]
#[allow(dead_code)]
pub enum WorkerTask {
    /// Executes one engine-captured point-read plan.
    Get { store_dir: PathBuf, plan: GetPlan },
    /// Executes one engine-captured scan-page plan.
    ScanPage { store_dir: PathBuf, plan: ScanPlan },
    /// Prepares flush output on a worker before owner-thread publish.
    Flush {
        store_dir: PathBuf,
        page_size_bytes: usize,
        plan: FlushTaskPlan,
    },
    /// Prepares compaction output on a worker before owner-thread publish.
    Compact {
        store_dir: PathBuf,
        page_size_bytes: usize,
        plan: CompactTaskPlan,
    },
    /// Prepares checkpoint artifacts on a worker before owner-thread publish.
    Checkpoint {
        store_dir: PathBuf,
        page_size_bytes: usize,
        checkpoint_generation: u64,
        plan: CheckpointTaskPlan,
    },
    /// Deletes already-safe durable files on a worker.
    Gc { plan: GcTaskPlan },
}

impl WorkerTask {
    /// Returns the stable task kind used by the owner loop.
    #[must_use]
    pub fn kind(&self) -> WorkerTaskKind {
        match self {
            Self::Get { .. } => WorkerTaskKind::Get,
            Self::ScanPage { .. } => WorkerTaskKind::ScanPage,
            Self::Flush { .. } => WorkerTaskKind::Flush,
            Self::Compact { .. } => WorkerTaskKind::Compact,
            Self::Checkpoint { .. } => WorkerTaskKind::Checkpoint,
            Self::Gc { .. } => WorkerTaskKind::Gc,
        }
    }
}

/// One owner-dispatched task envelope carrying a stable task id.
#[derive(Clone, Debug)]
pub struct WorkerTaskEnvelope {
    /// Stable owner-allocated task id.
    pub task_id: u64,
    /// Immutable task payload.
    pub task: WorkerTask,
}

/// One worker completion payload returned to the owner loop.
#[derive(Clone, Debug)]
pub enum WorkerCompletion {
    /// Completed point-read response.
    Get(GetResponse),
    /// Completed scan-page response.
    ScanPage(ScanPage),
    /// Flush worker finished preparing the captured plan.
    FlushPrepared(FlushTaskResult),
    /// Compaction worker finished preparing the captured plan.
    CompactPrepared(CompactTaskResult),
    /// Checkpoint worker finished preparing artifacts.
    CheckpointPrepared(crate::pezhai::engine::CheckpointTaskResult),
    /// GC worker finished deleting currently safe artifacts.
    GcPrepared(GcTaskResult),
}

/// One worker completion envelope delivered back to the owner loop.
#[derive(Debug)]
pub struct WorkerResultEnvelope {
    /// Stable owner-allocated task id.
    pub task_id: u64,
    /// Task kind used for owner-side routing.
    pub kind: WorkerTaskKind,
    /// Worker success or failure.
    pub result: Result<WorkerCompletion, KalanjiyamError>,
}

#[derive(Default)]
struct QueueState {
    closed: bool,
    tasks: VecDeque<WorkerTaskEnvelope>,
}

struct QueueInner {
    state: Mutex<QueueState>,
    cv: Condvar,
}

impl QueueInner {
    fn new() -> Self {
        Self {
            state: Mutex::new(QueueState::default()),
            cv: Condvar::new(),
        }
    }

    fn push(&self, task: WorkerTaskEnvelope) {
        let mut state = self
            .state
            .lock()
            .expect("worker queue mutex should not poison");
        if state.closed {
            return;
        }
        state.tasks.push_back(task);
        self.cv.notify_one();
    }

    fn pop(&self) -> Option<WorkerTaskEnvelope> {
        let mut state = self
            .state
            .lock()
            .expect("worker queue mutex should not poison");
        loop {
            if let Some(task) = state.tasks.pop_front() {
                return Some(task);
            }
            if state.closed {
                return None;
            }
            state = self
                .cv
                .wait(state)
                .expect("worker queue mutex should not poison");
        }
    }

    fn close(&self) {
        let mut state = self
            .state
            .lock()
            .expect("worker queue mutex should not poison");
        state.closed = true;
        self.cv.notify_all();
    }
}

/// Generic owner-managed worker pool.
pub struct WorkerPool {
    queue: Arc<QueueInner>,
    result_tx: mpsc::Sender<WorkerResultEnvelope>,
    joins: Vec<JoinHandle<()>>,
}

impl WorkerPool {
    /// Starts the configured number of stateless worker threads.
    #[must_use]
    pub fn new(thread_count: usize, result_tx: mpsc::Sender<WorkerResultEnvelope>) -> Self {
        let queue = Arc::new(QueueInner::new());
        let mut joins = Vec::with_capacity(thread_count);
        for worker_index in 0..thread_count {
            let queue_clone = Arc::clone(&queue);
            let result_tx_clone = result_tx.clone();
            joins.push(thread::spawn(move || {
                worker_loop(worker_index, queue_clone, result_tx_clone);
            }));
        }
        Self {
            queue,
            result_tx,
            joins,
        }
    }

    /// Enqueues one owner-dispatched task for background execution.
    pub fn enqueue(&self, task: WorkerTaskEnvelope) {
        self.queue.push(task);
    }

    /// Stops accepting new work and waits for worker threads to exit.
    pub fn shutdown(&mut self) {
        self.queue.close();
        let joins = std::mem::take(&mut self.joins);
        for join in joins {
            let _ = join.join();
        }
    }
}

impl Drop for WorkerPool {
    fn drop(&mut self) {
        self.shutdown();
        let _ = &self.result_tx;
    }
}

fn worker_loop(
    _worker_index: usize,
    queue: Arc<QueueInner>,
    result_tx: mpsc::Sender<WorkerResultEnvelope>,
) {
    while let Some(envelope) = queue.pop() {
        let kind = envelope.task.kind();
        let result = run_task(&envelope.task);
        if result_tx
            .blocking_send(WorkerResultEnvelope {
                task_id: envelope.task_id,
                kind,
                result,
            })
            .is_err()
        {
            return;
        }
    }
}

fn run_task(task: &WorkerTask) -> Result<WorkerCompletion, KalanjiyamError> {
    match task {
        WorkerTask::Get { store_dir, plan } => {
            execute_get_plan(store_dir, plan).map(WorkerCompletion::Get)
        }
        WorkerTask::ScanPage { store_dir, plan } => {
            execute_scan_plan(store_dir, plan).map(WorkerCompletion::ScanPage)
        }
        WorkerTask::Flush {
            store_dir,
            page_size_bytes,
            plan,
        } => execute_flush_task(store_dir, *page_size_bytes, plan)
            .map(WorkerCompletion::FlushPrepared),
        WorkerTask::Compact {
            store_dir,
            page_size_bytes,
            plan,
        } => execute_compact_task(store_dir, *page_size_bytes, plan)
            .map(WorkerCompletion::CompactPrepared),
        WorkerTask::Checkpoint {
            store_dir,
            page_size_bytes,
            checkpoint_generation,
            plan,
        } => execute_checkpoint_task(store_dir, *page_size_bytes, *checkpoint_generation, plan)
            .map(WorkerCompletion::CheckpointPrepared),
        WorkerTask::Gc { plan } => execute_gc_task(plan).map(WorkerCompletion::GcPrepared),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pezhai::engine::{
        CompactTaskPlan, DurableFileRef, FlushTaskPlan, GcTaskPlan, GetPlan, ScanPlan,
    };
    use tempfile::tempdir;

    #[test]
    fn worker_task_kind_and_closed_queue_cover_remaining_variants() {
        let compact = WorkerTask::Compact {
            store_dir: PathBuf::new(),
            page_size_bytes: 4096,
            plan: CompactTaskPlan::default(),
        };
        assert_eq!(compact.kind(), WorkerTaskKind::Compact);

        let queue = QueueInner::new();
        queue.close();
        queue.push(WorkerTaskEnvelope {
            task_id: 1,
            task: WorkerTask::Gc {
                plan: GcTaskPlan::default(),
            },
        });
        let state = queue
            .state
            .lock()
            .expect("worker queue mutex should not poison");
        assert!(state.closed);
        assert!(state.tasks.is_empty());
    }

    #[test]
    fn execute_task_routes_compaction_and_gc_work() {
        let tempdir = tempdir().expect("tempdir should be creatable for worker tests");
        let store_dir = tempdir.path().join("store");
        std::fs::create_dir_all(store_dir.join("wal")).expect("wal dir should be creatable");
        std::fs::create_dir_all(store_dir.join("meta")).expect("meta dir should be creatable");
        std::fs::create_dir_all(store_dir.join("data")).expect("data dir should be creatable");

        let compact_result = run_task(&WorkerTask::Compact {
            store_dir: store_dir.clone(),
            page_size_bytes: 4096,
            plan: CompactTaskPlan {
                source_generation: 9,
                input_file_ids: vec![],
                input_files: vec![],
                output_file_ids: vec![7],
            },
        })
        .expect("compact task should succeed with empty inputs");
        match compact_result {
            WorkerCompletion::CompactPrepared(result) => {
                assert_eq!(result.source_generation, 9);
                assert_eq!(result.input_file_ids, Vec::<u64>::new());
                assert_eq!(result.outputs.len(), 1);
            }
            other => panic!("expected CompactPrepared completion, got {other:?}"),
        }

        let gc_path = store_dir.join("data/00000000000000000042.kjm");
        std::fs::write(&gc_path, b"gc").expect("gc fixture file should be writable");
        let gc_result = run_task(&WorkerTask::Gc {
            plan: GcTaskPlan {
                deletable_files: vec![DurableFileRef::Data {
                    file_id: 42,
                    path: gc_path.clone(),
                }],
                prunable_manifest_generations: vec![3],
            },
        })
        .expect("gc task should succeed");
        match gc_result {
            WorkerCompletion::GcPrepared(result) => {
                assert_eq!(result.deleted_files.len(), 1);
                assert_eq!(result.pruned_manifest_generations, vec![3]);
            }
            other => panic!("expected GcPrepared completion, got {other:?}"),
        }
    }

    #[test]
    fn run_task_covers_get_scan_flush_and_checkpoint_variants() {
        let tempdir = tempdir().expect("tempdir should be creatable for worker tests");
        let store_dir = tempdir.path().join("store");
        std::fs::create_dir_all(store_dir.join("wal")).expect("wal dir should be creatable");
        std::fs::create_dir_all(store_dir.join("meta")).expect("meta dir should be creatable");
        std::fs::create_dir_all(store_dir.join("data")).expect("data dir should be creatable");

        let get_result = run_task(&WorkerTask::Get {
            store_dir: store_dir.clone(),
            plan: GetPlan {
                key: b"missing".to_vec(),
                snapshot_seqno: 1,
                data_generation: 0,
                frozen_memtables: Vec::new(),
                candidate_files: Vec::new(),
            },
        })
        .expect("empty get plan should complete");
        assert!(matches!(get_result, WorkerCompletion::Get(payload) if !payload.found));

        let scan_result = run_task(&WorkerTask::ScanPage {
            store_dir: store_dir.clone(),
            plan: ScanPlan {
                scan_id: 1,
                snapshot_handle: crate::pezhai::types::SnapshotHandle {
                    engine_instance_id: 1,
                    snapshot_id: 1,
                    snapshot_seqno: 1,
                    data_generation: 0,
                },
                range: crate::pezhai::types::KeyRange::default(),
                resume_after_key: None,
                max_records_per_page: 10,
                max_bytes_per_page: 1024,
                active_memtable: crate::pezhai::types::Memtable::empty(),
                frozen_memtables: Vec::new(),
                candidate_files: Vec::new(),
            },
        })
        .expect("empty scan plan should complete");
        assert!(matches!(scan_result, WorkerCompletion::ScanPage(payload) if payload.eof));

        let mut memtable = crate::pezhai::types::Memtable::empty();
        memtable.insert_put(1, b"ant", b"value");
        let flush_result = run_task(&WorkerTask::Flush {
            store_dir: store_dir.clone(),
            page_size_bytes: 4096,
            plan: FlushTaskPlan {
                source_generation: 1,
                source_frozen_memtable_id: 1,
                source_memtable: Arc::new(memtable.clone()),
                output_file_ids: vec![7],
            },
        })
        .expect("flush task should complete");
        assert!(matches!(
            flush_result,
            WorkerCompletion::FlushPrepared(result) if result.outputs.len() == 1
        ));

        let checkpoint_result = run_task(&WorkerTask::Checkpoint {
            store_dir,
            page_size_bytes: 4096,
            checkpoint_generation: 3,
            plan: crate::pezhai::engine::CheckpointTaskPlan::default(),
        })
        .expect("checkpoint task should complete");
        assert!(matches!(
            checkpoint_result,
            WorkerCompletion::CheckpointPrepared(result) if result.current.checkpoint_generation == 3
        ));
    }
}
