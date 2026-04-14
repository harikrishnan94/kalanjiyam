//! Library entry point for the `pezhai` storage engine crate.

mod config;
pub mod error;
mod idam;
mod iyakkam;
mod nilaimai;
mod pani;
mod pathivu;
pub mod sevai;

/// Crate-level error type shared by the Pezhai library surface.
pub use error::Error;
pub use iyakkam::{
    Bound, DeferredGet, DeferredScanPage, DurabilityWait, GetDecision, GetResponse, LevelStats,
    LogicalShardStats, PagedScan, PezhaiEngine, ScanCursor, ScanPageDecision, ScanPageLimits,
    ScanPageResponse, ScanRange, ScanRow, SnapshotHandle, StatsResponse, SyncResponse,
    WriteDecision,
};
