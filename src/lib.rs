//! Library entry point for the pezhai storage engine and its in-process host.

pub mod error;
pub mod pezhai {
    //! Engine-side modules for pezhai storage behavior and durable formats.

    pub mod codec;
    pub mod config;
    pub mod durable;
    pub mod engine;
    pub mod types;
}
pub mod sevai;

pub use error::{Error, ErrorKind, KalanjiyamError};
pub use pezhai::engine::PezhaiEngine;
pub use pezhai::types::{
    Bound, GetResponse, KeyRange, LevelStats, LogicalShardEntry, LogicalShardStats, ScanPage,
    ScanRow, SnapshotHandle, StatsResponse, SyncMode, SyncResponse,
};
pub use sevai::{
    ExternalMethod, ExternalRequest, ExternalResponse, ExternalResponsePayload, PezhaiServer,
    ServerBootstrapArgs, Status, StatusCode,
};
