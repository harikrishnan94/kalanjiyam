//! Library entry point for the `pezhai` storage engine crate.

mod config;
pub mod error;
pub mod sevai;

/// Crate-level error type shared by the Pezhai library surface.
pub use error::Error;
