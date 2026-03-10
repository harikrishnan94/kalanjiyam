//! Library entry point for `kalanjiyam`.
//!
//! This crate intentionally exposes only a tiny amount of functionality.
//! The goal is to keep a valid Rust library in place while the architecture,
//! domain model, and distributed systems design are still being shaped.

/// Returns the project name.
pub fn project_name() -> &'static str {
    "kalanjiyam"
}

/// Returns a one-line statement of intent for the repository.
pub fn intent() -> &'static str {
    "A distributed ACID-compliant key-value store, to be built incrementally."
}

#[cfg(test)]
mod tests {
    use super::{intent, project_name};

    #[test]
    fn project_identity_is_stable() {
        assert_eq!(project_name(), "kalanjiyam");
        assert!(intent().contains("ACID"));
    }
}
