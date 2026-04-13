//! Crate-wide error types shared by the engine surface and the sevai server runtime.

use std::fmt::{self, Display, Formatter};

/// Errors returned by the `pezhai` engine and the `pezhai::sevai` server.
#[derive(Debug)]
pub enum Error {
    /// Shared engine/server error for invalid config, API, or request arguments.
    InvalidArgument(String),
    /// Shared engine/server error for underlying filesystem or socket I/O failures.
    Io(std::io::Error),
    /// Shared engine/server error for checksum mismatches in durable or transport data.
    Checksum(String),
    /// Shared engine/server error for structurally invalid or self-inconsistent durable state.
    Corruption(String),
    /// Engine-facing error for stale background-work results or invalidated snapshot state.
    Stale(String),
    /// Shared engine/server error for cancelled work.
    Cancelled(String),
    /// Server-only admission error for saturated queues or shutdown pressure.
    Busy(String),
    /// Server-only transport error for malformed frames or protobuf payloads.
    Protocol(String),
    /// Server-only runtime error for a stopped or unavailable owner task.
    ServerUnavailable(String),
}

impl Display for Error {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidArgument(message) => write!(formatter, "invalid argument: {message}"),
            Self::Io(error) => write!(formatter, "I/O error: {error}"),
            Self::Checksum(message) => write!(formatter, "checksum error: {message}"),
            Self::Corruption(message) => write!(formatter, "corruption error: {message}"),
            Self::Stale(message) => write!(formatter, "stale state: {message}"),
            Self::Cancelled(message) => write!(formatter, "operation cancelled: {message}"),
            Self::Busy(message) => write!(formatter, "server busy: {message}"),
            Self::Protocol(message) => write!(formatter, "protocol error: {message}"),
            Self::ServerUnavailable(message) => write!(formatter, "server unavailable: {message}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(error) => Some(error),
            Self::InvalidArgument(_)
            | Self::Checksum(_)
            | Self::Corruption(_)
            | Self::Stale(_)
            | Self::Cancelled(_)
            | Self::Busy(_)
            | Self::Protocol(_)
            | Self::ServerUnavailable(_) => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::Io(error)
    }
}

impl From<crate::config::ConfigError> for Error {
    fn from(error: crate::config::ConfigError) -> Self {
        match error {
            crate::config::ConfigError::Io(error) => Self::Io(error),
            crate::config::ConfigError::Parse(error) => Self::InvalidArgument(error.to_string()),
            crate::config::ConfigError::Invalid(message) => Self::InvalidArgument(message),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Error;

    #[test]
    fn display_messages_cover_all_variants() {
        let invalid = Error::InvalidArgument("bad request".into());
        assert_eq!(invalid.to_string(), "invalid argument: bad request");

        let io_error = Error::Io(std::io::Error::other("disk"));
        assert!(io_error.to_string().contains("disk"));

        let checksum = Error::Checksum("crc32c mismatch".into());
        assert_eq!(checksum.to_string(), "checksum error: crc32c mismatch");

        let corruption = Error::Corruption("bad footer".into());
        assert_eq!(corruption.to_string(), "corruption error: bad footer");

        let stale = Error::Stale("flush result was superseded".into());
        assert_eq!(
            stale.to_string(),
            "stale state: flush result was superseded"
        );

        let cancelled = Error::Cancelled("queued op".into());
        assert_eq!(cancelled.to_string(), "operation cancelled: queued op");

        let busy = Error::Busy("full".into());
        assert_eq!(busy.to_string(), "server busy: full");

        let protocol = Error::Protocol("frame".into());
        assert_eq!(protocol.to_string(), "protocol error: frame");

        let unavailable = Error::ServerUnavailable("stopped".into());
        assert_eq!(unavailable.to_string(), "server unavailable: stopped");
    }

    #[test]
    fn source_is_exposed_only_for_io_errors() {
        let io_error = Error::Io(std::io::Error::other("disk"));
        assert!(std::error::Error::source(&io_error).is_some());

        let invalid = Error::InvalidArgument("bad request".into());
        assert!(std::error::Error::source(&invalid).is_none());
    }

    #[test]
    fn io_error_conversion_wraps_the_original_error() {
        let error: Error = std::io::Error::other("disk").into();
        assert_eq!(error.to_string(), "I/O error: disk");
    }
}
