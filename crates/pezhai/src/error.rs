//! Crate-wide error types shared by the library surface and server binary package.

use std::fmt::{self, Display, Formatter};

/// Errors returned by the scaffolded `sevai` server and bootstrap helpers.
#[derive(Debug)]
pub enum Error {
    /// The config file could not be read from disk.
    Io(std::io::Error),
    /// The config file or request shape was semantically invalid.
    InvalidArgument(String),
    /// The server rejected admission because it is saturated or stopping.
    Busy(String),
    /// The requested operation was cancelled before it completed.
    Cancelled(String),
    /// The server package's TCP adapter saw an invalid frame or protobuf payload.
    Protocol(String),
    /// The server owner task was no longer available to accept work.
    ServerUnavailable(String),
}

impl Display for Error {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io(error) => write!(formatter, "I/O error: {error}"),
            Self::InvalidArgument(message) => write!(formatter, "invalid argument: {message}"),
            Self::Busy(message) => write!(formatter, "server busy: {message}"),
            Self::Cancelled(message) => write!(formatter, "operation cancelled: {message}"),
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
            | Self::Busy(_)
            | Self::Cancelled(_)
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

#[cfg(test)]
mod tests {
    use super::Error;

    #[test]
    fn display_messages_cover_all_variants() {
        let io_error = Error::Io(std::io::Error::other("disk"));
        assert!(io_error.to_string().contains("disk"));

        let invalid = Error::InvalidArgument("bad request".into());
        assert_eq!(invalid.to_string(), "invalid argument: bad request");

        let busy = Error::Busy("full".into());
        assert_eq!(busy.to_string(), "server busy: full");

        let cancelled = Error::Cancelled("queued op".into());
        assert_eq!(cancelled.to_string(), "operation cancelled: queued op");

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
