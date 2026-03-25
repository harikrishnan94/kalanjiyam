//! Crate-level error types shared by the engine, durable layer, and server host.

use std::fmt::{Display, Formatter};

/// Stable error-code classes used by the engine and in-process server.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ErrorKind {
    /// Admission-time backpressure before visible mutation.
    Busy,
    /// The caller supplied invalid input or an invalid handle.
    InvalidArgument,
    /// Required filesystem or runtime I/O failed.
    Io,
    /// A checksum field did not match the covered bytes.
    Checksum,
    /// Durable bytes violated an invariant that should already hold.
    Corruption,
    /// A background result no longer matches current publish preconditions.
    Stale,
    /// A queued or delegated operation was cancelled before completion.
    Cancelled,
}

/// Legacy error enum retained for compatibility with existing config and type code.
///
/// New engine/server code should use `Error`, but this enum remains available
/// for modules that already return `KalanjiyamError`.
#[derive(Debug)]
pub enum KalanjiyamError {
    /// Admission-time backpressure before visible mutation.
    Busy(String),
    /// The caller supplied invalid input or an invalid handle.
    InvalidArgument(String),
    /// Required filesystem or runtime I/O failed.
    Io(std::io::Error),
    /// A checksum field did not match the covered bytes.
    Checksum(String),
    /// Durable bytes violated an invariant that should already hold.
    Corruption(String),
    /// A background result no longer matches current publish preconditions.
    Stale(String),
    /// A queued or delegated operation was cancelled before completion.
    Cancelled(String),
}

impl KalanjiyamError {
    /// Builds an `IO` error from context and the original source error.
    ///
    /// # Complexity
    /// Time: O(m), where `m` is the length of the formatted error message.
    /// Space: O(m).
    #[must_use]
    pub fn io(context: impl Into<String>, source: std::io::Error) -> Self {
        Self::Io(std::io::Error::new(
            source.kind(),
            format!("{}: {}", context.into(), source),
        ))
    }

    /// Returns the stable error kind.
    ///
    /// # Complexity
    /// Time: O(1).
    /// Space: O(1).
    #[must_use]
    pub fn kind(&self) -> ErrorKind {
        match self {
            Self::Busy(_) => ErrorKind::Busy,
            Self::InvalidArgument(_) => ErrorKind::InvalidArgument,
            Self::Io(_) => ErrorKind::Io,
            Self::Checksum(_) => ErrorKind::Checksum,
            Self::Corruption(_) => ErrorKind::Corruption,
            Self::Stale(_) => ErrorKind::Stale,
            Self::Cancelled(_) => ErrorKind::Cancelled,
        }
    }

    /// Returns the externally visible error-code name required by the specs.
    ///
    /// # Complexity
    /// Time: O(1).
    /// Space: O(1).
    #[must_use]
    pub fn code_name(&self) -> &'static str {
        match self.kind() {
            ErrorKind::Busy => "Busy",
            ErrorKind::InvalidArgument => "InvalidArgument",
            ErrorKind::Io => "IO",
            ErrorKind::Checksum => "Checksum",
            ErrorKind::Corruption => "Corruption",
            ErrorKind::Stale => "Stale",
            ErrorKind::Cancelled => "Cancelled",
        }
    }

    /// Returns the human-readable message.
    ///
    /// # Complexity
    /// Time: O(m), where `m` is the returned message length.
    /// Space: O(m).
    #[must_use]
    pub fn message(&self) -> String {
        match self {
            Self::Busy(message)
            | Self::InvalidArgument(message)
            | Self::Checksum(message)
            | Self::Corruption(message)
            | Self::Stale(message)
            | Self::Cancelled(message) => message.clone(),
            Self::Io(source) => source.to_string(),
        }
    }

    /// Returns whether retrying later may succeed.
    ///
    /// # Complexity
    /// Time: O(1).
    /// Space: O(1).
    #[must_use]
    pub fn retryable(&self) -> bool {
        match self.kind() {
            ErrorKind::Busy => true,
            ErrorKind::InvalidArgument => false,
            ErrorKind::Io => true,
            ErrorKind::Checksum => false,
            ErrorKind::Corruption => false,
            ErrorKind::Stale => false,
            ErrorKind::Cancelled => true,
        }
    }
}

impl Display for KalanjiyamError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Busy(message) => write!(f, "busy: {message}"),
            Self::InvalidArgument(message) => write!(f, "invalid argument: {message}"),
            Self::Io(source) => write!(f, "io: {source}"),
            Self::Checksum(message) => write!(f, "checksum: {message}"),
            Self::Corruption(message) => write!(f, "corruption: {message}"),
            Self::Stale(message) => write!(f, "stale: {message}"),
            Self::Cancelled(message) => write!(f, "cancelled: {message}"),
        }
    }
}

impl std::error::Error for KalanjiyamError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io(source) => Some(source),
            _ => None,
        }
    }
}

impl From<std::io::Error> for KalanjiyamError {
    fn from(value: std::io::Error) -> Self {
        Self::Io(value)
    }
}

/// Primary crate error used by engine, durable, and server code paths.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Error {
    kind: ErrorKind,
    message: String,
}

impl Error {
    /// Builds a new error from one stable kind and message.
    ///
    /// # Complexity
    /// Time: O(m), where `m` is the stored message length.
    /// Space: O(m).
    #[must_use]
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    /// Builds a `Busy` error.
    ///
    /// # Complexity
    /// Time: O(m), where `m` is the stored message length.
    /// Space: O(m).
    #[must_use]
    pub fn busy(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Busy, message)
    }

    /// Builds an `InvalidArgument` error.
    ///
    /// # Complexity
    /// Time: O(m), where `m` is the stored message length.
    /// Space: O(m).
    #[must_use]
    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::InvalidArgument, message)
    }

    /// Builds an `IO` error.
    ///
    /// # Complexity
    /// Time: O(m), where `m` is the stored message length.
    /// Space: O(m).
    #[must_use]
    pub fn io(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Io, message)
    }

    /// Builds a `Checksum` error.
    ///
    /// # Complexity
    /// Time: O(m), where `m` is the stored message length.
    /// Space: O(m).
    #[must_use]
    pub fn checksum(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Checksum, message)
    }

    /// Builds a `Corruption` error.
    ///
    /// # Complexity
    /// Time: O(m), where `m` is the stored message length.
    /// Space: O(m).
    #[must_use]
    pub fn corruption(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Corruption, message)
    }

    /// Builds a `Stale` error.
    ///
    /// # Complexity
    /// Time: O(m), where `m` is the stored message length.
    /// Space: O(m).
    #[must_use]
    pub fn stale(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Stale, message)
    }

    /// Builds a `Cancelled` error.
    ///
    /// # Complexity
    /// Time: O(m), where `m` is the stored message length.
    /// Space: O(m).
    #[must_use]
    pub fn cancelled(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Cancelled, message)
    }

    /// Returns the stable error kind.
    ///
    /// # Complexity
    /// Time: O(1).
    /// Space: O(1).
    #[must_use]
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// Returns the message content.
    ///
    /// # Complexity
    /// Time: O(1).
    /// Space: O(1).
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }

    /// Returns whether retrying later may succeed.
    ///
    /// # Complexity
    /// Time: O(1).
    /// Space: O(1).
    #[must_use]
    pub fn retryable(&self) -> bool {
        match self.kind {
            ErrorKind::Busy => true,
            ErrorKind::InvalidArgument => false,
            ErrorKind::Io => true,
            ErrorKind::Checksum => false,
            ErrorKind::Corruption => false,
            ErrorKind::Stale => false,
            ErrorKind::Cancelled => true,
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}: {}", self.code_name(), self.message)
    }
}

impl std::error::Error for Error {}

impl Error {
    fn code_name(&self) -> &'static str {
        match self.kind {
            ErrorKind::Busy => "Busy",
            ErrorKind::InvalidArgument => "InvalidArgument",
            ErrorKind::Io => "IO",
            ErrorKind::Checksum => "Checksum",
            ErrorKind::Corruption => "Corruption",
            ErrorKind::Stale => "Stale",
            ErrorKind::Cancelled => "Cancelled",
        }
    }
}

impl From<KalanjiyamError> for Error {
    fn from(value: KalanjiyamError) -> Self {
        Self {
            kind: value.kind(),
            message: value.message(),
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(value: std::io::Error) -> Self {
        Self::io(value.to_string())
    }
}

/// Shared result alias used by engine and server code.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;
    use std::error::Error as _;

    #[test]
    fn legacy_error_display_covers_all_non_io_variants() {
        let cases = [
            (KalanjiyamError::Busy("busy".to_string()), "busy: busy"),
            (
                KalanjiyamError::InvalidArgument("bad".to_string()),
                "invalid argument: bad",
            ),
            (
                KalanjiyamError::Checksum("crc".to_string()),
                "checksum: crc",
            ),
            (
                KalanjiyamError::Corruption("broken".to_string()),
                "corruption: broken",
            ),
            (KalanjiyamError::Stale("stale".to_string()), "stale: stale"),
            (
                KalanjiyamError::Cancelled("cancelled".to_string()),
                "cancelled: cancelled",
            ),
        ];

        for (error, display) in cases {
            assert_eq!(error.to_string(), display);
            assert!(error.source().is_none());
        }
    }

    #[test]
    fn error_display_covers_remaining_code_names() {
        let cases = [
            (Error::busy("busy"), "Busy: busy"),
            (Error::invalid_argument("bad"), "InvalidArgument: bad"),
            (Error::corruption("broken"), "Corruption: broken"),
            (Error::stale("stale"), "Stale: stale"),
            (Error::cancelled("cancelled"), "Cancelled: cancelled"),
        ];

        for (error, display) in cases {
            assert_eq!(error.to_string(), display);
        }
    }

    #[test]
    fn legacy_and_new_io_conversions_preserve_kind_and_message() {
        let legacy: KalanjiyamError = std::io::Error::other("legacy io").into();
        assert_eq!(legacy.kind(), ErrorKind::Io);
        assert_eq!(legacy.message(), "legacy io");

        let newer: Error = KalanjiyamError::Busy("busy".to_string()).into();
        assert_eq!(newer.kind(), ErrorKind::Busy);
        assert_eq!(newer.message(), "busy");
    }
}
