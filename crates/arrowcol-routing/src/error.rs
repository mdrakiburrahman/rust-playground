use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Stable category for routing-control failures.
#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, Error, PartialEq)]
#[non_exhaustive]
pub enum RoutingErrorKind {
    /// Configuration or caller data violated a bound.
    #[error("invalid routing configuration")]
    InvalidConfiguration,
    /// A pressure document was stale, future-dated, or inconsistent.
    #[error("invalid collector pressure snapshot")]
    InvalidSnapshot,
    /// No eligible collector can safely receive this operation.
    #[error("collector pool is unavailable")]
    PoolUnavailable,
    /// A table lease is draining and rejects new streams.
    #[error("table lease is draining")]
    LeaseDraining,
    /// A valid lease exists, but its holder is not safe for a new stream.
    #[error("table lease holder is unavailable")]
    LeaseHolderUnavailable,
    /// A compare-and-set lease mutation lost a race.
    #[error("routing lease compare-and-set conflicted")]
    Conflict,
    /// Persisted routing data violated an invariant.
    #[error("routing persistence state is corrupt")]
    CorruptState,
    /// SQL Server was unavailable or rejected an operation.
    #[error("routing database operation failed")]
    Database,
}

/// Structured routing failure with safe operator detail.
#[derive(Clone, Debug, Deserialize, Serialize, Eq, Error, PartialEq)]
#[error("{kind}: {message}")]
pub struct RoutingError {
    kind: RoutingErrorKind,
    message: String,
}

impl RoutingError {
    /// Creates a routing error.
    #[must_use]
    pub fn new(kind: RoutingErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
        }
    }

    /// Returns the machine-readable category.
    #[must_use]
    pub const fn kind(&self) -> RoutingErrorKind {
        self.kind
    }

    /// Returns safe diagnostic text.
    #[must_use]
    pub fn message(&self) -> &str {
        &self.message
    }
}

/// Result type used by routing APIs.
pub type RoutingResult<T> = Result<T, RoutingError>;
