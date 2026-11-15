use thiserror::Error;

/// Bounded, non-authoritative details of a rejected OAT routing decision.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct OatRoutingDiagnostic {
    /// Fixed ineligibility reason, if the holder was ineligible.
    pub reason: Option<&'static str>,
    /// Time used to evaluate the decision.
    pub decision_unix_ms: u64,
    /// Snapshot observation minus decision time.
    pub observed_minus_decision_ms: Option<i64>,
    /// Snapshot validation detail, if invalid.
    pub validation_reason: Option<String>,
    /// Collector registration start.
    pub registration_started_unix_ms: Option<u64>,
    /// Collector registration expiry.
    pub registration_expiry_unix_ms: Option<u64>,
    /// Immutable pin expiry.
    pub pin_expiry_unix_ms: u64,
    /// Routing lease expiry, if present.
    pub lease_expiry_unix_ms: Option<u64>,
    /// Time spent reading the pin.
    pub pin_read_ms: u64,
    /// Time spent loading the routing view.
    pub load_view_ms: u64,
}

/// Stable category for routing-control failures.
#[derive(Clone, Copy, Debug, Eq, Error, PartialEq)]
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
    /// A valid lease holder is currently rejecting new streams under hard pressure.
    #[error("table lease holder is overloaded")]
    LeaseHolderOverloaded,
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
#[derive(Clone, Debug, Eq, Error, PartialEq)]
#[error("{kind}: {message}")]
pub struct RoutingError {
    kind: RoutingErrorKind,
    message: String,
    oat_diagnostic: Option<Box<OatRoutingDiagnostic>>,
}

impl RoutingError {
    /// Creates a routing error.
    #[must_use]
    pub fn new(kind: RoutingErrorKind, message: impl Into<String>) -> Self {
        Self {
            kind,
            message: message.into(),
            oat_diagnostic: None,
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

    /// Returns decision-local evidence when available.
    #[must_use]
    pub fn oat_diagnostic(&self) -> Option<&OatRoutingDiagnostic> {
        self.oat_diagnostic.as_deref()
    }

    pub(crate) fn with_oat_diagnostic(mut self, diagnostic: OatRoutingDiagnostic) -> Self {
        self.oat_diagnostic = Some(Box::new(diagnostic));
        self
    }
}

/// Result type used by routing APIs.
pub type RoutingResult<T> = Result<T, RoutingError>;
