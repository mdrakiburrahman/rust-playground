//! Shared cloud-generation, telemetry, and safe diagnostic utilities for Arrowcol.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod diagnostics;
mod generation;
mod telemetry;

pub use diagnostics::safe_error_message;
pub use generation::CloudGeneration;
pub use telemetry::MetricsGuard;
