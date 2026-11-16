//! Bounded adaptive sizing for callers that rotate streaming files.
//!
//! The estimator stores only a compression-ratio estimate. Callers retain
//! ownership of batches and decide when to flush based on
//! [`AdaptiveFileSizeEstimator::input_bytes_target`].

use crate::CompletedFileObservation;
use std::error::Error;
use std::fmt::{self, Display, Formatter};

/// Default compressed Parquet target: 128 MiB.
pub const DEFAULT_TARGET_PARQUET_BYTES: u64 = 128 * 1024 * 1024;

/// Configuration for [`AdaptiveFileSizeEstimator`].
///
/// Ratios are durable Parquet bytes divided by logical input Arrow bytes.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct AdaptiveFileSizeConfig {
    /// Desired durable Parquet bytes per completed file.
    pub target_parquet_bytes: u64,
    /// Smallest logical-input threshold returned by the estimator.
    pub min_input_bytes: u64,
    /// Largest logical-input threshold returned by the estimator.
    pub max_input_bytes: u64,
    /// Initial durable-Parquet/logical-input ratio before observations.
    pub cold_start_parquet_ratio: f64,
    /// Weight assigned to one new observation; limited to `0.5` for stability.
    pub ewma_weight: f64,
    /// Lower clamp for observed and estimated Parquet/input ratios.
    pub min_parquet_ratio: f64,
    /// Upper clamp for observed and estimated Parquet/input ratios.
    pub max_parquet_ratio: f64,
}

impl Default for AdaptiveFileSizeConfig {
    fn default() -> Self {
        Self {
            target_parquet_bytes: DEFAULT_TARGET_PARQUET_BYTES,
            min_input_bytes: 16 * 1024 * 1024,
            max_input_bytes: 1024 * 1024 * 1024,
            cold_start_parquet_ratio: 0.25,
            ewma_weight: 0.2,
            min_parquet_ratio: 1.0 / 64.0,
            max_parquet_ratio: 4.0,
        }
    }
}

impl AdaptiveFileSizeConfig {
    /// Read adaptive sizing controls from validated catalog properties.
    ///
    /// # Errors
    /// Returns property errors or invalid cross-property estimator bounds.
    pub fn from_properties(props: &arrowcol_props::ResolvedProps) -> anyhow::Result<Self> {
        let config = Self {
            target_parquet_bytes: props.unsigned("arrowcol.writer.targetFileSize")?,
            min_input_bytes: props.unsigned("arrowcol.writer.minInputSize")?,
            max_input_bytes: props.unsigned("arrowcol.writer.maxInputSize")?,
            cold_start_parquet_ratio: props.float("arrowcol.writer.coldStartParquetRatio")?,
            ewma_weight: props.float("arrowcol.writer.ewmaWeight")?,
            min_parquet_ratio: props.float("arrowcol.writer.minParquetRatio")?,
            max_parquet_ratio: props.float("arrowcol.writer.maxParquetRatio")?,
        };
        config.validate()?;
        Ok(config)
    }

    /// Validate all bounds and floating-point inputs.
    ///
    /// # Errors
    /// Returns the offending configuration category.
    pub fn validate(&self) -> Result<(), FileSizeEstimatorError> {
        if self.target_parquet_bytes == 0 {
            return Err(FileSizeEstimatorError::InvalidTarget);
        }
        if self.min_input_bytes == 0 || self.min_input_bytes > self.max_input_bytes {
            return Err(FileSizeEstimatorError::InvalidInputBounds);
        }
        if !valid_ratio(self.min_parquet_ratio)
            || !valid_ratio(self.max_parquet_ratio)
            || self.min_parquet_ratio > self.max_parquet_ratio
            || !(self.target_parquet_bytes as f64 / self.min_parquet_ratio).is_finite()
        {
            return Err(FileSizeEstimatorError::InvalidRatioBounds);
        }
        if !valid_ratio(self.cold_start_parquet_ratio)
            || self.cold_start_parquet_ratio < self.min_parquet_ratio
            || self.cold_start_parquet_ratio > self.max_parquet_ratio
        {
            return Err(FileSizeEstimatorError::InvalidColdStartRatio);
        }
        if !self.ewma_weight.is_finite() || self.ewma_weight <= 0.0 || self.ewma_weight > 0.5 {
            return Err(FileSizeEstimatorError::InvalidEwmaWeight);
        }
        Ok(())
    }
}

fn valid_ratio(value: f64) -> bool {
    value.is_finite() && value > 0.0
}

/// Invalid estimator configuration or completed-file observation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum FileSizeEstimatorError {
    /// Compressed target was zero.
    InvalidTarget,
    /// Input clamps were zero or inverted.
    InvalidInputBounds,
    /// Cold-start ratio was non-finite, non-positive, or outside bounds.
    InvalidColdStartRatio,
    /// EWMA weight was non-finite, non-positive, or greater than one half.
    InvalidEwmaWeight,
    /// Ratio clamps were non-finite, non-positive, inverted, or unusably small.
    InvalidRatioBounds,
    /// Completed file contained no rows.
    EmptyObservation,
    /// Completed file contained no measurable logical input.
    ZeroInputBytes,
    /// Completed file contained no durable Parquet bytes.
    ZeroParquetBytes,
}

impl Display for FileSizeEstimatorError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::InvalidTarget => "target Parquet bytes must be positive",
            Self::InvalidInputBounds => "input byte clamps require 0 < minimum <= maximum",
            Self::InvalidColdStartRatio => {
                "cold-start Parquet/input ratio must be finite, positive, and within ratio bounds"
            }
            Self::InvalidEwmaWeight => "EWMA weight must be finite and in (0, 0.5]",
            Self::InvalidRatioBounds => "Parquet/input ratio bounds must be finite, positive, and ordered",
            Self::EmptyObservation => "completed-file observation must contain rows",
            Self::ZeroInputBytes => "completed-file observation must contain input bytes",
            Self::ZeroParquetBytes => "completed-file observation must contain Parquet bytes",
        })
    }
}

impl Error for FileSizeEstimatorError {}

/// A bounded EWMA of completed-file Parquet/input compression ratios.
///
/// This type never retains a [`arrow::array::RecordBatch`]. The caller tracks
/// the current file's input bytes and compares them with
/// [`Self::input_bytes_target`] or calls [`Self::should_flush`].
#[derive(Clone, Debug)]
pub struct AdaptiveFileSizeEstimator {
    config: AdaptiveFileSizeConfig,
    parquet_ratio: f64,
    observations: u64,
}

impl Default for AdaptiveFileSizeEstimator {
    fn default() -> Self {
        let config = AdaptiveFileSizeConfig::default();
        Self {
            parquet_ratio: config.cold_start_parquet_ratio,
            config,
            observations: 0,
        }
    }
}

impl AdaptiveFileSizeEstimator {
    /// Construct a cold estimator.
    ///
    /// # Errors
    /// Rejects zero/inverted bounds, non-finite ratios, overly aggressive EWMA
    /// weights, and ratios that could make target calculation non-finite.
    pub fn new(config: AdaptiveFileSizeConfig) -> Result<Self, FileSizeEstimatorError> {
        config.validate()?;
        Ok(Self {
            parquet_ratio: config.cold_start_parquet_ratio,
            config,
            observations: 0,
        })
    }

    /// Return the validated configuration.
    #[must_use]
    pub const fn config(&self) -> &AdaptiveFileSizeConfig {
        &self.config
    }

    /// Number of accepted completed-file observations.
    #[must_use]
    pub const fn observations(&self) -> u64 {
        self.observations
    }

    /// Current bounded estimate of durable Parquet bytes per logical input byte.
    #[must_use]
    pub const fn estimated_parquet_ratio(&self) -> f64 {
        self.parquet_ratio
    }

    /// Logical input-byte threshold for the next file.
    ///
    /// The result is always within the configured inclusive input clamps.
    #[must_use]
    pub fn input_bytes_target(&self) -> u64 {
        let desired = self.config.target_parquet_bytes as f64 / self.parquet_ratio;
        if !desired.is_finite() || desired >= self.config.max_input_bytes as f64 {
            self.config.max_input_bytes
        } else if desired <= self.config.min_input_bytes as f64 {
            self.config.min_input_bytes
        } else {
            desired.ceil() as u64
        }
    }

    /// Return whether the caller's current logical byte count reached the target.
    #[must_use]
    pub fn should_flush(&self, current_input_bytes: u64) -> bool {
        current_input_bytes >= self.input_bytes_target()
    }

    /// Learn from one completed durable file and return the next input target.
    ///
    /// Observed ratios are clamped before the conservative EWMA update. Invalid
    /// observations do not mutate estimator state.
    ///
    /// # Errors
    /// Rejects observations with no rows, input bytes, or Parquet bytes.
    pub fn observe(&mut self, observation: CompletedFileObservation) -> Result<u64, FileSizeEstimatorError> {
        if observation.rows == 0 {
            return Err(FileSizeEstimatorError::EmptyObservation);
        }
        if observation.input_bytes == 0 {
            return Err(FileSizeEstimatorError::ZeroInputBytes);
        }
        if observation.parquet_bytes == 0 {
            return Err(FileSizeEstimatorError::ZeroParquetBytes);
        }

        let observed = (observation.parquet_bytes as f64 / observation.input_bytes as f64)
            .clamp(self.config.min_parquet_ratio, self.config.max_parquet_ratio);
        let retained_weight = 1.0 - self.config.ewma_weight;
        self.parquet_ratio = self
            .parquet_ratio
            .mul_add(retained_weight, observed * self.config.ewma_weight)
            .clamp(self.config.min_parquet_ratio, self.config.max_parquet_ratio);
        self.observations = self.observations.saturating_add(1);
        Ok(self.input_bytes_target())
    }
}
