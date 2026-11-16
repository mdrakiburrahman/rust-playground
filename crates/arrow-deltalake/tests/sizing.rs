use anyhow::{Context, Result};
use arrow_deltalake::CompletedFileObservation;
use arrow_deltalake::sizing::{
    AdaptiveFileSizeConfig, AdaptiveFileSizeEstimator, FileSizeEstimatorError, FlushTrigger,
};

fn config() -> AdaptiveFileSizeConfig {
    AdaptiveFileSizeConfig {
        target_parquet_bytes: 1_000,
        min_input_bytes: 100,
        max_input_bytes: 10_000,
        cold_start_parquet_ratio: 0.5,
        ewma_weight: 0.25,
        min_parquet_ratio: 0.1,
        max_parquet_ratio: 2.0,
    }
}

fn observation(rows: usize, input_bytes: u64, parquet_bytes: u64) -> CompletedFileObservation {
    CompletedFileObservation {
        rows,
        input_bytes,
        parquet_bytes,
    }
}

/// Scenario: completed files repeatedly compress to one quarter of logical input.
/// Guarantees: the bounded EWMA converges smoothly and returns an input-byte target.
#[test]
fn estimator_converges_without_buffering_batches() -> Result<()> {
    let mut estimator = AdaptiveFileSizeEstimator::new(config())?;
    assert_eq!(estimator.input_bytes_target(), 2_000);
    for _ in 0..24 {
        estimator.observe(observation(100, 4_000, 1_000))?;
    }
    assert_eq!(estimator.observations(), 24);
    assert!((estimator.estimated_parquet_ratio() - 0.25).abs() < 0.001);
    assert!((3_990..=4_000).contains(&estimator.input_bytes_target()));
    assert!(estimator.should_flush(estimator.input_bytes_target()));
    Ok(())
}

/// Scenario: a consumer accepts the SDK defaults.
/// Guarantees: the compressed target is 128 MiB and cold-start sizing is bounded.
#[test]
fn estimator_defaults_to_128_mib_parquet_files() {
    let estimator = AdaptiveFileSizeEstimator::default();
    assert_eq!(estimator.config().target_parquet_bytes, 128 * 1024 * 1024);
    assert_eq!(estimator.input_bytes_target(), 512 * 1024 * 1024);
}

/// Scenario: configured cold-start ratios imply targets outside both input bounds.
/// Guarantees: every returned threshold obeys inclusive minimum and maximum clamps.
#[test]
fn estimator_clamps_input_targets() -> Result<()> {
    let mut high_ratio = config();
    high_ratio.min_input_bytes = 800;
    high_ratio.max_input_bytes = 5_000;
    high_ratio.cold_start_parquet_ratio = 2.0;
    let high_ratio = AdaptiveFileSizeEstimator::new(high_ratio)?;
    assert_eq!(high_ratio.input_bytes_target(), 800);

    let mut low_ratio = config();
    low_ratio.min_input_bytes = 800;
    low_ratio.max_input_bytes = 5_000;
    low_ratio.cold_start_parquet_ratio = 0.1;
    let low_ratio = AdaptiveFileSizeEstimator::new(low_ratio)?;
    assert_eq!(low_ratio.input_bytes_target(), 5_000);
    Ok(())
}

/// Scenario: the adaptive target is reached normally or clamped by input bounds.
/// Guarantees: each flush reports the threshold that caused it, without changing `should_flush`.
#[test]
fn estimator_reports_target_and_clamp_flush_triggers() -> Result<()> {
    let normal = AdaptiveFileSizeEstimator::new(config())?;
    assert_eq!(normal.flush_trigger(1_999), None);
    assert_eq!(normal.flush_trigger(2_000), Some(FlushTrigger::TargetParquetBytes));

    let mut minimum = config();
    minimum.min_input_bytes = 2_500;
    let minimum = AdaptiveFileSizeEstimator::new(minimum)?;
    assert_eq!(minimum.flush_trigger(2_499), None);
    assert_eq!(minimum.flush_trigger(2_500), Some(FlushTrigger::MinInputBytes));

    let mut maximum = config();
    maximum.max_input_bytes = 1_500;
    let maximum = AdaptiveFileSizeEstimator::new(maximum)?;
    assert_eq!(maximum.flush_trigger(1_499), None);
    assert_eq!(maximum.flush_trigger(1_500), Some(FlushTrigger::MaxInputBytes));
    Ok(())
}

/// Scenario: callers supply malformed floats, bounds, and empty completed-file counters.
/// Guarantees: bad input is rejected without mutating a live estimate.
#[test]
fn estimator_rejects_invalid_configuration_and_observations() -> Result<()> {
    let mut invalid = config();
    invalid.target_parquet_bytes = 0;
    assert_eq!(
        AdaptiveFileSizeEstimator::new(invalid)
            .err()
            .context("zero target unexpectedly succeeded")?,
        FileSizeEstimatorError::InvalidTarget
    );
    invalid = config();
    invalid.min_input_bytes = 0;
    assert_eq!(
        AdaptiveFileSizeEstimator::new(invalid)
            .err()
            .context("zero minimum unexpectedly succeeded")?,
        FileSizeEstimatorError::InvalidInputBounds
    );
    invalid = config();
    invalid.cold_start_parquet_ratio = f64::NAN;
    assert_eq!(
        AdaptiveFileSizeEstimator::new(invalid)
            .err()
            .context("NaN ratio unexpectedly succeeded")?,
        FileSizeEstimatorError::InvalidColdStartRatio
    );
    invalid = config();
    invalid.ewma_weight = 0.75;
    assert_eq!(
        AdaptiveFileSizeEstimator::new(invalid)
            .err()
            .context("aggressive EWMA unexpectedly succeeded")?,
        FileSizeEstimatorError::InvalidEwmaWeight
    );
    invalid = config();
    invalid.max_parquet_ratio = f64::INFINITY;
    assert_eq!(
        AdaptiveFileSizeEstimator::new(invalid)
            .err()
            .context("infinite bound unexpectedly succeeded")?,
        FileSizeEstimatorError::InvalidRatioBounds
    );

    let mut estimator = AdaptiveFileSizeEstimator::new(config())?;
    let original_ratio = estimator.estimated_parquet_ratio();
    assert_eq!(
        estimator.observe(observation(0, 1, 1)),
        Err(FileSizeEstimatorError::EmptyObservation)
    );
    assert_eq!(
        estimator.observe(observation(1, 0, 1)),
        Err(FileSizeEstimatorError::ZeroInputBytes)
    );
    assert_eq!(
        estimator.observe(observation(1, 1, 0)),
        Err(FileSizeEstimatorError::ZeroParquetBytes)
    );
    assert_eq!(estimator.observations(), 0);
    assert_eq!(estimator.estimated_parquet_ratio(), original_ratio);
    Ok(())
}

/// Scenario: counters approach their integer limits and the observed ratio is extreme.
/// Guarantees: conversion, EWMA, and target calculation stay finite and bounded.
#[test]
fn estimator_handles_extreme_counters_without_overflow() -> Result<()> {
    let extreme = AdaptiveFileSizeConfig {
        target_parquet_bytes: u64::MAX,
        min_input_bytes: 1,
        max_input_bytes: u64::MAX,
        cold_start_parquet_ratio: 1.0,
        ewma_weight: 0.5,
        min_parquet_ratio: 0.5,
        max_parquet_ratio: 2.0,
    };
    let mut estimator = AdaptiveFileSizeEstimator::new(extreme)?;
    let target = estimator.observe(observation(1, 1, u64::MAX))?;
    assert!((1..=u64::MAX).contains(&target));
    assert!(estimator.estimated_parquet_ratio().is_finite());
    Ok(())
}
