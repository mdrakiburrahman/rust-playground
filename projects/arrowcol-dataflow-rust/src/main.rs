use std::cell::RefCell;
use std::rc::Rc;

use arrowcol_dataflow_components as _;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    emit_wal_effective_config()?;
    otel_arrow_dfe::initialize_crypto_provider()?;
    let metrics = arrowcol_buffer::MetricsGuard::install("arrowcol-dataflow-rust")?;
    otel_arrow_dfe_telemetry::tracing_init::install_auxiliary_layer_factory(|| {
        Box::new(delta_kernel::metrics::ReportGeneratorLayer::new(
            arrow_deltalake::telemetry::kernel_metrics_reporter(),
        ))
    })
    .map_err(|_| "DFE metrics layer factory already installed")?;
    otel_arrow_dfe_telemetry::tracing_init::init_global_auxiliary_subscriber()?;
    arrowcol_buffer::CloudGeneration::initialize_from_environment()?;
    if let Some(generation) = arrowcol_buffer::CloudGeneration::process() {
        arrowcol_dataflow_components::configure_cloud_generation(generation.generation())?;
    }

    arrowcol_dataflow_components::initialize_shared_io()?;
    let shutdown_error = Rc::new(RefCell::new(None));
    let hook_error = Rc::clone(&shutdown_error);
    let result = otel_arrow_dfe::run_with_shutdown_status(move |clean| {
        let result = finish_shutdown(
            clean,
            arrow_deltalake::publisher::PublisherSession::close_shared,
            |clean| arrowcol_dataflow_components::finish_routing_shutdown(clean).map_err(|error| error.to_string()),
        );
        drop(metrics);
        if let Err(error) = result {
            eprintln!("dataflow shutdown failed: {error}");
            // After a clean engine exit the SDK would report success, so only this hook can fail
            // the process. An unclean exit is a startup error returned to `main`, or a controller
            // error the SDK already exits non-zero for; exiting here would hide that error.
            if clean {
                std::process::exit(1);
            }
            *hook_error.borrow_mut() = Some(error);
        }
    });
    propagate(result, shutdown_error.take())
}

fn emit_wal_effective_config() -> Result<(), Box<dyn std::error::Error>> {
    let config_path = config_path_from_args().unwrap_or_else(|| "config.yaml".into());
    let bytes = std::fs::read(&config_path)?;
    let config: serde_yaml::Value = serde_yaml::from_slice(&bytes)?;
    let Some(wal) = find_durable_buffer_config(&config) else {
        return Ok(());
    };

    let wal_flush_interval = wal_string(wal, "wal_flush_interval", "25ms")?;
    let retention_size_cap = wal_string(wal, "retention_size_cap", "10 GiB")?;
    let max_age = optional_wal_string(wal, "max_age")?;
    let size_cap_policy = wal_string(wal, "size_cap_policy", "backpressure")?;
    let poll_interval = wal_string(wal, "poll_interval", "100ms")?;
    let otlp_handling = wal_string(wal, "otlp_handling", "pass_through")?;
    let max_segment_open_duration = wal_string(wal, "max_segment_open_duration", "60s")?;
    let initial_retry_interval = wal_string(wal, "initial_retry_interval", "1s")?;
    let max_retry_interval = wal_string(wal, "max_retry_interval", "30s")?;
    let retry_multiplier = wal_scalar(wal, "retry_multiplier", "2.0")?.parse::<f64>()?;
    let max_in_flight = wal_scalar(wal, "max_in_flight", "1000")?.parse::<usize>()?;

    if !matches!(size_cap_policy.as_str(), "backpressure" | "drop_oldest") {
        return Err(format!("invalid WAL size_cap_policy {size_cap_policy:?}; expected backpressure or drop_oldest").into());
    }
    if !matches!(otlp_handling.as_str(), "pass_through" | "convert_to_arrow") {
        return Err(format!("invalid WAL otlp_handling {otlp_handling:?}; expected pass_through or convert_to_arrow").into());
    }
    let flush = humantime::parse_duration(&wal_flush_interval)?;
    let max_age_duration = max_age.as_deref().map(humantime::parse_duration).transpose()?;
    let poll = humantime::parse_duration(&poll_interval)?;
    let segment_open = humantime::parse_duration(&max_segment_open_duration)?;
    let initial_retry = humantime::parse_duration(&initial_retry_interval)?;
    let max_retry = humantime::parse_duration(&max_retry_interval)?;
    if poll.is_zero() || segment_open.is_zero() || initial_retry.is_zero() || max_retry < initial_retry {
        return Err("WAL poll, segment, and retry durations are outside supported bounds".into());
    }
    if !retry_multiplier.is_finite() || retry_multiplier <= 0.0 || max_in_flight == 0 {
        return Err("WAL retry_multiplier and max_in_flight must be positive and finite".into());
    }
    if retention_size_cap.trim().is_empty() {
        return Err("WAL retention_size_cap must not be empty".into());
    }

    let durability_class = derive_durability_class(flush, max_age_duration.is_some(), &size_cap_policy);
    let effective = serde_json::json!({
        "wal_flush_interval": wal_flush_interval,
        "retention_size_cap": retention_size_cap,
        "max_age": max_age,
        "size_cap_policy": size_cap_policy,
        "poll_interval": poll_interval,
        "otlp_handling": otlp_handling,
        "max_segment_open_duration": max_segment_open_duration,
        "initial_retry_interval": initial_retry_interval,
        "max_retry_interval": max_retry_interval,
        "retry_multiplier": retry_multiplier,
        "max_in_flight": max_in_flight,
        "durability_class": durability_class,
    });
    eprintln!("arrowcol_wal_effective_config {effective}");
    Ok(())
}

fn derive_durability_class(
    flush_interval: std::time::Duration,
    has_max_age: bool,
    size_cap_policy: &str,
) -> &'static str {
    if has_max_age || size_cap_policy == "drop_oldest" {
        "lossy_eviction"
    } else if !flush_interval.is_zero() {
        "fsync_batched"
    } else {
        "durable"
    }
}

fn config_path_from_args() -> Option<std::path::PathBuf> {
    let mut args = std::env::args_os().skip(1);
    while let Some(arg) = args.next() {
        if arg == "--config" {
            return args.next().map(Into::into);
        }
        if let Some(value) = arg.to_str().and_then(|arg| arg.strip_prefix("--config=")) {
            return Some(value.into());
        }
    }
    None
}

fn find_durable_buffer_config(config: &serde_yaml::Value) -> Option<&serde_yaml::Value> {
    if config.get("type").and_then(serde_yaml::Value::as_str) == Some("processor:durable_buffer") {
        return config.get("config");
    }
    match config {
        serde_yaml::Value::Mapping(values) => values.values().find_map(find_durable_buffer_config),
        serde_yaml::Value::Sequence(values) => values.iter().find_map(find_durable_buffer_config),
        _ => None,
    }
}

fn wal_scalar(
    config: &serde_yaml::Value,
    field: &str,
    default: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let value = config.get(field).cloned().unwrap_or_else(|| serde_yaml::Value::String(default.to_owned()));
    let raw = match value {
        serde_yaml::Value::String(value) => value,
        serde_yaml::Value::Null => "null".to_owned(),
        value => serde_yaml::to_string(&value)?.trim().to_owned(),
    };
    expand_wal_env(&raw)
}

fn wal_string(
    config: &serde_yaml::Value,
    field: &str,
    default: &str,
) -> Result<String, Box<dyn std::error::Error>> {
    let value = wal_scalar(config, field, default)?;
    if value == "null" {
        return Err(format!("WAL {field} must not be null").into());
    }
    Ok(value)
}

fn optional_wal_string(
    config: &serde_yaml::Value,
    field: &str,
) -> Result<Option<String>, Box<dyn std::error::Error>> {
    match config.get(field) {
        None | Some(serde_yaml::Value::Null) => Ok(None),
        Some(_) => match wal_scalar(config, field, "")?.as_str() {
            "" | "null" => Ok(None),
            value => Ok(Some(value.to_owned())),
        },
    }
}

fn expand_wal_env(value: &str) -> Result<String, Box<dyn std::error::Error>> {
    let Some(expression) = value.strip_prefix("${env:").and_then(|value| value.strip_suffix('}')) else {
        return Ok(value.to_owned());
    };
    let (name, default) = expression
        .split_once(":-")
        .ok_or_else(|| format!("invalid WAL environment expression {value:?}"))?;
    match std::env::var(name) {
        Ok(value) => Ok(value),
        Err(std::env::VarError::NotPresent) => Ok(default.to_owned()),
        Err(std::env::VarError::NotUnicode(_)) => Err(format!("WAL environment variable {name} must be UTF-8").into()),
    }
}

/// Keeps the engine's own error (for example `IncompatiblePdataWiring`) as the process error
/// and fails a run that otherwise succeeded when its shutdown hook failed.
fn propagate(
    result: Result<(), Box<dyn std::error::Error>>,
    shutdown_error: Option<String>,
) -> Result<(), Box<dyn std::error::Error>> {
    match (result, shutdown_error) {
        (Err(error), _) => Err(error),
        (Ok(()), Some(error)) => Err(format!("dataflow shutdown failed: {error}").into()),
        (Ok(()), None) => Ok(()),
    }
}

fn finish_shutdown<E: std::fmt::Display>(
    clean: bool,
    close: impl FnOnce() -> Result<(), E>,
    terminal: impl FnOnce(bool) -> Result<(), String>,
) -> Result<(), String> {
    let closed = close().map_err(|error| error.to_string());
    let terminal = terminal(clean && closed.is_ok());
    let mut errors = Vec::new();
    if !clean {
        errors.push("engine startup or controller drain failed".to_owned());
    }
    if let Err(error) = closed {
        errors.push(format!("publisher session close failed: {error}"));
    }
    if let Err(error) = terminal {
        errors.push(format!("terminal routing state failed: {error}"));
    }
    if errors.is_empty() {
        Ok(())
    } else {
        Err(errors.join("; "))
    }
}

#[cfg(test)]
mod tests {
    use std::cell::Cell;

    use super::*;

    #[test]
    fn wal_durability_class_reports_the_weakest_configured_guarantee() {
        let zero = std::time::Duration::ZERO;
        assert_eq!(derive_durability_class(zero, false, "backpressure"), "durable");
        assert_eq!(derive_durability_class(std::time::Duration::from_millis(25), false, "backpressure"), "fsync_batched");
        assert_eq!(derive_durability_class(zero, false, "drop_oldest"), "lossy_eviction");
        assert_eq!(derive_durability_class(zero, true, "backpressure"), "lossy_eviction");
    }

    #[test]
    fn clean_shutdown_closes_publisher_before_terminal_write() {
        let closed = Cell::new(false);
        let result = finish_shutdown(
            true,
            || {
                closed.set(true);
                Ok::<_, String>(())
            },
            |clean| {
                assert!(closed.get());
                assert!(clean);
                Ok(())
            },
        );
        assert!(result.is_ok());
    }

    #[test]
    fn failed_publisher_close_records_failed_and_returns_error() {
        let result = finish_shutdown(
            true,
            || Err("publication close failed"),
            |clean| {
                assert!(!clean);
                Ok(())
            },
        );
        assert!(result.is_err());
    }

    #[test]
    fn unclean_shutdown_attempts_publisher_close_before_recording_failed() {
        let closed = Cell::new(false);
        let result = finish_shutdown(
            false,
            || {
                closed.set(true);
                Ok::<_, String>(())
            },
            |clean| {
                assert!(closed.get());
                assert!(!clean);
                Ok(())
            },
        );
        assert_eq!(result, Err("engine startup or controller drain failed".to_owned()));
    }

    #[test]
    fn unclean_shutdown_preserves_close_and_terminal_errors_and_original_engine_error() {
        let result = finish_shutdown(
            false,
            || Err("outcome unknown; registration retained"),
            |clean| {
                assert!(!clean);
                Err("SQL unavailable".to_owned())
            },
        );
        assert_eq!(
            result,
            Err(concat!(
                "engine startup or controller drain failed; ",
                "publisher session close failed: outcome unknown; registration retained; ",
                "terminal routing state failed: SQL unavailable"
            )
            .to_owned())
        );
        let engine_error: Box<dyn std::error::Error> = "IncompatiblePdataWiring".into();
        let original = std::ptr::from_ref(engine_error.as_ref());
        let Err(returned) = propagate(Err(engine_error), result.err()) else {
            panic!("engine error must be preserved");
        };
        assert!(std::ptr::eq(original, std::ptr::from_ref(returned.as_ref())));
    }

    #[test]
    fn repeated_failed_startups_invoke_owned_cleanup_without_sweeping_other_members() {
        use arrow_deltalake::publisher::{MAX_PUBLISHERS, MemberState};
        use std::collections::BTreeMap;

        // The close callback models the publisher's guarded, owner-only metadata transaction.
        let retained = BTreeMap::from([(0, MemberState::Active), (1, MemberState::Uncertain)]);
        let mut members = retained.clone();
        assert_eq!(MAX_PUBLISHERS, 16);
        for owned in 2..(2 + MAX_PUBLISHERS * 2) {
            assert!(members.len() < MAX_PUBLISHERS);
            assert!(members.insert(owned, MemberState::Active).is_none());
            let result = finish_shutdown(
                false,
                || {
                    assert_eq!(members.remove(&owned), Some(MemberState::Active));
                    Ok::<_, String>(())
                },
                |clean| {
                    assert!(!clean);
                    Ok(())
                },
            );
            assert_eq!(result, Err("engine startup or controller drain failed".to_owned()));
            assert_eq!(members, retained);
        }
    }

    #[test]
    fn failed_terminal_write_is_an_error_even_after_clean_close() {
        let result = finish_shutdown(true, || Ok::<_, String>(()), |_| Err("SQL unavailable".to_owned()));
        assert!(result.is_err());
    }

    #[test]
    fn engine_error_is_returned_unchanged_after_failed_shutdown() {
        let result = propagate(
            Err("IncompatiblePdataWiring".into()),
            Some("engine startup or controller drain failed".to_owned()),
        );
        assert_eq!(
            result.map_err(|error| error.to_string()),
            Err("IncompatiblePdataWiring".to_owned())
        );
    }

    #[test]
    fn failed_shutdown_fails_an_otherwise_successful_run() {
        let result = propagate(Ok(()), Some("terminal routing state failed".to_owned()));
        assert!(result.is_err_and(|error| error.to_string().contains("terminal routing state failed")));
        assert!(propagate(Ok(()), None).is_ok());
    }
}
