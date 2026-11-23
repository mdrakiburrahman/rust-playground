use std::cell::RefCell;
use std::rc::Rc;

use arrowcol_dataflow_components as _;

#[cfg(feature = "allocator-mimalloc")]
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    ensure_target_cpu()?;
    if let Some(disabled) = configure_thp()? {
        eprintln!(
            "arrowcol_effective_config {}",
            serde_json::json!({"process":{"thp_disabled":disabled,"scope":"linux_thp_disable_flag"}})
        );
    }
    eprintln!(
        "arrowcol_effective_config {}",
        arrow_deltalake::writer::effective_startup_config_json()?
    );
    let build_config = serde_json::json!({
        "component": "build",
        "config": {
            "profile_hash": option_env!("COLLECTOR_BUILD_PROFILE_HASH").unwrap_or("65f0a48e5711a4fb91141fc590b170e6b614798bb14e6f952dd440c3be547108"),
            "allocator": if cfg!(feature = "allocator-mimalloc") { "mimalloc" } else { "system" },
            "lto": option_env!("COLLECTOR_BUILD_LTO").unwrap_or("off"),
            "codegen_units": option_env!("COLLECTOR_BUILD_CODEGEN_UNITS").and_then(|value| value.parse::<u16>().ok()).unwrap_or(16),
            "opt_level": option_env!("COLLECTOR_BUILD_OPT_LEVEL").and_then(|value| value.parse::<u8>().ok()).unwrap_or(3),
            "target_cpu": option_env!("COLLECTOR_BUILD_TARGET_CPU").unwrap_or("generic"),
            "log_filter": std::env::var("RUST_LOG").ok(),
        }
    });
    eprintln!("arrowcol_effective_config {build_config}");
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
    let result = otel_arrow_dfe::run_with_config_observer_and_shutdown_status(
        |spec| arrowcol_dataflow_components::observe_validated_topology(spec).map_err(Into::into),
        move |clean| {
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
        },
    );
    propagate(result, shutdown_error.take())
}

fn configure_thp() -> Result<Option<bool>, Box<dyn std::error::Error>> {
    const ENV: &str = "ARROWCOL_COLLECTOR_THP_DISABLED";
    let value = match std::env::var(ENV) {
        Ok(value) => value,
        Err(std::env::VarError::NotPresent) => return Ok(None),
        Err(error) => return Err(format!("{ENV}: {error}").into()),
    };
    let requested = match value.as_str() {
        "true" => true,
        "false" => false,
        _ => return Err(format!("{ENV} must be true or false").into()),
    };
    #[cfg(target_os = "linux")]
    {
        rustix::thread::disable_transparent_huge_pages(requested)
            .map_err(|error| format!("{ENV}: setting Linux process flag: {error}"))?;
        let actual = rustix::thread::transparent_huge_pages_are_disabled()
            .map_err(|error| format!("{ENV}: reading Linux process flag: {error}"))?;
        if actual != requested {
            return Err(format!("{ENV} did not match the Linux process flag").into());
        }
        Ok(Some(actual))
    }
    #[cfg(not(target_os = "linux"))]
    {
        let _ = requested;
        Err(format!("{ENV} is supported only on Linux").into())
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

fn ensure_target_cpu() -> Result<(), Box<dyn std::error::Error>> {
    let target_cpu = option_env!("COLLECTOR_BUILD_TARGET_CPU").unwrap_or("generic");
    let required: &[&str] = match target_cpu {
        "generic" => &[],
        "x86-64-v2" => &["cx16", "lahf_lm", "popcnt", "sse3", "ssse3", "sse4_1", "sse4_2"],
        "x86-64-v3" => &[
            "cx16", "lahf_lm", "popcnt", "sse3", "ssse3", "sse4_1", "sse4_2", "avx", "avx2", "bmi1", "bmi2", "fma",
            "abm", "movbe", "xsave",
        ],
        "x86-64-v4" => &[
            "cx16", "lahf_lm", "popcnt", "sse3", "ssse3", "sse4_1", "sse4_2", "avx", "avx2", "bmi1", "bmi2", "fma",
            "abm", "movbe", "xsave", "avx512f", "avx512bw", "avx512cd", "avx512dq", "avx512vl",
        ],
        _ => return Err(format!("unsupported compiled target CPU {target_cpu}").into()),
    };
    if required.is_empty() {
        return Ok(());
    }
    let cpu_info = std::fs::read_to_string("/proc/cpuinfo")?;
    let flags = cpu_info
        .lines()
        .find_map(|line| {
            line.strip_prefix("flags:")
                .or_else(|| line.strip_prefix("Features:"))
                .map(str::split_whitespace)
        })
        .map(|values| values.collect::<std::collections::HashSet<_>>())
        .ok_or_else(|| format!("cannot determine CPU flags required by collector target-cpu {target_cpu}"))?;
    let missing: Vec<_> = required.iter().copied().filter(|flag| !flags.contains(flag)).collect();
    if !missing.is_empty() {
        return Err(format!(
            "collector target-cpu {target_cpu} requires unavailable CPU flags: {}",
            missing.join(", ")
        )
        .into());
    }
    Ok(())
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

    #[cfg(target_os = "linux")]
    #[test]
    fn thp_flag_reaches_linux_kernel() -> Result<(), Box<dyn std::error::Error>> {
        if std::env::var_os("ARROWCOL_TEST_THP_CHILD").is_some() {
            let expected = std::env::var("ARROWCOL_COLLECTOR_THP_DISABLED")? == "true";
            assert_eq!(configure_thp()?, Some(expected));
            assert_eq!(rustix::thread::transparent_huge_pages_are_disabled()?, expected);
            return Ok(());
        }
        for value in ["true", "false"] {
            let child = std::process::Command::new(std::env::current_exe()?)
                .args(["--exact", "tests::thp_flag_reaches_linux_kernel"])
                .env_clear()
                .env("ARROWCOL_TEST_THP_CHILD", "1")
                .env("ARROWCOL_COLLECTOR_THP_DISABLED", value)
                .output()?;
            if !child.status.success() || !String::from_utf8_lossy(&child.stdout).contains("1 passed;") {
                return Err(format!(
                    "THP child failed: {}\n{}",
                    String::from_utf8_lossy(&child.stdout),
                    String::from_utf8_lossy(&child.stderr)
                )
                .into());
            }
        }
        Ok(())
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
