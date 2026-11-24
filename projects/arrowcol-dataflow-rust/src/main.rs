use std::cell::{Cell, RefCell};
use std::rc::Rc;

use arrowcol_dataflow_components as _;

#[cfg(feature = "allocator-mimalloc")]
#[global_allocator]
static ALLOCATOR: mimalloc::MiMalloc = mimalloc::MiMalloc;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::var_os(MIMALLOC_PROBE).is_some() {
        return Ok(());
    }
    if let Some(malloc) = configure_malloc()? {
        eprintln!(
            "arrowcol_effective_config {}",
            serde_json::json!({"process":{
                "glibc_malloc": serde_json::to_value(&malloc)?,
                "glibc_malloc_scope": "glibc_tunables_startup",
            }})
        );
    }
    if let Some(mimalloc) = configure_mimalloc()? {
        eprintln!("arrowcol_effective_config {}", serde_json::json!({"process": mimalloc}));
    }
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
            "source_revision": option_env!("COLLECTOR_BUILD_SOURCE_REVISION"),
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
    let observed_startup = Rc::new(Cell::new(false));
    let observer_started = Rc::clone(&observed_startup);
    let result = otel_arrow_dfe::run_with_config_observer_and_shutdown_status(
        move |spec| {
            arrowcol_dataflow_components::observe_validated_topology(spec)?;
            observer_started.set(true);
            Ok(())
        },
        move |clean| {
            let result = finish_shutdown(
                clean,
                arrow_deltalake::publisher::PublisherSession::close_shared,
                |clean| arrowcol_dataflow_components::finish_routing_shutdown(clean).map_err(|error| error.to_string()),
            );
            drop(metrics);
            if clean && observed_startup.get() && result.is_ok() {
                if let Some(evidence) = arrowcol_dataflow_components::terminal_publication_evidence() {
                    eprintln!("arrowcol_terminal_evidence {evidence}");
                }
                eprintln!("arrowcol_shutdown {{\"event\":\"drain_complete\"}}");
            }
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

const MALLOC_CONFIG: &str = "ARROWCOL_COLLECTOR_MALLOC_CONFIG";
const MALLOC_REEXEC: &str = "ARROWCOL_COLLECTOR_MALLOC_REEXEC";

/// glibc reads `GLIBC_TUNABLES` only while the process starts, so the collector re-executes itself
/// once with the requested malloc tunables and then confirms it was started with exactly them.
fn configure_malloc() -> Result<Option<arrowcol_tunables::CollectorMallocTunables>, Box<dyn std::error::Error>> {
    let raw = match std::env::var(MALLOC_CONFIG) {
        Ok(raw) => raw,
        Err(std::env::VarError::NotPresent) => return Ok(None),
        Err(error) => return Err(format!("{MALLOC_CONFIG}: {error}").into()),
    };
    let tunables: arrowcol_tunables::CollectorMallocTunables =
        serde_json::from_str(&raw).map_err(|error| format!("{MALLOC_CONFIG}: {error}"))?;
    tunables
        .validate()
        .map_err(|error| format!("{MALLOC_CONFIG}: {error}"))?;
    if cfg!(feature = "allocator-mimalloc") {
        return Err(format!("{MALLOC_CONFIG} tunes glibc malloc but this collector was built with mimalloc").into());
    }
    #[cfg(all(target_os = "linux", target_env = "gnu"))]
    {
        use std::os::unix::process::CommandExt;

        let inherited = match std::env::var("GLIBC_TUNABLES") {
            Ok(value) => value,
            Err(std::env::VarError::NotPresent) => String::new(),
            Err(error) => return Err(format!("GLIBC_TUNABLES: {error}").into()),
        };
        let requested = tunables.glibc_tunables();
        let mut wanted: Vec<&str> = requested.iter().map(String::as_str).collect();
        wanted.sort_unstable();
        let mut applied: Vec<&str> = inherited
            .split(':')
            .filter(|entry| entry.starts_with("glibc.malloc."))
            .collect();
        applied.sort_unstable();
        if applied == wanted {
            return Ok(Some(tunables));
        }
        if !applied.is_empty() || std::env::var_os(MALLOC_REEXEC).is_some() {
            return Err(format!(
                "{MALLOC_CONFIG}: GLIBC_TUNABLES malloc entries {applied:?} do not match the requested {wanted:?}"
            )
            .into());
        }
        let glibc_tunables = if inherited.is_empty() {
            requested.join(":")
        } else {
            format!("{inherited}:{}", requested.join(":"))
        };
        let mut arguments = std::env::args_os();
        let mut command = std::process::Command::new(std::env::current_exe()?);
        if let Some(program) = arguments.next() {
            command.arg0(program);
        }
        let error = command
            .args(arguments)
            .env("GLIBC_TUNABLES", glibc_tunables)
            .env(MALLOC_REEXEC, "1")
            .exec();
        Err(format!("{MALLOC_CONFIG}: re-executing with GLIBC_TUNABLES: {error}").into())
    }
    #[cfg(not(all(target_os = "linux", target_env = "gnu")))]
    {
        Err(format!("{MALLOC_CONFIG} is supported only on Linux glibc").into())
    }
}

const MIMALLOC_CONFIG: &str = "ARROWCOL_COLLECTOR_MIMALLOC_CONFIG";
const MIMALLOC_REEXEC: &str = "ARROWCOL_COLLECTOR_MIMALLOC_REEXEC";
const MIMALLOC_PROBE: &str = "ARROWCOL_COLLECTOR_MIMALLOC_PROBE";

/// mimalloc reads `MIMALLOC_*` once while the process loads, so the collector re-executes itself once
/// with the requested options. It then runs itself as a probe that exits before `main` does any work,
/// with `MIMALLOC_VERBOSE=1`, and requires mimalloc's own printout to show every requested value.
fn configure_mimalloc() -> Result<Option<serde_json::Value>, Box<dyn std::error::Error>> {
    let raw = match std::env::var(MIMALLOC_CONFIG) {
        Ok(raw) => raw,
        Err(std::env::VarError::NotPresent) => return Ok(None),
        Err(error) => return Err(format!("{MIMALLOC_CONFIG}: {error}").into()),
    };
    let tunables: arrowcol_tunables::CollectorMimallocTunables =
        serde_json::from_str(&raw).map_err(|error| format!("{MIMALLOC_CONFIG}: {error}"))?;
    tunables
        .validate()
        .map_err(|error| format!("{MIMALLOC_CONFIG}: {error}"))?;
    if !cfg!(feature = "allocator-mimalloc") {
        return Err(format!("{MIMALLOC_CONFIG} requires a collector built with allocator-mimalloc").into());
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;

        let options = tunables.mimalloc_options();
        let mut requested: Vec<(String, String)> = options
            .iter()
            .map(|option| (option.environment_name(), option.environment_value()))
            .collect();
        requested.sort_unstable();
        let mut inherited = Vec::new();
        for (name, value) in std::env::vars_os() {
            if !name.to_string_lossy().to_ascii_uppercase().starts_with("MIMALLOC_") {
                continue;
            }
            let (Ok(name), Ok(value)) = (name.into_string(), value.into_string()) else {
                return Err(format!("{MIMALLOC_CONFIG}: an inherited MIMALLOC_* variable is not UTF-8").into());
            };
            inherited.push((name, value));
        }
        inherited.sort_unstable();
        if inherited == requested {
            return probe_mimalloc(&tunables, &options).map(Some);
        }
        if !inherited.is_empty() || std::env::var_os(MIMALLOC_REEXEC).is_some() {
            return Err(format!(
                "{MIMALLOC_CONFIG}: inherited mimalloc options {inherited:?} do not match the requested {requested:?}"
            )
            .into());
        }
        let mut arguments = std::env::args_os();
        let mut command = std::process::Command::new(std::env::current_exe()?);
        if let Some(program) = arguments.next() {
            command.arg0(program);
        }
        let error = command.args(arguments).envs(requested).env(MIMALLOC_REEXEC, "1").exec();
        Err(format!("{MIMALLOC_CONFIG}: re-executing with mimalloc options: {error}").into())
    }
    #[cfg(not(unix))]
    {
        Err(format!("{MIMALLOC_CONFIG} is supported only on Unix").into())
    }
}

#[cfg(unix)]
fn probe_mimalloc(
    tunables: &arrowcol_tunables::CollectorMimallocTunables,
    options: &[arrowcol_tunables::MimallocOption],
) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
    use std::os::unix::process::CommandExt;

    let mut arguments = std::env::args_os();
    let mut command = std::process::Command::new(std::env::current_exe()?);
    if let Some(program) = arguments.next() {
        command.arg0(program);
    }
    let output = command
        .args(arguments)
        .env(MIMALLOC_PROBE, "1")
        .env("MIMALLOC_VERBOSE", "1")
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .output()?;
    let stderr = String::from_utf8_lossy(&output.stderr);
    if !output.status.success() {
        return Err(format!(
            "{MIMALLOC_CONFIG}: mimalloc probe failed with {}: {stderr}",
            output.status
        )
        .into());
    }
    let mut version = None;
    let mut effective = serde_json::Map::new();
    let mut warnings = Vec::new();
    for line in stderr.lines() {
        let line = line.strip_prefix("mimalloc: ").unwrap_or(line).trim();
        if let Some(option) = line.strip_prefix("option '") {
            let (name, value) = option
                .split_once("': ")
                .ok_or_else(|| format!("{MIMALLOC_CONFIG}: unrecognized mimalloc option line {line:?}"))?;
            let value: i64 = value
                .trim_end_matches(" KiB")
                .parse()
                .map_err(|error| format!("{MIMALLOC_CONFIG}: mimalloc option line {line:?}: {error}"))?;
            effective.insert(name.to_owned(), value.into());
        } else if line.starts_with('v') && line.contains(" (built on ") {
            version = line.split_whitespace().next().map(str::to_owned);
        } else if line.starts_with("warning:") || line.starts_with("error:") {
            warnings.push(line.to_owned());
        }
    }
    let version = version.ok_or_else(|| format!("{MIMALLOC_CONFIG}: mimalloc probe printed no version: {stderr}"))?;
    // `verbose` is the probe's own setting, not the collector's.
    effective.remove("verbose");
    for option in options {
        let actual = effective.get(option.name).and_then(serde_json::Value::as_i64);
        if actual != Some(option.value) {
            return Err(format!(
                "{MIMALLOC_CONFIG}: mimalloc reports {} = {actual:?}, requested {}; probe output: {stderr}",
                option.name, option.value
            )
            .into());
        }
    }
    Ok(serde_json::json!({
        "mimalloc": serde_json::to_value(tunables)?,
        "mimalloc_options": effective,
        "mimalloc_version": version,
        "mimalloc_warnings": warnings,
        "mimalloc_scope": "mimalloc_env_startup_verbose_probe",
    }))
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
    if target_cpu == "generic" {
        return Ok(());
    }
    if !matches!(target_cpu, "x86-64-v2" | "x86-64-v3" | "x86-64-v4") {
        return Err(format!("unsupported compiled target CPU {target_cpu}").into());
    }
    let cpu_info = std::fs::read_to_string("/proc/cpuinfo")?;
    if let Err(error) = arrowcol_tunables::verify_collector_target_cpu_flags(
        target_cpu,
        &cpu_info,
        "collector.target_cpu startup guard",
    ) {
        return Err(error.into());
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

    #[cfg(all(target_os = "linux", target_env = "gnu", not(feature = "allocator-mimalloc")))]
    #[test]
    fn malloc_tunables_reach_glibc_at_startup() -> Result<(), Box<dyn std::error::Error>> {
        if std::env::var_os("ARROWCOL_TEST_MALLOC_CHILD").is_some() {
            let configured = configure_malloc()?.is_some();
            let allocation = vec![1_u8; 8 << 20];
            let address = allocation.as_ptr().addr();
            let in_main_heap = std::fs::read_to_string("/proc/self/maps")?
                .lines()
                .filter(|line| line.ends_with("[heap]"))
                .filter_map(|line| line.split_once(' ')?.0.split_once('-'))
                .filter_map(|(start, end)| {
                    Some(usize::from_str_radix(start, 16).ok()?..usize::from_str_radix(end, 16).ok()?)
                })
                .any(|range| range.contains(&address));
            assert_eq!(in_main_heap, configured);
            assert_eq!(std::env::var_os(MALLOC_REEXEC).is_some(), configured);
            return Ok(());
        }
        for config in [Some(r#"{"arena_max":1,"mmap_threshold":33554432}"#), None] {
            let mut command = std::process::Command::new(std::env::current_exe()?);
            command
                .args(["--exact", "tests::malloc_tunables_reach_glibc_at_startup"])
                .env_clear()
                .env("ARROWCOL_TEST_MALLOC_CHILD", "1");
            if let Some(config) = config {
                command.env(MALLOC_CONFIG, config);
            }
            let child = command.output()?;
            if !child.status.success() || !String::from_utf8_lossy(&child.stdout).contains("1 passed;") {
                return Err(format!(
                    "malloc child {config:?} failed: {}\n{}",
                    String::from_utf8_lossy(&child.stdout),
                    String::from_utf8_lossy(&child.stderr)
                )
                .into());
            }
        }
        let mismatched = std::process::Command::new(std::env::current_exe()?)
            .args(["--exact", "tests::malloc_tunables_reach_glibc_at_startup"])
            .env_clear()
            .env("ARROWCOL_TEST_MALLOC_CHILD", "1")
            .env(MALLOC_CONFIG, r#"{"arena_max":1}"#)
            .env("GLIBC_TUNABLES", "glibc.malloc.arena_max=2")
            .output()?;
        assert!(!mismatched.status.success());
        let output = format!(
            "{}{}",
            String::from_utf8_lossy(&mismatched.stdout),
            String::from_utf8_lossy(&mismatched.stderr)
        );
        assert!(output.contains("do not match the requested"), "{output}");
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn mimalloc_options_reach_the_allocator_at_startup() -> Result<(), Box<dyn std::error::Error>> {
        const TEST: &str = "tests::mimalloc_options_reach_the_allocator_at_startup";
        if std::env::var_os(MIMALLOC_PROBE).is_some() {
            return Ok(());
        }
        if std::env::var_os("ARROWCOL_TEST_MIMALLOC_CHILD").is_some() {
            let result = configure_mimalloc();
            if !cfg!(feature = "allocator-mimalloc") {
                let error = result
                    .err()
                    .ok_or("a system-allocator build accepted mimalloc options")?;
                assert!(error.to_string().contains("allocator-mimalloc"), "{error}");
                return Ok(());
            }
            let readback = result?.ok_or("missing mimalloc readback")?;
            assert!(std::env::var_os(MIMALLOC_REEXEC).is_some());
            assert_eq!(readback["mimalloc_scope"], "mimalloc_env_startup_verbose_probe");
            assert_eq!(readback["mimalloc"]["arena_eager_commit"], "always");
            assert!(readback["mimalloc_options"].get("verbose").is_none());
            for (name, value) in [
                ("purge_delay", -1),
                ("arena_eager_commit", 1),
                ("arena_reserve", 65_536),
            ] {
                assert_eq!(readback["mimalloc_options"][name], value, "{name}");
            }
            return Ok(());
        }
        let run = |inherited: Option<(&str, &str)>| -> std::io::Result<std::process::Output> {
            let mut command = std::process::Command::new(std::env::current_exe()?);
            command
                .args(["--exact", TEST])
                .env_clear()
                .env("ARROWCOL_TEST_MIMALLOC_CHILD", "1")
                .env(
                    MIMALLOC_CONFIG,
                    r#"{"purge_delay_ms":-1,"arena_eager_commit":"always","arena_reserve_kib":65536}"#,
                );
            if let Some((name, value)) = inherited {
                command.env(name, value);
            }
            command.output()
        };
        let child = run(None)?;
        if !child.status.success() || !String::from_utf8_lossy(&child.stdout).contains("1 passed;") {
            return Err(format!(
                "mimalloc child failed: {}\n{}",
                String::from_utf8_lossy(&child.stdout),
                String::from_utf8_lossy(&child.stderr)
            )
            .into());
        }
        if cfg!(feature = "allocator-mimalloc") {
            let mismatched = run(Some(("MIMALLOC_PURGE_DELAY", "10")))?;
            assert!(!mismatched.status.success());
            let output = format!(
                "{}{}",
                String::from_utf8_lossy(&mismatched.stdout),
                String::from_utf8_lossy(&mismatched.stderr)
            );
            assert!(output.contains("do not match the requested"), "{output}");
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
