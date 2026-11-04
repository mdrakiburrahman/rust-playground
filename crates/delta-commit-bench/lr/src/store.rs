//! Container-rooted object stores and process memory probes.

use anyhow::{Context, Result, bail};
use object_store::azure::MicrosoftAzureBuilder;
use object_store::local::LocalFileSystem;
use object_store::path::Path;
use object_store::{ClientOptions, ObjectStore, RetryConfig};
use std::collections::HashMap;
use std::sync::{Arc, Mutex, OnceLock};
use std::time::Duration;
use url::Url;

/// Store rooted at the filesystem root or the Azure container, plus the table prefix within it.
/// Azure auth comes from the environment (`AZURE_USE_AZURE_CLI=true`, or managed identity on a VM).
/// One Azure store per container is shared by every table, so tables share one HTTP pool and one
/// token (a store per table sends one IMDS token request per table and gets throttled at 100 tables).
pub fn open(url: &Url) -> Result<(Arc<dyn ObjectStore>, Path)> {
    static AZURE: OnceLock<Mutex<HashMap<String, Arc<dyn ObjectStore>>>> = OnceLock::new();
    let prefix = Path::from_url_path(url.path())?;
    match url.scheme() {
        "file" => Ok((Arc::new(LocalFileSystem::new()), prefix)),
        "abfss" | "abfs" | "az" | "azure" => {
            let key = format!("{}://{}@{}", url.scheme(), url.username(), url.host_str().unwrap_or_default());
            let mut cache = AZURE.get_or_init(Default::default).lock().expect("store cache poisoned");
            if let Some(store) = cache.get(&key) {
                return Ok((store.clone(), prefix));
            }
            let store: Arc<dyn ObjectStore> = Arc::new(
                MicrosoftAzureBuilder::from_env()
                    .with_url(url.as_str())
                    .with_retry(RetryConfig::default())
                    .with_client_options(
                        ClientOptions::new().with_timeout(Duration::from_secs(120)).with_pool_max_idle_per_host(256),
                    )
                    .build()
                    .context("building Azure store")?,
            );
            cache.insert(key, store.clone());
            Ok((store, prefix))
        }
        other => bail!("unsupported scheme {other}"),
    }
}

fn status_kb(key: &str) -> u64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with(key))
                .and_then(|l| l.split_whitespace().nth(1))
                .and_then(|v| v.parse().ok())
        })
        .unwrap_or(0)
}

/// Resident set size in MiB.
pub fn rss_mib() -> f64 {
    status_kb("VmRSS:") as f64 / 1024.0
}

/// Peak resident set size in MiB.
pub fn hwm_mib() -> f64 {
    status_kb("VmHWM:") as f64 / 1024.0
}

/// Reset the peak RSS counter (Linux: write 5 to /proc/self/clear_refs).
pub fn reset_hwm() {
    let _ = std::fs::write("/proc/self/clear_refs", "5");
}

/// Abort the process with exit code 3 when RSS passes `cap_gib`, printing a JSON line.
pub fn rss_watchdog(cap_gib: f64, label: String) {
    if cap_gib <= 0.0 {
        return;
    }
    std::thread::spawn(move || {
        loop {
            let rss = rss_mib();
            if rss > cap_gib * 1024.0 {
                println!("{}", serde_json::json!({"kind": "rss_cap", "label": label, "rss_mib": rss, "cap_gib": cap_gib}));
                std::process::exit(3);
            }
            std::thread::sleep(Duration::from_millis(100));
        }
    });
}
