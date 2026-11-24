mod validation;

pub use validation::{
    ApplyBoundary, Capability, ComponentRole, DurabilityClass, capability_table, parse_tunable_duration,
};

pub use arrow_deltalake::PartitionGrouping;
pub use arrow_deltalake::writer::{
    Codec as ParquetCodec, ColumnOptions as ParquetColumnOptions, ParquetWriterConfig, SortColumn as ParquetSortColumn,
    Statistics as ParquetStatistics, ValueEncoding as ParquetValueEncoding, Version as ParquetWriterVersion,
};
pub use arrowcol_routing::AdmissionPressureGauge;

use serde::{Deserialize, Deserializer, Serialize, de::Error as _};
use std::str::FromStr;

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum CollectorCpuQuota {
    Cores(f64),
    Unlimited(UnlimitedCpuQuota),
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum UnlimitedCpuQuota {
    Unlimited,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ContainerMemoryMetric {
    #[default]
    Usage,
    WorkingSet,
}

impl ContainerMemoryMetric {
    #[must_use]
    pub fn is_usage(&self) -> bool {
        *self == Self::Usage
    }

    #[must_use]
    pub fn metric_name(self) -> &'static str {
        match self {
            Self::Usage => "container_memory_bytes",
            Self::WorkingSet => "container_working_set_bytes",
        }
    }
}

impl CollectorCpuQuota {
    /// # Errors
    /// Rejects quotas below 0.01 CPU, nonfinite, oversized or sub-nanocore quotas.
    pub fn nano_cpus(self) -> Result<u64, &'static str> {
        match self {
            Self::Unlimited(_) => Ok(0),
            Self::Cores(cores) if cores.is_finite() && (0.01..=4096.0).contains(&cores) => {
                let decimal = cores.to_string();
                let (whole, fraction) = decimal.split_once('.').unwrap_or((&decimal, ""));
                if fraction.len() > 9 {
                    return Err("platform.local.collector_cpu_quota supports at most nanocore precision");
                }
                let whole = whole
                    .parse::<u64>()
                    .map_err(|_| "platform.local.collector_cpu_quota is invalid")?;
                let fraction = format!("{fraction:0<9}")
                    .parse::<u64>()
                    .map_err(|_| "platform.local.collector_cpu_quota is invalid")?;
                Ok(whole * 1_000_000_000 + fraction)
            }
            Self::Cores(_) => Err(
                "platform.local.collector_cpu_quota must be a decimal from 0.01 to 4096 (nanocore precision) or \"unlimited\"",
            ),
        }
    }
}

/// Payload customer IDs are bounded printable identifiers, not delivery identities.
#[must_use]
pub fn valid_payload_customer_id(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Tunables {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub client: Option<ClientTunables>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub transport: Option<TransportTunables>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub arrow_ipc: Option<ArrowIpcTunables>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present_delta"
    )]
    pub delta: Option<DeltaTunables>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub router: Option<RouterTunables>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub collector: Option<CollectorTunables>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parquet_writer: Option<ParquetWriterConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wal: Option<WalTunables>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub maintenance: Option<MaintenanceTunables>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub logging: Option<LoggingTunables>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct DeltaTunables {
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present_u64"
    )]
    pub target_file_size_bytes: Option<u64>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present_u64"
    )]
    pub checkpoint_interval: Option<u64>,
}

fn deserialize_present_delta<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<DeltaTunables>, D::Error> {
    DeltaTunables::deserialize(deserializer).map(Some)
}

fn deserialize_present_u64<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<u64>, D::Error> {
    u64::deserialize(deserializer).map(Some)
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClientTunables {
    pub payload_customer_id: Option<String>,
    pub corpus_rows: Option<u64>,
    pub corpus_bytes: Option<u64>,
    pub batch_rows: Option<usize>,
    pub workers: Option<usize>,
    pub runtime_threads: Option<usize>,
    pub data_mode: Option<DataMode>,
    pub data_profile: Option<ClientDataProfile>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub build_profile: Option<ClientBuildProfileTunables>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub send_iter: Option<ClientSendIter>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub identity_builder: Option<ClientIdentityBuilder>,
    pub string_length_distribution: Option<ClientStringLengthDistribution>,
    pub value_variation: Option<ClientValueVariation>,
    pub guid_uniqueness: Option<ClientGuidUniqueness>,
    pub string_width: Option<usize>,
    pub cardinality: Option<usize>,
    pub null_fraction: Option<f64>,
    pub skew: Option<f64>,
    pub entropy: Option<f64>,
    pub seed: Option<u64>,
    pub max_memory_bytes: Option<u64>,
    pub interval_us: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub quiesce_timeout_ms: Option<u64>,
    pub connections_per_worker: Option<usize>,
    pub pod_ordinal: Option<u32>,
    pub streams_per_worker: Option<usize>,
    pub max_rows: Option<u64>,
    pub stream_retry_max_attempts: Option<u32>,
    pub stream_retry_base_backoff_ms: Option<u64>,
    pub stream_retry_max_backoff_ms: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TransportTunables {
    pub ipc_compression: Option<ArrowIpcCompression>,
    pub dictionary_handling: Option<DictionaryHandling>,
    pub max_flight_data_bytes: Option<u64>,
    pub max_message_bytes: Option<u64>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present_usize"
    )]
    pub grpc_max_encode_bytes: Option<usize>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present_usize"
    )]
    pub grpc_max_decode_bytes: Option<usize>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present_usize"
    )]
    pub socket_send_buffer_bytes: Option<usize>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present_usize"
    )]
    pub socket_recv_buffer_bytes: Option<usize>,
    pub grpc_send_compression: Option<GrpcCompression>,
    pub grpc_accept_compression: Option<GrpcCompression>,
    pub max_frame_bytes: Option<u32>,
    pub header_table_bytes: Option<u32>,
    pub max_header_list_bytes: Option<u32>,
    pub stream_window_bytes: Option<u32>,
    pub connection_window_bytes: Option<u32>,
    pub adaptive_window: Option<bool>,
    pub keepalive_interval_ms: Option<u64>,
    pub keepalive_timeout_ms: Option<u64>,
    pub keepalive_while_idle: Option<bool>,
    pub connect_timeout_ms: Option<u64>,
    pub request_timeout_ms: Option<u64>,
    pub tcp_nodelay: Option<bool>,
    pub tcp_keepalive_ms: Option<u64>,
    pub tcp_keepalive_interval_ms: Option<u64>,
    pub tcp_keepalive_retries: Option<u32>,
    pub rate_limit_rps: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rate_limit_period_ms: Option<u64>,
    pub endpoint_concurrency_limit: Option<usize>,
    pub endpoint_buffer_size: Option<usize>,
    pub connect_retries: Option<u32>,
    pub retry_backoff_ms: Option<u64>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ArrowIpcTunables {
    pub compression: Option<ArrowIpcCompression>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alignment: Option<u8>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RouterTunables {
    pub workers: Option<usize>,
    pub listener_tasks_per_fd: Option<usize>,
    pub keepalive_pool_size: Option<usize>,
    pub h2_max_concurrent_streams: Option<u32>,
    pub h2_stream_window_bytes: Option<u32>,
    pub h2_connection_window_bytes: Option<u32>,
    pub h2_idle_timeout_seconds: Option<u64>,
    pub sql_connect_timeout_ms: Option<u64>,
    pub sql_command_timeout_ms: Option<u64>,
    pub sql_pool_max_size: Option<usize>,
    pub sql_pool_acquire_timeout_ms: Option<u64>,
    pub refresh_interval_ms: Option<u64>,
    pub max_snapshot_age_ms: Option<u64>,
    pub max_future_skew_ms: Option<u64>,
    pub max_table_entries: Option<usize>,
    pub max_oldest_buffer_age_ms: Option<u64>,
    pub hard_pressure_bps: Option<u16>,
    pub excluded_admission_gauges: Option<Vec<AdmissionPressureGauge>>,
    pub warmup_ms: Option<u64>,
    pub minimum_warmup_bps: Option<u16>,
    pub lease_ttl_ms: Option<u64>,
    pub oat_run_pin_ttl_ms: Option<u64>,
    pub readiness_max_staleness_ms: Option<u64>,
    pub distinct_table_owners: Option<bool>,
    pub edge_replay_max_nonces: Option<usize>,
    pub upstream_max_h2_streams: Option<usize>,
    pub upstream_stream_window_bytes: Option<u32>,
    pub upstream_connection_window_bytes: Option<u32>,
    pub upstream_connection_timeout_ms: Option<u64>,
    pub upstream_total_connection_timeout_ms: Option<u64>,
    pub upstream_idle_timeout_seconds: Option<u64>,
    pub upstream_ping_interval_seconds: Option<u64>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum LogLevel {
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl LogLevel {
    fn as_str(self) -> &'static str {
        match self {
            Self::Error => "error",
            Self::Warn => "warn",
            Self::Info => "info",
            Self::Debug => "debug",
            Self::Trace => "trace",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum RouterLogLevel {
    Inherit,
    Error,
    Warn,
    Info,
    Debug,
    Trace,
}

impl RouterLogLevel {
    fn as_filter(self) -> Option<&'static str> {
        match self {
            Self::Inherit => None,
            Self::Error => Some("error"),
            Self::Warn => Some("warn"),
            Self::Info => Some("info"),
            Self::Debug => Some("debug"),
            Self::Trace => Some("trace"),
        }
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct LoggingTunables {
    pub router: Option<RouterLogLevel>,
    pub collector: Option<LogLevel>,
    pub maintenance: Option<LogLevel>,
}

pub const DEFAULT_COLLECTOR_SHUTDOWN_DRAIN_TIMEOUT_SECONDS: u32 = 60;
pub const MAX_COLLECTOR_SHUTDOWN_DRAIN_TIMEOUT_SECONDS: u32 = 1_500;

fn deserialize_shutdown_drain_timeout_seconds<'de, D>(deserializer: D) -> Result<Option<u32>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    u32::deserialize(deserializer).map(Some)
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct CollectorTunables {
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_shutdown_drain_timeout_seconds"
    )]
    pub shutdown_drain_timeout_seconds: Option<u32>,
    pub batching: Option<BatchingTunables>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_channel_capacity_object"
    )]
    pub channel_capacity: Option<ChannelCapacityTunables>,
    pub catalog: Option<CatalogTunables>,
    pub commit: Option<CommitTunables>,
    pub exporter: Option<ExporterTunables>,
    pub layout: Option<LayoutTunables>,
    pub limits: Option<LimitsTunables>,
    pub pressure: Option<PressureTunables>,
    pub routing: Option<RoutingTunables>,
    pub runtime: Option<RuntimeTunables>,
    pub socket: Option<CollectorSocketTunables>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present_bool"
    )]
    pub thp_disabled: Option<bool>,
    pub upload: Option<CollectorUploadTunables>,
    pub cpu_pressure: Option<CpuPressureMode>,
    #[serde(deserialize_with = "deserialize_collector_allocator")]
    pub allocator: Option<CollectorAllocator>,
    #[serde(deserialize_with = "deserialize_collector_lto")]
    pub lto: Option<CollectorLto>,
    #[serde(deserialize_with = "deserialize_collector_codegen_units")]
    pub codegen_units: Option<u16>,
    #[serde(deserialize_with = "deserialize_collector_opt_level")]
    pub opt_level: Option<CollectorOptLevel>,
    #[serde(deserialize_with = "deserialize_collector_target_cpu")]
    pub target_cpu: Option<CollectorTargetCpu>,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum CollectorAllocator {
    #[default]
    System,
    Mimalloc,
}
impl CollectorAllocator {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::System => "system",
            Self::Mimalloc => "mimalloc",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum CollectorLto {
    #[default]
    Off,
    #[serde(rename = "thin-local")]
    ThinLocal,
    Thin,
    Fat,
}
impl CollectorLto {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::ThinLocal => "thin-local",
            Self::Thin => "thin",
            Self::Fat => "fat",
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
pub enum CollectorOptLevel {
    #[serde(rename = "2")]
    O2,
    #[default]
    #[serde(rename = "3")]
    O3,
}
impl CollectorOptLevel {
    pub const fn as_u8(self) -> u8 {
        match self {
            Self::O2 => 2,
            Self::O3 => 3,
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
pub enum CollectorTargetCpu {
    #[default]
    #[serde(rename = "generic")]
    Generic,
    #[serde(rename = "x86-64-v2")]
    X86_64V2,
    #[serde(rename = "x86-64-v3")]
    X86_64V3,
    #[serde(rename = "x86-64-v4")]
    X86_64V4,
}
impl CollectorTargetCpu {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Generic => "generic",
            Self::X86_64V2 => "x86-64-v2",
            Self::X86_64V3 => "x86-64-v3",
            Self::X86_64V4 => "x86-64-v4",
        }
    }
}
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct ClientBuildProfile {
    pub lto: CollectorLto,
    pub codegen_units: u16,
    pub opt_level: CollectorOptLevel,
    pub target_cpu: CollectorTargetCpu,
}
impl Default for ClientBuildProfile {
    fn default() -> Self {
        Self {
            lto: CollectorLto::Off,
            codegen_units: 16,
            opt_level: CollectorOptLevel::O3,
            target_cpu: CollectorTargetCpu::Generic,
        }
    }
}
impl ClientBuildProfile {
    pub fn canonical_key(self) -> String {
        format!(
            "lto={};codegen_units={};opt_level={};target_cpu={}",
            self.lto.as_str(),
            self.codegen_units,
            self.opt_level.as_u8(),
            self.target_cpu.as_str()
        )
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ClientBuildProfileTunables {
    #[serde(default, deserialize_with = "deserialize_client_build_lto")]
    pub lto: Option<CollectorLto>,
    #[serde(default, deserialize_with = "deserialize_client_build_codegen_units")]
    pub codegen_units: Option<u16>,
    #[serde(default, deserialize_with = "deserialize_client_build_opt_level")]
    pub opt_level: Option<CollectorOptLevel>,
    #[serde(default, deserialize_with = "deserialize_client_build_target_cpu")]
    pub target_cpu: Option<CollectorTargetCpu>,
}
impl ClientBuildProfileTunables {
    pub fn build_profile(&self) -> ClientBuildProfile {
        ClientBuildProfile {
            lto: self.lto.unwrap_or_default(),
            codegen_units: self.codegen_units.unwrap_or(16),
            opt_level: self.opt_level.unwrap_or_default(),
            target_cpu: self.target_cpu.unwrap_or_default(),
        }
    }
}

#[derive(Clone, Copy)]
struct CollectorCpuFeatureRequirement {
    isa_feature: &'static str,
    cpuinfo_tokens: &'static [&'static str],
    minimum_level: u8,
}

const X86_64_FEATURES: &[CollectorCpuFeatureRequirement] = &[
    CollectorCpuFeatureRequirement {
        isa_feature: "CX16",
        cpuinfo_tokens: &["cx16"],
        minimum_level: 2,
    },
    CollectorCpuFeatureRequirement {
        isa_feature: "LAHF/SAHF",
        cpuinfo_tokens: &["lahf_lm"],
        minimum_level: 2,
    },
    CollectorCpuFeatureRequirement {
        isa_feature: "POPCNT",
        cpuinfo_tokens: &["popcnt"],
        minimum_level: 2,
    },
    CollectorCpuFeatureRequirement {
        isa_feature: "SSE3",
        cpuinfo_tokens: &["pni", "sse3"],
        minimum_level: 2,
    },
    CollectorCpuFeatureRequirement {
        isa_feature: "SSSE3",
        cpuinfo_tokens: &["ssse3"],
        minimum_level: 2,
    },
    CollectorCpuFeatureRequirement {
        isa_feature: "SSE4.1",
        cpuinfo_tokens: &["sse4_1"],
        minimum_level: 2,
    },
    CollectorCpuFeatureRequirement {
        isa_feature: "SSE4.2",
        cpuinfo_tokens: &["sse4_2"],
        minimum_level: 2,
    },
    CollectorCpuFeatureRequirement {
        isa_feature: "AVX",
        cpuinfo_tokens: &["avx"],
        minimum_level: 3,
    },
    CollectorCpuFeatureRequirement {
        isa_feature: "AVX2",
        cpuinfo_tokens: &["avx2"],
        minimum_level: 3,
    },
    CollectorCpuFeatureRequirement {
        isa_feature: "F16C",
        cpuinfo_tokens: &["f16c"],
        minimum_level: 3,
    },
    CollectorCpuFeatureRequirement {
        isa_feature: "BMI1",
        cpuinfo_tokens: &["bmi1"],
        minimum_level: 3,
    },
    CollectorCpuFeatureRequirement {
        isa_feature: "BMI2",
        cpuinfo_tokens: &["bmi2"],
        minimum_level: 3,
    },
    CollectorCpuFeatureRequirement {
        isa_feature: "FMA",
        cpuinfo_tokens: &["fma"],
        minimum_level: 3,
    },
    CollectorCpuFeatureRequirement {
        isa_feature: "LZCNT",
        cpuinfo_tokens: &["abm"],
        minimum_level: 3,
    },
    CollectorCpuFeatureRequirement {
        isa_feature: "MOVBE",
        cpuinfo_tokens: &["movbe"],
        minimum_level: 3,
    },
    CollectorCpuFeatureRequirement {
        isa_feature: "XSAVE",
        cpuinfo_tokens: &["xsave"],
        minimum_level: 3,
    },
    CollectorCpuFeatureRequirement {
        isa_feature: "AVX-512F",
        cpuinfo_tokens: &["avx512f"],
        minimum_level: 4,
    },
    CollectorCpuFeatureRequirement {
        isa_feature: "AVX-512BW",
        cpuinfo_tokens: &["avx512bw"],
        minimum_level: 4,
    },
    CollectorCpuFeatureRequirement {
        isa_feature: "AVX-512CD",
        cpuinfo_tokens: &["avx512cd"],
        minimum_level: 4,
    },
    CollectorCpuFeatureRequirement {
        isa_feature: "AVX-512DQ",
        cpuinfo_tokens: &["avx512dq"],
        minimum_level: 4,
    },
    CollectorCpuFeatureRequirement {
        isa_feature: "AVX-512VL",
        cpuinfo_tokens: &["avx512vl"],
        minimum_level: 4,
    },
];

/// Verifies host CPU flags against a supported collector target ISA.
///
/// # Errors
/// Returns an error when the target is unsupported or required CPU flags are missing.
pub fn verify_collector_target_cpu_flags(target_cpu: &str, cpu_info: &str, field: &str) -> Result<(), String> {
    let target_level = match target_cpu {
        "generic" => return Ok(()),
        "x86-64-v2" => 2,
        "x86-64-v3" => 3,
        "x86-64-v4" => 4,
        _ => return Err(format!("{field} uses unsupported collector target CPU {target_cpu}")),
    };
    let flags = cpu_info
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            (name.trim() == "flags").then_some(value)
        })
        .ok_or_else(|| format!("{field} cannot find host CPU flags"))?;
    let flags = flags.split_whitespace().collect::<std::collections::HashSet<_>>();
    if flags.is_empty() {
        return Err(format!("{field} found empty host CPU flags"));
    }
    let missing = X86_64_FEATURES
        .iter()
        .filter(|feature| {
            feature.minimum_level <= target_level && !feature.cpuinfo_tokens.iter().any(|token| flags.contains(*token))
        })
        .map(|feature| {
            format!(
                "{} (cpuinfo token: {})",
                feature.isa_feature,
                feature.cpuinfo_tokens.join(" or ")
            )
        })
        .collect::<Vec<_>>();
    if !missing.is_empty() {
        return Err(format!(
            "{field} {target_cpu} requires unavailable ISA features: {}",
            missing.join(", ")
        ));
    }
    Ok(())
}

/// Returns the Rust target features required by the selected CPU profile.
///
/// # Errors
///
/// Returns an error when the requested CPU profile is unsupported.
pub fn required_rust_target_features(target_cpu: &str) -> Result<&'static [&'static str], String> {
    const V2: &[&str] = &["cmpxchg16b", "popcnt", "sse3", "ssse3", "sse4.1", "sse4.2"];
    const V3: &[&str] = &[
        "cmpxchg16b",
        "popcnt",
        "sse3",
        "ssse3",
        "sse4.1",
        "sse4.2",
        "avx",
        "avx2",
        "bmi1",
        "bmi2",
        "f16c",
        "fma",
        "lzcnt",
        "movbe",
        "xsave",
    ];
    const V4: &[&str] = &[
        "cmpxchg16b",
        "popcnt",
        "sse3",
        "ssse3",
        "sse4.1",
        "sse4.2",
        "avx",
        "avx2",
        "bmi1",
        "bmi2",
        "f16c",
        "fma",
        "lzcnt",
        "movbe",
        "xsave",
        "avx512f",
        "avx512bw",
        "avx512cd",
        "avx512dq",
        "avx512vl",
    ];
    match target_cpu {
        "generic" => Ok(&[]),
        "x86-64-v2" => Ok(V2),
        "x86-64-v3" => Ok(V3),
        "x86-64-v4" => Ok(V4),
        _ => Err(format!("unsupported target CPU {target_cpu}")),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CollectorBuildProfile {
    pub allocator: CollectorAllocator,
    pub lto: CollectorLto,
    pub codegen_units: u16,
    pub opt_level: CollectorOptLevel,
    pub target_cpu: CollectorTargetCpu,
}
impl Default for CollectorBuildProfile {
    fn default() -> Self {
        Self {
            allocator: CollectorAllocator::System,
            lto: CollectorLto::Off,
            codegen_units: 16,
            opt_level: CollectorOptLevel::O3,
            target_cpu: CollectorTargetCpu::Generic,
        }
    }
}
impl CollectorBuildProfile {
    pub fn canonical_key(self) -> String {
        format!(
            "allocator={};lto={};codegen_units={};opt_level={};target_cpu={}",
            self.allocator.as_str(),
            self.lto.as_str(),
            self.codegen_units,
            self.opt_level.as_u8(),
            self.target_cpu.as_str()
        )
    }
}
impl CollectorTunables {
    pub fn has_build_profile(&self) -> bool {
        self.allocator.is_some()
            || self.lto.is_some()
            || self.codegen_units.is_some()
            || self.opt_level.is_some()
            || self.target_cpu.is_some()
    }
    pub fn build_profile(&self) -> CollectorBuildProfile {
        CollectorBuildProfile {
            allocator: self.allocator.unwrap_or_default(),
            lto: self.lto.unwrap_or_default(),
            codegen_units: self.codegen_units.unwrap_or(16),
            opt_level: self.opt_level.unwrap_or_default(),
            target_cpu: self.target_cpu.unwrap_or_default(),
        }
    }
}

fn deserialize_profile_enum<'de, D, T>(
    deserializer: D,
    field: &'static str,
    allowed: &'static str,
    parse: impl FnOnce(&str) -> Option<T>,
) -> Result<Option<T>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<String>::deserialize(deserializer)
        .map_err(|_| D::Error::custom(format!("{field} must be {allowed}")))?;
    value.map_or(Ok(None), |value| {
        parse(&value)
            .map(Some)
            .ok_or_else(|| D::Error::custom(format!("{field} must be {allowed}")))
    })
}
fn deserialize_collector_allocator<'de, D>(d: D) -> Result<Option<CollectorAllocator>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_profile_enum(d, "collector.allocator", "system or mimalloc", |v| match v {
        "system" => Some(CollectorAllocator::System),
        "mimalloc" => Some(CollectorAllocator::Mimalloc),
        _ => None,
    })
}
fn deserialize_collector_lto<'de, D>(d: D) -> Result<Option<CollectorLto>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_profile_enum(d, "collector.lto", "off, thin-local, thin or fat", |v| match v {
        "off" => Some(CollectorLto::Off),
        "thin-local" => Some(CollectorLto::ThinLocal),
        "thin" => Some(CollectorLto::Thin),
        "fat" => Some(CollectorLto::Fat),
        _ => None,
    })
}
fn deserialize_collector_codegen_units<'de, D>(d: D) -> Result<Option<u16>, D::Error>
where
    D: Deserializer<'de>,
{
    Option::<u16>::deserialize(d).map_err(|_| D::Error::custom("collector.codegen_units must be an integer in 1..=256"))
}
fn deserialize_collector_opt_level<'de, D>(d: D) -> Result<Option<CollectorOptLevel>, D::Error>
where
    D: Deserializer<'de>,
{
    let level =
        Option::<u8>::deserialize(d).map_err(|_| D::Error::custom("collector.opt_level must be integer 2 or 3"))?;
    level.map_or(Ok(None), |v| match v {
        2 => Ok(Some(CollectorOptLevel::O2)),
        3 => Ok(Some(CollectorOptLevel::O3)),
        _ => Err(D::Error::custom("collector.opt_level must be integer 2 or 3")),
    })
}
fn deserialize_collector_target_cpu<'de, D>(d: D) -> Result<Option<CollectorTargetCpu>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_profile_enum(
        d,
        "collector.target_cpu",
        "generic, x86-64-v2, x86-64-v3 or x86-64-v4",
        |v| match v {
            "generic" => Some(CollectorTargetCpu::Generic),
            "x86-64-v2" => Some(CollectorTargetCpu::X86_64V2),
            "x86-64-v3" => Some(CollectorTargetCpu::X86_64V3),
            "x86-64-v4" => Some(CollectorTargetCpu::X86_64V4),
            _ => None,
        },
    )
}

fn deserialize_client_build_lto<'de, D>(d: D) -> Result<Option<CollectorLto>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_profile_enum(
        d,
        "client.build_profile.lto",
        "off, thin-local, thin or fat",
        |value| match value {
            "off" => Some(CollectorLto::Off),
            "thin-local" => Some(CollectorLto::ThinLocal),
            "thin" => Some(CollectorLto::Thin),
            "fat" => Some(CollectorLto::Fat),
            _ => None,
        },
    )
}
fn deserialize_client_build_codegen_units<'de, D>(d: D) -> Result<Option<u16>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<u16>::deserialize(d)
        .map_err(|_| D::Error::custom("client.build_profile.codegen_units must be an integer in 1..=256"))?;
    value.map_or(Ok(None), |value| match value {
        1..=256 => Ok(Some(value)),
        _ => Err(D::Error::custom(
            "client.build_profile.codegen_units must be an integer in 1..=256",
        )),
    })
}
fn deserialize_client_build_opt_level<'de, D>(d: D) -> Result<Option<CollectorOptLevel>, D::Error>
where
    D: Deserializer<'de>,
{
    let value = Option::<u8>::deserialize(d)
        .map_err(|_| D::Error::custom("client.build_profile.opt_level must be integer 2 or 3"))?;
    value.map_or(Ok(None), |value| match value {
        2 => Ok(Some(CollectorOptLevel::O2)),
        3 => Ok(Some(CollectorOptLevel::O3)),
        _ => Err(D::Error::custom(
            "client.build_profile.opt_level must be integer 2 or 3",
        )),
    })
}
fn deserialize_client_build_target_cpu<'de, D>(d: D) -> Result<Option<CollectorTargetCpu>, D::Error>
where
    D: Deserializer<'de>,
{
    deserialize_profile_enum(
        d,
        "client.build_profile.target_cpu",
        "generic, x86-64-v2, x86-64-v3 or x86-64-v4",
        |value| match value {
            "generic" => Some(CollectorTargetCpu::Generic),
            "x86-64-v2" => Some(CollectorTargetCpu::X86_64V2),
            "x86-64-v3" => Some(CollectorTargetCpu::X86_64V3),
            "x86-64-v4" => Some(CollectorTargetCpu::X86_64V4),
            _ => None,
        },
    )
}

fn deserialize_channel_capacity_object<'de, D>(deserializer: D) -> Result<Option<ChannelCapacityTunables>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    ChannelCapacityTunables::deserialize(deserializer)
        .map(Some)
        .map_err(|error| {
            let message = error.to_string();
            if message.starts_with("collector.channel_capacity.") {
                serde::de::Error::custom(message)
            } else {
                serde::de::Error::custom("collector.channel_capacity must be an object with supported capacity fields")
            }
        })
}

macro_rules! channel_capacity_deserializer {
    ($function:ident, $field:literal) => {
        fn $function<'de, D>(deserializer: D) -> Result<Option<usize>, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            usize::deserialize(deserializer).map(Some).map_err(|_| {
                serde::de::Error::custom(format!(
                    "collector.channel_capacity.{} must be a non-negative integer",
                    $field
                ))
            })
        }
    };
}

channel_capacity_deserializer!(deserialize_control_node, "control_node");
channel_capacity_deserializer!(deserialize_control_pipeline, "control_pipeline");
channel_capacity_deserializer!(deserialize_control_completion, "control_completion");
channel_capacity_deserializer!(deserialize_pdata, "pdata");

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ChannelCapacityTunables {
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_control_node"
    )]
    pub control_node: Option<usize>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_control_pipeline"
    )]
    pub control_pipeline: Option<usize>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_control_completion"
    )]
    pub control_completion: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none", deserialize_with = "deserialize_pdata")]
    pub pdata: Option<usize>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct CollectorUploadTunables {
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_staging_mode"
    )]
    pub staging_mode: Option<arrow_deltalake::StagingMode>,
    pub mode: Option<CollectorUploadMode>,
    pub buffer_capacity_bytes: Option<usize>,
    pub request_concurrency: Option<usize>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
#[serde(default, deny_unknown_fields)]
pub struct CollectorSocketTunables {
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present_usize"
    )]
    pub recv_buffer_bytes: Option<usize>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present_usize"
    )]
    pub send_buffer_bytes: Option<usize>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present_bool"
    )]
    pub tcp_nodelay: Option<bool>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present_u32"
    )]
    pub h2_stream_window_bytes: Option<u32>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present_u32"
    )]
    pub h2_connection_window_bytes: Option<u32>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present_u32"
    )]
    pub h2_max_frame_bytes: Option<u32>,
}

fn deserialize_present_u32<'de, D: Deserializer<'de>>(deserializer: D) -> Result<Option<u32>, D::Error> {
    u32::deserialize(deserializer).map(Some)
}

impl CollectorSocketTunables {
    /// Validates listener buffer requests before they reach the OS.
    ///
    /// # Errors
    /// Returns an error for an invalid buffer size.
    pub fn validate(&self) -> Result<(), String> {
        for (name, value) in [
            ("collector.socket.recv_buffer_bytes", self.recv_buffer_bytes),
            ("collector.socket.send_buffer_bytes", self.send_buffer_bytes),
        ] {
            if let Some(value) = value
                && !(1..=1_073_741_823).contains(&value)
            {
                return Err(format!("{name} must be in 1..=1073741823"));
            }
        }
        for (name, value, min, max) in [
            ("h2_stream_window_bytes", self.h2_stream_window_bytes, 65_535, 64 << 20),
            (
                "h2_connection_window_bytes",
                self.h2_connection_window_bytes,
                65_535,
                256 << 20,
            ),
            ("h2_max_frame_bytes", self.h2_max_frame_bytes, 16_384, 1 << 20),
        ] {
            if value.is_some_and(|bytes| !(min..=max).contains(&bytes)) {
                return Err(format!("collector.socket.{name} must be in {min}..={max}"));
            }
        }
        Ok(())
    }
}

fn deserialize_present_bool<'de, D>(deserializer: D) -> Result<Option<bool>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    bool::deserialize(deserializer).map(Some)
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct CatalogTunables {
    pub maximum_cache_age: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RoutingTunables {
    pub connect_timeout: Option<String>,
    pub command_timeout: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ExporterTunables {
    pub authority_timeout: Option<String>,
}

/// Largest exact JSON byte budget that also fits a Tokio semaphore on this target.
pub const MAX_QUEUE_CAPACITY_BYTES: u64 = if ((usize::MAX >> 3) as u64) < 9_007_199_254_740_991 {
    (usize::MAX >> 3) as u64
} else {
    9_007_199_254_740_991
};

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct QueueCapacityTunables {
    pub bytes: Option<u64>,
    pub rows: Option<u32>,
    pub batches: Option<u32>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct AdaptiveFileTunables {
    pub target_parquet_bytes: Option<u64>,
    pub min_input_bytes: Option<u64>,
    pub max_input_bytes: Option<u64>,
    pub cold_start_parquet_ratio: Option<f64>,
    pub ewma_weight: Option<f64>,
    pub min_parquet_ratio: Option<f64>,
    pub max_parquet_ratio: Option<f64>,
}

/// Collection behavior when the oldest queued batch reaches its age cutoff.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum AgeFlushPolicy {
    #[default]
    Immediate,
    DrainReady,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct BatchingTunables {
    pub coalesce_publications: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub age_flush_policy: Option<AgeFlushPolicy>,
    pub global_capacity: Option<QueueCapacityTunables>,
    pub per_table_capacity: Option<QueueCapacityTunables>,
    pub max_arrow_memory_per_flush: Option<u64>,
    pub max_rows_per_flush: Option<u64>,
    pub max_batches_per_flush: Option<u64>,
    pub max_age: Option<String>,
    pub max_concurrent_flushes: Option<usize>,
    pub pipeline_depth: Option<usize>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_pending_commit_bytes"
    )]
    pub max_pending_commit_bytes: Option<u64>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_stage_file_concurrency"
    )]
    pub stage_file_concurrency: Option<usize>,
    pub adaptive_file: Option<AdaptiveFileTunables>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct CommitTunables {
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_txn_lookup_mode"
    )]
    pub txn_lookup_mode: Option<arrow_deltalake::TxnLookupMode>,
    pub batch_window: Option<String>,
    pub max_files_per_commit: Option<usize>,
    pub max_pending_files: Option<usize>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_delta_stats_columns"
    )]
    pub delta_stats_columns: Option<Vec<String>>,
}

fn deserialize_txn_lookup_mode<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<Option<arrow_deltalake::TxnLookupMode>, D::Error> {
    arrow_deltalake::TxnLookupMode::deserialize(d).map(Some)
}

fn deserialize_delta_stats_columns<'de, D>(deserializer: D) -> Result<Option<Vec<String>>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    Vec::<String>::deserialize(deserializer)
        .map(Some)
        .map_err(|_| serde::de::Error::custom("collector.commit.delta_stats_columns must be an array of column paths"))
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct LayoutTunables {
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_detach_ratio"
    )]
    pub detach_ratio: Option<u8>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present_sort_execution"
    )]
    pub sort_execution: Option<arrow_deltalake::layout::SortExecution>,
    pub enabled: Option<bool>,
    pub sort_columns: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sort_options: Option<Vec<arrow_deltalake::layout::SortKeyOptions>>,
    pub bloom_columns: Option<Vec<String>>,
    pub max_rows: Option<usize>,
    pub max_input_bytes: Option<usize>,
    pub row_group_rows: Option<usize>,
    pub page_rows: Option<usize>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct LimitsTunables {
    pub grpc_accept_compression: Option<GrpcCompression>,
    pub grpc_send_compression: Option<GrpcCompression>,
    pub max_metadata_bytes: Option<usize>,
    pub max_schema_bytes: Option<usize>,
    pub max_message_bytes: Option<usize>,
    #[serde(
        deserialize_with = "deserialize_batch_channel_capacity",
        skip_serializing_if = "Option::is_none"
    )]
    pub batch_channel_capacity: Option<usize>,
    #[serde(
        deserialize_with = "deserialize_put_result_channel_capacity",
        skip_serializing_if = "Option::is_none"
    )]
    pub put_result_channel_capacity: Option<usize>,
    pub max_stream_duration: Option<String>,
    pub admission_timeout: Option<String>,
    pub max_ddl_bytes: Option<usize>,
}

macro_rules! receiver_channel_deserializer {
    ($function:ident, $field:literal) => {
        fn $function<'de, D>(deserializer: D) -> Result<Option<usize>, D::Error>
        where
            D: serde::Deserializer<'de>,
        {
            usize::deserialize(deserializer).map(Some).map_err(|_| {
                serde::de::Error::custom(concat!(
                    "collector.limits.",
                    $field,
                    " must be a non-negative integer"
                ))
            })
        }
    };
}

receiver_channel_deserializer!(deserialize_batch_channel_capacity, "batch_channel_capacity");
receiver_channel_deserializer!(deserialize_put_result_channel_capacity, "put_result_channel_capacity");

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct PressureTunables {
    pub max_concurrent_streams: Option<usize>,
    pub rss_limit_bytes: Option<u64>,
    pub buffered_bytes_limit: Option<u64>,
    pub buffered_rows_limit: Option<u64>,
    pub buffered_batches_limit: Option<u64>,
    pub pressure_interval: Option<String>,
    pub heartbeat_interval: Option<String>,
    pub registration_ttl: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct RuntimeTunables {
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_timestamp_kernel"
    )]
    pub timestamp_kernel: Option<arrow_deltalake::TimestampKernel>,
    pub workers: Option<usize>,
    pub blocking_workers: Option<usize>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present_usize"
    )]
    pub blocking_fs_reserve: Option<usize>,
    pub upload_limit: Option<usize>,
    pub task_limit: Option<usize>,
    pub read_ahead: Option<usize>,
    pub batch_rows: Option<usize>,
    pub cpu_workers: Option<usize>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_cpu_dispatch"
    )]
    pub cpu_dispatch: Option<arrow_deltalake::CpuDispatch>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_cpu_placement"
    )]
    pub cpu_placement: Option<arrow_deltalake::CpuPlacement>,
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_parquet_column_workers"
    )]
    pub parquet_column_workers: Option<u8>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present_usize"
    )]
    pub cpu_queue_jobs: Option<usize>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present_partition_grouping"
    )]
    pub partition_grouping: Option<PartitionGrouping>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present_usize"
    )]
    pub event_interval: Option<usize>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present_usize"
    )]
    pub global_queue_interval: Option<usize>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present_usize"
    )]
    pub thread_stack_size: Option<usize>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present_usize"
    )]
    pub thread_keep_alive_ms: Option<usize>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_present_usize"
    )]
    pub cpu_thread_stack_size: Option<usize>,
    pub generated_partition_fast_path: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none", deserialize_with = "deserialize_binding")]
    pub generated_partition_binding: Option<arrow_deltalake::GeneratedPartitionBinding>,
    #[serde(
        skip_serializing_if = "Option::is_none",
        deserialize_with = "deserialize_file_mapping_cache"
    )]
    pub file_mapping_cache: Option<bool>,
}

fn deserialize_detach_ratio<'de, D>(deserializer: D) -> Result<Option<u8>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = u8::deserialize(deserializer)
        .map_err(|err| serde::de::Error::custom(format!("collector.layout.detach_ratio: {err}")))?;
    Ok(Some(value))
}

fn deserialize_present_partition_grouping<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<PartitionGrouping>, D::Error> {
    PartitionGrouping::deserialize(deserializer)
        .map(Some)
        .map_err(|error| serde::de::Error::custom(format!("collector.runtime.partition_grouping: {error}")))
}

fn deserialize_mode<'de, D, T>(d: D, path: &str) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: serde::de::DeserializeOwned,
{
    let raw = String::deserialize(d)
        .map_err(|_| <D::Error as serde::de::Error>::custom(format!("{path} must be a string")))?;
    serde_json::from_value(serde_json::Value::String(raw))
        .map(Some)
        .map_err(|error| serde::de::Error::custom(format!("{path}: {error}")))
}

fn deserialize_cpu_dispatch<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<Option<arrow_deltalake::CpuDispatch>, D::Error> {
    deserialize_mode(d, "collector.runtime.cpu_dispatch")
}

fn deserialize_staging_mode<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<Option<arrow_deltalake::StagingMode>, D::Error> {
    deserialize_mode(d, "collector.upload.staging_mode")
}

fn deserialize_cpu_placement<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<Option<arrow_deltalake::CpuPlacement>, D::Error> {
    deserialize_mode(d, "collector.runtime.cpu_placement")
}

fn deserialize_pending_commit_bytes<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<u64>, D::Error> {
    u64::deserialize(d).map(Some).map_err(|_| {
        serde::de::Error::custom("collector.batching.max_pending_commit_bytes must be an unsigned integer")
    })
}

fn deserialize_stage_file_concurrency<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<usize>, D::Error> {
    usize::deserialize(d)
        .map(Some)
        .map_err(|_| serde::de::Error::custom("collector.batching.stage_file_concurrency must be an integer in 1..=16"))
}

fn deserialize_timestamp_kernel<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<Option<arrow_deltalake::TimestampKernel>, D::Error> {
    deserialize_mode(d, "collector.runtime.timestamp_kernel")
}

fn deserialize_binding<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<arrow_deltalake::GeneratedPartitionBinding>, D::Error> {
    arrow_deltalake::GeneratedPartitionBinding::deserialize(deserializer)
        .map(Some)
        .map_err(|error| serde::de::Error::custom(format!("collector.runtime.generated_partition_binding: {error}")))
}

fn deserialize_present_sort_execution<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<arrow_deltalake::layout::SortExecution>, D::Error> {
    arrow_deltalake::layout::SortExecution::deserialize(deserializer)
        .map(Some)
        .map_err(|error| serde::de::Error::custom(format!("collector.layout.sort_execution: {error}")))
}

fn deserialize_present_usize<'de, D>(deserializer: D) -> Result<Option<usize>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    usize::deserialize(deserializer).map(Some)
}

fn deserialize_file_mapping_cache<'de, D: serde::Deserializer<'de>>(d: D) -> Result<Option<bool>, D::Error> {
    bool::deserialize(d)
        .map(Some)
        .map_err(|_| serde::de::Error::custom("collector.runtime.file_mapping_cache must be a boolean"))
}

fn deserialize_parquet_column_workers<'de, D>(deserializer: D) -> Result<Option<u8>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    u8::deserialize(deserializer)
        .map(Some)
        .map_err(|_| serde::de::Error::custom("collector.runtime.parquet_column_workers must be an unsigned integer"))
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ArrowIpcCompression {
    None,
    Lz4Frame,
    Zstd,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum DictionaryHandling {
    Hydrate,
    Resend,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum DataMode {
    Shared,
    Independent,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ClientDataProfile {
    #[default]
    Diagnostic,
    Realistic,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ClientSendIter {
    CloneVec,
    ArcCursor,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ClientStringLengthDistribution {
    #[default]
    Fixed,
    Uniform,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ClientIdentityBuilder {
    FormattedRows,
    DirectUtf8,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ClientValueVariation {
    #[default]
    Cardinality,
    PerRow,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum ClientGuidUniqueness {
    #[default]
    Cardinality,
    PerRow,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum CpuPressureMode {
    #[default]
    Off,
    Process,
}

impl CpuPressureMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Off => "off",
            Self::Process => "process",
        }
    }
}

impl FromStr for CpuPressureMode {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "off" => Ok(Self::Off),
            "process" => Ok(Self::Process),
            _ => Err("expected 'off' or 'process'"),
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum GrpcCompression {
    None,
    Gzip,
    Zstd,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct WalTunables {
    pub flush_interval_ms: Option<u64>,
    pub retention_size_cap_bytes: Option<u64>,
    pub max_age_secs: Option<u64>,
    pub size_cap_policy: Option<WalSizeCapPolicy>,
    pub max_segment_open_duration_ms: Option<u64>,
    pub poll_interval_ms: Option<u64>,
    pub initial_retry_interval_ms: Option<u64>,
    pub max_retry_interval_ms: Option<u64>,
    pub retry_multiplier: Option<f64>,
    pub max_in_flight: Option<usize>,
    pub otlp_handling: Option<WalOtlpHandling>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum WalSizeCapPolicy {
    Backpressure,
    DropOldest,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum WalOtlpHandling {
    PassThrough,
    ConvertToArrow,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct MaintenanceTunables {
    pub enabled: Option<bool>,
    pub operations: Option<Vec<MaintenanceOperation>>,
    pub apply: Option<bool>,
    pub parquet_writer: Option<ParquetWriterConfig>,
    pub optimize_upload_mode: Option<MaintenanceUploadMode>,
    pub optimize_upload_buffer_bytes: Option<usize>,
    pub optimize_upload_concurrency: Option<usize>,
    pub interval_secs: Option<u64>,
    pub max_idle_backoff_secs: Option<u64>,
    pub max_cycles: Option<u64>,
    pub max_consecutive_stale_plans: Option<u32>,
    pub max_concurrent_tables: Option<usize>,
    pub max_concurrent_decoded_bytes: Option<u64>,
    pub operation_timeout_secs: Option<u64>,
    pub commit_attempts: Option<u32>,
    pub commit_budget_secs: Option<u64>,
    pub catalog_runtime_threads: Option<usize>,
    pub catalog_rpc_timeout_secs: Option<u64>,
    pub catalog_max_decoding_message_bytes: Option<usize>,
    pub input_read_strategy: Option<MaintenanceInputReadStrategy>,
    pub input_prefetch_files: Option<u8>,
    pub granularity: Option<MaintenanceGranularity>,
    pub purge_retention_secs: Option<u64>,
    pub anchor_ms: Option<i64>,
    pub vacuum_retention_secs: Option<u64>,
    pub allow_short_vacuum_retention: Option<bool>,
    pub target_file_mib: Option<u64>,
    pub small_file_bytes: Option<u64>,
    pub min_files: Option<usize>,
    pub vacuum_max_deletes: Option<usize>,
    pub vacuum_max_listed_files: Option<usize>,
    pub layout_enabled: Option<bool>,
    pub layout_sort_columns: Option<Vec<String>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub layout_sort_options: Option<Vec<arrow_deltalake::layout::SortKeyOptions>>,
    pub layout_bloom_columns: Option<Vec<String>>,
    pub layout_max_rows: Option<usize>,
    pub layout_max_input_bytes: Option<usize>,
    pub layout_row_group_rows: Option<usize>,
    pub layout_page_rows: Option<usize>,
    pub max_files_per_bin: Option<usize>,
    pub max_bins_per_commit: Option<usize>,
    pub bin_concurrency: Option<usize>,
    pub max_actions_per_commit: Option<usize>,
    pub max_plan_bytes: Option<usize>,
    pub max_decoded_bytes_per_bin: Option<u64>,
    pub max_row_group_bytes: Option<u64>,
    pub max_batch_bytes: Option<usize>,
    pub batch_rows: Option<usize>,
    pub output_row_group_rows: Option<usize>,
    pub max_output_file_bytes: Option<u64>,
    pub max_active_files: Option<usize>,
    pub latest_partitions: Option<usize>,
    pub sealed_final: Option<bool>,
    pub sealed_partitions_only: Option<bool>,
    pub checkpoint_every_commits: Option<u64>,
    pub log_retention_secs: Option<u64>,
    pub max_prune_files: Option<usize>,
    pub purge_max_files_per_commit: Option<usize>,
    pub purge_max_plan_bytes: Option<usize>,
    pub vacuum_delete_chunk: Option<usize>,
    pub vacuum_max_plan_bytes: Option<usize>,
    pub replay_max_log_files: Option<usize>,
    pub replay_max_log_file_bytes: Option<u64>,
    pub replay_max_checkpoint_bytes: Option<u64>,
    pub operation_max_active_files: Option<usize>,
    pub discovery_max_log_files: Option<usize>,
    pub discovery_max_log_file_bytes: Option<u64>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceInputReadStrategy {
    Memory,
    Spool,
    Range,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceUploadMode {
    PutCreate,
    BufferedMultipart,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceGranularity {
    Minute,
    Hour,
    Day,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, Eq, PartialEq, Hash)]
#[serde(rename_all = "snake_case")]
pub enum MaintenanceOperation {
    Purge,
    Optimize,
    Vacuum,
    Checkpoint,
    PruneLog,
}

// PERF69-TUNABLES-BEGIN oat-evidence
// PERF69-TUNABLES-END oat-evidence
// PERF69-TUNABLES-BEGIN oat-campaign
// PERF69-TUNABLES-END oat-campaign
// PERF69-TUNABLES-BEGIN aks-custody
// PERF69-TUNABLES-END aks-custody
// PERF69-TUNABLES-BEGIN file-formation
// PERF69-TUNABLES-END file-formation
// PERF69-TUNABLES-BEGIN commit-pipeline
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, Eq, PartialEq)]
#[serde(rename_all = "snake_case")]
pub enum CollectorUploadMode {
    #[default]
    PutCreate,
    BufferedMultipart,
}

impl CollectorUploadMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PutCreate => "put_create",
            Self::BufferedMultipart => "buffered_multipart",
        }
    }
}

impl FromStr for CollectorUploadMode {
    type Err = &'static str;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        match value {
            "put_create" => Ok(Self::PutCreate),
            "buffered_multipart" => Ok(Self::BufferedMultipart),
            _ => Err("expected 'put_create' or 'buffered_multipart'"),
        }
    }
}
// PERF69-TUNABLES-END commit-pipeline
// PERF69-TUNABLES-BEGIN maintenance
// PERF69-TUNABLES-END maintenance
// PERF69-TUNABLES-BEGIN loadgen
// PERF69-TUNABLES-END loadgen
// PERF69-TUNABLES-BEGIN telemetry
// PERF69-TUNABLES-END telemetry
// PERF69-TUNABLES-BEGIN grafana
// PERF69-TUNABLES-END grafana
