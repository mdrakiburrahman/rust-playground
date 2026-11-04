//! Invented wide data schema and per-file statistics.
//!
//! The schema covers every type Delta collects statistics for (timestamp, date, integers, floats,
//! decimal, string), types it collects only null counts for (boolean, binary, map, array), and
//! nested struct leaves, which count toward `delta.dataSkippingNumIndexedCols` one by one.

use rand::Rng;
use rand::rngs::SmallRng;
use serde_json::{Value, json};
use std::fmt::Write;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Kind {
    Ts,
    Date,
    I64,
    I32,
    I16,
    I8,
    F64,
    F32,
    Dec,
    Bool,
    /// Low-cardinality string drawn from a dictionary of this many values.
    Dict(u32),
    /// Random lowercase hex of this length (ids).
    Hex(usize),
    /// Free text; stats keep the first 32 characters.
    Text,
    Binary,
    Map,
    List,
}

pub struct Leaf {
    /// Dotted path; nested leaves are `parent.child`.
    pub path: &'static str,
    pub kind: Kind,
}

const fn l(path: &'static str, kind: Kind) -> Leaf {
    Leaf { path, kind }
}

/// Leaves in schema order. `date` is the partition column and is not part of the stats.
pub const LEAVES: &[Leaf] = &[
    l("ts", Kind::Ts),
    l("observed_ts", Kind::Ts),
    l("event_date", Kind::Date),
    l("severity", Kind::I32),
    l("severity_text", Kind::Dict(6)),
    l("service", Kind::Dict(200)),
    l("host", Kind::Dict(5000)),
    l("region", Kind::Dict(40)),
    l("env", Kind::Dict(4)),
    l("tenant", Kind::Dict(2000)),
    l("trace_id", Kind::Hex(32)),
    l("span_id", Kind::Hex(16)),
    l("seq", Kind::I64),
    l("bytes", Kind::I64),
    l("latency_us", Kind::I64),
    l("dropped_attrs", Kind::I32),
    l("status_code", Kind::I16),
    l("retry", Kind::I8),
    l("cpu", Kind::F64),
    l("mem_ratio", Kind::F32),
    l("score", Kind::F64),
    l("amount", Kind::Dec),
    l("is_error", Kind::Bool),
    l("sampled", Kind::Bool),
    l("body", Kind::Text),
    l("logger", Kind::Dict(300)),
    l("method", Kind::Dict(8)),
    l("url", Kind::Text),
    l("user_agent", Kind::Text),
    l("k8s.namespace", Kind::Dict(100)),
    l("k8s.pod", Kind::Hex(20)),
    l("k8s.container", Kind::Dict(50)),
    l("resource.sdk_name", Kind::Dict(5)),
    l("resource.sdk_version", Kind::Dict(30)),
    l("resource.cloud_provider", Kind::Dict(3)),
    l("resource.cloud_region", Kind::Dict(40)),
    l("resource.host_arch", Kind::Dict(3)),
    l("http.status", Kind::I32),
    l("http.route", Kind::Text),
    l("http.client_ip", Kind::Dict(100_000)),
    l("version", Kind::Dict(20)),
    l("payload", Kind::Binary),
    l("attributes", Kind::Map),
    l("tags", Kind::List),
    l("ingest_ts", Kind::Ts),
];

/// Which leaves get statistics.
#[derive(Clone, Debug, clap::ValueEnum, PartialEq, Eq)]
pub enum StatsMode {
    /// No stats string.
    None,
    /// `delta.dataSkippingStatsColumns = ts,severity,service`.
    Few,
    /// Delta default: first 32 leaves.
    Default,
    /// `delta.dataSkippingNumIndexedCols = -1`.
    All,
}

impl StatsMode {
    pub fn leaves(&self) -> Vec<&'static Leaf> {
        match self {
            StatsMode::None => vec![],
            StatsMode::Few => LEAVES.iter().filter(|l| matches!(l.path, "ts" | "severity" | "service")).collect(),
            StatsMode::Default => LEAVES.iter().take(32).collect(),
            StatsMode::All => LEAVES.iter().collect(),
        }
    }

    pub fn table_config(&self) -> Vec<(&'static str, &'static str)> {
        match self {
            StatsMode::None => vec![("delta.dataSkippingNumIndexedCols", "0")],
            StatsMode::Few => vec![("delta.dataSkippingStatsColumns", "ts,severity,service")],
            StatsMode::Default => vec![],
            StatsMode::All => vec![("delta.dataSkippingNumIndexedCols", "-1")],
        }
    }
}

/// Delta `schemaString` for the data schema plus the `date` partition column.
pub fn schema_string() -> String {
    fn prim(t: Kind) -> Value {
        match t {
            Kind::Ts => json!("timestamp"),
            Kind::Date => json!("date"),
            Kind::I64 => json!("long"),
            Kind::I32 => json!("integer"),
            Kind::I16 => json!("short"),
            Kind::I8 => json!("byte"),
            Kind::F64 => json!("double"),
            Kind::F32 => json!("float"),
            Kind::Dec => json!("decimal(18,4)"),
            Kind::Bool => json!("boolean"),
            Kind::Dict(_) | Kind::Hex(_) | Kind::Text => json!("string"),
            Kind::Binary => json!("binary"),
            Kind::Map => json!({"type": "map", "keyType": "string", "valueType": "string", "valueContainsNull": true}),
            Kind::List => json!({"type": "array", "elementType": "string", "containsNull": true}),
        }
    }
    let field = |name: &str, t: Value| json!({"name": name, "type": t, "nullable": true, "metadata": {}});
    let mut fields: Vec<Value> = Vec::new();
    let mut i = 0;
    while i < LEAVES.len() {
        let leaf = &LEAVES[i];
        if let Some((parent, _)) = leaf.path.split_once('.') {
            let mut children = Vec::new();
            while i < LEAVES.len() && LEAVES[i].path.starts_with(&format!("{parent}.")) {
                let child = LEAVES[i].path.split_once('.').map(|(_, c)| c).unwrap_or_default();
                children.push(field(child, prim(LEAVES[i].kind)));
                i += 1;
            }
            fields.push(field(parent, json!({"type": "struct", "fields": children})));
        } else {
            fields.push(field(leaf.path, prim(leaf.kind)));
            i += 1;
        }
    }
    fields.push(field("date", json!("string")));
    json!({"type": "struct", "fields": fields}).to_string()
}

/// Stats for one file covering `[start_us, start_us + span_us)`.
pub struct FileStatsGen<'a> {
    pub leaves: &'a [&'static Leaf],
}

fn dict_word(prefix: &str, n: u32) -> String {
    format!("{prefix}-{n:06}")
}

fn hex(rng: &mut SmallRng, len: usize) -> String {
    const H: &[u8] = b"0123456789abcdef";
    (0..len).map(|_| H[rng.random_range(0..16)] as char).collect()
}

fn text(rng: &mut SmallRng) -> String {
    const WORDS: &[&str] = &[
        "request", "completed", "failed", "user", "session", "timeout", "retrying", "connection", "upstream",
        "GET", "POST", "/api/v1/orders", "/api/v2/items", "cache", "miss", "hit", "token", "expired",
    ];
    let mut s = String::new();
    while s.len() < 32 {
        if !s.is_empty() {
            s.push(' ');
        }
        s.push_str(WORDS[rng.random_range(0..WORDS.len())]);
    }
    s.truncate(32);
    s
}

fn ts_str(us: i64) -> String {
    let secs = us.div_euclid(1_000_000);
    let ms = us.rem_euclid(1_000_000) / 1000;
    let days = secs.div_euclid(86_400);
    let sod = secs.rem_euclid(86_400);
    let (y, m, d) = civil(days);
    format!("{y:04}-{m:02}-{d:02}T{:02}:{:02}:{:02}.{ms:03}Z", sod / 3600, sod / 60 % 60, sod % 60)
}

pub fn date_str(days: i64) -> String {
    let (y, m, d) = civil(days);
    format!("{y:04}-{m:02}-{d:02}")
}

/// Days since 1970-01-01 to civil date (Howard Hinnant's algorithm).
fn civil(z: i64) -> (i64, i64, i64) {
    let z = z + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z - era * 146_097;
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    (if m <= 2 { y + 1 } else { y }, m, d)
}

impl FileStatsGen<'_> {
    pub fn stats(&self, rng: &mut SmallRng, start_us: i64, span_us: i64, records: i64) -> String {
        let mut min = String::new();
        let mut max = String::new();
        let mut nulls = String::new();
        for leaf in self.leaves {
            let (lo, hi): (Option<String>, Option<String>) = match leaf.kind {
                Kind::Ts => {
                    let a = start_us + rng.random_range(0..1000);
                    (Some(format!("\"{}\"", ts_str(a))), Some(format!("\"{}\"", ts_str(a + span_us))))
                }
                Kind::Date => {
                    let d = start_us / 86_400_000_000;
                    (Some(format!("\"{}\"", date_str(d))), Some(format!("\"{}\"", date_str(d))))
                }
                Kind::I64 => {
                    let a: i64 = rng.random_range(0..1_000_000_000);
                    (Some(a.to_string()), Some((a + rng.random_range(1..1_000_000_000)).to_string()))
                }
                Kind::I32 => (Some(rng.random_range(0..10).to_string()), Some(rng.random_range(10..100_000).to_string())),
                Kind::I16 => (Some("100".into()), Some(rng.random_range(200..600).to_string())),
                Kind::I8 => (Some("0".into()), Some(rng.random_range(1..8).to_string())),
                Kind::F64 | Kind::F32 => {
                    let a: f64 = rng.random::<f64>();
                    (Some(format!("{a:.6}")), Some(format!("{:.6}", a + rng.random::<f64>() * 100.0)))
                }
                Kind::Dec => (Some("0.0000".into()), Some(format!("{}.{:04}", rng.random_range(1000..99_999), rng.random_range(0..10_000)))),
                Kind::Dict(n) => {
                    let p = leaf.path.rsplit('.').next().unwrap_or("v");
                    let a = rng.random_range(0..n.max(2) / 2);
                    let b = rng.random_range(n.max(2) / 2..n.max(2));
                    (Some(format!("\"{}\"", dict_word(p, a))), Some(format!("\"{}\"", dict_word(p, b))))
                }
                Kind::Hex(n) => (Some(format!("\"0{}\"", hex(rng, n.min(32) - 1))), Some(format!("\"f{}\"", hex(rng, n.min(32) - 1)))),
                Kind::Text => (Some(format!("\"{}\"", text(rng))), Some(format!("\"{}\"", text(rng)))),
                Kind::Bool | Kind::Binary | Kind::Map | Kind::List => (None, None),
            };
            let nc = if rng.random_range(0..4) == 0 { rng.random_range(0..records / 10 + 1) } else { 0 };
            let put = |buf: &mut String, v: &str| {
                if let Some((parent, child)) = leaf.path.split_once('.') {
                    // Nested leaves are written flat here and regrouped below.
                    let _ = write!(buf, "\u{1}{parent}\u{2}{child}\u{3}{v}");
                } else {
                    let _ = write!(buf, ",\"{}\":{}", leaf.path, v);
                }
            };
            if let (Some(lo), Some(hi)) = (lo, hi) {
                put(&mut min, &lo);
                put(&mut max, &hi);
            }
            put(&mut nulls, &nc.to_string());
        }
        format!(
            "{{\"numRecords\":{records},\"minValues\":{{{}}},\"maxValues\":{{{}}},\"nullCount\":{{{}}}}}",
            regroup(&min),
            regroup(&max),
            regroup(&nulls)
        )
    }
}

/// Turn the flat encoding from `stats` into JSON members with nested objects.
fn regroup(flat: &str) -> String {
    let mut out = String::new();
    let mut current_parent: Option<String> = None;
    let mut rest = flat;
    while !rest.is_empty() {
        if let Some(stripped) = rest.strip_prefix('\u{1}') {
            let p_end = stripped.find('\u{2}').unwrap_or(0);
            let parent = &stripped[..p_end];
            let after = &stripped[p_end + 1..];
            let c_end = after.find('\u{3}').unwrap_or(0);
            let child = &after[..c_end];
            let vrest = &after[c_end + 1..];
            let v_end = vrest.find(['\u{1}', ',']).unwrap_or(vrest.len());
            let value = &vrest[..v_end];
            if current_parent.as_deref() != Some(parent) {
                if current_parent.is_some() {
                    out.push('}');
                }
                let _ = write!(out, ",\"{parent}\":{{\"{child}\":{value}");
                current_parent = Some(parent.to_owned());
            } else {
                let _ = write!(out, ",\"{child}\":{value}");
            }
            rest = &vrest[v_end..];
        } else {
            if current_parent.take().is_some() {
                out.push('}');
            }
            let end = rest[1..].find(['\u{1}', ',']).map(|i| i + 1).unwrap_or(rest.len());
            // A top-level member may itself contain commas only inside quotes; values here never do.
            out.push_str(&rest[..end]);
            rest = &rest[end..];
        }
    }
    if current_parent.is_some() {
        out.push('}');
    }
    out.trim_start_matches(',').to_owned()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand::SeedableRng;

    #[test]
    fn stats_are_valid_json_for_every_mode() {
        let mut rng = SmallRng::seed_from_u64(1);
        for mode in [StatsMode::Few, StatsMode::Default, StatsMode::All] {
            let leaves = mode.leaves();
            let g = FileStatsGen { leaves: &leaves };
            let s = g.stats(&mut rng, 1_760_000_000_000_000, 60_000_000, 1_000_000);
            let v: Value = serde_json::from_str(&s).unwrap_or_else(|e| panic!("{mode:?}: {e}: {s}"));
            assert!(v["minValues"].is_object());
            if mode == StatsMode::All {
                assert!(v["minValues"]["resource"]["sdk_name"].is_string(), "{s}");
                assert!(v["nullCount"]["tags"].is_number(), "{s}");
            }
        }
        let _: Value = serde_json::from_str(&schema_string()).unwrap_or_else(|e| panic!("{e}"));
    }

    #[test]
    fn civil_dates() {
        assert_eq!(date_str(0), "1970-01-01");
        assert_eq!(date_str(20_000), "2024-10-04");
    }
}
