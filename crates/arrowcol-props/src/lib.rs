//! Authoritative table-property registry shared by catalog, writers and maintenance.

use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter};

/// The unit-aware type of a property.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PropType {
    Bool,
    Integer,
    Bytes,
    Duration,
    Float,
    Enum,
    StringList,
}

/// Where an explicit property may be set. Table properties also accept process defaults.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Scope {
    Table,
    Process,
}

/// Additional constraints applied after type parsing.
#[derive(Clone, Copy, Debug)]
pub enum Validator {
    Any,
    Positive,
    Minimum(u64),
    Range(f64, f64),
    OneOf(&'static [&'static str]),
}

/// One registry entry. Add entries here rather than duplicating consumer validation.
#[derive(Clone, Copy, Debug)]
pub struct PropKey {
    pub name: &'static str,
    pub prop_type: PropType,
    pub default: &'static str,
    pub validator: Validator,
    pub scope: Scope,
    pub doc: &'static str,
    pub since: &'static str,
}

macro_rules! key {
    ($name:literal, $kind:ident, $default:literal, $validator:expr, $doc:literal) => {
        PropKey {
            name: $name,
            prop_type: PropType::$kind,
            default: $default,
            validator: $validator,
            scope: Scope::Table,
            doc: $doc,
            since: "0.1",
        }
    };
}

/// Centrally documented, typed tunables. Unknown `arrowcol.*` keys are errors.
pub static REGISTRY: &[PropKey] = &[
    key!(
        "delta.appendOnly",
        Bool,
        "false",
        Validator::Any,
        "Append-only table policy. Syntax validation does not authorize feature/protocol changes."
    ),
    key!(
        "delta.targetFileSize",
        Bytes,
        "128 MiB",
        Validator::Positive,
        "Default compressed Parquet file target."
    ),
    key!(
        "delta.deletedFileRetentionDuration",
        Duration,
        "interval 7 days",
        Validator::Minimum(604_800_000),
        "Tombstone retention; at least seven days for reader safety."
    ),
    key!(
        "delta.logRetentionDuration",
        Duration,
        "interval 30 days",
        Validator::Minimum(604_800_000),
        "Retained transaction history; at least seven days."
    ),
    key!(
        "delta.checkpointInterval",
        Integer,
        "10",
        Validator::Positive,
        "Commits between checkpoints."
    ),
    key!(
        "arrowcol.maintenance.enabled",
        Bool,
        "true",
        Validator::Any,
        "Enable scheduled maintenance for this table."
    ),
    key!(
        "arrowcol.maintenance.interval",
        Duration,
        "60 seconds",
        Validator::Positive,
        "Minimum delay between maintenance passes."
    ),
    key!(
        "arrowcol.maintenance.operationTimeout",
        Duration,
        "300 seconds",
        Validator::Positive,
        "Per-operation execution budget."
    ),
    key!(
        "arrowcol.optimize.enabled",
        Bool,
        "true",
        Validator::Any,
        "Enable small-file compaction."
    ),
    key!(
        "arrowcol.optimize.minFiles",
        Integer,
        "2",
        Validator::Minimum(2),
        "Minimum small files per eligible partition."
    ),
    key!(
        "arrowcol.optimize.targetFileSize",
        Bytes,
        "128 MiB",
        Validator::Positive,
        "Compaction output target; explicit delta.targetFileSize is the fallback."
    ),
    key!(
        "arrowcol.optimize.smallFileSize",
        Bytes,
        "64 MiB",
        Validator::Positive,
        "Files strictly below this size are small. Defaults to half an explicitly configured target when unset."
    ),
    key!(
        "arrowcol.optimize.sealedPartitionsOnly",
        Bool,
        "true",
        Validator::Any,
        "Exclude newest and open ingestion partitions from compaction."
    ),
    key!(
        "arrowcol.optimize.latestPartitions",
        Integer,
        "0",
        Validator::Any,
        "Only the newest N eligible sealed partitions; zero means all."
    ),
    key!(
        "arrowcol.optimize.maxFilesPerBin",
        Integer,
        "64",
        Validator::Minimum(2),
        "Maximum input files per compaction bin."
    ),
    key!(
        "arrowcol.optimize.maxBinsPerCommit",
        Integer,
        "16",
        Validator::Positive,
        "Maximum bins committed in one pass."
    ),
    key!(
        "arrowcol.optimize.maxActionsPerCommit",
        Integer,
        "2048",
        Validator::Minimum(3),
        "Maximum add/remove actions in a commit."
    ),
    key!(
        "arrowcol.optimize.maxPlanSize",
        Bytes,
        "1 MiB",
        Validator::Minimum(1024),
        "Maximum serialized plan size."
    ),
    key!(
        "arrowcol.optimize.maxDecodedSizePerBin",
        Bytes,
        "1 GiB",
        Validator::Positive,
        "Maximum decoded input per bin."
    ),
    key!(
        "arrowcol.optimize.maxOutputFileSize",
        Bytes,
        "1023 MiB",
        Validator::Range(1.0, 1_073_741_823.0),
        "Hard maximum encoded output size."
    ),
    key!(
        "arrowcol.optimize.maxActiveFiles",
        Integer,
        "1000000",
        Validator::Positive,
        "Maximum files admitted to an optimize plan."
    ),
    key!(
        "arrowcol.vacuum.enabled",
        Bool,
        "true",
        Validator::Any,
        "Enable physical deletion of expired unreferenced files."
    ),
    key!(
        "arrowcol.vacuum.retention",
        Duration,
        "7 days",
        Validator::Minimum(604_800_000),
        "VACUUM retention; at least seven days and no longer than tombstone retention."
    ),
    key!(
        "arrowcol.vacuum.allowShortRetention",
        Bool,
        "false",
        Validator::Any,
        "Reader-risk acknowledgement only; does not bypass the seven-day property floor."
    ),
    key!(
        "arrowcol.vacuum.maxDeletes",
        Integer,
        "10000",
        Validator::Positive,
        "Maximum physical files deleted per pass."
    ),
    key!(
        "arrowcol.vacuum.maxListedFiles",
        Integer,
        "100000",
        Validator::Positive,
        "Maximum inventory entries admitted to a cheap probe."
    ),
    key!(
        "arrowcol.purge.enabled",
        Bool,
        "false",
        Validator::Any,
        "Enable partition retention; deletes logical data before OPTIMIZE and VACUUM."
    ),
    key!(
        "arrowcol.purge.retention",
        Duration,
        "30 days",
        Validator::Positive,
        "Logical partition retention window."
    ),
    key!(
        "arrowcol.partition.granularity",
        Enum,
        "hour",
        Validator::OneOf(&["minute", "hour", "day"]),
        "Time-partition granularity, replacing ARROWCOL_PARTITION_GRANULARITY."
    ),
    key!(
        "arrowcol.optimize.maxRowGroupSize",
        Bytes,
        "128 MiB",
        Validator::Positive,
        "Maximum uncompressed Parquet input row-group size."
    ),
    key!(
        "arrowcol.optimize.maxBatchSize",
        Bytes,
        "64 MiB",
        Validator::Positive,
        "Maximum decoded Arrow bytes per input batch."
    ),
    key!(
        "arrowcol.optimize.batchRows",
        Integer,
        "8192",
        Validator::Positive,
        "Rows read per input batch."
    ),
    key!(
        "arrowcol.layout.enabled",
        Bool,
        "false",
        Validator::Any,
        "Enable bounded sorted-file layout."
    ),
    key!(
        "arrowcol.layout.sortColumns",
        StringList,
        "customer_id",
        Validator::Positive,
        "Comma-separated sort columns; names must exist in the table schema."
    ),
    key!(
        "arrowcol.layout.bloomColumns",
        StringList,
        "customer_id",
        Validator::Any,
        "Comma-separated Bloom-filter columns; empty disables Bloom filters."
    ),
    key!(
        "arrowcol.layout.maxRows",
        Integer,
        "262144",
        Validator::Range(1.0, 4_294_967_295.0),
        "Maximum rows buffered per sorted file."
    ),
    key!(
        "arrowcol.layout.maxInputSize",
        Bytes,
        "64 MiB",
        Validator::Positive,
        "Maximum Arrow input retained per sorted file."
    ),
    key!(
        "arrowcol.layout.rowGroupRows",
        Integer,
        "16384",
        Validator::Positive,
        "Parquet row-group row limit."
    ),
    key!(
        "arrowcol.layout.pageRows",
        Integer,
        "2048",
        Validator::Positive,
        "Parquet data-page row limit."
    ),
    key!(
        "arrowcol.writer.targetFileSize",
        Bytes,
        "128 MiB",
        Validator::Positive,
        "Compressed-byte target for adaptive publication-group scheduling, not a per-file size guarantee; explicit delta.targetFileSize is the fallback."
    ),
    key!(
        "arrowcol.writer.minInputSize",
        Bytes,
        "16 MiB",
        Validator::Positive,
        "Minimum Arrow-input threshold for adaptive publication-group scheduling."
    ),
    key!(
        "arrowcol.writer.maxInputSize",
        Bytes,
        "1 GiB",
        Validator::Positive,
        "Maximum Arrow-input threshold for adaptive publication-group scheduling."
    ),
    key!(
        "arrowcol.writer.coldStartParquetRatio",
        Float,
        "0.25",
        Validator::Positive,
        "Initial encoded/logical compression ratio."
    ),
    key!(
        "arrowcol.writer.ewmaWeight",
        Float,
        "0.2",
        Validator::Range(f64::MIN_POSITIVE, 0.5),
        "Weight of each successfully committed file observation in the group-scheduling estimator."
    ),
    key!(
        "arrowcol.writer.minParquetRatio",
        Float,
        "0.015625",
        Validator::Positive,
        "Lower compression-ratio clamp."
    ),
    key!(
        "arrowcol.writer.maxParquetRatio",
        Float,
        "4",
        Validator::Positive,
        "Upper compression-ratio clamp."
    ),
];

/// A parsed value; sizes are bytes and durations are milliseconds.
#[derive(Clone, Debug, PartialEq)]
pub enum PropValue {
    Bool(bool),
    Unsigned(u64),
    Float(f64),
    Text(String),
    StringList(Vec<String>),
}

/// Invalid property or cross-property relationship.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PropError {
    pub key: String,
    pub message: String,
}

impl Display for PropError {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{}: {}", self.key, self.message)
    }
}

impl std::error::Error for PropError {}

fn error(key: &str, message: impl Into<String>) -> PropError {
    PropError {
        key: key.into(),
        message: message.into(),
    }
}

/// Parsed effective properties. Unknown non-ArrowCol entries are retained verbatim.
#[derive(Clone, Debug)]
pub struct ResolvedProps {
    values: BTreeMap<String, PropValue>,
    pub raw: BTreeMap<String, String>,
}

impl ResolvedProps {
    #[must_use]
    pub fn get(&self, key: &str) -> Option<&PropValue> {
        self.values.get(key)
    }

    /// Read a registered boolean.
    ///
    /// # Errors
    /// Returns an error for an absent or non-boolean key.
    pub fn boolean(&self, key: &str) -> Result<bool, PropError> {
        match self.get(key) {
            Some(PropValue::Bool(value)) => Ok(*value),
            _ => Err(error(key, "not a boolean property")),
        }
    }

    /// Read a registered integer, byte size or millisecond duration.
    ///
    /// # Errors
    /// Returns an error for an absent or differently typed key.
    pub fn unsigned(&self, key: &str) -> Result<u64, PropError> {
        match self.get(key) {
            Some(PropValue::Unsigned(value)) => Ok(*value),
            _ => Err(error(key, "not an unsigned property")),
        }
    }

    /// Read a registered float.
    ///
    /// # Errors
    /// Returns an error for an absent or non-float key.
    pub fn float(&self, key: &str) -> Result<f64, PropError> {
        match self.get(key) {
            Some(PropValue::Float(value)) => Ok(*value),
            _ => Err(error(key, "not a float property")),
        }
    }

    /// Read a registered enum.
    ///
    /// # Errors
    /// Returns an error for an absent or non-enum key.
    pub fn text(&self, key: &str) -> Result<&str, PropError> {
        match self.get(key) {
            Some(PropValue::Text(value)) => Ok(value),
            _ => Err(error(key, "not an enum property")),
        }
    }

    /// Read a registered column list.
    ///
    /// # Errors
    /// Returns an error for an absent or non-list key.
    pub fn string_list(&self, key: &str) -> Result<&[String], PropError> {
        match self.get(key) {
            Some(PropValue::StringList(value)) => Ok(value),
            _ => Err(error(key, "not a column-list property")),
        }
    }
}

impl PropKey {
    /// Parse and validate one value using its registry type and constraints.
    ///
    /// # Errors
    /// Rejects invalid types, overflow, non-finite floats and validator failures.
    pub fn parse(&self, raw: &str) -> Result<PropValue, PropError> {
        let raw = if self.prop_type == PropType::Bool && self.name.starts_with("delta.") {
            raw
        } else {
            raw.trim()
        };
        let value = match self.prop_type {
            PropType::Bool => PropValue::Bool(match raw {
                "true" => true,
                "false" => false,
                _ => return Err(error(self.name, "expected true or false")),
            }),
            PropType::Integer => {
                PropValue::Unsigned(raw.parse().map_err(|_| error(self.name, "expected unsigned integer"))?)
            }
            PropType::Bytes | PropType::Duration => {
                PropValue::Unsigned(parse_quantity(self.name, raw, self.prop_type)?)
            }
            PropType::Float => {
                let value: f64 = raw.parse().map_err(|_| error(self.name, "expected finite float"))?;
                if !value.is_finite() {
                    return Err(error(self.name, "expected finite float"));
                }
                PropValue::Float(value)
            }
            PropType::Enum => PropValue::Text(raw.into()),
            PropType::StringList => {
                let columns: Vec<String> = if raw.is_empty() {
                    Vec::new()
                } else {
                    raw.split(',').map(|s| s.trim().to_owned()).collect()
                };
                let mut unique = std::collections::BTreeSet::new();
                if columns.iter().any(|s| s.is_empty() || !unique.insert(s)) {
                    return Err(error(self.name, "column names must be nonempty and unique"));
                }
                PropValue::StringList(columns)
            }
        };
        let number = match &value {
            PropValue::Unsigned(n) => Some(*n as f64),
            PropValue::Float(n) => Some(*n),
            PropValue::StringList(v) => Some(v.len() as f64),
            _ => None,
        };
        let valid = match self.validator {
            Validator::Any => true,
            Validator::Positive => number.is_some_and(|n| n > 0.0),
            Validator::Minimum(min) => matches!(&value, PropValue::Unsigned(n) if *n >= min),
            Validator::Range(min, max) => number.is_some_and(|n| n >= min && n <= max),
            Validator::OneOf(choices) => choices.contains(&raw),
        };
        if !valid {
            return Err(error(self.name, format!("value violates {:?}", self.validator)));
        }
        Ok(value)
    }
}

fn parse_quantity(key: &str, raw: &str, kind: PropType) -> Result<u64, PropError> {
    let raw = raw.strip_prefix("interval ").unwrap_or(raw);
    let boundary = raw.find(|c: char| !c.is_ascii_digit()).unwrap_or(raw.len());
    let number: u64 = raw[..boundary]
        .parse()
        .map_err(|_| error(key, "expected an unsigned whole quantity"))?;
    let unit = raw[boundary..].trim().to_ascii_lowercase();
    let multiplier = match (kind, unit.as_str()) {
        (PropType::Bytes, "" | "b" | "bytes") | (PropType::Duration, "" | "ms" | "millisecond" | "milliseconds") => 1,
        (PropType::Bytes, "kb") => 1_000,
        (PropType::Bytes, "mb") => 1_000_000,
        (PropType::Bytes, "gb") => 1_000_000_000,
        (PropType::Bytes, "kib") => 1_024,
        (PropType::Bytes, "mib") => 1_048_576,
        (PropType::Bytes, "gib") => 1_073_741_824,
        (PropType::Duration, "s" | "second" | "seconds") => 1_000,
        (PropType::Duration, "m" | "minute" | "minutes") => 60_000,
        (PropType::Duration, "h" | "hour" | "hours") => 3_600_000,
        (PropType::Duration, "d" | "day" | "days") => 86_400_000,
        (PropType::Duration, "w" | "week" | "weeks") => 604_800_000,
        _ => return Err(error(key, format!("unsupported unit {unit:?}"))),
    };
    number
        .checked_mul(multiplier)
        .filter(|v| *v <= i64::MAX as u64)
        .ok_or_else(|| error(key, "quantity exceeds signed 64-bit consumer range"))
}

fn validate_map(properties: &BTreeMap<String, String>, table: bool) -> Result<(), PropError> {
    for (name, value) in properties {
        if let Some(key) = REGISTRY.iter().find(|key| key.name == name) {
            if table && key.scope == Scope::Process {
                return Err(error(name, "process-only property is not valid in TBLPROPERTIES"));
            }
            key.parse(value)?;
        } else if name.starts_with("arrowcol.") {
            let closest = REGISTRY.iter().min_by_key(|key| distance(name, key.name));
            let suggestion = closest.map_or(String::new(), |key| format!("; did you mean {}?", key.name));
            return Err(error(name, format!("unknown ArrowCol property{suggestion}")));
        }
    }
    Ok(())
}

fn distance(a: &str, b: &str) -> usize {
    let mut row: Vec<_> = (0..=b.chars().count()).collect();
    for (i, ac) in a.chars().enumerate() {
        let mut diagonal = row[0];
        row[0] = i + 1;
        for (j, bc) in b.chars().enumerate() {
            let previous = row[j + 1];
            row[j + 1] = (row[j] + 1).min(previous + 1).min(diagonal + usize::from(ac != bc));
            diagonal = previous;
        }
    }
    row.last().copied().unwrap_or(0)
}

/// Catalog DDL validation hook. No mutation occurs on rejection.
///
/// # Errors
/// Returns the offending key, with suggestions for unknown `arrowcol.*` names.
pub fn validate_table_properties(properties: &BTreeMap<String, String>) -> Result<(), PropError> {
    resolve(properties, &BTreeMap::new()).map(|_| ())
}

/// Resolve table properties over process defaults over registry defaults.
/// Validates even shadowed inputs, so typos and invalid defaults cannot hide.
///
/// # Errors
/// Rejects invalid keys, values, retention relationships and inverted bounds.
pub fn resolve(
    table: &BTreeMap<String, String>,
    process: &BTreeMap<String, String>,
) -> Result<ResolvedProps, PropError> {
    validate_map(process, false)?;
    validate_map(table, true)?;
    let mut raw = process.clone();
    raw.extend(table.clone());
    let mut values = BTreeMap::new();
    for key in REGISTRY {
        let fallback = if matches!(
            key.name,
            "arrowcol.optimize.targetFileSize" | "arrowcol.writer.targetFileSize"
        ) {
            table
                .get("delta.targetFileSize")
                .or_else(|| process.get("delta.targetFileSize"))
        } else {
            None
        };
        let source = table
            .get(key.name)
            .or_else(|| {
                if matches!(
                    key.name,
                    "arrowcol.optimize.targetFileSize" | "arrowcol.writer.targetFileSize"
                ) {
                    table.get("delta.targetFileSize")
                } else {
                    None
                }
            })
            .or_else(|| process.get(key.name))
            .or(fallback)
            .map_or(key.default, String::as_str);
        values.insert(key.name.into(), key.parse(source)?);
    }
    if !raw.contains_key("arrowcol.optimize.smallFileSize")
        && (raw.contains_key("delta.targetFileSize") || raw.contains_key("arrowcol.optimize.targetFileSize"))
    {
        let target = match values.get("arrowcol.optimize.targetFileSize") {
            Some(PropValue::Unsigned(value)) => *value,
            _ => return Err(error("arrowcol.optimize.targetFileSize", "target resolution failed")),
        };
        values.insert(
            "arrowcol.optimize.smallFileSize".into(),
            PropValue::Unsigned((target / 2).max(1)),
        );
    }
    let result = ResolvedProps { values, raw };
    for (min, max) in [
        ("arrowcol.writer.minInputSize", "arrowcol.writer.maxInputSize"),
        ("arrowcol.vacuum.retention", "delta.deletedFileRetentionDuration"),
        ("arrowcol.optimize.smallFileSize", "arrowcol.optimize.targetFileSize"),
        (
            "arrowcol.optimize.targetFileSize",
            "arrowcol.optimize.maxOutputFileSize",
        ),
    ] {
        if result.unsigned(min)? > result.unsigned(max)? {
            return Err(error(min, format!("must not exceed {max}")));
        }
    }
    let min = result.float("arrowcol.writer.minParquetRatio")?;
    let max = result.float("arrowcol.writer.maxParquetRatio")?;
    let cold = result.float("arrowcol.writer.coldStartParquetRatio")?;
    if !(result.unsigned("arrowcol.writer.targetFileSize")? as f64 / min).is_finite() {
        return Err(error(
            "arrowcol.writer.minParquetRatio",
            "target divided by minimum ratio must remain finite",
        ));
    }
    if min > max || cold < min || cold > max {
        return Err(error(
            "arrowcol.writer.coldStartParquetRatio",
            "requires minParquetRatio <= coldStartParquetRatio <= maxParquetRatio",
        ));
    }
    if result.unsigned("arrowcol.optimize.minFiles")? > result.unsigned("arrowcol.optimize.maxFilesPerBin")? {
        return Err(error("arrowcol.optimize.minFiles", "must not exceed maxFilesPerBin"));
    }
    Ok(result)
}

/// Deterministic documentation generated from the same registry used at runtime.
#[must_use]
pub fn documentation() -> String {
    use std::fmt::Write as _;
    let mut doc = String::from(
        "# Table properties\n\nGenerated by `nx run arrowcol-props:generate-docs`; do not edit this table by hand.\n\nTable TBLPROPERTIES override process defaults, which override the defaults below. Unknown `arrowcol.*` keys are rejected with a suggestion. Other Delta/vendor keys are preserved. Boolean values are `true`/`false`; byte sizes accept B, KB/MB/GB and KiB/MiB/GiB; durations accept integer milliseconds or units (including Spark `interval 7 days`). Column lists are comma-separated and unique.\n\n**Safety:** PURGE removes logical data. VACUUM deletes physical files; properties never bypass its seven-day minimum or retained-tombstone safety checks. Schema-specific column validation happens when the writer opens the table.\n\n| Property | Type | Default | Validator | Scope | Since | Purpose |\n| --- | --- | --- | --- | --- | --- | --- |\n",
    );
    for key in REGISTRY {
        let _ = writeln!(
            doc,
            "| `{}` | {:?} | `{}` | `{:?}` | {:?} | {} | {} |",
            key.name, key.prop_type, key.default, key.validator, key.scope, key.since, key.doc
        );
    }
    doc.push_str("\nSee [Delta maintenance](../operations/DELTA_MAINTENANCE.md) for operation ordering, negative probes and retention safety.\n");
    doc
}

#[cfg(test)]
mod tests {
    use super::*;

    fn map(entries: &[(&str, &str)]) -> BTreeMap<String, String> {
        entries.iter().map(|(k, v)| ((*k).into(), (*v).into())).collect()
    }

    #[test]
    fn defaults_are_valid_and_names_unique() -> Result<(), PropError> {
        resolve(&BTreeMap::new(), &BTreeMap::new())?;
        let names: std::collections::BTreeSet<_> = REGISTRY.iter().map(|k| k.name).collect();
        assert_eq!(names.len(), REGISTRY.len());
        Ok(())
    }

    #[test]
    fn table_overrides_process_and_delta_target_fallback() -> Result<(), PropError> {
        let small = resolve(&map(&[("delta.targetFileSize", "32 MiB")]), &BTreeMap::new())?;
        assert_eq!(small.unsigned("arrowcol.optimize.smallFileSize")?, 16 * 1024 * 1024);
        let props = resolve(
            &map(&[("delta.targetFileSize", "256 MiB")]),
            &map(&[("arrowcol.optimize.targetFileSize", "64 MiB")]),
        )?;
        assert_eq!(props.unsigned("arrowcol.optimize.targetFileSize")?, 256 * 1024 * 1024);
        let props = resolve(
            &map(&[
                ("arrowcol.optimize.targetFileSize", "512 MiB"),
                ("delta.targetFileSize", "256 MiB"),
            ]),
            &BTreeMap::new(),
        )?;
        assert_eq!(props.unsigned("arrowcol.optimize.targetFileSize")?, 512 * 1024 * 1024);
        Ok(())
    }

    #[test]
    fn unknown_keys_suggest_and_other_vendor_keys_survive() -> Result<(), PropError> {
        let err = match validate_table_properties(&map(&[("arrowcol.optimize.minFile", "2")])) {
            Err(err) => err,
            Ok(()) => panic!("typo must fail"),
        };
        assert!(err.message.contains("arrowcol.optimize.minFiles"));
        let props = resolve(
            &map(&[("delta.enableChangeDataFeed", "true"), ("vendor.tag", "x")]),
            &BTreeMap::new(),
        )?;
        assert_eq!(props.raw.get("vendor.tag").map(String::as_str), Some("x"));
        Ok(())
    }

    #[test]
    fn reject_bad_units_overflow_nonfinite_and_retention_boundaries() {
        for (key, value) in [
            ("arrowcol.optimize.minFiles", "1"),
            ("arrowcol.optimize.targetFileSize", "18446744073709551615 GiB"),
            ("arrowcol.writer.ewmaWeight", "NaN"),
            ("arrowcol.writer.ewmaWeight", "0.5001"),
            ("arrowcol.vacuum.retention", "604799999 ms"),
            ("delta.logRetentionDuration", "interval 6 days"),
            ("arrowcol.maintenance.interval", "1 fortnight"),
            ("arrowcol.layout.sortColumns", "x,x"),
        ] {
            assert!(
                validate_table_properties(&map(&[(key, value)])).is_err(),
                "{key}={value}"
            );
        }
        assert!(validate_table_properties(&map(&[("arrowcol.vacuum.retention", "604800000 ms")])).is_ok());
        assert!(validate_table_properties(&map(&[("arrowcol.vacuum.retention", "8 days")])).is_err());
    }

    #[test]
    fn rejects_shadowed_invalid_process_default() {
        assert!(
            resolve(
                &map(&[("arrowcol.maintenance.enabled", "true")]),
                &map(&[("arrowcol.maintenance.enabled", "yes")])
            )
            .is_err()
        );
    }

    #[test]
    fn append_only_is_a_strict_boolean_without_restricting_unknown_delta_keys() -> Result<(), PropError> {
        assert!(!resolve(&BTreeMap::new(), &BTreeMap::new())?.boolean("delta.appendOnly")?);
        for value in ["true", "false"] {
            validate_table_properties(&map(&[("delta.appendOnly", value)]))?;
        }
        for value in ["not-a-boolean", "TRUE", "1", "", " true", "false ", "\ntrue\n"] {
            assert!(validate_table_properties(&map(&[("delta.appendOnly", value)])).is_err());
        }
        let passthrough = map(&[("delta.vendorExtension", "opaque"), ("vendor.tag", "x")]);
        assert_eq!(resolve(&passthrough, &BTreeMap::new())?.raw, passthrough);
        Ok(())
    }
}
