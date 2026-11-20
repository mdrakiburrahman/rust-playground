use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};

pub mod blob;
pub mod quiver;

#[derive(Default, Serialize)]
pub struct Samples {
    #[serde(skip)]
    values: Vec<u64>,
}

impl Samples {
    pub fn record(&mut self, value: Duration) {
        self.values.push(value.as_nanos().min(u64::MAX as u128) as u64);
    }

    pub fn summary(&mut self) -> serde_json::Value {
        self.values.sort_unstable();
        let percentile = |numerator: usize, denominator: usize| {
            let rank = (self.values.len() * numerator).div_ceil(denominator);
            rank.checked_sub(1).map(|i| self.values[i] as f64 / 1_000_000.0)
        };
        serde_json::json!({
            "samples": self.values.len(),
            "p50_ms": percentile(50, 100),
            "p99_ms": percentile(99, 100),
            "p999_ms": percentile(999, 1000),
            "p999_has_1000_samples": self.values.len() >= 1000,
            "max_ms": self.values.last().map(|n| *n as f64 / 1_000_000.0)
        })
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Receipt {
    pub run: String,
    pub ids: Vec<i64>,
}

pub struct Oracle {
    file: File,
    run: String,
}

impl Oracle {
    pub fn create(path: &Path, run: String) -> Result<Self> {
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(path)
            .with_context(|| format!("create new oracle {}", path.display()))?;
        if let Some(parent) = path.parent() {
            File::open(parent)?.sync_all()?;
        }
        Ok(Self { file, run })
    }

    pub fn record(&mut self, ids: Vec<i64>) -> Result<()> {
        serde_json::to_writer(
            &mut self.file,
            &Receipt {
                run: self.run.clone(),
                ids,
            },
        )?;
        self.file.write_all(b"\n")?;
        self.file.sync_all()?;
        Ok(())
    }
}

pub fn read_receipts(bytes: &[u8]) -> Result<Vec<Receipt>> {
    let mut records = Vec::new();
    for line in bytes.split_inclusive(|byte| *byte == b'\n') {
        if !line.ends_with(b"\n") {
            break;
        }
        let receipt: Receipt = serde_json::from_slice(line).context("invalid complete oracle record")?;
        ensure!(
            !receipt.ids.is_empty(),
            "empty receipt cannot establish an acknowledged write"
        );
        records.push(receipt);
    }
    ensure!(!records.is_empty(), "oracle contains no complete receipts");
    Ok(records)
}

pub fn compare(receipts: &[Receipt], observed: &[i64], run: &str) -> Result<serde_json::Value> {
    ensure!(!receipts.is_empty(), "empty oracle");
    ensure!(receipts.iter().all(|r| r.run == run), "oracle run identity mismatch");
    let expected: BTreeSet<_> = receipts.iter().flat_map(|r| r.ids.iter().copied()).collect();
    ensure!(!expected.is_empty(), "oracle contains no acknowledged IDs");
    let mut counts = BTreeMap::new();
    for id in observed {
        *counts.entry(*id).or_insert(0_u64) += 1;
    }
    let missing: Vec<_> = expected.iter().filter(|id| !counts.contains_key(id)).copied().collect();
    Ok(serde_json::json!({
        "run": run,
        "acknowledged": expected.len(),
        "observed": observed.len(),
        "duplicates": counts.values().map(|count| count - 1).sum::<u64>(),
        "missing_count": missing.len(),
        "missing_ids": missing,
        "passed": missing.is_empty()
    }))
}

#[cfg(unix)]
pub fn cpu_seconds() -> Option<f64> {
    let mut usage = std::mem::MaybeUninit::<libc::rusage>::uninit();
    // getrusage initializes the complete structure on success.
    if unsafe { libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr()) } != 0 {
        return None;
    }
    let usage = unsafe { usage.assume_init() };
    Some(
        usage.ru_utime.tv_sec as f64
            + usage.ru_stime.tv_sec as f64
            + (usage.ru_utime.tv_usec + usage.ru_stime.tv_usec) as f64 / 1_000_000.0,
    )
}

#[cfg(not(unix))]
pub fn cpu_seconds() -> Option<f64> {
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn oracle_ignores_only_incomplete_tail() {
        let receipts = read_receipts(b"{\"run\":\"r\",\"ids\":[1,2]}\n{\"run\":").unwrap();
        assert_eq!(receipts[0].ids, vec![1, 2]);
        assert!(read_receipts(b"broken\n").is_err());
        assert!(read_receipts(b"{\"run\":\"r\",\"ids\":[]}\n").is_err());
        assert!(read_receipts(b"{\"run\":").is_err());
    }

    #[test]
    fn loss_is_detected_and_duplicates_are_allowed() {
        let receipts = vec![Receipt {
            run: "r".into(),
            ids: vec![1, 2],
        }];
        assert_eq!(compare(&receipts, &[1, 1, 2, 3], "r").unwrap()["passed"], true);
        let result = compare(&receipts, &[2], "r").unwrap();
        assert_eq!(result["missing_ids"], serde_json::json!([1]));
        assert_eq!(result["passed"], false);
        assert!(compare(&receipts, &[1, 2], "other").is_err());
        assert!(compare(&[], &[], "r").is_err());
    }

    #[test]
    fn oracle_never_overwrites_an_existing_run() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("oracle.jsonl");
        let mut oracle = Oracle::create(&path, "r".into()).unwrap();
        oracle.record(vec![1]).unwrap();
        assert!(Oracle::create(&path, "new".into()).is_err());
        assert_eq!(read_receipts(&std::fs::read(path).unwrap()).unwrap()[0].run, "r");
    }
}
