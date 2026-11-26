use anyhow::{Context, Result, bail};
use serde::Deserialize;
use serde_json::value::RawValue;
use std::io::{self, Read, Write};

const MAX_EVIDENCE: u64 = 16 * 1024 * 1024;
const MAX_MANIFEST: usize = 1024 * 1024;
const MAX_CLIENTS: usize = 1024;

#[derive(Deserialize)]
struct Evidence<'a> {
    #[serde(borrow)]
    client_phase_reports: Vec<Report<'a>>,
}

#[derive(Deserialize)]
struct Report<'a> {
    #[serde(borrow)]
    phase_counters: Phase<'a>,
}

#[derive(Deserialize)]
struct Phase<'a> {
    #[serde(borrow)]
    payload_manifest: &'a RawValue,
}

fn extract<'a>(input: &'a [u8]) -> Result<Vec<&'a str>> {
    if input.is_empty() || input.len() as u64 > MAX_EVIDENCE {
        bail!("invalid evidence size");
    }
    let evidence: Evidence<'_> = serde_json::from_slice(input).context("invalid client reports")?;
    if evidence.client_phase_reports.is_empty() || evidence.client_phase_reports.len() > MAX_CLIENTS {
        bail!("invalid client report count");
    }
    evidence
        .client_phase_reports
        .iter()
        .map(|report| {
            let manifest: &'a RawValue = report.phase_counters.payload_manifest;
            let raw = manifest.get();
            if !raw.starts_with('{') || raw.len() > MAX_MANIFEST {
                bail!("invalid terminal manifest size or type");
            }
            Ok(raw)
        })
        .collect()
}

fn main() -> Result<()> {
    let mut input = Vec::new();
    io::stdin().lock().take(MAX_EVIDENCE + 1).read_to_end(&mut input)?;
    let manifests = extract(&input)?;
    let stdout = io::stdout();
    let mut out = stdout.lock();
    for manifest in manifests {
        writeln!(out, "{}", manifest.len())?;
        out.write_all(manifest.as_bytes())?;
    }
    out.flush()?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_manifest_slices_and_rejects_ambiguous_evidence() -> Result<()> {
        let first = r#"{ "n":18446744073709551615, "s":"} \" [" }"#;
        let second = r#"{"n":9007199254740993}"#;
        let evidence = format!(
            r#"{{"client_phase_reports":[{{"phase_counters":{{"payload_manifest":{first}}}}},{{"phase_counters":{{"payload_manifest":{second}}}}}]}}"#
        );
        assert_eq!(extract(evidence.as_bytes())?, [first, second]);
        let input = br#"{"client_phase_reports":[{"phase_counters":{"payload_manifest":{"nanoseconds":1795385557123456789}}}]}"#;
        assert_eq!(extract(input)?, [r#"{"nanoseconds":1795385557123456789}"#]);
        assert!(extract(br#"{"client_phase_reports":[]}"#).is_err());
        assert!(
            extract(br#"{"client_phase_reports":[{"phase_counters":{"payload_manifest":{},"payload_manifest":{}}}]}"#)
                .is_err()
        );
        assert!(extract(br#"{"client_phase_reports":[{"phase_counters":{"payload_manifest":null}}]}"#).is_err());
        assert!(extract(br#"{"client_phase_reports":[{"phase_counters":{}}]}"#).is_err());
        let report = r#"{"phase_counters":{"payload_manifest":{}}}"#;
        let too_many = format!(
            r#"{{"client_phase_reports":[{}]}}"#,
            vec![report; MAX_CLIENTS + 1].join(",")
        );
        assert!(extract(too_many.as_bytes()).is_err());
        Ok(())
    }
}
