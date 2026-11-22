use std::io::{self, Read};

use anyhow::{Context, Result, bail};
use serde_json::Value;

const MAX_EVIDENCE_BYTES: u64 = 16 * 1024 * 1024;
const MAX_MANIFEST_BYTES: usize = 1024 * 1024;

fn extract(input: &[u8]) -> Result<Vec<u8>> {
    let evidence: Value = serde_json::from_slice(input).context("invalid OAT evidence JSON")?;
    let reports = evidence["client_phase_reports"]
        .as_array()
        .context("missing client reports")?;
    if reports.len() != 1 {
        bail!("expected exactly one client report");
    }
    let manifest = reports[0]["phase_counters"]["payload_manifest"]
        .as_object()
        .context("missing terminal payload manifest")?;
    let result = serde_json::to_vec(manifest)?;
    if result.len() > MAX_MANIFEST_BYTES {
        bail!("terminal payload manifest exceeds byte limit");
    }
    Ok(result)
}

fn main() -> Result<()> {
    let mut input = Vec::new();
    io::stdin()
        .lock()
        .take(MAX_EVIDENCE_BYTES + 1)
        .read_to_end(&mut input)?;
    if input.len() as u64 > MAX_EVIDENCE_BYTES {
        bail!("OAT evidence exceeds byte limit");
    }
    let output = extract(&input)?;
    io::Write::write_all(&mut io::stdout().lock(), &output)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_large_integer_values_and_requires_one_manifest() {
        let input = br#"{"client_phase_reports":[{"phase_counters":{"payload_manifest":{"nanoseconds":1795385557123456789}}}]}"#;
        let output = extract(input).unwrap();
        assert_eq!(output, br#"{"nanoseconds":1795385557123456789}"#);
        assert!(extract(br#"{"client_phase_reports":[]}"#).is_err());
        assert!(extract(br#"{"client_phase_reports":[{},{}]}"#).is_err());
        assert!(extract(br#"{"client_phase_reports":[{}]}"#).is_err());
    }
}
