#[cfg(test)]
mod failure_evidence_tests {
    use super::*;

    #[test]
    fn ownership_transport_failure_is_not_a_confirmed_pinning_violation() {
        let report = failed_client_pinning_report(
            &anyhow::anyhow!("DMV source connection failed"),
            Err(anyhow::anyhow!("metrics transport unavailable")),
            &["sqlserver_wait_stats".to_owned()],
        );
        assert!(
            report.as_ref().is_none_or(|report| report.violation.is_none()),
            "an unavailable observation is not evidence that ownership changed"
        );
    }

    #[test]
    fn failure_measurements_have_a_nullable_parquet_representation() -> Result<()> {
        let bytes = encode_parquet(&parquet_batch(&[])?)?;
        let reader = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(bytes))?;
        for field in ["rows", "duration_ms", "rows_per_second"] {
            assert!(
                reader.schema().field_with_name(field)?.is_nullable(),
                "unknown {field} must not require a fabricated zero"
            );
        }
        Ok(())
    }
}
