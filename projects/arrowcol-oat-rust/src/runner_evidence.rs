#[derive(Clone, Debug, Default, serde::Serialize)]
struct CellEvidence {
    downstream_comment: Option<&'static str>,
    resource_samples: Vec<ResourceSample>,
    resource_sampling_failures: u64,
}

#[derive(Clone, Debug, serde::Serialize)]
struct ResourceSample {
    sampled_at_unix_ms: u64,
    metrics: ResourceMetrics,
}

fn local_downstream_optional(kind: crate::config::OatMode) -> bool {
    kind == crate::config::OatMode::Local
}

impl CellEvidence {
    fn downstream_unavailable(&mut self, reason: &'static str) {
        self.downstream_comment.get_or_insert(reason);
        tracing::warn!(reason, "downstream observation unavailable; preserving local collector evidence");
    }

    async fn sample_resources(&mut self, platform: &dyn Platform, config: &OatConfig, start: u64, end: u64) {
        let Ok(now) = unix_time_ms_u64() else {
            self.resource_sampling_failures += 1;
            return;
        };
        if now < start || now >= end {
            return;
        }
        match platform.sample_resources(config).await {
            Ok(metrics) => match unix_time_ms_u64() {
                Ok(sampled_at_unix_ms) if sampled_at_unix_ms < end => {
                    self.resource_samples.push(ResourceSample { sampled_at_unix_ms, metrics });
                }
                _ => self.resource_sampling_failures += 1,
            },
            Err(_) => {
                self.resource_sampling_failures += 1;
                tracing::warn!("resource sample unavailable");
            }
        }
    }

    fn resource_summary(&self) -> Option<ResourceMetrics> {
        if self.resource_samples.is_empty() {
            return None;
        }
        Some(ResourceMetrics {
            cpu_millicores: self.resource_samples.iter().map(|sample| sample.metrics.cpu_millicores).sum::<f64>()
                / self.resource_samples.len() as f64,
            memory_bytes: self.resource_samples.iter().map(|sample| sample.metrics.memory_bytes).max()?,
        })
    }

    fn for_table(&self, client: Option<&ClientMetrics>) -> Value {
        json!({
            "version": 1,
            "downstream_status": if self.downstream_comment.is_some() { "unavailable" } else { "observed_or_not_attempted" },
            "downstream_comment": self.downstream_comment,
            "resources": {
                "scope": "collector_fleet_measurement_window_only",
                "cpu_summary": "arithmetic_mean_of_observed_samples",
                "memory_summary": "maximum_observed_sample_not_peak_rss",
                "samples": self.resource_samples,
                "sampling_failures": self.resource_sampling_failures,
                "unavailable_reason": self.resource_samples.is_empty().then_some("no_measurement_window_resource_samples"),
            },
            "rows": {
                "attempted": null,
                "sent": null,
                "client_completed": client.map(|metrics| metrics.rows),
                "wal_acked": null,
                "delta_committed": null,
                "unique_delivered": null,
                "unavailable_reason": "no_cell_scoped_commit_or_unique_delivery_evidence",
                "retry_semantics": "upload_completion_does_not_establish_exactly_once_delivery",
            },
            "durable_files": null,
            "durable_bytes": null,
            "durable_bytes_per_row": null,
            "compression_ratio": null,
            "compression_ratio_denominator": null,
            "stage_latency_percentiles": null,
            "backlog": null,
        })
    }
}

#[cfg(test)]
mod measurement_evidence_tests {
    use super::*;

    #[test]
    fn downstream_degradation_is_local_only() {
        assert!(local_downstream_optional(crate::config::OatMode::Local));
        assert!(!local_downstream_optional(crate::config::OatMode::Aks));
        let mut evidence = CellEvidence::default();
        evidence.downstream_unavailable("local_qap_initialization_unavailable");
        let public = evidence.for_table(None);
        assert_eq!(public["downstream_status"], "unavailable");
        assert!(public["rows"]["delta_committed"].is_null());
        assert!(public["compression_ratio"].is_null());
    }

    #[test]
    fn resource_summary_is_observed_mean_and_max_not_an_end_snapshot() {
        let mut evidence = CellEvidence::default();
        assert!(evidence.resource_summary().is_none());
        for (cpu_millicores, memory_bytes) in [(100.0, 200), (300.0, 100)] {
            evidence.resource_samples.push(ResourceSample {
                sampled_at_unix_ms: 1,
                metrics: ResourceMetrics { cpu_millicores, memory_bytes },
            });
        }
        let metrics = evidence.resource_summary().unwrap();
        assert_eq!(metrics.cpu_millicores, 200.0);
        assert_eq!(metrics.memory_bytes, 200);
    }
}
