fn wal_process_configuration(text: &str) -> Result<Option<Value>> {
    let mut latest: Option<Value> = None;
    for line in text
        .lines()
        .filter_map(|line| line.strip_prefix("arrowcol_wal_effective_config "))
    {
        let value: Value = serde_json::from_str(line).context("invalid WAL configuration record")?;
        match value["kind"].as_str() {
            Some("instance") => continue,
            Some("aggregate") => {}
            _ => bail!("unknown WAL configuration record kind"),
        }
        let generation = value["deployment_generation"]
            .as_u64()
            .context("missing WAL generation")?;
        let instances = value["instances"]
            .as_array()
            .context("missing WAL instance aggregate")?;
        if instances.is_empty() || value["instance_count"].as_u64() != Some(instances.len() as u64) {
            bail!("incomplete WAL instance aggregate");
        }
        if let Some(previous) = &latest {
            if previous["deployment_generation"] != generation {
                bail!("WAL deployment generation changed during the cell");
            }
            for old in previous["instances"]
                .as_array()
                .context("invalid previous WAL aggregate")?
            {
                if !instances.iter().any(|instance| instance == old) {
                    bail!("WAL effective instance settings changed during the cell");
                }
            }
        }
        latest = Some(value);
    }
    let Some(mut aggregate) = latest else {
        return Ok(None);
    };
    let instances = aggregate["instances"].as_array().context("missing WAL instances")?;
    let count = instances.len() as u64;
    let mut cores = HashSet::new();
    let mut weakest = 0;
    for instance in instances {
        let core = instance["core_id"].as_u64().context("WAL core ID missing")?;
        if core >= count
            || !cores.insert(core)
            || instance["num_cores"].as_u64() != Some(count)
            || instance["deployment_generation"] != aggregate["deployment_generation"]
        {
            bail!("WAL aggregate does not cover every effective processor core");
        }
        let flush = wal_nanos(instance, "wal_flush_interval_ns")?;
        if instance.get("max_age_ns").is_none() {
            bail!("WAL retention age readback missing");
        }
        let lossy = !instance["max_age_ns"].is_null() || instance["size_cap_policy"] == "drop_oldest";
        if !matches!(
            instance["size_cap_policy"].as_str(),
            Some("backpressure" | "drop_oldest")
        ) {
            bail!("unknown WAL retention policy");
        }
        if !instance["max_age_ns"].is_null() {
            wal_nanos(instance, "max_age_ns")?;
        }
        let strength = if lossy {
            2
        } else if flush > 0 {
            1
        } else {
            0
        };
        let class = ["durable", "fsync_batched", "lossy_eviction"][strength];
        if instance["durability_class"] != class {
            bail!("WAL instance class contradicts effective settings");
        }
        weakest = weakest.max(strength);
    }
    if aggregate["durability_class"] != ["durable", "fsync_batched", "lossy_eviction"][weakest] {
        bail!("WAL aggregate class is not the weakest effective instance class");
    }
    if let Some(instances) = aggregate["instances"].as_array_mut() {
        for instance in instances {
            instance
                .as_object_mut()
                .context("WAL instance must be an object")?
                .remove("instance_id");
        }
    }
    Ok(Some(aggregate))
}

fn wal_nanos(instance: &Value, field: &str) -> Result<u128> {
    let value = instance[field]
        .as_str()
        .context("WAL duration must be decimal nanoseconds")?;
    if value.is_empty() || !value.bytes().all(|byte| byte.is_ascii_digit()) {
        bail!("WAL duration must be unsigned decimal nanoseconds");
    }
    value.parse().context("WAL nanosecond duration exceeds u128")
}

fn verify_wal_configuration(config: &OatConfig, observed: &Value) -> Result<()> {
    let Some(settings) = &config.tunables.wal else {
        if observed.get("wal").is_some() {
            bail!("WAL consumer appeared without explicit typed topology authorization");
        }
        return Ok(());
    };
    let instances = observed
        .pointer("/wal/instances")
        .and_then(Value::as_array)
        .context("configured WAL has no complete consumer aggregate readback")?;
    let milliseconds = |value: Option<u64>| value.map(|value| json!((u128::from(value) * 1_000_000).to_string()));
    let requested = [
        ("wal_flush_interval_ns", milliseconds(settings.flush_interval_ms)),
        (
            "retention_size_cap_bytes",
            settings.retention_size_cap_bytes.map(|value| json!(value)),
        ),
        (
            "max_age_ns",
            settings
                .max_age_secs
                .map(|value| json!((u128::from(value) * 1_000_000_000).to_string())),
        ),
        (
            "size_cap_policy",
            settings.size_cap_policy.map(serde_json::to_value).transpose()?,
        ),
        (
            "max_segment_open_duration_ns",
            milliseconds(settings.max_segment_open_duration_ms),
        ),
        ("poll_interval_ns", milliseconds(settings.poll_interval_ms)),
        (
            "initial_retry_interval_ns",
            milliseconds(settings.initial_retry_interval_ms),
        ),
        ("max_retry_interval_ns", milliseconds(settings.max_retry_interval_ms)),
        ("retry_multiplier", settings.retry_multiplier.map(|value| json!(value))),
        ("max_in_flight", settings.max_in_flight.map(|value| json!(value))),
        (
            "otlp_handling",
            settings.otlp_handling.map(serde_json::to_value).transpose()?,
        ),
    ];
    for instance in instances {
        for (field, expected) in &requested {
            if let Some(expected) = expected
                && instance.get(*field) != Some(expected)
            {
                bail!("WAL consumer readback does not match configured {field}");
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod wal_readback_tests {
    use super::*;

    fn aggregate(flush: &str) -> Value {
        json!({"kind":"aggregate","deployment_generation":1,"instance_count":1,"durability_class":"fsync_batched",
            "instances":[{"instance_id":"private/runtime/path","deployment_generation":1,"core_id":0,"num_cores":1,
                "wal_flush_interval_ns":flush,"max_age_ns":null,"size_cap_policy":"backpressure",
                "retention_size_cap_bytes":4294967296_u64,"durability_class":"fsync_batched"}]})
    }

    #[test]
    fn validates_actual_wal_units_full_core_coverage_and_weakest_durability() -> Result<()> {
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        config.tunables = serde_json::from_value(json!({"wal":{"flush_interval_ms":7}}))?;
        let record = aggregate("7000000");
        let text = format!("arrowcol_wal_effective_config {record}\n");
        let observed = process_configurations(&text)?;
        verify_process_configuration(&config, ComponentRole::Collector, &observed)?;
        assert_eq!(observed["wal"]["durability_class"], "fsync_batched");
        assert!(!observed.to_string().contains("private/runtime/path"));
        let wrong = process_configurations(&format!("arrowcol_wal_effective_config {}\n", aggregate("8000000")))?;
        assert!(verify_process_configuration(&config, ComponentRole::Collector, &wrong).is_err());
        assert!(verify_process_configuration(&config, ComponentRole::Collector, &json!({})).is_err());
        let mut invalid = record.clone();
        invalid["instances"][0]["num_cores"] = json!(2);
        assert!(wal_process_configuration(&format!("arrowcol_wal_effective_config {invalid}\n")).is_err());
        invalid = record;
        invalid["instances"][0]["max_age_ns"] = json!("1000000");
        assert!(wal_process_configuration(&format!("arrowcol_wal_effective_config {invalid}\n")).is_err());
        config.tunables = Default::default();
        assert!(verify_process_configuration(&config, ComponentRole::Collector, &observed).is_err());
        Ok(())
    }
}
