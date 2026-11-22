fn parity_has_override(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Object(fields) => fields.values().any(parity_has_override),
        Value::Array(values) => !values.is_empty(),
        _ => true,
    }
}

fn parity_admission_readback(config: &OatConfig, logs: &[ProcessLog], clients: &[ClientProgress]) -> Result<Value> {
    if config.platform.kind != crate::config::OatMode::Local
        || config.tables.len() != 1
        || config.clients_per_table != 1
        || clients.len() != 1
    {
        bail!("parity admission requires one local table and client");
    }
    let [collector] = logs else {
        bail!("parity admission requires exactly one collector");
    };
    if !matches!(collector.role, ComponentRole::Collector) || !collector.complete || collector.image_id.is_none() {
        bail!("complete collector identity and startup readback are required before admission");
    }
    let collector_config = process_configurations(&collector.text)?;
    verify_process_configuration(config, ComponentRole::Collector, &collector_config)?;
    let writer = collector_config
        .get("parquet_writer")
        .filter(|value| value.is_object())
        .context("parity collector writer readback is missing")?;
    if parity_has_override(writer) {
        bail!("parity collector has a Parquet writer override");
    }
    let [client] = clients else {
        bail!("parity admission requires exactly one client");
    };
    if !client.ready || client.completed || client.failure.is_some() {
        bail!("parity client was not ready before admission");
    }
    let client_config = process_configurations(&client.output)?;
    let resolved_config = resolved_fleet_client_config(config, 0)?;
    verify_process_configuration(&resolved_config, ComponentRole::Client, &client_config)?;
    Ok(json!({
        "status": "verified_before_schedule",
        "observed_at_unix_ms": unix_time_ms_u64()?,
        "collector_image_id": collector.image_id,
        "collector": collector_config,
        "client": client_config,
    }))
}

#[cfg(test)]
mod parity_admission_tests {
    use super::*;

    #[test]
    fn writer_override_check_distinguishes_empty_policy_from_values() {
        assert!(!parity_has_override(&json!({"defaults":{"compression":null}, "columns":{}})));
        assert!(parity_has_override(&json!({"defaults":{"compression":"zstd"}})));
        assert!(parity_has_override(&json!({"columns":{"field":{"dictionary_enabled":false}}})));
    }
}
