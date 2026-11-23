use arrowcol_tunables::ComponentRole;
use otel_arrow_dfe_config::engine::EngineConfig;

fn tunable_leaf_evidence(config: &OatConfig) -> Result<Value> {
    config.tunables.classify_oat_leaves()
}

fn same_column_membership(requested: &Value, observed: &Value) -> bool {
    fn columns(value: &Value) -> Option<std::collections::BTreeSet<&str>> {
        let array = value.as_array()?;
        let columns = array.iter().map(Value::as_str)
            .collect::<Option<std::collections::BTreeSet<_>>>()?;
        (columns.len() == array.len()).then_some(columns)
    }
    match (columns(requested), columns(observed)) {
        (Some(requested), Some(observed)) => requested == observed,
        _ => false,
    }
}

fn verified_processes<'a>(
    config: &OatConfig, processes: Option<&'a Value>, role: ComponentRole, replicas: u32,
) -> Option<Vec<&'a Value>> {
    let processes = processes?.as_array()?.iter()
        .filter(|process| process["component"] == role_name(role))
        .collect::<Vec<_>>();
    let identities = processes.iter()
        .filter_map(|process| process["instance_id"].as_str().filter(|id| !id.is_empty()))
        .collect::<std::collections::BTreeSet<_>>();
    if replicas == 0 || processes.len() != replicas as usize || identities.len() != processes.len()
        || processes.iter().any(|process| {
            process["log_complete"] != true
                || process["startup_tunables_verified"] != true
                || process["effective_config_source"] != "consumer_startup_readback"
                || {
                    let mut observed = process["effective_config"].clone();
                    if matches!(role, ComponentRole::Collector)
                        && let Some(fields) = observed.as_object_mut()
                        && !process["writer_policy_overrides"].is_null()
                    {
                        fields.insert("parquet_writer".to_owned(), process["writer_policy_overrides"].clone());
                    }
                    verify_process_configuration(config, role, &observed).is_err()
                }
        }) {
        return None;
    }
    Some(processes)
}

fn promote_collector_readback(config: &OatConfig, evidence: &mut Value, processes: Option<&Value>, replicas: u32) -> Result<()> {
    let Some(collectors) = verified_processes(config, processes, ComponentRole::Collector, replicas) else {
        return Ok(());
    };
    let environment = role_environment(config, ComponentRole::Collector)?;
    let parquet_policy: Option<Value> = environment.get("ARROWCOL_PARQUET_CONFIG")
        .map(|value| serde_json::from_str(value)).transpose()?;
    let layout_policy = config.tunables.collector.as_ref().and_then(|collector| collector.layout.as_ref())
        .map(serde_json::to_value).transpose()?;
    let expected_tables = config.tables.iter().map(|table| canonical_dmv_table_name(table))
        .collect::<Result<std::collections::BTreeSet<_>>>()?;
    let table_policies = collectors.iter()
        .flat_map(|process| process["layout_table_policies"].as_array().into_iter().flatten())
        .collect::<Vec<_>>();
    let observed_tables = table_policies.iter()
        .filter_map(|policy| policy["table_name"].as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let tables_covered = !expected_tables.is_empty()
        && collectors.iter().all(|process| process["layout_table_policies"].is_array())
        && expected_tables.iter().all(|table| observed_tables.contains(table.as_str()))
        && table_policies.iter().all(|policy| {
            policy["table_name"].as_str().is_some_and(|table| expected_tables.contains(table))
                && policy["scope"] == "table_effective_at_file_construction_not_full_run_history"
        });
    let adaptive_policies = collectors.iter()
        .flat_map(|process| process["adaptive_table_policies"].as_array().into_iter().flatten())
        .collect::<Vec<_>>();
    let adaptive_tables = adaptive_policies.iter()
        .filter_map(|policy| policy["table_name"].as_str())
        .collect::<std::collections::BTreeSet<_>>();
    let adaptive_covered = !expected_tables.is_empty()
        && collectors.iter().all(|process| process["adaptive_table_policies"].is_array())
        && expected_tables.iter().all(|table| adaptive_tables.contains(table.as_str()))
        && adaptive_policies.iter().all(|policy| {
            policy["table_name"].as_str().is_some_and(|table| expected_tables.contains(table))
                && policy["scope"] == "installed_estimator_not_full_run_history"
        });
    let projected = ["ARROWCOL_COLLECTOR_COMMIT_CONFIG", "ARROWCOL_COLLECTOR_PRESSURE_CONFIG",
        "ARROWCOL_COLLECTOR_BATCHING_CONFIG"]
        .into_iter()
        .filter_map(|name| environment.get(name).map(|value| (name, value)))
        .map(|(name, value)| Ok((name, serde_json::from_str::<Value>(value)?)))
        .collect::<Result<std::collections::BTreeMap<_, _>>>()?;
    let projection = evidence["projection_only"]
        .as_array_mut()
        .context("projection_only must be an array")?;
    let mut promoted = Vec::new();
    projection.retain(|entry| {
        let Some(path) = entry["path"].as_str() else {
            return true;
        };
        if let Some(field) = path.strip_prefix("collector.batching.adaptive_file.")
            && matches!(field, "target_parquet_bytes" | "min_input_bytes" | "max_input_bytes"
                | "cold_start_parquet_ratio" | "ewma_weight" | "min_parquet_ratio" | "max_parquet_ratio")
        {
            let requested = projected.get("ARROWCOL_COLLECTOR_BATCHING_CONFIG")
                .and_then(|value| value["adaptive_file"].get(field))
                .filter(|value| !value.is_null());
            if adaptive_covered && requested.is_some()
                && adaptive_policies.iter().all(|policy| policy["config"].get(field) == requested)
            {
                promoted.push(json!({"path": path, "reason": "consumer_table_adaptive_readback"}));
                return false;
            }
            return true;
        }
        if let Some(field) = path.strip_prefix("collector.layout.")
            && matches!(
                field,
                "enabled"
                    | "sort_execution"
                    | "sort_columns"
                    | "bloom_columns"
                    | "max_rows"
                    | "max_input_bytes"
                    | "row_group_rows"
                    | "page_rows"
                    | "detach_ratio"
            )
        {
            let requested = layout_policy.as_ref().and_then(|policy| policy.get(field))
                .filter(|value| !value.is_null());
            if tables_covered && requested.is_some()
                && table_policies.iter().all(|record| {
                    (field == "enabled" || record["config"]["enabled"] == true)
                        && if field == "bloom_columns" {
                            requested.zip(record["config"].get(field))
                                .is_some_and(|(requested, observed)| same_column_membership(requested, observed))
                        } else {
                            record["config"].get(field) == requested
                        }
                })
            {
                promoted.push(json!({"path": path, "reason": "consumer_table_writer_readback"}));
                return false;
            }
            return true;
        }
        if let Some(field) = path.strip_prefix("parquet_writer.")
            && matches!(field, "writer_version" | "max_row_group_bytes" | "write_batch_size"
                | "offset_index_enabled" | "column_index_truncate_length" | "statistics_truncate_length")
        {
            let requested = parquet_policy.as_ref().and_then(|policy| policy.get(field))
                .filter(|value| !value.is_null());
            if tables_covered && requested.is_some()
                && table_policies.iter().all(|record| {
                    record["parquet_properties"].get(field) == requested
                        && (!matches!(field, "column_index_truncate_length" | "statistics_truncate_length")
                            || record["parquet_column_options"].as_object().is_some_and(|columns| {
                                columns.values().any(|column| {
                                    if field == "column_index_truncate_length" {
                                        column["statistics"] == "page"
                                    } else {
                                        matches!(column["statistics"].as_str(), Some("chunk" | "page"))
                                    }
                                })
                            }))
                })
            {
                promoted.push(json!({"path": path, "reason": "consumer_table_writer_readback"}));
                return false;
            }
            return true;
        }
        let column_option = path.strip_prefix("parquet_writer.defaults.")
            .map(|field| (None, field))
            .or_else(|| path.strip_prefix("parquet_writer.columns.").and_then(|path| {
                path.rsplit_once('.').map(|(column, field)| (Some(column), field))
            }));
        if let Some((column, field)) = column_option
            && matches!(field, "encoding" | "dictionary_enabled" | "dictionary_page_size_limit"
                | "data_page_size_limit" | "statistics" | "write_page_header_statistics"
                | "bloom_filter_enabled" | "bloom_filter_fpp" | "bloom_filter_ndv"
                | "data_page_v2_compression_ratio_threshold")
        {
            let Some(policy) = parquet_policy.as_ref() else { return true; };
            let options = if let Some(column) = column {
                policy.get("columns").and_then(|columns| columns.get(column))
            } else {
                policy.get("defaults")
            };
            let Some(requested) = options.and_then(|options| options.get(field)).filter(|value| !value.is_null()) else {
                return true;
            };
            let matches = tables_covered && table_policies.iter().all(|record| {
                let Some(columns) = record["parquet_column_options"].as_object().filter(|columns| !columns.is_empty()) else {
                    return false;
                };
                let applicable = |name: &str, actual: &Value| {
                    (field != "dictionary_page_size_limit" || actual["dictionary_enabled"] == true)
                        && (field != "write_page_header_statistics" || actual["statistics"] == "page")
                        && (!matches!(field, "bloom_filter_fpp" | "bloom_filter_ndv") || actual["bloom_filter_enabled"] == true)
                        && (field != "data_page_v2_compression_ratio_threshold"
                            || (record["parquet_properties"]["writer_version"] == "v2"
                                && record["parquet_columns"][name]["compression"].as_str()
                                    .is_some_and(|codec| codec != "uncompressed")))
                };
                if let Some(column) = column {
                    columns.get(column).is_some_and(|actual| applicable(column, actual) && actual.get(field) == Some(requested))
                } else {
                    let effective = columns.iter().filter(|(name, actual)| {
                        applicable(name, actual) && policy.get("columns").and_then(|columns| columns.get(*name))
                            .and_then(|options| options.get(field)).is_none_or(Value::is_null)
                    }).collect::<Vec<_>>();
                    !effective.is_empty() && effective.iter().all(|(_, actual)| actual.get(field) == Some(requested))
                }
            });
            if matches {
                promoted.push(json!({"path": path, "reason": "consumer_table_writer_readback"}));
                return false;
            }
            return true;
        }
        let codec = if path == "parquet_writer.defaults.compression" {
            Some((None, "compression"))
        } else if path == "parquet_writer.defaults.compression_level" {
            Some((None, "compression_level"))
        } else if let Some(column) = path.strip_prefix("parquet_writer.columns.") {
            column.strip_suffix(".compression_level").map(|column| (Some(column), "compression_level"))
                .or_else(|| column.strip_suffix(".compression").map(|column| (Some(column), "compression")))
        } else {
            None
        };
        if let Some((column, field)) = codec {
            let Some(policy) = parquet_policy.as_ref() else { return true; };
            let options = if let Some(column) = column {
                policy.get("columns").and_then(|columns| columns.get(column))
            } else {
                policy.get("defaults")
            };
            let Some(requested) = options.and_then(|options| options.get(field)).filter(|value| !value.is_null()) else {
                return true;
            };
            let Some(requested_codec) = options.and_then(|options| options.get("compression")).filter(|value| value.is_string()) else {
                return true;
            };
            let matches = tables_covered && table_policies.iter().all(|record| {
                let Some(columns) = record["parquet_columns"].as_object().filter(|columns| !columns.is_empty()) else {
                    return false;
                };
                let agrees = |actual: &Value| {
                    actual.get(field) == Some(requested) && actual.get("compression") == Some(requested_codec)
                };
                if let Some(column) = column {
                    columns.get(column).is_some_and(agrees)
                } else {
                    let effective = columns.iter().filter(|(name, _)| {
                        policy.get("columns").and_then(|columns| columns.get(*name))
                            .and_then(|options| options.get("compression")).is_none_or(Value::is_null)
                    }).collect::<Vec<_>>();
                    !effective.is_empty() && effective.iter().all(|(_, actual)| agrees(actual))
                }
            });
            if matches {
                promoted.push(json!({"path": path, "reason": "consumer_table_writer_readback"}));
                return false;
            }
            return true;
        }
        let parquet_field = match path {
            "parquet_writer.max_row_group_rows" => Some(("max_row_group_rows", "row_group_rows")),
            "parquet_writer.data_page_row_count_limit" => Some(("data_page_row_count_limit", "page_rows")),
            _ => None,
        };
        if let Some((requested_field, observed_field)) = parquet_field {
            let Some(requested) = parquet_policy.as_ref().and_then(|policy| policy.get(requested_field))
                .filter(|value| value.as_u64().is_some_and(|number| number > 0)) else {
                return true;
            };
            if tables_covered
                && table_policies.iter().all(|policy| {
                    policy["config"]["enabled"] == true
                        && policy["config"][observed_field] == *requested
                })
            {
                promoted.push(json!({"path": path, "reason": "consumer_table_writer_readback"}));
                return false;
            }
            return true;
        }
        let runtime = match path {
            "collector.runtime.timestamp_kernel" => Some(("timestamp_kernel", "ARROWCOL_COLLECTOR_TIMESTAMP_KERNEL")),
            "collector.runtime.workers" => Some(("runtime_workers", "ARROWCOL_COLLECTOR_RUNTIME_WORKERS")),
            "collector.runtime.blocking_workers" => Some(("blocking_workers", "ARROWCOL_COLLECTOR_BLOCKING_WORKERS")),
            "collector.runtime.blocking_fs_reserve" => Some(("blocking_fs_reserve", "ARROWCOL_COLLECTOR_BLOCKING_FS_RESERVE")),
            "collector.runtime.task_limit" => Some(("task_limit", "ARROWCOL_COLLECTOR_TASK_LIMIT")),
            "collector.runtime.read_ahead" => Some(("read_ahead", "ARROWCOL_COLLECTOR_READ_AHEAD")),
            "collector.runtime.batch_rows" => Some(("batch_rows", "ARROWCOL_COLLECTOR_BATCH_ROWS")),
            "collector.runtime.cpu_workers" => Some(("cpu_workers", "ARROWCOL_COLLECTOR_CPU_WORKERS")),
            "collector.runtime.partition_grouping" => {
                Some(("partition_grouping", "ARROWCOL_COLLECTOR_PARTITION_GROUPING"))
            }
            "collector.runtime.file_mapping_cache" => {
                Some(("file_mapping_cache", "ARROWCOL_COLLECTOR_FILE_MAPPING_CACHE"))
            }
            "collector.runtime.generated_partition_binding" => Some((
                "generated_partition_binding",
                "ARROWCOL_COLLECTOR_GENERATED_PARTITION_BINDING",
            )),
            "collector.runtime.cpu_queue_jobs" => Some(("cpu_queue_jobs", "ARROWCOL_COLLECTOR_CPU_QUEUE_JOBS")),
            "collector.runtime.event_interval" => Some(("event_interval", "ARROWCOL_COLLECTOR_EVENT_INTERVAL")),
            "collector.runtime.global_queue_interval" => Some(("global_queue_interval", "ARROWCOL_COLLECTOR_GLOBAL_QUEUE_INTERVAL")),
            "collector.runtime.thread_stack_size" => Some(("thread_stack_size", "ARROWCOL_COLLECTOR_THREAD_STACK_SIZE")),
            "collector.runtime.thread_keep_alive_ms" => Some(("thread_keep_alive_ms", "ARROWCOL_COLLECTOR_THREAD_KEEP_ALIVE_MS")),
            "collector.runtime.cpu_thread_stack_size" => Some(("cpu_thread_stack_size", "ARROWCOL_COLLECTOR_CPU_THREAD_STACK_SIZE")),
            _ => None,
        };
        if let Some((field, variable)) = runtime {
            if environment.contains_key(variable)
                && collectors.iter().all(|process| process["effective_config"]["runtime"].get(field).is_some())
                && (field != "cpu_thread_stack_size" || collectors.iter().all(|process| {
                    process["effective_config"]["cpu_pool"]["thread_stack_size"]
                        == process["effective_config"]["runtime"][field]
                }))
            {
                let reason = if field == "cpu_thread_stack_size" {
                    "consumer_cpu_pool_readback"
                } else {
                    "consumer_startup_readback"
                };
                promoted.push(json!({"path": path, "reason": reason}));
                return false;
            }
            return true;
        }
        if let Some(field) = path.strip_prefix("collector.batching.")
            && matches!(field,
                "coalesce_publications" | "age_flush_policy" | "pipeline_depth"
                | "global_capacity.bytes" | "global_capacity.rows" | "global_capacity.batches"
                | "per_table_capacity.bytes" | "per_table_capacity.rows" | "per_table_capacity.batches"
                | "max_arrow_memory_per_flush" | "max_rows_per_flush" | "max_batches_per_flush"
                | "max_age" | "max_concurrent_flushes")
        {
            let pointer = format!("/{}", field.replace('.', "/"));
            if projected.get("ARROWCOL_COLLECTOR_BATCHING_CONFIG")
                .and_then(|value| value.pointer(&pointer)).is_some()
                && collectors.iter().all(|process| process["effective_config"]["batching"].pointer(&pointer).is_some())
            {
                promoted.push(json!({"path": path, "reason": "consumer_startup_readback"}));
                return false;
            }
            return true;
        }
        let (section, field, variable) = match path {
            "collector.commit.batch_window" | "collector.commit.max_files_per_commit"
            | "collector.commit.max_pending_files" => ("commit", path.rsplit('.').next().unwrap_or_default(), "ARROWCOL_COLLECTOR_COMMIT_CONFIG"),
            "collector.pressure.max_concurrent_streams" | "collector.pressure.rss_limit_bytes"
            | "collector.pressure.buffered_bytes_limit" | "collector.pressure.buffered_rows_limit"
            | "collector.pressure.buffered_batches_limit" | "collector.pressure.pressure_interval"
            | "collector.pressure.heartbeat_interval" | "collector.pressure.registration_ttl" => ("pressure", path.rsplit('.').next().unwrap_or_default(), "ARROWCOL_COLLECTOR_PRESSURE_CONFIG"),
            _ => return true,
        };
        if projected.get(variable).and_then(|value| value.get(field)).is_none() {
            return true;
        }
        if collectors.iter().any(|process| process["effective_config"][section].get(field).is_none()) {
            return true;
        }
        promoted.push(json!({"path": path, "reason": "consumer_startup_readback"}));
        false
    });
    evidence["consumer_verified"]
        .as_array_mut()
        .context("consumer_verified must be an array")?
        .extend(promoted);
    Ok(())
}

fn promote_router_readback(config: &OatConfig, evidence: &mut Value, processes: Option<&Value>, replicas: u32) -> Result<()> {
    let Some(routers) = verified_processes(config, processes, ComponentRole::Router, replicas) else {
        return Ok(());
    };
    if !role_environment(config, ComponentRole::Router)?.contains_key("ARROWCOL_ROUTER_EXCLUDED_ADMISSION_GAUGES")
        || routers.iter().any(|process| process.pointer("/effective_config/routing/excluded_admission_gauges").is_none())
    {
        return Ok(());
    }
    let projection = evidence["projection_only"].as_array_mut().context("projection_only must be an array")?;
    let mut promoted = Vec::new();
    projection.retain(|entry| {
        if entry["path"] == "router.excluded_admission_gauges" {
            promoted.push(json!({"path": entry["path"], "reason": "consumer_startup_readback"}));
            false
        } else {
            true
        }
    });
    evidence["consumer_verified"].as_array_mut().context("consumer_verified must be an array")?.extend(promoted);
    Ok(())
}

fn promote_client_readback(config: &OatConfig, evidence: &mut Value, reports: Option<&Vec<Value>>) -> Result<()> {
    let Some(reports) = reports else {
        return Ok(());
    };
    let ids = reports.iter()
        .filter_map(|report| report["client_id"].as_str().filter(|id| !id.is_empty()))
        .collect::<std::collections::BTreeSet<_>>();
    if reports.len() != config.clients_per_table || ids.len() != reports.len()
        || reports.is_empty()
        || reports.iter().any(|report| verify_process_configuration(config, ComponentRole::Client, &report["consumer_effective_config"]).is_err())
    {
        return Ok(());
    }
    let environment = role_environment(config, ComponentRole::Client)?;
    let projection = evidence["projection_only"].as_array_mut().context("projection_only must be an array")?;
    let mut promoted = Vec::new();
    projection.retain(|entry| {
        let (field, variable) = match entry["path"].as_str() {
            Some("client.payload_customer_id") => ("payload_customer_id", "ARROWCOL_CLIENT_PAYLOAD_CUSTOMER_ID"),
            Some("client.corpus_rows") => ("corpus_rows", "ARROWCOL_CLIENT_CORPUS_ROWS"),
            Some("client.corpus_bytes") => ("corpus_bytes", "ARROWCOL_CLIENT_CORPUS_BYTES"),
            Some("client.workers") => ("workers", "ARROWCOL_CLIENT_WORKERS"),
            Some("client.runtime_threads") => ("runtime_threads", "ARROWCOL_CLIENT_RUNTIME_THREADS"),
            Some("client.batch_rows") => ("batch_rows", "ARROWCOL_CLIENT_BATCH_ROWS"),
            Some("client.connections_per_worker") => ("connections_per_worker", "ARROWCOL_CLIENT_CONNECTIONS_PER_WORKER"),
            Some("client.streams_per_worker") => ("streams_per_worker", "ARROWCOL_CLIENT_STREAMS_PER_WORKER"),
            Some("client.interval_us") => ("interval_us", "ARROWCOL_CLIENT_INTERVAL_US"),
            Some("client.data_mode") => ("data_mode", "ARROWCOL_CLIENT_DATA_MODE"),
            Some("client.data_profile") => ("data_profile", "ARROWCOL_CLIENT_DATA_PROFILE"),
            Some("client.max_memory_bytes") => ("max_memory_bytes", "ARROWCOL_CLIENT_MAX_MEMORY_BYTES"),
            Some("client.stream_retry_max_attempts") => ("stream_retry_max_attempts", "ARROWCOL_CLIENT_STREAM_RETRY_MAX_ATTEMPTS"),
            Some("client.stream_retry_base_backoff_ms") => ("stream_retry_base_backoff_ms", "ARROWCOL_CLIENT_STREAM_RETRY_BASE_BACKOFF_MS"),
            Some("client.stream_retry_max_backoff_ms") => ("stream_retry_max_backoff_ms", "ARROWCOL_CLIENT_STREAM_RETRY_MAX_BACKOFF_MS"),
            Some("transport.max_message_bytes") => ("max_message_bytes", "ARROWCOL_CLIENT_MAX_MESSAGE_BYTES"),
            Some("transport.grpc_send_compression") => ("grpc_send_compression", "ARROWCOL_CLIENT_GRPC_SEND_COMPRESSION"),
            Some("transport.grpc_accept_compression") => ("grpc_accept_compression", "ARROWCOL_CLIENT_GRPC_ACCEPT_COMPRESSION"),
            Some("transport.ipc_compression") | Some("arrow_ipc.compression") => ("ipc_compression", "ARROWCOL_CLIENT_IPC_COMPRESSION"),
            Some("arrow_ipc.alignment") => ("ipc_alignment", "ARROWCOL_CLIENT_IPC_ALIGNMENT"),
            Some("transport.max_flight_data_bytes") => ("max_flight_data_bytes", "ARROWCOL_CLIENT_MAX_FLIGHT_DATA_BYTES"),
            Some("transport.max_frame_bytes") => ("max_frame_bytes", "ARROWCOL_CLIENT_MAX_FRAME_BYTES"),
            Some("transport.header_table_bytes") => ("header_table_bytes", "ARROWCOL_CLIENT_HEADER_TABLE_BYTES"),
            Some("transport.max_header_list_bytes") => ("max_header_list_bytes", "ARROWCOL_CLIENT_MAX_HEADER_LIST_BYTES"),
            Some("transport.stream_window_bytes") => ("stream_window_bytes", "ARROWCOL_CLIENT_STREAM_WINDOW_BYTES"),
            Some("transport.connection_window_bytes") => ("connection_window_bytes", "ARROWCOL_CLIENT_CONNECTION_WINDOW_BYTES"),
            Some("transport.adaptive_window") => ("adaptive_window", "ARROWCOL_CLIENT_ADAPTIVE_WINDOW"),
            Some("transport.keepalive_interval_ms") => ("keepalive_interval_ms", "ARROWCOL_CLIENT_KEEPALIVE_INTERVAL_MS"),
            Some("transport.keepalive_timeout_ms") => ("keepalive_timeout_ms", "ARROWCOL_CLIENT_KEEPALIVE_TIMEOUT_MS"),
            Some("transport.keepalive_while_idle") => ("keepalive_while_idle", "ARROWCOL_CLIENT_KEEPALIVE_WHILE_IDLE"),
            Some("transport.connect_timeout_ms") => ("connect_timeout_ms", "ARROWCOL_CLIENT_CONNECT_TIMEOUT_MS"),
            Some("transport.request_timeout_ms") => ("request_timeout_ms", "ARROWCOL_CLIENT_REQUEST_TIMEOUT_MS"),
            Some("transport.tcp_nodelay") => ("tcp_nodelay", "ARROWCOL_CLIENT_TCP_NODELAY"),
            Some("transport.tcp_keepalive_ms") => ("tcp_keepalive_ms", "ARROWCOL_CLIENT_TCP_KEEPALIVE_MS"),
            Some("transport.tcp_keepalive_interval_ms") => ("tcp_keepalive_interval_ms", "ARROWCOL_CLIENT_TCP_KEEPALIVE_INTERVAL_MS"),
            Some("transport.tcp_keepalive_retries") => ("tcp_keepalive_retries", "ARROWCOL_CLIENT_TCP_KEEPALIVE_RETRIES"),
            Some("transport.rate_limit_period_ms") => ("rate_limit_period_ms", "ARROWCOL_CLIENT_RATE_LIMIT_PERIOD_MS"),
            Some("transport.rate_limit_rps") => ("rate_limit_rps", "ARROWCOL_CLIENT_RATE_LIMIT_RPS"),
            Some("transport.connect_retries") => ("connect_retries", "ARROWCOL_CLIENT_CONNECT_RETRIES"),
            Some("transport.retry_backoff_ms") => ("retry_backoff_ms", "ARROWCOL_CLIENT_RETRY_BACKOFF_MS"),
            Some("transport.endpoint_concurrency_limit") => ("endpoint_concurrency_limit", "ARROWCOL_CLIENT_ENDPOINT_CONCURRENCY_LIMIT"),
            Some("transport.endpoint_buffer_size") => ("endpoint_buffer_size", "ARROWCOL_CLIENT_ENDPOINT_BUFFER_SIZE"),
            _ => return true,
        };
        let client = config.tunables.client.as_ref();
        if (field == "corpus_rows" && client.and_then(|client| client.corpus_bytes).unwrap_or(0) > 0)
            || (field == "corpus_bytes" && client.and_then(|client| client.corpus_rows).unwrap_or(0) > 0)
        {
            return true;
        }
        if !environment.contains_key(variable)
            || reports.iter().any(|report| report["consumer_effective_config"].get(field).is_none())
        {
            return true;
        }
        promoted.push(json!({"path": entry["path"], "reason": "consumer_startup_readback"}));
        false
    });
    evidence["consumer_verified"].as_array_mut().context("consumer_verified must be an array")?.extend(promoted);
    Ok(())
}

#[cfg(test)]
fn inert_tunable_fields(config: &OatConfig) -> Result<Vec<Value>> {
    Ok(tunable_leaf_evidence(config)?["inert_fields"]
        .as_array()
        .context("inert_fields must be an array")?
        .clone())
}

fn capability_evidence() -> Vec<Value> {
    arrowcol_tunables::capability_table()
        .iter()
        .map(|capability| {
            json!({
                "name": capability.name,
                "default": capability.default,
                "consumer_startup_proof": "not_established_by_capability_catalog",
                "boundary": match capability.boundary {
                    arrowcol_tunables::ApplyBoundary::PerCellRuntime => "per_cell_runtime",
                    arrowcol_tunables::ApplyBoundary::ProcessStartup => "process_startup",
                    arrowcol_tunables::ApplyBoundary::RebuildOnly => "rebuild_only",
                },
            })
        })
        .collect()
}

fn role_environment(config: &OatConfig, role: ComponentRole) -> Result<std::collections::BTreeMap<String, String>> {
    config.tunables.validate_for_oat()?;
    if matches!(role, ComponentRole::Collector)
        && config.tunables.logging.as_ref().and_then(|logging| logging.collector).is_some()
    {
        let pipeline = if config.tunables.wal.is_some() {
            include_str!("../../arrowcol-dataflow-rust/config.quiver.yaml")
        } else {
            include_str!("../../arrowcol-dataflow-rust/config.yaml")
        };
        validate_collector_pipeline_log_level(pipeline)?;
    }
    config.tunables.environment_for(role)
}

fn validate_collector_pipeline_log_level(pipeline_yaml: &str) -> Result<()> {
    let pipeline: serde_yaml::Value = serde_yaml::from_str(pipeline_yaml)
        .context("parse rendered collector pipeline for logging precedence")?;
    let mut engine = pipeline
        .get("engine")
        .cloned()
        .unwrap_or_else(|| serde_yaml::Value::Mapping(serde_yaml::Mapping::new()));
    // The DFE expands the drain setting's environment placeholder before loading the profile.
    if let Some(mapping) = engine.as_mapping_mut() {
        mapping.remove("shutdown_drain_timeout_seconds");
    }
    let engine: EngineConfig = serde_yaml::from_value(engine)
        .context("parse collector engine settings for logging precedence")?;
    if engine.telemetry.logs.level.is_some() {
        bail!("logging.collector cannot override engine.telemetry.logs.level in the rendered pipeline");
    }
    Ok(())
}

fn compose_tunables(config: &OatConfig) -> Result<Value> {
    let mut services = serde_json::Map::new();
    for (service, role) in [
        ("oat-client", ComponentRole::Client),
        ("router", ComponentRole::Router),
        ("dataflow-1", ComponentRole::Collector),
    ] {
        let mut environment = role_environment(config, role)?;
        if matches!(role, ComponentRole::Collector) {
            if config.platform.kind == crate::config::OatMode::Local
                && (config.campaign_profile == Some(crate::config::CampaignProfile::SingleHotTable)
                    || config.platform.local.partitioned_table_bootstrap)
                && let Some(granularity) = config.tunables.maintenance.as_ref()
                    .filter(|settings| settings.enabled == Some(true))
                    .and_then(|settings| settings.granularity)
            {
                let value = serde_json::to_value(granularity)?;
                environment.insert(
                    "ARROWCOL_PARTITION_GRANULARITY".to_owned(),
                    value.as_str().context("invalid typed partition granularity")?.to_owned(),
                );
            }
            for &(legacy, canonical, _) in arrow_deltalake::delta::PROCESS_WRITER_PROPERTY_ENVIRONMENT_ALIASES {
                if let Some(value) = environment.get(canonical).cloned() {
                    environment.insert(legacy.to_owned(), value);
                }
            }
        }
        if !environment.is_empty() {
            services.insert(service.to_owned(), json!({ "environment": environment }));
        }
    }
    compose_maintenance(config, &mut services)?;
    Ok(json!({ "services": services }))
}

fn local_dataflow_overlay(config: &OatConfig, state_dir: &Path, overlay: &mut Value) -> Result<()> {
    let services = overlay["services"]
        .as_object_mut()
        .context("Compose tunable services missing")?;
    let collector = services.entry("dataflow-1").or_insert_with(|| json!({}));
    if let Some(bytes) = config.platform.local.collector_memory_limit_bytes {
        collector["mem_limit"] = json!(bytes);
    }
    if let Some(cpus) = &config.platform.local.collector_cpus {
        collector["cpuset"] = json!(cpus.iter().map(u32::to_string).collect::<Vec<_>>().join(","));
    }
    collector["stop_grace_period"] = json!(format!("{}s", config.collector_stop_grace_seconds));
    collector["ports"] = json!([{
        "host_ip": config.platform.local.bind_host.map_or_else(|| "127.0.0.1".to_owned(), |host| host.to_string()),
        "target": 32011,
        "published": "0",
        "protocol": "tcp"
    }]);
    collector["entrypoint"] = json!(["/bin/sh", "-ec"]);
    collector["command"] = json!([concat!(
        "export ARROWCOL_INSTANCE_KEY=\"oat-$(hostname)\"; ",
        "export ARROWCOL_ADVERTISED_FLIGHT_ENDPOINT=\"$(hostname -i):32010\"; ",
        "export ARROWCOL_QUIVER_PATH=\"/oat-quiver/$(hostname)\"; ",
        "exec /usr/bin/tini -- /usr/local/bin/arrowcol-dataflow-rust --num-cores \"${LOADTEST_CPUS:-2}\" --config /etc/oat/dataflow.yaml"
    )]);
    let mut volumes = vec![json!({"type":"bind", "source":state_dir.join("dataflow.yaml"),
        "target":"/etc/oat/dataflow.yaml", "read_only":true, "bind":{"create_host_path":false}})];
    if config.tunables.wal.is_some() {
        volumes.push(json!({"type":"bind", "source":state_dir.join("quiver"),
            "target":"/oat-quiver", "bind":{"create_host_path":false}}));
    }
    collector["volumes"] = json!(volumes);
    Ok(())
}

fn inject_container_environment(
    container: &mut Value,
    variables: &std::collections::BTreeMap<String, String>,
) -> Result<()> {
    if variables.is_empty() {
        return Ok(());
    }
    let container = container
        .as_object_mut()
        .context("workload container must be an object")?;
    let environment = container
        .entry("env")
        .or_insert_with(|| json!([]))
        .as_array_mut()
        .context("workload container env must be an array")?;
    for (name, value) in variables {
        let mut existing = environment
            .iter_mut()
            .filter(|entry| entry["name"].as_str() == Some(name));
        if let Some(entry) = existing.next() {
            *entry = json!({ "name": name, "value": value });
            if existing.next().is_some() {
                bail!("duplicate tunable environment name {name}");
            }
        } else {
            environment.push(json!({ "name": name, "value": value }));
        }
    }
    Ok(())
}

fn inject_deployment_environment(
    deployment: &mut Value,
    name: &str,
    variables: &std::collections::BTreeMap<String, String>,
) -> Result<()> {
    if variables.is_empty() {
        return Ok(());
    }
    let containers = deployment
        .pointer_mut("/spec/template/spec/containers")
        .and_then(Value::as_array_mut)
        .context("workload Deployment has no containers")?;
    let mut selected = containers
        .iter_mut()
        .filter(|container| container["name"].as_str() == Some(name));
    let container = selected.next().context("tunable target container does not exist")?;
    inject_container_environment(container, variables)?;
    if selected.next().is_some() {
        bail!("tunable target container is ambiguous");
    }
    Ok(())
}

fn inject_workload_environment(config: &OatConfig, resources: &mut [Value]) -> Result<()> {
    for resource in resources {
        if resource["kind"].as_str() != Some("Deployment") {
            continue;
        }
        inject_maintenance_arguments(config, resource)?;
        let role_and_container = match resource.pointer("/metadata/name").and_then(Value::as_str) {
            Some("arrowcol-router") => Some((ComponentRole::Router, "router")),
            Some(name) if name == config.dfe_deployment => {
                Some((ComponentRole::Collector, config.dfe_container.as_str()))
            }
            Some("delta-maintenance") => Some((ComponentRole::Maintenance, "maintenance")),
            _ => None,
        };
        if let Some((role, container)) = role_and_container {
            let variables = role_environment(config, role)?;
            if role == ComponentRole::Collector
                && config.campaign_profile == Some(crate::config::CampaignProfile::ScaleOutSingleTable)
            {
                validate_collector_exporter_override(resource, container, &variables)?;
            }
            inject_deployment_environment(resource, container, &variables)?;
        }
    }
    Ok(())
}

fn validate_collector_exporter_override(
    deployment: &Value,
    container_name: &str,
    variables: &std::collections::BTreeMap<String, String>,
) -> Result<()> {
    let containers = deployment
        .pointer("/spec/template/spec/containers")
        .and_then(Value::as_array)
        .context("collector Deployment has no containers")?;
    let mut selected = containers.iter().filter(|item| item["name"].as_str() == Some(container_name));
    let container = selected.next().context("collector container is missing")?;
    if selected.next().is_some() {
        bail!("collector container is ambiguous");
    }
    let entries = container["env"].as_array().context("collector environment is missing")?;
    let mut entries = entries
        .iter()
        .filter(|entry| entry["name"].as_str() == Some("ARROWCOL_COLLECTOR_EXPORTER_CONFIG"));
    let entry = entries.next();
    if entries.next().is_some() {
        bail!("rendered collector exporter override is duplicated");
    }
    let Some(entry) = entry else {
        return Ok(());
    };
    let value = entry["value"].as_str().context("collector exporter override must have a literal value")?;
    let rendered: arrowcol_tunables::ExporterTunables =
        serde_json::from_str(value).context("invalid rendered collector exporter override")?;
    let name = "ARROWCOL_COLLECTOR_EXPORTER_CONFIG";
    let requested: arrowcol_tunables::ExporterTunables = serde_json::from_str(
        variables.get(name).context("unrequested collector exporter override")?,
    ).context("invalid collector exporter tunable override")?;
    if serde_json::to_value(rendered)? != serde_json::to_value(requested)? {
        bail!("collector exporter override differs from requested tunables");
    }
    Ok(())
}

#[cfg(test)]
mod tunable_injection_tests {
    use super::*;

    #[test]
    fn rust_runtime_exporter_config_matches_collector_parser_without_override() -> Result<()> {
        runtime_exporter_config_matches_collector_parser(None)
    }

    #[test]
    fn rust_runtime_exporter_config_matches_collector_parser_with_override() -> Result<()> {
        runtime_exporter_config_matches_collector_parser(Some("10s"))
    }

    fn runtime_exporter_config_matches_collector_parser(timeout: Option<&str>) -> Result<()> {
        let mut config = config()?;
        config.campaign_profile = Some(crate::config::CampaignProfile::ScaleOutSingleTable);
        config.tables = vec!["sqlserver_availability_replica_states".to_owned()];
        config.dfe_deployment = "arrowcol".to_owned();
        config.dfe_container = "dataflow".to_owned();
        config.tunables = arrowcol_tunables::Tunables::parse_oat_json(json!({
            "collector":timeout.map(|value| json!({"exporter":{"authority_timeout":value}})).unwrap_or_else(|| json!({})),
            "maintenance":{"enabled":true,"apply":true,
                "operations":["purge","optimize","vacuum","checkpoint","prune_log"]}
        }))?;
        let mut runtime = json!({
            "namespace":"arrowcol-oat", "oatRunId":"00000000-0000-0000-0000-000000000003",
            "workloadIdentityClientId":"00000000-0000-0000-0000-000000000001",
            "subscriptionId":"00000000-0000-0000-0000-000000000002", "stamp":"89152",
            "deltaRoot":"abfss://onelake@arrowcollector89152.dfs.core.windows.net/dataflow",
            "effective_stop_budgets":{"maintenance_grace_seconds":4800,"maintenance_stop_timeout_ms":5100000},
            "images": (["sqlserver","catalog","router","dataflow","maintenance"].into_iter()
                .map(|role| (role.to_owned(), format!("arrowcollector89152.azurecr.io/{role}@sha256:{}", "a".repeat(64))))
                .collect::<std::collections::BTreeMap<_,_>>()),
            "quiver":false,
            "maintenance":{"tables":["telemetry.sqlserver.sqlserver_availability_replica_states"],
                "operations":["optimize","checkpoint","purge","vacuum","prune-log"],"apply":true}
        });
        project_scale_out_runtime(&mut runtime, &config, 1)?;
        let script = r#"
import { readFileSync } from 'node:fs';
import { renderRuntime } from './projects/arrowcol-oat/runtime.ts';
const input = JSON.parse(readFileSync(0, 'utf8'));
process.stdout.write(JSON.stringify(await renderRuntime(input, 'workload')));
"#;
        let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../..");
        let mut child = std::process::Command::new("node")
            .current_dir(root)
            .args(["--import", "tsx", "--input-type=module", "-e", script])
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .context("start actual TypeScript runtime renderer")?;
        child.stdin.take().context("renderer stdin missing")?.write_all(&serde_json::to_vec(&runtime)?)?;
        let output = child.wait_with_output().context("wait for runtime renderer")?;
        if !output.status.success() {
            bail!("runtime renderer failed: {}", String::from_utf8_lossy(&output.stderr));
        }
        let mut resources: Vec<Value> = serde_json::from_slice(&output.stdout)?;
        assert_exporter_configs(&resources, None)?;
        let rendered_collector = resources.iter().find(|resource| resource["kind"] == "Deployment"
            && resource["metadata"]["name"] == config.dfe_deployment).context("collector Deployment missing")?.clone();
        inject_workload_environment(&config, &mut resources)?;
        assert_exporter_configs(&resources, timeout)?;
        let collector = resources.iter_mut().find(|resource| resource["kind"] == "Deployment"
            && resource["metadata"]["name"] == config.dfe_deployment).context("collector Deployment missing")?;
        configure_collector_deployment(&config, collector, 1)?;
        assert_exporter_configs(std::slice::from_ref(collector), timeout)?;
        let configured = collector.clone();
        configure_collector_deployment(&config, collector, 1)?;
        assert_eq!(*collector, configured);
        let mut fresh = rendered_collector;
        configure_collector_deployment(&config, &mut fresh, 1)?;
        assert_exporter_configs(std::slice::from_ref(&fresh), timeout)?;
        assert_eq!(fresh, configured);
        for invalid in [
            "{",
            "{}",
            r#"{"startup_table":{}}"#,
            r#"{"authority_timeout":"10s","extra":true}"#,
            r#"{"authority_timeout":"20s"}"#,
            r#"{"authority_timeout":"10s","authority_timeout":"10s"}"#,
        ] {
            let mut invalid_deployment = configured.clone();
            let entries = invalid_deployment.pointer_mut("/spec/template/spec/containers/0/env")
                .and_then(Value::as_array_mut).context("collector env missing")?;
            entries.retain(|entry| entry["name"] != "ARROWCOL_COLLECTOR_EXPORTER_CONFIG");
            entries.push(json!({"name":"ARROWCOL_COLLECTOR_EXPORTER_CONFIG","value":invalid}));
            assert!(configure_collector_deployment(&config, &mut invalid_deployment.clone(), 1).is_err(), "{invalid}");
            assert!(inject_workload_environment(&config, std::slice::from_mut(&mut invalid_deployment)).is_err(), "{invalid}");
        }
        let mut duplicated = configured.clone();
        let entries = duplicated.pointer_mut("/spec/template/spec/containers/0/env")
            .and_then(Value::as_array_mut).context("collector env missing")?;
        let entry = json!({"name":"ARROWCOL_COLLECTOR_EXPORTER_CONFIG","value":r#"{"authority_timeout":"10s"}"#});
        entries.retain(|entry| entry["name"] != "ARROWCOL_COLLECTOR_EXPORTER_CONFIG");
        entries.extend([entry.clone(),entry]);
        assert!(configure_collector_deployment(&config, &mut duplicated.clone(), 1).is_err());
        assert!(inject_workload_environment(&config, std::slice::from_mut(&mut duplicated)).is_err());
        Ok(())
    }

    fn assert_exporter_configs(resources: &[Value], timeout: Option<&str>) -> Result<()> {
        let mut count = 0;
        for resource in resources {
            if let Some(containers) = resource.pointer("/spec/template/spec/containers").and_then(Value::as_array) {
                for container in containers {
                    if let Some(env) = container["env"].as_array() {
                        for entry in env.iter().filter(|entry| entry["name"] == "ARROWCOL_COLLECTOR_EXPORTER_CONFIG") {
                            let parsed: arrowcol_tunables::ExporterTunables = serde_json::from_str(
                                entry["value"].as_str().context("exporter config must be a string")?
                            ).context("actual collector exporter parser rejected emitted config")?;
                            assert_eq!(parsed.authority_timeout.as_deref(), timeout);
                            count += 1;
                        }
                    }
                }
            }
        }
        assert_eq!(count, usize::from(timeout.is_some()));
        Ok(())
    }

    #[test]
    fn local_collector_port_uses_bind_host() -> Result<()> {
        let proof_config = std::env::var_os("OAT_BIND_PROOF_CONFIG");
        let source = match &proof_config {
            Some(path) => std::fs::read_to_string(path)?,
            None => include_str!("../../../docs/experiments/arrowcol-oat/oat-config.quick.json").to_owned(),
        };
        let mut config: OatConfig = serde_json::from_str(&source)?;
        for (name, host, expected) in [
            ("default", None, "127.0.0.1"),
            ("gateway", Some("172.18.0.1".parse()?), "172.18.0.1"),
            ("ipv6", Some("::1".parse()?), "::1"),
        ] {
            config.platform.local.bind_host = host;
            let mut overlay = compose_tunables(&config)?;
            local_dataflow_overlay(&config, Path::new("/owned/run"), &mut overlay)?;
            assert_eq!(
                overlay["services"]["dataflow-1"]["ports"],
                json!([{"host_ip":expected,"target":32011,"published":"0","protocol":"tcp"}])
            );
            if proof_config.is_some() {
                println!("OAT_BIND_OVERLAY_{name}={overlay}");
            }
        }
        Ok(())
    }

    #[test]
    fn overload_local_memory_cap_is_explicit_and_scoped_to_the_collector() -> Result<()> {
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        let mut overlay = compose_tunables(&config)?;
        local_dataflow_overlay(&config, Path::new("/owned/run"), &mut overlay)?;
        assert!(overlay["services"]["dataflow-1"].get("mem_limit").is_none());
        config.platform.local.collector_memory_limit_bytes = Some(4 * 1024_u64.pow(3));
        config.validate()?;
        local_dataflow_overlay(&config, Path::new("/owned/run"), &mut overlay)?;
        assert_eq!(overlay["services"]["dataflow-1"]["mem_limit"], 4 * 1024_u64.pow(3));
        for (service, value) in overlay["services"].as_object().context("services missing")? {
            if service != "dataflow-1" { assert!(value.get("mem_limit").is_none()); }
        }

        for invalid in [json!("4GiB"), json!("4096Mi"), json!(-1), json!(1.5)] {
            assert!(serde_json::from_value::<crate::config::LocalOptions>(
                json!({"collector_memory_limit_bytes":invalid})).is_err());
        }
        config.platform.local.collector_memory_limit_bytes = Some(1);
        assert!(config.validate().is_err());
        Ok(())
    }

    #[test]
    fn collector_cpu_allocation_is_local_and_scoped() -> Result<()> {
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        config.platform.local.stable_image_build = true;
        config.replica_counts = vec![1];
        config.platform.local.collector_cpus = Some(vec![1, 3]);
        config.validate()?;
        let mut overlay = compose_tunables(&config)?;
        local_dataflow_overlay(&config, Path::new("/owned/run"), &mut overlay)?;
        assert_eq!(overlay["services"]["dataflow-1"]["cpuset"], "1,3");
        for (service, value) in overlay["services"].as_object().context("services missing")? {
            if service != "dataflow-1" {
                assert!(value.get("cpuset").is_none());
            }
        }
        config.platform.local.collector_cpus = Some(vec![3, 1]);
        assert!(config.validate().is_err());
        config.platform.local.collector_cpus = Some(vec![1, 1]);
        assert!(config.validate().is_err());
        config.platform.local.collector_cpus = Some(vec![]);
        assert!(config.validate().is_err());
        Ok(())
    }

    #[test]
    fn local_campaign_granularity_reaches_both_consumers_and_gates_admission() -> Result<()> {
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        config.campaign_profile = Some(crate::config::CampaignProfile::SingleHotTable);
        config.tunables = serde_json::from_value(json!({
            "maintenance":{"enabled":true,"operations":["purge"],"granularity":"hour"}
        }))?;
        let overlay = compose_tunables(&config)?;
        let collector_env = overlay.pointer("/services/dataflow-1/environment")
            .and_then(Value::as_object).context("collector environment missing")?
            .iter().map(|(key, value)| Ok((key.clone(), value.as_str().context("not a string")?.to_owned())))
            .collect::<Result<std::collections::BTreeMap<_, _>>>()?;
        let writer = arrow_deltalake::delta::resolve_process_writer_properties_from_environment(&collector_env)?;
        assert_eq!(writer.get("arrowcol.partition.granularity").map(String::as_str), Some("hour"));
        assert_eq!(overlay.pointer("/services/delta-maintenance/environment/ARROWCOL_MAINT_GRANULARITY"), Some(&json!("hour")));
        let table = canonical_dmv_table_name(&config.tables[0])?;
        let collector = json!({"Config":{"Env":["ARROWCOL_PARTITION_GRANULARITY=hour"]}});
        let event = |granularity: &str| format!("arrowcol_maintenance_table_config {}\n", json!({
            "table": table,
            "config": {"active":true,"table_policy":{"arrowcol.partition.granularity":granularity}}
        }));
        assert!(local_partition_policy_evidence("hour", &table, &collector, "")?.is_none());
        let evidence = local_partition_policy_evidence("hour", &table, &collector, &event("hour"))?
            .context("missing evidence")?;
        assert_eq!(evidence["maintenance_table_effective"], "hour");
        assert!(local_partition_policy_evidence("hour", &table, &collector, &event("minute")).is_err());
        let mut properties = std::collections::BTreeMap::new();
        assert_eq!(validate_local_table_partition_property(&properties, "hour")?, None);
        properties.insert("arrowcol.partition.granularity".to_owned(), "hour".to_owned());
        assert_eq!(validate_local_table_partition_property(&properties, "hour")?, Some("hour"));
        properties.insert("arrowcol.partition.granularity".to_owned(), "minute".to_owned());
        assert!(validate_local_table_partition_property(&properties, "hour").is_err());
        assert!(local_partition_policy_evidence(
            "hour", &table, &json!({"Config":{"Env":["ARROWCOL_PARTITION_GRANULARITY=minute"]}}), &event("hour")
        ).is_err());
        assert!(serde_json::from_value::<arrowcol_tunables::Tunables>(
            json!({"maintenance":{"granularity":"hours"}})
        ).is_err());
        config.campaign_profile = None;
        let noncampaign = compose_tunables(&config)?;
        assert!(noncampaign.pointer("/services/dataflow-1/environment/ARROWCOL_PARTITION_GRANULARITY").is_none());
        config.platform.local.partitioned_table_bootstrap = true;
        let bootstrap = compose_tunables(&config)?;
        assert_eq!(bootstrap.pointer("/services/dataflow-1/environment/ARROWCOL_PARTITION_GRANULARITY"), Some(&json!("hour")));
        config.platform.local.partitioned_table_bootstrap = false;
        config.campaign_profile = Some(crate::config::CampaignProfile::SingleHotTable);
        config.tunables.maintenance.as_mut().context("maintenance missing")?.enabled = Some(false);
        let disabled = compose_tunables(&config)?;
        assert!(disabled.pointer("/services/dataflow-1/environment/ARROWCOL_PARTITION_GRANULARITY").is_none());
        config.tunables.maintenance.as_mut().context("maintenance missing")?.enabled = Some(true);
        config.tunables.maintenance.as_mut().context("maintenance missing")?.granularity = None;
        let unset = compose_tunables(&config)?;
        assert!(unset.pointer("/services/dataflow-1/environment/ARROWCOL_PARTITION_GRANULARITY").is_none());
        Ok(())
    }

    #[test]
    #[ignore = "requires built delta-maintenance binary"]
    fn local_campaign_granularity_reaches_real_maintenance_consumer() -> Result<()> {
        let root = std::env::temp_dir().join(format!("oat-partition-policy-{}", uuid::Uuid::new_v4()));
        struct TempRoot(PathBuf);
        impl Drop for TempRoot {
            fn drop(&mut self) {
                let _ = std::fs::remove_dir_all(&self.0);
            }
        }
        std::fs::create_dir(&root)?;
        let root = TempRoot(root);
        let table = "telemetry.sqlserver.sqlserver_availability_replica_states";
        arrowcol_loadgen::initialize::initialize_local(arrowcol_loadgen::cli::InitializeLocalArgs {
            partition_by_ingestion_date: true, unpartitioned: false, table: table.to_owned(), dataflow_root: root.0.clone(),
        })?;
        let mut config: OatConfig = serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?;
        config.campaign_profile = Some(crate::config::CampaignProfile::SingleHotTable);
        config.tables = vec!["sqlserver_availability_replica_states".to_owned()];
        config.tunables = serde_json::from_value(json!({
            "maintenance":{"enabled":true,"operations":["purge"],"granularity":"hour",
                "purge_retention_secs":2592000}
        }))?;
        let overlay = compose_tunables(&config)?;
        let environment = overlay.pointer("/services/dataflow-1/environment")
            .and_then(Value::as_object).context("collector environment missing")?;
        let env = environment.iter().map(|(key, value)|
            Ok(format!("{key}={}", value.as_str().context("environment value is not a string")?)))
            .collect::<Result<Vec<_>>>()?;
        let collector = json!({"Config":{"Env": env}});
        let url = url::Url::from_directory_path(&root.0).map_err(|()| anyhow::anyhow!("invalid root"))?;
        let binary = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/debug/delta-maintenance");
        let output = std::process::Command::new(binary).env_clear()
            .envs(role_environment(&config, ComponentRole::Maintenance)?)
            .args(["--root", url.as_str(), "--table", table, "--operation", "purge",
                "--interval-secs", "60", "--max-cycles", "1", "--no-metrics-listener"])
            .output()?;
        let stderr = String::from_utf8(output.stderr)?;
        if !output.status.success() { bail!("maintenance fixture failed: {stderr}"); }
        verify_process_configuration(&config, ComponentRole::Maintenance, &process_configurations(&stderr)?)?;
        let evidence = local_partition_policy_evidence("hour", table, &collector, &stderr)?
            .context("real maintenance consumer omitted effective table policy")?;
        assert_eq!(evidence["maintenance_table_effective"], "hour");
        let metadata_url = url::Url::from_directory_path(root.0.join(
            "telemetry/sqlserver/sqlserver_availability_replica_states"
        )).map_err(|()| anyhow::anyhow!("invalid table"))?;
        let properties = arrow_deltalake::delta::DeltaTable::open_for_maintenance_probe(&metadata_url)?
            .maintenance_properties()?;
        assert!(!properties.contains_key("arrowcol.partition.granularity"));
        Ok(())
    }

    #[test]
    fn collector_upload_json_reaches_runtime() -> Result<()> {
        if std::env::var_os("OAT_UPLOAD_RUNTIME_CHILD").is_some() {
            let actual = serde_json::to_value(arrow_deltalake::DeltaRuntimeConfig::from_env()?)?;
            let expected: Value = serde_json::from_str(&std::env::var("OAT_UPLOAD_RUNTIME_EXPECTED")?)?;
            for (key, value) in expected.as_object().context("expected runtime configuration must be an object")? {
                assert_eq!(actual[key], *value, "{key}");
            }
            println!("OAT_UPLOAD_RUNTIME_READBACK {actual}");
            return Ok(());
        }
        for (tunables, expected) in [
            (json!({"collector":{"runtime":{"timestamp_kernel":"fused_checked"}}}), json!({"timestamp_kernel":"fused_checked"})),
            (json!({"collector":{"runtime":{"upload_limit":1},"upload":{"mode":"buffered_multipart","buffer_capacity_bytes":1048576,"request_concurrency":16}}}),
             json!({"upload_mode":"buffered_multipart","upload_buffer_capacity_bytes":1048576,"upload_request_concurrency":16,"upload_limit":1})),
            (json!({"collector":{"runtime":{"upload_limit":256},"upload":{"mode":"put_create"}}}),
             json!({"upload_mode":"put_create","upload_buffer_capacity_bytes":8388608,"upload_request_concurrency":2,"upload_limit":256})),
            (json!({"collector":{"upload":{"mode":"buffered_multipart","buffer_capacity_bytes":1048576,"request_concurrency":2}}}),
             json!({"upload_mode":"buffered_multipart","upload_buffer_capacity_bytes":1048576,"upload_request_concurrency":2,"upload_limit":32})),
            (json!({"collector":{"runtime":{"file_mapping_cache":true}}}), json!({"file_mapping_cache":true})),
            (json!({"collector":{"runtime":{"cpu_workers":4,"cpu_queue_jobs":2,"blocking_workers":4,"blocking_fs_reserve":2}}}),
             json!({"cpu_workers":4,"cpu_queue_jobs":2,"blocking_workers":4,"blocking_fs_reserve":2})),
            (json!({"collector":{"runtime":{"event_interval":7,"global_queue_interval":13,"thread_stack_size":2097152,"thread_keep_alive_ms":3000,"cpu_thread_stack_size":2097152}}}),
             json!({"event_interval":7,"global_queue_interval":13,"thread_stack_size":2097152,"thread_keep_alive_ms":3000,"cpu_thread_stack_size":2097152})),
        ] {
            let settings = arrowcol_tunables::Tunables::parse_oat_json(tunables)?;
            let environment = settings.environment_for(ComponentRole::Collector)?;
            let output = std::process::Command::new(std::env::current_exe()?)
                .arg("--exact")
                .arg("runner::tunable_injection_tests::collector_upload_json_reaches_runtime")
                .arg("--nocapture")
                .env_clear()
                .env("OAT_UPLOAD_RUNTIME_CHILD", "1")
                .env("OAT_UPLOAD_RUNTIME_EXPECTED", expected.to_string())
                .envs(environment)
                .output()?;
            let stdout = String::from_utf8_lossy(&output.stdout);
            anyhow::ensure!(
                output.status.success() && stdout.contains("1 passed;"),
                "collector upload runtime rehearsal failed: {stdout}\n{}",
                String::from_utf8_lossy(&output.stderr)
            );
            let actual: Value = serde_json::from_str(
                stdout
                    .lines()
                    .find_map(|line| line.split_once("OAT_UPLOAD_RUNTIME_READBACK ").map(|(_, json)| json))
                    .context("collector runtime readback missing")?,
            )?;
            let mut config = config()?;
            config.tunables = settings;
            verify_process_configuration(&config, ComponentRole::Collector, &json!({"runtime":actual}))?;
            assert!(verify_process_configuration(&config, ComponentRole::Collector, &json!({"runtime":{}})).is_err());
        }
        Ok(())
    }

    #[test]
    #[ignore = "requires built arrowcol-dataflow-rust"]
    fn upload_only_reaches_actual_collector_startup() -> Result<()> {
        let binary = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("target")
            .join("debug")
            .join(format!("arrowcol-dataflow-rust{}", std::env::consts::EXE_SUFFIX));
        let mut config = config()?;
        config.tunables = arrowcol_tunables::Tunables::parse_oat_json(json!({
            "collector":{"upload":{"mode":"buffered_multipart","buffer_capacity_bytes":1048576,"request_concurrency":3}}
        }))?;
        let output = std::process::Command::new(binary)
            .arg("--help")
            .env_clear()
            .envs(role_environment(&config, ComponentRole::Collector)?)
            .output()
            .context("launch actual collector for startup readback")?;
        anyhow::ensure!(output.status.success(), "collector startup failed: {}", String::from_utf8_lossy(&output.stderr));
        let observed = process_configurations(&String::from_utf8(output.stderr)?)?;
        assert_eq!(observed["runtime"]["upload_mode"], "buffered_multipart");
        assert_eq!(observed["runtime"]["upload_buffer_capacity_bytes"], 1048576);
        assert_eq!(observed["runtime"]["upload_request_concurrency"], 3);
        verify_process_configuration(&config, ComponentRole::Collector, &observed)?;
        let process = json!({
            "component": "collector", "instance_id": "collector-1", "log_complete": true,
            "startup_tunables_verified": true, "effective_config_source": "consumer_startup_readback",
            "effective_config": observed,
        });
        let initial = tunable_leaf_evidence(&config)?;
        let mut covered = initial.clone();
        promote_collector_readback(&config, &mut covered, Some(&json!([process.clone()])), 1)?;
        assert_eq!(covered, initial);
        let mut wrong_process = process;
        wrong_process["effective_config"]["runtime"]["upload_mode"] = json!("put_create");
        let mut uncovered = initial.clone();
        promote_collector_readback(&config, &mut uncovered, Some(&json!([wrong_process])), 1)?;
        assert_eq!(uncovered, initial);
        let mut missing = observed.clone();
        missing["runtime"].as_object_mut().context("missing runtime readback")?.remove("upload_mode");
        assert!(verify_process_configuration(&config, ComponentRole::Collector, &missing).is_err());
        let mut wrong = observed;
        wrong["runtime"]["upload_request_concurrency"] = json!(2);
        assert!(verify_process_configuration(&config, ComponentRole::Collector, &wrong).is_err());
        Ok(())
    }

    #[test]
    #[ignore = "requires built arrowcol-dataflow-rust"]
    fn detach_ratio_reaches_actual_collector_startup() -> Result<()> {
        let binary = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/debug/arrowcol-dataflow-rust");
        let mut config = config()?;
        config.tunables = arrowcol_tunables::Tunables::parse_oat_json(json!({
            "collector":{"layout":{"detach_ratio":5}}
        }))?;
        let output = std::process::Command::new(binary).arg("--help").env_clear()
            .envs(role_environment(&config, ComponentRole::Collector)?).output()?;
        anyhow::ensure!(output.status.success(), "collector startup failed: {}", String::from_utf8_lossy(&output.stderr));
        let observed = process_configurations(&String::from_utf8(output.stderr)?)?;
        verify_process_configuration(&config, ComponentRole::Collector, &observed)?;
        assert_eq!(observed["runtime"]["layout_detach_ratio"], 5);
        Ok(())
    }

    fn config() -> Result<OatConfig> {
        Ok(serde_json::from_str(include_str!(
            "../../../docs/experiments/arrowcol-oat/oat-config.quick.json"
        ))?)
    }

    #[test]
    #[ignore = "requires built arrowcol-dataflow-rust"]
    fn timestamp_kernel_reaches_actual_collector_startup() -> Result<()> {
        let binary = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../target/debug/arrowcol-dataflow-rust");
        let mut config = config()?;
        config.tunables = arrowcol_tunables::Tunables::parse_oat_json(json!({
            "collector":{"runtime":{"timestamp_kernel":"fused_checked"}}
        }))?;
        let output = std::process::Command::new(binary).arg("--help").env_clear()
            .envs(role_environment(&config, ComponentRole::Collector)?).output()?;
        anyhow::ensure!(output.status.success(), "collector startup failed: {}", String::from_utf8_lossy(&output.stderr));
        let mut observed = process_configurations(&String::from_utf8(output.stderr)?)?;
        verify_process_configuration(&config, ComponentRole::Collector, &observed)?;
        observed["runtime"]["timestamp_kernel"] = json!("reference");
        assert!(verify_process_configuration(&config, ComponentRole::Collector, &observed).is_err());
        Ok(())
    }

    #[test]
    fn cpu_stack_evidence_requires_the_started_pool() -> Result<()> {
        let mut config = config()?;
        config.tunables = arrowcol_tunables::Tunables::parse_oat_json(json!({
            "collector":{"runtime":{"cpu_thread_stack_size":4194304}}
        }))?;
        let mut process = json!({
            "component":"collector", "instance_id":"collector-1", "log_complete":true,
            "startup_tunables_verified":true, "effective_config_source":"consumer_startup_readback",
            "effective_config":{"runtime":{"cpu_thread_stack_size":4194304}}
        });
        for observed in [None, Some(2097152), Some(4194304)] {
            if let Some(size) = observed {
                process["effective_config"]["cpu_pool"] = json!({"thread_stack_size":size});
            }
            let mut evidence = tunable_leaf_evidence(&config)?;
            promote_collector_readback(&config, &mut evidence, Some(&json!([process.clone()])), 1)?;
            let verified = evidence["consumer_verified"].as_array().context("consumer_verified array")?;
            assert_eq!(verified.iter().any(|entry| entry["path"] == "collector.runtime.cpu_thread_stack_size"),
                observed == Some(4194304));
        }
        Ok(())
    }

    #[test]
    fn collector_shutdown_drain_timeout_reaches_dfe_profiles_and_readback() -> Result<()> {
        const CHILD: &str = "OAT_DFE_DRAIN_CHILD";
        const ENV: &str = "ARROWCOL_COLLECTOR_SHUTDOWN_DRAIN_TIMEOUT_SECONDS";
        let mut config = config()?;
        config.collector_stop_grace_seconds = 540;
        config.tunables = arrowcol_tunables::Tunables::parse_oat_json(json!({
            "collector": {"shutdown_drain_timeout_seconds": 240}
        }))?;
        config.run_pin_ttl_ms = 86_400_000;
        config.validate()?;
        if std::env::var_os(CHILD).is_some() {
            use otel_arrow_dfe_config::{engine::OtelDataflowSpec, env_substitution::substitute_env_vars};
            for template in [
                include_str!("../../arrowcol-dataflow-rust/config.yaml"),
                include_str!("../../arrowcol-dataflow-rust/config.quiver.yaml"),
            ] {
                let value = substitute_env_vars(template)?;
                let parsed = OtelDataflowSpec::from_yaml(&value)?;
                assert_eq!(
                    arrowcol_tunables::MAX_COLLECTOR_SHUTDOWN_DRAIN_TIMEOUT_SECONDS,
                    otel_arrow_dfe_config::engine::MAX_SHUTDOWN_DRAIN_TIMEOUT_SECONDS,
                );
                let actual = parsed.engine.shutdown_drain_timeout_seconds.get();
                assert_eq!(actual, 240);
                let line = format!("df_engine_effective_config {}", json!({
                    "engine": {"shutdown_drain_timeout_seconds": actual}
                }));
                let text = filter_process_evidence_text(&line)?;
                let observed = process_configurations(&text)?;
                verify_process_configuration(&config, ComponentRole::Collector, &observed)?;
            }
            for observed in [json!({}), json!({"engine":{"shutdown_drain_timeout_seconds": 60}})] {
                assert!(verify_process_configuration(&config, ComponentRole::Collector, &observed).is_err());
            }
            return Ok(());
        }
        let compose = compose_tunables(&config)?;
        let expected = compose["services"]["dataflow-1"]["environment"][ENV].as_str().context("missing Compose tunable")?;
        let mut deployment = json!({"spec":{"template":{"spec":{"containers":[{"name":"dataflow"}]}}}});
        inject_deployment_environment(&mut deployment, "dataflow", &role_environment(&config, ComponentRole::Collector)?)?;
        assert_eq!(deployment["spec"]["template"]["spec"]["containers"][0]["env"], json!([{"name": ENV,"value":expected}]));
        let output = std::process::Command::new(std::env::current_exe()?)
            .env(CHILD, "1")
            .env(ENV, expected)
            .args(["collector_shutdown_drain_timeout_reaches_dfe_profiles_and_readback", "--nocapture"])
            .output()?;
        anyhow::ensure!(
            output.status.success() && String::from_utf8_lossy(&output.stdout).contains("1 passed;"),
            "DFE drain config subprocess failed: {}\n{}",
            String::from_utf8_lossy(&output.stdout),
            String::from_utf8_lossy(&output.stderr)
        );
        Ok(())
    }

    #[test]
    fn collector_logging_rejects_an_explicit_pipeline_engine_level() -> Result<()> {
        validate_collector_pipeline_log_level("engine: {}\n")?;
        let error = validate_collector_pipeline_log_level("engine:\n  telemetry:\n    logs:\n      level: debug\n");
        assert!(error.is_err());
        let mut config = config()?;
        config.tunables.logging = Some(arrowcol_tunables::LoggingTunables {
            collector: Some(arrowcol_tunables::LogLevel::Debug),
            ..Default::default()
        });
        assert!(role_environment(&config, ComponentRole::Collector).is_ok());
        Ok(())
    }

    #[test]
    fn generated_partition_fast_path_is_injected_and_read_back() -> Result<()> {
        const ENV: &str = "ARROWCOL_COLLECTOR_GENERATED_PARTITION_FAST_PATH";
        for enabled in [false, true] {
            let mut config = config()?;
            config.tunables = arrowcol_tunables::Tunables::parse_oat_json(json!({
                "collector":{"runtime":{"generated_partition_fast_path":enabled}}
            }))?;
            let environment = role_environment(&config, ComponentRole::Collector)?;
            assert_eq!(environment[ENV], enabled.to_string());
            let compose = compose_tunables(&config)?;
            assert_eq!(compose["services"]["dataflow-1"]["environment"][ENV], enabled.to_string());
            let mut deployment = json!({"spec":{"template":{"spec":{"containers":[{"name":"dataflow"}]}}}});
            inject_deployment_environment(&mut deployment, "dataflow", &environment)?;
            assert!(deployment["spec"]["template"]["spec"]["containers"][0]["env"]
                .as_array().context("deployment environment")?
                .iter().any(|entry| entry["name"] == ENV && entry["value"].as_str() == Some(environment[ENV].as_str())));
            let actual = json!({"runtime":{"generated_partition_fast_path":enabled}});
            verify_process_configuration(&config, ComponentRole::Collector, &actual)?;
            assert!(verify_process_configuration(
                &config, ComponentRole::Collector,
                &json!({"runtime":{"generated_partition_fast_path":!enabled}})
            ).is_err());
            assert!(verify_process_configuration(&config, ComponentRole::Collector, &json!({})).is_err());
        }
        assert!(arrowcol_tunables::Tunables::parse_oat_json(json!({
            "collector":{"runtime":{"generated_partition_fast_path":"true"}}
        })).is_err());
        Ok(())
    }

    #[test]
    fn aged_batching_policy_reaches_compose_aks_and_effective_readback() -> Result<()> {
        let mut config = config()?;
        config.tunables = arrowcol_tunables::Tunables::parse_oat_json(json!({
            "collector":{"batching":{"age_flush_policy":"drain_ready"}}
        }))?;
        let environment = role_environment(&config, ComponentRole::Collector)?;
        let requested: Value = serde_json::from_str(&environment["ARROWCOL_COLLECTOR_BATCHING_CONFIG"])?;
        assert_eq!(requested, json!({"age_flush_policy":"drain_ready"}));
        let compose = compose_tunables(&config)?;
        assert_eq!(compose["services"]["dataflow-1"]["environment"], json!(environment));
        let mut resources = vec![json!({
            "kind":"Deployment", "metadata":{"name":config.dfe_deployment},
            "spec":{"template":{"spec":{"containers":[{"name":config.dfe_container,"env":[]}]}}}
        })];
        inject_workload_environment(&config, &mut resources)?;
        let injected = resources[0].pointer("/spec/template/spec/containers/0/env")
            .and_then(Value::as_array).context("injected environment")?;
        assert!(injected.iter().any(|entry| entry["name"] == "ARROWCOL_COLLECTOR_BATCHING_CONFIG"
            && entry["value"] == environment["ARROWCOL_COLLECTOR_BATCHING_CONFIG"]));
        let startup = |policy: &str| format!("arrowcol_effective_config {}\n", json!({
            "component":"exporter","config":{"batching":{"age_flush_policy":policy,"max_age":"30s"}}
        }));
        let actual = process_configurations(&startup("drain_ready"))?;
        verify_process_configuration(&config, ComponentRole::Collector, &actual)?;
        assert_eq!(actual["batching"]["age_flush_policy"], requested["age_flush_policy"]);
        for policy in ["immediate", "drain_forever"] {
            assert!(verify_process_configuration(
                &config, ComponentRole::Collector, &process_configurations(&startup(policy))?
            ).is_err());
        }
        assert!(verify_process_configuration(&config, ComponentRole::Collector, &json!({})).is_err());
        Ok(())
    }

    #[test]
    fn typed_nondefault_client_value_has_identical_compose_and_aks_injection() -> Result<()> {
        let mut config = config()?;
        config.tunables = serde_json::from_value(json!({ "client": { "batch_rows": 257 } }))?;
        let compose = compose_tunables(&config)?;
        assert_eq!(
            compose["services"]["oat-client"]["environment"]["ARROWCOL_CLIENT_BATCH_ROWS"],
            "257"
        );
        assert_eq!(compose["services"]["router"]["environment"]["RUST_LOG"], "warn,tiberius=error");
        let mut client = json!({ "env": [{ "name": "ARROWCOL_OAT_RUN_ID", "value": "owned" }] });
        inject_container_environment(&mut client, &role_environment(&config, ComponentRole::Client)?)?;
        assert_eq!(client["env"][1]["value"], "257");
        assert_eq!(client["env"][0]["value"], "owned");
        Ok(())
    }

    #[test]
    fn dmv_transport_defaults_remain_valid_through_compose_and_aks_projection() -> Result<()> {
        let mut config = config()?;
        config.tunables = serde_json::from_value(json!({"transport":{"max_message_bytes":4_194_304}}))?;
        config.validate()?;
        assert!(config.tunables.validate().is_err());
        let environment = role_environment(&config, ComponentRole::Client)?;
        assert!(environment.values().any(|value| value == "4194304"));
        let compose = compose_tunables(&config)?;
        assert_eq!(compose["services"]["oat-client"]["environment"], json!(environment));
        let mut client = json!({"env":[]});
        inject_container_environment(&mut client, &environment)?;
        let expected = environment
            .iter()
            .map(|(name, value)| json!({"name":name,"value":value}))
            .collect::<Vec<_>>();
        assert_eq!(client["env"], json!(expected));
        Ok(())
    }

    #[test]
    fn local_overlay_resolves_all_oat_layout_aliases_without_conflicts() -> Result<()> {
        let mut config = config()?;
        config.tunables = serde_json::from_value(json!({
            "collector": {
                "layout": {
                    "enabled": true,
                    "sort_columns": ["session_id"],
                    "sort_options": [{"column":"session_id","direction":"descending","nulls":"first"}],
                    "bloom_columns": ["database_name"],
                    "max_rows": 4096,
                    "max_input_bytes": 16777216,
                    "row_group_rows": 1024,
                    "page_rows": 256
                }
            }
        }))?;
        let mut overlay = compose_tunables(&config)?;
        local_dataflow_overlay(&config, Path::new("/owned/run"), &mut overlay)?;
        assert_eq!(
            overlay["services"]["dataflow-1"]["ports"],
            json!([{"host_ip":"127.0.0.1","target":32011,"published":"0","protocol":"tcp"}])
        );
        let environment = overlay["services"]["dataflow-1"]["environment"]
            .as_object()
            .context("local collector environment must be an object")?
            .iter()
            .map(|(name, value)| {
                Ok((
                    name.clone(),
                    value.as_str()
                        .context("local collector environment values must be strings")?
                        .to_owned(),
                ))
            })
            .collect::<Result<std::collections::BTreeMap<_, _>>>()?;
        let effective =
            arrow_deltalake::delta::resolve_process_writer_properties_from_environment(&environment)?;
        let aliases = arrow_deltalake::delta::PROCESS_WRITER_PROPERTY_ENVIRONMENT_ALIASES;
        assert_eq!(
            aliases.iter().filter(|(_, canonical, _)| environment.contains_key(*canonical)).count(),
            aliases.len(),
            "the test configuration should set every aliased layout knob"
        );
        for &(legacy, canonical, property) in aliases {
            let value = environment.get(canonical).context("missing canonical OAT layout value")?;
            assert_eq!(environment.get(legacy), Some(value));
            assert_eq!(effective.get(property), Some(value));
        }
        assert_eq!(effective["arrowcol.layout.rowGroupRows"], "1024");
        assert_eq!(effective["arrowcol.layout.pageRows"], "256");
        Ok(())
    }

    #[test]
    fn injection_preserves_identity_and_refuses_ambiguous_targets() -> Result<()> {
        let variables = std::collections::BTreeMap::from([("ARROWCOL_CLIENT_BATCH_ROWS".to_owned(), "257".to_owned())]);
        let mut workload = json!({"spec":{"template":{"spec":{"containers":[{"name":"dataflow","env":[
            {"name":"AZURE_CLIENT_ID","valueFrom":{"secretKeyRef":{"name":"identity","key":"client"}}},
            {"name":"ARROWCOL_CLIENT_BATCH_ROWS","value":"1"}
        ]}]}}}});
        inject_deployment_environment(&mut workload, "dataflow", &variables)?;
        assert!(
            workload
                .pointer("/spec/template/spec/containers/0/env/0/valueFrom")
                .is_some()
        );
        assert_eq!(
            workload
                .pointer("/spec/template/spec/containers/0/env/1/value")
                .and_then(Value::as_str),
            Some("257")
        );
        assert!(inject_deployment_environment(&mut workload, "missing", &variables).is_err());
        Ok(())
    }

    #[test]
    fn empty_layout_json_reaches_resolved_policy() -> Result<()> {
        if std::env::var_os("OAT_R1_CHILD").is_some() {
            let config = arrow_deltalake::delta::effective_layout_startup_config()?;
            assert_eq!(config["process_policy"]["enabled"], true);
            assert_eq!(config["process_policy"]["sort_columns"], json!([]));
            assert_eq!(config["process_policy"]["bloom_columns"], json!([]));
            return Ok(());
        }
        let tunables = arrowcol_tunables::Tunables::parse_oat_json(json!({
            "collector": {"layout": {"enabled": true, "sort_columns": [], "bloom_columns": []}}
        }))?;
        let environment = tunables.environment_for(ComponentRole::Collector)?;
        assert_eq!(environment["ARROWCOL_COLLECTOR_SORT_COLUMNS"], "");
        assert_eq!(environment["ARROWCOL_COLLECTOR_BLOOM_COLUMNS"], "");
        let mut command = std::process::Command::new(std::env::current_exe()?);
        for (legacy, canonical, _) in arrow_deltalake::delta::PROCESS_WRITER_PROPERTY_ENVIRONMENT_ALIASES {
            command.env_remove(legacy).env_remove(canonical);
        }
        let status = command
            .envs(environment)
            .env("OAT_R1_CHILD", "1")
            .args([
                "--exact",
                "runner::tunable_injection_tests::empty_layout_json_reaches_resolved_policy",
                "--nocapture",
            ])
            .status()?;
        assert!(status.success());
        Ok(())
    }

    #[test]
    fn local_quiver_storage_is_run_owned_and_partitioned_by_process_instance() -> Result<()> {
        let mut config = config()?;
        let state = Path::new("/owned/run");
        let mut overlay = compose_tunables(&config)?;
        local_dataflow_overlay(&config, state, &mut overlay)?;
        assert_eq!(
            overlay["services"]["dataflow-1"]["volumes"]
                .as_array()
                .context("missing mounts")?
                .len(),
            1
        );
        config.tunables = serde_json::from_value(json!({"wal":{"flush_interval_ms":7}}))?;
        let mut overlay = compose_tunables(&config)?;
        local_dataflow_overlay(&config, state, &mut overlay)?;
        let collector = &overlay["services"]["dataflow-1"];
        assert_eq!(collector["environment"]["ARROWCOL_WAL_FLUSH_INTERVAL"], "7ms");
        assert_eq!(collector["volumes"][1]["source"], "/owned/run/quiver");
        assert_eq!(collector["volumes"][1]["bind"]["create_host_path"], false);
        assert!(
            collector["command"][0]
                .as_str()
                .context("missing command")?
                .contains("/oat-quiver/$(hostname)")
        );
        Ok(())
    }
}
