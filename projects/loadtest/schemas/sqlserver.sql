-- Versioned public synthetic SQL Server DMV schemas used by the load benchmark.
-- Fields are ordered and required; schema registration is performed through Flight SQL.

CREATE TABLE telemetry.sqlserver.sqlserver_active_sessions (
    customer_id VARCHAR NOT NULL,
    sample_time_utc TIMESTAMP(9) WITH TIME ZONE NOT NULL,
    sql_server_instance_name VARCHAR NOT NULL,
    machine_name VARCHAR NOT NULL,
    database_id SMALLINT NOT NULL,
    database_name VARCHAR NOT NULL,
    session_id SMALLINT NOT NULL,
    session_status VARCHAR NOT NULL,
    connection_id VARCHAR NOT NULL
);

CREATE TABLE telemetry.sqlserver.sqlserver_availability_group_states (
    customer_id VARCHAR NOT NULL,
    sample_time_utc TIMESTAMP(9) WITH TIME ZONE NOT NULL,
    sql_server_instance_name VARCHAR NOT NULL,
    machine_name VARCHAR NOT NULL,
    availability_group_id VARCHAR NOT NULL,
    availability_group_name VARCHAR NOT NULL,
    failure_condition_level INT NOT NULL,
    automated_backup_preference_desc VARCHAR NOT NULL,
    is_distributed BOOLEAN NOT NULL,
    primary_replica VARCHAR NOT NULL,
    primary_recovery_health_desc VARCHAR NOT NULL,
    secondary_recovery_health_desc VARCHAR NOT NULL,
    synchronization_health_desc VARCHAR NOT NULL
);

CREATE TABLE telemetry.sqlserver.sqlserver_availability_replica_states (
    customer_id VARCHAR NOT NULL,
    sample_time_utc TIMESTAMP(9) WITH TIME ZONE NOT NULL,
    sql_server_instance_name VARCHAR NOT NULL,
    machine_name VARCHAR NOT NULL,
    availability_group_id VARCHAR NOT NULL,
    availability_group_name VARCHAR NOT NULL,
    replica_server_name VARCHAR NOT NULL,
    replica_role VARCHAR NOT NULL,
    operational_state_desc VARCHAR NOT NULL,
    recovery_health_desc VARCHAR NOT NULL,
    synchronization_health_desc VARCHAR NOT NULL,
    connected_state_desc VARCHAR NOT NULL,
    last_connect_error_number INT NOT NULL,
    last_connect_error_timestamp TIMESTAMP(9) WITH TIME ZONE NOT NULL,
    endpoint_url VARCHAR NOT NULL,
    availability_mode_desc VARCHAR NOT NULL,
    failover_mode_desc VARCHAR NOT NULL,
    primary_role_allow_connections_desc VARCHAR NOT NULL,
    secondary_role_allow_connections_desc VARCHAR NOT NULL,
    availability_replica_create_date TIMESTAMP(9) WITH TIME ZONE NOT NULL,
    availability_replica_modify_date TIMESTAMP(9) WITH TIME ZONE NOT NULL,
    seeding_mode_desc VARCHAR NOT NULL
);

CREATE TABLE telemetry.sqlserver.sqlserver_client_connections (
    customer_id VARCHAR NOT NULL,
    sample_time_utc TIMESTAMP(9) WITH TIME ZONE NOT NULL,
    sql_server_instance_name VARCHAR NOT NULL,
    machine_name VARCHAR NOT NULL,
    host_name VARCHAR NOT NULL,
    program_name VARCHAR NOT NULL,
    client_interface_name VARCHAR NOT NULL,
    database_name VARCHAR NOT NULL,
    request_end_time TIMESTAMP(9) WITH TIME ZONE NOT NULL,
    total_writes BIGINT NOT NULL,
    total_reads BIGINT NOT NULL,
    elapsed_time_ms INT NOT NULL,
    connection_login_time TIMESTAMP(9) WITH TIME ZONE NOT NULL,
    connection_count INT NOT NULL
);

CREATE TABLE telemetry.sqlserver.sqlserver_cpu_utilization (
    customer_id VARCHAR NOT NULL,
    sample_time_utc TIMESTAMP(9) WITH TIME ZONE NOT NULL,
    process_sample_time_utc TIMESTAMP(9) WITH TIME ZONE NOT NULL,
    sql_server_instance_name VARCHAR NOT NULL,
    machine_name VARCHAR NOT NULL,
    avg_cpu_percent DOUBLE NOT NULL,
    sql_process_cpu_percent INT NOT NULL,
    idle_cpu_percent INT NOT NULL
);

CREATE TABLE telemetry.sqlserver.sqlserver_database_properties (
    customer_id VARCHAR NOT NULL,
    sample_time_utc TIMESTAMP(9) WITH TIME ZONE NOT NULL,
    collection_time_utc TIMESTAMP(9) WITH TIME ZONE NOT NULL,
    sql_server_instance_name VARCHAR NOT NULL,
    machine_name VARCHAR NOT NULL,
    database_id SMALLINT NOT NULL,
    database_name VARCHAR NOT NULL,
    is_primary_replica BOOLEAN NOT NULL,
    create_date TIMESTAMP(9) WITH TIME ZONE NOT NULL,
    compatibility_level SMALLINT NOT NULL,
    collation_name VARCHAR NOT NULL,
    user_access_desc VARCHAR NOT NULL,
    is_read_only BOOLEAN NOT NULL,
    updateability VARCHAR NOT NULL,
    is_auto_shrink_on BOOLEAN NOT NULL,
    state_desc VARCHAR NOT NULL,
    is_in_standby BOOLEAN NOT NULL,
    snapshot_isolation_state TINYINT UNSIGNED NOT NULL,
    is_read_committed_snapshot_on BOOLEAN NOT NULL,
    recovery_model_desc VARCHAR NOT NULL,
    page_verify_option_desc VARCHAR NOT NULL,
    is_auto_create_stats_on BOOLEAN NOT NULL,
    is_auto_update_stats_on BOOLEAN NOT NULL,
    is_auto_update_stats_async_on BOOLEAN NOT NULL,
    is_parameterization_forced BOOLEAN NOT NULL,
    is_published BOOLEAN NOT NULL,
    is_subscribed BOOLEAN NOT NULL,
    is_merge_published BOOLEAN NOT NULL,
    is_distributor BOOLEAN NOT NULL,
    is_broker_enabled BOOLEAN NOT NULL,
    log_reuse_wait_desc VARCHAR NOT NULL,
    containment_desc VARCHAR NOT NULL,
    is_cdc_enabled BOOLEAN NOT NULL,
    is_encrypted BOOLEAN NOT NULL,
    delayed_durability_desc VARCHAR NOT NULL,
    is_accelerated_database_recovery_on BOOLEAN NOT NULL,
    last_good_checkdb_time TIMESTAMP(9) WITH TIME ZONE NOT NULL,
    notable_db_scoped_configs VARCHAR NOT NULL,
    query_store_actual_state_desc VARCHAR NOT NULL,
    query_store_query_capture_mode_desc VARCHAR NOT NULL,
    force_last_good_plan_actual_state VARCHAR NOT NULL,
    count_suspect_pages INT NOT NULL,
    is_ledger_on BOOLEAN NOT NULL,
    is_change_feed_enabled BOOLEAN NOT NULL
);

CREATE TABLE telemetry.sqlserver.sqlserver_database_replica_states (
    customer_id VARCHAR NOT NULL,
    sample_time_utc TIMESTAMP(9) WITH TIME ZONE NOT NULL,
    sql_server_instance_name VARCHAR NOT NULL,
    machine_name VARCHAR NOT NULL,
    database_id INT NOT NULL,
    database_name VARCHAR NOT NULL,
    availability_group_id VARCHAR NOT NULL,
    replica_id VARCHAR NOT NULL,
    group_database_id VARCHAR NOT NULL,
    is_local BOOLEAN NOT NULL,
    is_primary_replica BOOLEAN NOT NULL,
    synchronization_state TINYINT UNSIGNED NOT NULL,
    is_commit_participant BOOLEAN NOT NULL,
    synchronization_health TINYINT UNSIGNED NOT NULL,
    database_state TINYINT UNSIGNED NOT NULL,
    is_suspended BOOLEAN NOT NULL,
    suspend_reason TINYINT UNSIGNED NOT NULL,
    recovery_lsn_str VARCHAR NOT NULL,
    truncation_lsn_str VARCHAR NOT NULL,
    last_sent_lsn_str VARCHAR NOT NULL,
    last_sent_time TIMESTAMP(9) WITH TIME ZONE NOT NULL,
    last_received_lsn_str VARCHAR NOT NULL,
    last_received_time TIMESTAMP(9) WITH TIME ZONE NOT NULL,
    last_hardened_lsn_str VARCHAR NOT NULL,
    last_hardened_time TIMESTAMP(9) WITH TIME ZONE NOT NULL,
    last_redone_lsn_str VARCHAR NOT NULL,
    last_redone_time TIMESTAMP(9) WITH TIME ZONE NOT NULL,
    log_send_queue_size_kb BIGINT NOT NULL,
    log_send_rate BIGINT NOT NULL,
    redo_queue_size_kb BIGINT NOT NULL,
    redo_rate BIGINT NOT NULL,
    end_of_log_lsn_str VARCHAR NOT NULL,
    last_commit_lsn_str VARCHAR NOT NULL,
    last_commit_time TIMESTAMP(9) WITH TIME ZONE NOT NULL,
    secondary_lag_seconds BIGINT NOT NULL,
    quorum_commit_lsn_str VARCHAR NOT NULL,
    quorum_commit_time TIMESTAMP(9) WITH TIME ZONE NOT NULL
);

CREATE TABLE telemetry.sqlserver.sqlserver_database_storage_utilization (
    customer_id VARCHAR NOT NULL,
    sample_time_utc TIMESTAMP(9) WITH TIME ZONE NOT NULL,
    collection_time_utc TIMESTAMP(9) WITH TIME ZONE NOT NULL,
    sql_server_instance_name VARCHAR NOT NULL,
    machine_name VARCHAR NOT NULL,
    database_id SMALLINT NOT NULL,
    database_name VARCHAR NOT NULL,
    is_primary_replica BOOLEAN NOT NULL,
    data_size_used_mb DOUBLE NOT NULL,
    data_size_allocated_mb DOUBLE NOT NULL,
    count_data_files INT NOT NULL,
    log_size_used_mb DOUBLE NOT NULL,
    log_size_allocated_mb DOUBLE NOT NULL,
    count_log_files INT NOT NULL,
    persistent_version_store_size_mb DOUBLE NOT NULL,
    online_index_version_store_size_mb DOUBLE NOT NULL
);

CREATE TABLE telemetry.sqlserver.sqlserver_memory_utilization (
    customer_id VARCHAR NOT NULL,
    sample_time_utc TIMESTAMP(9) WITH TIME ZONE NOT NULL,
    sql_server_instance_name VARCHAR NOT NULL,
    machine_name VARCHAR NOT NULL,
    memory_clerk_type VARCHAR NOT NULL,
    memory_clerk_name VARCHAR NOT NULL,
    memory_size_mb DOUBLE NOT NULL,
    virtual_memory_committed_mb DOUBLE NOT NULL
);

CREATE TABLE telemetry.sqlserver.sqlserver_performance_counters_common (
    customer_id VARCHAR NOT NULL,
    sample_time_utc TIMESTAMP(9) WITH TIME ZONE NOT NULL,
    sql_server_instance_name VARCHAR NOT NULL,
    machine_name VARCHAR NOT NULL,
    object_name VARCHAR NOT NULL,
    counter_name VARCHAR NOT NULL,
    instance_name VARCHAR NOT NULL,
    database_id INT NOT NULL,
    database_name VARCHAR NOT NULL,
    cntr_value DOUBLE NOT NULL,
    cntr_type INT NOT NULL
);

CREATE TABLE telemetry.sqlserver.sqlserver_performance_counters_detailed (
    customer_id VARCHAR NOT NULL,
    sample_time_utc TIMESTAMP(9) WITH TIME ZONE NOT NULL,
    sql_server_instance_name VARCHAR NOT NULL,
    machine_name VARCHAR NOT NULL,
    object_name VARCHAR NOT NULL,
    counter_name VARCHAR NOT NULL,
    instance_name VARCHAR NOT NULL,
    database_id INT NOT NULL,
    database_name VARCHAR NOT NULL,
    cntr_value DOUBLE NOT NULL,
    cntr_type INT NOT NULL
);

CREATE TABLE telemetry.sqlserver.sqlserver_storage_io (
    customer_id VARCHAR NOT NULL,
    sample_time_utc TIMESTAMP(9) WITH TIME ZONE NOT NULL,
    sql_server_instance_name VARCHAR NOT NULL,
    machine_name VARCHAR NOT NULL,
    database_id SMALLINT NOT NULL,
    database_name VARCHAR NOT NULL,
    file_id SMALLINT NOT NULL,
    file_type VARCHAR NOT NULL,
    file_size_mb DOUBLE NOT NULL,
    file_max_size_mb DOUBLE NOT NULL,
    io_stall_read_ms BIGINT NOT NULL,
    num_of_reads BIGINT NOT NULL,
    num_of_bytes_read BIGINT NOT NULL,
    io_stall_write_ms BIGINT NOT NULL,
    num_of_writes BIGINT NOT NULL,
    num_of_bytes_written BIGINT NOT NULL,
    io_stall_queued_read_ms BIGINT NOT NULL,
    io_stall_queued_write_ms BIGINT NOT NULL,
    size_on_disk_bytes BIGINT NOT NULL
);

CREATE TABLE telemetry.sqlserver.sqlserver_wait_stats (
    customer_id VARCHAR NOT NULL,
    sample_time_utc TIMESTAMP(9) WITH TIME ZONE NOT NULL,
    sql_server_instance_name VARCHAR NOT NULL,
    machine_name VARCHAR NOT NULL,
    wait_type VARCHAR NOT NULL,
    wait_time_ms BIGINT NOT NULL,
    resource_wait_time_ms BIGINT NOT NULL,
    signal_wait_time_ms BIGINT NOT NULL,
    max_wait_time_ms BIGINT NOT NULL,
    waiting_tasks_count BIGINT NOT NULL,
    wait_category VARCHAR NOT NULL
);
