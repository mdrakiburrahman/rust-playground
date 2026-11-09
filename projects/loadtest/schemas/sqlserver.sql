-- Versioned public synthetic SQL Server DMV schemas used by the load benchmark.
-- Fields are ordered and required; schema registration is performed through Flight SQL.

CREATE TABLE telemetry.sqlserver.sqlserver_active_sessions (
    customer_id STRING NOT NULL,
    sample_time_utc TIMESTAMP NOT NULL,
    sql_server_instance_name STRING NOT NULL,
    machine_name STRING NOT NULL,
    database_id SMALLINT NOT NULL,
    database_name STRING NOT NULL,
    session_id SMALLINT NOT NULL,
    session_status STRING NOT NULL,
    connection_id STRING NOT NULL
) USING DELTA;

CREATE TABLE telemetry.sqlserver.sqlserver_availability_group_states (
    customer_id STRING NOT NULL,
    sample_time_utc TIMESTAMP NOT NULL,
    sql_server_instance_name STRING NOT NULL,
    machine_name STRING NOT NULL,
    availability_group_id STRING NOT NULL,
    availability_group_name STRING NOT NULL,
    failure_condition_level INT NOT NULL,
    automated_backup_preference_desc STRING NOT NULL,
    is_distributed BOOLEAN NOT NULL,
    primary_replica STRING NOT NULL,
    primary_recovery_health_desc STRING NOT NULL,
    secondary_recovery_health_desc STRING NOT NULL,
    synchronization_health_desc STRING NOT NULL
) USING DELTA;

CREATE TABLE telemetry.sqlserver.sqlserver_availability_replica_states (
    customer_id STRING NOT NULL,
    sample_time_utc TIMESTAMP NOT NULL,
    sql_server_instance_name STRING NOT NULL,
    machine_name STRING NOT NULL,
    availability_group_id STRING NOT NULL,
    availability_group_name STRING NOT NULL,
    replica_server_name STRING NOT NULL,
    replica_role STRING NOT NULL,
    operational_state_desc STRING NOT NULL,
    recovery_health_desc STRING NOT NULL,
    synchronization_health_desc STRING NOT NULL,
    connected_state_desc STRING NOT NULL,
    last_connect_error_number INT NOT NULL,
    last_connect_error_timestamp TIMESTAMP NOT NULL,
    endpoint_url STRING NOT NULL,
    availability_mode_desc STRING NOT NULL,
    failover_mode_desc STRING NOT NULL,
    primary_role_allow_connections_desc STRING NOT NULL,
    secondary_role_allow_connections_desc STRING NOT NULL,
    availability_replica_create_date TIMESTAMP NOT NULL,
    availability_replica_modify_date TIMESTAMP NOT NULL,
    seeding_mode_desc STRING NOT NULL
) USING DELTA;

CREATE TABLE telemetry.sqlserver.sqlserver_client_connections (
    customer_id STRING NOT NULL,
    sample_time_utc TIMESTAMP NOT NULL,
    sql_server_instance_name STRING NOT NULL,
    machine_name STRING NOT NULL,
    host_name STRING NOT NULL,
    program_name STRING NOT NULL,
    client_interface_name STRING NOT NULL,
    database_name STRING NOT NULL,
    request_end_time TIMESTAMP NOT NULL,
    total_writes BIGINT NOT NULL,
    total_reads BIGINT NOT NULL,
    elapsed_time_ms INT NOT NULL,
    connection_login_time TIMESTAMP NOT NULL,
    connection_count INT NOT NULL
) USING DELTA;

CREATE TABLE telemetry.sqlserver.sqlserver_cpu_utilization (
    customer_id STRING NOT NULL,
    sample_time_utc TIMESTAMP NOT NULL,
    process_sample_time_utc TIMESTAMP NOT NULL,
    sql_server_instance_name STRING NOT NULL,
    machine_name STRING NOT NULL,
    avg_cpu_percent DOUBLE NOT NULL,
    sql_process_cpu_percent INT NOT NULL,
    idle_cpu_percent INT NOT NULL
) USING DELTA;

CREATE TABLE telemetry.sqlserver.sqlserver_database_properties (
    customer_id STRING NOT NULL,
    sample_time_utc TIMESTAMP NOT NULL,
    collection_time_utc TIMESTAMP NOT NULL,
    sql_server_instance_name STRING NOT NULL,
    machine_name STRING NOT NULL,
    database_id SMALLINT NOT NULL,
    database_name STRING NOT NULL,
    is_primary_replica BOOLEAN NOT NULL,
    create_date TIMESTAMP NOT NULL,
    compatibility_level SMALLINT NOT NULL,
    collation_name STRING NOT NULL,
    user_access_desc STRING NOT NULL,
    is_read_only BOOLEAN NOT NULL,
    updateability STRING NOT NULL,
    is_auto_shrink_on BOOLEAN NOT NULL,
    state_desc STRING NOT NULL,
    is_in_standby BOOLEAN NOT NULL,
    snapshot_isolation_state SMALLINT NOT NULL,
    is_read_committed_snapshot_on BOOLEAN NOT NULL,
    recovery_model_desc STRING NOT NULL,
    page_verify_option_desc STRING NOT NULL,
    is_auto_create_stats_on BOOLEAN NOT NULL,
    is_auto_update_stats_on BOOLEAN NOT NULL,
    is_auto_update_stats_async_on BOOLEAN NOT NULL,
    is_parameterization_forced BOOLEAN NOT NULL,
    is_published BOOLEAN NOT NULL,
    is_subscribed BOOLEAN NOT NULL,
    is_merge_published BOOLEAN NOT NULL,
    is_distributor BOOLEAN NOT NULL,
    is_broker_enabled BOOLEAN NOT NULL,
    log_reuse_wait_desc STRING NOT NULL,
    containment_desc STRING NOT NULL,
    is_cdc_enabled BOOLEAN NOT NULL,
    is_encrypted BOOLEAN NOT NULL,
    delayed_durability_desc STRING NOT NULL,
    is_accelerated_database_recovery_on BOOLEAN NOT NULL,
    last_good_checkdb_time TIMESTAMP NOT NULL,
    notable_db_scoped_configs STRING NOT NULL,
    query_store_actual_state_desc STRING NOT NULL,
    query_store_query_capture_mode_desc STRING NOT NULL,
    force_last_good_plan_actual_state STRING NOT NULL,
    count_suspect_pages INT NOT NULL,
    is_ledger_on BOOLEAN NOT NULL,
    is_change_feed_enabled BOOLEAN NOT NULL
) USING DELTA;

CREATE TABLE telemetry.sqlserver.sqlserver_database_replica_states (
    customer_id STRING NOT NULL,
    sample_time_utc TIMESTAMP NOT NULL,
    sql_server_instance_name STRING NOT NULL,
    machine_name STRING NOT NULL,
    database_id INT NOT NULL,
    database_name STRING NOT NULL,
    availability_group_id STRING NOT NULL,
    replica_id STRING NOT NULL,
    group_database_id STRING NOT NULL,
    is_local BOOLEAN NOT NULL,
    is_primary_replica BOOLEAN NOT NULL,
    synchronization_state SMALLINT NOT NULL,
    is_commit_participant BOOLEAN NOT NULL,
    synchronization_health SMALLINT NOT NULL,
    database_state SMALLINT NOT NULL,
    is_suspended BOOLEAN NOT NULL,
    suspend_reason SMALLINT NOT NULL,
    recovery_lsn_str STRING NOT NULL,
    truncation_lsn_str STRING NOT NULL,
    last_sent_lsn_str STRING NOT NULL,
    last_sent_time TIMESTAMP NOT NULL,
    last_received_lsn_str STRING NOT NULL,
    last_received_time TIMESTAMP NOT NULL,
    last_hardened_lsn_str STRING NOT NULL,
    last_hardened_time TIMESTAMP NOT NULL,
    last_redone_lsn_str STRING NOT NULL,
    last_redone_time TIMESTAMP NOT NULL,
    log_send_queue_size_kb BIGINT NOT NULL,
    log_send_rate BIGINT NOT NULL,
    redo_queue_size_kb BIGINT NOT NULL,
    redo_rate BIGINT NOT NULL,
    end_of_log_lsn_str STRING NOT NULL,
    last_commit_lsn_str STRING NOT NULL,
    last_commit_time TIMESTAMP NOT NULL,
    secondary_lag_seconds BIGINT NOT NULL,
    quorum_commit_lsn_str STRING NOT NULL,
    quorum_commit_time TIMESTAMP NOT NULL
) USING DELTA;

CREATE TABLE telemetry.sqlserver.sqlserver_database_storage_utilization (
    customer_id STRING NOT NULL,
    sample_time_utc TIMESTAMP NOT NULL,
    collection_time_utc TIMESTAMP NOT NULL,
    sql_server_instance_name STRING NOT NULL,
    machine_name STRING NOT NULL,
    database_id SMALLINT NOT NULL,
    database_name STRING NOT NULL,
    is_primary_replica BOOLEAN NOT NULL,
    data_size_used_mb DOUBLE NOT NULL,
    data_size_allocated_mb DOUBLE NOT NULL,
    count_data_files INT NOT NULL,
    log_size_used_mb DOUBLE NOT NULL,
    log_size_allocated_mb DOUBLE NOT NULL,
    count_log_files INT NOT NULL,
    persistent_version_store_size_mb DOUBLE NOT NULL,
    online_index_version_store_size_mb DOUBLE NOT NULL
) USING DELTA;

CREATE TABLE telemetry.sqlserver.sqlserver_memory_utilization (
    customer_id STRING NOT NULL,
    sample_time_utc TIMESTAMP NOT NULL,
    sql_server_instance_name STRING NOT NULL,
    machine_name STRING NOT NULL,
    memory_clerk_type STRING NOT NULL,
    memory_clerk_name STRING NOT NULL,
    memory_size_mb DOUBLE NOT NULL,
    virtual_memory_committed_mb DOUBLE NOT NULL
) USING DELTA;

CREATE TABLE telemetry.sqlserver.sqlserver_performance_counters_common (
    customer_id STRING NOT NULL,
    sample_time_utc TIMESTAMP NOT NULL,
    sql_server_instance_name STRING NOT NULL,
    machine_name STRING NOT NULL,
    object_name STRING NOT NULL,
    counter_name STRING NOT NULL,
    instance_name STRING NOT NULL,
    database_id INT NOT NULL,
    database_name STRING NOT NULL,
    cntr_value DOUBLE NOT NULL,
    cntr_type INT NOT NULL
) USING DELTA;

CREATE TABLE telemetry.sqlserver.sqlserver_performance_counters_detailed (
    customer_id STRING NOT NULL,
    sample_time_utc TIMESTAMP NOT NULL,
    sql_server_instance_name STRING NOT NULL,
    machine_name STRING NOT NULL,
    object_name STRING NOT NULL,
    counter_name STRING NOT NULL,
    instance_name STRING NOT NULL,
    database_id INT NOT NULL,
    database_name STRING NOT NULL,
    cntr_value DOUBLE NOT NULL,
    cntr_type INT NOT NULL
) USING DELTA;

CREATE TABLE telemetry.sqlserver.sqlserver_storage_io (
    customer_id STRING NOT NULL,
    sample_time_utc TIMESTAMP NOT NULL,
    sql_server_instance_name STRING NOT NULL,
    machine_name STRING NOT NULL,
    database_id SMALLINT NOT NULL,
    database_name STRING NOT NULL,
    file_id SMALLINT NOT NULL,
    file_type STRING NOT NULL,
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
) USING DELTA;

CREATE TABLE telemetry.sqlserver.sqlserver_wait_stats (
    customer_id STRING NOT NULL,
    sample_time_utc TIMESTAMP NOT NULL,
    sql_server_instance_name STRING NOT NULL,
    machine_name STRING NOT NULL,
    wait_type STRING NOT NULL,
    wait_time_ms BIGINT NOT NULL,
    resource_wait_time_ms BIGINT NOT NULL,
    signal_wait_time_ms BIGINT NOT NULL,
    max_wait_time_ms BIGINT NOT NULL,
    waiting_tasks_count BIGINT NOT NULL,
    wait_category STRING NOT NULL
) USING DELTA;

CREATE TABLE telemetry.sqlserver.sqlserver_active_requests (
    customer_id STRING NOT NULL,
    sample_time_utc TIMESTAMP NOT NULL,
    sql_server_instance_name STRING NOT NULL,
    machine_name STRING NOT NULL,
    request_id INT NOT NULL,
    session_id SMALLINT NOT NULL,
    status STRING NOT NULL,
    command STRING NOT NULL,
    database_id SMALLINT NOT NULL,
    blocking_session_id SMALLINT NOT NULL,
    wait_type STRING NOT NULL,
    wait_time_ms BIGINT NOT NULL,
    wait_resource STRING NOT NULL,
    cpu_time_ms BIGINT NOT NULL,
    total_elapsed_time_ms BIGINT NOT NULL,
    reads BIGINT NOT NULL,
    writes BIGINT NOT NULL,
    logical_reads BIGINT NOT NULL,
    transaction_id BIGINT NOT NULL,
    start_time TIMESTAMP NOT NULL,
    sql_handle STRING NOT NULL,
    plan_handle STRING NOT NULL
) USING DELTA;

CREATE TABLE telemetry.sqlserver.sqlserver_blocking_sessions (
    customer_id STRING NOT NULL,
    sample_time_utc TIMESTAMP NOT NULL,
    sql_server_instance_name STRING NOT NULL,
    machine_name STRING NOT NULL,
    session_id SMALLINT NOT NULL,
    blocking_session_id SMALLINT NOT NULL,
    request_id INT NOT NULL,
    database_id SMALLINT NOT NULL,
    wait_type STRING NOT NULL,
    wait_time_ms BIGINT NOT NULL,
    wait_resource STRING NOT NULL,
    status STRING NOT NULL,
    host_name STRING NOT NULL,
    program_name STRING NOT NULL,
    open_transaction_count INT NOT NULL
) USING DELTA;

CREATE TABLE telemetry.sqlserver.sqlserver_database_file_stats (
    customer_id STRING NOT NULL,
    sample_time_utc TIMESTAMP NOT NULL,
    sql_server_instance_name STRING NOT NULL,
    machine_name STRING NOT NULL,
    database_id SMALLINT NOT NULL,
    file_id SMALLINT NOT NULL,
    file_name STRING NOT NULL,
    num_of_reads BIGINT NOT NULL,
    num_of_bytes_read BIGINT NOT NULL,
    io_stall_read_ms BIGINT NOT NULL,
    num_of_writes BIGINT NOT NULL,
    num_of_bytes_written BIGINT NOT NULL,
    io_stall_write_ms BIGINT NOT NULL,
    io_stall_queued_read_ms BIGINT NOT NULL,
    io_stall_queued_write_ms BIGINT NOT NULL,
    size_on_disk_bytes BIGINT NOT NULL
) USING DELTA;

CREATE TABLE telemetry.sqlserver.sqlserver_database_index_operational_stats (
    customer_id STRING NOT NULL,
    sample_time_utc TIMESTAMP NOT NULL,
    sql_server_instance_name STRING NOT NULL,
    machine_name STRING NOT NULL,
    database_id SMALLINT NOT NULL,
    object_id INT NOT NULL,
    index_id INT NOT NULL,
    partition_number INT NOT NULL,
    leaf_insert_count BIGINT NOT NULL,
    leaf_delete_count BIGINT NOT NULL,
    leaf_update_count BIGINT NOT NULL,
    leaf_ghost_count BIGINT NOT NULL,
    range_scan_count BIGINT NOT NULL,
    singleton_lookup_count BIGINT NOT NULL,
    forwarded_fetch_count BIGINT NOT NULL,
    row_lock_count BIGINT NOT NULL,
    row_lock_wait_count BIGINT NOT NULL,
    row_lock_wait_ms BIGINT NOT NULL,
    page_latch_wait_count BIGINT NOT NULL,
    page_latch_wait_ms BIGINT NOT NULL
) USING DELTA;

CREATE TABLE telemetry.sqlserver.sqlserver_database_index_usage (
    customer_id STRING NOT NULL,
    sample_time_utc TIMESTAMP NOT NULL,
    sql_server_instance_name STRING NOT NULL,
    machine_name STRING NOT NULL,
    database_id SMALLINT NOT NULL,
    object_id INT NOT NULL,
    index_id INT NOT NULL,
    index_name STRING NOT NULL,
    user_seeks BIGINT NOT NULL,
    user_scans BIGINT NOT NULL,
    user_lookups BIGINT NOT NULL,
    user_updates BIGINT NOT NULL,
    last_user_seek TIMESTAMP NOT NULL,
    last_user_scan TIMESTAMP NOT NULL,
    last_user_lookup TIMESTAMP NOT NULL,
    last_user_update TIMESTAMP NOT NULL
) USING DELTA;

CREATE TABLE telemetry.sqlserver.sqlserver_database_log_stats (
    customer_id STRING NOT NULL,
    sample_time_utc TIMESTAMP NOT NULL,
    sql_server_instance_name STRING NOT NULL,
    machine_name STRING NOT NULL,
    database_id SMALLINT NOT NULL,
    recovery_model STRING NOT NULL,
    total_log_size_mb DOUBLE NOT NULL,
    active_log_size_mb DOUBLE NOT NULL,
    log_backup_time TIMESTAMP NOT NULL,
    log_since_last_log_backup_mb DOUBLE NOT NULL,
    log_checkpoint_lsn STRING NOT NULL,
    log_recovery_lsn STRING NOT NULL,
    log_min_lsn STRING NOT NULL,
    total_vlf_count INT NOT NULL,
    active_vlf_count INT NOT NULL,
    active_vlf_size_mb DOUBLE NOT NULL,
    log_truncation_holdup_reason STRING NOT NULL
) USING DELTA;

CREATE TABLE telemetry.sqlserver.sqlserver_database_partition_stats (
    customer_id STRING NOT NULL,
    sample_time_utc TIMESTAMP NOT NULL,
    sql_server_instance_name STRING NOT NULL,
    machine_name STRING NOT NULL,
    database_id SMALLINT NOT NULL,
    object_id INT NOT NULL,
    index_id INT NOT NULL,
    partition_number INT NOT NULL,
    row_count BIGINT NOT NULL,
    used_page_count BIGINT NOT NULL,
    reserved_page_count BIGINT NOT NULL,
    in_row_data_page_count BIGINT NOT NULL,
    lob_used_page_count BIGINT NOT NULL,
    row_overflow_used_page_count BIGINT NOT NULL
) USING DELTA;

CREATE TABLE telemetry.sqlserver.sqlserver_latch_stats (
    customer_id STRING NOT NULL,
    sample_time_utc TIMESTAMP NOT NULL,
    sql_server_instance_name STRING NOT NULL,
    machine_name STRING NOT NULL,
    latch_class STRING NOT NULL,
    waiting_requests_count BIGINT NOT NULL,
    wait_time_ms BIGINT NOT NULL,
    max_wait_time_ms BIGINT NOT NULL,
    signal_wait_time_ms BIGINT NOT NULL
) USING DELTA;

CREATE TABLE telemetry.sqlserver.sqlserver_lock_waits (
    customer_id STRING NOT NULL,
    sample_time_utc TIMESTAMP NOT NULL,
    sql_server_instance_name STRING NOT NULL,
    machine_name STRING NOT NULL,
    session_id SMALLINT NOT NULL,
    blocking_session_id SMALLINT NOT NULL,
    exec_context_id INT NOT NULL,
    wait_duration_ms BIGINT NOT NULL,
    wait_type STRING NOT NULL,
    resource_address STRING NOT NULL,
    blocking_task_address STRING NOT NULL,
    resource_description STRING NOT NULL,
    database_id SMALLINT NOT NULL
) USING DELTA;

CREATE TABLE telemetry.sqlserver.sqlserver_locks (
    customer_id STRING NOT NULL,
    sample_time_utc TIMESTAMP NOT NULL,
    sql_server_instance_name STRING NOT NULL,
    machine_name STRING NOT NULL,
    resource_type STRING NOT NULL,
    resource_subtype STRING NOT NULL,
    resource_database_id SMALLINT NOT NULL,
    resource_description STRING NOT NULL,
    resource_associated_entity_id BIGINT NOT NULL,
    request_mode STRING NOT NULL,
    request_status STRING NOT NULL,
    request_session_id SMALLINT NOT NULL,
    request_owner_type STRING NOT NULL,
    request_owner_id BIGINT NOT NULL,
    resource_lock_partition INT NOT NULL
) USING DELTA;

CREATE TABLE telemetry.sqlserver.sqlserver_memory_clerks (
    customer_id STRING NOT NULL,
    sample_time_utc TIMESTAMP NOT NULL,
    sql_server_instance_name STRING NOT NULL,
    machine_name STRING NOT NULL,
    memory_clerk_type STRING NOT NULL,
    memory_clerk_name STRING NOT NULL,
    memory_node_id INT NOT NULL,
    pages_kb BIGINT NOT NULL,
    virtual_memory_reserved_kb BIGINT NOT NULL,
    virtual_memory_committed_kb BIGINT NOT NULL,
    awe_allocated_kb BIGINT NOT NULL,
    shared_memory_reserved_kb BIGINT NOT NULL,
    shared_memory_committed_kb BIGINT NOT NULL
) USING DELTA;

CREATE TABLE telemetry.sqlserver.sqlserver_missing_indexes (
    customer_id STRING NOT NULL,
    sample_time_utc TIMESTAMP NOT NULL,
    sql_server_instance_name STRING NOT NULL,
    machine_name STRING NOT NULL,
    database_id SMALLINT NOT NULL,
    object_id INT NOT NULL,
    index_handle INT NOT NULL,
    statement STRING NOT NULL,
    equality_columns STRING NOT NULL,
    inequality_columns STRING NOT NULL,
    included_columns STRING NOT NULL,
    unique_compiles BIGINT NOT NULL,
    user_seeks BIGINT NOT NULL,
    user_scans BIGINT NOT NULL,
    avg_total_user_cost DOUBLE NOT NULL,
    avg_user_impact DOUBLE NOT NULL,
    last_user_seek TIMESTAMP NOT NULL
) USING DELTA;

CREATE TABLE telemetry.sqlserver.sqlserver_query_memory_grants (
    customer_id STRING NOT NULL,
    sample_time_utc TIMESTAMP NOT NULL,
    sql_server_instance_name STRING NOT NULL,
    machine_name STRING NOT NULL,
    session_id SMALLINT NOT NULL,
    request_id INT NOT NULL,
    scheduler_id INT NOT NULL,
    dop INT NOT NULL,
    request_time TIMESTAMP NOT NULL,
    grant_time TIMESTAMP NOT NULL,
    requested_memory_kb BIGINT NOT NULL,
    required_memory_kb BIGINT NOT NULL,
    granted_memory_kb BIGINT NOT NULL,
    used_memory_kb BIGINT NOT NULL,
    max_used_memory_kb BIGINT NOT NULL,
    wait_time_ms BIGINT NOT NULL,
    resource_semaphore_id INT NOT NULL,
    queue_id INT NOT NULL,
    wait_order INT NOT NULL,
    timeout_sec INT NOT NULL,
    plan_handle STRING NOT NULL,
    sql_handle STRING NOT NULL
) USING DELTA;

CREATE TABLE telemetry.sqlserver.sqlserver_query_stats (
    customer_id STRING NOT NULL,
    sample_time_utc TIMESTAMP NOT NULL,
    sql_server_instance_name STRING NOT NULL,
    machine_name STRING NOT NULL,
    sql_handle STRING NOT NULL,
    plan_handle STRING NOT NULL,
    statement_start_offset INT NOT NULL,
    statement_end_offset INT NOT NULL,
    plan_generation_num BIGINT NOT NULL,
    creation_time TIMESTAMP NOT NULL,
    last_execution_time TIMESTAMP NOT NULL,
    execution_count BIGINT NOT NULL,
    total_worker_time BIGINT NOT NULL,
    last_worker_time BIGINT NOT NULL,
    min_worker_time BIGINT NOT NULL,
    max_worker_time BIGINT NOT NULL,
    total_elapsed_time BIGINT NOT NULL,
    last_elapsed_time BIGINT NOT NULL,
    min_elapsed_time BIGINT NOT NULL,
    max_elapsed_time BIGINT NOT NULL,
    total_logical_reads BIGINT NOT NULL,
    last_logical_reads BIGINT NOT NULL,
    total_physical_reads BIGINT NOT NULL,
    total_logical_writes BIGINT NOT NULL,
    last_logical_writes BIGINT NOT NULL
) USING DELTA;

CREATE TABLE telemetry.sqlserver.sqlserver_resource_semaphores (
    customer_id STRING NOT NULL,
    sample_time_utc TIMESTAMP NOT NULL,
    sql_server_instance_name STRING NOT NULL,
    machine_name STRING NOT NULL,
    resource_semaphore_id INT NOT NULL,
    target_memory_kb BIGINT NOT NULL,
    max_target_memory_kb BIGINT NOT NULL,
    total_memory_kb BIGINT NOT NULL,
    available_memory_kb BIGINT NOT NULL,
    granted_memory_kb BIGINT NOT NULL,
    used_memory_kb BIGINT NOT NULL,
    grantee_count INT NOT NULL,
    waiter_count INT NOT NULL,
    timeout_error_count BIGINT NOT NULL,
    forced_grant_count BIGINT NOT NULL,
    pool_id INT NOT NULL
) USING DELTA;

CREATE TABLE telemetry.sqlserver.sqlserver_scheduler_stats (
    customer_id STRING NOT NULL,
    sample_time_utc TIMESTAMP NOT NULL,
    sql_server_instance_name STRING NOT NULL,
    machine_name STRING NOT NULL,
    scheduler_id INT NOT NULL,
    status STRING NOT NULL,
    is_online BOOLEAN NOT NULL,
    is_idle BOOLEAN NOT NULL,
    current_tasks_count INT NOT NULL,
    runnable_tasks_count INT NOT NULL,
    current_workers_count INT NOT NULL,
    active_workers_count INT NOT NULL,
    work_queue_count INT NOT NULL,
    pending_disk_io_count INT NOT NULL,
    load_factor INT NOT NULL,
    yield_count BIGINT NOT NULL,
    total_cpu_usage_ms BIGINT NOT NULL,
    last_timer_activity TIMESTAMP NOT NULL,
    parent_node_id INT NOT NULL
) USING DELTA;

CREATE TABLE telemetry.sqlserver.sqlserver_session_transactions (
    customer_id STRING NOT NULL,
    sample_time_utc TIMESTAMP NOT NULL,
    sql_server_instance_name STRING NOT NULL,
    machine_name STRING NOT NULL,
    session_id SMALLINT NOT NULL,
    transaction_id BIGINT NOT NULL,
    transaction_descriptor STRING NOT NULL,
    enlist_count INT NOT NULL,
    is_user_transaction BOOLEAN NOT NULL,
    is_local BOOLEAN NOT NULL
) USING DELTA;

CREATE TABLE telemetry.sqlserver.sqlserver_tempdb_space_usage (
    customer_id STRING NOT NULL,
    sample_time_utc TIMESTAMP NOT NULL,
    sql_server_instance_name STRING NOT NULL,
    machine_name STRING NOT NULL,
    session_id SMALLINT NOT NULL,
    request_id INT NOT NULL,
    exec_context_id INT NOT NULL,
    user_objects_alloc_page_count BIGINT NOT NULL,
    user_objects_dealloc_page_count BIGINT NOT NULL,
    internal_objects_alloc_page_count BIGINT NOT NULL,
    internal_objects_dealloc_page_count BIGINT NOT NULL,
    total_allocated_pages BIGINT NOT NULL,
    total_deallocated_pages BIGINT NOT NULL
) USING DELTA;

CREATE TABLE telemetry.sqlserver.sqlserver_transaction_stats (
    customer_id STRING NOT NULL,
    sample_time_utc TIMESTAMP NOT NULL,
    sql_server_instance_name STRING NOT NULL,
    machine_name STRING NOT NULL,
    transaction_id BIGINT NOT NULL,
    name STRING NOT NULL,
    transaction_begin_time TIMESTAMP NOT NULL,
    transaction_type SMALLINT NOT NULL,
    transaction_state SMALLINT NOT NULL,
    transaction_uow STRING NOT NULL,
    transaction_sequence_num BIGINT NOT NULL,
    transaction_isolation_level SMALLINT NOT NULL,
    transaction_type_desc STRING NOT NULL,
    transaction_state_desc STRING NOT NULL
) USING DELTA;
