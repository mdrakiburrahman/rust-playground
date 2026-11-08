#!/usr/bin/env bash
# Captures real SQL Server wait statistics (sys.dm_os_wait_stats) as CSV rows
# for the telemetry.sqlserver.sqlserver_wait_stats table.
#
# Usage: bin/quiver-cli/data/capture-wait-stats.sh [snapshots] [interval-seconds] > file.csv
#
# Reads from the loadtest SQL Server container; override with SQLSERVER_CONTAINER.
# Wait counters are cumulative since the instance started, so successive
# snapshots grow while the server is busy. wait_category follows the Query
# Store wait categories.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/../../.." && pwd)"
container="${SQLSERVER_CONTAINER:-rust-playground-loadtest-latest-sqlserver-1}"
snapshots="${1:-30}"
interval="${2:-5}"
password="$(cat "$root/projects/arrowcol-infra/secrets/sqlserver-sa-password.txt")"

read -r -d '' query <<'SQL' || true
SET NOCOUNT ON;
DECLARE @sample datetime2(7) = SYSUTCDATETIME();
SELECT
    CONVERT(varchar(27), @sample, 126) + 'Z',
    CAST(@@SERVERNAME AS varchar(128)),
    CAST(SERVERPROPERTY('MachineName') AS varchar(128)),
    wait_type,
    wait_time_ms,
    wait_time_ms - signal_wait_time_ms,
    signal_wait_time_ms,
    max_wait_time_ms,
    waiting_tasks_count,
    CASE
        WHEN wait_type = 'SOS_SCHEDULER_YIELD' THEN 'CPU'
        WHEN wait_type = 'THREADPOOL' THEN 'Worker Thread'
        WHEN wait_type LIKE 'LCK[_]M[_]%' THEN 'Lock'
        WHEN wait_type LIKE 'LATCH[_]%' THEN 'Latch'
        WHEN wait_type LIKE 'PAGELATCH[_]%' THEN 'Buffer Latch'
        WHEN wait_type LIKE 'PAGEIOLATCH[_]%' THEN 'Buffer IO'
        WHEN wait_type = 'RESOURCE_SEMAPHORE_QUERY_COMPILE' THEN 'Compilation'
        WHEN wait_type LIKE 'CLR[_]%' OR wait_type LIKE 'SQLCLR%' THEN 'SQL CLR'
        WHEN wait_type LIKE 'DBMIRROR%' THEN 'Mirroring'
        WHEN wait_type LIKE 'XACT%' OR wait_type LIKE 'DTC%' OR wait_type LIKE 'TRAN[_]MARKLATCH[_]%'
            OR wait_type LIKE 'MSQL[_]XACT[_]%' OR wait_type = 'TRANSACTION_MUTEX' THEN 'Transaction'
        WHEN wait_type LIKE 'SLEEP[_]%' OR wait_type IN ('LAZYWRITER_SLEEP', 'SQLTRACE_BUFFER_FLUSH',
            'SQLTRACE_INCREMENTAL_FLUSH_SLEEP', 'SQLTRACE_WAIT_ENTRIES', 'FT_IFTS_SCHEDULER_IDLE_WAIT',
            'XE_DISPATCHER_WAIT', 'REQUEST_FOR_DEADLOCK_SEARCH', 'LOGMGR_QUEUE', 'ONDEMAND_TASK_QUEUE',
            'CHECKPOINT_QUEUE', 'XE_TIMER_EVENT') THEN 'Idle'
        WHEN wait_type LIKE 'PREEMPTIVE[_]%' THEN 'Preemptive'
        WHEN wait_type = 'BROKER_RECEIVE_WAITFOR' THEN 'User Wait'
        WHEN wait_type LIKE 'BROKER[_]%' THEN 'Service Broker'
        WHEN wait_type IN ('LOGMGR', 'LOGBUFFER', 'LOGMGR_RESERVE_APPEND', 'LOGMGR_FLUSH', 'LOGMGR_PMM_LOG',
            'CHKPT', 'WRITELOG') THEN 'Tran Log IO'
        WHEN wait_type IN ('ASYNC_NETWORK_IO', 'NET_WAITFOR_PACKET', 'PROXY_NETWORK_IO',
            'EXTERNAL_SCRIPT_NETWORK_IOF') THEN 'Network IO'
        WHEN wait_type IN ('CXPACKET', 'EXCHANGE', 'CXCONSUMER') THEN 'Parallelism'
        WHEN wait_type IN ('RESOURCE_SEMAPHORE', 'CMEMTHREAD', 'CMEMPARTITIONED', 'EE_PMOLOCK',
            'MEMORY_ALLOCATION_EXT', 'RESERVED_MEMORY_ALLOCATION_EXT', 'MEMORY_GRANT_UPDATE') THEN 'Memory'
        WHEN wait_type IN ('WAITFOR', 'WAIT_FOR_RESULTS') THEN 'User Wait'
        WHEN wait_type IN ('TRACEWRITE', 'SQLTRACE_LOCK', 'SQLTRACE_FILE_BUFFER',
            'SQLTRACE_FILE_WRITE_IO_COMPLETION', 'SQLTRACE_FILE_READ_IO_COMPLETION',
            'SQLTRACE_PENDING_BUFFER_WRITERS', 'SQLTRACE_SHUTDOWN', 'QUERY_TRACEOUT', 'TRACE_EVTNOTIF')
            THEN 'Tracing'
        WHEN wait_type LIKE 'FT[_]%' OR wait_type IN ('FULLTEXT GATHERER', 'MSSEARCH',
            'PWAIT_RESOURCE_SEMAPHORE_FT_PARALLEL_QUERY_SYNC') THEN 'Full Text Search'
        WHEN wait_type IN ('ASYNC_IO_COMPLETION', 'IO_COMPLETION', 'BACKUPIO', 'WRITE_COMPLETION',
            'IO_QUEUE_LIMIT', 'IO_RETRY') THEN 'Other Disk IO'
        WHEN wait_type IN ('LOG_RATE_GOVERNOR', 'POOL_LOG_RATE_GOVERNOR', 'HADR_THROTTLE_LOG_RATE_GOVERNOR',
            'INSTANCE_LOG_RATE_GOVERNOR') THEN 'Log Rate Governor'
        WHEN wait_type LIKE 'SE[_]REPL[_]%' OR wait_type LIKE 'REPL[_]%' OR wait_type LIKE 'HADR[_]%'
            OR wait_type LIKE 'PWAIT[_]HADR[_]%' OR wait_type IN ('REPLICA_WRITES', 'FCB_REPLICA_WRITE',
            'FCB_REPLICA_READ', 'PWAIT_HADRSIM') THEN 'Replication'
        ELSE 'Unknown'
    END
FROM sys.dm_os_wait_stats
WHERE waiting_tasks_count > 0
ORDER BY wait_type;
SQL

echo "customer_id,sample_time_utc,sql_server_instance_name,machine_name,wait_type,wait_time_ms,resource_wait_time_ms,signal_wait_time_ms,max_wait_time_ms,waiting_tasks_count,wait_category"
for snapshot in $(seq 1 "$snapshots"); do
  docker exec -e SQLCMDPASSWORD="$password" "$container" /opt/mssql-tools18/bin/sqlcmd \
    -S localhost -U sa -C -b -h -1 -W -s ',' -Q "$query"
  if [ "$snapshot" -lt "$snapshots" ]; then
    sleep "$interval"
  fi
done | awk 'NF { printf "customer-%04d,%s\n", row++ % 16, $0 }'
