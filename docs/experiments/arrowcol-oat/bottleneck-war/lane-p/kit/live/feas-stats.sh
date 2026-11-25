B="$ROOT/bin/3259d8cd0fff5eeac77d77308898721cf696f7bd"; LG="$ROOT/target/release/arrowcol-loadgen"
Y="$ROOT/fix/base.yaml"; [ -f "$Y" ] || exit 20
busy() { ps -eo stat=,comm= | awk '$1 !~ /^Z/ && $2 ~ /^(arrowcol-|delta-maint|cargo|rustc|cc1)/' | wc -l; }
[ "$(busy)" -eq 0 ] || { echo busy; exit 21; }
for p in 8817 32010 32011; do ss -ltn | grep -q ":$p " && { echo "port $p busy"; exit 22; }; done
F="$ROOT/feas/stats-$(date -u +%H%M%S)"; mkdir -p "$F/delta" || exit 23
export BWA_CORES=12 BWA_CCPUS=0-11 BWA_KCPUS=12-15 BWA_NCLIENTS=2 BWA_PD=16 BWA_MCF=24 BWA_CLIENT_SPW=1
export BWA_BUF_BYTES=17200000000 BWA_GCAP_BYTES=17200000000 BWA_TCAP_BYTES=16000000000
export BWA_BUF_ROWS=100000000 BWA_GCAP_ROWS=100000000 BWA_TCAP_ROWS=100000000
export ARROWCOL_COLLECTOR_PARQUET_COLUMN_WORKERS=1 BWA_MRPF=2000000 BWA_TARGET=33554432
export ARROWCOL_PARTITION_GRANULARITY=hour ARROWCOL_LAYOUT_ROW_GROUP_ROWS=262144
export ARROWCOL_LAYOUT_MAX_INPUT_BYTES=1073741824 ARROWCOL_LAYOUT_MAX_ROWS=2000000
export BWA_ROOT="$F/delta" ARROWCOL_COLLECTOR_DELTA_STATS_COLUMNS="${STATS:-[]}"
head -c 32 /dev/urandom | od -An -tx1 | tr -d ' \n' > "$F/hmac"; chmod 600 "$F/hmac"
export ARROWCOL_CATALOG_DDL_IDENTITY_SECRET_FILE="$F/hmac"
echo "F=$F STATS=$ARROWCOL_COLLECTOR_DELTA_STATS_COLUMNS"
"$LG" initialize-local --partition-by-ingestion-date --table telemetry.sqlserver.sqlserver_availability_replica_states \
  --dataflow-root "$F/delta" >"$F/init.log" 2>&1; echo "init=$?"; tail -5 "$F/init.log"
L=$(find "$F/delta" -type d -name _delta_log | head -1); echo "log=$L"
[ -n "$L" ] && grep -o '"configuration":{[^}]*}' "$L/00000000000000000000.json"
taskset -c 12-15 "$B/arrowcol-catalog-rust" --listen 127.0.0.1:8817 --warehouse-root "$F/delta" \
  --state-dir "$F/catalog-state" --ingestion-layout >"$F/catalog.log" 2>&1 &
CAT=$!
for i in $(seq 1 100); do "$B/arrowcol-catalog-rust" --listen 127.0.0.1:8817 --healthcheck >/dev/null 2>&1 && break; sleep 0.2; done
taskset -c 0-11 "$B/arrowcol-dataflow-rust" --config "$Y" --num-cores 12 >"$F/collector.log" 2>&1 &
COL=$!
for i in $(seq 1 300); do curl -sf http://127.0.0.1:32011/readyz >/dev/null && break; sleep 0.2; done
echo "ready after $i"
taskset -c 12-15 "$B/arrowcol-dmv" --endpoint http://127.0.0.1:32010 --client-id bwa-ddl \
  --table sqlserver_availability_replica_states --rows 8 >"$F/ddl.log" 2>&1; echo "ddl=$?"; tail -3 "$F/ddl.log"
L=$(find "$F/delta" -type d -name _delta_log | head -1); echo "log=$L"; ls "$L"
S=$(( $(date +%s%3N) + 2000 ))
taskset -c 12-15 "$B/arrowcol-dmv" --endpoint http://127.0.0.1:32010 --client-id bwa-0 \
  --table sqlserver_availability_replica_states --rows 8192 --workers 4 --streams-per-worker 1 --runtime-threads 4 \
  --interval-us 0 --batch-rows 65536 --corpus-rows 65536 --string-width 0 --max-flight-data-bytes 67108864 \
  --max-message-bytes 134217728 --max-memory-bytes 1073741824 --pod-ordinal 0 --warmup-secs 2 --duration-secs 8 \
  --run-start-unix-ms "$S" >"$F/client-0.log" 2>&1; echo "client=$?"; tail -4 "$F/client-0.log"
sleep 6
echo "== commits"; ls "$L" | tail -5; ls "$L" | grep -c '\.json$'
for v in 00000000000000000000 00000000000000000001 00000000000000000002; do
  [ -f "$L/$v.json" ] || continue
  echo "-- v$v"; grep -o '"operation":"[^"]*"' "$L/$v.json" | head -1
  grep -o '"engineInfo":"[^"]*"' "$L/$v.json" | head -1
  grep -o '"configuration":{[^}]*}' "$L/$v.json" | head -1
done
LAST=$(ls "$L"/*.json | tail -1); echo "-- last $LAST"
grep -o '"operation":"[^"]*"' "$LAST" | head -1
grep -o '"stats":"[^"]\{0,240\}' "$LAST" | head -2
grep -c '"add"' "$LAST"
echo "== errors"; grep -iE 'error|mismatch|fail' "$F/collector.log" | grep -v metrics | head -8
grep -iE 'error|mismatch|fail' "$F/catalog.log" | head -5
kill -TERM "$COL" "$CAT" 2>/dev/null; wait "$COL" "$CAT" 2>/dev/null
[ "$(busy)" -eq 0 ] && echo clean || echo LEFTOVER
du -sh "$F"
