#!/usr/bin/env bash
# One timed cell: fresh Delta root, collector, N direct clients, committed rows/s from the Delta log.
# Knobs come from BWA_* (see base.yaml) and ARROWCOL_COLLECTOR_* env; client shape from BWA_CLIENT_*.
set -uo pipefail
H=/tmp/bwa/harness
BIN=${BWA_BIN:-/workspaces/rust-playground/target/release}
CELL=${CELL:?}
D=${D:-60}
W=${W:-20}
CCPUS=${CCPUS:-0-23}
KCPUS=${KCPUS:-24-31}
NCLIENTS=${NCLIENTS:-1}
export BWA_CORES=${BWA_CORES:-24}
export BWA_ROOT=/tmp/bwa/delta
OUT=/tmp/bwa/runs/$CELL
rm -rf "$BWA_ROOT" "$OUT"
mkdir -p "$BWA_ROOT" "$OUT"
env | grep -E '^(BWA_|ARROWCOL_)' | sort >"$OUT/env.txt"

export ARROWCOL_PARTITION_GRANULARITY=${ARROWCOL_PARTITION_GRANULARITY:-hour}
export ARROWCOL_LAYOUT_ROW_GROUP_ROWS=${ARROWCOL_LAYOUT_ROW_GROUP_ROWS:-262144}
export ARROWCOL_LAYOUT_MAX_INPUT_BYTES=${ARROWCOL_LAYOUT_MAX_INPUT_BYTES:-1073741824}
export ARROWCOL_LAYOUT_MAX_ROWS=${ARROWCOL_LAYOUT_MAX_ROWS:-2000000}

export ARROWCOL_CATALOG_DDL_IDENTITY_SECRET_FILE=/tmp/bwa/hmac
[[ -s /tmp/bwa/hmac ]] || openssl rand -hex 32 >/tmp/bwa/hmac
rm -rf /tmp/bwa/catalog-state
taskset -c "$KCPUS" "$BIN/arrowcol-catalog-rust" --listen 127.0.0.1:8817 --warehouse-root "$BWA_ROOT" \
  --state-dir /tmp/bwa/catalog-state --ingestion-layout >"$OUT/catalog.log" 2>&1 &
catpid=$!
for _ in $(seq 1 120); do
  "$BIN/arrowcol-catalog-rust" --listen 127.0.0.1:8817 --healthcheck >/dev/null 2>&1 && break
  sleep 0.25
done
taskset -c "$CCPUS" "$BIN/arrowcol-dataflow-rust" --config "$H/base.yaml" --num-cores "$BWA_CORES" >"$OUT/collector.log" 2>&1 &
cpid=$!
ready=false
for _ in $(seq 1 240); do
  curl -fsS http://127.0.0.1:32011/readyz >"$OUT/ready.json" 2>/dev/null && { ready=true; break; }
  kill -0 $cpid 2>/dev/null || break
  sleep 0.25
done
fail() {
  echo "{\"cell\":\"$CELL\",\"error\":\"$1\"}" | tee -a /tmp/bwa/results.jsonl
  kill -TERM $cpid 2>/dev/null; wait $cpid 2>/dev/null
  kill -TERM $catpid 2>/dev/null; wait $catpid 2>/dev/null
  exit 1
}
[[ $ready == true ]] || fail "collector not ready: $(tail -c 400 "$OUT/collector.log" | tr '"\n' "' ")"

"$BIN/arrowcol-dmv" --endpoint http://127.0.0.1:32010 --client-id bwa-ddl \
  --table "${BWA_TABLE:-sqlserver_availability_replica_states}" --rows 8 >"$OUT/ddl.log" 2>&1 \
  || fail "ddl: $(tail -c 400 "$OUT/ddl.log" | tr '"\n' "' ")"

start=$(( $(date +%s%3N) + 8000 ))
ws=$(( start + W * 1000 ))
we=$(( ws + D * 1000 ))
pids=()
for i in $(seq 1 "$NCLIENTS"); do
  taskset -c "$KCPUS" "$BIN/arrowcol-dmv" --endpoint http://127.0.0.1:32010 --client-id "bwa-$i" \
    --table "${BWA_TABLE:-sqlserver_availability_replica_states}" --rows 8192 \
    --workers "${BWA_CLIENT_WORKERS:-8}" --streams-per-worker "${BWA_CLIENT_SPW:-2}" \
    --runtime-threads "${BWA_CLIENT_THREADS:-8}" --interval-us 0 \
    --batch-rows "${BWA_CLIENT_BATCH_ROWS:-65536}" --corpus-rows "${BWA_CLIENT_CORPUS_ROWS:-65536}" \
    --max-flight-data-bytes 67108864 --max-message-bytes 134217728 --max-memory-bytes 1073741824 \
    --pod-ordinal "$i" --warmup-secs "$W" --duration-secs "$D" --run-start-unix-ms "$start" \
    >"$OUT/client-$i.log" 2>&1 &
  pids+=($!)
done

ticks() { awk '{print $14+$15}' /proc/$1/stat 2>/dev/null || echo 0; }
sleep_until() { local now; now=$(date +%s%3N); (( $1 > now )) && sleep "$(awk -v d=$(( $1 - now )) 'BEGIN{print d/1000}')"; }
sleep_until "$ws"
c0=$(ticks $cpid); k0=0; for p in "${pids[@]}"; do k0=$(( k0 + $(ticks $p) )); done
curl -fsS http://127.0.0.1:32011/metrics >"$OUT/metrics-start.txt" 2>/dev/null
: >"$OUT/pressure.jsonl"
for t in $(seq 2500 5000 $(( D * 1000 ))); do
  sleep_until $(( ws + t ))
  curl -fsS http://127.0.0.1:32011/v1/pressure 2>/dev/null | jq -c '{a:.rich.accepting_deliveries, dfe:.rich.dfe_state, delta:.rich.delta_state, bb:.rich.buffered_bytes.current, bbl:.rich.buffered_bytes.limit, br:.rich.buffered_rows.current, brl:.rich.buffered_rows.limit, pc:.rich.pending_commits.current, age:.rich.oldest_buffer_age_ms}' >>"$OUT/pressure.jsonl" 2>/dev/null
done
sleep_until "$we"
c1=$(ticks $cpid); k1=0; for p in "${pids[@]}"; do k1=$(( k1 + $(ticks $p) )); done
curl -fsS http://127.0.0.1:32011/metrics >"$OUT/metrics-end.txt" 2>/dev/null
rss=$(awk '/VmHWM/{print $2}' /proc/$cpid/status 2>/dev/null)
for p in "${pids[@]}"; do wait "$p"; done
sleep 3
kill -TERM $cpid 2>/dev/null
for _ in $(seq 1 120); do kill -0 $cpid 2>/dev/null || break; sleep 0.5; done
kill -KILL $cpid 2>/dev/null
wait $cpid 2>/dev/null
kill -TERM $catpid 2>/dev/null; wait $catpid 2>/dev/null

hz=$(getconf CLK_TCK)
admitted=$(grep -ho 'measured_rows=[0-9]*' "$OUT"/client-*.log | cut -d= -f2 | awk '{s+=$1} END{print s+0}')
cerr=$(grep -l '^Error' "$OUT"/client-*.log | wc -l)
stats=$(node "$H/delta-stats.mjs" "$BWA_ROOT" "$ws" "$we")
press=$(jq -s '{samples:length, blocked_frac:(if length>0 then (map(select(.a==false))|length)/length else null end),
  bytes_fill:(if length>0 then (map(.bb/.bbl)|add/length) else null end), rows_fill:(if length>0 then (map(.br/.brl)|add/length) else null end),
  age_ms_avg:(if length>0 then (map(.age)|add/length) else null end), dfe:(map(.dfe)|group_by(.)|map({(.[0]):length})|add)}' "$OUT/pressure.jsonl")
waiters=$(grep -E '^arrowcol_exporter_capacity_waiters|^arrowcol_exporter_group_occupancy_seconds_total' "$OUT/metrics-end.txt" \
  | sed -E 's/^arrowcol_exporter_capacity_waiters\{scope="([a-z]+)",resource="([a-z]+)"\} /w_\1_\2 /; s/^arrowcol_exporter_group_occupancy_seconds_total\{state="([a-z_]+)"\} /occ_\1 /' \
  | jq -R 'split(" ") | {(.[0]): (.[1]|tonumber)}' | jq -s 'add // {}')
knobs=$(env | grep -E '^(BWA_|ARROWCOL_COLLECTOR_)' | grep -v -E '^BWA_(ROOT|BIN)=' | sort | jq -R 'split("=") | {(.[0]): (.[1:] | join("="))}' | jq -s add)
jq -cn --arg cell "$CELL" --argjson stats "$stats" --argjson knobs "$knobs" --argjson press "$press" --argjson waiters "$waiters" \
  --argjson admitted "${admitted:-0}" --argjson d "$D" --argjson nclients "$NCLIENTS" --argjson cerr "$cerr" \
  --argjson ccores "$(awk -v a=$c0 -v b=$c1 -v h=$hz -v d=$D 'BEGIN{print (b-a)/h/d}')" \
  --argjson kcores "$(awk -v a=$k0 -v b=$k1 -v h=$hz -v d=$D 'BEGIN{print (b-a)/h/d}')" \
  --argjson rss_kb "${rss:-0}" --arg host "$(hostname)" --arg sha "$(git -C /workspaces/rust-playground rev-parse --short HEAD)" \
  '{cell:$cell, sha:$sha, host:$host, d:$d, nclients:$nclients, client_error_logs:$cerr,
    committed_rps:$stats.committed_rows_per_second, admitted_rps:($admitted/$d),
    bytes_per_row:$stats.parquet_bytes_per_row, rows_per_file:$stats.rows_per_file,
    files_per_commit:$stats.files_per_commit, commits:$stats.commits, commit_gap_p50:$stats.commit_gap_ms_p50,
    collector_cores:$ccores, client_cores:$kcores, collector_rss_kb:$rss_kb, pressure:$press, waiters:$waiters, knobs:$knobs, stats:$stats}' \
  | tee -a /tmp/bwa/results.jsonl
du -sh "$BWA_ROOT" | awk '{print "delta_size", $1}'
rm -rf "$BWA_ROOT"
