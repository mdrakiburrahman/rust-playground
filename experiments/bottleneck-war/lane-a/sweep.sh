#!/usr/bin/env bash
# Runs every cell line of a plan file sequentially: "<cell> VAR=value ...". Skips cells already in results.jsonl.
set -uo pipefail
PLAN=${1:?plan file}
while read -r cell rest; do
  [[ -z "$cell" || "$cell" == \#* ]] && continue
  grep -q "\"cell\":\"$cell\"" /tmp/bwa/results.jsonl 2>/dev/null && continue
  echo "=== $(date -u +%H:%M:%S) $cell $rest"
  env $rest CELL="$cell" /tmp/bwa/harness/cell.sh </dev/null >/tmp/bwa/last-cell.log 2>&1
  grep "\"cell\":\"$cell\"" /tmp/bwa/results.jsonl | tail -1 | jq -c '{c:.cell,rps:.committed_rps,adm:.admitted_rps,bpr:(.bytes_per_row//0|floor),rpf:.rows_per_file,fpc:.files_per_commit,cc:.collector_cores,kc:.client_cores,rss:((.collector_rss_kb//0)/1048576|floor),blk:.pressure.blocked_frac,bf:.pressure.bytes_fill,rf:.pressure.rows_fill,age:.pressure.age_ms_avg,err:(.error//null)}' 2>/dev/null
done <"$PLAN"
echo SWEEP_DONE
