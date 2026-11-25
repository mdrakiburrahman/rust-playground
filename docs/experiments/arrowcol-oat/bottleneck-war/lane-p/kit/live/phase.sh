#!/usr/bin/env bash
# Lane P timed phase wrapper. Caller has already applied the cd and origin guards in <root>/rp.
# usage: ROOT=<root> bash phase.sh <phase-name> <plan.json>
name=$1; plan=$2
[ -n "$ROOT" ] && [ -n "$name" ] && [ -f "$plan" ] || { echo "USAGE_ERROR"; exit 20; }
case "$PWD" in "$ROOT/rp") ;; *) echo "NOT_IN_GUARDED_CHECKOUT $PWD"; exit 3 ;; esac
out="$ROOT/runs/$name"; log="$ROOT/runs/$name.phase.log"
[ -e "$out" ] && { echo "REFUSE_EXISTING_OUTPUT $out"; exit 21; }
snap() {
  echo "== $1 $(date -u +%FT%T.%3NZ)"
  echo "loadavg $(cat /proc/loadavg)"
  ps -eo pid,stat,pcpu,etimes,comm --sort=-pcpu | head -8
  df -B1 --output=avail,size "$ROOT" | tail -1 | awk '{printf "free_bytes %s of %s\n", $1, $2}'
  free -m | awk 'NR==2{printf "mem_used_mb %s avail_mb %s\n", $3, $7}'
  grep -E '^(allocstall_movable|pgscan_direct|compact_stall|thp_fault_alloc) ' /proc/vmstat | tr '\n' ' '; echo
}
ready=0
for i in $(seq 1 36); do
  l1=$(cut -d' ' -f1 /proc/loadavg)
  busy=$(ps -eo stat=,comm= | awk '$1 !~ /^Z/ && $2 ~ /^(arrowcol-|delta-maint|cargo|rustc|cc1)/' | wc -l)
  if awk -v l="$l1" 'BEGIN{exit !(l < 2.0)}' && [ "$busy" -eq 0 ]; then ready=1; break; fi
  sleep 5
done
snap PRE | tee -a "$log"
[ "$ready" -eq 1 ] || { echo "HOST_BUSY load1=$l1 busy=$busy; no arm launched" | tee -a "$log"; exit 22; }
export BWA_BIN="$ROOT/bin/3259d8cd0fff5eeac77d77308898721cf696f7bd"
echo "plan_sha256 $(sha256sum "$plan" | cut -d' ' -f1) plan=$plan out=$out" | tee -a "$log"
# Untimed, unscored warm-up of the defaults: absorbs first-cell cold-memory reclaim before the timed arms.
warm="$out.warm"
node -e 'const fs=require("fs");const p=JSON.parse(fs.readFileSync(process.argv[1],"utf8"));
fs.writeFileSync(process.argv[2],JSON.stringify({duration:p.duration,warmup:p.warmup,common:p.common,
cells:[{id:"warm",grid:"warm",x:"warm",y:"warm",env:{}}]},null,2)+"\n",{flag:"wx"})' "$plan" "$warm.plan.json" || { echo "WARM_PLAN_FAILED"; exit 23; }
node --experimental-strip-types "$ROOT/fix/score.ts" "$warm.plan.json" "$warm" > "$warm.stdout" 2> "$warm.stderr"
echo "WARMUP_RC=$? (ignored) $(tail -c 300 "$warm.stdout" | tr '\n' ' ')" | tee -a "$log"
snap WARMED | tee -a "$log"
node --experimental-strip-types "$ROOT/fix/score.ts" "$plan" "$out" 2> "$ROOT/runs/$name.stderr" | tee "$ROOT/runs/$name.stdout"
rc=${PIPESTATUS[0]}
snap POST | tee -a "$log"
rows=$( [ -f "$out/results.jsonl" ] && wc -l < "$out/results.jsonl" || echo 0)
echo "RUNNER_RC=$rc ROWS=$rows" | tee -a "$log"
tail -5 "$ROOT/runs/$name.stderr"
exit "$rc"
