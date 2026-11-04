#!/usr/bin/env bash
# Follow-up Q1 (self-drain with epoch) and Q2 (inferred fencing under routing noise).
# Usage: scripts/ownership.sh <table-base-url> <work-dir> [scenario...]
#   EPOCHS=drain|tenure (default tenure). drain = owner appId <id>/<table>, drain under epochs (doc
#   as written). tenure = every ownership tenure and drain claims an epoch.
#   Scenarios: crash-before crash-after pause-inflight two-holders drain-load misroute-5 misroute-30
#   misroute-120 flip (default: all).
# Example: EPOCHS=tenure scripts/ownership.sh file:///tmp/dcb-own /tmp/dcb-own-work
# Each scenario writes <work>/<name>/check.json and route.log; scripts/own_sum.py summarizes.
set -uo pipefail
BASE=${1:?table base url}
WORK=${2:?work dir}
shift 2
SCEN=${*:-crash-before crash-after pause-inflight two-holders drain-load misroute-5 misroute-30 misroute-120 flip}
EPOCHS=${EPOCHS:-tenure}
# XFENCE=off drops cross-identity fencing (--infer-fence, --fence-identity). Same-identity epoch
# claims stay. Scenario names get a -nox suffix.
XFENCE=${XFENCE:-on}
XSUF=$([ "$XFENCE" = off ] && echo -nox)
HOT=${HOT:-1.2}
DCB=${DCB:-$(dirname "$0")/../../../target/release/dcb}
RUN_ID=${RUN_ID:-$(date +%s)}
mkdir -p "$WORK"

scenario() { local name=$1-$EPOCHS${XSUF}; mkdir -p "$WORK/$name"; D=$WORK/$name; T=$BASE/$RUN_ID-$name; N=$name; }
route() { echo "$1" >"$D/route"; echo "$(date +%s%3N) $1" >>"$D/route.log"; }
# In a background subshell, exec so that $! is the dcb process and SIGSTOP pauses it.
x() { if [ "$BASHPID" != "$$" ]; then exec "$@"; else "$@"; fi; }
dcb() {
  local a=()
  while [ $# -gt 0 ]; do
    if [ "$XFENCE" = off ] && [ "$1" = --infer-fence ]; then shift; continue; fi
    if [ "$XFENCE" = off ] && [ "$1" = --fence-identity ]; then shift 2; continue; fi
    a+=("$1"); shift
  done
  x "$DCB" run ${DCB_ARGS:-} --epochs "$EPOCHS" --checkpoint-every 50 --encoders 2 --route-file "$D/route" "${a[@]}"
}
# A backlog builder: one file per commit with slowed log writes, so pending manifests pile up.
backlog() { echo --rate 20 --max-files-per-commit 1 --fault-delay-ms 150 --fault-scope log; }
check() {
  "$DCB" check --table-url "$T" --wal-dir "$D/wal-a" --wal-dir "$D/wal-b" $(ls "$D"/*.sent.jsonl | sed 's/^/--sent /') \
    >"$D/check.json" 2>"$D/check.err"
  python3 -c "
import json; c=json.load(open('$D/check.json'))
print(f'$N'.ljust(28), *(f'{k}={c[k]!s:5}' for k in ('g1_no_loss','g2_no_duplicates','g3_atomic','g4_valid')),
      f'accepted={c[\"accepted_files\"]} missing={c[\"missing_accepted_count\"]} dups={c[\"duplicate_paths_count\"]} pending={c[\"manifests_pending\"]} appIds={len(c[\"txn_app_ids\"])}')"
}

for s in $SCEN; do case $s in
crash-before|crash-after)
  # X crashes with a pending manifest, Y takes T and fences X, X restarts and drains.
  point=${s#crash-}; scenario q1-$s
  route inst-a
  dcb --table-url "$T" --create --app-id inst-a --wal-dir "$D/wal-a" --rate 10 --duration 30 \
    --fault-crash-$point-log-put 20 --out "$D/x1.jsonl" 2>"$D/x1.err"
  route inst-b
  dcb --table-url "$T" --app-id inst-b --wal-dir "$D/wal-b" --fence-identity inst-a --rate "$HOT" --duration 15 \
    --out "$D/y.jsonl" 2>"$D/y.err" &
  Y=$!; sleep 5
  dcb --table-url "$T" --app-id inst-a --wal-dir "$D/wal-a" --drain-only --out "$D/x2.jsonl" 2>"$D/x2.err"
  wait $Y; check ;;
pause-inflight)
  # X paused with a backlog and a commit in flight; Y fences; X resumes, loses, and self-drains.
  scenario q1-$s
  route inst-a
  dcb --table-url "$T" --create --app-id inst-a --wal-dir "$D/wal-a" $(backlog) --duration 30 \
    --out "$D/x.jsonl" 2>"$D/x.err" &
  X=$!; sleep 6; kill -STOP $X; route inst-b
  dcb --table-url "$T" --app-id inst-b --wal-dir "$D/wal-b" --fence-identity inst-a --rate "$HOT" --duration 20 \
    --out "$D/y.jsonl" 2>"$D/y.err" &
  Y=$!; sleep 4; kill -CONT $X
  wait $X; wait $Y; check ;;
two-holders)
  # X paused; Y fences; an adopter A mounts X's WAL (same directory) and drains slowly; X resumes
  # and drains the same WAL at the same time.
  # The adopter's log delay (A_DELAYS, ms) varies the interleaving of the two claims.
  for ad in ${A_DELAYS:-0 100 300}; do
    scenario q1-$s-$ad
    route inst-a
    dcb --table-url "$T" --create --app-id inst-a --wal-dir "$D/wal-a" $(backlog) --duration 30 \
      --out "$D/x.jsonl" 2>"$D/x.err" &
    X=$!; sleep 6; kill -STOP $X; route inst-b
    dcb --table-url "$T" --app-id inst-b --wal-dir "$D/wal-b" --fence-identity inst-a --rate "$HOT" --duration 25 \
      --out "$D/y.jsonl" 2>"$D/y.err" &
    Y=$!; sleep 2
    dcb --table-url "$T" --app-id inst-a --wal-dir "$D/wal-a" --drain-only --fault-delay-ms "$ad" --fault-scope log \
      --out "$D/a.jsonl" 2>"$D/a.err" &
    A=$!; sleep 2; kill -CONT $X
    wait $X; wait $A; wait $Y; check
  done ;;
drain-load)
  # X crashes with a large backlog; Y commits at the hot-table rate (and 10x) while X drains.
  for rate in "$HOT" 12; do
    scenario q1-$s-$rate
    route inst-a
    dcb --table-url "$T" --create --app-id inst-a --wal-dir "$D/wal-a" $(backlog) --duration 60 \
      --fault-crash-after-log-put 40 --out "$D/x1.jsonl" 2>"$D/x1.err"
    route inst-b
    dcb --table-url "$T" --app-id inst-b --wal-dir "$D/wal-b" --fence-identity inst-a --rate "$rate" --duration 40 \
      --out "$D/y.jsonl" 2>"$D/y.err" &
    Y=$!; sleep 10
    echo "$(date +%s%3N) drain-start" >>"$D/route.log"
    dcb --table-url "$T" --app-id inst-a --wal-dir "$D/wal-a" --drain-only --out "$D/x2.jsonl" 2>"$D/x2.err"
    echo "$(date +%s%3N) drain-end" >>"$D/route.log"
    wait $Y; check
  done ;;
misroute-*)
  # Inferred fencing: the router sends T to Y for M seconds, then back to X.
  m=${s#misroute-}; scenario q2-$s
  route inst-a
  dur=$((40 + m))
  dcb --table-url "$T" --create --app-id inst-a --wal-dir "$D/wal-a" --infer-fence --rate 10 --duration $dur \
    --out "$D/x.jsonl" 2>"$D/x.err" &
  X=$!; sleep 2
  dcb --table-url "$T" --app-id inst-b --wal-dir "$D/wal-b" --infer-fence --rate 10 --duration $((dur - 2)) \
    --out "$D/y.jsonl" 2>"$D/y.err" &
  Y=$!; sleep 18; route inst-b; sleep "$m"; route inst-a
  wait $X; wait $Y; check ;;
flip)
  # Inferred fencing: traffic alternates between X and Y every second for 60 s.
  scenario q2-$s
  route inst-a
  dcb --table-url "$T" --create --app-id inst-a --wal-dir "$D/wal-a" --infer-fence --rate 10 --duration 100 \
    --out "$D/x.jsonl" 2>"$D/x.err" &
  X=$!; sleep 2
  dcb --table-url "$T" --app-id inst-b --wal-dir "$D/wal-b" --infer-fence --rate 10 --duration 98 \
    --out "$D/y.jsonl" 2>"$D/y.err" &
  Y=$!; sleep 18
  for i in $(seq 1 30); do route inst-b; sleep 1; route inst-a; sleep 1; done
  wait $X; wait $Y; check ;;
*) echo "unknown scenario $s" >&2 ;;
esac; done
