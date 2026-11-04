#!/usr/bin/env bash
# Failure-injection scenarios (experiment 7, plus identity change and replay).
# Usage: scripts/faults.sh <table-base-url> <work-dir> [rate] [duration]
# Example: scripts/faults.sh file:///tmp/dcb-faults /tmp/dcb-faults-work
#          AZURE_USE_AZURE_CLI=true scripts/faults.sh abfss://bench@acct.dfs.core.windows.net/faults /tmp/w
# Each scenario writes <work>/<name>/check.json; the summary prints one line per scenario.
set -uo pipefail
BASE=${1:?table base url}
WORK=${2:?work dir}
RATE=${3:-10}
DUR=${4:-15}
DCB=${DCB:-$(dirname "$0")/../../../target/release/dcb}
[ -x "$DCB" ] || DCB=$(dirname "$0")/../../../target/debug/dcb
RUN_ID=${RUN_ID:-$(date +%s)}
mkdir -p "$WORK"

common() { echo --rate "$RATE" --encoders 4 --duration "$DUR" --checkpoint-every 50; }

check() { # name table wal... -- sent...
  local name=$1 url=$2; shift 2
  local args=()
  while [ $# -gt 0 ] && [ "$1" != "--" ]; do args+=(--wal-dir "$1"); shift; done
  [ "${1:-}" = "--" ] && shift
  while [ $# -gt 0 ]; do args+=(--sent "$1"); shift; done
  "$DCB" check --table-url "$url" "${args[@]}" >"$WORK/$name/check.json" 2>"$WORK/$name/check.err"
  python3 - "$WORK/$name" "$name" <<'PY'
import json,sys,glob,collections
d,name=sys.argv[1],sys.argv[2]
c=json.load(open(d+"/check.json"))
outcomes=collections.Counter(); lib=0; loop=0; inj=0; amb=0
for f in glob.glob(d+"/*.jsonl"):
    if f.endswith(".sent.jsonl"): continue
    for l in open(f):
        r=json.loads(l)
        if r.get("kind")=="commit":
            outcomes[r["outcome"]]+=1; lib+=r["library_retries"]; loop+=max(r["loop_attempts"]-1,0)
            inj+=r["storage"]["injected_errors"]; amb+=r["storage"]["injected_ambiguous"]
print(f'{name:28s} G1={c["g1_no_loss"]!s:5} G2={c["g2_no_duplicates"]!s:5} G3={c["g3_atomic"]!s:5} G4={c["g4_valid"]!s:5} '
      f'accepted={c["accepted_files"]} missing={c["missing_accepted_count"]} dups={c["duplicate_paths_count"]} '
      f'sent_not_accepted={c["sent_not_accepted"]} pending={c["manifests_pending"]} '
      f'inj503={inj} ambiguous={amb} lib_retries={lib} loop_retries={loop} outcomes={dict(outcomes)}')
PY
}

scenario() { local name=$1; mkdir -p "$WORK/$name"; echo "$BASE/$RUN_ID-$name"; }

# 1. Crash after the log write, before the loop records success; restart and replay.
for idem in txn scan-log; do
  n=crash-after-log-$idem; T=$(scenario $n); D=$WORK/$n
  "$DCB" run ${DCB_ARGS:-} --table-url "$T" --create --app-id inst-a --wal-dir "$D/wal" --idempotency $idem $(common) \
    --fault-crash-after-log-put 20 --out "$D/r1.jsonl" 2>"$D/r1.err"
  "$DCB" run ${DCB_ARGS:-} --table-url "$T" --app-id inst-a --wal-dir "$D/wal" --idempotency $idem --rate 1 --encoders 1 --duration 3 \
    --out "$D/r2.jsonl" 2>"$D/r2.err"
  check $n "$T" "$D/wal" -- "$D/r1.sent.jsonl" "$D/r2.sent.jsonl"
done

# 3. Ambiguous commit: the log write lands, the client sees an error.
for idem in txn scan-log; do
  for kind in timeout conflict; do
    n=ambiguous-$kind-$idem; T=$(scenario $n); D=$WORK/$n
    "$DCB" run ${DCB_ARGS:-} --table-url "$T" --create --app-id inst-a --wal-dir "$D/wal" --idempotency $idem $(common) \
      --fault-ambiguous-rate 0.2 --fault-ambiguous-kind $kind --out "$D/r1.jsonl" 2>"$D/r1.err"
    check $n "$T" "$D/wal" -- "$D/r1.sent.jsonl"
  done
done

# 4. Injected 503s on log requests (after the Azure client's own retries are exhausted).
n=errors-503-log; T=$(scenario $n); D=$WORK/$n
"$DCB" run ${DCB_ARGS:-} --table-url "$T" --create --app-id inst-a --wal-dir "$D/wal" $(common) \
  --fault-error-rate 0.1 --fault-scope log --out "$D/r1.jsonl" 2>"$D/r1.err"
check $n "$T" "$D/wal" -- "$D/r1.sent.jsonl"

n=throttle-200ms-log; T=$(scenario $n); D=$WORK/$n
"$DCB" run ${DCB_ARGS:-} --table-url "$T" --create --app-id inst-a --wal-dir "$D/wal" $(common) \
  --fault-delay-ms 200 --fault-scope log --out "$D/r1.jsonl" 2>"$D/r1.err"
check $n "$T" "$D/wal" -- "$D/r1.sent.jsonl"

# Identity change on restart: crash before or after the log write, then restart under a new appId.
# no-adopt ignores the old identity's WAL; adopt recommits its pending manifests under the new appId.
for point in before after; do
  for mode in no-adopt adopt; do
    n=identity-change-$mode-crash-$point; T=$(scenario $n); D=$WORK/$n
    "$DCB" run ${DCB_ARGS:-} --table-url "$T" --create --app-id inst-a --wal-dir "$D/wal" $(common) \
      --fault-crash-$point-log-put 20 --out "$D/r1.jsonl" 2>"$D/r1.err"
    extra=()
    [ $mode = adopt ] && extra=(--adopt-from inst-a)
    "$DCB" run ${DCB_ARGS:-} --table-url "$T" --app-id inst-b --wal-dir "$D/wal" "${extra[@]}" --rate 1 --encoders 1 --duration 3 \
      --out "$D/r2.jsonl" 2>"$D/r2.err"
    check $n "$T" "$D/wal" -- "$D/r1.sent.jsonl" "$D/r2.sent.jsonl"
  done
done

# Crash before the log write with the same identity: replay must commit the pending manifest.
n=crash-before-log-txn; T=$(scenario $n); D=$WORK/$n
"$DCB" run ${DCB_ARGS:-} --table-url "$T" --create --app-id inst-a --wal-dir "$D/wal" $(common) \
  --fault-crash-before-log-put 20 --out "$D/r1.jsonl" 2>"$D/r1.err"
"$DCB" run ${DCB_ARGS:-} --table-url "$T" --app-id inst-a --wal-dir "$D/wal" --rate 1 --encoders 1 --duration 3 \
  --out "$D/r2.jsonl" 2>"$D/r2.err"
check $n "$T" "$D/wal" -- "$D/r1.sent.jsonl" "$D/r2.sent.jsonl"

# Zombie owner: pause A (likely mid-commit, log requests are slowed), B fences A's appId and adopts its
# WAL, then A resumes. A must stop with outcome "fenced" and never land a commit after the fence.
# A second adoption pass by B picks up anything A wrote to its WAL after the fence.
n=zombie-fence; T=$(scenario $n); D=$WORK/$n
# B adopts from its own copy of A's WAL (replicated or reattached disk), so A keeps running on resume.
copy_a_wal() { mkdir -p "$D/wal-b"; cp -r "$D"/wal/inst-a_* "$D/wal-b/"; }
"$DCB" run ${DCB_ARGS:-} --table-url "$T" --create --app-id inst-a --wal-dir "$D/wal" $(common) \
  --fault-delay-ms 150 --fault-scope log --out "$D/r1.jsonl" 2>"$D/r1.err" &
A=$!
sleep 5; kill -STOP $A; copy_a_wal
"$DCB" run ${DCB_ARGS:-} --table-url "$T" --app-id inst-b --wal-dir "$D/wal-b" --adopt-from inst-a --rate 1 --encoders 1 --duration 3 \
  --out "$D/r2.jsonl" 2>"$D/r2.err"
kill -CONT $A; wait $A; echo "  zombie exit=$? ($(tail -c 300 "$D/r1.err" | tr '\n' ' '))"
copy_a_wal
"$DCB" run ${DCB_ARGS:-} --table-url "$T" --app-id inst-b --wal-dir "$D/wal-b" --adopt-from inst-a --rate 1 --encoders 1 --duration 2 \
  --out "$D/r3.jsonl" 2>"$D/r3.err"
check $n "$T" "$D/wal-b" -- "$D/r1.sent.jsonl" "$D/r2.sent.jsonl" "$D/r3.sent.jsonl"
python3 - "$T" "$D" <<'PY'
import json,glob,sys,os,urllib.parse
t,d=sys.argv[1],sys.argv[2]
log=urllib.parse.urlparse(t).path+"/_delta_log"
fence=None; after=[]
if not t.startswith("file://"):
    # Remote table: fence version from inst-b's log, inst-a's landed versions from its commit records.
    import re
    err=re.sub(r"\x1b\[[0-9;]*m","",open(d+"/r2.err").read())
    m=re.search(r"fence_version=Some\((\d+)\)",err)
    fence=int(m.group(1)) if m else None
    if fence is not None:
        after=[r["delta_version"] for r in map(json.loads,open(d+"/r1.jsonl")) if r.get("kind")=="commit"
               and r["outcome"] in ("ok","ok_after_loop_retry","landed_after_error") and r["delta_version"]>fence]
for f in (sorted(glob.glob(log+"/*.json")) if t.startswith("file://") else []):
    v=int(os.path.basename(f)[:20])
    for l in open(f):
        a=json.loads(l)
        x=a.get("txn")
        if x and x["appId"].startswith("inst-a/"):
            if x["version"]>=1<<62: fence=v
            elif fence is not None: after.append(v)
a_out=[json.loads(l)["outcome"] for l in open(d+"/r1.jsonl") if '"kind":"commit"' in l]
print(f"  fence_version={fence} inst-a commits after fence={after} inst-a outcomes after resume tail={a_out[-3:]}")
PY

# Encoder killed mid-upload (process abort after the first multipart part). The partial blob must never be referenced.
n=crash-mid-upload; T=$(scenario $n); D=$WORK/$n
"$DCB" run ${DCB_ARGS:-} --table-url "$T" --create --app-id inst-a --wal-dir "$D/wal" $(common) --multipart \
  --fault-crash-mid-upload 30 --out "$D/r1.jsonl" 2>"$D/r1.err"
"$DCB" run ${DCB_ARGS:-} --table-url "$T" --app-id inst-a --wal-dir "$D/wal" --rate 1 --encoders 1 --duration 3 \
  --out "$D/r2.jsonl" 2>"$D/r2.err"
check $n "$T" "$D/wal" -- "$D/r1.sent.jsonl" "$D/r2.sent.jsonl"

# 5. Concurrent compaction-style commits while appends commit.
n=concurrent-optimize; T=$(scenario $n); D=$WORK/$n
"$DCB" run ${DCB_ARGS:-} --table-url "$T" --create --app-id inst-a --wal-dir "$D/wal" $(common) --out "$D/r1.jsonl" 2>"$D/r1.err" &
sleep 3
"$DCB" optimize --table-url "$T" --files 5 --every-ms 500 --duration $((DUR - 4)) --out "$D/optimize.log" 2>"$D/opt.err"
wait
check $n "$T" "$D/wal" -- "$D/r1.sent.jsonl"
python3 -c "
import json,collections
c=collections.Counter((json.loads(l)['outcome'],(json.loads(l)['error'] or '')[:60]) for l in open('$D/optimize.log'))
print('  optimize outcomes:',dict(c))"
