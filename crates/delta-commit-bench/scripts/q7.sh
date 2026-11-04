#!/usr/bin/env bash
# Q7: crashes around tenure-epoch claims, and an adopter whose claim is the first invalidation.
# Usage: scripts/q7.sh <table-base-url> <work-dir> [scenario...]
#   Scenarios: claim-crash-before claim-crash-after adopter-invalidated-first (default: all).
#   DCB_ARGS is passed to every `dcb run` (e.g. DCB_ARGS="--lib kernel").
# Example: DCB_ARGS="--lib kernel" scripts/q7.sh file:///tmp/dcb-q7 /tmp/dcb-q7-work
# Each scenario writes <work>/<name>/check.json; scripts/own_sum.py summarizes. Runs in tenure mode.
set -uo pipefail
BASE=${1:?table base url}
WORK=${2:?work dir}
shift 2
SCEN=${*:-claim-crash-before claim-crash-after adopter-invalidated-first}
HOT=${HOT:-1.2}
DCB=${DCB:-$(dirname "$0")/../../../target/release/dcb}
RUN_ID=${RUN_ID:-$(date +%s)}
mkdir -p "$WORK"

scenario() { local name=q7-$1; mkdir -p "$WORK/$name"; D=$WORK/$name; T=$BASE/$RUN_ID-$name; N=$name; }
route() { echo "$1" >"$D/route"; echo "$(date +%s%3N) $1" >>"$D/route.log"; }
# In a background subshell, exec so that $! is the dcb process and SIGSTOP pauses it.
x() { if [ "$BASHPID" != "$$" ]; then exec "$@"; else "$@"; fi; }
dcb() { x "$DCB" run ${DCB_ARGS:-} --epochs tenure --checkpoint-every 50 --encoders 2 --route-file "$D/route" "$@"; }
backlog() { echo --rate 20 --max-files-per-commit 1 --fault-delay-ms 150 --fault-scope log; }
check() {
  "$DCB" check --table-url "$T" --wal-dir "$D/wal-a" --wal-dir "$D/wal-b" $(ls "$D"/*.sent.jsonl | sed 's/^/--sent /') \
    >"$D/check.json" 2>"$D/check.err"
  python3 -c "
import json; c=json.load(open('$D/check.json'))
print(f'$N'.ljust(28), *(f'{k}={c[k]!s:5}' for k in ('g1_no_loss','g2_no_duplicates','g3_atomic','g4_valid')),
      f'accepted={c[\"accepted_files\"]} missing={c[\"missing_accepted_count\"]} dups={c[\"duplicate_paths_count\"]} pending={c[\"manifests_pending\"]} appIds={len(c[\"txn_app_ids\"])}')"
}
# Outcome counts of a run's commit records, and whether it aborted on an injected fault.
outcomes() {
  python3 - "$1" "$2" <<'EOF'
import collections, json, sys
c = collections.Counter()
retries = 0
for line in open(sys.argv[1]):
    if line.startswith("{"):
        e = json.loads(line)
        k = e.get("kind")
        c[e["outcome"] if k == "commit" else k] += 1
        retries += e.get("library_retries") or 0
fault = next((l.strip() for l in open(sys.argv[2]) if l.startswith("fault:")), "")
print(f"  {sys.argv[1].rsplit('/', 1)[-1]}: {dict(c)} library_retries={retries} {fault}")
EOF
}
# After the first claim that fences X's first epoch appId, no commit may carry that appId as a
# writer. Remote tables: claim version from a1's abort line, X's landed versions from x.jsonl.
x_after_claim() {
  case $T in file://*) ;; *)
    python3 - "$D" <<'EOF'
import json, re, sys
d = sys.argv[1]
m = re.search(r"claim_version=Some\((\d+)\)", open(f"{d}/a1.err").read())
cv = int(m.group(1)) if m else None
rs = [json.loads(l) for l in open(f"{d}/x.jsonl")]
e1 = next((r["app_id"] for r in rs if r.get("kind") == "commit"), None)
late = [r["delta_version"] for r in rs if r.get("kind") == "commit" and r["app_id"] == e1
        and r["outcome"] in ("ok", "ok_after_loop_retry", "landed_after_error") and cv is not None and r["delta_version"] > cv]
print(f"  x-after-claim: {e1} invalidated at v{cv} by claim; commits by it after: {late or 'none'}")
EOF
    return ;; esac
  python3 - "${T#file://}/_delta_log" <<'EOF'
import glob, json, os, sys
OFF = 1 << 62
log = []
for p in sorted(glob.glob(f"{sys.argv[1]}/*.json")):
    v = int(os.path.basename(p).split(".")[0])
    acts = [json.loads(l) for l in open(p) if l.strip()]
    log.append((v, acts))
def txns(acts):
    return [(a["txn"]["appId"], a["txn"]["version"]) for a in acts if "txn" in a]
def kind(acts):
    return next((a["commitInfo"].get("dcbKind") for a in acts if "commitInfo" in a), None)
e1 = next((app for v, acts in log for app, tv in txns(acts) if app.startswith("inst-a/") and app.endswith("#e1")), None)
if e1 is None:
    print("  x-after-claim: no inst-a #e1 appId found"); sys.exit()
fv = next((v for v, acts in log for app, tv in txns(acts) if app == e1 and tv >= OFF), None)
late = [v for v, acts in log if fv is not None and v > fv
        and any(app == e1 and tv < OFF for app, tv in txns(acts))]
print(f"  x-after-claim: {e1} invalidated at v{fv} by {kind(dict(log)[fv]) if fv is not None else '-'}; "
      f"commits by it after: {late or 'none'}")
EOF
}

for s in $SCEN; do case $s in
claim-crash-before|claim-crash-after)
  # X crashes with pending manifests; Y owns T and fences X. X restarts and aborts right before
  # (or right after) its drain claim; a final restart drains.
  point=${s#claim-crash-}; scenario $s
  route inst-a
  dcb --table-url "$T" --create --app-id inst-a --wal-dir "$D/wal-a" --rate 10 --duration 30 \
    --fault-crash-before-log-put 20 --out "$D/x1.jsonl" 2>"$D/x1.err"
  route inst-b
  dcb --table-url "$T" --app-id inst-b --wal-dir "$D/wal-b" --fence-identity inst-a --rate "$HOT" --duration 15 \
    --out "$D/y.jsonl" 2>"$D/y.err" &
  Y=$!; sleep 5
  dcb --table-url "$T" --app-id inst-a --wal-dir "$D/wal-a" --drain-only --fault-crash-$point-claim 1 \
    --out "$D/x2.jsonl" 2>"$D/x2.err"
  sleep 1
  dcb --table-url "$T" --app-id inst-a --wal-dir "$D/wal-a" --drain-only --out "$D/x3.jsonl" 2>"$D/x3.err"
  wait $Y; check
  for p in x1 x2 x3 y; do outcomes "$D/$p.jsonl" "$D/$p.err"; done ;;
adopter-invalidated-first)
  # X paused with a backlog and a commit in flight. Adopter A mounts X's WAL (same directory),
  # claims X's identity, which invalidates X's current epoch appId, and aborts right after the
  # claim. X resumes and must lose. A restarts and drains; X restarts and drains.
  scenario $s; mkdir -p "$D/wal-b"
  route inst-a
  dcb --table-url "$T" --create --app-id inst-a --wal-dir "$D/wal-a" $(backlog) --duration 30 \
    --out "$D/x.jsonl" 2>"$D/x.err" &
  X=$!; sleep 6; kill -STOP $X; route none
  dcb --table-url "$T" --app-id inst-a --wal-dir "$D/wal-a" --drain-only --fault-crash-after-claim 1 \
    --out "$D/a1.jsonl" 2>"$D/a1.err"
  kill -CONT $X; wait $X
  dcb --table-url "$T" --app-id inst-a --wal-dir "$D/wal-a" --drain-only --out "$D/a2.jsonl" 2>"$D/a2.err"
  dcb --table-url "$T" --app-id inst-a --wal-dir "$D/wal-a" --drain-only --out "$D/x2.jsonl" 2>"$D/x2.err"
  check
  for p in x a1 a2 x2; do outcomes "$D/$p.jsonl" "$D/$p.err"; done
  x_after_claim ;;
*) echo "unknown scenario $s" >&2 ;;
esac; done
