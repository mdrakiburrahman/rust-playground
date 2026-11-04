#!/usr/bin/env bash
# Long-retention runs against ADLS Gen2 (HNS), from a VM in the bench VNet (managed identity auth).
# Usage: lr/scripts/azure.sh <abfss-base-url> <out-dir> [step...]
#   Steps: gen dl kernel ckpt q6 multi (default: all, in that order).
# Example: AZURE_CLIENT_ID=<mi client id> lr/scripts/azure.sh abfss://bench@acct.dfs.core.windows.net/lr ~/lr-out
set -uo pipefail
BASE=${1:?abfss base url}
OUT=${2:?out dir}
shift 2
STEPS=${*:-gen dl kernel ckpt q6 multi}
B=${LRB:-$HOME/lrb}
BJ=${LRB_JE:-$HOME/lrb-je}
mkdir -p "$OUT"
ulimit -n "$(ulimit -Hn)"
export LR_CKPT_COMPRESSION=zstd LR_CKPT_ROWGROUP=131072
T() { /usr/bin/time -f "time_rss_kb=%M wall=%e" "$@"; }

for step in $STEPS; do
  case $step in
  gen)
    for n in 200000 1000000 30000000; do
      T "$B" gen --table-url "$BASE/c$n" --active $n --threads 14 >"$OUT/gen-c$n.json" 2>"$OUT/gen-c$n.err"
    done
    for i in $(seq -f "%03g" 0 99); do
      "$B" gen --table-url "$BASE/warm/t$i" --active 550000 --threads 14 >/dev/null 2>>"$OUT/gen-warm.err"
    done
    ;;
  dl)
    for n in 200000 1000000; do
      for lib in dl1 dl2; do
        T "$B" load --lib $lib --table-url "$BASE/c$n" >"$OUT/load-$lib-c$n.json" 2>"$OUT/load-$lib-c$n.err"
        T "$B" commit --lib $lib --table-url "$BASE/c$n" --commits 5 --app-id "inst-az-$lib/t#e1" \
          >"$OUT/commit-$lib-c$n.json" 2>"$OUT/commit-$lib-c$n.err"
      done
    done
    ;;
  kernel)
    for n in 1000000 30000000; do
      T "$B" load --lib kernel --table-url "$BASE/c$n" >"$OUT/load-kernel-c$n.json" 2>"$OUT/load-kernel-c$n.err"
      T "$B" commit --lib kernel --table-url "$BASE/c$n" --commits 300 --app-id "inst-az/t#e1" \
        >"$OUT/commit-kernel-c$n.json" 2>"$OUT/commit-kernel-c$n.err"
      T "$B" commit --lib kernel --table-url "$BASE/c$n" --commits 300 --interval-ms 100 --app-id "inst-az10/t#e1" \
        >"$OUT/commit10-kernel-c$n.json" 2>"$OUT/commit10-kernel-c$n.err"
    done
    ;;
  ckpt)
    T "$B" checkpoint --lib kernel --table-url "$BASE/c30000000" >"$OUT/ckpt-kernel-c30m.json" 2>"$OUT/ckpt-kernel-c30m.err"
    T "$B" load --lib kernel --table-url "$BASE/c30000000" >"$OUT/load2-kernel-c30m.json" 2>"$OUT/load2-kernel-c30m.err"
    ;;
  q6)
    # Drop one day's partition (Q6_DAY, default 1) while kernel appends run at 10 commits/s.
    for n in 1000000 30000000; do
      T "$B" maint --table-url "$BASE/c$n" --kind drop --active $n --day "${Q6_DAY:-1}" --delay-s 10 \
        >"$OUT/q6-maint-c$n.json" 2>"$OUT/q6-maint-c$n.err" &
      m=$!
      "$B" commit --lib kernel --table-url "$BASE/c$n" --commits 300 --interval-ms 100 --trace \
        --app-id "inst-q6/t#e1" >"$OUT/q6-commit-c$n.json" 2>"$OUT/q6-commit-c$n.err"
      wait $m
    done
    ;;
  multi)
    T "$BJ" multi --hot "$BASE/c30000000" --hot-interval-ms 100 --hot-ckpt-every 3000 \
      --warm-prefix "$BASE/warm/t" --warm-count 100 --warm-interval-ms 1000 --warm-ckpt-every 300 \
      --max-commits 16 --max-ckpts 3 --duration-s 600 >"$OUT/multi.json" 2>"$OUT/multi.err"
    ;;
  *) echo "unknown step $step" >&2; exit 2 ;;
  esac
  echo "$(date +%T) done $step"
done
