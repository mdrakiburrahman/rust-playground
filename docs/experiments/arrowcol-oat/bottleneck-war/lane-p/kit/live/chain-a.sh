B=kit/reference/defaults-rate.json
F=kit/reference/data/defaults-rate/defaults-rate-a4az2c/footer.json
R="$ROOT/runs"
del() {
  n=$(find "$R/$1" "$R/$1.warm" -type f -name '*.parquet' 2>/dev/null | wc -l)
  find "$R/$1" "$R/$1.warm" -type f -name '*.parquet' -delete 2>/dev/null
  echo "PARQUET_DELETED $1=$n"; df -h "$ROOT" | tail -1
}
phase() {
  echo "### PHASE $1"
  ROOT="$ROOT" bash "$ROOT/tools/phase.sh" "$1" "$R/$2"; prc=$?
  echo "PHASE_RC $1=$prc"; del "$1"; sha256sum "$R/$1/results.jsonl" 2>/dev/null
  return $prc
}
( cd "$ROOT" || exit 3; node kit/screen-plan.mjs conditional $B $F runs/cond2.json runs/pilot3.json runs/pilot3/results.jsonl ); echo "COND_PLAN_RC=$?"
s=$(sha256sum "$R/cond2.json" | cut -d' ' -f1); echo "$s cond2.json"
[ "$s" = e91cce7b87238156dc7b9ac6c730c659ad7cb39aaaadf3f5eaee2b65daf893f2 ] || { echo "COND2_PLAN_NOT_IDENTICAL"; exit 31; }
phase cond2 cond2.json || exit 32
( cd "$ROOT" || exit 3; node kit/confirm.mjs select $B $F runs/pilot3.json runs/pilot3/results.jsonl runs/grid.json runs/grid/results.jsonl runs/selection.json runs/room-fast.json runs/cond2.json runs/cond2/results.jsonl ); src=$?
echo "SELECT_RC=$src"
[ "$src" -eq 0 ] || { echo "SCREENING_STOP conditional attempts exhausted"; exit 33; }
sha256sum "$R/selection.json" "$R/room-fast.json"
( cd "$ROOT" || exit 3; node kit/screen-plan.mjs headroom $B $F runs/room-default2.json ); echo "ROOM_PLAN_RC=$?"
s=$(sha256sum "$R/room-default2.json" | cut -d' ' -f1); echo "$s room-default2.json"
[ "$s" = c13c7161e2ba9877bdf272b4522dcfa09e7ff8950e8c3f91af78b002499fb9aa ] || { echo "ROOM_PLAN_NOT_IDENTICAL"; exit 34; }
sleep 150
phase room-default2 room-default2.json || exit 35
sleep 150
phase room-fast room-fast.json || exit 36
( cd "$ROOT" || exit 3; node kit/confirm.mjs register $B $F runs/selection.json runs/room-default2.json runs/room-default2/results.jsonl runs/room-fast.json runs/room-fast/results.jsonl "$ROOT/fix" runs/confirm.json runs/registration.json ); rrc=$?
echo "REGISTER_RC=$rrc"
[ "$rrc" -eq 0 ] || exit 37
sha256sum "$R/confirm.json" "$R/registration.json"
echo CHAIN_A_DONE
