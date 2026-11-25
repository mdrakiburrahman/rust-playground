SHA=$(git rev-parse HEAD)
[ "$SHA" = 3259d8cd0fff5eeac77d77308898721cf696f7bd ] || exit 10
[ -z "$(git status --porcelain)" ] || exit 11
B="$ROOT/bin/$SHA"; L="$ROOT/target/release/arrowcol-loadgen"
[ -x "$L" ] && [ -f "$ROOT/build-loadgen.end" ] && [ -f "$B/build-provenance.json" ] || exit 30
[ -e "$B/arrowcol-loadgen" ] && { echo "LOADGEN_ALREADY_INSTALLED"; exit 32; }
for n in arrowcol-dataflow-rust arrowcol-dmv arrowcol-catalog-rust delta-maintenance; do cmp -s "$ROOT/target/release/$n" "$B/$n" || exit 31; done
echo "== loadgen build record"; cat "$ROOT/build-loadgen.start" "$ROOT/build-loadgen.end"; grep -m3 -E 'Compiling arrowcol-loadgen|Finished' "$ROOT/build-loadgen.log"
OLD=$(sha256sum "$B/build-provenance.json" | cut -d' ' -f1); echo "old_provenance_sha256 $OLD"
chmod u+w "$B" "$B/build-provenance.json" || exit 33
install -m 0555 "$L" "$B/arrowcol-loadgen" || exit 34
export ROOT B OLD
node - <<'EOF' || exit 24
const fs = require('fs'), c = require('crypto');
const h = p => c.createHash('sha256').update(fs.readFileSync(p)).digest('hex');
const e = process.env, R = e.ROOT, f = e.B + '/build-provenance.json';
const p = JSON.parse(fs.readFileSync(f, 'utf8'));
for (const [n, v] of Object.entries(p.binaries)) if (h(e.B + '/' + n) !== v) throw new Error('binary drift ' + n);
if (h(R + '/target/release/arrowcol-loadgen') !== h(e.B + '/arrowcol-loadgen')) throw new Error('install mismatch');
p.binaries['arrowcol-loadgen'] = h(e.B + '/arrowcol-loadgen');
p.aux_builds = { 'arrowcol-loadgen': {
  role: 'OAT local table initializer (initialize-local); runs before the catalog in every cell',
  source: p.collector_catalog_maintenance_source, profile_key: p.profile_key, release_lto: p.release_lto,
  codegen_units: p.codegen_units, opt_level: p.opt_level, strip: p.strip, rustflags: p.rustflags,
  cargo_home: p.cargo_home, target_dir: p.target_dir, collector_build_env: null,
  build_start: fs.readFileSync(R + '/build-loadgen.start', 'utf8').trim(),
  build_end: fs.readFileSync(R + '/build-loadgen.end', 'utf8').trim(),
  build_log_sha256: h(R + '/build-loadgen.log') } };
p.previous_provenance_sha256 = e.OLD;
fs.writeFileSync(f, JSON.stringify(p, null, 2) + '\n');
console.log(JSON.stringify({ binaries: p.binaries, aux_builds: p.aux_builds, previous_provenance_sha256: p.previous_provenance_sha256 }));
EOF
chmod 0444 "$B/build-provenance.json"; chmod 0555 "$B"
echo "new_provenance_sha256 $(sha256sum "$B/build-provenance.json" | cut -d' ' -f1)"
ls -la "$B"
