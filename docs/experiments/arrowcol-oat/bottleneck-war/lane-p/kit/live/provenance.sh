SHA=$(git rev-parse HEAD)
[ "$SHA" = 3259d8cd0fff5eeac77d77308898721cf696f7bd ] || exit 10
[ -z "$(git status --porcelain)" ] || exit 11
SUB=$(git -C submodules/otel-arrow rev-parse HEAD)
export CARGO_HOME="$ROOT/cargo" CARGO_TARGET_DIR="$ROOT/target"
KEY='allocator=mimalloc;lto=off;codegen_units=16;opt_level=3;target_cpu=generic'
PH=$(printf '%s' "$KEY" | sha256sum | cut -d ' ' -f 1)
F="$ROOT/tools/footer"; B="$ROOT/bin/$SHA"
[ -x "$B/arrowcol-dataflow-rust" ] && [ -x "$ROOT/fix/footer" ] && [ -f "$ROOT/build.end" ] || exit 30
for n in arrowcol-dataflow-rust arrowcol-dmv arrowcol-catalog-rust delta-maintenance; do cmp -s "$CARGO_TARGET_DIR/release/$n" "$B/$n" || exit 31; done
export ROOT
export B SHA SUB PH KEY F
node - <<'EOF' || exit 24
const fs = require('fs'), cp = require('child_process'), c = require('crypto');
const h = p => c.createHash('sha256').update(fs.readFileSync(p)).digest('hex');
const e = process.env, R = e.ROOT;
const names = ['arrowcol-dataflow-rust', 'arrowcol-dmv', 'arrowcol-catalog-rust', 'delta-maintenance'];
const p = {
  collector_catalog_maintenance_source: e.SHA, client_source: e.SHA, otel_arrow_submodule: e.SUB,
  allocator: 'mimalloc', features: 'arrowcol-dataflow-rust/allocator-mimalloc', release_lto: 'off', codegen_units: 16,
  opt_level: 3, strip: 'none', target_cpu: 'generic', rustflags: null, profile_key: e.KEY, profile_hash: e.PH,
  collector_build_env: { COLLECTOR_BUILD_SOURCE_REVISION: e.SHA, COLLECTOR_BUILD_PROFILE_HASH: e.PH, COLLECTOR_BUILD_ALLOCATOR: 'mimalloc' },
  rustc: cp.execFileSync('rustc', ['--version'], { encoding: 'utf8' }).trim(),
  cargo: cp.execFileSync('cargo', ['--version'], { encoding: 'utf8' }).trim(),
  cargo_home: e.CARGO_HOME, target_dir: e.CARGO_TARGET_DIR, cargo_lock_sha256: h('Cargo.lock'),
  build_start: fs.readFileSync(R + '/build.start', 'utf8').trim(), build_end: fs.readFileSync(R + '/build.end', 'utf8').trim(),
  build_log_sha256: h(R + '/build.log'),
  binaries: Object.fromEntries(names.map(n => [n, h(e.B + '/' + n)])),
  footer_tool: { sha256: h(R + '/fix/footer'), source_sha256: h(e.F + '/src/main.rs'), manifest_sha256: h(e.F + '/Cargo.toml'),
    lock_sha256: h(e.F + '/Cargo.lock'), parquet: '58.4.0' },
  recipe: 'projects/arrowcol-dataflow-rust/Dockerfile profile (mimalloc, lto off, 16 CGU, opt 3, generic); strip none',
};
fs.writeFileSync(e.B + '/build-provenance.json', JSON.stringify(p, null, 2) + '\n');
console.log(JSON.stringify(p));
EOF
chmod 0444 "$B/build-provenance.json"; chmod 0555 "$B"
echo "BWA_BIN=$B"; ls -la "$B" "$ROOT/fix/footer"
echo "== post-build load"; uptime; df -h "$ROOT" | tail -1
