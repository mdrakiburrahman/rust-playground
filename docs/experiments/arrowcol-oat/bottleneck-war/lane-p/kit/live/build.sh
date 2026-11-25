echo "== pre-build load"; uptime; ps -eo pid,pcpu,etimes,comm --sort=-pcpu | head -6; df -h "$ROOT" | tail -1
SHA=$(git rev-parse HEAD)
[ "$SHA" = 3259d8cd0fff5eeac77d77308898721cf696f7bd ] || exit 10
[ -z "$(git status --porcelain)" ] || exit 11
GIT_TERMINAL_PROMPT=0 git submodule update --init --recursive >"$ROOT/submodule.log" 2>&1 || { tail -20 "$ROOT/submodule.log"; exit 12; }
[ -z "$(git status --porcelain)" ] || { git status --porcelain | head; exit 13; }
SUB=$(git -C submodules/otel-arrow rev-parse HEAD)
echo "submodule=$SUB"
export CARGO_HOME="$ROOT/cargo" CARGO_TARGET_DIR="$ROOT/target"
export CARGO_PROFILE_RELEASE_LTO=off CARGO_PROFILE_RELEASE_CODEGEN_UNITS=16 CARGO_PROFILE_RELEASE_OPT_LEVEL=3 CARGO_PROFILE_RELEASE_STRIP=none
KEY='allocator=mimalloc;lto=off;codegen_units=16;opt_level=3;target_cpu=generic'
PH=$(printf '%s' "$KEY" | sha256sum | cut -d ' ' -f 1)
export COLLECTOR_BUILD_PROFILE_HASH=$PH COLLECTOR_BUILD_ALLOCATOR=mimalloc COLLECTOR_BUILD_LTO=off COLLECTOR_BUILD_CODEGEN_UNITS=16
export COLLECTOR_BUILD_SOURCE_REVISION=$SHA COLLECTOR_BUILD_OPT_LEVEL=3 COLLECTOR_BUILD_TARGET_CPU=generic
export ARROWCOL_BUILD_LTO=off ARROWCOL_BUILD_CODEGEN_UNITS=16 ARROWCOL_BUILD_OPT_LEVEL=3 ARROWCOL_BUILD_TARGET_CPU=generic
unset RUSTFLAGS
mkdir -p "$CARGO_HOME" "$CARGO_TARGET_DIR" "$ROOT/tools" "$ROOT/fix" "$ROOT/bin" || exit 14
date -u +%FT%TZ > "$ROOT/build.start"
cargo build --locked --release -j16 -p collector-chain -p arrowcol-catalog-rust -p delta-maintenance -p arrowcol-dataflow-rust \
  --bin arrowcol-dmv --bin arrowcol-catalog-rust --bin delta-maintenance --bin arrowcol-dataflow-rust \
  --features arrowcol-dataflow-rust/allocator-mimalloc >"$ROOT/build.log" 2>&1 || { tail -40 "$ROOT/build.log"; exit 15; }
date -u +%FT%TZ > "$ROOT/build.end"
tail -3 "$ROOT/build.log"
[ -z "$(git status --porcelain)" ] || { git status --porcelain | head; exit 16; }
F="$ROOT/tools/footer"; mkdir -p "$F/src" || exit 17
cat >"$F/Cargo.toml" <<'EOF'
[package]
name = "bwp-footer"
version = "0.1.0"
edition = "2021"
publish = false

[dependencies]
parquet = { version = "=58.4.0", default-features = false, features = ["snap", "brotli", "flate2-zlib-rs", "lz4", "zstd"] }
serde_json = "=1.0.151"

[workspace]
EOF
cp "$ROOT/fix/bc4-footer.rs" "$F/src/main.rs" || exit 18
cp Cargo.lock "$F/Cargo.lock" || exit 19
(cd "$F" && CARGO_TARGET_DIR="$F/target" cargo build --release -j16 >"$F/build.log" 2>&1) || { tail -30 "$F/build.log"; exit 20; }
grep -A1 -E '^name = "parquet"$' "$F/Cargo.lock"
B="$ROOT/bin/$SHA"; rm -rf "$B"; mkdir -p "$B" || exit 21
for n in arrowcol-dataflow-rust arrowcol-dmv arrowcol-catalog-rust delta-maintenance; do install -m 0555 "$CARGO_TARGET_DIR/release/$n" "$B/$n" || exit 22; done
install -m 0555 "$F/target/release/bwp-footer" "$ROOT/fix/footer" || exit 23
export ROOT B SHA SUB PH KEY F
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
