# Collector knob screening

| Item | Contract |
|---|---|
| Subject | One collector, one synthetic DMV table, local filesystem Delta |
| Rate | New Delta versions visible between start/end boundary scans; never client ACK rate |
| Client | Replayed diagnostic corpus, zero interval, configurable workers/streams/processes |
| Evidence | Per-cell JSONL, raw client and collector logs, metrics, pressure, commit boundary manifests |
| Resources | Collector user/system CPU, minor/major faults, disk bytes and endpoint RSS |
| Failures | Null committed rate and a comment; raw logs retained; command exits nonzero |
| Kustainer | Not started; its measurement is null with an explicit collector-only comment |
| Isolation | One timed cell at a time; refuse occupied service ports; only stop children created here |
| Storage | Run directories are unique. Generated Delta data is deleted **after** evidence is persisted. |

```bash
cargo build --locked --release -p arrowcol-dataflow-rust -p collector-chain \
  -p arrowcol-catalog-rust --bin arrowcol-dataflow-rust --bin arrowcol-dmv \
  --bin arrowcol-catalog-rust --features arrowcol-dataflow-rust/allocator-mimalloc
npx --no-install tsx --test experiments/bottleneck-war/lane-a/measure.test.ts
npx --no-install tsc -p experiments/bottleneck-war/lane-a/tsconfig.json
npx --no-install tsx experiments/bottleneck-war/lane-a/runner.ts \
  experiments/bottleneck-war/lane-a/smoke.json out/bwa
```

Plans specify `duration`, `warmup`, a `common` environment map and `cells` with
unique `id`, `grid`, `x`, `y` and `env` overrides. `BWA_BIN` selects a prepared
binary directory. `BWA_CCPUS` and `BWA_KCPUS` default to `0-23` and `24-31`.
`BWA_NCLIENTS` defaults to one. The YAML lists collector knobs and defaults;
`BWA_CLIENT_WORKERS`, `BWA_CLIENT_SPW`, `BWA_CLIENT_THREADS`,
`BWA_CLIENT_BATCH_ROWS`, `BWA_CLIENT_CORPUS_ROWS` and `BWA_CLIENT_STRING_WIDTH`
configure the load. Runtime knobs use `ARROWCOL_COLLECTOR_*`.

Use the same binary, allocator and payload in every comparison arm. The output
records binary/config hashes and startup settings. Completed cell IDs are skipped
on resume: use a new ID and a new output directory when changing an experiment.
Never remove `runner.lock` until its recorded process is confirmed stopped.

These are screening measurements, not AKS qualification. The diagnostic corpus
is not the canonical production row shape. Client ACKs measure admission, not
durability, and do not establish offered-capacity headroom. Local filesystem
results do not measure remote streaming uploads or cloud commit latency.
Boundary scan lag is recorded; coarse, bursty commits require longer repeat runs.
