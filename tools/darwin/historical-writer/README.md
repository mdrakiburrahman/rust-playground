# Historical Diagnostic Delta writer fixture

| Item | Scope |
|---|---|
| Workload | Actual `arrowcol-dmv` Diagnostic generator at `6f5832394236c3df60bdd8f24744c1c3f212ff87`; 65,536 generated corpus rows, shared worker-0 corpus, `direct-e64-confirmwide`, seed 20260923, width 0/default, cardinality 1024. |
| Baseline | Current branch workspace lock and coalesced `BatchedTable` / `DeltaFileWriter` staging, local unpartitioned table, layout settings from the historical deployment: 262,144 row-group rows, 2,000,000 max rows and 1 GiB max input. Schema-dependent layout sorts and blooms on `customer_id`. |
| Predeclared candidate | Disable dictionary **only** on `last_connect_error_number`. The captured corpus has 1,024 distinct values in 65,536 rows and baseline data pages use dictionary encoding. This is not a high-cardinality corpus; the hypothesis is that plain INT32 avoids dictionary work, at the cost of file bytes. |
| Checks | One file and one row group, UNCOMPRESSED codec, selected column's actual data-page encoding, normalized physical schema and every decoded value in order; the driver rejects changes to corpus, sort metadata or other column footers. |
| Timing | Each process warms the coalesced stage once, then measures process CPU and elapsed time through local staging and file close. Input generation, capture, decoding and local table-owner startup are outside that timer. No remote upload or Delta Add/commit throughput is measured. |

Build the historical client and run from this fixture's checkout. The client worktree and output path must not already exist. Initialize the historical worktree's submodules and the current checkout's submodules before building.

```sh
git worktree add --detach /tmp/darwin-historical-client 6f5832394236c3df60bdd8f24744c1c3f212ff87
git -C /tmp/darwin-historical-client submodule update --init --recursive
CARGO_TARGET_DIR=/tmp/darwin-historical-client-target cargo build --locked --release --manifest-path /tmp/darwin-historical-client/Cargo.toml -p collector-chain --bin arrowcol-dmv
git submodule update --init --recursive
CARGO_TARGET_DIR=/tmp/darwin-historical-writer-target npx --no-install tsx tools/darwin/historical-writer/run.ts --client /tmp/darwin-historical-client-target/release/arrowcol-dmv --out /tmp/darwin-historical-results
```

The driver builds with `--locked`, uses a scrubbed environment, runs three alternating pairs after a per-process warmup, and saves each command, exit, stdout and stderr plus `summary.json` outside Git. Each arm regenerates and captures the same source corpus before its timer. The source/config pin is not proof of the deployed image's source or its effective table overrides; local staging does not reproduce Azure upload or ingestion partition creation.
