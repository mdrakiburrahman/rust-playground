# Offline Realistic delivery equality

| Input/output | Contract |
| --- | --- |
| `snapshot.json` | `{ "table": "sqlserver_availability_replica_states", "rewrite_commit_count": 0, "manifest_paths": ["/run/completed-manifest.json"], "active_parquet_paths": ["/run/active-ingestion.parquet"] }`; paths must come from the trusted stopped-snapshot reconciler. |
| Terminal manifests | One per client (1–1,024), complete identity-bearing Realistic v1, all prepared batches ACKed exactly once, with no retry, ambiguity, error or truncation. The extractor preserves the exact manifest slices embedded in the OAT evidence; these are not claimed to be the original producer log bytes. |
| Exact equality | Complete unique run/incarnation/worker/sequence/row identities, with worker IDs 0–1,023. Missing, foreign, duplicate, rewritten or aliased input fails. Numeric u64 tokens are parsed exactly, not rounded through JavaScript numbers. |
| Scope | Whole active ingestion snapshot including warmup and drain; producer first-send cohorts are not Delta commit-window membership. Physical Add/history reconciliation and business payload checks are separate. |
| One-shot v1 | Complete ACKed `unscheduled` ranges with null bounds use a separate cohort; no window is invented. Scheduled ranges require matching client windows. |
| Memory | Default bitmaps <=512 MiB, <=65,536 ranges and <=64 MiB manifest sources. The reader projects only `endpoint_url`, one file and 8,192-row batch at a time; Parquet buffers are additional. |
| Frozen artifacts | Pin reader SHA256 and framed SHA256 of both TypeScript modules. Original manifest and snapshot bytes are hashed; file and artifact identities are checked before and after verification. |
| Result | Compact counts and hashes; nonzero exit on failure. This tool does not discover files, read Delta history or qualify throughput. |
| OAT integration | Every report must use the same inspected image and business corpus; every client readback is checked and retained. Cumulative completed rows are checked and summed against stopped receipts. Evidence is bounded to 16 MiB, each extracted manifest to 1 MiB. |

```bash
cargo build --locked --release --package collector-chain --bin delivery-identity-reader
VERIFIER_SHA=$(npx --no-install tsx tools/darwin/src/delivery-identity-cli.ts --artifact-hash)
DECODER_SHA=$(sha256sum target/release/delivery-identity-reader | cut -d' ' -f1)
npx --no-install tsx tools/darwin/src/delivery-identity-cli.ts \
  --input "$SNAPSHOT_JSON" --decoder target/release/delivery-identity-reader \
  --expected-decoder-sha256 "$DECODER_SHA" --expected-verifier-sha256 "$VERIFIER_SHA"
```

Integration can call `verifyDeliveryIdentitySet(fullManifests,
AsyncIterable<string>, { table, rewriteCommitCount: 0, maxBitmapBytes,
maxRanges })` while separately binding original evidence JSON bytes.
`verifyActiveParquetIdentitySet` takes the CLI input and frozen artifact hashes.
Decoder children are stopped and reaped on early failure or interruption.
