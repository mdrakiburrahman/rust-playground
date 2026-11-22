# Offline Realistic delivery equality

| Input/output | Contract |
|---|---|
| `snapshot.json` | `{ "table": "sqlserver_availability_replica_states", "rewrite_commit_count": 0, "manifest_paths": ["/run/completed-manifest.json"], "active_parquet_paths": ["/run/active-ingestion.parquet"] }`; paths must come from the trusted stopped-snapshot reconciler. |
| Terminal manifest | Complete identity-bearing Realistic v1, all prepared batches ACKed exactly once, with no retry, ambiguity, error or truncation. Preserve original serialized bytes: JavaScript reserialization may round nanosecond integers. |
| Exact equality | Complete unique run/incarnation/worker/sequence/row identities. Missing, foreign, duplicate, rewritten or aliased input fails. |
| Scope | Whole active ingestion snapshot including warmup and drain; producer first-send cohorts are not Delta commit-window membership. Physical Add/history reconciliation and business payload checks are separate. |
| Memory | Default bitmaps <=512 MiB, <=65,536 ranges and <=64 MiB manifest sources. The reader projects only `endpoint_url`, one file and 8,192-row batch at a time; Parquet buffers are additional. |
| Frozen artifacts | Pin reader SHA256 and framed SHA256 of both TypeScript modules. Original manifest and snapshot bytes are hashed; file and artifact identities are checked before and after verification. |
| Result | Compact counts and hashes; nonzero exit on failure. This tool does not discover files, read Delta history or qualify throughput. |

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
