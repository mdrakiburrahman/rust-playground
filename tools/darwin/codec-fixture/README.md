# Darwin codec fixture

| Item | Contract |
| --- | --- |
| Scope | Token-free local Parquet writer and independent decode; not Delta commit throughput. |
| Corpus | `collector_chain::dmv::batch`, availability-replica schema, 65,536 distinct synthetic rows repeated 16 times. This is not the historical `dmv_client` Diagnostic generator. |
| Policy | Registry-validated codec plus compatible level, or dictionary on one physical column; all other candidate values must match the baseline. Legacy LZ4 is excluded. |
| Output | One `DARWIN_CODEC_RESULT=` JSON line; `wall_ns` covers file creation through finish, CPU is process-wide ticks, and bytes include the footer. Footer reports codec family; startup readback reports requested codec/level. |
| Correctness | Each decoded schema and value is compared with the prebuilt input; every row group codec is checked against schema-resolved properties. |

| Command | Result |
| --- | --- |
| `cargo test --locked --release --manifest-path tools/darwin/codec-fixture/Cargo.toml` | Validate coordinate, typed registry, and physical column rules. |
| `npx --no-install tsx tools/darwin/codec-fixture/run.ts --pairs 1` | Run one Zstd3 control/Zstd1 candidate pair. |
| `npx --no-install tsx tools/darwin/codec-fixture/run.ts --pairs 3 --comparison codec-snappy-lz4raw --candidate snappy` | Run three alternating Zstd3/Snappy pairs. |

Install repository Node dependencies before running the driver. Cargo uses this
fixture's checked-in lock; on a fresh machine it downloads locked dependencies.
The driver builds release mode, selects candidates from `throughput-sweeps.ts`
against `experiments/darwin/candidate.oat.json`, and writes one JSON result per
run to stdout. Only `HOME`, `PATH`, optional Cargo/Rustup locations and target
directory, and the candidate Parquet policy reach the fixture. The fixture
rejects mismatched startup policy and verifies decoded values and footer codecs.
