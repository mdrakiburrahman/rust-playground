# Darwin codec fixture

| Item | Contract |
| --- | --- |
| Scope | Token-free local Parquet writer and independent decode; not Delta commit throughput. |
| Corpus | `collector_chain::dmv::batch`, availability-replica schema, 65,536 distinct synthetic rows repeated 16 times. This is not the historical `dmv_client` Diagnostic generator. |
| Policy | Registry-validated codec plus compatible level, or dictionary on one physical column; all other candidate values must match the baseline. Legacy LZ4 is excluded. |
| Output | One `DARWIN_CODEC_RESULT=` JSON line; `wall_ns` covers file creation through finish, CPU is process-wide ticks, and bytes include the footer. Footer reports codec family; startup readback reports requested codec/level. |
| Correctness | Each decoded schema and value is compared with the prebuilt input; every row group codec is checked against schema-resolved properties. |

Build and run with a frozen dependency lock and cached dependencies for comparisons.
The candidate's `tunables.parquet_writer` must also be set as `ARROWCOL_PARQUET_CONFIG`
before the process starts. Pass baseline and candidate JSON paths as the two arguments.
Run `cargo test --release --manifest-path tools/darwin/codec-fixture/Cargo.toml` for policy tests.
