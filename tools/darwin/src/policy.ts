export const experimentPaths = {
  "client-encode": ["bin/collector-chain/src/dmv_client.rs"],
  "router-sql": ["projects/arrowcol-router-rust/src/router.rs", "crates/arrowcol-routing/src/sql_server.rs"],
  "flight-envelope": ["crates/arrowcol-flight/src/service.rs", "crates/arrowcol-dataflow-components/src/envelope.rs"],
  "layout-sort": ["crates/arrow-deltalake/src/layout.rs"],
  "parquet-encode": ["crates/arrow-deltalake/src/lib.rs"],
  "commit-pipeline": ["crates/arrowcol-dataflow-components/src/exporter.rs", "crates/arrow-deltalake/src/delta.rs", "crates/arrow-deltalake/src/commit.rs"],
  "maintenance-active": ["crates/arrow-deltalake/src/maintenance.rs"],
  "maintenance-idle": ["crates/arrow-deltalake/src/maintenance.rs"],
  "throughput-config": ["experiments/darwin/candidate.oat.json"],
  "compression-config": ["experiments/darwin/candidate.oat.json"],
} as const;

export type Experiment = keyof typeof experimentPaths;

export function experimentPathsFor(name: string): readonly string[] {
  if (!Object.hasOwn(experimentPaths, name)) throw new Error("unknown experiment");
  return experimentPaths[name as Experiment];
}
