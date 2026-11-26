import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import { checkThroughputCandidate, type ThroughputGroup } from "./throughput-policy.js";

type JsonObject = Record<string, unknown>;
const baseline: unknown = JSON.parse(readFileSync("experiments/darwin/candidate.oat.json", "utf8"));

function object(value: unknown): JsonObject {
  assert.ok(value && typeof value === "object" && !Array.isArray(value));
  return value as JsonObject;
}

function set(root: unknown, path: string, value: unknown): void {
  const parts = path.split(".");
  const field = parts.pop();
  assert.ok(field);
  let current = object(root);
  for (const part of parts) {
    current[part] ??= {};
    current = object(current[part]);
  }
  current[field] = value;
}

function remove(root: unknown, path: string): void {
  const parts = path.split(".");
  const field = parts.pop();
  assert.ok(field);
  let current = object(root);
  for (const part of parts) current = object(current[part]);
  delete current[field];
}

const grid: ReadonlyArray<[ThroughputGroup, string, readonly (number | string)[]]> = [
  ["client", "tunables.client.batch_rows", [4096, 16384, 65536]],
  ["client", "tunables.client.workers", [1, 2, 4]],
  ["client", "tunables.client.connections_per_worker", [1, 2]],
  ["client", "tunables.client.streams_per_worker", [1, 2, 4]],
  ["batching", "tunables.collector.batching.pipeline_depth", [1, 2, 4, 8]],
  ["batching", "tunables.collector.batching.stage_file_concurrency", [1, 7, 16]],
  ["batching", "tunables.collector.batching.stage_files_per_group", [1, 7, 16]],
  ["batching", "tunables.collector.batching.max_concurrent_flushes", [1, 2, 4]],
  ["batching", "tunables.collector.batching.age_flush_policy", ["immediate", "drain_ready"]],
  ["commit", "tunables.collector.commit.batch_window", ["2ms", "10ms"]],
  ["commit", "tunables.collector.commit.max_files_per_commit", [1, 4, 16]],
  ["runtime", "tunables.collector.runtime.cpu_workers", [1, 2, 4]],
  ["runtime", "tunables.collector.runtime.parquet_column_workers", [1, 8, 16]],
  ["runtime", "tunables.collector.runtime.parquet_encode_mode", ["column_waves", "column_pipeline"]],
  ["runtime", "tunables.collector.runtime.cpu_dispatch", ["shared_fifo", "bounded_shards"]],
  ["runtime", "tunables.collector.runtime.cpu_placement", ["os", "compact_allowed", "scatter_allowed", "physical_first"]],
  ["runtime", "tunables.collector.runtime.upload_limit", [1, 2, 4]],
  ["router", "tunables.router.sql_pool_max_size", [1, 2, 4, 8]],
];

test("unchanged frozen seed is a valid control even with out-of-grid baseline values", () => {
  assert.equal(checkThroughputCandidate(baseline, structuredClone(baseline)), "control");
  const candidate = structuredClone(baseline);
  set(candidate, "tunables.client.workers", 4);
  assert.equal(checkThroughputCandidate(baseline, candidate), "client");
});

test("accepts every listed value in its single group", () => {
  for (const [group, path, values] of grid) {
    for (const value of values) {
      const candidate = structuredClone(baseline);
      set(candidate, path, value);
      const result = checkThroughputCandidate(baseline, candidate);
      assert.ok(result === group || result === "control", `${path}=${value}`);
    }
  }
  const candidate = structuredClone(baseline);
  set(candidate, "tunables.collector.batching.pipeline_depth", 4);
  set(candidate, "tunables.collector.batching.age_flush_policy", "drain_ready");
  assert.equal(checkThroughputCandidate(baseline, candidate), "batching");
});

test("rejects type, enum and grid errors without mutating either input", () => {
  for (const [path, value] of [
    ["tunables.client.workers", "2"], ["tunables.client.workers", 0],
    ["tunables.client.batch_rows", 4096.5], ["tunables.client.workers", NaN],
    ["tunables.client.streams_per_worker", null], ["tunables.collector.batching.pipeline_depth", 9],
    ["tunables.collector.batching.stage_file_concurrency", 17], ["tunables.collector.runtime.cpu_dispatch", "bounded"],
    ["tunables.collector.batching.stage_files_per_group", 0], ["tunables.collector.runtime.parquet_column_workers", 17],
    ["tunables.collector.runtime.parquet_encode_mode", "pipeline"],
    ["tunables.collector.runtime.cpu_placement", "compact"],
    ["tunables.collector.batching.age_flush_policy", "drain-ready"],
    ["tunables.collector.commit.batch_window", "0ms"], ["tunables.collector.commit.batch_window", "0s"],
    ["tunables.collector.runtime.upload_limit", 5], ["tunables.router.sql_pool_max_size", 16],
  ] as const) {
    const candidate = structuredClone(baseline);
    set(candidate, path, value);
    const frozen = structuredClone(candidate);
    assert.throws(() => checkThroughputCandidate(baseline, candidate), /invalid/);
    assert.deepEqual(candidate, frozen);
  }
});

test("rejects cross-group changes including explicit overrides of effective defaults", () => {
  for (const [first, value] of [
    ["tunables.collector.batching.pipeline_depth", 1],
    ["tunables.collector.batching.age_flush_policy", "immediate"],
    ["tunables.collector.commit.batch_window", "2ms"],
  ] as const) {
    const candidate = structuredClone(baseline);
    set(candidate, first, value);
    set(candidate, "tunables.client.workers", 4);
    assert.throws(() => checkThroughputCandidate(baseline, candidate), /more than one coordinate group/);
  }
});

test("rejects deletion, object substitution and protected edits", () => {
  for (const path of ["tunables.client.workers", "tunables.router.sql_pool_max_size", "tunables.collector.batching", "tunables.collector.runtime", "tunables.parquet_writer"]) {
    const candidate = structuredClone(baseline);
    remove(candidate, path);
    assert.throws(() => checkThroughputCandidate(baseline, candidate), /cannot remove|protected field/);
  }
  for (const [path, value] of [
    ["tunables.collector", {}], ["tunables.client", null], ["tunables.collector.commit", {}],
    ["tunables.collector.commit", []], ["tunables.collector.batching.coalesce_publications", false],
    ["tunables.client.seed", 1], ["tunables.client.entropy", 0.1],
    ["tunables.parquet_writer.defaults.compression", "snappy"],
    ["tunables.collector.layout", { enabled: false }], ["replica_counts", [2]],
    ["tables", ["another_table"]], ["tunables.collector.commit.max_pending_files", 999],
  ] as const) {
    const candidate = structuredClone(baseline);
    set(candidate, path, value);
    assert.throws(() => checkThroughputCandidate(baseline, candidate));
  }
});

test("rejects keys that impersonate sanctioned paths", () => {
  for (const [path, name, value] of [
    ["", "tunables.client.workers", 2],
    ["tunables", "client.workers", 2],
    ["", "", { tunables: { client: { workers: 2 } } }],
  ] as const) {
    const candidate = structuredClone(baseline);
    const parent = path ? object(object(candidate)[path]) : object(candidate);
    parent[name] = value;
    assert.throws(() => checkThroughputCandidate(baseline, candidate), /protected field/);
  }
});

test("rejects unsafe baseline topology rather than treating it as a valid control", () => {
  for (const [path, value] of [
    ["replica_counts", [2]], ["tables", ["a", "b"]], ["tunables.collector.batching.coalesce_publications", false],
  ] as const) {
    const unsafe = structuredClone(baseline);
    set(unsafe, path, value);
    assert.throws(() => checkThroughputCandidate(unsafe, structuredClone(unsafe)), /baseline must use/);
  }
});
