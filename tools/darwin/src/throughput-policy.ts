import { isDeepStrictEqual } from "node:util";

type JsonObject = Record<string, unknown>;

export type ThroughputGroup = "control" | "client" | "batching" | "commit" | "runtime" | "router";

type Field = {
  readonly group: Exclude<ThroughputGroup, "control">;
  readonly values: readonly (number | string)[];
};

const fields: Readonly<Record<string, Field>> = {
  "tunables.client.batch_rows": { group: "client", values: [4096, 16384, 65536] },
  "tunables.client.workers": { group: "client", values: [1, 2, 4] },
  "tunables.client.connections_per_worker": { group: "client", values: [1, 2] },
  "tunables.client.streams_per_worker": { group: "client", values: [1, 2, 4] },
  "tunables.collector.batching.pipeline_depth": { group: "batching", values: [1, 2, 4, 8] },
  "tunables.collector.batching.stage_file_concurrency": { group: "batching", values: Array.from({ length: 16 }, (_, i) => i + 1) },
  "tunables.collector.batching.max_concurrent_flushes": { group: "batching", values: [1, 2, 4] },
  "tunables.collector.batching.age_flush_policy": { group: "batching", values: ["immediate", "drain_ready"] },
  "tunables.collector.commit.batch_window": { group: "commit", values: ["2ms", "10ms"] },
  "tunables.collector.commit.max_files_per_commit": { group: "commit", values: [1, 4, 16] },
  "tunables.collector.runtime.cpu_workers": { group: "runtime", values: [1, 2, 4] },
  "tunables.collector.runtime.cpu_dispatch": { group: "runtime", values: ["shared_fifo", "bounded_shards"] },
  "tunables.collector.runtime.cpu_placement": { group: "runtime", values: ["os", "compact_allowed", "scatter_allowed", "physical_first"] },
  "tunables.collector.runtime.upload_limit": { group: "runtime", values: [1, 2, 4] },
  "tunables.router.sql_pool_max_size": { group: "router", values: [1, 2, 4, 8] },
};

const prefixes = new Set(
  Object.keys(fields).flatMap((field) => {
    const parts = field.split(".");
    return parts.slice(0, -1).map((_, index) => parts.slice(0, index + 1).join("."));
  }),
);

function object(value: unknown, label: string): JsonObject {
  if (typeof value !== "object" || value === null || Array.isArray(value)
      || Object.getPrototypeOf(value) !== Object.prototype) {
    throw new Error(`${label} must be a JSON object`);
  }
  return value as JsonObject;
}

export function checkThroughputCandidate(baseline: unknown, candidate: unknown): ThroughputGroup {
  const original = object(baseline, "baseline");
  const proposed = object(candidate, "candidate");
  const tunables = object(original.tunables, "baseline.tunables");
  const collector = object(tunables.collector, "baseline.tunables.collector");
  const batching = object(collector.batching, "baseline.tunables.collector.batching");
  if (!Array.isArray(original.replica_counts) || !isDeepStrictEqual(original.replica_counts, [1])
      || !Array.isArray(original.tables) || original.tables.length !== 1
      || typeof original.tables[0] !== "string" || original.tables[0].length === 0
      || batching.coalesce_publications !== true) {
    throw new Error("baseline must use one table, one writer, and publication coalescing");
  }

  const groups = new Set<Exclude<ThroughputGroup, "control">>();
  function compare(before: unknown, after: unknown, beforePresent: boolean, afterPresent: boolean, path: string): void {
    if (beforePresent === afterPresent && (!beforePresent || isDeepStrictEqual(before, after))) return;
    if (Object.hasOwn(fields, path)) {
      if (!afterPresent) throw new Error(`cannot remove ${path}`);
      const field = fields[path];
      if (!field.values.some((value) => value === after)) throw new Error(`invalid ${path}`);
      groups.add(field.group);
      return;
    }
    if (path === "" || prefixes.has(path)) {
      if (!afterPresent) throw new Error(`cannot remove ${path}`);
      const left = beforePresent ? object(before, `baseline.${path}`) : {};
      const right = object(after, `candidate.${path}`);
      const names = new Set([...Object.keys(left), ...Object.keys(right)]);
      if (!beforePresent && names.size === 0) throw new Error(`empty added object ${path}`);
      for (const name of names) {
        if ((!name || name.includes("."))
            && (!Object.hasOwn(left, name) || !Object.hasOwn(right, name) || !isDeepStrictEqual(left[name], right[name]))) {
          throw new Error(`protected field changed: ${path}.${name}`);
        }
        compare(left[name], right[name], Object.hasOwn(left, name), Object.hasOwn(right, name), path ? `${path}.${name}` : name);
      }
      return;
    }
    throw new Error(`protected field changed: ${path}`);
  }

  compare(original, proposed, true, true, "");
  if (groups.size > 1) throw new Error("candidate changes more than one coordinate group");
  return groups.values().next().value ?? "control";
}
