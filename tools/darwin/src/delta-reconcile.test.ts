import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtemp, mkdir, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { tableFromArrays, tableToIPC } from "apache-arrow";
import { Compression, Table as WasmTable, WriterPropertiesBuilder, writeParquet } from "parquet-wasm";
import { reconcileStoppedSnapshot } from "./delta-reconcile.js";
import type { OatMeasurement, TimedReceipt } from "./metrics-adapter.js";

test("reconciles timed commits with decoded active Delta Adds and rejects changed evidence", async () => {
  const root = await mkdtemp(join(tmpdir(), "darwin-reconcile-"));
  try {
    const tableRoot = join(root, "orders", "table");
    await mkdir(join(tableRoot, "_delta_log"), { recursive: true });
    const parquet = (rows: number[]) => Buffer.from(writeParquet(
      WasmTable.fromIPCStream(tableToIPC(tableFromArrays({ value: rows }), "stream")),
      new WriterPropertiesBuilder().setCompression(Compression.ZSTD).build(),
    ));
    const one = parquet([1, 2, 3]);
    const two = parquet([4, 5]);
    const files: Record<string, Buffer> = {
      "first.parquet": one,
      "second.parquet": two,
      "_delta_log/00000000000000000000.json": Buffer.from(JSON.stringify({ metaData: { schemaString: JSON.stringify({ fields: [] }) } }) + "\n"),
      "_delta_log/00000000000000000001.json": Buffer.from(JSON.stringify({ add: { path: "first.parquet", size: one.length, dataChange: true } }) + "\n"),
      "_delta_log/00000000000000000002.json": Buffer.from(JSON.stringify({ add: { path: "second.parquet", size: two.length, dataChange: true } }) + "\n"),
    };
    const sha = (bytes: Buffer) => createHash("sha256").update(bytes).digest("hex");
    for (const [path, bytes] of Object.entries(files)) await writeFile(join(tableRoot, path), bytes);
    const manifest = Buffer.from(JSON.stringify({
      schema_version: 1, run_id: "abc-123", table: "orders", latestVersion: 2, rowCount: 5, schemaString: JSON.stringify({ fields: [] }),
      files: Object.entries(files).map(([path, bytes]) => ({ path, size: bytes.length, sha256: sha(bytes) })),
    }));
    await writeFile(join(root, "orders", "manifest.json"), manifest);
    await writeFile(join(root, "complete.json"), JSON.stringify({
      schema_version: 1, run_id: "abc-123", tables: ["orders"],
      manifests: [{ path: "orders/manifest.json", size: manifest.length, sha256: sha(manifest) }],
    }));
    await writeFile(join(root, "stopped.json"), JSON.stringify({ schema_version: 1, run_id: "abc-123", writers: [{
      id: "collector", service: "dataflow-1", was_running: true, exit_code: 0,
      finished_at: "2026-01-01T00:00:00Z", oom_killed: false,
    }] }));
    const measurement: OatMeasurement = {
      startUnixMs: 1000, endUnixMs: 151000, rows: 3, parquetBytes: one.length, files: 1, commits: 1, completeBuckets: 5,
      durableRowsPerSecond: 3 / 150, parquetBytesPerRow: one.length / 3,
      commitLatencySeconds: { p50: 0.1, p99: 0.1, samples: 1 },
      cpuMillicoresMean: null, memoryBytesMaxObserved: null, rssBytesMaxObserved: null,
      rssUnavailableReason: "OAT resource samples do not expose process RSS", resourceSampleCount: 0,
      downstreamRowsPerSecond: null, downstreamUnavailableReason: "downstream throughput not measured",
    };
    const receipt: TimedReceipt = {
      tableId: "a".repeat(64), tableName: "telemetry.sqlserver.orders", version: 1, rows: 3, files: 1,
      inputBytes: 0, parquetBytes: one.length, minIngestedAtMs: 0, maxIngestedAtMs: 1,
      finishedAtUnixMs: 1000, elapsedSeconds: 0.1,
    };
    const laterReceipt = { ...receipt, version: 2, rows: 2, parquetBytes: two.length, finishedAtUnixMs: 151000 };
    const reconcile = (receipts: TimedReceipt[], allReceipts: TimedReceipt[] = [...receipts, laterReceipt]) =>
      reconcileStoppedSnapshot(root, "abc-123", "orders", "telemetry.sqlserver.orders", "a".repeat(64),
        measurement, receipts, allReceipts, Compression.ZSTD);
    assert.deepEqual(await reconcile([receipt]),
      { rows: 3, parquetBytes: one.length, activeRows: 5, latestVersion: 2, commits: 1 });
    await assert.rejects(reconcile([{ ...receipt, rows: 4 }]), /receipt counters/);
    await assert.rejects(reconcile([{ ...receipt, tableId: "b".repeat(64) }], [receipt, laterReceipt]), /selected commit receipt/);
    await assert.rejects(reconcile([{ ...receipt, finishedAtUnixMs: 151000 }]), /selected commit receipt/);
    await assert.rejects(reconcile([receipt], [receipt]), /version 2 has no cumulative collector receipt/);
    await assert.rejects(reconcile([receipt], [receipt, laterReceipt, { ...laterReceipt, version: 3 }]), /cannot be attributed/);
    await assert.rejects(reconcile([], [receipt, laterReceipt]), /window receipt selection disagree/);
    const changed = await readFile(join(tableRoot, "first.parquet"));
    changed[0] ^= 1;
    await writeFile(join(tableRoot, "first.parquet"), changed);
    await assert.rejects(reconcile([receipt]), /digest mismatch/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
