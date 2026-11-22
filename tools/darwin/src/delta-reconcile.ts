import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { lstat, readFile, readdir } from "node:fs/promises";
import { join } from "node:path";
import { tableFromIPC } from "apache-arrow";
import { readParquet } from "parquet-wasm";
import type { OatMeasurement, TimedReceipt } from "./metrics-adapter.js";

type JsonObject = Record<string, unknown>;
type InventoryEntry = { path: string; size: number; sha256: string };
type Add = { path: string; size: number; version: number; dataChange: boolean };

function object(value: unknown, label: string): JsonObject {
  if (typeof value !== "object" || value === null || Array.isArray(value)) throw new Error(`${label} must be an object`);
  return value as JsonObject;
}

function uint(value: unknown, label: string): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < 0) throw new Error(`${label} must be a nonnegative safe integer`);
  return value;
}

function safePath(value: unknown): string {
  if (typeof value !== "string" || !value || value.startsWith("/") || /[\\:?#\0]/.test(value)
    || /%2e|%2f/i.test(value)) throw new Error("unsafe Delta or snapshot path");
  let decoded: string;
  try { decoded = decodeURIComponent(value); } catch { throw new Error("invalid encoded Delta path"); }
  if (/[\\\0]/.test(decoded) || decoded.split("/").some(part => !part || part === "." || part === "..")) {
    throw new Error("unsafe Delta or snapshot path");
  }
  return decoded;
}

async function digest(path: string): Promise<{ size: number; sha256: string }> {
  const stat = await lstat(path);
  if (!stat.isFile() || stat.isSymbolicLink()) throw new Error("snapshot input is not a regular file");
  const hash = createHash("sha256");
  for await (const chunk of createReadStream(path)) hash.update(chunk);
  return { size: stat.size, sha256: hash.digest("hex") };
}

async function json(path: string): Promise<JsonObject> {
  const stat = await lstat(path);
  if (!stat.isFile() || stat.isSymbolicLink() || stat.size > 64 * 1024 * 1024) throw new Error("invalid snapshot JSON file");
  return object(JSON.parse(await readFile(path, "utf8")), path);
}

async function inventory(root: string, prefix = ""): Promise<string[]> {
  const paths: string[] = [];
  for (const entry of await readdir(join(root, prefix), { withFileTypes: true })) {
    const path = prefix ? `${prefix}/${entry.name}` : entry.name;
    safePath(path);
    if (entry.isSymbolicLink()) throw new Error("snapshot contains a symlink");
    if (entry.isDirectory()) paths.push(...await inventory(root, path));
    else if (entry.isFile()) paths.push(path);
    else throw new Error("snapshot contains a non-file");
  }
  return paths.sort();
}

function entries(raw: unknown): InventoryEntry[] {
  if (!Array.isArray(raw)) throw new Error("snapshot inventory is missing");
  return raw.map(item => {
    const entry = object(item, "inventory entry");
    const path = safePath(entry.path);
    const size = uint(entry.size, "inventory size");
    if (typeof entry.sha256 !== "string" || !/^[a-f0-9]{64}$/.test(entry.sha256)) throw new Error("invalid inventory digest");
    return { path, size, sha256: entry.sha256 };
  });
}

export async function reconcileStoppedSnapshot(
  root: string, runId: string, table: string, canonicalTableName: string, expectedTableId: string,
  measurement: OatMeasurement, receipts: TimedReceipt[],
): Promise<{ rows: number; parquetBytes: number; activeRows: number; latestVersion: number; commits: number }> {
  if (!/^[a-f0-9-]+$/.test(runId) || !/^[a-zA-Z0-9_]+$/.test(table)) throw new Error("invalid snapshot identity");
  if (!canonicalTableName.endsWith(`.${table}`) || !/^[a-f0-9]{64}$/.test(expectedTableId)) throw new Error("invalid trusted table identity");
  const complete = await json(join(root, "complete.json"));
  const stopped = await json(join(root, "stopped.json"));
  if (complete.schema_version !== 1 || complete.run_id !== runId || stopped.schema_version !== 1 || stopped.run_id !== runId
    || !Array.isArray(complete.tables) || complete.tables.length !== 1 || complete.tables[0] !== table
    || !Array.isArray(stopped.writers) || stopped.writers.length === 0) throw new Error("snapshot completion or stopped identity mismatch");
  const writers = stopped.writers.map(writer => object(writer, "stopped writer"));
  const collectors = writers.filter(writer => typeof writer.service === "string" && /^dataflow-\d+$/.test(writer.service));
  if (collectors.length !== 1 || collectors[0]!.was_running !== true
    || (collectors[0]!.exit_code !== 0 && collectors[0]!.exit_code !== 143)
    || writers.some(writer => typeof writer.id !== "string" || !writer.id || writer.oom_killed !== false
      || typeof writer.finished_at !== "string" || !(Date.parse(writer.finished_at) > 0))) {
    throw new Error("snapshot collector stop is not proven");
  }
  const manifests = entries(complete.manifests);
  if (manifests.length !== 1 || manifests[0]!.path !== `${table}/manifest.json`) throw new Error("snapshot manifest reference mismatch");
  const manifestPath = join(root, table, "manifest.json");
  const manifestDigest = await digest(manifestPath);
  if (manifestDigest.size !== manifests[0]!.size || manifestDigest.sha256 !== manifests[0]!.sha256) throw new Error("snapshot manifest digest mismatch");
  const manifest = await json(manifestPath);
  if (manifest.schema_version !== 1 || manifest.run_id !== runId || manifest.table !== table) throw new Error("snapshot manifest identity mismatch");
  const latestVersion = uint(manifest.latestVersion, "snapshot version");
  const expectedActiveRows = uint(manifest.rowCount, "snapshot rows");
  const tableRoot = join(root, table, "table");
  const listed = entries(manifest.files);
  const listedPaths = listed.map(entry => entry.path).sort();
  if (new Set(listedPaths).size !== listedPaths.length || JSON.stringify(listedPaths) !== JSON.stringify(await inventory(tableRoot))) {
    throw new Error("snapshot inventory differs from manifest");
  }
  for (const entry of listed) {
    const actual = await digest(join(tableRoot, entry.path));
    if (actual.size !== entry.size || actual.sha256 !== entry.sha256) throw new Error("snapshot file digest mismatch");
  }
  const active = new Map<string, Add>();
  const referenced = new Map<string, Add>();
  const selected = new Map<number, TimedReceipt>();
  for (const receipt of receipts) {
    if (receipt.tableName !== canonicalTableName || receipt.tableId !== expectedTableId || selected.has(receipt.version)
      || receipt.finishedAtUnixMs < measurement.startUnixMs || receipt.finishedAtUnixMs >= measurement.endUnixMs) {
      throw new Error("invalid selected commit receipt");
    }
    selected.set(receipt.version, receipt);
  }
  const selectedAdds = new Map<number, Add[]>();
  let schemaSeen = false;
  for (let version = 0; version <= latestVersion; version++) {
    const path = join(tableRoot, "_delta_log", `${String(version).padStart(20, "0")}.json`);
    const log = await readFile(path, "utf8");
    for (const line of log.split("\n").filter(Boolean)) {
      const action = object(JSON.parse(line), "Delta action");
      if (action.metaData != null) {
        const metadata = object(action.metaData, "Delta metadata");
        if (typeof metadata.schemaString !== "string" || !Array.isArray(object(JSON.parse(metadata.schemaString), "Delta schema").fields)) {
          throw new Error("invalid Delta schema");
        }
        schemaSeen = true;
      }
      if (action.add != null) {
        const value = object(action.add, "Delta Add");
        if (value.deletionVector != null || (value.dataChange !== undefined && typeof value.dataChange !== "boolean")) {
          throw new Error("unsupported Delta Add");
        }
        const add: Add = { path: safePath(value.path), size: uint(value.size, "Delta Add size"), version, dataChange: value.dataChange !== false };
        const previous = referenced.get(add.path);
        if (previous && previous.size !== add.size) throw new Error("historical Delta Add changed size");
        referenced.set(add.path, add);
        active.set(add.path, add);
        if (selected.has(version) && add.dataChange) {
          const adds = selectedAdds.get(version) ?? [];
          adds.push(add);
          selectedAdds.set(version, adds);
        }
      }
      if (action.remove != null) {
        const remove = object(action.remove, "Delta Remove");
        if (remove.deletionVector != null) throw new Error("unsupported Delta Remove");
        active.delete(safePath(remove.path));
      }
    }
  }
  if (!schemaSeen || !active.size) throw new Error("snapshot has no schema or active files");
  const decoded = new Map<string, number>();
  let activeRows = 0;
  for (const add of referenced.values()) {
    const path = join(tableRoot, add.path);
    const stat = await lstat(path);
    if (!stat.isFile() || stat.isSymbolicLink() || stat.size !== add.size) throw new Error("active Parquet file size mismatch");
    const rows = tableFromIPC(readParquet(await readFile(path)).intoIPCStream()).numRows;
    decoded.set(add.path, rows);
    if (active.has(add.path)) activeRows = uint(activeRows + rows, "active rows");
  }
  if (activeRows !== expectedActiveRows) throw new Error("decoded active snapshot rows do not match manifest");
  let rows = 0;
  let parquetBytes = 0;
  for (const [version, receipt] of selected) {
    const adds = selectedAdds.get(version) ?? [];
    if (adds.length !== receipt.files) throw new Error("receipt file count does not match Delta version");
    let versionRows = 0;
    let versionBytes = 0;
    for (const add of adds) {
      if (active.get(add.path)?.version !== version || !decoded.has(add.path)) throw new Error("receipt Add is not active in stopped snapshot");
      versionRows = uint(versionRows + decoded.get(add.path)!, "decoded version rows");
      versionBytes = uint(versionBytes + add.size, "version bytes");
    }
    if (versionRows !== receipt.rows || versionBytes !== receipt.parquetBytes) throw new Error("receipt counters differ from independently decoded Delta Adds");
    rows = uint(rows + versionRows, "selected rows");
    parquetBytes = uint(parquetBytes + versionBytes, "selected bytes");
  }
  if (rows !== measurement.rows || parquetBytes !== measurement.parquetBytes) throw new Error("reconciled window totals differ from measurement");
  return { rows, parquetBytes, activeRows, latestVersion, commits: selected.size };
}
