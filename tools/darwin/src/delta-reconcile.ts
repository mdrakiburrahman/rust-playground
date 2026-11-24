import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { lstat, readFile, readdir } from "node:fs/promises";
import { join } from "node:path";
import { tableFromIPC } from "apache-arrow";
import { Compression, readMetadata, readParquet } from "parquet-wasm";
import { CoverageUnavailableError, type OatMeasurement, type TimedReceipt } from "./metrics-adapter.js";

type JsonObject = Record<string, unknown>;
type InventoryEntry = { path: string; size: number; sha256: string };
type Add = { path: string; size: number; version: number; dataChange: boolean };
type FileGeometry = { count: number; min: number | null; p50: number | null; max: number | null; mean: number | null };

function fileGeometry(values: number[]): FileGeometry {
  const sorted = [...values].sort((a, b) => a - b);
  if (!sorted.length) return { count: 0, min: null, p50: null, max: null, mean: null };
  const middle = Math.floor(sorted.length / 2);
  return {
    count: sorted.length, min: sorted[0]!, max: sorted[sorted.length - 1]!,
    p50: sorted.length % 2 ? sorted[middle]! : (sorted[middle - 1]! + sorted[middle]!) / 2,
    mean: sorted.reduce((sum, value) => sum + value, 0) / sorted.length,
  };
}

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
  measurement: OatMeasurement, receipts: TimedReceipt[], allReceipts: TimedReceipt[], expectedCodec: Compression,
  rawPreload: unknown, decodeFile?: (path: string, active: boolean) => Promise<number>,
  verifyActive?: (paths: string[]) => Promise<void>,
  verifyAllActiveCodecs = false,
  expectedWriterLevel?: number | null,
): Promise<{
  rows: number; parquetBytes: number; activeRows: number; latestVersion: number; commits: number;
  activeFileRows: FileGeometry; windowFileRows: FileGeometry;
  parquetFooter: { physicalCodec: string; rowGroups: number; columnChunks: number; filesWithDeclaration: number;
    filesMissingDeclaration: number; verifiedWriterLevel: number | null; unrelatedKeyCount: number };
}> {
  if (!/^[a-f0-9-]+$/.test(runId) || !/^[a-zA-Z0-9_]+$/.test(table)) throw new Error("invalid snapshot identity");
  if (!canonicalTableName.endsWith(`.${table}`) || !/^[a-f0-9]{64}$/.test(expectedTableId)) throw new Error("invalid trusted table identity");
  const controlFiles = verifyActive ? await Promise.all(["complete.json", "stopped.json"].map(async path => ({
    path: join(root, path), digest: await digest(join(root, path)),
  }))) : [];
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
  const preload = object(rawPreload, "pre-load Delta evidence");
  if (preload.table !== canonicalTableName
    || typeof preload.latest_version !== "number" || !Number.isSafeInteger(preload.latest_version) || preload.latest_version < 0 || preload.latest_version > latestVersion
    || typeof preload.observed_at_unix_ms !== "number" || !Number.isSafeInteger(preload.observed_at_unix_ms) || preload.observed_at_unix_ms <= 0
    || preload.observed_at_unix_ms >= measurement.startUnixMs
    || JSON.stringify(preload.partition_columns) !== JSON.stringify(["__arrowcol_ingestion"])
    || preload.granularity !== "hour" || !Array.isArray(preload.logs)
    || preload.logs.length !== preload.latest_version + 1) {
    throw new CoverageUnavailableError("pre-load Delta setup boundary is unavailable or invalid");
  }
  const expectedActiveRows = uint(manifest.rowCount, "snapshot rows");
  const tableRoot = join(root, table, "table");
  const listed = entries(manifest.files);
  const listedPaths = listed.map(entry => entry.path).sort();
  if (new Set(listedPaths).size !== listedPaths.length || JSON.stringify(listedPaths) !== JSON.stringify(await inventory(tableRoot))) {
    throw new Error("snapshot inventory differs from manifest");
  }
  const logs = listedPaths.filter(path => /^_delta_log\/\d{20}\.json$/.test(path));
  if (logs.length !== latestVersion + 1
    || logs.some((path, version) => path !== `_delta_log/${String(version).padStart(20, "0")}.json`)) {
    throw new Error("snapshot Delta log history is incomplete");
  }
  for (const entry of listed) {
    const actual = await digest(join(tableRoot, entry.path));
    if (actual.size !== entry.size || actual.sha256 !== entry.sha256) throw new Error("snapshot file digest mismatch");
  }
  for (const [version, rawEntry] of preload.logs.entries()) {
    const entry = object(rawEntry, "pre-load Delta log");
    const snapshotEntry = listed.find(file => file.path === `_delta_log/${String(version).padStart(20, "0")}.json`);
    if (entry.version !== version || typeof entry.sha256 !== "string" || !/^[a-f0-9]{64}$/.test(entry.sha256)
      || snapshotEntry?.sha256 !== entry.sha256) {
      throw new CoverageUnavailableError("pre-load Delta setup prefix changed in the stopped snapshot");
    }
  }
  const active = new Map<string, Add>();
  const referenced = new Map<string, Add>();
  const all = new Map<number, TimedReceipt>();
  for (const receipt of allReceipts) {
    if (receipt.tableName !== canonicalTableName || receipt.tableId !== expectedTableId
      || receipt.version < 1 || receipt.version > latestVersion || all.has(receipt.version)) {
      throw new CoverageUnavailableError("cumulative receipt cannot be attributed to a unique Delta version");
    }
    all.set(receipt.version, receipt);
  }
  const selected = new Map<number, TimedReceipt>();
  for (const receipt of receipts) {
    if (receipt.tableName !== canonicalTableName || receipt.tableId !== expectedTableId || selected.has(receipt.version)
      || receipt.finishedAtUnixMs < measurement.startUnixMs || receipt.finishedAtUnixMs >= measurement.endUnixMs) {
      throw new Error("invalid selected commit receipt");
    }
    if (JSON.stringify(all.get(receipt.version)) !== JSON.stringify(receipt)) {
      throw new CoverageUnavailableError("window receipt is absent from cumulative version evidence");
    }
    selected.set(receipt.version, receipt);
  }
  for (const receipt of all.values()) {
    const inWindow = receipt.finishedAtUnixMs >= measurement.startUnixMs && receipt.finishedAtUnixMs < measurement.endUnixMs;
    if (selected.has(receipt.version) !== inWindow) {
      throw new CoverageUnavailableError("cumulative and window receipt selection disagree");
    }
  }
  const ingestionAdds = new Map<number, Add[]>();
  let latestSchema: string | undefined;
  for (let version = 0; version <= latestVersion; version++) {
    if (version <= preload.latest_version && all.has(version)) {
      throw new CoverageUnavailableError(`pre-load Delta version ${version} has an ingestion receipt`);
    }
    const path = join(tableRoot, "_delta_log", `${String(version).padStart(20, "0")}.json`);
    const log = await readFile(path, "utf8");
    const adds: Add[] = [];
    for (const line of log.split("\n").filter(Boolean)) {
      const action = object(JSON.parse(line), "Delta action");
      if (action.metaData != null) {
        const metadata = object(action.metaData, "Delta metadata");
        if (typeof metadata.schemaString !== "string" || !Array.isArray(object(JSON.parse(metadata.schemaString), "Delta schema").fields)) {
          throw new Error("invalid Delta schema");
        }
        latestSchema = metadata.schemaString;
      }
      if (action.add != null) {
        if (version <= preload.latest_version) throw new CoverageUnavailableError("pre-load Delta prefix contains an Add");
        const value = object(action.add, "Delta Add");
        if (value.deletionVector != null || (value.dataChange !== undefined && typeof value.dataChange !== "boolean")) {
          throw new Error("unsupported Delta Add");
        }
        const add: Add = { path: safePath(value.path), size: uint(value.size, "Delta Add size"), version, dataChange: value.dataChange !== false };
        if (!add.dataChange || referenced.has(add.path)) {
          throw new CoverageUnavailableError("fresh-table ingestion contains a rewrite or duplicate Add");
        }
        referenced.set(add.path, add);
        active.set(add.path, add);
        adds.push(add);
      }
      if (action.remove != null) {
        if (version <= preload.latest_version) throw new CoverageUnavailableError("pre-load Delta prefix contains a Remove");
        throw new CoverageUnavailableError("fresh-table ingestion contains a Delta Remove");
      }
    }
    if (adds.length) {
      if (!all.has(version)) throw new CoverageUnavailableError(`Delta version ${version} has no cumulative collector receipt`);
      ingestionAdds.set(version, adds);
    } else if (all.has(version)) {
      throw new CoverageUnavailableError(`Delta version ${version} has a collector receipt but no ingestion Adds`);
    }
  }
  if (!latestSchema || latestSchema !== manifest.schemaString || !active.size || !expectedActiveRows) {
    throw new Error("snapshot has no consistent schema or active files");
  }
  const selectedPaths = new Set([...ingestionAdds.entries()]
    .filter(([version]) => selected.has(version)).flatMap(([, adds]) => adds.map(add => add.path)));
  const parquetFooter = { physicalCodec: Compression[expectedCodec]!.toLowerCase(), rowGroups: 0, columnChunks: 0,
    filesWithDeclaration: 0, filesMissingDeclaration: 0, verifiedWriterLevel: expectedWriterLevel ?? null, unrelatedKeyCount: 0 };
  const decoded = new Map<string, number>();
  let activeRows = 0;
  const activeFileRows: number[] = [];
  for (const add of referenced.values()) {
    const path = join(tableRoot, add.path);
    const stat = await lstat(path);
    if (!stat.isFile() || stat.isSymbolicLink() || stat.size !== add.size) throw new Error("active Parquet file size mismatch");
    const bytes = await readFile(path);
    const metadata = readMetadata(bytes);
    let footerRows: number;
    try {
      const fileMetadata = metadata.fileMetadata();
      try {
        footerRows = fileMetadata.numRows();
        if (selectedPaths.has(add.path) || (verifyAllActiveCodecs && active.has(add.path))) {
          const kv = fileMetadata.keyValueMetadata();
          const key = "arrowcol.writer.codec_levels.v1";
          const present = kv.has(key);
          parquetFooter.unrelatedKeyCount += kv.size - Number(present);
          if (present) parquetFooter.filesWithDeclaration++;
          else {
            parquetFooter.filesMissingDeclaration++;
            if (expectedWriterLevel !== undefined) throw new Error("selected Parquet writer codec declaration is missing (legacy file)");
          }
          if (!metadata.numRowGroups()) throw new Error("selected Parquet file has no row groups");
          for (let i = 0; i < metadata.numRowGroups(); i++) {
            const group = metadata.rowGroup(i);
            try {
              const width = group.numColumns();
              if (!width) throw new Error("selected Parquet file has no physical columns");
              for (let j = 0; j < width; j++) {
                const column = group.column(j);
                try {
                  if (column.compression() !== expectedCodec) throw new Error("selected Parquet codec differs from expected codec");
                } finally { column.free(); }
              }
              if (present && i === 0) {
                const value: unknown = kv.get(key);
                let columns: unknown;
                if (typeof value === "string" && value.length <= 2 + 32 * width) {
                  try { columns = JSON.parse(value); } catch { /* Never expose parser input. */ }
                }
                if (!Array.isArray(columns) || columns.length !== width || !columns.every(pair => {
                  if (!Array.isArray(pair) || pair.length !== 2 || pair[0] !== parquetFooter.physicalCodec) return false;
                  if (expectedWriterLevel !== undefined) return pair[1] === expectedWriterLevel;
                  switch (expectedCodec) {
                    case Compression.ZSTD: return Number.isInteger(pair[1]) && pair[1] >= 1 && pair[1] <= 22;
                    case Compression.GZIP: return Number.isInteger(pair[1]) && pair[1] >= 0 && pair[1] <= 9;
                    case Compression.BROTLI: return Number.isInteger(pair[1]) && pair[1] >= 0 && pair[1] <= 11;
                    default: return pair[1] === null;
                  }
                })) throw new Error("selected Parquet writer codec or level declaration differs");
              }
              parquetFooter.rowGroups++;
              parquetFooter.columnChunks += width;
            } finally { group.free(); }
          }
        }
      } finally { fileMetadata.free(); }
    } finally { metadata.free(); }
    const rows = decodeFile
      ? uint(await decodeFile(path, active.has(add.path)), "decoded file rows")
      : tableFromIPC(readParquet(bytes).intoIPCStream()).numRows;
    if (rows !== footerRows) throw new Error("Parquet decoded rows differ from footer");
    decoded.set(add.path, rows);
    if (active.has(add.path)) {
      activeRows = uint(activeRows + rows, "active rows");
      activeFileRows.push(rows);
    }
  }
  if (activeRows !== expectedActiveRows) throw new Error("decoded active snapshot rows do not match manifest");
  let rows = 0;
  let parquetBytes = 0;
  const windowFileRows: number[] = [];
  for (const [version, receipt] of all) {
    const adds = ingestionAdds.get(version) ?? [];
    if (adds.length !== receipt.files) throw new Error("receipt file count does not match Delta version");
    let versionRows = 0;
    let versionBytes = 0;
    for (const add of adds) {
      if (active.get(add.path)?.version !== version || !decoded.has(add.path)) throw new Error("receipt Add is not active in stopped snapshot");
      versionRows = uint(versionRows + decoded.get(add.path)!, "decoded version rows");
      if (selected.has(version)) windowFileRows.push(decoded.get(add.path)!);
      versionBytes = uint(versionBytes + add.size, "version bytes");
    }
    if (versionRows !== receipt.rows || versionBytes !== receipt.parquetBytes) throw new Error("receipt counters differ from independently decoded Delta Adds");
    if (selected.has(version)) {
      rows = uint(rows + versionRows, "selected rows");
      parquetBytes = uint(parquetBytes + versionBytes, "selected bytes");
    }
  }
  if (rows !== measurement.rows || parquetBytes !== measurement.parquetBytes) throw new Error("reconciled window totals differ from measurement");
  if (verifyActive) {
    const inventoryByPath = new Map(listed.map(file => [file.path, file]));
    const activeFiles = [...active.keys()].map(path => {
      const entry = inventoryByPath.get(path);
      if (!entry) throw new Error("active file is absent from snapshot inventory");
      return { path: join(tableRoot, path), size: entry.size, sha256: entry.sha256 };
    });
    const check = async () => {
      for (const file of activeFiles) {
        const actual = await digest(file.path);
        if (actual.size !== file.size || actual.sha256 !== file.sha256) {
          throw new Error("active snapshot changed during identity verification");
        }
      }
    };
    await check();
    await verifyActive(activeFiles.map(file => file.path));
    await check();
    for (const control of [...controlFiles, { path: manifestPath, digest: manifestDigest }]) {
      const actual = await digest(control.path);
      if (actual.size !== control.digest.size || actual.sha256 !== control.digest.sha256) {
        throw new Error("snapshot control evidence changed during identity verification");
      }
    }
    for (const file of listed.filter(entry => !active.has(entry.path))) {
      const actual = await digest(join(tableRoot, file.path));
      if (actual.size !== file.size || actual.sha256 !== file.sha256) {
        throw new Error("snapshot history changed during identity verification");
      }
    }
  }
  return {
    rows, parquetBytes, activeRows, latestVersion, commits: selected.size, parquetFooter,
    activeFileRows: fileGeometry(activeFileRows), windowFileRows: fileGeometry(windowFileRows),
  };
}
