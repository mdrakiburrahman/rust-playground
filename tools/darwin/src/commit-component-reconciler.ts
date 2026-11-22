import { createHash } from "node:crypto";
import { lstat, readFile, readdir, realpath } from "node:fs/promises";
import path from "node:path";
import type { CommitPhysicalSummary } from "./commit-component-adapter.js";

const GROUPS = 65;
const ROWS = 16_384;

function object(value: unknown, label: string): Record<string, unknown> {
  if (typeof value !== "object" || value === null || Array.isArray(value)) {
    throw new Error(`invalid ${label}`);
  }
  return value as Record<string, unknown>;
}

function integer(value: unknown, label: string, minimum = 0): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < minimum) {
    throw new Error(`invalid ${label}`);
  }
  return value;
}

function relativePath(value: unknown): string {
  if (typeof value !== "string") throw new Error("invalid Add path");
  const decoded = decodeURIComponent(value);
  if (!decoded || path.posix.isAbsolute(decoded) || decoded.includes("\\")
    || decoded.split("/").some(part => !part || part === "." || part === "..")) {
    throw new Error("unsafe Add path");
  }
  return decoded;
}

async function physicalFiles(root: string): Promise<Set<string>> {
  const result = new Set<string>();
  async function visit(directory: string, relative: string): Promise<void> {
    for (const entry of await readdir(directory, { withFileTypes: true })) {
      const name = relative ? `${relative}/${entry.name}` : entry.name;
      if (entry.isSymbolicLink()) throw new Error("symlink in stopped table");
      if (entry.isDirectory()) {
        if (name !== "_delta_log") await visit(path.join(directory, entry.name), name);
      } else if (entry.isFile()) {
        if (name.endsWith(".parquet")) result.add(name);
      } else {
        throw new Error("non-file in stopped table");
      }
    }
  }
  await visit(root, "");
  return result;
}

/**
 * Validate a stopped table using a separately admitted delta-inspect summary.
 * The host must settle the entire candidate process tree before calling this,
 * and must obtain `snapshot` from the trusted oracle, not the candidate.
 */
export async function reconcileStoppedCommitTable(
  root: string,
  snapshot: unknown,
): Promise<CommitPhysicalSummary> {
  if (!path.isAbsolute(root) || path.resolve(root) !== root || await realpath(root) !== root) {
    throw new Error("table root is not canonical");
  }
  const rootStat = await lstat(root);
  if (!rootStat.isDirectory() || rootStat.isSymbolicLink()) throw new Error("invalid table root");

  const outer = object(snapshot, "oracle snapshot");
  const tables = object(outer.tables, "oracle tables");
  if (Object.keys(tables).length !== 1 || !Object.hasOwn(tables, "table")) throw new Error("wrong oracle table set");
  const table = object(tables.table, "oracle table");
  const version = integer(table.version, "snapshot version", 1);
  if (version > GROUPS) throw new Error("unexpected snapshot version");
  if (integer(outer.files, "snapshot file count") !== GROUPS
    || integer(table.active_files, "active file count") !== GROUPS
    || integer(table.rows, "active rows") !== GROUPS * ROWS
    || !Array.isArray(table.partition_columns) || table.partition_columns.length !== 0
    || !Array.isArray(table.files) || table.files.length !== GROUPS) {
    throw new Error("wrong active snapshot shape");
  }

  const log = path.join(root, "_delta_log");
  const logStat = await lstat(log);
  if (!logStat.isDirectory() || logStat.isSymbolicLink()) throw new Error("invalid Delta log directory");
  const entries = await readdir(log, { withFileTypes: true });
  const expectedLogs = new Set(Array.from({ length: version + 1 }, (_, index) =>
    `${String(index).padStart(20, "0")}.json`));
  for (const entry of entries) {
    if (!entry.isFile() || entry.isSymbolicLink() || !expectedLogs.delete(entry.name)) {
      throw new Error("unexpected Delta log artifact");
    }
  }
  if (expectedLogs.size !== 0) throw new Error("missing Delta log version");

  const adds = new Map<string, number>();
  const identities = new Set<string>();
  const cohorts: number[] = [];
  let warmupBytes = 0;
  for (let index = 0; index <= version; index++) {
    const file = path.join(log, `${String(index).padStart(20, "0")}.json`);
    const stat = await lstat(file);
    if (!stat.isFile() || stat.nlink !== 1 || stat.size < 1 || stat.size > 4 * 1024 * 1024) {
      throw new Error("invalid Delta log file");
    }
    const content = await readFile(file, "utf8");
    const txns = new Set<string>();
    let addCount = 0;
    for (const line of content.split("\n").filter(Boolean)) {
      const action = object(JSON.parse(line) as unknown, "Delta action");
      if (action.remove !== undefined) throw new Error("unexpected Remove action");
      if (action.txn !== undefined) {
        const txn = object(action.txn, "transaction");
        if (typeof txn.appId !== "string" || !/^continuous-\d{4}$/.test(txn.appId)
          || txn.version !== 0 || identities.has(txn.appId)) {
          throw new Error("invalid or duplicate transaction identity");
        }
        identities.add(txn.appId);
        txns.add(txn.appId);
      }
      if (action.add !== undefined) {
        const add = object(action.add, "Add");
        const name = relativePath(add.path);
        const bytes = integer(add.size, "Add size", 1);
        if (adds.has(name) || add.deletionVector != null || add.dataChange !== true
          || Object.keys(object(add.partitionValues, "Add partition values")).length !== 0) {
          throw new Error("duplicate or unsupported Add");
        }
        if (typeof add.stats !== "string"
          || integer(object(JSON.parse(add.stats) as unknown, "Add stats").numRecords, "Add rows") !== ROWS) {
          throw new Error("invalid Add row count");
        }
        adds.set(name, bytes);
        if (index === 1) warmupBytes += bytes;
        addCount++;
      }
    }
    if (index === 0) {
      if (txns.size !== 0 || addCount !== 0) throw new Error("unexpected initial transaction or Add");
    } else {
      if (txns.size < 1 || txns.size > 8 || txns.size !== addCount) throw new Error("invalid commit cohort");
      if (index === 1 && (txns.size !== 1 || !txns.has("continuous-0000"))) {
        throw new Error("invalid warmup cohort");
      }
      cohorts.push(txns.size);
    }
  }
  if (identities.size !== GROUPS || adds.size !== GROUPS) throw new Error("wrong committed identity or Add count");
  for (let group = 0; group < GROUPS; group++) {
    if (!identities.has(`continuous-${String(group).padStart(4, "0")}`)) throw new Error("missing transaction identity");
  }

  const files = await physicalFiles(root);
  if (files.size !== adds.size) throw new Error("physical files differ from committed Adds");
  let bytes = 0;
  const seen = new Set<string>();
  for (const item of table.files) {
    const file = object(item, "oracle file");
    const name = relativePath(file.path);
    const size = integer(file.bytes, "oracle file size", 1);
    if (seen.has(name) || adds.get(name) !== size || !files.has(name)
      || integer(file.rows, "oracle file rows") !== ROWS
      || Object.keys(object(file.partition_values, "oracle partition values")).length !== 0) {
      throw new Error("active file differs from committed Add");
    }
    const stat = await lstat(path.join(root, name));
    if (!stat.isFile() || stat.isSymbolicLink() || stat.nlink !== 1 || stat.size !== size) {
      throw new Error("physical file size differs from committed Add");
    }
    seen.add(name);
    bytes += size;
  }
  if (!Number.isSafeInteger(bytes) || integer(table.bytes, "oracle bytes") !== bytes) {
    throw new Error("active byte total differs from files");
  }
  const hashes = object(table.row_hashes, "oracle row hashes");
  if (Object.keys(hashes).length !== GROUPS * ROWS) throw new Error("wrong distinct row count");
  for (let group = 0; group < GROUPS; group++) {
    for (let row = 0; row < ROWS; row++) {
      const id = group * ROWS + row;
      const payload = `value-${String(group).padStart(4, "0")}-${String(row).padStart(8, "0")}`;
      const digest = `sha256:${createHash("sha256").update(JSON.stringify({ id, payload })).digest("hex")}`;
      if (hashes[digest] !== 1) throw new Error("missing, duplicate or altered durable row");
    }
  }
  return {
    timedRows: (GROUPS - 1) * ROWS,
    timedBytes: bytes - warmupBytes,
    activeFiles: GROUPS,
    activeRows: GROUPS * ROWS,
    activeBytes: bytes,
    deltaVersions: version,
    cohortSizes: cohorts,
  };
}
