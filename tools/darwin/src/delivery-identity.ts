const U64_MAX = (1n << 64n) - 1n;
const IDENTITY_FORMAT = "https://x.invalid/v1/{run_uuid_simple32}/{incarnation_uuid_simple32}/{worker_decimal}/{logical_batch_sequence_base36}/{row_ordinal_decimal}";
const COHORT_RULE = "first_send_unix_ms < start => warmup; start <= first_send_unix_ms < end => measurement; otherwise post_window; bounds are [start,end)";
const COHORTS = ["warmup", "measurement", "post_window"] as const;
type Cohort = typeof COHORTS[number];
type ObjectValue = Record<string, unknown>;

export interface IdentityOptions {
  table: string;
  rewriteCommitCount: number;
  maxBitmapBytes?: number;
  maxRanges?: number;
}

export interface IdentityVerdict {
  verified: true;
  scope: "exact_whole_snapshot_delivery_identity_set_equality";
  expectedRows: number;
  observedRows: number;
  expectedBatches: number;
  manifestCount: number;
  rangeCount: number;
  bitmapBytes: number;
  cohortRows: Record<Cohort, number>;
  cohortScope: "producer_first_send_cohorts_including_warmup_and_drain_not_Delta_commit_window";
}

interface Range {
  first: bigint;
  last: bigint;
  rowsPerBatch: number;
  batches: number;
  cohort: Cohort;
  rows: number;
  bitmap: Uint8Array;
}

function object(value: unknown, name: string): ObjectValue {
  if (typeof value !== "object" || value === null || Array.isArray(value)) throw new Error(`${name} must be an object`);
  return value as ObjectValue;
}

function integer(value: unknown, name: string, min = 0, max = Number.MAX_SAFE_INTEGER): number {
  if (typeof value !== "number" || !Number.isSafeInteger(value) || value < min || value > max) throw new Error(`invalid ${name}`);
  return value;
}

function u64(value: unknown, name: string): bigint {
  if (typeof value === "number") return BigInt(integer(value, name));
  if (typeof value !== "string" || !/^(0|[1-9][0-9]{0,19})$/.test(value)) throw new Error(`invalid exact ${name}`);
  const parsed = BigInt(value);
  if (parsed > U64_MAX) throw new Error(`${name} exceeds u64`);
  return parsed;
}

function uuid(value: unknown): string {
  if (typeof value !== "string" || !/^[a-f0-9]{32}$/.test(value)) throw new Error("malformed manifest UUID");
  return value;
}

function base36(value: string): bigint {
  if (!/^(0|[1-9a-z][0-9a-z]{0,12})$/.test(value)) throw new Error("noncanonical delivery sequence");
  let result = 0n;
  for (const char of value) {
    const digit = char.charCodeAt(0) <= 57 ? char.charCodeAt(0) - 48 : char.charCodeAt(0) - 87;
    result = result * 36n + BigInt(digit);
    if (result > U64_MAX) throw new Error("delivery sequence exceeds u64");
  }
  return result;
}

function cohort(value: unknown): Cohort {
  if (typeof value !== "string" || !COHORTS.includes(value as Cohort)) throw new Error("unsupported delivery cohort");
  return value as Cohort;
}

function counter(manifest: ObjectValue, name: string, expected: number): void {
  if (integer(manifest[name], name) !== expected) throw new Error(`manifest ${name} does not reconcile`);
}

function indexManifests(manifests: readonly unknown[], options: IdentityOptions): {
  namespaces: Map<string, Range[]>; expectedRows: number; expectedBatches: number; bytes: number; ranges: number;
} {
  if (typeof options.table !== "string" || !/^[a-z][a-z0-9_]*$/.test(options.table)) throw new Error("invalid expected table");
  if (options.rewriteCommitCount !== 0) throw new Error("delivery equality requires zero reconciled rewrite commits");
  const maxBytes = integer(options.maxBitmapBytes ?? 512 * 1024 * 1024, "bitmap limit", 1, 2_147_483_647);
  const maxRanges = integer(options.maxRanges ?? 65_536, "range limit", 1, 1_000_000);
  if (manifests.length === 0 || manifests.length > 1024) throw new Error("invalid manifest count");
  const namespaces = new Map<string, Range[]>();
  const incarnations = new Set<string>();
  let expectedRows = 0;
  let expectedBatches = 0;
  let bytes = 0;
  let ranges = 0;
  let sharedBounds: string | null = null;
  for (const raw of manifests) {
    const manifest = object(raw, "terminal manifest");
    if (manifest.schema !== "arrowcol.logical_batch_manifest.v1" || manifest.version !== 1
      || manifest.data_profile !== "realistic" || manifest.identity_status !== "identity_bearing"
      || manifest.eligible_for_identity_reconciliation !== true || manifest.complete !== true
      || manifest.truncated !== false || manifest.record_capacity_reached !== false
      || manifest.identity_column !== "endpoint_url" || manifest.identity_format !== IDENTITY_FORMAT || manifest.cohort_rule !== COHORT_RULE
      || manifest.terminal_failure !== undefined) throw new Error("incomplete, truncated or unsupported identity manifest");
    if (manifest.run_uuid_source !== "client_generated" || manifest.incarnation_uuid_source !== "client_generated"
      || manifest.oat_run_uuid !== null) throw new Error("unsupported identity namespace provenance");
    const run = uuid(manifest.run_uuid);
    const incarnation = uuid(manifest.incarnation_uuid);
    const incarnationKey = `${run}/${incarnation}`;
    if (incarnations.has(incarnationKey)) throw new Error("duplicate terminal manifest incarnation");
    incarnations.add(incarnationKey);
    if (typeof manifest.payload_corpus_sha256 !== "string" || !/^[a-f0-9]{64}$/.test(manifest.payload_corpus_sha256)
      || manifest.payload_corpus_hash_version !== 2 || typeof manifest.payload_corpus_hash_scope !== "string"
      || manifest.payload_corpus_hash_scope.length === 0) throw new Error("unsupported business corpus evidence");
    const bounds = object(manifest.measurement_bounds, "measurement bounds");
    const start = integer(bounds.start_unix_ms_inclusive, "measurement start");
    const end = integer(bounds.end_unix_ms_exclusive, "measurement end", start + 1);
    const binding = `${start}:${end}`;
    if (sharedBounds !== null && sharedBounds !== binding) throw new Error("manifests have different shared windows");
    sharedBounds = binding;
    if (!Array.isArray(manifest.ranges) || manifest.ranges.length === 0) throw new Error("manifest has no acknowledged ranges");
    const count = integer(manifest.record_count, "record count", 1, 1_000_000);
    if (manifest.record_cap !== 1_000_000) throw new Error("unsupported manifest record cap");
    for (const name of ["prepared_logical_batches", "attempted_logical_batches", "acknowledged_logical_batches"]) counter(manifest, name, count);
    for (const name of ["not_admitted_logical_batches", "ambiguous_logical_batches", "failed_logical_batches", "pending_logical_batches", "retry_attempts"]) {
      counter(manifest, name, 0);
    }
    let manifestBatches = 0;
    for (const item of manifest.ranges) {
      if (++ranges > maxRanges) throw new Error("range metadata exceeds configured memory bound");
      const range = object(item, "delivery range");
      const supported = ["table", "worker", "first_sequence", "last_sequence", "batch_count", "rows_per_batch", "cohort", "attempts", "outcome", "first_send_unix_ns", "last_outcome_unix_ms"];
      if (Object.keys(range).some(key => !supported.includes(key))) throw new Error("unsupported delivery range fields");
      if (range.table !== options.table || range.outcome !== "acknowledged" || range.attempts !== 1) {
        throw new Error("foreign table, nonacknowledged or retried delivery range");
      }
      const worker = integer(range.worker, "worker", 0, 65_535);
      const first = u64(range.first_sequence, "first sequence");
      const last = u64(range.last_sequence, "last sequence");
      const batches = integer(range.batch_count, "batch count", 1, 1_000_000);
      if (last < first || last - first + 1n !== BigInt(batches)) throw new Error("delivery range sequence count mismatch");
      const rowsPerBatch = integer(range.rows_per_batch, "rows per batch", 1, 4_294_967_295);
      const rows = integer(rowsPerBatch * batches, "range row count", 1);
      const rangeCohort = cohort(range.cohort);
      if (range.first_send_unix_ns === null || range.first_send_unix_ns === undefined
        || range.last_outcome_unix_ms === null || range.last_outcome_unix_ms === undefined) throw new Error("acknowledged range lacks send/outcome evidence");
      if (typeof range.first_send_unix_ns === "number") {
        if (!Number.isInteger(range.first_send_unix_ns) || range.first_send_unix_ns <= 0
          || range.first_send_unix_ns > Number(U64_MAX)) throw new Error("invalid send timestamp");
      } else if (u64(range.first_send_unix_ns, "send timestamp") === 0n) throw new Error("invalid send timestamp");
      integer(range.last_outcome_unix_ms, "outcome time", 1);
      const key = `${run}/${incarnation}/${worker}`;
      const entries = namespaces.get(key) ?? [];
      entries.push({ first, last, rowsPerBatch, batches, cohort: rangeCohort, rows, bitmap: new Uint8Array(0) });
      namespaces.set(key, entries);
      manifestBatches += batches;
      expectedBatches = integer(expectedBatches + batches, "total expected batches", 1);
      expectedRows = integer(expectedRows + rows, "total expected rows", 1);
      bytes += Math.ceil(rows / 8);
      if (!Number.isSafeInteger(bytes) || bytes > maxBytes) throw new Error("delivery row bitmaps exceed configured memory bound");
    }
    if (manifestBatches !== count) throw new Error("range batches do not reconcile with complete manifest");
  }
  for (const entries of namespaces.values()) {
    entries.sort((a, b) => a.first < b.first ? -1 : a.first > b.first ? 1 : 0);
    if (entries[0]!.first !== 0n) throw new Error("incomplete worker sequence: must start at zero");
    for (let index = 1; index < entries.length; index++) {
      if (entries[index]!.first <= entries[index - 1]!.last) throw new Error("duplicate or overlapping manifest batch namespaces");
      if (entries[index]!.first !== entries[index - 1]!.last + 1n) throw new Error("incomplete worker sequence: missing batches");
    }
  }
  for (const entries of namespaces.values()) {
    for (const range of entries) range.bitmap = new Uint8Array(Math.ceil(range.rows / 8));
  }
  return { namespaces, expectedRows, expectedBatches, bytes, ranges };
}

function findRange(entries: Range[], sequence: bigint): Range | undefined {
  let low = 0;
  let high = entries.length - 1;
  while (low <= high) {
    const middle = Math.floor((low + high) / 2);
    const range = entries[middle]!;
    if (sequence < range.first) high = middle - 1;
    else if (sequence > range.last) low = middle + 1;
    else return range;
  }
  return undefined;
}

export async function verifyDeliveryIdentitySet(
  manifests: readonly unknown[], endpointUrls: AsyncIterable<string>, options: IdentityOptions,
): Promise<IdentityVerdict> {
  const indexed = indexManifests(manifests, options);
  let observedRows = 0;
  const cohortRows: Record<Cohort, number> = { warmup: 0, measurement: 0, post_window: 0 };
  for await (const value of endpointUrls) {
    if (typeof value !== "string" || value.length > 160) throw new Error("malformed delivery identity");
    const match = /^https:\/\/x\.invalid\/v1\/([a-f0-9]{32})\/([a-f0-9]{32})\/(0|[1-9][0-9]{0,4})\/([0-9a-z]{1,13})\/(0|[1-9][0-9]{0,9})$/.exec(value);
    if (!match) throw new Error("malformed or noncanonical delivery identity");
    const worker = integer(Number(match[3]), "identity worker", 0, 65_535);
    const sequence = base36(match[4]!);
    const row = integer(Number(match[5]), "row ordinal", 0, 4_294_967_295);
    const entries = indexed.namespaces.get(`${match[1]}/${match[2]}/${worker}`);
    const range = entries && findRange(entries, sequence);
    if (!range || row >= range.rowsPerBatch) throw new Error("foreign delivery row identity");
    const bit = Number(sequence - range.first) * range.rowsPerBatch + row;
    const byte = Math.floor(bit / 8);
    const mask = 1 << (bit % 8);
    if ((range.bitmap[byte]! & mask) !== 0) throw new Error("duplicate delivery row identity");
    range.bitmap[byte] = range.bitmap[byte]! | mask;
    observedRows++;
    cohortRows[range.cohort]++;
  }
  if (observedRows !== indexed.expectedRows) {
    throw new Error(`missing delivery row identities: expected=${indexed.expectedRows} observed=${observedRows}`);
  }
  return {
    verified: true, scope: "exact_whole_snapshot_delivery_identity_set_equality",
    expectedRows: indexed.expectedRows, observedRows, expectedBatches: indexed.expectedBatches,
    manifestCount: manifests.length, rangeCount: indexed.ranges, bitmapBytes: indexed.bytes, cohortRows,
    cohortScope: "producer_first_send_cohorts_including_warmup_and_drain_not_Delta_commit_window",
  };
}
