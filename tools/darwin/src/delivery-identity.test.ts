import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { parseManifestJson, verifyDeliveryIdentitySet } from "./delivery-identity.ts";
import { verifierArtifactSha256, verifyActiveParquetIdentitySet } from "./delivery-identity-cli.ts";

const run = "a".repeat(32);
const incarnation = "b".repeat(32);
const table = "sqlserver_availability_replica_states";
const options = { table, rewriteCommitCount: 0 };
const identity = (sequence: number, row: number, worker = 0): string =>
  `https://x.invalid/v1/${run}/${incarnation}/${worker}/${sequence.toString(36)}/${row}`;
const range = (first: number, last: number, rows: number, cohort: string, worker = 0) => ({
  table, worker, first_sequence: first, last_sequence: last, batch_count: last - first + 1,
  rows_per_batch: rows, cohort, attempts: 1, outcome: "acknowledged",
  first_send_unix_ns: cohort === "warmup" ? 999_000_000 : cohort === "measurement" ? 1_500_000_000 : 2_000_000_000,
  last_outcome_unix_ms: 2100,
});
const manifest = (ranges = [range(0, 0, 2, "warmup"), range(1, 1, 3, "measurement"), range(2, 2, 2, "post_window")]) => {
  const count = ranges.reduce((total, item) => total + item.batch_count, 0);
  return {
    schema: "arrowcol.logical_batch_manifest.v1", version: 1, data_profile: "realistic",
    identity_status: "identity_bearing", eligible_for_identity_reconciliation: true,
    complete: true, truncated: false, record_capacity_reached: false,
    identity_column: "endpoint_url",
    identity_format: "https://x.invalid/v1/{run_uuid_simple32}/{incarnation_uuid_simple32}/{worker_decimal}/{logical_batch_sequence_base36}/{row_ordinal_decimal}",
    run_uuid: run, incarnation_uuid: incarnation, run_uuid_source: "client_generated",
    incarnation_uuid_source: "client_generated", oat_run_uuid: null,
    measurement_bounds: { start_unix_ms_inclusive: 1000, end_unix_ms_exclusive: 2000 },
    cohort_rule: "first_send_unix_ms < start => warmup; start <= first_send_unix_ms < end => measurement; otherwise post_window; bounds are [start,end)",
    record_count: count, record_cap: 1_000_000, prepared_logical_batches: count,
    attempted_logical_batches: count, acknowledged_logical_batches: count,
    not_admitted_logical_batches: 0, ambiguous_logical_batches: 0, failed_logical_batches: 0,
    pending_logical_batches: 0, retry_attempts: 0,
    payload_corpus_sha256: "c".repeat(64), payload_corpus_hash_version: 2,
    payload_corpus_hash_scope: "business payload excluding delivery identities",
    ranges,
  };
};
const rows = [identity(0, 0), identity(0, 1), identity(1, 0), identity(1, 1), identity(1, 2), identity(2, 0), identity(2, 1)];
async function* stream(values: readonly string[]) { yield* values; }

test("raw manifest parsing preserves u64 tokens and rejects duplicates and overflow", async () => {
  const source = JSON.stringify(manifest()).replace("999000000", "18446744073709551615");
  const parsed = parseManifestJson(source) as { ranges: { first_send_unix_ns: unknown }[] };
  assert.equal(parsed.ranges[0]!.first_send_unix_ns, "18446744073709551615");
  assert.equal((await verifyDeliveryIdentitySet([parsed], stream(rows), options)).observedRows, 7);
  const overflow = parseManifestJson(source.replace("18446744073709551615", "18446744073709551616"));
  await assert.rejects(verifyDeliveryIdentitySet([overflow], stream(rows), options), /u64/);
  const rounded = manifest();
  rounded.ranges[0]!.first_send_unix_ns = Number.MAX_SAFE_INTEGER + 1;
  await assert.rejects(verifyDeliveryIdentitySet([rounded], stream(rows), options), /send timestamp/);
  assert.throws(() => parseManifestJson('{"x":1,"\\u0078":2}'), /duplicate/);
  assert.throws(() => parseManifestJson('{"x":[1,]}'), /invalid/);
});

test("worker ids are limited to 0 through 1023 in manifests and observed identities", async () => {
  const high = manifest([range(0, 0, 1, "warmup", 1023)]);
  assert.equal((await verifyDeliveryIdentitySet([high], stream([identity(0, 0, 1023)]), options)).observedRows, 1);
  await assert.rejects(verifyDeliveryIdentitySet([manifest([range(0, 0, 1, "warmup", 1024)])], stream([]), options), /worker/);
  await assert.rejects(verifyDeliveryIdentitySet([high], stream([identity(0, 0, 1024)]), options), /worker/);
});

test("whole snapshot includes warmup/drain, permits file reordering and independent client namespaces", async () => {
  const second = { ...manifest(), run_uuid: "d".repeat(32) };
  const values = [...rows.slice(3), ...rows.slice(0, 3), ...rows.map(value => value.replace(run, "d".repeat(32)))];
  const verdict = await verifyDeliveryIdentitySet([manifest(), second], stream(values), options);
  assert.equal(verdict.expectedRows, 14);
  assert.equal(verdict.observedRows, 14);
  assert.deepEqual(verdict.cohortRows, { unscheduled: 0, warmup: 4, measurement: 6, post_window: 4 });
});

test("clients may reuse worker IDs only in disjoint incarnations and every client's rows must be present", async () => {
  const otherIncarnation = "e".repeat(32);
  const second = { ...manifest(), incarnation_uuid: otherIncarnation };
  const otherRows = rows.map(row => row.replace(incarnation, otherIncarnation));
  assert.equal((await verifyDeliveryIdentitySet([manifest(), second], stream([...rows, ...otherRows]), options)).observedRows, 14);
  await assert.rejects(verifyDeliveryIdentitySet([manifest(), second], stream([...rows, ...otherRows.slice(1)]), options), /missing/);
  await assert.rejects(verifyDeliveryIdentitySet([manifest(), second],
    stream([...rows, ...otherRows.slice(0, -1), otherRows[0]!]), options), /duplicate/);
  await assert.rejects(verifyDeliveryIdentitySet([manifest(), manifest()], stream([...rows, ...rows]), options), /duplicate terminal/);
});

test("legitimate one-shot v1 deliveries are included separately without inventing a measurement window", async () => {
  const oneShot = { ...manifest([range(0, 0, 2, "unscheduled")]), run_uuid: "d".repeat(32), measurement_bounds: null };
  const values = [...rows, identity(0, 0).replace(run, "d".repeat(32)), identity(0, 1).replace(run, "d".repeat(32))];
  const result = await verifyDeliveryIdentitySet([manifest(), oneShot], stream(values), options);
  assert.equal(result.observedRows, 9);
  assert.deepEqual(result.cohortRows, { unscheduled: 2, warmup: 2, measurement: 3, post_window: 2 });
  await assert.rejects(verifyDeliveryIdentitySet([{ ...oneShot, measurement_bounds: manifest().measurement_bounds }], stream(values), options), /contradicts/);
});

test("rejects duplicate replacement even when row count matches, missing rows and foreign namespaces/ordinals", async () => {
  await assert.rejects(verifyDeliveryIdentitySet([manifest()], stream([rows[0]!, ...rows.slice(0, -1)]), options), /duplicate/);
  await assert.rejects(verifyDeliveryIdentitySet([manifest()], stream(rows.slice(0, -1)), options), /missing/);
  for (const foreign of [rows[0]!.replace(run, "f".repeat(32)), identity(9, 0), identity(0, 2), identity(0, 0, 1)]) {
    await assert.rejects(verifyDeliveryIdentitySet([manifest()], stream([foreign]), options), /foreign/);
  }
});

test("canonical decimal/base36 boundaries preserve exact delivery equality", async () => {
  const value = manifest([range(0, 35, 11, "warmup"), range(36, 36, 11, "measurement")]);
  const values = Array.from({ length: 37 }, (_, sequence) => Array.from({ length: 11 }, (_, row) => identity(sequence, row))).flat();
  assert.equal((await verifyDeliveryIdentitySet([value], stream(values), options)).observedRows, 407);
  for (const bad of [identity(0, 0).replace("/0/0", "/00/0"), identity(35, 0).replace("/z/", "/Z/"),
    identity(0, 0) + "?x=1", identity(0, 0) + "\n", identity(0, 0).replace("/0/0", "/01/0")]) {
    await assert.rejects(verifyDeliveryIdentitySet([value], stream([bad]), options), /canonical|malformed/);
  }
  await assert.rejects(verifyDeliveryIdentitySet([value], stream([identity(0, 0).replace(/\/0\/0$/, "/3w5e11264sgsg/0")]), options), /exceeds u64/);
});

test("rejects incomplete/truncated/diagnostic manifests, retries, ambiguity, missing counters and rewrites", async () => {
  for (const change of [
    { complete: false }, { truncated: true }, { data_profile: "diagnostic" }, { version: 2 },
    { record_capacity_reached: true }, { retry_attempts: 1 }, { ambiguous_logical_batches: 1 },
    { failed_logical_batches: 1 }, { pending_logical_batches: 1 }, { acknowledged_logical_batches: undefined },
    { record_count: 4 }, { terminal_failure: "stopped" },
  ]) {
    await assert.rejects(verifyDeliveryIdentitySet([{ ...manifest(), ...change }], stream(rows), options));
  }
  await assert.rejects(verifyDeliveryIdentitySet([manifest()], stream(rows), { ...options, rewriteCommitCount: 1 }), /rewrite/);
  for (const change of [{ attempts: 2 }, { outcome: "ambiguous" }, { cohort: "unsupported" },
    { batch_count: 2 }, { first_sequence: Number.MAX_SAFE_INTEGER + 1 }, { first_send_unix_ns: null },
    { extra_range_field: 1 }]) {
    const value = manifest();
    Object.assign(value.ranges[0]!, change);
    await assert.rejects(verifyDeliveryIdentitySet([value], stream(rows), options));
  }
});

test("rejects omitted batches, overlapping ranges, duplicate terminal namespaces and inconsistent bounds", async () => {
  for (const value of [
    manifest([range(1, 1, 2, "measurement")]),
    manifest([range(0, 0, 2, "warmup"), range(2, 2, 2, "post_window")]),
    manifest([range(0, 1, 2, "warmup"), range(1, 2, 2, "measurement")]),
  ]) {
    await assert.rejects(verifyDeliveryIdentitySet([value], stream(rows), options), /incomplete|overlapping/);
  }
  await assert.rejects(verifyDeliveryIdentitySet([manifest(), manifest()], stream(rows), options), /duplicate terminal/);
  const different = { ...manifest(), run_uuid: "d".repeat(32), measurement_bounds: { start_unix_ms_inclusive: 1001, end_unix_ms_exclusive: 2000 } };
  await assert.rejects(verifyDeliveryIdentitySet([manifest(), different], stream(rows), options), /different shared windows/);
});

test("memory/range limits fail before consuming any observed rows", async () => {
  let consumed = false;
  async function* observed() { consumed = true; yield* rows; }
  await assert.rejects(verifyDeliveryIdentitySet([manifest()], observed(), { ...options, maxBitmapBytes: 1 }), /memory bound/);
  await assert.rejects(verifyDeliveryIdentitySet([manifest()], observed(), { ...options, maxRanges: 2 }), /memory bound/);
  assert.equal(consumed, false);
});

const linux = { skip: process.platform !== "linux" };

test("stream adapter keeps original nanosecond bytes/cohorts and rejects aliasing, truncation and wrong hashes", linux, async () => {
  const dir = await mkdtemp(join(tmpdir(), "delivery-verifier-"));
  try {
    const paths = [join(dir, "warmup.parquet"), join(dir, "measurement.parquet"), join(dir, "drain.parquet")];
    await Promise.all([rows.slice(0, 2), rows.slice(2, 5), rows.slice(5)].map((values, index) => writeFile(paths[index]!, values.join("\n") + "\n")));
    const decoder = join(dir, "decoder.cjs");
    const script = "#!/usr/bin/env node\nconst fs=require('node:fs');const text=fs.readFileSync(process.argv[3],'utf8');process.stdout.write(text);process.stderr.write('DELIVERY_IDENTITY_READER_COMPLETED rows='+(text.split('\\n').length-1)+'\\n');\n";
    await writeFile(decoder, script, { mode: 0o755 });
    const manifestPath = join(dir, "manifest.json");
    const original = manifest();
    original.measurement_bounds = { start_unix_ms_inclusive: 1_710_000_000_000, end_unix_ms_exclusive: 1_710_000_001_000 };
    original.ranges.forEach(item => { item.last_outcome_unix_ms = 1_710_000_001_001; });
    const serialized = JSON.stringify(original)
      .replace('"first_send_unix_ns":999000000', '"first_send_unix_ns":1709999999999999999')
      .replace('"first_send_unix_ns":1500000000', '"first_send_unix_ns":1710000000500000001')
      .replace('"first_send_unix_ns":2000000000', '"first_send_unix_ns":1710000001000000001');
    await writeFile(manifestPath, serialized);
    const input = { table, rewrite_commit_count: 0, manifest_paths: [manifestPath], active_parquet_paths: paths };
    const config = { decoderPath: decoder, expectedDecoderSha256: createHash("sha256").update(script).digest("hex"), expectedVerifierSha256: await verifierArtifactSha256() };
    const result = await verifyActiveParquetIdentitySet(input, config);
    assert.equal(result.activeFileCount, 3);
    assert.equal(result.activeFileRows, 7);
    assert.deepEqual(result.cohortRows, { unscheduled: 0, warmup: 2, measurement: 3, post_window: 2 });
    assert.equal(result.manifestSources[0]!.sha256, createHash("sha256").update(await readFile(manifestPath)).digest("hex"));
    await assert.rejects(verifyActiveParquetIdentitySet({ ...input, active_parquet_paths: [paths[0]!, paths[0]!] }, config), /aliased/);
    await assert.rejects(verifyActiveParquetIdentitySet(input, { ...config, expectedDecoderSha256: "0".repeat(64) }), /hash differs/);
    await assert.rejects(verifyActiveParquetIdentitySet(input, { ...config, expectedVerifierSha256: "0".repeat(64) }), /hash differs/);
    await writeFile(paths[2]!, rows.slice(5).join("\n"));
    await assert.rejects(verifyActiveParquetIdentitySet(input, config), /truncated decoder/);
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});

test("early identity failure and interruption stop/reap exact decoders despite ignored SIGTERM", linux, async () => {
  const dir = await mkdtemp(join(tmpdir(), "delivery-decoder-cleanup-"));
  try {
    const file = join(dir, "active.parquet");
    const decoder = join(dir, "decoder.cjs");
    await writeFile(file, "");
    const manifestPath = join(dir, "manifest.json");
    await writeFile(manifestPath, JSON.stringify(manifest()));
    for (const mode of ["duplicate", "abort"]) {
      const pidFile = join(dir, `${mode}-pid`);
      const script = `#!/usr/bin/env node\nconst fs=require('node:fs');process.on('SIGTERM',()=>{});fs.writeFileSync(${JSON.stringify(pidFile)},String(process.pid));process.stdout.write(${JSON.stringify(mode === "duplicate" ? rows[0] + "\n" + rows[0] + "\n" : "")});setInterval(()=>{},10000);\n`;
      await writeFile(decoder, script, { mode: 0o755 });
      const controller = new AbortController();
      const start = Date.now();
      const verification = verifyActiveParquetIdentitySet(
        { table, rewrite_commit_count: 0, manifest_paths: [manifestPath], active_parquet_paths: [file] },
        { decoderPath: decoder, expectedDecoderSha256: createHash("sha256").update(script).digest("hex"), expectedVerifierSha256: await verifierArtifactSha256(), signal: controller.signal },
      );
      if (mode === "abort") {
        while (true) {
          try { await readFile(pidFile); break; }
          catch (error) {
            if (!(error instanceof Error) || !("code" in error) || error.code !== "ENOENT") throw error;
          }
          assert.ok(Date.now() - start < 2000);
          await new Promise(done => setTimeout(done, 10));
        }
        controller.abort(new Error("stop owned decoder"));
      }
      await assert.rejects(verification, mode === "duplicate" ? /duplicate/ : /stop owned decoder/);
      assert.ok(Date.now() - start < 5000);
      const pid = Number(await readFile(pidFile, "utf8"));
      assert.throws(() => process.kill(pid, 0), { code: "ESRCH" });
    }
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});
