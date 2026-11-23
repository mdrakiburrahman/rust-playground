import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { chmod, mkdir, mkdtemp, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { Table, Utf8, tableToIPC, vectorFromArray } from "apache-arrow";
import { Table as WasmTable, writeParquet } from "parquet-wasm";
import { artifactHash, decodeExtractedManifests, hashTsx, verifyReconciledDelivery } from "./identity-reconcile.js";

test("extractor frames preserve bytes and reject truncation, noncanonical lengths, and wrong counts", () => {
  const first = Buffer.from('{"n":18446744073709551615}');
  const second = Buffer.from('{"n":9007199254740993}');
  const framed = Buffer.concat([Buffer.from(`${first.length}\n`), first, Buffer.from(`${second.length}\n`), second]);
  assert.deepEqual(decodeExtractedManifests(framed, 2), [first, second]);
  assert.throws(() => decodeExtractedManifests(framed, 1), /extra/);
  assert.throws(() => decodeExtractedManifests(framed, 3), /count/);
  assert.throws(() => decodeExtractedManifests(framed.subarray(0, -1), 2), /truncated/);
  assert.throws(() => decodeExtractedManifests(Buffer.from("02\n{}"), 1), /length/);
  assert.throws(() => decodeExtractedManifests(Buffer.from("1048577\n"), 1), /oversized/);
});

test("binds original evidence and rejects duplicate delivered identities", async () => {
  const dir = await mkdtemp(join(tmpdir(), "darwin-identity-gate-"));
  try {
    const table = "orders";
    const prefix = `https://x.invalid/v1/${"a".repeat(32)}/${"b".repeat(32)}/0/0/`;
    const manifest = {
      schema: "arrowcol.logical_batch_manifest.v1", version: 1, data_profile: "realistic",
      identity_status: "identity_bearing", eligible_for_identity_reconciliation: true,
      complete: true, truncated: false, record_capacity_reached: false,
      identity_column: "endpoint_url",
      identity_format: "https://x.invalid/v1/{run_uuid_simple32}/{incarnation_uuid_simple32}/{worker_decimal}/{logical_batch_sequence_base36}/{row_ordinal_decimal}",
      run_uuid: "a".repeat(32), incarnation_uuid: "b".repeat(32),
      run_uuid_source: "client_generated", incarnation_uuid_source: "client_generated", oat_run_uuid: null,
      measurement_bounds: { start_unix_ms_inclusive: 1000, end_unix_ms_exclusive: 2000 },
      cohort_rule: "first_send_unix_ms < start => warmup; start <= first_send_unix_ms < end => measurement; otherwise post_window; bounds are [start,end)",
      record_count: 1, record_cap: 1_000_000, prepared_logical_batches: 1, attempted_logical_batches: 1,
      acknowledged_logical_batches: 1, not_admitted_logical_batches: 0, ambiguous_logical_batches: 0,
      failed_logical_batches: 0, pending_logical_batches: 0, retry_attempts: 0,
      payload_corpus_sha256: "c".repeat(64), payload_corpus_hash_version: 2, payload_corpus_hash_scope: "business",
      ranges: [{ table, worker: 0, first_sequence: 0, last_sequence: 0, batch_count: 1, rows_per_batch: 2,
        cohort: "measurement", attempts: 1, outcome: "acknowledged", first_send_unix_ns: 123456789,
        last_outcome_unix_ms: 2100 }],
    };
    const original = JSON.stringify(manifest).replace("123456789", "1795385557123456789");
    const other = original.replace('"run_uuid":"' + "a".repeat(32), '"run_uuid":"' + "d".repeat(32));
    const otherPrefix = prefix.replace("a".repeat(32), "d".repeat(32));
    const evidence = `{"client_phase_reports":[{"phase_counters":{"payload_manifest":${original}}},{"phase_counters":{"payload_manifest":${other}}}]}`;
    const extractor = process.env.DARWIN_TEST_EXTRACTOR ?? join(dir, "extractor.cjs");
    const decoder = process.env.DARWIN_TEST_READER ?? join(dir, "decoder.cjs");
    const realParquet = Boolean(process.env.DARWIN_TEST_READER);
    if (!process.env.DARWIN_TEST_EXTRACTOR) {
      await writeFile(extractor, `#!${process.execPath}\nprocess.stdin.resume();process.stdin.on("end",()=>{for(const m of ${JSON.stringify([original, other])})process.stdout.write(Buffer.byteLength(m)+"\\n"+m)});\n`);
      await chmod(extractor, 0o700);
    }
    if (!realParquet) {
      await writeFile(decoder, `#!${process.execPath}\nconst s=require("fs").readFileSync(process.argv[3],"utf8");process.stdout.write(s);process.stderr.write("DELIVERY_IDENTITY_READER_COMPLETED rows="+s.split("\\n").filter(Boolean).length+"\\n");\n`);
      await chmod(decoder, 0o700);
    }
    const data = join(dir, "active.parquet");
    const result = join(dir, "results.parquet");
    const writeRows = async (rows: string[]) => writeFile(data, realParquet
      ? writeParquet(WasmTable.fromIPCStream(tableToIPC(new Table({ endpoint_url: vectorFromArray(rows, new Utf8()) }), "stream")))
      : `${rows.join("\n")}\n`);
    await writeRows([`${prefix}0`, `${prefix}1`, `${otherPrefix}0`, `${otherPrefix}1`]);
    await writeFile(result, "bound result");
    const digest = async (path: string) => createHash("sha256").update(await readFile(path)).digest("hex");
    const node = await realpath(process.execPath);
    const tsxRoot = await realpath(join(process.cwd(), "node_modules", "tsx"));
    const verifier = join(process.cwd(), "tools", "darwin", "src", "delivery-identity-cli.ts");
    const tools = {
      node, nodeSha256: await digest(node), tsxRoot, tsxSha256: await hashTsx(tsxRoot),
      decoder, decoderSha256: await digest(decoder), extractor, extractorSha256: await digest(extractor),
      verifier, verifierArtifactSha256: await artifactHash(verifier), runtime: process.cwd(), env: process.env,
    };
    const first = join(dir, "first");
    await mkdir(first);
    const verdict = await verifyReconciledDelivery(tools, first, result, await digest(result), evidence, table, [data]);
    assert.equal(verdict.verified, true);
    assert.equal(verdict.activeFileRows, 4);
    assert.equal(verdict.manifestCount, 2);
    assert.equal(await readFile(join(first, "identity-manifest-0000.json"), "utf8"), original);
    assert.equal(await readFile(join(first, "identity-manifest-0001.json"), "utf8"), other);
    assert.equal(await readFile(join(first, "identity-evidence.json"), "utf8"), evidence);
    await writeRows([`${prefix}0`, `${prefix}1`, `${otherPrefix}0`, `${otherPrefix}0`]);
    const second = join(dir, "second");
    await mkdir(second);
    await assert.rejects(verifyReconciledDelivery(tools, second, result, await digest(result), evidence, table, [data]));
    await writeRows([`${prefix}0`, `${prefix}1`, `${otherPrefix}0`]);
    const third = join(dir, "third");
    await mkdir(third);
    await assert.rejects(verifyReconciledDelivery(tools, third, result, await digest(result), evidence, table, [data]));
  } finally {
    await rm(dir, { recursive: true, force: true });
  }
});
