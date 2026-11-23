import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { chmod, mkdtemp, mkdir, readFile, realpath, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { test } from "node:test";
import { Table, Utf8, tableFromArrays, tableFromIPC, tableToIPC, vectorFromArray } from "apache-arrow";
import { Compression, readParquet, Table as WasmTable, WriterPropertiesBuilder, writeParquet } from "parquet-wasm";
import { assertParityProcessRevision, DIAGNOSTIC_REALISTIC_DEFAULT_WRITER_VERSION, DIAGNOSTIC_REALISTIC_RECONCILIATION_VERSION, extractClientEvidence, extractClientReports, extractStoppedReceipts, readReconciledOatResult, type ClientDataProfile } from "./oat-result.js";
import { artifactHash, hashTsx } from "./identity-reconcile.js";
import type { OatMeasurement } from "./metrics-adapter.js";

function reportFor(profile: ClientDataProfile) {
  const hash = "a".repeat(64);
  const scope = profile === "diagnostic" ? "arrow-logical-v2-all-columns" : "arrow-logical-v2-excluding-endpoint_url";
  const identityEligible = profile === "realistic";
  const preparation: Record<string, unknown> = {
    data_profile: profile, steady_state_generation: identityEligible,
    payload_corpus_sha256: hash, payload_corpus_hash_version: 2, payload_corpus_hash_scope: scope,
  };
  const payloadManifest: Record<string, unknown> = {
    data_profile: profile, payload_corpus_sha256: hash, payload_corpus_hash_version: 2,
    payload_corpus_hash_scope: scope, identity_status: identityEligible ? "identity_bearing" : "diagnostic_only",
    eligible_for_identity_reconciliation: identityEligible, complete: identityEligible, truncated: false,
  };
  const clientResources: Record<string, unknown> = {
    cpu_core_equivalents: 2.5, start_sample_unix_ms: 100, end_sample_unix_ms: 200,
  };
  return { phase_counters: { preparation, payload_manifest: payloadManifest, measurement_window: { client_resources: clientResources } } };
}

test("multiple reports bind client images, configs, CPU and checked completed totals", () => {
  const make = () => ({
    ...reportFor("realistic"),
    execution_image: { source: "docker_container_inspect", image_id: `sha256:${"e".repeat(64)}` },
    consumer_effective_config: { workers: 8 },
    phase_counters: { ...reportFor("realistic").phase_counters,
      process_cumulative: { completed_rows: 10, ambiguous_rows: 0 } },
  });
  const first = make();
  const second = make();
  const result = extractClientReports([first, second], "realistic");
  assert.equal(result.totalCompletedRows, 20);
  assert.equal(result.clientEvidence.cpu.coreEquivalents, 5);
  assert.equal(result.reports.length, 2);
  assert.deepEqual(result.reports.map(report => report.config), [{ workers: 8 }, { workers: 8 }]);
  assert.throws(() => extractClientReports([first, { ...second,
    execution_image: { ...second.execution_image, image_id: `sha256:${"f".repeat(64)}` } }], "realistic"), /images differ/);
  assert.throws(() => extractClientReports([first, { ...second, consumer_effective_config: null }], "realistic"), /client readback/);
  assert.throws(() => extractClientReports([first, { ...second, phase_counters: { ...second.phase_counters,
    process_cumulative: { completed_rows: Number.MAX_SAFE_INTEGER, ambiguous_rows: 0 } } }], "realistic"), /overflow/);
  assert.throws(() => extractClientReports([first, { ...second, phase_counters: { ...second.phase_counters,
    process_cumulative: { completed_rows: 10, ambiguous_rows: 1 } } }], "realistic"), /ambiguous/);
  assert.throws(() => extractClientReports([], "realistic"), /1024/);
});

test("a control revision applies only to the collector", () => {
  const collector = { component: "collector", source_revision: "main" };
  const router = { component: "router", source_revision: "observer" };
  assert.doesNotThrow(() => assertParityProcessRevision(collector, "observer", "main"));
  assert.doesNotThrow(() => assertParityProcessRevision(router, "observer", "main"));
  assert.throws(() => assertParityProcessRevision(collector, "observer"), /source revision/);
  assert.throws(() => assertParityProcessRevision({ ...router, source_revision: "main" }, "observer", "main"), /source revision/);
});

test("refuses a diagnostic result without the physical parity gate", async () => {
  await assert.rejects(readReconciledOatResult("unused", "unused", "orders", "unused", null, "diagnostic"),
    /requires a physical footer inspector/);
  await assert.rejects(readReconciledOatResult("unused", "unused", "orders", "unused", 3, "diagnostic", {
    config: {}, decodeFile: async () => 0,
  }), /requires a physical footer inspector/);
  await assert.rejects(readReconciledOatResult("unused", "unused", "orders", "unused", null, "realistic"),
    /requires its Zstd level/);
  await assert.rejects(readReconciledOatResult("unused", "unused", "orders", "unused", 3, "realistic"),
    /requires full delivery identity reconciliation/);
  await assert.rejects(readReconciledOatResult("unused", "unused", "orders", "unused", 3, "realistic", {
    config: {}, decodeFile: async () => 0,
  }), /explicitly qualified parity gate/);
  await assert.rejects(readReconciledOatResult("unused", "unused", "orders", "unused",
    { codec: "snappy", level: null }, "realistic"), /typed compression requires qualified preflight/);
});

test("diagnostic Realistic reconciles whole-snapshot identities and deduplicated half-open commits", async () => {
  const root = await mkdtemp(join(process.cwd(), ".diagnostic-realistic-test-"));
  try {
    const table = "orders";
    const run = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee";
    const revision = "f".repeat(40);
    const start = 1_795_385_557_000;
    const end = start + 150_000;
    const snapshot = join(root, "snapshots", run);
    const tableRoot = join(snapshot, table, "table");
    await mkdir(join(tableRoot, "_delta_log"), { recursive: true });
    const sha = (bytes: Uint8Array | string) => createHash("sha256").update(bytes).digest("hex");
    const namespaces = [
      { run_uuid: "a".repeat(32), incarnation_uuid: "b".repeat(32) },
      { run_uuid: "d".repeat(32), incarnation_uuid: "e".repeat(32) },
    ];
    const ids = (index: number, count: number) => Array.from({ length: count }, (_, row) =>
      `https://x.invalid/v1/${namespaces[index]!.run_uuid}/${namespaces[index]!.incarnation_uuid}/0/0/${row}`);
    const firstIds = ids(0, 3);
    const secondIds = ids(1, 2);
    const parquet = (rows: string[], codec = Compression.UNCOMPRESSED) => Buffer.from(writeParquet(
      WasmTable.fromIPCStream(tableToIPC(new Table({ endpoint_url: vectorFromArray(rows, new Utf8()) }), "stream")),
      new WriterPropertiesBuilder().setCompression(codec).build(),
    ));
    let first = parquet(firstIds);
    const second = parquet(secondIds);
    let totalRows = 5;
    const schema = JSON.stringify({ fields: [] });
    const files: Record<string, Buffer> = {
      "first.parquet": first, "second.parquet": second,
      "_delta_log/00000000000000000000.json": Buffer.from(JSON.stringify({ metaData: { schemaString: schema } }) + "\n"),
      "_delta_log/00000000000000000001.json": Buffer.from(JSON.stringify({ add: { path: "first.parquet", size: first.length, dataChange: true } }) + "\n"),
      "_delta_log/00000000000000000002.json": Buffer.from(JSON.stringify({ add: { path: "second.parquet", size: second.length, dataChange: true } }) + "\n"),
    };
    const writeSnapshot = async () => {
      for (const [path, bytes] of Object.entries(files)) await writeFile(join(tableRoot, path), bytes);
      const manifest = Buffer.from(JSON.stringify({ schema_version: 1, run_id: run, table, latestVersion: 2,
        rowCount: totalRows, schemaString: schema,
        files: Object.entries(files).map(([path, bytes]) => ({ path, size: bytes.length, sha256: sha(bytes) })) }));
      await writeFile(join(snapshot, table, "manifest.json"), manifest);
      await writeFile(join(snapshot, "complete.json"), JSON.stringify({ schema_version: 1, run_id: run, tables: [table],
        manifests: [{ path: `${table}/manifest.json`, size: manifest.length, sha256: sha(manifest) }] }));
    };
    const tableId = sha(`file:///delta/dataflow/telemetry/sqlserver/${table}/`);
    const receipt = {
      table_id: tableId, table_name: `telemetry.sqlserver.${table}`, delta_version: 1,
      rows: 3, files: 1, input_bytes: 0, parquet_bytes: first.length, min_ingested_at_ms: 0,
      max_ingested_at_ms: 1, finished_at_unix_ms: start, elapsed_seconds: 0.1,
    };
    const later = { ...receipt, delta_version: 2, rows: 2, parquet_bytes: second.length, finished_at_unix_ms: end };
    const cumulative = { scope: "all_new_ingestion_commits_from_this_cell_including_warmup", rows: 5, commits: 2,
      commits_by_version: [receipt, receipt, later] };
    const clients = [3, 2].map((completed_rows, index) => ({
      ...reportFor("realistic"),
      execution_image: { source: "docker_container_inspect", image_id: `sha256:${"e".repeat(64)}` },
      consumer_effective_config: { workers: 14, streams_per_worker: 2 },
      phase_counters: { ...reportFor("realistic").phase_counters,
        payload_manifest: { ...reportFor("realistic").phase_counters.payload_manifest,
          ...namespaces[index], schema: "arrowcol.logical_batch_manifest.v1", version: 1,
          record_capacity_reached: false, identity_column: "endpoint_url",
          identity_format: "https://x.invalid/v1/{run_uuid_simple32}/{incarnation_uuid_simple32}/{worker_decimal}/{logical_batch_sequence_base36}/{row_ordinal_decimal}",
          run_uuid_source: "client_generated", incarnation_uuid_source: "client_generated", oat_run_uuid: null,
          measurement_bounds: { start_unix_ms_inclusive: start, end_unix_ms_exclusive: end },
          cohort_rule: "first_send_unix_ms < start => warmup; start <= first_send_unix_ms < end => measurement; otherwise post_window; bounds are [start,end)",
          record_count: 1, record_cap: 1_000_000, prepared_logical_batches: 1, attempted_logical_batches: 1,
          acknowledged_logical_batches: 1, not_admitted_logical_batches: 0, ambiguous_logical_batches: 0,
          failed_logical_batches: 0, pending_logical_batches: 0, retry_attempts: 0,
          ranges: [{ table, worker: 0, first_sequence: 0, last_sequence: 0, batch_count: 1,
            rows_per_batch: completed_rows, cohort: "measurement", attempts: 1, outcome: "acknowledged",
            first_send_unix_ns: 123456789, last_outcome_unix_ms: start + 1000 }] },
        process_cumulative: { completed_rows, ambiguous_rows: 0 } },
    }));
    const evidence = {
      version: 1,
      processes: [{ component: "collector", source_revision: revision, log_complete: true,
        image_id: `sha256:${"b".repeat(64)}`, effective_config_source: "consumer_startup_readback",
        effective_config: { commit_telemetry: { schema_version: 1, scope: "new_ingestion_addfiles_after_sync", clock: "system_unix_ms" } },
        writer_policy_overrides: { defaults: { compression: null, compression_level: null },
          columns: {}, max_row_group_rows: null } }],
      client_phase_reports: clients,
      client_schedule: { effective: { run_start_unix_ms: start - 50 } },
      parity_admission_readback: { status: "verified_before_schedule", observed_at_unix_ms: start - 75,
        collector_image_id: `sha256:${"b".repeat(64)}`,
        collector: { parquet_writer: { defaults: { compression: null }, columns: {} } } },
      preload_delta: { table: `telemetry.sqlserver.${table}`, latest_version: 0, observed_at_unix_ms: start - 100,
        partition_columns: ["__arrowcol_ingestion"], granularity: "hour",
        logs: [{ version: 0, sha256: sha(files["_delta_log/00000000000000000000.json"]!) }] },
      durable_commits: {
        run_cumulative_output: cumulative,
        output_throughput: {
          scope: "new_ingestion_commits_finished_in_half_open_shared_window_not_client_cohort",
          clock: "collector_system_unix_ms_shared_schedule_clock_skew_not_measured",
          warmup_backlog: "included_when_commit_finishes_in_window",
          rewrite_commits: "excluded_by_ingestion_only_producer_capability",
          measurement_start_unix_ms: start, measurement_end_unix_ms: end, window_seconds: 150,
          rows: 3, files: 1, commits: 1, parquet_bytes: first.length,
          rows_per_second: 3 / 150, bytes_per_row: first.length / 3,
          commit_latency_seconds: { p50: 0.1, p99: 0.1, samples: 1,
            scope: "commit_retry_and_sync_not_encoding_or_admission" },
        },
        complete_30_second_commit_windows: Array.from({ length: 5 }, (_, i) => ({
          measurement_start_unix_ms: start + i * 30000, measurement_end_unix_ms: start + (i + 1) * 30000,
          window_seconds: 30, rows: i === 0 ? 3 : 0, parquet_bytes: i === 0 ? first.length : 0,
        })),
      },
      resources: { samples: [] }, downstream_status: "unavailable", downstream_comment: "not measured",
    };
    await writeSnapshot();
    const writeStopped = async () => writeFile(join(snapshot, "stopped.json"), JSON.stringify({
      schema_version: 1, run_id: run, writers: [{
        id: "collector", service: "dataflow-1", was_running: true, oom_killed: false, exit_code: 0,
        finished_at: "2027-01-01T00:00:00Z",
        stopped_commit_output: { source: "complete_stopped_collector_logs", tables: { [`telemetry.sqlserver.${table}`]: cumulative } },
      }],
    }));
    await writeStopped();
    const originalNumber = '"first_send_unix_ns":123456789';
    const exactNumber = '"first_send_unix_ns":1795385557123456789';
    const fragments = clients.map(client => JSON.stringify(client.phase_counters.payload_manifest).replace(originalNumber, exactNumber));
    const evidenceBytes = () => JSON.stringify(evidence).replaceAll(originalNumber, exactNumber);
    const resultPath = join(root, "results.parquet");
    const writeResult = async () => {
      const row = tableFromArrays({
        schema_version: [3], mode: ["local"], table: [table], status: ["succeeded"], git_sha: [revision],
        pinning_ok: [true], replicas: [1], run_id: [run], evidence_json: [evidenceBytes()],
      });
      await writeFile(resultPath, writeParquet(WasmTable.fromIPCStream(tableToIPC(row, "stream"))));
    };
    await writeResult();
    const extractor = process.env.DARWIN_TEST_EXTRACTOR ?? join(root, "extractor.cjs");
    const reader = process.env.DARWIN_TEST_READER ?? join(root, "reader.cjs");
    if (!process.env.DARWIN_TEST_EXTRACTOR) {
      await writeFile(extractor, `#!${process.execPath}\n` + String.raw`
const text = require("node:fs").readFileSync(0, "utf8");
const key = '"payload_manifest":';
let pos = 0;
while ((pos = text.indexOf(key, pos)) !== -1) {
  const start = pos + key.length;
  let depth = 0, quoted = false, escaped = false, end = start;
  for (; end < text.length; end++) {
    const c = text[end];
    if (quoted) {
      if (escaped) escaped = false;
      else if (c === "\\") escaped = true;
      else if (c === '"') quoted = false;
    } else if (c === '"') quoted = true;
    else if (c === "{") depth++;
    else if (c === "}" && --depth === 0) { end++; break; }
  }
  const fragment = text.slice(start, end);
  process.stdout.write(Buffer.byteLength(fragment) + "\n" + fragment);
  pos = end;
}
`);
      await chmod(extractor, 0o700);
    }
    if (!process.env.DARWIN_TEST_READER) {
      await writeFile(reader, `#!${process.execPath}\nconst fs=require("node:fs");(async()=>{const a=await import("apache-arrow"),p=await import("parquet-wasm");const t=a.tableFromIPC(p.readParquet(fs.readFileSync(process.argv[3])).intoIPCStream());const rows=[...t.getChild("endpoint_url")];process.stdout.write(rows.join("\\n")+"\\n");process.stderr.write("DELIVERY_IDENTITY_READER_COMPLETED rows="+rows.length+"\\n")})().catch(e=>{console.error(e);process.exitCode=1});\n`);
      await chmod(reader, 0o700);
    }
    const digest = async (path: string) => sha(await readFile(path));
    const node = await realpath(process.execPath);
    const tsxRoot = await realpath(join(process.cwd(), "node_modules", "tsx"));
    const verifier = join(process.cwd(), "tools", "darwin", "src", "delivery-identity-cli.ts");
    const tools = { node, nodeSha256: await digest(node), tsxRoot, tsxSha256: await hashTsx(tsxRoot),
      decoder: reader, decoderSha256: await digest(reader), extractor, extractorSha256: await digest(extractor),
      verifier, verifierArtifactSha256: await artifactHash(verifier), runtime: process.cwd(), env: process.env };
    const decoded: string[] = [];
    const diagnostic = { diagnosticOnlyRealistic: true as const, expectedClientConfig: { workers: 14, streams_per_worker: 2 },
      decodeFile: async (path: string) => {
      decoded.push(path);
      return tableFromIPC(readParquet(await readFile(path)).intoIPCStream()).numRows;
    } };
    let attempt = 0;
    const read = async (option = diagnostic, parity?: Parameters<typeof readReconciledOatResult>[6]) => {
      const outputDir = join(root, `proof-${attempt++}`);
      await mkdir(outputDir);
      return readReconciledOatResult(resultPath, join(root, "snapshots"), table, revision, null, "realistic",
        parity, { tools, outputDir }, option);
    };
    const result = await read();
    assert.equal(DIAGNOSTIC_REALISTIC_RECONCILIATION_VERSION, 1);
    assert.equal(DIAGNOSTIC_REALISTIC_DEFAULT_WRITER_VERSION, 1);
    assert.equal(result.kind, "diagnostic-only");
    assert.equal(result.diagnosticOnlyRealistic, true);
    assert.equal(result.diagnosticRealisticReconciliationVersion, 1);
    assert.equal(result.deliveryIdentity?.verified, true);
    assert.equal(result.deliveryIdentity?.kind, "diagnostic-only");
    assert.equal(result.deliveryIdentity?.scope, "exact_whole_snapshot_delivery_identity_set_equality");
    assert.equal(result.deliveryIdentity?.expectedRows, 5);
    assert.equal(result.deliveryIdentity?.observedRows, 5);
    assert.equal(result.deliveryIdentity?.manifestCount, 2);
    const originalEvidence = Buffer.from(evidenceBytes());
    const proofRoot = join(root, "proof-0", "diagnostic-only");
    assert.deepEqual(await readFile(join(proofRoot, "identity-evidence.json")), originalEvidence);
    assert.equal(result.deliveryIdentity?.evidenceSha256, sha(originalEvidence));
    const persistedVerdict = JSON.parse(await readFile(join(proofRoot, "identity-result.json"), "utf8"));
    assert.equal(persistedVerdict.kind, "diagnostic-only");
    assert.equal(persistedVerdict.scope, "exact_whole_snapshot_delivery_identity_set_equality");
    const sources = result.deliveryIdentity?.manifestSources as { sha256: string; bytes: number }[];
    for (let i = 0; i < fragments.length; i++) {
      assert.equal(sources[i]!.sha256, sha(fragments[i]!));
      assert.equal(sources[i]!.bytes, Buffer.byteLength(fragments[i]!));
      assert.deepEqual(await readFile(join(proofRoot, `identity-manifest-${String(i).padStart(4, "0")}.json`)),
        Buffer.from(fragments[i]!));
    }
    assert.deepEqual(result.clientEffectiveConfigs, [
      { workers: 14, streams_per_worker: 2 }, { workers: 14, streams_per_worker: 2 },
    ]);
    assert.equal(result.measurement.rows, 3);
    assert.equal(result.activeRows, 5);
    assert.equal(result.measurement.durableRowsPerSecond, 3 / 150);
    assert.deepEqual(decoded.sort(), [join(tableRoot, "first.parquet"), join(tableRoot, "second.parquet")].sort());
    await assert.rejects(readReconciledOatResult(resultPath, join(root, "snapshots"), table, revision,
      { codec: "zstd", level: 3 }, "realistic", undefined, { tools, outputDir: root }, diagnostic),
    /requires the unchanged default writer/);
    await assert.rejects(readReconciledOatResult(resultPath, join(root, "snapshots"), table, revision, 3, "realistic"),
      /requires full delivery identity/);
    await assert.rejects(readReconciledOatResult(resultPath, join(root, "snapshots"), table, revision, null, "realistic",
      undefined, undefined, diagnostic), /requires a physical decoder and identity tools/);
    await assert.rejects(readReconciledOatResult(resultPath, join(root, "snapshots"), table, revision, null, "realistic",
      undefined, { tools: undefined as unknown as typeof tools, outputDir: root }, diagnostic), /requires a physical decoder and identity tools/);
    await assert.rejects(read({ ...diagnostic, decodeFile: undefined as unknown as typeof diagnostic.decodeFile }),
      /requires a physical decoder and identity tools/);
    const invalidToolsDir = join(root, "invalid-tools");
    await mkdir(invalidToolsDir);
    await assert.rejects(readReconciledOatResult(resultPath, join(root, "snapshots"), table, revision, null, "realistic",
      undefined, { tools: { ...tools, extractorSha256: "0".repeat(64) }, outputDir: invalidToolsDir }, diagnostic),
    /frozen identity verifier tools changed/);
    await assert.rejects(read(diagnostic, { config: {}, decodeFile: diagnostic.decodeFile, allowWriterOverrides: true }),
      /without parity qualification/);
    await assert.rejects(read({ ...diagnostic, decodeFile: async () => 99 }), /decoded rows differ from footer/);
    await assert.rejects(read({ ...diagnostic, expectedClientConfig: { workers: 8, streams_per_worker: 2 } }), /readback differs/);
    const startupWriter = evidence.processes[0]!.writer_policy_overrides;
    (startupWriter.defaults as Record<string, unknown>).compression = "zstd";
    await writeResult();
    await assert.rejects(read(), /collector writer readback contains an override/);
    (startupWriter.defaults as Record<string, unknown>).compression = null;
    const admissionWriter = evidence.parity_admission_readback.collector.parquet_writer;
    (admissionWriter.defaults as Record<string, unknown>).compression = "zstd";
    await writeResult();
    await assert.rejects(read(), /pre-admission writer readback contains an override/);
    (admissionWriter.defaults as Record<string, unknown>).compression = null;
    evidence.parity_admission_readback.collector_image_id = `sha256:${"c".repeat(64)}`;
    await writeResult();
    await assert.rejects(read(), /pre-admission collector identity or timing/);
    evidence.parity_admission_readback.collector_image_id = `sha256:${"b".repeat(64)}`;
    await writeResult();
    const wrongCodec = parquet(secondIds, Compression.ZSTD);
    files["second.parquet"] = wrongCodec;
    files["_delta_log/00000000000000000002.json"] = Buffer.from(
      JSON.stringify({ add: { path: "second.parquet", size: wrongCodec.length, dataChange: true } }) + "\n",
    );
    later.parquet_bytes = wrongCodec.length;
    await writeSnapshot();
    await writeStopped();
    await writeResult();
    await assert.rejects(read(), /Parquet codec differs/);
    files["second.parquet"] = second;
    files["_delta_log/00000000000000000002.json"] = Buffer.from(
      JSON.stringify({ add: { path: "second.parquet", size: second.length, dataChange: true } }) + "\n",
    );
    later.parquet_bytes = second.length;
    await writeSnapshot();
    await writeStopped();
    await writeResult();
    cumulative.commits_by_version[1] = { ...receipt, rows: 4 };
    await writeResult();
    await assert.rejects(read(), /conflicting commit receipts/);
    cumulative.commits_by_version[1] = receipt;
    await writeResult();
    clients[0]!.phase_counters.payload_manifest.retry_attempts = 1;
    await writeResult();
    await assert.rejects(read(), /retry_attempts does not reconcile/);
    clients[0]!.phase_counters.payload_manifest.retry_attempts = 0;
    const ranges = clients[0]!.phase_counters.payload_manifest.ranges;
    clients[0]!.phase_counters.payload_manifest.ranges = [];
    await writeResult();
    await assert.rejects(read(), /manifest has no acknowledged ranges/);
    clients[0]!.phase_counters.payload_manifest.ranges = ranges;
    clients[0]!.phase_counters.payload_manifest.ranges[0]!.worker = 1024;
    await writeResult();
    await assert.rejects(read(), /invalid worker/);
    clients[0]!.phase_counters.payload_manifest.ranges[0]!.worker = 0;
    await writeResult();
    const setRows = async (rows: string[]) => {
      first = parquet(rows);
      files["first.parquet"] = first;
      files["_delta_log/00000000000000000001.json"] = Buffer.from(
        JSON.stringify({ add: { path: "first.parquet", size: first.length, dataChange: true } }) + "\n",
      );
      totalRows = rows.length + 2;
      receipt.rows = rows.length;
      receipt.parquet_bytes = first.length;
      cumulative.rows = totalRows;
      clients[0]!.phase_counters.process_cumulative.completed_rows = rows.length;
      const output = evidence.durable_commits.output_throughput;
      output.rows = rows.length;
      output.parquet_bytes = first.length;
      output.rows_per_second = rows.length / 150;
      output.bytes_per_row = first.length / rows.length;
      evidence.durable_commits.complete_30_second_commit_windows[0]!.rows = rows.length;
      evidence.durable_commits.complete_30_second_commit_windows[0]!.parquet_bytes = first.length;
      await writeSnapshot();
      await writeStopped();
      await writeResult();
    };
    await setRows([firstIds[0]!, firstIds[0]!, firstIds[2]!]);
    await assert.rejects(read(), /duplicate delivery row identity/);
    await setRows([firstIds[0]!, firstIds[1]!, `https://x.invalid/v1/${"f".repeat(32)}/${"f".repeat(32)}/0/0/0`]);
    await assert.rejects(read(), /foreign delivery row identity/);
    await setRows(firstIds.slice(0, 2));
    await assert.rejects(read(), /missing delivery row identities/);
    await setRows(firstIds);
    files["_delta_log/00000000000000000001.json"] = Buffer.from(
      JSON.stringify({ add: { path: "first.parquet", size: first.length, dataChange: false } }) + "\n",
    );
    await writeSnapshot();
    await assert.rejects(read(), /rewrite or duplicate Add/);
    files["_delta_log/00000000000000000001.json"] = Buffer.from(
      JSON.stringify({ add: { path: "first.parquet", size: first.length + 1, dataChange: true } }) + "\n",
    );
    await writeSnapshot();
    await assert.rejects(read(), /Parquet file size mismatch/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("accepts matching Realistic evidence and reports client CPU separately", () => {
  const report = reportFor("realistic");
  const evidence = extractClientEvidence(report, "realistic");
  assert.equal(evidence.profile, "realistic");
  assert.equal(evidence.identityEligibleForReconciliation, true);
  assert.equal(evidence.payloadCorpusSha256, "a".repeat(64));
  assert.equal(evidence.cpu.coreEquivalents, 2.5);
  report.phase_counters.payload_manifest.payload_corpus_sha256 = "b".repeat(64);
  assert.throws(() => extractClientEvidence(report, "realistic"), /corpus evidence/);
  report.phase_counters.payload_manifest.payload_corpus_sha256 = "a".repeat(64);
  report.phase_counters.measurement_window.client_resources.cpu_core_equivalents = -1;
  assert.throws(() => extractClientEvidence(report, "realistic"), /CPU evidence/);
});

test("accepts honest Diagnostic evidence without claiming unique delivery", () => {
  const report = reportFor("diagnostic");
  assert.equal(report.phase_counters.payload_manifest.complete, false);
  const evidence = extractClientEvidence(report, "diagnostic");
  assert.equal(evidence.profile, "diagnostic");
  assert.equal(evidence.identityEligibleForReconciliation, false);
  assert.equal(evidence.cpu.coreEquivalents, 2.5);
});

test("rejects profile, hash contract, unavailable, and truncated evidence", () => {
  const mismatchedProfile = reportFor("realistic");
  assert.throws(() => extractClientEvidence(mismatchedProfile, "diagnostic"), /corpus evidence/);

  const invalidEvidence = [
    (report: ReturnType<typeof reportFor>) => { delete report.phase_counters.preparation.payload_corpus_sha256; },
    (report: ReturnType<typeof reportFor>) => { report.phase_counters.payload_manifest.payload_corpus_hash_version = 1; },
    (report: ReturnType<typeof reportFor>) => { report.phase_counters.preparation.payload_corpus_hash_scope = "wrong-scope"; },
    (report: ReturnType<typeof reportFor>) => { report.phase_counters.payload_manifest.truncated = true; },
    (report: ReturnType<typeof reportFor>) => { report.phase_counters.payload_manifest.eligible_for_identity_reconciliation = false; },
  ];
  for (const corrupt of invalidEvidence) {
    const report = reportFor("realistic");
    corrupt(report);
    assert.throws(() => extractClientEvidence(report, "realistic"), /corpus evidence/);
  }
});

test("extends the frozen window with complete stopped collector receipts", () => {
  const measurement: OatMeasurement = {
    startUnixMs: 1000, endUnixMs: 151000, rows: 3, parquetBytes: 60, files: 1, commits: 1, completeBuckets: 5,
    durableRowsPerSecond: 3 / 150, parquetBytesPerRow: 20,
    commitLatencySeconds: { p50: 0.1, p99: 0.1, samples: 1 },
    cpuMillicoresMean: null, memoryBytesMaxObserved: null, rssBytesMaxObserved: null,
    rssUnavailableReason: "unavailable", resourceSampleCount: 0,
    downstreamRowsPerSecond: null, downstreamUnavailableReason: "unavailable",
  };
  const receipt = {
    table_id: "one-id", table_name: "one", delta_version: 2, rows: 3, files: 1,
    input_bytes: 100, parquet_bytes: 60, min_ingested_at_ms: 0, max_ingested_at_ms: 100,
    finished_at_unix_ms: 1000, elapsed_seconds: 0.1,
  };
  const drain = { ...receipt, delta_version: 3, rows: 5, finished_at_unix_ms: 151000 };
  const cumulative = (items: typeof receipt[]) => ({
    scope: "all_new_ingestion_commits_from_this_cell_including_warmup",
    rows: items.reduce((sum, item) => sum + item.rows, 0), commits: items.length, commits_by_version: items,
  });
  const evidence = { durable_commits: { run_cumulative_output: cumulative([receipt]) } };
  const stopped = (items: typeof receipt[]) => ({
    schema_version: 1, run_id: "run", writers: [{
      service: "dataflow-1", was_running: true, oom_killed: false, exit_code: 0,
      finished_at: "2026-01-01T00:00:00Z",
      stopped_commit_output: { source: "complete_stopped_collector_logs", tables: { one: cumulative(items) } },
    }],
  });
  const read = (snapshot: unknown) => extractStoppedReceipts(evidence, snapshot, "run", "one", measurement);
  assert.deepEqual(read(stopped([receipt, drain])).all.map(item => item.version), [2, 3]);
  assert.deepEqual(read(stopped([receipt, drain])).selected.map(item => item.version), [2]);
  assert.throws(() => read(stopped([{ ...receipt, elapsed_seconds: 0.2 }, drain])), /disagree/);
  assert.throws(() => read(stopped([drain])), /receipt/);
  const incomplete = stopped([receipt, drain]);
  incomplete.writers[0]!.stopped_commit_output.source = "incomplete";
  assert.throws(() => read(incomplete), /complete stopped/);
  assert.throws(() => extractStoppedReceipts(evidence, stopped([receipt, drain]), "wrong", "one", measurement), /identity/);
});
