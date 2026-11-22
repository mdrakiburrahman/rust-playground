import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { join } from "node:path";
import { tableFromIPC } from "apache-arrow";
import { Compression, readParquet } from "parquet-wasm";
import { reconcileStoppedSnapshot } from "./delta-reconcile.js";
import { adaptOatEvidence, extractTimedReceipts, type OatMeasurement } from "./metrics-adapter.js";

type JsonObject = Record<string, unknown>;

function object(value: unknown, label: string): JsonObject {
  if (typeof value !== "object" || value === null || Array.isArray(value)) throw new Error(`${label} must be an object`);
  return value as JsonObject;
}

function imageId(value: unknown): string {
  if (typeof value !== "string" || !/^sha256:[a-f0-9]{64}$/.test(value)) throw new Error("local image identity is unavailable");
  return value;
}

export async function readReconciledOatResult(
  resultsPath: string, snapshotRoot: string, table: string, sourceRevision: string, compressionLevel: number,
): Promise<{ measurement: OatMeasurement; images: string[]; activeRows: number; snapshotVersion: number; cellRunId: string }> {
  const bytes = await readFile(resultsPath);
  if (!bytes.length || bytes.length > 16 * 1024 * 1024) throw new Error("OAT result is empty or too large");
  const arrow = tableFromIPC(readParquet(bytes).intoIPCStream());
  if (arrow.numRows !== 1) throw new Error("expected exactly one OAT result row");
  const arrowRow = arrow.toArray()[0];
  const row = Object.fromEntries(arrow.schema.fields.map(field => [field.name, arrowRow[field.name]]));
  if (row.schema_version !== 3 || row.mode !== "local" || row.table !== table || row.status !== "succeeded"
    || row.git_sha !== sourceRevision || row.pinning_ok !== true || Number(row.replicas) !== 1
    || typeof row.run_id !== "string" || !/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(row.run_id)
    || typeof row.evidence_json !== "string") {
    throw new Error("OAT result identity, source, pinning, or status is invalid");
  }
  const evidence = object(JSON.parse(row.evidence_json), "OAT evidence");
  if (evidence.version !== 1 || !Array.isArray(evidence.processes)) throw new Error("OAT evidence version or processes are invalid");
  const processes = evidence.processes.map(process => object(process, "OAT process"));
  const collectors = processes.filter(process => process.component === "collector");
  if (collectors.length !== 1) throw new Error("expected exactly one collector");
  const collector = collectors[0]!;
  const effective = object(collector.effective_config, "collector effective config");
  const capability = object(effective.commit_telemetry, "collector commit capability");
  const policy = object(object(collector.writer_policy_overrides, "writer policy").defaults, "writer defaults");
  if (collector.log_complete !== true || collector.effective_config_source !== "consumer_startup_readback"
    || capability.schema_version !== 1 || capability.scope !== "new_ingestion_addfiles_after_sync"
    || capability.clock !== "system_unix_ms" || policy.compression !== "zstd" || policy.compression_level !== compressionLevel) {
    throw new Error("collector capability or configured compression does not match the run");
  }
  const images = processes.map(process => {
    if (typeof process.component !== "string" || process.log_complete !== true) throw new Error("incomplete OAT process evidence");
    return `${process.component}:${imageId(process.image_id)}`;
  });
  if (!Array.isArray(evidence.client_phase_reports) || evidence.client_phase_reports.length !== 1) {
    throw new Error("expected one client image receipt");
  }
  const client = object(object(evidence.client_phase_reports[0], "client report").execution_image, "client image");
  if (client.source !== "docker_container_inspect") throw new Error("client image provenance is unavailable");
  images.push(`client:${imageId(client.image_id)}`);
  images.sort();
  const measurement = adaptOatEvidence(evidence);
  const canonicalTable = `telemetry.sqlserver.${table}`;
  const receipts = extractTimedReceipts(evidence, canonicalTable, measurement);
  const expectedTableId = createHash("sha256").update(`file:///delta/dataflow/telemetry/sqlserver/${table}/`).digest("hex");
  const proof = await reconcileStoppedSnapshot(
    join(snapshotRoot, row.run_id), row.run_id, table, canonicalTable, expectedTableId, measurement, receipts, Compression.ZSTD,
  );
  return { measurement, images, activeRows: proof.activeRows, snapshotVersion: proof.latestVersion, cellRunId: row.run_id };
}
