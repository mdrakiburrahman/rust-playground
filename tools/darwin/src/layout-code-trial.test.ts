import assert from "node:assert/strict";
import { test } from "node:test";
import { execFileSync } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";
import { LayoutCodeTrial, assertCollectorShutdown, assertSettledDelivery, comparableRunConfig, compareLayoutReadbacks, compareOatImages, inspectLayoutContainers, scopedProject } from "./layout-code-trial.js";

const baseline = `sha256:${"a".repeat(64)}`;
const candidate = `sha256:${"b".repeat(64)}`;
const router = `sha256:${"c".repeat(64)}`;
const catalog = `sha256:${"d".repeat(64)}`;
const client = `sha256:${"e".repeat(64)}`;
const project = "oat-a";
function container(service: string, image: string, configuredImage = image) {
  return { Id: (service === "dataflow-1" ? "1" : service === "router" ? "2" : "3").repeat(64), Config: { Image: configuredImage, Labels: { "com.docker.compose.project": project, "com.docker.compose.service": service } },
    Image: image, State: { Running: true }, RestartCount: 0, HostConfig: { Memory: 4096 } };
}
function containers(image: string) {
  return [container("dataflow-1", image), container("router", router), container("arrowcol-catalog", catalog)];
}
function evidence(image: string) {
  return { processes: [
    { component: "collector", image, image_id: image, log_complete: true, effective_config_source: "consumer_startup_readback", effective_config: { layout: true }, writer_policy_overrides: {}, layout_table_policies: [
      { table_id: "f".repeat(64), table_name: "telemetry.sqlserver.sqlserver_active_sessions", scope: "table_effective_at_file_construction_not_full_run_history", config: { enabled: true, sort_columns: ["session_id"] } },
    ] },
    { component: "router", image_id: router, log_complete: true, effective_config: {}, writer_policy_overrides: null },
  ], client_phase_reports: [{ execution_image: { source: "docker_container_inspect", image_id: client } }] };
}
function config(root: string) {
  return { platform: { kind: "local", compose_state_root: `${root}/state`, local: { collector_memory_limit_bytes: 4096, snapshot_root: `${root}/snapshots` } },
    replica_counts: [1], tables: ["sqlserver_active_sessions"], warmup_seconds: 60, measure_seconds: 150,
    result_url: pathToFileURL(`${root}/results.parquet`).href, website_url: pathToFileURL(`${root}/site/results.parquet`).href };
}

test("requires the live collector image ID, exact immutable reference, role and memory cap", () => {
  assert.equal(inspectLayoutContainers(containers(baseline), project, baseline, [baseline, candidate], 4096).length, 3);
  for (const bad of [
    [container("dataflow-1", candidate), ...containers(baseline).slice(1)],
    [container("dataflow-1", baseline, "collector:dev"), ...containers(baseline).slice(1)],
    [container("dataflow-1", baseline), container("router", candidate), container("arrowcol-catalog", catalog)],
    [container("dataflow-1", baseline), container("router", router), container("router", router)],
    [container("not-collector", baseline), ...containers(baseline).slice(1)],
  ]) assert.throws(() => inspectLayoutContainers(bad, project, baseline, [baseline, candidate], 4096));
  const wrongCap = containers(baseline); wrongCap[0].HostConfig.Memory = 8192;
  assert.throws(() => inspectLayoutContainers(wrongCap, project, baseline, [baseline, candidate], 4096));
  const wrongProject = containers(baseline); wrongProject[0].Config.Labels["com.docker.compose.project"] = "other";
  assert.throws(() => inspectLayoutContainers(wrongProject, project, baseline, [baseline, candidate], 4096));
  const restarted = containers(baseline); restarted[0].RestartCount = 1;
  assert.throws(() => inspectLayoutContainers(restarted, project, baseline, [baseline, candidate], 4096));
});

test("requires matching enabled effective table layout and nonempty sort keys", () => {
  compareLayoutReadbacks(evidence(baseline), evidence(candidate));
  for (const mutate of [
    (e: ReturnType<typeof evidence>) => { e.processes[0].layout_table_policies![0].config.enabled = false; },
    (e: ReturnType<typeof evidence>) => { e.processes[0].layout_table_policies![0].config.sort_columns = []; },
    (e: ReturnType<typeof evidence>) => { e.processes[0].layout_table_policies![0].config.sort_columns = ["other_key"]; },
    (e: ReturnType<typeof evidence>) => { e.processes[0].layout_table_policies![0].table_name = "other"; },
    (e: ReturnType<typeof evidence>) => { e.processes[0].layout_table_policies = []; },
  ]) { const bad = evidence(candidate); mutate(bad); assert.throws(() => compareLayoutReadbacks(evidence(baseline), bad)); }
});

test("binds OAT collector, other roles and client to their observed immutable image IDs", () => {
  const br = inspectLayoutContainers(containers(baseline), project, baseline, [baseline, candidate], 4096);
  const cr = inspectLayoutContainers(containers(candidate), project, candidate, [baseline, candidate], 4096);
  const images = { baseline, candidate };
  compareOatImages(evidence(baseline), evidence(candidate), images, br, cr);
  const wrong = evidence(candidate); wrong.processes[0].image_id = baseline;
  assert.throws(() => compareOatImages(evidence(baseline), wrong, images, br, cr));
  const tag = evidence(candidate); tag.processes[0].image = "collector:dev";
  assert.throws(() => compareOatImages(evidence(baseline), tag, images, br, cr));
  const clientAlias = evidence(candidate); clientAlias.client_phase_reports[0].execution_image.image_id = baseline;
  assert.throws(() => compareOatImages(evidence(baseline), clientAlias, images, br, cr));
  const otherClient = evidence(candidate); otherClient.client_phase_reports[0].execution_image.image_id = router;
  assert.throws(() => compareOatImages(evidence(baseline), otherClient, images, br, cr));
  const changedRouter = evidence(candidate); changedRouter.processes[1].effective_config = { layout: true };
  assert.throws(() => compareOatImages(evidence(baseline), changedRouter, images, br, cr));
});

test("permits only run-owned paths to differ in otherwise identical local settings", () => {
  assert.deepEqual(comparableRunConfig(config("/tmp/a"), "/tmp/a"), comparableRunConfig(config("/tmp/b"), "/tmp/b"));
  const invalid = config("/tmp/b"); invalid.platform.compose_state_root = "/tmp/a/state";
  assert.throws(() => comparableRunConfig(invalid, "/tmp/b"));
  const unsafe = config("/tmp/b"); unsafe.replica_counts = [2];
  assert.throws(() => comparableRunConfig(unsafe, "/tmp/b"));
  const altered = config("/tmp/b"); altered.measure_seconds = 151;
  assert.notDeepEqual(comparableRunConfig(config("/tmp/a"), "/tmp/a"), comparableRunConfig(altered, "/tmp/b"));
});

test("refuses a source revision other than the pinned baseline before creating snapshots", async () => {
  const root = await mkdtemp(join(tmpdir(), "layout-source-"));
  try {
    const repo = join(root, "repo");
    execFileSync("git", ["init", "-q", repo]);
    await writeFile(join(repo, "file"), "not the baseline");
    execFileSync("git", ["-C", repo, "add", "file"]);
    execFileSync("git", ["-C", repo, "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "not baseline"]);
    await assert.rejects(LayoutCodeTrial.prepare(repo, join(root, "home")), /exact pinned baseline/);
  } finally { await rm(root, { recursive: true, force: true }); }
});

test("requires observed drain of the captured collector and binds the cell project", () => {
  const containerId = "1".repeat(64);
  const shutdown = { shutdown_drain: { schema_version: 1, collectors: [{ container_id: containerId, drain: { status: "observed", sigterm_sent: true, exit_code: 0, log_complete: true } }] } };
  assertCollectorShutdown(shutdown, containerId);
  assert.throws(() => assertCollectorShutdown(shutdown, "2".repeat(64)));
  const bad = structuredClone(shutdown); bad.shutdown_drain.collectors[0].drain.status = "unavailable";
  assert.throws(() => assertCollectorShutdown(bad, containerId));
  assert.equal(scopedProject("arrowcol-oat-tuned", "01234567-89ab-cdef-0123-456789abcdef"), "arrowcol-oat-tu-0123456789abcdef0123456789abcdef");
});

test("refuses unsettled and inconsistent client acknowledgment evidence", () => {
  const data = { reconciliation: { status: "within_bound", settled: true, scope: "process_cumulative_client_rows_vs_this_cells_new_ingestion_commits_including_warmup", completed_rows: 10, ambiguous_rows: 2, delta_rows: 11 },
    durable_commits: { run_cumulative_output: { rows: 11, settled: true } }, client_phase_reports: [{ phase_counters: { process_cumulative: { completed_rows: 10, ambiguous_rows: 2 } } }] };
  assertSettledDelivery(data);
  for (const change of [
    (v: typeof data) => { v.durable_commits.run_cumulative_output.settled = false; },
    (v: typeof data) => { v.reconciliation.status = "unsettled"; },
    (v: typeof data) => { v.durable_commits.run_cumulative_output.rows = 13; v.reconciliation.delta_rows = 13; },
    (v: typeof data) => { v.client_phase_reports[0].phase_counters.process_cumulative.completed_rows = 9; },
  ]) { const bad = structuredClone(data); change(bad); assert.throws(() => assertSettledDelivery(bad)); }
});
