import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtemp, readFile, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";
import { promisify } from "node:util";
import {
  evaluateCommitComponent,
  type AdmittedCommitPin,
  type CommitCellRequest,
  type CommitComponentHost,
} from "./commit-component-adapter.js";

const pin: AdmittedCommitPin = {
  revision: "6fa4c07731921cfa7f52e6419bad08da3dab0801",
  binarySha256: "8a61cb097391091640df3d6e6f89d34d37b32512af9ce4780eb0df46780f14e2",
  fixtureSha256: "f3806825344e082b9af27606eaa901a28fc08863069d2d04331e6b016d8c9ccb",
  entrySha256: "bb33de6dbcdfe6c6ffde2834fb44b41b3905fb988f0893aca478d65f4df73e06",
};

function completeResult(): Record<string, unknown> {
  return {
    schema: "arrowcol-commit-component-v1",
    scope: "local_component_not_dfe_ack_or_azure",
    storage: "local_filesystem",
    workload: "public_synthetic_id_payload_v1",
    timing_boundary: "generate_input_through_nonforced_durable_receipts",
    batch_window_ms: 2000,
    max_files_per_commit: 128,
    max_pending_files: 512,
    max_in_flight_groups: 8,
    max_in_flight_input_bytes: 16 * 1024 * 1024,
    peak_in_flight_groups: 8,
    peak_staging_groups: 8,
    peak_submitted_unconsumed_receipts: 8,
    peak_in_flight_input_bytes: 5_769_408,
    timed_groups: 64,
    timed_receipts: 64,
    timed_durable_rows: 1_048_576,
    timed_physical_bytes: 36_232_640,
    active_files_including_warmup: 65,
    active_rows_including_warmup: 1_064_960,
    active_physical_bytes_including_warmup: 36_798_775,
    delta_versions: 9,
    cohort_sizes: [1, 8, 8, 8, 8, 8, 8, 8, 8],
    process_cpu_seconds: 1,
    stage_wall_sum_seconds: 2,
    preparation_build_sum_seconds: 0.4,
    preparation_sort_sum_seconds: 0,
    encode_sum_seconds: 0.8,
    submit_sum_seconds: 0.01,
    receipt_wait_sum_seconds: 130,
    commit_service_sum_seconds: 0.2,
    wall_seconds: 16.5,
    upload_seconds: null,
    replay: "deduplicated",
    verification: "stopped_writer_delta_log_kernel_snapshot_and_parquet",
  };
}

async function scratch(): Promise<string> {
  return mkdtemp(path.join(tmpdir(), "commit-component-test-"));
}

function fakeHost(
  root: string,
  run: (request: CommitCellRequest) => Promise<void>,
  candidate: AdmittedCommitPin = pin,
): CommitComponentHost {
  return {
    scratchRoot: root,
    control: pin,
    candidate,
    async runAdmittedCell(request) {
      await run(request);
      return { ...(request.arm === "control" ? pin : candidate), exitCode: 0, settled: true };
    },
  };
}

test("three alternating fresh A-A cells remain unscored", async (t) => {
  const root = await scratch();
  t.after(() => rm(root, { recursive: true, force: true }));
  const seen: CommitCellRequest[] = [];
  const host = fakeHost(root, async (request) => {
    seen.push(request);
    await writeFile(request.resultPath, JSON.stringify(completeResult()), { flag: "wx" });
  });
  const result = await evaluateCommitComponent(host);
  assert.deepEqual(seen.map(({ arm, pair }) => `${arm}${pair}`), [
    "control1", "candidate1", "candidate2", "control2", "control3", "candidate3",
  ]);
  assert.equal(new Set(seen.map(({ resultPath }) => resultPath)).size, 6);
  assert.equal(result.valid, 0);
  assert.equal(result.combined_score, 0);
  assert.equal(result.accepted, false);
  assert.match(result.reason, /independent stopped Delta and Parquet/);
  assert.match(result.feedback, /2s commit window/);
  assert.equal(result.calibration?.geometricMean, 1);
});

test("refuses failed, unsettled or mismatched executions and incomplete results", async (t) => {
  const root = await scratch();
  t.after(() => rm(root, { recursive: true, force: true }));
  const valid = fakeHost(root, async ({ resultPath }) => {
    await writeFile(resultPath, JSON.stringify(completeResult()), { flag: "wx" });
  });
  await assert.rejects(evaluateCommitComponent({
    ...valid,
    async runAdmittedCell(request) {
      const receipt = await valid.runAdmittedCell(request);
      return { ...receipt, exitCode: 1 };
    },
  }), /exit zero and settle/);
  await assert.rejects(evaluateCommitComponent({
    ...valid,
    async runAdmittedCell(request) {
      const receipt = await valid.runAdmittedCell(request);
      return { ...receipt, settled: false };
    },
  }), /exit zero and settle/);
  await assert.rejects(evaluateCommitComponent({
    ...valid,
    async runAdmittedCell(request) {
      const receipt = await valid.runAdmittedCell(request);
      return { ...receipt, binarySha256: "f".repeat(64) };
    },
  }), /admitted pin/);
  const incomplete = fakeHost(root, async ({ resultPath }) => {
    const result = completeResult();
    delete result.timed_physical_bytes;
    await writeFile(resultPath, JSON.stringify(result), { flag: "wx" });
  });
  await assert.rejects(evaluateCommitComponent(incomplete), /timed physical bytes/);
  const missing = fakeHost(root, async () => {});
  await assert.rejects(evaluateCommitComponent(missing), /ENOENT/);
  const oversized = fakeHost(root, async ({ resultPath }) => {
    await writeFile(resultPath, "x".repeat(64 * 1024 + 1), { flag: "wx" });
  });
  await assert.rejects(evaluateCommitComponent(oversized), /bounded regular file/);
});

test("rejects foreign fixture pins and symlink results", async (t) => {
  const root = await scratch();
  t.after(() => rm(root, { recursive: true, force: true }));
  let calls = 0;
  const different = { ...pin, fixtureSha256: "e".repeat(64) };
  await assert.rejects(evaluateCommitComponent(fakeHost(root, async () => { calls++; }, different)), /same protected fixture/);
  assert.equal(calls, 0);
  const source = path.join(root, "untrusted.json");
  await writeFile(source, JSON.stringify(completeResult()));
  await assert.rejects(evaluateCommitComponent(fakeHost(root, async ({ resultPath }) => {
    await symlink(source, resultPath);
  })), /ELOOP/);
});

test("different candidate never receives an A-A ratio or a score", async (t) => {
  const root = await scratch();
  t.after(() => rm(root, { recursive: true, force: true }));
  const candidate = { ...pin, revision: "b".repeat(40), binarySha256: "c".repeat(64) };
  const result = await evaluateCommitComponent(fakeHost(root, async ({ resultPath }) => {
    await writeFile(resultPath, JSON.stringify(completeResult()), { flag: "wx" });
  }, candidate));
  assert.equal(result.calibration, null);
  assert.equal(result.valid, 0);
  assert.equal(result.combined_score, 0);
});

const runAa = process.env.DARWIN_COMMIT_AA === "1";
test("known pinned fixture release binary A-A", { skip: !runAa, timeout: 240_000 }, async () => {
  const repo = process.cwd();
  const binary = path.join(repo, "target/release/examples/darwin_commit_component");
  const fixture = path.join(repo, "crates/arrow-deltalake/tests/darwin_commit_pipeline.rs");
  const entry = path.join(repo, "crates/arrow-deltalake/examples/darwin_commit_component.rs");
  const digest = async (file: string) => createHash("sha256").update(await readFile(file)).digest("hex");
  assert.equal(await digest(binary), pin.binarySha256);
  assert.equal(await digest(fixture), pin.fixtureSha256);
  assert.equal(await digest(entry), pin.entrySha256);
  const root = await scratch();
  const exec = promisify(execFile);
  const host = fakeHost(root, async ({ resultPath }) => {
    assert.equal(await digest(binary), pin.binarySha256);
    await exec(binary, ["--result", resultPath], {
      env: { PATH: "/usr/bin:/bin", HOME: root, TMPDIR: root },
      timeout: 90_000,
      maxBuffer: 64 * 1024,
    });
  });
  const result = await evaluateCommitComponent(host);
  assert.equal(result.valid, 0);
  assert.equal(result.combined_score, 0);
  assert.equal(result.calibration?.wallSeconds.control.length, 3);
  assert.equal(result.calibration?.wallSeconds.candidate.length, 3);
  console.log(`COMMIT_AA ${JSON.stringify(result)}`);
});
