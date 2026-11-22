import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { appendFile, mkdtemp, readFile, rm, stat, symlink, truncate, writeFile } from "node:fs/promises";
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
import { reconcileStoppedCommitTable } from "./commit-component-reconciler.js";

const pin: AdmittedCommitPin = {
  revision: "81e0f384730c518e85a7dff7f2ec7cc39b0addfb",
  binarySha256: "05a75a0f1082063a4bac08a44667c8802be3e4b74187a8d47a143873149d2598",
  fixtureSha256: "26003fc4ce54e18f19ace9f549a2b9c01ca11e5c67522090b902c09a0ee53a42",
  entrySha256: "f87b78dadd63b0b0560034223be8ce14fe7cddddd9037dee2a19cb4b42848f18",
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
    async reconcileStoppedCell() {
      return {
        timedRows: 1_048_576,
        timedBytes: 36_232_640,
        activeFiles: 65,
        activeRows: 1_064_960,
        activeBytes: 36_798_775,
        deltaVersions: 9,
        cohortSizes: [1, 8, 8, 8, 8, 8, 8, 8, 8],
      };
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
  assert.equal(new Set(seen.map(({ tablePath }) => tablePath)).size, 6);
  assert.equal(result.valid, 0);
  assert.equal(result.combined_score, 0);
  assert.equal(result.accepted, false);
  assert.match(result.reason, /admission and process-tree settlement/);
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
    async reconcileStoppedCell() {
      assert.fail("must not reconcile before process-tree settlement");
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
  await assert.rejects(evaluateCommitComponent({
    ...valid,
    async reconcileStoppedCell(tablePath) {
      return { ...await valid.reconcileStoppedCell(tablePath), activeBytes: 1 };
    },
  }), /physical activeBytes/);
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
test("known pinned fixture and external oracle A-A", { skip: !runAa, timeout: 1_200_000 }, async () => {
  const repo = process.cwd();
  const binary = path.join(repo, "target/release/examples/darwin_commit_component");
  const oracle = path.join(repo, "target/release/delta-inspect");
  const fixture = path.join(repo, "crates/arrow-deltalake/tests/darwin_commit_pipeline.rs");
  const entry = path.join(repo, "crates/arrow-deltalake/examples/darwin_commit_component.rs");
  const digest = async (file: string) => createHash("sha256").update(await readFile(file)).digest("hex");
  const oracleSha256 = "a0e35d7b3abf55404e4181950984ab57dee009b0c91b4b7f49b4268afd18f973";
  assert.equal(await digest(binary), pin.binarySha256);
  assert.equal(await digest(oracle), oracleSha256);
  assert.equal(await digest(fixture), pin.fixtureSha256);
  assert.equal(await digest(entry), pin.entrySha256);
  const root = await scratch();
  const exec = promisify(execFile);
  await exec("git", ["merge-base", "--is-ancestor", pin.revision, "HEAD"], { cwd: repo });
  const host = fakeHost(root, async ({ resultPath, tablePath }) => {
    assert.equal(await digest(binary), pin.binarySha256);
    const execution = exec("/usr/bin/setsid", [binary, "--result", resultPath, "--table-root", tablePath], {
      env: { PATH: "/usr/bin:/bin", HOME: root, TMPDIR: root },
      timeout: 90_000,
      maxBuffer: 64 * 1024,
    });
    const pid = execution.child.pid;
    assert.ok(pid);
    await execution;
    try {
      process.kill(-pid, 0);
    } catch (error) {
      if (error instanceof Error && "code" in error && error.code === "ESRCH") return;
      throw error;
    }
    assert.fail("fixture process group did not settle");
  });
  let exercisedTampering = false;
  host.reconcileStoppedCell = async (tablePath) => {
    assert.equal(await digest(oracle), oracleSha256);
    const { stdout } = await exec(oracle, ["summarize", path.dirname(tablePath), "--table", "table"], {
      env: { PATH: "/usr/bin:/bin", HOME: root, TMPDIR: root },
      timeout: 240_000,
      maxBuffer: 512 * 1024 * 1024,
    });
    const snapshot = JSON.parse(stdout) as {
      tables: { table: { files: Array<{ path: string }>; row_hashes: Record<string, number> } };
    };
    const summary = await reconcileStoppedCommitTable(tablePath, snapshot);
    if (!exercisedTampering) {
      const log = path.join(tablePath, "_delta_log", "00000000000000000001.json");
      const original = await readFile(log, "utf8");
      const changed = original.replace(/("appId"\s*:\s*")continuous-0000(")/, "$1continuous-9999$2");
      assert.notEqual(changed, original);
      try {
        await writeFile(log, changed);
        await assert.rejects(reconcileStoppedCommitTable(tablePath, snapshot), /warmup cohort|transaction identity/);
      } finally {
        await writeFile(log, original);
      }
      const parquet = path.join(tablePath, snapshot.tables.table.files[0]!.path);
      const size = (await stat(parquet)).size;
      try {
        await appendFile(parquet, Buffer.from([0]));
        await assert.rejects(reconcileStoppedCommitTable(tablePath, snapshot), /physical file size/);
      } finally {
        await truncate(parquet, size);
      }
      const hashes = snapshot.tables.table.row_hashes;
      const key = Object.keys(hashes)[0]!;
      const value = hashes[key]!;
      delete hashes[key];
      try {
        await assert.rejects(reconcileStoppedCommitTable(tablePath, snapshot), /distinct row count/);
      } finally {
        hashes[key] = value;
      }
      exercisedTampering = true;
      console.log("COMMIT_NEGATIVE rejected altered transaction, physical bytes and missing row");
    }
    return summary;
  };
  const result = await evaluateCommitComponent(host);
  assert.equal(result.valid, 0);
  assert.equal(result.combined_score, 0);
  assert.equal(exercisedTampering, true);
  assert.equal(result.calibration?.wallSeconds.control.length, 3);
  assert.equal(result.calibration?.wallSeconds.candidate.length, 3);
  console.log(`COMMIT_AA ${JSON.stringify(result)}`);
});
