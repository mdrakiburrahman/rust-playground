import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { cp, link, lstat, mkdir, mkdtemp, readdir, rm, symlink, unlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import test from "node:test";
import { checkCadence, compareStopped, evaluateMaintenance, FIXTURE_REVISION, prepareMaintenance, treeHash } from "./maintenance-adapter.js";

test("cadence verifier rejects a missing operation", () => {
  const operations = [...Array.from({ length: 90 }, () => [0, 1, 2, 3, 4]).flat(), 0, 0, 1, 1, 1];
  const trace = `Breakpoint 1: where = fixture\n${operations.map((n) => `     rdx = 0x${n.toString(16).padStart(16, "0")}`).join("\n")}\nProcess 1 exited with status = 0 (0x00000000)\n`;
  checkCadence("idle", trace, 0);
  assert.throws(() => checkCadence("idle", trace.replace("     rdx = 0x0000000000000002\n", ""), 0), /cadence differs/);
});

test("snapshot hashing rejects symbolic and hard links", async () => {
  const root = await mkdtemp(join(tmpdir(), "darwin-links-"));
  try {
    await writeFile(join(root, "data"), "value");
    await symlink("data", join(root, "symlink"));
    await assert.rejects(treeHash(root), /non-regular/);
    await unlink(join(root, "symlink"));
    await link(join(root, "data"), join(root, "hardlink"));
    await assert.rejects(treeHash(root), /hard-linked/);
  } finally {
    await rm(root, { recursive: true });
  }
});

test("real maintenance control and missing protected file", {
  skip: process.env.DARWIN_MAINTENANCE_E2E !== "1",
  timeout: 1_200_000,
}, async () => {
  const repo = resolve(".");
  const output = process.env.DARWIN_EVIDENCE_ROOT ?? join(await mkdtemp(join(tmpdir(), "darwin-control-")), "evidence");
  const prepared = await prepareMaintenance({
    repo,
    baseRevision: FIXTURE_REVISION,
    comparatorSource: join(repo, "crates/arrow-deltalake/examples/darwin_maintenance_verify.rs"),
    sandboxSource: join(repo, "tools/darwin/src/maintenance-sandbox.rs"),
    output,
  });
  assert.equal(prepared.bindings.baselineBinarySha256, prepared.bindings.candidateBinarySha256);
  const sandboxProbe = join(prepared.root, "sandbox-probe");
  await mkdir(sandboxProbe);
  const probeEnv = { HOME: sandboxProbe, PATH: "/usr/bin:/bin", LANG: "C.UTF-8" };
  const caps = spawnSync("/usr/bin/unshare", ["-Urnmpf", "--mount-proc", prepared.sandboxBinary,
    "/usr/bin/cat", "/proc/self/status", sandboxProbe], { encoding: "utf8", env: probeEnv });
  assert.match(caps.stdout, /^CapEff:\s+0000000000000000$/m);
  assert.match(caps.stdout, /^CapPrm:\s+0000000000000000$/m);
  const forbidden = join(prepared.root, "forbidden-write");
  const touch = spawnSync("/usr/bin/unshare", ["-Urnmpf", "--mount-proc", prepared.sandboxBinary,
    "/usr/bin/touch", forbidden, sandboxProbe], { encoding: "utf8", env: probeEnv });
  assert.notEqual(touch.status, 0);
  await assert.rejects(lstat(forbidden), { code: "ENOENT" });
  console.log("Sandbox probe: no effective/permitted capabilities; external write denied");
  for (const lane of ["active", "idle"] as const) {
    const result = await evaluateMaintenance(prepared, lane) as {
      lane: string; scope: string; accepted: boolean; pairs: unknown[]; audits: unknown[];
    };
    assert.equal(result.lane, `maintenance-${lane}`);
    assert.equal(result.scope, "maintenance-only");
    assert.equal(result.accepted, false);
    assert.equal(result.pairs.length, 3);
    assert.equal(result.audits.length, 2);
  }
  const bad = join(prepared.root, "negative-missing-protected");
  await cp(join(prepared.root, "active-1-baseline"), bad, { recursive: true, force: false, errorOnExist: true });
  const protectedDirectory = join(bad, "active", "__arrowcol_ingestion=2024-01-01-00-02");
  const names = (await readdir(protectedDirectory)).filter((name) => name.endsWith(".parquet"));
  assert.ok(names.length >= 1);
  await unlink(join(protectedDirectory, names[0]));
  const failure: unknown = await compareStopped(prepared, "active", bad).then(() => null, (error: unknown) => error);
  assert.ok(failure instanceof Error);
  assert.match(failure.message, /No such file or directory/);
  await writeFile(join(prepared.root, "negative-missing-protected-error.txt"), `${failure.message}\n`, { flag: "wx" });
  console.log(`Maintenance control evidence: ${prepared.root}`);
});
