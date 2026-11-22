import assert from "node:assert/strict";
import { cp, mkdtemp, readdir, unlink } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join, resolve } from "node:path";
import test from "node:test";
import { checkCadence, compareStopped, evaluateMaintenance, FIXTURE_REVISION, prepareMaintenance } from "./maintenance-adapter.js";

test("cadence verifier rejects a missing operation", () => {
  const operations = [...Array.from({ length: 90 }, () => [0, 1, 2, 3, 4]).flat(), 0, 0, 1, 1, 1];
  const trace = `Breakpoint 1: where = fixture\n${operations.map((n) => `     rdx = 0x${n.toString(16).padStart(16, "0")}`).join("\n")}\nProcess 1 exited with status = 0 (0x00000000)\n`;
  checkCadence("idle", trace, 0);
  assert.throws(() => checkCadence("idle", trace.replace("     rdx = 0x0000000000000002\n", ""), 0), /cadence differs/);
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
  await assert.rejects(compareStopped(prepared, "active", bad));
  console.log(`Maintenance control evidence: ${prepared.root}`);
});
