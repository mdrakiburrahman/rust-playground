import assert from "node:assert/strict";
import { chmod, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { createOriginalDeadline, readOriginalDeadline } from "./tier2-deadline.js";

test("persists one original 24-hour deadline and rejects reset or binding drift", async () => {
  const root = await mkdtemp(join(tmpdir(), "tier2-deadline-"));
  try {
    const path = join(root, "deadline.json");
    const hash = "a".repeat(64);
    const first = await createOriginalDeadline(path, hash, 1000);
    assert.equal(first.deadlineUnixMs, 86_401_000);
    assert.deepEqual(await readOriginalDeadline(path, hash, 2000), first);
    await assert.rejects(createOriginalDeadline(path, hash, 2000), /EEXIST/);
    await assert.rejects(readOriginalDeadline(path, "b".repeat(64), 2000), /another series/);
    await assert.rejects(readOriginalDeadline(path, hash, 999), /expired|inconsistent/);
    await assert.rejects(readOriginalDeadline(path, hash, first.deadlineUnixMs), /expired/);
    await chmod(path, 0o600);
    await writeFile(path, JSON.stringify({ ...first, deadlineUnixMs: first.deadlineUnixMs + 1 }));
    await assert.rejects(readOriginalDeadline(path, hash, 2000), /inconsistent/);
  } finally { await rm(root, { recursive: true, force: true }); }
});
