import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { mkdir, mkdtemp, rm, symlink, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { test } from "node:test";
import { checkCandidatePaths, type Manifest } from "./paths.js";

const hash = (value: string) => createHash("sha256").update(value).digest("hex");

test("enforces protected files and rejects extras, removals and symlinks", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "darwin-paths-"));
  try {
    execFileSync("git", ["init", "-q", root]);
    await writeFile(path.join(root, "allowed.rs"), "before");
    await writeFile(path.join(root, "protected.ts"), "trusted");
    execFileSync("git", ["-C", root, "add", "allowed.rs", "protected.ts"]);
    const baseline: Manifest = {
      "allowed.rs": { sha256: hash("before"), executable: false },
      "protected.ts": { sha256: hash("trusted"), executable: false },
    };
    await writeFile(path.join(root, "allowed.rs"), "after");
    assert.deepEqual(await checkCandidatePaths(root, baseline, ["allowed.rs"]), []);
    await writeFile(path.join(root, "protected.ts"), "tampered");
    await writeFile(path.join(root, "extra.ts"), "extra");
    assert.deepEqual(await checkCandidatePaths(root, baseline, ["allowed.rs"]), [
      "changed protected file: protected.ts", "extra file: extra.ts",
    ]);
    await rm(path.join(root, "protected.ts"));
    await symlink("allowed.rs", path.join(root, "protected.ts"));
    assert.deepEqual(await checkCandidatePaths(root, baseline, ["allowed.rs"]), [
      "extra file: extra.ts", "not a regular file: protected.ts",
    ]);
    await rm(path.join(root, "protected.ts"));
    assert.deepEqual(await checkCandidatePaths(root, baseline, ["allowed.rs"]), [
      "extra file: extra.ts", "missing file: protected.ts",
    ]);
    await assert.rejects(() => checkCandidatePaths(root, baseline, ["../other"]), /invalid allowed path/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("requires a clean initialized submodule at the pinned revision", async () => {
  const root = await mkdtemp(path.join(os.tmpdir(), "darwin-submodule-"));
  try {
    const submodule = path.join(root, "sub");
    execFileSync("git", ["init", "-q", root]);
    await mkdir(submodule);
    execFileSync("git", ["init", "-q", submodule]);
    await writeFile(path.join(submodule, "file"), "pinned");
    execFileSync("git", ["-C", submodule, "add", "file"]);
    execFileSync("git", [
      "-C", submodule, "-c", "user.name=Test", "-c", "user.email=test@example.invalid",
      "commit", "-qm", "base",
    ]);
    const revision = execFileSync("git", ["-C", submodule, "rev-parse", "HEAD"], { encoding: "utf8" }).trim();
    execFileSync("git", ["-C", root, "-c", "advice.addEmbeddedRepo=false", "add", "sub"]);
    assert.deepEqual(await checkCandidatePaths(root, {}, [], { sub: revision }), []);
    await writeFile(path.join(submodule, "file"), "changed");
    assert.deepEqual(await checkCandidatePaths(root, {}, [], { sub: revision }), [
      "changed or uninitialized submodule: sub",
    ]);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
