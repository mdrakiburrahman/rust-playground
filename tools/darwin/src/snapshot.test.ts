import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { test } from "node:test";
import { snapshot } from "./snapshot.js";

test("snapshots committed source once and refuses dirty source or an existing trusted home", async () => {
  const temporary = await mkdtemp(path.join(os.tmpdir(), "darwin-snapshot-"));
  const repo = path.join(temporary, "repo");
  const home = path.join(temporary, "home");
  try {
    execFileSync("git", ["init", "-q", repo]);
    await writeFile(path.join(repo, "fixture"), "committed");
    execFileSync("git", ["-C", repo, "add", "fixture"]);
    execFileSync("git", [
      "-C", repo, "-c", "user.name=Test", "-c", "user.email=test@example.invalid",
      "commit", "-qm", "base",
    ]);
    const manifest = await snapshot(repo, home);
    assert.equal(manifest.commit, execFileSync("git", ["-C", repo, "rev-parse", "HEAD"], { encoding: "utf8" }).trim());
    await writeFile(path.join(repo, "fixture"), "modified");
    assert.equal(await readFile(path.join(home, "trusted", "src", "fixture"), "utf8"), "committed");
    await assert.rejects(() => snapshot(repo, path.join(temporary, "another")), /clean committed/);
    await writeFile(path.join(repo, "fixture"), "committed");
    await assert.rejects(() => snapshot(repo, home), /EEXIST/);
  } finally {
    await rm(temporary, { recursive: true, force: true });
  }
});
