import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { hasPinnedSubmodules } from "./replay.js";

test("accepts a pinned submodule and rejects an uninitialized one", async () => {
  const root = await mkdtemp(join(tmpdir(), "darwin-runtime-"));
  try {
    const source = join(root, "source");
    const repo = join(root, "repo");
    execFileSync("git", ["init", "-q", source]);
    await writeFile(join(source, "file"), "pinned");
    execFileSync("git", ["-C", source, "add", "file"]);
    execFileSync("git", ["-C", source, "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "pinned"]);
    execFileSync("git", ["init", "-q", repo]);
    execFileSync("git", ["-C", repo, "-c", "protocol.file.allow=always", "submodule", "add", "-q", source, "sub"]);
    assert.equal(hasPinnedSubmodules(repo, process.env), true);
    execFileSync("git", ["-C", repo, "submodule", "deinit", "-f", "sub"], { stdio: "ignore" });
    assert.equal(hasPinnedSubmodules(repo, process.env), false);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
