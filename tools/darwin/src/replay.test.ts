import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { test } from "node:test";
import { cleanEnv, hasPinnedSubmodules } from "./replay.js";

test("the isolated runtime can execute npx without inheriting credentials", async () => {
  const home = await mkdtemp(join(tmpdir(), "darwin-home-"));
  try {
    const env = cleanEnv(home, 33554432);
    assert.match(execFileSync("npx", ["--version"], { env, encoding: "utf8" }), /^\d+\./);
    assert.equal(env.COPILOT_GITHUB_TOKEN, undefined);
  } finally {
    await rm(home, { recursive: true, force: true });
  }
});

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
