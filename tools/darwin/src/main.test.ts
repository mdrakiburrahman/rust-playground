import assert from "node:assert/strict";
import { execFileSync, spawnSync } from "node:child_process";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { test } from "node:test";
import { snapshot } from "./snapshot.js";

test("scores invalid candidates but propagates replay infrastructure failures", async () => {
  const temporary = await mkdtemp(path.join(os.tmpdir(), "darwin-cli-"));
  const repo = path.join(temporary, "repo");
  const home = path.join(temporary, "home");
  const configPath = path.join(repo, "experiments", "darwin", "candidate.oat.json");
  const config = (level: number) => JSON.stringify({
    tunables: { parquet_writer: { defaults: { compression: "zstd", compression_level: level } } },
  });

  try {
    execFileSync("git", ["init", "-q", repo]);
    await mkdir(path.dirname(configPath), { recursive: true });
    await writeFile(configPath, config(3));
    execFileSync("git", ["-C", repo, "add", "."]);
    execFileSync("git", ["-C", repo, "-c", "user.name=Test", "-c", "user.email=test@example.invalid", "commit", "-qm", "base"]);
    await snapshot(repo, home);
    const main = fileURLToPath(new URL("./main.ts", import.meta.url));
    const invoke = () => spawnSync(process.execPath, [
      "--import", import.meta.resolve("tsx"), main, "evaluate", "--home", home,
      "--experiment", "compression-config", "--smoke", "--bind-host", "127.0.0.1",
      "--endpoint-host", "127.0.0.1", "--controller-stack-bytes", "33554432",
    ], { cwd: repo, encoding: "utf8" });
    await writeFile(configPath, config(999));
    const invalid = invoke();
    assert.equal(invalid.status, 0);
    assert.match(invalid.stdout, /@RESULT .*"valid":0.*"combined_score":0/);
    await writeFile(configPath, config(6));
    const infrastructure = invoke();
    assert.equal(infrastructure.status, 1);
    assert.doesNotMatch(infrastructure.stdout, /@RESULT /);
    assert.match(infrastructure.stderr, /producer\.json/);
  } finally {
    await rm(temporary, { recursive: true, force: true });
  }
});

test("qualification infrastructure failures report null measurement and exit nonzero", async () => {
  const temporary = await mkdtemp(path.join(os.tmpdir(), "darwin-qualification-cli-"));
  try {
    execFileSync("git", ["init", "-q", temporary]);
    const main = fileURLToPath(new URL("./main.ts", import.meta.url));
    const run = spawnSync(process.execPath, ["--import", import.meta.resolve("tsx"), main, "qualify",
      "--home", path.join(temporary, "missing"), "--roles", path.join(temporary, "roles.json"),
      "--bind-host", "127.0.0.1", "--endpoint-host", "127.0.0.1", "--controller-stack-bytes", "33554432",
    ], { cwd: temporary, encoding: "utf8" });
    assert.equal(run.status, 1);
    const result = JSON.parse(run.stdout.replace(/^@RESULT /, ""));
    assert.equal(result.durable_rows_per_second, null);
    assert.ok(result.measurement_unavailable_reason);
    assert.match(run.stderr, /@FEEDBACK /);
  } finally { await rm(temporary, { recursive: true, force: true }); }
});

test("codec smoke cannot be promoted into a series", async () => {
  const temporary = await mkdtemp(path.join(os.tmpdir(), "darwin-codec-cli-"));
  try {
    execFileSync("git", ["init", "-q", temporary]);
    const main = fileURLToPath(new URL("./main.ts", import.meta.url));
    const run = spawnSync(process.execPath, ["--import", import.meta.resolve("tsx"), main, "qualify-codec-smoke",
      "--home", path.join(temporary, "missing"), "--series"], { cwd: temporary, encoding: "utf8" });
    assert.equal(run.status, 1);
    const result = JSON.parse(run.stdout.replace(/^@RESULT /, ""));
    assert.equal(result.experiment, "compression-config");
    assert.equal(result.valid, 0);
    assert.equal(result.accepted, false);
    assert.equal(result.durable_rows_per_second, null);
    assert.match(result.reason, /cannot run a qualification series/);
  } finally { await rm(temporary, { recursive: true, force: true }); }
});
