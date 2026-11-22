import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { chmodSync, mkdirSync, mkdtempSync, rmSync } from "node:fs";
import { readFile } from "node:fs/promises";
import os from "node:os";
import path from "node:path";
import { connect } from "node:net";
import test from "node:test";
import { encodeDispatchRequest } from "./dispatch-contract.js";
import { dispatchArrowColMaintenanceControl, startArrowColCompressionPreflight } from "./production-preflight.js";

const CONFIG_PATH = "experiments/darwin/candidate.oat.json";
const repositoryInput = process.env.DARWIN_PREFLIGHT_TEST_REPO;
const homeInput = process.env.DARWIN_PREFLIGHT_TEST_HOME;

test("maintenance control fails closed without an injected adapter", async () => {
  await assert.rejects(
    dispatchArrowColMaintenanceControl({ repoRoot: "", darwinHome: "" }, {
      mode: "control", experiments: ["maintenance-active"],
    }),
    /adapter is not injected/,
  );
});

type JsonObject = Record<string, unknown>;

function record(value: unknown, label: string): JsonObject {
  if (typeof value !== "object" || value === null || Array.isArray(value)) throw new Error(`${label} must be an object`);
  return value as JsonObject;
}

function git(root: string, args: string[], env: NodeJS.ProcessEnv, input?: Buffer): Buffer {
  const output = execFileSync("git", ["-C", root, ...args], { env, input, maxBuffer: 64 * 1024 * 1024 });
  return Buffer.isBuffer(output) ? output : Buffer.from(output);
}

function gitText(root: string, args: string[], env: NodeJS.ProcessEnv, input?: Buffer): string {
  return git(root, args, env, input).toString("utf8").trimEnd();
}

function gitEnvironment(home: string, index?: string): NodeJS.ProcessEnv {
  return {
    HOME: home,
    PATH: process.env.PATH ?? "/usr/local/bin:/usr/bin:/bin",
    LANG: "C.UTF-8",
    LC_ALL: "C.UTF-8",
    GIT_CONFIG_NOSYSTEM: "1",
    GIT_CONFIG_GLOBAL: "/dev/null",
    ...(index ? { GIT_INDEX_FILE: index } : {}),
    GIT_AUTHOR_NAME: "ArrowCol preflight test",
    GIT_AUTHOR_EMAIL: "preflight@example.invalid",
    GIT_AUTHOR_DATE: "2000-01-01T00:00:00Z",
    GIT_COMMITTER_NAME: "ArrowCol preflight test",
    GIT_COMMITTER_EMAIL: "preflight@example.invalid",
    GIT_COMMITTER_DATE: "2000-01-01T00:00:00Z",
  };
}

function addCommit(
  repoRoot: string,
  base: string,
  files: ReadonlyMap<string, Buffer>,
  ref: string,
  temporaryRoot: string,
): string {
  const index = path.join(temporaryRoot, `${path.basename(ref)}.index`);
  const env = gitEnvironment(temporaryRoot, index);
  gitText(repoRoot, ["read-tree", base], env);
  for (const [file, contents] of files) {
    const blob = gitText(repoRoot, ["hash-object", "-w", "--stdin"], env, contents);
    gitText(repoRoot, ["update-index", "--add", "--cacheinfo", `100644,${blob},${file}`], env);
  }
  const tree = gitText(repoRoot, ["write-tree"], env);
  const commit = gitText(repoRoot, ["commit-tree", tree, "-p", base, "-m", path.basename(ref)], env);
  gitText(repoRoot, ["update-ref", ref, commit], env);
  return commit;
}

function configWith(baseline: Buffer, changes: (defaults: JsonObject) => void): Buffer {
  const root = record(structuredClone(JSON.parse(baseline.toString("utf8")) as unknown), "OAT config");
  const tunables = record(root.tunables, "tunables");
  const writer = record(tunables.parquet_writer, "Parquet writer");
  const defaults = record(writer.defaults, "Parquet defaults");
  changes(defaults);
  return Buffer.from(`${JSON.stringify(root, null, 2)}\n`);
}

async function send(socketPath: string, candidateCommit: string): Promise<unknown> {
  return new Promise((resolve, reject) => {
    const socket = connect(socketPath);
    const chunks: Buffer[] = [];
    socket.setTimeout(120_000);
    socket.on("connect", () => socket.end(encodeDispatchRequest(candidateCommit)));
    socket.on("data", (chunk) => chunks.push(Buffer.from(chunk)));
    socket.on("end", () => {
      try { resolve(JSON.parse(Buffer.concat(chunks).toString("utf8")) as unknown); }
      catch (error) { reject(error); }
    });
    socket.on("timeout", () => socket.destroy(new Error("trusted preflight response timed out")));
    socket.on("error", reject);
  });
}

test("fails closed when the trusted worker receives a model token", async () => {
  const previous = process.env.COPILOT_GITHUB_TOKEN;
  process.env.COPILOT_GITHUB_TOKEN = "test-only-sentinel";
  try { await assert.rejects(startArrowColCompressionPreflight({ socketPath: "", repoRoot: "", darwinHome: "", startOneArm: () => undefined }), /must not receive model token/); }
  finally { if (previous === undefined) delete process.env.COPILOT_GITHUB_TOKEN; else process.env.COPILOT_GITHUB_TOKEN = previous; }
});

test("trusted ArrowCol preflight validates committed configs before an inert callback", {
  skip: !repositoryInput || !homeInput,
}, async () => {
  if (!repositoryInput || !homeInput) throw new Error("preflight test inputs disappeared");
  const repoRoot = await import("node:fs/promises").then(({ realpath }) => realpath(repositoryInput));
  const darwinHome = await import("node:fs/promises").then(({ realpath }) => realpath(homeInput));
  const temporaryRoot = mkdtempSync(path.join(os.tmpdir(), "arrowcol-preflight-test-"));
  const env = gitEnvironment(temporaryRoot);
  const references: string[] = [];
  const referencePrefix = `refs/darwin-preflight-test/${randomUUID()}`;
  let listener: Awaited<ReturnType<typeof startArrowColCompressionPreflight>> | undefined;
  try {
    const manifest = record(JSON.parse(await readFile(path.join(darwinHome, "trusted", "manifest.json"), "utf8")) as unknown, "snapshot manifest");
    const base = typeof manifest.commit === "string" ? manifest.commit : "";
    assert.match(base, /^[0-9a-f]{40}$/);
    const baseline = git(repoRoot, ["show", `${base}:${CONFIG_PATH}`], env);
    const seed = JSON.parse(baseline.toString("utf8")) as unknown;
    const defaults = record(record(record(seed, "config").tunables, "tunables").parquet_writer, "writer").defaults;
    assert.equal(record(defaults, "defaults").compression, "zstd");

    const allowedRef = `${referencePrefix}/allowed`;
    const protectedRef = `${referencePrefix}/protected`;
    const codecRef = `${referencePrefix}/codec`;
    const levelRef = `${referencePrefix}/level`;
    const allowedCommit = addCommit(repoRoot, base, new Map([[CONFIG_PATH, configWith(baseline, (value) => { value.compression_level = 7; })]]), allowedRef, temporaryRoot);
    references.push(allowedRef);
    const baseReadme = git(repoRoot, ["show", `${base}:README.md`], env);
    const protectedCommit = addCommit(repoRoot, base, new Map([
      [CONFIG_PATH, configWith(baseline, (value) => { value.compression_level = 5; })],
      ["README.md", Buffer.concat([baseReadme, Buffer.from("unapproved\n")])],
    ]), protectedRef, temporaryRoot);
    references.push(protectedRef);
    const codecCommit = addCommit(repoRoot, base, new Map([[CONFIG_PATH, configWith(baseline, (value) => { value.compression = "snappy"; })]]), codecRef, temporaryRoot);
    references.push(codecRef);
    const levelCommit = addCommit(repoRoot, base, new Map([[CONFIG_PATH, configWith(baseline, (value) => { value.compression_level = 23; })]]), levelRef, temporaryRoot);
    references.push(levelRef);

    const socketDirectory = path.join(temporaryRoot, "socket");
    mkdirSync(socketDirectory, { mode: 0o700 });
    chmodSync(socketDirectory, 0o700);
    const socketPath = path.join(socketDirectory, "preflight.sock");
    const started: Array<{ lane: string; candidateCommit: string; compressionLevel: number }> = [];
    listener = await startArrowColCompressionPreflight({
      socketPath,
      repoRoot,
      darwinHome,
      startOneArm: (dispatch) => {
        started.push(dispatch);
        return {
          testOnlyInertWorkload: true,
          candidateCommit: dispatch.candidateCommit,
          compressionLevel: dispatch.compressionLevel,
        };
      },
    });

    assert.deepEqual(await send(socketPath, allowedCommit), {
      version: 1,
      ok: true,
      result: { testOnlyInertWorkload: true, candidateCommit: allowedCommit, compressionLevel: 7 },
    });
    for (const rejected of [protectedCommit, codecCommit, levelCommit]) {
      assert.deepEqual(await send(socketPath, rejected), { version: 1, ok: false, error: "request rejected" });
    }
    assert.deepEqual(started, [{ lane: "compression-config", candidateCommit: allowedCommit, compressionLevel: 7 }]);
  } finally {
    if (listener) await listener.close();
    for (const reference of references) gitText(repoRoot, ["update-ref", "-d", reference], env);
    rmSync(temporaryRoot, { recursive: true, force: true });
  }
});
