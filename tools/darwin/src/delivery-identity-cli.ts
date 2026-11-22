import { createHash } from "node:crypto";
import { spawn, type ChildProcess } from "node:child_process";
import { readFile, realpath, stat } from "node:fs/promises";
import { resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { verifyDeliveryIdentitySet, type IdentityOptions } from "./delivery-identity.ts";

export interface SnapshotInput {
  table: string;
  rewrite_commit_count: number;
  manifest_paths: string[];
  active_parquet_paths: string[];
}

export interface SnapshotOptions {
  decoderPath: string;
  expectedDecoderSha256: string;
  expectedVerifierSha256: string;
  maxBitmapBytes?: number;
  maxRanges?: number;
  signal?: AbortSignal;
}

async function digestFile(path: string): Promise<string> {
  const { createReadStream } = await import("node:fs");
  const hash = createHash("sha256");
  for await (const chunk of createReadStream(path)) hash.update(chunk);
  return hash.digest("hex");
}

export async function verifierArtifactSha256(): Promise<string> {
  const hash = createHash("sha256").update("arrowcol.delivery-verifier.artifact.v1\0");
  for (const name of ["delivery-identity.ts", "delivery-identity-cli.ts"]) {
    hash.update(await readFile(new URL(name, import.meta.url)));
    hash.update("\0");
  }
  return hash.digest("hex");
}

async function fileIdentity(path: string): Promise<string> {
  const value = await stat(path, { bigint: true });
  if (!value.isFile()) throw new Error("active Parquet path is not a regular file");
  return `${value.dev}:${value.ino}:${value.size}:${value.mtimeNs}:${value.ctimeNs}`;
}

interface Exit { code: number | null; signal: NodeJS.Signals | null; error?: Error }

async function waitBounded(completion: Promise<Exit>, milliseconds: number): Promise<Exit | null> {
  let timer: NodeJS.Timeout | undefined;
  try {
    return await Promise.race([
      completion,
      new Promise<null>(resolveTimeout => { timer = setTimeout(() => resolveTimeout(null), milliseconds); }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

async function stopOwned(child: ChildProcess, completion: Promise<Exit>): Promise<void> {
  if (child.exitCode === null && child.signalCode === null) child.kill("SIGTERM");
  if (await waitBounded(completion, 1000)) return;
  if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
  if (!await waitBounded(completion, 3000)) throw new Error("owned Parquet decoder did not stop/reap within its bound");
}

async function* decodeFile(
  decoder: string, path: string, signal: AbortSignal | undefined, onRows: (rows: number) => void,
): AsyncGenerator<string> {
  signal?.throwIfAborted();
  const child = spawn(decoder, ["--file", path, "--batch-rows", "8192"], { stdio: ["ignore", "pipe", "pipe"] });
  const completion = new Promise<Exit>(done => {
    child.once("error", error => done({ code: null, signal: null, error }));
    child.once("close", (code, childSignal) => done({ code, signal: childSignal }));
  });
  let abortTimer: NodeJS.Timeout | undefined;
  let stderr = "";
  let stderrOverflow = false;
  child.stderr!.on("data", (chunk: Buffer) => {
    if (stderr.length + chunk.length > 32_768) {
      stderrOverflow = true;
      child.kill("SIGTERM");
    } else stderr += chunk.toString("utf8");
  });
  const abort = (): void => {
    clearTimeout(abortTimer);
    if (child.exitCode === null && child.signalCode === null) child.kill("SIGTERM");
    abortTimer = setTimeout(() => {
      if (child.exitCode === null && child.signalCode === null) child.kill("SIGKILL");
    }, 1000);
  };
  signal?.addEventListener("abort", abort, { once: true });
  if (signal?.aborted) abort();
  let partial = "";
  let rows = 0;
  try {
    for await (const chunk of child.stdout!) {
      signal?.throwIfAborted();
      const text = partial + (chunk as Buffer).toString("utf8");
      let offset = 0;
      for (let end = text.indexOf("\n"); end !== -1; end = text.indexOf("\n", offset)) {
        if (end - offset > 160) throw new Error("decoder line exceeds identity protocol bound");
        const identity = text.slice(offset, end);
        offset = end + 1;
        rows++;
        yield identity;
      }
      partial = text.slice(offset);
      if (partial.length > 160) throw new Error("decoder partial line exceeds identity protocol bound");
    }
    if (partial.length !== 0) throw new Error("truncated decoder identity stream");
    const exit = await completion;
    signal?.throwIfAborted();
    if (exit.error) throw exit.error;
    if (exit.code !== 0 || exit.signal !== null || stderrOverflow) throw new Error(`Parquet decoder failed: ${stderr.trim()}`);
    const count = /^DELIVERY_IDENTITY_READER_COMPLETED rows=(0|[1-9][0-9]*)\n$/.exec(stderr);
    if (!count || !Number.isSafeInteger(Number(count[1])) || Number(count[1]) !== rows) {
      throw new Error("Parquet decoder terminal row count does not reconcile");
    }
    onRows(rows);
  } finally {
    signal?.removeEventListener("abort", abort);
    clearTimeout(abortTimer);
    await stopOwned(child, completion);
  }
}

export async function verifyActiveParquetIdentitySet(input: SnapshotInput, options: SnapshotOptions) {
  if (!Array.isArray(input.manifest_paths) || input.manifest_paths.length === 0 || input.manifest_paths.length > 1024
    || !Array.isArray(input.active_parquet_paths)
    || input.active_parquet_paths.length === 0 || input.active_parquet_paths.length > 1_000_000) {
    throw new Error("expected complete manifests and exact nonempty trusted active Parquet paths");
  }
  for (const expected of [options.expectedDecoderSha256, options.expectedVerifierSha256]) {
    if (!/^[a-f0-9]{64}$/.test(expected)) throw new Error("expected frozen decoder and verifier SHA256 bindings");
  }
  options.signal?.throwIfAborted();
  const verifierHash = await verifierArtifactSha256();
  if (verifierHash !== options.expectedVerifierSha256) throw new Error("frozen verifier artifact hash differs");
  const manifests: unknown[] = [];
  const manifestSources: { path: string; identity: string; sha256: string; bytes: number }[] = [];
  let manifestBytes = 0;
  for (const path of input.manifest_paths) {
    if (typeof path !== "string" || path.length === 0) throw new Error("invalid original terminal manifest path");
    const canonical = await realpath(path);
    const identity = await fileIdentity(canonical);
    if ((await stat(canonical)).size > 1_048_576) throw new Error("terminal manifest exceeds producer byte cap");
    const bytes = await readFile(canonical);
    manifestBytes += bytes.length;
    if (bytes.length > 1_048_576 || manifestBytes > 64 * 1024 * 1024) throw new Error("original manifest sources exceed memory bound");
    if (await fileIdentity(canonical) !== identity) throw new Error("terminal manifest source changed while reading");
    manifests.push(JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes)));
    manifestSources.push({ path: canonical, identity, sha256: createHash("sha256").update(bytes).digest("hex"), bytes: bytes.length });
  }
  const decoder = await realpath(options.decoderPath);
  const decoderIdentity = await fileIdentity(decoder);
  const decoderHash = await digestFile(decoder);
  if (decoderHash !== options.expectedDecoderSha256) throw new Error("frozen Parquet decoder executable hash differs");
  const files: { path: string; identity: string; rows: number | null }[] = [];
  const unique = new Set<string>();
  for (const path of input.active_parquet_paths) {
    if (typeof path !== "string" || path.length === 0) throw new Error("invalid trusted active path");
    const canonical = await realpath(path);
    const identity = await fileIdentity(canonical);
    const inode = identity.split(":").slice(0, 2).join(":");
    if (unique.has(inode)) throw new Error("duplicate or aliased active Parquet file");
    unique.add(inode);
    files.push({ path: canonical, identity, rows: null });
  }
  async function* identities(): AsyncGenerator<string> {
    for (const file of files) {
      options.signal?.throwIfAborted();
      if (await fileIdentity(file.path) !== file.identity) throw new Error("active Parquet file changed before decode");
      yield* decodeFile(decoder, file.path, options.signal, rows => { file.rows = rows; });
      if (await fileIdentity(file.path) !== file.identity) throw new Error("active Parquet file changed during decode");
    }
  }
  const identityOptions: IdentityOptions = {
    table: input.table, rewriteCommitCount: input.rewrite_commit_count,
    maxBitmapBytes: options.maxBitmapBytes, maxRanges: options.maxRanges,
  };
  const verdict = await verifyDeliveryIdentitySet(manifests, identities(), identityOptions);
  const fileBindings = createHash("sha256").update("arrowcol.active-delivery-files.v1\0");
  let activeFileRows = 0;
  for (const file of files) {
    if (await fileIdentity(file.path) !== file.identity) throw new Error("stopped active Parquet snapshot changed");
    if (file.rows === null) throw new Error("active file lacks completed decoder accounting");
    activeFileRows += file.rows;
    fileBindings.update(JSON.stringify(file)).update("\n");
  }
  for (const source of manifestSources) {
    if (await fileIdentity(source.path) !== source.identity) throw new Error("original terminal manifest source changed during verification");
  }
  if (await fileIdentity(decoder) !== decoderIdentity || await digestFile(decoder) !== decoderHash
    || await verifierArtifactSha256() !== verifierHash) throw new Error("frozen executable/verifier artifact changed during verification");
  return {
    ...verdict,
    activeFileCount: files.length, activeFileRows, activeFileBindingsSha256: fileBindings.digest("hex"),
    manifestSources,
    decoderSha256: decoderHash, verifierArtifactSha256: verifierHash, nodeVersion: process.version,
    artifactScope: "two TypeScript source files; decoder executable hashed separately; caller pins Node/tsx",
    reconciliationScope: "caller-approved stopped active ingestion Parquet paths; caller supplies reconciled rewrite count; Delta log not read here",
  };
}

async function main(): Promise<void> {
  const { values } = parseArgs({
    allowPositionals: false, strict: true,
    options: {
      input: { type: "string" }, decoder: { type: "string" },
      "expected-decoder-sha256": { type: "string" }, "expected-verifier-sha256": { type: "string" },
      "max-bitmap-bytes": { type: "string" }, "max-ranges": { type: "string" },
      "artifact-hash": { type: "boolean" },
    },
  });
  if (values["artifact-hash"]) {
    if (Object.keys(values).length !== 1) throw new Error("--artifact-hash cannot be combined with verification arguments");
    console.log(await verifierArtifactSha256());
    return;
  }
  if (!values.input || !values.decoder || !values["expected-decoder-sha256"] || !values["expected-verifier-sha256"]) {
    throw new Error("required: --input --decoder --expected-decoder-sha256 --expected-verifier-sha256");
  }
  const controller = new AbortController();
  const abort = (): void => controller.abort(new Error("offline identity verification interrupted"));
  process.once("SIGINT", abort);
  process.once("SIGTERM", abort);
  try {
    if ((await stat(values.input)).size > 64 * 1024 * 1024) throw new Error("snapshot input exceeds 64MiB bound");
    const text = await readFile(values.input);
    if (text.length > 64 * 1024 * 1024) throw new Error("snapshot input exceeds 64MiB bound");
    const parsed: unknown = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(text));
    if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)) throw new Error("snapshot input must be an object");
    const allowed = ["table", "rewrite_commit_count", "manifest_paths", "active_parquet_paths"];
    if (Object.keys(parsed).some(key => !allowed.includes(key))) throw new Error("unsupported snapshot input fields");
    const number = (value: string | undefined): number | undefined => {
      if (value === undefined) return undefined;
      if (!/^[1-9][0-9]*$/.test(value)) throw new Error("memory/range bounds must be canonical positive integers");
      return Number(value);
    };
    const verdict = await verifyActiveParquetIdentitySet(parsed as SnapshotInput, {
      decoderPath: values.decoder, expectedDecoderSha256: values["expected-decoder-sha256"],
      expectedVerifierSha256: values["expected-verifier-sha256"],
      maxBitmapBytes: number(values["max-bitmap-bytes"]), maxRanges: number(values["max-ranges"]),
      signal: controller.signal,
    });
    console.log(JSON.stringify({ ...verdict, snapshotInputSha256: createHash("sha256").update(text).digest("hex") }));
  } finally {
    process.removeListener("SIGINT", abort);
    process.removeListener("SIGTERM", abort);
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch(error => {
    console.error(JSON.stringify({ verified: false, reason: error instanceof Error ? error.message : String(error) }));
    process.exitCode = 1;
  });
}
