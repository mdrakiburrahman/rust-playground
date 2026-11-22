import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { readFile, readdir, writeFile } from "node:fs/promises";
import { join } from "node:path";

export interface IdentityTools {
  node: string;
  nodeSha256: string;
  tsxRoot: string;
  tsxSha256: string;
  decoder: string;
  decoderSha256: string;
  extractor: string;
  extractorSha256: string;
  verifier: string;
  verifierArtifactSha256: string;
  runtime: string;
  env: NodeJS.ProcessEnv;
}

async function hash(path: string): Promise<string> {
  return createHash("sha256").update(await readFile(path)).digest("hex");
}

export async function hashTsx(root: string): Promise<string> {
  const hash = createHash("sha256").update("arrowcol.tsx-package.v1\0");
  async function visit(relative: string): Promise<void> {
    const entries = await readdir(join(root, relative), { withFileTypes: true });
    for (const entry of entries.sort((a, b) => a.name.localeCompare(b.name))) {
      const path = join(relative, entry.name);
      if (entry.isDirectory()) await visit(path);
      else if (entry.isFile()) {
        const bytes = await readFile(join(root, path));
        hash.update(`${path}\0${bytes.length}\0`).update(bytes);
      } else throw new Error("tsx package contains a non-file");
    }
  }
  await visit("");
  return hash.digest("hex");
}

export async function artifactHash(verifier: string): Promise<string> {
  const hash = createHash("sha256").update("arrowcol.delivery-verifier.artifact.v1\0");
  for (const name of ["delivery-identity.ts", "delivery-identity-cli.ts"]) {
    hash.update(await readFile(join(verifier, "..", name))).update("\0");
  }
  return hash.digest("hex");
}

async function checkTools(tools: IdentityTools): Promise<void> {
  if (await hash(tools.node) !== tools.nodeSha256 || await hashTsx(tools.tsxRoot) !== tools.tsxSha256
    || await hash(tools.decoder) !== tools.decoderSha256 || await hash(tools.extractor) !== tools.extractorSha256
    || await artifactHash(tools.verifier) !== tools.verifierArtifactSha256) {
    throw new Error("frozen identity verifier tools changed");
  }
}

export async function verifyReconciledDelivery(
  tools: IdentityTools, outputDir: string, resultPath: string, expectedResultSha256: string,
  evidenceJson: string, table: string, activePaths: string[],
): Promise<Record<string, unknown>> {
  if (activePaths.length === 0) throw new Error("no reconciled active ingestion files");
  await checkTools(tools);
  const resultSha256 = await hash(resultPath);
  if (resultSha256 !== expectedResultSha256) throw new Error("OAT result changed before identity verification");
  const evidence = Buffer.from(evidenceJson, "utf8");
  const evidenceSha256 = createHash("sha256").update(evidence).digest("hex");
  const manifest = execFileSync(tools.extractor, [], {
    input: evidence, env: tools.env, maxBuffer: 1024 * 1024 + 1, timeout: 60_000,
  });
  const manifestPath = join(outputDir, "identity-manifest.json");
  const evidencePath = join(outputDir, "identity-evidence.json");
  const inputPath = join(outputDir, "identity-snapshot.json");
  await writeFile(evidencePath, evidence, { flag: "wx", mode: 0o400 });
  await writeFile(manifestPath, manifest, { flag: "wx", mode: 0o400 });
  const input = JSON.stringify({ table, rewrite_commit_count: 0, manifest_paths: [manifestPath], active_parquet_paths: activePaths });
  await writeFile(inputPath, input, { flag: "wx", mode: 0o400 });
  const output = execFileSync(tools.node, [
    "--import", "tsx", tools.verifier, "--input", inputPath, "--decoder", tools.decoder,
    "--expected-decoder-sha256", tools.decoderSha256, "--expected-verifier-sha256", tools.verifierArtifactSha256,
  ], { cwd: tools.runtime, env: tools.env, encoding: "utf8", maxBuffer: 1024 * 1024, timeout: 3_600_000 });
  const parsed: unknown = JSON.parse(output);
  if (typeof parsed !== "object" || parsed === null || Array.isArray(parsed)
    || (parsed as Record<string, unknown>).verified !== true
    || (parsed as Record<string, unknown>).scope !== "exact_whole_snapshot_delivery_identity_set_equality"
    || (parsed as Record<string, unknown>).activeFileCount !== activePaths.length
    || (parsed as Record<string, unknown>).snapshotInputSha256 !== createHash("sha256").update(input).digest("hex")) {
    throw new Error("identity verifier did not produce a valid bound verdict");
  }
  if (await hash(resultPath) !== resultSha256 || await hash(evidencePath) !== evidenceSha256
    || await hash(manifestPath) !== createHash("sha256").update(manifest).digest("hex")
    || await hash(inputPath) !== createHash("sha256").update(input).digest("hex")) {
    throw new Error("identity evidence changed during verification");
  }
  await checkTools(tools);
  return {
    ...(parsed as Record<string, unknown>), resultSha256, evidenceSha256,
    extractorSha256: tools.extractorSha256, nodeSha256: tools.nodeSha256, tsxSha256: tools.tsxSha256,
  };
}
