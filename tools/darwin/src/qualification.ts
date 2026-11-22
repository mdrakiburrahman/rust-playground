import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdir, readFile, rmdir, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { inspectParityHost } from "./parity-preflight.js";
import { buildFooterInspector } from "./parity-replay.js";
import { qualificationConfig } from "./qualification-config.js";
import { runObservedArm } from "./qualification-observer.js";
import { cleanEnv, prepareIdentityTools, prepareReplayRuntime, replayCompressionArm, sha256 } from "./replay.js";
import { bindCodeArm, codeCommonConfigHash, type CodeRole } from "./tier2-binding.js";
import { createOriginalDeadline, readOriginalDeadline } from "./tier2-deadline.js";
import { ARM_RUN_BUDGET_MS, CLEANUP_RESERVE_MS, RECONCILIATION_RESERVE_MS, runTier2, type ArmBinding, type ArmRole, type Tier2Arm } from "./tier2.js";
import type { SnapshotManifest } from "./snapshot.js";

type JsonObject = Record<string, unknown>;
function object(value: unknown, label: string): JsonObject {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error(`${label} must be an object`);
  return value as JsonObject;
}
function hash(value: string): string { return createHash("sha256").update(value).digest("hex"); }
export function collectorForBinding(binding: ArmBinding, baseline: CodeRole, candidate: CodeRole): CodeRole {
  for (const collector of [baseline, candidate]) {
    if (binding.collectorImageId === collector.imageId && binding.collectorRevision === collector.revision) return collector;
  }
  throw new Error("arm binding does not identify a declared collector");
}
function role(value: unknown): CodeRole {
  const parsed = object(value, "collector role");
  if (typeof parsed.imageId !== "string" || !/^sha256:[a-f0-9]{64}$/.test(parsed.imageId)
    || typeof parsed.revision !== "string" || !/^[a-f0-9]{40}$/.test(parsed.revision)) {
    throw new Error("collector role requires an immutable image and full revision");
  }
  return { imageId: parsed.imageId, revision: parsed.revision };
}
function executableHash(image: CodeRole, env: NodeJS.ProcessEnv): string {
  const label = execFileSync("docker", ["image", "inspect", "--format",
    '{{.Id}} {{index .Config.Labels "org.opencontainers.image.revision"}}', image.imageId], { encoding: "utf8", env }).trim();
  if (label !== `${image.imageId} ${image.revision}`) throw new Error("collector image revision is not pinned");
  const output = execFileSync("docker", ["run", "--rm", "--network", "none", "--entrypoint", "sha256sum",
    image.imageId, "/usr/local/bin/arrowcol-dataflow-rust"], { encoding: "utf8", env, timeout: 60_000 }).trim();
  const match = /^([a-f0-9]{64})\s+\/usr\/local\/bin\/arrowcol-dataflow-rust$/.exec(output);
  if (!match) throw new Error("collector executable hash is unavailable");
  return match[1]!;
}

export async function replayQualification(
  repo: string, home: string, rolesPath: string, bindHost: string, endpointHost: string,
  stackBytes: number, runSeries: boolean,
): Promise<JsonObject> {
  if (!/^[0-9a-fA-F:.]+$/.test(bindHost) || !/^[0-9a-fA-F:.]+$/.test(endpointHost)) {
    throw new Error("local bind and endpoint hosts must be explicit IP addresses");
  }
  const trusted = join(home, "trusted");
  const manifest = JSON.parse(await readFile(join(trusted, "manifest.json"), "utf8")) as SnapshotManifest;
  const producer = object(JSON.parse(await readFile(join(trusted, "producer.json"), "utf8")), "producer");
  if (producer.revision !== manifest.commit || !/^[a-f0-9]{40}$/.test(manifest.commit)) {
    throw new Error("qualification requires the frozen producer revision");
  }
  const roles = object(JSON.parse(await readFile(rolesPath, "utf8")), "role manifest");
  const baseline = role(roles.baseline);
  const candidate = role(roles.candidate);
  const common = object(roles.nonCollectorImages, "non-collector images");
  const nonCollectorImages: Record<string, string> = {};
  for (const [name, id] of Object.entries(common)) {
    if (typeof id !== "string") throw new Error("invalid non-collector image");
    nonCollectorImages[name] = id;
  }
  if (typeof roles.observerImageId !== "string" || !/^sha256:[a-f0-9]{64}$/.test(roles.observerImageId)) {
    throw new Error("observer image must be immutable");
  }
  const observerImageId = roles.observerImageId;
  const historicalPath = join(trusted, "src", "experiments", "darwin", "historical-parity.oat.json");
  const seedHash = await sha256(historicalPath);
  if (manifest.files["experiments/darwin/historical-parity.oat.json"]?.sha256 !== seedHash) throw new Error("trusted seed changed");
  const config = qualificationConfig(JSON.parse(await readFile(historicalPath, "utf8")));
  const configHash = codeCommonConfigHash(config);
  for (const collector of [baseline, candidate]) {
    bindCodeArm(config, { ...nonCollectorImages, collector: collector.imageId }, collector);
  }
  if (baseline.imageId === candidate.imageId && baseline.revision === candidate.revision) {
    throw new Error("layout trial requires a distinct candidate collector");
  }
  const planHash = hash(JSON.stringify({ revision: manifest.commit, configHash, seedHash, baseline, candidate,
    nonCollectorImages, observerImageId, stackBytes, runSeries }));
  const data = join(home, "data");
  await mkdir(data, { recursive: true });
  const lock = join(data, "evaluating");
  await mkdir(lock);
  let completed = false;
  try {
    const plan = join(data, "qualification");
    await mkdir(plan);
    const original = await createOriginalDeadline(join(plan, "deadline.json"), planHash);
    await writeFile(join(plan, "plan.json"), `${JSON.stringify({ version: 1, planHash, baseline, candidate,
      nonCollectorImages, observerImageId, config, configHash, seedHash, runSeries, original })}\n`, { flag: "wx", mode: 0o400 });
    const { runtime, binaryHash } = await prepareReplayRuntime(repo, home, stackBytes);
    const identityTools = await prepareIdentityTools(home, runtime, manifest, stackBytes);
    const footer = await buildFooterInspector(runtime, join(trusted, "prepare-home"), stackBytes);
    const env = cleanEnv(join(trusted, "prepare-home"), stackBytes);
    const source = join(trusted, "src", "tools", "darwin", "src", "physics-observer.ts");
    const sourceSha256 = await sha256(source);
    if (sourceSha256 !== manifest.files["tools/darwin/src/physics-observer.ts"]?.sha256) throw new Error("observer source changed");
    const bundle = join(plan, "observer.mjs");
    execFileSync(join(runtime, "node_modules", ".bin", "esbuild"), [source, "--bundle", "--platform=node", "--format=esm",
      `--outfile=${bundle}`], { cwd: runtime, env, stdio: "inherit", timeout: 60_000 });
    const bundleSha256 = await sha256(bundle);
    const observerSha256 = hash(JSON.stringify({ sourceSha256, bundleSha256, observerImageId, sampleMs: 1000, maxGapMs: 5000 }));
    const executables = new Map([
      [baseline.imageId, executableHash(baseline, env)],
      [candidate.imageId, executableHash(candidate, env)],
    ]);
    await writeFile(join(plan, "toolchain.json"), `${JSON.stringify({
      controllerSha256: binaryHash, footerSha256: footer.sha256, identityTools: {
        nodeSha256: identityTools.nodeSha256, tsxSha256: identityTools.tsxSha256,
        decoderSha256: identityTools.decoderSha256, extractorSha256: identityTools.extractorSha256,
        verifierArtifactSha256: identityTools.verifierArtifactSha256,
      },
      observerSourceSha256: sourceSha256, observerBundleSha256: bundleSha256,
      collectorExecutables: Object.fromEntries(executables),
    })}\n`, { flag: "wx", mode: 0o400 });
    const decodeFile = async (path: string): Promise<number> => {
      if (await sha256(footer.path) !== footer.sha256) throw new Error("footer decoder changed");
      const remaining = original.deadlineUnixMs - Date.now() - CLEANUP_RESERVE_MS;
      if (remaining <= 0) throw new Error("original deadline leaves no reconciliation time");
      const output = execFileSync(footer.path, ["--decode", path], { env, timeout: Math.min(600_000, remaining), encoding: "utf8" }).trim();
      if (await sha256(footer.path) !== footer.sha256 || !/^(0|[1-9][0-9]*)$/.test(output)
        || !Number.isSafeInteger(Number(output))) throw new Error("invalid decoded row count");
      return Number(output);
    };
    const execute = async (label: string, selected: CodeRole, deadlineUnixMs: number) => {
      await readOriginalDeadline(join(plan, "deadline.json"), planHash);
      const host = await inspectParityHost(16 * 1024 ** 3, 86 * 1024 ** 3);
      const images = { ...nonCollectorImages, collector: selected.imageId };
      const bound = bindCodeArm(config, images, selected);
      const result = await replayCompressionArm(data, label, bound, 3, manifest.commit, runtime, home, binaryHash,
        identityTools, bindHost, endpointHost, stackBytes, {
          collectorRevision: selected.revision, expectedImages: images, deadlineUnixMs, decodeFile,
          launch: (root, run) => runObservedArm(root, run, {
            collectorImageId: selected.imageId, collectorRevision: selected.revision,
            executableSha256: executables.get(selected.imageId)!, observerImageId, observerBundlePath: bundle,
            observerBundleSha256: bundleSha256, observerSourcePath: source, observerSourceSha256: sourceSha256,
            deadlineUnixMs,
          }),
        });
      await readOriginalDeadline(join(plan, "deadline.json"), planHash);
      if (!result.observer || result.deliveryIdentity?.verified !== true) throw new Error("qualified arm evidence is incomplete");
      await writeFile(join(result.runRoot, "host.json"), `${JSON.stringify(host)}\n`, { flag: "wx", mode: 0o400 });
      await writeFile(join(result.runRoot, "receipt.json"), `${JSON.stringify(result)}\n`, { flag: "wx", mode: 0o400 });
      return result;
    };
    const start = Date.now();
    if (start + ARM_RUN_BUDGET_MS + CLEANUP_RESERVE_MS + RECONCILIATION_RESERVE_MS > original.deadlineUnixMs) {
      throw new Error("original deadline leaves no room for smoke and cleanup");
    }
    const smoke = await execute("smoke", baseline, start + ARM_RUN_BUDGET_MS);
    if (!runSeries) {
      const report = { kind: "smoke", valid: 0, accepted: false, reason: "single arm cannot qualify", original, smoke };
      await writeFile(join(plan, "result.json"), `${JSON.stringify(report)}\n`, { flag: "wx", mode: 0o400 });
      completed = true;
      return { ...report, reportPath: join(plan, "result.json") };
    }
    const baseBinding: ArmBinding = {
      collectorImageId: baseline.imageId, collectorRevision: baseline.revision, nonCollectorImages,
      controllerSha256: binaryHash, observerSha256, workloadSha256: smoke.clientEvidence.payloadCorpusSha256,
      normalizedConfigSha256: configHash, treatmentSha256: hash("generated_partition_fast_path not requested; legacy capability may be unavailable"),
    };
    const candidateBinding = { ...baseBinding, collectorImageId: candidate.imageId, collectorRevision: candidate.revision };
    const runArm = async (pair: number, armRole: ArmRole, binding: ArmBinding, armDeadline: number): Promise<Tier2Arm> => {
      const selected = collectorForBinding(binding, baseline, candidate);
      const result = await execute(`${armRole}-${pair}`, selected, armDeadline);
      if (result.clientEvidence.payloadCorpusSha256 !== baseBinding.workloadSha256 || !result.observer
        || result.deliveryIdentity?.verified !== true || result.cellRunId === smoke.cellRunId || result.runRoot === smoke.runRoot) {
        throw new Error("arm corpus, identity, or freshness differs from frozen smoke");
      }
      return { pair, role: armRole, binding, cellRunId: result.cellRunId, runRoot: result.runRoot,
        measurement: result.measurement, deliveryIdentity: { verified: true }, observer: result.observer };
    };
    const control = await runTier2("noise-control", baseBinding, baseBinding, original.deadlineUnixMs, runArm);
    await writeFile(join(plan, "control.json"), `${JSON.stringify(control)}\n`, { flag: "wx", mode: 0o400 });
    const trial = await runTier2("trial", baseBinding, candidateBinding, original.deadlineUnixMs, runArm, control);
    await writeFile(join(plan, "trial.json"), `${JSON.stringify(trial)}\n`, { flag: "wx", mode: 0o400 });
    const report = { kind: "tier2", valid: 0, accepted: false, reason: "Tier-0 and Tier-1 candidate evidence not bound",
      original, smoke, control, trial };
    await writeFile(join(plan, "result.json"), `${JSON.stringify(report)}\n`, { flag: "wx", mode: 0o400 });
    completed = true;
    return { ...report, reportPath: join(plan, "result.json") };
  } finally { if (completed) await rmdir(lock); }
}
