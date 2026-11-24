import { execFileSync } from "node:child_process";
import { chmod, mkdir, readFile, symlink, writeFile } from "node:fs/promises";
import path from "node:path";
import { checkCompressionCandidate } from "./compression-policy.js";
import { CoverageUnavailableError } from "./metrics-adapter.js";
import { parseCollectorCpus, replayParityArm } from "./parity-replay.js";
import { parseParityProfile, parsePartitionFastPath } from "./parity-preflight.js";
import { checkCandidatePaths } from "./paths.js";
import { experimentPathsFor } from "./policy.js";
import { prepareReplayRuntime, replayCompressionPair } from "./replay.js";
import { replayQualification } from "./qualification.js";
import { snapshot, type SnapshotManifest } from "./snapshot.js";

function option(args: string[], name: string, fallback?: string): string {
  const index = args.indexOf(name);
  if (index < 0 && fallback) return fallback;
  if (index < 0 || !args[index + 1] || args[index + 1].startsWith("--")) {
    throw new Error(`${name} is required`);
  }
  return args[index + 1];
}

function shellQuote(value: string): string {
  return `'${value.replaceAll("'", "'\\''")}'`;
}

async function setup(repo: string, home: string, producer?: string): Promise<void> {
  const head = execFileSync("git", ["-C", repo, "rev-parse", "HEAD"], { encoding: "utf8" }).trim();
  const revision = producer ?? head;
  if (!/^[a-f0-9]{40}$/.test(revision)) throw new Error("producer must be a full Git SHA");
  execFileSync("git", ["-C", repo, "merge-base", "--is-ancestor", revision, head]);
  const manifest = await snapshot(repo, home);
  const trusted = path.join(path.resolve(home), "trusted");
  await writeFile(path.join(trusted, "producer.json"), `${JSON.stringify({ revision })}\n`, { flag: "wx" });
  const source = path.join(trusted, "src", "tools", "darwin", "src");
  const output = path.join(trusted, "compiled");
  await symlink(path.join(repo, "node_modules"), path.join(trusted, "node_modules"), "dir");
  const tsc = path.join(repo, "node_modules", "typescript", "bin", "tsc");
  execFileSync(process.execPath, [
    tsc, "--ignoreConfig", "--target", "ES2022", "--module", "NodeNext", "--moduleResolution", "NodeNext",
    "--skipLibCheck", "--types", "node", "--rootDir", source, "--outDir", output,
    ...["main.ts", "paths.ts", "policy.ts", "snapshot.ts", "compression-policy.ts", "metrics-adapter.ts",
      "score.ts", "delta-reconcile.ts", "oat-result.ts", "replay.ts"].map((file) => path.join(source, file)),
  ], { cwd: repo, stdio: "inherit" });
  await writeFile(path.join(output, "package.json"), '{"type":"module"}\n', { flag: "wx" });
  const launcher = path.join(trusted, "bin", "harness");
  await mkdir(path.dirname(launcher));
  // Keep the OAT launcher's owned-server recovery path while selecting its release build.
  const cargo = path.join(trusted, "bin", "cargo");
  await writeFile(cargo, `#!/bin/sh
set -eu
if [ "$#" -eq 7 ] && [ "$1" = build ] && [ "$2" = --locked ] && [ "$3" = --package ] && [ "$4" = arrowcol-oat-rust ] && [ "$5" = --bin ] && [ "$6" = arrowcol-oat-rust ] && [ "$7" = --message-format=json-render-diagnostics ]; then
  exec /usr/local/cargo/bin/cargo "$@" --release
fi
exec /usr/local/cargo/bin/cargo "$@"
`, { flag: "wx" });
  await chmod(cargo, 0o755);
  const script = `#!/bin/sh
set -eu
home="$(mktemp -d /tmp/arrowcol-darwin-home.XXXXXXXX)"
trap 'rmdir "$home"' EXIT
env -i PATH=/usr/local/bin:/usr/bin:/bin HOME="$home" DARWIN_HOME=${shellQuote(path.resolve(home))} ${shellQuote(process.execPath)} ${shellQuote(path.join(output, "main.js"))} "$@"
`;
  await writeFile(launcher, script, { flag: "wx" });
  await chmod(launcher, 0o755);
  console.log(`Snapshot ${manifest.commit} prepared with OAT producer ${revision}.`);
}

async function check(home: string, candidate: string, experiment: string): Promise<number> {
  const manifest = JSON.parse(await readFile(path.join(home, "trusted", "manifest.json"), "utf8")) as SnapshotManifest;
  const relativeHome = path.relative(candidate, home).split(path.sep).join("/");
  const stateDirectory = /^(\.darwin\/[A-Za-z0-9_-]+)(?:\/[A-Za-z0-9_-]+)*$/.exec(relativeHome)?.[1];
  const problems = await checkCandidatePaths(candidate, manifest.files, experimentPathsFor(experiment), manifest.submodules, stateDirectory);
  return problems.length;
}

async function main(args: string[]): Promise<void> {
  const command = args[0];
  const repo = execFileSync("git", ["rev-parse", "--show-toplevel"], { encoding: "utf8" }).trim();
  const home = path.resolve(option(args, "--home", process.env.DARWIN_HOME));
  if (command === "setup") {
    await setup(repo, home, args.includes("--producer") ? option(args, "--producer") : undefined);
    return;
  }
  if (command === "prepare") {
    const prepared = await prepareReplayRuntime(repo, home, Number(option(args, "--controller-stack-bytes")));
    console.log(`Prepared release OAT controller SHA-256 ${prepared.binaryHash}.`);
    return;
  }
  if (command === "parity") {
    const hasCollectorImage = args.includes("--collector-image-id");
    const hasCollectorRevision = args.includes("--collector-source-revision");
    if (hasCollectorImage !== hasCollectorRevision) throw new Error("collector image ID and source revision must be supplied together");
    const collectorOverride = hasCollectorImage ? {
      imageId: option(args, "--collector-image-id"), sourceRevision: option(args, "--collector-source-revision"),
    } : undefined;
    const receipt = await replayParityArm(
      repo, home, Number(option(args, "--workers")), option(args, "--bind-host"), option(args, "--endpoint-host"),
      Number(option(args, "--controller-stack-bytes")), collectorOverride,
      args.includes("--collector-cpus") ? parseCollectorCpus(option(args, "--collector-cpus")) : undefined,
      parseParityProfile(option(args, "--profile", "historical-parity-diagnostic")),
      args.includes("--generated-partition-fast-path") ?
        parsePartitionFastPath(option(args, "--generated-partition-fast-path")) : undefined,
      args.includes("--diagnostic-prelaunch-only"),
    );
    const prelaunchOnly = args.includes("--diagnostic-prelaunch-only");
    console.log(prelaunchOnly ? "@FEEDBACK Diagnostic prelaunch only; no arm started."
      : "@FEEDBACK One local parity arm is diagnostic and does not qualify a winner.");
    console.log(`@RESULT ${JSON.stringify({ version: 1,
      experiment: receipt.profile === "historical-parity-diagnostic" ? "historical-parity" : receipt.profile, valid: 0,
      combined_score: 0, accepted: false, reason: prelaunchOnly ? "diagnostic prelaunch only" : "single local parity arm", receipt })}`);
    return;
  }
  if (command === "qualify" || command === "qualify-codec-smoke") {
    const codecSmoke = command === "qualify-codec-smoke";
    const experiment = codecSmoke ? "compression-config" : "layout-sort";
    let qualification: Awaited<ReturnType<typeof replayQualification>>;
    try {
      if (codecSmoke && args.includes("--series")) throw new Error("codec smoke cannot run a qualification series");
      let candidate: unknown;
      if (codecSmoke) {
        const candidateRoot = path.resolve(option(args, "--candidate", repo));
        const problems = await check(home, candidateRoot, experiment);
        if (problems) throw new Error(`candidate violates ${problems} path checks`);
        candidate = JSON.parse(await readFile(path.join(candidateRoot, "experiments", "darwin", "candidate.oat.json"), "utf8")) as unknown;
      }
      qualification = await replayQualification(repo, home, path.resolve(option(args, "--roles")),
        option(args, "--bind-host"), option(args, "--endpoint-host"),
        Number(option(args, "--controller-stack-bytes")), args.includes("--series"), candidate);
    } catch (error) {
      const reason = error instanceof Error ? error.message : "unexpected qualification error";
      console.error(`@FEEDBACK ${reason}`);
      console.log(`@RESULT ${JSON.stringify({ version: 1, experiment,
        tier: args.includes("--series") ? "tier2-series" : "tier2-smoke", objective: "durable-rate",
        valid: 0, combined_score: 0, accepted: false, reason, durable_rows_per_second: null,
        measurement_unavailable_reason: reason })}`);
      process.exitCode = 1;
      return;
    }
    console.log("@FEEDBACK Local Tier-2 evidence alone cannot qualify a winner.");
    console.log(`@RESULT ${JSON.stringify({ version: 1, experiment,
      tier: args.includes("--series") ? "tier2-series" : "tier2-smoke", objective: "durable-rate",
      valid: 0, combined_score: 0, accepted: false, reason: qualification.reason, qualification })}`);
    return;
  }
  if (command !== "preflight" && command !== "evaluate") throw new Error("unsupported operation");
  const experiment = option(args, "--experiment");
  const candidate = path.resolve(option(args, "--candidate", repo));
  const problems = await check(home, candidate, experiment);
  if (!args.includes("--stub")) {
    if (experiment !== "compression-config") throw new Error("this lane is not implemented and is refused");
    const baseline = JSON.parse(await readFile(path.join(home, "trusted", "src", "experiments", "darwin", "candidate.oat.json"), "utf8")) as unknown;
    checkCompressionCandidate(baseline, baseline);
    const candidatePath = path.join(candidate, "experiments", "darwin", "candidate.oat.json");
    const candidateText = problems ? undefined : await readFile(candidatePath, "utf8");
    let changed: unknown;
    try {
      if (problems) throw new Error(`candidate violates ${problems} path checks`);
      if (candidateText === undefined) throw new Error("candidate configuration is unavailable");
      changed = JSON.parse(candidateText) as unknown;
      checkCompressionCandidate(baseline, changed);
    } catch (error) {
      if (command === "preflight") throw error;
      const reason = error instanceof Error ? error.message : "unexpected evaluator error";
      console.log(`@FEEDBACK ${reason}`);
      console.log(`@RESULT ${JSON.stringify({
        version: 1, experiment, tier: "tier2-smoke", objective: "durable-rate", valid: 0,
        combined_score: 0, accepted: false, reason,
      })}`);
      return;
    }
    if (command === "preflight") {
      console.log("Config-only compression preflight passed; Tier 1 qualification and code lanes remain unavailable.");
      return;
    }
    if (!args.includes("--smoke")) throw new Error("only a single-pair smoke replay is implemented; qualification remains unavailable");
    let result: Awaited<ReturnType<typeof replayCompressionPair>>;
    try {
      result = await replayCompressionPair(
        repo, home, changed, option(args, "--bind-host"), option(args, "--endpoint-host"), Number(option(args, "--controller-stack-bytes")),
        args.includes("--baseline-only"),
      );
    } catch (error) {
      if (!(error instanceof CoverageUnavailableError)) throw error;
      console.log(`@FEEDBACK Delta version coverage unavailable: ${error.message}`);
      console.log(`@RESULT ${JSON.stringify({
        version: 1, experiment, tier: "tier2-smoke", objective: "durable-rate", valid: 0,
        combined_score: 0, accepted: false, reason: "Delta version coverage unavailable",
        unattributed_rows: null, unattributed_reason: error.message,
      })}`);
      process.exitCode = 1;
      return;
    }
    if (args.includes("--baseline-only")) {
      console.log("@FEEDBACK One independently reconciled baseline is diagnostic, not a qualified winner.");
      console.log(`@RESULT ${JSON.stringify({
        version: 1, experiment, tier: "tier2-smoke", objective: "durable-rate", valid: 0,
        combined_score: 0, accepted: false, reason: "one baseline is not qualified", single_run: result,
      })}`);
      return;
    }
    console.log("@FEEDBACK One independently reconciled pair is diagnostic, not a qualified winner.");
    console.log(`@RESULT ${JSON.stringify({
      version: 1, experiment, tier: "tier2-smoke", objective: "durable-rate", valid: 0,
      combined_score: 0, accepted: false, reason: "one pair is not qualified",
      replay: result,
    })}`);
    return;
  }
  if (command === "preflight") {
    if (problems) throw new Error(`candidate violates ${problems} path checks`);
    console.log("Stub preflight passed path checks only; production-region and evidence checks remain unavailable.");
    return;
  }
  const reason = problems ? "candidate violates path checks" : "stub evaluation does not produce measurements";
  console.log(`@FEEDBACK ${reason}`);
  console.log(`@RESULT ${JSON.stringify({
    version: 1, experiment, tier: "stub", objective: "stub", proxy: null,
    valid: 0, combined_score: 0, accepted: false,
    reason, durable_rows_per_second: null, parquet_bytes_per_row: null,
    commit_latency_seconds: null, resources: null,
    downstream: { rows_per_second: null, unavailable_reason: "stub has no downstream observation" },
    window: null,
  })}`);
}

main(process.argv.slice(2)).catch((error: unknown) => {
  console.error(error instanceof Error ? error.message : "unexpected evaluator error");
  process.exitCode = 1;
});
