import { execFileSync } from "node:child_process";
import { chmod, mkdir, readFile, writeFile } from "node:fs/promises";
import path from "node:path";
import { checkCandidatePaths } from "./paths.js";
import { experimentPathsFor } from "./policy.js";
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

async function setup(repo: string, home: string): Promise<void> {
  const manifest = await snapshot(repo, home);
  const trusted = path.join(path.resolve(home), "trusted");
  const source = path.join(trusted, "src", "tools", "darwin", "src");
  const output = path.join(trusted, "compiled");
  const tsc = path.join(repo, "node_modules", "typescript", "bin", "tsc");
  execFileSync(process.execPath, [
    tsc, "--ignoreConfig", "--target", "ES2022", "--module", "NodeNext", "--moduleResolution", "NodeNext",
    "--skipLibCheck", "--types", "node", "--rootDir", source, "--outDir", output,
    ...["main.ts", "paths.ts", "policy.ts", "snapshot.ts"].map((file) => path.join(source, file)),
  ], { cwd: repo, stdio: "inherit" });
  await writeFile(path.join(output, "package.json"), '{"type":"module"}\n', { flag: "wx" });
  const launcher = path.join(trusted, "bin", "harness");
  await mkdir(path.dirname(launcher));
  const script = `#!/bin/sh
set -eu
home="$(mktemp -d /tmp/arrowcol-darwin-home.XXXXXXXX)"
trap 'rmdir "$home"' EXIT
env -i PATH=/usr/local/bin:/usr/bin:/bin HOME="$home" DARWIN_HOME=${shellQuote(path.resolve(home))} ${shellQuote(process.execPath)} ${shellQuote(path.join(output, "main.js"))} "$@"
`;
  await writeFile(launcher, script, { flag: "wx" });
  await chmod(launcher, 0o755);
  console.log(`Snapshot ${manifest.commit} prepared; only stub evaluation is available.`);
}

async function check(home: string, candidate: string, experiment: string): Promise<number> {
  const manifest = JSON.parse(await readFile(path.join(home, "trusted", "manifest.json"), "utf8")) as SnapshotManifest;
  const relativeHome = path.relative(candidate, home).split(path.sep).join("/");
  const stateDirectory = /^\.darwin\/[A-Za-z0-9_-]+$/.test(relativeHome) ? relativeHome : undefined;
  const problems = await checkCandidatePaths(candidate, manifest.files, experimentPathsFor(experiment), manifest.submodules, stateDirectory);
  return problems.length;
}

async function main(args: string[]): Promise<void> {
  const command = args[0];
  const repo = execFileSync("git", ["rev-parse", "--show-toplevel"], { encoding: "utf8" }).trim();
  const home = path.resolve(option(args, "--home", process.env.DARWIN_HOME));
  if (command === "setup") {
    await setup(repo, home);
    return;
  }
  if (command !== "preflight" && command !== "evaluate") throw new Error("unsupported operation");
  const experiment = option(args, "--experiment");
  const candidate = path.resolve(option(args, "--candidate", repo));
  if (!args.includes("--stub")) {
    throw new Error("real evaluation is unavailable: production-region guards, fixtures, isolated execution and reconciliation are not implemented");
  }
  const problems = await check(home, candidate, experiment);
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
