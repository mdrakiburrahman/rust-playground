import { execFileSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { buildThroughputManifests, renderFrozenThroughputConfig } from "../../../projects/arrowcol-oat/throughput-sweeps.js";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const args = process.argv.slice(2);
const options = new Map<string, string>();
for (let index = 0; index < args.length; index += 2) {
  const key = args[index];
  if (!["--pairs", "--comparison", "--candidate"].includes(key) || !args[index + 1] || options.has(key)) {
    throw new Error("expected --pairs N and/or --comparison ID --candidate ID");
  }
  options.set(key, args[index + 1]);
}
if (options.has("--comparison") !== options.has("--candidate")) throw new Error("comparison and candidate must be paired");
const rawPairs = options.get("--pairs") ?? "1";
if (!/^[1-9][0-9]*$/.test(rawPairs)) throw new Error("pairs must be 1..10");
const pairs = Number(rawPairs);
if (!Number.isSafeInteger(pairs) || pairs > 10) throw new Error("pairs must be 1..10");
const baseline = JSON.parse(readFileSync(path.join(root, "experiments/darwin/candidate.oat.json"), "utf8"));
const manifest = buildThroughputManifests(baseline).find((item) => item.cohort === "C1");
if (!manifest) throw new Error("codec sweep C1 is missing");
const candidate = renderFrozenThroughputConfig(baseline, manifest,
  options.get("--comparison") ?? "codec-zstd-levels", options.get("--candidate") ?? "zstd-1").config;
const directory = mkdtempSync(path.join(tmpdir(), "darwin-codec-pair-"));
const env: NodeJS.ProcessEnv = { PATH: process.env.PATH, HOME: process.env.HOME };
for (const key of ["CARGO_HOME", "RUSTUP_HOME", "CARGO_TARGET_DIR"]) {
  if (process.env[key] !== undefined) env[key] = process.env[key];
}

try {
  const baselinePath = path.join(directory, "baseline.json");
  const candidatePath = path.join(directory, "candidate.json");
  writeFileSync(baselinePath, JSON.stringify(baseline));
  writeFileSync(candidatePath, JSON.stringify(candidate));
  const manifestPath = path.join(root, "tools/darwin/codec-fixture/Cargo.toml");
  execFileSync("cargo", ["build", "--locked", "--release", "--manifest-path", manifestPath], {
    cwd: root, env, stdio: "inherit",
  });
  const metadata = JSON.parse(execFileSync("cargo", ["metadata", "--locked", "--no-deps", "--format-version", "1",
    "--manifest-path", manifestPath], { cwd: root, env, encoding: "utf8" }));
  const binary = path.join(metadata.target_directory, "release", "darwin-codec-fixture");
  const run = (label: string, value: any, configPath: string) => {
    const output = execFileSync(binary, [baselinePath, configPath], {
      cwd: root, env: { ...env, ARROWCOL_PARQUET_CONFIG: JSON.stringify(value.tunables.parquet_writer) },
      encoding: "utf8", stdio: ["ignore", "pipe", "inherit"],
    }).trim();
    const prefix = "DARWIN_CODEC_RESULT=";
    if (!output.startsWith(prefix)) throw new Error("fixture result is missing");
    const result = JSON.parse(output.slice(prefix.length));
    if (result.validation !== "schema_and_every_ordered_value_equal") throw new Error("unverified fixture output");
    console.log(JSON.stringify({ label, result }));
    return result;
  };
  for (let pair = 1; pair <= pairs; pair++) {
    const order = pair % 2 ? ["baseline", "candidate"] : ["candidate", "baseline"];
    const results: Record<string, any> = {};
    for (const label of order) {
      results[label] = label === "baseline"
        ? run(`${pair}-baseline`, baseline, baselinePath)
        : run(`${pair}-candidate`, candidate, candidatePath);
    }
    if (results.baseline.corpus_sha256_arrow_ipc !== results.candidate.corpus_sha256_arrow_ipc ||
        results.baseline.rows !== results.candidate.rows) {
      throw new Error("paired corpus differs");
    }
  }
} finally {
  rmSync(directory, { recursive: true, force: true });
}
