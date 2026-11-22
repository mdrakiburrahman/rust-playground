import { execFileSync } from "node:child_process";
import { readFileSync, realpathSync } from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { parseDocument } from "yaml";

export const CAMPAIGN_LANES = [
  "client-encode",
  "router-sql",
  "flight-envelope",
  "layout-sort",
  "parquet-encode",
  "commit-pipeline",
  "maintenance-active",
  "maintenance-idle",
  "throughput-config",
  "compression-config",
] as const;

const laneSet = new Set<string>(CAMPAIGN_LANES);
const evaluatorPath = "/usr/local/bin/arrowcol-darwin-eval";

type RecordValue = Record<string, unknown>;

function isRecord(value: unknown): value is RecordValue {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function parseYaml(filePath: string): unknown {
  const document = parseDocument(readFileSync(filePath, "utf8"), {
    customTags: [
      { tag: "!eval", resolve: (value: string) => value },
      { tag: "!exec", resolve: (value: string) => value },
    ],
  });
  if (document.errors.length > 0) {
    throw new Error(document.errors.map((error) => error.message).join("\n"));
  }
  return document.toJS();
}

function relativeTrackedPath(repoRoot: string, filePath: string): string {
  const absolute = path.resolve(filePath);
  const real = realpathSync(absolute);
  if (real !== absolute) throw new Error("configuration paths may not be symlinks");
  const relative = path.relative(repoRoot, real);
  if (!relative || relative === ".." || relative.startsWith(`..${path.sep}`) || path.isAbsolute(relative)) {
    throw new Error("configuration paths must stay inside the repository");
  }
  const normalized = relative.split(path.sep).join("/");
  try {
    execFileSync("git", ["-C", repoRoot, "ls-files", "--error-unmatch", "--", normalized], {
      stdio: "ignore",
    });
  } catch {
    throw new Error(`configuration must be tracked: ${normalized}`);
  }
  return normalized;
}

function validateLaneConfig(value: unknown, lane: string, filePath: string): void {
  if (!isRecord(value) || value.name !== lane) {
    throw new Error(`${filePath} must declare name: ${lane}`);
  }
  if (!Array.isArray(value.agents) || value.agents.length === 0) {
    throw new Error(`${filePath} must declare at least one agent`);
  }
  if (!isRecord(value.evaluator) || !Array.isArray(value.evaluator.commands)
    || value.evaluator.commands.length !== 1
    || value.evaluator.commands[0] !== `${evaluatorPath} ${lane}`) {
    throw new Error(`${filePath} must use exactly "${evaluatorPath} ${lane}"`);
  }
}

function validateSingleConfig(repoRoot: string, configPath: string, mode: "campaign" | "toy"): number {
  const relative = relativeTrackedPath(repoRoot, configPath);
  const config = parseYaml(configPath);
  if (!isRecord(config)) throw new Error("Darwin config must be a mapping");

  if (mode === "toy") {
    if (relative !== "tools/darwin-runner/toy.yaml" || config.name !== "toy"
      || !isRecord(config.evaluator) || !Array.isArray(config.evaluator.commands)
      || config.evaluator.commands.length !== 1 || config.evaluator.commands[0] !== "/usr/bin/printf 1"
      || config.container !== undefined) {
      throw new Error("toy mode accepts only the tracked no-network parser fixture");
    }
    if (!Array.isArray(config.agents) || config.agents.length === 0) {
      throw new Error("toy config must declare at least one agent");
    }
    return 1;
  }

  if (typeof config.name !== "string" || !laneSet.has(config.name)) {
    throw new Error("single-experiment config name is not an allowed campaign lane");
  }
  validateLaneConfig(config, config.name, relative);
  return 1;
}

function validateManifest(repoRoot: string, manifestPath: string, value: RecordValue): number {
  const manifestRelative = relativeTrackedPath(repoRoot, manifestPath);
  if (manifestRelative !== "darwin.yaml") {
    throw new Error("campaign manifest must be the tracked repository-root darwin.yaml");
  }
  if (Object.keys(value).some((key) => key !== "experiments") || !Array.isArray(value.experiments)) {
    throw new Error("campaign manifest may contain only an experiments list");
  }

  const experiments = value.experiments;
  if (experiments.length !== CAMPAIGN_LANES.length) {
    throw new Error(`campaign manifest must contain exactly ${CAMPAIGN_LANES.length} lanes`);
  }

  const seen = new Set<string>();
  const manifestDirectory = path.dirname(manifestPath);
  for (const [index, rawExperiment] of experiments.entries()) {
    if (!isRecord(rawExperiment) || Object.keys(rawExperiment).some((key) => key !== "name" && key !== "config")) {
      throw new Error(`experiments[${index}] must contain only name and config`);
    }
    const { name, config } = rawExperiment;
    if (typeof name !== "string" || !laneSet.has(name)) {
      throw new Error(`experiments[${index}] has an unknown lane name`);
    }
    if (seen.has(name)) throw new Error(`duplicate campaign lane: ${name}`);
    seen.add(name);
    if (typeof config !== "string" || path.isAbsolute(config)) {
      throw new Error(`experiments[${index}].config must be a relative path`);
    }

    const configPath = path.resolve(manifestDirectory, config);
    const configRelative = relativeTrackedPath(repoRoot, configPath);
    const expectedRelative = `experiments/darwin/${name}.yaml`;
    if (configRelative !== expectedRelative) {
      throw new Error(`lane ${name} must reference ${expectedRelative}`);
    }
    validateLaneConfig(parseYaml(configPath), name, configRelative);
  }

  for (const lane of CAMPAIGN_LANES) {
    if (!seen.has(lane)) throw new Error(`campaign manifest is missing lane: ${lane}`);
  }
  return experiments.length;
}

export function validateDarwinYaml(filePath: string, mode: "campaign" | "toy" = "campaign"): number {
  const absolutePath = realpathSync(path.resolve(filePath));
  const repoRoot = realpathSync(execFileSync("git", ["-C", path.dirname(absolutePath), "rev-parse", "--show-toplevel"], {
    encoding: "utf8",
  }).trim());
  const value = parseYaml(absolutePath);
  if (isRecord(value) && Object.hasOwn(value, "experiments")) {
    if (mode !== "campaign") throw new Error("toy mode cannot validate a campaign manifest");
    return validateManifest(repoRoot, absolutePath, value);
  }
  return validateSingleConfig(repoRoot, absolutePath, mode);
}

function main(): void {
  const [filePath, modeArg] = process.argv.slice(2);
  if (!filePath || (modeArg !== undefined && modeArg !== "--toy")) {
    throw new Error("usage: config-check.ts <Darwin YAML> [--toy]");
  }
  const count = validateDarwinYaml(filePath, modeArg === "--toy" ? "toy" : "campaign");
  console.log(`Darwin evaluator isolation check passed (${count} config${count === 1 ? "" : "s"}).`);
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) main();
