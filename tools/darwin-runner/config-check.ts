import { readFileSync } from "node:fs";
import { parseDocument } from "yaml";

const configPath = process.argv[2];
if (!configPath) {
  throw new Error("usage: config-check.ts <Darwin YAML>");
}

const document = parseDocument(readFileSync(configPath, "utf8"), {
  customTags: [{ tag: "!eval", resolve: (value: string) => value }],
});
if (document.errors.length > 0) {
  throw new Error(document.errors.map((error) => error.message).join("\n"));
}

const config = document.toJS() as {
  name?: string;
  evaluator?: { commands?: unknown };
  agents?: unknown;
  defaults?: { evaluator?: { commands?: unknown } };
  experiments?: Array<{ name?: string; evaluator?: { commands?: unknown } }>;
};

let commandCount = 0;
function validateCommands(location: string, commands: unknown, required: boolean): void {
  if (commands === undefined && !required) return;
  if (!Array.isArray(commands) || commands.length === 0) {
    throw new Error(`${location}.commands must use the isolated candidate executor`);
  }
  for (const command of commands) {
    commandCount++;
    if (
      typeof command !== "string" ||
      !/^env\s+-u\s+COPILOT_GITHUB_TOKEN\s+\/usr\/local\/bin\/darwin-candidate-exec(?:\s|$)/.test(command)
    ) {
      throw new Error(`${location}.commands must begin with "env -u COPILOT_GITHUB_TOKEN /usr/local/bin/darwin-candidate-exec"`);
    }
  }
}

const isManifest = Array.isArray(config.experiments);
if (!isManifest) {
  if (!config.name || !Array.isArray(config.agents) || config.agents.length === 0) {
    throw new Error("single-experiment config requires a name and at least one agent");
  }
  validateCommands("evaluator", config.evaluator?.commands, true);
} else {
  const defaultCommands = config.defaults?.evaluator?.commands;
  validateCommands("defaults.evaluator", defaultCommands, false);
  for (const [index, experiment] of (config.experiments ?? []).entries()) {
    const name = experiment.name ? ` (${experiment.name})` : "";
    validateCommands(
      `experiments[${index}]${name}.evaluator`,
      experiment.evaluator?.commands ?? defaultCommands,
      true,
    );
  }
}

if (commandCount === 0 || (isManifest && !config.experiments?.length)) {
  throw new Error("configuration has no isolated evaluator commands");
}

console.log(`Darwin evaluator isolation check passed (${commandCount} command${commandCount === 1 ? "" : "s"}).`);
