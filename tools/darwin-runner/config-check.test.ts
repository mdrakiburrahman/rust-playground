import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdirSync, mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import path from "node:path";
import test from "node:test";
import { CAMPAIGN_LANES, validateDarwinYaml } from "./config-check.js";

interface FixtureOptions {
  command?: string;
  names?: string[];
  missingConfig?: string;
  untrackedConfig?: string;
}

function config(lane: string, command: string): string {
  return `name: ${lane}\nagents:\n  - provider: copilot\nevaluator:\n  commands:\n    - ${JSON.stringify(command)}\n  metrics:\n    score: !exec "float(stdout[-1])"\n`;
}

function manifest(names: string[] = [...CAMPAIGN_LANES]): string {
  return `experiments:\n${names.map((name) => `  - name: ${name}\n    config: experiments/darwin/${name}.yaml`).join("\n")}\n`;
}

function createRepo(options: FixtureOptions = {}): string {
  const root = mkdtempSync(path.join(tmpdir(), "darwin-config-check-"));
  execFileSync("git", ["init", "-q"], { cwd: root });
  execFileSync("git", ["config", "user.name", "Runner test"], { cwd: root });
  execFileSync("git", ["config", "user.email", "runner-test@example.invalid"], { cwd: root });
  mkdirSync(path.join(root, "experiments", "darwin"), { recursive: true });
  const names = options.names ?? [...CAMPAIGN_LANES];
  writeFileSync(path.join(root, "darwin.yaml"), manifest(names));
  execFileSync("git", ["add", "darwin.yaml"], { cwd: root });
  for (const lane of CAMPAIGN_LANES) {
    const command = lane === "compression-config"
      ? options.command ?? `/usr/local/bin/arrowcol-darwin-eval ${lane}`
      : `/usr/local/bin/arrowcol-darwin-eval ${lane}`;
    const file = path.join(root, "experiments", "darwin", `${lane}.yaml`);
    if (lane === options.missingConfig) continue;
    writeFileSync(file, config(lane, command));
    if (lane !== options.untrackedConfig) execFileSync("git", ["add", path.relative(root, file)], { cwd: root });
  }
  execFileSync("git", ["commit", "-qm", "fixture"], { cwd: root });
  return root;
}

function withRepo(options: FixtureOptions, run: (root: string) => void): void {
  const root = createRepo(options);
  try { run(root); } finally { rmSync(root, { recursive: true, force: true }); }
}

test("accepts the exact complete external lane manifest and inert !exec tags", () => {
  withRepo({}, (root) => {
    assert.equal(validateDarwinYaml(path.join(root, "darwin.yaml")), CAMPAIGN_LANES.length);
  });
});

test("rejects shell tails, expansions and executor prefixes", () => {
  for (const command of [
    "/usr/local/bin/arrowcol-darwin-eval compression-config; printf 1",
    '/usr/local/bin/arrowcol-darwin-eval "$(touch /tmp/darwin-injection)"',
    "env -u COPILOT_GITHUB_TOKEN /usr/local/bin/arrowcol-darwin-eval compression-config",
  ]) {
    withRepo({ command }, (root) => {
      assert.throws(() => validateDarwinYaml(path.join(root, "darwin.yaml")), /must use exactly/);
    });
  }
});

test("rejects duplicate, unknown and missing lanes", () => {
  withRepo({ names: [...CAMPAIGN_LANES.slice(0, -1), CAMPAIGN_LANES[0]] }, (root) => {
    assert.throws(() => validateDarwinYaml(path.join(root, "darwin.yaml")), /duplicate campaign lane/);
  });
  withRepo({ names: [...CAMPAIGN_LANES.slice(0, -1), "unknown-lane"] }, (root) => {
    assert.throws(() => validateDarwinYaml(path.join(root, "darwin.yaml")), /unknown lane/);
  });
  withRepo({ names: CAMPAIGN_LANES.slice(0, -1) }, (root) => {
    assert.throws(() => validateDarwinYaml(path.join(root, "darwin.yaml")), /exactly 10 lanes/);
  });
});

test("rejects missing and untracked lane files", () => {
  withRepo({ missingConfig: "compression-config" }, (root) => {
    assert.throws(() => validateDarwinYaml(path.join(root, "darwin.yaml")), /ENOENT/);
  });
  withRepo({ untrackedConfig: "compression-config" }, (root) => {
    assert.throws(() => validateDarwinYaml(path.join(root, "darwin.yaml")), /configuration must be tracked/);
  });
});

test("accepts only the fixed toy command in explicit toy mode", () => {
  const root = mkdtempSync(path.join(tmpdir(), "darwin-toy-check-"));
  try {
    execFileSync("git", ["init", "-q"], { cwd: root });
    execFileSync("git", ["config", "user.name", "Runner test"], { cwd: root });
    execFileSync("git", ["config", "user.email", "runner-test@example.invalid"], { cwd: root });
    mkdirSync(path.join(root, "tools", "darwin-runner"), { recursive: true });
    const toy = path.join(root, "tools", "darwin-runner", "toy.yaml");
    writeFileSync(toy, config("toy", "/usr/bin/printf 1"));
    execFileSync("git", ["add", "tools/darwin-runner/toy.yaml"], { cwd: root });
    execFileSync("git", ["commit", "-qm", "toy fixture"], { cwd: root });
    assert.equal(validateDarwinYaml(toy, "toy"), 1);
    assert.throws(() => validateDarwinYaml(toy), /not an allowed campaign lane/);
  } finally { rmSync(root, { recursive: true, force: true }); }
});
