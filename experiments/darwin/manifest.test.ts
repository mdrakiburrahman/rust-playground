import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { test } from "node:test";
import { parseDocument, isScalar } from "yaml";
import { experimentPaths } from "../../tools/darwin/src/policy.js";

const lanes = Object.keys(experimentPaths);
const tags = [
  { tag: "!eval", resolve: (value: string) => value },
  { tag: "!exec", resolve: (value: string) => value },
];

function object(value: unknown): Record<string, unknown> {
  assert.ok(value && typeof value === "object" && !Array.isArray(value));
  return value as Record<string, unknown>;
}

function keys(value: unknown, expected: string[]): Record<string, unknown> {
  const result = object(value);
  assert.deepEqual(Object.keys(result).sort(), [...expected].sort());
  return result;
}

function loadLane(lane: string) {
  const source = readFileSync(`experiments/darwin/${lane}.yaml`, "utf8");
  const doc = parseDocument(source, { customTags: tags, uniqueKeys: true });
  assert.deepEqual(doc.errors, []);
  assert.deepEqual(doc.warnings, []);
  const result = keys(doc.toJS(), ["name", "executor", "container", "agents", "samplers", "prompt", "evaluator"]);
  const node = doc.getIn(["evaluator", "metrics", "combined_score"], true);
  assert.ok(isScalar(node));
  assert.equal(node.tag, "!exec");
  for (const path of [["evaluator", "metrics", "valid"], ["evaluator", "artifacts", "feedback"]]) {
    const scalar = doc.getIn(path, true);
    assert.ok(isScalar(scalar));
    assert.equal(scalar.tag, "!eval");
  }
  return result;
}

test("all policy lanes have complete and narrowly scoped offline configurations", () => {
  assert.equal(lanes.length, 10);
  let firstMetrics: unknown;
  let firstArtifacts: unknown;
  for (const lane of lanes) {
    const config = loadLane(lane);
    assert.equal(config.name, lane);
    assert.deepEqual(config.container, { provider: "docker" });
    assert.deepEqual(config.samplers, [{ provider: "islands" }]);
    assert.deepEqual(config.agents, [{ provider: "copilot", timeout: 900, models: [{ name: "claude-sonnet-5" }] }]);
    const executor = keys(config.executor, ["parallelism", "termination"]);
    assert.equal(executor.parallelism, 1);
    const termination = keys(executor.termination, ["max_iterations", "early_stopping"]);
    assert.equal(termination.max_iterations, lane.endsWith("-config") ? 12 : 24);
    assert.deepEqual(termination.early_stopping, {
      max_iterations_without_improvement: 8, metric: "combined_score", min_improvement: 0.02, improvement_type: "absolute",
    });
    const prompt = keys(config.prompt, ["provider", "system", "agent"]);
    assert.equal(prompt.provider, "default");
    assert.equal(typeof prompt.system, "string");
    assert.equal(typeof prompt.agent, "string");
    for (const path of experimentPaths[lane as keyof typeof experimentPaths]) {
      assert.ok((prompt.agent as string).includes(path), `${lane} must describe ${path}`);
    }
    const evaluator = keys(config.evaluator, ["timeout", "retries", "builtin_metrics", "imports", "commands", "metrics", "artifacts"]);
    assert.equal(evaluator.timeout, lane.endsWith("-config") ? 7200 : 1800);
    assert.equal(evaluator.retries, 0);
    assert.deepEqual(evaluator.builtin_metrics, []);
    assert.deepEqual(evaluator.imports, ["json", "math"]);
    assert.deepEqual(evaluator.commands, [`/usr/local/bin/arrowcol-darwin-eval ${lane}`]);
    const metrics = keys(evaluator.metrics, ["combined_score", "valid"]);
    const artifacts = keys(evaluator.artifacts, ["feedback"]);
    if (!firstMetrics) { firstMetrics = metrics; firstArtifacts = artifacts; }
    assert.deepEqual(metrics, firstMetrics);
    assert.deepEqual(artifacts, firstArtifacts);
    assert.match(metrics.combined_score as string, /len\(records\) != 1/);
    assert.match(metrics.combined_score as string, /not nonempty\[-1\]\.startswith/);
    assert.match(metrics.combined_score as string, /not math\.isfinite\(score\)/);
    assert.match(metrics.combined_score as string, /valid == 0 and score != 0/);
    assert.match(metrics.valid as string, /\["valid"\]/);
    assert.match(artifacts.feedback as string, /@FEEDBACK /);
  }
});

function validateManifest(value: unknown): string[] {
  const manifest = keys(value, ["experiments"]);
  assert.ok(Array.isArray(manifest.experiments));
  const seen = new Set<string>();
  for (const raw of manifest.experiments) {
    const entry = keys(raw, ["name", "config"]);
    assert.equal(typeof entry.name, "string");
    const name = entry.name as string;
    assert.ok(Object.hasOwn(experimentPaths, name), "unknown lane");
    assert.ok(!seen.has(name), "duplicate lane");
    assert.equal(entry.config, `experiments/darwin/${name}.yaml`);
    seen.add(name);
  }
  assert.deepEqual([...seen].sort(), [...lanes].sort(), "missing lane");
  return [...seen];
}

test("manifest maps every policy lane exactly once to a complete relative file", () => {
  const doc = parseDocument(readFileSync("darwin.yaml", "utf8"), { uniqueKeys: true });
  assert.deepEqual(doc.errors, []);
  assert.deepEqual(doc.warnings, []);
  for (const lane of validateManifest(doc.toJS())) assert.equal(loadLane(lane).name, lane);
});

test("offline manifest lint rejects unknown, duplicate, missing, and redirected lanes", () => {
  const manifest = () => ({ experiments: lanes.map((name) => ({ name, config: `experiments/darwin/${name}.yaml` })) });
  const unknown = manifest(); unknown.experiments.push({ name: "unknown", config: "unknown.yaml" });
  assert.throws(() => validateManifest(unknown), /unknown lane/);
  const duplicate = manifest(); duplicate.experiments.push({ ...duplicate.experiments[0] });
  assert.throws(() => validateManifest(duplicate), /duplicate lane/);
  const missing = manifest(); missing.experiments.pop();
  assert.throws(() => validateManifest(missing), /missing lane/);
  const redirected = manifest(); redirected.experiments[0].config = "../untrusted.yaml";
  assert.throws(() => validateManifest(redirected));
});
