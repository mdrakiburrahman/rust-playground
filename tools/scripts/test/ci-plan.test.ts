import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { availableParallelism } from 'node:os';
import test from 'node:test';
import { fileURLToPath } from 'node:url';
import { runInNewContext } from 'node:vm';

import { parse } from 'yaml';
import type { TargetConfiguration } from '@nx/devkit';

import {
  createVerificationPlan,
  requireVerificationTask,
  verificationArguments,
  type VerificationGraph,
} from '../src/ci-plan.js';

function fixture(): VerificationGraph {
  const graph: VerificationGraph = {
    graph: { nodes: {} },
    tasks: { tasks: {}, dependencies: {} },
  };
  addTask(graph, 'sdk:test', 'cargo test --locked --package sdk');
  addTask(graph, 'sdk:lint', 'cargo clippy --locked --package sdk');
  addTask(graph, 'sdk:format-check', 'cargo fmt --package sdk -- --check');
  addTask(graph, 'collector:build', 'cargo build --locked --package collector');
  addTask(graph, 'collector:config-check', 'target/debug/collector --validate', ['collector:build']);
  addTask(graph, 'scripts:typecheck', 'tsc --noEmit');
  addTask(graph, 'scripts:test', 'tsx --test test.ts');
  addTask(graph, 'scripts:verify', undefined, ['scripts:typecheck', 'scripts:test']);
  addTask(graph, 'loadtest:reader-build', 'cargo build --locked --package delta-inspect');
  for (const project of ['collector', 'sample']) {
    addTask(graph, `${project}:image`, undefined, [], {
      executor: 'rust-playground-build:docker-build',
      options: { image: `ghcr.io/example/${project}`, tags: ['dev'], output: 'load' },
      configurations: {
        'ci-verify': {
          cacheFrom: ['type=gha,version=2,scope=example'],
          cacheTo: ['type=gha,version=2,scope=example,mode=max'],
        },
      },
    });
  }
  addTask(graph, 'loadtest:build', undefined, ['collector:image', 'sample:image', 'loadtest:reader-build']);
  addTask(graph, 'loadtest:e2e', 'tsx e2e.ts', ['loadtest:build']);
  addTask(graph, 'root:verify', undefined, Object.keys(graph.tasks.tasks));
  return graph;
}

function addTask(
  graph: VerificationGraph,
  id: string,
  command?: string,
  dependencies: string[] = [],
  configuration: TargetConfiguration = {
    executor: 'nx:run-commands', options: command ? { command } : { commands: [] },
  },
): void {
  const [project, target] = id.split(':');
  assert.ok(project && target);
  const node = graph.graph.nodes[project] ?? {
    name: project, type: 'lib', data: { root: project, targets: {} },
  };
  node.data.targets ??= {};
  node.data.targets[target] = configuration;
  graph.graph.nodes[project] = node;
  graph.tasks.tasks[id] = {
    id, target: { project, target }, projectRoot: project, overrides: {}, outputs: [], cache: false,
  };
  graph.tasks.dependencies[id] = dependencies;
}

test('fans out independent material checks, preserving prerequisite closures and artifact handoffs', () => {
  const plan = createVerificationPlan(fixture());
  assert.deepEqual(plan.node.flatMap(({ tasks }) => tasks), [
    'scripts:test',
    'scripts:typecheck',
  ]);
  assert.deepEqual(plan.rust.flatMap(({ tasks }) => tasks), [
    'collector:config-check', 'sdk:format-check', 'sdk:lint', 'sdk:test',
  ]);
  assert.deepEqual(plan.images.map(({ task }) => task), ['collector:image', 'sample:image']);
  assert.deepEqual(plan.images[0]?.image_refs, ['ghcr.io/example/collector:dev']);
  assert.equal(plan.reader, 'loadtest:reader-build');
  assert.equal(plan.integration, 'loadtest:e2e');
  assert.equal(plan.materialTasks.length, 11);
  assert.equal(plan.rust.some(({ task }) => task === 'collector:build'), false);
  assert.equal(plan.rust.length, 2);
  assert.equal(plan.node.some(({ task }) => task === 'scripts:verify'), false);
});

test('new projects are discovered without editing a hard-coded CI project list', () => {
  const graph = fixture();
  addTask(graph, 'new-package:test', 'tsx --test another.ts');
  assert.ok(
    createVerificationPlan(graph).node.some(({ tasks }) =>
      tasks.includes('new-package:test'),
    ),
  );
});

test('Rust commands with environment assignments still use the pinned source environment', () => {
  const graph = fixture();
  addTask(graph, 'new-rust:test', 'RUST_TEST_THREADS=1 cargo test --locked');
  assert.ok(createVerificationPlan(graph).rust.some(({ task }) => task === 'new-rust:test'));
  assert.ok(
    !createVerificationPlan(graph).node.some(({ tasks }) =>
      tasks.includes('new-rust:test'),
    ),
  );
});

test('nonempty verify targets retain their commands and dependent checks', () => {
  const graph = fixture();
  addTask(graph, 'scripts:verify', 'node -e "console.log(\'verified\')"', ['scripts:typecheck', 'scripts:test']);
  assert.deepEqual(
    createVerificationPlan(graph).node.flatMap(({ tasks }) => tasks),
    ['scripts:verify'],
  );
});

test('missing and cyclic prerequisites fail even behind empty aggregation nodes', () => {
  const missing = fixture();
  missing.tasks.dependencies['root:verify']?.push('missing:target');
  assert.throws(() => createVerificationPlan(missing), /Missing Nx prerequisite/u);
  const cycle = fixture();
  cycle.tasks.dependencies['sdk:test'] = ['root:verify'];
  assert.throws(() => createVerificationPlan(cycle), /Cyclic Nx prerequisites/u);
});

test('same-project Cargo roots batch shared prerequisites instead of rebuilding them', () => {
  const graph = fixture();
  addTask(graph, 'collector:other-check', 'target/debug/collector --other', ['collector:build']);
  const plan = createVerificationPlan(graph);
  const collector = plan.rust.find(
    ({ key }) => key === 'collector-checks',
  );
  assert.deepEqual(collector?.tasks, [
    'collector:config-check',
    'collector:other-check',
  ]);
});

test('cross-project shared prerequisites still require an artifact handoff', () => {
  const graph = fixture();
  addTask(
    graph,
    'other:check',
    'target/debug/other --check',
    ['collector:build'],
  );
  assert.throws(() => createVerificationPlan(graph), /shared by/u);
});

test('missing integration producers and unexpected E2E prerequisites fail closed', () => {
  const missing = fixture();
  delete missing.tasks.tasks['loadtest:reader-build'];
  assert.throws(() => createVerificationPlan(missing), /Required CI task is missing/u);
  const extra = fixture();
  extra.tasks.dependencies['loadtest:e2e']?.push('sdk:test');
  assert.throws(() => createVerificationPlan(extra), /E2E prerequisites must be exactly/u);
});

test('image jobs must load tagged images and cannot silently omit build prerequisites', () => {
  const invalid = fixture();
  const target = invalid.graph.nodes.collector?.data.targets?.image;
  assert.ok(target);
  target.options = { image: 'ghcr.io/example/collector', output: 'push', tags: ['dev'] };
  assert.throws(() => createVerificationPlan(invalid), /must load explicitly tagged images/u);
  const dependent = fixture();
  dependent.tasks.dependencies['collector:image'] = ['sdk:test'];
  assert.throws(() => createVerificationPlan(dependent), /explicit prerequisite handoff/u);
});

test('image caches use JSON configurations so Nx cannot split BuildKit comma parameters', () => {
  const graph = fixture();
  const image = graph.graph.nodes.collector?.data.targets?.image;
  assert.ok(image);
  image.configurations = {};
  assert.throws(() => createVerificationPlan(graph), /ci-verify cache configuration/u);
});
test('Nx arguments default to available CPUs and never interpolate task strings into a shell command', () => {
  assert.deepEqual(verificationArguments('sdk:test', 8), [
    'run', 'sdk:test', '--parallel=8', '--outputStyle=static',
  ]);
  assert.ok(verificationArguments().includes(`--parallel=${availableParallelism()}`));
  for (const task of ['sdk:test;echo unsafe', '--skip:checks', '../escape:test', 'sdk:test\nother']) {
    assert.throws(() => requireVerificationTask(task), /Invalid Nx/u);
  }
  for (const parallel of [0, -1, 1.5, Infinity]) {
    assert.throws(() => verificationArguments(undefined, parallel), /positive integer/u);
  }
});

test('same-project Cargo checks share compilation caches without dropping targets or creating a cross-product', () => {
  assert.deepEqual(verificationArguments(['sdk:lint', 'sdk:test', 'sdk:build'], 4), [
    'run-many', '--projects=sdk', '--targets=lint,test,build', '--parallel=4', '--outputStyle=static',
  ]);
  assert.throws(() => verificationArguments([]), /must not be empty/u);
  assert.throws(() => verificationArguments(['sdk:lint', 'other:test']), /one project/u);
  assert.throws(() => verificationArguments(['sdk:test:ci']), /unconfigured targets/u);
});
interface Workflow {
  jobs: Record<string, {
    name?: string;
    needs?: string | string[];
    if?: string;
    strategy?: { 'fail-fast'?: boolean; 'max-parallel'?: number };
    uses?: string;
    steps?: Array<{
      env?: Record<string, string>;
      name?: string;
      if?: string;
      run?: string;
      uses?: string;
      with?: Record<string, unknown>;
    }>;
  }>;
}

function workflow(name: string): Workflow {
  return parse(readFileSync(fileURLToPath(new URL(`../../../.github/workflows/${name}`, import.meta.url)), 'utf8')) as Workflow;
}

test('failed integration diagnostics exclude private raw logs at the actual uploader', () => {
  const uploader = workflow('ci.yml').jobs.e2e?.steps?.find(step => step.with?.name === 'telemetry-e2e-diagnostics');
  assert.ok(uploader);
  assert.ok(uploader.uses?.startsWith('actions/upload-artifact@'));
  const path = uploader.with?.path;
  assert.ok(typeof path === 'string');
  const patterns = path.split(/\r?\n/u).map(line => line.trim()).filter(Boolean);
  assert.ok(patterns.includes('onelake/e2e/'));
  assert.ok(patterns.includes('onelake/benchmark/latest/results/'));
  assert.ok(patterns.includes('!**/*.private.log'), 'The real uploader must exclude private raw logs, not merely name them private');
});

test('workflow starts environment, Node checks, and image builds without unrelated barriers or concurrency caps', () => {
  const jobs = workflow('ci.yml').jobs;
  assert.equal(jobs.environment?.needs, undefined);
  assert.equal(jobs.plan?.needs, undefined);
  assert.equal(jobs.node?.needs, 'plan');
  assert.equal(jobs.images?.needs, 'plan');
  assert.deepEqual(jobs.rust?.needs, ['plan', 'environment']);
  for (const name of ['node', 'images', 'rust']) {
    assert.equal(jobs[name]?.strategy?.['max-parallel'], undefined);
    assert.equal(jobs[name]?.strategy?.['fail-fast'], false);
  }
  assert.deepEqual(jobs.e2e?.needs, ['plan', 'images', 'reader']);
  assert.ok(!jobs.e2e?.needs?.includes('rust'));
  assert.ok(!jobs.e2e?.needs?.includes('node'));
  assert.ok(!jobs.e2e?.steps?.some(({ with: options }) => options?.name === 'ci-source-environment'));
  const nodeVerify = jobs.node?.steps?.find(
    ({ run }) => run?.endsWith('tools-scripts:ci-verify-host'),
  );
  assert.match(nodeVerify?.env?.CI_TASKS ?? '', /matrix\.tasks/u);
});

test('the stable final required check rejects failure, cancellation, and skipped branches', () => {
  const jobs = workflow('ci.yml').jobs;
  assert.equal(jobs.verify?.name, 'Verify in source devcontainer');
  assert.equal(jobs.verify?.if, 'always()');
  assert.deepEqual(new Set(jobs.verify?.needs), new Set(Object.keys(jobs).filter((name) => name !== 'verify')));
  const script = jobs.verify?.steps?.[0]?.run ?? '';
  assert.match(script, /job\.result !== "success"/u);
  assert.match(script, /process\.exit\(1\)/u);
  const source = script.slice(script.indexOf("'") + 1, script.lastIndexOf("'"));
  for (const result of ['success', 'failure', 'cancelled', 'skipped']) {
    let exitCode = 0;
    runInNewContext(source, {
      process: {
        env: { RESULTS: JSON.stringify({ node: { result }, rust: { result: 'success' } }) },
        exit(code: number) { exitCode = code; },
      },
      console: { error() {} },
    });
    assert.equal(exitCode, result === 'success' ? 0 : 1, result);
  }
});

test('the reusable source shard loads artifacts before verification and always cleans scoped resources', () => {
  const steps = workflow('ci-container.yml').jobs.shard?.steps ?? [];
  const start = steps.findIndex(({ run }) => run?.endsWith('tools-scripts:ci-start-artifact'));
  const verify = steps.findIndex(({ run }) => run?.endsWith('tools-scripts:ci-verify'));
  const importArtifacts = steps.findIndex(({ run }) => run?.endsWith('tools-scripts:ci-import-integration'));
  assert.ok(importArtifacts >= 0 && importArtifacts < start && start < verify);
  assert.ok(steps.some(({ run }) => run === 'docker load --input .temp/ci/environment/environment.tar.zst'));
  const cleanup = steps.find(({ run }) => run?.endsWith('tools-scripts:ci-cleanup'));
  assert.equal(cleanup?.if, 'always()');
  assert.ok(steps.some(({ with: options }) => options?.name === 'ci-delta-reader'));
  const compiledCache = steps.find(({ name }) => name === "Restore this shard's compiled Cargo cache");
  assert.equal(compiledCache?.with?.path, 'target/ci/build');
  assert.match(compiledCache?.if ?? '', /inputs\.tasks != ''/u);
  const sourceSave = steps.find(({ name }) => name === 'Save the shared Cargo source cache once');
  assert.match(sourceSave?.if ?? '', /inputs\.key == 'rust-checks'/u);
  assert.equal(sourceSave?.with?.path, 'target/ci/cargo/registry\ntarget/ci/cargo/git\n');
  for (const step of steps.filter(({ uses }) => uses)) {
    assert.match(step.uses ?? '', /^[A-Za-z0-9_/-]+@[a-f0-9]{40}$/u);
  }
});

test('the shared CI image and post-create use the same pinned cargo-make version', () => {
  const dockerfile = readFileSync(fileURLToPath(new URL('../../../.github/ci/Dockerfile', import.meta.url)), 'utf8');
  const postCreate = readFileSync(fileURLToPath(new URL('../../../.devcontainer/scripts/post-create.ts', import.meta.url)), 'utf8');
  const pin = postCreate.match(/const cargoMakeVersion = '([^']+)'/u)?.[1];
  assert.ok(pin);
  assert.ok(dockerfile.includes(`cargo install cargo-make --version ${pin} --locked`));
  assert.match(dockerfile, /CARGO_INCREMENTAL=0/u);
  assert.match(dockerfile, /CARGO_PROFILE_DEV_DEBUG=0/u);
  assert.match(dockerfile, /CARGO_PROFILE_TEST_DEBUG=0/u);
  assert.match(dockerfile, /-fuse-ld=mold/u);
});
