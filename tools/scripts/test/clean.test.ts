import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  readFileSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import test, { type TestContext } from 'node:test';
import { fileURLToPath } from 'node:url';

import { cleanProject } from '../src/clean.js';

function fixture(context: TestContext): string {
  const root = mkdtempSync(join(tmpdir(), 'rust-playground-clean-'));
  context.after(() => rmSync(root, { recursive: true, force: true }));
  return root;
}

function file(root: string, path: string, contents = 'keep or remove'): void {
  mkdirSync(dirname(join(root, path)), { recursive: true });
  writeFileSync(join(root, path), contents);
}

function project(root: string, path: string): void {
  file(root, join(path, 'project.json'), JSON.stringify({ name: path }));
}

// Scenario: loadtest owns local telemetry and conventional temporary directories.
// Guarantees: its clean removes only that state, preserving managed caches, configuration, and other projects.
test('cleans loadtest state without removing caches or another project', (context) => {
  const root = fixture(context);
  project(root, 'projects/loadtest');
  const removed = [
    'projects/loadtest/.temp/scratch',
    'projects/loadtest/.ci-logs/ci.log',
    'projects/loadtest/logs/demo.log',
    'projects/loadtest/coverage/result.json',
    'onelake/demo/logs/part.parquet',
    'onelake/e2e/run/inspection.json',
  ];
  const retained = [
    '.nx/cache/artifact',
    'node_modules/dependency/package.json',
    '.cargo/registry/cache/archive',
    'target/debug/collector',
    'target/images/hello-world-load.txt',
    'projects/loadtest/node_modules/dependency/package.json',
    'projects/loadtest/target/debug/reader',
    'projects/loadtest/.env',
    'projects/loadtest/dist/build.js',
    'projects/loadtest/out/build.js',
    'crates/greeting/logs/other.log',
    'logs/root.log',
    '.devcontainer/.env',
    '.cache/auth-automation/profile',
  ];
  for (const path of [...removed, ...retained]) file(root, path);
  const result = cleanProject('projects/loadtest', root);
  assert.deepEqual(result.sort(), [
    'projects/loadtest/.temp',
    'projects/loadtest/.ci-logs',
    'projects/loadtest/logs',
    'projects/loadtest/coverage',
    'onelake',
  ].sort());
  for (const path of removed) assert(!existsSync(join(root, path)), path);
  for (const path of retained) assert(existsSync(join(root, path)), path);
  assert.deepEqual(cleanProject('projects/loadtest', root), []);
});

// Scenario: root cleanup has completed its child targets.
// Guarantees: root-owned scratch/log/coverage is removed without sweeping child projects or caches.
test('root cleanup is scoped to root-owned temporary state', (context) => {
  const root = fixture(context);
  project(root, '.');
  for (const path of ['.temp/root', 'logs/root.log', 'coverage/root', '.ci-logs/root']) {
    file(root, path);
  }
  for (const path of ['.nx/cache/root', 'target/debug/root', 'onelake/demo/data', 'crates/greeting/logs/child']) {
    file(root, path);
  }
  assert.equal(cleanProject('.', root).length, 4);
  for (const path of ['.nx/cache/root', 'target/debug/root', 'onelake/demo/data', 'crates/greeting/logs/child']) {
    assert(existsSync(join(root, path)), path);
  }
  assert(existsSync(root));
});

// Scenario: callers attempt an outside-workspace, cache, or nonexistent project.
// Guarantees: validation fails before deleting any workspace state.
test('rejects unsafe project roots before deleting anything', (context) => {
  const root = fixture(context);
  project(root, '.');
  file(root, '.temp/sentinel');
  for (const path of ['..', '../outside', root, '', 'node_modules/nx', '.nx/cache', 'target', '.cargo', 'submodules/otel-arrow', 'missing']) {
    assert.throws(() => cleanProject(path, root), /./u, path);
    assert(existsSync(join(root, '.temp/sentinel')));
  }
});

// Scenario: a temporary directory is a symlink to a managed cache.
// Guarantees: cleanup unlinks the temporary entry but never follows it to cache contents.
test('does not follow temporary-directory symlinks', (context) => {
  const root = fixture(context);
  project(root, 'crates/greeting');
  file(root, '.nx/cache/sentinel');
  symlinkSync(join(root, '.nx/cache'), join(root, 'crates/greeting/logs'), 'junction');
  assert.deepEqual(cleanProject('crates/greeting', root), ['crates/greeting/logs']);
  assert(!existsSync(join(root, 'crates/greeting/logs')));
  assert(existsSync(join(root, '.nx/cache/sentinel')));
});

// Scenario: a project directory redirects through a symlink to another project.
// Guarantees: its temporary state is not removed through that redirected path.
test('rejects symlinked project directories and manifests', (context) => {
  const root = fixture(context);
  project(root, 'real');
  file(root, 'real/logs/sentinel');
  symlinkSync(join(root, 'real'), join(root, 'alias'), 'junction');
  assert.throws(() => cleanProject('alias', root), /not be a symlink/u);
  assert(existsSync(join(root, 'real/logs/sentinel')));
  mkdirSync(join(root, 'other'));
  symlinkSync(join(root, 'real/project.json'), join(root, 'other/project.json'));
  assert.throws(() => cleanProject('other', root), /regular project\.json/u);
});

// Scenario: the CLI is run from an Nx target's workspace cwd and given extra arguments.
// Guarantees: normal cleanup succeeds, malformed invocation fails, and caches are not affected.
test('CLI cleans the selected project and reports invalid arguments', (context) => {
  const root = fixture(context);
  project(root, 'crates/greeting');
  file(root, 'crates/greeting/.temp/scratch');
  file(root, 'node_modules/sentinel');
  const script = fileURLToPath(new URL('../src/clean.ts', import.meta.url));
  const run = (args: string[]) => spawnSync(
    process.execPath,
    ['--import', import.meta.resolve('tsx'), script, ...args],
    { cwd: root, encoding: 'utf8' },
  );
  const result = run(['crates/greeting']);
  assert.equal(result.status, 0, result.stderr);
  assert.match(result.stdout, /Removed temporary state/u);
  assert(!existsSync(join(root, 'crates/greeting/.temp')));
  assert(existsSync(join(root, 'node_modules/sentinel')));
  const invalid = run(['crates/greeting', 'node_modules']);
  assert.notEqual(invalid.status, 0);
  assert.match(invalid.stderr, /Usage:/u);
});

// Scenario: the workspace lists every registered Nx project.
// Guarantees: every project has an uncached clean, and the root aggregates all child clean targets.
test('every project exposes clean and the root includes every child', () => {
  const workspace = fileURLToPath(new URL('../../../', import.meta.url));
  const manifests = [
    'project.json',
    '.devcontainer/project.json',
    'bin/hello-world/project.json',
    'bin/quiver-cli/project.json',
    'crates/arrowcol-buffer/project.json',
    'crates/arrowcol-catalog/project.json',
    'crates/arrowcol-dataflow-components/project.json',
    'crates/arrowcol-flight/project.json',
    'crates/arrowcol-protocol/project.json',
    'crates/arrowcol-receipts/project.json',
    'crates/arrowcol-routing/project.json',
    'crates/arrowcol-sdk-gen/project.json',
    'crates/arrowcol-sql-server/project.json',
    'crates/arrow-deltalake/project.json',
    'crates/arrow-tenant-stream/project.json',
    'crates/delta-maintenance/project.json',
    'crates/greeting/project.json',
    'crates/otel-deltalake-exporter/project.json',
    'projects/arrowcol-dataflow-rust/project.json',
    'projects/arrowcol-router-rust/project.json',
    'projects/arrowcol-rust/project.json',
    'projects/kusto-explorer/project.json',
    'projects/kusto-bridge/project.json',
    'projects/loadtest/project.json',
    'projects/otelcol-rust/project.json',
    'tools/auth-automation/project.json',
    'tools/libs/arrowcol-sdk-gen/project.json',
    'tools/libs/rust-playground-build/project.json',
    'tools/scripts/project.json',
  ];
  const children: string[] = [];
  for (const manifest of manifests) {
    const config = JSON.parse(readFileSync(resolve(workspace, manifest), 'utf8'));
    const clean = config.targets.clean;
    assert(clean, `${config.name}:clean is missing`);
    assert.equal(clean.cache, false, config.name);
    assert.equal(clean.executor, 'nx:run-commands', config.name);
    assert.equal(clean.options.forwardAllArgs, false, config.name);
    assert.equal(clean.options.cwd, '{workspaceRoot}', config.name);
    const root = dirname(manifest);
    assert.equal(clean.options.command, `npx --no-install tsx tools/scripts/src/clean.ts ${root}`);
    if (manifest !== 'project.json') children.push(config.name);
  }
  const root = JSON.parse(readFileSync(join(workspace, 'project.json'), 'utf8'));
  assert.equal(root.targets.clean.dependsOn.length, 1);
  assert.equal(root.targets.clean.dependsOn[0].target, 'clean');
  assert.deepEqual([...root.targets.clean.dependsOn[0].projects].sort(), [...children].sort());
});
