import assert from 'node:assert/strict';
import { copyFileSync, mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { spawnSync } from 'node:child_process';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

import { readJsonFile, type TargetConfiguration } from '@nx/devkit';

import { createVerificationPlan, type VerificationGraph } from '../src/ci-plan.js';
import { runChecked, SpawnCommandRunner } from '../src/process.js';

const workspace = fileURLToPath(new URL('../../../', import.meta.url));

test('Markdown discovery includes new and hidden files but excludes third-party and ignored output', () => {
  const scratch = join(workspace, '.nx');
  mkdirSync(scratch, { recursive: true });
  const repository = mkdtempSync(join(scratch, 'markdown-lint-'));
  const binary = join(workspace, 'node_modules/markdownlint-cli2/markdownlint-cli2-bin.mjs');
  const write = (name: string, content: string): void => {
    const path = join(repository, name);
    mkdirSync(dirname(path), { recursive: true });
    writeFileSync(path, content);
  };
  const lint = (...args: string[]) => {
    const result = spawnSync(process.execPath, [binary, ...args], {
      cwd: repository,
      encoding: 'utf8',
    });
    assert.ifError(result.error);
    return { status: result.status, output: result.stdout + result.stderr };
  };

  try {
    runChecked(new SpawnCommandRunner(), 'git', ['init', '--quiet', repository]);
    copyFileSync(join(workspace, '.markdownlint-cli2.yaml'), join(repository, '.markdownlint-cli2.yaml'));
    write('.gitignore', 'generated/\n');
    const invalid = '#Missing space\n';
    for (const name of [
      'node_modules/package/README.md',
      'submodules/upstream/README.md',
      '.temp/output/README.md',
      'target/output/README.md',
      'dist/output/README.md',
      'coverage/output/README.md',
      'generated/README.md',
    ]) {
      write(name, invalid);
    }
    write('README.md', '# Valid\n\nDocumentation.\n');
    assert.equal(lint().status, 0);

    for (const name of [
      'docs/new/guide.md',
      '.github/new/guide.md',
      '.devcontainer/guide.markdown',
      'docs/UPPER.MD',
      'docs/UPPER.MARKDOWN',
    ]) {
      write(name, invalid);
      const result = lint();
      assert.equal(result.status, 1, result.output);
      assert.ok(result.output.includes(name), result.output);
      assert.equal(readFileSync(join(repository, name), 'utf8'), invalid);
      write(name, '# Valid\n\nDocumentation.\n');
      assert.equal(lint().status, 0);
    }

    write('.github/pull_request_template.md', '## Summary\n\n<!-- Describe the change. -->\n');
    assert.equal(lint().status, 0);
    write('docs/not-a-template.md', '## Summary\n');
    assert.equal(lint().status, 1);
    write('docs/not-a-template.md', '# Summary\n');

    write('docs/fix.md', '# Fix\n\nText with trailing spaces.  \n\n\n');
    const before = readFileSync(join(repository, 'docs/fix.md'), 'utf8');
    assert.equal(lint().status, 1);
    const fixed = lint('--fix');
    assert.equal(fixed.status, 0, fixed.output);
    assert.notEqual(readFileSync(join(repository, 'docs/fix.md'), 'utf8'), before);
    assert.equal(lint().status, 0);
  } finally {
    rmSync(repository, { recursive: true, force: true });
  }
});

test('root lint retains Clippy dependencies and adds uncached Markdown checks', () => {
  const root = readJsonFile<{ targets: Record<string, TargetConfiguration> }>(join(workspace, 'project.json'));
  const scripts = readJsonFile<{ targets: Record<string, TargetConfiguration> }>(join(workspace, 'tools/scripts/project.json'));
  const dependencies = root.targets.lint?.dependsOn;
  assert.ok(dependencies?.some((dependency) =>
    typeof dependency !== 'string' && dependency.target === 'markdown-lint' &&
    Array.isArray(dependency.projects) && dependency.projects.includes('tools-scripts')));
  assert.ok(dependencies?.some((dependency) =>
    typeof dependency !== 'string' && dependency.target === 'lint' &&
    Array.isArray(dependency.projects) && dependency.projects.includes('arrowcol-buffer')));
  assert.equal(scripts.targets['markdown-lint']?.cache, false);
  assert.equal(scripts.targets['markdown-lint-fix']?.cache, false);
  assert.deepEqual(scripts.targets['markdown-lint']?.options?.commands, [
    'npx --no-install tsc --project docs/tsconfig.json',
    'npx --no-install tsx --test docs/validate.test.ts',
    'npx --no-install tsx docs/validate.ts',
    'npx --no-install markdownlint-cli2',
  ]);
  assert.equal(scripts.targets['markdown-lint']?.options?.parallel, false);
  assert.equal(scripts.targets['markdown-lint-fix']?.options?.command, 'npx --no-install markdownlint-cli2 --fix');
  assert.equal(root.targets['fail-on-untracked-files']?.cache, false);
  assert.deepEqual(root.targets['fail-on-untracked-files']?.dependsOn, [
    { projects: ['tools-scripts'], target: 'clean-worktree' },
  ]);
});

test('the actual Nx verification graph schedules Markdown lint in a Node shard', () => {
  const scratch = join(workspace, '.temp');
  mkdirSync(scratch, { recursive: true });
  const directory = mkdtempSync(join(scratch, 'markdown-graph-'));
  const graphFile = join(directory, 'graph.json');
  try {
    runChecked(new SpawnCommandRunner(), 'npx', [
      '--no-install', 'nx', 'run-many', '-t', 'verify', '--all', `--graph=${graphFile}`,
    ], { cwd: workspace, stdout: 'capture', stderr: 'capture' });
    const plan = createVerificationPlan(readJsonFile<VerificationGraph>(graphFile));
    assert.ok(plan.node.some(({ tasks }) => tasks.includes('tools-scripts:markdown-lint')));
    assert.ok(plan.materialTasks.includes('tools-scripts:markdown-lint'));
    assert.ok(!plan.materialTasks.includes('tools-scripts:markdown-lint-fix'));
    assert.ok(!plan.materialTasks.includes('tools-scripts:clean-worktree'));
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});
