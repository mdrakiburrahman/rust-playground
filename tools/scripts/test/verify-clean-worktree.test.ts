import assert from 'node:assert/strict';
import {
  existsSync,
  mkdirSync,
  mkdtempSync,
  rmSync,
  writeFileSync,
} from 'node:fs';
import { dirname, join } from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

import {
  type CommandOptions,
  type CommandResult,
  type CommandRunner,
  runChecked,
  SpawnCommandRunner,
} from '../src/process.js';
import {
  verifyCleanWorktree,
  type WorktreeOutput,
} from '../src/verify-clean-worktree.js';

interface CommandCall {
  readonly args: readonly string[];
  readonly command: string;
  readonly options?: CommandOptions;
}

class FakeRunner implements CommandRunner {
  readonly calls: CommandCall[] = [];

  constructor(private readonly results: CommandResult[]) {}

  run(
    command: string,
    args: readonly string[],
    options?: CommandOptions,
  ): CommandResult {
    this.calls.push({ args: [...args], command, options });
    const result = this.results.shift();
    assert.ok(result, `Unexpected command: ${command} ${args.join(' ')}`);
    return result;
  }

  assertComplete(): void {
    assert.equal(this.results.length, 0);
  }
}

class FakeOutput implements WorktreeOutput {
  readonly errors: string[] = [];

  stderr(message: string): void {
    this.errors.push(message);
  }
}

const success = (stdout = ''): CommandResult => ({
  exitCode: 0,
  stderr: '',
  stdout,
});

test('accepts a clean worktree', () => {
  const runner = new FakeRunner([success()]);
  const output = new FakeOutput();

  assert.equal(verifyCleanWorktree(runner, output), true);
  assert.deepEqual(output.errors, []);
  runner.assertComplete();
});

test('reports tracked and untracked changes plus the tracked diff', () => {
  const runner = new FakeRunner([
    success('generated.ts\ntracked.rs\n'),
    success('diff --git a/tracked.rs b/tracked.rs\n'),
  ]);
  const output = new FakeOutput();

  assert.equal(verifyCleanWorktree(runner, output), false);
  assert.match(output.errors.join(''), /generated\.ts/u);
  assert.match(output.errors.join(''), /tracked\.rs/u);
  assert.match(output.errors.join(''), /diff --git/u);
  runner.assertComplete();
});

test('honors repository ignore rules through git ls-files', () => {
  const runner = new FakeRunner([success()]);

  verifyCleanWorktree(runner);

  assert.deepEqual(runner.calls[0], {
    command: 'git',
    args: [
      'ls-files',
      '--other',
      '--modified',
      '--directory',
      '--exclude-standard',
      '--no-empty-directory',
    ],
    options: { stdout: 'capture' },
  });
  runner.assertComplete();
});

test('detects real artifacts, ignores ignored files, and cleans its fixture', () => {
  const workspaceRoot = fileURLToPath(new URL('../../../', import.meta.url));
  const scratchRoot = join(
    workspaceRoot,
    '.nx',
    'worktree-verification-tests',
  );
  mkdirSync(scratchRoot, { recursive: true });
  const repository = mkdtempSync(join(scratchRoot, 'repository-'));
  const runner = new SpawnCommandRunner();
  const trackedFile = join(repository, 'tracked.txt');
  const untrackedFile = join(repository, 'generated.txt');
  const ignoredFile = join(repository, 'logs', 'verification.log');

  const git = (args: readonly string[]): void => {
    runChecked(runner, 'git', ['-C', repository, ...args], {
      stderr: 'capture',
      stdout: 'capture',
    });
  };

  try {
    git(['init', '--quiet']);
    writeFileSync(join(repository, '.gitignore'), 'logs/\n');
    writeFileSync(trackedFile, 'clean\n');
    git(['add', '.']);
    git([
      '-c',
      'commit.gpgSign=false',
      '-c',
      'user.name=Clean Worktree Test',
      '-c',
      'user.email=clean-worktree@example.invalid',
      'commit',
      '--quiet',
      '-m',
      'baseline',
    ]);

    assert.equal(verifyCleanWorktree(runner, undefined, repository), true);

    mkdirSync(dirname(ignoredFile), { recursive: true });
    writeFileSync(ignoredFile, 'ignored\n');
    assert.equal(verifyCleanWorktree(runner, undefined, repository), true);

    writeFileSync(trackedFile, 'modified\n');
    const trackedOutput = new FakeOutput();
    assert.equal(
      verifyCleanWorktree(runner, trackedOutput, repository),
      false,
    );
    assert.match(trackedOutput.errors.join(''), /tracked\.txt/u);
    assert.match(trackedOutput.errors.join(''), /diff --git/u);

    writeFileSync(trackedFile, 'clean\n');
    writeFileSync(untrackedFile, 'untracked\n');
    const untrackedOutput = new FakeOutput();
    assert.equal(
      verifyCleanWorktree(runner, untrackedOutput, repository),
      false,
    );
    assert.match(untrackedOutput.errors.join(''), /generated\.txt/u);

    rmSync(untrackedFile);
    assert.equal(verifyCleanWorktree(runner, undefined, repository), true);
  } finally {
    rmSync(repository, { force: true, recursive: true });
    rmSync(scratchRoot, { force: true, recursive: true });
  }

  assert.equal(existsSync(repository), false);
  assert.equal(existsSync(scratchRoot), false);
});
