import assert from 'node:assert/strict';
import {
  existsSync,
  copyFileSync,
  mkdirSync,
  mkdtempSync,
  rmSync,
  symlinkSync,
  writeFileSync,
} from 'node:fs';
import { dirname, join } from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

import {
  CommandError,
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

test('repository ignores dependency and target aliases without hiding source', () => {
  const workspace = fileURLToPath(new URL('../../../', import.meta.url));
  const scratch = join(workspace, '.nx', 'worktree-verification-tests');
  mkdirSync(scratch, { recursive: true });
  const repository = mkdtempSync(join(scratch, 'alias-ignore-'));
  const runner = new SpawnCommandRunner();
  const git = (args: readonly string[]) => runChecked(
    runner, 'git', ['-C', repository, ...args],
    { stdout: 'capture', stderr: 'capture' },
  );
  try {
    git(['init', '--quiet']);
    copyFileSync(join(workspace, '.gitignore'), join(repository, '.gitignore'));
    for (const prefix of ['', 'nested/']) {
      mkdirSync(join(repository, prefix), { recursive: true });
      for (const name of ['node_modules', 'target']) {
        const relative = `${prefix}${name}`;
        const fixture = join(repository, relative);
        for (const kind of ['directory', 'symlink', 'file']) {
          if (kind === 'directory') mkdirSync(fixture);
          else if (kind === 'symlink') symlinkSync(repository, fixture, 'junction');
          else writeFileSync(fixture, 'generated output\n');
          assert.equal(git(['check-ignore', '--', relative]).stdout.trim(), relative, kind);
          rmSync(fixture, { recursive: kind === 'directory' });
        }
      }
    }
    for (const relative of ['source.rs', 'nested/source.rs', 'target-source.rs', 'node_modules-source.ts']) {
      writeFileSync(join(repository, relative), 'source remains visible\n');
      const result = runner.run('git', ['-C', repository, 'check-ignore', '--', relative], {
        stdout: 'capture', stderr: 'capture',
      });
      assert.equal(result.exitCode, 1, relative);
      assert.equal(result.stdout, '');
    }
    const status = git(['status', '--porcelain=v1', '--untracked-files=all']).stdout;
    assert.match(status, /\?\? source\.rs/u);
    assert.match(status, /\?\? nested\/source\.rs/u);
  } finally {
    rmSync(repository, { recursive: true, force: true });
  }
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
    success('diff --git a/staged.rs b/staged.rs\n'),
  ]);
  const output = new FakeOutput();

  assert.equal(verifyCleanWorktree(runner, output), false);
  assert.match(output.errors.join(''), /generated\.ts/u);
  assert.match(output.errors.join(''), /tracked\.rs/u);
  assert.match(output.errors.join(''), /diff --git/u);
  assert.match(output.errors.join(''), /Tracked staged diff/u);
  runner.assertComplete();
});

test('checks index, worktree, and submodules while honoring Git ignore rules', () => {
  const runner = new FakeRunner([success()]);

  verifyCleanWorktree(runner);

  assert.deepEqual(runner.calls[0], {
    command: 'git',
    args: [
      'status',
      '--porcelain=v1',
      '--untracked-files=normal',
      '--ignore-submodules=none',
    ],
    options: { stdout: 'capture' },
  });
  runner.assertComplete();
});

test('propagates Git failures instead of accepting an unknown worktree state', () => {
  const runner = new FakeRunner([{ exitCode: 7, stdout: '', stderr: 'Git failed' }]);
  assert.throws(
    () => verifyCleanWorktree(runner),
    (error: unknown) => error instanceof CommandError && error.exitCode === 7,
  );
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

    writeFileSync(trackedFile, 'staged\n');
    git(['add', 'tracked.txt']);
    const stagedOutput = new FakeOutput();
    assert.equal(verifyCleanWorktree(runner, stagedOutput, repository), false);
    assert.match(stagedOutput.errors.join(''), /Tracked staged diff/u);
    assert.match(stagedOutput.errors.join(''), /\+staged/u);

    // A worktree restored to HEAD must not hide a different staged version.
    writeFileSync(trackedFile, 'clean\n');
    assert.equal(verifyCleanWorktree(runner, new FakeOutput(), repository), false);
    git(['add', 'tracked.txt']);
    assert.equal(verifyCleanWorktree(runner, undefined, repository), true);

    rmSync(trackedFile);
    const deletedOutput = new FakeOutput();
    assert.equal(verifyCleanWorktree(runner, deletedOutput, repository), false);
    assert.match(deletedOutput.errors.join(''), /tracked\.txt/u);
    git(['add', 'tracked.txt']);
    assert.equal(verifyCleanWorktree(runner, new FakeOutput(), repository), false);
    writeFileSync(trackedFile, 'clean\n');
    git(['add', 'tracked.txt']);

    git(['mv', 'tracked.txt', 'renamed.txt']);
    const renamedOutput = new FakeOutput();
    assert.equal(verifyCleanWorktree(runner, renamedOutput, repository), false);
    assert.match(renamedOutput.errors.join(''), /renamed\.txt/u);
    git(['mv', 'renamed.txt', 'tracked.txt']);
    assert.equal(verifyCleanWorktree(runner, undefined, repository), true);

    writeFileSync(untrackedFile, 'staged addition\n');
    git(['add', 'generated.txt']);
    assert.equal(verifyCleanWorktree(runner, new FakeOutput(), repository), false);
  } finally {
    rmSync(repository, { force: true, recursive: true });
    rmSync(scratchRoot, { force: true, recursive: true });
  }

  assert.equal(existsSync(repository), false);
  assert.equal(existsSync(scratchRoot), false);
});
