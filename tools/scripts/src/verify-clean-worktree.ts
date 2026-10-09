#!/usr/bin/env node

import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

import {
  CommandError,
  type CommandRunner,
  runChecked,
  SpawnCommandRunner,
} from './process.js';

export interface WorktreeOutput {
  stderr(message: string): void;
}

const systemOutput: WorktreeOutput = {
  stderr(message) {
    process.stderr.write(message);
  },
};

export function verifyCleanWorktree(
  runner: CommandRunner,
  output: WorktreeOutput = systemOutput,
  repository?: string,
): boolean {
  const gitArgs = (args: readonly string[]): string[] =>
    repository === undefined ? [...args] : ['-C', repository, ...args];

  const changes = runChecked(
    runner,
    'git',
    gitArgs([
      'ls-files',
      '--other',
      '--modified',
      '--directory',
      '--exclude-standard',
      '--no-empty-directory',
    ]),
    { stdout: 'capture' },
  ).stdout.trim();

  if (!changes) {
    return true;
  }

  output.stderr(
    `Verification left tracked modifications or unexpected untracked files:\n${changes}\n`,
  );

  const diff = runChecked(
    runner,
    'git',
    gitArgs(['--no-pager', 'diff', '--no-ext-diff', '--']),
    { stdout: 'capture' },
  ).stdout.trim();
  if (diff) {
    output.stderr(`\nTracked worktree diff:\n${diff}\n`);
  }

  return false;
}

function isMainModule(): boolean {
  return (
    process.argv[1] !== undefined &&
    import.meta.url === pathToFileURL(resolve(process.argv[1])).href
  );
}

if (isMainModule()) {
  try {
    if (!verifyCleanWorktree(new SpawnCommandRunner())) {
      process.exitCode = 1;
    }
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    process.stderr.write(`verify-clean-worktree: ${message}\n`);
    process.exitCode = error instanceof CommandError ? error.exitCode : 1;
  }
}
