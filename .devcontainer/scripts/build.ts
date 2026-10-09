#!/usr/bin/env node

import { spawnSync } from 'node:child_process';
import { realpathSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

import {
  imageBuildConfigFile,
  immutableImage,
  readContentHash,
  targetPlatform,
} from './config.ts';

const defaultRepositoryRoot = realpathSync(
  join(dirname(fileURLToPath(import.meta.url)), '..', '..'),
);

export interface BuildDependencies {
  execute(command: string, args: readonly string[], cwd: string): void;
  nodeExecutable: string;
  stdout(message: string): void;
}

export interface BuildInvocation {
  args: readonly string[];
  command: string;
}

const defaultDependencies: BuildDependencies = {
  execute(command, args, cwd) {
    const result = spawnSync(command, [...args], { cwd, stdio: 'inherit' });
    if (result.error) {
      throw new Error(`Unable to start "${command}": ${result.error.message}`, {
        cause: result.error,
      });
    }
    if (result.status !== 0) {
      throw new Error(
        `Command "${command}" failed with exit code ${String(result.status)}.`,
      );
    }
  },
  nodeExecutable: process.execPath,
  stdout(message) {
    process.stdout.write(message);
  },
};

export function createBuildInvocation(
  hash: string,
  push: boolean,
  repositoryRoot: string,
  nodeExecutable = process.execPath,
): BuildInvocation {
  const args = [
    join(
      repositoryRoot,
      'node_modules',
      '@devcontainers',
      'cli',
      'devcontainer.js',
    ),
    'build',
    '--workspace-folder',
    '.',
    '--config',
    imageBuildConfigFile,
    '--image-name',
    immutableImage(hash),
    '--platform',
    targetPlatform,
    '--frozen-lockfile',
  ];
  if (push) {
    args.push('--push', 'true');
  }
  return {
    args,
    command: nodeExecutable,
  };
}

export function buildDevcontainer(
  push: boolean,
  dependencies: BuildDependencies = defaultDependencies,
  repositoryRoot = defaultRepositoryRoot,
): void {
  const hash = readContentHash(repositoryRoot);
  const invocation = createBuildInvocation(
    hash,
    push,
    repositoryRoot,
    dependencies.nodeExecutable,
  );
  dependencies.stdout(
    `${push ? 'Building and pushing' : 'Building'} ${immutableImage(hash)} for ${targetPlatform}.\n`,
  );
  dependencies.execute(invocation.command, invocation.args, repositoryRoot);
}

function isMainModule(): boolean {
  return (
    process.argv[1] !== undefined &&
    import.meta.url === pathToFileURL(resolve(process.argv[1])).href
  );
}

if (isMainModule()) {
  try {
    if (process.argv.length > 2) {
      throw new Error('Usage: node .devcontainer/scripts/build.ts');
    }
    buildDevcontainer(false);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    process.stderr.write(`devcontainer-build: ${message}\n`);
    process.exitCode = 1;
  }
}
