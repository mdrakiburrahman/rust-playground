#!/usr/bin/env node

import { mkdirSync, writeFileSync } from 'node:fs';
import { homedir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const defaultRepositoryRoot = resolve(
  dirname(fileURLToPath(import.meta.url)),
  '..',
  '..',
);

export interface InitializeDependencies {
  createDirectory(path: string): void;
  homeDirectory(): string;
  writeFile(path: string, contents: string): void;
}

const defaultDependencies: InitializeDependencies = {
  createDirectory(path) {
    mkdirSync(path, { recursive: true });
  },
  homeDirectory: homedir,
  writeFile(path, contents) {
    writeFileSync(path, contents, 'utf8');
  },
};

function composeEnvironmentValue(value: string): string {
  if (/[\0\r\n]/u.test(value)) {
    throw new Error('Host home directory contains an unsupported character.');
  }

  const normalized = value.replaceAll('\\', '/');
  if (/^[A-Za-z0-9_./:-]+$/u.test(normalized)) {
    return normalized;
  }

  return `"${normalized
    .replaceAll('\\', '\\\\')
    .replaceAll('"', '\\"')
    .replaceAll('$', '$$')}"`;
}

export function initializeHost(
  dependencies: InitializeDependencies = defaultDependencies,
  repositoryRoot = defaultRepositoryRoot,
): void {
  const home = dependencies.homeDirectory().trim();
  if (home.length === 0) {
    throw new Error('Unable to determine the host home directory.');
  }

  const azureDirectory = join(home, '.azure');
  const githubDirectory = join(home, '.config', 'gh');
  dependencies.createDirectory(azureDirectory);
  dependencies.createDirectory(githubDirectory);

  const contents = [
    `HOST_AZURE_DIR=${composeEnvironmentValue(azureDirectory)}`,
    `HOST_GH_CONFIG_DIR=${composeEnvironmentValue(githubDirectory)}`,
    '',
  ].join('\n');
  dependencies.writeFile(join(repositoryRoot, '.devcontainer', '.env'), contents);
}

function isMainModule(): boolean {
  return (
    process.argv[1] !== undefined &&
    import.meta.url === pathToFileURL(resolve(process.argv[1])).href
  );
}

if (isMainModule()) {
  try {
    initializeHost();
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    process.stderr.write(`devcontainer-initialize: ${message}\n`);
    process.exitCode = 1;
  }
}
