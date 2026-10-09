#!/usr/bin/env node

import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import {
  existsSync,
  readFileSync,
  realpathSync,
  writeFileSync,
} from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

import {
  composeFile,
  contentHashFile,
  imageRepository,
} from './config.ts';

const defaultRepositoryRoot = realpathSync(
  join(dirname(fileURLToPath(import.meta.url)), '..', '..'),
);
const imageBuildInputs = new Set([
  '.dockerignore',
  '.devcontainer/Dockerfile',
  '.devcontainer/devcontainer.build.json',
  '.devcontainer/devcontainer-lock.json',
]);
const imageLinePattern = new RegExp(
  `^(\\s*image:\\s*)${imageRepository.replaceAll('.', '\\.')}:[^\\s#]+(\\s*)$`,
  'gmu',
);

const usage = `Usage: node .devcontainer/scripts/content-hash.ts [--write|--check|--print]

Hashes Git-tracked and non-ignored candidate files under .devcontainer after
normalizing line endings. --write updates the checked-in hash and Compose image.
`;

export interface HashEntry {
  contents: string;
  path: string;
}

export interface ContentHashDependencies {
  fileExists(path: string): boolean;
  listFiles(repositoryRoot: string): readonly string[];
  readFile(path: string): string;
  stdout(message: string): void;
  writeFile(path: string, contents: string): void;
}

type ContentHashMode = 'check' | 'print' | 'write';

class UsageError extends Error {
  constructor(argument: string) {
    super(`Unknown argument: ${argument}`);
    this.name = 'UsageError';
  }
}

const defaultDependencies: ContentHashDependencies = {
  fileExists: existsSync,
  listFiles(repositoryRoot) {
    const result = spawnSync(
      'git',
      [
        'ls-files',
        '--cached',
        '--others',
        '--exclude-standard',
        '--',
        '.devcontainer',
        '.dockerignore',
      ],
      {
        cwd: repositoryRoot,
        encoding: 'utf8',
        stdio: ['ignore', 'pipe', 'pipe'],
      },
    );
    if (result.error) {
      throw new Error(`Unable to start git: ${result.error.message}`, {
        cause: result.error,
      });
    }
    if (result.status !== 0) {
      throw new Error(
        `git ls-files failed with exit code ${String(result.status)}: ${result.stderr.trim()}`,
      );
    }
    return result.stdout
      .split(/\r?\n/u)
      .map((path) => path.trim().replaceAll('\\', '/'))
      .filter((path) => path.length > 0);
  },
  readFile(path) {
    return readFileSync(path, 'utf8');
  },
  stdout(message) {
    process.stdout.write(message);
  },
  writeFile(path, contents) {
    writeFileSync(path, contents, 'utf8');
  },
};

export function normalizeText(contents: string): string {
  return contents.replace(/\r\n?/gu, '\n');
}

export function isContentHashInput(path: string): boolean {
  const normalized = path.replaceAll('\\', '/');
  return imageBuildInputs.has(normalized);
}

export function computeContentHash(entries: readonly HashEntry[]): string {
  const hash = createHash('sha256');
  const sortedEntries = [...entries].sort((left, right) => {
    if (left.path < right.path) {
      return -1;
    }
    if (left.path > right.path) {
      return 1;
    }
    return 0;
  });
  for (const entry of sortedEntries) {
    const normalizedPath = entry.path.replaceAll('\\', '/');
    const normalizedContents = normalizeText(entry.contents);
    hash.update(`${Buffer.byteLength(normalizedPath)}\0${normalizedPath}\0`);
    hash.update(`${Buffer.byteLength(normalizedContents)}\0`);
    hash.update(normalizedContents);
    hash.update('\0');
  }
  return hash.digest('hex');
}

export function renderComposeImage(
  composeContents: string,
  hash: string,
): string {
  let replacements = 0;
  const rendered = normalizeText(composeContents).replace(
    imageLinePattern,
    (_match, prefix: string, suffix: string) => {
      replacements += 1;
      return `${prefix}${imageRepository}:${hash}${suffix}`;
    },
  );
  if (replacements !== 1) {
    throw new Error(
      `${composeFile} must contain exactly one ${imageRepository} image reference.`,
    );
  }
  return rendered.endsWith('\n') ? rendered : `${rendered}\n`;
}

function computeRepositoryHash(
  repositoryRoot: string,
  dependencies: ContentHashDependencies,
): string {
  const files = [...new Set(dependencies.listFiles(repositoryRoot))]
    .map((path) => path.replaceAll('\\', '/'))
    .filter(isContentHashInput)
    .sort();
  if (files.length === 0) {
    throw new Error('No devcontainer hash inputs were discovered.');
  }
  return computeContentHash(
    files.map((path) => ({
      contents: dependencies.readFile(join(repositoryRoot, ...path.split('/'))),
      path,
    })),
  );
}

export function updateContentHash(
  mode: ContentHashMode,
  dependencies: ContentHashDependencies = defaultDependencies,
  repositoryRoot = defaultRepositoryRoot,
): string {
  const hash = computeRepositoryHash(repositoryRoot, dependencies);
  const hashPath = join(repositoryRoot, ...contentHashFile.split('/'));
  const composePath = join(repositoryRoot, ...composeFile.split('/'));
  const expectedHashContents = `${hash}\n`;
  const expectedComposeContents = renderComposeImage(
    dependencies.readFile(composePath),
    hash,
  );

  if (mode === 'write') {
    if (
      !dependencies.fileExists(hashPath) ||
      dependencies.readFile(hashPath) !== expectedHashContents
    ) {
      dependencies.writeFile(hashPath, expectedHashContents);
    }
    if (dependencies.readFile(composePath) !== expectedComposeContents) {
      dependencies.writeFile(composePath, expectedComposeContents);
    }
    dependencies.stdout(`devcontainer content hash: ${hash}\n`);
    return hash;
  }

  if (mode === 'check') {
    const stale: string[] = [];
    if (
      !dependencies.fileExists(hashPath) ||
      dependencies.readFile(hashPath) !== expectedHashContents
    ) {
      stale.push(contentHashFile);
    }
    if (dependencies.readFile(composePath) !== expectedComposeContents) {
      stale.push(composeFile);
    }
    if (stale.length > 0) {
      throw new Error(
        `Generated devcontainer references are stale: ${stale.join(', ')}. Run nx run devcontainer:tag.`,
      );
    }
  }

  dependencies.stdout(`${hash}\n`);
  return hash;
}

function parseMode(args: readonly string[]): ContentHashMode {
  if (args.length === 0) {
    return 'print';
  }
  if (args.length !== 1) {
    throw new UsageError(args.join(' '));
  }
  switch (args[0]) {
    case '--check':
      return 'check';
    case '--print':
      return 'print';
    case '--write':
      return 'write';
    case '-h':
    case '--help':
      process.stdout.write(usage);
      return 'print';
    default:
      throw new UsageError(args[0] ?? '');
  }
}

function isMainModule(): boolean {
  return (
    process.argv[1] !== undefined &&
    import.meta.url === pathToFileURL(resolve(process.argv[1])).href
  );
}

if (isMainModule()) {
  try {
    const args = process.argv.slice(2);
    if (args.length === 1 && (args[0] === '-h' || args[0] === '--help')) {
      process.stdout.write(usage);
    } else {
      updateContentHash(parseMode(args));
    }
  } catch (error) {
    if (error instanceof UsageError) {
      process.stderr.write(`${error.message}\n${usage}`);
      process.exitCode = 2;
    } else {
      const message = error instanceof Error ? error.message : String(error);
      process.stderr.write(`devcontainer-content-hash: ${message}\n`);
      process.exitCode = 1;
    }
  }
}
