#!/usr/bin/env node

import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { realpathSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

import { createBuildInvocation } from './build.ts';
import {
  imageRepository,
  immutableImage,
  readContentHash,
} from './config.ts';

const defaultRepositoryRoot = realpathSync(
  join(dirname(fileURLToPath(import.meta.url)), '..', '..'),
);
const missingManifestPattern =
  /\b(?:manifest unknown|name unknown|no such manifest|manifest .* not found)\b/iu;
const branchAliasPrefix = 'branch-';
const maximumTagLength = 128;
const aliasHashLength = 12;

const usage = `Usage: node .devcontainer/scripts/publish.ts [--branch <name>]

Publishes the immutable content-hash image when absent, then updates the current
branch alias. Only the exact main branch updates main and latest; every other
branch uses the branch- namespace. Docker authentication must already be configured.
`;

export interface ProcessResult {
  status: number | null;
  stderr: string;
  stdout: string;
}

export interface PublishDependencies {
  capture(command: string, args: readonly string[], cwd: string): ProcessResult;
  environment: NodeJS.ProcessEnv;
  execute(command: string, args: readonly string[], cwd: string): void;
  nodeExecutable: string;
  sleep?(milliseconds: number): void;
  stdout(message: string): void;
}

export interface PublishOptions {
  branch?: string;
}

interface RemoteManifest {
  identity: string;
  reference: string;
}

class UsageError extends Error {
  constructor(message: string) {
    super(message);
    this.name = 'UsageError';
  }
}

const defaultDependencies: PublishDependencies = {
  capture(command, args, cwd) {
    const result = spawnSync(command, [...args], {
      cwd,
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'pipe'],
    });
    if (result.error) {
      throw new Error(`Unable to start "${command}": ${result.error.message}`, {
        cause: result.error,
      });
    }
    return {
      status: result.status,
      stderr: result.stderr,
      stdout: result.stdout,
    };
  },
  environment: process.env,
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
  sleep(milliseconds) {
    Atomics.wait(
      new Int32Array(new SharedArrayBuffer(4)),
      0,
      0,
      milliseconds,
    );
  },
  stdout(message) {
    process.stdout.write(message);
  },
};

function canonicalJson(value: unknown): string {
  if (Array.isArray(value)) {
    return `[${value.map(canonicalJson).join(',')}]`;
  }
  if (value !== null && typeof value === 'object') {
    const object = value as Record<string, unknown>;
    return `{${Object.keys(object)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${canonicalJson(object[key])}`)
      .join(',')}}`;
  }
  return JSON.stringify(value);
}

function manifestIdentity(manifest: unknown): string {
  if (manifest !== null && typeof manifest === 'object') {
    const object = manifest as Record<string, unknown>;
    const config = object.config;
    if (config !== null && typeof config === 'object') {
      const digest = (config as Record<string, unknown>).digest;
      if (typeof digest === 'string' && digest.length > 0) {
        return `config:${digest}`;
      }
    }
    if (Array.isArray(object.manifests)) {
      const descriptors = [...object.manifests].sort((left, right) => {
        const leftJson = canonicalJson(left);
        const rightJson = canonicalJson(right);
        return leftJson < rightJson ? -1 : leftJson > rightJson ? 1 : 0;
      });
      return `index:${canonicalJson(descriptors)}`;
    }
  }
  return `manifest:${canonicalJson(manifest)}`;
}

function inspectRemoteManifest(
  reference: string,
  dependencies: PublishDependencies,
  repositoryRoot: string,
): RemoteManifest | undefined {
  for (let attempt = 0; attempt < 3; attempt += 1) {
    const result = dependencies.capture(
      'docker',
      ['manifest', 'inspect', reference],
      repositoryRoot,
    );
    if (result.status === 0) {
      let manifest: unknown;
      try {
        manifest = JSON.parse(result.stdout);
      } catch (error) {
        throw new Error(`Registry returned invalid manifest JSON for ${reference}.`, {
          cause: error,
        });
      }
      return { identity: manifestIdentity(manifest), reference };
    }

    const errorOutput = `${result.stderr}\n${result.stdout}`.trim();
    if (missingManifestPattern.test(errorOutput)) {
      return undefined;
    }
    if (attempt < 2 && isTransientRegistryError(errorOutput)) {
      dependencies.sleep?.(1_000 * (attempt + 1));
      continue;
    }
    throw new Error(
      `Unable to inspect remote manifest ${reference} (exit ${String(result.status)}): ${errorOutput}`,
    );
  }
  throw new Error(`Unable to inspect remote manifest ${reference}.`);
}

function isTransientRegistryError(value: string): boolean {
  return /\b(?:EOF|connection reset|TLS handshake timeout|temporary failure|timeout|unexpected status(?: code)?:? 5\d\d)\b/iu.test(
    value,
  );
}

export function branchAlias(branch: string): string {
  return branchAliasForIdentity(normalizeBranchIdentity(branch));
}

export function publicationAliases(branch: string): readonly string[] {
  const identity = normalizeBranchIdentity(branch);
  return identity === 'main'
    ? ['main', 'latest']
    : [branchAliasForIdentity(identity)];
}

function normalizeBranchIdentity(branch: string): string {
  return branch.trim().replace(/^refs\/heads\//u, '');
}

function branchAliasForIdentity(identity: string): string {
  const slug = identity
    .toLowerCase()
    .replace(/[^a-z0-9_.-]+/gu, '-')
    .replace(/^[.-]+/u, '')
    .replace(/-+/gu, '-')
    .replace(/[.-]+$/u, '');
  if (slug.length === 0) {
    throw new Error(`Branch name cannot produce a container tag: ${identity}`);
  }

  const alias = `${branchAliasPrefix}${slug}`;
  if (alias.length <= maximumTagLength) {
    return alias;
  }

  const suffix = createHash('sha256')
    .update(identity)
    .digest('hex')
    .slice(0, aliasHashLength);
  const maximumSlugLength =
    maximumTagLength -
    branchAliasPrefix.length -
    1 -
    aliasHashLength;
  const truncatedSlug = slug
    .slice(0, maximumSlugLength)
    .replace(/[.-]+$/u, '');
  return `${branchAliasPrefix}${truncatedSlug}-${suffix}`;
}

function resolveBranch(
  requestedBranch: string | undefined,
  dependencies: PublishDependencies,
  repositoryRoot: string,
): string {
  if (requestedBranch !== undefined) {
    return requestedBranch;
  }
  const environmentBranch = dependencies.environment.GITHUB_REF_NAME?.trim();
  if (environmentBranch) {
    return environmentBranch;
  }
  const result = dependencies.capture(
    'git',
    ['branch', '--show-current'],
    repositoryRoot,
  );
  if (result.status !== 0) {
    throw new Error(
      `Unable to determine the current branch (exit ${String(result.status)}): ${result.stderr.trim()}`,
    );
  }
  const branch = result.stdout.trim();
  if (branch.length === 0) {
    throw new Error(
      'Cannot publish from a detached HEAD without --branch or GITHUB_REF_NAME.',
    );
  }
  return branch;
}

export function publishDevcontainer(
  options: PublishOptions,
  dependencies: PublishDependencies = defaultDependencies,
  repositoryRoot = defaultRepositoryRoot,
): void {
  const hash = readContentHash(repositoryRoot);
  const immutableReference = immutableImage(hash);
  let immutableManifest = inspectRemoteManifest(
    immutableReference,
    dependencies,
    repositoryRoot,
  );

  if (immutableManifest === undefined) {
    const build = createBuildInvocation(
      hash,
      true,
      repositoryRoot,
      dependencies.nodeExecutable,
    );
    dependencies.stdout(`Publishing missing image ${immutableReference}.\n`);
    dependencies.execute(build.command, build.args, repositoryRoot);
    immutableManifest = inspectRemoteManifest(
      immutableReference,
      dependencies,
      repositoryRoot,
    );
    if (immutableManifest === undefined) {
      throw new Error(
        `Image ${immutableReference} is still missing after the pushed build.`,
      );
    }
  } else {
    dependencies.stdout(`Image ${immutableReference} already exists; skipping push.\n`);
  }

  const aliases = publicationAliases(
    resolveBranch(options.branch, dependencies, repositoryRoot),
  );
  const staleAliases: string[] = [];
  for (const tag of aliases) {
    const reference = `${imageRepository}:${tag}`;
    const manifest = inspectRemoteManifest(reference, dependencies, repositoryRoot);
    if (manifest?.identity === immutableManifest.identity) {
      dependencies.stdout(`Alias ${reference} is current; skipping push.\n`);
    } else {
      staleAliases.push(reference);
    }
  }

  if (staleAliases.length === 0) {
    return;
  }

  const createArguments = [
    'buildx',
    'imagetools',
    'create',
    '--prefer-index=false',
  ];
  for (const reference of staleAliases) {
    createArguments.push('--tag', reference);
  }
  createArguments.push(immutableReference);
  dependencies.execute('docker', createArguments, repositoryRoot);

  for (const reference of staleAliases) {
    const manifest = inspectRemoteManifest(reference, dependencies, repositoryRoot);
    if (manifest?.identity !== immutableManifest.identity) {
      throw new Error(`Published alias ${reference} does not match ${immutableReference}.`);
    }
  }
}

function parseArguments(args: readonly string[]): PublishOptions | 'help' {
  if (args.length === 0) {
    return {};
  }
  if (args.length === 1 && (args[0] === '-h' || args[0] === '--help')) {
    return 'help';
  }
  if (args.length === 2 && args[0] === '--branch' && args[1]?.length) {
    return { branch: args[1] };
  }
  throw new UsageError(`Invalid arguments: ${args.join(' ')}`);
}

function isMainModule(): boolean {
  return (
    process.argv[1] !== undefined &&
    import.meta.url === pathToFileURL(resolve(process.argv[1])).href
  );
}

if (isMainModule()) {
  try {
    const options = parseArguments(process.argv.slice(2));
    if (options === 'help') {
      process.stdout.write(usage);
    } else {
      publishDevcontainer(options);
    }
  } catch (error) {
    if (error instanceof UsageError) {
      process.stderr.write(`${error.message}\n${usage}`);
      process.exitCode = 2;
    } else {
      const message = error instanceof Error ? error.message : String(error);
      process.stderr.write(`devcontainer-publish: ${message}\n`);
      process.exitCode = 1;
    }
  }
}
