#!/usr/bin/env node

import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

import { Command } from 'commander';

import {
  createRegistryTagMetadata,
  inspectPackageVisibility,
  loginToRegistry,
  RegistryCommandError,
  resolveExecutionEnvironment,
  verifyRemoteManifest,
  type PackageNeedsUiChangeResult,
  type RegistryExecutionEnvironment,
  type RegistryImageInput,
} from './registry-lib.js';
import {
  type CommandRunner,
  SpawnCommandRunner,
} from './process.js';

export interface RegistryCliDependencies {
  readonly env: Readonly<NodeJS.ProcessEnv>;
  readonly runner: CommandRunner;
  readonly stderr: (message: string) => void;
  readonly stdout: (message: string) => void;
}

interface EnvironmentOptions {
  readonly environment?: string;
}

interface OwnerOptions extends EnvironmentOptions {
  readonly owner?: string;
}

interface ImageOptions extends OwnerOptions {
  readonly image?: string;
  readonly repository?: string;
}

interface TagOptions extends EnvironmentOptions {
  readonly branch?: string;
  readonly defaultBranch?: string;
  readonly sha?: string;
}

interface ManifestOptions extends ImageOptions {
  readonly tag?: string;
}

export class RegistryNeedsUiChangeError extends Error {
  readonly exitCode = 2;
  readonly result: PackageNeedsUiChangeResult;

  constructor(result: PackageNeedsUiChangeResult) {
    super(
      `GHCR package ${result.owner}/${result.packageName} requires a package settings UI visibility change.`,
    );
    this.name = 'RegistryNeedsUiChangeError';
    this.result = result;
  }
}

export function createRegistryProgram(
  dependencies: RegistryCliDependencies,
): Command {
  const program = new Command()
    .name('registry')
    .description('Manage rust-playground GHCR authentication and metadata.')
    .showHelpAfterError();

  program
    .command('login')
    .description('Authenticate Docker to ghcr.io without exposing the token.')
    .option('--owner <owner>', 'GitHub user or organization that owns images.')
    .option('--environment <environment>', 'Execution environment: ci or local.')
    .action((options: OwnerOptions) => {
      const environment = resolveEnvironment(options, dependencies.env);
      const result = loginToRegistry(
        {
          env: dependencies.env,
          environment,
          owner: resolveOwner(options.owner, dependencies.env),
        },
        dependencies.runner,
      );
      writeJson(dependencies.stdout, result);
    });

  program
    .command('tags')
    .description('Emit branch, immutable Git SHA, and latest tag metadata.')
    .option('--branch <branch>', 'Source branch name.')
    .option('--sha <sha>', 'Full Git commit object ID.')
    .option(
      '--default-branch <branch>',
      'Default branch that also publishes latest.',
    )
    .option('--environment <environment>', 'Execution environment: ci or local.')
    .action((options: TagOptions) => {
      const environment = resolveEnvironment(options, dependencies.env);
      const result = createRegistryTagMetadata({
        branch: resolveBranch(
          options.branch,
          environment,
          dependencies.env,
          dependencies.runner,
        ),
        defaultBranch:
          firstValue(
            options.defaultBranch,
            dependencies.env.REGISTRY_DEFAULT_BRANCH,
          ) ?? 'main',
        gitSha: resolveGitSha(
          options.sha,
          environment,
          dependencies.env,
          dependencies.runner,
        ),
      });
      writeJson(dependencies.stdout, result);
    });

  program
    .command('manifest')
    .description('Verify that a remote GHCR image tag has a manifest.')
    .option('--owner <owner>', 'GitHub user or organization that owns images.')
    .option('--repository <repository>', 'GitHub repository name.')
    .option('--image <image>', 'Repository-relative container image name.')
    .option('--tag <tag>', 'Expected remote container tag.')
    .option('--environment <environment>', 'Execution environment: ci or local.')
    .action((options: ManifestOptions) => {
      resolveEnvironment(options, dependencies.env);
      const result = verifyRemoteManifest(
        {
          ...resolveImage(options, dependencies.env),
          tag: requiredValue(
            firstValue(options.tag, dependencies.env.REGISTRY_TAG),
            '--tag or REGISTRY_TAG',
          ),
        },
        dependencies.runner,
      );
      writeJson(dependencies.stdout, result);
    });

  program
    .command('public')
    .description(
      'Verify public package visibility or report the required UI change.',
    )
    .option('--owner <owner>', 'GitHub user that owns the package.')
    .option('--repository <repository>', 'GitHub repository name.')
    .option('--image <image>', 'Repository-relative container image name.')
    .option('--environment <environment>', 'Execution environment: ci or local.')
    .action((options: ImageOptions) => {
      resolveEnvironment(options, dependencies.env);
      const result = inspectPackageVisibility(
        resolveImage(options, dependencies.env),
        dependencies.runner,
      );
      if (result.status === 'needs-ui-change') {
        throw new RegistryNeedsUiChangeError(result);
      }
      writeJson(dependencies.stdout, result);
    });

  return program;
}

export async function runRegistryCli(
  argv: readonly string[],
  dependencies: RegistryCliDependencies,
): Promise<void> {
  try {
    await createRegistryProgram(dependencies).parseAsync([...argv]);
  } catch (error) {
    if (error instanceof RegistryNeedsUiChangeError) {
      writeJson(dependencies.stdout, error.result);
      process.exitCode = error.exitCode;
      return;
    }

    const message = error instanceof Error ? error.message : String(error);
    dependencies.stderr(`registry: ${message}\n`);
    process.exitCode =
      error instanceof RegistryCommandError ? error.exitCode : 1;
  }
}

function resolveEnvironment(
  options: EnvironmentOptions,
  env: Readonly<NodeJS.ProcessEnv>,
): RegistryExecutionEnvironment {
  return resolveExecutionEnvironment(options.environment, env);
}

function resolveImage(
  options: ImageOptions,
  env: Readonly<NodeJS.ProcessEnv>,
): RegistryImageInput {
  const repositoryCoordinates = githubRepository(env.GITHUB_REPOSITORY);
  return {
    image: requiredValue(
      firstValue(options.image, env.REGISTRY_IMAGE),
      '--image or REGISTRY_IMAGE',
    ),
    owner: resolveOwner(options.owner, env),
    repository: requiredValue(
      firstValue(
        options.repository,
        env.REGISTRY_REPOSITORY,
        repositoryCoordinates?.repository,
      ),
      '--repository, REGISTRY_REPOSITORY, or GITHUB_REPOSITORY',
    ),
  };
}

function resolveOwner(
  explicitOwner: string | undefined,
  env: Readonly<NodeJS.ProcessEnv>,
): string {
  return requiredValue(
    firstValue(
      explicitOwner,
      env.REGISTRY_OWNER,
      env.GITHUB_REPOSITORY_OWNER,
      githubRepository(env.GITHUB_REPOSITORY)?.owner,
    ),
    '--owner, REGISTRY_OWNER, GITHUB_REPOSITORY_OWNER, or GITHUB_REPOSITORY',
  );
}

function resolveBranch(
  explicitBranch: string | undefined,
  environment: RegistryExecutionEnvironment,
  env: Readonly<NodeJS.ProcessEnv>,
  runner: CommandRunner,
): string {
  const branch = firstValue(
    explicitBranch,
    env.REGISTRY_BRANCH,
    env.GITHUB_HEAD_REF,
    env.GITHUB_REF_NAME,
  );
  if (branch !== undefined) {
    return branch;
  }
  if (environment === 'ci') {
    throw new RegistryCommandError(
      'CI tag metadata requires --branch, REGISTRY_BRANCH, GITHUB_HEAD_REF, or GITHUB_REF_NAME.',
    );
  }
  return readGitValue(
    runner,
    ['branch', '--show-current'],
    'Unable to determine the local branch; pass --branch explicitly.',
  );
}

function resolveGitSha(
  explicitSha: string | undefined,
  environment: RegistryExecutionEnvironment,
  env: Readonly<NodeJS.ProcessEnv>,
  runner: CommandRunner,
): string {
  const sha = firstValue(explicitSha, env.REGISTRY_SHA, env.GITHUB_SHA);
  if (sha !== undefined) {
    return sha;
  }
  if (environment === 'ci') {
    throw new RegistryCommandError(
      'CI tag metadata requires --sha, REGISTRY_SHA, or GITHUB_SHA.',
    );
  }
  return readGitValue(
    runner,
    ['rev-parse', 'HEAD'],
    'Unable to determine the local Git SHA; pass --sha explicitly.',
  );
}

function readGitValue(
  runner: CommandRunner,
  args: readonly string[],
  errorMessage: string,
): string {
  const result = runner.run('git', args, {
    stderr: 'capture',
    stdout: 'capture',
  });
  const value = firstValue(result.stdout);
  if (result.exitCode !== 0 || value === undefined) {
    throw new RegistryCommandError(errorMessage, result.exitCode || 1);
  }
  return value;
}

function githubRepository(
  value: string | undefined,
): { readonly owner: string; readonly repository: string } | undefined {
  const normalized = firstValue(value);
  if (normalized === undefined) {
    return undefined;
  }
  const parts = normalized.split('/');
  if (parts.length !== 2 || !parts[0] || !parts[1]) {
    throw new RegistryCommandError(
      `Invalid GITHUB_REPOSITORY "${normalized}"; expected owner/repository.`,
    );
  }
  return {
    owner: parts[0],
    repository: parts[1],
  };
}

function requiredValue(
  value: string | undefined,
  sourceDescription: string,
): string {
  if (value === undefined) {
    throw new RegistryCommandError(`${sourceDescription} must be set.`);
  }
  return value;
}

function firstValue(
  ...values: ReadonlyArray<string | undefined>
): string | undefined {
  for (const value of values) {
    const normalized = value?.trim();
    if (normalized) {
      return normalized;
    }
  }
  return undefined;
}

function writeJson(
  write: (message: string) => void,
  value: unknown,
): void {
  write(`${JSON.stringify(value)}\n`);
}

function isMainModule(): boolean {
  return (
    process.argv[1] !== undefined &&
    import.meta.url === pathToFileURL(resolve(process.argv[1])).href
  );
}

if (isMainModule()) {
  await runRegistryCli(process.argv, {
    env: process.env,
    runner: new SpawnCommandRunner(),
    stderr: (message) => process.stderr.write(message),
    stdout: (message) => process.stdout.write(message),
  });
}
