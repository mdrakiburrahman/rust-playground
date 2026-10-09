#!/usr/bin/env node

import { resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

import { Command } from 'commander';

import {
  cleanupDevcontainer,
  createCiLabel,
  type DevcontainerKind,
  prepareCiEnvironment,
  startDevcontainer,
  systemFileSystem,
  type CiFileSystem,
  verifyDevcontainer,
} from './ci-lib.js';
import {
  CommandError,
  type CommandRunner,
  SpawnCommandRunner,
} from './process.js';

const repositoryRoot = fileURLToPath(new URL('../../..', import.meta.url));

export interface CiCliDependencies {
  readonly env: Readonly<NodeJS.ProcessEnv>;
  readonly fileSystem: CiFileSystem;
  readonly repositoryRoot: string;
  readonly runner: CommandRunner;
  readonly stderr: (message: string) => void;
  readonly stdout: (message: string) => void;
}

export function createCiProgram(dependencies: CiCliDependencies): Command {
  const program = new Command()
    .name('ci')
    .description('Run the rust-playground CI devcontainer lifecycle.')
    .showHelpAfterError();

  program
    .command('prepare')
    .description('Create only the host credential state required by Compose.')
    .action(() => {
      prepareCiEnvironment({
        fileSystem: dependencies.fileSystem,
        home: requiredEnvironment('HOME', dependencies.env),
        workspace: workflowWorkspace(dependencies),
      });
    });

  addStartCommand(program, 'source', dependencies);
  addStartCommand(program, 'published', dependencies);

  program
    .command('verify')
    .description('Run every Nx verification target in the exact CI container.')
    .action(() => {
      verifyDevcontainer(
        {
          containerId: requiredEnvironment(
            'CONTAINER_ID',
            dependencies.env,
          ),
        },
        dependencies.runner,
      );
    });

  program
    .command('cleanup')
    .description('Remove only the recorded or labeled CI Compose resources.')
    .action(() => {
      cleanupDevcontainer(
        {
          ciLabels: cleanupLabels(dependencies.env),
          composeProject: dependencies.env.COMPOSE_PROJECT,
          containerId: dependencies.env.CONTAINER_ID,
          workspace: workflowWorkspace(dependencies),
        },
        dependencies.runner,
        dependencies.fileSystem,
      );
    });

  return program;
}

export async function runCiCli(
  argv: readonly string[],
  dependencies: CiCliDependencies,
): Promise<void> {
  try {
    await createCiProgram(dependencies).parseAsync([...argv]);
  } catch (error) {
    const message = error instanceof Error ? error.message : String(error);
    dependencies.stderr(`ci: ${message}\n`);
    process.exitCode = error instanceof CommandError ? error.exitCode : 1;
  }
}

function addStartCommand(
  program: Command,
  kind: DevcontainerKind,
  dependencies: CiCliDependencies,
): void {
  program
    .command(`start-${kind}`)
    .description(
      kind === 'source'
        ? 'Build and start the source devcontainer Compose override.'
        : 'Start the immutable published devcontainer image.',
    )
    .action(() => {
      startDevcontainer(
        {
          githubOutput: requiredEnvironment(
            'GITHUB_OUTPUT',
            dependencies.env,
          ),
          kind,
          runAttempt: requiredEnvironment(
            'GITHUB_RUN_ATTEMPT',
            dependencies.env,
          ),
          runId: requiredEnvironment('GITHUB_RUN_ID', dependencies.env),
          workspace: workflowWorkspace(dependencies),
        },
        dependencies.runner,
        dependencies.fileSystem,
        dependencies.stdout,
      );
    });
}

function cleanupLabels(
  env: Readonly<NodeJS.ProcessEnv>,
): readonly string[] {
  const explicitLabel = normalizeOptional(env.CI_LABEL);
  if (explicitLabel !== undefined) {
    return [explicitLabel];
  }

  const runId = normalizeOptional(env.GITHUB_RUN_ID);
  const runAttempt = normalizeOptional(env.GITHUB_RUN_ATTEMPT);
  if (runId === undefined && runAttempt === undefined) {
    return [];
  }
  if (runId === undefined || runAttempt === undefined) {
    throw new Error(
      'GITHUB_RUN_ID and GITHUB_RUN_ATTEMPT must both be set for label-based cleanup.',
    );
  }
  return [
    createCiLabel(runId, runAttempt, 'source'),
    createCiLabel(runId, runAttempt, 'published'),
  ];
}

function workflowWorkspace(dependencies: CiCliDependencies): string {
  return (
    normalizeOptional(dependencies.env.GITHUB_WORKSPACE) ??
    dependencies.repositoryRoot
  );
}

function requiredEnvironment(
  name: string,
  env: Readonly<NodeJS.ProcessEnv>,
): string {
  const value = normalizeOptional(env[name]);
  if (value === undefined) {
    throw new Error(`${name} must be set.`);
  }
  return value;
}

function normalizeOptional(value: string | undefined): string | undefined {
  const normalized = value?.trim();
  return normalized ? normalized : undefined;
}

function isMainModule(): boolean {
  return (
    process.argv[1] !== undefined &&
    import.meta.url === pathToFileURL(resolve(process.argv[1])).href
  );
}

if (isMainModule()) {
  await runCiCli(process.argv, {
    env: process.env,
    fileSystem: systemFileSystem,
    repositoryRoot,
    runner: new SpawnCommandRunner(),
    stderr: (message) => process.stderr.write(message),
    stdout: (message) => process.stdout.write(message),
  });
}
