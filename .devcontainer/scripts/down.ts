#!/usr/bin/env node

import { spawnSync } from 'node:child_process';
import { realpathSync } from 'node:fs';
import { dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const defaultRepositoryRoot = realpathSync(
  join(dirname(fileURLToPath(import.meta.url)), '..', '..'),
);

const usage = `Usage: node .devcontainer/scripts/down.ts [--volumes] [--dry-run]

Stops only Compose projects discovered through this workspace's devcontainer
working-directory label. Use --volumes to remove their named volumes.
`;

export interface DownOptions {
  dryRun: boolean;
  removeVolumes: boolean;
}

export interface DownDependencies {
  capture(command: string, args: readonly string[]): string;
  execute(command: string, args: readonly string[]): void;
  stderr(message: string): void;
  stdout(message: string): void;
}

interface ParsedArguments extends DownOptions {
  help: boolean;
}

class CommandError extends Error {
  readonly exitCode: number;

  constructor(command: string, exitCode: number | null) {
    super(`Command "${command}" failed with exit code ${String(exitCode)}.`);
    this.name = 'CommandError';
    this.exitCode = exitCode ?? 1;
  }
}

class UsageError extends Error {
  constructor(argument: string) {
    super(`Unknown argument: ${argument}`);
    this.name = 'UsageError';
  }
}

const defaultDependencies: DownDependencies = {
  capture(command, args) {
    const result = spawnSync(command, [...args], {
      encoding: 'utf8',
      stdio: ['ignore', 'pipe', 'inherit'],
    });
    if (result.error) {
      throw new Error(`Unable to start "${command}": ${result.error.message}`, {
        cause: result.error,
      });
    }
    if (result.status !== 0) {
      throw new CommandError(command, result.status);
    }
    return result.stdout;
  },
  execute(command, args) {
    const result = spawnSync(command, [...args], { stdio: 'inherit' });
    if (result.error) {
      throw new Error(`Unable to start "${command}": ${result.error.message}`, {
        cause: result.error,
      });
    }
    if (result.status !== 0) {
      throw new CommandError(command, result.status);
    }
  },
  stderr(message) {
    process.stderr.write(message);
  },
  stdout(message) {
    process.stdout.write(message);
  },
};

export function parseDownArguments(args: readonly string[]): ParsedArguments {
  const parsed: ParsedArguments = {
    dryRun: false,
    help: false,
    removeVolumes: false,
  };
  for (const argument of args) {
    switch (argument) {
      case '--volumes':
        parsed.removeVolumes = true;
        break;
      case '--dry-run':
        parsed.dryRun = true;
        break;
      case '-h':
      case '--help':
        return { ...parsed, help: true };
      default:
        throw new UsageError(argument);
    }
  }
  return parsed;
}

function formatCommand(command: string, args: readonly string[]): string {
  return [command, ...args]
    .map((argument) =>
      /^[A-Za-z0-9_./:=,\\-]+$/u.test(argument)
        ? argument
        : JSON.stringify(argument),
    )
    .join(' ');
}

export function downDevcontainer(
  options: DownOptions,
  dependencies: DownDependencies = defaultDependencies,
  repositoryRoot = defaultRepositoryRoot,
): void {
  const devcontainerDirectory = join(repositoryRoot, '.devcontainer');
  const composeFile = join(devcontainerDirectory, 'docker-compose.yml');
  const containerIds = dependencies
    .capture('docker', [
      'ps',
      '-aq',
      '--filter',
      `label=com.docker.compose.project.working_dir=${devcontainerDirectory}`,
    ])
    .split(/\r?\n/u)
    .map((value) => value.trim())
    .filter((value) => value.length > 0);

  if (containerIds.length === 0) {
    dependencies.stdout(`No devcontainer resources found for ${repositoryRoot}.\n`);
    return;
  }

  const composeProjects = new Set<string>();
  for (const containerId of containerIds) {
    const project = dependencies
      .capture('docker', [
        'inspect',
        '--format',
        '{{ index .Config.Labels "com.docker.compose.project" }}',
        containerId,
      ])
      .trim();
    if (project.length > 0 && project !== '<no value>') {
      composeProjects.add(project);
    } else {
      dependencies.stderr(
        `Skipping unlabeled container ${containerId}; it is not a Compose resource.\n`,
      );
    }
  }

  if (composeProjects.size === 0) {
    throw new Error('No Compose project labels were found; nothing was removed.');
  }

  for (const project of composeProjects) {
    const args = [
      'compose',
      '--project-name',
      project,
      '--project-directory',
      devcontainerDirectory,
      '--file',
      composeFile,
      'down',
      '--remove-orphans',
    ];
    if (options.removeVolumes) {
      args.push('--volumes');
    }

    if (options.dryRun) {
      dependencies.stdout(`Would stop Compose project: ${project}\n`);
      dependencies.stdout(`Would run: ${formatCommand('docker', args)}\n`);
    } else {
      dependencies.stdout(`Stopping Compose project: ${project}\n`);
      dependencies.execute('docker', args);
    }
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
    const options = parseDownArguments(process.argv.slice(2));
    if (options.help) {
      process.stdout.write(usage);
    } else {
      downDevcontainer(options);
    }
  } catch (error) {
    if (error instanceof UsageError) {
      process.stderr.write(`${error.message}\n${usage}`);
      process.exitCode = 2;
    } else {
      const message = error instanceof Error ? error.message : String(error);
      process.stderr.write(`devcontainer-down: ${message}\n`);
      process.exitCode = error instanceof CommandError ? error.exitCode : 1;
    }
  }
}
