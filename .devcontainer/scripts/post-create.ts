#!/usr/bin/env node

import { spawnSync } from 'node:child_process';
import { accessSync, constants, realpathSync } from 'node:fs';
import { delimiter, dirname, join, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

const expectedNodeMajor = 24;
const expectedRustVersion = '1.96.1';
const cargoMakeVersion = '0.37.24';
const defaultRepositoryRoot = realpathSync(
  join(dirname(fileURLToPath(import.meta.url)), '..', '..'),
);

const usage = `Usage: node .devcontainer/scripts/post-create.ts [--dry-run]

Validates the pinned Rust and Node.js toolchains, verifies rustfmt and Clippy,
installs the pinned cargo-make release when needed, and finally runs npm ci.
`;

export interface PostCreateOptions {
  dryRun: boolean;
}

export interface PostCreateDependencies {
  capture(command: string, args: readonly string[], cwd: string): string;
  commandExists(command: string): boolean;
  execute(command: string, args: readonly string[], cwd: string): void;
  nodeVersion: string;
  stdout(message: string): void;
}

interface ParsedArguments extends PostCreateOptions {
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

const defaultDependencies: PostCreateDependencies = {
  capture(command, args, cwd) {
    const result = spawnSync(command, [...args], {
      cwd,
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
  commandExists(command) {
    for (const pathEntry of (process.env.PATH ?? '').split(delimiter)) {
      if (pathEntry.length === 0) {
        continue;
      }
      try {
        accessSync(resolve(pathEntry, command), constants.X_OK);
        return true;
      } catch {
        // Continue through PATH entries; absence is expected.
      }
    }
    return false;
  },
  execute(command, args, cwd) {
    const result = spawnSync(command, [...args], { cwd, stdio: 'inherit' });
    if (result.error) {
      throw new Error(`Unable to start "${command}": ${result.error.message}`, {
        cause: result.error,
      });
    }
    if (result.status !== 0) {
      throw new CommandError(command, result.status);
    }
  },
  nodeVersion: process.version,
  stdout(message) {
    process.stdout.write(message);
  },
};

export function parsePostCreateArguments(
  args: readonly string[],
): ParsedArguments {
  const parsed: ParsedArguments = { dryRun: false, help: false };
  for (const argument of args) {
    switch (argument) {
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

export function runPostCreate(
  options: PostCreateOptions,
  dependencies: PostCreateDependencies = defaultDependencies,
  repositoryRoot = defaultRepositoryRoot,
): void {
  if (options.dryRun) {
    dependencies.stdout(`Would require: rustc ${expectedRustVersion}\n`);
    dependencies.stdout(`Would require: Node.js ${expectedNodeMajor}.x\n`);
    dependencies.stdout('Would verify: rustfmt and cargo clippy\n');
    dependencies.stdout(
      `Would ensure: cargo-make ${cargoMakeVersion} (installed with --locked)\n`,
    );
    dependencies.stdout('Would normalize: node_modules ownership\n');
    dependencies.stdout('Would trust: the mounted Git workspace\n');
    dependencies.stdout('Would run: npm ci\n');
    return;
  }

  const rustcOutput = dependencies.capture(
    'rustc',
    ['--version'],
    repositoryRoot,
  ).trim();
  const rustVersion = rustcOutput.split(/\s+/u)[1];
  if (rustVersion !== expectedRustVersion) {
    throw new Error(`Expected Rust ${expectedRustVersion}, found: ${rustcOutput}`);
  }

  if (!dependencies.nodeVersion.startsWith(`v${expectedNodeMajor}.`)) {
    throw new Error(
      `Expected Node.js ${expectedNodeMajor}, found: ${dependencies.nodeVersion}`,
    );
  }

  dependencies.capture('rustfmt', ['--version'], repositoryRoot);
  dependencies.capture('cargo', ['clippy', '--version'], repositoryRoot);

  let installCargoMake = !dependencies.commandExists('cargo-make');
  let forceCargoMakeInstall = false;
  if (!installCargoMake) {
    const installedVersion = dependencies
      .capture('cargo-make', ['--version'], repositoryRoot)
      .trim();
    if (!new RegExp(`\\b${cargoMakeVersion.replaceAll('.', '\\.')}\\b`, 'u').test(
      installedVersion,
    )) {
      installCargoMake = true;
      forceCargoMakeInstall = true;
    }
  }

  if (installCargoMake) {
    const args = [
      'install',
      'cargo-make',
      '--version',
      cargoMakeVersion,
      '--locked',
    ];
    if (forceCargoMakeInstall) {
      args.push('--force');
    }
    dependencies.execute('cargo', args, repositoryRoot);
  }

  const userId = numericId(
    dependencies.capture('id', ['-u'], repositoryRoot),
    'user ID',
  );
  const groupId = numericId(
    dependencies.capture('id', ['-g'], repositoryRoot),
    'group ID',
  );
  dependencies.execute(
    'sudo',
    [
      'chown',
      '-R',
      `${userId}:${groupId}`,
      join(repositoryRoot, 'node_modules'),
      join(repositoryRoot, '.nx'),
      join(repositoryRoot, 'target'),
    ],
    repositoryRoot,
  );
  dependencies.execute(
    'git',
    ['config', '--global', '--add', 'safe.directory', repositoryRoot],
    repositoryRoot,
  );
  dependencies.execute('npm', ['ci'], repositoryRoot);
}

function numericId(value: string, description: string): string {
  const normalized = value.trim();
  if (!/^\d+$/u.test(normalized)) {
    throw new Error(`Unable to determine the container ${description}.`);
  }
  return normalized;
}

function isMainModule(): boolean {
  return (
    process.argv[1] !== undefined &&
    import.meta.url === pathToFileURL(resolve(process.argv[1])).href
  );
}

if (isMainModule()) {
  try {
    const options = parsePostCreateArguments(process.argv.slice(2));
    if (options.help) {
      process.stdout.write(usage);
    } else {
      runPostCreate(options);
    }
  } catch (error) {
    if (error instanceof UsageError) {
      process.stderr.write(`${error.message}\n${usage}`);
      process.exitCode = 2;
    } else {
      const message = error instanceof Error ? error.message : String(error);
      process.stderr.write(`post-create: ${message}\n`);
      process.exitCode = error instanceof CommandError ? error.exitCode : 1;
    }
  }
}
