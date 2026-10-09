import { spawnSync } from 'node:child_process';

export type CommandOutput = 'capture' | 'ignore' | 'inherit';

export interface CommandOptions {
  readonly cwd?: string;
  readonly env?: NodeJS.ProcessEnv;
  readonly input?: string;
  readonly stderr?: CommandOutput;
  readonly stdout?: CommandOutput;
}

export interface CommandResult {
  readonly exitCode: number;
  readonly stderr: string;
  readonly stdout: string;
}

export interface CommandRunner {
  run(
    command: string,
    args: readonly string[],
    options?: CommandOptions,
  ): CommandResult;
}

export class CommandError extends Error {
  readonly args: readonly string[];
  readonly command: string;
  readonly exitCode: number;

  constructor(command: string, args: readonly string[], exitCode: number) {
    super(
      `Command failed with exit code ${exitCode}: ${formatCommand(command, args)}`,
    );
    this.name = 'CommandError';
    this.args = [...args];
    this.command = command;
    this.exitCode = exitCode;
  }
}

export class SpawnCommandRunner implements CommandRunner {
  run(
    command: string,
    args: readonly string[],
    options: CommandOptions = {},
  ): CommandResult {
    const stdout = options.stdout ?? 'inherit';
    const stderr = options.stderr ?? 'inherit';
    const result = spawnSync(command, [...args], {
      cwd: options.cwd,
      encoding: 'utf8',
      env: options.env,
      input: options.input,
      shell: false,
      stdio: [
        options.input === undefined ? 'ignore' : 'pipe',
        toStdio(stdout),
        toStdio(stderr),
      ],
      windowsHide: true,
    });

    if (result.error) {
      throw new Error(`Unable to start "${command}": ${result.error.message}`, {
        cause: result.error,
      });
    }

    return {
      exitCode: result.status ?? 1,
      stderr: result.stderr ?? '',
      stdout: result.stdout ?? '',
    };
  }
}

export function runChecked(
  runner: CommandRunner,
  command: string,
  args: readonly string[],
  options?: CommandOptions,
): CommandResult {
  const result = runner.run(command, args, options);
  if (result.exitCode !== 0) {
    throw new CommandError(command, args, result.exitCode);
  }
  return result;
}

function toStdio(output: CommandOutput): 'ignore' | 'inherit' | 'pipe' {
  return output === 'capture' ? 'pipe' : output;
}

function formatCommand(command: string, args: readonly string[]): string {
  return [command, ...args].map((part) => JSON.stringify(part)).join(' ');
}
