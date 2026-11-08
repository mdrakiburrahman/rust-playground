import { spawn, spawnSync } from 'node:child_process';

export async function runStreamingTree(program: string, args: string[], options: {
  timeoutMs: number;
  capture: boolean;
  onLine?: (line: string) => void;
  onInterrupt?: () => void;
  failureDetail?: (stderr: string) => string;
}): Promise<string> {
  if (process.platform === 'win32') throw new Error('Process-tree execution requires a Linux environment.');
  return new Promise((resolve, reject) => {
    const child = spawn(program, args, { stdio: ['ignore', 'pipe', 'pipe'], shell: false, detached: true });
    let output = '';
    let pending = '';
    let stderr = '';
    let stderrOverflow = false;
    let failure: string | undefined;
    let killTimer: NodeJS.Timeout | undefined;
    const signalChild = (signal: NodeJS.Signals) => {
      try {
        if (child.pid) process.kill(-child.pid, signal);
      } catch (error) {
        const code = (error as NodeJS.ErrnoException).code;
        if (code !== 'ESRCH') failure = `process-group signal failed: ${code ?? 'unknown'}`;
      }
    };
    const terminate = () => {
      signalChild('SIGTERM');
      killTimer ??= setTimeout(() => signalChild('SIGKILL'), 360_000);
    };
    const timer = setTimeout(() => { failure = 'timeout'; terminate(); }, options.timeoutMs);
    const onSignal = () => {
      failure = 'interrupted';
      options.onInterrupt?.();
      terminate();
    };
    process.on('SIGINT', onSignal);
    process.on('SIGTERM', onSignal);
    child.stdout.on('data', (chunk: Buffer) => {
      if (options.onLine) {
        pending += chunk.toString('utf8');
        const lines = pending.split('\n');
        pending = lines.pop()!.slice(-4096);
        for (const line of lines) {
          try { options.onLine(line); }
          catch { failure = 'receipt write failed'; terminate(); }
        }
      }
      if (!options.capture) return;
      output += chunk.toString('utf8');
      if (output.length > 16 * 1024 * 1024) { failure = 'output limit'; terminate(); }
    });
    child.stderr.on('data', (chunk: Buffer) => {
      if (!options.failureDetail || stderrOverflow) return;
      stderr += chunk.toString('utf8');
      if (stderr.length > 64 * 1024) { stderr = ''; stderrOverflow = true; }
    });
    child.on('error', error => { failure = `unable to start: ${(error as NodeJS.ErrnoException).code ?? 'unknown'}`; });
    child.on('close', (code, signal) => {
      clearTimeout(timer);
      if (killTimer) clearTimeout(killTimer);
      process.off('SIGINT', onSignal);
      process.off('SIGTERM', onSignal);
      if (failure || code !== 0) {
        let detail = '';
        try { detail = options.failureDetail?.(stderrOverflow ? 'stderr exceeded capture limit.' : stderr).slice(0, 2000) ?? ''; }
        catch { detail = 'Failure detail unavailable.'; }
        reject(new Error(`${program} failed (${failure ?? signal ?? code ?? 'spawn error'}).${detail ? ` ${detail}` : ''}`));
      }
      else resolve(output);
    });
  });
}

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
