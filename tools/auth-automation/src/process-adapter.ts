import { spawn } from "node:child_process";
import type {
  CommandSpec,
  ProcessAdapter,
  ProcessOutputHandlers,
  ProcessResult,
  RunningProcess,
  TimerAdapter,
} from "./contracts.js";
import {
  NodeProcessTreeTerminator,
  type ProcessTreeTerminator,
} from "./process-tree.js";
import { withTimeout } from "./timing.js";

const MAX_CAPTURE_LENGTH = 64 * 1024;

function appendBounded(current: string, chunk: string): string {
  return `${current}${chunk}`.slice(-MAX_CAPTURE_LENGTH);
}

interface OutputRouter {
  push(chunk: string): void;
  flush(): void;
}

function createOutputRouter(
  marker: string | undefined,
  onProcessId: (processId: number) => void,
  emit: (chunk: string) => void,
): OutputRouter {
  if (!marker) {
    return {
      push: emit,
      flush: () => undefined,
    };
  }

  let pending = "";
  let markerReceived = false;
  const emitLine = (line: string): void => {
    const trimmed = line.trim();
    if (trimmed.startsWith(marker)) {
      const processIdText = trimmed.slice(marker.length);
      if (/^\d+$/.test(processIdText)) {
        onProcessId(Number(processIdText));
        markerReceived = true;
        return;
      }
    }
    emit(line);
  };

  return {
    push: (chunk) => {
      if (markerReceived) {
        emit(chunk);
        return;
      }
      pending += chunk;
      let lineEnd = pending.indexOf("\n");
      while (lineEnd >= 0) {
        const line = pending.slice(0, lineEnd + 1);
        pending = pending.slice(lineEnd + 1);
        emitLine(line);
        lineEnd = pending.indexOf("\n");
      }
      if (markerReceived && pending) {
        emit(pending);
        pending = "";
      }
    },
    flush: () => {
      if (pending) {
        emitLine(pending);
        pending = "";
      }
    },
  };
}

export class NodeProcessAdapter implements ProcessAdapter {
  constructor(
    private readonly timer: TimerAdapter,
    private readonly processTreeTerminator: ProcessTreeTerminator =
      new NodeProcessTreeTerminator(),
    private readonly platform: NodeJS.Platform = process.platform,
  ) {}

  start(
    command: CommandSpec,
    handlers: ProcessOutputHandlers = {},
  ): RunningProcess {
    const child = spawn(command.command, [...command.args], {
      detached: this.platform !== "win32",
      shell: false,
      windowsHide: true,
      stdio: ["ignore", "pipe", "pipe"],
    });

    let stdout = "";
    let stderr = "";
    let settled = false;
    let resolveProcessExited!: () => void;
    const processExited = new Promise<void>((resolve) => {
      resolveProcessExited = resolve;
    });
    let resolveWslProcessId:
      | ((processId: number | undefined) => void)
      | undefined;
    let wslProcessIdResolved = false;
    const wslProcessId =
      command.termination?.kind === "wsl"
        ? new Promise<number | undefined>((resolve) => {
            resolveWslProcessId = resolve;
          })
        : undefined;
    const recordWslProcessId = (processId: number): void => {
      if (!wslProcessIdResolved) {
        wslProcessIdResolved = true;
        resolveWslProcessId?.(processId);
      }
    };

    const stdoutRouter = createOutputRouter(
      undefined,
      recordWslProcessId,
      (chunk) => {
        stdout = appendBounded(stdout, chunk);
        handlers.onStdout?.(chunk);
      },
    );
    const stderrRouter = createOutputRouter(
      command.termination?.pidMarker,
      recordWslProcessId,
      (chunk) => {
        stderr = appendBounded(stderr, chunk);
        handlers.onStderr?.(chunk);
      },
    );

    child.stdout.setEncoding("utf8");
    child.stderr.setEncoding("utf8");
    child.stdout.on("data", (chunk: string) => {
      stdoutRouter.push(chunk);
    });
    child.stderr.on("data", (chunk: string) => {
      stderrRouter.push(chunk);
    });

    const completion = new Promise<ProcessResult>((resolve, reject) => {
      child.once("error", (error) => {
        if (!settled) {
          settled = true;
          stdoutRouter.flush();
          stderrRouter.flush();
          if (!wslProcessIdResolved) {
            wslProcessIdResolved = true;
            resolveWslProcessId?.(undefined);
          }
          resolveProcessExited();
          reject(
            new Error(`Unable to start ${command.label}: ${error.message}`),
          );
        }
      });
      child.once("close", (exitCode, signal) => {
        if (!settled) {
          settled = true;
          stdoutRouter.flush();
          stderrRouter.flush();
          if (!wslProcessIdResolved) {
            wslProcessIdResolved = true;
            resolveWslProcessId?.(undefined);
          }
          resolveProcessExited();
          resolve({ exitCode, signal, stdout, stderr });
        }
      });
    });
    let termination: Promise<void> | undefined;

    return {
      pid: child.pid,
      completion,
      terminate: async () => {
        if (!termination) {
          termination = (async () => {
            if (settled || child.pid === undefined) {
              await processExited;
              return;
            }
            await this.processTreeTerminator.terminate({
              rootPid: child.pid,
              platform: this.platform,
              command,
              processExited,
              wslProcessId,
            });
          })();
        }
        await termination;
      },
    };
  }

  async run(command: CommandSpec, timeoutMs: number): Promise<ProcessResult> {
    const running = this.start(command);
    return withTimeout(
      running.completion,
      timeoutMs,
      this.timer,
      `${command.label} timed out after ${timeoutMs}ms.`,
      () => running.terminate(),
    );
  }
}
