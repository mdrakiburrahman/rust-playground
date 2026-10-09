import type { TimerAdapter } from "./contracts.js";

export class SystemTimer implements TimerAdapter {
  now(): number {
    return Date.now();
  }

  setTimeout(callback: () => void, delayMs: number): unknown {
    return globalThis.setTimeout(callback, delayMs);
  }

  clearTimeout(handle: unknown): void {
    globalThis.clearTimeout(handle as NodeJS.Timeout);
  }

  sleep(delayMs: number): Promise<void> {
    return new Promise((resolve) => {
      globalThis.setTimeout(resolve, delayMs);
    });
  }
}

export async function withTimeout<T>(
  promise: Promise<T>,
  timeoutMs: number,
  timer: TimerAdapter,
  message: string,
  onTimeout?: () => void | Promise<void>,
): Promise<T> {
  if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
    await onTimeout?.();
    throw new Error(message);
  }

  let timeoutHandle: unknown;
  const timeout = new Promise<{ readonly kind: "timeout" }>((resolve) => {
    timeoutHandle = timer.setTimeout(() => {
      resolve({ kind: "timeout" });
    }, timeoutMs);
  });

  try {
    const outcome = await Promise.race([
      promise.then((value) => ({ kind: "value" as const, value })),
      timeout,
    ]);
    if (outcome.kind === "timeout") {
      try {
        await onTimeout?.();
      } catch (error) {
        const detail = error instanceof Error ? error.message : String(error);
        throw new Error(`${message} Process cleanup failed: ${detail}`, {
          cause: error,
        });
      }
      throw new Error(message);
    }
    return outcome.value;
  } finally {
    if (timeoutHandle !== undefined) {
      timer.clearTimeout(timeoutHandle);
    }
  }
}
