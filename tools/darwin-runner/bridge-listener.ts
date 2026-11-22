import { chmod, lstat, realpath, unlink } from "node:fs/promises";
import net from "node:net";
import path from "node:path";
import type { ConfigValidator, Workload } from "./dispatch-contract.js";

const MAX_ACTIVE_CONNECTIONS = 16;
const MAX_RESPONSE_BYTES = 4096;
const REQUEST_TIMEOUT_MS = 5000;
const REJECTION = Buffer.from('{"version":1,"ok":false,"error":"request rejected"}');

type DispatchContract = typeof import("./dispatch-contract.js");

export interface TrustedBridgeOptions {
  socketPath: string;
  repoRoot: string;
  producerCommit: string;
  configPath: string;
  validateConfig: ConfigValidator;
  startWorkload: Workload;
}

export interface TrustedBridgeListener {
  close(): Promise<void>;
}

function errorCode(error: unknown): string | undefined {
  return typeof error === "object" && error !== null && "code" in error
    && typeof error.code === "string" ? error.code : undefined;
}

function inside(parent: string, child: string): boolean {
  const relative = path.relative(parent, child);
  return relative === "" || (relative !== ".." && !relative.startsWith(`..${path.sep}`) && !path.isAbsolute(relative));
}

async function assertSocketLocation(socketPath: string, repoRoot: string): Promise<void> {
  if (!path.isAbsolute(socketPath) || path.resolve(socketPath) !== socketPath) {
    throw new Error("trusted bridge socket path must be absolute and normalized");
  }
  const [canonicalRepo, canonicalDirectory] = await Promise.all([
    realpath(repoRoot),
    realpath(path.dirname(socketPath)),
  ]);
  if (inside(canonicalRepo, socketPath)) {
    throw new Error("trusted bridge socket must be outside the repository snapshot");
  }
  if (canonicalDirectory !== path.dirname(socketPath)) {
    throw new Error("trusted bridge socket directory may not contain symlinks");
  }
  const directory = await lstat(canonicalDirectory);
  const uid = process.getuid?.();
  if (!directory.isDirectory() || uid === undefined
    || (directory.mode & 0o022) !== 0 || (directory.uid !== 0 && directory.uid !== uid)) {
    throw new Error("trusted bridge socket directory must be owned and not group- or world-writable");
  }
  try {
    await lstat(socketPath);
    throw new Error("trusted bridge socket path already exists");
  } catch (error) {
    if (errorCode(error) !== "ENOENT") throw error;
  }
}

export async function startTrustedBridgeListener(options: TrustedBridgeOptions): Promise<TrustedBridgeListener> {
  await assertSocketLocation(options.socketPath, options.repoRoot);
  const contract = await import(new URL("./dispatch-contract.ts", import.meta.url).href) as DispatchContract;
  let openConnections = 0;
  let workloadActive = false;
  const server = net.createServer({ allowHalfOpen: true }, (socket) => {
    if (openConnections >= MAX_ACTIVE_CONNECTIONS) {
      socket.end(REJECTION);
      return;
    }
    openConnections++;
    let completed = false;
    let bytes = 0;
    const chunks: Buffer[] = [];
    const timer = setTimeout(() => {
      if (!completed) {
        completed = true;
        socket.end(REJECTION);
      }
    }, REQUEST_TIMEOUT_MS);

    socket.on("close", () => {
      clearTimeout(timer);
      openConnections--;
    });
    socket.on("error", (error) => {
      console.error(`trusted bridge socket connection failed: ${error.message}`);
    });
    socket.on("data", (chunk) => {
      if (completed) return;
      const data = Buffer.from(chunk);
      bytes += data.length;
      if (bytes > contract.MAX_REQUEST_BYTES) {
        completed = true;
        socket.end(REJECTION);
        return;
      }
      chunks.push(data);
    });
    socket.on("end", async () => {
      if (completed) return;
      completed = true;
      clearTimeout(timer);
      if (workloadActive) {
        socket.end(REJECTION);
        return;
      }
      workloadActive = true;
      try {
        const result = await contract.dispatchRequest(
          options.repoRoot,
          options.producerCommit,
          options.configPath,
          Buffer.concat(chunks),
          options.validateConfig,
          options.startWorkload,
        );
        const response = JSON.stringify({ version: 1, ok: true, result });
        if (response === undefined || Buffer.byteLength(response) > MAX_RESPONSE_BYTES) {
          throw new Error("trusted bridge response exceeds the size limit");
        }
        socket.end(response);
      } catch (error) {
        console.error(`trusted bridge request rejected: ${error instanceof Error ? error.message : "unknown error"}`);
        socket.end(REJECTION);
      } finally {
        workloadActive = false;
      }
    });
  });

  await new Promise<void>((resolve, reject) => {
    const onError = (error: Error) => {
      server.off("listening", onListening);
      reject(error);
    };
    const onListening = () => {
      server.off("error", onError);
      resolve();
    };
    server.once("error", onError);
    server.once("listening", onListening);
    server.listen(options.socketPath);
  });

  try {
    await chmod(options.socketPath, 0o666);
    const socket = await lstat(options.socketPath);
    if (!socket.isSocket()) throw new Error("trusted bridge path is not a Unix socket");
    return {
      close: async () => {
        await new Promise<void>((resolve, reject) => {
          server.close((error) => error ? reject(error) : resolve());
        });
        try {
          const current = await lstat(options.socketPath);
          if (current.isSocket() && current.dev === socket.dev && current.ino === socket.ino) {
            await unlink(options.socketPath);
          }
        } catch (error) {
          if (errorCode(error) !== "ENOENT") throw error;
        }
      },
    };
  } catch (error) {
    await new Promise<void>((resolve) => server.close(() => resolve()));
    throw error;
  }
}
