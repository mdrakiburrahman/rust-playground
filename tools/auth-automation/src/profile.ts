import { randomUUID } from "node:crypto";
import {
  mkdir,
  readFile,
  rm,
  writeFile,
} from "node:fs/promises";
import path from "node:path";
import type {
  BrowserKind,
  FileSystemAdapter,
  HostEnvironment,
} from "./contracts.js";

export interface BrowserProfile {
  readonly browserKind: BrowserKind;
  readonly profilePath: string;
}

function resolveApplicationCacheRoot(environment: HostEnvironment): string {
  const pathApi =
    environment.platform === "win32" ? path.win32 : path.posix;
  if (environment.platform === "win32") {
    return (
      environment.variables.LOCALAPPDATA ??
      pathApi.join(environment.homeDirectory(), "AppData", "Local")
    );
  }
  return (
    environment.variables.XDG_CACHE_HOME ??
    (environment.platform === "darwin"
      ? pathApi.join(environment.homeDirectory(), "Library", "Caches")
      : pathApi.join(environment.homeDirectory(), ".cache"))
  );
}

export function resolveBrowserProfile(
  environment: HostEnvironment,
): BrowserProfile {
  const pathApi =
    environment.platform === "win32" ? path.win32 : path.posix;
  if (environment.platform === "win32") {
    return {
      browserKind: "edge",
      profilePath: pathApi.join(
        resolveApplicationCacheRoot(environment),
        "rust-playground",
        "auth-automation",
        "msedge-profile",
      ),
    };
  }

  return {
    browserKind: "chromium",
    profilePath: pathApi.join(
      resolveApplicationCacheRoot(environment),
      "rust-playground",
      "auth-automation",
      "chromium-profile",
    ),
  };
}

export function resolveRuntimeRoot(environment: HostEnvironment): string {
  const pathApi =
    environment.platform === "win32" ? path.win32 : path.posix;
  return pathApi.join(
    resolveApplicationCacheRoot(environment),
    "rust-playground",
    "auth-automation",
    "runtime",
  );
}

export class NodeFileSystemAdapter implements FileSystemAdapter {
  async ensureDirectory(directoryPath: string): Promise<void> {
    await mkdir(directoryPath, { recursive: true });
  }

  async createUniqueDirectory(
    parentPath: string,
    prefix: string,
  ): Promise<string> {
    await mkdir(parentPath, { recursive: true });
    for (let attempt = 0; attempt < 5; attempt += 1) {
      const directoryPath = path.join(
        parentPath,
        `${prefix}${randomUUID()}`,
      );
      try {
        await mkdir(directoryPath);
        return directoryPath;
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code !== "EEXIST") {
          throw error;
        }
      }
    }
    throw new Error(
      "Unable to create a unique auth-automation runtime directory.",
    );
  }

  async writeTextFile(filePath: string, content: string): Promise<void> {
    await writeFile(filePath, content, {
      encoding: "utf8",
      flag: "wx",
    });
  }

  async readTextFile(filePath: string): Promise<string | undefined> {
    try {
      return await readFile(filePath, "utf8");
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code === "ENOENT") {
        return undefined;
      }
      throw error;
    }
  }

  async removeDirectory(directoryPath: string): Promise<void> {
    await rm(directoryPath, { recursive: true, force: true });
  }
}
