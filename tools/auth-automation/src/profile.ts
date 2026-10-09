import { mkdir } from "node:fs/promises";
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

export function resolveBrowserProfile(
  environment: HostEnvironment,
): BrowserProfile {
  const pathApi =
    environment.platform === "win32" ? path.win32 : path.posix;
  if (environment.platform === "win32") {
    const basePath =
      environment.variables.LOCALAPPDATA ??
      pathApi.join(environment.homeDirectory(), "AppData", "Local");
    return {
      browserKind: "edge",
      profilePath: pathApi.join(
        basePath,
        "rust-playground",
        "auth-automation",
        "msedge-profile",
      ),
    };
  }

  const basePath =
    environment.variables.XDG_CACHE_HOME ??
    (environment.platform === "darwin"
      ? pathApi.join(environment.homeDirectory(), "Library", "Caches")
      : pathApi.join(environment.homeDirectory(), ".cache"));
  return {
    browserKind: "chromium",
    profilePath: pathApi.join(
      basePath,
      "rust-playground",
      "auth-automation",
      "chromium-profile",
    ),
  };
}

export class NodeFileSystemAdapter implements FileSystemAdapter {
  async ensureDirectory(directoryPath: string): Promise<void> {
    await mkdir(directoryPath, { recursive: true });
  }
}
