import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { lstat, readFile, realpath } from "node:fs/promises";
import path from "node:path";

export interface FileFingerprint {
  sha256: string;
  executable: boolean;
}

export type Manifest = Record<string, FileFingerprint>;

async function fingerprint(root: string, relative: string): Promise<FileFingerprint> {
  const parts = relative.split("/");
  for (let index = 1; index < parts.length; index++) {
    const ancestor = await lstat(path.join(root, ...parts.slice(0, index)));
    if (!ancestor.isDirectory() || ancestor.isSymbolicLink()) {
      throw new Error(`not a regular file: ${relative}`);
    }
  }
  const file = path.join(root, relative);
  const metadata = await lstat(file);
  if (!metadata.isFile() || metadata.isSymbolicLink()) {
    throw new Error(`not a regular file: ${relative}`);
  }
  return {
    sha256: createHash("sha256").update(await readFile(file)).digest("hex"),
    executable: (metadata.mode & 0o111) !== 0,
  };
}

function safePath(relative: string): boolean {
  return relative !== "" && !relative.startsWith("/")
    && !relative.split("/").some((component) => component === "" || component === "." || component === "..");
}

/**
 * Check every tracked and non-ignored untracked candidate file against a trusted
 * snapshot. This is a path-level guard, not a production-region or sandbox check.
 */
export async function checkCandidatePaths(
  root: string,
  baseline: Manifest,
  allowed: readonly string[],
  submodules: Readonly<Record<string, string>> = {},
  stateDirectory?: string,
): Promise<string[]> {
  root = await realpath(root);
  const problems: string[] = [];
  if (stateDirectory !== undefined && (!/^\.darwin\/[A-Za-z0-9_-]+$/.test(stateDirectory)
    || Object.keys(baseline).some(relative => relative.startsWith(`${stateDirectory}/`))
    || Object.keys(submodules).some(relative => relative === stateDirectory || relative.startsWith(`${stateDirectory}/`)))) {
    throw new Error("invalid evaluator state directory");
  }
  const allowedPaths = new Set(allowed);
  for (const relative of allowedPaths) {
    if (!safePath(relative) || !Object.hasOwn(baseline, relative)) {
      throw new Error(`invalid allowed path: ${relative}`);
    }
  }
  const paths = execFileSync(
    "git", ["-C", root, "ls-files", "-z", "--cached", "--others", "--exclude-standard"],
    { encoding: "utf8", maxBuffer: 64 * 1024 * 1024 },
  ).split("\0").filter(Boolean);
  const seen = new Set<string>();
  for (const relative of new Set(paths)) {
    if (!safePath(relative)) throw new Error(`unsafe candidate path: ${relative}`);
    if (stateDirectory && relative.startsWith(`${stateDirectory}/`)) continue;
    seen.add(relative);
    if (Object.hasOwn(submodules, relative)) {
      try {
        const directory = path.join(root, relative);
        const stat = await lstat(directory);
        if (!stat.isDirectory() || stat.isSymbolicLink() || await realpath(directory) !== directory) {
          problems.push(`invalid submodule: ${relative}`);
          continue;
        }
        const git = (...args: string[]) => execFileSync(
          "git", ["-C", directory, ...args], { encoding: "utf8", stdio: ["ignore", "pipe", "ignore"] },
        ).trimEnd();
        if (git("rev-parse", "--show-toplevel") !== directory
          || git("rev-parse", "HEAD") !== submodules[relative]
          || git("status", "--porcelain", "--untracked-files=all", "--ignore-submodules=none") !== ""
          || git("submodule", "status", "--recursive").split("\n").some((line) => line && !line.startsWith(" "))) {
          problems.push(`changed or uninitialized submodule: ${relative}`);
        }
      } catch (error) {
        if (error instanceof Error && (("code" in error && error.code === "ENOENT")
          || ("status" in error && typeof error.status === "number"))) {
          problems.push(`unavailable submodule: ${relative}`);
          continue;
        }
        throw error;
      }
      continue;
    }
    if (!Object.hasOwn(baseline, relative)) {
      problems.push(`extra file: ${relative}`);
      continue;
    }
    let actual: FileFingerprint;
    try {
      actual = await fingerprint(root, relative);
    } catch (error) {
      if (error instanceof Error && "code" in error && error.code === "ENOENT") {
        problems.push(`missing file: ${relative}`);
        continue;
      }
      if (error instanceof Error && error.message === `not a regular file: ${relative}`) {
        problems.push(error.message);
        continue;
      }
      throw error;
    }
    const expected = baseline[relative];
    if (actual.executable !== expected.executable
      || (!allowedPaths.has(relative) && actual.sha256 !== expected.sha256)) {
      problems.push(`changed protected file: ${relative}`);
    }
  }
  for (const relative of Object.keys(baseline)) {
    if (!safePath(relative)) throw new Error(`unsafe baseline path: ${relative}`);
    if (!seen.has(relative)) problems.push(`missing file: ${relative}`);
  }
  for (const relative of Object.keys(submodules)) {
    if (!safePath(relative) || Object.hasOwn(baseline, relative)) {
      throw new Error(`invalid submodule path: ${relative}`);
    }
    if (!seen.has(relative)) problems.push(`missing submodule: ${relative}`);
  }
  return problems.sort();
}
