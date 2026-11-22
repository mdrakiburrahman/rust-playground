import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { lstat, readFile } from "node:fs/promises";
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
): Promise<string[]> {
  const problems: string[] = [];
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
    seen.add(relative);
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
  return problems.sort();
}
