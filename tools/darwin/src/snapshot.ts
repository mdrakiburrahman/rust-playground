import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { lstat, mkdir, readFile, realpath, unlink, writeFile } from "node:fs/promises";
import path from "node:path";
import type { Manifest } from "./paths.js";

export interface SnapshotManifest {
  commit: string;
  files: Manifest;
  submodules: Record<string, string>;
  submoduleFiles: Manifest;
}

function git(root: string, ...args: string[]): string {
  return execFileSync("git", ["-C", root, ...args], {
    encoding: "utf8", maxBuffer: 64 * 1024 * 1024,
  });
}

function safePath(value: string): boolean {
  return value !== "" && !value.startsWith("/")
    && !value.split("/").some((part) => part === "" || part === "." || part === "..");
}

interface TreeEntry {
  mode: string;
  oid: string;
  relative: string;
}

function tree(root: string, revision: string): TreeEntry[] {
  return git(root, "ls-tree", "-r", "-z", revision).split("\0").filter(Boolean).map((entry) => {
    const match = /^(100644|100755|160000) (?:blob|commit) ([0-9a-f]{40,64})\t(.+)$/.exec(entry);
    if (!match || !safePath(match[3])) throw new Error("unsupported or unsafe Git tree entry");
    return { mode: match[1], oid: match[2], relative: match[3] };
  });
}

async function archive(
  root: string,
  revision: string,
  source: string,
  archivePath: string,
  prefix: string,
  manifest: SnapshotManifest,
): Promise<void> {
  const entries = tree(root, revision);
  await mkdir(source, { recursive: true });
  execFileSync("git", ["-C", root, "archive", "--format=tar", "-o", archivePath, revision]);
  execFileSync("tar", ["-xf", archivePath, "-C", source]);
  await unlink(archivePath);
  for (const entry of entries) {
    const fullRelative = prefix ? `${prefix}/${entry.relative}` : entry.relative;
    if (entry.mode === "160000") {
      if (!prefix) manifest.submodules[fullRelative] = entry.oid;
      const child = path.join(root, entry.relative);
      if (await realpath(child) !== child || git(child, "rev-parse", "--show-toplevel").trim() !== child
        || git(child, "rev-parse", "HEAD").trim() !== entry.oid
        || git(child, "status", "--porcelain", "--untracked-files=all", "--ignore-submodules=none").trim()) {
        throw new Error(`unavailable, dirty or mismatched submodule: ${fullRelative}`);
      }
      await archive(child, entry.oid, path.join(source, entry.relative), archivePath, fullRelative, manifest);
      continue;
    }
    const target = path.join(source, entry.relative);
    const metadata = await lstat(target);
    if (!metadata.isFile() || metadata.isSymbolicLink()) throw new Error(`invalid archived file: ${fullRelative}`);
    const fingerprint = {
      sha256: createHash("sha256").update(await readFile(target)).digest("hex"),
      executable: entry.mode === "100755",
    };
    if (((metadata.mode & 0o111) !== 0) !== fingerprint.executable) {
      throw new Error(`invalid archived mode: ${fullRelative}`);
    }
    (prefix ? manifest.submoduleFiles : manifest.files)[fullRelative] = fingerprint;
  }
}

export async function snapshot(repo: string, home: string): Promise<SnapshotManifest> {
  repo = await realpath(repo);
  if (git(repo, "rev-parse", "--show-toplevel").trim() !== repo) {
    throw new Error("snapshot repository must be its Git root");
  }
  const status = git(repo, "status", "--porcelain=v1", "-z", "--untracked-files=normal", "--ignore-submodules=none");
  const changes = status.split("\0").filter(Boolean);
  if (changes.some((change) => change !== "?? .darwin/")) {
    throw new Error("snapshot requires clean committed source");
  }
  const commit = git(repo, "rev-parse", "HEAD").trim();
  const manifest: SnapshotManifest = { commit, files: {}, submodules: {}, submoduleFiles: {} };
  const homePath = path.resolve(home);
  await mkdir(homePath, { recursive: true });
  if (await realpath(homePath) !== homePath) throw new Error("snapshot home must not traverse symlinks");
  try {
    await lstat(path.join(homePath, "data"));
    throw new Error("snapshot home already contains Darwin data");
  } catch (error) {
    if (!(error instanceof Error && "code" in error && error.code === "ENOENT")) throw error;
  }
  const trusted = path.join(homePath, "trusted");
  await mkdir(trusted);
  await archive(repo, commit, path.join(trusted, "src"), path.join(trusted, "archive.tar"), "", manifest);
  await writeFile(path.join(trusted, "manifest.json"), `${JSON.stringify(manifest, null, 2)}\n`, { flag: "wx" });
  return manifest;
}
