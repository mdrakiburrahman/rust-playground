import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { isAbsolute, join, resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const LANES = Object.freeze({
  "layout-sort-finish-v1": {
    path: "crates/arrow-deltalake/src/layout.rs", item: "FileSortBuffer::finish", mode: "100644",
  },
  "maintenance-active-apply-v1": {
    path: "crates/arrow-deltalake/src/maintenance.rs", item: "DeltaTable::apply_prepared_optimize", mode: "100644",
  },
  "maintenance-idle-vacuum-v1": {
    path: "crates/arrow-deltalake/src/maintenance.rs", item: "DeltaTable::vacuum_once", mode: "100644",
  },
} as const);

export type SourceLane = keyof typeof LANES;
type BodySpan = { start: number; end: number };
type SourceIdentity = { commit: string; tree: string; blob: string; sourceSha256: string; body: BodySpan };
export type VerifiedSource = {
  version: 1;
  verdict: "verified-source";
  lane: SourceLane;
  path: string;
  item: string;
  mode: string;
  seed: SourceIdentity;
  candidate: SourceIdentity;
  unchangedPrefixSha256: string;
  unchangedSuffixSha256: string;
};

const MAX_SOURCE_BYTES = 1_048_576;
const MAX_TREE_BYTES = 64 * 1024 * 1024;
const PARSER = fileURLToPath(new URL("./parser/target/release/darwin-region-parser", import.meta.url));

function git(repo: string, args: string[], maxBuffer = MAX_SOURCE_BYTES + 1): Buffer {
  const env = Object.fromEntries(Object.entries(process.env).filter(([key]) => !key.toUpperCase().startsWith("GIT_")));
  return execFileSync("git", ["--no-replace-objects", "-C", repo, ...args], {
    env: { ...env, GIT_NO_REPLACE_OBJECTS: "1", GIT_OPTIONAL_LOCKS: "0",
      GIT_CONFIG_NOSYSTEM: "1", GIT_CONFIG_GLOBAL: "/dev/null", GIT_GRAFT_FILE: "/dev/null" },
    timeout: 10_000,
    maxBuffer,
    stdio: ["ignore", "pipe", "pipe"],
  });
}

function readEntry(repo: string, commit: string, lane: SourceLane) {
  const spec = LANES[lane];
  if (!/^[0-9a-f]{40}$/.test(commit)) throw new Error("commit must be a full lowercase SHA-1");
  if (git(repo, ["cat-file", "-t", commit], 1024).toString().trim() !== "commit") {
    throw new Error("input does not identify a commit");
  }
  const entry = git(repo, ["ls-tree", "-z", commit, "--", spec.path], 4096).toString("utf8");
  const match = /^([0-7]{6}) (blob|tree|commit) ([0-9a-f]{40})\t([^\0]+)\0$/.exec(entry);
  if (!match || match[1] !== spec.mode || match[2] !== "blob" || match[4] !== spec.path) {
    throw new Error("allowlisted path must be one regular non-executable blob with unchanged mode");
  }
  const blob = match[3];
  const sizeText = git(repo, ["cat-file", "-s", blob], 1024).toString().trim();
  if (!/^[0-9]+$/.test(sizeText)) throw new Error("invalid Git blob size");
  const size = Number(sizeText);
  if (!Number.isSafeInteger(size) || size < 1 || size > MAX_SOURCE_BYTES) {
    throw new Error("source blob must contain 1..=1048576 bytes");
  }
  const bytes = git(repo, ["cat-file", "blob", blob]);
  if (bytes.length !== size) throw new Error("Git blob size mismatch");
  return { blob, bytes };
}

function bodySpan(source: Buffer, lane: SourceLane): BodySpan {
  const output = execFileSync(PARSER, [lane], {
    input: source, timeout: 10_000, maxBuffer: 4096, stdio: ["pipe", "pipe", "pipe"],
  }).toString("ascii");
  const match = /^([0-9]+):([0-9]+)\n$/.exec(output);
  if (!match) throw new Error("invalid trusted parser response");
  const start = Number(match[1]);
  const end = Number(match[2]);
  if (!Number.isSafeInteger(start) || !Number.isSafeInteger(end)
    || start < 1 || end < start || end >= source.length
    || source[start - 1] !== 123 || source[end] !== 125) {
    throw new Error("invalid trusted parser source span");
  }
  return { start, end };
}

function digest(bytes: Buffer): string {
  return createHash("sha256").update(bytes).digest("hex");
}

function region(repo: string, seedCommit: string, candidateCommit: string, lane: SourceLane) {
  const seed = readEntry(repo, seedCommit, lane);
  const candidate = readEntry(repo, candidateCommit, lane);
  const seedBody = bodySpan(seed.bytes, lane);
  const candidateBody = bodySpan(candidate.bytes, lane);
  if (!seed.bytes.subarray(0, seedBody.start).equals(candidate.bytes.subarray(0, candidateBody.start))
    || !seed.bytes.subarray(seedBody.end).equals(candidate.bytes.subarray(candidateBody.end))) {
    throw new Error("changes outside the allowlisted method body");
  }
  return {
    seed, candidate, seedBody, candidateBody,
    unchangedPrefixSha256: digest(seed.bytes.subarray(0, seedBody.start)),
    unchangedSuffixSha256: digest(seed.bytes.subarray(seedBody.end)),
  };
}

/** Additional file-region gate; never substitutes for the runner's whole-tree preflight. */
export function checkRegion(repo: string, seedCommit: string, candidateCommit: string) {
  const lane = "layout-sort-finish-v1";
  const proof = region(repo, seedCommit, candidateCommit, lane);
  return {
    version: 1, verdict: "scope-only-pass", lane, ...LANES[lane],
    seed: { commit: seedCommit, blob: proof.seed.blob, body: proof.seedBody },
    candidate: { commit: candidateCommit, blob: proof.candidate.blob, body: proof.candidateBody },
    unchangedPrefixSha256: proof.unchangedPrefixSha256,
    unchangedSuffixSha256: proof.unchangedSuffixSha256,
    scope: "allowlisted-file-only; existing whole-tree preflight remains mandatory; no execution approval",
  };
}

type TreeEntry = { mode: string; type: string; oid: string };
function treeEntries(repo: string, commit: string): Map<string, TreeEntry> {
  const output = git(repo, ["ls-tree", "-r", "-t", "-z", "--full-tree", commit], MAX_TREE_BYTES);
  const result = new Map<string, TreeEntry>();
  let start = 0;
  while (start < output.length) {
    const end = output.indexOf(0, start);
    if (end < 0) throw new Error("invalid Git tree output");
    const entry = output.subarray(start, end);
    const tab = entry.indexOf(9);
    const header = tab < 0 ? "" : entry.subarray(0, tab).toString("ascii");
    const match = /^([0-7]{6}) (blob|tree|commit) ([0-9a-f]{40})$/.exec(header);
    if (!match || tab === entry.length - 1) throw new Error("invalid Git tree entry");
    const key = entry.subarray(tab + 1).toString("hex");
    if (result.has(key)) throw new Error("duplicate Git tree path");
    result.set(key, { mode: match[1], type: match[2], oid: match[3] });
    start = end + 1;
  }
  return result;
}

function checkRepository(repo: string) {
  if (git(repo, ["rev-parse", "--is-shallow-repository"], 1024).toString().trim() !== "false") {
    throw new Error("shallow repository is not supported");
  }
  if (git(repo, ["for-each-ref", "--format=%(refname)", "refs/replace"], 1024 * 1024).length) {
    throw new Error("replacement refs are not supported");
  }
  const commonDir = git(repo, ["rev-parse", "--git-common-dir"], 4096).toString().trim();
  const graft = join(isAbsolute(commonDir) ? commonDir : resolve(repo, commonDir), "info", "grafts");
  if (existsSync(graft) && readFileSync(graft).length !== 0) throw new Error("Git grafts are not supported");
}

/** Proves source identity and scope only; it does not authorize or execute candidate code. */
export function verifySource(repo: string, seedCommit: string, candidateCommit: string, lane: SourceLane): VerifiedSource {
  if (!Object.hasOwn(LANES, lane)) throw new Error("unknown source lane");
  checkRepository(repo);
  const proof = region(repo, seedCommit, candidateCommit, lane);
  if (proof.seed.blob === proof.candidate.blob) throw new Error("the allowlisted file must change");
  try {
    git(repo, ["merge-base", "--is-ancestor", seedCommit, candidateCommit], 1024);
  } catch {
    throw new Error("seed must be an ancestor of candidate");
  }
  const seedEntries = treeEntries(repo, seedCommit);
  const candidateEntries = treeEntries(repo, candidateCommit);
  const spec = LANES[lane];
  const target = Buffer.from(spec.path).toString("hex");
  const parts = spec.path.split("/");
  const ancestors = new Set(parts.slice(0, -1).map((_, i) => Buffer.from(parts.slice(0, i + 1).join("/")).toString("hex")));
  if (seedEntries.size !== candidateEntries.size) throw new Error("tree entries differ outside the allowlisted file");
  for (const [path, before] of seedEntries) {
    const after = candidateEntries.get(path);
    if (!after || before.mode !== after.mode || before.type !== after.type) {
      throw new Error("tree path, mode or type changed");
    }
    if (path === target) {
      if (before.mode !== spec.mode || before.type !== "blob"
        || before.oid !== proof.seed.blob || after.oid !== proof.candidate.blob) {
        throw new Error("allowlisted tree entry does not match verified source");
      }
    } else if (ancestors.has(path)) {
      if (before.mode !== "040000" || before.type !== "tree") throw new Error("invalid allowlisted path ancestor");
    } else if (before.oid !== after.oid) {
      throw new Error("tree entries differ outside the allowlisted file");
    }
  }
  const tree = (commit: string) => git(repo, ["rev-parse", `${commit}^{tree}`], 1024).toString().trim();
  return {
    version: 1, verdict: "verified-source", lane, ...spec,
    seed: { commit: seedCommit, tree: tree(seedCommit), blob: proof.seed.blob,
      sourceSha256: digest(proof.seed.bytes), body: proof.seedBody },
    candidate: { commit: candidateCommit, tree: tree(candidateCommit), blob: proof.candidate.blob,
      sourceSha256: digest(proof.candidate.bytes), body: proof.candidateBody },
    unchangedPrefixSha256: proof.unchangedPrefixSha256,
    unchangedSuffixSha256: proof.unchangedSuffixSha256,
  };
}

function main(args: string[]) {
  if (args.length !== 6 || args[0] !== "--repo" || args[2] !== "--seed" || args[4] !== "--candidate") {
    throw new Error("usage: check.ts --repo <object-repository> --seed <full-trusted-commit> --candidate <full-commit>");
  }
  console.log(JSON.stringify(checkRegion(resolve(args[1]), args[3], args[5])));
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  try {
    main(process.argv.slice(2));
  } catch (error) {
    console.error(JSON.stringify({ verdict: "reject", error: error instanceof Error ? error.message : String(error) }));
    process.exitCode = 1;
  }
}
