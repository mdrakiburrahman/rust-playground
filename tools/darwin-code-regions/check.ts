import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { resolve } from "node:path";
import { fileURLToPath, pathToFileURL } from "node:url";

const LANE = Object.freeze({
  id: "layout-sort-finish-v1",
  path: "crates/arrow-deltalake/src/layout.rs",
  item: "FileSortBuffer::finish",
  mode: "100644",
});
const MAX_SOURCE_BYTES = 1_048_576;
const PARSER = fileURLToPath(new URL("./parser/target/release/darwin-region-parser", import.meta.url));

function git(repo: string, args: string[], maxBuffer = MAX_SOURCE_BYTES + 1): Buffer {
  return execFileSync("git", ["--no-replace-objects", "-C", repo, ...args], {
    env: { ...process.env, GIT_NO_REPLACE_OBJECTS: "1", GIT_OPTIONAL_LOCKS: "0" },
    timeout: 10_000,
    maxBuffer,
    stdio: ["ignore", "pipe", "pipe"],
  });
}

function readEntry(repo: string, commit: string) {
  if (!/^[0-9a-f]{40}$/.test(commit)) throw new Error("commit must be a full lowercase SHA-1");
  if (git(repo, ["cat-file", "-t", commit], 1024).toString().trim() !== "commit") {
    throw new Error("input does not identify a commit");
  }
  const entry = git(repo, ["ls-tree", "-z", commit, "--", LANE.path], 4096).toString("utf8");
  const match = /^([0-7]{6}) (blob|tree|commit) ([0-9a-f]{40})\t([^\0]+)\0$/.exec(entry);
  if (!match || match[1] !== LANE.mode || match[2] !== "blob" || match[4] !== LANE.path) {
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

function bodySpan(source: Buffer) {
  const output = execFileSync(PARSER, [], {
    input: source,
    timeout: 10_000,
    maxBuffer: 4096,
    stdio: ["pipe", "pipe", "pipe"],
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

/** Additional file-region gate; never substitutes for the runner's whole-tree preflight. */
export function checkRegion(repo: string, seedCommit: string, candidateCommit: string) {
  const seed = readEntry(repo, seedCommit);
  const candidate = readEntry(repo, candidateCommit);
  const seedBody = bodySpan(seed.bytes);
  const candidateBody = bodySpan(candidate.bytes);
  if (!seed.bytes.subarray(0, seedBody.start).equals(candidate.bytes.subarray(0, candidateBody.start))
    || !seed.bytes.subarray(seedBody.end).equals(candidate.bytes.subarray(candidateBody.end))) {
    throw new Error("changes outside the allowlisted method body");
  }
  return {
    version: 1,
    verdict: "scope-only-pass",
    lane: LANE.id,
    path: LANE.path,
    item: LANE.item,
    mode: LANE.mode,
    seed: { commit: seedCommit, blob: seed.blob, body: seedBody },
    candidate: { commit: candidateCommit, blob: candidate.blob, body: candidateBody },
    unchangedPrefixSha256: createHash("sha256").update(seed.bytes.subarray(0, seedBody.start)).digest("hex"),
    unchangedSuffixSha256: createHash("sha256").update(seed.bytes.subarray(seedBody.end)).digest("hex"),
    scope: "allowlisted-file-only; existing whole-tree preflight remains mandatory; no execution approval",
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
