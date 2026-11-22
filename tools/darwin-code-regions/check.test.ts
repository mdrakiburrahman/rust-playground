import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { createHash } from "node:crypto";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { checkRegion, verifySource, type SourceLane } from "./check.js";

const layout = "crates/arrow-deltalake/src/layout.rs";
const seed = `// Unicode offsets: café 🦀
struct FileSortBuffer;
impl FileSortBuffer {
    #[must_use]
    pub fn finish(self) -> usize {
        let text = r#"} fn finish() { #[test]"#;
        /* nested { /* } */ } */
        text.len()
    }
    fn adjacent(&self) -> usize { 1 }
}
#[cfg(test)]
mod tests {
    #[test]
    fn adjacent_test() { assert_eq!(1, 1); }
}
`;

function fixture() {
  const repo = mkdtempSync(join(tmpdir(), "darwin-regions-"));
  const git = (...args: string[]) => execFileSync("git", ["-C", repo, ...args], {
    encoding: "utf8",
    stdio: ["pipe", "pipe", "pipe"],
    env: {
      ...process.env,
      GIT_AUTHOR_NAME: "Scope test", GIT_AUTHOR_EMAIL: "scope@example.invalid",
      GIT_COMMITTER_NAME: "Scope test", GIT_COMMITTER_EMAIL: "scope@example.invalid",
    },
  }).trim();
  git("init", "-q");
  const commit = (source: string | Buffer, mode = "100644", target = layout) => {
    const blob = execFileSync("git", ["-C", repo, "hash-object", "-w", "--stdin"], {
      input: source, encoding: "utf8",
    }).trim();
    git("read-tree", "--empty");
    git("update-index", "--add", "--cacheinfo", mode, blob, target);
    return git("commit-tree", git("write-tree"), "-m", "inert source fixture");
  };
  return { repo, git, commit, cleanup: () => rmSync(repo, { recursive: true, force: true }) };
}

test("actual reviewed layout blobs pass without checking out or executing the candidate", () => {
  const repo = fileURLToPath(new URL("../../", import.meta.url));
  const result = checkRegion(repo,
    "24d5237cbd15645ef238064fe5981b603c826be1",
    "f3c4425c93d394632af0f0c1f11c631d23fe7811");
  assert.equal(result.verdict, "scope-only-pass");
  assert.notEqual(result.seed.blob, result.candidate.blob);
  assert.equal(verifySource(repo,
    "24d5237cbd15645ef238064fe5981b603c826be1",
    "f3c4425c93d394632af0f0c1f11c631d23fe7811", "layout-sort-finish-v1").verdict, "verified-source");
});

test("parser spans preserve Unicode, strings, nested comments, and CRLF", () => {
  const f = fixture();
  try {
    for (const source of [seed, seed.replaceAll("\n", "\r\n")]) {
      const original = f.commit(source);
      const changed = f.commit(source.replace("text.len()", "text.len() + 1"));
      assert.equal(checkRegion(f.repo, original, changed).verdict, "scope-only-pass");
      assert.equal(checkRegion(f.repo, original, original).verdict, "scope-only-pass");
    }
  } finally { f.cleanup(); }
});

test("reject adjacent production/test/signature/attribute and nested test changes", () => {
  const f = fixture();
  try {
    const original = f.commit(seed);
    for (const changed of [
      seed.replace("fn adjacent(&self) -> usize { 1 }", "fn adjacent(&self) -> usize { 2 }"),
      seed.replace("assert_eq!(1, 1)", "assert_eq!(1, 2)"),
      seed.replace("pub fn finish(self)", "pub fn finish(&self)"),
      seed.replace("#[must_use]", "#[inline]"),
      seed.replace("text.len()", "#[test] fn inline_test() {} text.len()"),
      seed.replace("text.len()", "#![allow(unused)] text.len()"),
      seed.replace("pub fn finish(self) -> usize {", "pub fn finish(self) -> usize {\n#![cfg(test)]"),
      seed.replace("text.len()", "#[allow(unused)] let value = 1; text.len()"),
      seed.replace("text.len()", "opaque_macro!()"),
      seed.replace("text.len()", "fn helper() {} text.len()"),
      seed.replace("pub fn finish", "pub fn missing"),
      seed + "impl FileSortBuffer { fn finish(self) {} }\n",
      seed.replace("text.len()", "let broken = ;"),
    ]) {
      assert.throws(() => checkRegion(f.repo, original, f.commit(changed)));
    }
  } finally { f.cleanup(); }
});

test("reject wrong object types, mutable refs, missing path, modes, invalid UTF-8 and oversized blobs", () => {
  const f = fixture();
  try {
    const original = f.commit(seed);
    for (const candidate of [
      f.commit(seed, "100755"),
      f.commit("target", "120000"),
      f.commit(seed, "100644", "elsewhere.rs"),
      f.commit(Buffer.from([0xff])),
      f.commit(" ".repeat(1_048_577)),
    ]) assert.throws(() => checkRegion(f.repo, original, candidate));
    assert.throws(() => checkRegion(f.repo, original, "HEAD"));
    const blob = checkRegion(f.repo, original, original).seed.blob;
    assert.throws(() => checkRegion(f.repo, original, blob));
    assert.throws(() => checkRegion(f.repo, original, f.git("rev-parse", `${original}^{tree}`)));
    assert.throws(() => checkRegion(f.repo, blob, original));
    assert.throws(() => checkRegion(f.repo, f.commit(seed, "100755"), original));
  } finally { f.cleanup(); }
});

test("replacement objects cannot substitute a different immutable seed", () => {
  const f = fixture();
  try {
    const original = f.commit(seed);
    const candidate = f.commit(seed.replace("text.len()", "text.len() + 1"));
    const replacement = f.commit(seed.replace("assert_eq!(1, 1)", "assert_eq!(1, 2)"));
    f.git("replace", original, replacement);
    assert.equal(checkRegion(f.repo, original, candidate).verdict, "scope-only-pass");
    assert.throws(() => checkRegion(f.repo, replacement, candidate), /outside the allowlisted/);
  } finally { f.cleanup(); }
});

test("verified source binds one real tree change to the same descendant and method", () => {
  const f = fixture();
  try {
    const blob = (value: string) => execFileSync("git", ["-C", f.repo, "hash-object", "-w", "--stdin"], {
      input: value, encoding: "utf8",
    }).trim();
    const write = (base: string | null, source: string, extra?: () => void) => {
      f.git("read-tree", base ? `${base}^{tree}` : "--empty");
      f.git("update-index", "--add", "--cacheinfo", "100644", blob(source), layout);
      extra?.();
      const tree = f.git("write-tree");
      return f.git("commit-tree", tree, ...(base ? ["-p", base] : []), "-m", "inert fixture");
    };
    const original = write(null, seed, () => {
      f.git("update-index", "--add", "--cacheinfo", "100644", blob("stable"), "other.txt");
    });
    const changed = seed.replace("text.len()", "text.len() + 1");
    const candidate = write(original, changed);
    const verified = verifySource(f.repo, original, candidate, "layout-sort-finish-v1");
    assert.equal(verified.verdict, "verified-source");
    assert.equal(verified.candidate.commit, candidate);
    assert.equal(verified.candidate.tree, f.git("rev-parse", `${candidate}^{tree}`));
    assert.equal(verified.candidate.blob, f.git("rev-parse", `${candidate}:${layout}`));
    assert.equal(verified.candidate.sourceSha256, createHash("sha256").update(changed).digest("hex"));
    const previousGitDir = process.env.GIT_DIR;
    try {
      process.env.GIT_DIR = join(f.repo, "not-a-repository");
      assert.equal(verifySource(f.repo, original, candidate, "layout-sort-finish-v1").candidate.blob, verified.candidate.blob);
    } finally {
      if (previousGitDir === undefined) delete process.env.GIT_DIR;
      else process.env.GIT_DIR = previousGitDir;
    }
    assert.throws(() => verifySource(f.repo, original, original, "layout-sort-finish-v1"), /must change/);
    assert.throws(() => verifySource(f.repo, original, write(null, changed), "layout-sort-finish-v1"), /ancestor/);
    assert.throws(() => verifySource(f.repo, original, "HEAD", "layout-sort-finish-v1"), /full lowercase/);
    assert.throws(() => verifySource(f.repo, original, candidate, "bad" as SourceLane), /unknown/);
    for (const extra of [
      () => f.git("update-index", "--add", "--cacheinfo", "100644", blob("extra"), "added"),
      () => f.git("update-index", "--force-remove", "other.txt"),
      () => f.git("update-index", "--add", "--cacheinfo", "100644", blob("changed"), "other.txt"),
      () => f.git("update-index", "--add", "--cacheinfo", "100755", blob("stable"), "other.txt"),
      () => f.git("update-index", "--add", "--cacheinfo", "120000", blob("stable"), "other.txt"),
      () => f.git("update-index", "--add", "--cacheinfo", "160000", original, "submodule"),
      () => f.git("update-index", "--add", "--cacheinfo", "100755", blob(changed), layout),
      () => f.git("update-index", "--add", "--cacheinfo", "120000", blob(changed), layout),
    ]) assert.throws(() => verifySource(f.repo, original, write(original, changed, extra), "layout-sort-finish-v1"));
    const submoduleSeed = write(null, seed, () =>
      f.git("update-index", "--add", "--cacheinfo", "160000", original, "submodule"));
    const submoduleChanged = write(submoduleSeed, changed, () =>
      f.git("update-index", "--add", "--cacheinfo", "160000", candidate, "submodule"));
    assert.throws(() => verifySource(f.repo, submoduleSeed, submoduleChanged, "layout-sort-finish-v1"), /outside/);
    const emptyTree = execFileSync("git", ["-C", f.repo, "mktree"], { input: "", encoding: "utf8" }).trim();
    const rootEntries = execFileSync("git", ["-C", f.repo, "ls-tree", "-z", candidate]);
    const treeWithEmpty = execFileSync("git", ["-C", f.repo, "mktree", "-z"], {
      input: Buffer.concat([rootEntries, Buffer.from(`040000 tree ${emptyTree}\tempty-dir\0`)]), encoding: "utf8",
    }).trim();
    const extraTree = f.git("commit-tree", treeWithEmpty, "-p", original, "-m", "inert fixture");
    assert.throws(() => verifySource(f.repo, original, extraTree, "layout-sort-finish-v1"), /tree/);
    const outsideMethod = write(original, changed.replace("assert_eq!(1, 1)", "assert_eq!(1, 2)"));
    assert.throws(() => verifySource(f.repo, original, outsideMethod, "layout-sort-finish-v1"), /outside/);
    const macro = write(original, seed.replace("text.len()", "panic!()"));
    assert.throws(() => verifySource(f.repo, original, macro, "layout-sort-finish-v1"));
    f.git("replace", original, candidate);
    assert.throws(() => verifySource(f.repo, original, candidate, "layout-sort-finish-v1"), /replacement/);
    f.git("replace", "-d", original);
    writeFileSync(join(f.repo, ".git", "info", "grafts"), `${candidate} ${original}\n`);
    assert.throws(() => verifySource(f.repo, original, candidate, "layout-sort-finish-v1"), /grafts/);
    rmSync(join(f.repo, ".git", "info", "grafts"));
    writeFileSync(join(f.repo, ".git", "shallow"), `${original}\n`);
    assert.throws(() => verifySource(f.repo, original, candidate, "layout-sort-finish-v1"), /shallow/);
  } finally { f.cleanup(); }
});

test("the real layout method admits a sole body change without executing it", () => {
  const realRepo = fileURLToPath(new URL("../../", import.meta.url));
  const commit = execFileSync("git", ["-C", realRepo, "rev-parse", "HEAD"], { encoding: "utf8" }).trim();
  const source = execFileSync("git", ["-C", realRepo, "show", `${commit}:${layout}`]);
  const span = checkRegion(realRepo, commit, commit).seed.body;
  const f = fixture();
  try {
    const original = f.commit(source);
    const changed = Buffer.concat([source.subarray(0, span.start), Buffer.from("\n "), source.subarray(span.start)]);
    const newBlob = execFileSync("git", ["-C", f.repo, "hash-object", "-w", "--stdin"], {
      input: changed, encoding: "utf8",
    }).trim();
    f.git("read-tree", `${original}^{tree}`);
    f.git("update-index", "--add", "--cacheinfo", "100644", newBlob, layout);
    const candidate = f.git("commit-tree", f.git("write-tree"), "-p", original, "-m", "inert fixture");
    const verified = verifySource(f.repo, original, candidate, "layout-sort-finish-v1");
    assert.equal(verified.candidate.sourceSha256, createHash("sha256").update(changed).digest("hex"));
  } finally { f.cleanup(); }
});

test("public maintenance active and idle methods are parser-compatible but other changes reject", () => {
  const realRepo = fileURLToPath(new URL("../../", import.meta.url));
  const path = "crates/arrow-deltalake/src/maintenance.rs";
  const source = execFileSync("git", ["--no-replace-objects", "-C", realRepo, "show", `HEAD:${path}`], { encoding: "utf8" });
  const fixtures: [SourceLane, string][] = [
    ["maintenance-active-apply-v1",
      "self.apply_optimize_reusing(&prepared.plan, config, control, Some(&prepared.state), None, None)"],
    ["maintenance-idle-vacuum-v1", "control.check()?;\n        let (state, replay) = self.prepare_vacuum(config)?;"],
  ];
  for (const [lane, needle] of fixtures) {
    const f = fixture();
    try {
      assert.equal(source.split(needle).length, 2);
      const original = f.commit(source, "100644", path);
      const changed = source.replace(needle, `\n        ${needle}`);
      const candidateBlob = execFileSync("git", ["-C", f.repo, "hash-object", "-w", "--stdin"], {
        input: changed, encoding: "utf8",
      }).trim();
      f.git("read-tree", `${original}^{tree}`);
      f.git("update-index", "--add", "--cacheinfo", "100644", candidateBlob, path);
      const candidate = f.git("commit-tree", f.git("write-tree"), "-p", original, "-m", "inert fixture");
      assert.equal(verifySource(f.repo, original, candidate, lane).lane, lane);
      const other = lane === "maintenance-active-apply-v1" ? "maintenance-idle-vacuum-v1" : "maintenance-active-apply-v1";
      assert.throws(() => verifySource(f.repo, original, candidate, other), /outside/);
    } finally { f.cleanup(); }
  }
});
