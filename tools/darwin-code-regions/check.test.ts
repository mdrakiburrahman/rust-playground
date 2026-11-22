import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";
import { checkRegion } from "./check.js";

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
