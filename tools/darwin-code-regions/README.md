# Darwin production-region scope check

| Contract | First lane |
| --- | --- |
| Trusted allowlist | Root inherent `FileSortBuffer::finish` in `crates/arrow-deltalake/src/layout.rs`, mode `100644` |
| Mutable region | Method body interior only; braces, signature, attributes, other functions/tests and every other byte in this file stay identical |
| Inputs | Full immutable seed/candidate commit IDs; trusted Git repository supplies their tree entries and blobs |
| Parser | Trusted standalone Rust helper, `syn` + source byte spans; no macro expansion or candidate compilation/execution |
| Deliberate restrictions | Reject missing/ambiguous items, nested items/attributes/macros inside the body, invalid UTF-8, BOM/shebang, and blobs over 1 MiB |
| Output | JSON scope-only receipt with commit/blob IDs, body spans and unchanged prefix/suffix SHA-256; rejection exits nonzero |

Run from a **trusted checkout**, never the candidate checkout. Build the helper
from trusted source and its separate lockfile; this does not build the candidate
or any production crate:

```bash
npx --no-install nx run darwin-code-regions:build-parser
npx --no-install tsx tools/darwin-code-regions/check.ts \
  --repo /workspaces/rust-playground \
  --seed f3c4425c93d394632af0f0c1f11c631d23fe7811 \
  --candidate 24d5237cbd15645ef238064fe5981b603c826be1
npx --no-install nx run darwin-code-regions:test
npx --no-install nx run darwin-code-regions:typecheck
```

The runner owner chooses the trusted seed and trusted checker build. Candidate
input cannot choose a path, Rust item, parser, or allowlist. Git replacement
objects are disabled. The two historical commits must exist in the object
repository for the actual-candidate regression.

**Additional gate only:** this checks the one allowlisted file, not the rest of
the commit tree. Keep the runner's existing ancestry, whole-tree/path, metadata,
test, and native-evaluation rejection gates. The historical positive pair also
contains fixture changes in other files; this file-region result does not approve
those changes. A runner must validate the exact same candidate commit at every
gate. No dispatch integration or existing rejection is changed here.

This is a source-scope proof, not a sandbox, correctness verdict, benchmark
qualification, or authorization to execute native code. It does not resolve
macros or infer behavior. Later methods/modules need deliberate trusted opt-in.
