# Darwin production-source scope checks

| Contract | Behavior |
| --- | --- |
| Trusted lanes | `layout-sort-finish-v1`: `FileSortBuffer::finish`; `maintenance-active-apply-v1`: `DeltaTable::apply_prepared_optimize`; `maintenance-idle-vacuum-v1`: `DeltaTable::vacuum_once` |
| Paths | Layout lane: `crates/arrow-deltalake/src/layout.rs`; maintenance lanes: `crates/arrow-deltalake/src/maintenance.rs`. Each must be a regular `100644` blob |
| Mutable region | Method body interior only; braces, signature, attributes, other functions/tests and every other byte in this file stay identical |
| Inputs | Full immutable seed/candidate commit IDs; trusted Git repository supplies their tree entries and blobs |
| Parser | Trusted standalone Rust helper, `syn` + source byte spans; no macro expansion or candidate compilation/execution |
| Deliberate restrictions | Reject missing/ambiguous items, nested items/attributes/macros inside the body, invalid UTF-8, BOM/shebang, and blobs over 1 MiB |
| Full source admission | `verifySource(repo, seedCommit, candidateCommit, lane)` requires seed ancestry and exactly one changed allowlisted regular file across the full trees, including modes, directories and submodule identities |
| Binding | `VerifiedSource` returns commit/tree/blob IDs, SHA-256 hashes and body spans for both sources; it is not permission to execute |
| Legacy CLI | `checkRegion` / `check.ts` still checks the layout file only, and is not full source admission |

`SourceLane` and `VerifiedSource` are exported from `check.ts`. Call
`verifySource` directly in trusted code, using the exact candidate commit at
every subsequent gate; do not accept a candidate-supplied receipt or path.
Callers must bind any materialized source to the returned candidate commit,
blob and SHA-256. The Git object repository, checkout running this checker and
parser build must be trusted and protected from concurrent modification.
Shallow repositories, replacement refs and nonempty legacy graft files are
rejected; Git replacement objects and inherited Git environment overrides are
disabled. The comparison includes tree entries, modes, symlinks and gitlinks.

The idle recipe's `probe_no_work_with_inventory` contains macros, so it is
deliberately **not** admitted by this parser. The idle lane covers its
`vacuum_once` fallback, not the probe. Methods outside the listed lanes are
rejected rather than inferred from the recipe.

Run from a **trusted checkout**, never the candidate checkout. Build the helper
from trusted source and its separate lockfile; this does not build the candidate
or any production crate:

```bash
npx --no-install nx run darwin-code-regions:build-parser
npx --no-install tsx tools/darwin-code-regions/check.ts \
  --repo /workspaces/rust-playground \
  --seed 24d5237cbd15645ef238064fe5981b603c826be1 \
  --candidate f3c4425c93d394632af0f0c1f11c631d23fe7811
npx --no-install nx run darwin-code-regions:test
npx --no-install nx run darwin-code-regions:typecheck
```

The runner owner chooses the trusted seed and trusted checker build. Candidate
input cannot choose a path, Rust item, parser, or allowlist. Git replacement
objects are disabled. The two historical commits must exist in the object
repository for the actual-candidate regression.

**Legacy CLI additional gate only:** it checks one file, not the rest of the
commit tree. The specific historical pair above changes only the allowed file
and also passes `verifySource`. Keep existing metadata, test, and
native-evaluation rejection gates; no dispatch is changed here.

This is a source-scope proof, not a sandbox, correctness verdict, benchmark
qualification, or authorization to execute native code. It does not resolve
macros or infer behavior. Later methods/modules need deliberate trusted opt-in.
