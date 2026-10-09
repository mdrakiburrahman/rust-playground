# Rust project layout

This repository is set up as a monorepo for small Rust applications, shared
libraries, and their Nx automation. For developers familiar with a Go
monorepo, the layout follows the same `cmd` and `pkg` separation:

- `bin/` - For independent binary crates. Nothing should depend on any crates
  in here. This is like `cmd` in the `go` project. Crates within this folder
  will often contain accessories to the corresponding source code like
  `Dockerfile`s.
- `crates/` - For library crates. Library crates may depend on each other and
  binary crates may also depend on them. This is just like `pkg` in the `go`
  project.
- `Cargo.toml` - This root level Cargo configuration defines the workspace and
  points to all other crates both bin and lib.
- `rust-toolchain.toml` - This pins the rust toolchain version that this repo
  uses.

The root workspace uses `bin/*` and `crates/*` members, so a crate placed at
the expected depth is discovered by Cargo. Shared dependency versions,
package metadata, and lint policy live in the root `Cargo.toml`; package
manifests opt into them with `.workspace = true`.

## Dependency direction

Dependencies flow from applications toward libraries:

```text
bin/<application>  --->  crates/<library>  --->  crates/<lower-level-library>
```

A library must never depend on a crate under `bin/`, and one binary must never
be used as another binary's library. Move shared behavior into `crates/`
instead.

The current example follows that rule:

- `bin/hello-world` owns CLI parsing, process behavior, and its runtime
  `Dockerfile`.
- `crates/greeting` owns reusable validation and greeting formatting.

## Cargo and Nx responsibilities

Cargo remains the source of truth for Rust packages and dependency resolution.
Each Rust package also owns a `project.json` so Nx can provide:

- package-scoped `format`, `format-check`, `lint`, `build`, `test`, and
  `verify` targets;
- explicit project dependencies that mirror Cargo dependencies;
- cache inputs appropriate to source, tests, and production builds;
- non-Cargo targets such as runtime image build, smoke test, and publication.

The root `rust` Nx project aggregates every Rust package. Its target
dependencies are explicit so Nx schedules each package once, while the
workspace-only `lockfile-check` and `doc-test` targets run once at the root.

Inspect the current graph and target definitions with:

```bash
npx nx show project rust
npx nx show project greeting
npx nx show project hello-world
npx nx graph
```

See [Adding Rust projects](adding-projects.md) for the exact Cargo and Nx
changes required for a new crate.
