# Adding Rust projects

Use the existing `greeting` library and `hello-world` application as templates.
Cargo discovers crates one directory below `crates/` or `bin/`, but the root
Nx aggregate is intentionally explicit and must also be updated.

Choose a lowercase kebab-case package and Nx project name. The examples below
use `my-tool` and `my-library`.

## Add an independent binary

1. Generate the crate without a nested Git repository:

   ```bash
   cargo new --bin bin/my-tool --vcs none
   ```

2. Use these existing files as templates:

   ```text
   bin/hello-world/Cargo.toml
   bin/hello-world/README.md
   bin/hello-world/project.json
   ```

   In the new `Cargo.toml`, set `name` and `description`, inherit the workspace
   package fields, use `.workspace = true` dependencies, and retain
   `[lints] workspace = true`.

   In the copied `project.json`:

   - replace every `hello-world` package, project, path, output, and image
     occurrence with the new values;
   - set `sourceRoot` to `bin/my-tool/src`;
   - set `implicitDependencies` to the library Nx projects used by the binary;
   - keep `format`, `format-check`, `lint`, `build`, `test`, and `verify`;
   - keep `image`, `image-smoke`, and `publish` only when the binary owns a
     reviewed `Dockerfile`.

3. Add third-party versions to root `[workspace.dependencies]`, then reference
   them from the package manifest with `.workspace = true`. Add a shared local
   library there in the same form as `greeting` when other packages should be
   able to opt into it.

4. Add `my-tool` to the root `project.json`:

   - root `implicitDependencies`;
   - each Rust aggregate target's `dependsOn[0].projects` list for `format`,
     `format-check`, `lint`, `build`, and `test`.

5. If the binary imports a workspace library, keep Cargo and Nx aligned:

   ```toml
   [dependencies]
   my-library.workspace = true
   ```

   ```json
   {
     "implicitDependencies": ["my-library"]
   }
   ```

6. Update `Cargo.lock`, inspect the Nx project, and run its checks:

   ```bash
   cargo check --package my-tool
   cargo metadata --locked --format-version 1 --no-deps
   npx nx show project my-tool
   npx nx run my-tool:verify
   npx nx run rust:verify
   ```

## Add a reusable library

1. Generate the crate:

   ```bash
   cargo new --lib crates/my-library --vcs none
   ```

2. Use these existing files as templates:

   ```text
   crates/greeting/Cargo.toml
   crates/greeting/README.md
   crates/greeting/project.json
   ```

   In the copied `project.json`, replace every `greeting` occurrence, set
   `sourceRoot` to `crates/my-library/src`, and preserve the library target
   commands. Add `implicitDependencies` only when this library depends on
   another workspace library.

3. Add shared third-party dependencies to root `[workspace.dependencies]` and
   opt into them from the new manifest. If other packages will use this
   library through workspace inheritance, add:

   ```toml
   [workspace.dependencies]
   my-library = { path = "crates/my-library" }
   ```

4. Add `my-library` to the same root `project.json` aggregate lists described
   for a binary.

5. Update the lockfile and verify both package and workspace:

   ```bash
   cargo check --package my-library
   cargo metadata --locked --format-version 1 --no-deps
   npx nx show project my-library
   npx nx run my-library:verify
   npx nx run rust:verify
   ```

## Template checks before copying

Confirm the current templates still expose the expected target sets:

```bash
npx nx show project hello-world
npx nx show project greeting
```

After adding either kind of project, also run:

```bash
npx nx run-many -t verify --all --parallel=1
```

If a new runtime image is published, add its package path and explicit
publication target to the relevant workflow, then follow
[Container publishing](../containers/publishing.md). Do not place registry
credentials or feed configuration in a Cargo manifest, `project.json`, or
Dockerfile.
