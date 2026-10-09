# hello-world

`hello-world` is an independent binary crate under `bin/`. Nothing in the
workspace should depend on a `bin/` crate; applications instead compose shared
libraries such as `crates/greeting`.

Run the CLI from the workspace root:

```bash
npx nx run hello-world:run -- --name Ferris
```

The equivalent Cargo command is:

```bash
cargo run --locked --package hello-world -- --name Ferris
```

Build and smoke-test its local Linux amd64 image:

```bash
npx nx run hello-world:image-smoke
docker run --rm --platform linux/amd64 \
  ghcr.io/mdrakiburrahman/rust-playground/hello-world:dev \
  --name Ferris
```

After publication and public manifest verification:

```bash
docker run --rm --platform linux/amd64 \
  ghcr.io/mdrakiburrahman/rust-playground/hello-world:latest \
  --name Ferris
```

See [container publishing](../../docs/containers/publishing.md) for branch,
SHA, and `latest` behavior.
