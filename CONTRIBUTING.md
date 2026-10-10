# Contributing

If you use Windows, use it only to enter WSL. All development happens on
Linux inside the repository's
[VS Code devcontainer](https://code.visualstudio.com/docs/devcontainers/containers).
CI uses the same development environment.

## Enter the devcontainer

After completing the one-time setup below, run these commands from WSL:

```bash
cd /workspaces/rust-playground
npm ci
npx nx run devcontainer:up
WORKSPACE_HEX=$(printf '%s' "$(wslpath -w .)" | od -An -tx1 | tr -d '[:space:]')
code --folder-uri "vscode-remote://dev-container+${WORKSPACE_HEX}/workspaces/rust-playground"
```

This starts the default immutable devcontainer image and opens the repository
inside it. To enter the same container from a terminal without VS Code, run:

```bash
npx --no-install devcontainer exec \
  --workspace-folder . \
  --config .devcontainer/devcontainer.json \
  bash
```

## First-time setup

1. Install VS Code and its WSL and Dev Containers extensions from PowerShell:

   ```powershell
   winget install -e --id Microsoft.VisualStudioCode
   code --install-extension ms-vscode-remote.remote-wsl
   code --install-extension ms-vscode-remote.remote-containers
   ```

1. Run the Windows bootstrap from PowerShell 7 as Administrator:

   ```powershell
   $GIT_ROOT = git rev-parse --show-toplevel
   & "$GIT_ROOT\dev\bootstrap-dev-env.ps1"
   ```

1. Enter Ubuntu 24.04, clone your fork into the WSL filesystem, and configure
   your Git identity:

   ```bash
   sudo install -d -m 0775 -o "$USER" -g "$USER" /workspaces
   cd /workspaces

   read -rp "Enter your name (for example, FirstName LastName): " user_name
   read -rp "Enter your email: " user_email
   read -rp "Enter your fork URL: " git_fork_url

   git config --global user.name "$user_name"
   git config --global user.email "$user_email"
   git clone "$git_fork_url" rust-playground
   cd rust-playground
   git submodule update --init --recursive
   ```

   Do not clone the development copy under `/mnt/c`; Docker bind mounts and
   Linux filesystem operations are substantially more reliable under
   `/workspaces`.

   To reuse Git for Windows credentials from WSL:

   ```bash
   git config --global credential.helper \
     '/mnt/c/"Program Files"/Git/mingw64/bin/git-credential-manager.exe'
   ```

1. Run the idempotent Linux host bootstrap:

   ```bash
   GIT_ROOT=$(git rev-parse --show-toplevel)
   chmod +x "$GIT_ROOT/dev/bootstrap-dev-env.sh"
   "$GIT_ROOT/dev/bootstrap-dev-env.sh"
   ```

   The bootstrap installs and configures Azure CLI, GitHub CLI, Docker,
   Node.js, and the Dev Containers CLI. It also restarts Docker and clears
   existing Docker containers, volumes, and networks.

1. Start and enter the devcontainer:

   ```bash
   npm ci
   npx nx run devcontainer:up
   WORKSPACE_HEX=$(printf '%s' "$(wslpath -w .)" | od -An -tx1 | tr -d '[:space:]')
   code --folder-uri "vscode-remote://dev-container+${WORKSPACE_HEX}/workspaces/rust-playground"
   ```

   The devcontainer initialization creates the host credential directories
   used for the read/write mounts at `~/.azure` and `~/.config/gh`.
   Repository dependencies are installed inside the container by its
   post-create command.

1. Authenticate from inside the devcontainer when needed:

   ```bash
   az login
   gh auth login
   ```

   For local package publication, add the `write:packages` scope:

   ```bash
   gh auth refresh --hostname github.com --scopes write:packages
   ```

   Authentication changes are shared with the WSL host through the mounted
   credential directories. Never copy either directory into the repository.

## Verify changes

Run checks inside the devcontainer:

```bash
npx nx run-many -t verify --all --parallel=1
npx nx run hello-world:image-smoke
```

For Rust-only changes:

```bash
npx nx run rust:verify
```

## Rust collector and local telemetry lake

The `otelcol-rust` Nx project builds `df_engine` directly from the editable
`submodules/otel-arrow` fork. Initialize the full source and nested proto
submodules after cloning or switching to this change:

```bash
git submodule update --init --recursive
```

CI checks out the committed gitlink SHA, not a moving branch tip. For local
collector development, explicitly attach the submodule to the exploration
branch (ordinary initialization leaves it detached):

```bash
git -C submodules/otel-arrow switch dev/mdrrahman/explore
npx --no-install nx run otelcol-rust:build
npx --no-install nx run otelcol-rust:image-smoke
```

If the branch is not yet local, create it from the remote tracking branch:

```bash
git -C submodules/otel-arrow switch --track origin/dev/mdrrahman/explore
```

The collector uses the fork's pinned Rust toolchain, independently of this
workspace's Rust version. Rustup installs it when first invoked there.
Native builds also need `protoc`; the source devcontainer includes
`protobuf-compiler`. When using an older prebuilt devcontainer, rebuild from
source or install that missing package before native collector builds.
The collector Dockerfile includes its own build prerequisites.

Both native and image builds consume local source, including uncommitted edits:
no Cargo git dependency or remote prebuilt collector bypasses the submodule.
Rebuild and restart the two-container demo after changing collector code:

```bash
npx --no-install nx run loadtest:up
npx --no-install nx run loadtest:logs
npx --no-install nx run loadtest:inspect
npx --no-install nx run loadtest:down
```

`loadtest:up` builds both Dockerfiles and starts an instrumented hello-world
loop. Parquet appears under gitignored `onelake/demo/`; shutdown preserves it.
This is a **local folder**, not a connection to Microsoft Fabric OneLake.
Run as your non-root devcontainer user: Compose uses that user's UID/GID
for collector writes so files remain readable without broad permission changes.
Ports are private to the Compose network.

To execute finite E2E verification and inspect its output:

```bash
npx --no-install nx run loadtest:e2e
ONELAKE_INSPECT_PATH=onelake/e2e/<run-id> \
  npx --no-install nx run loadtest:inspect
```

Each E2E run gets a fresh directory and verifies actual log records,
counter datapoints, and correlated spans from readable Parquet. On failure,
inspect that directory's `compose.log`. The exporter stores OTAP tables with
`id`/`parent_id` relationships, not a single flattened table for each signal.
See [loadtest](projects/loadtest/README.md) for the layout.

The runner resolves bind mounts through the current devcontainer's Docker
mount metadata. With a custom hostname or remote Docker daemon, set
`LOADTEST_HOST_WORKSPACE` to the daemon-visible path to this checkout.

To incorporate remote fork changes without discarding your local edits:

```bash
git -C submodules/otel-arrow fetch origin
git -C submodules/otel-arrow switch dev/mdrrahman/explore
git -C submodules/otel-arrow merge --ff-only origin/dev/mdrrahman/explore
git submodule update --init --recursive
```

When committing your own collector changes, push them to the fork first,
then stage and commit `submodules/otel-arrow` in rust-playground to pin the
new SHA. Run E2E before pushing the superproject. Do not use
`git submodule update --remote` in CI or reset a dirty submodule.

## Build the devcontainer from source

The default workflow uses the immutable image pinned in
`.devcontainer/docker-compose.yml`. Only use the source configuration when
changing the devcontainer itself:

```bash
npx nx run devcontainer:down
npx nx run devcontainer:up-source
```

In VS Code, choose **Dev Containers: Attach to Running Container** and select
the `rust-playground (source)` container.

## Stop the devcontainer

After closing VS Code, run this from the WSL host:

```bash
npx nx run devcontainer:down
```

Add `-- --volumes` only when the workspace's named volumes should also be
deleted:

```bash
npx nx run devcontainer:down -- --volumes
```

The cleanup target affects only this workspace and never performs a global
Docker prune.

For editor-free operation, monitoring, and troubleshooting, see
[Headless devcontainer operations](docs/devcontainer/headless-operations.md).
For image publication, see
[Container publishing](docs/containers/publishing.md).
