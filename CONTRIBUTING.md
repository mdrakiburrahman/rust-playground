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

For collector source development and submodule updates, see the
[otelcol-rust README](projects/otelcol-rust/README.md). For the Compose demo,
local Parquet output, and E2E verification, see the
[loadtest README](projects/loadtest/README.md).

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
