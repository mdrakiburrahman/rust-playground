# Contributing

The supported development environment is Linux in the repository's local
[VS Code devcontainer](https://code.visualstudio.com/docs/devcontainers/containers).
Windows is used only to enter WSL. CI builds and tests the same source
devcontainer.

## Windows and WSL prerequisites

Run the Windows bootstrap from PowerShell 7 as Administrator:

```powershell
.\dev\bootstrap-dev-env.ps1
```

> [!WARNING]
> This is the full developer-machine bootstrap. It removes Docker Desktop and
> existing WSL distributions, configures Defender and `.wslconfig`, and
> installs a fresh Ubuntu 24.04 distribution.

Clone the repository into the WSL filesystem rather than `/mnt/c`:

```bash
sudo install -d -m 0775 -o "$USER" -g "$USER" /workspaces
cd /workspaces
git clone https://github.com/mdrakiburrahman/rust-playground.git rust-playground
cd rust-playground
```

Set the repository-local developer identity that commits should use:

```bash
git config --local user.name "Raki Rahman"
git config --local user.email "mdrakiburrahman@gmail.com"
```

To share Git for Windows credentials with WSL, configure its bundled
credential manager from inside WSL:

```bash
git config --global credential.helper \
  '/mnt/c/"Program Files"/Git/mingw64/bin/git-credential-manager.exe'
```

Create the host credential directories that the devcontainer mounts:

```bash
mkdir -p "$HOME/.azure" "$HOME/.config/gh"
chmod 700 "$HOME/.azure" "$HOME/.config/gh"
```

Run the Linux host bootstrap:

```bash
./dev/bootstrap-dev-env.sh
```

This is the full administrator bootstrap for generic host prerequisites. It
configures Azure CLI, Docker, Node.js, and the Dev Containers CLI, replaces the
Docker daemon configuration, adjusts namespace sysctls, and clears existing
Docker containers, volumes, and networks.

The bootstrap runs `az login` when the native Linux Azure CLI is not already
authenticated. For local package publication, a fresh GitHub CLI login inside
the devcontainer must request the `write:packages` package scope:

```bash
gh auth login --hostname github.com --git-protocol https --web \
  --scopes write:packages
```

Add that scope to an existing login with:

```bash
gh auth refresh --hostname github.com --scopes write:packages
```

The Azure and GitHub CLI directories are mounted read/write, so login and
logout changes made in the devcontainer also affect the WSL host state. Never
copy either credential directory into the repository.

## Configure and open the devcontainer

Install the root tooling from the checked-in lockfile:

```bash
npm ci
```

The source configuration builds `.devcontainer/Dockerfile` and applies the
locked features from `.devcontainer/source/devcontainer.json`:

```bash
npx nx run devcontainer:up-source
```

The published configuration pulls the immutable image pinned in
`.devcontainer/docker-compose.yml`:

```bash
npx nx run devcontainer:up
```

The `initializeCommand` creates `~/.azure` and `~/.config/gh` when needed and
writes the ignored `.devcontainer/.env` consumed by Compose. Repository
dependencies are installed by the post-create step, not maintained manually
on the host.

Open the repository from WSL:

```bash
code .
```

Choose **Dev Containers: Reopen in Container** to use the default published
configuration. To work in the source-built container, start it with the Nx
target above and choose **Dev Containers: Attach to Running Container**.

## Verify changes

Inside the devcontainer:

```bash
npx nx run-many -t verify --all --parallel=1
npx nx run hello-world:image-smoke
```

For Rust-only changes, the aggregate target is:

```bash
npx nx run rust:verify
```

Stop only this workspace's Compose project when finished:

```bash
npx nx run devcontainer:down
```

Add `-- --volumes` only when the workspace's named volumes should also be
deleted. The cleanup target never performs a global Docker prune.

For editor-free WSL operation, source and published lifecycle commands, the
fresh-machine regression sequence, monitoring, and safe cleanup, see
[Headless devcontainer operations](docs/devcontainer/headless-operations.md).
For image publication and verification, see
[Container publishing](docs/containers/publishing.md).
