# Headless devcontainer operations on WSL

This guide runs the same local-first development environment as VS Code
without opening the editor. The source configuration builds an Ubuntu 24.04
workspace from `.devcontainer/Dockerfile`; the published configuration starts
the immutable GHCR image pinned in `.devcontainer/docker-compose.yml`. Both run
commands as the non-root `vscode` user and use Docker-from-Docker to reach the
WSL host daemon.

## One-time host setup

Run the Windows bootstrap from PowerShell 7 as Administrator.

> [!WARNING]
> This is the full developer-machine bootstrap. It removes Docker Desktop and
> existing WSL distributions, configures Defender and `.wslconfig`, and
> installs a fresh Ubuntu 24.04 distribution.

```powershell
.\dev\bootstrap-dev-env.ps1
```

Then enter Ubuntu 24.04 and run the Linux bootstrap. It performs the full
administrator setup for generic host prerequisites, including Docker daemon
configuration and namespace sysctls, and clears existing Docker containers,
volumes, and networks.

```bash
./dev/bootstrap-dev-env.sh
```

## Configure credentials and host tools

Create the host directories mounted into the workspace:

```bash
mkdir -p "$HOME/.azure" "$HOME/.config/gh"
chmod 700 "$HOME/.azure" "$HOME/.config/gh"
```

The host's `~/.azure` directory is bind-mounted read/write at
`/home/vscode/.azure`, and `~/.config/gh` is mounted at
`/home/vscode/.config/gh`. Run `az login` in WSL before starting the container
when Azure access is needed. Run `gh auth login` in the devcontainer before
local GHCR publication. Login and logout changes in the container therefore
also affect the mounted WSL host state.

Install the pinned host CLI dependencies:

```bash
npm ci
```

The devcontainer `initializeCommand` creates the credential directories when
needed and writes the ignored `.devcontainer/.env` used by Compose. Repository
dependencies, including Nx, are installed inside the workspace by its
post-create step.

## Build and verify the source configuration

`devcontainer:build` explicitly builds the content-hashed source image for
`linux/amd64`. `devcontainer:up-source` starts the local Compose build, while
`devcontainer:test-source` starts it and runs every Nx `verify` target:

```bash
npx nx run devcontainer:build
npx nx run devcontainer:up-source
npx nx run devcontainer:test-source
```

The three targets are useful independently; a normal source regression can use
only `devcontainer:test-source` because it depends on `up-source`.

The equivalent direct CLI flow captures the exact source-built workspace
container:

```bash
up_json="$(
  npx --no-install devcontainer up \
    --workspace-folder . \
    --config .devcontainer/source/devcontainer.json \
    --frozen-lockfile
)"
container_id="$(jq -er '.containerId' <<<"$up_json")"

docker exec \
  --user vscode \
  --workdir /workspaces/rust-playground \
  "$container_id" \
  bash -lc 'npx nx run-many -t verify --all --parallel=1'
```

## Start and verify the published configuration

The default configuration uses the exact immutable image reference checked
into `.devcontainer/docker-compose.yml`:

```bash
npx nx run devcontainer:up
npx nx run devcontainer:test
```

A normal prebuilt regression can use only `devcontainer:test` because it
depends on `up`.

Capture the exact published-image workspace directly with:

```bash
up_json="$(
  npx --no-install devcontainer up \
    --workspace-folder . \
    --config .devcontainer/devcontainer.json
)"
container_id="$(jq -er '.containerId' <<<"$up_json")"

docker exec \
  --user vscode \
  --workdir /workspaces/rust-playground \
  "$container_id" \
  bash -lc 'npx nx run-many -t verify --all --parallel=1'
```

## Run unattended

Use `tmux` for work that must survive a terminal disconnect:

```bash
mkdir -p logs/devcontainer
tmux new-session -s rust-playground
npx nx run devcontainer:test-source 2>&1 \
  | tee "logs/devcontainer/verify-$(date -u +%Y%m%dT%H%M%SZ).log"
```

Detach with `Ctrl-b d` and return with:

```bash
tmux attach-session -t rust-playground
```

From Windows, a headless source start can also be dispatched directly into
WSL:

```powershell
wsl.exe -d Ubuntu-24.04 --cd /workspaces/rust-playground -- bash -lc "npm ci && npx nx run devcontainer:up-source"
```

Keep Docker managed by WSL systemd so the daemon remains available after the
launching terminal closes.

## Monitor the workspace

Capture the workspace ID and its exact Compose project:

```bash
up_json="$(
  npx --no-install devcontainer up \
    --workspace-folder . \
    --config .devcontainer/source/devcontainer.json \
    --frozen-lockfile
)"
container_id="$(jq -er '.containerId' <<<"$up_json")"
compose_project="$(
  docker inspect \
    --format '{{ index .Config.Labels "com.docker.compose.project" }}' \
    "$container_id"
)"
```

Useful checks:

```bash
docker stats "$container_id"
docker logs --tail 100 "$container_id"
docker inspect --format '{{json .State}}' "$container_id" | jq
docker ps --filter "label=com.docker.compose.project=$compose_project"
docker exec "$container_id" docker version
```

`docker version` inside the workspace uses Docker-from-Docker: the CLI talks to
the WSL host daemon through a forwarded socket. Containers started there are
siblings, not children of the workspace.

## Scoped cleanup

The `down` target discovers Compose projects from this workspace's container
labels and stops only those projects:

```bash
npx nx run devcontainer:down
```

To also delete this workspace's named volumes:

```bash
npx nx run devcontainer:down -- --volumes
```

Inspect the exact cleanup command without changing Docker state:

```bash
node .devcontainer/scripts/down.ts --dry-run
```

Do not use `docker system prune`; it affects unrelated projects.

## Final fresh-machine regression sequence

This is the final end-to-end order for a branch that has already been pushed
to a recoverable remote.

1. From a Windows checkout, rebuild WSL exactly with the destructive bootstrap:

   ```powershell
   Set-Location C:\git\rust-playground
   .\dev\bootstrap-dev-env.ps1
   ```

2. In the new Ubuntu 24.04 distribution, clone into the WSL filesystem, select
   the branch under test, and bootstrap the Linux host:

   ```bash
   sudo install -d -m 0775 -o "$USER" -g "$USER" /workspaces
   cd /workspaces
   git clone https://github.com/mdrakiburrahman/rust-playground.git
   cd rust-playground
   git switch <branch-under-test>
   ./dev/bootstrap-dev-env.sh
   npm ci
   ```

3. Make the source-built environment green, smoke-test the runtime image, log
   in to GHCR, and publish the devcontainer plus branch/SHA runtime tags from
   that exact container:

   ```bash
   npx nx run devcontainer:test-source
   npx --no-install devcontainer exec \
     --workspace-folder . \
     --config .devcontainer/source/devcontainer.json \
     bash -lc 'npx nx run hello-world:image-smoke'
   npx --no-install devcontainer exec \
     --workspace-folder . \
     --config .devcontainer/source/devcontainer.json \
     bash -lc 'npx nx run tools-scripts:registry-login -- --owner mdrakiburrahman --environment local && npx nx run devcontainer:publish && npx nx run hello-world:publish'
   ```

   Complete the public-visibility and manifest checks in
   [container publishing](../containers/publishing.md), then tear down only the
   source workspace:

   ```bash
   npx nx run devcontainer:down -- --volumes
   ```

4. Remove the exact local immutable devcontainer tag so the next start must use
   the registry, make the prebuilt environment green, smoke-test the runtime
   image again, and tear down:

   ```bash
   content_hash="$(cat .devcontainer/content-hash.txt)"
   docker image rm \
     "ghcr.io/mdrakiburrahman/rust-playground/devcontainer:${content_hash}" \
     2>/dev/null || true
   npx nx run devcontainer:test
   npx --no-install devcontainer exec \
     --workspace-folder . \
     --config .devcontainer/devcontainer.json \
     bash -lc 'npx nx run hello-world:image-smoke'
   npx nx run devcontainer:down -- --volumes
   ```

This sequence defines the expected regression operation. It does not claim
that CI or either publication workflow is currently green.

## Troubleshooting

- **`.devcontainer/.env` not found:** run
  `node .devcontainer/scripts/initialize.ts`, then start the workspace again.
- **Source feature lock is stale:** use the checked-in lockfile and
  `--frozen-lockfile`; update it only as a deliberate dependency change.
- **Generated image reference is stale:** run
  `npx nx run devcontainer:tag`, review both generated changes, and rerun
  `npx nx run devcontainer:verify`.
- **Published image cannot be pulled:** verify the exact content-hash manifest
  and public package visibility as described in the publishing guide.
- **Azure authentication is missing:** run `az login` in WSL and verify
  `az account show`; the mounted directory must be owned by the WSL user.
- **GitHub authentication is missing:** run `gh auth login` in the
  devcontainer and verify `gh auth status`.
- **Docker forwarding fails:** verify `docker version` on the WSL host and in
  the workspace.
- **Stale project:** run `npx nx run devcontainer:down -- --volumes`, then start
  the selected configuration again. Do not use a global Docker prune.
