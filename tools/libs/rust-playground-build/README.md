# rust-playground-build

Local Nx plugin for general-purpose Docker Buildx image builds. It intentionally
contains no registry login, package-feed authentication, CI pre-task hooks, or
repository-specific publishing policy.

## Docker build executor

Use `rust-playground-build:docker-build` from an Nx target:

```json
{
  "executor": "rust-playground-build:docker-build",
  "options": {
    "file": "{absProjectRoot}/Dockerfile",
    "context": "{absWorkspaceRoot}",
    "image": "ghcr.io/example/application",
    "tags": ["{gitBranch}", "sha-{gitSha}"],
    "platforms": ["linux/amd64", "linux/arm64"],
    "output": "push",
    "manifestFile": "{absWorkspaceRoot}/artifacts/application-images.txt"
  }
}
```

| Option | Required | Behavior |
| --- | --- | --- |
| `file` | Yes | Dockerfile path. |
| `context` | No | Build context; defaults to the Dockerfile directory. |
| `image` | Yes | Repository without a tag or digest. |
| `tags` | Yes | Non-empty list of tags, validated after expansion. |
| `buildArgs` | No | Values passed as separate `--build-arg` arguments. |
| `platforms` | No | `os/architecture[/variant]` values. |
| `output` | Yes | `load` adds only `--load`; `push` adds only `--push`. |
| `manifestFile` | No | Newline-delimited full image references written after success. |

Load mode accepts zero or one platform because Docker cannot load a
multi-platform manifest into the local engine. Use push mode for multiple
platforms.

## Tokens

String options support:

- `{absWorkspaceRoot}`: absolute Nx workspace root.
- `{absProjectRoot}`: absolute current project root.
- `{projectName}`: current Nx project name.
- `{env:NAME}`: exact value of a required environment variable.
- `{gitSha}`: lowercase full Git object ID.
- `{gitShortSha}`: first 12 characters of the Git object ID.
- `{gitBranch}`: lowercase Docker-tag-safe branch value.

Git values first use common CI environment variables, including
`REGISTRY_SHA`, `GITHUB_SHA`, `REGISTRY_BRANCH`, and `GITHUB_HEAD_REF`, and
otherwise use argument-array Git commands. Missing requested values fail the
executor. Branch tokens are normalized, sanitized, and bounded to 128
characters. Every final tag is validated against Docker's tag grammar.

Docker and Git commands are spawned directly with argument arrays and
`shell: false`. Verbose command logging redacts assigned build-argument values.

The root package intentionally is not an npm workspace. The plugin
`project.json` therefore marks this package as locally resolvable metadata so
Nx 23 can resolve `rust-playground-build:docker-build` without modifying the
root package manifest or lockfile.
