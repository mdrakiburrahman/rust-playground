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
    "tags": ["{gitBranchTag}", "sha-{gitSha}"],
    "immutableTags": ["sha-{gitSha}"],
    "requireCleanWorktree": true,
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
| `immutableTags` | No | Subset of push tags that are published only when their remote manifest is missing. |
| `requireCleanWorktree` | No | Reject tracked or untracked Git changes before any Docker command. |
| `buildArgs` | No | Values passed as separate `--build-arg` arguments. |
| `platforms` | No | `os/architecture[/variant]` values. |
| `output` | Yes | `load` adds only `--load`; `push` adds only `--push`. |
| `manifestFile` | No | Newline-delimited full image references written after success. |

Load mode accepts zero or one platform because Docker cannot load a
multi-platform manifest into the local engine. Use push mode for multiple
platforms.

For immutable push tags, the executor runs
`docker buildx imagetools inspect` before the build. An existing manifest is
omitted from the build while mutable tags continue to update. Only explicit
missing-manifest responses are accepted as absent; authentication, registry,
network, and other inspection failures stop publication. All missing immutable
and mutable tags are passed to one build. If every selected tag is an existing
immutable tag, the executor succeeds without a build.

When `requireCleanWorktree` is enabled, `git status --porcelain=v1
--untracked-files=all` must report no tracked or untracked changes before the
first Docker inspection or build command.

## Tokens

String options support:

- `{absWorkspaceRoot}`: absolute Nx workspace root.
- `{absProjectRoot}`: absolute current project root.
- `{projectName}`: current Nx project name.
- `{env:NAME}`: exact value of a required environment variable.
- `{gitSha}`: lowercase full Git object ID.
- `{gitShortSha}`: first 12 characters of the Git object ID.
- `{gitBranch}`: lowercase Docker-tag-safe branch value.
- `{gitBranchTag}`: `branch-`-namespaced branch tag bounded to 128
  characters.

Git values first use common CI environment variables, including
`REGISTRY_SHA`, `GITHUB_SHA`, `REGISTRY_BRANCH`, and `GITHUB_HEAD_REF`, and
otherwise use argument-array Git commands. Missing requested values fail the
executor. Branch tokens are normalized and sanitized. Values that exceed their
available tag space retain a readable prefix plus a deterministic 16-character
SHA-256 suffix, so long branches with the same prefix remain distinct.
`{gitBranchTag}` includes its `branch-` namespace in the 128-character bound,
preventing branch names from colliding with release or immutable namespaces.
Every final tag is validated against Docker's tag grammar.

Docker and Git commands are spawned directly with argument arrays and
`shell: false`. Verbose command logging redacts assigned build-argument values.

The root package intentionally is not an npm workspace. The plugin
`project.json` therefore marks this package as locally resolvable metadata so
Nx 23 can resolve `rust-playground-build:docker-build` without modifying the
root package manifest or lockfile.
