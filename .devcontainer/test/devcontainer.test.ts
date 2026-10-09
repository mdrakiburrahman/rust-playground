import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { join, resolve } from 'node:path';
import test from 'node:test';

import {
  createBuildInvocation,
  type BuildDependencies,
  buildDevcontainer,
} from '../scripts/build.ts';
import {
  computeContentHash,
  isContentHashInput,
  renderComposeImage,
} from '../scripts/content-hash.ts';
import {
  downDevcontainer,
  type DownDependencies,
} from '../scripts/down.ts';
import {
  initializeHost,
  type InitializeDependencies,
} from '../scripts/initialize.ts';
import {
  runPostCreate,
  type PostCreateDependencies,
} from '../scripts/post-create.ts';
import {
  branchAlias,
  publishDevcontainer,
  type ProcessResult,
  type PublishDependencies,
} from '../scripts/publish.ts';

const repositoryRoot = resolve(
  new URL('../../', import.meta.url).pathname.replace(/^\/([A-Za-z]:)/u, '$1'),
);

test('normal and source configurations share workspace, user, and lifecycle behavior', () => {
  const normal = JSON.parse(
    readFileSync(join(repositoryRoot, '.devcontainer', 'devcontainer.json'), 'utf8'),
  ) as Record<string, unknown>;
  const source = JSON.parse(
    readFileSync(
      join(
        repositoryRoot,
        '.devcontainer',
        'source',
        'devcontainer.json',
      ),
      'utf8',
    ),
  ) as Record<string, unknown>;
  for (const key of [
    'service',
    'runServices',
    'workspaceFolder',
    'shutdownAction',
    'containerUser',
    'remoteUser',
    'updateRemoteUserUID',
    'initializeCommand',
    'postCreateCommand',
    'waitFor',
    'customizations',
  ]) {
    assert.deepEqual(source[key], normal[key], `configuration mismatch: ${key}`);
  }
  assert.equal(normal.dockerComposeFile, 'docker-compose.yml');
  assert.deepEqual(source.dockerComposeFile, [
    '../docker-compose.yml',
    '../docker-compose.local.yml',
  ]);

  const compose = readFileSync(
    join(repositoryRoot, '.devcontainer', 'docker-compose.yml'),
    'utf8',
  );
  assert.match(
    compose,
    /image: ghcr\.io\/mdrakiburrahman\/rust-playground\/devcontainer:[a-f0-9]{64}/u,
  );
  assert.match(compose, /target: \/workspaces\/rust-playground/u);
  assert.match(compose, /target: \/home\/vscode\/\.azure/u);
  assert.match(compose, /target: \/home\/vscode\/\.config\/gh/u);
  assert.match(
    compose,
    /source: node_modules[\s\S]*target: \/workspaces\/rust-playground\/node_modules/u,
  );
  assert.match(
    compose,
    /source: nx_cache[\s\S]*target: \/workspaces\/rust-playground\/\.nx/u,
  );
  assert.equal(compose.match(/read_only: false/gu)?.length, 2);

  const localCompose = readFileSync(
    join(repositoryRoot, '.devcontainer', 'docker-compose.local.yml'),
    'utf8',
  );
  assert.match(localCompose, /dockerfile: \.devcontainer\/Dockerfile/u);
});

test('source and image-build configurations share pinned tools and features', () => {
  const source = JSON.parse(
    readFileSync(
      join(
        repositoryRoot,
        '.devcontainer',
        'source',
        'devcontainer.json',
      ),
      'utf8',
    ),
  ) as {
    features: Record<string, Record<string, unknown>>;
  };
  const imageBuild = JSON.parse(
    readFileSync(
      join(repositoryRoot, '.devcontainer', 'devcontainer.build.json'),
      'utf8',
    ),
  ) as {
    build: Record<string, unknown>;
    dockerComposeFile?: unknown;
    features: Record<string, Record<string, unknown>>;
  };
  assert.deepEqual(imageBuild.build, {
    dockerfile: 'Dockerfile',
    context: '..',
  });
  assert.equal(imageBuild.dockerComposeFile, undefined);
  assert.deepEqual(imageBuild.features, source.features);
  assert.deepEqual(source.features['ghcr.io/devcontainers/features/rust:1'], {
    version: '1.96.1',
    profile: 'minimal',
    components: 'rustfmt,clippy',
  });
  assert.equal(
    source.features['ghcr.io/devcontainers/features/node:2']?.version,
    '24',
  );
  for (const feature of [
    'ghcr.io/devcontainers/features/azure-cli:1',
    'ghcr.io/devcontainers/features/github-cli:1',
    'ghcr.io/devcontainers/features/docker-outside-of-docker:1',
  ]) {
    assert.ok(source.features[feature], `missing feature: ${feature}`);
  }
});

test('Dockerfile is Ubuntu 24.04 with the purposeful Rust build packages', () => {
  const dockerfile = readFileSync(
    join(repositoryRoot, '.devcontainer', 'Dockerfile'),
    'utf8',
  );
  assert.match(
    dockerfile,
    /^FROM mcr\.microsoft\.com\/devcontainers\/base:ubuntu-24\.04$/mu,
  );
  for (const packageName of [
    'build-essential',
    'clang',
    'cmake',
    'curl',
    'git',
    'jq',
    'libssl-dev',
    'lldb',
    'mold',
    'pkg-config',
  ]) {
    assert.ok(
      dockerfile
        .split(/\r?\n/u)
        .some((line) => line.trim() === `${packageName} \\`),
      packageName,
    );
  }
});

test('feature lock covers source and image-build features with immutable digests', () => {
  const source = JSON.parse(
    readFileSync(
      join(
        repositoryRoot,
        '.devcontainer',
        'source',
        'devcontainer.json',
      ),
      'utf8',
    ),
  ) as {
    features: Record<string, unknown>;
  };
  const lock = JSON.parse(
    readFileSync(
      join(repositoryRoot, '.devcontainer', 'devcontainer-lock.json'),
      'utf8',
    ),
  ) as {
    features: Record<
      string,
      { integrity: string; resolved: string; version: string }
    >;
  };
  const sourceLock = JSON.parse(
    readFileSync(
      join(
        repositoryRoot,
        '.devcontainer',
        'source',
        'devcontainer-lock.json',
      ),
      'utf8',
    ),
  ) as typeof lock;
  const imageBuild = JSON.parse(
    readFileSync(
      join(repositoryRoot, '.devcontainer', 'devcontainer.build.json'),
      'utf8',
    ),
  ) as {
    features: Record<string, unknown>;
  };
  assert.deepEqual(
    Object.keys(lock.features).sort(),
    Object.keys(source.features).sort(),
  );
  assert.deepEqual(
    Object.keys(lock.features).sort(),
    Object.keys(imageBuild.features).sort(),
  );
  assert.deepEqual(sourceLock, lock);
  for (const [feature, entry] of Object.entries(lock.features)) {
    assert.match(entry.version, /^\d+\.\d+\.\d+$/u, feature);
    assert.match(entry.integrity, /^sha256:[a-f0-9]{64}$/u, feature);
    assert.ok(entry.resolved.endsWith(`@${entry.integrity}`), feature);
  }
});

test('Nx project exposes the complete devcontainer lifecycle', () => {
  const project = JSON.parse(
    readFileSync(join(repositoryRoot, '.devcontainer', 'project.json'), 'utf8'),
  ) as {
    targets: Record<
      string,
      {
        options?: {
          command?: string;
        };
      }
    >;
  };
  for (const target of [
    'build',
    'up-source',
    'up',
    'test-source',
    'test',
    'scripts-typecheck',
    'scripts-test',
    'verify',
    'down',
    'tag',
    'publish',
  ]) {
    assert.ok(project.targets[target], `missing target: ${target}`);
  }
  for (const target of ['up-source', 'test-source']) {
    const command = project.targets[target]?.options?.command ?? '';
    assert.match(
      command,
      /--config \.devcontainer\/source\/devcontainer\.json/u,
    );
    assert.doesNotMatch(command, /--platform|--push/u);
  }
});

test('initialize creates host credential directories and Compose environment state', () => {
  const directories: string[] = [];
  const files = new Map<string, string>();
  const dependencies: InitializeDependencies = {
    createDirectory(path) {
      directories.push(path);
    },
    homeDirectory() {
      return 'C:\\Users\\developer';
    },
    writeFile(path, contents) {
      files.set(path, contents);
    },
  };

  initializeHost(dependencies, 'C:\\repo');

  assert.deepEqual(directories, [
    join('C:\\Users\\developer', '.azure'),
    join('C:\\Users\\developer', '.config', 'gh'),
  ]);
  assert.equal(
    files.get(join('C:\\repo', '.devcontainer', '.env')),
    [
      'HOST_AZURE_DIR=C:/Users/developer/.azure',
      'HOST_GH_CONFIG_DIR=C:/Users/developer/.config/gh',
      '',
    ].join('\n'),
  );
});

test('content hash is order-independent, path-aware, and line-ending normalized', () => {
  const left = computeContentHash([
    { path: '.devcontainer/b', contents: 'two\r\nlines\r\n' },
    { path: '.devcontainer/a', contents: 'one\rline' },
  ]);
  const right = computeContentHash([
    { path: '.devcontainer/a', contents: 'one\nline' },
    { path: '.devcontainer/b', contents: 'two\nlines\n' },
  ]);
  assert.equal(left, right);
  assert.notEqual(
    left,
    computeContentHash([
      { path: '.devcontainer/a', contents: 'two\nlines\n' },
      { path: '.devcontainer/b', contents: 'one\nline' },
    ]),
  );
  assert.equal(isContentHashInput('.devcontainer/Dockerfile'), true);
  assert.equal(isContentHashInput('.dockerignore'), true);
  assert.equal(
    isContentHashInput('.devcontainer/devcontainer.build.json'),
    true,
  );
  assert.equal(
    isContentHashInput('.devcontainer/devcontainer-lock.json'),
    true,
  );
  assert.equal(
    isContentHashInput('.devcontainer/source/devcontainer.json'),
    false,
  );
  assert.equal(
    isContentHashInput('.devcontainer/docker-compose.local.yml'),
    false,
  );
  assert.equal(isContentHashInput('.devcontainer/docker-compose.yml'), false);
  assert.equal(isContentHashInput('.devcontainer/content-hash.txt'), false);
  assert.equal(isContentHashInput('.devcontainer/.env'), false);
  assert.equal(
    isContentHashInput('.devcontainer/scripts/post-create.ts'),
    false,
  );
  assert.equal(
    isContentHashInput('.devcontainer/test/devcontainer.test.ts'),
    false,
  );
});

test('Compose image rendering updates exactly the immutable image reference', () => {
  const hash = 'a'.repeat(64);
  const rendered = renderComposeImage(
    `services:\n  workspace:\n    image: ghcr.io/mdrakiburrahman/rust-playground/devcontainer:${'0'.repeat(64)}\n`,
    hash,
  );
  assert.match(rendered, new RegExp(`${hash}\\n$`, 'u'));
  assert.throws(
    () => renderComposeImage('services:\n  workspace: {}\n', hash),
    /exactly one/u,
  );
});

test('post-create validates tools, installs pinned cargo-make, then runs npm ci', () => {
  const events: string[] = [];
  const dependencies: PostCreateDependencies = {
    capture(command, args, cwd) {
      events.push(`capture:${command}:${args.join(' ')}:${cwd}`);
      if (command === 'rustc') {
        return 'rustc 1.96.1 (stable)\n';
      }
      if (command === 'id') {
        return '1000\n';
      }
      return `${command} version\n`;
    },
    commandExists(command) {
      events.push(`exists:${command}`);
      return false;
    },
    execute(command, args, cwd) {
      events.push(`execute:${command}:${args.join(' ')}:${cwd}`);
    },
    nodeVersion: 'v24.17.0',
    stdout() {},
  };

  runPostCreate({ dryRun: false }, dependencies, '/repo');

  assert.deepEqual(events, [
    'capture:rustc:--version:/repo',
    'capture:rustfmt:--version:/repo',
    'capture:cargo:clippy --version:/repo',
    'exists:cargo-make',
    'execute:cargo:install cargo-make --version 0.37.24 --locked:/repo',
    'capture:id:-u:/repo',
    'capture:id:-g:/repo',
    `execute:sudo:chown -R 1000:1000 ${join('/repo', 'node_modules')} ${join('/repo', '.nx')}:/repo`,
    'execute:npm:ci:/repo',
  ]);
});

test('post-create replaces an unpinned cargo-make and fails wrong Rust early', () => {
  const executions: string[] = [];
  const dependencies: PostCreateDependencies = {
    capture(command) {
      if (command === 'rustc') {
        return 'rustc 1.96.1 (stable)\n';
      }
      if (command === 'cargo-make') {
        return 'cargo-make 0.37.23\n';
      }
      if (command === 'id') {
        return '1000\n';
      }
      return 'ok\n';
    },
    commandExists() {
      return true;
    },
    execute(command, args) {
      executions.push(`${command} ${args.join(' ')}`);
    },
    nodeVersion: 'v24.0.0',
    stdout() {},
  };
  runPostCreate({ dryRun: false }, dependencies, '/repo');
  assert.deepEqual(executions, [
    'cargo install cargo-make --version 0.37.24 --locked --force',
    `sudo chown -R 1000:1000 ${join('/repo', 'node_modules')} ${join('/repo', '.nx')}`,
    'npm ci',
  ]);

  assert.throws(
    () =>
      runPostCreate(
        { dryRun: false },
        {
          ...dependencies,
          capture(command) {
            return command === 'rustc' ? 'rustc 1.95.0 (stable)\n' : 'ok\n';
          },
        },
        '/repo',
      ),
    /Expected Rust 1\.96\.1/u,
  );
});

test('post-create dry-run is dependency-free and side-effect free', () => {
  const output: string[] = [];
  runPostCreate(
    { dryRun: true },
    {
      capture() {
        assert.fail('dry-run must not execute commands');
      },
      commandExists() {
        assert.fail('dry-run must not inspect PATH');
      },
      execute() {
        assert.fail('dry-run must not execute commands');
      },
      nodeVersion: 'v24.0.0',
      stdout(message) {
        output.push(message);
      },
    },
    '/repo',
  );
  assert.match(output.join(''), /cargo-make 0\.37\.24/u);
  assert.match(output.join(''), /npm ci/u);
});

test('down discovers unique workspace Compose projects and removes only those projects', () => {
  const executions: Array<{ args: readonly string[]; command: string }> = [];
  const stderr: string[] = [];
  const dependencies: DownDependencies = {
    capture(_command, args) {
      if (args[0] === 'ps') {
        return 'container-one\ncontainer-two\ncontainer-three\n';
      }
      const id = args.at(-1);
      return id === 'container-three' ? '<no value>\n' : 'workspace-project\n';
    },
    execute(command, args) {
      executions.push({ args, command });
    },
    stderr(message) {
      stderr.push(message);
    },
    stdout() {},
  };

  downDevcontainer(
    { dryRun: false, removeVolumes: true },
    dependencies,
    '/repo',
  );
  assert.deepEqual(executions, [
    {
      command: 'docker',
      args: [
        'compose',
        '--project-name',
        'workspace-project',
        '--project-directory',
        join('/repo', '.devcontainer'),
        '--file',
        join('/repo', '.devcontainer', 'docker-compose.yml'),
        'down',
        '--remove-orphans',
        '--volumes',
      ],
    },
  ]);
  assert.match(stderr.join(''), /container-three/u);
});

test('build uses the Dockerfile config, local pinned CLI, lock, and linux/amd64', () => {
  const hash = 'b'.repeat(64);
  const invocation = createBuildInvocation(
    hash,
    true,
    repositoryRoot,
    '/node',
  );
  assert.equal(invocation.command, '/node');
  assert.equal(
    createBuildInvocation(hash, false, repositoryRoot).command,
    process.execPath,
  );
  assert.deepEqual(invocation.args, [
    join(
      repositoryRoot,
      'node_modules',
      '@devcontainers',
      'cli',
      'devcontainer.js',
    ),
    'build',
    '--workspace-folder',
    '.',
    '--config',
    '.devcontainer/devcontainer.build.json',
    '--image-name',
    `ghcr.io/mdrakiburrahman/rust-playground/devcontainer:${hash}`,
    '--platform',
    'linux/amd64',
    '--frozen-lockfile',
    '--push',
    'true',
  ]);
});

test('build command reads the checked-in hash and does not push by default', () => {
  const executions: Array<{ args: readonly string[]; command: string }> = [];
  const dependencies: BuildDependencies = {
    execute(command, args) {
      executions.push({ args, command });
    },
    nodeExecutable: '/node',
    stdout() {},
  };
  buildDevcontainer(false, dependencies, repositoryRoot);
  assert.equal(executions.length, 1);
  assert.doesNotMatch(executions[0]?.args.join(' ') ?? '', /--push/u);
});

function manifestResult(
  manifest: unknown,
  status = 0,
  stderr = '',
): ProcessResult {
  return {
    status,
    stderr,
    stdout: status === 0 ? JSON.stringify(manifest) : '',
  };
}

function provenanceIndex(reverse = false): unknown {
  const manifests = [
    {
      mediaType: 'application/vnd.oci.image.manifest.v1+json',
      digest: 'sha256:image',
      size: 1234,
      platform: {
        architecture: 'amd64',
        os: 'linux',
      },
    },
    {
      mediaType: 'application/vnd.oci.image.manifest.v1+json',
      digest: 'sha256:provenance',
      size: 567,
      annotations: {
        'vnd.docker.reference.digest': 'sha256:image',
        'vnd.docker.reference.type': 'attestation-manifest',
      },
      platform: {
        architecture: 'unknown',
        os: 'unknown',
      },
    },
  ];
  return {
    schemaVersion: 2,
    mediaType: 'application/vnd.oci.image.index.v1+json',
    manifests: reverse ? manifests.reverse() : manifests,
  };
}

test('publish is idempotent for an OCI index with provenance', () => {
  const captures: string[] = [];
  const executions: string[] = [];
  const dependencies: PublishDependencies = {
    capture(command, args) {
      captures.push(`${command} ${args.join(' ')}`);
      return manifestResult(
        args.at(-1)?.endsWith(':feature-example')
          ? provenanceIndex(true)
          : provenanceIndex(),
      );
    },
    environment: {},
    execute(command, args) {
      executions.push(`${command} ${args.join(' ')}`);
    },
    nodeExecutable: '/node',
    stdout() {},
  };
  publishDevcontainer({ branch: 'feature/example' }, dependencies, repositoryRoot);
  assert.equal(captures.length, 2);
  assert.deepEqual(executions, []);
});

test('publish builds a missing immutable image and updates main/latest aliases', () => {
  const executions: string[] = [];
  const inspectCounts = new Map<string, number>();
  const manifest = provenanceIndex();
  const dependencies: PublishDependencies = {
    capture(command, args) {
      assert.equal(command, 'docker');
      const reference = args.at(-1) ?? '';
      const count = inspectCounts.get(reference) ?? 0;
      inspectCounts.set(reference, count + 1);
      if (count === 0) {
        return manifestResult(undefined, 1, 'manifest unknown');
      }
      return manifestResult(manifest);
    },
    environment: {},
    execute(command, args) {
      executions.push(`${command} ${args.join(' ')}`);
    },
    nodeExecutable: '/node',
    stdout() {},
  };

  publishDevcontainer({ branch: 'main' }, dependencies, repositoryRoot);

  assert.match(executions[0] ?? '', /^\/node .*devcontainer\.js build/u);
  assert.match(executions[0] ?? '', /--config \.devcontainer\/devcontainer\.build\.json/u);
  assert.match(executions[0] ?? '', /--push true/u);
  assert.equal(executions.length, 2);
  assert.match(
    executions[1] ?? '',
    /^docker buildx imagetools create --prefer-index=false /u,
  );
  assert.match(executions[1] ?? '', /--tag .*:main/u);
  assert.match(executions[1] ?? '', /--tag .*:latest/u);
  assert.ok(executions[1]?.endsWith(`:${readFileSync(
    join(repositoryRoot, '.devcontainer', 'content-hash.txt'),
    'utf8',
  ).trim()}`));
  assert.doesNotMatch(executions.join('\n'), /docker image (?:pull|tag|push)/u);
});

test('publish rejects an alias that flattened an OCI index', () => {
  let aliasInspections = 0;
  const executions: string[] = [];
  const dependencies: PublishDependencies = {
    capture(_command, args) {
      const reference = args.at(-1) ?? '';
      if (!reference.endsWith(':feature-example')) {
        return manifestResult(provenanceIndex());
      }
      aliasInspections += 1;
      return aliasInspections === 1
        ? manifestResult(undefined, 1, 'manifest unknown')
        : manifestResult({
            schemaVersion: 2,
            config: { digest: 'sha256:image' },
          });
    },
    environment: {},
    execute(command, args) {
      executions.push(`${command} ${args.join(' ')}`);
    },
    nodeExecutable: '/node',
    stdout() {},
  };

  assert.throws(
    () =>
      publishDevcontainer(
        { branch: 'feature/example' },
        dependencies,
        repositoryRoot,
      ),
    /does not match/u,
  );
  assert.equal(executions.length, 1);
  assert.match(executions[0] ?? '', /^docker buildx imagetools create /u);
});

test('publish retries transient registry inspection failures', () => {
  let captures = 0;
  const sleeps: number[] = [];
  const dependencies: PublishDependencies = {
    capture() {
      captures += 1;
      if (captures === 1) {
        return manifestResult(undefined, 1, 'Get "https://ghcr.io/v2/": EOF');
      }
      return manifestResult(provenanceIndex());
    },
    environment: {},
    execute() {
      assert.fail('current manifests must not be republished');
    },
    nodeExecutable: '/node',
    sleep(milliseconds) {
      sleeps.push(milliseconds);
    },
    stdout() {},
  };

  publishDevcontainer({ branch: 'feature/example' }, dependencies, repositoryRoot);

  assert.equal(captures, 3);
  assert.deepEqual(sleeps, [1_000]);
});

test('publish treats registry errors as failures instead of missing manifests', () => {
  const dependencies: PublishDependencies = {
    capture() {
      return manifestResult(undefined, 1, 'unauthorized: authentication required');
    },
    environment: {},
    execute() {
      assert.fail('registry errors must stop publication');
    },
    nodeExecutable: '/node',
    stdout() {},
  };
  assert.throws(
    () => publishDevcontainer({ branch: 'feature' }, dependencies, repositoryRoot),
    /Unable to inspect remote manifest/u,
  );
});

test('branch aliases are valid, deterministic, and bounded', () => {
  assert.equal(branchAlias('refs/heads/Feature/Add API'), 'feature-add-api');
  const long = branchAlias(`feature/${'x'.repeat(200)}`);
  assert.ok(long.length <= 128);
  assert.equal(long, branchAlias(`feature/${'x'.repeat(200)}`));
});
