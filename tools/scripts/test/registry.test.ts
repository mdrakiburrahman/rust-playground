import assert from 'node:assert/strict';
import test from 'node:test';

import {
  createRegistryImage,
  createRegistryTagMetadata,
  encodePackageName,
  inspectPackageVisibility,
  loginToRegistry,
  RegistryCommandError,
  resolveExecutionEnvironment,
  sanitizeBranchName,
  verifyRemoteManifest,
} from '../src/registry-lib.js';
import type {
  CommandOptions,
  CommandResult,
  CommandRunner,
} from '../src/process.js';

interface CommandCall {
  readonly args: readonly string[];
  readonly command: string;
  readonly options?: CommandOptions;
}

class FakeRunner implements CommandRunner {
  readonly calls: CommandCall[] = [];

  constructor(private readonly results: CommandResult[]) {}

  run(
    command: string,
    args: readonly string[],
    options?: CommandOptions,
  ): CommandResult {
    this.calls.push({ args: [...args], command, options });
    const result = this.results.shift();
    assert.ok(result, `Unexpected command: ${command} ${args.join(' ')}`);
    return result;
  }

  assertComplete(): void {
    assert.equal(this.results.length, 0);
  }
}

const success = (stdout = ''): CommandResult => ({
  exitCode: 0,
  stderr: '',
  stdout,
});

const packageJson = (
  name: string,
  visibility: string,
  htmlUrl = `https://github.com/users/octocat/packages/container/${encodeURIComponent(name)}`,
): string =>
  JSON.stringify({
    html_url: htmlUrl,
    name,
    owner: { login: 'octocat' },
    package_type: 'container',
    visibility,
  });

test('uses GHCR_TOKEN first in CI and sends it only through docker stdin', () => {
  const runner = new FakeRunner([success('Login Succeeded\n')]);
  const token = 'ghcr-test-token';

  const result = loginToRegistry(
    {
      env: {
        GHCR_TOKEN: token,
        GITHUB_TOKEN: 'fallback-token',
      },
      environment: 'ci',
      owner: 'OctoCat',
    },
    runner,
  );

  assert.deepEqual(result, {
    credentialSource: 'GHCR_TOKEN',
    owner: 'octocat',
    registry: 'ghcr.io',
    status: 'authenticated',
  });
  assert.deepEqual(runner.calls, [
    {
      args: [
        'login',
        'ghcr.io',
        '--username',
        'octocat',
        '--password-stdin',
      ],
      command: 'docker',
      options: {
        input: `${token}\n`,
        stderr: 'capture',
        stdout: 'capture',
      },
    },
  ]);
  assert.doesNotMatch(JSON.stringify(result), new RegExp(token, 'u'));
  assert.doesNotMatch(runner.calls[0]?.args.join(' ') ?? '', new RegExp(token, 'u'));
  runner.assertComplete();
});

test('falls back to GITHUB_TOKEN in CI and rejects missing credentials', () => {
  const fallbackRunner = new FakeRunner([success()]);
  const fallback = loginToRegistry(
    {
      env: { GITHUB_TOKEN: 'github-test-token' },
      environment: 'ci',
      owner: 'octocat',
    },
    fallbackRunner,
  );
  assert.equal(fallback.credentialSource, 'GITHUB_TOKEN');
  assert.equal(
    fallbackRunner.calls[0]?.options?.input,
    'github-test-token\n',
  );

  const missingRunner = new FakeRunner([]);
  assert.throws(
    () =>
      loginToRegistry(
        {
          env: {},
          environment: 'ci',
          owner: 'octocat',
        },
        missingRunner,
      ),
    /GHCR_TOKEN or GITHUB_TOKEN/u,
  );
  assert.deepEqual(missingRunner.calls, []);
});

test('obtains local credentials from gh without exposing them in argv', () => {
  const token = 'local-test-token';
  const runner = new FakeRunner([
    success(`${token}\n`),
    success('Login Succeeded\n'),
  ]);

  const result = loginToRegistry(
    {
      env: {
        GHCR_TOKEN: 'ignored-outside-ci',
      },
      environment: 'local',
      owner: 'octocat',
    },
    runner,
  );

  assert.equal(result.credentialSource, 'gh-auth-token');
  assert.deepEqual(runner.calls[0], {
    args: ['auth', 'token'],
    command: 'gh',
    options: {
      stderr: 'capture',
      stdout: 'capture',
    },
  });
  assert.equal(runner.calls[1]?.options?.input, `${token}\n`);
  for (const call of runner.calls) {
    assert.doesNotMatch(call.args.join(' '), new RegExp(token, 'u'));
  }
  runner.assertComplete();
});

test('registry login failures do not include the credential', () => {
  const token = 'failure-test-token';
  const runner = new FakeRunner([
    {
      exitCode: 9,
      stderr: token,
      stdout: token,
    },
  ]);

  assert.throws(
    () =>
      loginToRegistry(
        {
          env: { GHCR_TOKEN: token },
          environment: 'ci',
          owner: 'octocat',
        },
        runner,
      ),
    (error: unknown) => {
      assert.ok(error instanceof RegistryCommandError);
      assert.equal(error.exitCode, 9);
      assert.doesNotMatch(error.message, new RegExp(token, 'u'));
      return true;
    },
  );
});

test('creates reusable branch, immutable SHA, and main/latest metadata', () => {
  const sha = 'ABCDEF0123456789ABCDEF0123456789ABCDEF01';
  const metadata = createRegistryTagMetadata({
    branch: 'refs/heads/main',
    gitSha: sha,
  });

  assert.equal(metadata.branchTag, 'main');
  assert.equal(metadata.gitSha, sha.toLowerCase());
  assert.equal(metadata.shaTag, `sha-${sha.toLowerCase()}`);
  assert.equal(metadata.isDefaultBranch, true);
  assert.equal(metadata.publishLatest, true);
  assert.deepEqual(metadata.tags, [
    { immutable: false, kind: 'branch', value: 'main' },
    {
      immutable: true,
      kind: 'git-sha',
      value: `sha-${sha.toLowerCase()}`,
    },
    { immutable: false, kind: 'latest', value: 'latest' },
  ]);
  assert.deepEqual(metadata.values, [
    'main',
    `sha-${sha.toLowerCase()}`,
    'latest',
  ]);
});

test('sanitizes feature branches without publishing latest', () => {
  const metadata = createRegistryTagMetadata({
    branch: 'Feature/Add Registry@V2',
    gitSha: 'b'.repeat(40),
  });

  assert.equal(sanitizeBranchName('Feature/Add Registry@V2'), 'feature-add-registry-v2');
  assert.equal(metadata.branchTag, 'feature-add-registry-v2');
  assert.equal(metadata.publishLatest, false);
  assert.deepEqual(metadata.values, [
    'feature-add-registry-v2',
    `sha-${'b'.repeat(40)}`,
  ]);
  assert.throws(
    () =>
      createRegistryTagMetadata({
        branch: 'main',
        gitSha: 'abc123',
      }),
    /full 40- or 64-character/u,
  );
});

test('normalizes GHCR coordinates and encodes nested package names', () => {
  assert.deepEqual(
    createRegistryImage({
      image: 'DevContainer',
      owner: 'OctoCat',
      repository: 'Rust-Playground',
    }),
    {
      image: 'devcontainer',
      owner: 'octocat',
      packageName: 'rust-playground/devcontainer',
      reference: 'ghcr.io/octocat/rust-playground/devcontainer',
      repository: 'rust-playground',
    },
  );
  assert.equal(
    encodePackageName('rust-playground/devcontainer'),
    'rust-playground%2Fdevcontainer',
  );
  assert.equal(
    encodePackageName('rust-playground/hello-world'),
    'rust-playground%2Fhello-world',
  );
});

test('verifies the exact remote manifest without printing it', () => {
  const runner = new FakeRunner([success()]);

  const result = verifyRemoteManifest(
    {
      image: 'hello-world',
      owner: 'octocat',
      repository: 'rust-playground',
      tag: `sha-${'c'.repeat(40)}`,
    },
    runner,
  );

  assert.deepEqual(result, {
    reference:
      `ghcr.io/octocat/rust-playground/hello-world:sha-${'c'.repeat(40)}`,
    status: 'verified',
  });
  assert.deepEqual(runner.calls[0], {
    args: [
      'manifest',
      'inspect',
      `ghcr.io/octocat/rust-playground/hello-world:sha-${'c'.repeat(40)}`,
    ],
    command: 'docker',
    options: {
      stderr: 'capture',
      stdout: 'ignore',
    },
  });
  runner.assertComplete();
});

test('reports a clear manifest error for an absent tag', () => {
  const runner = new FakeRunner([
    {
      exitCode: 1,
      stderr: 'no such manifest',
      stdout: '',
    },
  ]);

  assert.throws(
    () =>
      verifyRemoteManifest(
        {
          image: 'hello-world',
          owner: 'octocat',
          repository: 'rust-playground',
          tag: 'missing',
        },
        runner,
      ),
    /Remote image manifest was not found or is not accessible.*missing/u,
  );
});

test('returns a machine-readable UI action for a private user package', () => {
  const packageName = 'rust-playground/devcontainer';
  const runner = new FakeRunner([
    success(`[${packageJson(packageName, 'private')}]\n`),
  ]);

  const result = inspectPackageVisibility(
    {
      image: 'devcontainer',
      owner: 'octocat',
      repository: 'rust-playground',
    },
    runner,
  );

  assert.deepEqual(result, {
    currentVisibility: 'private',
    image: 'devcontainer',
    owner: 'octocat',
    packageName,
    reference: 'ghcr.io/octocat/rust-playground/devcontainer',
    repository: 'rust-playground',
    settingsUrl:
      'https://github.com/users/octocat/packages/container/rust-playground%2Fdevcontainer/settings',
    status: 'needs-ui-change',
    targetVisibility: 'public',
  });
  assert.equal(runner.calls.length, 1);
  assert.equal(runner.calls[0]?.args.includes('PATCH'), false);
  runner.assertComplete();
});

test('verifies public visibility through the encoded public package endpoint', () => {
  const packageName = 'rust-playground/hello-world';
  const runner = new FakeRunner([
    success(`[${packageJson(packageName, 'public')}]\n`),
    success(packageJson(packageName, 'public')),
  ]);

  const result = inspectPackageVisibility(
    {
      image: 'hello-world',
      owner: 'octocat',
      repository: 'rust-playground',
    },
    runner,
  );

  assert.equal(result.status, 'public');
  assert.deepEqual(runner.calls[1]?.args, [
    'api',
    'users/octocat/packages/container/rust-playground%2Fhello-world',
    '--method',
    'GET',
    '--header',
    'Accept: application/vnd.github+json',
  ]);
  assert.equal(
    runner.calls.some(({ args }) => args.includes('PATCH')),
    false,
  );
  runner.assertComplete();
});

test('paginates package lookup without interpolating the package into a query', () => {
  const firstPage = Array.from({ length: 100 }, (_, index) =>
    JSON.parse(packageJson(`unrelated-${index}`, 'private')),
  );
  const targetName = 'rust-playground/devcontainer';
  const runner = new FakeRunner([
    success(JSON.stringify(firstPage)),
    success(`[${packageJson(targetName, 'private')}]\n`),
  ]);

  const result = inspectPackageVisibility(
    {
      image: 'devcontainer',
      owner: 'octocat',
      repository: 'rust-playground',
    },
    runner,
  );

  assert.equal(result.status, 'needs-ui-change');
  assert.match(runner.calls[1]?.args[1] ?? '', /page=2$/u);
  runner.assertComplete();
});

test('resolves explicit and detected execution environments', () => {
  assert.equal(resolveExecutionEnvironment('local', { CI: 'true' }), 'local');
  assert.equal(resolveExecutionEnvironment(undefined, { CI: 'true' }), 'ci');
  assert.equal(resolveExecutionEnvironment(undefined, {}), 'local');
  assert.throws(
    () => resolveExecutionEnvironment('production', {}),
    /expected "ci" or "local"/u,
  );
});
