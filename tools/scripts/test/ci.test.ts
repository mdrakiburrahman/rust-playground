import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { join, win32 } from 'node:path';
import test from 'node:test';
import { fileURLToPath } from 'node:url';

import {
  cleanupDevcontainer,
  createCiLabel,
  extractContainerId,
  normalizeComposeProject,
  prepareCiEnvironment,
  startDevcontainer,
  type CiFileSystem,
  verifyDevcontainer,
} from '../src/ci-lib.js';
import {
  CommandError,
  type CommandOptions,
  type CommandResult,
  type CommandRunner,
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

class FakeFileSystem implements CiFileSystem {
  readonly appends: Array<[string, string]> = [];
  readonly directories: string[] = [];
  readonly removed: string[] = [];
  readonly writes: Array<[string, string]> = [];

  appendFile(filePath: string, contents: string): void {
    this.appends.push([filePath, contents]);
  }

  ensurePrivateDirectory(directoryPath: string): void {
    this.directories.push(directoryPath);
  }

  removeFile(filePath: string): void {
    this.removed.push(filePath);
  }

  writePrivateFile(filePath: string, contents: string): void {
    this.writes.push([filePath, contents]);
  }
}

const success = (stdout = ''): CommandResult => ({
  exitCode: 0,
  stderr: '',
  stdout,
});

const failure = (exitCode: number, stdout = ''): CommandResult => ({
  exitCode,
  stderr: 'failure',
  stdout,
});

const sourceContainerId = 'a'.repeat(64);
const publishedContainerId = 'b'.repeat(64);

test('prepares only private host credential state required by Compose', () => {
  const fileSystem = new FakeFileSystem();
  const home = win32.join('C:\\', 'Users', 'runner');
  const workspace = win32.join('C:\\', 'checkout');

  prepareCiEnvironment({
    fileSystem,
    home,
    workspace,
  });

  assert.deepEqual(fileSystem.directories, [
    join(home, '.azure'),
    join(home, '.config', 'gh'),
  ]);
  assert.deepEqual(fileSystem.writes, [
    [
      join(workspace, '.devcontainer', '.env'),
      [
        'HOST_AZURE_DIR=C:/Users/runner/.azure',
        'HOST_GH_CONFIG_DIR=C:/Users/runner/.config/gh',
        '',
      ].join('\n'),
    ],
  ]);
  assert.equal(
    fileSystem.writes.some(([filePath]) => filePath === join(workspace, '.env')),
    false,
  );
});

test('normalizes lifecycle identifiers without accepting container names', () => {
  assert.equal(
    extractContainerId(
      JSON.stringify({
        containerId: sourceContainerId,
        remoteUser: 'vscode',
      }),
    ),
    sourceContainerId,
  );
  assert.equal(extractContainerId('{"containerId":"workspace"}'), undefined);
  assert.equal(extractContainerId('not JSON'), undefined);
  assert.equal(normalizeComposeProject('<no value>\n'), undefined);
  assert.equal(
    normalizeComposeProject('rust-playground-ci\n'),
    'rust-playground-ci',
  );
  assert.throws(
    () => normalizeComposeProject('Unsafe Project'),
    /Invalid Docker Compose project/u,
  );
  assert.equal(
    createCiLabel('42', '3', 'source'),
    'rust-playground.ci-run=42-3-source',
  );
  assert.equal(
    createCiLabel('42', '3', 'published'),
    'rust-playground.ci-run=42-3-published',
  );
});

test('starts the source config, validates the exact container, and writes outputs', () => {
  const workspace = join('C:\\', 'checkout');
  const runner = new FakeRunner([
    success(`${JSON.stringify({ containerId: sourceContainerId })}\n`),
    success(`${sourceContainerId}\n`),
    success('100-2-source\n'),
    success('rust-playground-ci\n'),
  ]);
  const fileSystem = new FakeFileSystem();
  const output: string[] = [];

  startDevcontainer(
    {
      githubOutput: join('C:\\', 'github', 'output'),
      kind: 'source',
      runAttempt: '2',
      runId: '100',
      workspace,
    },
    runner,
    fileSystem,
    (value) => output.push(value),
  );

  assert.deepEqual(runner.calls[0], {
    args: [
      '--no-install',
      'devcontainer',
      'up',
      '--workspace-folder',
      workspace,
      '--config',
      join(workspace, '.devcontainer', 'source', 'devcontainer.json'),
      '--frozen-lockfile',
      '--id-label',
      'rust-playground.ci-run=100-2-source',
    ],
    command: 'npx',
    options: {
      cwd: workspace,
      stdout: 'capture',
    },
  });
  assert.deepEqual(
    runner.calls.slice(1).map(({ args }) => args),
    [
      ['inspect', '--format', '{{ .Id }}', sourceContainerId],
      [
        'inspect',
        '--format',
        '{{ index .Config.Labels "rust-playground.ci-run" }}',
        sourceContainerId,
      ],
      [
        'inspect',
        '--format',
        '{{ index .Config.Labels "com.docker.compose.project" }}',
        sourceContainerId,
      ],
    ],
  );
  assert.deepEqual(fileSystem.appends, [
    [
      join('C:\\', 'github', 'output'),
      'ci_label=rust-playground.ci-run=100-2-source\nstart_kind=source\n',
    ],
    [
      join('C:\\', 'github', 'output'),
      `container_id=${sourceContainerId}\ncompose_project=rust-playground-ci\n`,
    ],
  ]);
  assert.deepEqual(output, [
    `${JSON.stringify({ containerId: sourceContainerId })}\n`,
  ]);
  runner.assertComplete();
});

test('published startup uses the immutable-image config without source locking', () => {
  const workspace = join('C:\\', 'checkout');
  const runner = new FakeRunner([
    success(JSON.stringify({ containerId: publishedContainerId })),
    success(publishedContainerId),
    success('101-1-published'),
    success('rust-playground-published'),
  ]);

  startDevcontainer(
    {
      githubOutput: join('C:\\', 'github', 'output'),
      kind: 'published',
      runAttempt: '1',
      runId: '101',
      workspace,
    },
    runner,
    new FakeFileSystem(),
    () => undefined,
  );

  assert.deepEqual(runner.calls[0]?.args, [
    '--no-install',
    'devcontainer',
    'up',
    '--workspace-folder',
    workspace,
    '--config',
    join(workspace, '.devcontainer', 'devcontainer.json'),
    '--id-label',
    'rust-playground.ci-run=101-1-published',
  ]);
  runner.assertComplete();
});

test('records a labeled partial startup before returning the original failure', () => {
  const runner = new FakeRunner([
    failure(17, 'not JSON\n'),
    success(`${sourceContainerId}\n`),
    success(`${sourceContainerId}\n`),
    success('200-1-source\n'),
    success('partial-project\n'),
  ]);
  const fileSystem = new FakeFileSystem();

  assert.throws(
    () =>
      startDevcontainer(
        {
          githubOutput: join('C:\\', 'github', 'output'),
          kind: 'source',
          runAttempt: '1',
          runId: '200',
          workspace: join('C:\\', 'checkout'),
        },
        runner,
        fileSystem,
        () => undefined,
      ),
    (error: unknown) => error instanceof CommandError && error.exitCode === 17,
  );

  assert.deepEqual(runner.calls[1], {
    args: [
      'ps',
      '-aq',
      '--no-trunc',
      '--filter',
      'label=rust-playground.ci-run=200-1-source',
    ],
    command: 'docker',
    options: {
      stderr: 'capture',
      stdout: 'capture',
    },
  });
  assert.deepEqual(fileSystem.appends.at(-1), [
    join('C:\\', 'github', 'output'),
    `container_id=${sourceContainerId}\ncompose_project=partial-project\n`,
  ]);
  runner.assertComplete();
});

test('failed startup still emits the unique cleanup label when no container exists', () => {
  const runner = new FakeRunner([
    failure(9, 'startup failed\n'),
    success(''),
  ]);
  const fileSystem = new FakeFileSystem();

  assert.throws(
    () =>
      startDevcontainer(
        {
          githubOutput: join('C:\\', 'github', 'output'),
          kind: 'source',
          runAttempt: '4',
          runId: '300',
          workspace: join('C:\\', 'checkout'),
        },
        runner,
        fileSystem,
        () => undefined,
      ),
    (error: unknown) => error instanceof CommandError && error.exitCode === 9,
  );
  assert.deepEqual(fileSystem.appends, [
    [
      join('C:\\', 'github', 'output'),
      'ci_label=rust-playground.ci-run=300-4-source\nstart_kind=source\n',
    ],
  ]);
  runner.assertComplete();
});

test('successful startup fails closed if no exact container can be recovered', () => {
  const runner = new FakeRunner([success('not JSON\n'), success('')]);

  assert.throws(
    () =>
      startDevcontainer(
        {
          githubOutput: join('C:\\', 'github', 'output'),
          kind: 'source',
          runAttempt: '1',
          runId: '301',
          workspace: join('C:\\', 'checkout'),
        },
        runner,
        new FakeFileSystem(),
        () => undefined,
      ),
    /succeeded without returning a container ID/u,
  );
  runner.assertComplete();
});

test('startup rejects a container whose inspected CI label does not match', () => {
  const runner = new FakeRunner([
    success(JSON.stringify({ containerId: sourceContainerId })),
    success(sourceContainerId),
    success('999-1-source'),
  ]);

  assert.throws(
    () =>
      startDevcontainer(
        {
          githubOutput: join('C:\\', 'github', 'output'),
          kind: 'source',
          runAttempt: '1',
          runId: '302',
          workspace: join('C:\\', 'checkout'),
        },
        runner,
        new FakeFileSystem(),
        () => undefined,
      ),
    /expected rust-playground\.ci-run=302-1-source/u,
  );
  runner.assertComplete();
});

test('runs the full verification aggregation and clean-tree gate in the exact container', () => {
  const runner = new FakeRunner([success()]);

  verifyDevcontainer({ containerId: sourceContainerId }, runner);

  assert.deepEqual(runner.calls, [
    {
      args: [
        'exec',
        '--user',
        'vscode',
        '--workdir',
        '/workspaces/rust-playground',
        sourceContainerId,
        'bash',
        '-lc',
        'npx nx run-many -t verify --all --parallel=1 && npx nx run tools-scripts:clean-worktree',
      ],
      command: 'docker',
      options: undefined,
    },
  ]);
  runner.assertComplete();
});

test('cleans only the recorded Compose project and its volumes', () => {
  const workspace = join('C:\\', 'checkout');
  const runner = new FakeRunner([success()]);
  const fileSystem = new FakeFileSystem();

  cleanupDevcontainer(
    {
      ciLabels: ['rust-playground.ci-run=400-1-source'],
      composeProject: 'recorded-project',
      containerId: sourceContainerId,
      workspace,
    },
    runner,
    fileSystem,
  );

  assert.deepEqual(runner.calls, [
    {
      args: [
        'compose',
        '--project-name',
        'recorded-project',
        '--project-directory',
        join(workspace, '.devcontainer'),
        '--file',
        join(workspace, '.devcontainer', 'docker-compose.yml'),
        'down',
        '--volumes',
        '--remove-orphans',
      ],
      command: 'docker',
      options: undefined,
    },
  ]);
  assert.deepEqual(fileSystem.removed, [
    join(workspace, '.devcontainer', '.env'),
  ]);
  runner.assertComplete();
});

test('discovers and deduplicates partial-startup projects by the unique label', () => {
  const workspace = join('C:\\', 'checkout');
  const secondContainerId = 'c'.repeat(64);
  const label = 'rust-playground.ci-run=401-2-source';
  const runner = new FakeRunner([
    success(`${sourceContainerId}\n${secondContainerId}\n`),
    success(sourceContainerId),
    success('401-2-source'),
    success('partial-project'),
    success(secondContainerId),
    success('401-2-source'),
    success('partial-project'),
    success(),
  ]);
  const fileSystem = new FakeFileSystem();

  cleanupDevcontainer(
    {
      ciLabels: [label],
      workspace,
    },
    runner,
    fileSystem,
  );

  assert.deepEqual(runner.calls[0], {
    args: [
      'ps',
      '-aq',
      '--no-trunc',
      '--filter',
      `label=${label}`,
    ],
    command: 'docker',
    options: {
      stderr: 'capture',
      stdout: 'capture',
    },
  });
  assert.equal(
    runner.calls.filter(({ args }) => args[0] === 'compose').length,
    1,
  );
  assert.equal(
    runner.calls.some(
      ({ args }) => args[0] === 'rm' && args.includes(sourceContainerId),
    ),
    false,
  );
  assert.deepEqual(fileSystem.removed, [
    join(workspace, '.devcontainer', '.env'),
  ]);
  runner.assertComplete();
});

test('force-removes only a uniquely labeled non-Compose partial container', () => {
  const workspace = join('C:\\', 'checkout');
  const runner = new FakeRunner([
    success(`${sourceContainerId}\n`),
    success(sourceContainerId),
    success('402-1-source'),
    success('<no value>\n'),
    success(),
  ]);
  const fileSystem = new FakeFileSystem();

  cleanupDevcontainer(
    {
      ciLabels: ['rust-playground.ci-run=402-1-source'],
      workspace,
    },
    runner,
    fileSystem,
  );

  assert.deepEqual(runner.calls.at(-1), {
    args: ['rm', '--force', sourceContainerId],
    command: 'docker',
    options: undefined,
  });
  assert.deepEqual(fileSystem.removed, [
    join(workspace, '.devcontainer', '.env'),
  ]);
  runner.assertComplete();
});

test('refuses to clean an exact container with a mismatched run label', () => {
  const workspace = join('C:\\', 'checkout');
  const runner = new FakeRunner([
    success(''),
    success(sourceContainerId),
    success('999-1-source'),
  ]);
  const fileSystem = new FakeFileSystem();

  assert.throws(
    () =>
      cleanupDevcontainer(
        {
          ciLabels: ['rust-playground.ci-run=403-1-source'],
          containerId: sourceContainerId,
          workspace,
        },
        runner,
        fileSystem,
      ),
    /does not have an expected CI run label/u,
  );
  assert.equal(
    runner.calls.some(({ args }) => args[0] === 'rm' || args[0] === 'compose'),
    false,
  );
  assert.deepEqual(fileSystem.removed, [
    join(workspace, '.devcontainer', '.env'),
  ]);
  runner.assertComplete();
});

test('removes generated CI environment state even when Compose cleanup fails', () => {
  const workspace = join('C:\\', 'checkout');
  const runner = new FakeRunner([failure(12)]);
  const fileSystem = new FakeFileSystem();

  assert.throws(
    () =>
      cleanupDevcontainer(
        {
          composeProject: 'failed-project',
          workspace,
        },
        runner,
        fileSystem,
      ),
    (error: unknown) => error instanceof CommandError && error.exitCode === 12,
  );
  assert.deepEqual(fileSystem.removed, [
    join(workspace, '.devcontainer', '.env'),
  ]);
  runner.assertComplete();
});

test('Nx targets expose CI lifecycle without adding verification recursion', () => {
  const repositoryRoot = fileURLToPath(new URL('../../../', import.meta.url));
  const project = JSON.parse(
    readFileSync(join(repositoryRoot, 'tools', 'scripts', 'project.json'), 'utf8'),
  ) as {
    targets: Record<
      string,
      {
        dependsOn?: readonly string[];
        options?: { command?: string; commands?: readonly string[] };
      }
    >;
  };

  for (const target of [
    'typecheck',
    'test',
    'verify',
    'clean-worktree',
    'ci-prepare',
    'ci-start-source',
    'ci-start-published',
    'ci-verify',
    'ci-cleanup',
    'registry-login',
    'registry-tags',
    'registry-manifest',
    'registry-public',
  ]) {
    assert.ok(project.targets[target], `missing target: ${target}`);
  }
  assert.deepEqual(project.targets.verify?.dependsOn, ['typecheck', 'test']);
  assert.deepEqual(project.targets.verify?.options?.commands, []);
  assert.doesNotMatch(
    JSON.stringify(project.targets.verify),
    /ci-start|devcontainer:(?:up|test)/u,
  );
  assert.match(
    project.targets['ci-start-source']?.options?.command ?? '',
    /ci\.ts start-source$/u,
  );
});
