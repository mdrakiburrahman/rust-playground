import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import * as path from 'node:path';
import { test } from 'node:test';
import type { ExecutorContext } from '@nx/devkit';
import {
  type CommandOptions,
  type CommandResult,
  type CommandRunner,
  type DockerBuildExecutorDependencies,
  runDockerBuildExecutorWithDependencies,
  sanitizeGitBranch,
  validateDockerTag,
} from './executor.js';
import type { DockerBuildExecutorSchema } from './schema.js';

interface Invocation {
  readonly args: readonly string[];
  readonly command: string;
  readonly options?: CommandOptions;
}

interface Harness {
  readonly dependencies: DockerBuildExecutorDependencies;
  readonly errors: string[];
  readonly information: string[];
  readonly invocations: Invocation[];
  readonly manifests: Array<{
    readonly contents: string;
    readonly filePath: string;
  }>;
}

const successfulCommand: CommandResult = {
  status: 0,
};

function createContext(isVerbose = false): ExecutorContext {
  const root = path.resolve('workspace');
  return {
    isVerbose,
    projectName: 'hello-world',
    projectsConfigurations: {
      projects: {
        'hello-world': {
          name: 'hello-world',
          root: 'bin/hello-world',
          targets: {},
        },
      },
      version: 2,
    },
    root,
    targetName: 'image',
  } as unknown as ExecutorContext;
}

function createHarness(
  handler: (
    command: string,
    args: readonly string[],
    options?: CommandOptions,
  ) => CommandResult = () => successfulCommand,
  environment: NodeJS.ProcessEnv = {},
): Harness {
  const errors: string[] = [];
  const information: string[] = [];
  const invocations: Invocation[] = [];
  const manifests: Array<{ contents: string; filePath: string }> = [];
  const commandRunner: CommandRunner = {
    run(command, args, options) {
      invocations.push({
        args: [...args],
        command,
        ...(options ? { options } : {}),
      });
      return handler(command, args, options);
    },
  };

  return {
    dependencies: {
      commandRunner,
      environment,
      logger: {
        error: (message) => errors.push(message),
        info: (message) => information.push(message),
      },
      manifestWriter: {
        write(filePath, contents) {
          manifests.push({ contents, filePath });
        },
      },
    },
    errors,
    information,
    invocations,
    manifests,
  };
}

function baseOptions(
  overrides: Partial<DockerBuildExecutorSchema> = {},
): DockerBuildExecutorSchema {
  return {
    file: '{absProjectRoot}/Dockerfile',
    image: 'example.test/{projectName}',
    output: 'load',
    tags: ['dev'],
    ...overrides,
  };
}

test('builds a load command as an argument array and writes a scan manifest', async () => {
  const context = createContext(true);
  const harness = createHarness(() => successfulCommand, {
    BUILD_ID: 'build-42',
    TARGET_PLATFORM: 'linux/amd64',
  });
  const result = await runDockerBuildExecutorWithDependencies(
    baseOptions({
      buildArgs: [
        'PROJECT={projectName}',
        'ROOT={absWorkspaceRoot}',
        'LITERAL=$(touch should-not-run)',
      ],
      manifestFile:
        '{absWorkspaceRoot}/artifacts/{projectName}-images.txt',
      platforms: ['{env:TARGET_PLATFORM}'],
      tags: ['dev', 'ci-{env:BUILD_ID}'],
    }),
    context,
    harness.dependencies,
  );

  const workspaceRoot = path.resolve(context.root);
  const projectRoot = path.resolve(workspaceRoot, 'bin/hello-world');
  assert.deepEqual(result, { success: true });
  assert.deepEqual(harness.invocations, [
    {
      args: [
        'buildx',
        'build',
        '--file',
        path.join(projectRoot, 'Dockerfile'),
        '--tag',
        'example.test/hello-world:dev',
        '--tag',
        'example.test/hello-world:ci-build-42',
        '--build-arg',
        'PROJECT=hello-world',
        '--build-arg',
        `ROOT=${workspaceRoot}`,
        '--build-arg',
        'LITERAL=$(touch should-not-run)',
        '--platform',
        'linux/amd64',
        '--load',
        projectRoot,
      ],
      command: 'docker',
      options: {
        cwd: workspaceRoot,
      },
    },
  ]);
  assert.deepEqual(harness.manifests, [
    {
      contents:
        'example.test/hello-world:dev\n' +
        'example.test/hello-world:ci-build-42\n',
      filePath: path.join(
        workspaceRoot,
        'artifacts',
        'hello-world-images.txt',
      ),
    },
  ]);
  assert.equal(harness.errors.length, 0);
  assert.equal(harness.information.length, 1);
  assert.match(harness.information[0] ?? '', /PROJECT=<redacted>/u);
  assert.doesNotMatch(harness.information[0] ?? '', /PROJECT=hello-world/u);
});

test('pushes multiple platforms and expands sanitized CI Git tokens', async () => {
  const sha = 'ABCDEF0123456789ABCDEF0123456789ABCDEF01';
  const harness = createHarness(() => successfulCommand, {
    REGISTRY_BRANCH: 'refs/heads/Feature/Add Registry@V2',
    REGISTRY_SHA: sha,
  });
  const result = await runDockerBuildExecutorWithDependencies(
    baseOptions({
      context: '{absWorkspaceRoot}',
      output: 'push',
      platforms: ['linux/amd64', 'linux/arm64/v8'],
      tags: ['{gitBranch}', 'sha-{gitSha}', 'short-{gitShortSha}'],
    }),
    createContext(),
    harness.dependencies,
  );

  assert.deepEqual(result, { success: true });
  assert.equal(harness.invocations.length, 1);
  const args = harness.invocations[0]?.args ?? [];
  assert.ok(args.includes('--push'));
  assert.ok(!args.includes('--load'));
  assert.equal(args.at(-1), path.resolve(createContext().root));
  assert.deepEqual(
    args.slice(args.indexOf('--platform'), args.indexOf('--platform') + 2),
    ['--platform', 'linux/amd64,linux/arm64/v8'],
  );
  assert.ok(args.includes('example.test/hello-world:feature-add-registry-v2'));
  assert.ok(
    args.includes(`example.test/hello-world:sha-${sha.toLowerCase()}`),
  );
  assert.ok(
    args.includes(
      `example.test/hello-world:short-${sha.toLowerCase().slice(0, 12)}`,
    ),
  );
});

test('falls back to non-shell Git commands only when Git tokens are requested', async () => {
  const sha = 'd'.repeat(40);
  const harness = createHarness((command, args) => {
    if (command === 'git' && args.join(' ') === 'rev-parse HEAD') {
      return { status: 0, stdout: `${sha}\n` };
    }
    if (command === 'git' && args.join(' ') === 'branch --show-current') {
      return { status: 0, stdout: 'users/example/change\n' };
    }
    return successfulCommand;
  });

  const result = await runDockerBuildExecutorWithDependencies(
    baseOptions({
      output: 'push',
      tags: ['sha-{gitShortSha}', '{gitBranch}'],
    }),
    createContext(),
    harness.dependencies,
  );

  assert.deepEqual(result, { success: true });
  assert.deepEqual(
    harness.invocations.map(({ args, command }) => ({
      args,
      command,
    })),
    [
      {
        args: ['rev-parse', 'HEAD'],
        command: 'git',
      },
      {
        args: ['branch', '--show-current'],
        command: 'git',
      },
      {
        args: [
          'buildx',
          'build',
          '--file',
          path.join(
            path.resolve(createContext().root, 'bin/hello-world'),
            'Dockerfile',
          ),
          '--tag',
          `example.test/hello-world:sha-${sha.slice(0, 12)}`,
          '--tag',
          'example.test/hello-world:users-example-change',
          '--push',
          path.resolve(createContext().root, 'bin/hello-world'),
        ],
        command: 'docker',
      },
    ],
  );
});

test('rejects invalid plans before Docker starts', async (suite) => {
  const cases: Array<{
    readonly expected: RegExp;
    readonly name: string;
    readonly options: DockerBuildExecutorSchema;
  }> = [
    {
      expected: /tags must contain at least one value/u,
      name: 'empty tags',
      options: baseOptions({ tags: [] }),
    },
    {
      expected: /Invalid Docker tag "feature\/unsafe"/u,
      name: 'invalid tag',
      options: baseOptions({ tags: ['feature/unsafe'] }),
    },
    {
      expected: /Invalid Docker tag/u,
      name: 'tag longer than 128 characters',
      options: baseOptions({ tags: ['a'.repeat(129)] }),
    },
    {
      expected: /duplicate value "same"/u,
      name: 'duplicate expanded tags',
      options: baseOptions({ tags: ['same', '{env:TAG}'] }),
    },
    {
      expected: /output must be either "load" or "push"/u,
      name: 'unknown output mode',
      options: baseOptions({
        output: 'archive' as DockerBuildExecutorSchema['output'],
      }),
    },
    {
      expected: /platforms must contain at least one value/u,
      name: 'empty platforms',
      options: baseOptions({ platforms: [] }),
    },
    {
      expected: /lowercase os\/architecture/u,
      name: 'invalid platform',
      options: baseOptions({ platforms: ['Linux/AMD64'] }),
    },
    {
      expected: /load output supports at most one platform/u,
      name: 'multi-platform load',
      options: baseOptions({
        platforms: ['linux/amd64', 'linux/arm64'],
      }),
    },
    {
      expected: /provide tags through the tags option/u,
      name: 'tag embedded in image',
      options: baseOptions({ image: 'example.test/application:latest' }),
    },
  ];

  for (const testCase of cases) {
    await suite.test(testCase.name, async () => {
      const environment =
        testCase.name === 'duplicate expanded tags' ? { TAG: 'same' } : {};
      const harness = createHarness(() => successfulCommand, environment);
      const result = await runDockerBuildExecutorWithDependencies(
        testCase.options,
        createContext(),
        harness.dependencies,
      );

      assert.deepEqual(result, { success: false });
      assert.equal(harness.invocations.length, 0);
      assert.match(harness.errors.join('\n'), testCase.expected);
    });
  }
});

test('reports missing or unusable dynamic token values', async (suite) => {
  await suite.test('missing environment variable', async () => {
    const harness = createHarness();
    const result = await runDockerBuildExecutorWithDependencies(
      baseOptions({ tags: ['{env:UNSET_TAG}'] }),
      createContext(),
      harness.dependencies,
    );

    assert.deepEqual(result, { success: false });
    assert.match(harness.errors.join('\n'), /UNSET_TAG.*is not set/u);
    assert.equal(harness.invocations.length, 0);
  });

  await suite.test('invalid Git SHA', async () => {
    const harness = createHarness(() => successfulCommand, {
      REGISTRY_SHA: 'not-a-sha',
    });
    const result = await runDockerBuildExecutorWithDependencies(
      baseOptions({ tags: ['sha-{gitSha}'] }),
      createContext(),
      harness.dependencies,
    );

    assert.deepEqual(result, { success: false });
    assert.match(harness.errors.join('\n'), /Git SHA.*is invalid/u);
    assert.equal(harness.invocations.length, 0);
  });

  await suite.test('detached Git branch', async () => {
    const harness = createHarness(() => successfulCommand, {
      REGISTRY_BRANCH: 'HEAD',
    });
    const result = await runDockerBuildExecutorWithDependencies(
      baseOptions({ tags: ['{gitBranch}'] }),
      createContext(),
      harness.dependencies,
    );

    assert.deepEqual(result, { success: false });
    assert.match(harness.errors.join('\n'), /does not contain a usable/u);
    assert.equal(harness.invocations.length, 0);
  });
});

test('handles spawn errors, exit statuses, and signals explicitly', async (suite) => {
  const cases: Array<{
    readonly expected: RegExp;
    readonly name: string;
    readonly result: CommandResult;
  }> = [
    {
      expected: /Failed to start docker buildx build: docker is missing/u,
      name: 'spawn error',
      result: {
        error: new Error('docker is missing'),
        status: null,
      },
    },
    {
      expected: /exited with status 23: build failed/u,
      name: 'non-zero status',
      result: {
        status: 23,
        stderr: 'build failed\n',
      },
    },
    {
      expected: /terminated by signal SIGTERM/u,
      name: 'signal',
      result: {
        signal: 'SIGTERM',
        status: null,
      },
    },
    {
      expected: /ended without an exit status/u,
      name: 'missing status',
      result: {
        status: null,
      },
    },
  ];

  for (const testCase of cases) {
    await suite.test(testCase.name, async () => {
      const harness = createHarness(() => testCase.result);
      const result = await runDockerBuildExecutorWithDependencies(
        baseOptions({ manifestFile: 'artifacts/images.txt' }),
        createContext(),
        harness.dependencies,
      );

      assert.deepEqual(result, { success: false });
      assert.match(harness.errors.join('\n'), testCase.expected);
      assert.equal(harness.manifests.length, 0);
    });
  }
});

test('turns manifest write failures into executor failures', async () => {
  const harness = createHarness();
  const dependencies: DockerBuildExecutorDependencies = {
    ...harness.dependencies,
    manifestWriter: {
      write() {
        throw new Error('manifest disk is read-only');
      },
    },
  };

  const result = await runDockerBuildExecutorWithDependencies(
    baseOptions({ manifestFile: 'artifacts/images.txt' }),
    createContext(),
    dependencies,
  );

  assert.deepEqual(result, { success: false });
  assert.match(harness.errors.join('\n'), /manifest disk is read-only/u);
});

test('validates and sanitizes tag values deterministically', () => {
  assert.equal(validateDockerTag(' release_1.2-rc1 '), 'release_1.2-rc1');
  assert.equal(
    sanitizeGitBranch('refs/heads/Feature/Add Registry@V2'),
    'feature-add-registry-v2',
  );
  assert.equal(sanitizeGitBranch(`feature/${'a'.repeat(200)}`).length, 128);
  assert.throws(() => validateDockerTag('-invalid'), /Invalid Docker tag/u);
  assert.throws(() => sanitizeGitBranch('---'), /does not contain a usable/u);
});

test('publishes a complete strict executor schema', () => {
  const schema = JSON.parse(
    readFileSync(new URL('./schema.json', import.meta.url), 'utf8'),
  ) as {
    additionalProperties?: boolean;
    properties?: Record<string, unknown>;
    required?: string[];
  };

  assert.equal(schema.additionalProperties, false);
  assert.deepEqual(schema.required, ['file', 'image', 'tags', 'output']);
  assert.deepEqual(Object.keys(schema.properties ?? {}).sort(), [
    'buildArgs',
    'context',
    'file',
    'image',
    'manifestFile',
    'output',
    'platforms',
    'tags',
  ]);
});
