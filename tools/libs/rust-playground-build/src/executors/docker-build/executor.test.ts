import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import * as path from 'node:path';
import { test } from 'node:test';
import type { ExecutorContext } from '@nx/devkit';
import {
  type CommandOptions,
  type CommandResult,
  type CommandRunner,
  createGitBranchTag,
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

interface RuntimeProjectConfiguration {
  readonly targets: {
    readonly build: { readonly cache?: boolean };
    readonly publish: {
      readonly configurations?: {
        readonly main?: { readonly tags?: string[] };
      };
      readonly options: DockerBuildExecutorSchema;
    };
  };
}

function readRuntimeProject(): RuntimeProjectConfiguration {
  return JSON.parse(
    readFileSync(
      new URL(
        '../../../../../../bin/hello-world/project.json',
        import.meta.url,
      ),
      'utf8',
    ),
  ) as RuntimeProjectConfiguration;
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
      tags: [
        '{gitBranchTag}',
        'sha-{gitSha}',
        'short-{gitShortSha}',
      ],
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
  assert.ok(
    args.includes(
      'example.test/hello-world:branch-feature-add-registry-v2',
    ),
  );
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

test('rejects dirty tracked or untracked publication worktrees before Docker starts', async (suite) => {
  const cases = [
    {
      changes: ' M bin/hello-world/src/main.rs\n',
      name: 'tracked change',
    },
    {
      changes: '?? bin/hello-world/local.txt\n',
      name: 'untracked change',
    },
  ] as const;

  for (const testCase of cases) {
    await suite.test(testCase.name, async () => {
      const harness = createHarness((command, args) => {
        if (
          command === 'git' &&
          args.join(' ') ===
            'status --porcelain=v1 --untracked-files=all'
        ) {
          return { status: 0, stdout: testCase.changes };
        }
        return successfulCommand;
      });
      const result = await runDockerBuildExecutorWithDependencies(
        baseOptions({
          output: 'push',
          requireCleanWorktree: true,
        }),
        createContext(),
        harness.dependencies,
      );

      assert.deepEqual(result, { success: false });
      assert.deepEqual(
        harness.invocations.map(({ args, command }) => ({
          args,
          command,
        })),
        [
          {
            args: [
              'status',
              '--porcelain=v1',
              '--untracked-files=all',
            ],
            command: 'git',
          },
        ],
      );
      assert.match(
        harness.errors.join('\n'),
        /worktree must be clean.*tracked or untracked/isu,
      );
    });
  }
});

test('publishes mutable and missing immutable tags in one build', async () => {
  const sha = 'a'.repeat(40);
  const immutableTag = `sha-${sha}`;
  const immutableReference =
    `example.test/hello-world:${immutableTag}`;
  const harness = createHarness((command, args) => {
    if (command === 'git') {
      return { status: 0, stdout: '' };
    }
    if (
      command === 'docker' &&
      args.join(' ').startsWith('buildx imagetools inspect ')
    ) {
      return {
        status: 1,
        stderr: `ERROR: ${immutableReference}: not found`,
      };
    }
    return successfulCommand;
  });
  const result = await runDockerBuildExecutorWithDependencies(
    baseOptions({
      immutableTags: [immutableTag],
      manifestFile: 'artifacts/publish-images.txt',
      output: 'push',
      requireCleanWorktree: true,
      tags: ['branch-feature', immutableTag],
    }),
    createContext(),
    harness.dependencies,
  );

  assert.deepEqual(result, { success: true });
  assert.deepEqual(
    harness.invocations.map(({ args, command, options }) => ({
      args,
      command,
      options,
    })),
    [
      {
        args: [
          'status',
          '--porcelain=v1',
          '--untracked-files=all',
        ],
        command: 'git',
        options: {
          captureOutput: true,
          cwd: path.resolve(createContext().root),
        },
      },
      {
        args: [
          'buildx',
          'imagetools',
          'inspect',
          immutableReference,
        ],
        command: 'docker',
        options: {
          captureOutput: true,
          cwd: path.resolve(createContext().root),
        },
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
          'example.test/hello-world:branch-feature',
          '--tag',
          immutableReference,
          '--push',
          path.resolve(createContext().root, 'bin/hello-world'),
        ],
        command: 'docker',
        options: {
          cwd: path.resolve(createContext().root),
        },
      },
    ],
  );
  assert.deepEqual(harness.manifests, [
    {
      contents:
        'example.test/hello-world:branch-feature\n' +
        `${immutableReference}\n`,
      filePath: path.resolve(
        createContext().root,
        'artifacts/publish-images.txt',
      ),
    },
  ]);
});

test('repeat publication omits existing immutable tags but updates mutable tags', async () => {
  const immutableTag = `sha-${'b'.repeat(40)}`;
  const immutableReference =
    `example.test/hello-world:${immutableTag}`;
  const harness = createHarness((command, args) => {
    if (
      command === 'docker' &&
      args.join(' ').startsWith('buildx imagetools inspect ')
    ) {
      return { status: 0, stdout: 'Name: existing manifest\n' };
    }
    return successfulCommand;
  });
  const result = await runDockerBuildExecutorWithDependencies(
    baseOptions({
      immutableTags: [immutableTag],
      manifestFile: 'artifacts/repeat-images.txt',
      output: 'push',
      tags: ['branch-feature', immutableTag],
    }),
    createContext(),
    harness.dependencies,
  );

  assert.deepEqual(result, { success: true });
  assert.equal(harness.invocations.length, 2);
  assert.deepEqual(harness.invocations[0]?.args, [
    'buildx',
    'imagetools',
    'inspect',
    immutableReference,
  ]);
  assert.deepEqual(harness.invocations[1]?.args, [
    'buildx',
    'build',
    '--file',
    path.join(
      path.resolve(createContext().root, 'bin/hello-world'),
      'Dockerfile',
    ),
    '--tag',
    'example.test/hello-world:branch-feature',
    '--push',
    path.resolve(createContext().root, 'bin/hello-world'),
  ]);
  assert.deepEqual(harness.manifests, [
    {
      contents:
        'example.test/hello-world:branch-feature\n' +
        `${immutableReference}\n`,
      filePath: path.resolve(
        createContext().root,
        'artifacts/repeat-images.txt',
      ),
    },
  ]);
  assert.match(
    harness.information.join('\n'),
    /Immutable image tag already exists/u,
  );
});

test('skips Docker builds when every selected tag is an existing immutable tag', async () => {
  const immutableTag = `sha-${'c'.repeat(40)}`;
  const immutableReference =
    `example.test/hello-world:${immutableTag}`;
  const harness = createHarness(() => ({
    status: 0,
    stdout: 'Name: existing manifest\n',
  }));
  const result = await runDockerBuildExecutorWithDependencies(
    baseOptions({
      immutableTags: [immutableTag],
      manifestFile: 'artifacts/immutable-images.txt',
      output: 'push',
      tags: [immutableTag],
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
        args: [
          'buildx',
          'imagetools',
          'inspect',
          immutableReference,
        ],
        command: 'docker',
      },
    ],
  );
  assert.deepEqual(harness.manifests, [
    {
      contents: `${immutableReference}\n`,
      filePath: path.resolve(
        createContext().root,
        'artifacts/immutable-images.txt',
      ),
    },
  ]);
  assert.match(
    harness.information.join('\n'),
    /skipping docker buildx build/u,
  );
});

test('fails closed on authentication and transient manifest inspection errors', async (suite) => {
  const immutableTag = `sha-${'d'.repeat(40)}`;
  const cases: Array<{
    readonly error?: Error;
    readonly expected: RegExp;
    readonly name: string;
    readonly status: number | null;
    readonly stderr?: string;
  }> = [
    {
      expected: /401 Unauthorized/u,
      name: 'authentication failure',
      status: 1,
      stderr:
        'unexpected status from HEAD request to ' +
        `https://example.test/v2/hello-world/manifests/${immutableTag}: 401 Unauthorized`,
    },
    {
      expected: /503 Service Unavailable/u,
      name: 'transient registry failure',
      status: 1,
      stderr:
        'unexpected status from HEAD request to ' +
        `https://example.test/v2/hello-world/manifests/${immutableTag}: 503 Service Unavailable`,
    },
    {
      expected: /authentication required/u,
      name: 'missing text accompanied by authentication failure',
      status: 1,
      stderr: 'manifest not found: authentication required',
    },
    {
      expected: /other-image.*not found/u,
      name: 'not found response for a different reference',
      status: 1,
      stderr: 'ERROR: example.test/other-image:latest: not found',
    },
    {
      expected: /resolver host not found/u,
      name: 'generic not found response',
      status: 1,
      stderr: 'manifest resolver host not found',
    },
    {
      error: new Error('docker is missing'),
      expected: /Failed to start docker buildx imagetools inspect/u,
      name: 'inspection spawn failure',
      status: null,
    },
  ];

  for (const testCase of cases) {
    await suite.test(testCase.name, async () => {
      const harness = createHarness(() => ({
        ...(testCase.error ? { error: testCase.error } : {}),
        status: testCase.status,
        ...(testCase.stderr ? { stderr: testCase.stderr } : {}),
      }));
      const result = await runDockerBuildExecutorWithDependencies(
        baseOptions({
          immutableTags: [immutableTag],
          output: 'push',
          tags: [immutableTag],
        }),
        createContext(),
        harness.dependencies,
      );

      assert.deepEqual(result, { success: false });
      assert.equal(harness.invocations.length, 1);
      assert.deepEqual(
        harness.invocations[0]?.args.slice(0, 3),
        ['buildx', 'imagetools', 'inspect'],
      );
      assert.match(harness.errors.join('\n'), testCase.expected);
      assert.equal(harness.manifests.length, 0);
    });
  }
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
    {
      expected: /immutableTags must contain at least one value/u,
      name: 'empty immutable tags',
      options: baseOptions({ immutableTags: [] }),
    },
    {
      expected: /immutableTags can only be used with output "push"/u,
      name: 'immutable load tag',
      options: baseOptions({
        immutableTags: ['dev'],
      }),
    },
    {
      expected: /not present in tags after token expansion/u,
      name: 'immutable tag is not selected',
      options: baseOptions({
        immutableTags: ['sha-deadbeef'],
        output: 'push',
      }),
    },
    {
      expected: /requireCleanWorktree must be a boolean/u,
      name: 'invalid clean worktree option',
      options: baseOptions({
        requireCleanWorktree: 'yes' as unknown as boolean,
      }),
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
  const sharedLongPrefix = `feature/${'a'.repeat(180)}`;
  const firstLongBranch = `${sharedLongPrefix}-one`;
  const secondLongBranch = `${sharedLongPrefix}-two`;
  const firstLongTag = createGitBranchTag(firstLongBranch);
  const secondLongTag = createGitBranchTag(secondLongBranch);

  assert.equal(validateDockerTag(' release_1.2-rc1 '), 'release_1.2-rc1');
  assert.equal(
    sanitizeGitBranch('refs/heads/Feature/Add Registry@V2'),
    'feature-add-registry-v2',
  );
  assert.equal(createGitBranchTag('refs/heads/latest'), 'branch-latest');
  assert.equal(firstLongTag.length, 128);
  assert.match(firstLongTag, /^branch-[a-z0-9-]+-[0-9a-f]{16}$/u);
  assert.equal(createGitBranchTag(firstLongBranch), firstLongTag);
  assert.notEqual(firstLongTag, secondLongTag);
  assert.equal(sanitizeGitBranch(firstLongBranch).length, 128);
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
    'immutableTags',
    'manifestFile',
    'output',
    'platforms',
    'requireCleanWorktree',
    'tags',
  ]);
});

test('configures runtime publication namespaces, immutability, and uncached Cargo builds', () => {
  const helloWorldProject = readRuntimeProject();
  const greetingProject = JSON.parse(
    readFileSync(
      new URL(
        '../../../../../../crates/greeting/project.json',
        import.meta.url,
      ),
      'utf8',
    ),
  ) as {
    targets: {
      build: { cache?: boolean };
    };
  };

  assert.deepEqual(helloWorldProject.targets.publish.options.tags, [
    '{gitBranchTag}',
    'sha-{gitSha}',
  ]);
  assert.deepEqual(
    helloWorldProject.targets.publish.options.immutableTags,
    ['sha-{gitSha}'],
  );
  assert.equal(
    helloWorldProject.targets.publish.options.requireCleanWorktree,
    true,
  );
  assert.deepEqual(
    helloWorldProject.targets.publish.configurations?.main?.tags,
    ['sha-{gitSha}', 'latest'],
  );
  assert.equal(helloWorldProject.targets.build.cache, false);
  assert.equal(greetingProject.targets.build.cache, false);
});

test('actual runtime publish target bounds very long branch tags without collisions', async () => {
  const options = readRuntimeProject().targets.publish.options;
  const sha = 'e'.repeat(40);

  async function executeTarget(branch: string): Promise<string> {
    const harness = createHarness((command, args) => {
      if (
        command === 'docker' &&
        args[0] === 'buildx' &&
        args[1] === 'imagetools'
      ) {
        return { status: 0, stdout: 'Name: existing manifest\n' };
      }
      return successfulCommand;
    }, {
      REGISTRY_BRANCH: branch,
      REGISTRY_SHA: sha,
    });
    const result = await runDockerBuildExecutorWithDependencies(
      options,
      createContext(),
      harness.dependencies,
    );

    assert.deepEqual(result, { success: true });
    assert.equal(harness.errors.length, 0);
    const buildInvocation = harness.invocations.find(
      ({ args, command }) =>
        command === 'docker' &&
        args[0] === 'buildx' &&
        args[1] === 'build',
    );
    assert.ok(buildInvocation);
    const tagArgumentIndex = buildInvocation.args.indexOf('--tag');
    assert.ok(tagArgumentIndex >= 0);
    const imageReference =
      buildInvocation.args[tagArgumentIndex + 1] ?? '';
    return imageReference.slice(imageReference.lastIndexOf(':') + 1);
  }

  const sharedLongPrefix = `feature/${'z'.repeat(180)}`;
  const firstBranch = `${sharedLongPrefix}-one`;
  const secondBranch = `${sharedLongPrefix}-two`;
  const firstTag = await executeTarget(firstBranch);
  const repeatedFirstTag = await executeTarget(firstBranch);
  const secondTag = await executeTarget(secondBranch);

  assert.equal(firstTag, createGitBranchTag(firstBranch));
  assert.equal(firstTag.length, 128);
  assert.match(firstTag, /^branch-[a-z0-9-]+-[0-9a-f]{16}$/u);
  assert.equal(repeatedFirstTag, firstTag);
  assert.notEqual(secondTag, firstTag);
});
