import assert from 'node:assert/strict';
import test from 'node:test';

import type {
  CommandOptions,
  CommandResult,
  CommandRunner,
} from '../src/process.js';
import { runRegistryCli } from '../src/registry.js';

class PrivatePackageRunner implements CommandRunner {
  readonly calls: Array<{
    readonly args: readonly string[];
    readonly command: string;
    readonly options?: CommandOptions;
  }> = [];

  run(
    command: string,
    args: readonly string[],
    options?: CommandOptions,
  ): CommandResult {
    this.calls.push({ args: [...args], command, options });
    return {
      exitCode: 0,
      stderr: '',
      stdout: JSON.stringify([
        {
          html_url:
            'https://github.com/users/octocat/packages/container/rust-playground%2Fdevcontainer',
          name: 'rust-playground/devcontainer',
          owner: { login: 'octocat' },
          package_type: 'container',
          visibility: 'private',
        },
      ]),
    };
  }
}

test('registry-public emits a machine-readable needs-ui-change error', async () => {
  const runner = new PrivatePackageRunner();
  const stdout: string[] = [];
  const stderr: string[] = [];
  const previousExitCode = process.exitCode;

  try {
    process.exitCode = undefined;
    await runRegistryCli(
      [
        'node',
        'registry',
        'public',
        '--owner',
        'octocat',
        '--repository',
        'rust-playground',
        '--image',
        'devcontainer',
        '--environment',
        'ci',
      ],
      {
        env: {},
        runner,
        stderr: (message) => stderr.push(message),
        stdout: (message) => stdout.push(message),
      },
    );

    assert.equal(process.exitCode, 2);
    assert.deepEqual(stderr, []);
    assert.equal(stdout.length, 1);
    assert.deepEqual(JSON.parse(stdout[0] ?? '') as unknown, {
      currentVisibility: 'private',
      image: 'devcontainer',
      owner: 'octocat',
      packageName: 'rust-playground/devcontainer',
      reference: 'ghcr.io/octocat/rust-playground/devcontainer',
      repository: 'rust-playground',
      settingsUrl:
        'https://github.com/users/octocat/packages/container/rust-playground%2Fdevcontainer/settings',
      status: 'needs-ui-change',
      targetVisibility: 'public',
    });
    assert.equal(
      runner.calls.some(({ args }) => args.includes('PATCH')),
      false,
    );
  } finally {
    process.exitCode = previousExitCode;
  }
});
