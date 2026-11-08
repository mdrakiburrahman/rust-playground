import assert from 'node:assert/strict';
import test from 'node:test';

import {
  CommandError,
  type CommandOptions,
  type CommandResult,
  type CommandRunner,
  runChecked,
  runStreamingTree,
  SpawnCommandRunner,
} from '../src/process.js';
import { kubectlFailureDetail } from '../src/kubectl-failure.js';

test('streaming failures suppress stderr by default and opt in to bounded sanitized cleanup context', async () => {
  const secret = 'test-only-sensitive-env';
  const script = `process.stderr.write('OCI runtime exec failed: kill not found; Bearer auth-token; Basic basic-token; token=assignment-secret https://example.test/path?sig=query-secret ${secret}'); process.exit(1);`;
  const invoke = (failureDetail?: (stderr: string) => string) =>
    runStreamingTree(process.execPath, ['-e', script], { timeoutMs: 5000, capture: true, failureDetail });
  await assert.rejects(invoke(), error => {
    assert.ok(error instanceof Error);
    assert.doesNotMatch(error.message, /OCI|sensitive|auth-token/);
    return true;
  });
  await assert.rejects(invoke(kubectlFailureDetail(['exec', '--', secret], text => text, [secret])), error => {
    assert.ok(error instanceof Error);
    assert.match(error.message, /Retained cleanup exec: OCI runtime exec failed: kill not found/);
    assert.doesNotMatch(error.message, /auth-token|basic-token|assignment-secret|query-secret|sensitive-env|example.test/);
    return true;
  });
});

test('oversized or rejected stderr detail never leaks raw subprocess output', async () => {
  for (const failureDetail of [kubectlFailureDetail(['get'], text => text, []), () => { throw new Error('secret'); }]) {
    await assert.rejects(runStreamingTree(process.execPath,
      ['-e', "process.stderr.write('private'.repeat(20000));process.exit(1)"],
      { timeoutMs: 5000, capture: true, failureDetail }), error => {
      assert.ok(error instanceof Error);
      assert.ok(error.message.length < 2100);
      assert.doesNotMatch(error.message, /private|secret/);
      return true;
    });
  }
});

test('passes stdin separately from argv and captures requested streams', () => {
  const runner = new SpawnCommandRunner();
  const input = 'test-only-secret';
  const script = [
    "let value = '';",
    "process.stdin.setEncoding('utf8');",
    "process.stdin.on('data', (chunk) => { value += chunk; });",
    "process.stdin.on('end', () => {",
    "  process.stdout.write(`stdin:${value}`);",
    "  process.stderr.write('stderr');",
    '});',
  ].join('\n');

  const result = runner.run(process.execPath, ['--eval', script], {
    input,
    stderr: 'capture',
    stdout: 'capture',
  });

  assert.equal(result.exitCode, 0);
  assert.equal(result.stdout, `stdin:${input}`);
  assert.equal(result.stderr, 'stderr');
});

test('checked command errors include argv but never include stdin', () => {
  const secret = 'not-for-error-output';
  const runner: CommandRunner = {
    run(
      _command: string,
      _args: readonly string[],
      _options?: CommandOptions,
    ): CommandResult {
      return {
        exitCode: 17,
        stderr: 'failure',
        stdout: '',
      };
    },
  };

  assert.throws(
    () =>
      runChecked(runner, 'example', ['safe-argument'], {
        input: secret,
      }),
    (error: unknown) => {
      assert.ok(error instanceof CommandError);
      assert.equal(error.exitCode, 17);
      assert.match(error.message, /safe-argument/u);
      assert.doesNotMatch(error.message, new RegExp(secret, 'u'));
      return true;
    },
  );
});
