import assert from 'node:assert/strict';
import test from 'node:test';

import {
  CommandError,
  type CommandOptions,
  type CommandResult,
  type CommandRunner,
  runChecked,
  SpawnCommandRunner,
} from '../src/process.js';

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
