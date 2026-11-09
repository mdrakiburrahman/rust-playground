import assert from 'node:assert/strict';
import test from 'node:test';

import {
  SdkGenerationTool,
  fixtureArtifacts,
  evolvedFixtureArtifacts,
  generatedOutput,
  type CommandRunner,
} from '../src/index.js';
import { runCli } from '../src/cli.js';

class RecordingRunner implements CommandRunner {
  readonly calls: Array<{
    readonly args: readonly string[];
    readonly command: string;
  }> = [];

  run(command: string, args: readonly string[]) {
    this.calls.push({ command, args: [...args] });
    return { exitCode: 0, stderr: '', stdout: '' };
  }
}

test('generate fixture preserves separately generated v1 and v2 clients', () => {
  const runner = new RecordingRunner();
  const tool = new SdkGenerationTool(process.cwd(), runner);
  const result = tool.generateFixture();

  assert.equal(result.commands.length, 4);
  assert.deepEqual(
    runner.calls.map((call) => call.command),
    ['cargo', 'cargo', 'cargo', 'cargo'],
  );
  assert.ok(runner.calls[0]?.args.includes('fixture'));
  assert.ok(runner.calls[1]?.args.includes('generate'));
  assert.ok(runner.calls[1]?.args.includes(fixtureArtifacts));
  assert.ok(runner.calls[1]?.args.includes(generatedOutput));
  assert.ok(runner.calls[2]?.args.includes('--previous'));
  assert.ok(runner.calls[2]?.args.includes(evolvedFixtureArtifacts));
  assert.ok(runner.calls[3]?.args.includes(evolvedFixtureArtifacts));
});

test('compatibility compares consecutive versions and runs the real Buf WIRE_JSON CLI', () => {
  const runner = new RecordingRunner();
  new SdkGenerationTool(process.cwd(), runner).compatibility();
  assert.deepEqual(runner.calls[0]?.args.slice(-4), [
    '--previous', fixtureArtifacts, '--current', evolvedFixtureArtifacts,
  ]);
  assert.deepEqual(runner.calls[1], {
    command: 'npx',
    args: [
      '--no-install', 'buf', 'breaking', `${evolvedFixtureArtifacts}/schema.proto`,
      '--against', `${fixtureArtifacts}/schema.proto`, '--config', 'buf.yaml',
    ],
  });
});
test('check CLI delegates to deterministic check workflow', () => {
  const runner = new RecordingRunner();
  const tool = new SdkGenerationTool(process.cwd(), runner);
  const commands = runCli(['check'], tool);

  assert.equal(commands.length, 2);
  assert.ok(runner.calls[0]?.args.includes('check'));
});

test('generate CLI hydrates SQL against a catalog rather than silently reading fixture artifacts', () => {
  const runner = new RecordingRunner();
  const tool = new SdkGenerationTool(process.cwd(), runner);
  runCli(['generate', '--catalog-endpoint', 'http://localhost:8817',
    '--table', 'telemetry.prod.sdk_events', '--ddl', 'schemas/events.sql'], tool);
  assert.ok(runner.calls[0]?.args.includes('--catalog-endpoint'));
  assert.ok(runner.calls[0]?.args.includes('schemas/events.sql'));
  assert.ok(!runner.calls[0]?.args.includes('--artifacts'));
});
test('CLI rejects unknown workflows', () => {
  const runner = new RecordingRunner();
  const tool = new SdkGenerationTool(process.cwd(), runner);
  assert.throws(() => runCli(['unknown'], tool), /Unknown arrowcol SDK workflow/u);
});
