import assert from 'node:assert/strict';
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import test from 'node:test';

import {
  SdkGenerationTool,
  fixtureArtifacts,
  evolvedFixtureArtifacts,
  evolvedGeneratedOutput,
  generatedOutput,
  validationRoot,
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
      '--no-install', 'buf', 'breaking', `${evolvedFixtureArtifacts}/descriptor.pb#format=binpb`,
      '--against', `${fixtureArtifacts}/descriptor.pb#format=binpb`, '--config', 'buf.yaml',
    ],
  });
});

test('generated validation stages the shared runtime beside both versioned clients', (t) => {
  const workspace = mkdtempSync(join(tmpdir(), 'arrowcol-sdk-validation-'));
  t.after(() => rmSync(workspace, { recursive: true, force: true }));
  const versions = [
    [generatedOutput, validationRoot],
    [evolvedGeneratedOutput, `${validationRoot}-v2`],
  ] as const;
  for (const [source] of versions) {
    for (const directory of ['rust', 'typescript', 'descriptors']) {
      mkdirSync(join(workspace, source, directory), { recursive: true });
    }
    const runtime = join(workspace, source, 'runtime/arrowcol-sdk-runtime');
    mkdirSync(join(runtime, 'src'), { recursive: true });
    writeFileSync(join(workspace, source, 'rust/Cargo.toml'),
      '[dependencies]\narrowcol-sdk-runtime = { path = "../runtime/arrowcol-sdk-runtime" }\n');
    writeFileSync(join(runtime, 'Cargo.toml'), '[package]\nname = "arrowcol-sdk-runtime"\nversion = "0.1.0"\n');
    writeFileSync(join(runtime, 'src/lib.rs'), `// Runtime packaged with ${source}\n`);
  }

  new SdkGenerationTool(workspace, new RecordingRunner()).testGenerated();

  for (const [source, staged] of versions) {
    const manifest = join(workspace, staged, 'rust/Cargo.toml');
    const dependency = readFileSync(manifest, 'utf8').match(/path = "([^"]+)"/u)?.[1];
    assert.ok(dependency, 'The staged client must reference its shared runtime');
    const runtime = join(dirname(manifest), dependency);
    for (const file of ['Cargo.toml', 'src/lib.rs']) {
      assert.equal(readFileSync(join(runtime, file), 'utf8'),
        readFileSync(join(workspace, source, 'runtime/arrowcol-sdk-runtime', file), 'utf8'));
    }
  }
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
