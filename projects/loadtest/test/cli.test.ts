import assert from 'node:assert/strict';
import { test } from 'node:test';

import { benchmarkUsage, parseBenchmarkOptions, parseDuration } from '../src/cli.js';

test('durations accept whole-number units', () => {
  assert.equal(parseDuration('30', 'd'), 30);
  assert.equal(parseDuration('2m', 'd'), 120);
  assert.equal(parseDuration('1H', 'd'), 3600);
  assert.throws(() => parseDuration('1.5m', 'd'), /must look like/u);
});

test('the normal command has no execution naming or generic fault modes', () => {
  assert(!('runId' in parseBenchmarkOptions([])));
  assert(!/run-id|chaos|soak/u.test(benchmarkUsage));
  for (const option of ['--run-id', '--chaos', '--soak']) {
    assert.throws(() => parseBenchmarkOptions([option, 'value']), /Unknown option/u);
  }
  const defaults = parseBenchmarkOptions([]);
  assert.equal(defaults.quiver, false);
  assert.equal(defaults.crashAfterSeconds, undefined);
  assert.equal(defaults.drainTimeoutSeconds, 480, 'the fixed five-minute cached gate needs its own bounded outer validation budget');
});

test('--quiver enables disk persistence and --crash-after requires it', () => {
  assert.equal(parseBenchmarkOptions(['--quiver']).quiver, true);
  const crash = parseBenchmarkOptions(['--quiver', '--crash-after', '20s']);
  assert.deepEqual([crash.quiver, crash.crashAfterSeconds], [true, 20]);
  assert.equal(parseBenchmarkOptions(['--crash-after=1m', '--quiver']).crashAfterSeconds, 60);
  assert.throws(() => parseBenchmarkOptions(['--crash-after', '20s']), /requires --quiver/u);
  assert.throws(() => parseBenchmarkOptions(['--quiver', '--crash-after', '0s']), /must be positive/u);
  assert.match(benchmarkUsage, /--quiver/u);
});

test('--crash-after must interrupt live load', () => {
  const crash = parseBenchmarkOptions(['--quiver', '--warmup', '5s', '--measure', '60s', '--crash-after', '15s']);
  assert.equal(crash.crashAfterSeconds, 15);
  assert.throws(() => parseBenchmarkOptions(['--quiver', '--warmup', '5s', '--measure', '10s', '--crash-after', '15s']),
    /shorter than --warmup \+ --measure/u);
  assert.equal(parseBenchmarkOptions(['--quiver', '--crash-after', '1h']).crashAfterSeconds, 3600, 'continuous runs accept any delay');
});

test('defaults and overrides', () => {
  assert.equal(parseBenchmarkOptions([]).warmupSeconds, 30);
  const options = parseBenchmarkOptions(['--warmup=2m', '--concurrency', '8', '--batch-rows', '64']);
  assert.deepEqual([options.warmupSeconds, options.concurrency, options.batchRows], [120, 8, 64]);
  assert.throws(() => parseBenchmarkOptions(['--unknown', '1']), /Unknown/u);
  assert.throws(() => parseBenchmarkOptions(['--concurrency']), /requires a value/u);
});

test('final compaction defaults on only locally; explicit Azure requests fail before effects', () => {
  assert.equal(parseBenchmarkOptions(['--validate'], {}).finalCompaction, true);
  assert.equal(parseBenchmarkOptions(['--validate', '--no-final-compaction'], {}).finalCompaction, false);
  assert.equal(parseBenchmarkOptions(['--validate', '--mode', 'azure'], {}).finalCompaction, false);
  assert.equal(parseBenchmarkOptions(['--validate', '--no-final-compaction'], { ARROWCOL_MODE: 'azure' }).finalCompaction, false);
  assert.throws(() => parseBenchmarkOptions(['--validate', '--mode', 'azure', '--final-compaction'], {}), /unsupported in Azure/u);
  assert.throws(() => parseBenchmarkOptions(['--validate', '--final-compaction'], { ARROWCOL_MODE: 'azure' }), /unsupported in Azure/u);
  assert.throws(() => parseBenchmarkOptions(['--final-compaction'], {}), /requires --validate/u);
  assert.throws(() => parseBenchmarkOptions(['--no-final-compaction', '--final-compaction'], {}), /only once/u);
});
