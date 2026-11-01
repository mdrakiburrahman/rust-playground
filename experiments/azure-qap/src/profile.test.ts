import test from 'node:test';
import assert from 'node:assert/strict';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { formatDuration, helpText, loadProfile, MIB, parseDuration, parseOperations, parseOptions, profileFromJson,
  tailFloorMs, trialBytes } from './profile.js';

const SCENARIO = path.join(path.dirname(fileURLToPath(import.meta.url)), '..', 'scenario.json');
const defaults = loadProfile(SCENARIO);

test('scenario.json defaults are the quick demo', () => {
  assert.deepEqual(defaults, {
    operations: ['optimize', 'purge', 'vacuum', 'reload'], files: 2, fileMib: 1, repetitions: 1, parallel: 4,
    tailMs: 120_000, logWaitMs: 300_000, budgetMs: 900_000, settleMs: 30_000, pollMs: 10_000, logPollMs: 30_000,
  });
  assert.equal(trialBytes(defaults), 6 * MIB);
});

test('durations accept s/m/h, combinations, bare seconds and zero', () => {
  assert.equal(parseDuration('90s'), 90_000);
  assert.equal(parseDuration('2m'), 120_000);
  assert.equal(parseDuration('1h'), 3_600_000);
  assert.equal(parseDuration('1h30m'), 5_400_000);
  assert.equal(parseDuration('300'), 300_000);
  assert.equal(parseDuration('0'), 0);
  for (const bad of ['', '5x', '-1s', '1.5m', 'm'])
    assert.throws(() => parseDuration(bad, '--tail'), /--tail must look like/);
  assert.equal(formatDuration(90_000), '1m30s');
  assert.equal(formatDuration(3_600_000), '1h');
  assert.equal(formatDuration(0), '0s');
});

test('CLI flags override scenario.json without mutating the defaults', () => {
  const options = parseOptions(['--files', '16', '--file-mib', '32', '--repetitions', '2', '--tail', '10m',
    '--operations', 'vacuum,purge,vacuum', '--parallel', '2', '--log-wait', '0', '--budget', '1h',
    '--env', 'other.env', '--out', 'out/dir'], defaults);
  assert.deepEqual(options.profile, { ...defaults, operations: ['vacuum', 'purge'], files: 16, fileMib: 32, repetitions: 2,
    parallel: 2, tailMs: 600_000, logWaitMs: 0, budgetMs: 3_600_000 });
  assert.equal(options.env, 'other.env');
  assert.equal(options.out, 'out/dir');
  assert.deepEqual(defaults.operations, ['optimize', 'purge', 'vacuum', 'reload']);
  assert.deepEqual(parseOptions([], defaults), { profile: defaults, env: '.env', out: undefined, help: false });
});

test('the documented long run needs --budget 1h because its tails alone exceed the 15m default', () => {
  const long = ['--files', '16', '--file-mib', '32', '--repetitions', '2', '--tail', '10m'];
  assert.equal(tailFloorMs(defaults), 120_000);
  assert.throws(() => parseOptions(long, defaults), /8 trials, 4 at a time, need at least 20m for their tails alone; raise --budget/);
  const profile = parseOptions([...long, '--budget', '1h'], defaults).profile;
  assert.deepEqual(profile, { ...defaults, files: 16, fileMib: 32, repetitions: 2, tailMs: 600_000, budgetMs: 3_600_000 });
  assert.equal(tailFloorMs(profile), 1_200_000);
  assert.match(helpText(defaults), /--repetitions 2 --tail 10m --budget 1h\n/);
});

test('reload is the positive control and cannot run without StorageBlobLogs', () => {
  assert.throws(() => parseOptions(['--log-wait', '0'], defaults), /leave reload out of --operations/);
  assert.deepEqual(parseOptions(['--log-wait', '0', '--operations', 'optimize,purge,vacuum'], defaults).profile.operations,
    ['optimize', 'purge', 'vacuum']);
});

test('the tail window has no minimum but every knob is bounded', () => {
  assert.equal(parseOptions(['--tail', '0'], defaults).profile.tailMs, 0);
  assert.equal(parseOptions(['--tail', '90s'], defaults).profile.tailMs, 90_000);
  const cases: [string[], RegExp][] = [
    [['--files', '1'], /files must be between 2 and 128/],
    [['--files', 'abc'], /--files must be a whole number/],
    [['--file-mib', '64'], /fileMib must be between 1 and 32/],
    [['--files', '128', '--file-mib', '32'], /One trial would seed 8256 MiB/],
    [['--parallel', '0'], /parallel must be between 1 and 8/],
    [['--budget', '30s'], /budgetMs must be between 1m and 24h/],
    [['--operations', 'optimize,bogus'], /unknown: bogus/],
    [['--operations', ','], /--operations must be a comma list/],
    [['--nope'], /Unknown option/],
    [['--keep'], /Unknown option '--keep'/],
    [['positional'], /Unexpected argument|positional/i],
  ];
  for (const [argv, error] of cases) assert.throws(() => parseOptions(argv, defaults), error, argv.join(' '));
});

test('--help skips validation and documents every flag with its default', () => {
  assert.equal(parseOptions(['--help', '--files', '1'], defaults).help, true);
  assert.equal(parseOptions(['-h'], defaults).help, true);
  const text = helpText(defaults);
  for (const flag of ['--operations', '--files', '--file-mib', '--repetitions', '--parallel', '--tail', '--log-wait', '--budget',
    '--env', '--out', '--help'])
    assert.match(text, new RegExp(`${flag}\\b`));
  assert.doesNotMatch(text, /--keep/);
  assert.match(text, /Cleanup always runs/);
  assert.match(text, /positive control/);
  assert.match(text, /default 2m\)/);
  assert.match(text, /docs\/experiments\/azure-qap\/README\.md/);
});

test('scenario.json must list exactly the known keys', () => {
  const raw = { operations: ['purge'], files: 2, fileMib: 1, repetitions: 1, parallel: 1, tail: '0', logWait: '0', budget: '5m',
    settle: '0', poll: '5s', logPoll: '30s' };
  assert.equal(profileFromJson(raw).budgetMs, 300_000);
  assert.throws(() => profileFromJson({ ...raw, extra: 1 }), /unknown: extra/);
  const { poll: _, ...missing } = raw;
  assert.throws(() => profileFromJson(missing), /missing: poll/);
  assert.throws(() => profileFromJson({ ...raw, tail: 30 }), /tail must be a duration string/);
  assert.throws(() => profileFromJson([]), /must be an object/);
  assert.deepEqual(parseOperations(' Reload , optimize '), ['reload', 'optimize']);
});
