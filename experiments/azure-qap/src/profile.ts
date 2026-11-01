import fs from 'node:fs';
import { parseArgs } from 'node:util';

/** `reload` drops and recreates the external table: a known re-read, used as the positive control. */
export const OPERATIONS = ['optimize', 'purge', 'vacuum', 'reload'] as const;
export type Operation = typeof OPERATIONS[number];

export const MIB = 1024 * 1024;
const SECOND = 1000;
const MINUTE = 60 * SECOND;
const HOUR = 60 * MINUTE;
/** Same ceiling as `arrowcol-experiment seed-scale --max-total-bytes`. */
export const MAX_TRIAL_BYTES = 2 * 1024 * MIB;

export interface Profile {
  operations: Operation[];
  /** Files per multi-file partition role (expired and compact). */
  files: number;
  fileMib: number;
  repetitions: number;
  parallel: number;
  /** Quiet watch window after the mutation once QAP has caught up. */
  tailMs: number;
  /** Longest wait for StorageBlobLogs to arrive after a trial's window closes. */
  logWaitMs: number;
  /** Hard cap for all trials; cleanup gets a separate bounded allowance. */
  budgetMs: number;
  /** Quiet hold after the first catch-up, before the mutation. */
  settleMs: number;
  pollMs: number;
  logPollMs: number;
}

export interface Options {
  profile: Profile;
  env: string;
  out?: string;
  help: boolean;
}

type Limits = { [K in keyof Profile]?: [number, number] };
const LIMITS: Limits = {
  files: [2, 128], fileMib: [1, 32], repetitions: [1, 10], parallel: [1, 8],
  tailMs: [0, 6 * HOUR], logWaitMs: [0, 2 * HOUR], budgetMs: [MINUTE, 24 * HOUR],
  settleMs: [0, 30 * MINUTE], pollMs: [SECOND, 5 * MINUTE], logPollMs: [5 * SECOND, 10 * MINUTE],
};

/** Accepts `90s`, `2m`, `1h`, `1h30m`, bare seconds (`300`) and `0`. */
export function parseDuration(text: string, name = 'duration'): number {
  const value = text.trim().toLowerCase();
  if (/^\d+$/.test(value)) return Number(value) * SECOND;
  const match = /^(?:(\d+)h)?(?:(\d+)m)?(?:(\d+)s)?$/.exec(value);
  if (!value || !match) throw new Error(`${name} must look like 90s, 2m, 1h or 1h30m (got "${text}").`);
  return (Number(match[1] ?? 0) * 3600 + Number(match[2] ?? 0) * 60 + Number(match[3] ?? 0)) * SECOND;
}

export function formatDuration(ms: number): string {
  if (ms <= 0) return '0s';
  let seconds = Math.round(ms / SECOND);
  const hours = Math.floor(seconds / 3600);
  seconds -= hours * 3600;
  const minutes = Math.floor(seconds / 60);
  seconds -= minutes * 60;
  return `${hours ? `${hours}h` : ''}${minutes ? `${minutes}m` : ''}${seconds ? `${seconds}s` : ''}` || '0s';
}

/** Bytes seeded for one trial: expired and compact roles hold `files` each, control and live one each. */
export function trialBytes(profile: Pick<Profile, 'files' | 'fileMib'>): number {
  return (2 * profile.files + 2) * profile.fileMib * MIB;
}

export function validateProfile(profile: Profile): Profile {
  for (const [key, [min, max]] of Object.entries(LIMITS) as [keyof Profile, [number, number]][]) {
    const value = profile[key];
    if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < min || value > max) {
      const unit = key.endsWith('Ms');
      throw new Error(`${key} must be between ${unit ? formatDuration(min) : min} and ${unit ? formatDuration(max) : max}.`);
    }
  }
  if (!profile.operations.length || new Set(profile.operations).size !== profile.operations.length ||
      profile.operations.some(op => !OPERATIONS.includes(op)))
    throw new Error(`operations must be a non-empty subset of ${OPERATIONS.join(',')}.`);
  if (trialBytes(profile) > MAX_TRIAL_BYTES)
    throw new Error(`One trial would seed ${trialBytes(profile) / MIB} MiB; lower --files or --file-mib (limit 2048 MiB).`);
  if (profile.operations.includes('reload') && profile.logWaitMs === 0)
    throw new Error('reload is the positive control and needs StorageBlobLogs; with --log-wait 0 leave reload out of --operations.');
  if (tailFloorMs(profile) > profile.budgetMs)
    throw new Error(`${profile.operations.length * profile.repetitions} trials, ${profile.parallel} at a time, need at least ` +
      `${formatDuration(tailFloorMs(profile))} for their tails alone; raise --budget (e.g. --budget 1h) or --parallel, or shorten --tail.`);
  return profile;
}

/** Lower bound for the trial phase: every wave of parallel trials spends at least one full tail window. */
export function tailFloorMs(profile: Pick<Profile, 'operations' | 'repetitions' | 'parallel' | 'tailMs'>): number {
  return Math.ceil(profile.operations.length * profile.repetitions / profile.parallel) * profile.tailMs;
}

export function parseOperations(text: string): Operation[] {
  const items = text.split(',').map(item => item.trim().toLowerCase()).filter(Boolean);
  const unknown = items.filter(item => !(OPERATIONS as readonly string[]).includes(item));
  if (!items.length || unknown.length)
    throw new Error(`--operations must be a comma list of ${OPERATIONS.join(', ')}${unknown.length ? ` (unknown: ${unknown.join(', ')})` : ''}.`);
  return [...new Set(items)] as Operation[];
}

const KEYS = ['operations', 'files', 'fileMib', 'repetitions', 'parallel', 'tail', 'logWait', 'budget', 'settle',
  'poll', 'logPoll'] as const;

export function profileFromJson(value: unknown): Profile {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('scenario.json must be an object.');
  const raw = value as Record<string, unknown>;
  const unknown = Object.keys(raw).filter(key => !(KEYS as readonly string[]).includes(key));
  const missing = KEYS.filter(key => !(key in raw));
  if (unknown.length || missing.length)
    throw new Error(`scenario.json keys mismatch (unknown: ${unknown.join(',') || '-'}; missing: ${missing.join(',') || '-'}).`);
  const text = (key: string) => {
    if (typeof raw[key] !== 'string') throw new Error(`scenario.json ${key} must be a duration string.`);
    return parseDuration(raw[key] as string, key);
  };
  if (!Array.isArray(raw.operations) || raw.operations.some(op => typeof op !== 'string'))
    throw new Error('scenario.json operations must be a list of names.');
  return validateProfile({
    operations: parseOperations(raw.operations.join(',')),
    files: raw.files as number, fileMib: raw.fileMib as number,
    repetitions: raw.repetitions as number, parallel: raw.parallel as number,
    tailMs: text('tail'), logWaitMs: text('logWait'), budgetMs: text('budget'),
    settleMs: text('settle'), pollMs: text('poll'), logPollMs: text('logPoll'),
  });
}

export function loadProfile(file: string): Profile {
  return profileFromJson(JSON.parse(fs.readFileSync(file, 'utf8')));
}

function integer(text: string, flag: string): number {
  if (!/^\d+$/.test(text.trim())) throw new Error(`${flag} must be a whole number (got "${text}").`);
  return Number(text);
}

export function parseOptions(argv: readonly string[], defaults: Profile): Options {
  const { values } = parseArgs({ args: [...argv], strict: true, allowPositionals: false, options: {
    operations: { type: 'string' },
    files: { type: 'string' },
    'file-mib': { type: 'string' },
    repetitions: { type: 'string' },
    parallel: { type: 'string' },
    tail: { type: 'string' },
    'log-wait': { type: 'string' },
    budget: { type: 'string' },
    env: { type: 'string', default: '.env' },
    out: { type: 'string' },
    help: { type: 'boolean', short: 'h', default: false },
  } });
  const profile: Profile = { ...defaults, operations: [...defaults.operations] };
  if (values.operations !== undefined) profile.operations = parseOperations(values.operations);
  if (values.files !== undefined) profile.files = integer(values.files, '--files');
  if (values['file-mib'] !== undefined) profile.fileMib = integer(values['file-mib'], '--file-mib');
  if (values.repetitions !== undefined) profile.repetitions = integer(values.repetitions, '--repetitions');
  if (values.parallel !== undefined) profile.parallel = integer(values.parallel, '--parallel');
  if (values.tail !== undefined) profile.tailMs = parseDuration(values.tail, '--tail');
  if (values['log-wait'] !== undefined) profile.logWaitMs = parseDuration(values['log-wait'], '--log-wait');
  if (values.budget !== undefined) profile.budgetMs = parseDuration(values.budget, '--budget');
  return { profile: values.help ? profile : validateProfile(profile), env: values.env, out: values.out, help: values.help };
}

export function helpText(defaults: Profile): string {
  const d = defaults;
  return `Azure Kusto QAP demo: watch how Query Acceleration reacts to Delta maintenance.

For each operation it seeds a tiny Hive-partitioned Delta table, accelerates it with QAP,
runs the real maintenance (or a known reload), watches QAP state, then counts which
UNCHANGED Parquet files Kusto's managed identity read again (StorageBlobLogs).

Usage:
  npx nx run azure-qap-experiment:run                     # quick default (~5 min + log wait)
  npx nx run azure-qap-experiment:run -- --files 16 --file-mib 32 --repetitions 2 --tail 10m --budget 1h

Options (defaults from experiments/azure-qap/scenario.json):
  --operations <list>  ${OPERATIONS.join(',')} (default ${d.operations.join(',')}; reload = positive control)
  --files <n>          files per multi-file partition role, 2..128 (default ${d.files})
  --file-mib <n>       MiB per seeded file, 1..32 (default ${d.fileMib})
  --repetitions <n>    trials per operation, 1..10 (default ${d.repetitions})
  --parallel <n>       concurrent trials, 1..8 (default ${d.parallel})
  --tail <dur>         watch window after the mutation once QAP caught up (default ${formatDuration(d.tailMs)})
  --log-wait <dur>     max wait for StorageBlobLogs, 0 = skip (default ${formatDuration(d.logWaitMs)})
  --budget <dur>       hard cap for all trials incl. log wait (default ${formatDuration(d.budgetMs)}); cleanup gets up to
                       5m more. Must cover ceil(trials / parallel) x tail, so raise it for longer runs
  --env <file>         private env file (default .env; ARROWCOL_RESET_GENERATION may come from .env.azure)
  --out <dir>          report directory (default .temp/experiments/azure-qap/<runId>)
  -h, --help           show this help

Durations accept 90s, 2m, 1h, 1h30m or bare seconds.
Cleanup always runs, also on failure and Ctrl+C: the run's Kusto tables are dropped and
its fixture directory runs/<runId>/ is deleted. Copy the printed KQL to explore while it runs.
Exit code is nonzero on errors, failed trials, failed cleanup, or when an included reload
(the positive control) was not seen re-reading every unchanged file, for whatever reason,
including logs still pending or the budget running out. --log-wait 0 requires leaving
reload out of --operations.
Requires the provisioned sandbox: npx nx run azure-infra:provision
Docs: docs/experiments/azure-qap/README.md`;
}
