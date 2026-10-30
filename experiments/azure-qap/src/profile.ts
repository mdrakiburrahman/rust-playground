import fs from 'node:fs';

export interface Profile {
  format: 1;
  rowsPerFile: number;
  filesPerPartition: number;
  partitions: number;
  appendBatches: number;
  appendIntervalMs: number;
  observeSeconds: number;
  sampleIntervalMs: number;
  qapWaitSeconds: number;
  vacuumRetentionSeconds: number;
}

export function profile(value: unknown, allowImmediateVacuum: boolean): Profile {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Experiment profile must be an object.');
  const item = value as Record<string, unknown>;
  const keys = ['format', 'rowsPerFile', 'filesPerPartition', 'partitions', 'appendBatches',
    'appendIntervalMs', 'observeSeconds', 'sampleIntervalMs', 'qapWaitSeconds', 'vacuumRetentionSeconds'];
  if (Object.keys(item).sort().join(',') !== keys.sort().join(',') || item.format !== 1)
    throw new Error('Unknown or missing experiment profile settings.');
  const integer = (name: string, minimum: number, maximum: number): number => {
    const value = item[name];
    if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < minimum || value > maximum)
      throw new Error(`${name} must be an integer in ${minimum}..${maximum}.`);
    return value;
  };
  const result: Profile = {
    format: 1, rowsPerFile: integer('rowsPerFile', 2, 1024),
    filesPerPartition: integer('filesPerPartition', 2, 16), partitions: integer('partitions', 4, 4),
    appendBatches: integer('appendBatches', 4, 64), appendIntervalMs: integer('appendIntervalMs', 100, 10_000),
    observeSeconds: integer('observeSeconds', 5, 120), sampleIntervalMs: integer('sampleIntervalMs', 1000, 10_000),
    qapWaitSeconds: integer('qapWaitSeconds', 30, 1200),
    vacuumRetentionSeconds: integer('vacuumRetentionSeconds', 0, 604_800),
  };
  const maximumRows = result.rowsPerFile * (result.filesPerPartition * result.partitions + result.appendBatches * 3);
  if (maximumRows > 90_000) throw new Error('Profile exceeds the bounded exact-query/oracle row budget.');
  if (result.vacuumRetentionSeconds < 604_800 && !allowImmediateVacuum)
    throw new Error('Short VACUUM retention can break QAP/old-snapshot readers. Explicit --allow-immediate-vacuum is required.');
  return result;
}

export function loadProfile(filename: string, allowImmediateVacuum: boolean): Profile {
  return profile(JSON.parse(fs.readFileSync(filename, 'utf8')), allowImmediateVacuum);
}
