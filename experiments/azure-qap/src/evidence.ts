export interface DeltaFile {
  path: string;
  bytes: number;
  partition: string;
}

export interface Snapshot {
  version: number;
  files: DeltaFile[];
  rows: number;
  fingerprint: string;
}

export interface AccelerationSample {
  at: string;
  version?: number;
  completionPercent?: number;
  pendingFiles?: number;
  pendingBytes?: number;
  acceleratedRows?: number;
  lastIngestionAt?: string;
  healthy?: boolean;
}

export interface OriginEvent {
  at: string;
  path: string;
  kind: 'ingestion' | 'merge' | 'removal';
  bytes?: number;
}

export interface DiagnosticCapability {
  originHistory: boolean;
  completeHistory: boolean;
  distinguishesIngestionFromMerge: boolean;
  positiveControlDetected: boolean;
  backgroundCpu: boolean;
  reason: string;
}

export interface TrialEvidence {
  before: Snapshot;
  after: Snapshot;
  mutationStartedAt: string;
  observationEndedAt: string;
  samples: AccelerationSample[];
  originEvents: OriginEvent[];
  capabilities: DiagnosticCapability;
  correctness: 'passed' | 'failed' | 'unavailable';
}

export interface Assessment {
  status: 'passed' | 'finding' | 'inconclusive' | 'failed';
  classification:
    | 'only-added-or-rewritten-files'
    | 'unchanged-files-reingested'
    | 'whole-cache-rebuilt'
    | 'insufficient-evidence'
    | 'incorrect-query-result';
  addedFiles: number;
  addedBytes: number;
  removedFiles: number;
  unchangedFiles: number;
  unchangedBytes: number;
  unchangedReingestedFiles?: number;
  observedReingestedBytes?: number;
  reason: string;
}

function assertSnapshot(snapshot: Snapshot): void {
  if (!Number.isSafeInteger(snapshot.version) || snapshot.version < 0 ||
      !Number.isSafeInteger(snapshot.rows) || snapshot.rows < 0 || !snapshot.fingerprint)
    throw new Error('Delta snapshot requires an exact version, row count and fingerprint.');
  const paths = new Set<string>();
  for (const file of snapshot.files) {
    if (!file.path || paths.has(file.path) || !Number.isSafeInteger(file.bytes) || file.bytes < 0)
      throw new Error('Delta file inventory contains an invalid size or duplicate/empty path.');
    paths.add(file.path);
  }
}

export function assess(evidence: TrialEvidence): Assessment {
  assertSnapshot(evidence.before);
  assertSnapshot(evidence.after);
  const before = new Map(evidence.before.files.map(file => [file.path, file]));
  const after = new Map(evidence.after.files.map(file => [file.path, file]));
  const added = evidence.after.files.filter(file => !before.has(file.path));
  const removed = evidence.before.files.filter(file => !after.has(file.path));
  const unchanged = evidence.after.files.filter(file => before.has(file.path));
  for (const file of unchanged) {
    if (before.get(file.path)?.bytes !== file.bytes)
      throw new Error('An existing Delta file changed size; immutable file identity is not trustworthy.');
  }
  const base = {
    addedFiles: added.length,
    addedBytes: added.reduce((sum, file) => sum + file.bytes, 0),
    removedFiles: removed.length,
    unchangedFiles: unchanged.length,
    unchangedBytes: unchanged.reduce((sum, file) => sum + file.bytes, 0),
  };
  if (evidence.correctness === 'failed')
    return { ...base, status: 'failed', classification: 'incorrect-query-result',
      reason: 'Cached/fallback results did not match the independent exact-version Delta oracle.' };
  const capability = evidence.capabilities;
  if (!capability.originHistory || !capability.distinguishesIngestionFromMerge)
    return { ...base, status: 'inconclusive', classification: 'insufficient-evidence',
      reason: capability.reason + ' Aggregate completion, table version, extent merges and query fallback do not prove cache ingestion.' };
  const start = Date.parse(evidence.mutationStartedAt);
  const end = Date.parse(evidence.observationEndedAt);
  if (!Number.isFinite(start) || !Number.isFinite(end) || end < start)
    throw new Error('Invalid mutation observation interval.');
  const ingested = new Set(evidence.originEvents.filter(event => {
    const at = Date.parse(event.at);
    if (!Number.isFinite(at)) throw new Error('Origin history contains an invalid timestamp.');
    return event.kind === 'ingestion' && at >= start && at <= end;
  }).map(event => event.path));
  const repeated = unchanged.filter(file => ingested.has(file.path));
  const measured = {
    unchangedReingestedFiles: repeated.length,
    observedReingestedBytes: repeated.reduce((sum, file) => sum + file.bytes, 0),
  };
  if (repeated.length) {
    const full = unchanged.length === evidence.before.files.length &&
      repeated.length === unchanged.length;
    return { ...base, ...measured, status: 'finding',
      classification: full ? 'whole-cache-rebuilt' : 'unchanged-files-reingested',
      reason: full
        ? 'Every pre-existing source file was ingested again during the observation interval.'
        : 'Previously cached, unchanged source files were ingested again; rewritten files were excluded.' };
  }
  if (!capability.completeHistory || !capability.positiveControlDetected || evidence.correctness !== 'passed')
    return { ...base, ...measured, status: 'inconclusive', classification: 'insufficient-evidence',
      reason: 'A negative result requires complete origin ingestion history, a detected full-reload control and exact-version correctness.' };
  return { ...base, ...measured, status: 'passed', classification: 'only-added-or-rewritten-files',
    reason: 'Complete calibrated origin history found no unchanged-file reingestion; added/replacement files are legitimate new work.' };
}

export function deltaChanges(before: Snapshot, after: Snapshot): Pick<Assessment,
  'addedFiles' | 'addedBytes' | 'removedFiles' | 'unchangedFiles' | 'unchangedBytes'> {
  const result = assess({
    before, after, mutationStartedAt: '', observationEndedAt: '', samples: [], originEvents: [],
    correctness: 'unavailable',
    capabilities: { originHistory: false, completeHistory: false, distinguishesIngestionFromMerge: false,
      positiveControlDetected: false, backgroundCpu: false, reason: 'Inventory only.' },
  });
  return { addedFiles: result.addedFiles, addedBytes: result.addedBytes, removedFiles: result.removedFiles,
    unchangedFiles: result.unchangedFiles, unchangedBytes: result.unchangedBytes };
}
