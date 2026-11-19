import { debugTuple, evidenceList, evidenceObject, evidenceText, parseEvidence, requireEvidence, uint } from './benchmark-evidence-json.js';
import { decodedRecords, manifestBounds, NOT_QUALIFIED } from './benchmark-manifest-audit.js';

const message = 'Delta logical publication audit receipt';
const publicationPrefix = 'arrowcol.publication.';
const fileSuffix = /^[A-Za-z0-9_.-]+\.parquet$/;
const sum = <T>(items: Iterable<T>, get: (item: T) => bigint) => {
  let total = 0n;
  for (const item of items) total += get(item);
  return total;
};
function append<K, V>(map: Map<K, V[]>, key: K, value: V) {
  const entries = map.get(key);
  if (entries) entries.push(value);
  else map.set(key, [value]);
}
function sameSet<T>(a: Set<T>, b: Set<T>) { return a.size === b.size && [...a].every(value => b.has(value)); }
export function someUint(value: unknown, label: string) {
  const digits = /^Some\((0|[1-9]\d*)\)$/.exec(evidenceText(value))?.[1];
  requireEvidence(digits !== undefined, `Unavailable or malformed ${label}`);
  return uint(BigInt(digits), label);
}
export function debugList(value: unknown) {
  const text = evidenceText(value);
  requireEvidence(text.length <= 8 * 1024 * 1024, 'Oversized receipt debug list');
  return evidenceList(parseEvidence(text, true));
}
function receivedAt(value: unknown) {
  const text = evidenceText(value);
  const match = /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d(?:\.(\d{1,9}))?Z$/.exec(text);
  requireEvidence(match, 'Receipt timestamp is not UTC RFC3339');
  const millis = Date.parse(text);
  requireEvidence(Number.isFinite(millis) && new Date(millis).toISOString().slice(0, 19) === text.slice(0, 19), 'Invalid receipt timestamp');
  const fraction = match[1] ?? '';
  const startNs = BigInt(Date.parse(`${text.slice(0, 19)}Z`)) * 1_000_000n + BigInt(fraction.padEnd(9, '0') || '0');
  return { millis: BigInt(millis), startNs, endNs: startNs + 10n ** BigInt(9 - fraction.length) };
}
function parseReceipt(event: Record<string, unknown>, fields: Record<string, unknown>) {
  requireEvidence(fields.qualification_state === 'committed' && fields.delta_outcome === 'Committed', 'Publication unresolved or not new');
  const physical = evidenceText(fields.physical_publication_id);
  requireEvidence(/^[a-f0-9]{32}$|^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/.test(physical), 'Noncanonical physical UUID');
  const files = new Map<bigint, string>(), fileNames = new Set<string>();
  for (const entry of debugList(fields.committed_files)) {
    const match = /^(0|[1-9]\d*):([A-Za-z0-9_.-]+\.parquet)$/.exec(evidenceText(entry));
    requireEvidence(match, 'Unsupported committed file name');
    const index = uint(BigInt(match[1]), 'file index'), name = match[2];
    requireEvidence(!files.has(index) && !fileNames.has(name), 'Duplicate committed file');
    files.set(index, name);
    fileNames.add(name);
  }
  requireEvidence(files.size > 0, 'Committed publication has no files');
  const references = new Set<bigint>(), transactions = new Map<string, bigint>();
  const prefix = publicationPrefix + physical;
  const admitted = uint(fields.admitted_at_unix_ms, 'admission timestamp');
  for (const entry of debugList(fields.transaction_files)) {
    const tuple = debugTuple(entry);
    requireEvidence(tuple.length === 4, 'Invalid transaction mapping');
    const app = evidenceText(tuple[0]);
    requireEvidence(app === prefix || (app.startsWith(prefix) && /^\.partition\.[A-Za-z0-9_.-]+$/.test(app.slice(prefix.length))),
      'Transaction does not belong to this physical publication');
    requireEvidence(!transactions.has(app), 'Duplicate mapped transaction');
    transactions.set(app, uint(tuple[1], 'transaction version'));
    requireEvidence(uint(tuple[2], 'transaction mapping ingestion stamp') === admitted,
      'Transaction mapping ingestion stamp differs from physical admission');
    const list = evidenceList(tuple[3]);
    requireEvidence(list.length > 0, 'Transaction has no file membership');
    const local = new Set<bigint>();
    for (const value of list) {
      const index = uint(value, 'file index');
      requireEvidence(files.has(index) && !local.has(index), 'Unproven or duplicate transaction file index');
      local.add(index);
      references.add(index);
    }
  }
  requireEvidence(references.size === files.size, 'File/transaction membership mismatch');
  const rows = uint(fields.input_rows, 'input rows'), canonical = someUint(fields.canonical_logical_bytes, 'canonical logical bytes');
  requireEvidence(rows > 0n && canonical > 0n, 'Empty input publication');
  const timestamp = receivedAt(event.timestamp), observed = timestamp.millis;
  requireEvidence(observed >= admitted, 'Receipt observation precedes admission');
  return { logical: evidenceText(fields.logical_publication_id), physical: physical.replaceAll('-', ''),
    ordinal: uint(fields.fragment_ordinal, 'fragment ordinal'), version: someUint(fields.authoritative_delta_version, 'authoritative Delta version'),
    rows, canonical, files: fileNames, transactions, admitted, observed, observedStartNs: timestamp.startNs, observedEndNs: timestamp.endNs };
}

type Receipt = ReturnType<typeof parseReceipt>;
type Parent = ReturnType<typeof decodedRecords> extends Generator<infer T> ? T : never;
export type ClockOffset = { lowerNs: bigint; upperNs: bigint };
export type DirectReconciliationTiming = { clients: readonly ClockOffset[]; collector: ClockOffset };
type Options = { diagnostic: true; timing: DirectReconciliationTiming | null };
type File = { rows: bigint; bytes: bigint };
const totals = () => ({ rows: 0n, canonical_bytes: 0n });
function rate(total: bigint, duration: bigint) { return Number(total * 1000n) / Number(duration); }

export async function reconcilePublications(manifests: readonly unknown[], events: AsyncIterable<unknown> | Iterable<unknown>,
  delta: ReadonlyMap<bigint, AsyncIterable<unknown> | Iterable<unknown>>, baseline: bigint, options?: Options) {
  uint(baseline, 'baseline Delta version');
  requireEvidence(manifests.length > 0, 'No client manifests');
  const { start, end } = manifestBounds(manifests[0]), duration = end - start;
  const expected = new Map<string, Parent & { client: number }>(), tables = new Set<string>();
  const timing = options?.timing;
  if (timing) {
    requireEvidence(timing.clients.length === manifests.length, 'Clock evidence does not cover every manifest');
    for (const offset of [...timing.clients, timing.collector])
      requireEvidence(offset.lowerNs >= -100_000_000n && offset.upperNs <= 100_000_000n &&
        offset.lowerNs <= offset.upperNs, 'Invalid clock offset interval');
  }
  for (const [client, manifest] of manifests.entries()) {
    const bounds = manifestBounds(manifest);
    requireEvidence(bounds.start === start && bounds.end === end, 'Clients do not share exact measurement bounds');
    for (const record of decodedRecords(manifest)) {
      requireEvidence(!expected.has(record.id), 'Duplicate logical identity across manifests');
      expected.set(record.id, { ...record, client });
      tables.add(record.table);
    }
  }
  requireEvidence(tables.size === 1, 'More than one client table');
  const versions = [...delta.keys()].sort((a, b) => a < b ? -1 : a > b ? 1 : 0);
  requireEvidence(versions.length > 0 && versions.every((version, index) => version === BigInt(index)), 'Incomplete Delta JSON history');
  requireEvidence(delta.has(baseline), 'Missing baseline Delta version');
  const adds = new Map<bigint, Map<string, File>>(), seenPaths = new Set<string>();
  const occurrences = new Map<string, Array<{ version: bigint; txnVersion: bigint }>>();
  for (const version of versions) {
    const files = new Map<string, File>();
    for await (const entry of delta.get(version)!) {
      const action = evidenceObject(entry);
      if (Object.hasOwn(action, 'txn')) {
        const txn = evidenceObject(action.txn);
        uint(txn.lastUpdated, 'Delta transaction timestamp');
        append(occurrences, evidenceText(txn.appId), { version, txnVersion: uint(txn.version, 'Delta transaction version') });
      }
      if (Object.hasOwn(action, 'remove') && version > baseline) {
        requireEvidence(evidenceObject(action.remove).dataChange === false, 'Unexpected data-changing removal during the run');
      }
      if (!Object.hasOwn(action, 'add')) continue;
      const add = evidenceObject(action.add), path = evidenceText(add.path);
      requireEvidence(typeof add.dataChange === 'boolean' && path && !path.includes('\\'), 'Missing dataChange or invalid Delta file path');
      if (add.dataChange && version > baseline) requireEvidence(!seenPaths.has(path), 'Delta file path was added more than once');
      seenPaths.add(path);
      if (!add.dataChange) continue;
      const name = path.slice(path.lastIndexOf('/') + 1);
      requireEvidence(fileSuffix.test(name) && !files.has(name), 'Unsupported or ambiguous Delta file basename');
      const stats = evidenceObject(parseEvidence(evidenceText(add.stats)));
      files.set(name, { rows: uint(stats.numRecords, 'Delta file row count'), bytes: uint(add.size, 'Delta file size') });
    }
    adds.set(version, files);
  }
  const parents = new Map<string, Receipt[]>(), commits = new Map<bigint, Receipt[]>();
  const physical = new Set<string>(), fragments = new Set<string>(), mapped = new Set<string>();
  let diagnosticClockInconsistencies = 0;
  for await (const event of events) {
    const object = evidenceObject(event), fields = evidenceObject(object.fields);
    if (fields.message !== message) continue;
    const item = parseReceipt(object, fields), parent = expected.get(item.logical);
    requireEvidence(parent, 'Receipt has an unknown logical parent');
    requireEvidence(!physical.has(item.physical), 'Duplicate physical publication');
    const fragment = `${item.logical}/${item.ordinal}`;
    requireEvidence(!fragments.has(fragment), 'Duplicate logical fragment');
    physical.add(item.physical);
    fragments.add(fragment);
    const files = adds.get(item.version);
    requireEvidence(files && item.version > baseline, 'Receipt version is outside the run');
    requireEvidence([...item.files].every(file => files.has(file)), 'Receipt references missing Delta adds');
    for (const [app, txnVersion] of item.transactions) {
      const found = occurrences.get(app);
      requireEvidence(found?.length === 1 && found[0].version === item.version && found[0].txnVersion === txnVersion,
        'Transaction is missing, replayed or bound to the wrong Delta version');
      mapped.add(app);
    }
    if (parent.send === 'measurement' && item.admitted < start) {
      if (!options?.diagnostic) requireEvidence(false, 'Measurement input predates its clock boundary');
      diagnosticClockInconsistencies++;
    }
    append(parents, item.logical, item);
    append(commits, item.version, item);
  }
  requireEvidence(parents.size === expected.size, 'Missing parent publications after drain');
  const declared = new Set([...occurrences].filter(([app, entries]) =>
    app.startsWith(publicationPrefix) && entries.some(entry => entry.version > baseline)).map(([app]) => app));
  requireEvidence(sameSet(mapped, declared), 'Unattributed publication transaction');
  for (const [id, receipts] of parents) {
    const parent = expected.get(id)!;
    requireEvidence(BigInt(receipts.length) === parent.fragments && receipts.every(r => r.ordinal < parent.fragments),
      'Missing or out-of-range fragment ordinals');
    requireEvidence(sum(receipts, r => r.rows) === parent.rows && sum(receipts, r => r.canonical) === parent.canonical,
      'Parent row or canonical-byte mismatch');
  }
  let physicalBytes = 0n;
  for (const [version, files] of adds) {
    if (version <= baseline) continue;
    const receipts = commits.get(version) ?? [], union = new Set<string>();
    for (const receipt of receipts) for (const file of receipt.files) union.add(file);
    requireEvidence(sameSet(union, new Set(files.keys())), 'Unattributed or missing data-changing Delta files');
    requireEvidence(sum(receipts, r => r.rows) === sum(files.values(), f => f.rows), 'Delta version row total differs from physical input receipts');
    physicalBytes += sum(files.values(), f => f.bytes);
  }
  const count = (duration + 59_999n) / 60_000n;
  requireEvidence(count <= 10_080n, 'Measurement interval exceeds seven days');
  const minutes = Array.from({ length: Number(count) }, (_, i) => ({
    minute: i + 1, duration_ms: i + 1 === Number(count) ? duration - BigInt(i) * 60_000n : 60_000n, ...totals(),
  }));
  const excluded: Record<string, ReturnType<typeof totals>> = {};
  const timedMinutes = minutes.map(minute => ({ ...minute }));
  const timedExcluded: Record<string, ReturnType<typeof totals>> = {};
  for (const receipts of commits.values()) {
    // One atomic commit gets one observation time, even with multiple receipt lines.
    const observed = receipts.reduce((latest, item) => item.observed > latest ? item.observed : latest, receipts[0].observed);
    const window = observed < start ? 'before_window' : observed < end ? 'in_window' : 'after_window';
    const observedStartNs = receipts.reduce((latest, item) => item.observedStartNs > latest ? item.observedStartNs : latest, receipts[0].observedStartNs);
    const observedEndNs = receipts.reduce((latest, item) => item.observedEndNs > latest ? item.observedEndNs : latest, receipts[0].observedEndNs);
    for (const receipt of receipts) {
      const parent = expected.get(receipt.logical)!, send = parent.send;
      const bucket = send === 'measurement' && window === 'in_window' ? minutes[Number((observed - start) / 60_000n)] :
        (excluded[`${send}:${window}`] ??= totals());
      bucket.rows += receipt.rows;
      bucket.canonical_bytes += receipt.canonical;
      if (timing) {
        const client = timing.clients[parent.client], collector = timing.collector;
        const first = observedStartNs - collector.upperNs + client.lowerNs;
        const lastExclusive = observedEndNs - collector.lowerNs + client.upperNs;
        const startNs = start * 1_000_000n, endNs = end * 1_000_000n;
        const firstMinute = first >= startNs ? (first - startNs) / 60_000_000_000n : -1n;
        const lastMinute = lastExclusive > startNs ? (lastExclusive - 1n - startNs) / 60_000_000_000n : -1n;
        const sendInside = parent.firstSend >= startNs && parent.lastSend < endNs;
        let destination: ReturnType<typeof totals>;
        if (sendInside && first >= startNs && lastExclusive <= endNs && firstMinute === lastMinute)
          destination = timedMinutes[Number(firstMinute)];
        else {
          const reason = send !== 'measurement' ? `${send}:excluded` :
            first < endNs && lastExclusive > startNs ? 'boundary_uncertain' :
            lastExclusive <= startNs ? 'before_window' : 'after_window';
          destination = timedExcluded[reason] ??= totals();
        }
        destination.rows += receipt.rows;
        destination.canonical_bytes += receipt.canonical;
      }
    }
  }
  const rows = sum(minutes, m => m.rows), canonical = sum(minutes, m => m.canonical_bytes);
  return {
    status: 'INPUT_TO_DELTA_RECONCILED', qualification: NOT_QUALIFIED,
    limits: [
      'Coalesced efb receipt schema only; complete JSON history and exact shared client bounds required.',
      'Timestamps are post-commit receipt observations, not storage commit times.',
      'Mapping ingestion stamps match physical admission; Delta txn.lastUpdated is separate metadata.',
      'No independent clock, object-existence, pod-count, backlog, maintenance or source/image proof.',
      'Physical file totals include warmup and drain; they are not scored logical-byte throughput.',
    ],
    measurement_start_unix_ms: start, measurement_end_unix_ms: end, clients: manifests.length, logical_parents: expected.size,
    physical_publications: physical.size, new_commits: commits.size, scored_rows: rows, scored_canonical_bytes: canonical,
    rows_per_second: rate(rows, duration), canonical_bytes_per_second: rate(canonical, duration),
    minutes: minutes.map(m => ({ ...m, rows_per_second: rate(m.rows, m.duration_ms), canonical_bytes_per_second: rate(m.canonical_bytes, m.duration_ms) })),
    excluded, all_run_physical_bytes: physicalBytes,
    ...(options?.diagnostic ? { diagnostic_clock_inconsistencies: diagnosticClockInconsistencies,
      timed_scoring: timing ? {
        scored_rows: sum(timedMinutes, m => m.rows), scored_canonical_bytes: sum(timedMinutes, m => m.canonical_bytes),
        rows_per_second: rate(sum(timedMinutes, m => m.rows), duration),
        canonical_bytes_per_second: rate(sum(timedMinutes, m => m.canonical_bytes), duration),
        minutes: timedMinutes.map(m => ({ ...m, rows_per_second: rate(m.rows, m.duration_ms),
          canonical_bytes_per_second: rate(m.canonical_bytes, m.duration_ms) })),
        excluded: timedExcluded,
        limit: 'Clock intervals cover sampled endpoints only; continuous clock behavior and throughput qualification are not established.',
      } : null } : {}),
  };
}
