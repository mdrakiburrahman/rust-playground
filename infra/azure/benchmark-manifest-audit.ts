import { evidenceList, evidenceObject, evidenceText, requireEvidence, uint } from './benchmark-evidence-json.js';

export const MAX_MANIFEST_BYTES = 1_048_576;
export const MAX_RECORDS = 1_100_000n;
export const ENCODING = 'dictionary_bitpack_base64url_v1';
export const NOT_QUALIFIED = 'NOT_ESTABLISHED_BY_THIS_AUDIT';
const small = (value: unknown, label: string) => Number(uint(value, label, MAX_RECORDS));

export function* decodeFacts(value: unknown, count: number) {
  const encoded = evidenceObject(value);
  requireEvidence(encoded.encoding === ENCODING && encoded.bit_order === 'least_significant_index_bits_first', 'Unsupported fact encoding or bit order');
  requireEvidence(small(encoded.index_count, 'index count') === count, 'Index count mismatch');
  const dictionary = evidenceList(encoded.dictionary);
  requireEvidence(dictionary.length > 0 && dictionary.length <= count, 'Invalid fact dictionary');
  const width = Number(uint(encoded.index_bits, 'index width', 32n));
  requireEvidence(width === 32 - Math.clz32(dictionary.length - 1), 'Noncanonical index width');
  const keys = new Set<string>();
  const facts = dictionary.map(value => {
    const item = evidenceObject(value);
    const fragments = uint(item.decoded_physical_batch_count, 'physical fragment count');
    const canonical = uint(item.canonical_logical_bytes, 'canonical logical bytes');
    requireEvidence(fragments > 0n && item.canonical_logical_bytes_unavailable_reason === null, 'Empty or unavailable fact');
    const key = `${fragments}/${canonical}`;
    requireEvidence(!keys.has(key), 'Duplicate dictionary fact');
    keys.add(key);
    return { fragments, canonical };
  });
  const text = evidenceText(encoded.indices_base64url);
  const packed = Buffer.from(text, 'base64url');
  requireEvidence(packed.toString('base64url') === text, 'Noncanonical base64url');
  const bits = width * count;
  requireEvidence(packed.length === Math.ceil(bits / 8), 'Packed index length mismatch');
  requireEvidence(bits % 8 === 0 || packed[packed.length - 1] >> (bits % 8) === 0, 'Nonzero packed padding');
  let seen = 0;
  for (let position = 0; position < count; position++) {
    const offset = position * width, begin = Math.floor(offset / 8);
    let word = 0;
    for (let byte = begin; byte < Math.ceil((offset + width) / 8); byte++) {
      word |= packed[byte] << (8 * (byte - begin));
    }
    const index = (word >>> (offset % 8)) & ((1 << width) - 1);
    requireEvidence(index < facts.length && index <= seen, 'Fact index out of bounds or not in first-observation order');
    if (index === seen) seen++;
    yield facts[index];
  }
  requireEvidence(seen === facts.length, 'Unused dictionary entry');
}

function cohort(timestamp: bigint, start: bigint, end: bigint) {
  return timestamp < start ? 'warmup' : timestamp < end ? 'measurement' : 'post_window';
}

export function manifestBounds(value: unknown) {
  const bounds = evidenceObject(evidenceObject(value).measurement_bounds);
  const start = uint(bounds.start_unix_ms_inclusive, 'measurement start');
  const end = uint(bounds.end_unix_ms_exclusive, 'measurement end');
  requireEvidence(end > start, 'Nonpositive measurement interval');
  return { start, end };
}

export function* decodedRecords(value: unknown) {
  const manifest = evidenceObject(value);
  requireEvidence(manifest.schema === 'arrowcol.logical_batch_manifest.v1' && manifest.version === 1n, 'Unsupported manifest schema or version');
  requireEvidence(manifest.identity_status === 'identity_bearing' && manifest.eligible_for_identity_reconciliation === true &&
    manifest.complete === true && manifest.truncated === false && manifest.record_capacity_reached === false &&
    !manifest.terminal_failure && manifest.batch_facts_encoding === ENCODING, 'Manifest is unresolved, incomplete or unsupported');
  const run = evidenceText(manifest.run_uuid), incarnation = evidenceText(manifest.incarnation_uuid);
  requireEvidence(/^[0-9a-f]{32}$/.test(run) && /^[0-9a-f]{32}$/.test(incarnation), 'Invalid run or incarnation UUID');
  const { start, end } = manifestBounds(manifest);
  const total = small(manifest.record_count, 'record count');
  requireEvidence(total > 0 && small(manifest.record_cap, 'record cap') >= total, 'Empty or capped manifest');
  for (const field of ['prepared_logical_batches', 'attempted_logical_batches', 'acknowledged_logical_batches']) {
    requireEvidence(small(manifest[field], field) === total, `${field} mismatch`);
  }
  for (const field of ['not_admitted_logical_batches', 'ambiguous_logical_batches', 'failed_logical_batches',
    'pending_logical_batches', 'retry_attempts']) {
    requireEvidence(uint(manifest[field], field) === 0n, `Scored manifest has ${field}`);
  }
  const ranges = evidenceList(manifest.ranges).map(evidenceObject);
  requireEvidence(ranges.length > 0 && ranges.length <= total, 'Invalid ranges');
  const intervals = new Map<bigint, Array<{ first: bigint; last: bigint }>>();
  let countSum = 0;
  for (const item of ranges) {
    const worker = uint(item.worker, 'worker', 65535n);
    const first = uint(item.first_sequence, 'first sequence'), last = uint(item.last_sequence, 'last sequence');
    const count = small(item.batch_count, 'batch count');
    requireEvidence(first <= last && BigInt(count) === last - first + 1n, 'Sequence interval length mismatch');
    countSum += count;
    const spans = intervals.get(worker) ?? [];
    spans.push({ first, last });
    intervals.set(worker, spans);
  }
  requireEvidence(countSum === total, 'Range total differs from record count');
  for (const spans of intervals.values()) {
    spans.sort((a, b) => a.first < b.first ? -1 : a.first > b.first ? 1 : 0);
    requireEvidence(spans.every((span, i) => i === 0 || spans[i - 1].last < span.first), 'Overlapping logical identities');
  }
  for (const item of ranges) {
    const table = evidenceText(item.table), rows = uint(item.rows_per_batch, 'rows per batch', 4294967295n);
    requireEvidence(table && rows > 0n && item.attempts === 1n && item.outcome === 'acknowledged', 'Parent is empty or not single-attempt acknowledged');
    const firstSend = uint(item.first_send_unix_ns, 'first send'), lastSend = uint(item.last_send_unix_ns, 'last send');
    const firstAck = uint(item.first_admitted_unix_ms, 'first admission'), lastAck = uint(item.last_admitted_unix_ms, 'last admission');
    requireEvidence(firstSend <= lastSend && firstAck <= lastAck && firstSend / 1_000_000n <= firstAck &&
      lastSend / 1_000_000n <= lastAck, 'Reversed timestamps or admission precedes send');
    requireEvidence(firstAck === item.first_outcome_unix_ms && lastAck === item.last_outcome_unix_ms, 'Single-attempt ACK/outcome bounds differ');
    const send = cohort(firstSend / 1_000_000n, start, end), admission = cohort(firstAck, start, end);
    requireEvidence(send === item.cohort && send === cohort(lastSend / 1_000_000n, start, end) &&
      admission === item.first_admitted_cohort && admission === cohort(lastAck, start, end), 'Send or admission cohort/bounds mismatch');
    let fragmentSum = 0n, byteSum = 0n, sequence = uint(item.first_sequence, 'first sequence');
    for (const { fragments, canonical } of decodeFacts(item.batch_facts, small(item.batch_count, 'batch count'))) {
      fragmentSum += fragments;
      byteSum += canonical;
      yield { id: `${run}/${incarnation}/${item.worker}/${(sequence++).toString(36)}`, table, rows, fragments, canonical,
        send, admission, firstSend, lastSend };
    }
    requireEvidence(fragmentSum === item.decoded_physical_batch_count && byteSum === item.canonical_logical_bytes &&
      item.canonical_logical_bytes_unavailable_reason === null, 'Range fact totals differ or unavailable');
  }
}

export function auditManifest(value: unknown) {
  const tables = new Set<string>();
  const cohorts: Record<string, { parents: number; rows: bigint; physical_batches: bigint; canonical_bytes: bigint }> = {};
  for (const record of decodedRecords(value)) {
    tables.add(record.table);
    const bucket = cohorts[`${record.send}->${record.admission}`] ??= { parents: 0, rows: 0n, physical_batches: 0n, canonical_bytes: 0n };
    bucket.parents++;
    bucket.rows += record.rows;
    bucket.physical_batches += record.fragments;
    bucket.canonical_bytes += record.canonical;
  }
  requireEvidence(tables.size === 1, 'Multiple tables in client manifest');
  const manifest = evidenceObject(value), { start, end } = manifestBounds(value);
  return { status: 'CLIENT_MANIFEST_VALID', qualification: NOT_QUALIFIED,
    scope: 'Exact client identities/facts/cohorts only; no collector, Delta, NIC, latency or backlog proof',
    run_uuid: manifest.run_uuid, incarnation_uuid: manifest.incarnation_uuid, duration_ms: end - start,
    parents: manifest.record_count, ranges: evidenceList(manifest.ranges).length, cohorts };
}
