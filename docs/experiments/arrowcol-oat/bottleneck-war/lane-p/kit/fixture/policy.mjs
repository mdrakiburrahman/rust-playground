// Offline pre-parser and evidence admission rules shared by the Lane P kit and the generated runner.
import path from 'node:path';
import { isDeepStrictEqual as equal } from 'node:util';

export const BASE_SHA = '3259d8cd0fff5eeac77d77308898721cf696f7bd';
export const PREPARSER_NOTICE = 'Offline pre-parser only: it mirrors arrow-deltalake writer.rs and delta_stats.rs at ' +
  BASE_SHA + ' (crate unchanged since cb33bfa1) and adds stricter "kit rule" checks. The Rust parser, the OAT validator and the runtime readback and ' +
  'footer audit remain authoritative; passing here does not prove the collector accepts a plan.';
export const DRIFT_BOUND = 0.05;
export const HARNESS_FILES = ['score.ts', 'measure.ts', 'audit.mjs', 'policy.mjs', 'base.yaml'];
export const BINARIES = ['arrowcol-dataflow-rust', 'arrowcol-dmv', 'arrowcol-catalog-rust', 'arrowcol-loadgen'];
export const IDENTITY_FIELDS = ['sha', 'dirty', 'host', 'binaries', 'maintenance_sha256', 'build_provenance',
  'config_sha256', 'harness_sha256', 'harness_files', 'footer_sha256', 'node_version', 'allocator_env', 'storage'];
export const CODECS = ['uncompressed', 'snappy', 'gzip', 'brotli', 'lz4', 'lz4_raw', 'zstd'];
export const ENCODINGS = {
  plain: null, rle: ['BOOLEAN'], delta_binary_packed: ['INT32', 'INT64'], delta_length_byte_array: ['BYTE_ARRAY'],
  delta_byte_array: ['BYTE_ARRAY', 'FIXED_LEN_BYTE_ARRAY'],
  byte_stream_split: ['INT32', 'INT64', 'FLOAT', 'DOUBLE', 'FIXED_LEN_BYTE_ARRAY'],
};
export const STATISTICS = ['none', 'chunk', 'page'];
export const COLUMN_FIELDS = ['compression', 'compression_level', 'encoding', 'dictionary_enabled',
  'dictionary_page_size_limit', 'data_page_size_limit', 'statistics', 'write_page_header_statistics',
  'bloom_filter_enabled', 'bloom_filter_fpp', 'bloom_filter_ndv', 'data_page_v2_compression_ratio_threshold'];
export const WRITER_FIELDS = ['defaults', 'columns', 'writer_version', 'max_row_group_rows', 'max_row_group_bytes',
  'data_page_row_count_limit', 'write_batch_size', 'offset_index_enabled', 'column_index_truncate_length',
  'statistics_truncate_length', 'sorting_columns'];
const LEVELS = { gzip: [0, 9], brotli: [0, 11], zstd: [1, 22] };
const POSITIVE = ['max_row_group_rows', 'max_row_group_bytes', 'data_page_row_count_limit', 'write_batch_size',
  'column_index_truncate_length', 'statistics_truncate_length'];
const DICTIONARY_TOKENS = ['rle_dictionary', 'plain_dictionary', 'dictionary'];
const PLAN_FIELDS = ['duration', 'warmup', 'common', 'cells'];
const CELL_FIELDS = ['id', 'grid', 'x', 'y', 'env'];

const fail = msg => { throw new Error(msg); };
const check = (ok, msg) => { if (!ok) fail(msg); };
const isObject = v => v !== null && typeof v === 'object' && !Array.isArray(v);
const hex64 = v => typeof v === 'string' && /^[0-9a-f]{64}$/.test(v);
const usize = v => Number.isSafeInteger(v) && v > 0;
const finite = v => typeof v === 'number' && Number.isFinite(v);
const kit = msg => 'kit rule: ' + msg;

function fields(value, allowed, where) {
  for (const key of Object.keys(value)) {
    check(allowed.includes(key), `${where}: unknown field ${key}`);
    check(value[key] !== null, `${where}.${key}: ` + kit('null is rejected (Rust treats null as inherit; omit the field)'));
  }
}

function json(text, name) {
  check(typeof text === 'string', `${name}: expected a JSON string`);
  try { return JSON.parse(text); } catch (error) { return fail(`${name}: invalid JSON (${error.message})`); }
}

function columnOptions(o, where) {
  check(isObject(o), `${where}: expected an object`);
  fields(o, COLUMN_FIELDS, where);
  const has = key => Object.hasOwn(o, key);
  if (has('compression')) check(CODECS.includes(o.compression), `${where}.compression: expected one of ${CODECS.join(', ')}`);
  if (has('compression_level')) {
    check(Number.isInteger(o.compression_level) && Math.abs(o.compression_level) < 2 ** 31, `${where}.compression_level: expected an i32 integer`);
    check(has('compression'), `${where}: compression_level requires compression`);
    const range = LEVELS[o.compression];
    check(range, `${where}: ${o.compression} does not accept a compression level`);
    check(o.compression_level >= range[0] && o.compression_level <= range[1],
      `${where}: ${o.compression} compression level must be ${range[0]}..=${range[1]}`);
  }
  if (has('encoding')) {
    check(!DICTIONARY_TOKENS.includes(o.encoding),
      `${where}.encoding: ${o.encoding} is not a fallback encoder; use dictionary_enabled (upstream set_encoding panics on dictionary encodings)`);
    check(typeof o.encoding === 'string' && Object.hasOwn(ENCODINGS, o.encoding),
      `${where}.encoding: expected one of ${Object.keys(ENCODINGS).join(', ')}`);
  }
  for (const key of ['dictionary_enabled', 'write_page_header_statistics', 'bloom_filter_enabled']) {
    if (has(key)) check(typeof o[key] === 'boolean', `${where}.${key}: expected a JSON boolean`);
  }
  for (const key of ['dictionary_page_size_limit', 'data_page_size_limit']) {
    if (has(key)) check(usize(o[key]), `${where}.${key}: expected a positive integer`);
  }
  if (has('statistics')) check(STATISTICS.includes(o.statistics), `${where}.statistics: expected none, chunk or page`);
  if (has('bloom_filter_fpp')) check(finite(o.bloom_filter_fpp) && o.bloom_filter_fpp > 0 && o.bloom_filter_fpp < 1,
    `${where}.bloom_filter_fpp: must be in (0,1)`);
  if (has('bloom_filter_ndv')) check(usize(o.bloom_filter_ndv), `${where}.bloom_filter_ndv: expected a positive integer`);
  if (has('data_page_v2_compression_ratio_threshold')) {
    check(finite(o.data_page_v2_compression_ratio_threshold) && o.data_page_v2_compression_ratio_threshold > 0,
      `${where}.data_page_v2_compression_ratio_threshold: must be finite and positive`);
  }
  check(o.bloom_filter_enabled !== false || (!has('bloom_filter_fpp') && !has('bloom_filter_ndv')),
    `${where}: disabled Bloom filter cannot specify fpp/ndv (those setters enable it)`);
}

/** Leaves are [{path, physical}] from a reference footer. Without leaves, schema-dependent checks are skipped. */
export function validateWriterConfig(value, leaves = null) {
  const w = typeof value === 'string' ? json(value, 'ARROWCOL_PARQUET_CONFIG') : value;
  check(isObject(w), 'ARROWCOL_PARQUET_CONFIG: expected a JSON object');
  fields(w, WRITER_FIELDS, 'parquet writer');
  check(!Object.hasOwn(w, 'sorting_columns'),
    'parquet writer.sorting_columns: ' + kit('unsupported in Lane P; sort metadata must match a verified order and the screened layout is unsorted'));
  const defaults = w.defaults ?? {};
  const columns = w.columns ?? {};
  columnOptions(defaults, 'defaults');
  check(isObject(columns), 'columns: expected an object keyed by physical leaf path');
  const physical = leaves ? new Map(leaves.map(leaf => [leaf.path, leaf.physical])) : null;
  for (const [leaf, options] of Object.entries(columns)) {
    check(leaf.length > 0, 'Parquet column path cannot be empty');
    if (physical) check(physical.has(leaf), `unknown physical Parquet column ${leaf}`);
    columnOptions(options, `columns.${leaf}`);
  }
  if (Object.hasOwn(w, 'writer_version')) check(['v1', 'v2'].includes(w.writer_version), 'writer_version: expected v1 or v2 (not 1.0/2.0)');
  for (const key of POSITIVE) if (Object.hasOwn(w, key)) check(usize(w[key]), `${key}: expected a positive integer`);
  if (Object.hasOwn(w, 'offset_index_enabled')) check(typeof w.offset_index_enabled === 'boolean', 'offset_index_enabled: expected a JSON boolean');
  const scopes = physical ? [...physical.keys()] : [null, ...Object.keys(columns)];
  const effective = (leaf, key) => leaf === null ? defaults[key] : columns[leaf]?.[key] ?? defaults[key];
  for (const leaf of scopes) {
    const where = leaf ?? 'defaults';
    const encoding = effective(leaf, 'encoding');
    const type = physical?.get(leaf);
    if (encoding && type) check(ENCODINGS[encoding] === null || ENCODINGS[encoding].includes(type),
      `Parquet column ${leaf}: encoding ${encoding} is invalid for physical type ${type}`);
    if (encoding) check(effective(leaf, 'dictionary_enabled') === false, `${where}: ` +
      kit(`fallback encoding ${encoding} is unverifiable while the dictionary is enabled; set dictionary_enabled=false`));
    if (effective(leaf, 'dictionary_page_size_limit') !== undefined) check(effective(leaf, 'dictionary_enabled') !== false,
      `${where}: ` + kit('dictionary_page_size_limit is inactive when the dictionary is disabled'));
    const statistics = effective(leaf, 'statistics');
    if (w.offset_index_enabled === false) check(statistics === 'none' || statistics === 'chunk', `${where}: ` +
      kit('offset_index_enabled=false requires explicit statistics=none or chunk; page statistics force offset indexes'));
    if (effective(leaf, 'write_page_header_statistics') === true) check(statistics !== 'none' && statistics !== 'chunk',
      `${where}: ` + kit('write_page_header_statistics needs page statistics to be exercised'));
    if (effective(leaf, 'data_page_v2_compression_ratio_threshold') !== undefined) {
      const codec = effective(leaf, 'compression');
      check(w.writer_version === 'v2' && codec && codec !== 'uncompressed', `${where}: ` +
        kit('data_page_v2_compression_ratio_threshold needs explicit writer_version=v2 and a compressing codec'));
    }
  }
  return w;
}

export function validateStatsColumns(value, leaves = null) {
  const columns = typeof value === 'string' ? json(value, 'ARROWCOL_COLLECTOR_DELTA_STATS_COLUMNS') : value;
  check(Array.isArray(columns), 'ARROWCOL_COLLECTOR_DELTA_STATS_COLUMNS: expected a JSON array of column paths');
  check(columns.length <= 256, 'delta stats columns: at most 256 columns');
  check(new Set(columns).size === columns.length, 'delta stats columns: duplicate column');
  const known = leaves && new Set(leaves.map(leaf => leaf.path));
  for (const column of columns) {
    check(typeof column === 'string' && Buffer.byteLength(column) >= 1 && Buffer.byteLength(column) <= 1024,
      'delta stats column: expected a 1..1024-byte string');
    check(column.split('.').every(part => /^[A-Za-z0-9_]+$/.test(part)), `delta stats column ${column}: invalid dotted path`);
    if (known) check(known.has(column), `delta stats column ${column}: not a primitive leaf of the fixture schema`);
  }
  return columns;
}

const integer = (min, max) => value => /^(0|[1-9][0-9]{0,15})$/.test(value) && Number(value) >= min && Number(value) <= max;
const positive = integer(1, Number.MAX_SAFE_INTEGER);
const oneOf = (...values) => value => values.includes(value);
const duration = value => /^[1-9][0-9]{0,6}(ms|s|m)$/.test(value);
const cpulist = value => /^\d{1,4}(-\d{1,4})?(,\d{1,4}(-\d{1,4})?)*$/.test(value) &&
  value.split(',').every(part => { const [a, b = a] = part.split('-').map(Number); return a <= b; });
/** Every name a plan may set. Runner-internal names, BWA_ROOT and BWA_BIN are deliberately absent. */
export const ENV_RULES = {
  ARROWCOL_PARQUET_CONFIG: (v, leaves) => validateWriterConfig(v, leaves),
  ARROWCOL_COLLECTOR_DELTA_STATS_COLUMNS: (v, leaves) => validateStatsColumns(v, leaves),
  ARROWCOL_COLLECTOR_PARQUET_COLUMN_WORKERS: v => check(v === '1',
    kit('ARROWCOL_COLLECTOR_PARQUET_COLUMN_WORKERS must be 1 (Lane Q owns column parallelism; the audit reads back 1)')),
  BWA_CORES: integer(1, 128), BWA_PDATA: positive, BWA_STREAMS: positive, BWA_RSS: positive,
  BWA_BUF_BYTES: positive, BWA_BUF_ROWS: positive, BWA_BCC: positive,
  BWA_TXN: oneOf('per_id', 'batched', 'batched_cached', 'incremental'), BWA_BW: duration,
  BWA_MFC: positive, BWA_MPF: positive, BWA_GCAP_BYTES: positive, BWA_GCAP_ROWS: positive,
  BWA_TCAP_BYTES: positive, BWA_TCAP_ROWS: positive, BWA_MAMF: positive, BWA_MRPF: positive,
  BWA_MAXAGE: duration, BWA_MCF: positive, BWA_SFC: positive, BWA_TARGET: positive, BWA_PD: positive,
  BWA_ISOLATION: oneOf('l0', 'l1', 'l5'), BWA_NCLIENTS: integer(1, 16), BWA_CCPUS: cpulist, BWA_KCPUS: cpulist,
  BWA_TABLE: oneOf('sqlserver_availability_replica_states'),
  BWA_CHECKPOINT_EVERY_COMMITS: integer(1, 100000), BWA_CLIENT_WORKERS: integer(1, 256),
  BWA_CLIENT_SPW: integer(1, 256), BWA_CLIENT_THREADS: integer(1, 256), BWA_CLIENT_BATCH_ROWS: positive,
  BWA_CLIENT_CORPUS_ROWS: positive, BWA_CLIENT_STRING_WIDTH: integer(0, 1e9),
};

export function validateEnv(env, leaves = null, where = 'env') {
  check(isObject(env), `${where}: expected an object`);
  for (const [name, value] of Object.entries(env)) {
    check(Object.hasOwn(ENV_RULES, name), `${where}: unknown or forbidden environment name ${name}`);
    check(typeof value === 'string' && !/[\r\n\0]/.test(value), `${where}.${name}: expected a single-line string`);
    let ok;
    try { ok = ENV_RULES[name](value, leaves); } catch (error) { fail(`${where}.${name}: ${error.message}`); }
    check(ok !== false, `${where}.${name}: invalid value ${JSON.stringify(value)}`);
  }
  return { ...env };
}

export function validatePlan(data, leaves = null) {
  check(isObject(data), 'plan: expected an object');
  for (const key of Object.keys(data)) check(PLAN_FIELDS.includes(key), `plan: unknown field ${key}`);
  for (const key of PLAN_FIELDS) check(Object.hasOwn(data, key), `plan: missing field ${key}`);
  check(Number.isInteger(data.duration) && data.duration >= 5 && data.duration <= 1800, 'plan.duration: integer 5..1800');
  check(Number.isInteger(data.warmup) && data.warmup >= 0 && data.warmup <= 300, 'plan.warmup: integer 0..300');
  const common = validateEnv(data.common, leaves, 'plan.common');
  check(Array.isArray(data.cells) && data.cells.length > 0, 'plan.cells: expected a non-empty array');
  const ids = new Set();
  const cells = data.cells.map((cell, i) => {
    check(isObject(cell), `plan.cells[${i}]: expected an object`);
    for (const key of Object.keys(cell)) check(CELL_FIELDS.includes(key), `plan.cells[${i}]: unknown field ${key}`);
    check(typeof cell.id === 'string' && /^[a-zA-Z0-9][a-zA-Z0-9._-]{0,100}$/.test(cell.id) && !ids.has(cell.id),
      `plan.cells[${i}]: invalid or duplicate id`);
    ids.add(cell.id);
    for (const key of ['grid', 'x', 'y']) check(typeof cell[key] === 'string' && cell[key].length > 0, `plan.cells[${i}].${key}: expected a string`);
    return { id: cell.id, grid: cell.grid, x: cell.x, y: cell.y, env: validateEnv(cell.env, leaves, `plan.cells[${i}].env`) };
  });
  return { duration: data.duration, warmup: data.warmup, common, cells };
}

export function identityProblems(r) {
  const problems = [];
  const need = (ok, msg) => { if (!ok) problems.push(msg); };
  need(typeof r.sha === 'string' && /^[0-9a-f]{40}$/.test(r.sha), 'identity.sha: expected a 40-hex commit');
  need(r.dirty === '', 'identity.dirty: source tree must be clean');
  need(typeof r.host === 'string' && r.host.length > 0, 'identity.host: missing');
  need(isObject(r.binaries) && equal(Object.keys(r.binaries).sort(), [...BINARIES].sort()) &&
    BINARIES.every(name => hex64(r.binaries[name])), `identity.binaries: expected SHA-256 for exactly ${BINARIES.join(', ')}`);
  need(r.maintenance_sha256 === null || hex64(r.maintenance_sha256), 'identity.maintenance_sha256: expected null or SHA-256');
  need(isObject(r.build_provenance) && r.build_provenance.allocator === 'mimalloc' &&
    r.build_provenance.collector_catalog_maintenance_source === r.sha,
  'identity.build_provenance: expected mimalloc and collector_catalog_maintenance_source equal to sha');
  const built = r.build_provenance?.binaries;
  need(isObject(built) && BINARIES.every(name => hex64(built[name]) && built[name] === r.binaries?.[name]) &&
    (built['delta-maintenance'] ?? null) === r.maintenance_sha256,
  'identity.build_provenance.binaries: expected the recorded build hashes to equal the executed binaries');
  for (const key of ['config_sha256', 'harness_sha256', 'footer_sha256']) need(hex64(r[key]), `identity.${key}: expected SHA-256`);
  need(isObject(r.harness_files) && equal(Object.keys(r.harness_files).sort(), [...HARNESS_FILES].sort()) &&
    HARNESS_FILES.every(file => hex64(r.harness_files[file])) && r.harness_files['score.ts'] === r.harness_sha256 &&
    r.harness_files['base.yaml'] === r.config_sha256, `identity.harness_files: expected SHA-256 for exactly ${HARNESS_FILES.join(', ')}`);
  need(typeof r.node_version === 'string' && /^v\d+\.\d+\.\d+$/.test(r.node_version), 'identity.node_version: missing');
  need(isObject(r.allocator_env) && equal(Object.keys(r.allocator_env).sort(), ['GLIBC_TUNABLES', 'LD_PRELOAD']) &&
    Object.values(r.allocator_env).every(v => v === null || typeof v === 'string'), 'identity.allocator_env: malformed');
  need(r.storage === 'local_filesystem', 'identity.storage: expected local_filesystem');
  return problems;
}

export const identity = r => Object.fromEntries(IDENTITY_FIELDS.map(key => [key, r[key]]));
export const corpus = r => Array.isArray(r.client) ? r.client.map(c => c?.preparation?.payload_corpus_sha256) : null;
export const isolationOf = (plan, cell) => cell.env.BWA_ISOLATION ?? plan.common.BWA_ISOLATION ?? 'l5';
const close = (a, b) => finite(a) && finite(b) && Math.abs(a - b) <= 1e-9 * Math.max(1, Math.abs(b));

/** Problems that make one runner row inadmissible as evidence for `cell` of the plan whose bytes hash to planSha. */
export function rowProblems(r, { plan, cell, planSha, footerSha = null, synthetic = false }) {
  if (!isObject(r)) return ['row: expected an object'];
  const problems = identityProblems(r);
  const need = (ok, msg) => { if (!ok) problems.push(`${r.cell ?? '?'}: ${msg}`); };
  need((r.synthetic_non_performance === true) === synthetic,
    synthetic ? 'synthetic test mode admits only synthetic_non_performance rows' : 'synthetic_non_performance rows are not evidence');
  need(r.plan_sha256 === planSha, 'plan_sha256 differs from the plan being admitted');
  need(r.cell === cell.id && r.grid === cell.grid && r.x === cell.x && r.y === cell.y, `expected cell ${cell.id}`);
  need(equal(r.knobs, { ...plan.common, ...cell.env }), 'knobs differ from the planned configuration');
  need(r.duration === plan.duration && r.warmup === plan.warmup, 'duration/warmup differ from the plan');
  if (footerSha !== null) need(r.footer_sha256 === footerSha, 'footer_sha256 differs from the registered footer binary');
  need(r.status === 'ok', `status ${r.status}`);
  const isolation = isolationOf(plan, cell);
  need(r.isolation_level === isolation, `isolation ${r.isolation_level}, expected ${isolation}`);
  need(r.retry_offered_rps === 0, 'retries were offered');
  const s = r.stats;
  need(isObject(s) && finite(s.seconds) && Math.abs(s.seconds - plan.duration) <= Math.max(1, 0.01 * plan.duration),
    'measured window does not match the planned duration');
  need(isObject(s) && finite(s.start_observed_ms) && finite(s.end_observed_ms) && s.end_observed_ms > s.start_observed_ms,
    'missing observed window bounds');
  need(Array.isArray(r.client) && r.client.length > 0 && corpus(r).every(hex64), 'missing client payload corpus hashes');
  if (isolation !== 'l5') {
    need(isObject(s) && s.rows === 0 && s.commits === 0, 'diagnostic arm committed rows');
    need(finite(r.diagnostic_rps) && r.diagnostic_rps > 0 && finite(r.first_attempt_offered_rps) && r.first_attempt_offered_rps > 0,
      'diagnostic rates missing');
    return problems;
  }
  need(isObject(s) && usize(s.rows) && usize(s.commits) && usize(s.files) && usize(s.bytes), 'empty or malformed Delta window');
  need(finite(r.committed_rps) && r.committed_rps > 0 && close(r.committed_rps, s?.committed_rps) &&
    close(s?.committed_rps, s?.rows / s?.seconds), 'committed_rps is not finite, positive and consistent with stats');
  need(finite(r.bytes_per_row) && r.bytes_per_row > 0 && close(r.bytes_per_row, s?.bytes / s?.rows), 'bytes_per_row inconsistent');
  need(finite(s?.files_per_commit) && s.files_per_commit > 0, 'files_per_commit missing');
  const p = r.physical;
  need(isObject(p) && p.rows === s?.rows && p.bytes === s?.bytes && p.files === s?.files && hex64(p.metadata_sha256),
    'physical footer reconciliation missing or different');
  need(isObject(p) && ['user_ns_per_row', 'system_ns_per_row'].every(k => finite(p[k]) && p[k] >= 0), 'CPU ns/row missing');
  need(isObject(r.collector) && ['user_cores', 'system_cores'].every(k => finite(r.collector[k]) && r.collector[k] >= 0),
    'collector CPU missing');
  return problems;
}

export const bookendDrift = (first, last) => last.committed_rps / first.committed_rps - 1;

/** A runner may resume only a clean, in-order prefix of the same plan written by the same identity. */
export function resumeProblems(prior, plan, current, output) {
  const problems = [];
  if (prior.length > plan.cells.length) problems.push(`${prior.length} rows exceed ${plan.cells.length} planned cells`);
  const runs = new Set();
  prior.forEach((r, i) => {
    const cell = plan.cells[i];
    const at = `row ${i + 1} (${r?.cell})`;
    if (!cell || r?.cell !== cell.id) problems.push(`${at}: expected ${cell?.id ?? 'no row'}; reordered, replayed or different plan`);
    if (r?.status !== 'ok') problems.push(`${at}: status ${r?.status}; failed output needs a fresh directory`);
    if (r?.plan_sha256 !== current.plan_sha256) problems.push(`${at}: written for a different plan`);
    if (cell && !equal(r?.knobs, { ...plan.common, ...cell.env })) problems.push(`${at}: knobs differ`);
    for (const key of IDENTITY_FIELDS) if (!equal(r?.[key], current[key])) problems.push(`${at}: identity ${key} differs`);
    const run = typeof r?.run === 'string' ? path.resolve(r.run) : '';
    if (path.dirname(run) !== path.resolve(output) || runs.has(run)) problems.push(`${at}: run directory is not unique under ${output}`);
    runs.add(run);
  });
  return problems;
}
