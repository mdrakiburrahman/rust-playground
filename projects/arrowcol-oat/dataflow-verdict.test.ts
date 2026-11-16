import assert from 'node:assert/strict';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { join } from 'node:path';
import { tmpdir } from 'node:os';
import { pathToFileURL } from 'node:url';
import test from 'node:test';
import { tableFromArrays, tableToIPC } from 'apache-arrow';
import { Table as WasmTable, writeParquet } from 'parquet-wasm';
import type { run_tree } from '../../infra/azure/bootstrap.js';
import {
  CLEANUP_TIMEOUT_MS, MAX_RESULT_BYTES, RESULT_TIMEOUT_MS, collectDataFlow, dataFlowExitCode,
  decodeResultRows, evaluateDataFlow, fetchResultBytes, formatDataFlow, readResultBytes, reportDataFlowRun, verifyRunCleanup,
} from './dataflow-verdict.js';
import type { OatRunStatus } from './launch.js';

const orchestration = '11111111-1111-4111-8111-111111111111';
const cell = '22222222-2222-4222-8222-222222222222';
const sha = 'a'.repeat(40);
const subscriptionId = '00000000-0000-0000-0000-000000000001';
const config = {
  stamp: '00000', namespace: 'arrow-collector', kubectl_context: 'arrow-collector-00000', subscriptionId,
  qap_database: 'experimentqap', tables: ['sqlserver_active_sessions'], replica_counts: [1], dfe_worker_concurrency: [8],
};
const status: OatRunStatus = { id: orchestration, state: 'succeeded', progress: 'published', result_count: 1, error: null };
function row(overrides: Record<string, unknown> = {}) {
  return {
    schema_version: 2, run_id: cell, mode: 'aks', stamp: '00000',
    tunables_json: JSON.stringify({ orchestration_run_id: orchestration }),
    git_sha: sha, status: 'succeeded', pinning_ok: true, table_owner: 'owner-a',
    rows: 100n, duration_ms: 1000n, rows_per_second: 100, replicas: 1n, dfe_worker_concurrency: 8n,
    table: 'sqlserver_active_sessions', tables: 1, qap_observation_complete: 1n, qap_observed_tables: 1n,
    qap_completion_percent: 100, qap_pending_artifacts: 0n, qap_unhealthy_catalogs: 0n, ...overrides,
  };
}
const ok = (stdout: string) => ({ returncode: 0, stdout, stderr: '' });

function outputEvidence() {
  return {
    version: 1, downstream_status: 'observed_or_not_attempted',
    commit_window_output_throughput: {
      scope: 'new_ingestion_commits_finished_in_half_open_shared_window_not_client_cohort',
      clock: 'collector_system_unix_ms_shared_schedule_clock_skew_not_measured',
      warmup_backlog: 'included_when_commit_finishes_in_window',
      rewrite_commits: 'excluded_by_ingestion_only_producer_capability',
      unique_delivered_rows: null, complete_delivery_established: false,
      measurement_start_unix_ms: 1000, measurement_end_unix_ms: 2000, window_seconds: 1,
      rows: 10, files: 1, input_bytes: 100, parquet_bytes: 50, commits: 1, rows_per_second: 10,
      bytes_per_row: 5 as number | null, compression_ratio: 2 as number | null,
      compression_ratio_numerator: 'collector_reported_input_bytes',
      compression_ratio_denominator: 'committed_parquet_file_bytes_including_metadata',
      commit_latency_seconds: {
        p50: 0.1 as number | null, p95: 0.2 as number | null, p99: 0.3 as number | null,
        samples: 1, scope: 'commit_retry_and_sync_not_encoding_or_admission',
      },
    },
    processes: [{
      component: 'collector', image: 'dataflow:source-pinned', image_id: `sha256:${'a'.repeat(64)}`,
      source_revision: sha, log_complete: true, effective_config_source: 'consumer_startup_readback',
      effective_config: { commit_telemetry: { schema_version: 1, scope: 'new_ingestion_addfiles_after_sync', clock: 'system_unix_ms' } },
    }],
  };
}

function resultParquet(mode: 'local' | 'aks', revision = sha): Uint8Array {
  const table = tableFromArrays({
    schema_version: [2], run_id: [cell], mode: [mode], stamp: ['00000'],
    tunables_json: [JSON.stringify({ orchestration_run_id: orchestration })], git_sha: [revision],
    status: ['succeeded'], pinning_ok: [true], table_owner: ['owner-a'], rows: [100n],
    duration_ms: [1000n], rows_per_second: [100], replicas: [1n], dfe_worker_concurrency: [8n],
    table: ['sqlserver_active_sessions'], tables: [1], qap_observation_complete: [1n],
    qap_observed_tables: [1n], qap_completion_percent: [100], qap_pending_artifacts: [0n],
    qap_unhealthy_catalogs: [0n],
  });
  return writeParquet(WasmTable.fromIPCStream(tableToIPC(table, 'stream')));
}

test('only the launched orchestration, exact source revision and full matrix can pass', () => {
  const result = evaluateDataFlow(status, [row(), row({ tunables_json: '{"orchestration_run_id":"old"}' })], sha, 'aks', config);
  assert.equal(result.pass, true);
  assert.equal(dataFlowExitCode(result), 0);
  assert.match(formatDataFlow(result), /DATA FLOW: PASS/);
  assert.match(formatDataFlow(result), /experimentqap; observation only, NOT ADX row-count proof/);
  for (const [rows, expectedSha, reason] of [
    [[row({ tunables_json: '{"orchestration_run_id":"old"}' })], sha, /no Parquet rows match/],
    [[row({ git_sha: 'b'.repeat(40) })], sha, /git_sha differs/],
    [[row()], undefined, /expected server revision unavailable/],
    [[row(), row()], sha, /duplicate matrix/],
    [[row({ table: 'unknown' })], sha, /unexpected/],
  ] as const) {
    const invalid = evaluateDataFlow(status, [...rows], expectedSha, 'aks', config);
    assert.equal(invalid.pass, false);
    assert.match(invalid.reasons.join('; '), reason);
    assert.equal(dataFlowExitCode(invalid), 1);
  }
});

test('failed state, empty output, bad throughput and pinning fail with concrete reasons', () => {
  const failed = evaluateDataFlow({ ...status, state: 'failed', error: 'client crashed' }, [], sha, 'local', config);
  assert.equal(failed.pass, false);
  assert.match(formatDataFlow(failed), /client crashed/);
  for (const [overrides, reason] of [
    [{ rows: 0n }, /zero or invalid measured rows/],
    [{ duration_ms: 0n }, /duration/],
    [{ rows_per_second: Number.NaN }, /rows per second/],
    [{ pinning_ok: false }, /pinning mismatch/],
    [{ table_owner: null }, /missing owner/],
    [{ status: 'pinning_violation' }, /result status/],
    [{ stamp: '99999' }, /stamp mismatch/],
    [{ run_id: 'invalid' }, /invalid cell run_id/],
  ] as const) {
    const invalid = evaluateDataFlow(status, [row(overrides)], sha, 'aks', config);
    assert.equal(invalid.pass, false);
    assert.match(invalid.reasons.join('; '), reason);
  }
  assert.equal(evaluateDataFlow({ ...status, result_count: 0 }, [], sha, 'aks', config).pass, false);
});

test('AKS QAP requires every observation health condition; local omits it', () => {
  for (const override of [
    { qap_observation_complete: 0n }, { qap_observed_tables: 0n }, { qap_completion_percent: 99 },
    { qap_pending_artifacts: 1n }, { qap_unhealthy_catalogs: null },
  ]) {
    const invalid = evaluateDataFlow(status, [row(override)], sha, 'aks', config);
    assert.equal(invalid.pass, false);
    assert.match(invalid.reasons.join('; '), /QAP observation incomplete/);
  }
  assert.equal(evaluateDataFlow(status, [row({ mode: 'local', qap_completion_percent: null })], sha, 'local', config).pass, true);
});

test('network summary retains the explicit accepted-risk warning and preflight warnings', () => {
  const result = evaluateDataFlow(status, [row()], sha, 'aks', config);
  assert.match(formatDataFlow(result, { provider: 'cilium', enforcing: true }), /network: cilium \(enforced\)/);
  assert.match(formatDataFlow(result, { provider: 'none', enforcing: false }, ['no policy engine']), /network: NOT ENFORCED.*passwordless port; no public ingress/);
  assert.match(formatDataFlow(result, { provider: 'none', enforcing: false }, ['no policy engine']), /WARNING: no policy engine/);
  assert.doesNotMatch(formatDataFlow(result), /network:/);
});

test('v3 preserves local degraded evidence without relaxing AKS or pinning gates', () => {
  const degraded = {
    schema_version: 3, mode: 'local', measure_seconds: 1, qap_completion_percent: null,
    evidence_json: JSON.stringify({ ...outputEvidence(), downstream_status: 'unavailable' }),
  };
  const local = evaluateDataFlow(status, [row(degraded)], sha, 'local', config);
  assert.equal(local.pass, true);
  assert.match(formatDataFlow(local), /collector-only evidence/);
  assert.equal(evaluateDataFlow(status, [row({ ...degraded, pinning_ok: false })], sha, 'local', config).pass, false);
  assert.equal(evaluateDataFlow(status, [row({ ...degraded, mode: 'aks' })], sha, 'aks', config).pass, false);
  assert.equal(evaluateDataFlow(status, [row({ ...degraded, evidence_json: null })], sha, 'local', config).pass, false);
});

test('v3 requires evidenced collector output, but known zero commits remain valid without delivery claims', () => {
  const evaluate = (evidence: unknown) => evaluateDataFlow(status,
    [row({ schema_version: 3, measure_seconds: 1, evidence_json: JSON.stringify(evidence) })], sha, 'aks', config);
  assert.equal(evaluate(outputEvidence()).pass, true);
  const zero = outputEvidence();
  Object.assign(zero.commit_window_output_throughput, {
    rows: 0, files: 0, input_bytes: 0, parquet_bytes: 0, commits: 0, rows_per_second: 0,
    bytes_per_row: null, compression_ratio: null,
    commit_latency_seconds: { p50: null, p95: null, p99: null, samples: 0, scope: 'commit_retry_and_sync_not_encoding_or_admission' },
  });
  assert.equal(evaluate(zero).pass, true);
  assert.match(formatDataFlow(evaluate(zero)), /known-zero.*not a delivery\/drain proof/);
  for (const change of [
    (value: ReturnType<typeof outputEvidence>) => { value.processes = []; },
    (value: ReturnType<typeof outputEvidence>) => { value.processes[0].log_complete = false; },
    (value: ReturnType<typeof outputEvidence>) => { value.processes[0].source_revision = 'b'.repeat(40); },
    (value: ReturnType<typeof outputEvidence>) => { value.processes[0].effective_config.commit_telemetry.scope = 'all_rewrites'; },
    (value: ReturnType<typeof outputEvidence>) => { value.commit_window_output_throughput.rows_per_second = 100; },
    (value: ReturnType<typeof outputEvidence>) => { value.commit_window_output_throughput.measurement_end_unix_ms = 2001; },
    (value: ReturnType<typeof outputEvidence>) => { value.commit_window_output_throughput.compression_ratio = 10; },
    (value: ReturnType<typeof outputEvidence>) => { value.commit_window_output_throughput.commit_latency_seconds.p95 = 0.01; },
  ]) {
    const invalid = outputEvidence(); change(invalid);
    assert.equal(evaluate(invalid).pass, false);
  }
  const unavailable = { ...outputEvidence(), commit_window_output_throughput: {
    rows: null, rows_per_second: null, unavailable_reason: 'collector_commit_log_incomplete',
  } };
  assert.match(evaluate(unavailable).reasons.join(), /output telemetry unavailable/);
  assert.equal(evaluate({ version: 1 }).pass, false);
});

test('run reporting returns failure exit status for a runner exception and success only after evidence passes', async (t) => {
  const output: string[] = [];
  t.mock.method(console, 'log', (message: string) => output.push(message));
  const noCommands: typeof run_tree = async () => { throw new Error('unexpected command'); };
  assert.equal(await reportDataFlowRun(async (progress) => {
    progress({ ...status, state: 'failed', error: 'client crashed' });
    throw new Error('client crashed');
  }, config, 'aks', sha, noCommands), 1);
  assert.match(output.join('\n'), /DATA FLOW: FAIL/);
  assert.match(output.join('\n'), /state: failed/);
  assert.match(output.join('\n'), /client crashed/);
  output.length = 0;
  assert.equal(await reportDataFlowRun(async () => status, config, 'aks', sha, noCommands,
    { provider: 'cilium', enforcing: true }, [], async (actual, actualConfig, actualMode, actualSha) => {
      assert.equal(actual.id, orchestration);
      return evaluateDataFlow(actual, [row()], actualSha, actualMode, actualConfig);
    }), 0);
  assert.match(output.join('\n'), /DATA FLOW: PASS/);
  assert.match(output.join('\n'), /network: cilium \(enforced\)/);
});

test('mirror reader pins a 30-second timeout and 16-MiB cap, including chunked responses', async () => {
  assert.equal(RESULT_TIMEOUT_MS, 30_000);
  assert.equal(MAX_RESULT_BYTES, 16 * 1024 * 1024);
  const fetcher: typeof fetch = async (_url, init) => {
    assert.equal(init?.redirect, 'error');
    assert.equal(init?.cache, 'no-store');
    assert.ok(init?.signal);
    return new Response(new Uint8Array([1, 2]));
  };
  assert.deepEqual([...await fetchResultBytes(new URL('https://example.test/'), fetcher)], [1, 2]);
  await assert.rejects(fetchResultBytes(new URL('https://example.test/'), async () =>
    new Response('x', { headers: { 'content-length': String(MAX_RESULT_BYTES + 1) } })), /size cap/);
  await assert.rejects(fetchResultBytes(new URL('https://example.test/'), async () =>
    new Response(new ReadableStream({
      start(controller) { controller.enqueue(new Uint8Array(MAX_RESULT_BYTES)); controller.enqueue(new Uint8Array(1)); controller.close(); },
    }))), /size cap/);
  await assert.rejects(fetchResultBytes(new URL('https://example.test/'), async () => new Response('')), /empty/);
  await assert.rejects(fetchResultBytes(new URL('https://example.test/'), async () => new Response('', { status: 503 })), /503/);
});

test('mirror deadline aborts both connection establishment and a stalled response body', async (t) => {
  t.mock.timers.enable({ apis: ['setTimeout'] });
  const connecting = fetchResultBytes(new URL('https://example.test/'), async (_url, init) => new Promise((_resolve, reject) => {
    init!.signal!.addEventListener('abort', () => reject(new Error('connection aborted')), { once: true });
  }));
  t.mock.timers.tick(RESULT_TIMEOUT_MS);
  await assert.rejects(connecting, /connection aborted/);
  const reading = fetchResultBytes(new URL('https://example.test/'), async (_url, init) =>
    new Response(new ReadableStream({
      start(controller) {
        init!.signal!.addEventListener('abort', () => controller.error(new Error('body aborted')), { once: true });
      },
    })));
  t.mock.timers.tick(RESULT_TIMEOUT_MS);
  await assert.rejects(reading, /body aborted/);
});

test('real Parquet decodes and an unreadable local result fails closed', async () => {
  const bytes = writeParquet(WasmTable.fromIPCStream(tableToIPC(tableFromArrays({ rows: [1, 2] }), 'stream')));
  assert.equal(decodeResultRows(bytes).length, 2);
  assert.throws(() => decodeResultRows(new Uint8Array()), /empty/);
  const directory = await mkdtemp(join(tmpdir(), 'oat-verdict-test-'));
  try {
    const path = join(directory, 'results.parquet');
    await writeFile(path, bytes);
    assert.deepEqual([...await readResultBytes({ result_url: pathToFileURL(path).href }, 'local', async () => { throw new Error('no commands'); }, fetch, () => {})], [...bytes]);
    const result = await collectDataFlow(status, { ...config, result_url: pathToFileURL(join(directory, 'missing.parquet')).href }, 'local', sha);
    assert.equal(result.pass, false);
    assert.match(result.reasons.join(), /ENOENT/);
  } finally { await rm(directory, { recursive: true, force: true }); }
});

test('local evidence collection passes only a readable Parquet containing this run and revision', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'oat-verdict-local-'));
  const path = join(directory, 'results.parquet');
  const localConfig = { ...config, result_url: pathToFileURL(path).href };
  const writeResult = async (revision: string) => {
    await writeFile(path, resultParquet('local', revision));
  };
  try {
    await writeResult(sha);
    assert.equal((await collectDataFlow(status, localConfig, 'local', sha)).pass, true);
    await writeResult('b'.repeat(40));
    const stale = await collectDataFlow(status, localConfig, 'local', sha);
    assert.equal(stale.pass, false);
    assert.match(stale.reasons.join(), /git_sha differs/);
    await writeFile(path, '');
    assert.equal((await collectDataFlow(status, localConfig, 'local', sha)).pass, false);
  } finally { await rm(directory, { recursive: true, force: true }); }
});

test('AKS collection preserves verified rows when cleanup evidence fails and passes only after absence', async (t) => {
  const bytes = resultParquet('aks');
  t.mock.method(globalThis, 'fetch', async () => new Response(Buffer.from(bytes)));
  let cleanupFails = true;
  const environment: NodeJS.ProcessEnv = { AZURE_SUBSCRIPTION_ID: subscriptionId };
  const runner: typeof run_tree = async (args) => {
    if (args[1] === 'storage') return ok('https://arrowcollector00000.z1.web.core.windows.net/');
    if (args[0] === 'kubectl') return cleanupFails
      ? { returncode: 1, stdout: '', stderr: 'not authorized' } : ok('{"items":[]}');
    return ok('[]');
  };
  const failed = await collectDataFlow(status, config, 'aks', sha, runner, environment);
  assert.equal(failed.pass, false);
  assert.equal(failed.rows.length, 1);
  assert.match(failed.reasons.join(), /evidence command failed: kubectl/);
  assert.doesNotMatch(failed.reasons.join(), /no Parquet rows/);
  cleanupFails = false;
  const succeeded = await collectDataFlow(status, config, 'aks', sha, runner, environment);
  assert.equal(succeeded.pass, true);
  assert.match(succeeded.lines.join(), /zero run-scoped namespaces or federated credentials/);
});

test('AKS mirror endpoint lookup uses configured subscription despite ambient selection', async () => {
  const logs: string[] = [];
  const previous = process.env.AZURE_SUBSCRIPTION_ID;
  process.env.AZURE_SUBSCRIPTION_ID = '00000000-0000-0000-0000-000000000002';
  let calls = 0;
  try {
    const runner: typeof run_tree = async (args) => {
      calls++;
      assert.deepEqual(args, ['az', 'storage', 'account', 'show', '-g', 'arrow-collector-rg',
        '-n', 'arrowcollector00000', '--subscription', subscriptionId, '--query', 'primaryEndpoints.web', '-o', 'tsv']);
      return ok('https://arrowcollector00000.z1.web.core.windows.net/');
    };
    const bytes = await readResultBytes(config, 'aks', runner, async () => new Response('PAR1'), (message) => logs.push(message));
    assert.equal(Buffer.from(bytes).toString(), 'PAR1');
    assert.equal(calls, 1);
    assert.match(logs.join(), /public sanitized/);
    assert.doesNotMatch(logs.join(), /WARNING:.*authenticated/);
  } finally {
    if (previous === undefined) delete process.env.AZURE_SUBSCRIPTION_ID;
    else process.env.AZURE_SUBSCRIPTION_ID = previous;
  }
});

test('AKS mirror lookup rejects missing or malformed selected subscription before fallback', async () => {
  for (const subscription of [undefined, 'not-a-subscription']) {
    let calls = 0;
    const logs: string[] = [];
    const runner: typeof run_tree = async () => { calls++; return ok(''); };
    await assert.rejects(readResultBytes({ ...config, subscriptionId: subscription }, 'aks', runner,
      async () => new Response('PAR1'), (message) => logs.push(message)),
    /AKS result lookup selected subscription is missing or invalid/);
    assert.equal(calls, 0);
    assert.deepEqual(logs, []);
  }
});

test('AKS reads sanitized mirror by configured path and visibly falls back only on unavailable mirror', async () => {
  const logs: string[] = [];
  const runner: typeof run_tree = async (args, timeout) => {
    assert.equal(timeout, 30);
    assert.deepEqual(args.slice(0, 4), ['az', 'storage', 'account', 'show']);
    return ok('https://arrowcollector00000.z1.web.core.windows.net/');
  };
  const bytes = await readResultBytes({ ...config, website_url: 'abfss://$web@arrowcollector00000.dfs.core.windows.net/custom/results.parquet' }, 'aks', runner, async (url) => {
    assert.equal(String(url), 'https://arrowcollector00000.z1.web.core.windows.net/custom/results.parquet');
    return new Response('PAR1');
  }, (message) => logs.push(message));
  assert.equal(Buffer.from(bytes).toString(), 'PAR1');
  assert.match(logs.join(), /public sanitized/);
  const fallback: typeof run_tree = async (args) => {
    if (args.includes('account')) return ok('https://arrowcollector00000.z1.web.core.windows.net/');
    if (args.includes('show')) return ok('{"size":4,"etag":"v1"}');
    assert.equal(args[args.indexOf('--if-match') + 1], 'v1');
    const path = args[args.indexOf('--file') + 1]!;
    await writeFile(path, 'PAR1');
    return ok('');
  };
  const fallbackBytes = await readResultBytes(config, 'aks', fallback, async () => new Response('', { status: 503 }), (message) => logs.push(message));
  assert.equal(Buffer.from(fallbackBytes).toString(), 'PAR1');
  assert.match(logs.join(), /WARNING:.*authenticated/);
});

test('cleanup waits for Terminating namespaces and scoped FICs, but ignores the native trust', async () => {
  let now = 0, reads = 0;
  const runner: typeof run_tree = async (args) => {
    if (args[0] === 'kubectl') {
      assert.equal(args[2], 'arrow-collector-00000');
      assert.ok(args.includes(`arrowcol-oat/run-id=${cell}`));
      reads++;
      return ok(JSON.stringify({ items: reads === 1 ? [{ status: { phase: 'Terminating' } }] : [] }));
    }
    assert.deepEqual(args, [
      'az', 'identity', 'federated-credential', 'list', '-g', 'arrow-collector-rg',
      '--identity-name', 'arrow-collector', '--subscription', subscriptionId, '-o', 'json',
    ]);
    return ok(JSON.stringify([{ name: 'azure-qap-arrow-collector-00000', subject: 'system:serviceaccount:arrow-collector:arrow-collector' }]));
  };
  await verifyRunCleanup(
    [row()], config, runner, () => now, async (ms) => { now += ms; }, { AZURE_SUBSCRIPTION_ID: subscriptionId },
  );
  assert.equal(reads, 2);
  assert.equal(now, 5000);
  const leaked: typeof run_tree = async (args) => ok(JSON.stringify(args[0] === 'kubectl' ? { items: [] } : [
    { name: 'unexpected-name', subject: `system:serviceaccount:arrow-collector-${cell.replaceAll('-', '')}:oat` },
  ]));
  await assert.rejects(verifyRunCleanup(
    [row()], config, leaked, () => now, async (ms) => { now += ms; }, { AZURE_SUBSCRIPTION_ID: subscriptionId },
  ), /cleanup timed out/);
  assert.equal(now, 5000 + CLEANUP_TIMEOUT_MS);
});

test('cleanup fails closed when the selected subscription is missing or conflicts with the environment', async () => {
  let calls = 0;
  const runner: typeof run_tree = async () => { calls++; return ok('[]'); };
  const sleep = async () => {};
  await assert.rejects(verifyRunCleanup([row()], { ...config, subscriptionId: undefined }, runner, () => 0, sleep, {
    AZURE_SUBSCRIPTION_ID: subscriptionId,
  }), /selected subscription is missing or invalid/);
  await assert.rejects(verifyRunCleanup([row()], config, runner, () => 0, sleep, {
    AZURE_SUBSCRIPTION_ID: '00000000-0000-0000-0000-000000000002',
  }), /conflicts with the selected subscription/);
  assert.equal(calls, 0);
});
