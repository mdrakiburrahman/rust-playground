import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { pathToFileURL } from 'node:url';
import { tableFromArrays, tableToIPC } from 'apache-arrow';
import { Table as WasmTable, writeParquet } from 'parquet-wasm';
import { assertLocalFailedDisposition, localDispositionSuffix, recordLocalFailedDisposition } from './local-failed-disposition.js';

for (const resultStatus of ['failed', 'pinning_violation']) test(`local ${resultStatus} disposition preserves evidence and refuses changed or live custody`, async t => {
  const root = await mkdtemp(path.join(os.tmpdir(), 'oat-local-disposition-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const receiptRoot = path.join(root, 'receipts');
  const runRoot = path.join(root, 'run');
  const stateRoot = path.join(runRoot, 'state');
  await mkdir(receiptRoot);
  await mkdir(stateRoot, { recursive: true });
  const invocation = '123e4567-e89b-42d3-a456-426614174001';
  const run = '123e4567-e89b-42d3-a456-426614174002';
  const cell = '123e4567-e89b-42d3-a456-426614174003';
  const resultPath = path.join(runRoot, 'results.parquet');
  const markerPath = path.join(runRoot, 'original-ownership-marker.json');
  const logPath = path.join(runRoot, 'run.log');
  const receiptPath = path.join(receiptRoot, `${invocation}.json`);
  const project = 'arrowcol-oat-hi-123e4567e89b42d3a456426614174003';
  const body = JSON.stringify({
    platform: { kind: 'local', compose_project: 'arrowcol-oat-historical-parity', compose_state_root: stateRoot },
    result_url: pathToFileURL(resultPath).href, tables: ['test_table'], replica_counts: [1], dfe_worker_concurrency: [32],
  });
  const hash = createHash('sha256').update(body).digest('hex');
  const marker = {
    version: 1, runId: run, ownerPid: 123, ownerStartTicks: '100', serverPid: 124, serverStartTicks: '200',
    serverUrl: 'http://127.0.0.1:1234/', serverExecutable: '/tmp/oat',
    submitted_config_body: body, submitted_config_sha256: hash, source_config_sha256: 'a'.repeat(64),
  };
  const error = resultStatus === 'failed' ? 'guard unavailable' : 'pinning_violation: no active owner';
  const receipt = {
    version: 1, invocation_id: invocation, run_id: run, creation_state: 'accepted', run_state: 'failed',
    started_at_unix_ms: 1,
    cleanup_state: 'unsettled', config_binding_error: null, server_pid: 124, server_start_ticks: '200',
    server_executable: '/tmp/oat', server_url: marker.serverUrl, submitted_config_body: body,
    submitted_config_sha256: hash, source_config_sha256: marker.source_config_sha256,
    terminal_observation: {
      observed_at_unix_ms: 10, status: { id: run, state: 'failed', config_sha256: hash, error, started_at_unix_ms: 2, finished_at_unix_ms: 9 },
    },
  };
  const table = tableFromArrays({
    run_id: [cell], mode: ['local'], status: [resultStatus], pinning_ok: [false], table: ['test_table'],
    evidence_json: [JSON.stringify({ overload_guard: { run_id: cell } })],
  });
  await writeFile(resultPath, writeParquet(WasmTable.fromIPCStream(tableToIPC(table, 'stream'))));
  await writeFile(receiptPath, JSON.stringify(receipt));
  await writeFile(markerPath, JSON.stringify(marker));
  await writeFile(logPath, `${run} ${project} ${error}`);
  const originals = await Promise.all([receiptPath, markerPath, resultPath, logPath].map(file => readFile(file)));
  const env = {
    docker: (_args: string[]) => '',
    processStartTicks: async (_pid: number) => undefined,
    bootId: async () => '123e4567-e89b-42d3-a456-426614174004',
  };
  if (resultStatus === 'pinning_violation') {
    await writeFile(receiptPath, JSON.stringify({
      ...receipt, terminal_observation: { ...receipt.terminal_observation,
        status: { ...receipt.terminal_observation.status, error: 'guard unavailable' } },
    }));
    await assert.rejects(recordLocalFailedDisposition(receiptRoot, invocation, markerPath, env), /Failed cell identity mismatch/);
    await writeFile(receiptPath, originals[0]!);
    const invalid = tableFromArrays({
      run_id: [cell], mode: ['local'], status: [resultStatus], pinning_ok: [true], table: ['test_table'],
      evidence_json: [JSON.stringify({ overload_guard: { run_id: cell } })],
    });
    await writeFile(resultPath, writeParquet(WasmTable.fromIPCStream(tableToIPC(invalid, 'stream'))));
    await assert.rejects(recordLocalFailedDisposition(receiptRoot, invocation, markerPath, env), /Failed cell identity mismatch/);
    await writeFile(resultPath, originals[2]!);
  }
  await recordLocalFailedDisposition(receiptRoot, invocation, markerPath, env);
  const dispositionPath = receiptPath + localDispositionSuffix;
  await assertLocalFailedDisposition(receiptPath, dispositionPath, env);
  assert.deepEqual(await Promise.all([receiptPath, markerPath, resultPath, logPath].map(file => readFile(file))), originals);
  await assert.rejects(recordLocalFailedDisposition(receiptRoot, invocation, markerPath, env), { code: 'EEXIST' });
  await assert.rejects(assertLocalFailedDisposition(receiptPath, dispositionPath, {
    ...env, docker: () => 'live-container',
  }), /resources remain/);
  await assert.rejects(assertLocalFailedDisposition(receiptPath, dispositionPath, {
    ...env, processStartTicks: async pid => pid === 123 ? '100' : undefined,
  }), /still active/);
  await assert.rejects(assertLocalFailedDisposition(receiptPath, dispositionPath, {
    ...env, bootId: async () => '123e4567-e89b-42d3-a456-426614174005',
  }), /disposition or evidence changed/);
  await writeFile(receiptPath, JSON.stringify({ ...receipt, run_id: cell }));
  await assert.rejects(assertLocalFailedDisposition(receiptPath, dispositionPath, env), /ownership marker do not match/);
  await writeFile(receiptPath, originals[0]!);
  await writeFile(logPath, 'altered');
  await assert.rejects(assertLocalFailedDisposition(receiptPath, dispositionPath, env), /Run log does not bind/);
});
