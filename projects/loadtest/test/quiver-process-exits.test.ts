import assert from 'node:assert/strict';
import { once } from 'node:events';
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { test } from 'node:test';

import { BENCHMARK_PROJECT } from '../src/cli.js';
import { killQuiverPublishers, recordQuiverSuccessors, QUIVER_PROCESS_EXITS_FILE } from '../src/quiver-process-exits.js';

async function fixture() {
  const results = mkdtempSync(path.join(tmpdir(), 'quiver-process-exits-'));
  writeFileSync(path.join(results, 'run-metadata.json'), JSON.stringify({ run_id: 'run-1' }));
  const state = { phase: 'running', wrongOwner: false, wrongIp: false, wrongShape: false,
    oom: false, exitCode: 137, reusedUuid: false };
  const instance = (successor: boolean): string =>
    `2${successor ? '2222222' : '1111111'}-1111-4111-8111-111111111111`;
  const server = createServer((request, response) => {
    response.setHeader('content-type', 'application/json');
    const pressure = { instance_id: instance(state.phase === 'restarted' && !state.reusedUuid) };
    response.end(JSON.stringify(state.wrongShape ? pressure : { rich: pressure }));
  });
  server.listen(0, '127.0.0.1');
  await once(server, 'listening');
  const address = server.address();
  assert(address && typeof address !== 'string');
  const endpoints = { dataflow: `http://127.0.0.1:${address.port}/dataflow` };
  const docker = (args: string[]): string => {
    const id = 'b'.repeat(64);
    if (args[0] === 'ps') return id;
    return JSON.stringify([{
      Id: id,
      Config: { Labels: { 'com.docker.compose.project': state.wrongOwner ? 'foreign' : BENCHMARK_PROJECT,
        'com.docker.compose.service': 'dataflow-1',
        'com.docker.compose.project.config_files': '/work/projects/loadtest/compose.yaml' } },
      NetworkSettings: { Networks: { owned: { IPAddress: state.phase === 'killed' ? '' : state.wrongIp ? '10.0.0.99' : '127.0.0.1' } } },
      State: { Running: state.phase !== 'killed', Restarting: false, OOMKilled: state.oom, Dead: false,
        ExitCode: state.phase === 'killed' ? state.exitCode : 0,
        StartedAt: state.phase === 'restarted' ? '2026-01-01T00:00:02Z' : '2026-01-01T00:00:00Z',
        FinishedAt: state.phase === 'killed' ? '2026-01-01T00:00:01Z' : '0001-01-01T00:00:00Z' },
    }]);
  };
  const options = { workspace: '/work', results, docker, endpoints,
    kill: async () => { state.phase = 'killed'; } };
  return { results, state, options, async close() {
    await new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
    rmSync(results, { recursive: true, force: true });
  } };
}

test('records same-run owned SIGKILL exits and distinct successor process UUIDs', async () => {
  const f = await fixture();
  try {
    const killed = await killQuiverPublishers(f.options);
    f.state.phase = 'restarted';
    await recordQuiverSuccessors({ ...f.options, killed });
    const proof = JSON.parse(readFileSync(path.join(f.results, QUIVER_PROCESS_EXITS_FILE), 'utf8'));
    assert.equal(proof.run_id, 'run-1');
    assert.equal(proof.publishers.length, 1);
    for (const publisher of proof.publishers) {
      assert.equal(publisher.exit_code, 137);
      assert.notEqual(publisher.instance_id, publisher.successor_instance_id);
      assert(publisher.started_at_unix_ms < publisher.finished_at_unix_ms);
      assert(publisher.finished_at_unix_ms < publisher.successor_started_at_unix_ms);
    }
  } finally { await f.close(); }
});

test('refuses foreign ownership or an exit that was not the controlled SIGKILL', async () => {
  const f = await fixture();
  try {
    f.state.wrongOwner = true;
    await assert.rejects(killQuiverPublishers(f.options), {
      code: 'ERR_ASSERTION',
      message: /^Crash container project ownership changed(?:\n|$)/u,
      actual: 'foreign',
      expected: BENCHMARK_PROJECT,
    });
    f.state.wrongOwner = false;
    f.state.wrongIp = true;
    await assert.rejects(killQuiverPublishers(f.options), /Pressure endpoint is not the owned/u);
    f.state.wrongIp = false;
    f.state.wrongShape = true;
    await assert.rejects(killQuiverPublishers(f.options), /Missing crash collector UUID/u);
    f.state.wrongShape = false;
    f.state.oom = true;
    await assert.rejects(killQuiverPublishers(f.options), /OOM termination/u);
    f.state.oom = false;
    f.state.exitCode = 0;
    await assert.rejects(killQuiverPublishers(f.options), /did not exit from SIGKILL/u);
  } finally { await f.close(); }
});

test('refuses a reused collector UUID and run identity changed across restart', async () => {
  const f = await fixture();
  try {
    const killed = await killQuiverPublishers(f.options);
    f.state.phase = 'restarted';
    f.state.reusedUuid = true;
    await assert.rejects(recordQuiverSuccessors({ ...f.options, killed }), /reused the killed collector UUID/u);
    f.state.reusedUuid = false;
    writeFileSync(path.join(f.results, 'run-metadata.json'), JSON.stringify({ run_id: 'other' }));
    await assert.rejects(recordQuiverSuccessors({ ...f.options, killed }), /Restart run identity changed/u);
  } finally { await f.close(); }
});
