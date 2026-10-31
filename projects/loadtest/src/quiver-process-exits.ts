import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import path from 'node:path';

import { BENCHMARK_PROJECT } from './cli.js';
import { atomicJson } from './publisher-release.js';

export const QUIVER_PROCESS_EXITS_FILE = 'quiver-process-exits.json';
type Docker = (args: string[]) => string;
type Pool = 'direct' | 'dataflow';
interface ProcessIdentity { id: string; startedAt: number; finishedAt: number; running: boolean; exitCode: number }
export interface KilledPublisher {
  implementation_pool: Pool;
  instance_key: string;
  instance_id: string;
  container_id: string;
  started_at_unix_ms: number;
  finished_at_unix_ms: number;
  exit_code: number;
}
export interface KilledPublishers { run_id: string; publishers: KilledPublisher[] }

function runId(results: string): string {
  const metadata = JSON.parse(readFileSync(path.join(results, 'run-metadata.json'), 'utf8')) as { run_id?: unknown };
  assert(typeof metadata.run_id === 'string' && metadata.run_id.length > 0, 'Missing crash run identity');
  return metadata.run_id;
}

function processIdentity(workspace: string, pool: Pool, endpoint: string, docker: Docker): ProcessIdentity {
  const service = `${pool}-1`;
  const ids = docker(['ps', '--all', '--quiet',
    '--filter', `label=com.docker.compose.project=${BENCHMARK_PROJECT}`,
    '--filter', `label=com.docker.compose.service=${service}`]).trim().split(/\s+/u).filter(Boolean);
  assert.equal(ids.length, 1, `Expected one owned ${service} crash generation`);
  const values = JSON.parse(docker(['inspect', ids[0]])) as Array<{
    Id: string; Config: { Labels: Record<string, string> };
    State: { Running: boolean; Restarting: boolean; OOMKilled: boolean; Dead: boolean;
      ExitCode: number; StartedAt: string; FinishedAt: string };
    NetworkSettings: { Networks: Record<string, { IPAddress: string }> };
  }>;
  assert.equal(values.length, 1);
  const [value] = values;
  assert(value && /^[a-f0-9]{64}$/u.test(value.Id), 'Missing crash container identity');
  assert.equal(value.Config.Labels['com.docker.compose.project'], BENCHMARK_PROJECT,
    'Crash container project ownership changed');
  assert.equal(value.Config.Labels['com.docker.compose.service'], service,
    'Crash container service ownership changed');
  assert(value.Config.Labels['com.docker.compose.project.config_files']?.split(',')
    .includes(path.join(workspace, 'projects/loadtest/compose.yaml')), 'Crash container ownership changed');
  const address = new URL(endpoint);
  assert(address.protocol === 'http:' && (!value.State.Running || Object.values(value.NetworkSettings.Networks)
    .some(network => network.IPAddress === address.hostname)), 'Pressure endpoint is not the owned crash container');
  assert.equal(value.State.Restarting, false, 'Crash process is restarting outside the controller');
  assert.equal(value.State.OOMKilled, false, 'OOM termination is not the requested SIGKILL');
  assert.equal(value.State.Dead, false, 'Docker reports a dead crash container');
  const startedAt = Date.parse(value.State.StartedAt);
  const finishedAt = Date.parse(value.State.FinishedAt);
  assert(Number.isSafeInteger(startedAt) && startedAt > 0, 'Missing crash process start time');
  return { id: value.Id, startedAt, finishedAt, running: value.State.Running, exitCode: value.State.ExitCode };
}

async function pressureIdentity(endpoint: string, pool: Pool): Promise<string> {
  const response = await fetch(`${endpoint}/v1/pressure`, { signal: AbortSignal.timeout(5_000), redirect: 'error' });
  assert(response.ok, `Crash pressure identity HTTP ${response.status}`);
  const value = await response.json() as { instance_id?: unknown; rich?: { instance_id?: unknown } };
  const instance = pool === 'direct' ? value.instance_id : value.rich?.instance_id;
  assert(typeof instance === 'string'
    && /^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/iu.test(instance), 'Missing crash collector UUID');
  return instance.toLowerCase();
}

export async function killQuiverPublishers(options: {
  workspace: string; results: string; docker: Docker; endpoints: Record<Pool, string>; kill: () => Promise<void>;
}): Promise<KilledPublishers> {
  const id = runId(options.results);
  const before = await Promise.all((['direct', 'dataflow'] as const).map(async pool => {
    const process = processIdentity(options.workspace, pool, options.endpoints[pool], options.docker);
    assert(process.running, 'Collector already stopped before controlled crash');
    const instance = await pressureIdentity(options.endpoints[pool], pool);
    assert.deepEqual(processIdentity(options.workspace, pool, options.endpoints[pool], options.docker), process,
      'Collector process changed during crash identity capture');
    return { pool, process, instance };
  }));
  await options.kill();
  const publishers = before.map(({ pool, process, instance }): KilledPublisher => {
    const after = processIdentity(options.workspace, pool, options.endpoints[pool], options.docker);
    assert.equal(after.id, process.id, 'Killed container changed');
    assert.equal(after.startedAt, process.startedAt, 'Killed process generation changed');
    assert.equal(after.running, false, 'SIGKILL did not stop the collector');
    assert.equal(after.exitCode, 137, 'Collector did not exit from SIGKILL');
    assert(Number.isSafeInteger(after.finishedAt) && after.finishedAt >= process.startedAt
      && after.finishedAt <= Date.now(), 'Missing killed process finish time');
    return { implementation_pool: pool, instance_key: `loadtest-${pool}-1`, instance_id: instance,
      container_id: process.id, started_at_unix_ms: process.startedAt,
      finished_at_unix_ms: after.finishedAt, exit_code: after.exitCode };
  });
  assert.equal(runId(options.results), id, 'Crash run identity changed');
  return { run_id: id, publishers };
}

export async function recordQuiverSuccessors(options: {
  workspace: string; results: string; docker: Docker; endpoints: Record<Pool, string>; killed: KilledPublishers;
}): Promise<void> {
  assert.equal(runId(options.results), options.killed.run_id, 'Restart run identity changed');
  const publishers = await Promise.all(options.killed.publishers.map(async publisher => {
    const pool = publisher.implementation_pool;
    const process = processIdentity(options.workspace, pool, options.endpoints[pool], options.docker);
    assert(process.running && process.startedAt >= publisher.finished_at_unix_ms,
      'Restart did not create a new live process generation');
    const instance = await pressureIdentity(options.endpoints[pool], pool);
    assert.notEqual(instance, publisher.instance_id, 'Restart reused the killed collector UUID');
    assert.deepEqual(processIdentity(options.workspace, pool, options.endpoints[pool], options.docker), process,
      'Successor process changed during identity capture');
    return { ...publisher, successor_instance_id: instance, successor_container_id: process.id,
      successor_started_at_unix_ms: process.startedAt };
  }));
  atomicJson(path.join(options.results, QUIVER_PROCESS_EXITS_FILE),
    { run_id: options.killed.run_id, publishers });
}
