import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFileSync, realpathSync, renameSync, writeFileSync } from 'node:fs';
import path from 'node:path';

import { stoppedAttemptMutators, type StoppedMutator } from './attempt-evidence.js';
import { BENCHMARK_PROJECT } from './cli.js';
import { executableIdentity } from './provenance.js';

type Docker = (args: string[], timeoutMs?: number) => string;
export interface SqlIdentity { id: string; startedAt: string; image: string }

export function sqlIdentity(workspace: string, docker: Docker): SqlIdentity {
  const ids = docker(['ps', '--all', '--quiet',
    '--filter', `label=com.docker.compose.project=${BENCHMARK_PROJECT}`,
    '--filter', 'label=com.docker.compose.service=sqlserver']).trim().split(/\s+/u).filter(Boolean);
  assert.equal(ids.length, 1, 'Expected exactly one owned SQL Server generation');
  const [value] = JSON.parse(docker(['inspect', ids[0]])) as Array<{
    Id: string; Image: string; Config: { Labels: Record<string, string> };
    State: { Running: boolean; StartedAt: string };
  }>;
  assert(value && /^[a-f0-9]{64}$/u.test(value.Id), 'SQL Server identity is missing');
  const labels = value.Config.Labels;
  assert.equal(labels['com.docker.compose.project'], BENCHMARK_PROJECT);
  assert.equal(labels['com.docker.compose.service'], 'sqlserver');
  assert(labels['com.docker.compose.project.config_files']?.split(',')
    .includes(path.join(workspace, 'projects/loadtest/compose.yaml')), 'SQL Server Compose ownership changed');
  assert.equal(value.State.Running, true, 'SQL Server stopped before release proof');
  assert(value.State.StartedAt && value.Image, 'SQL Server generation is incomplete');
  return { id: value.Id, startedAt: value.State.StartedAt, image: value.Image };
}

export async function admitPublisherRelease(options: {
  workspace: string; directory: string; docker: Docker; sql: SqlIdentity; sdk: string;
  stop: () => Promise<string[]>; release: () => Promise<void>;
  compact?: () => Promise<void>;
}): Promise<StoppedMutator[]> {
  const { workspace, directory, docker } = options;
  const results = path.join(directory, 'results');
  const metadata = JSON.parse(readFileSync(path.join(results, 'run-metadata.json'), 'utf8')) as { run_id?: unknown };
  assert(typeof metadata.run_id === 'string' && metadata.run_id.length > 0, 'Missing same-run identity');
  const provenance = JSON.parse(readFileSync(path.join(results, 'provenance.json'), 'utf8')) as {
    helpers?: { host?: Record<string, { sha256: string; bytes: number }> };
  };
  const sdk = provenance.helpers?.host?.['arrowcol-sdk-gen'];
  assert(sdk && /^[a-f0-9]{64}$/u.test(sdk.sha256), 'Missing same-run SDK provenance');
  assert.deepEqual(executableIdentity(options.sdk), sdk, 'SDK changed since stack startup');
  const services = await options.stop();
  const stopped = stoppedAttemptMutators(workspace, docker, services);
  const stoppedGenerations = () => stoppedAttemptMutators(workspace, docker, services).map(container => {
    const started = Date.parse(docker(['inspect', '--format', '{{.State.StartedAt}}', container.id]).trim());
    assert(Number.isSafeInteger(started) && started > 0, 'Stopped writer process start is unknown');
    return { ...container, started_at_unix_ms: started };
  });
  const generations = stoppedGenerations();
  const mutators = { run_id: metadata.run_id, clean: true, services, containers: generations, finished_at: new Date().toISOString() };
  atomicJson(path.join(results, 'mutator-drain.json'), mutators);
  assert.deepEqual(sqlIdentity(workspace, docker), options.sql, 'SQL Server generation changed since stack start');
  await options.release();
  assert.deepEqual(sqlIdentity(workspace, docker), options.sql, 'SQL Server generation changed during SQL release read');
  assert.deepEqual(stoppedGenerations(), generations, 'Writer generation changed during release');
  assert.deepEqual(executableIdentity(options.sdk), sdk, 'SDK changed during release proof');
  const bytes = readFileSync(path.join(results, 'publisher-release.json'));
  const proof = JSON.parse(bytes.toString('utf8')) as {
    passed?: unknown; run_id?: unknown; errors?: unknown; routing?: unknown; receipts?: unknown;
  };
  assert(proof.passed === true && proof.run_id === metadata.run_id && Array.isArray(proof.errors)
    && proof.errors.length === 0 && proof.routing && proof.receipts, 'SQL publisher release is not proven');
  const sha256 = (bytes: Buffer): string => createHash('sha256').update(bytes).digest('hex');
  atomicJson(path.join(results, 'publisher-admission.json'), {
    passed: true, run_id: metadata.run_id, sql: options.sql, root: realpathSync(directory), mutators: stopped,
    sdk_sha256: sdk.sha256, release_sha256: sha256(bytes),
    protocol: 'sql-terminal-state-and-owned-clean-exit',
    dataflow_proof: 'routing-stopped-and-owned-clean-exit; no-receipts-owner',
  });
  if (options.compact) {
    await options.compact();
    assert.deepEqual(sqlIdentity(workspace, docker), options.sql, 'SQL Server generation changed during final compaction');
    assert.deepEqual(stoppedGenerations(), generations, 'Writer generation changed during final compaction');
    assert.deepEqual(executableIdentity(options.sdk), sdk, 'SDK changed during final compaction');
  }
  return stopped;
}

export function atomicJson(file: string, value: unknown): void {
  writeFileSync(`${file}.next`, `${JSON.stringify(value, null, 2)}\n`);
  renameSync(`${file}.next`, file);
}
