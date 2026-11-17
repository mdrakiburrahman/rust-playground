import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { chmodSync, mkdtempSync, rmSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { test } from 'node:test';
import { EffectBoundary, withEffectBoundary, type EffectEnvelope } from '../../infra/azure/effect-boundary.js';
import { StorageBridgeDispatcher, type StorageBridgeAuthority, type StorageBridgeRequest } from './aks-d47-bridge.js';
import type { Azure } from '../../infra/azure/bootstrap.js';

function canonical(value: unknown): string {
  if (Array.isArray(value)) return '[' + value.map(canonical).join(',') + ']';
  if (value !== null && typeof value === 'object')
    return '{' + Object.keys(value).sort().map(key => JSON.stringify(key) + ':' +
      canonical((value as Record<string, unknown>)[key])).join(',') + '}';
  const result = JSON.stringify(value);
  if (result === undefined) throw new Error('Value is not JSON.');
  return result;
}

function sha(value: string): string {
  return createHash('sha256').update(value, 'utf8').digest('hex');
}

function signedRequest(authority: StorageBridgeAuthority, deadline: number): StorageBridgeRequest {
  const unsigned = {
    schema_version: 1 as const,
    kind: 'storage_http_request' as const,
    request_id: 1,
    authority,
    original_deadline_unix_ms: deadline,
    remaining_timeout_ms: 10_000,
    target: 'object_store' as const,
    method: 'GET',
    url: `https://${authority.storage_account}.blob.core.windows.net/${new URL(authority.table_root_url).username}` +
      `${new URL(authority.table_root_url).pathname}_delta_log/00000000000000000000.json`,
    headers: [] as [string, string][],
    body: '',
    body_sha256: sha(''),
  };
  return { ...unsigned, request_sha256: sha(canonical(unsigned)) };
}

test('StorageBridge returns its durable actual response after restart without forwarding twice', async t => {
  const directory = mkdtempSync(path.join(os.tmpdir(), 'oat-d47-storage-bridge-'));
  chmodSync(directory, 0o700);
  t.after(() => rmSync(directory, { recursive: true, force: true }));

  const campaignId = 'c0ffee00-0000-4000-8000-000000000001';
  const runId = 'c0ffee00-0000-4000-8000-000000000002';
  const authority: StorageBridgeAuthority = {
    campaign_id: campaignId,
    cell_run_id: runId,
    owner_id: 'c0ffee00-0000-4000-8000-000000000003',
    fencing_token: 7,
    storage_account: 'arrowcollector68590',
    table_root_url: `abfss://checkpoints@arrowcollector68590.dfs.core.windows.net/campaigns/${campaignId}/runs/${runId}/dataflow/telemetry/sqlserver/sqlserver_availability_replica_states/`,
    control_container_url: 'https://arrowcollector68590.blob.core.windows.net/checkpoints',
    sandbox_id: 'offline-stamp',
    reset_generation: 'c0ffee00-0000-4000-8000-000000000004',
  };
  const deadline = Date.now() + 60_000;
  const request = signedRequest(authority, deadline);
  const headersHash = sha(canonical(request.headers));
  const effectPath = `${new URL(request.url).pathname}|headers-sha256=${headersHash}`;
  const effect: EffectEnvelope['effects'][number] = {
    id: 'fresh-log-read',
    transport: 'azure-rest',
    method: 'GET',
    resource: `https://${authority.storage_account}.blob.core.windows.net|https://storage.azure.com/`,
    path: effectPath,
    max_calls: 1,
    body_sha256: sha(''),
  };
  const envelope: EffectEnvelope = {
    schema_version: 1,
    campaign_id: campaignId,
    run_id: runId,
    subscription_id: 'c0ffee00-0000-4000-8000-000000000005',
    resource_group: 'offline',
    storage_account: authority.storage_account,
    state_container: 'checkpoints',
    effects: [effect],
  };
  const azure = {
    c: { account: authority.storage_account },
    async token() { return 'offline-test-token'; },
  } as unknown as Azure;
  let forwards = 0;
  const fetcher: typeof fetch = async () => {
    forwards++;
    return new Response('actual-provider-body', { status: 200, headers: { etag: '"fresh-table-log"' } });
  };
  const ledgerPath = path.join(directory, 'storage-ledger.jsonl');
  const first = new StorageBridgeDispatcher({ authority, originalDeadlineUnixMs: deadline, envelope, azure,
    ledgerPath, fetcher });
  const boundary = new EffectBoundary(envelope, path.join(directory, 'effects.jsonl'));
  const initial = await withEffectBoundary(boundary, () => first.exchange(request));
  assert.equal(initial.outcome, 'response');
  if (initial.outcome !== 'response') throw new Error('Expected the actual response.');
  assert.equal(initial.body, 'actual-provider-body');
  assert.equal(initial.headers.find(([name]) => name === 'etag')?.[1], '"fresh-table-log"');

  const restarted = new StorageBridgeDispatcher({ authority, originalDeadlineUnixMs: deadline, envelope, azure,
    ledgerPath, fetcher });
  const replayed = await withEffectBoundary(boundary, () => restarted.exchange(request));
  assert.deepEqual(replayed, initial);
  assert.equal(forwards, 1);
});
