import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { chmodSync, mkdtempSync, rmSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { test } from 'node:test';
import { EffectBoundary, withEffectBoundary, type EffectEnvelope } from '../../infra/azure/effect-boundary.js';
import { StorageBridgeDispatcher, type StorageBridgeAuthority, type StorageBridgeRequest } from './aks-d47-bridge.js';
import type { Azure } from '../../infra/azure/bootstrap.js';
import { newCampaignState, type CampaignStore, type RunInventory, type StoredCampaign } from './aks-campaign.js';

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

function signedRequest(authority: StorageBridgeAuthority, deadline: number, method = 'GET', body = ''): StorageBridgeRequest {
  const headers: [string, string][] = method === 'PUT' ? [['if-none-match', '*'], ['content-type', 'application/json']] : [];
  const unsigned = {
    schema_version: 1 as const,
    kind: 'storage_http_request' as const,
    request_id: 1,
    authority,
    original_deadline_unix_ms: deadline,
    remaining_timeout_ms: 10_000,
    target: 'object_store' as const,
    method,
    url: `https://${authority.storage_account}.blob.core.windows.net/${new URL(authority.table_root_url).username}` +
      `${new URL(authority.table_root_url).pathname}_delta_log/00000000000000000000.json`,
    headers,
    body,
    body_sha256: sha(body),
  };
  return { ...unsigned, request_sha256: sha(canonical(unsigned)) };
}

function envelopeFor(authority: StorageBridgeAuthority, request: StorageBridgeRequest): EffectEnvelope {
  const headersHash = sha(canonical(request.headers));
  const effect: EffectEnvelope['effects'][number] = {
    id: 'storage-request',
    transport: 'azure-rest',
    method: request.method,
    resource: `https://${authority.storage_account}.blob.core.windows.net|https://storage.azure.com/`,
    path: `${new URL(request.url).pathname}|headers-sha256=${headersHash}`,
    max_calls: 1,
    body_sha256: sha(request.body),
  };
  return {
    schema_version: 1,
    campaign_id: authority.campaign_id,
    run_id: authority.cell_run_id,
    subscription_id: 'c0ffee00-0000-4000-8000-000000000005',
    resource_group: 'offline',
    storage_account: authority.storage_account,
    state_container: 'checkpoints',
    effects: [effect],
  };
}

function mutableCustodyStore(authority: StorageBridgeAuthority, now: number) {
  const step = () => ({ state: 'not_started' as const, deadline_unix_ms: null, reason_code: null });
  const inventory: RunInventory = {
    schema_version: 1,
    campaign_id: authority.campaign_id,
    owner_id: authority.owner_id,
    fencing_token: authority.fencing_token,
    orchestration_run_id: authority.cell_run_id,
    cell_run_id: authority.cell_run_id,
    load_admission: 'not_authorized',
    planned_cell_duration_ms: 0,
    effective_stop_budgets: { maintenance_grace_seconds: 900, maintenance_stop_timeout_ms: 1_200_000 },
    namespace: { name: `arrowcol-${authority.cell_run_id.replaceAll('-', '')}`, uid: null, owner_token: authority.owner_id },
    resources: [],
    fic: null,
    qap: null,
    cleanup: { stop_admission: step(), bounded_drain: step(), evidence_capture: step(), stop_maintenance: step(),
      terminal_capture: step(), handoff: step() },
  };
  let current: StoredCampaign = {
    state: {
      ...newCampaignState(authority.campaign_id, authority.owner_id, now),
      fencingToken: authority.fencing_token,
      runInventory: inventory,
    },
    etag: '"custody-1"',
  };
  const store: CampaignStore = {
    async read() { return structuredClone(current); },
    async write() { throw new Error('custody witness must not write campaign state'); },
  };
  return {
    store,
    fence() {
      current = { ...current, state: { ...current.state, ownerId: 'c0ffee00-0000-4000-8000-000000000009',
        fencingToken: authority.fencing_token + 1 }, etag: '"custody-2"' };
    },
  };
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
    fencing_token: 1,
    storage_account: 'arrowcollector68590',
    table_root_url: `abfss://checkpoints@arrowcollector68590.dfs.core.windows.net/campaigns/${campaignId}/runs/${runId}/dataflow/telemetry/sqlserver/sqlserver_availability_replica_states/`,
    control_container_url: 'https://arrowcollector68590.blob.core.windows.net/checkpoints',
    sandbox_id: 'offline-stamp',
    reset_generation: 'c0ffee00-0000-4000-8000-000000000004',
  };
  const now = Date.now(), deadline = now + 60_000;
  const request = signedRequest(authority, deadline);
  const envelope = envelopeFor(authority, request);
  const custody = mutableCustodyStore(authority, now);
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
    store: custody.store, phase: 'owned-run-table-initialization', planIdentity: { run_id: runId }, ledgerPath, fetcher });
  const boundary = new EffectBoundary(envelope, path.join(directory, 'effects.jsonl'));
  const initial = await withEffectBoundary(boundary, () => first.exchange(request));
  assert.equal(initial.outcome, 'response');
  if (initial.outcome !== 'response') throw new Error('Expected the actual response.');
  assert.equal(initial.body, 'actual-provider-body');
  assert.equal(initial.headers.find(([name]) => name === 'etag')?.[1], '"fresh-table-log"');

  const restarted = new StorageBridgeDispatcher({ authority, originalDeadlineUnixMs: deadline, envelope, azure,
    store: custody.store, phase: 'owned-run-table-initialization', planIdentity: { run_id: runId }, ledgerPath, fetcher });
  const replayed = await withEffectBoundary(boundary, () => restarted.exchange(request));
  assert.deepEqual(replayed, initial);
  assert.equal(forwards, 1);
  const changedDeadline = deadline + 60_000;
  assert.throws(() => new StorageBridgeDispatcher({ authority, originalDeadlineUnixMs: changedDeadline, envelope, azure,
    store: custody.store, phase: 'owned-run-table-initialization', planIdentity: { run_id: runId }, ledgerPath, fetcher }),
  /immutable deadline, phase, plan or allocation identity/);
  const mismatched = new EffectBoundary({ ...envelope, effects: [{ ...envelope.effects[0]!, id: 'other-allocation' }] },
    path.join(directory, 'mismatched-effects.jsonl'));
  await assert.rejects(withEffectBoundary(mismatched, () => restarted.exchange(request)), /exact active effect allocation/);
  assert.equal(forwards, 1);
});

test('StorageBridge revalidates current custody after token preparation before a commit forward', async t => {
  const directory = mkdtempSync(path.join(os.tmpdir(), 'oat-d47-storage-fence-'));
  chmodSync(directory, 0o700);
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const authority: StorageBridgeAuthority = {
    campaign_id: 'c0ffee00-0000-4000-8000-000000000001',
    cell_run_id: 'c0ffee00-0000-4000-8000-000000000002',
    owner_id: 'c0ffee00-0000-4000-8000-000000000003',
    fencing_token: 1,
    storage_account: 'arrowcollector68590',
    table_root_url: 'abfss://checkpoints@arrowcollector68590.dfs.core.windows.net/campaigns/c0ffee00-0000-4000-8000-000000000001/runs/c0ffee00-0000-4000-8000-000000000002/dataflow/telemetry/sqlserver/sqlserver_availability_replica_states/',
    control_container_url: 'https://arrowcollector68590.blob.core.windows.net/checkpoints',
    sandbox_id: 'offline-stamp',
    reset_generation: 'c0ffee00-0000-4000-8000-000000000004',
  };
  const now = Date.now(), deadline = now + 60_000, request = signedRequest(authority, deadline, 'PUT', '{}');
  const envelope = envelopeFor(authority, request), custody = mutableCustodyStore(authority, now);
  let fetches = 0;
  const azure = {
    c: { account: authority.storage_account },
    async token() { custody.fence(); return 'offline-test-token'; },
  } as unknown as Azure;
  const dispatcher = new StorageBridgeDispatcher({ authority, originalDeadlineUnixMs: deadline, envelope, azure,
    store: custody.store, phase: 'owned-run-table-initialization', planIdentity: { run_id: authority.cell_run_id },
    ledgerPath: path.join(directory, 'storage-ledger.jsonl'), fetcher: async () => {
      fetches++;
      return new Response('', { status: 201 });
    } });
  const boundary = new EffectBoundary(envelope, path.join(directory, 'effects.jsonl'));
  const reply = await withEffectBoundary(boundary, () => dispatcher.exchange(request));
  assert.equal(reply.outcome, 'unknown');
  assert.equal(fetches, 0);
});
