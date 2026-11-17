import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { chmodSync, mkdtempSync, readFileSync, rmSync } from 'node:fs';
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

function signedRequest(authority: StorageBridgeAuthority, deadline: number, method = 'GET', body = '',
  options: { requestId?: number; clientRequestId?: string; date?: string; extraHeaders?: [string, string][] } = {}): StorageBridgeRequest {
  const date = options.date ?? new Date().toUTCString();
  const headers: [string, string][] = [
    ['x-ms-client-request-id', options.clientRequestId ?? 'c0ffee00-0000-4000-8000-000000000006'],
    ['date', date], ['x-ms-date', date],
    ...(method === 'PUT' ? [['if-none-match', '*'], ['content-type', 'application/json']] as [string, string][] : []),
    ...(options.extraHeaders ?? []),
  ];
  const unsigned = {
    schema_version: 1 as const,
    kind: 'storage_http_request' as const,
    request_id: options.requestId ?? 1,
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

function resignRequest(request: StorageBridgeRequest, headers: [string, string][], requestId = request.request_id): StorageBridgeRequest {
  const { request_sha256, ...rest } = request;
  void request_sha256;
  const unsigned = { ...rest, request_id: requestId, headers };
  return { ...unsigned, request_sha256: sha(canonical(unsigned)) };
}

function envelopeFor(authority: StorageBridgeAuthority, request: StorageBridgeRequest, maxCalls = 1): EffectEnvelope {
  const stableHeaders = request.headers.filter(([name]) => !['x-ms-client-request-id', 'date', 'x-ms-date'].includes(name.toLowerCase()));
  const headersHash = sha(canonical(stableHeaders));
  const url = new URL(request.url);
  const effect: EffectEnvelope['effects'][number] = {
    id: 'storage-request',
    transport: 'azure-rest',
    method: request.method,
    resource: `https://${authority.storage_account}.blob.core.windows.net|https://storage.azure.com/`,
    path: `${url.pathname}${url.search}|headers-sha256=${headersHash}`,
    max_calls: maxCalls,
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

test('StorageBridge normalizes only approved volatile headers and retains raw request evidence', async t => {
  const directory = mkdtempSync(path.join(os.tmpdir(), 'oat-d47-header-normalization-'));
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
  const now = Date.now(), deadline = now + 60_000;
  const request1 = signedRequest(authority, deadline, 'GET', '', { requestId: 1,
    clientRequestId: 'c0ffee00-0000-4000-8000-000000000006', date: new Date(now).toUTCString() });
  const request2 = signedRequest(authority, deadline, 'GET', '', { requestId: 2,
    clientRequestId: 'c0ffee00-0000-4000-8000-000000000007', date: new Date(now + 2_000).toUTCString() });
  const changedStableHeader = signedRequest(authority, deadline, 'GET', '', { requestId: 3,
    clientRequestId: 'c0ffee00-0000-4000-8000-000000000008', date: new Date(now + 3_000).toUTCString(),
    extraHeaders: [['user-agent', 'changed-marker']] });
  const envelope = envelopeFor(authority, request1, 3), custody = mutableCustodyStore(authority, now);
  let forwards = 0;
  const azure = { c: { account: authority.storage_account }, async token() { return 'offline-test-token'; } } as unknown as Azure;
  const ledgerPath = path.join(directory, 'storage-ledger.jsonl');
  const dispatcher = new StorageBridgeDispatcher({ authority, originalDeadlineUnixMs: deadline, envelope, azure,
    store: custody.store, phase: 'owned-run-table-initialization', planIdentity: { run_id: authority.cell_run_id },
    ledgerPath, fetcher: async () => { forwards++; return new Response('ok', { status: 200 }); } });
  const boundary = new EffectBoundary(envelope, path.join(directory, 'effects.jsonl'));
  const first = await withEffectBoundary(boundary, () => dispatcher.exchange(request1));
  const second = await withEffectBoundary(boundary, () => dispatcher.exchange(request2));
  const changed = await withEffectBoundary(boundary, () => dispatcher.exchange(changedStableHeader));
  assert.equal(first.outcome, 'response');
  assert.equal(second.outcome, 'response');
  assert.equal(changed.outcome, 'definitely_not_sent');
  assert.equal(forwards, 2);
  const rows = readFileSync(ledgerPath, 'utf8').trim().split('\n').map(line => JSON.parse(line) as Record<string, unknown>);
  const responseRows = rows.filter(row => row.state === 'response');
  assert.equal(responseRows.length, 3);
  assert.deepEqual((responseRows[0]!.request_evidence as { headers: [string, string][] }).headers, request1.headers);
  assert.deepEqual((responseRows[1]!.request_evidence as { headers: [string, string][] }).headers, request2.headers);
  assert.deepEqual((responseRows[2]!.request_evidence as { headers: [string, string][] }).headers, changedStableHeader.headers);
});

test('StorageBridge rejects malformed volatile headers before durable request admission', async t => {
  const directory = mkdtempSync(path.join(os.tmpdir(), 'oat-d47-invalid-headers-'));
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
  const now = Date.now(), deadline = now + 60_000, request = signedRequest(authority, deadline);
  const envelope = envelopeFor(authority, request), custody = mutableCustodyStore(authority, now);
  let tokens = 0, forwards = 0;
  const azure = { c: { account: authority.storage_account }, async token() { tokens++; return 'offline-test-token'; } } as unknown as Azure;
  const ledgerPath = path.join(directory, 'storage-ledger.jsonl');
  const dispatcher = new StorageBridgeDispatcher({ authority, originalDeadlineUnixMs: deadline, envelope, azure,
    store: custody.store, phase: 'owned-run-table-initialization', planIdentity: { run_id: authority.cell_run_id },
    ledgerPath, fetcher: async () => { forwards++; return new Response('', { status: 200 }); } });
  const validDate = new Date(now).toUTCString();
  const invalidCases: Array<{ headers: [string, string][]; error: RegExp }> = [
    { headers: request.headers.filter(([name]) => name.toLowerCase() !== 'x-ms-client-request-id'), error: /one canonical lowercase UUID v4/ },
    { headers: [...request.headers, ['X-MS-CLIENT-REQUEST-ID', 'c0ffee00-0000-4000-8000-000000000006']], error: /duplicated or not permitted/ },
    { headers: request.headers.map(([name, value]) => name.toLowerCase() === 'x-ms-client-request-id' ? [name, value.toUpperCase()] : [name, value]), error: /one canonical lowercase UUID v4/ },
    { headers: request.headers.map(([name, value]) => name.toLowerCase() === 'date' ? [name, 'not-a-date'] : [name, value]), error: /current IMF-fixdate/ },
    { headers: request.headers.map(([name, value]) => name.toLowerCase() === 'date' ? [name, new Date(now - 600_000).toUTCString()] : [name, value]), error: /current IMF-fixdate/ },
    { headers: [...request.headers, ['DATE', validDate]], error: /duplicated or not permitted/ },
  ];
  for (const invalid of invalidCases) {
    await assert.rejects(dispatcher.exchange(resignRequest(request, invalid.headers)), invalid.error);
  }
  assert.equal(tokens, 0);
  assert.equal(forwards, 0);
  assert.equal(readFileSync(ledgerPath, 'utf8').trim().split('\n').length, 1);
});

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
