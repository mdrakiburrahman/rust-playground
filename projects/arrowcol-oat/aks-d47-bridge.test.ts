import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { chmodSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { test } from 'node:test';
import { EffectBoundary, withEffectBoundary, type EffectEnvelope } from '../../infra/azure/effect-boundary.js';
import { initializeOwnedRunTable, PLANNED_CONTROL_OPERATION_IDS, StorageBridgeDispatcher, type StorageBridgeAuthority, type StorageBridgeRequest } from './aks-d47-bridge.js';
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
  options: { requestId?: number; clientRequestId?: string; date?: string; extraHeaders?: [string, string][]; url?: string } = {}): StorageBridgeRequest {
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
    url: options.url ?? (`https://${authority.storage_account}.blob.core.windows.net/${new URL(authority.table_root_url).username}` +
      `${new URL(authority.table_root_url).pathname}_delta_log/00000000000000000000.json`),
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

function signedControlRequest(authority: StorageBridgeAuthority, deadline: number, requestId: number, url: string,
  headers: [string, string][], method = 'PUT', operationId?: string): StorageBridgeRequest {
  const volatile: [string, string][] = [
    ['x-ms-client-request-id', 'c0ffee00-0000-4000-8000-' + String(requestId).padStart(12, '0')],
    ['date', new Date(deadline - 30_000).toUTCString()], ['x-ms-date', new Date(deadline - 30_000).toUTCString()],
  ];
  const unsigned = { schema_version: 1 as const, kind: 'storage_http_request' as const, request_id: requestId,
    authority, original_deadline_unix_ms: deadline, remaining_timeout_ms: 10_000, target: 'control' as const,
    method, url, headers: ([...headers, ...(operationId ? [['x-arrowcol-operation-id', operationId] as [string, string]] : []),
      ...volatile, ['x-ms-version', '2023-11-03'], ...(method === 'PUT' ? [['content-length', '0'] as [string, string]] : [])] as [string, string][]),
    body: '', body_sha256: sha('') };
  return { ...unsigned, request_sha256: sha(canonical(unsigned)) };
}

function envelopeForRequests(authority: StorageBridgeAuthority, requests: StorageBridgeRequest[]): EffectEnvelope {
  const effects = requests.map((request, index) => {
    const stableHeaders = request.headers.filter(([name]) => !['x-ms-client-request-id', 'date', 'x-ms-date'].includes(name.toLowerCase()));
    const headersHash = sha(canonical(stableHeaders)), url = new URL(request.url);
    return { id: 'storage-request-' + index, transport: 'azure-rest' as const, method: request.method,
      resource: url.origin + '|https://storage.azure.com/',
      path: url.pathname + url.search + '|headers-sha256=' + headersHash, max_calls: 1, body_sha256: sha(request.body) };
  });
  return { schema_version: 1, campaign_id: authority.campaign_id, run_id: authority.cell_run_id,
    subscription_id: 'c0ffee00-0000-4000-8000-000000000005', resource_group: 'offline',
    storage_account: authority.storage_account, state_container: 'checkpoints', effects };
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

test('StorageBridge accepts the approved Delta log list cursors', async t => {
  const directory = mkdtempSync(path.join(os.tmpdir(), 'oat-d47-list-cursors-'));
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
  const prefix = new URL(authority.table_root_url).pathname.slice(1) + '_delta_log/';
  const cursors = [prefix.slice(0, -1), prefix + '00000000000000000000'];
  for (const [index, startFrom] of cursors.entries()) {
    const query = new URLSearchParams([['restype', 'container'], ['comp', 'list'], ['prefix', prefix], ['startFrom', startFrom]]);
    const request = signedRequest(authority, deadline, 'GET', '', { requestId: 1,
      url: 'https://arrowcollector68590.blob.core.windows.net/checkpoints?' + query.toString() });
    const custody = mutableCustodyStore(authority, now), ledgerPath = path.join(directory, 'storage-' + index + '.jsonl');
    const azure = { c: { account: authority.storage_account }, async token() { return 'offline-test-token'; } } as unknown as Azure;
    const dispatcher = new StorageBridgeDispatcher({ authority, originalDeadlineUnixMs: deadline, envelope: envelopeFor(authority, request),
      azure, store: custody.store, phase: 'owned-run-table-initialization', planIdentity: { run_id: authority.cell_run_id },
      ledgerPath, fetcher: async () => new Response('<EnumerationResults><Blobs></Blobs><NextMarker></NextMarker></EnumerationResults>',
        { status: 200, headers: { 'content-type': 'application/xml' } }) });
    const boundary = new EffectBoundary(envelopeFor(authority, request), path.join(directory, 'effects-' + index + '.jsonl'));
    const result = await withEffectBoundary(boundary, () => dispatcher.exchange(request));
    assert.equal(result.outcome, 'response');
  }
});

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
test('planned control identities admit the exact 11-operation registration and clean-release trace', async t => {
  const directory = mkdtempSync(path.join(os.tmpdir(), 'oat-d47-planned-ids-'));
  chmodSync(directory, 0o700);
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const authority: StorageBridgeAuthority = {
    campaign_id: 'c0ffee00-0000-4000-8000-000000000001', cell_run_id: 'c0ffee00-0000-4000-8000-000000000002',
    owner_id: 'c0ffee00-0000-4000-8000-000000000003', fencing_token: 1, storage_account: 'arrowcollector68590',
    table_root_url: 'abfss://checkpoints@arrowcollector68590.dfs.core.windows.net/campaigns/c0ffee00-0000-4000-8000-000000000001/runs/c0ffee00-0000-4000-8000-000000000002/dataflow/telemetry/sqlserver/sqlserver_availability_replica_states/',
    control_container_url: 'https://arrowcollector68590.blob.core.windows.net/checkpoints', sandbox_id: 'offline-stamp',
    reset_generation: 'c0ffee00-0000-4000-8000-000000000004',
  };
  const now = Date.now(), deadline = now + 60_000, publisherId = 'c0ffee00-0000-4000-8000-000000000011';
  const leaseIds = ['c0ffee00-0000-4000-8000-000000000012', 'c0ffee00-0000-4000-8000-000000000013'] as const;
  const controlPlan = { publisher_id: publisherId, generation: authority.reset_generation, control_lease_ids: leaseIds,
    control_operation_ids: PLANNED_CONTROL_OPERATION_IDS } as const;
  const baseUrl = authority.control_container_url, leaseUrl = baseUrl + '?comp=lease&restype=container';
  const metadataUrl = baseUrl + '?restype=container&comp=metadata';
  const read = (id: number, index: number) => signedControlRequest(authority, deadline, id,
    baseUrl + '?restype=container', [], 'HEAD', PLANNED_CONTROL_OPERATION_IDS[index]);
  const request = (id: number, index: number, action: 'acquire' | 'metadata' | 'release', leaseId: string,
    state?: 'active' | 'uncertain' | 'removed') => {
    if (action === 'acquire') return signedControlRequest(authority, deadline, id, leaseUrl,
      [['x-ms-lease-action', 'acquire'], ['x-ms-lease-duration', '15'], ['x-ms-proposed-lease-id', leaseId]],
      'PUT', PLANNED_CONTROL_OPERATION_IDS[index]);
    if (action === 'release') return signedControlRequest(authority, deadline, id, leaseUrl,
      [['x-ms-lease-action', 'release'], ['x-ms-lease-id', leaseId]], 'PUT', PLANNED_CONTROL_OPERATION_IDS[index]);
    const registry = state === 'removed' ? '{}' : JSON.stringify({ [publisherId]: { generation: authority.reset_generation,
      state: state === 'uncertain' ? 'uncertain' : 'active' } });
    return signedControlRequest(authority, deadline, id, metadataUrl,
      [['x-ms-lease-id', leaseId], ['x-ms-meta-publishers', registry]], 'PUT', PLANNED_CONTROL_OPERATION_IDS[index]);
  };
  const requests = [read(1, 0), request(2, 1, 'acquire', leaseIds[0]), read(3, 2),
    request(4, 3, 'metadata', leaseIds[0], 'active'), request(5, 4, 'release', leaseIds[0]), read(6, 5), read(7, 6),
    request(8, 7, 'acquire', leaseIds[1]), read(9, 8), request(10, 9, 'metadata', leaseIds[1], 'removed'),
    request(11, 10, 'release', leaseIds[1])];
  const envelope = envelopeForRequests(authority, requests), custody = mutableCustodyStore(authority, now);
  const azure = { c: { account: authority.storage_account }, async token() { return 'offline-test-token'; } } as unknown as Azure;
  const statuses = [200, 201, 200, 200, 200, 200, 200, 201, 200, 200, 200];
  let forwards = 0;
  const dispatcher = new StorageBridgeDispatcher({ authority, originalDeadlineUnixMs: deadline, envelope, azure,
    store: custody.store, ledgerPath: path.join(directory, 'storage-ledger.jsonl'), controlPlan,
    planIdentity: { plan: { publisher_id: publisherId, generation: authority.reset_generation,
      control_lease_ids: leaseIds, control_operation_ids: PLANNED_CONTROL_OPERATION_IDS } },
    fetcher: async () => new Response('', { status: statuses[forwards++]! }) });
  const boundary = new EffectBoundary(envelope, path.join(directory, 'effects.jsonl'));
  for (const item of requests) assert.equal((await withEffectBoundary(boundary, () => dispatcher.exchange(item))).outcome, 'response');
  assert.equal(forwards, 11);
  assert.equal(dispatcher.recordedRequestCount, 11);
  assert.doesNotThrow(() => dispatcher.assertControlPlanComplete());
});
test('planned control dispatcher rejects missing, duplicate, unknown, reused and mutated markers before forwarding', async t => {
  const directory = mkdtempSync(path.join(os.tmpdir(), 'oat-d47-planned-refusal-'));
  chmodSync(directory, 0o700);
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const authority: StorageBridgeAuthority = {
    campaign_id: 'c0ffee00-0000-4000-8000-000000000001', cell_run_id: 'c0ffee00-0000-4000-8000-000000000002',
    owner_id: 'c0ffee00-0000-4000-8000-000000000003', fencing_token: 1, storage_account: 'arrowcollector68590',
    table_root_url: 'abfss://checkpoints@arrowcollector68590.dfs.core.windows.net/campaigns/c0ffee00-0000-4000-8000-000000000001/runs/c0ffee00-0000-4000-8000-000000000002/dataflow/telemetry/sqlserver/sqlserver_availability_replica_states/',
    control_container_url: 'https://arrowcollector68590.blob.core.windows.net/checkpoints', sandbox_id: 'offline-stamp',
    reset_generation: 'c0ffee00-0000-4000-8000-000000000004',
  };
  const now = Date.now(), deadline = now + 60_000, publisherId = 'c0ffee00-0000-4000-8000-000000000011';
  const leaseIds = ['c0ffee00-0000-4000-8000-000000000012', 'c0ffee00-0000-4000-8000-000000000013'] as const;
  const controlPlan = { publisher_id: publisherId, generation: authority.reset_generation, control_lease_ids: leaseIds,
    control_operation_ids: PLANNED_CONTROL_OPERATION_IDS } as const;
  const base = authority.control_container_url, headUrl = base + '?restype=container';
  const makeHead = (id: number, marker: string | undefined, extra: [string, string][] = []) => {
    const request = signedControlRequest(authority, deadline, id, headUrl, extra, 'HEAD', marker);
    return request;
  };
  const expected = makeHead(1, PLANNED_CONTROL_OPERATION_IDS[0]);
  const missing = makeHead(1, undefined);
  const duplicate = resignRequest(expected, [...expected.headers, ['x-arrowcol-operation-id', PLANNED_CONTROL_OPERATION_IDS[0]]]);
  const unknown = makeHead(1, 'registration-read-unplanned');
  const reused = makeHead(2, PLANNED_CONTROL_OPERATION_IDS[0], [['x-ms-version', '2023-11-04']]);
  const mutated = makeHead(1, PLANNED_CONTROL_OPERATION_IDS[0], [['x-ms-version', '2023-11-04']]);
  const variants = [missing, duplicate, unknown, reused, mutated];
  for (const [index, invalid] of variants.entries()) {
    const caseDir = path.join(directory, String(index));
    const envelope = envelopeForRequests(authority, [expected, invalid]);
    const custody = mutableCustodyStore(authority, now), azure = { c: { account: authority.storage_account },
      async token() { return 'offline-test-token'; } } as unknown as Azure;
    let forwards = 0;
    const dispatcher = new StorageBridgeDispatcher({ authority, originalDeadlineUnixMs: deadline, envelope, azure,
      store: custody.store, ledgerPath: path.join(caseDir, 'storage-ledger.jsonl'), controlPlan,
      planIdentity: { plan: controlPlan }, fetcher: async () => { forwards++; return new Response(null, { status: 200 }); } });
    const boundary = new EffectBoundary(envelope, path.join(caseDir, 'effects.jsonl'));
    await assert.rejects(withEffectBoundary(boundary, () => dispatcher.exchange(invalid)));
    assert.equal(forwards, 0);
    assert.equal(dispatcher.recordedRequestCount, 0);
  }
});
test('planned control restart serves an exact cached reply without forwarding again', async t => {
  const directory = mkdtempSync(path.join(os.tmpdir(), 'oat-d47-planned-restart-'));
  chmodSync(directory, 0o700);
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const authority: StorageBridgeAuthority = {
    campaign_id: 'c0ffee00-0000-4000-8000-000000000001', cell_run_id: 'c0ffee00-0000-4000-8000-000000000002',
    owner_id: 'c0ffee00-0000-4000-8000-000000000003', fencing_token: 1, storage_account: 'arrowcollector68590',
    table_root_url: 'abfss://checkpoints@arrowcollector68590.dfs.core.windows.net/campaigns/c0ffee00-0000-4000-8000-000000000001/runs/c0ffee00-0000-4000-8000-000000000002/dataflow/telemetry/sqlserver/sqlserver_availability_replica_states/',
    control_container_url: 'https://arrowcollector68590.blob.core.windows.net/checkpoints', sandbox_id: 'offline-stamp',
    reset_generation: 'c0ffee00-0000-4000-8000-000000000004',
  };
  const now = Date.now(), deadline = now + 60_000, publisherId = 'c0ffee00-0000-4000-8000-000000000011';
  const leaseIds = ['c0ffee00-0000-4000-8000-000000000012', 'c0ffee00-0000-4000-8000-000000000013'] as const;
  const controlPlan = { publisher_id: publisherId, generation: authority.reset_generation, control_lease_ids: leaseIds,
    control_operation_ids: PLANNED_CONTROL_OPERATION_IDS } as const;
  const read = signedControlRequest(authority, deadline, 1, authority.control_container_url + '?restype=container', [], 'HEAD',
    PLANNED_CONTROL_OPERATION_IDS[0]);
  const acquire = signedControlRequest(authority, deadline, 2, authority.control_container_url + '?comp=lease&restype=container',
    [['x-ms-lease-action', 'acquire'], ['x-ms-lease-duration', '15'], ['x-ms-proposed-lease-id', leaseIds[0]]], 'PUT',
    PLANNED_CONTROL_OPERATION_IDS[1]);
  const envelope = envelopeForRequests(authority, [read, acquire]), custody = mutableCustodyStore(authority, now);
  const azure = { c: { account: authority.storage_account }, async token() { return 'offline-test-token'; } } as unknown as Azure;
  const ledgerPath = path.join(directory, 'storage-ledger.jsonl');
  let forwards = 0;
  const create = () => new StorageBridgeDispatcher({ authority, originalDeadlineUnixMs: deadline, envelope, azure,
    store: custody.store, ledgerPath, controlPlan, planIdentity: { plan: controlPlan },
    fetcher: async () => { forwards++; return new Response(null, { status: 200 }); } });
  const boundary = new EffectBoundary(envelope, path.join(directory, 'effects.jsonl'));
  const first = create(), initial = await withEffectBoundary(boundary, () => first.exchange(read));
  assert.equal(initial.outcome, 'response');
  const restarted = create();
  assert.deepEqual(await withEffectBoundary(boundary, () => restarted.exchange(read)), initial);
  await assert.rejects(withEffectBoundary(boundary, () => restarted.exchange(acquire)), /recovered bridge ledger permits cached replies and bounded reads only/);
  assert.equal(forwards, 1);
});
test('real loadgen initializer composes with the Node BR3 planned-identity parent', { skip: !process.env.OAT_BR3_LOADGEN_BIN }, async t => {
  const executable = process.env.OAT_BR3_LOADGEN_BIN!;
  const directory = mkdtempSync(path.join(os.tmpdir(), 'oat-d47-real-composition-'));
  chmodSync(directory, 0o700);
  t.after(() => rmSync(directory, { recursive: true, force: true }));
  const now = Date.now(), deadline = now + 300_000;
  const campaign = 'c0ffee00-0000-4000-8000-000000000001', run = 'c0ffee00-0000-4000-8000-000000000002';
  const owner = 'c0ffee00-0000-4000-8000-000000000003', generation = 'c0ffee00-0000-4000-8000-000000000004';
  const publisherId = 'c0ffee00-0000-4000-8000-000000000011';
  const controlLeaseIds = ['c0ffee00-0000-4000-8000-000000000012', 'c0ffee00-0000-4000-8000-000000000013'] as const;
  const account = 'arrowcollector68590', container = 'checkpoints';
  const campaignRoot = 'abfss://' + container + '@' + account + '.dfs.core.windows.net/campaigns/' + campaign + '/';
  const runRoot = campaignRoot + 'runs/' + run + '/';
  const tableRoot = runRoot + 'dataflow/telemetry/sqlserver/sqlserver_availability_replica_states/';
  const authority: StorageBridgeAuthority = { campaign_id: campaign, cell_run_id: run, owner_id: owner, fencing_token: 1,
    storage_account: account, table_root_url: tableRoot, control_container_url: 'https://' + account + '.blob.core.windows.net/' + container,
    sandbox_id: 'offline-stamp', reset_generation: generation };
  const plan = { account, container, campaign_root: campaignRoot, run_root: runRoot, campaign_id: campaign, run_id: run,
    generation, publisher_id: publisherId, control_lease_ids: [...controlLeaseIds],
    control_operation_ids: [...PLANNED_CONTROL_OPERATION_IDS],
    table: 'telemetry.sqlserver.sqlserver_availability_replica_states', partition_by_ingestion_date: true,
    owner_id: owner, fencing_token: 1 };
  const planPath = path.join(directory, 'plan.json'), bridgeInputPath = path.join(directory, 'bridge-input.json');
  writeFileSync(planPath, JSON.stringify(plan)); chmodSync(planPath, 0o600);
  writeFileSync(bridgeInputPath, JSON.stringify({ authority, original_deadline_unix_ms: deadline })); chmodSync(bridgeInputPath, 0o600);
  const firstHead = signedControlRequest(authority, deadline, 1,
    authority.control_container_url + '?restype=container', [], 'HEAD');
  const acquire = signedControlRequest(authority, deadline, 2,
    authority.control_container_url + '?comp=lease&restype=container',
    [['x-ms-lease-action', 'acquire'], ['x-ms-lease-duration', '15'], ['x-ms-proposed-lease-id', controlLeaseIds[0]]]);
  const secondHead = signedControlRequest(authority, deadline, 3,
    authority.control_container_url + '?restype=container', [], 'HEAD');
  const envelope = envelopeForRequests(authority, [firstHead, acquire, secondHead]);
  const custody = mutableCustodyStore(authority, now);
  const azure = { c: { account }, async token() { return 'offline-test-token'; } } as unknown as Azure;
  const ledgerPath = path.join(directory, 'storage-ledger.jsonl');
  const options = { executable, artifactIdentity: { source_commit: '2d329ab907f15c8123a08b933c01d98670bfd755',
    executable_sha256: createHash('sha256').update(readFileSync(executable)).digest('hex') }, planPath, bridgeInputPath,
    ledgerPath, rootDirectory: process.cwd(), envelope, store: custody.store, azure,
    fetcher: async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = new URL(String(input)), headers = new Headers(init?.headers);
      if (init?.method === 'HEAD') return new Response(null, { status: 200, headers: { etag: '\"offline-etag\"',
        'x-ms-meta-sandboxid': 'offline-stamp', 'x-ms-meta-resetstate': 'ready-v1', 'x-ms-meta-resetgeneration': generation,
        'x-ms-meta-publisherregistryversion': '1', 'x-ms-meta-publishers': '{}', 'x-ms-meta-occupancy': 'released' } });
      if (url.searchParams.get('comp') === 'lease' && headers.get('x-ms-lease-action') === 'acquire')
        return new Response(null, { status: 201, headers: { 'x-ms-lease-id': headers.get('x-ms-proposed-lease-id')! } });
      if (init?.method === 'PUT') return new Response(null, { status: 200 });
      return new Response('', { status: 500 });
    }, now: Date.now };
  const boundary = new EffectBoundary(envelope, path.join(directory, 'effects.jsonl'));
  if (process.env.OAT_BR3_DISCOVER === '1') {
    try { await withEffectBoundary(boundary, () => initializeOwnedRunTable(options)); }
    catch (error) { console.log('BR3_DISCOVERY_ERROR', String(error)); }
    console.log('BR3_DISCOVERY_LEDGER', readFileSync(ledgerPath, 'utf8'));
    return;
  }
  await withEffectBoundary(boundary, () => initializeOwnedRunTable(options));
});
