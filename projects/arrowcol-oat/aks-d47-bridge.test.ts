import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { chmodSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import { execFileSync } from 'node:child_process';
import os from 'node:os';
import path from 'node:path';
import { test } from 'node:test';
import { EffectBoundary, withEffectBoundary, type EffectEnvelope } from '../../infra/azure/effect-boundary.js';
import { initializeOwnedRunTable, PLANNED_CONTROL_OPERATION_IDS, PLANNED_STORAGE_OPERATION_IDS,
  runFixtureSetupChild, StorageBridgeDispatcher, type FixtureDispatcherOptions, type FixtureSetupInput,
  type StorageBridgeAuthority, type StorageBridgeRequest } from './aks-d47-bridge.js';
import type { Azure } from '../../infra/azure/bootstrap.js';
import { createBlobCampaignStore, newCampaignState, publishRunInventory, runInventoryPublishReceipt,
  type CampaignStore, type RunInventory, type StoredCampaign } from './aks-campaign.js';

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

function sortedHeaderPairs(headers: [string, string][]): [string, string][] {
  return [...headers].sort(([leftName, leftValue], [rightName, rightValue]) => leftName < rightName ? -1 : leftName > rightName ? 1 :
    leftValue < rightValue ? -1 : leftValue > rightValue ? 1 : 0);
}

function signedControlRequest(authority: StorageBridgeAuthority, deadline: number, requestId: number, url: string,
  headers: [string, string][], method = 'PUT', operationId?: string): StorageBridgeRequest {
  const volatile: [string, string][] = [
    ['x-ms-client-request-id', 'c0ffee00-0000-4000-8000-' + String(requestId).padStart(12, '0')],
    ['date', new Date(deadline - 30_000).toUTCString()], ['x-ms-date', new Date(deadline - 30_000).toUTCString()],
  ];
  const unsigned = { schema_version: 1 as const, kind: 'storage_http_request' as const, request_id: requestId,
    authority, original_deadline_unix_ms: deadline, remaining_timeout_ms: 10_000, target: 'control' as const,
    method, url, headers: sortedHeaderPairs([...headers, ...(operationId ? [['x-arrowcol-operation-id', operationId] as [string, string]] : []),
      ...volatile, ['x-ms-version', '2023-11-03'], ...(method === 'PUT' ? [['content-length', '0'] as [string, string]] : [])] as [string, string][]),
    body: '', body_sha256: sha('') };
  return { ...unsigned, request_sha256: sha(canonical(unsigned)) };
}

function signedObjectRequest(authority: StorageBridgeAuthority, deadline: number, requestId: number, method: string, url: string, body = '', extraHeaders: [string, string][] = []): StorageBridgeRequest {
  const unsigned = { schema_version: 1 as const, kind: 'storage_http_request' as const, request_id: requestId, authority,
    original_deadline_unix_ms: deadline, remaining_timeout_ms: 10_000, target: 'object_store' as const, method, url,
    headers: sortedHeaderPairs([['date', new Date(deadline - 30_000).toUTCString()], ['x-ms-version', '2023-11-03'],
      ['user-agent', 'arrowcol-qap/' + authority.cell_run_id + '/owned-run-table-initialization'],
      ['x-ms-client-request-id', 'c0ffee00-0000-4000-8000-' + String(requestId).padStart(12, '0')], ...extraHeaders] as [string, string][]),
    body, body_sha256: sha(body) };
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
test('planned control identities admit the exact 13-operation admission, create and clean-release trace', async t => {
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
    control_operation_ids: PLANNED_CONTROL_OPERATION_IDS, storage_operation_ids: PLANNED_STORAGE_OPERATION_IDS } as const;
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
    read(8, 7), read(9, 8), request(10, 9, 'acquire', leaseIds[1]), read(11, 10),
    request(12, 11, 'metadata', leaseIds[1], 'removed'), request(13, 12, 'release', leaseIds[1])];
  const envelope = envelopeForRequests(authority, requests), custody = mutableCustodyStore(authority, now);
  const azure = { c: { account: authority.storage_account }, async token() { return 'offline-test-token'; } } as unknown as Azure;
  const statuses = [200, 201, 200, 200, 200, 200, 200, 200, 200, 201, 200, 200, 200];
  let forwards = 0;
  const dispatcher = new StorageBridgeDispatcher({ authority, originalDeadlineUnixMs: deadline, envelope, azure,
    store: custody.store, ledgerPath: path.join(directory, 'storage-ledger.jsonl'), controlPlan,
    planIdentity: { plan: { publisher_id: publisherId, generation: authority.reset_generation,
      control_lease_ids: leaseIds, control_operation_ids: PLANNED_CONTROL_OPERATION_IDS, storage_operation_ids: PLANNED_STORAGE_OPERATION_IDS } },
    fetcher: async () => new Response('', { status: statuses[forwards++]! }) });
  const boundary = new EffectBoundary(envelope, path.join(directory, 'effects.jsonl'));
  for (const item of requests) assert.equal((await withEffectBoundary(boundary, () => dispatcher.exchange(item))).outcome, 'response');
  assert.equal(forwards, 13);
  assert.equal(dispatcher.recordedRequestCount, 13);
  assert.doesNotThrow(() => dispatcher.assertControlPlanComplete());
  assert.throws(() => dispatcher.assertDeltaCommitReadback({ table_id: 'c0ffee00-0000-4000-8000-000000000099',
    version: 0, schema_json: '{}', partition_columns: ['__arrowcol_ingestion'], active_files: 0 },
    { delta_v0: { body: '', body_sha256: '', timestamp: 0, txn_id: '', table_id: '', created_time: 0 } }),
  /exactly one actual version-zero commit request/);
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
    control_operation_ids: PLANNED_CONTROL_OPERATION_IDS, storage_operation_ids: PLANNED_STORAGE_OPERATION_IDS } as const;
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
    control_operation_ids: PLANNED_CONTROL_OPERATION_IDS, storage_operation_ids: PLANNED_STORAGE_OPERATION_IDS } as const;
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
test('Delta terminal proof correlates the actual accepted commit with an identical successful readback', async t => {
  const root = mkdtempSync(path.join(os.tmpdir(), 'oat-d47-delta-proof-'));
  chmodSync(root, 0o700);
  t.after(() => rmSync(root, { recursive: true, force: true }));
  const authority: StorageBridgeAuthority = {
    campaign_id: 'c0ffee00-0000-4000-8000-000000000001', cell_run_id: 'c0ffee00-0000-4000-8000-000000000002',
    owner_id: 'c0ffee00-0000-4000-8000-000000000003', fencing_token: 1, storage_account: 'arrowcollector68590',
    table_root_url: 'abfss://checkpoints@arrowcollector68590.dfs.core.windows.net/campaigns/c0ffee00-0000-4000-8000-000000000001/runs/c0ffee00-0000-4000-8000-000000000002/dataflow/telemetry/sqlserver/sqlserver_availability_replica_states/',
    control_container_url: 'https://arrowcollector68590.blob.core.windows.net/checkpoints', sandbox_id: 'offline-stamp',
    reset_generation: 'c0ffee00-0000-4000-8000-000000000004',
  };
  const now = Date.now(), deadline = now + 60_000, tableId = 'c0ffee00-0000-4000-8000-000000000014';
  const schemaJson = JSON.stringify({ type: 'struct', fields: [{ name: '__arrowcol_ingestion', type: 'string', nullable: false, metadata: {} }] });
  const txnId = 'c0ffee00-0000-4000-8000-000000000015';
  const body = [JSON.stringify({ commitInfo: { timestamp: 1, txnId } }),
    JSON.stringify({ protocol: { minReaderVersion: 3, minWriterVersion: 7, readerFeatures: [], writerFeatures: ['invariants'] } }),
    JSON.stringify({ metaData: { id: tableId, format: { provider: 'parquet', options: {} }, schemaString: schemaJson,
      partitionColumns: ['__arrowcol_ingestion'], configuration: {}, createdTime: 1 } })].join('\n') + '\n';
  const deltaPlan = { body, body_sha256: sha(body), timestamp: 1, txn_id: txnId, table_id: tableId, created_time: 1 };
  const url = `https://${authority.storage_account}.blob.core.windows.net/checkpoints${new URL(authority.table_root_url).pathname}_delta_log/00000000000000000000.json`;
  const makeDispatcher = (statuses: number[], includeReadback: boolean) => {
    const directory = mkdtempSync(path.join(root, 'case-'));
    chmodSync(directory, 0o700);
    const rawCommit = signedObjectRequest(authority, deadline, 1, 'PUT', url, body);
    const commit = resignRequest(rawCommit, [...rawCommit.headers, ['if-none-match', '*'], ['content-type', 'application/json']]);
    const readback = signedObjectRequest(authority, deadline, 2, 'GET', url);
    const requests = includeReadback ? [commit, readback] : [commit];
    const envelope = envelopeForRequests(authority, requests), custody = mutableCustodyStore(authority, now);
    const azure = { c: { account: authority.storage_account }, async token() { return 'offline-test-token'; } } as unknown as Azure;
    let forwarded = 0;
    const dispatcher = new StorageBridgeDispatcher({ authority, originalDeadlineUnixMs: deadline, envelope, azure,
      store: custody.store, ledgerPath: path.join(directory, 'storage-ledger.jsonl'),
      fetcher: async () => {
        const status = statuses[forwarded++];
        if (status === undefined) throw new Error('Unexpected provider request.');
        return new Response(status === 200 && forwarded > 1 ? body : null, { status });
      } });
    const boundary = new EffectBoundary(envelope, path.join(directory, 'effects.jsonl'));
    return { dispatcher, boundary, requests };
  };
  const expected = { table_id: tableId, version: 0, schema_json: schemaJson,
    partition_columns: ['__arrowcol_ingestion'], active_files: 0 };
  const success = makeDispatcher([201, 200], true);
  for (const request of success.requests)
    assert.equal((await withEffectBoundary(success.boundary, () => success.dispatcher.exchange(request))).outcome, 'response');
  assert.doesNotThrow(() => success.dispatcher.assertDeltaCommitReadback(expected, { delta_v0: deltaPlan }));
  assert.throws(() => success.dispatcher.assertDeltaCommitReadback({ ...expected,
    table_id: 'c0ffee00-0000-4000-8000-000000000099' }, { delta_v0: deltaPlan }), /differs from the actual committed table identity/);
  assert.throws(() => success.dispatcher.assertDeltaCommitReadback({ ...expected,
    schema_json: JSON.stringify({ type: 'struct', fields: [] }) }, { delta_v0: deltaPlan }), /differs from the actual committed table identity/);
  const alteredProtocolBody = body.replace('"minWriterVersion":7', '"minWriterVersion":8');
  assert.notEqual(alteredProtocolBody, body);
  assert.throws(() => success.dispatcher.assertDeltaCommitReadback(expected, { delta_v0: { ...deltaPlan,
    body: alteredProtocolBody, body_sha256: sha(alteredProtocolBody) } }), /exact plan-time body/);
  const incomplete = makeDispatcher([201], false);
  await withEffectBoundary(incomplete.boundary, () => incomplete.dispatcher.exchange(incomplete.requests[0]));
  assert.throws(() => incomplete.dispatcher.assertDeltaCommitReadback(expected, { delta_v0: deltaPlan }), /missing the actual version-zero log readback/);
  const failedReadback = makeDispatcher([201, 503], true);
  for (const request of failedReadback.requests)
    await withEffectBoundary(failedReadback.boundary, () => failedReadback.dispatcher.exchange(request));
  assert.throws(() => failedReadback.dispatcher.assertDeltaCommitReadback(expected, { delta_v0: deltaPlan }), /readback is incomplete, failed or differs/);
});

test('real loadgen initializer composes with the Node BR3 planned-identity parent', { skip: !process.env.OAT_BR3_LOADGEN_BIN }, async t => {
  const executable = process.env.OAT_BR3_LOADGEN_BIN!;
  const directory = mkdtempSync(path.join(os.tmpdir(), 'oat-d47-real-composition-'));
  chmodSync(directory, 0o700);
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
  const preliminaryPlan = { account, container, campaign_root: campaignRoot, run_root: runRoot, campaign_id: campaign, run_id: run,
    generation, publisher_id: publisherId, control_lease_ids: [...controlLeaseIds],
    control_operation_ids: [...PLANNED_CONTROL_OPERATION_IDS],
    storage_operation_ids: [...PLANNED_STORAGE_OPERATION_IDS],
    table: 'telemetry.sqlserver.sqlserver_availability_replica_states', partition_by_ingestion_date: true,
    owner_id: owner, fencing_token: 1 };
  const preliminaryPlanPath = path.join(directory, 'preliminary-plan.json');
  const planPath = path.join(directory, 'plan.json'), bridgeInputPath = path.join(directory, 'bridge-input.json');
  writeFileSync(preliminaryPlanPath, JSON.stringify(preliminaryPlan)); chmodSync(preliminaryPlanPath, 0o600);
  execFileSync(executable, ['plan-owned-run-table', '--plan', preliminaryPlanPath, '--output', planPath], { cwd: process.cwd(), stdio: 'pipe' });
  chmodSync(planPath, 0o600);
  const plan = JSON.parse(readFileSync(planPath, 'utf8')) as typeof preliminaryPlan & { delta_v0: { body: string; body_sha256: string; timestamp: number; txn_id: string; table_id: string; created_time: number } };
  writeFileSync(bridgeInputPath, JSON.stringify({ authority, original_deadline_unix_ms: deadline })); chmodSync(bridgeInputPath, 0o600);
  const controlBase = authority.control_container_url;
  const controlLeaseUrl = controlBase + '?comp=lease&restype=container';
  const controlMetadataUrl = controlBase + '?restype=container&comp=metadata';
  const controlRead = (id: number, index: number) => signedControlRequest(authority, deadline, id,
    controlBase + '?restype=container', [], 'HEAD', PLANNED_CONTROL_OPERATION_IDS[index]);
  const controlAcquire = (id: number, index: number, leaseId: string) => signedControlRequest(authority, deadline, id,
    controlLeaseUrl, [['x-ms-lease-action', 'acquire'], ['x-ms-lease-duration', '15'], ['x-ms-proposed-lease-id', leaseId]],
    'PUT', PLANNED_CONTROL_OPERATION_IDS[index]);
  const controlRelease = (id: number, index: number, leaseId: string) => signedControlRequest(authority, deadline, id,
    controlLeaseUrl, [['x-ms-lease-action', 'release'], ['x-ms-lease-id', leaseId]], 'PUT', PLANNED_CONTROL_OPERATION_IDS[index]);
  const controlMetadata = (id: number, index: number, leaseId: string, registry: string) => signedControlRequest(authority, deadline, id,
    controlMetadataUrl, [['x-ms-lease-id', leaseId], ['x-ms-meta-occupancy', 'released'],
      ['x-ms-meta-publisherregistryversion', '1'], ['x-ms-meta-publishers', registry],
      ['x-ms-meta-resetgeneration', generation], ['x-ms-meta-resetstate', 'ready-v1'], ['x-ms-meta-sandboxid', 'offline-stamp']],
    'PUT', PLANNED_CONTROL_OPERATION_IDS[index]);
  const activeRegistry = JSON.stringify({ [publisherId]: { generation, state: 'active' } });
  const controlRequests = [controlRead(1, 0), controlAcquire(2, 1, controlLeaseIds[0]), controlRead(3, 2),
    controlMetadata(4, 3, controlLeaseIds[0], activeRegistry), controlRelease(5, 4, controlLeaseIds[0]), controlRead(6, 5),
    controlRead(7, 6), controlRead(8, 7), controlRead(9, 8), controlAcquire(10, 9, controlLeaseIds[1]), controlRead(11, 10),
    controlMetadata(12, 11, controlLeaseIds[1], '{}'), controlRelease(13, 12, controlLeaseIds[1])];
  const listPrefix = new URL(authority.table_root_url).pathname.slice(1);
  const listUrl = controlBase + '?restype=container&comp=list&prefix=' + encodeURIComponent(listPrefix);
  const initialListing = signedObjectRequest(authority, deadline, 14, 'GET', listUrl, '', [['x-arrowcol-operation-id', PLANNED_STORAGE_OPERATION_IDS[0]]]);
  const deltaLogPrefix = listPrefix + '_delta_log/';
  const logListingUrl = controlBase + '?restype=container&comp=list&prefix=' + encodeURIComponent(deltaLogPrefix) +
    '&startFrom=' + encodeURIComponent(deltaLogPrefix.slice(0, -1));
  const logListing = signedObjectRequest(authority, deadline, 15, 'GET', logListingUrl, '', [['x-arrowcol-operation-id', PLANNED_STORAGE_OPERATION_IDS[1]]]);
  const logUrl = `https://${account}.blob.core.windows.net/${container}/${listPrefix}_delta_log/00000000000000000000.json`;
  const checkpointUrl = `https://${account}.blob.core.windows.net/${container}/${listPrefix}_delta_log/_last_checkpoint`;
  const versionedLogListingUrl = controlBase + "?restype=container&comp=list&prefix=" +
    encodeURIComponent(deltaLogPrefix) + "&startFrom=" + encodeURIComponent(deltaLogPrefix + "00000000000000000000");
  const deltaCommit = signedObjectRequest(authority, deadline, 16, 'PUT', logUrl, plan.delta_v0.body,
    [['if-none-match', '*'], ['x-ms-blob-type', 'BlockBlob'], ['content-length', String(Buffer.byteLength(plan.delta_v0.body, 'utf8'))],
      ['x-arrowcol-operation-id', PLANNED_STORAGE_OPERATION_IDS[2]]]);
  const postwriteListing = signedObjectRequest(authority, deadline, 17, 'GET', listUrl, '',
    [['x-arrowcol-operation-id', PLANNED_STORAGE_OPERATION_IDS[3]]]);
  const deltaLastCheckpoint = signedObjectRequest(authority, deadline, 18, 'GET', checkpointUrl, '',
    [['content-length', '0'], ['x-arrowcol-operation-id', PLANNED_STORAGE_OPERATION_IDS[4]]]);
  const deltaVersionedLogListing = signedObjectRequest(authority, deadline, 19, "GET", versionedLogListingUrl, "",
    [["x-arrowcol-operation-id", PLANNED_STORAGE_OPERATION_IDS[5]]]);
  const deltaReadback = signedObjectRequest(authority, deadline, 20, "GET", logUrl, "",
    [["content-length", "0"], ["x-arrowcol-operation-id", PLANNED_STORAGE_OPERATION_IDS[6]]]);
  const deltaReadbackFollowup = signedObjectRequest(authority, deadline, 21, "GET", logUrl, "",
    [["content-length", "0"], ["x-arrowcol-operation-id", PLANNED_STORAGE_OPERATION_IDS[7]]]);
  const envelope = envelopeForRequests(authority, [...controlRequests, initialListing, logListing, deltaCommit, postwriteListing, deltaLastCheckpoint, deltaVersionedLogListing, deltaReadback, deltaReadbackFollowup]);
  const custody = mutableCustodyStore(authority, now);
  const azure = { c: { account }, async token() { return 'offline-test-token'; } } as unknown as Azure;
  const ledgerPath = path.join(directory, 'storage-ledger.jsonl');
  let publisherRegistry = '{}';
  let deltaLogBody: string | undefined;
  const options = { executable, planPath, bridgeInputPath,
    ledgerPath, rootDirectory: process.cwd(), envelope, store: custody.store, azure,
    fetcher: async (input: RequestInfo | URL, init?: RequestInit) => {
      const url = new URL(String(input)), headers = new Headers(init?.headers);
      if (init?.method === 'HEAD') return new Response(null, { status: 200, headers: { etag: '"offline-etag"',
        'x-ms-meta-sandboxid': 'offline-stamp', 'x-ms-meta-resetstate': 'ready-v1', 'x-ms-meta-resetgeneration': generation,
        'x-ms-meta-publisherregistryversion': '1', 'x-ms-meta-publishers': publisherRegistry,
        'x-ms-meta-occupancy': 'released' } });
      if (url.searchParams.get('comp') === 'lease' && headers.get('x-ms-lease-action') === 'acquire')
        return new Response(null, { status: 201, headers: { 'x-ms-lease-id': headers.get('x-ms-proposed-lease-id')! } });
      if (url.searchParams.get('comp') === 'metadata' && init?.method === 'PUT') {
        publisherRegistry = headers.get('x-ms-meta-publishers') ?? '{}';
        return new Response(null, { status: 200 });
      }
      if (url.searchParams.get('comp') === 'lease' && headers.get('x-ms-lease-action') === 'release') return new Response(null, { status: 200 });
      if (url.pathname.endsWith('/_delta_log/00000000000000000000.json') && init?.method === 'PUT') {
        deltaLogBody = typeof init.body === 'string' ? init.body : await new Response(init.body).text();
        return new Response(null, { status: 201, headers: { etag: '"delta-v0"' } });
      }
      if (url.pathname.endsWith('/_delta_log/_last_checkpoint') && init?.method === 'GET') return new Response(null,
        { status: 404, headers: { 'x-ms-error-code': 'BlobNotFound' } });
      if (url.pathname.endsWith('/_delta_log/00000000000000000000.json') && init?.method === 'GET')
        return deltaLogBody === undefined ? new Response(null, { status: 404 }) :
          new Response(deltaLogBody, { status: 200, headers: { "content-length": String(Buffer.byteLength(deltaLogBody, "utf8")), "last-modified": "Tue, 17 Nov 2026 11:03:18 GMT", etag: "\"offline-delta-v0\"" } });
      if (url.searchParams.get('comp') === 'list') {
        const prefix = url.searchParams.get('prefix') ?? '';
        const name = listPrefix + '_delta_log/00000000000000000000.json';
        const blob = deltaLogBody !== undefined && name.startsWith(prefix)
          ? '<Blob><Name>' + name + '</Name><Properties><Last-Modified>Tue, 17 Nov 2026 11:03:18 GMT</Last-Modified>' +
            '<Content-Length>' + Buffer.byteLength(deltaLogBody, 'utf8') + '</Content-Length><Content-Type>application/json</Content-Type>' +
            '<Etag>"offline-delta-v0"</Etag></Properties></Blob>' : '';
        return new Response('<?xml version="1.0" encoding="utf-8"?><EnumerationResults><Blobs>' + blob +
          '</Blobs><NextMarker></NextMarker></EnumerationResults>',
          { status: 200, headers: { 'content-type': 'application/xml' } });
      }
      return new Response('', { status: 500 });
    }, now: Date.now };
  const boundary = new EffectBoundary(envelope, path.join(directory, 'effects.jsonl'));
  await withEffectBoundary(boundary, () => initializeOwnedRunTable(options));
});

test('real Rust zero-data fixture composes with response-derived Node phase allocations', {
  skip: !process.env.OAT_D47_FIXTURE_BIN,
}, async t => {
  const executable = process.env.OAT_D47_FIXTURE_BIN!;
  const directory = mkdtempSync(path.join(os.tmpdir(), 'oat-d47-fixture-composition-'));
  chmodSync(directory, 0o700);
  t.after(() => rmSync(directory, { recursive: true, force: true }));

  const now = Date.now(), deadline = now + 300_000;
  const campaign = 'c0ffee00-0000-4000-8000-000000000101';
  const owner = 'c0ffee00-0000-4000-8000-000000000102';
  const orchestration = 'c0ffee00-0000-4000-8000-000000000103';
  const run = 'c0ffee00-0000-4000-8000-000000000104';
  const namespace = 'fixture-' + run.replaceAll('-', '');
  const serviceAccount = 'fixture-sa';
  const server = 'https://offline.invalid/';
  const subscription = 'c0ffee00-0000-4000-8000-000000000107';
  const identityId = `/subscriptions/${subscription}/resourceGroups/offline/providers/Microsoft.ManagedIdentity/userAssignedIdentities/fixture`;
  const ficPlan = {
    version: 1 as const, runId: run, namespace, serviceAccount,
    identityId,
    resourceId: identityId + '/federatedIdentityCredentials/arrowcol-run-' + run,
    clientId: 'c0ffee00-0000-4000-8000-000000000105',
    tenantId: 'c0ffee00-0000-4000-8000-000000000106',
    aksId: `/subscriptions/${subscription}/resourceGroups/offline/providers/Microsoft.ContainerService/managedClusters/fixture`, server,
    credentialName: 'arrowcol-run-' + run, issuer: 'https://offline.invalid/issuer',
    subject: `system:serviceaccount:${namespace}:${serviceAccount}`,
    audience: 'api://AzureADTokenExchange',
  };
  const notStarted = () => ({ state: 'not_started' as const, deadline_unix_ms: null, reason_code: null });
  const inventory: RunInventory = {
    schema_version: 1, campaign_id: campaign, owner_id: owner, fencing_token: 1,
    orchestration_run_id: orchestration, cell_run_id: run, load_admission: 'not_authorized',
    namespace: { name: namespace, uid: null, owner_token: owner }, resources: [],
    fic: { resource_id: null, name: null, state: 'planned', absence: null, plan: ficPlan },
    qap: null, planned_cell_duration_ms: 0,
    evidence_destination: { account: 'offlinestorage', container: 'checkpoints',
      blob_prefix: `campaigns/${campaign}/cells/${run}/evidence` },
    effective_stop_budgets: { maintenance_grace_seconds: 4_800, maintenance_stop_timeout_ms: 5_100_000 },
    cleanup: { stop_admission: notStarted(), bounded_drain: notStarted(), evidence_capture: notStarted(),
      stop_maintenance: notStarted(), terminal_capture: notStarted(), handoff: notStarted() },
  };
  const authority = { campaign_id: campaign, cell_run_id: run, owner_id: owner, fencing_token: 1,
    namespace, server, context: 'reviewed-offline-context' };
  const image = 'registry.k8s.io/pause@sha256:' + 'a'.repeat(64);
  const manifests = [
    { apiVersion: 'v1', kind: 'Namespace', metadata: { name: namespace } },
    { apiVersion: 'v1', kind: 'ServiceAccount', metadata: { name: serviceAccount, namespace }, automountServiceAccountToken: false },
    ...(['client', 'collector', 'maintenance'] as const).map(role => {
      const name = role === 'maintenance' ? 'delta-maintenance' : 'arrowcol-' + role;
      const app = role === 'client' ? 'arrowcol-oat-client' : name;
      return { apiVersion: 'apps/v1', kind: 'Deployment', metadata: { name, namespace, labels: { app } },
        spec: { replicas: 1, selector: { matchLabels: { app } }, template: {
          metadata: { labels: { app } }, spec: { serviceAccountName: serviceAccount, automountServiceAccountToken: false,
            containers: [{ name: role, image, resources: { requests: { cpu: '10m', memory: '16Mi' },
              limits: { cpu: '100m', memory: '64Mi' } } }] },
        } } };
    }),
  ];
  const input: FixtureSetupInput = { schema_version: 1, authority, original_deadline_unix_ms: deadline, inventory,
    service_account: serviceAccount, manifests, runtime_directory: path.join(directory, 'runtime'),
    stamp_config_path: path.join(directory, 'stamp.yaml'), bootstrap_env_file: path.join(directory, 'campaign.env') };
  const inputPath = path.join(directory, 'fixture-input.json'), ledgerPath = path.join(directory, 'fixture-ledger.jsonl');
  writeFileSync(inputPath, JSON.stringify(input), { mode: 0o600 });
  chmodSync(inputPath, 0o600);

  const resource = 'https://offlinestorage.blob.core.windows.net/checkpoints';
  const activePath = '/checkpoints/active.json';
  const effects: EffectEnvelope['effects'] = [
    { id: 'active-read', transport: 'campaign-blob', method: 'GET', resource, path: activePath, max_calls: 1 },
    { id: 'active-write', transport: 'campaign-blob', method: 'PUT', resource, path: activePath, max_calls: 1 },
    { id: 'baseline-read', transport: 'campaign-blob', method: 'GET', resource,
      path: `/checkpoints/campaigns/${campaign}/baseline.json`, max_calls: 1 },
  ];
  const envelope: EffectEnvelope = { schema_version: 1, campaign_id: campaign, run_id: run,
    subscription_id: subscription, resource_group: 'offline',
    storage_account: 'offlinestorage', state_container: 'checkpoints', effects };

  let stored: StoredCampaign = { state: newCampaignState(campaign, owner, now), etag: '"0"' };
  let revision = 0;
  const store = createBlobCampaignStore({ account: 'offlinestorage', container: 'checkpoints', subscriptionId: subscription },
    async (input, init) => {
      const request = new URL(String(input)), method = (init?.method ?? 'GET').toUpperCase();
      if (request.pathname !== activePath) throw new Error('Unexpected offline campaign blob path.');
      if (method === 'GET') return new Response(JSON.stringify(stored.state), { status: 200, headers: { etag: stored.etag } });
      if (method === 'PUT') {
        const headers = new Headers(init?.headers);
        if (headers.get('if-match') !== stored.etag || typeof init?.body !== 'string')
          throw new Error('Offline campaign CAS condition or body changed.');
        stored = { state: JSON.parse(init.body) as StoredCampaign['state'], etag: `"${++revision}"` };
        return new Response(null, { status: 201, headers: { etag: stored.etag } });
      }
      throw new Error('Unexpected offline campaign blob method.');
    }, async () => 'offline-token');
  const azure = { c: { account: 'offlinestorage' } } as unknown as Azure;
  const env = { CAMPAIGN_ID: campaign, CAMPAIGN_OWNER_ID: owner, CAMPAIGN_FENCING_TOKEN: '1' };
  const namespaceIdentity: Record<string, unknown> = {
    apiVersion: 'v1', kind: 'Namespace', metadata: { name: namespace,
      labels: { 'arrowcol-oat/run-id': run, 'arrowcol-oat/owner-token': owner },
      annotations: { 'arrowcol-oat/run-id': run, 'arrowcol-oat/owner-token': owner },
      uid: 'c0ffee00-0000-4000-8000-000000000108', resourceVersion: '17' },
  };
  let createdResources: Record<string, unknown>[] = [];
  let kubeCommands = 0, publications = 0;
  const campaignMain: NonNullable<FixtureDispatcherOptions['campaignMain']> = async (_args, dependencies) => {
    publications++;
    const updated = await publishRunInventory(dependencies?.store ?? store, dependencies?.runInventoryInput, now,
      dependencies?.runInventoryWriteOptions);
    console.log(JSON.stringify({ ok: true, receipt: runInventoryPublishReceipt(updated) }));
  };
  const runCommand = async (command: string[], stdin: Buffer | undefined) => {
    kubeCommands++;
    const args = command.slice(1);
    const action = args.find(value => value === 'get' || value === 'create');
    const body = stdin ? JSON.parse(stdin.toString('utf8')) as Record<string, unknown> : undefined;
    if (action === 'get' && args.includes('namespace')) {
      return { returncode: 0, stdout: kubeCommands === 1 ? '' : JSON.stringify(namespaceIdentity), stderr: '' };
    }
    if (action === 'create' && args.includes('-f') && body?.kind === 'Namespace') {
      return { returncode: 0, stdout: JSON.stringify(namespaceIdentity), stderr: '' };
    }
    if (action === 'get' && args.includes('--ignore-not-found')) {
      return { returncode: 0, stdout: JSON.stringify({ items: [] }), stderr: '' };
    }
    if (action === 'create' && body?.kind === 'List' && Array.isArray(body.items)) {
      createdResources = body.items.map((item, index) => {
        const resource = structuredClone(item) as Record<string, unknown>;
        const metadata = resource.metadata as Record<string, unknown>;
        metadata.uid = `c0ffee00-0000-4000-8000-${String(109 + index).padStart(12, '0')}`;
        metadata.resourceVersion = '17';
        if (resource.kind === 'Deployment') metadata.generation = 1;
        return resource;
      });
      return { returncode: 0, stdout: createdResources.map(value => JSON.stringify(value)).join('\n') + '\n', stderr: '' };
    }
    if (action === 'get') return { returncode: 0, stdout: JSON.stringify({ items: createdResources }), stderr: '' };
    throw new Error('Unexpected offline fixture command: ' + JSON.stringify(command));
  };

  const outerBoundary = new EffectBoundary(envelope, path.join(directory, 'outer-effects.jsonl'));
  const receipt = await withEffectBoundary(outerBoundary, () => runFixtureSetupChild(executable, inputPath, {
    input, rootDirectory: process.cwd(), ledgerPath, envelope, store, azure, env, campaignMain, runCommand, now: () => now,
  }));
  assert.equal(receipt.load_admitted, false);
  assert.equal(receipt.inventory.load_admission, 'not_authorized');
  assert.equal(receipt.inventory.planned_cell_duration_ms, 0);
  assert.equal(receipt.inventory.resources.length, 4);
  assert.ok(receipt.inventory.resources.every(item => item.state === 'observed' && item.uid));
  assert.equal(receipt.command_responses.length, 10);
  assert.equal(publications, 4);
  assert.equal(kubeCommands, 6);
  assert.equal((await store.read())?.state.runInventory?.namespace.uid,
    (namespaceIdentity.metadata as Record<string, unknown>).uid);

  for (let requestId = 1; requestId <= 10; requestId++) {
    const planPath = `${ledgerPath}.phase-${requestId}-command.plan.json`;
    const plan = JSON.parse(readFileSync(planPath, 'utf8')) as Record<string, unknown>;
    assert.equal(plan.request_id, requestId);
    assert.equal(plan.phase, 'command');
    assert.match(String(plan.allocation_sha256), /^[0-9a-f]{64}$/);
    assert.match(String(plan.predecessor_journal_sha256), /^[0-9a-f]{64}$/);
    assert.match(String(plan.predecessor_response_sha256), /^[0-9a-f]{64}$/);
  }
});
