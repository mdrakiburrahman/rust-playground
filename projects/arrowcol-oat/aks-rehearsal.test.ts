import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { test } from 'node:test';
import { CampaignConflictError, startCampaign, type CampaignState, type CampaignStore, type StoredCampaign } from './aks-campaign.js';
import { assertRehearsalPathIsolated, ReadOnlyAzureAdapter, rehearsalCampaignRecordPath,
  runRehearsalWatchdogOnce, type RehearsalEvidenceRecord } from './aks-rehearsal.js';
import type { WatchdogReadOnlyAzure } from './aks-watchdog.js';

const campaignId = 'c0ffee00-0000-4000-8000-000000000001';
const ownerId = 'c0ffee00-0000-4000-8000-000000000002';
const subscriptionId = 'c0ffee00-0000-4000-8000-000000000003';

class MemoryStore implements CampaignStore {
  current?: StoredCampaign;
  revision = 0;
  writes = 0;

  async read() { return this.current ? structuredClone(this.current) : undefined; }

  async write(state: CampaignState, etag?: string) {
    if (etag !== this.current?.etag) throw new CampaignConflictError();
    const next = `etag-${++this.revision}`;
    this.writes++;
    this.current = { state: structuredClone(state), etag: next };
    return next;
  }
}

test('rehearsal record path is fixed to a disjoint prefix', () => {
  assert.equal(rehearsalCampaignRecordPath(campaignId), `_arrowcol-custody-rehearsal/${campaignId}/active.json`);
  assert.doesNotThrow(() => assertRehearsalPathIsolated(rehearsalCampaignRecordPath(campaignId)));
  assert.throws(() => assertRehearsalPathIsolated('active.json'), /not isolated/);
  assert.throws(() => assertRehearsalPathIsolated('_arrowcol-custody/campaigns/run/active.json'), /not isolated/);
  assert.throws(() => assertRehearsalPathIsolated('_arrowcol-custody-rehearsal/../active.json'), /not isolated/);
});

test('non-terminal real campaign blocks before any isolated write', async () => {
  const realStore = new MemoryStore(), isolatedStore = new MemoryStore();
  await startCampaign(realStore, campaignId, ownerId);
  const evidence: RehearsalEvidenceRecord[] = [];
  const readOnlyAzure: WatchdogReadOnlyAzure = { readArmStatus: async () => ({ status: 200, body: {} }) };
  await assert.rejects(runRehearsalWatchdogOnce({ env: { ARM_SUBSCRIPTION_ID: subscriptionId }, rehearsalId: campaignId,
    ownerId, realStore, isolatedStore, readOnlyAzure, evidence: event => evidence.push(event) }), /Real campaign record is non-terminal/);
  assert.equal(isolatedStore.writes, 0);
  assert.equal(evidence.some(event => event.event === 'real_campaign_read' && event.status === 'active'), true);
});

test('read-only ARM adapter refuses non-GET and off-host requests before sending', async () => {
  const sent: string[] = [], evidence: RehearsalEvidenceRecord[] = [];
  const adapter = new ReadOnlyAzureAdapter(async (method, url) => {
    sent.push(`${method} ${url}`);
    return { status: 200, body: Buffer.from('{"status":"Succeeded"}'), requestId: 'request-1' };
  }, event => evidence.push(event));
  const url = `https://management.azure.com/subscriptions/${subscriptionId}/operations/one?api-version=2024-01-01`;
  assert.deepEqual(await adapter.readArmStatus(url), { status: 200, body: { status: 'Succeeded' } });
  await assert.rejects(adapter.request('PUT', url), /refuses non-GET/);
  await assert.rejects(adapter.request('GET', 'https://example.com/resource'), /non-management/);
  assert.deepEqual(sent, [`GET ${url}`]);
  assert.equal(evidence[0]?.request_id, 'request-1');
  assert.equal(evidence[0]?.response_body_sha256, createHash('sha256').update('{"status":"Succeeded"}').digest('hex'));
});
