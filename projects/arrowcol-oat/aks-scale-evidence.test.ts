import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { test, type TestContext } from 'node:test';
import { Azure, Config, canonical_json } from '../../infra/azure/bootstrap.js';
import { newCampaignState, type CampaignStore, type RunInventory } from './aks-campaign.js';
import { createScaleEvidenceVerifier, readBoundedSourceBytes, readOfficialCellMeasurement } from './aks-scale-evidence.js';
import type { ScaleMeasurementEnvelope } from './aks-scale.js';

const hash = (value: string | Uint8Array) => createHash('sha256').update(value).digest('hex');
const campaignId = 'c0ffee00-0000-4000-8000-000000000001', ownerId = 'c0ffee00-0000-4000-8000-000000000002';
const cellId = 'c0ffee00-0000-4000-8000-000000000003', orchestrationId = 'c0ffee00-0000-4000-8000-000000000004';
const now = 1_800_000_000_000;

function syntheticAuthenticatedTransport(t: TestContext, supplied?: Uint8Array) {
  const azure = new Azure(new Config({ AZURE_SUBSCRIPTION_ID: campaignId, AZURE_STAMP: '12345' }));
  const state = newCampaignState(campaignId, ownerId, now - 120_000);
  state.mode = 'armed';
  const step = () => ({ state: 'not_started' as const, deadline_unix_ms: null, reason_code: null });
  const inventory: RunInventory = { schema_version: 1, campaign_id: campaignId, owner_id: ownerId, fencing_token: 1,
    orchestration_run_id: orchestrationId, cell_run_id: cellId, load_admission: 'authorized',
    namespace: { name: 'synthetic-offline-' + cellId.replaceAll('-', ''), uid: null, owner_token: ownerId },
    resources: [], fic: null, qap: null,
    evidence_destination: { account: azure.c.account, container: azure.c.checkpoints, blob_prefix: 'synthetic-offline/' + cellId },
    cleanup: { stop_admission: step(), bounded_drain: step(), evidence_capture: step(), stop_maintenance: step(),
      terminal_capture: step(), handoff: step() } };
  const effective = canonical_json({ typed_oat_config: { tables: ['synthetic_offline_table'] } });
  const document = { schema_version: 1, scope: 'original_official_cell_window_not_paired_scale_authorization',
    origin: { campaign_id: campaignId, owner_id: ownerId, fencing_token: 1, orchestration_run_id: orchestrationId,
      cell_run_id: cellId, namespace_uid: null },
    submitted_config_sha256: 'b'.repeat(64), effective_config_json: effective, effective_config_sha256: hash(effective),
    source_revision: 'a'.repeat(40),
    official_result: { run_id: cellId, table: 'synthetic_offline_table', status: 'succeeded', git_sha: 'a'.repeat(40),
      tunables: { orchestration_run_id: orchestrationId }, evidence: { version: 1, commit_window_output_throughput: {
        scope: 'new_ingestion_commits_finished_in_half_open_shared_window_not_client_cohort',
        measurement_start_unix_ms: now - 60_000, measurement_end_unix_ms: now - 30_000,
        window_seconds: 30, rows_per_second: 1234,
      } } } };
  let bytes = supplied ?? Buffer.from(canonical_json(document));
  const decoded = JSON.parse(new TextDecoder().decode(bytes)) as typeof document;
  const origin = decoded.origin, output = decoded.official_result.evidence.commit_window_output_throughput;
  Object.assign(inventory, { campaign_id: origin.campaign_id, owner_id: origin.owner_id, fencing_token: origin.fencing_token,
    orchestration_run_id: origin.orchestration_run_id, cell_run_id: origin.cell_run_id,
    namespace: { ...inventory.namespace, uid: origin.namespace_uid } });
  state.campaignId = origin.campaign_id; state.ownerId = origin.owner_id; state.fencingToken = origin.fencing_token;
  if (supplied) state.startedAtMs = 1;
  inventory.official_measurement = { schema_version: 1,
    blob: inventory.evidence_destination!.blob_prefix + '/official-cell-measurement-v1.json',
    etag: '"synthetic-offline-etag"', version: 'synthetic-offline-version', sha256: hash(bytes), byte_length: bytes.byteLength,
    effective_config_sha256: decoded.effective_config_sha256, submitted_config_sha256: decoded.submitted_config_sha256,
    source_revision: decoded.source_revision, measurement_start_unix_ms: output.measurement_start_unix_ms,
    measurement_end_unix_ms: output.measurement_end_unix_ms };
  state.runInventory = inventory;
  let reads = 0, tokenReads = 0, status = 200, returnedEtag = inventory.official_measurement.etag, changed = false;
  const token = { ownerId: state.ownerId, fencingToken: state.fencingToken };
  t.mock.method(azure, 'token', async (resource: string) => {
    assert.equal(resource, 'https://storage.azure.com/');
    tokenReads++;
    return 'synthetic-offline-token';
  });
  const store: CampaignStore = { read: async () => ({ state: structuredClone(state), etag: '"synthetic-custody"' }),
    write: async () => { throw new Error('read-only evidence transport must not write custody'); } };
  const fetcher: typeof fetch = async (input, init) => {
    reads++;
    const url = new URL(String(input)), reference = inventory.official_measurement!;
    assert.equal(url.origin, `https://${azure.c.account}.blob.core.windows.net`);
    assert.equal(url.pathname, '/' + azure.c.checkpoints + '/' + reference.blob);
    assert.equal(url.searchParams.get('versionid'), reference.version);
    assert.equal(new Headers(init?.headers).get('if-match'), reference.etag);
    assert.equal(new Headers(init?.headers).get('authorization'), 'Bearer synthetic-offline-token');
    assert.equal(init?.redirect, 'error');
    if (changed) state.fencingToken++;
    return new Response(new Uint8Array(bytes), { status, headers: { etag: returnedEtag, 'x-ms-version-id': reference.version!,
      'content-length': String(bytes.byteLength) } });
  };
  return { state, inventory, document, read: () => readOfficialCellMeasurement({ azure, store, token,
    deadlineAtMs: now + 60_000, now: () => now, fetcher }),
    options: { azure, store, token, deadlineAtMs: now + 60_000, now: () => now, fetcher },
    counts: () => ({ reads, tokenReads }), corrupt: () => { bytes = Buffer.from('not the original bytes'); },
    failStatus: () => { status = 403; }, wrongEtag: () => { returnedEtag = '"forged"'; }, fence: () => { changed = true; } };
}

test('immutable measurement reader uses selected authenticated storage and cannot authorize an unpaired scale trial', async t => {
  const f = syntheticAuthenticatedTransport(t), observed = await f.read();
  assert.equal(observed.committedRowsPerSecond, 1234);
  assert.deepEqual(observed.unavailableReasons, []);
  assert.deepEqual(f.counts(), { reads: 1, tokenReads: 1 });
  const envelope: ScaleMeasurementEnvelope = { schema_version: 2, campaign_id: campaignId, cell_run_id: cellId,
    observed_at_ms: observed.endMs!, window_ms: observed.endMs! - observed.startMs!,
    measurement: { committedRowsPerSecond: 1234, qapPendingBacklog: null } };
  const verifier = createScaleEvidenceVerifier(f.options), window = { startMs: observed.startMs!, endMs: observed.endMs!,
    notBeforeMs: observed.startMs!, deadlineAtMs: f.options.deadlineAtMs };
  await assert.rejects(verifier.verifyMeasurement(envelope, f.state, window), /paired baseline\/trial cell custody/);
  for (const invalid of [{ ...window, startMs: window.startMs + 1 }, { ...window, endMs: window.endMs - 1 },
    { ...window, notBeforeMs: window.startMs + 1 }, { ...window, deadlineAtMs: window.deadlineAtMs + 1 }])
    await assert.rejects(verifier.verifyMeasurement(envelope, f.state, invalid), /original cell\/window\/deadline/);
});

test('immutable measurement reader rejects changed bytes, source identity, permissions and custody', async t => {
  for (const fail of ['bytes', 'etag', 'status', 'fence', 'source', 'deadline'] as const) {
    const f = syntheticAuthenticatedTransport(t);
    if (fail === 'bytes') f.corrupt();
    if (fail === 'etag') f.wrongEtag();
    if (fail === 'status') f.failStatus();
    if (fail === 'fence') f.fence();
    if (fail === 'source') f.inventory.evidence_destination!.account = 'wrongselectedaccount';
    const read = fail === 'deadline' ? readOfficialCellMeasurement({ ...f.options, deadlineAtMs: now }) : f.read();
    await assert.rejects(read, /byte bound|immutable publication|ETag|status|custody|selected stamp|deadline/);
    if (fail === 'source' || fail === 'deadline') assert.equal(f.counts().reads, 0);
  }
});

test('source streaming cap stops retention on overflow and rejects a truncated declared body', async () => {
  let cancelled = false;
  const stream = new ReadableStream<Uint8Array>({ start(controller) {
    controller.enqueue(new Uint8Array(6));
    controller.enqueue(new Uint8Array(6));
  }, cancel() { cancelled = true; } });
  await assert.rejects(readBoundedSourceBytes(new Response(stream), 8), /byte bound/);
  assert.equal(cancelled, true);
  await assert.rejects(readBoundedSourceBytes(new Response('tiny', { headers: { 'content-length': '8' } }), 8), /truncated/);
  await assert.rejects(readBoundedSourceBytes(new Response('tiny', { headers: { 'content-length': '9' } }), 8), /byte bound/);
});

test('actual Rust immutable producer bytes remain an original window with unavailable submitted identity', {
  skip: !process.env.OAT_OFFLINE_MEASUREMENT_FIXTURE,
}, async t => {
  const bytes = await readFile(process.env.OAT_OFFLINE_MEASUREMENT_FIXTURE!);
  const f = syntheticAuthenticatedTransport(t, bytes), observed = await f.read();
  assert.equal(observed.startMs, 1000);
  assert.equal(observed.endMs, 31_000);
  assert.equal(observed.committedRowsPerSecond, 0);
  assert.ok(observed.unavailableReasons.includes('authoritative_submitted_config_identity_unavailable'));
  assert.ok(observed.unavailableReasons.includes('original_official_cell_did_not_succeed'));
});
