import test from 'node:test';
import assert from 'node:assert/strict';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { attestBackgroundTable, captureBackground, clusterStatus, controlCluster, observedQapPressure,
  profileFromCluster, profileMatches, requireSameBackground, validateClusterProfile, validateQapCapacity,
  validateReceipt, type BackgroundAttestation, type ControllerIO, type QapCapacity, type Receipt } from './aks-qap-cluster.js';
import { captureWriterShutdown, clockErrorBound } from './aks-qap-observer.js';

const fixed = { sku: 'Standard_E8ads_v5', nodes: 4, autoscale: { enabled: false, minimum: 4, maximum: 4 } };

test('cluster profiles enforce both node and vCPU ceilings including optimized autoscale maxima', () => {
  assert.equal(validateClusterProfile(fixed).nodes, 4);
  assert.equal(validateClusterProfile({ sku: 'Standard_E32ads_v5', nodes: 2,
    autoscale: { enabled: false, minimum: 2, maximum: 2 } }).sku, 'Standard_E32ads_v5');
  for (const invalid of [
    { ...fixed, sku: 'Standard_E64ads_v5' },
    { ...fixed, sku: 'Standard_E32ads_v5' },
    { ...fixed, nodes: 9, autoscale: { enabled: true, minimum: 2, maximum: 9 } },
    { ...fixed, nodes: 4, autoscale: { enabled: true, minimum: 2, maximum: 8.5 } },
    { ...fixed, sku: 'Standard_E16ads_v5', autoscale: { enabled: true, minimum: 2, maximum: 8 } },
    { ...fixed, autoscale: { enabled: false, minimum: 2, maximum: 4 } },
    { ...fixed, command: 'arbitrary cloud command' },
    { ...fixed, autoscale: { ...fixed.autoscale, coefficient: 2 } },
  ]) assert.throws(() => validateClusterProfile(invalid), /Unsupported|exceeds|pin|unexpected|integer/);
});

test('readback only permits node movement inside the exact configured optimized autoscale bounds', () => {
  const wanted = validateClusterProfile({ ...fixed, autoscale: { enabled: true, minimum: 2, maximum: 8 } });
  const actual = profileFromCluster({ sku: { name: wanted.sku, capacity: 6 },
    properties: { optimizedAutoscale: { isEnabled: true, minimum: 2, maximum: 8 } } });
  assert.equal(profileMatches(actual, wanted, true), true);
  assert.equal(profileMatches(actual, wanted, false), false);
  assert.equal(profileMatches({ ...actual, sku: 'Standard_E4ads_v5' }, wanted, true), false);
  assert.equal(profileMatches({ ...actual, autoscale: { enabled: true, minimum: 3, maximum: 8 } }, wanted, true), false);
});

function observation(at: number, pending: number, consumed: number, resource = 'QueryAcceleration') {
  return { endedAtMs: at, before: { pendingFiles: pending },
    capacity: { Tables: [{ Columns: ['Resource', 'Total', 'Consumed'].map(ColumnName => ({ ColumnName })),
      Rows: [[resource, 4, consumed]] }] } };
}

test('scale permission needs three actual QAP saturated samples spanning 30 seconds, not growing idle backlog', () => {
  const saturated = [observation(0, 10, 4), observation(15_000, 10, 4), observation(30_000, 10, 4)];
  assert.equal(observedQapPressure(saturated), true);
  assert.equal(observedQapPressure([observation(0, 1, 0), observation(15_000, 3, 0), observation(30_000, 10, 0)]), false);
  assert.equal(observedQapPressure([observation(0, 10, 0), observation(29_000, 10, 4),
    observation(30_000, 10, 4), observation(31_000, 10, 4)]), false);
  assert.equal(observedQapPressure([observation(0, 10, 4), observation(30_000, 10, 4), observation(30_000, 10, 4)]), false);
  assert.equal(observedQapPressure(saturated.slice(0, 2)), false);
  assert.equal(observedQapPressure(saturated.map(row => ({ ...row, error: 'observation unavailable' }))), false);
  assert.equal(observedQapPressure([observation(0, 0, 4), observation(15_000, 0, 4), observation(30_000, 0, 4)]), false);
  assert.equal(observedQapPressure([observation(0, 10, 4, 'Ingestions'), observation(15_000, 10, 4, 'Ingestions'),
    observation(30_000, 10, 4, 'Ingestions')]), false);
});

type Row = Record<string, unknown>;
const baseline: QapCapacity = { ClusterMaximumConcurrentOperations: 100, CoreUtilizationCoefficient: 0.5 };
const candidate: QapCapacity = { ClusterMaximumConcurrentOperations: 100, CoreUtilizationCoefficient: 1 };
const stampTags = { project: 'rust-playground', purpose: 'disposable-arrowcol-sandbox', sandboxId: 'stamp-test' };
const saturated = [observation(0, 10, 4), observation(15_000, 10, 4), observation(30_000, 10, 4)];
function response(records: Row[]) {
  const columns = Object.keys(records[0] ?? {});
  return { Tables: [{ Columns: columns.map(ColumnName => ({ ColumnName })), Rows: records.map(row => columns.map(key => row[key])) }] };
}
function capacityResponse(qap: QapCapacity, ingestion = 512) {
  return response([{ Policy: JSON.stringify({ IngestionCapacity: { ClusterMaximumConcurrentOperations: ingestion },
    QueryAccelerationCapacity: qap }) }]);
}
function passive() {
  const policy = { IsEnabled: true, Hot: '7.00:00:00', MaxAge: '00:05:00' };
  const reason = 'DeltaTableNotFound: Delta table does not exist';
  const definition = { TableName: 'arrowcol_old', TableType: 'Delta', Folder: '', DocString: '',
    Properties: JSON.stringify({ StorageConnectionString: 'https://example.invalid/missing' }),
    schema: { OrderedColumns: [{ Name: 'id', Type: 'System.Int64' }] } };
  const details = { TableName: 'arrowcol_old', QueryAccelerationPolicy: JSON.stringify(policy),
    QueryAccelerationState: JSON.stringify({ IsHealthy: false, NotHealthyReason: reason, RowCount: 0, AcceleratedRowCount: 0 }) };
  const statistics = { ExternalTableName: 'arrowcol_old', IsEnabled: true, HotSize: 0,
    NotHealthyReason: reason, AccelerationPendingDataFilesCount: 0, AccelerationPendingDataFilesSize: 0 };
  return { definition, details, statistics, policy };
}
function background(): BackgroundAttestation {
  const table = passive();
  return { observedAt: new Date(0).toISOString(), databases: ['test'],
    tables: [attestBackgroundTable('test', table.definition, table.details, table.policy, table.statistics)],
    effective: { Resource: 'QueryAcceleration', Total: 4, Consumed: 0, Remaining: 4 }, runningQueries: [], recentQueries: [] };
}
function harness() {
  const state = {
    cluster: { etag: '"one"', sku: { name: fixed.sku, capacity: fixed.nodes }, tags: { ...stampTags },
      properties: { state: 'Running', provisioningState: 'Succeeded',
        optimizedAutoscale: { isEnabled: false, minimum: 4, maximum: 4 } } } as Row,
    policy: { ...baseline }, ingestion: 512, background: background(), submissions: [] as string[],
    patches: [] as { body: Row; policy: QapCapacity }[], saved: [] as Receipt[],
    outcome: 'success' as 'success' | 'lost-applied' | 'lost-unapplied' | 'partial-applied',
  };
  const receipt: Receipt = { schemaVersion: 2, owner: '00000000-0000-4000-8000-000000000001', clusterId: 'cluster',
    deadlineMs: 24 * 3600_000, initial: structuredClone(state.cluster), initialProfile: validateClusterProfile(fixed),
    initialCapacityPolicy: capacityResponse(baseline), mutations: [], policyMutations: [], background: background() };
  const io: ControllerIO = {
    readCluster: async () => structuredClone(state.cluster),
    patchCluster: async (cluster, body) => {
      assert.equal(cluster.etag, state.cluster.etag);
      state.patches.push({ body: structuredClone(body), policy: { ...state.policy } });
      state.cluster = { ...state.cluster, ...body, properties: { ...state.cluster.properties as Row, ...body.properties as Row } };
      return { 'x-ms-request-id': 'request' };
    },
    management: async command => {
      if (command === '.show cluster policy capacity') return capacityResponse(state.policy, state.ingestion);
      if (command === '.show capacity') return response([{ Resource: 'QueryAcceleration', Total: 32 * state.policy.CoreUtilizationCoefficient,
        Consumed: 0, Remaining: 32 * state.policy.CoreUtilizationCoefficient }]);
      assert.match(command, /^\.alter-merge cluster policy capacity '/);
      assert.equal(receipt.policyMutations?.at(-1)?.state, 'intent');
      assert.equal(state.saved.at(-1)?.policyMutations?.at(-1)?.state, 'intent');
      assert.equal((state.cluster.tags as Row).qapCampaignOwner, receipt.owner);
      state.submissions.push(command);
      const payload = JSON.parse(command.slice(command.indexOf("'") + 1, -1)) as Row;
      assert.deepEqual(Object.keys(payload), ['QueryAccelerationCapacity']);
      if (state.outcome !== 'lost-unapplied') state.policy = payload.QueryAccelerationCapacity as QapCapacity;
      if (state.outcome === 'lost-applied' || state.outcome === 'lost-unapplied') throw new Error('Connection lost');
      if (state.outcome === 'partial-applied') return { ...capacityResponse(state.policy), HasErrors: true };
      return capacityResponse(state.policy);
    },
    background: async () => structuredClone(state.background),
    availableSkus: async () => [{ sku: { name: fixed.sku } }],
    save: async value => { state.saved.push(structuredClone(value)); },
    now: () => 60_000, sleep: async () => { throw new Error('Unexpected convergence wait'); },
  };
  return { state, receipt, io };
}

test('capacity input is a strict, bounded QAP-only policy, without ingestion fields or coercion', () => {
  assert.deepEqual(validateQapCapacity(baseline), baseline);
  for (const value of [
    { ...baseline, ClusterMaximumConcurrentOperations: 0 }, { ...baseline, ClusterMaximumConcurrentOperations: 101 },
    { ...baseline, ClusterMaximumConcurrentOperations: 1.5 }, { ...baseline, CoreUtilizationCoefficient: 0.09 },
    { ...baseline, CoreUtilizationCoefficient: 1.1 }, { ...baseline, CoreUtilizationCoefficient: NaN },
    { ...baseline, CoreUtilizationCoefficient: '0.5' }, { ...baseline, IngestionCapacity: {} },
    { CoreUtilizationCoefficient: 0.5 },
  ]) assert.throws(() => validateQapCapacity(value), /requires|integer|unexpected/);
});

test('idle missing-source background attestation rejects health, pending work, changed definitions and activity', () => {
  const expected = background();
  requireSameBackground(expected, structuredClone(expected));
  requireSameBackground(expected, { ...structuredClone(expected),
    recentQueries: [{ Database: 'test', Text: 'external_table("arrowcol_old_other_campaign")' }] });
  for (const change of [
    (value: BackgroundAttestation) => { value.tables[0]!.policy.MaxAge = '00:01:00'; },
    (value: BackgroundAttestation) => { value.tables[0]!.definition.Properties = { source: 'different' }; },
    (value: BackgroundAttestation) => { value.tables[0]!.definition.schema = { OrderedColumns: [] }; },
    (value: BackgroundAttestation) => { value.tables = []; },
    (value: BackgroundAttestation) => { value.tables.push({ ...value.tables[0]!, name: 'new_table' }); },
    (value: BackgroundAttestation) => { value.tables[0]!.eligibility.IsHealthy = true; },
    (value: BackgroundAttestation) => { value.tables[0]!.eligibility.AccelerationPendingDataFilesCount = 1; },
    (value: BackgroundAttestation) => { value.effective.Consumed = 1; },
    (value: BackgroundAttestation) => { value.runningQueries = [{ Text: '1', State: 'Running' }]; },
    (value: BackgroundAttestation) => { value.recentQueries = [{ Database: 'test', Text: 'external_table("arrowcol_old")' }]; },
    (value: BackgroundAttestation) => { value.recentQueries = [{ Database: 'other', Text: 'database("test").external_table("arrowcol_old")' }]; },
  ]) {
    const actual = structuredClone(expected);
    change(actual);
    assert.throws(() => requireSameBackground(expected, actual), /attestation|changed|activity|active|invalid/i);
  }
  const table = passive();
  for (const statistics of [{ ...table.statistics, HotSize: 1 }, { ...table.statistics, AccelerationPendingDataFilesSize: 1 },
    { ...table.statistics, NotHealthyReason: 'InaccessibleDeltaTable: forbidden' }])
    assert.throws(() => attestBackgroundTable('test', table.definition, table.details, table.policy, statistics), /strictly idle/);
  assert.throws(() => attestBackgroundTable('test', table.definition, { ...table.details,
    QueryAccelerationState: JSON.stringify({ IsHealthy: false, NotHealthyReason: table.statistics.NotHealthyReason,
      RowCount: 1, AcceleratedRowCount: 0 }) }, table.policy, table.statistics), /strictly idle/);
});

test('inventory uses only documented read commands and records missing-source background without mutations', async () => {
  const table = passive(), calls: string[] = [];
  const result = await captureBackground(['test', 'test'], async (database, command) => {
    assert.equal(database, 'test');
    calls.push(command);
    if (command === '.show external tables') return response([table.definition]);
    if (command.endsWith('statistics')) return response([table.statistics]);
    if (command.endsWith('details')) return response([table.details]);
    if (command.endsWith('schema as json')) return response([{ TableName: table.definition.TableName,
      DatabaseName: database, Schema: JSON.stringify(table.definition.schema) }]);
    if (command.endsWith('policy query_acceleration')) return response([{ Policy: JSON.stringify(table.policy) }]);
    if (command === '.show capacity') return response([background().effective]);
    if (command === '.show running queries' || command.startsWith('.show queries')) return response([]);
    throw new Error(`Unexpected command ${command}`);
  });
  assert.equal(result.tables.length, 1);
  assert.ok(calls.every(command => command.startsWith('.show ')));
});

test('policy-only apply conditionally claims ownership; restore reverts QAP before ownership release and preserves ingestion', async () => {
  const { state, receipt, io } = harness();
  const applied = await controlCluster('apply', receipt, { capacity: candidate, observations: saturated }, io);
  assert.equal(applied.owned, true);
  assert.equal(applied.policyRestored, false);
  assert.deepEqual(state.patches[0]!.body, { tags: { ...stampTags, qapCampaignOwner: receipt.owner } });
  assert.equal(receipt.policyMutations![0]!.state, 'verified');
  assert.equal(receipt.policyMutations![0]!.effective!.Total, 32);
  state.ingestion = 700;
  const restored = await controlCluster('restore', receipt, {}, io);
  assert.equal(restored.restored, true);
  assert.deepEqual(state.patches.at(-1)!.policy, baseline);
  assert.equal(state.ingestion, 700);
  assert.equal(state.submissions.length, 2);
  assert.ok(receipt.restoredAt);
});

test('expanded receipt admits the bounded fixed E32 trial and restores the original policy and topology', async () => {
  const { state, receipt, io } = harness();
  const sku = 'Standard_E32ads_v5' as const;
  const profile = { sku, nodes: 12, autoscale: { enabled: false, minimum: 12, maximum: 12 } };
  const capacity = { ClusterMaximumConcurrentOperations: 256, CoreUtilizationCoefficient: 0.5 };
  io.availableSkus = async () => [{ sku: { name: sku } }];
  const options = { profile, capacity, observations: saturated };
  await assert.rejects(controlCluster('apply', receipt, options, io), /exceeds/);
  receipt.expanded = true;
  await assert.rejects(controlCluster('apply', receipt, { ...options, profile: {
    ...profile, autoscale: { ...profile.autoscale, enabled: true },
  } }, io), /exceeds/);
  assert.throws(() => validateClusterProfile({
    ...profile, nodes: 13, autoscale: { enabled: false, minimum: 13, maximum: 13 },
  }, true), /exceeds/);
  assert.equal(state.patches.length, 0);
  const applied = await controlCluster('apply', receipt, options, io);
  assert.deepEqual(applied.profile, profile);
  assert.deepEqual(applied.capacity, capacity);
  assert.equal((await controlCluster('restore', receipt, {}, io)).restored, true);
  assert.deepEqual(state.patches.at(-1)!.policy, baseline);
});

test('lost or partial successful policy responses reconcile exact live policy with no second submission', async () => {
  for (const outcome of ['lost-applied', 'partial-applied'] as const) {
    const { state, receipt, io } = harness();
    state.outcome = outcome;
    await controlCluster('apply', receipt, { capacity: candidate, observations: saturated }, io);
    assert.equal(state.submissions.length, 1);
    assert.equal(receipt.policyMutations![0]!.state, 'verified');
    assert.ok(state.saved.some(value => value.policyMutations?.at(-1)?.state === 'uncertain'));
  }
});

test('unknown policy outcomes cannot be retried or release ownership; later exact readback permits guarded restoration', async () => {
  const { state, receipt, io } = harness();
  state.outcome = 'lost-unapplied';
  await assert.rejects(controlCluster('apply', receipt, { capacity: candidate, observations: saturated }, io), /uncertain/);
  assert.equal(receipt.policyMutations![0]!.state, 'uncertain');
  assert.equal((await clusterStatus(receipt, io)).restored, false);
  await assert.rejects(controlCluster('apply', receipt, { capacity: candidate, observations: saturated }, io), /uncertain/);
  await assert.rejects(controlCluster('restore', receipt, {}, io), /uncertain/);
  assert.equal(state.submissions.length, 1);
  assert.equal(state.patches.length, 1);
  state.policy = { ...candidate };
  state.outcome = 'success';
  assert.equal((await controlCluster('restore', receipt, {}, io)).restored, true);
  assert.equal(state.submissions.length, 2);
});

test('uncertain restore retains ownership until a fresh exact original-policy readback, then never resubmits', async () => {
  const { state, receipt, io } = harness();
  await controlCluster('apply', receipt, { capacity: candidate, observations: saturated }, io);
  state.outcome = 'lost-unapplied';
  await assert.rejects(controlCluster('restore', receipt, {}, io), /uncertain/);
  assert.equal(state.patches.length, 1);
  state.policy = { ...baseline };
  const restored = await controlCluster('restore', receipt, {}, io);
  assert.equal(restored.restored, true);
  assert.equal(state.submissions.length, 2);
  assert.equal(state.patches.length, 2);
});

test('partial readback and unknown ownership PATCH results retain recoverable intents without replay', async () => {
  const partial = harness(), manage = partial.io.management;
  let failCapacityRead = true;
  partial.io.management = async command => {
    if (command === '.show capacity' && failCapacityRead) {
      failCapacityRead = false;
      throw new Error('Effective capacity read unavailable');
    }
    return manage(command);
  };
  await assert.rejects(controlCluster('apply', partial.receipt, { capacity: candidate, observations: saturated }, partial.io),
    /capacity read unavailable/);
  assert.equal(partial.receipt.policyMutations![0]!.state, 'accepted');
  assert.equal((await controlCluster('restore', partial.receipt, {}, partial.io)).restored, true);
  assert.equal(partial.state.submissions.length, 2);

  const unknown = harness();
  let patches = 0;
  unknown.io.patchCluster = async () => { patches++; throw new Error('PATCH connection lost'); };
  await assert.rejects(controlCluster('apply', unknown.receipt, { capacity: candidate, observations: saturated }, unknown.io),
    /PATCH connection lost/);
  await assert.rejects(controlCluster('restore', unknown.receipt, {}, unknown.io), /remains uncertain/);
  await assert.rejects(controlCluster('apply', unknown.receipt, { capacity: candidate, observations: saturated }, unknown.io),
    /remains uncertain/);
  assert.equal((await clusterStatus(unknown.receipt, unknown.io)).restored, false);
  assert.equal(patches, 1);
  assert.equal(unknown.state.submissions.length, 0);
});

test('last-moment owned-policy drift and restoration-reserve expiry prevent management submissions', async () => {
  const drift = harness(), save = drift.io.save;
  drift.io.save = async receipt => {
    await save(receipt);
    if (receipt.policyMutations?.at(-1)?.state === 'intent')
      drift.state.policy = { ...baseline, ClusterMaximumConcurrentOperations: 20 };
  };
  await assert.rejects(controlCluster('apply', drift.receipt, { capacity: candidate, observations: saturated }, drift.io),
    /changed before submission/);
  assert.equal(drift.state.submissions.length, 0);
  assert.equal(drift.state.policy.ClusterMaximumConcurrentOperations, 20);
  assert.equal(drift.receipt.policyMutations![0]!.state, 'not-submitted');
  const late = harness();
  late.io.now = () => late.receipt.deadlineMs - 90 * 60_000;
  await assert.rejects(controlCluster('apply', late.receipt, { capacity: candidate, observations: saturated }, late.io), /reserve/);
  assert.equal(late.state.patches.length, 0);
});

test('a failed pre-submission policy read does not strand ownership or permit replay of an uncertain write', async () => {
  const { state, receipt, io } = harness(), save = io.save, management = io.management;
  let failGuard = false;
  io.save = async value => {
    await save(value);
    if (value.policyMutations?.at(-1)?.state === 'intent') failGuard = true;
  };
  io.management = async command => {
    if (command === '.show cluster policy capacity' && failGuard) {
      failGuard = false;
      throw new Error('Preflight read unavailable');
    }
    return management(command);
  };
  await assert.rejects(controlCluster('apply', receipt, { capacity: candidate, observations: saturated }, io), /Preflight/);
  assert.equal(receipt.policyMutations![0]!.state, 'not-submitted');
  assert.equal((await controlCluster('restore', receipt, {}, io)).restored, true);
  assert.equal(state.submissions.length, 0);
  assert.equal(state.patches.length, 2);
});

test('foreign policy/ownership changes and passive background activity refuse mutations', async () => {
  const drift = harness();
  await controlCluster('apply', drift.receipt, { capacity: candidate, observations: saturated }, drift.io);
  drift.state.policy = { ...candidate, ClusterMaximumConcurrentOperations: 20 };
  await assert.rejects(controlCluster('restore', drift.receipt, {}, drift.io), /outside the campaign/);
  assert.equal(drift.state.patches.length, 1);
  assert.equal((await clusterStatus(drift.receipt, drift.io)).restored, false);
  drift.state.policy = { ...candidate };
  (drift.state.cluster.tags as Row).qapCampaignOwner = 'someone-else';
  await assert.rejects(controlCluster('restore', drift.receipt, {}, drift.io), /ownership was lost/);
  for (const alteration of [
    (attestation: BackgroundAttestation) => { attestation.tables[0]!.policy.Hot = '1.00:00:00'; },
    (attestation: BackgroundAttestation) => { attestation.runningQueries = [{ Text: 'active' }]; },
  ]) {
    const { state, receipt, io } = harness();
    alteration(state.background);
    await assert.rejects(controlCluster('apply', receipt, { capacity: candidate, observations: saturated }, io), /changed|active/);
    assert.equal(state.patches.length, 0);
    assert.equal(state.submissions.length, 0);
  }
});

test('legacy receipts remain restorable but cannot authorize apply or silently bypass a missing v2 attestation', async () => {
  const { receipt, io, state } = harness();
  delete receipt.background;
  assert.throws(() => validateReceipt(receipt, 'cluster'), /attestation/);
  receipt.schemaVersion = 1;
  delete receipt.policyMutations;
  validateReceipt(receipt, 'cluster');
  await assert.rejects(controlCluster('apply', receipt, { capacity: candidate, observations: saturated }, io), /version 2/);
  assert.equal((await controlCluster('restore', receipt, {}, io)).restored, true);
  assert.equal(state.submissions.length, 0);
  const scaled = validateClusterProfile({ ...fixed, nodes: 8, autoscale: { enabled: false, minimum: 8, maximum: 8 } });
  state.cluster.sku = { name: scaled.sku, capacity: 8 };
  state.cluster.tags = { ...stampTags, qapCampaignOwner: receipt.owner };
  (state.cluster.properties as Row).optimizedAutoscale = { isEnabled: false, minimum: 8, maximum: 8 };
  receipt.mutations.push({ kind: 'apply', submittedAt: new Date(0).toISOString(), profile: scaled, state: 'verified' });
  assert.equal((await controlCluster('restore', receipt, {}, io)).restored, true);
  assert.equal(state.patches.length, 1);
  state.policy = { ...candidate };
  assert.equal((await clusterStatus(receipt, io)).restored, false);
  await assert.rejects(controlCluster('restore', receipt, {}, io), /outside the campaign/);
});

test('producer clock uncertainty includes both request endpoints and never manufactures a missing bound', () => {
  assert.equal(clockErrorBound([]), null);
  assert.equal(clockErrorBound([{ startedAtMs: 1000, endedAtMs: 1100, producerAtMs: 1050 }]), null);
  assert.equal(clockErrorBound([{ startedAtMs: 1000, endedAtMs: 1100, producerAtMs: 1050 },
    { startedAtMs: 2000, endedAtMs: 2180, producerAtMs: 1950 }]), 230);
  assert.throws(() => clockErrorBound([{ startedAtMs: 1000, endedAtMs: 999, producerAtMs: 1000 },
    { startedAtMs: 2000, endedAtMs: 2180, producerAtMs: 2100 }]), /Invalid/);
});

test('terminal source capture retains records emitted while the post-window writer stops', async () => {
  const directory = await mkdtemp(join(tmpdir(), 'qap-stop-'));
  try {
    const marker = join(directory, 'stop');
    const logs = await captureWriterShutdown([process.execPath, '-e', `
      const fs = require('node:fs');
      console.log('before stop');
      const timer = setInterval(() => {
        if (!fs.existsSync(process.argv[1])) return;
        console.log('late durable commit');
        clearInterval(timer);
      }, 10);
    `, marker], () => writeFile(marker, 'stop'));
    assert.equal(logs, 'before stop\nlate durable commit\n');
  } finally { await rm(directory, { recursive: true, force: true }); }
});

test('unavailable or incomplete final log capture fails rather than stopping or certifying the source', async () => {
  let stopped = false;
  await assert.rejects(captureWriterShutdown([process.execPath, '-e', 'process.exit(1)'],
    async () => { stopped = true; }), /Writer log capture failed/);
  assert.equal(stopped, false);
  await assert.rejects(captureWriterShutdown([process.execPath, '-e',
    'console.log("before stop"); setInterval(() => {}, 1000)'], async () => {}, 100), /abort/i);
});
