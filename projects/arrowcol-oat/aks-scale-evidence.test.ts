import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { readFile } from 'node:fs/promises';
import { test, type TestContext } from 'node:test';
import { Azure, Config, canonical_json } from '../../infra/azure/bootstrap.js';
import { createBlobCampaignStore, newCampaignState, type CampaignStore, type RunInventory } from './aks-campaign.js';
import { assessCommitRateResolution, createScaleEvidenceVerifier, readBoundedSourceBytes, readOfficialCellMeasurement, readOfficialKustoWindow,
  readOfficialNativeWindow } from './aks-scale-evidence.js';
import type { ScaleMeasurementEnvelope } from './aks-scale.js';

const hash = (value: string | Uint8Array) => createHash('sha256').update(value).digest('hex');
const campaignId = 'c0ffee00-0000-4000-8000-000000000001', ownerId = 'c0ffee00-0000-4000-8000-000000000002';
const cellId = 'c0ffee00-0000-4000-8000-000000000003', orchestrationId = 'c0ffee00-0000-4000-8000-000000000004';
const now = 1_800_000_000_000;

function syntheticAuthenticatedTransport(t: TestContext, supplied?: Uint8Array, window?: { startMs: number; endMs: number }) {
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
  const seconds = window ? (window.endMs - window.startMs) / 1000 : 30;
  const screen = { durable_rows_per_s: 1234, rate_resolution_rows_per_second: 30 / seconds, window_commit_count: 100, max_commit_rows: 30 };
  const outputFixture = { teardown_outcome: 'succeeded',
    scope: 'new_ingestion_commits_finished_in_half_open_shared_window_not_client_cohort',
    measurement_start_unix_ms: window?.startMs ?? now - 60_000, measurement_end_unix_ms: window?.endMs ?? now - 30_000,
    window_seconds: seconds, rows_per_second: 1234 };
  const document = { schema_version: 1, scope: 'original_official_cell_window_not_paired_scale_authorization',
    origin: { campaign_id: campaignId, owner_id: ownerId, fencing_token: 1, orchestration_run_id: orchestrationId,
      cell_run_id: cellId, namespace_uid: null },
    submitted_config_sha256: 'b'.repeat(64), effective_config_json: effective, effective_config_sha256: hash(effective),
    source_revision: 'a'.repeat(40),
    official_result: { run_id: cellId, table: 'synthetic_offline_table', status: 'succeeded', git_sha: 'a'.repeat(40),
      tunables: { orchestration_run_id: orchestrationId }, evidence: { version: 1, teardown_outcome: 'succeeded',
        screen_v1: structuredClone(screen), durable_commits: { output_throughput: structuredClone(outputFixture),
          delta_log_reconciliation: { csv: structuredClone(screen) }, collector_teardown_admissions: { status: 'clear', cancelled_ambiguous: 0 }, run_cumulative_output: { teardown_outcome: 'succeeded' },
          complete_30_second_commit_windows: [{ teardown_outcome: 'succeeded', rate_resolution_rows_per_second: 1, window_commit_count: 100 }],
          post_teardown_delta_reconciliation: { status: 'matched' } },
        reconciliation: { status: 'within_bound', settled: true, ambiguous_rows: 0 }, commit_window_output_throughput: structuredClone(outputFixture) } } };
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
    publishSyntheticDocument: () => {
      bytes = Buffer.from(canonical_json(document));
      Object.assign(inventory.official_measurement!, { sha256: hash(bytes), byte_length: bytes.byteLength,
        effective_config_sha256: document.effective_config_sha256 });
    },
    failStatus: () => { status = 403; }, wrongEtag: () => { returnedEtag = '"forged"'; }, fence: () => { changed = true; } };
}

test('official reader caps actual custody transport before and after the immutable object under its original deadline', async t => {
  for (const defect of ['none', 'initial-overflow', 'final-overflow', 'blocked-final', 'changed-custody'] as const) {
    await t.test(defect, async t => {
      const f = syntheticAuthenticatedTransport(t);
      let clock = now, afterObjectReads = 0, pulls = 0, cancelled = false;
      t.mock.method(Date, 'now', () => clock);
      f.options.now = () => clock;
      f.options.deadlineAtMs = now + 100;
      const artifactFetch = f.options.fetcher;
      f.options.fetcher = async (input, init) => {
        const response = await artifactFetch(input, init);
        clock += 50;
        return response;
      };
      if (defect === 'changed-custody') f.fence();
      const fetchCustody: typeof fetch = async (input, init) => {
        const afterObject = f.counts().reads > 0;
        if (afterObject) afterObjectReads++;
        assert.equal(new URL(String(input)).pathname, '/' + f.options.azure.c.checkpoints + '/active.json');
        assert.equal(new Headers(init?.headers).get('authorization'), 'Bearer synthetic-offline-token');
        assert.ok(init?.signal, 'the actual custody transport must receive the original deadline signal');
        if (defect === 'initial-overflow' || defect === 'final-overflow' && afterObject)
          return new Response(new ReadableStream<Uint8Array>({
            pull(controller) {
              assert.ok(++pulls <= 2, 'overflow must cancel before a third chunk is requested');
              controller.enqueue(new Uint8Array(2_097_153));
            },
            cancel() { cancelled = true; },
          }, { highWaterMark: 0 }), { headers: { etag: '"synthetic-custody"' } });
        if (defect === 'blocked-final' && afterObject)
          return new Response(new ReadableStream<Uint8Array>({ cancel() { cancelled = true; } }),
            { headers: { etag: '"synthetic-custody"' } });
        return new Response(JSON.stringify(f.state), { headers: { etag: '"synthetic-custody"' } });
      };
      const store = createBlobCampaignStore({ account: f.options.azure.c.account,
        container: f.options.azure.c.checkpoints, subscriptionId: campaignId }, fetchCustody,
      resource => f.options.azure.token(resource));
      const read = store.read.bind(store);
      store.read = options => {
        assert.deepEqual(options, { maxResponseBodyBytes: 4_194_304, deadlineUnixMs: now + 100 });
        return read(options);
      };
      f.options.store = store;
      const keepAlive = setTimeout(() => {}, 1000);
      try {
        if (defect === 'none') {
          assert.equal((await readOfficialCellMeasurement(f.options)).committedRowsPerSecond, 1234);
          assert.equal(afterObjectReads, 1);
        } else {
          await assert.rejects(readOfficialCellMeasurement(f.options),
            defect.includes('overflow') ? /byte cap/ : defect === 'blocked-final' ? /original deadline/ : /fenc|inventory|custody/i);
          if (defect !== 'changed-custody') assert.equal(cancelled, true);
          if (defect.includes('overflow')) assert.equal(pulls, 2);
          assert.equal(f.counts().reads, defect === 'initial-overflow' ? 0 : 1);
        }
      } finally { clearTimeout(keepAlive); }
    });
  }
});

function syntheticNativeTransport(t: TestContext) {
  const f = syntheticAuthenticatedTransport(t), start = now - 60_000, end = now - 30_000;
  const namespaceUid = 'c0ffee00-0000-4000-8000-000000000005';
  f.inventory.namespace.uid = namespaceUid;
  Object.assign(f.document.origin, { namespace_uid: namespaceUid });
  f.options.azure.c.uri = 'https://synthetic-offline.kusto.windows.net';
  const context = 'synthetic-offline-context', server = 'https://synthetic-offline.invalid';
  f.inventory.fic = { resource_id: '/synthetic/federatedIdentityCredentials/offline', name: 'offline',
    state: 'observed', absence: null, plan: { version: 1, runId: cellId, namespace: f.inventory.namespace.name,
      serviceAccount: 'oat', identityId: '/synthetic/identity', resourceId: '/synthetic/federatedIdentityCredentials/offline',
      clientId: campaignId, tenantId: ownerId, aksId: '/synthetic/aks', server, credentialName: 'offline',
      issuer: 'https://synthetic-offline.invalid/oidc/', subject: 'system:serviceaccount:synthetic:oat',
      audience: 'api://AzureADTokenExchange' } };
  f.inventory.qap = { database: 'synthetic', state: 'observed', absence: [],
    bindings: [{ external_table: 'synthetic_external', relative_path: 'synthetic_offline_table' }],
    reference: { schema_version: 1, account: f.options.azure.c.account, container: f.options.azure.c.checkpoints,
      blob: 'synthetic/qap.json', run_id: cellId, nonce: ownerId, manifest_sha256: 'f'.repeat(64),
      namespace_uid: namespaceUid, namespace_owner_token: ownerId } };
  const effective = { typed_oat_config: { tables: ['synthetic_offline_table'],
    qap_endpoint: f.options.azure.c.uri, kubectl_context: context },
    resolved_aks_resources: { collector: { limits: { cpu: '2', memory: '1Gi' } } } };
  f.document.effective_config_json = canonical_json(effective);
  f.document.effective_config_sha256 = hash(f.document.effective_config_json);
  const identity = { role: 'collector', pod: 'collector-pod', uid: 'synthetic-pod-uid', node: 'synthetic-node',
    container: 'collector', container_id: 'containerd://' + 'a'.repeat(64) };
  const frames = [start - 1000, start + 9000, start + 19_000, end + 1000].map((at, i) => {
    const time = at + 50;
    return { request_start_unix_ms: at, request_end_unix_ms: at + 100,
      qap: { observed_unix_ms: at + 50, tables: { synthetic_external: { available: true,
        statistics: { AccelerationPendingDataFilesCount: 10 + 2 * i, AccelerationPendingDataFilesSize: 1000 + 200 * i,
          AccelerationCompletePercentage: 50, LastUpdatedDateTime: '2010-01-01T00:00:00Z', Eligible: true, Fresh: null },
        state: { IsHealthy: true, NotHealthyReason: null } } } },
      pods: [{ identity: { ...identity, namespace: f.inventory.namespace.name,
        started_unix_ns: (BigInt(start - 10_000) * 1_000_000n).toString() },
        limits: { cpu: '2', memory: '1Gi' },
        cpu: { time_unix_ns: (BigInt(time) * 1_000_000n).toString(), time_resolution_ns: '1000000000',
          usage_core_nanoseconds: (9_007_199_254_740_993n + BigInt(time - start + 1000) * 2_000_000n).toString() },
        memory: { time_unix_ns: (BigInt(time) * 1_000_000n).toString(), time_resolution_ns: '1000000000',
          working_set_bytes: '1000000000' },
        network: { time_unix_ns: (BigInt(time) * 1_000_000n).toString(), time_resolution_ns: '1000000000',
          scope: 'pod_network_not_container_or_link_capacity',
          rx_bytes: (BigInt(time - start + 1000) * 1000n).toString(),
          tx_bytes: (BigInt(time - start + 1000) * 2000n).toString() } }] };
  });
  const history = { schema_version: 1, scope: 'original_cell_source_observations_not_scale_authorization',
    binding: { campaign_id: campaignId, owner_id: ownerId, fencing_token: 1, cell_run_id: cellId,
      namespace_uid: namespaceUid, namespace: f.inventory.namespace.name, server, kube_context: context,
      qap_endpoint: f.options.azure.c.uri, qap_database: f.inventory.qap.database,
      qap_bindings: f.inventory.qap.bindings, qap_reference: f.inventory.qap.reference },
    window_start_unix_ms: start, window_end_unix_ms: end, cadence_ms: 10_000, sample_timeout_ms: 3000,
    samples: frames, sampling_failures: 0, samples_dropped: 0, pod_targets: [identity] };
  Object.assign(f.document.official_result.evidence, { native_telemetry: history });
  f.publishSyntheticDocument();
  return { ...f, history };
}

test('native observations use immutable cell custody, original windows and exact stable container counters', async t => {
  const f = syntheticNativeTransport(t), measured = await readOfficialNativeWindow(f.options);
  assert.equal(measured.qap.unit, 'pending_data_files');
  assert.equal(measured.qap.pendingDataFiles, 14);
  assert.equal(measured.qap.pendingDataBytes, 1400);
  assert.equal(measured.qap.growing, true);
  assert.equal(measured.qap.samples[0]!.lastUpdatedDateTime, '2010-01-01T00:00:00Z');
  assert.equal(measured.pods[0]!.cpuCores, 2);
  assert.equal(measured.pods[0]!.cpuSaturated, null);
  assert.equal(measured.pods[0]!.counterObservationScope, 'bracketing_source_intervals_not_original_window_averages');
  assert.equal(measured.pods[0]!.memorySaturated, true);
  assert.equal(measured.pods[0]!.rxBytesPerSecond, 1_000_000);
  assert.equal(measured.pods[0]!.networkSaturated, null);
  assert.equal(measured.pods[0]!.throttling, null);
  assert.match(measured.authorization, /hold/);
});

test('native dimensions independently refuse wrong source, resource identity, partial history and counter resets', async t => {
  for (const defect of ['qap-origin', 'qap-table', 'pod-id', 'reset', 'future-counter', 'server', 'partial', 'cell'] as const) {
    const f = syntheticNativeTransport(t);
    if (defect === 'qap-origin') f.history.binding.qap_endpoint = 'https://wrong-offline.invalid';
    if (defect === 'qap-table') f.history.samples[1]!.qap.tables.synthetic_external.available = false;
    if (defect === 'pod-id') f.history.samples[1]!.pods[0]!.identity.container_id = 'containerd://restarted';
    if (defect === 'reset') f.history.samples[1]!.pods[0]!.cpu.usage_core_nanoseconds = '1';
    if (defect === 'future-counter') f.history.samples[1]!.pods[0]!.cpu.time_unix_ns = (BigInt(now) * 1_000_000n).toString();
    if (defect === 'server') f.history.binding.server = 'https://wrong-offline.invalid';
    if (defect === 'partial') f.history.samples.pop();
    if (defect === 'cell') f.history.binding.cell_run_id = ownerId;
    f.publishSyntheticDocument();
    const measured = await readOfficialNativeWindow(f.options);
    if (['qap-origin', 'qap-table', 'partial', 'cell'].includes(defect)) assert.equal(measured.qap.growing, null, defect);
    else assert.equal(measured.qap.growing, true, defect);
    if (['pod-id', 'reset', 'future-counter'].includes(defect)) {
      assert.equal(measured.pods[0]!.cpuSaturated, null, defect);
      if (defect !== 'pod-id') assert.equal(measured.pods[0]!.memorySaturated, true, defect);
    }
    if (['qap-origin', 'qap-table'].includes(defect)) assert.equal(measured.pods[0]!.cpuCores, 2, defect);
    assert.match(measured.authorization, /hold/);
  }
});

test('native throttling needs timestamped CFS history and ambiguous CPU windows remain unknown', async t => {
  for (const timestamps of [true, false]) {
    const f = syntheticNativeTransport(t);
    f.history.samples.forEach((frame, i) => Object.assign(frame.pods[0]!, { throttling: {
      time_unix_ns: timestamps ? frame.pods[0]!.cpu.time_unix_ns : null,
      time_resolution_ns: '1000000', periods: String(1000 + 100 * i), throttled_periods: String(100 + 20 * i),
    } }));
    f.publishSyntheticDocument();
    const measured = await readOfficialNativeWindow(f.options);
    assert.equal(measured.pods[0]!.throttling, timestamps ? 0.2 : null);
    assert.equal(measured.pods[0]!.throttled, null);
    assert.equal(measured.pods[0]!.cpuSaturated, null);
  }
  const f = syntheticNativeTransport(t);
  const base = 9_007_199_254_740_993n, first = f.history.samples[0]!.request_start_unix_ms;
  f.history.samples.forEach(frame => {
    frame.pods[0]!.cpu.usage_core_nanoseconds = (base + BigInt(frame.request_start_unix_ms - first) * 1_800_000n).toString();
  });
  f.publishSyntheticDocument();
  assert.equal((await readOfficialNativeWindow(f.options)).pods[0]!.cpuSaturated, null,
    'boundary contributions or source-time precision cannot become a false positive at the 90% boundary');
});

test('original-window counter saturation does not inherit an exterior CPU average', async t => {
  for (const [name, increments, saturated] of [
    ['92 percent original usage diluted by idle exterior', [0n, 16_652_000_000n, 35_052_000_000n, 55_200_000_000n], null],
    ['outside-only burst cannot prove original saturation', [0n, 64_000_000_000n, 64_000_000_000n, 64_000_000_000n], null],
    ['interior work alone proves saturation', [0n, 1_000_000_000n, 56_000_000_000n, 60_000_000_000n], true],
    ['all bracketing work is below the original threshold', [0n, 10_000_000_000n, 20_000_000_000n, 25_000_000_000n], false],
  ] as const) {
    await t.test(name, async t => {
      const f = syntheticNativeTransport(t);
      f.history.samples.forEach((frame, i) => {
        frame.pods[0]!.cpu.usage_core_nanoseconds = (9_007_199_254_740_993n + increments[i]!).toString();
      });
      f.publishSyntheticDocument();
      const measured = await readOfficialNativeWindow(f.options);
      assert.equal(measured.pods[0]!.cpuSaturated, saturated);
    });
  }
});

test('original-window counter saturation does not inherit an exterior CFS fraction', async t => {
  for (const [name, counts, saturated] of [
    ['boundary contributions can hide original throttling', [0, 0, 20, 20], null],
    ['outside-only throttling cannot prove original throttling', [0, 40, 40, 40], null],
    ['interior throttling proves the original threshold', [0, 0, 40, 40], true],
    ['all bracketing throttling is below the original threshold', [0, 0, 1, 1], false],
  ] as const) {
    await t.test(name, async t => {
      const f = syntheticNativeTransport(t);
      f.history.samples.forEach((frame, i) => Object.assign(frame.pods[0]!, { throttling: {
        time_unix_ns: frame.pods[0]!.cpu.time_unix_ns, time_resolution_ns: '1000000',
        periods: String(1000 + 100 * i), throttled_periods: String(100 + counts[i]!),
      } }));
      f.publishSyntheticDocument();
      assert.equal((await readOfficialNativeWindow(f.options)).pods[0]!.throttled, saturated);
    });
  }
});

test('original-window CPU bounds do not assign a precision-straddling counter to the exterior', async t => {
  for (const [resolution, expected] of [['1', false], ['1000000000', null]] as const) {
    await t.test(resolution + 'ns precision', async t => {
      const f = syntheticNativeTransport(t), start = f.history.window_start_unix_ms;
      const increments = [0n, 16_652_000_000n, 35_052_000_000n, 55_200_000_000n];
      f.history.samples.forEach((frame, i) => {
        frame.pods[0]!.cpu.usage_core_nanoseconds = (9_007_199_254_740_993n + increments[i]!).toString();
      });
      const boundary = structuredClone(f.history.samples[0]!);
      boundary.request_start_unix_ms = start - 100;
      boundary.request_end_unix_ms = start + 100;
      boundary.qap.observed_unix_ms = start - 50;
      for (const metric of [boundary.pods[0]!.cpu, boundary.pods[0]!.memory, boundary.pods[0]!.network])
        metric.time_unix_ns = (BigInt(start - 50) * 1_000_000n).toString();
      boundary.pods[0]!.cpu.time_resolution_ns = resolution;
      boundary.pods[0]!.cpu.usage_core_nanoseconds = (9_007_199_254_740_993n + 10_000_000_000n).toString();
      f.history.samples.splice(1, 0, boundary);
      f.publishSyntheticDocument();
      assert.equal((await readOfficialNativeWindow(f.options)).pods[0]!.cpuSaturated, expected);
    });
  }
});

test('actual Rust native pod/cAdvisor producer bytes cross the authenticated immutable reader', async t => {
  const path = process.env.OAT_OFFLINE_NATIVE_PODS_FIXTURE;
  if (!path) { t.skip('requires retained synthetic Rust native producer bytes'); return; }
  const pods: unknown = JSON.parse(await readFile(path, 'utf8')) as unknown;
  assert.ok(Array.isArray(pods));
  assert.equal(pods.length, 4);
  const f = syntheticNativeTransport(t);
  f.history.samples.forEach((frame, i) => Object.assign(frame, { pods: [pods[i]] }));
  f.publishSyntheticDocument();
  const measured = await readOfficialNativeWindow(f.options);
  assert.equal(measured.pods[0]!.cpuCores, 2);
  assert.equal(measured.pods[0]!.cpuSaturated, null);
  assert.equal(measured.pods[0]!.memorySaturated, true);
  assert.equal(measured.pods[0]!.throttling, 0.2);
  assert.equal(measured.pods[0]!.throttled, null);
  assert.match(measured.authorization, /hold/);
});

test('raw submitted bytes must match the server identity, not merely equivalent JSON or a source label', async t => {
  const raw = Buffer.from('{\n  "rows_per_table": 10, "note": "\\u03bb"\n}\n');
  for (const defect of ['none', 'boundary', 'missing', 'reserialized', 'oversize'] as const) {
    await t.test(defect, async t => {
      const f = syntheticAuthenticatedTransport(t);
      const size = defect === 'boundary' ? 2_097_152 : defect === 'oversize' ? 2_097_153 : raw.byteLength;
      const submitted = Buffer.concat([raw, Buffer.alloc(size - raw.byteLength, 0x20)]);
      f.document.submitted_config_sha256 = hash(submitted);
      f.inventory.official_measurement!.submitted_config_sha256 = hash(submitted);
      f.publishSyntheticDocument();
      const supplied = defect === 'missing' ? undefined : defect === 'reserialized'
        ? Buffer.from(JSON.stringify(JSON.parse(raw.toString('utf8')) as unknown)) : submitted;
      const observed = await readOfficialCellMeasurement({ ...f.options, submittedConfigBytes: supplied });
      const matching = defect === 'none' || defect === 'boundary';
      assert.equal(observed.unavailableReasons.some(reason => reason.startsWith('submitted_config_bytes_')), !matching);
      assert.equal(observed.submittedConfigIdentity, matching ? 'verified_exact_server_bytes' : 'unknown');
      assert.equal(observed.committedRowsPerSecond, 1234);
    });
  }
});

test('real raw POST/GET identity crosses Rust immutable publication, custody and the native reader without JSON reconstruction', {
  skip: !process.env.OAT_OFFLINE_RAW_IDENTITY_FIXTURE,
}, async t => {
  const proof = JSON.parse(await readFile(process.env.OAT_OFFLINE_RAW_IDENTITY_FIXTURE!, 'utf8')) as {
    fixture_source: string; cases: { submitted_config_body: string; official_measurement_utf8: string }[];
  };
  assert.equal(proof.fixture_source, 'retained_loopback_post_get');
  assert.ok(process.env.GIT_SHA, 'actual raw-byte witness requires the exact source revision');
  assert.equal(proof.cases.length, 2);
  assert.deepEqual(JSON.parse(proof.cases[0]!.submitted_config_body) as unknown,
    JSON.parse(proof.cases[1]!.submitted_config_body) as unknown);
  assert.notEqual(hash(proof.cases[0]!.submitted_config_body), hash(proof.cases[1]!.submitted_config_body));
  for (const entry of proof.cases) {
    const f = syntheticAuthenticatedTransport(t, Buffer.from(entry.official_measurement_utf8));
    const exactBytes = Buffer.from(entry.submitted_config_body, 'utf8');
    const matched = await readOfficialNativeWindow({ ...f.options, submittedConfigBytes: exactBytes });
    assert.equal(matched.original.submittedConfigIdentity, 'verified_exact_server_bytes');
    assert.equal(matched.original.reference.submitted_config_sha256, hash(exactBytes));
    assert.equal(matched.original.reference.source_revision, process.env.GIT_SHA);
    assert.notEqual(matched.original.reference.effective_config_sha256, hash(exactBytes));
    assert.equal(matched.original.startMs, 1_000);
    assert.equal(matched.original.endMs, 31_000);
    assert.equal(matched.original.committedRowsPerSecond, null);
    assert.ok(matched.original.unavailableReasons.includes('original_official_cell_did_not_succeed'));
    assert.match(matched.authorization, /hold/);
    const otherBytes = Buffer.from(proof.cases.find(other => other !== entry)!.submitted_config_body, 'utf8');
    const mismatch = await readOfficialNativeWindow({ ...f.options, submittedConfigBytes: otherBytes });
    assert.equal(mismatch.original.submittedConfigIdentity, 'unknown');
    assert.ok(mismatch.original.unavailableReasons.includes('submitted_config_bytes_differ_from_server_identity'));
    const missing = await readOfficialNativeWindow(f.options);
    assert.equal(missing.original.submittedConfigIdentity, 'unknown');
    assert.ok(missing.original.unavailableReasons.includes('submitted_config_bytes_unavailable'));
  }
});

test('immutable measurement reader uses selected authenticated storage and cannot authorize an unpaired scale trial', async t => {
  const f = syntheticAuthenticatedTransport(t), raw = Buffer.from('{"synthetic_offline_accepted_submission":true}');
  f.document.submitted_config_sha256 = hash(raw);
  f.inventory.official_measurement!.submitted_config_sha256 = hash(raw);
  f.publishSyntheticDocument();
  const options = { ...f.options, submittedConfigBytes: raw }, observed = await readOfficialCellMeasurement(options);
  assert.equal(observed.committedRowsPerSecond, 1234);
  assert.deepEqual(observed.unavailableReasons, []);
  assert.deepEqual(f.counts(), { reads: 1, tokenReads: 1 });
  const envelope: ScaleMeasurementEnvelope = { schema_version: 2, campaign_id: campaignId, cell_run_id: cellId,
    observed_at_ms: observed.endMs!, window_ms: observed.endMs! - observed.startMs!,
    measurement: { committedRowsPerSecond: 1234, qapPendingBacklog: null } };
  const verifier = createScaleEvidenceVerifier(options), window = { startMs: observed.startMs!, endMs: observed.endMs!,
    notBeforeMs: observed.startMs!, deadlineAtMs: f.options.deadlineAtMs };
  await assert.rejects(verifier.verifyMeasurement(envelope, f.state, window), /paired baseline\/trial cell custody/);
  for (const invalid of [{ ...window, startMs: window.startMs + 1 }, { ...window, endMs: window.endMs - 1 },
    { ...window, notBeforeMs: window.startMs + 1 }, { ...window, deadlineAtMs: window.deadlineAtMs + 1 }])
    await assert.rejects(verifier.verifyMeasurement(envelope, f.state, invalid), /original cell\/window\/deadline/);
});

test('injected scale verifier reports low-commit windows while keeping admission closed', async t => {
  const f = syntheticAuthenticatedTransport(t), raw = Buffer.from('exact submission');
  f.document.submitted_config_sha256 = hash(raw);
  f.inventory.official_measurement!.submitted_config_sha256 = hash(raw);
  f.document.official_result.evidence.durable_commits.complete_30_second_commit_windows[0]!.window_commit_count = 99;
  f.publishSyntheticDocument();
  const verifier = createScaleEvidenceVerifier({ ...f.options, submittedConfigBytes: raw });
  const envelope: ScaleMeasurementEnvelope = { schema_version: 2, campaign_id: campaignId, cell_run_id: cellId,
    observed_at_ms: now - 30_000, window_ms: 30_000, measurement: { committedRowsPerSecond: 1234, qapPendingBacklog: null } };
  const window = { startMs: now - 60_000, endMs: now - 30_000, notBeforeMs: now - 60_000, deadlineAtMs: f.options.deadlineAtMs };
  await assert.rejects(verifier.verifyMeasurement(envelope, f.state, window), error => {
    assert.match(String(error), /paired baseline\/trial cell custody/);
    assert.match(String(error), /low_commit_windows: 1 \(99\)/);
    return true;
  });
  Object.assign(f.document.official_result.evidence.screen_v1, { rate_resolution_rows_per_second: null });
  Object.assign(f.document.official_result.evidence.durable_commits.delta_log_reconciliation.csv, { rate_resolution_rows_per_second: null });
  f.publishSyntheticDocument();
  await assert.rejects(verifier.verifyMeasurement(envelope, f.state, window), error => {
    assert.match(String(error), /provisional_commit_rate_resolution_unavailable/);
    assert.match(String(error), /low_commit_windows: 1 \(99\)/);
    return true;
  });
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

function syntheticMonitorTransport(t: TestContext) {
  const start = now - 120_000, end = now, f = syntheticAuthenticatedTransport(t, undefined, { startMs: start, endMs: end });
  const resourceId = f.options.azure.c.cluster_id, tokens: string[] = [];
  t.mock.method(f.options.azure, 'token', async (resource: string) => {
    tokens.push(resource);
    if (resource === 'https://storage.azure.com/') return 'synthetic-offline-token';
    assert.equal(resource, 'https://management.azure.com/');
    return 'synthetic-offline-arm-token';
  });
  const metric = (name: string, value: number) => ({ id: `${resourceId}/providers/Microsoft.Insights/metrics/${name}`,
    type: 'Microsoft.Insights/metrics', name: { value: name }, unit: 'Percent',
    timeseries: [{ metadatavalues: [], data: [start, start + 60_000].map(timestamp => ({
      timeStamp: new Date(timestamp).toISOString(), average: value })) }] });
  const body = { interval: 'PT1M', timespan: new Date(start).toISOString() + '/' + new Date(end).toISOString(),
    value: [metric('CPU', 92), metric('IngestionUtilization', 20)] };
  let metricReads = 0, status = 200, afterRead = () => {}, declaredSize: string | undefined;
  const fetcher: typeof fetch = async (input, init) => {
    if (new URL(String(input)).hostname !== 'management.azure.com') return f.options.fetcher(input, init);
    metricReads++;
    const url = new URL(String(input));
    assert.equal(url.pathname, `${resourceId}/providers/Microsoft.Insights/metrics`);
    assert.equal(url.searchParams.get('timespan'), new Date(start).toISOString() + '/' + new Date(end).toISOString());
    assert.equal(url.searchParams.get('metricnames'), 'CPU,IngestionUtilization');
    assert.equal(url.searchParams.get('interval'), 'PT1M');
    assert.equal(url.searchParams.get('aggregation'), 'Average');
    assert.equal(url.searchParams.get('AutoAdjustTimegrain'), 'false');
    assert.equal(url.searchParams.get('api-version'), '2023-10-01');
    assert.equal(new Headers(init?.headers).get('authorization'), 'Bearer synthetic-offline-arm-token');
    assert.equal(init?.redirect, 'error');
    assert.ok(init?.signal);
    afterRead();
    return new Response(JSON.stringify(body), { status, headers: declaredSize ? { 'content-length': declaredSize } : {} });
  };
  return { ...f, body, tokens, options: { ...f.options, fetcher }, reads: () => metricReads,
    setStatus: (value: number) => { status = value; }, afterRead: (fn: () => void) => { afterRead = fn; },
    setDeclaredSize: (value: string) => { declaredSize = value; } };
}

test('Azure Monitor binds native percent metrics to exact selected ARM resource and complete original grains', async t => {
  const f = syntheticMonitorTransport(t), measured = await readOfficialKustoWindow(f.options);
  assert.equal(measured.resourceId, f.options.azure.c.cluster_id);
  assert.equal(measured.cpu.averagePercent, 92);
  assert.equal(measured.ingestionUtilization.averagePercent, 20);
  assert.equal(measured.saturated, true);
  assert.equal(measured.responseSha256, hash(JSON.stringify(f.body)));
  assert.deepEqual(f.tokens, ['https://storage.azure.com/', 'https://management.azure.com/']);
  assert.equal(f.reads(), 1);
});

test('one unknown Kusto metric never becomes false and does not erase independent measured saturation', async t => {
  for (const [ingestion, expected] of [[95, true], [20, null]] as const) {
    const f = syntheticMonitorTransport(t);
    f.body.value[0].id = '/subscriptions/other/providers/Microsoft.Insights/metrics/CPU';
    f.body.value[1].timeseries[0].data.forEach(point => { point.average = ingestion; });
    const result = await readOfficialKustoWindow(f.options);
    assert.equal(result.cpu.averagePercent, null);
    assert.equal(result.saturated, expected);
    assert.match(result.cpu.unavailableReason!, /resource/);
  }
  const f = syntheticMonitorTransport(t);
  f.body.value.forEach(metric => metric.timeseries[0].data.forEach(point => { point.average = 10; }));
  assert.equal((await readOfficialKustoWindow(f.options)).saturated, false);
});

test('partial, duplicated, reordered, wrong-unit and changed-grain metrics stay UNKNOWN', async t => {
  for (const change of ['missing', 'duplicate', 'reorder', 'unit', 'grain', 'window'] as const) {
    const f = syntheticMonitorTransport(t);
    if (change === 'missing') f.body.value[0].timeseries[0].data.pop();
    if (change === 'duplicate') f.body.value.push(structuredClone(f.body.value[0]));
    if (change === 'reorder') f.body.value[0].timeseries[0].data.reverse();
    if (change === 'unit') f.body.value[0].unit = 'Count';
    if (change === 'grain') f.body.interval = 'PT5M';
    if (change === 'window') f.body.timespan = new Date(now - 180_000).toISOString() + '/' + new Date(now).toISOString();
    const result = await readOfficialKustoWindow(f.options);
    assert.equal(result.cpu.averagePercent, null, change);
    assert.equal(result.saturated, null, change);
  }
});

test('misaligned original windows are not expanded or replaced with a later Monitor snapshot', async t => {
  const f = syntheticAuthenticatedTransport(t), result = await readOfficialKustoWindow(f.options);
  assert.equal(result.saturated, null);
  assert.match(result.cpu.unavailableReason!, /original_window/);
  assert.equal(f.counts().tokenReads, 1);
});

test('Monitor permission errors stay UNKNOWN while changed custody, late reads and oversized bodies refuse', async t => {
  const denied = syntheticMonitorTransport(t);
  denied.setStatus(403);
  assert.match((await readOfficialKustoWindow(denied.options)).cpu.unavailableReason!, /HTTP_403/);
  const fenced = syntheticMonitorTransport(t);
  fenced.afterRead(() => { fenced.state.fencingToken++; });
  await assert.rejects(readOfficialKustoWindow(fenced.options), /custody/);
  const late = syntheticMonitorTransport(t);
  let clock = now;
  late.afterRead(() => { clock = late.options.deadlineAtMs; });
  await assert.rejects(readOfficialKustoWindow({ ...late.options, now: () => clock }), /deadline/);
  const oversized = syntheticMonitorTransport(t);
  oversized.setDeclaredSize('1048577');
  await assert.rejects(readOfficialKustoWindow(oversized.options), /byte bound/);
});

test('actual Rust immutable producer bytes remain an original window with unavailable submitted identity', {
  skip: !process.env.OAT_OFFLINE_MEASUREMENT_FIXTURE,
}, async t => {
  const bytes = await readFile(process.env.OAT_OFFLINE_MEASUREMENT_FIXTURE!);
  const f = syntheticAuthenticatedTransport(t, bytes), observed = await f.read();
  assert.equal(observed.startMs, 1000);
  assert.equal(observed.endMs, 31_000);
  assert.equal(observed.committedRowsPerSecond, null);
  assert.ok(observed.unavailableReasons.includes('authoritative_submitted_config_identity_unavailable'));
  assert.ok(observed.unavailableReasons.includes('original_official_cell_did_not_succeed'));
});


test('official reader never reports failed or nonfinal cells as performance', async t => {
  for (const defect of ['failed', 'missing-teardown', 'unsettled', 'ambiguous', 'delta-mismatch', 'admissions-missing', 'admissions-ambiguous'] as const) {
    await t.test(defect, async t => {
      const f = syntheticAuthenticatedTransport(t);
      const result = f.document.official_result;
      if (defect === 'failed') { result.status = 'failed'; Object.assign(result.evidence, { screen_v1: null }); }
      if (defect === 'missing-teardown') result.evidence.teardown_outcome = 'pending';
      if (defect === 'unsettled') result.evidence.reconciliation.settled = false;
      if (defect === 'ambiguous') result.evidence.reconciliation.ambiguous_rows = 1;
      if (defect === 'delta-mismatch') result.evidence.durable_commits.post_teardown_delta_reconciliation.status = 'mismatch';
      if (defect === 'admissions-missing') result.evidence.durable_commits.collector_teardown_admissions.status = 'unavailable';
      if (defect === 'admissions-ambiguous') result.evidence.durable_commits.collector_teardown_admissions.status = 'ambiguous';
      f.publishSyntheticDocument();
      const read = await f.read();
      assert.equal(read.committedRowsPerSecond, null);
      assert.ok(read.unavailableReasons.includes(defect === 'failed' ? 'original_official_cell_did_not_succeed' : 'final_teardown_or_delivery_evidence_unavailable'));
    });
  }
});


test('original cell resolution is provisional when reconciliation or window resolution is absent', async t => {
  for (const defect of ['none', 'missing-screen-resolution', 'mismatched-csv', 'missing-window-resolution', 'different-output', 'failed-screen'] as const) {
    await t.test(defect, async t => {
      const f = syntheticAuthenticatedTransport(t);
      const evidence = f.document.official_result.evidence;
      if (defect === 'missing-screen-resolution') {
        Object.assign(evidence.screen_v1, { rate_resolution_rows_per_second: null });
        Object.assign(evidence.durable_commits.delta_log_reconciliation.csv, { rate_resolution_rows_per_second: null });
      }
      if (defect === 'mismatched-csv') evidence.durable_commits.delta_log_reconciliation.csv.max_commit_rows++;
      if (defect === 'missing-window-resolution') Object.assign(evidence.durable_commits.complete_30_second_commit_windows[0]!, { rate_resolution_rows_per_second: null });
      if (defect === 'different-output') evidence.durable_commits.output_throughput.rows_per_second++;
      if (defect === 'failed-screen') f.document.official_result.status = 'failed';
      f.publishSyntheticDocument();
      const measured = await f.read();
      if (defect === 'none') assert.equal(measured.rateResolutionRowsPerSecond, 1);
      else if (defect === 'different-output') {
        assert.equal(measured.committedRowsPerSecond, null);
        assert.ok(measured.unavailableReasons.includes('published_commit_window_differs_from_durable_output'));
      } else if (defect === 'failed-screen') {
        assert.equal(measured.committedRowsPerSecond, null);
        assert.ok(measured.unavailableReasons.includes('failed_cell_screen_v1_not_null'));
      } else assert.ok(measured.unavailableReasons.includes('provisional_commit_rate_resolution_unavailable'));
    });
  }
});

test('resolution comparison respects baseline and paired thresholds without authorizing the trial', async t => {
  const f = syntheticAuthenticatedTransport(t);
  const windows = f.document.official_result.evidence.durable_commits.complete_30_second_commit_windows;
  windows[0]!.window_commit_count = 99;
  windows.push({ ...windows[0]!, window_commit_count: 100 });
  f.publishSyntheticDocument();
  const original = await f.read();
  assert.deepEqual(original.lowCommitWindows, [{ index: 1, commitCount: 99 }]);
  const cell = (rate: number, resolution: number | null) => ({ ...original, unavailableReasons: [],
    committedRowsPerSecond: rate, rateResolutionRowsPerSecond: resolution });
  const before = cell(1000, 10), after = cell(1000, 10);
  assert.equal(assessCommitRateResolution(cell(1010, 10), before, after, 'AB'), 'resolved');
  assert.equal(assessCommitRateResolution(cell(1009, 10), before, after, 'AB'), 'unresolved');
  assert.equal(assessCommitRateResolution(cell(1010, 10), before, cell(1015, 10), 'BA'), 'unresolved');
  assert.equal(assessCommitRateResolution(cell(1020, 10), before, cell(1000, 10.01), 'AB'), 'invalid_aa');
  assert.equal(assessCommitRateResolution(cell(1020, null), before, after, 'AB'), 'provisional');
  assert.equal(assessCommitRateResolution(cell(Number.NaN, 10), before, after, 'AB'), 'provisional');
  assert.equal(assessCommitRateResolution({ ...cell(1020, 10), unavailableReasons: ['provisional_commit_rate_resolution_unavailable'] }, before, after, 'AB'), 'provisional');
});
