import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import fs from 'node:fs';
import { delimiter, join } from 'node:path';
import test, { type TestContext } from 'node:test';
import { Azure, Config, Failure, HttpFailure, TAGS, canonical_json, runtime, type HttpReceipt } from './bootstrap.js';
import { armDirectBenchmark, type DirectRelease } from './benchmark-direct-arm.js';
import { runDirectBenchmarkCli } from './benchmark-direct-cli.js';
import { directEvidencePrefix } from './benchmark-evidence-custody.js';
import { directArmRecordPath, finishDirectTerminalCustody } from './benchmark-terminal-custody.js';
import { plan_run_fic, prepare_run_fic, read_run_fic_record, run_fic_ledger_path } from './run_fic.js';
import { validateEffects, validateRecoveryTargets, watchSelectedRun } from '../../projects/arrowcol-oat/aks-scoped-watchdog.js';
import { parseEnvelope } from './effect-boundary.js';
import { inspectDirectRecovery } from './benchmark-direct-recovery.js';
import { ENCODING, MAX_RECORDS } from './benchmark-manifest-audit.js';
import { parseEvidence } from './benchmark-evidence-json.js';
import { fakeKubectl } from './benchmark-direct-e2e.fixture.js';
import { CampaignConflictError, campaignMain, newCampaignState, publishRunInventory, validateCampaignState,
  type CampaignStore, type InventoryStep, type RunInventory } from '../../projects/arrowcol-oat/aks-campaign.js';
import { buildAksPoolScalePayload } from '../../projects/arrowcol-oat/aks-arm-pool.js';

const uuid = (n: number) => `00000000-0000-0000-0000-${String(n).padStart(12, '0')}`;
const origin = { runId: uuid(1), owner: uuid(2), namespaceUid: uuid(3), cloudGeneration: uuid(4),
  namespace: 'arrow-collector-' + uuid(1).replaceAll('-', ''), clients: 1, sourceRevision: 'b'.repeat(40), fencingToken: 1 };
const image = `fixture/image@sha256:${'a'.repeat(64)}`;
const release: DirectRelease = { revision: origin.sourceRevision, schemaFingerprint: 'fixture-schema',
  images: { writer: image, maintenance: image, sql: image, client: image, router: image, catalog: image } };
const markers = { 'arrowcol-oat/run-id': origin.runId, 'arrowcol-oat/owner-token': origin.owner };
const table = 'telemetry.sqlserver.sqlserver_availability_replica_states';
const root = `direct-baseline-1/runs/${origin.runId}/dataflow/${table.replaceAll('.', '/')}`;
const sha = (bytes: string | Buffer) => createHash('sha256').update(bytes).digest('hex');
const lines = (...actions: unknown[]) => Buffer.from(actions.map(a => JSON.stringify(a)).join('\n') + '\n');
type Event = { event: string; [key: string]: any };
type ObjectValue = Record<string, any>;
type Fault = 'none' | 'arm-write-unknown' | 'arm-write-not-observed' | 'arm-readback' | 'held-schedule-drift' | 'release-unknown' | 'release-replaced' |
  'arm-replaced' | 'client-replaced' | 'stop-unknown' | 'exit-unknown' | 'audit-invalid' | 'seal-unknown' |
  'seal-readback' | 'seal-replaced' | 'namespace-unknown' | 'namespace-replaced' | 'namespace-present' |
  'fic-replaced' | 'fic-unknown' | 'clock-skew' | 'clock-missing' | 'clock-boundary' | 'clock-namespace' |
  'client-transport' | 'router-transport' | 'receiver-transport' | 'parquet-config';

function kubeObjects(now: number) {
  const objects: Record<string, ObjectValue> = {};
  let nextUid = 10;
  const meta = (name: string) => ({ name, namespace: origin.namespace, uid: uuid(nextUid++), resourceVersion: '17',
    generation: 1, labels: { ...markers }, annotations: { ...markers } });
  const put = (key: string, value: ObjectValue) => objects[key] = value;
  const ns = put('namespace/' + origin.namespace, { apiVersion: 'v1', kind: 'Namespace', metadata: meta(origin.namespace) });
  ns.metadata.uid = origin.namespaceUid;
  const roles: Record<string, string> = {};
  const startedAt = new Date(now - 60_000).toISOString();
  const pod = (name: string, container: string, ownerKind: string, ownerUid: string) => ({ apiVersion: 'v1', kind: 'Pod',
    metadata: { ...meta(name), ownerReferences: [{ kind: ownerKind, uid: ownerUid, controller: true }] },
    spec: { nodeName: 'node-1', terminationGracePeriodSeconds: 30, containers: [{ name: container, image }] },
    status: { phase: 'Running', podIP: '10.0.0.47', conditions: [{ type: 'Ready', status: 'True' }],
      containerStatuses: [{ name: container, ready: true, restartCount: 0, imageID: image, containerID: 'containerd://' + name,
        state: { running: { startedAt } } }] } });
  for (const [role, container] of [['arrowcol', 'dataflow'], ['delta-maintenance', 'maintenance'],
    ['arrowcol-sql', 'sqlserver'], ['arrowcol-catalog', 'catalog']]) {
    const deployment = put('deployment/' + role, { apiVersion: 'apps/v1', kind: 'Deployment',
      metadata: meta(role), spec: { replicas: 1 }, status: { observedGeneration: 1 } });
    const rs = put('replicaset/' + role, { kind: 'ReplicaSet', metadata: { ...meta(role + '-rs'),
      ownerReferences: [{ kind: 'Deployment', uid: deployment.metadata.uid, controller: true }] } });
    rs.metadata.labels.app = role;
    const p = put('pod/' + role + '-original', pod(role + '-original', container, 'ReplicaSet', rs.metadata.uid));
    p.metadata.ownerReferences[0].name = rs.metadata.name;
    p.metadata.labels.app = role; roles[role] = p.metadata.name;
  }
  const writer = objects['pod/' + roles.arrowcol];
  writer.spec.nodeName = 'writer-node';
  Object.assign(writer.spec, { volumes: [{ name: 'staging', persistentVolumeClaim: { claimName: 'arrowcol-writer-staging' } }] });
  writer.spec.containers[0].volumeMounts = [{ name: 'staging', mountPath: '/var/lib/arrowcol/staging' }];
  const pvc = (name: string, storageClassName: string) => put('pvc/' + name, { apiVersion: 'v1', kind: 'PersistentVolumeClaim',
    metadata: meta(name), spec: { volumeName: 'pv-' + name, storageClassName }, status: { phase: 'Bound' } });
  pvc('arrowcol-writer-staging', `arrowcol-${origin.runId.replaceAll('-', '')}-local-csi`);
  const job = put('job/direct-load-confirmwide', { apiVersion: 'batch/v1', kind: 'Job', metadata: meta('direct-load-confirmwide'),
    spec: { suspend: false, completionMode: 'Indexed', completions: 1, parallelism: 1 }, status: {} });
  const client = put('pod/client-0', pod('client-0', 'generator', 'Job', job.metadata.uid));
  Object.assign(client.metadata.labels, { 'batch.kubernetes.io/job-name': job.metadata.name });
  client.metadata.annotations['batch.kubernetes.io/job-completion-index'] = '0';
  Object.assign(client.spec, { volumes: [{ name: 'evidence', ephemeral: {} }] });
  client.spec.containers[0].volumeMounts = [{ name: 'evidence', mountPath: '/evidence' }];
  pvc('client-0-evidence', 'managed-csi').metadata.ownerReferences = [{ kind: 'Pod', uid: client.metadata.uid, controller: true }];
  put('configmap/direct-schedule-confirmwide', { apiVersion: 'v1', kind: 'ConfigMap', metadata: meta('direct-schedule-confirmwide'),
    data: { start: 'pending', writerLifetime: 'pending', clientLifetimes: 'pending' } });
  const router = put('daemonset/arrowcol-router', { apiVersion: 'apps/v1', kind: 'DaemonSet', metadata: meta('arrowcol-router'),
    spec: { template: { spec: { containers: [{ name: 'router', image }] } } },
    status: { observedGeneration: 1, desiredNumberScheduled: 2, currentNumberScheduled: 2, numberReady: 2 } });
  const rp = put('pod/router-original', pod('router-original', 'router', 'DaemonSet', router.metadata.uid));
  rp.metadata.labels.app = 'arrowcol-router';
  const wrp = put('pod/writer-router-original', pod('writer-router-original', 'router', 'DaemonSet', router.metadata.uid));
  wrp.metadata.labels.app = 'arrowcol-router'; wrp.spec.nodeName = 'writer-node';
  const registration = put('job/direct-catalog-verified', { apiVersion: 'batch/v1', kind: 'Job', metadata: meta('direct-catalog-verified'),
    status: { succeeded: 1, conditions: [{ type: 'Complete', status: 'True' }] } });
  const cp = put('pod/catalog-register', pod('catalog-register', 'register', 'Job', registration.metadata.uid));
  cp.metadata.labels['batch.kubernetes.io/job-name'] = registration.metadata.name;
  cp.status.phase = 'Succeeded';
  cp.status.containerStatuses[0].state = { terminated: { exitCode: 0 } } as any;
  return { objects, roles };
}

function loadEvidence(start: number, end: number, receiptAt = start + 2000) {
  const admitted = start + 1000, physical = '3'.repeat(32), app = 'arrowcol.publication.' + physical;
  const fact = { decoded_physical_batch_count: 1, canonical_logical_bytes: 20, canonical_logical_bytes_unavailable_reason: null };
  const manifest = { schema: 'arrowcol.logical_batch_manifest.v1', version: 1, identity_status: 'identity_bearing',
    eligible_for_identity_reconciliation: true, complete: true, truncated: false, record_capacity_reached: false,
    batch_facts_encoding: ENCODING, run_uuid: '1'.repeat(32), incarnation_uuid: '2'.repeat(32),
    measurement_bounds: { start_unix_ms_inclusive: start, end_unix_ms_exclusive: end }, record_count: 1,
    record_cap: Number(MAX_RECORDS), prepared_logical_batches: 1, attempted_logical_batches: 1, acknowledged_logical_batches: 1,
    not_admitted_logical_batches: 0, ambiguous_logical_batches: 0, failed_logical_batches: 0, pending_logical_batches: 0, retry_attempts: 0,
    ranges: [{ table, worker: 0, first_sequence: 0, last_sequence: 0, batch_count: 1, rows_per_batch: 2,
      cohort: 'measurement', attempts: 1, outcome: 'acknowledged', ...fact,
      // Preserve exact integer ns in the emitted JSON, rather than rounding a JS number.
      first_send_unix_ns: 'SEND_NS', last_send_unix_ns: 'SEND_NS', first_admitted_unix_ms: admitted,
      last_admitted_unix_ms: admitted, first_admitted_cohort: 'measurement', first_outcome_unix_ms: admitted,
      last_outcome_unix_ms: admitted, batch_facts: { encoding: ENCODING, bit_order: 'least_significant_index_bits_first',
        index_count: 1, index_bits: 0, dictionary: [fact], indices_base64url: '' } }] };
  const receipt = { timestamp: new Date(receiptAt).toISOString(), fields: { message: 'Delta logical publication audit receipt',
    logical_publication_id: `${'1'.repeat(32)}/${'2'.repeat(32)}/0/0`, physical_publication_id: physical, fragment_ordinal: 0,
    input_rows: 2, canonical_logical_bytes: 'Some(20)', admitted_at_unix_ms: admitted, qualification_state: 'committed',
    delta_outcome: 'Committed', authoritative_delta_version: 'Some(1)', committed_files: '["0:a.parquet"]',
    transaction_files: `[("${app}", 1, ${admitted}, [0])]` } };
  return { logs: { 'writer-stdout': '', 'writer-stderr': 'arrowcol_effective_config {"parquet_writer":{"defaults":{"compression":"zstd","compression_level":2}}}\narrowcol_effective_config {"component":"receiver","config":{"transport":{"initial_stream_window_size":1048576,"initial_connection_window_size":16777216,"adaptive_window":false,"tcp_nodelay":true}}}\n' + JSON.stringify(receipt) + '\n', 'client-stderr': '',
    'client-stdout': 'OAT_CLIENT_EFFECTIVE_CONFIG {"stream_window_bytes":1048576,"connection_window_bytes":16777216,"adaptive_window":false,"tcp_nodelay":true}\n' +
      'OAT_CLIENT_METRICS ' + JSON.stringify({ phase: 'completed', logical_batch_manifest: manifest })
      .replaceAll('"SEND_NS"', String(BigInt(admitted) * 1_000_000n)) + '\n' },
    commit: lines({ txn: { appId: app, version: 1, lastUpdated: admitted } },
      { add: { path: 'hour=0/a.parquet', size: 40, dataChange: true, stats: '{"numRecords":2}' } }) };
}

async function fixture(t: TestContext, fault: Fault, held = false) {
  const directory = fs.mkdtempSync(join(process.cwd(), 'infra/azure/.direct-e2e-'));
  fs.chmodSync(directory, 0o700);
  t.after(() => fs.rmSync(directory, { recursive: true, force: true }));
  const trace = join(directory, 'trace.jsonl'), statePath = join(directory, 'state.json');
  fs.writeFileSync(trace, '');
  fs.writeFileSync(join(directory, 'package.json'), '{"type":"commonjs"}');
  const events = (): Event[] => fs.readFileSync(trace, 'utf8').trim().split('\n').filter(Boolean).map(line => JSON.parse(line));
  const record = (event: string, extra = {}) => fs.appendFileSync(trace, JSON.stringify({ event, ...extra }) + '\n');
  const initialNow = 1_795_104_000_000;
  let now = initialNow;
  t.mock.method(Date, 'now', () => now);
  const state = { ...kubeObjects(now), trace, now, runId: origin.runId, image, helperUid: uuid(99), fault, loaded: false,
    logs: {} as Record<string, string>, catalogLog: '', registration: { instance_id: uuid(90),
      instance_key: `direct-single-writer-${origin.runId}|cloud=${origin.cloudGeneration}`, implementation_pool: 'dataflow',
      flight_endpoint: '10.0.0.47:32010', started_at_utc: new Date(now - 60_000).toISOString().replace('.000Z', '.0000000Z') } };
  const c = new Config({ AZURE_SUBSCRIPTION_ID: uuid(80), AZURE_STAMP: '68590' }), azure = new Azure(c);
  state.catalogLog = JSON.stringify({ kind: 'oat_run_table_registered', run_id: origin.runId, table, provider: 'delta',
    location: `abfss://${c.data}@${c.account}.dfs.core.windows.net/${root}`, schema_version: 1,
    catalog_table_count: 1, schema_fingerprint: release.schemaFingerprint });
  const save = () => fs.writeFileSync(statePath, JSON.stringify(state));
  save();
  const kubectl = join(directory, 'kubectl');
  fs.writeFileSync(kubectl, `#!${process.execPath}\nconst __name=(f)=>f;\n(${fakeKubectl.toString()})();\n`, { mode: 0o700 });
  const oldPath = process.env.PATH, oldState = process.env.DIRECT_E2E_STATE;
  process.env.PATH = directory + delimiter + oldPath; process.env.DIRECT_E2E_STATE = statePath;
  t.after(() => { process.env.PATH = oldPath; if (oldState === undefined) delete process.env.DIRECT_E2E_STATE;
    else process.env.DIRECT_E2E_STATE = oldState; });
  const stored = new Map<string, { bytes: Buffer; etag: string }>();
  const commits = [lines({ protocol: { minReaderVersion: 1, minWriterVersion: 2 } }, { metaData: { id: uuid(70) } })];
  let revision = 0, lease = '', credential: ObjectValue | null = null, namespaceAbsent = false, controlVersion = '"control-0"';
  let control: Record<string, string> = { sandboxid: c.sandbox, publisherregistryversion: '1', resetstate: 'ready-v1',
    resetgeneration: origin.cloudGeneration, occupancy: 'released', publishers: canonical_json({ [uuid(91)]: { generation: origin.cloudGeneration, state: 'active' } }) };
  const ledgerPath = run_fic_ledger_path(c);
  const privateDir = (name: string) => { const path = join(directory, name); fs.mkdirSync(path, { mode: 0o700 }); return path; };
  const baseline = privateDir('baseline'), terminal = privateDir('terminal'), delta = privateDir('delta');
  const ficPath = join(directory, 'fic.json');
  const tags = { ...TAGS, sandboxId: c.sandbox }, issuer = 'https://fixture.oic.prod-aks.azure.com/tenant/cluster/';
  const native = { id: c.identity_id + '/federatedIdentityCredentials/azure-qap-' + c.cluster, name: 'azure-qap-' + c.cluster,
    properties: { issuer, audiences: ['api://AzureADTokenExchange'], subject: `system:serviceaccount:${c.kubernetesNamespace}:${c.kubernetesServiceAccount}` } };
  t.mock.method(azure, 'arm', async (method: string, resource: string) => {
    assert.equal(method, 'GET');
    if (resource === c.identity_id) return { tags, properties: { clientId: uuid(80), tenantId: uuid(80) } };
    if (resource.includes('/managedClusters/')) return { tags, properties: { fqdn: 'fixture.azmk8s.io',
      oidcIssuerProfile: { issuerURL: issuer, enabled: true }, securityProfile: { workloadIdentity: { enabled: true } } } };
    if (resource.includes('/federatedIdentityCredentials/arrowcol-run-')) return structuredClone(credential);
    if (resource === c.storage_id) return { tags };
    if (resource.endsWith('/containers/' + c.checkpoints)) return { properties: { metadata: { sandboxid: c.sandbox } } };
    throw new Error('Unhandled fake ARM read ' + resource);
  });
  t.mock.method(azure, 'collection', async () => [native, ...(credential ? [structuredClone(credential)] : [])]);
  let loseNamespaceReceipt = false;
  let physicalEtag = '"physical"';
  const blob: Azure['blob'] = async (method, container, path = '', query, body, headers = {}, _missing, _idempotent, capture): ReturnType<Azure['blob']> => {
    record('blob', { method, container, path });
    if (container === c.data) {
      if (!path) {
        assert.equal(method, 'GET'); assert.equal(query?.prefix, root + '/_delta_log/');
        const xml = commits.map((_, v) => `<Blob><Name>${root}/_delta_log/${String(v).padStart(20, '0')}.json</Name><Properties><Etag>"v${v}"</Etag></Properties></Blob>`).join('');
        return [Buffer.from(`<EnumerationResults><Prefix>${root}/_delta_log/</Prefix><Blobs>${xml}</Blobs><NextMarker /></EnumerationResults>`),
          { ':status': '200', 'x-ms-request-id': 'fixture-list' }];
      }
      if (method === 'HEAD') { assert.equal(path, root + '/hour=0/a.parquet');
        return [null, { ':status': '200', 'x-ms-request-id': 'fixture-head', etag: physicalEtag, 'content-length': '40' }]; }
      assert.equal(method, 'GET');
      const v = Number(path.split('/').at(-1)!.slice(0, -5));
      assert.equal(headers['If-Match'], `"v${v}"`); assert.ok(commits[v]);
      return [commits[v], { ':status': '200', etag: `"v${v}"` }];
    }
    assert.equal(container, c.checkpoints);
    const current = stored.get(path);
    if (query?.comp === 'lease') {
      assert.equal(path, ledgerPath); assert.equal(method, 'PUT');
      let receipt: HttpReceipt;
      if (headers['x-ms-lease-action'] === 'acquire') {
        assert.equal(lease, ''); assert.equal(headers['If-Match'], current?.etag);
        assert.equal(headers['x-ms-lease-duration'], '-1'); lease = headers['x-ms-proposed-lease-id'];
        receipt = { status: 201, bodyBase64: '', headers: { 'x-ms-lease-id': lease,
          'x-ms-request-id': 'fixture-acquire', 'x-ms-client-request-id': headers['x-ms-client-request-id'] } };
      } else {
        assert.equal(headers['x-ms-lease-action'], 'release'); assert.equal(headers['x-ms-lease-id'], lease); lease = '';
        receipt = { status: 200, bodyBase64: '', headers: { 'x-ms-request-id': 'fixture-release',
          'x-ms-client-request-id': headers['x-ms-client-request-id'] } };
      }
      capture?.(receipt); return [Buffer.alloc(0), { ':status': String(receipt.status), ...receipt.headers }];
    }
    if (method === 'GET') {
      if (headers['If-Match'] && current?.etag !== headers['If-Match']) throw new HttpFailure(412, 'fixture stale read');
      if (path === directArmRecordPath(origin) && current) {
        if (fault === 'arm-readback') return [Buffer.from('changed'), { ':status': '200', etag: current.etag }];
        if (fault === 'held-schedule-drift') {
          const live = JSON.parse(fs.readFileSync(statePath, 'utf8'));
          live.objects['configmap/direct-schedule-confirmwide'].metadata.resourceVersion = '18';
          fs.writeFileSync(statePath, JSON.stringify(live));
        }
        record('arm-readback');
      }
      if (path.endsWith('/seal.json') && current) {
        if (fault === 'seal-readback') return [Buffer.from('changed'), { ':status': '200', etag: current.etag }];
        record('seal-readback');
        if (fault === 'seal-replaced') {
          const original = Buffer.from(current.bytes), originalEtag = current.etag;
          current.bytes = Buffer.from('replaced after readback'); current.etag = `"replacement-${++revision}"`;
          return [original, { ':status': '200', etag: originalEtag }];
        }
      }
      return [current ? Buffer.from(current.bytes) : null, current ? { ':status': '200', etag: current.etag } : {}];
    }
    assert.equal(method, 'PUT');
    if (current) assert.equal(headers['If-Match'], current.etag, 'every overwrite must use CAS');
    else assert.equal(headers['If-None-Match'], '*', 'every creation must be conditional');
    if (path === ledgerPath && lease) assert.equal(headers['x-ms-lease-id'], lease);
    const bytes = Buffer.isBuffer(body) ? Buffer.from(body) : Buffer.from(String(body));
    const etag = `"object-${++revision}"`;
    if (path === directArmRecordPath(origin)) {
      record('arm-write');
      if (fault === 'arm-write-not-observed') throw new Failure('Injected arm write response lost without storage effect');
    }
    stored.set(path, { bytes, etag });
    if (path === directArmRecordPath(origin) && fault === 'arm-write-unknown') throw new Failure('Injected arm write response lost');
    if (path.endsWith('/seal.json')) { record('seal-write'); if (fault === 'seal-unknown') throw new Failure('Injected seal response lost'); }
    if (loseNamespaceReceipt && path.startsWith('_arrowcol-custody/namespaces/') &&
        JSON.parse(bytes.toString()).phase === 'complete') {
      loseNamespaceReceipt = false;
      throw new Failure('Injected completed namespace journal acknowledgement lost');
    }
    const receipt = { status: 201, bodyBase64: '', headers: { etag, 'x-ms-request-id': 'fixture-put' } };
    capture?.(receipt); return [null, { ':status': '201', ...receipt.headers }];
  };
  t.mock.method(azure, 'blob', blob);
  t.mock.method(azure, 'request', async (method: string, url: string, resource: string, body?: string | Buffer | null,
    headers: Record<string, string> = {}, _missing?: boolean, idempotent?: boolean, capture?: (r: HttpReceipt) => void) => {
    assert.equal(resource, 'https://management.azure.com/'); assert.ok(lease); assert.equal(idempotent, false);
    const id = new URL(url).pathname;
    assert.equal(id, c.identity_id + '/federatedIdentityCredentials/arrowcol-run-' + origin.runId);
    let receipt: HttpReceipt;
    if (method === 'GET') receipt = { status: credential ? 200 : 404,
      bodyBase64: Buffer.from(JSON.stringify(credential ?? { error: { code: 'ResourceNotFound' } })).toString('base64'),
      headers: { 'x-ms-request-id': 'fixture-postcondition' } };
    else {
      assert.ok(headers['x-ms-client-request-id']);
      if (method === 'PUT') credential = { id, name: id.split('/').at(-1), ...JSON.parse(String(body)),
        systemData: { createdAt: new Date(initialNow).toISOString() } };
      else {
        assert.equal(method, 'DELETE'); record('fic-delete');
        assert.ok(namespaceAbsent);
        const journal = [...stored].find(([key]) => key.startsWith('_arrowcol-custody/namespaces/'));
        assert.equal(JSON.parse(journal![1].bytes.toString()).phase, 'complete');
        assert.ok(events().some(e => e.event === 'namespace-positive'));
        assert.ok(events().some(e => e.event === 'namespace-absence'));
        credential = null;
        if (fault === 'fic-unknown') throw new Failure('Injected FIC DELETE response lost after effect');
      }
      receipt = { status: method === 'PUT' ? 201 : 204,
        bodyBase64: method === 'PUT' ? Buffer.from(JSON.stringify(credential)).toString('base64') : '',
        headers: { 'x-ms-request-id': 'fixture-arm-mutation' } };
    }
    capture?.(receipt); return [Buffer.from(receipt.bodyBase64, 'base64'), { ':status': String(receipt.status) }];
  });
  t.mock.method(azure, 'head_control', async () => {
    const current = JSON.parse(fs.readFileSync(statePath, 'utf8'));
    if (current.objects['pod/' + current.roles['delta-maintenance']].status.containerStatuses[0].state.terminated)
      control.publishers = '{}'; // Models both cooperative publishers releasing after their positive clean exits.
    return [structuredClone(control), controlVersion];
  });
  t.mock.method(azure, 'set_control', async (metadata: Record<string, string>, etag: string) => {
    assert.equal(etag, controlVersion); control = structuredClone(metadata); controlVersion = '"control-1"'; record('publisher-fence');
  });
  const originalProcess = runtime.run_tree;
  t.mock.method(runtime, 'run_tree', async (args: string[]) => {
    if (args.includes('--context')) return originalProcess(args, 20);
    if (events().some(e => e.event === 'recovery-namespace-delete') && !namespaceAbsent) {
      namespaceAbsent = true;
      record('namespace-positive');
    }
    assert.equal(args[0], 'kubectl'); assert.ok(args.includes('--insecure-skip-tls-verify=false'));
    if (args.includes('delete')) {
      record('namespace-delete');
      const options = JSON.parse(fs.readFileSync(args[args.indexOf('-f') + 1], 'utf8'));
      assert.deepEqual(options.preconditions, { uid: origin.namespaceUid });
      assert.ok(events().some(e => e.event === 'seal-readback'));
      const journal = [...stored].find(([key]) => key.startsWith('_arrowcol-custody/namespaces/'));
      assert.equal(JSON.parse(journal![1].bytes.toString()).phase, 'intent');
      namespaceAbsent = fault !== 'namespace-present';
      if (fault === 'namespace-unknown') return { returncode: 1, stdout: '', stderr: 'Injected namespace DELETE response lost' };
      const result = { kind: 'Namespace', metadata: { name: origin.namespace,
        uid: fault === 'namespace-replaced' ? uuid(98) : origin.namespaceUid, deletionTimestamp: new Date(now).toISOString() } };
      if (fault !== 'namespace-replaced') record('namespace-positive');
      return { returncode: 0, stdout: JSON.stringify(result), stderr: '' };
    }
    if (args.includes('namespace')) {
      if (args.includes('--ignore-not-found')) {
        record(namespaceAbsent ? 'namespace-absence' : 'namespace-present');
        // Bound the production polling path without sleeping for its five-minute deadline.
        if (fault === 'namespace-present') now += 301_000;
        return { returncode: 0, stdout: namespaceAbsent ? '' : 'namespace/' + origin.namespace, stderr: '' };
      }
      return { returncode: 0, stdout: JSON.stringify(state.objects['namespace/' + origin.namespace]), stderr: '' };
    }
    assert.ok(args.includes('serviceaccount'));
    return { returncode: 0, stdout: JSON.stringify({ metadata: { name: 'arrowcol-oat', namespace: origin.namespace, uid: uuid(81),
      annotations: { 'azure.workload.identity/client-id': uuid(80) } } }), stderr: '' };
  });
  await prepare_run_fic(azure, origin.runId, origin.namespace, 'arrowcol-oat', ficPath);
  let campaignState = newCampaignState(uuid(5), origin.owner, now - 1000), campaignVersion = 0;
  campaignState.mode = 'armed';
  let loseBookkeeping = false, fenced = false;
  const campaignEtag = () => `"campaign-${campaignVersion}"`;
  const store: CampaignStore = { read: async () => ({ state: structuredClone(campaignState), etag: campaignEtag() }),
    write: async (next, expected) => {
      if (expected !== campaignEtag()) throw new CampaignConflictError();
      campaignState = structuredClone(validateCampaignState(next));
      const etag = `"campaign-${++campaignVersion}"`;
      if (loseBookkeeping && next.runInventory?.fic?.state === 'removed') {
        loseBookkeeping = false;
        throw new Error('Injected campaign bookkeeping response loss after FIC removal');
      }
      return etag;
    } };
  const untouched = (): InventoryStep => ({ state: 'not_started', deadline_unix_ms: null, reason_code: null });
  const recordFic = read_run_fic_record(ficPath, c);
  const resources: RunInventory['resources'] = [
    { api_version: 'apps/v1', kind: 'Deployment', name: 'arrowcol', role: 'collector' },
    { api_version: 'apps/v1', kind: 'Deployment', name: 'delta-maintenance', role: 'maintenance' },
    { api_version: 'batch/v1', kind: 'Job', name: 'direct-load-confirmwide', role: 'client' },
  ].map(resource => ({ ...resource, role: resource.role as RunInventory['resources'][number]['role'],
    namespace: origin.namespace, uid: null, absence: null, state: 'planned' }));
  const initial: RunInventory = { schema_version: 1, campaign_id: uuid(5), owner_id: origin.owner, orchestration_run_id: uuid(6),
    cell_run_id: origin.runId, fencing_token: 1, namespace: { name: origin.namespace, uid: null, owner_token: origin.owner },
    load_admission: 'not_authorized', resources, fic: { state: 'planned', resource_id: null, name: null, absence: null }, qap: null,
    cleanup: { stop_admission: untouched(), bounded_drain: untouched(), evidence_capture: untouched(),
      stop_maintenance: untouched(), terminal_capture: untouched(), handoff: untouched() },
    planned_cell_duration_ms: 600_000, effective_stop_budgets: { maintenance_grace_seconds: 90, maintenance_stop_timeout_ms: 390_000 } };
  await publishRunInventory(store, initial);
  const ficPlan = await plan_run_fic(azure, origin.runId, origin.namespace, 'arrowcol-oat');
  const observed: RunInventory = { ...initial, namespace: { ...initial.namespace, uid: origin.namespaceUid },
    resources: resources.map(resource => ({ ...resource, state: 'observed', uid: state.objects[`${resource.kind.toLowerCase()}/${resource.name}`].metadata.uid })),
    evidence_destination: { account: c.account, container: c.checkpoints, blob_prefix: directEvidencePrefix(origin) },
    fic: { state: 'observed', resource_id: `${recordFic.identityId}/federatedIdentityCredentials/${recordFic.credentialName}`,
      name: recordFic.credentialName, absence: null, plan: ficPlan }, load_admission: held ? 'not_authorized' : 'authorization_pending' };
  await publishRunInventory(store, observed);
  if (!held) await publishRunInventory(store, { ...observed, load_admission: 'authorized' });
  const campaign = { store, campaignId: uuid(5), ownerId: origin.owner };
  const cliEnvironment = { ...process.env, CAMPAIGN_ID: uuid(5), CAMPAIGN_OWNER_ID: origin.owner, CAMPAIGN_FENCING_TOKEN: '1' };
  const envFile = join(directory, 'bootstrap.env'), stampFile = join(directory, 'stamp.yaml');
  const releaseFile = join(directory, 'release.json'), kubeconfig = join(directory, 'kubeconfig');
  fs.writeFileSync(envFile, '', { mode: 0o600 }); fs.writeFileSync(stampFile, 'fixture: true\n', { mode: 0o600 });
  fs.writeFileSync(releaseFile, JSON.stringify(release), { mode: 0o600 }); fs.writeFileSync(kubeconfig, 'fixture', { mode: 0o600 });
  const loadConfig = () => ({ config: c, env: cliEnvironment, positionals: [], once: false });
  const cliArgs = (action: 'arm' | 'prepare' | 'finish' = 'arm') => [action, '--env-file', envFile, '--stamp-config', stampFile,
    '--fic-record', ficPath, '--release-file', releaseFile, '--kubeconfig', kubeconfig, '--context', 'fixture',
    ...(action !== 'finish' ? ['--clients', '1', '--baseline-directory', baseline] :
      ['--run-id', origin.runId, '--terminal-directory', terminal, '--delta-directory', delta])];
  const runCli = (action: 'arm' | 'prepare' | 'finish' = 'arm', selectedStore: CampaignStore = store, extraArgs: string[] = []) => {
    const args = cliArgs(action);
    for (let index = 0; index < extraArgs.length; index += 2) {
      const existing = args.indexOf(extraArgs[index]!);
      if (existing >= 0) args.splice(existing, 2);
    }
    return runDirectBenchmarkCli([...args, ...extraArgs], { azure, store: selectedStore, loadConfig,
      readBaseline: async (_azure, state) => ({ status: 'captured', campaign_id: state.campaignId,
        owner_id: state.ownerId, fencing_token: state.fencingToken, blob: 'fixture-baseline', etag: '"fixture-baseline"',
        captured_at_unix_ms: state.startedAtMs }), log: () => {} });
  };
  const arm = () => armDirectBenchmark(azure, campaign, origin, release, 'fixture', ficPath, baseline);
  const load = (armed: Awaited<ReturnType<typeof arm>>) => {
    const current = JSON.parse(fs.readFileSync(statePath, 'utf8'));
    assert.equal(current.objects['configmap/direct-schedule-confirmwide'].data.start, String(armed.startMs - 60_000));
    Object.assign(state, current);
    now = armed.endMs + 1000; state.now = now; state.loaded = true;
    campaignState.heartbeatAtMs = now - 1;
    const generated = loadEvidence(armed.startMs, armed.endMs, fault === 'clock-boundary' ? armed.endMs - 50 : undefined); state.logs = generated.logs; commits.push(generated.commit);
    const p = state.objects['pod/client-0'], cs = p.status.containerStatuses[0];
    cs.state = { terminated: { startedAt: cs.state.running.startedAt, finishedAt: new Date(now).toISOString(),
      containerID: cs.containerID, exitCode: 0, reason: 'Completed' } };
    p.status.phase = 'Succeeded'; p.metadata.resourceVersion = '27';
    state.objects['job/direct-load-confirmwide'].status = { succeeded: 1, conditions: [{ type: 'Complete', status: 'True' }] };
    if (fault === 'client-replaced') p.metadata.uid = uuid(98);
    if (fault === 'arm-replaced') stored.set(directArmRecordPath(origin), { bytes: Buffer.from('replacement'), etag: '"replacement"' });
    if (fault === 'fic-replaced') credential!.systemData.createdAt = new Date(initialNow + 1000).toISOString();
    save(); record('load');
  };
  return { arm, runCli, cliArgs, releaseFile, load, store, loseBookkeeping: () => { loseBookkeeping = true; },
    scheduleData: () => JSON.parse(fs.readFileSync(statePath, 'utf8')).objects['configmap/direct-schedule-confirmwide'].data,
    driftScheduleVersion: () => {
      const live = JSON.parse(fs.readFileSync(statePath, 'utf8'));
      live.objects['configmap/direct-schedule-confirmwide'].metadata.resourceVersion = '34';
      fs.writeFileSync(statePath, JSON.stringify(live));
    },
    c, azure, directory, advanceClock: (ms: number) => {
      now += ms;
      state.now = now;
      const live = JSON.parse(fs.readFileSync(statePath, 'utf8'));
      live.now = now;
      fs.writeFileSync(statePath, JSON.stringify(live));
    }, addInterruptedCommit: (start: number, end: number) => { commits.push(loadEvidence(start, end).commit); }, loseNamespaceReceipt: () => { loseNamespaceReceipt = true; },
    replacePhysical: () => { physicalEtag = '"replacement-physical"'; },
    staleAuthority: (expired: boolean) => { if (!fenced && expired) campaignState.heartbeatAtMs = now - 180_000;
      else if (fenced && !expired) campaignState.heartbeatAtMs = now - 1; fenced = expired; },
    finish: (armed: Awaited<ReturnType<typeof arm>>) =>
    finishDirectTerminalCustody(azure, campaign, armed, 'fixture', terminal, delta, ficPath), events, stored, terminal, ficPath,
    namespaceAbsent: () => namespaceAbsent, credential: () => credential, control: () => control };
}

test('selected watchdog waits for original FIC, fences authorized null-QAP producer loss, and does not replay a lost response', async t => {
  const f = await fixture(t, "none");
  const armed = await f.arm();
  f.load(armed);
  const planPath = join(f.directory, 'watch-envelope.json'), stateDir = join(f.directory, 'watch-state');
  const ficPath = join(f.directory, 'watch-fic.json');
  fs.mkdirSync(stateDir, { mode: 0o700 });
  const { subscription, group, account, checkpoints } = f.c;
  const target = `https://${account}.blob.core.windows.net/${checkpoints}`;
  const blobPath = `/${checkpoints}/active.json`;
  fs.writeFileSync(planPath, JSON.stringify({ schema_version: 1, campaign_id: uuid(5), run_id: origin.runId,
    subscription_id: subscription, resource_group: group, storage_account: account, state_container: checkpoints, effects: [
      { id: 'get', transport: 'campaign-blob', method: 'GET', resource: target, path: blobPath, max_calls: 25 },
      { id: 'put', transport: 'campaign-blob', method: 'PUT', resource: target, path: blobPath, max_calls: 1 },
      { id: 'token', transport: 'process', method: 'AZURE_ACCOUNT_GET-ACCESS-TOKEN', resource: 'az', max_calls: 25,
        path: JSON.stringify(['account', 'get-access-token', '--subscription', subscription, '--resource',
          'https://storage.azure.com/', '--query', 'accessToken', '--output', 'tsv', '--only-show-errors']) },
    ] }), { mode: 0o600 });
  const args = ['--env-file', 'fixture.env', '--stamp-config', 'fixture.yaml', '--effect-envelope', planPath,
    '--state-dir', stateDir, '--fic-record', ficPath, '--owner-id', origin.owner, '--namespace', origin.namespace,
    '--namespace-owner', origin.owner, '--expires-at', new Date(Date.now() + 300_000).toISOString()];
  let exists = false, writes = 0, replaceBetweenCheckAndPut = false, dropResponse = false;
  const fetcher: typeof fetch = async (input, init) => {
    assert.equal(String(input), `${target}/active.json`);
    const current = (await f.store.read())!;
    if (init?.method !== 'PUT') return exists ?
      new Response(JSON.stringify(current.state), { headers: { etag: current.etag } }) : new Response(null, { status: 404 });
    writes++;
    const etag = new Headers(init.headers).get('if-match');
    assert.equal(etag, current.etag);
    if (replaceBetweenCheckAndPut) {
      const replacement = newCampaignState(uuid(90), origin.owner, Date.now());
      await f.store.write(replacement, current.etag);
      return new Response(null, { status: 412 });
    }
    const next = JSON.parse(String(init.body));
    const nextEtag = await f.store.write(next, current.etag);
    if (dropResponse) throw new Error('injected conditional PUT response loss');
    return new Response(null, { status: 201, headers: { etag: nextEtag } });
  };
  const watch = (verify: (tick: () => Promise<Record<string, unknown>>) => Promise<void>, selected = stateDir,
    extra: string[] = [], signal = new AbortController().signal) =>
    watchSelectedRun([...args.map(arg => arg === stateDir ? selected : arg), ...extra], signal,
      async (_signal, tick) => { await verify(tick); return 0; },
      { config: f.c, fetcher, token: async () => 'fixture-token', azure: f.azure });
  await watch(async tick => {
    assert.equal((await tick()).scopeStatus, 'waiting_campaign');
    exists = true;
    assert.equal((await tick()).scopeStatus, 'waiting_binding');
    fs.copyFileSync(f.ficPath, ficPath);
    assert.equal((await tick()).scopeStatus, 'bound_fence_only');
    assert.equal(writes, 0);
    f.staleAuthority(true);
    dropResponse = true;
    await assert.rejects(tick, /conditional PUT response loss/);
  });
  assert.equal(writes, 1);
  const fenced = (await f.store.read())!;
  assert.equal(fenced.state.status, 'fenced');
  assert.equal(fenced.state.runInventory?.load_admission, 'authorized');
  assert.equal(fenced.state.runInventory?.qap, null);
  assert.equal(fenced.state.cleanupSteps[0]?.status, 'pending');
  assert.ok(fs.readFileSync(join(stateDir, 'effects.jsonl'), 'utf8').includes('unknown_after_forward'));
  const previous = f.events().length;
  await watch(async tick => assert.equal((await tick()).scopeStatus, 'fenced_cleanup_blocked'));
  assert.equal(writes, 1);
  await assert.rejects(f.runCli('finish'), /does not authorize/i);
  assert.equal(f.events().length, previous);
  assert.equal(f.namespaceAbsent(), false);
  assert.ok(f.credential());

  const recoveryDir = join(f.directory, 'recovery-state');
  fs.mkdirSync(recoveryDir, { mode: 0o700 });
  fs.unlinkSync(ficPath);
  const options = ['--release-file', f.releaseFile];
  await watch(async tick => assert.equal((await tick()).scopeStatus, 'direct_recovery_unsealed'), recoveryDir, options);
  assert.equal(writes, 1, 'inspection must not attempt a second campaign write or destructive custody');
  assert.equal(f.namespaceAbsent(), false);
  assert.ok(f.credential());
  const armPath = directArmRecordPath(armed.origin), originalArm = f.stored.get(armPath)!;
  f.stored.set(armPath, { ...originalArm, bytes: Buffer.from('{}') });
  await assert.rejects(watch(async tick => { await tick(); }, recoveryDir, options), /arm record does not match/);
  f.stored.set(armPath, originalArm);
  const shutdown = AbortSignal.abort(new Error('offline supervisor shutdown'));
  await assert.rejects(watch(async tick => { await tick(); }, recoveryDir, options, shutdown), /offline supervisor shutdown/);
  assert.equal(writes, 1);
  const expiration = args.indexOf('--expires-at') + 1, until = args[expiration];
  args[expiration] = new Date(Date.now() - 1).toISOString();
  await assert.rejects(watch(async tick => { await tick(); }, recoveryDir, options), /expiry must be/);
  args[expiration] = until;
  fs.copyFileSync(f.ficPath, ficPath);

  const successor = newCampaignState(uuid(89), origin.owner, Date.now());
  await f.store.write(successor, fenced.etag);
  await assert.rejects(watch(async tick => { await tick(); }), /refusing adoption/);
  assert.equal(writes, 1);

  const raceDir = join(f.directory, 'race-state');
  fs.mkdirSync(raceDir, { mode: 0o700 });
  await f.store.write({ ...fenced.state, status: 'active', fencingToken: 1, cleanupStatus: 'not-started', cleanupSteps: [],
    fenceReason: undefined, fencedAtMs: undefined }, (await f.store.read())!.etag);
  replaceBetweenCheckAndPut = true;
  await assert.rejects(watch(async tick => { await tick(); }, raceDir), /conflict/i);
  assert.equal(writes, 2, 'never retry a losing conditional fence');
  const replacement = (await f.store.read())!;
  assert.equal(replacement.state.campaignId, uuid(90));
  assert.equal(replacement.state.status, 'active');
  await f.store.write(fenced.state, replacement.etag);
  await watch(async tick => assert.equal((await tick()).scopeStatus, 'fenced_cleanup_blocked'), raceDir);
  assert.equal(writes, 2);
});

for (const scenario of ['admitted', 'pre-admission', 'pre-admission drift', 'setup settlement'] as const) test(`offline CLI handles genuine held arm: ${scenario}`, async t => {
  const settle = scenario === 'setup settlement', loseAdmission = scenario.startsWith('pre-admission'), drift = scenario === 'pre-admission drift';
  const f = await fixture(t, 'arm-write-unknown', true);
  const original = (await f.store.read())!, ficRecord = read_run_fic_record(f.ficPath, f.c);
  const poolId = ficRecord.aksId + '/agentPools/arrowe192';
  const armEndpoint = 'https://management.azure.com';
  const armOperation = (suffix: string) => `${armEndpoint}/subscriptions/${f.c.subscription}/providers/Microsoft.ContainerService/locations/westus/operations/${suffix}?api-version=2024-10-01`;
  const putUrl = `${armEndpoint}${poolId}?api-version=2024-10-01`, firstRequest = uuid(87);
  const idlePool = { id: poolId, properties: { mode: 'User', vmSize: 'Standard_E192ids_v6', count: 0,
    minCount: 0, maxCount: 1, enableAutoScaling: true, provisioningState: 'Succeeded' } };
  const raisedPool = { ...idlePool, properties: { ...idlePool.properties, minCount: 1,
    tags: { 'arrowcol-direct-setup-attempt': firstRequest } } };
  if (settle) {
    const body = (source: object, minimum: 0 | 1) => canonical_json(buildAksPoolScalePayload(source,
      { count: 0, autoscale: true, minimum, maximum: 1 }, minimum === 0 ? 'direct-restore-floor' : 'direct-first-floor'));
    await f.store.write({ ...original.state, directSetup: { schemaVersion: 1, runId: origin.runId,
      baselineEtag: '"fixture-baseline"', firstPool: { resourceId: poolId, expectedEtag: '"idle-before"',
        bodySha256: sha(body(raisedPool, 1)), requestId: firstRequest,
        response: { status: 202, requestId: 'original-ack', operationUrl: armOperation('raise') } },
      originalPoolBodySha256: sha(body(idlePool, 0)), originalPoolHasTags: false,
      namespaceAttempt: { nonce: uuid(83), bodySha256: 'a'.repeat(64), uid: origin.namespaceUid },
      accountAttempt: { nonce: uuid(84), bodySha256: 'b'.repeat(64), uid: ficRecord.serviceAccountUid },
      ficAttempt: true, heldStaging: true } }, original.etag);
  }
  assert.equal((await f.store.read())!.state.runInventory?.load_admission, 'not_authorized');
  await assert.rejects(f.runCli('arm'), settle ? /only held preparation/ : /does not authorize/);
  const path = directArmRecordPath(origin);
  if (loseAdmission) {
    await assert.rejects(f.runCli('prepare', { read: f.store.read.bind(f.store), async write() {
      throw new Error('fixture producer stopped before admission CAS');
    } }), /readback/);
  } else {
    const prepared = await f.runCli('prepare');
    assert.equal(prepared.action, 'prepare');
    if (prepared.action !== 'prepare') throw new Error('Pending arm result expected');
    assert.equal(prepared.status, 'prepared_not_released');
  }
  assert.equal((await f.store.read())!.state.runInventory?.load_admission, loseAdmission ? 'not_authorized' : 'authorized');
  const arm = JSON.parse(f.stored.get(path)!.bytes.toString());
  assert.equal(arm.origin.clients, 1);
  assert.equal(arm.clients.length, 1);
  assert.deepEqual(f.scheduleData(), { start: 'pending', writerLifetime: 'pending', clientLifetimes: 'pending' });
  assert.deepEqual(f.events().filter(event => ['arm-write', 'arm-readback', 'barrier-attempt', 'release'].includes(event.event))
    .map(event => event.event), ['arm-write', 'arm-readback']);
  await assert.rejects(f.runCli('finish'), settle ? /only held preparation/ : loseAdmission ? /does not authorize/ : /never released the schedule/);
  const { subscription, account, checkpoints } = f.c;
  const saved = (name: string, value: string) => {
    const file = join(f.directory, name);
    fs.writeFileSync(file, value, { mode: 0o600 });
    return file;
  };
  const cli = ['--import', 'tsx', 'projects/arrowcol-oat/aks-watchdog-envelope.ts',
    '--env-file', saved('allocation.env', [
      `AZURE_SUBSCRIPTION_ID=${subscription}`, `ARM_SUBSCRIPTION_ID=${subscription}`,
      `CAMPAIGN_STORAGE_ACCOUNT=${account}`, `CAMPAIGN_STATE_CONTAINER=${checkpoints}`,
    ].join('\n') + '\n'),
    '--stamp-config', saved('allocation.yaml', JSON.stringify(f.c.stampConfig)),
    '--campaign-record', saved('campaign.json', JSON.stringify((await f.store.read())!.state)),
    '--fic-record', f.ficPath, '--campaign-reads', '600'];
  const armBytes = f.stored.get(directArmRecordPath(arm.origin))!.bytes.toString();
  const savedArm = JSON.parse(armBytes);
  const readSet = { schema_version: 1, arm_sha256: createHash('sha256').update(canonical_json(savedArm)).digest('hex'),
    max_delta_version: 0, parquet_paths: [], delta_markers: [] };
  const readsPath = saved('reads.json', JSON.stringify(readSet));
  const run = (...args: string[]) => spawnSync(process.execPath, [...cli, ...args], { encoding: 'utf8' });
  const fence = run('--mode', 'fence');
  assert.equal(fence.status, 0, fence.stderr);
  const fenceEnvelope = parseEnvelope(JSON.parse(fence.stdout));
  assert.equal(validateEffects(fenceEnvelope, false, false), 'campaign-fence');
  assert.throws(() => validateEffects({ ...fenceEnvelope, effects: fenceEnvelope.effects.map(rule =>
    rule.id === 'campaign-fence' ? { ...rule, max_calls: 2 } : rule) }, false, false), /one conditional PUT/);

  const recoveryArgs = ['--mode', 'recovery', '--context', 'fixture', '--release-file', f.releaseFile,
    '--arm-record', saved('durable-arm.json', armBytes), '--delta-read-set', readsPath];
  const recovery = run(...recoveryArgs);
  assert.equal(recovery.status, 0, recovery.stderr);
  assert.equal(run(...recoveryArgs).stdout, recovery.stdout);
  const envelope = parseEnvelope(JSON.parse(recovery.stdout));
  assert.equal(validateEffects(envelope, true, true), 'campaign-fence');
  const fic = read_run_fic_record(f.ficPath, f.c);
  validateRecoveryTargets(envelope, f.c, savedArm, fic, 'fixture');
  for (const [id, mutate, reason] of [
    ['namespace-delete', { body_sha256: '0'.repeat(64) }, /exact original namespace UID/],
    ['stop-arrowcol', { path: '{}' }, /exact conditional stop/],
    ['fic-delete', { path: `${fic.identityId}/another?api-version=2023-01-31` }, /exact original FIC/],
  ] as const) {
    const changed = parseEnvelope({ ...envelope, effects: envelope.effects.map(rule => rule.id === id ? { ...rule, ...mutate } : rule) });
    assert.throws(() => validateRecoveryTargets(changed, f.c, savedArm, fic, 'fixture'), reason);
  }
  assert.ok(envelope.effects.some(rule => rule.method === 'PUT' && rule.path.endsWith('/interrupted/delta-00000000000000000000.json')));
  assert.ok(!envelope.effects.some(rule => rule.id === 'delta-commit-1' || rule.id.startsWith('delta-file-')));
  saved('reads.json', JSON.stringify({ ...readSet, arm_sha256: '0'.repeat(64) }));
  const mismatch = run(...recoveryArgs);
  assert.equal(mismatch.status, 1);
  assert.equal(mismatch.stdout, '');
  assert.match(mismatch.stderr, /original arm hash/);

  f.advanceClock(181_000);
  f.staleAuthority(true);
  if (drift) f.driftScheduleVersion();
  const stateDir = join(f.directory, 'generated-envelope');
  fs.mkdirSync(stateDir, { mode: 0o700 });
  const envelopePath = saved('generated-recovery.json', recovery.stdout);
  let fences = 0;
  await watchSelectedRun(['--env-file', 'fixture.env', '--stamp-config', 'fixture.yaml',
    '--effect-envelope', envelopePath, '--state-dir', stateDir, '--fic-record', f.ficPath,
    '--owner-id', origin.owner, '--namespace', origin.namespace, '--namespace-owner', origin.owner,
    '--expires-at', new Date(Date.now() + 300_000).toISOString(),
    '--release-file', f.releaseFile, '--recover-interrupted-context', 'fixture'],
  new AbortController().signal, async (_signal, tick) => {
    if (drift) await assert.rejects(tick(), /Original pre-admission pending barrier changed/);
    else assert.equal((await tick()).scopeStatus, 'direct_recovery_preserved_not_scored');
    return 0;
  },
  { config: f.c, azure: f.azure, token: async () => 'fixture-token', fetcher: async (_input, init) => {
    const current = (await f.store.read())!;
    if (init?.method === 'PUT') {
      assert.equal(new Headers(init.headers).get('if-match'), current.etag);
      assert.equal(fences++, 0);
      const etag = await f.store.write(JSON.parse(String(init.body)), current.etag);
      return new Response(null, { status: 201, headers: { etag } });
    }
    return new Response(JSON.stringify(current.state), { headers: { etag: current.etag } });
  } });
  assert.equal(f.namespaceAbsent(), !drift);
  if (drift) assert.ok(f.credential());
  else assert.equal(f.credential(), null);
  assert.equal(fences, 1);
  assert.equal(f.scheduleData().start, 'pending');
  assert.ok(!f.events().some(event => ['barrier-attempt', 'release', 'load'].includes(event.event)));
  const seal = [...f.stored.entries()].find(([key]) => key.endsWith('/interrupted/seal.json'))?.[1];
  if (drift) assert.equal(seal, undefined);
  else assert.equal(JSON.parse(seal!.bytes.toString()).result, 'interrupted_not_scored');
  assert.equal((await f.store.read())!.state.runInventory?.load_admission, loseAdmission ? 'not_authorized' : 'authorized');
  if (settle) {
    const originalRequest = f.azure.request.bind(f.azure);
    let reversals = 0;
    t.mock.method(f.azure, 'request', (async (method, url, resource, body, headers, missing, idempotent, capture) => {
      if (url === armOperation('raise') || url === armOperation('restore')) {
        assert.equal(method, 'GET');
        return [Buffer.from('{"status":"Succeeded"}'), { ':status': '200' }];
      }
      if (url === putUrl) {
        if (method === 'GET') return [Buffer.from(JSON.stringify(reversals ? idlePool : raisedPool)),
          { ':status': '200', etag: reversals ? '"idle-after"' : '"raised-live"' }];
        assert.equal(method, 'PUT');
        const reversal = (await f.store.read())!.state.directSetup!.reversal!;
        assert.equal(headers?.['If-Match'], '"raised-live"');
        assert.equal(headers?.['x-ms-client-request-id'], reversal.requestId);
        assert.equal(sha(body!), reversal.bodySha256);
        assert.equal(reversals++, 0);
        return [null, { ':status': '202', 'x-ms-request-id': 'settlement-ack', 'azure-asyncoperation': armOperation('restore') }];
      }
      return originalRequest(method, url, resource, body, headers, missing, idempotent, capture);
    }) satisfies Azure['request']);
    const env = { CAMPAIGN_ID: uuid(5), CAMPAIGN_OWNER_ID: origin.owner, CAMPAIGN_FENCING_TOKEN: '2' };
    const settleHeld = () => campaignMain(['settle-direct-held', origin.owner, '2', 'fixture', f.releaseFile], {
      azure: f.azure, store: f.store, env, loadConfig: positionals => ({ config: f.c, env, positionals, once: false }),
      directSetupBaseline: async () => ({ status: 'captured', campaign_id: uuid(5), owner_id: origin.owner,
        fencing_token: 1, blob: 'fixture-baseline', etag: '"fixture-baseline"', captured_at_unix_ms: original.state.startedAtMs }),
      directSetupKubernetes: async (_server, args) => args[1] === 'namespace' ? null : { kind: 'List', metadata: {}, items: [] },
    });
    await settleHeld();
    assert.equal((await f.store.read())!.state.status, 'fenced');
    await settleHeld();
    assert.equal((await f.store.read())!.state.status, 'setup_retired');
    assert.equal(reversals, 1);
  }
});

for (const [fault, reason] of [
  ['arm-readback', /readback mismatch/],
  ['arm-write-not-observed', /outcome UNKNOWN/],
  ['held-schedule-drift', /changed|no longer matches/],
] as const) {
  test('preparation never releases after ' + fault, async t => {
    const f = await fixture(t, fault, true);
    await assert.rejects(f.runCli('prepare'), reason);
    assert.equal((await f.store.read())!.state.runInventory?.load_admission, 'not_authorized');
    assert.equal(f.scheduleData().start, 'pending');
    assert.equal(f.events().filter(event => event.event === 'arm-write').length, 1);
    assert.ok(!f.events().some(event => ['barrier-attempt', 'release'].includes(event.event)));
  });
}

for (const fault of ['none', 'stop-unknown', 'exit-unknown'] as const) test(
  'selected recovery of the original unsealed run: ' + fault, async t => {
  const f = await fixture(t, fault);
  const arm = await f.arm();
  f.advanceClock(181_000);
  f.addInterruptedCommit(arm.startMs, arm.endMs);
  f.staleAuthority(true);
  const before = (await f.store.read())!;
  const { reconcileOnce } = await import('../../projects/arrowcol-oat/aks-reconcile.js');
  await reconcileOnce(f.store, Date.now(), true);
  assert.notEqual((await f.store.read())!.etag, before.etag);
  fs.unlinkSync(f.ficPath);
  const { subscription, group, account, checkpoints } = f.c;
  const resource = `https://${account}.blob.core.windows.net/${checkpoints}`;
  const path = `/${checkpoints}/active.json`;
  const stateDir = join(f.directory, 'selected-recovery'), envelope = join(f.directory, 'selected.json');
  fs.mkdirSync(stateDir, { mode: 0o700 });
  const scope = ['--context', 'fixture', '--server', 'https://fixture.azmk8s.io', '--insecure-skip-tls-verify=false'];
  const route = [...scope, '--request-timeout=15s', '-n', origin.namespace];
  const rules = [
    { id: 'get', transport: 'campaign-blob', method: 'GET', resource, path, max_calls: 600 },
    { id: 'put', transport: 'campaign-blob', method: 'PUT', resource, path, max_calls: 1 },
    { id: 'token', transport: 'process', method: 'AZURE_ACCOUNT_GET-ACCESS-TOKEN', resource: 'az', max_calls: 25,
      path: JSON.stringify(['account', 'get-access-token', '--subscription', subscription, '--resource',
        'https://storage.azure.com/', '--query', 'accessToken', '--output', 'tsv', '--only-show-errors']) },
  ];
  const add = (id: string, method: string, target: string, max_calls = 20, digest?: string) =>
    rules.push({ id, transport: 'process', method, resource: 'kubectl', path: target, max_calls,
      ...(digest ? { body_sha256: digest } : {}) });
  const gets = [['namespace', origin.namespace], ['daemonset', 'arrowcol-router'], ['pods', '-l', 'app=arrowcol-router'],
    ['endpointslices', '-l', 'kubernetes.io/service-name=arrowcol-router'],
    ['endpointslices', '-l', 'kubernetes.io/service-name=arrowcol-router-node-local'],
    ['deployment', 'arrowcol-sql'], ['replicasets', '-l', 'app=arrowcol-sql'], ['pods', '-l', 'app=arrowcol-sql']];
  for (const [role, uid] of [['arrowcol-router', arm.routerUid], ['arrowcol', arm.writer.deploymentUid],
    ['delta-maintenance', arm.maintenance.deploymentUid]] as const) {
    add('stop-' + role, 'KUBECTL_DIRECT_STOP', JSON.stringify({ argv: [...route, 'patch',
      role === 'arrowcol-router' ? 'daemonset' : 'deployment', role, '--type=json', '--patch-file=/dev/stdin', '-o', 'json'], uid }), 1);
    if (role !== 'arrowcol-router') {
      const pod = role === 'arrowcol' ? arm.writer : arm.maintenance;
      gets.push(['deployment', role], ['pod', pod.podName], ['pods', '-l', 'app=' + role]);
      add('exit-' + role, 'KUBECTL_DIRECT_EXIT_WATCH', JSON.stringify({ context: 'fixture', server: 'https://fixture.azmk8s.io',
        namespace: origin.namespace, pod: pod.podName, uid: pod.podUid }));
    }
  }
  gets.forEach((args, index) => add('observe-' + index, 'KUBECTL_GET', JSON.stringify([...route, 'get', ...args, '-o', 'json'])));
  const namespaceBody = Buffer.from(canonical_json({ apiVersion: 'v1', kind: 'DeleteOptions', preconditions: { uid: origin.namespaceUid } }));
  const { createHash } = await import('node:crypto');
  add('namespace-delete', 'KUBECTL_DELETE', JSON.stringify(scope.slice(2).concat('--request-timeout', '15s',
    'delete', '--raw', '/api/v1/namespaces/' + origin.namespace, '-f', '-')), 1,
  createHash('sha256').update(namespaceBody).digest('hex'));
  add('sql-read', 'KUBECTL_DIRECT_SQL_READ', JSON.stringify({ context: 'fixture', server: 'https://fixture.azmk8s.io',
    namespace: origin.namespace, pod: arm.sql.podName, uid: arm.sql.podUid, run: origin.runId, generation: origin.cloudGeneration }));
  fs.writeFileSync(envelope, JSON.stringify({ schema_version: 1, campaign_id: uuid(5), run_id: origin.runId,
    subscription_id: subscription, resource_group: group, storage_account: account, state_container: checkpoints, effects: rules }), { mode: 0o600 });
  const args = ['--env-file', 'fixture.env', '--stamp-config', 'fixture.yaml', '--effect-envelope', envelope, '--state-dir', stateDir,
    '--fic-record', f.ficPath, '--owner-id', origin.owner, '--namespace', origin.namespace, '--namespace-owner', origin.owner,
    '--expires-at', new Date(Date.now() + 300_000).toISOString(), '--release-file', f.releaseFile, '--recover-interrupted-context', 'fixture'];
  const run = (signal = new AbortController().signal) => watchSelectedRun(args, signal, async (_signal, tick) => {
    assert.equal((await tick()).scopeStatus, 'direct_recovery_preserved_not_scored');
    return 0;
  }, { config: f.c, azure: f.azure, token: async () => 'fixture-token', fetcher: async (_input, init) => {
    assert.notEqual(init?.method, 'PUT');
    const current = (await f.store.read())!;
    return new Response(JSON.stringify(current.state), { headers: { etag: current.etag } });
  } });
  await assert.rejects(run(AbortSignal.abort(new Error('supervisor cancelled'))), /supervisor cancelled/);
  if (fault !== 'none') {
    await assert.rejects(run(), fault === 'stop-unknown' ? /stop outcome UNKNOWN/ : /positive status event/);
    const patches = f.events().filter(event => event.event === 'stop').length;
    await assert.rejects(run(), fault === 'stop-unknown' ? /no PATCH replay/ : /terminal Pod evidence is unavailable/);
    assert.equal(f.events().filter(event => event.event === 'stop').length, patches);
    assert.equal(f.namespaceAbsent(), false);
    assert.ok(f.credential());
    assert.ok(![...f.stored.keys()].some(key => key.endsWith('/interrupted/seal.json')));
    return;
  }
  await run();
  assert.equal(f.namespaceAbsent(), true);
  assert.equal(f.credential(), null);
  assert.equal(f.events().filter(e => e.event === 'exit-watch').length, 2);
  assert.ok(f.events().findIndex(e => e.event === 'fic-delete') <
    f.events().map(e => e.event === 'blob' && e.container === f.c.data).lastIndexOf(true));
  await run();
  assert.equal(f.events().filter(e => e.event === 'fic-delete').length, 1);
  const receipt = fs.readFileSync(join(stateDir, 'effects.jsonl'), 'utf8').trim().split('\n').map(line => JSON.parse(line));
  const stops = receipt.filter(row => row.event === 'dispatch' && row.method === 'KUBECTL_DIRECT_STOP');
  assert.equal(stops.length, 3);
  assert.ok(stops.every(row => /^[a-f0-9]{64}$/.test(row.body_sha256)));
  const interruptedSeal = [...f.stored.keys()].find(key => key.endsWith('/interrupted/seal.json'))!;
  const originalSeal = f.stored.get(interruptedSeal)!;
  f.stored.delete(interruptedSeal);
  await assert.rejects(run, /no interrupted evidence seal/);
  f.stored.set(interruptedSeal, originalSeal);
  const originalCommit = [...f.stored.keys()].find(key => key.endsWith('/interrupted/delta-00000000000000000001.json'))!;
  assert.ok(originalCommit, 'custody must preserve a committed transaction even though the client never completed');
  const originalCopy = f.stored.get(originalCommit)!;
  f.stored.set(originalCommit, { ...originalCopy, bytes: Buffer.from('different') });
  await assert.rejects(run, /Delta commit copy is missing or changed/);
  f.stored.set(originalCommit, originalCopy);
  f.replacePhysical();
  await assert.rejects(run, /Delta history or physical files changed/);
  assert.equal(f.events().filter(e => e.event === 'fic-delete').length, 1);
});

test('real selected-watchdog entrypoint rejects incomplete invocation before accessing the control plane', () => {
  const child = spawnSync(process.execPath, ['--import', 'tsx', 'projects/arrowcol-oat/aks-watchdog.ts',
    '--watch-selected-run', '--owner-id', origin.owner], { encoding: 'utf8', timeout: 5_000 });
  assert.ifError(child.error);
  assert.equal(child.status, 1);
  assert.match(child.stderr, /Scoped watchdog requires the exact owner/);
  assert.equal(child.stdout, '');
});

test('production direct CLI preserves durable arm readback before barrier release', async t => {
  const f = await fixture(t, 'none');
  const armed = await f.runCli();
  assert.equal(armed.action, 'arm');
  f.load(armed.arm);
  const finished = await f.runCli('finish');
  assert.equal(finished.action, 'finish');
  const completed = (await f.store.read())!;
  assert.equal(completed.etag, finished.handoffEtag);
  assert.ok(Object.values(completed.state.runInventory!.cleanup).every(step => step.state === 'complete'));
  assert.ok(completed.state.runInventory!.resources.every(resource => resource.state === 'removed'));
  assert.equal(completed.state.runInventory!.fic?.state, 'removed');
  const recoverySnapshot = { ...completed, state: { ...completed.state, status: 'fenced' as const, fencingToken: 2 } };
  // Assert only independent readback here: the original positive seal and removal were produced by the existing finish fixture.
  const readOnly: CampaignStore = { read: async () => recoverySnapshot,
    write: async () => { throw new Error('Independent inspection cannot write'); } };
  const inspect = () => inspectDirectRecovery(f.azure, readOnly, recoverySnapshot, release, new AbortController().signal, f.directory);
  assert.ok((await inspect()).seal);
  f.replacePhysical();
  await assert.rejects(inspect, /physical file versions changed/);
  const sealPath = `${directEvidencePrefix(armed.arm.origin)}/seal.json`;
  f.stored.delete(sealPath);
  await assert.rejects(inspect, /deletion has no original terminal seal/);
  assert.equal(JSON.parse(f.stored.get(`_arrowcol-custody/campaigns/${uuid(5)}/cells/${origin.runId}/handoff.json`)!.bytes.toString()).phase, 'complete');
  const events = f.events(), index = (name: string) => events.findIndex(e => e.event === name);
  assert.ok(index('arm-readback') >= 0 && index('arm-readback') < index('release'));
  assert.ok(index('seal-readback') < index('namespace-delete'));
  assert.ok(index('namespace-positive') < index('namespace-absence') && index('namespace-absence') < index('fic-delete'));
});

test('real direct executable rejects malformed arguments and private config with nonzero exit', async t => {
  const f = await fixture(t, 'none');
  const executable = join(process.cwd(), 'node_modules', 'tsx', 'dist', 'cli.mjs');
  const invoke = (...args: string[]) => spawnSync(process.execPath, [executable, 'infra/azure/benchmark-direct-cli.ts', ...args],
    { encoding: 'utf8', timeout: 15_000, env: Object.fromEntries(Object.entries(process.env).filter(([key]) => key !== 'KUBECONFIG')) });
  for (const [args, expected] of [[['finish'], /missing a required option/], [f.cliArgs('arm'), /configuration|stamp/i]] as const) {
    const output = invoke(...args);
    assert.ifError(output.error);
    assert.equal(output.status, 1, output.stderr);
    assert.match(output.stderr, expected);
    assert.equal(output.stdout, '');
  }
});

test('direct finish reconciles a committed seal and FIC bookkeeping interruption without mutation replay', async t => {
  const f = await fixture(t, 'seal-unknown');
  const armed = await f.runCli();
  assert.equal(armed.action, 'arm');
  f.load(armed.arm);
  await assert.rejects(f.runCli('finish'), /seal response lost/);
  assert.equal(f.namespaceAbsent(), false);
  f.loseNamespaceReceipt();
  await assert.rejects(f.runCli('finish'), /namespace journal acknowledgement lost/);
  assert.equal(f.namespaceAbsent(), true);
  assert.notEqual(f.credential(), null);
  f.loseBookkeeping();
  await assert.rejects(f.runCli('finish'), /unknown/i);
  assert.equal(f.namespaceAbsent(), true);
  assert.equal(f.credential(), null);
  f.staleAuthority(true);
  await assert.rejects(f.runCli('finish'), /fenced/i);
  f.staleAuthority(false);
  await assert.rejects(f.runCli('finish', f.store, ['--run-id', uuid(99)]), /does not authorize/);
  const completed = await f.runCli('finish');
  assert.equal(completed.action, 'finish');
  assert.equal((await f.store.read())!.state.runInventory!.cleanup.handoff.state, 'complete');
  for (const event of ['seal-write', 'namespace-delete', 'fic-delete'])
    assert.equal(f.events().filter(record => record.event === event).length, 1, event);
});

test('production direct CLI rejects missing authority and bad pins before mutation', async t => {
  const f = await fixture(t, 'none');
  await assert.rejects(f.runCli('arm', undefined, ['--unknown', 'value']), /unknown or inapplicable option/);
  await assert.rejects(f.runCli('arm', undefined, ['--clients', '0']), /--clients must be an integer/);
  const missing: CampaignStore = { read: async () => undefined, write: async () => { throw new Error('unexpected write'); } };
  await assert.rejects(f.runCli('arm', missing), /No active campaign state exists/);
  assert.equal(f.events().some(event => ['arm-write', 'release', 'barrier-attempt'].includes(event.event)), false);
  fs.writeFileSync(f.releaseFile, JSON.stringify({ ...release, images: { ...release.images, router: 'mutable:tag' } }), { mode: 0o600 });
  await assert.rejects(f.runCli(), /image digest is invalid/);
  assert.equal(f.events().some(event => ['arm-write', 'release', 'barrier-attempt'].includes(event.event)), false);
});

for (const fault of ['none', 'clock-boundary', 'clock-skew', 'clock-missing', 'clock-namespace'] as const) test(`real direct custody lane: ${fault}`, async t => {
  const f = await fixture(t, fault), arm = await f.arm();
  assert.deepEqual(f.stored.get(directArmRecordPath(origin))!.bytes, Buffer.from(canonical_json(arm)));
  f.load(arm);
  const result = await f.finish(arm);
  const events = f.events(), index = (name: string) => events.findIndex(e => e.event === name);
  for (const [before, after] of [['arm-write', 'arm-readback'], ['arm-readback', 'release'], ['release', 'load'],
    ['load', 'stop'], ['stop', 'export-create'], ['export-bytes', 'seal-write'], ['seal-write', 'seal-readback'],
    ['seal-readback', 'namespace-delete'], ['namespace-positive', 'namespace-absence'], ['namespace-absence', 'fic-delete']])
    assert.ok(index(before) >= 0 && index(before) < index(after), `${before} must precede ${after}`);
  assert.deepEqual(events.filter(e => e.event === 'stop').map(e => e.name), ['arrowcol-router', 'arrowcol', 'delta-maintenance']);
  assert.equal(f.credential(), null); assert.equal(read_run_fic_record(f.ficPath, new Config({ AZURE_SUBSCRIPTION_ID: uuid(80), AZURE_STAMP: '68590' })).phase, 'deleted');
  assert.equal(result.receipt.namespace_record.namespaceUid, origin.namespaceUid);
  const seal = JSON.parse(f.stored.get(result.seal.path)!.bytes.toString());
  const terminal = parseEvidence(fs.readFileSync(join(f.terminal, 'direct-terminal.json'), 'utf8')) as any;
  assert.equal(terminal.diagnostic_reconciliation.scored_rows, 2n);
  assert.equal(terminal.scoring.status, fault === 'none' || fault === 'clock-boundary' ? 'sampled-clock-bounds-valid' : 'invalid');
  assert.equal(terminal.scoring.scored?.scored_rows ?? null, fault === 'none' ? 2n : fault === 'clock-boundary' ? 0n : null);
  if (fault === 'clock-boundary') {
    assert.equal(terminal.scoring.scored.excluded.after_window.rows, 2n);
    assert.equal(terminal.scoring.scored.scored_canonical_bytes, 0n);
  }
  assert.equal(terminal.qualification.status, 'not-qualified');
  assert.ok(terminal.limitations.some((value: string) => value.includes('does not establish NetworkPolicy enforcement')));
  assert.ok(terminal.limitations.some((value: string) => value.includes('NetworkPolicy not enforced (dataplane policy=none)')));
  assert.equal(terminal.timing.failures.length, fault === 'clock-missing' ? 2 : 0);
  assert.equal(terminal.timing.after.length, fault === 'clock-missing' ? 0 : 2);
  assert.equal(terminal.arm.provenance.clocks.length, 2);
  assert.equal(terminal.arm.provenance.transport.clients[0].config.tcp_nodelay, true);
  assert.equal(terminal.arm.provenance.transport.routers[0].config.upstream.connection_window_bytes, 16777216n);
  assert.equal(terminal.arm.provenance.transport.receiver.config.tcp_nodelay, true);
  assert.equal(terminal.arm.provenance.parquet.config.compression_level, 2n);
  assert.ok(terminal.arm.provenance.clocks.every((clock: any) => clock.timeNamespace === 'time:[1]'));
  assert.ok(terminal.timing.after.every((clock: any) => clock.timeNamespace ===
    (fault === 'clock-namespace' ? 'time:[2]' : 'time:[1]')));
  assert.deepEqual([...new Set(events.filter(e => e.event === 'time-namespace' && !e.loaded).map(e => e.name))].sort(),
    ['arrowcol-original', 'client-0']);
  if (fault !== 'clock-missing') assert.deepEqual(events.filter(e => e.event === 'time-namespace' && e.loaded).map(e => e.name).sort(),
    ['arrowcol-original', 'router-original']);
  if (fault === 'none') {
    assert.deepEqual(terminal.timing.after.map((reading: any) => reading.node).sort(), ['node-1', 'writer-node']);
    assert.equal(terminal.timing.after.find((reading: any) => reading.node === 'node-1').podUid,
      arm.routers.find(router => router.nodeName === 'node-1')!.uid);
  }
  assert.equal(f.control().resetstate, 'fenced-v1');
  for (const file of seal.files) {
    const bytes = Buffer.concat(file.chunks.map((chunk: any) => {
      const stored = f.stored.get(chunk.path)!; assert.equal(stored.etag, chunk.etag); assert.equal(sha(stored.bytes), chunk.sha256);
      return stored.bytes;
    }));
    assert.equal(bytes.length, file.bytes); assert.equal(sha(bytes), file.sha256);
  }
  assert.deepEqual(seal.files.map((file: any) => file.key).sort(), ['writer-stdout.raw', 'writer-stderr.raw',
    'client-0-stdout.raw', 'client-0-stderr.raw', 'delta-00000000000000000000.json', 'delta-00000000000000000001.json', 'direct-terminal.json'].sort());
});

const failures: [Fault, RegExp, 'arm' | 'terminal', boolean, boolean][] = [
  ['arm-write-unknown', /arm write response lost/, 'arm', false, false],
  ['arm-readback', /arm record readback mismatch/, 'arm', false, false],
  ['client-transport', /Client effective transport differs/, 'arm', false, false],
  ['router-transport', /Router effective transport differs/, 'arm', false, false],
  ['receiver-transport', /Receiver effective transport differs/, 'arm', false, false],
  ['parquet-config', /Parquet startup policy differs/, 'arm', false, false],
  ['release-unknown', /Command failed/, 'arm', false, false],
  ['release-replaced', /schedule identity changed/, 'arm', false, false],
  ['arm-replaced', /arm record is missing or changed/, 'terminal', false, false],
  ['client-replaced', /replaced or disappeared/, 'terminal', false, false],
  ['stop-unknown', /PATCH.*UNKNOWN|PATCH.*unknown|Command failed/i, 'terminal', false, false],
  ['exit-unknown', /positive status event/, 'terminal', false, false],
  ['audit-invalid', /terminal failure/, 'terminal', false, false],
  ['seal-unknown', /seal response lost/, 'terminal', false, false],
  ['seal-readback', /readback mismatch/, 'terminal', false, false],
  ['seal-replaced', /fixture stale read/, 'terminal', false, false],
  ['namespace-unknown', /Kubernetes request failed/, 'terminal', true, false],
  ['namespace-replaced', /does not affirm the original UID/, 'terminal', true, false],
  ['namespace-present', /still pending/, 'terminal', true, false],
  ['fic-replaced', /changed since definitive creation/, 'terminal', true, false],
  ['fic-unknown', /FIC DELETE response lost/, 'terminal', true, true],
];
for (const [fault, message, phase, namespaceDelete, ficDelete] of failures) test(`direct custody fails closed: ${fault}`, async t => {
  const f = await fixture(t, fault);
  if (phase === 'arm') await assert.rejects(f.arm(), message);
  else { const arm = await f.arm(); f.load(arm); await assert.rejects(f.finish(arm), message); }
  const events = f.events();
  assert.equal(events.some(e => e.event === 'namespace-delete'), namespaceDelete);
  assert.equal(events.some(e => e.event === 'fic-delete'), ficDelete);
  if ((fault.startsWith('arm-') || fault.endsWith('-transport') || fault === 'parquet-config') && phase === 'arm') {
    assert.equal(events.some(e => e.event === 'release'), false);
    assert.equal(events.some(e => e.event === 'barrier-attempt'), false);
  }
  if (phase === 'arm') assert.equal(events.some(e => e.event === 'load'), false);
  if (!namespaceDelete) assert.equal(events.some(e => e.event === 'namespace-positive'), false);
  if (fault === 'audit-invalid') assert.equal(events.some(e => e.event === 'seal-write'), false);
  if (fault === 'namespace-unknown' || fault === 'namespace-replaced') assert.equal(events.some(e => e.event === 'namespace-absence'), false);
  if (fault === 'namespace-unknown' || fault === 'namespace-replaced' || fault === 'namespace-present') {
    assert.equal(f.namespaceAbsent(), fault !== 'namespace-present');
    const journal = [...f.stored].find(([key]) => key.startsWith('_arrowcol-custody/namespaces/'))!;
    const proof = JSON.parse(journal[1].bytes.toString());
    assert.equal(proof.phase, fault === 'namespace-present' ? 'responded' : 'intent');
    assert.equal(proof.receipt, null);
  }
  assert.equal(events.filter(e => e.event === 'namespace-delete').length, namespaceDelete ? 1 : 0, 'unknown namespace mutation must never replay');
  assert.equal(events.filter(e => e.event === 'fic-delete').length, ficDelete ? 1 : 0, 'unknown FIC mutation must never replay');
  assert.equal(read_run_fic_record(f.ficPath, new Config({ AZURE_SUBSCRIPTION_ID: uuid(80), AZURE_STAMP: '68590' })).phase,
    ficDelete ? 'delete-submitted' : 'ready');
});
