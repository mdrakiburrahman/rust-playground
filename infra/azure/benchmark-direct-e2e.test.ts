import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import fs from 'node:fs';
import { delimiter, join } from 'node:path';
import test, { type TestContext } from 'node:test';
import { Azure, Config, Failure, HttpFailure, TAGS, canonical_json, runtime, type HttpReceipt } from './bootstrap.js';
import { armDirectBenchmark, type DirectRelease } from './benchmark-direct-arm.js';
import { directArmRecordPath, finishDirectTerminalCustody } from './benchmark-terminal-custody.js';
import { prepare_run_fic, read_run_fic_record, run_fic_ledger_path } from './run_fic.js';
import { ENCODING, MAX_RECORDS } from './benchmark-manifest-audit.js';
import { parseEvidence } from './benchmark-evidence-json.js';
import { fakeKubectl } from './benchmark-direct-e2e.fixture.js';
import type { CampaignStore, StoredCampaign } from '../../projects/arrowcol-oat/aks-campaign.js';

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
type Fault = 'none' | 'arm-write-unknown' | 'arm-readback' | 'release-unknown' | 'release-replaced' |
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

async function fixture(t: TestContext, fault: Fault) {
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
        return [null, { ':status': '200', 'x-ms-request-id': 'fixture-head', etag: '"physical"', 'content-length': '40' }]; }
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
    stored.set(path, { bytes, etag });
    if (path === directArmRecordPath(origin)) { record('arm-write'); if (fault === 'arm-write-unknown') throw new Failure('Injected arm write response lost'); }
    if (path.endsWith('/seal.json')) { record('seal-write'); if (fault === 'seal-unknown') throw new Failure('Injected seal response lost'); }
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
  t.mock.method(runtime, 'run_tree', async (args: string[]) => {
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
  const campaignState = { status: 'active', campaignId: uuid(5), ownerId: origin.owner, fencingToken: 1,
    heartbeatAtMs: now - 1, expiresAtMs: now + 3_600_000, runInventory: { campaign_id: uuid(5), owner_id: origin.owner,
      cell_run_id: origin.runId, fencing_token: 1, namespace: { name: origin.namespace, uid: origin.namespaceUid, owner_token: origin.owner },
      load_admission: 'authorized', fic: { state: 'observed' }, qap: null, cleanup: { handoff: { state: 'pending' } } } };
  const store: CampaignStore = { read: async () => { campaignState.heartbeatAtMs = now - 1;
    return { state: campaignState, etag: '"campaign"' } as StoredCampaign; },
    write: async () => { throw new Error('Rehearsal must not rewrite producer authority'); } };
  const campaign = { store, campaignId: uuid(5), ownerId: origin.owner };
  const arm = () => armDirectBenchmark(azure, campaign, origin, release, 'fixture', ficPath, baseline);
  const load = (armed: Awaited<ReturnType<typeof arm>>) => {
    const current = JSON.parse(fs.readFileSync(statePath, 'utf8'));
    assert.equal(current.objects['configmap/direct-schedule-confirmwide'].data.start, String(armed.startMs - 60_000));
    Object.assign(state, current);
    now = armed.endMs + 1000; state.now = now; state.loaded = true;
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
  return { arm, load, finish: (armed: Awaited<ReturnType<typeof arm>>) =>
    finishDirectTerminalCustody(azure, campaign, armed, 'fixture', terminal, delta, ficPath), events, stored, terminal, ficPath,
    namespaceAbsent: () => namespaceAbsent, credential: () => credential, control: () => control };
}

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
