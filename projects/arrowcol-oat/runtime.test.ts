import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { chmod, mkdir, readFile, rm, stat, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import test from 'node:test';
import { parse } from 'yaml';
import { prepareSecrets, renderRuntime, validateRuntime, type RuntimeConfig } from './runtime.js';

const config = (): RuntimeConfig => ({
  namespace: 'arrowcol-oat', oatRunId: '00000000-0000-0000-0000-000000000003',
  workloadIdentityClientId: '00000000-0000-0000-0000-000000000001',
  subscriptionId: '00000000-0000-0000-0000-000000000002', stamp: '89152',
  deltaRoot: 'abfss://onelake@arrowcollector89152.dfs.core.windows.net/dataflow',
  images: Object.fromEntries(['sqlserver', 'catalog', 'router', 'dataflow'].map((role) =>
    [role, `arrowcollector89152.azurecr.io/${role}@sha256:${'a'.repeat(64)}`])) as RuntimeConfig['images'],
});

test('rejects mutable images, QAP namespace, wrong stamp and unknown fields', () => {
  assert.throws(() => validateRuntime({ ...config(), namespace: 'azure-qap' }));
  assert.throws(() => validateRuntime({ ...config(), namespace: 'kube-public' }));
  assert.throws(() => validateRuntime({ ...config(), unexpected: true }));
  assert.throws(() => validateRuntime({ ...config(), stamp: '89151' }));
  assert.throws(() => validateRuntime({ ...config(), stamp: 89152 }));
  const c = config(); c.images.dataflow = 'example:latest';
  assert.throws(() => validateRuntime(c));
  assert.throws(() => validateRuntime({ ...config(), quiver: 'true' }), /explicit boolean/);
});

test('only explicit Quiver topology installs the durable processor and isolated pod storage', async () => {
  for (const quiver of [false, true]) {
    const objects = await renderRuntime({ ...config(), quiver }, 'workload');
    const engine = parse(objects.find(o => o.kind === 'ConfigMap')!.data['dataflow.yaml']);
    const pipeline = engine.groups.arrowcol.pipelines.ingest;
    assert.equal(pipeline.nodes.flight.config.native_quiver, quiver);
    assert.equal(Boolean(pipeline.nodes.durable), quiver);
    assert.equal(engine.policies.resources, undefined);
    assert.deepEqual(pipeline.connections.map((connection: { from: string; to: string }) => `${connection.from}:${connection.to}`),
      quiver ? ['flight:durable', 'durable:delta'] : ['flight:delta']);
    const pod = objects.find(o => o.kind === 'Deployment' && o.metadata.name === 'arrowcol')!.spec.template.spec;
    assert.equal(pod.volumes.some((volume: { name: string }) => volume.name === 'quiver'), quiver);
    assert.equal(pod.containers[0].volumeMounts.some((mount: { name: string; readOnly: boolean }) =>
      mount.name === 'quiver' && !mount.readOnly), quiver);
  }
});

test('maintenance requires a pinned sixth image and explicit allowlist/operation/apply controls', async () => {
  const c = config();
  c.maintenance = { tables: ['telemetry.sqlserver.sqlserver_cpu_utilization'], operations: ['optimize'], apply: false };
  assert.throws(() => validateRuntime(c), /pinned maintenance image/);
  c.images.maintenance = 'example:latest';
  assert.throws(() => validateRuntime(c), /registry manifest digest/);
  c.images.maintenance = `arrowcollector89152.azurecr.io/delta-maintenance@sha256:${'b'.repeat(64)}`;
  const objects = await renderRuntime(c, 'workload');
  const worker = objects.find(o => o.kind === 'Deployment' && o.metadata.name === 'delta-maintenance')!;
  assert.equal(worker.spec.replicas, 0);
  const process = worker.spec.template.spec.containers[0];
  assert.equal(process.image, c.images.maintenance);
  assert.ok(process.args.includes(c.maintenance.tables[0]));
  assert.ok(!process.args.includes('--apply') && !process.args.includes('--root'));
  assert.equal(process.env.find((value: { name: string }) => value.name === 'ARROWCOL_CATALOG_DDL_IDENTITY_SECRET_FILE').value, '/run/auth/internal-hmac');
  assert.equal(worker.spec.template.metadata.labels['azure.workload.identity/use'], 'true');
  assert.equal(worker.spec.template.spec.automountServiceAccountToken, false);
  const catalog = objects.find(o => o.metadata.name === 'catalog-internal')!;
  assert.ok(catalog.spec.ingress[0].from.some((source: { podSelector: { matchLabels: { app: string } } }) => source.podSelector.matchLabels.app === 'delta-maintenance'));
  for (const bad of [{ ...c.maintenance, tables: [] }, { ...c.maintenance, tables: ['*'] },
    { ...c.maintenance, operations: ['unknown'] }, { ...c.maintenance, apply: undefined }]) {
    assert.throws(() => validateRuntime({ ...c, maintenance: bad }), /explicit qualified table/);
  }
});

test('isolated integrated topology pins images, WI, identity, probes and pooled table ownership', async () => {
  const c = config();
  const objects = await renderRuntime(c, 'workload');
  const get = (kind: string, name: string) => objects.find((o) => o.kind === kind && o.metadata.name === name)!;
  const dfe = get('Deployment', 'arrowcol');
  assert.equal(objects.filter((o) => o.kind === 'Deployment').length, 3);
  assert.equal(dfe.spec.strategy.type, 'Recreate');
  assert.equal(dfe.spec.template.spec.nodeSelector['arrow-collector/role'], 'arrowcol');
  assert.ok(dfe.spec.template.spec.tolerations.some((t: { key: string; value: string; effect: string }) =>
    t.key === 'arrow-collector/role' && t.value === 'arrowcol' && t.effect === 'NoSchedule'));
  assert.equal(dfe.spec.template.metadata.labels['azure.workload.identity/use'], 'true');
  assert.equal(dfe.spec.template.spec.serviceAccountName, 'arrowcol-oat');
  const container = dfe.spec.template.spec.containers[0];
  assert.equal(container.image, c.images.dataflow);
  assert.deepEqual(container.args.slice(-2), ['--num-cores', '1']);
  assert.equal(container.env.find((e: any) => e.name === 'ARROWCOL_INSTANCE_KEY').valueFrom.fieldRef.fieldPath, 'metadata.uid');
  assert.equal(container.env.find((e: any) => e.name === 'POD_IP').valueFrom.fieldRef.fieldPath, 'status.podIP');
  assert.equal(container.env.find((e: any) => e.name === 'ARROWCOL_RESET_GENERATION').valueFrom.secretKeyRef.name, 'arrowcol-generation');
  assert.equal(container.env.find((e: any) => e.name === 'ARROWCOL_PARTITION_GRANULARITY').value, 'hour');
  assert.equal(container.readinessProbe.httpGet.port, 32011);
  assert.equal(container.env.find((e: any) => e.name === 'ARROWCOL_CATALOG_DDL_IDENTITY_SECRET_FILE').value, '/run/auth/internal-hmac');
  assert.ok(dfe.spec.template.spec.containers[0].volumeMounts.some((mount: any) =>
    mount.name === 'auth' && mount.mountPath === '/run/auth' && mount.readOnly));
  assert.ok(dfe.spec.template.spec.volumes.some((volume: any) =>
    volume.name === 'auth' && volume.secret.secretName === 'arrowcol-router-auth'));
  const configs = get('ConfigMap', 'arrowcol-config').data;
  const engine = parse(configs['dataflow.yaml']);
  assert.equal(engine.policies.resources, undefined);
  assert.equal(engine.groups.arrowcol.pipelines.ingest.nodes.flight.type, 'receiver:arrow_flight');
  assert.equal(engine.groups.arrowcol.pipelines.ingest.nodes.delta.type, 'exporter:arrow_deltalake');
  const router = parse(configs['router.yaml']);
  assert.equal(router.auth.authenticator, 'dev_only_allow_all');
  assert.equal(router.routing.oat_run_id, c.oatRunId);
  assert.equal(router.upstream.tls, false);
  assert.equal(router.upstream.max_h2_streams, 1);
  assert.equal(router.upstream.idle_timeout_seconds, 0);
  assert.equal(router.routing.distinct_table_owners, false);
  assert.equal(router.routing.lease_ttl_ms, 3_600_000);
  assert.equal(router.auth.edge_secret_file, '/run/auth/edge-hmac');
  assert.equal(objects.some((o) => o.kind === 'Ingress' || o.spec?.type === 'LoadBalancer'), false);
  const catalog = get('Deployment', 'arrowcol-catalog').spec.template.spec;
  assert.ok(catalog.containers[0].args.includes('--ingestion-layout'));
  assert.deepEqual(catalog.containers[0].args.slice(-2), ['--ddl-identity-secret-file', '/run/auth/internal-hmac']);
  assert.equal(
    catalog.containers[0].env.find((e: any) => e.name === 'ARROWCOL_CATALOG_DDL_IDENTITY_SECRET_FILE').value,
    '/run/auth/internal-hmac',
  );
  assert.ok(catalog.containers[0].volumeMounts.some((mount: any) =>
    mount.name === 'auth' && mount.mountPath === '/run/auth' && mount.readOnly));
  assert.ok(catalog.volumes.some((volume: any) =>
    volume.name === 'auth' && volume.secret.secretName === 'arrowcol-router-auth'));
  const catalogIngress = get('NetworkPolicy', 'catalog-internal').spec.ingress;
  assert.deepEqual(catalogIngress[0].from, [
    { podSelector: { matchLabels: { app: 'arrowcol' } } },
    { podSelector: { matchLabels: { app: 'arrowcol-oat-client' } } },
  ]);
  const rules = get('NetworkPolicy', 'flight-router-only').spec.ingress;
  assert.deepEqual(rules[0].from, [{ podSelector: { matchLabels: { app: 'arrowcol-router' } } }]);
  const clientIngress = get('NetworkPolicy', 'router-oat-client-ingress').spec.ingress;
  assert.deepEqual(clientIngress, [{
    from: [{ podSelector: { matchLabels: { app: 'arrowcol-oat-client' } } }],
    ports: [{ protocol: 'TCP', port: 50050 }],
  }]);
});

test('configured existing federation subject is used by every runtime phase', async () => {
  const c = { ...config(), namespace: 'arrow-collector', serviceAccount: 'arrow-collector' };
  for (const phase of ['identity', 'foundation', 'bootstrap', 'workload'] as const) {
    const objects = await renderRuntime(c, phase);
    for (const object of objects) {
      if (object.kind !== 'Namespace') assert.equal(object.metadata.namespace, c.namespace);
      if (object.spec?.template) {
        assert.equal(object.spec.template.spec.serviceAccountName, c.serviceAccount);
        assert.equal(object.spec.template.spec.enableServiceLinks, false);
      }
      if (object.kind === 'ServiceAccount') {
        assert.equal(object.metadata.name, c.serviceAccount);
        assert.equal(object.metadata.annotations['azure.workload.identity/client-id'], c.workloadIdentityClientId);
        assert.equal(object.automountServiceAccountToken, false);
      }
    }
  }
  for (const serviceAccount of ['', 'default', 'Bad_Name', '../other', 'a'.repeat(64), null, 3]) {
    assert.throws(() => validateRuntime({ ...c, serviceAccount }), /serviceAccount/);
  }
});

test('unpublished draft images cannot be rendered as deployable resources', async () => {
  const c = config();
  c.images.sqlserver = 'unpublished';
  assert.equal(validateRuntime(c, true), c);
  assert.throws(() => validateRuntime(c), /registry manifest digest/);
  for (const phase of ['foundation', 'bootstrap', 'workload'] as const) {
    await assert.rejects(renderRuntime(c, phase), /registry manifest digest/);
  }
  const identity = await renderRuntime(c, 'identity');
  assert.deepEqual(identity.map(o => o.kind), ['Namespace', 'ServiceAccount']);
  assert.equal(identity.some(o => o.spec?.template || o.kind === 'Secret'), false);
});

test('SQL persistence and sequential bootstrap keep migration credentials out of runtime', async () => {
  const foundation = await renderRuntime(config(), 'foundation');
  assert.equal(foundation.filter((o) => o.kind === 'PersistentVolumeClaim').length, 2);
  const denyIndex = foundation.findIndex((o) => o.kind === 'NetworkPolicy' && o.metadata.name === 'default-deny-ingress');
  assert.ok(denyIndex >= 0 && denyIndex < foundation.findIndex((o) => o.kind === 'Deployment'));
  const sql = foundation.find((o) => o.kind === 'Deployment')!.spec.template.spec.containers[0];
  assert.deepEqual(sql.securityContext.capabilities, { drop: ['ALL'], add: ['NET_BIND_SERVICE'] });
  assert.equal(sql.securityContext.allowPrivilegeEscalation, false);
  const sqlIngress = foundation.find((o) => o.kind === 'NetworkPolicy' && o.metadata.name === 'sql-internal')!.spec.ingress;
  assert.ok(sqlIngress[0].from.some((source: any) => source.podSelector.matchLabels.app === 'arrowcol-oat-pin-init'));
  const [job] = await renderRuntime(config(), 'bootstrap');
  assert.equal(job.metadata.name, 'arrowcol-migrate-v11');
  assert.deepEqual(job.spec.template.spec.initContainers[0].command, ['/usr/local/bin/database-bootstrap']);
  assert.deepEqual(job.spec.template.spec.containers[0].command, ['/usr/local/bin/arrowcol-migrate']);
  assert.equal(job.spec.backoffLimit, 0);
  const workload = await renderRuntime(config(), 'workload');
  for (const deployment of workload.filter((o) => o.kind === 'Deployment')) {
    assert.deepEqual(deployment.spec.template.spec.containers[0].securityContext.capabilities, { drop: ['ALL'] });
  }
  assert.equal(JSON.stringify(workload).includes('arrowcol-sql-migration'), false);
  assert.equal(JSON.stringify(workload).includes('arrowcol-sql-admin'), false);
});

test('secret preparation never rotates an existing key or invents a generation', async () => {
  const root = resolve('out/arrowcol-oat-tests', randomUUID());
  const directory = resolve(root, 'secrets');
  const bootstrap = resolve(root, 'bootstrap.env');
  await mkdir(root, { recursive: true });
  try {
    await writeFile(bootstrap, 'AZURE_STAMP=89152\n');
    await assert.rejects(prepareSecrets(directory, bootstrap), /captured/);
    await writeFile(bootstrap, 'ARROWCOL_RESET_GENERATION=00000000-0000-0000-0000-000000000001\n');
    await prepareSecrets(directory, bootstrap);
    const key = await readFile(resolve(directory, 'edge-hmac'));
    await prepareSecrets(directory, bootstrap);
    assert.deepEqual(await readFile(resolve(directory, 'edge-hmac')), key);
    assert.equal((await stat(resolve(directory, 'edge-hmac'))).mode & 0o777, 0o600);
    await writeFile(bootstrap, 'ARROWCOL_RESET_GENERATION=00000000-0000-0000-0000-000000000002\n');
    await assert.rejects(prepareSecrets(directory, bootstrap), /do not rotate/);
    await chmod(directory, 0o755);
    await assert.rejects(prepareSecrets(directory, bootstrap), /0700/);
  } finally { await rm(root, { recursive: true, force: true }); }
});
