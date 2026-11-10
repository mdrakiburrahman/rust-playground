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
  assert.equal(get('Deployment', 'arrowcol-catalog').spec.template.spec.containers[0].args.at(-1), '--ingestion-layout');
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
