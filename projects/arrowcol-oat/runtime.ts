import { randomBytes, randomUUID } from 'node:crypto';
import { chmod, lstat, mkdir, readFile, rename, rm, stat, writeFile } from 'node:fs/promises';
import { dirname, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { parse, stringify } from 'yaml';

export interface RuntimeConfig {
  namespace: string;
  oatRunId: string;
  serviceAccount?: string;
  workloadIdentityClientId: string;
  subscriptionId: string;
  stamp: string;
  deltaRoot: string;
  images: { sqlserver: string; catalog: string; router: string; dataflow: string };
}
type Resource = Record<string, any>;
export type Phase = 'identity' | 'foundation' | 'bootstrap' | 'workload';
const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const dns = /^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$/;
const roles = ['sqlserver', 'catalog', 'router', 'dataflow'] as const;

export function validateRuntime(input: unknown, allowUnpublishedImages = false): RuntimeConfig {
  if (!input || typeof input !== 'object' || Array.isArray(input)) throw new Error('runtime config must be an object');
  const c = input as RuntimeConfig;
  const required = ['deltaRoot', 'images', 'namespace', 'oatRunId', 'workloadIdentityClientId', 'subscriptionId', 'stamp'];
  if (required.some((key) => !Object.hasOwn(c, key)) || Object.keys(c).some((key) => ![...required, 'serviceAccount'].includes(key))) {
    throw new Error('runtime config has missing or unknown fields');
  }
  if (typeof c.namespace !== 'string' || !dns.test(c.namespace) || c.namespace.startsWith('kube-') || ['default', 'azure-qap'].includes(c.namespace)) {
    throw new Error('use a dedicated OAT namespace (not the QAP namespace)');
  }
  if (typeof c.oatRunId !== 'string' || !uuid.test(c.oatRunId)) throw new Error('oatRunId must be the current OAT run UUID');
  if (Object.hasOwn(c, 'serviceAccount') &&
      (typeof c.serviceAccount !== 'string' || !dns.test(c.serviceAccount) || c.serviceAccount === 'default')) {
    throw new Error('serviceAccount must be a non-default Kubernetes DNS label');
  }
  if (typeof c.workloadIdentityClientId !== 'string' || !uuid.test(c.workloadIdentityClientId)) throw new Error('workloadIdentityClientId must be the provisioned UAMI client UUID');
  if (typeof c.subscriptionId !== 'string' || !uuid.test(c.subscriptionId) ||
      typeof c.stamp !== 'string' || !/^[0-9]{5}$/.test(c.stamp)) throw new Error('subscriptionId and five-digit stamp are required');
  if (typeof c.deltaRoot !== 'string' ||
      !/^abfss:\/\/[a-z0-9-]+@[a-z0-9]+\.dfs\.core\.windows\.net\/[a-z0-9/_-]*dataflow$/.test(c.deltaRoot) ||
      c.deltaRoot.includes('//', 8)) {
    throw new Error('deltaRoot must be the bootstrapped credential-free abfss dataflow root');
  }
  if (new URL(c.deltaRoot).hostname !== `arrowcollector${c.stamp}.dfs.core.windows.net`) throw new Error('dataflow root must match the selected stamp');
  if (!c.images || Object.keys(c.images).sort().join() !== [...roles].sort().join()) throw new Error('all four images are required');
  for (const role of roles) {
    if (allowUnpublishedImages && c.images[role] === 'unpublished') continue;
    if (typeof c.images[role] !== 'string' ||
        !/^[a-z0-9]+\.azurecr\.io\/[a-z0-9][a-z0-9/_.-]*@sha256:[a-f0-9]{64}$/.test(c.images[role])) {
      throw new Error(`${role} must be an ACR image pinned by registry manifest digest, not a local image ID or tag`);
    }
  }
  return c;
}

const security = {
  runAsNonRoot: true, runAsUser: 10001, runAsGroup: 10001,
  allowPrivilegeEscalation: false, capabilities: { drop: ['ALL'] },
};
const env = (name: string, value: string) => ({ name, value });
const secretEnv = (name: string, secret: string, key: string) => ({
  name, valueFrom: { secretKeyRef: { name: secret, key } },
});
const secretVolume = (name: string, secretName: string) => ({
  name, secret: { secretName, defaultMode: 0o440 },
});
const mount = (name: string, mountPath: string, readOnly = true) => ({ name, mountPath, readOnly });
const resources = (cpu: string, memory: string, limitCpu = cpu, limitMemory = memory) => ({
  requests: { cpu, memory }, limits: { cpu: limitCpu, memory: limitMemory },
});
const httpProbe = (path: string, port: number) => ({ httpGet: { path, port }, timeoutSeconds: 3, periodSeconds: 5, failureThreshold: 6 });
const execProbe = (command: string[]) => ({ exec: { command }, timeoutSeconds: 8, periodSeconds: 10, failureThreshold: 6 });

export async function renderRuntime(input: unknown, phase: Phase): Promise<Resource[]> {
  const c = validateRuntime(input, phase === 'identity');
  const serviceAccount = c.serviceAccount ?? 'arrowcol-oat';
  if (!['identity', 'foundation', 'bootstrap', 'workload'].includes(phase)) throw new Error('unknown runtime phase');
  const object = (apiVersion: string, kind: string, name: string, body: Resource): Resource => ({
    apiVersion, kind, metadata: { name, namespace: c.namespace, labels: { 'app.kubernetes.io/part-of': 'arrowcol-oat' } }, ...body,
  });
  const service = (name: string, port: number) => object('v1', 'Service', name, {
    spec: { type: 'ClusterIP', selector: { app: name }, ports: [{ port, targetPort: port }] },
  });
  const pvc = (name: string, size: string) => object('v1', 'PersistentVolumeClaim', name, {
    spec: { accessModes: ['ReadWriteOnce'], storageClassName: 'managed-csi', resources: { requests: { storage: size } } },
  });
  const pod = (name: string, containers: Resource[], volumes: Resource[], identity = false): Resource => ({
    metadata: { labels: { app: name, 'app.kubernetes.io/part-of': 'arrowcol-oat', ...(identity ? { 'azure.workload.identity/use': 'true' } : {}) } },
    spec: {
      serviceAccountName: serviceAccount, automountServiceAccountToken: false,
      enableServiceLinks: false,
      nodeSelector: { 'arrow-collector/role': 'arrowcol', 'kubernetes.io/arch': 'amd64' },
      tolerations: [{
        key: 'arrow-collector/role', operator: 'Equal', value: 'arrowcol', effect: 'NoSchedule',
      }],
      securityContext: { fsGroup: 10001, fsGroupChangePolicy: 'OnRootMismatch', seccompProfile: { type: 'RuntimeDefault' } },
      terminationGracePeriodSeconds: 360, containers, volumes,
    },
  });
  const deployment = (name: string, containers: Resource[], volumes: Resource[], identity = false) => object('apps/v1', 'Deployment', name, {
    // Recreate prevents old SQL table owners and replacement pods overlapping in a matrix case.
    spec: { replicas: 1, strategy: { type: 'Recreate' }, selector: { matchLabels: { app: name } }, template: pod(name, containers, volumes, identity) },
  });
  const network = (name: string, app: string, from: string[], port: number) => object('networking.k8s.io/v1', 'NetworkPolicy', name, {
    spec: {
      podSelector: { matchLabels: { app } }, policyTypes: ['Ingress'],
      ingress: [{ from: from.map((source) => ({ podSelector: { matchLabels: { app: source } } })), ports: [{ protocol: 'TCP', port }] }],
    },
  });
  const identity = [
    { apiVersion: 'v1', kind: 'Namespace', metadata: { name: c.namespace, labels: { 'app.kubernetes.io/part-of': 'arrowcol-oat' } } },
    object('v1', 'ServiceAccount', serviceAccount, {
      metadata: { name: serviceAccount, namespace: c.namespace, annotations: { 'azure.workload.identity/client-id': c.workloadIdentityClientId } },
      automountServiceAccountToken: false,
    }),
  ];
  if (phase === 'identity') return identity;
  if (phase === 'foundation') {
    return [
      ...identity,
      object('networking.k8s.io/v1', 'NetworkPolicy', 'default-deny-ingress', { spec: { podSelector: {}, policyTypes: ['Ingress'] } }),
      network('sql-internal', 'arrowcol-sql', ['arrowcol', 'arrowcol-router', 'arrowcol-migrate', 'arrowcol-oat-pin-init'], 1433),
      pvc('arrowcol-sql', '32Gi'), pvc('arrowcol-catalog', '16Gi'),
      service('arrowcol-sql', 1433),
      deployment('arrowcol-sql', [{
        name: 'sqlserver', image: c.images.sqlserver,
        // The pinned sqlservr executable has cap_net_bind_service=ep; dropping it denies exec.
        securityContext: { ...security, capabilities: { drop: ['ALL'], add: ['NET_BIND_SERVICE'] } },
        env: [env('ACCEPT_EULA', 'Y'), env('MSSQL_PID', 'Developer'), env('MSSQL_MEMORY_LIMIT_MB', '2048'),
          env('MSSQL_SA_PASSWORD_FILE', '/run/sa/password')],
        ports: [{ containerPort: 1433 }], resources: resources('1', '3Gi', '2', '4Gi'),
        startupProbe: { ...execProbe(['/usr/local/bin/sqlserver-healthcheck']), failureThreshold: 60 },
        readinessProbe: execProbe(['/usr/local/bin/sqlserver-healthcheck']),
        volumeMounts: [mount('data', '/var/opt/mssql', false), mount('sa', '/run/sa')],
      }], [{ name: 'data', persistentVolumeClaim: { claimName: 'arrowcol-sql' } }, secretVolume('sa', 'arrowcol-sql-admin')]),
    ];
  }
  if (phase === 'bootstrap') {
    const template = pod('arrowcol-migrate', [{
      name: 'migrate', image: c.images.sqlserver, command: ['/usr/local/bin/arrowcol-migrate'],
      securityContext: security, resources: resources('250m', '256Mi', '1', '512Mi'),
      env: [env('SQLSERVER_HOST', 'arrowcol-sql'), env('ARROWCOL_MIGRATOR_PASSWORD_FILE', '/run/migration/password')],
      volumeMounts: [mount('migration', '/run/migration')],
    }], [secretVolume('sa', 'arrowcol-sql-admin'), secretVolume('app', 'arrowcol-sql-runtime'), secretVolume('migration', 'arrowcol-sql-migration')]);
    template.spec.restartPolicy = 'Never';
    template.spec.initContainers = [{
      name: 'bootstrap', image: c.images.sqlserver, command: ['/usr/local/bin/database-bootstrap'],
      securityContext: security, resources: resources('250m', '256Mi', '1', '512Mi'),
      env: [env('SQLSERVER_HOST', 'arrowcol-sql'), env('SQLSERVER_SA_PASSWORD_FILE', '/run/sa/password'),
        env('ARROWCOL_APP_PASSWORD_FILE', '/run/app/password'), env('ARROWCOL_MIGRATOR_PASSWORD_FILE', '/run/migration/password')],
      volumeMounts: [mount('sa', '/run/sa'), mount('app', '/run/app'), mount('migration', '/run/migration')],
    }];
    return [object('batch/v1', 'Job', 'arrowcol-migrate-v11', { spec: { backoffLimit: 0, activeDeadlineSeconds: 600, template } })];
  }
  const dataflow = parse(await readFile('projects/arrowcol-dataflow-rust/config.yaml', 'utf8'));
  // --num-cores owns the effective policy; no group/pipeline override may shadow it.
  delete dataflow.policies.resources;
  const router = parse(await readFile('projects/arrowcol-router-rust/config.docker.yaml', 'utf8'));
  router.sql_server.host = 'arrowcol-sql';
  router.sql_server.password_file = '/run/app/password';
  router.sql_server.trust_server_certificate = true;
  router.routing.distinct_table_owners = false;
  router.routing.warmup_ms = 1000;
  router.routing.lease_ttl_ms = 3600000;
  router.upstream.tls = false;
  router.upstream.sni = '';
  router.upstream.max_h2_streams = 1;
  router.upstream.idle_timeout_seconds = 0;
  router.auth.edge_secret_file = '/run/auth/edge-hmac';
  router.auth.internal_signing_secret_file = '/run/auth/internal-hmac';
  router.routing.oat_run_id = c.oatRunId;
  const cloudEnv = [
    env('ARROWCOL_DELTA_ROOT', c.deltaRoot),
    env('ARROWCOL_AZURE_MODE', 'stamped'), env('AZURE_SUBSCRIPTION_ID', c.subscriptionId),
    env('AZURE_STAMP', c.stamp), env('AZURE_RESOURCE_GROUP', 'arrow-collector-rg'),
    env('AZURE_STORAGE_ACCOUNT', `arrowcollector${c.stamp}`), env('AZURE_STORAGE_CHECKPOINT_CONTAINER', 'checkpoints'),
    secretEnv('ARROWCOL_RESET_GENERATION', 'arrowcol-generation', 'generation'),
  ];
  return [
    object('v1', 'ConfigMap', 'arrowcol-config', { data: { 'dataflow.yaml': stringify(dataflow), 'router.yaml': stringify(router) } }),
    service('arrowcol-catalog', 8817), service('arrowcol-router', 50050),
    deployment('arrowcol-catalog', [{
      name: 'catalog', image: c.images.catalog, securityContext: security,
      args: ['--warehouse-root', c.deltaRoot, '--state-dir', '/data/catalog', '--ingestion-layout'],
      env: cloudEnv, ports: [{ containerPort: 8817 }], resources: resources('500m', '1Gi', '2', '4Gi'),
      startupProbe: { ...execProbe(['/usr/local/bin/arrowcol-catalog-rust', '--healthcheck']), failureThreshold: 60 },
      readinessProbe: execProbe(['/usr/local/bin/arrowcol-catalog-rust', '--healthcheck']),
      volumeMounts: [mount('state', '/data', false)],
    }], [{ name: 'state', persistentVolumeClaim: { claimName: 'arrowcol-catalog' } }], true),
    deployment('arrowcol', [{
      name: 'dataflow', image: c.images.dataflow, securityContext: security,
      args: ['--config', '/etc/oat/dataflow.yaml', '--num-cores', '1'],
      env: [...cloudEnv, env('ARROWCOL_SQL_HOST', 'arrowcol-sql'), env('ARROWCOL_SQL_PASSWORD_FILE', '/run/app/password'),
        env('ARROWCOL_PARTITION_GRANULARITY', 'hour'),
        env('ARROWCOL_SQL_TRUST_CERT', 'true'), env('ARROWCOL_CATALOG_ENDPOINT', 'http://arrowcol-catalog:8817'),
        { name: 'ARROWCOL_INSTANCE_KEY', valueFrom: { fieldRef: { fieldPath: 'metadata.uid' } } },
        { name: 'POD_IP', valueFrom: { fieldRef: { fieldPath: 'status.podIP' } } },
        env('ARROWCOL_ADVERTISED_FLIGHT_ENDPOINT', '$(POD_IP):32010')],
      ports: [{ containerPort: 32010 }, { containerPort: 32011 }], resources: resources('2', '4Gi', '2', '6Gi'),
      startupProbe: { ...httpProbe('/readyz', 32011), failureThreshold: 120 },
      readinessProbe: httpProbe('/readyz', 32011), livenessProbe: httpProbe('/healthz', 32011),
      volumeMounts: [mount('config', '/etc/oat'), mount('app', '/run/app')],
    }], [{ name: 'config', configMap: { name: 'arrowcol-config' } }, secretVolume('app', 'arrowcol-sql-runtime')], true),
    deployment('arrowcol-router', [{
      name: 'router', image: c.images.router, securityContext: security,
      args: ['--config', '/etc/oat/router.yaml'],
      ports: [{ containerPort: 50050 }], resources: resources('500m', '512Mi', '2', '1Gi'),
      startupProbe: { ...httpProbe('/readyz', 50050), failureThreshold: 120 },
      readinessProbe: httpProbe('/readyz', 50050), livenessProbe: httpProbe('/healthz', 50050),
      volumeMounts: [mount('config', '/etc/oat'), mount('app', '/run/app'), mount('auth', '/run/auth')],
    }], [{ name: 'config', configMap: { name: 'arrowcol-config' } }, secretVolume('app', 'arrowcol-sql-runtime'), secretVolume('auth', 'arrowcol-router-auth')]),
    network('catalog-internal', 'arrowcol-catalog', ['arrowcol'], 8817),
    network('flight-router-only', 'arrowcol', ['arrowcol-router'], 32010),
    network('router-oat-client-ingress', 'arrowcol-router', ['arrowcol-oat-client'], 50050),
    // Operator port-forward is the only ingress to the router; no LoadBalancer, Ingress or NodePort.
  ];
}

export async function prepareSecrets(directory: string, bootstrapEnv: string): Promise<void> {
  const values = (await readFile(bootstrapEnv, 'utf8')).split(/\r?\n/).filter((line) => /^(?:export )?ARROWCOL_RESET_GENERATION=/.test(line));
  if (values.length !== 1) throw new Error('bootstrap env must contain exactly one captured ARROWCOL_RESET_GENERATION');
  const generation = values[0].slice(values[0].indexOf('=') + 1).replace(/^(['"])(.*)\1$/, '$2');
  if (!uuid.test(generation)) throw new Error('bootstrap generation is not a UUID; never synthesize one');
  await mkdir(directory, { recursive: true, mode: 0o700 });
  if ((await stat(directory)).mode & 0o077) throw new Error('secret directory must have mode 0700');
  for (const key of ['sa-password', 'app-password', 'migration-password', 'edge-hmac', 'internal-hmac', 'generation']) {
    const path = resolve(directory, key);
    const value = key === 'generation' ? generation : `Aa1!${randomBytes(32).toString('hex')}`;
    try { await writeFile(path, value, { flag: 'wx', mode: 0o600 }); }
    catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'EEXIST') throw error;
      const existing = await readFile(path, 'utf8');
      if ((await stat(path)).mode & 0o077 || !existing || (key === 'generation' && existing !== generation)) {
        throw new Error(`unsafe existing ${key}; do not rotate secrets or publisher generation during a run`);
      }
    }
  }
}

export async function writePrivateFile(filename: string, content: string): Promise<void> {
  await mkdir(dirname(filename), { recursive: true, mode: 0o700 });
  try {
    const info = await lstat(filename);
    if (!info.isFile() || info.mode & 0o077) throw new Error('Refusing to overwrite an unsafe private input file.');
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
  }
  const pending = `${filename}.${randomUUID()}.pending`;
  try {
    await writeFile(pending, content, { flag: 'wx', mode: 0o600 });
    await rename(pending, filename);
  } finally { await rm(pending, { force: true }); }
}

export function renderBootstrapEnv(values: Record<string, string>, generation: string): string {
  if (!uuid.test(generation)) throw new Error('bootstrap generation is not a UUID; never synthesize one');
  const entries = { ...values, ARROWCOL_RESET_GENERATION: generation };
  for (const [key, value] of Object.entries(entries)) {
    if (!/^[A-Z][A-Z0-9_]*$/.test(key) || /[\r\n\0]/.test(value)) throw new Error('Invalid bootstrap environment entry.');
  }
  return Object.entries(entries).map(([key, value]) => `${key}=${value}\n`).join('');
}

export interface PrivateInputs {
  directory: string;
  runtime: RuntimeConfig;
  bootstrapValues: Record<string, string>;
  resetGeneration: string;
  tenantId: string;
  assertion: string;
  clientImage: string;
  qapEndpoint: string;
  oatTemplate: Record<string, unknown>;
}

export async function generatePrivateInputs(input: PrivateInputs): Promise<{
  runtimeConfigPath: string; bootstrapEnvPath: string; aksConfigPath: string;
}> {
  const runtime = validateRuntime(input.runtime);
  if (Object.values(runtime.images).some(image => !image.startsWith(`arrowcollector${runtime.stamp}.azurecr.io/`))) {
    throw new Error('All runtime images must use the selected stamp registry.');
  }
  if (!uuid.test(input.tenantId)) throw new Error('Azure tenant must be a UUID.');
  if (!/^[a-z0-9]+\.azurecr\.io\/arrowcol-oat-client@sha256:[a-f0-9]{64}$/.test(input.clientImage) ||
      !input.clientImage.startsWith(`arrowcollector${runtime.stamp}.azurecr.io/`)) {
    throw new Error('DMV client must be pinned by manifest digest in the selected stamp registry.');
  }
  if (!/^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+$/.test(input.assertion) || input.assertion.length > 16_384) {
    throw new Error('Kubernetes returned an invalid host federation assertion.');
  }
  let claims: { exp?: unknown };
  try { claims = JSON.parse(Buffer.from(input.assertion.split('.')[1]!, 'base64url').toString('utf8')); }
  catch { throw new Error('Kubernetes returned invalid host assertion claims.'); }
  if (!claims || typeof claims.exp !== 'number' || claims.exp <= Date.now() / 1000 + 60) {
    throw new Error('Host assertion is expired or expires within one minute; run `npx --no-install arrowcol-oat aks setup`.');
  }
  const platform = input.oatTemplate.platform;
  if (!platform || typeof platform !== 'object' || Array.isArray(platform) ||
      (platform as Record<string, unknown>).kind !== 'aks') throw new Error('Expected the committed AKS OAT template.');
  const endpoint = new URL(input.qapEndpoint);
  if (endpoint.protocol !== 'https:' || !endpoint.hostname.startsWith(`arrow-collector-${runtime.stamp}.`) ||
      !endpoint.hostname.endsWith('.kusto.windows.net') || endpoint.username || endpoint.password ||
      endpoint.search || endpoint.hash || endpoint.port || endpoint.pathname !== '/') throw new Error('QAP endpoint must match the selected stamp.');
  const directory = resolve(input.directory);
  await mkdir(directory, { recursive: true, mode: 0o700 });
  const directoryInfo = await lstat(directory);
  if (!directoryInfo.isDirectory()) throw new Error('Private input directory must be a real directory.');
  await chmod(directory, 0o700);
  const runtimeConfigPath = resolve(directory, 'runtime.json');
  try {
    const existing = JSON.parse(await readFile(runtimeConfigPath, 'utf8')) as RuntimeConfig;
    if (existing.stamp !== runtime.stamp || existing.subscriptionId !== runtime.subscriptionId) {
      throw new Error('Stale AKS private inputs belong to another stamp; run `npx --no-install arrowcol-oat aks destroy --confirm arrow-collector-rg` before deploying.');
    }
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
  }
  const bootstrapEnvPath = resolve(directory, 'bootstrap.env');
  const aksConfigPath = resolve(directory, 'oat-config.aks.json');
  await writePrivateFile(bootstrapEnvPath, renderBootstrapEnv(input.bootstrapValues, input.resetGeneration));
  const secrets = resolve(directory, 'secrets');
  try {
    const info = await lstat(secrets);
    if (!info.isDirectory() || info.mode & 0o077) throw new Error('Secret directory must be a real directory with mode 0700.');
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
  }
  for (const key of ['sa-password', 'app-password', 'migration-password', 'edge-hmac', 'internal-hmac', 'generation']) {
    try {
      const info = await lstat(resolve(secrets, key));
      if (!info.isFile() || info.mode & 0o077) throw new Error('Secret files must be regular files with mode 0600.');
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
    }
  }
  await prepareSecrets(secrets, bootstrapEnvPath);
  const assertionFile = resolve(secrets, 'publisher-assertion.jwt');
  await writePrivateFile(assertionFile, `${input.assertion}\n`);
  await writePrivateFile(resolve(directory, 'aks.env'), renderBootstrapEnv({
    ...input.bootstrapValues, ARROWCOL_MODE: 'azure', AZURE_CLIENT_ID: runtime.workloadIdentityClientId,
    AZURE_TENANT_ID: input.tenantId, AZURE_FEDERATED_TOKEN_FILE: assertionFile,
  }, input.resetGeneration));
  await writePrivateFile(runtimeConfigPath, `${JSON.stringify(runtime, null, 2)}\n`);
  const account = `arrowcollector${runtime.stamp}`;
  await writePrivateFile(aksConfigPath, `${JSON.stringify({
    ...input.oatTemplate,
    platform: { ...platform, runtime_config_file: runtimeConfigPath, bootstrap_env_file: bootstrapEnvPath },
    stamp: runtime.stamp, kubectl_context: `arrow-collector-${runtime.stamp}`,
    qap_endpoint: input.qapEndpoint, qap_database: 'arrowcol', dmv_client_image: input.clientImage,
    result_url: `abfss://experiments@${account}.dfs.core.windows.net/arrowcol-oat/results.parquet`,
    website_url: `abfss://$web@${account}.dfs.core.windows.net/results.parquet`,
  }, null, 2)}\n`);
  return { runtimeConfigPath, bootstrapEnvPath, aksConfigPath };
}

async function main(args: string[]): Promise<void> {
  if (args[0] === 'prepare-secrets' && args.length === 3) {
    await prepareSecrets(args[2], args[1]);
    console.log('Private runtime secret files ready; no cluster changes made.');
  } else if (args[0] === 'render' && args.length === 4) {
    const items = await renderRuntime(JSON.parse(await readFile(args[1], 'utf8')), args[2] as Phase);
    await writeFile(args[3], JSON.stringify({ apiVersion: 'v1', kind: 'List', items }, null, 2));
    console.log(`Rendered ${items.length} resources; no cluster changes made.`);
  } else {
    throw new Error('Usage: runtime.ts render <runtime.json> <identity|foundation|bootstrap|workload> <output.json> | prepare-secrets <supported-bootstrap.env> <private-directory>');
  }
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main(process.argv.slice(2)).catch((error: unknown) => { console.error(error instanceof Error ? error.message : 'Runtime preparation failed'); process.exitCode = 1; });
}
