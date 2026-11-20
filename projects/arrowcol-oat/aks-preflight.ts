import { execFileSync } from 'node:child_process';
import { lstat, readFile } from 'node:fs/promises';
import { isAbsolute, relative, resolve, sep } from 'node:path';
import { loadStampConfig } from '../../infra/azure/stamp.js';
import { validateRuntime, type RuntimeConfig } from './runtime.js';

export interface PreflightCheck {
  name: string;
  ready: boolean;
  detail: string;
  next: string;
}

export interface PreflightReport {
  ready: boolean;
  checks: PreflightCheck[];
  nextCommand: string;
  warnings: string[];
  network: { provider: string; enforcing: boolean };
}

interface CommandResult {
  stdout: string;
  exitCode: number;
}

interface PreflightOptions {
  workspaceRoot?: string;
  stampConfigPath?: string;
  environment?: NodeJS.ProcessEnv;
  runCommand?: (command: string, args: string[], timeoutMs?: number) => CommandResult;
  fetcher?: typeof fetch;
  now?: () => number;
}

type JsonRecord = Record<string, unknown>;
const setupCommand = 'npx --no-install arrowcol-oat aks setup';
const publisherNamespace = 'arrow-collector';
const publisherServiceAccount = 'arrow-collector';
const resourceGroup = 'arrow-collector-rg';
const identityName = 'arrow-collector';
const uuid = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function isRecord(value: unknown): value is JsonRecord {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}
function text(value: unknown): string | undefined {
  return typeof value === 'string' && value.trim() ? value : undefined;
}
function at(value: unknown, key: string): string | undefined {
  return isRecord(value) ? text(value[key]) : undefined;
}
function records(value: unknown): JsonRecord[] {
  return Array.isArray(value) ? value.filter(isRecord) : [];
}
function publisherCredentialMatches(actual: JsonRecord, expected: { name: string; issuer: string; subject: string }): boolean {
  const audiences = Array.isArray(actual.audiences) ? [...actual.audiences].sort() : [];
  return actual.name === expected.name && actual.issuer === expected.issuer && actual.subject === expected.subject &&
    JSON.stringify(audiences) === JSON.stringify(['api://AzureADTokenExchange']);
}
function condition(value: unknown, name: string): boolean {
  return records(value).some((item) => item.type === name && item.status === 'True');
}
function object(value: unknown, label: string): JsonRecord {
  if (!isRecord(value)) throw new Error(`${label} must be an object.`);
  return value;
}
function requireValue(value: string | undefined, label: string): string {
  if (!value) throw new Error(`${label} is missing; run \`${setupCommand}\`.`);
  return value;
}

async function privateText(filename: string): Promise<string> {
  const info = await lstat(filename);
  if (!info.isFile() || info.isSymbolicLink() || (info.mode & 0o777) !== 0o600 || info.size > 1024 * 1024) {
    throw new Error('Generated AKS state must be a bounded 0600 regular file, not a symlink.');
  }
  return readFile(filename, 'utf8');
}

async function publisherAssertion(filename: string, now: () => number): Promise<string> {
  const candidate = (await privateText(filename)).trim();
  const parts = candidate.split('.');
  if (parts.length !== 3) throw new Error('Malformed assertion');
  const claims: unknown = JSON.parse(Buffer.from(parts[1]!, 'base64url').toString('utf8'));
  const timestamp = now();
  if (!isRecord(claims) || typeof claims.exp !== 'number' || !Number.isFinite(claims.exp) || claims.exp * 1000 <= timestamp ||
      (claims.nbf !== undefined && (typeof claims.nbf !== 'number' || !Number.isFinite(claims.nbf) || claims.nbf * 1000 > timestamp))) {
    throw new Error('Expired or not-yet-valid assertion');
  }
  return candidate;
}

export async function loadAksEnvironment(
  workspaceRoot: string,
  base: NodeJS.ProcessEnv = process.env,
): Promise<NodeJS.ProcessEnv> {
  let contents: string;
  try {
    contents = await privateText(resolve(workspaceRoot, 'out/arrowcol-oat/aks.env'));
  } catch (error) {
    if (isRecord(error) && error.code === 'ENOENT') return { ...base };
    throw new Error(`Could not load private AKS environment; run \`${setupCommand}\`.`, { cause: error });
  }
  const values: NodeJS.ProcessEnv = {};
  for (const raw of contents.split(/\r?\n/)) {
    const line = raw.trim();
    if (!line || line.startsWith('#')) continue;
    const match = /^(?:export )?([A-Z][A-Z0-9_]*)=(.*)$/.exec(line);
    if (!match || Object.hasOwn(values, match[1]!)) {
      throw new Error(`Invalid or duplicate AKS environment assignment; run \`${setupCommand}\`.`);
    }
    let value = match[2]!;
    if (value.startsWith('"') || value.startsWith("'")) {
      if (value.length < 2 || value.at(-1) !== value[0]) throw new Error('Unclosed AKS environment quote.');
      value = value.slice(1, -1);
    }
    values[match[1]!] = value;
  }
  return { ...values, ...Object.fromEntries(Object.entries(base).filter(([, value]) => value !== undefined)) };
}

function matchesAction(pattern: string, action: string): boolean {
  const expression = pattern.split('*').map((part) => part.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')).join('.*');
  return new RegExp(`^${expression}$`, 'i').test(action);
}

export function permitsAction(permissions: unknown, action: string): boolean {
  return records(permissions).some((permission) => {
    const actions = Array.isArray(permission.actions) ? permission.actions : [];
    const denied = Array.isArray(permission.notActions) ? permission.notActions : [];
    return actions.some((entry) => typeof entry === 'string' && matchesAction(entry, action)) &&
      !denied.some((entry) => typeof entry === 'string' && matchesAction(entry, action));
  });
}

export async function runAksPreflight(configPath: string, options: PreflightOptions = {}): Promise<PreflightReport> {
  const root = resolve(options.workspaceRoot ?? process.cwd());
  const environment = options.environment ?? process.env;
  const fetcher = options.fetcher ?? fetch;
  const now = options.now ?? Date.now;
  const runCommand = options.runCommand ?? ((command: string, args: string[], timeoutMs = 30_000): CommandResult => {
    try {
      return { stdout: execFileSync(command, args, {
        encoding: 'utf8', timeout: timeoutMs, maxBuffer: 8 * 1024 * 1024, env: environment, stdio: ['ignore', 'pipe', 'pipe'],
      }), exitCode: 0 };
    } catch (error) {
      return { stdout: '', exitCode: isRecord(error) && typeof error.status === 'number' ? error.status : 1 };
    }
  });
  const checks: PreflightCheck[] = [];
  const warnings: string[] = [];
  const network = { provider: 'unknown', enforcing: false };
  const nextCommand = 'npx --no-install arrowcol-oat --mode aks --preflight-only';
  const output = (command: string, args: string[], timeoutMs?: number): string => {
    const result = runCommand(command, args, timeoutMs);
    if (result.exitCode !== 0) throw new Error(`${command} exited with status ${result.exitCode}; command output withheld.`);
    return result.stdout.trim();
  };
  const json = (command: string, args: string[], timeoutMs?: number): unknown => {
    try { return JSON.parse(output(command, args, timeoutMs)); }
    catch { throw new Error(`${command} returned an unsuccessful or invalid JSON response; output withheld.`); }
  };
  const check = async (name: string, work: () => Promise<string> | string, next = `Run \`${setupCommand}\`, then retry.`) => {
    try { checks.push({ name, ready: true, detail: await work(), next }); }
    catch (error) {
      checks.push({ name, ready: false, detail: error instanceof Error ? error.message : 'Read-only check failed.', next });
    }
  };

  let config: JsonRecord = {};
  let platform: JsonRecord = {};
  let configError: string | undefined;
  try {
    config = object(JSON.parse(await readFile(resolve(root, configPath), 'utf8')), 'OAT config');
    platform = object(config.platform, 'OAT platform');
  } catch { configError = `AKS config is missing or invalid; run \`${setupCommand}\`.`; }
  const stamp = text(config.stamp);
  const context = text(config.kubectl_context) ?? (stamp ? `arrow-collector-${stamp}` : undefined);
  let runtime: RuntimeConfig | undefined;
  let runtimeError: string | undefined;
  let bootstrap: string | undefined;
  try {
    const filename = resolve(root, requireValue(text(platform.runtime_config_file), 'runtime_config_file'));
    runtime = validateRuntime(JSON.parse(await privateText(filename)));
  } catch { runtimeError = `Private runtime.json is missing or invalid; run \`${setupCommand}\`.`; }
  try {
    bootstrap = await privateText(resolve(root, requireValue(text(platform.bootstrap_env_file), 'bootstrap_env_file')));
  } catch { /* The independent runtime gate reports unavailable private bootstrap state. */ }
  let selectedStamp: ReturnType<typeof loadStampConfig> | undefined;
  try {
    const stampConfigFile = options.stampConfigPath ?? 'infra/azure/stamp.yaml';
    const stampConfigPath = resolve(root, stampConfigFile);
    const relativeStampConfigPath = relative(root, stampConfigPath);
    if (isAbsolute(stampConfigFile) || relativeStampConfigPath === '..' ||
        relativeStampConfigPath.split(sep)[0] === '..' || isAbsolute(relativeStampConfigPath)) {
      throw new Error('Stamp config file must be within the workspace.');
    }
    selectedStamp = loadStampConfig(stampConfigPath);
  } catch { /* Gate 1 reports it. */ }
  const subscription = runtime?.subscriptionId ?? environment.AZURE_SUBSCRIPTION_ID ?? selectedStamp?.subscription;
  const clientId = runtime?.workloadIdentityClientId ?? environment.AZURE_CLIENT_ID;
  const clusterName = stamp && /^\d{5}$/.test(stamp) ? `arrow-collector-${stamp}` : undefined;
  const kubeArgs = (args: string[]) => ['--context', requireValue(context, 'kubectl_context'), '--request-timeout=30s', ...args];
  const kube = (args: string[]) => object(json('kubectl', kubeArgs(args)), 'Kubernetes response');
  const azArgs = (args: string[]) => [...args, '--subscription', requireValue(subscription, 'subscription'), '-o', 'json'];
  const az = (args: string[]) => object(json('az', azArgs(args)), 'Azure response');
  let aks: JsonRecord | undefined;
  let apiServer: string | undefined;
  let issuer: string | undefined;
  let principalId: string | undefined;
  let identityId: string | undefined;
  let assertion: string | undefined;
  let assertionPath: string | undefined;
  const tokens = new Map<string, string>();
  const federatedToken = async (resource: string): Promise<string> => {
    const cached = tokens.get(resource);
    if (cached) return cached;
    if (!assertion) throw new Error(`A valid private federated assertion is required; run \`${setupCommand}\`.`);
    const tenant = requireValue(environment.AZURE_TENANT_ID, 'AZURE_TENANT_ID');
    if (!uuid.test(tenant) || !clientId || !uuid.test(clientId)) throw new Error('Invalid federation tenant or client identity.');
    const response = await fetcher(`https://login.microsoftonline.com/${tenant}/oauth2/v2.0/token`, {
      method: 'POST', headers: { 'content-type': 'application/x-www-form-urlencoded' },
      body: new URLSearchParams({
        client_id: clientId, grant_type: 'client_credentials', scope: `${resource.replace(/\/$/, '')}/.default`,
        client_assertion_type: 'urn:ietf:params:oauth:client-assertion-type:jwt-bearer', client_assertion: assertion,
      }),
      signal: AbortSignal.timeout(30_000),
    });
    if (!response.ok) throw new Error(`Federated token exchange failed (HTTP ${response.status}); response withheld.`);
    const body: unknown = await response.json();
    const token = at(body, 'access_token');
    if (!token) throw new Error('Federated token exchange returned no access token; response withheld.');
    tokens.set(resource, token);
    return token;
  };
  const request = async (url: string, resource: string, init?: RequestInit): Promise<unknown> => {
    const token = await federatedToken(resource);
    const response = await fetcher(url, {
      ...init, headers: { ...init?.headers, authorization: `Bearer ${token}` }, signal: AbortSignal.timeout(30_000),
    });
    if (!response.ok) throw new Error(`Read-only provider request failed (HTTP ${response.status}); response withheld.`);
    return response.json();
  };

  await check('Configuration, stamp, and destinations', () => {
    if (configError) throw new Error(configError);
    if (platform.kind !== 'aks' || !stamp || !/^\d{5}$/.test(stamp)) throw new Error(`AKS config needs a five-digit stamp; run \`${setupCommand}\`.`);
    if (!selectedStamp || selectedStamp.stamp !== stamp || selectedStamp.subscription !== subscription) {
      throw new Error(`Private stamp YAML and AKS configuration must select the same stamp/subscription; run \`${setupCommand}\`.`);
    }
    const results = text(config.result_url), website = text(config.website_url);
    if (!results?.startsWith(`abfss://experiments@arrowcollector${stamp}.dfs.core.windows.net/`) ||
        !website?.startsWith(`abfss://$web@arrowcollector${stamp}.dfs.core.windows.net/`) || results === website) {
      throw new Error('AKS destinations must use separate selected-stamp experiments and root $web paths.');
    }
    const validation = runCommand('cargo', ['run', '--locked', '--package', 'arrowcol-oat-rust', '--', '--validate-config', resolve(root, configPath)], 300_000);
    if (validation.exitCode !== 0) throw new Error(`Rust configuration validation failed; run \`${setupCommand}\`.`);
    return 'Selected five-digit stamp, private stamp YAML and separate result/website destinations match.';
  });

  await check('Kubernetes context and credentials', async () => {
    const current = output('kubectl', ['config', 'current-context']);
    const local = object(json('kubectl', ['config', 'view', '--minify', '--context', requireValue(context, 'context'), '-o', 'json']), 'Kubeconfig');
    apiServer = at(records(local.clusters)[0]?.cluster, 'server');
    const user = object(records(local.users)[0]?.user, 'Kubeconfig user');
    const exec = object(user.exec, 'Kubeconfig exec');
    const args = Array.isArray(exec.args) ? exec.args : [];
    output('kubelogin', ['--version']);
    output('kubectl', kubeArgs(['get', '--raw=/version']));
    const account = az(['account', 'show']);
    aks = az(['aks', 'show', '-g', resourceGroup, '-n', requireValue(clusterName, 'cluster name')]);
    issuer = at(aks.oidcIssuerProfile, 'issuerUrl');
    if (current !== context || exec.command !== 'kubelogin' || !args.includes('--disable-environment-override') ||
        args[args.indexOf('--subscription') + 1] !== subscription ||
        !apiServer || new URL(apiServer).protocol !== 'https:' ||
        new URL(apiServer).hostname.toLowerCase() !== at(aks, 'fqdn')?.toLowerCase() ||
        account.id !== subscription || account.tenantId !== environment.AZURE_TENANT_ID ||
        environment.ARROWCOL_MODE !== 'azure' || environment.AZURE_CLIENT_ID !== clientId ||
        (environment.AZURE_SUBSCRIPTION_ID !== undefined && environment.AZURE_SUBSCRIPTION_ID !== subscription)) {
      throw new Error(`Kubeconfig, live cluster, selected subscription or publisher environment mismatch; run \`${setupCommand}\`.`);
    }
    const path = requireValue(environment.AZURE_FEDERATED_TOKEN_FILE, 'AZURE_FEDERATED_TOKEN_FILE');
    if (!isAbsolute(path)) throw new Error('Federated assertion path must be absolute.');
    let candidate: string;
    try {
      candidate = await publisherAssertion(path, now);
    } catch { throw new Error(`Federated assertion is invalid or expired; run \`${setupCommand}\`.`); }
    assertion = candidate;
    assertionPath = path;
    return 'Selected live API endpoint, bounded kube authentication and private unexpired publisher assertion verified.';
  });

  await check('Fixed publisher identity', () => {
    const ns = kube(['get', 'ns', publisherNamespace, '-o', 'json']);
    const sa = kube(['-n', publisherNamespace, 'get', 'sa', publisherServiceAccount, '-o', 'json']);
    const metadata = object(sa.metadata, 'ServiceAccount metadata');
    if (at(ns.status, 'phase') !== 'Active' || (isRecord(ns.metadata) && ns.metadata.deletionTimestamp) ||
        !clientId || at(metadata.annotations, 'azure.workload.identity/client-id') !== clientId) {
      throw new Error('Fixed host publisher namespace/ServiceAccount is absent, terminating or annotated for a different identity.');
    }
    return 'Fixed host publisher identity matches; benchmark namespaces and workload identities remain runner-owned.';
  });

  await check('Federated credential', () => {
    if (!issuer) throw new Error('Selected live cluster OIDC issuer is unavailable.');
    const identity = az(['identity', 'show', '-g', resourceGroup, '-n', identityName]);
    principalId = at(identity, 'principalId');
    identityId = at(identity, 'id');
    const credentials = records(json('az', azArgs(['identity', 'federated-credential', 'list', '-g', resourceGroup, '--identity-name', identityName])));
    const nativeCredentials = clusterName
      ? credentials.filter((credential) => credential.name === `azure-qap-${clusterName}`)
      : [];
    const nativeCredential = nativeCredentials.length === 1 ? nativeCredentials[0] : undefined;
    if (!principalId || !identityId || identity.clientId !== clientId || !nativeCredential ||
      nativeCredential.issuer !== issuer || nativeCredential.subject !== `system:serviceaccount:${publisherNamespace}:${publisherServiceAccount}` ||
      !Array.isArray(nativeCredential.audiences) || nativeCredential.audiences.length !== 1 ||
      nativeCredential.audiences[0] !== 'api://AzureADTokenExchange') {
      throw new Error('Required AKS-native federation name and trust tuple do not exactly match the live cluster and selected UAMI.');
    }
    if (!selectedStamp || !selectedStamp.publisherFederatedCredentials.every((expected) =>
      credentials.some((credential) => publisherCredentialMatches(credential, expected)))) {
      throw new Error('Configured stamp publisher federation does not exactly match the selected stamp configuration and UAMI.');
    }
    return 'Exact fixed publisher federation and all configured stamp publisher federation verified.';
  });

  await check('Run-scoped runtime and bootstrap inputs', () => {
    if (!runtime) throw new Error(runtimeError);
    if (runtime.stamp !== stamp || runtime.subscriptionId !== subscription || runtime.workloadIdentityClientId !== clientId) {
      throw new Error('Runtime configuration identity does not match the selected stamp.');
    }
    const generations = (bootstrap ?? '').split(/\r?\n/).filter((line) => /^(?:export )?ARROWCOL_RESET_GENERATION=/.test(line));
    const generation = generations[0]?.slice(generations[0].indexOf('=') + 1).replace(/^(['"])(.*)\1$/, '$2');
    if (generations.length !== 1 || !generation || !uuid.test(generation) || generation !== environment.ARROWCOL_RESET_GENERATION) {
      throw new Error('Private bootstrap generation is missing, invalid or differs from the merged AKS environment.');
    }
    return 'Private runtime and captured bootstrap generation are valid; per-run secrets/workloads will be created by the runner.';
  });

  await check('Namespace permissions and planned pool capacity', () => {
    for (const verb of ['create', 'delete']) {
      if (output('kubectl', kubeArgs(['auth', 'can-i', verb, 'namespaces'])) !== 'yes') throw new Error(`Operator cannot ${verb} run namespaces.`);
    }
    const live = aks ?? az(['aks', 'show', '-g', resourceGroup, '-n', requireValue(clusterName, 'cluster name')]);
    const pools = records(live.agentPoolProfiles);
    for (const role of ['arrowcol', 'client']) {
      const eligible = pools.filter((pool) => at(pool.nodeLabels, 'arrow-collector/role') === role &&
        pool.osType === 'Linux' && pool.provisioningState === 'Succeeded' &&
        ((typeof pool.count === 'number' && pool.count > 0) ||
         (pool.enableAutoScaling === true && typeof pool.maxCount === 'number' && pool.maxCount > 0)));
      if (!eligible.length) throw new Error(`No live ${role} pool has capacity or autoscaler headroom.`);
      const nodes = records(kube(['get', 'nodes', '-l', `arrow-collector/role=${role}`, '-o', 'json']).items);
      if (nodes.length && !nodes.some((node) =>
        condition(isRecord(node.status) ? node.status.conditions : undefined, 'Ready') &&
        !(isRecord(node.spec) && node.spec.unschedulable))) throw new Error(`Existing ${role} nodes are not schedulable/Ready.`);
    }
    const namespaceList = kube(['get', 'ns', '-o', 'json']);
    if (!Array.isArray(namespaceList.items)) throw new Error('Namespace inventory is not a Kubernetes list.');
    const namespaces = records(namespaceList.items)
      .map((item) => at(item.metadata, 'name')).filter((name) => name && /^arrow-collector-[a-f0-9]{32}$/.test(name));
    if (namespaces.length) warnings.push(`${namespaces.length} run-scoped namespace(s) remain; investigate possible orphan runs before concurrent execution. No cleanup was attempted.`);
    return 'Operator can create/delete namespaces; role pools have current capacity or scale-from-zero headroom. No scaling was attempted.';
  });

  await check('Immutable ACR images', () => {
    if (!runtime) throw new Error(runtimeError);
    const images = [...Object.values(runtime.images), config.dmv_client_image];
    for (const image of images) {
      const match = typeof image === 'string' && new RegExp(`^arrowcollector${stamp}\\.azurecr\\.io/([a-z0-9][a-z0-9/_.-]*)@(sha256:[a-f0-9]{64})$`).exec(image);
      if (!match) throw new Error('All five images, including the DMV client, must be immutable digests in the selected stamp registry.');
      const found = az(['acr', 'repository', 'show', '-n', `arrowcollector${stamp}`, '--image', `${match[1]}@${match[2]}`]);
      if (found.digest !== match[2]) throw new Error('Registry manifest digest does not match the selected immutable image.');
    }
    return 'All four runtime images and the fifth DMV client digest exist in the selected registry.';
  });

  await check('Metrics API availability', () => {
    const api = kube(['get', 'apiservice', 'v1beta1.metrics.k8s.io', '-o', 'json']);
    if (!condition(isRecord(api.status) ? api.status.conditions : undefined, 'Available')) throw new Error('metrics.k8s.io APIService is not Available=True.');
    return 'Metrics APIService is Available; per-run pod coverage is checked during the benchmark, not before pods exist.';
  });

  await check('Selected QAP endpoint/database and binding permission', async () => {
    const database = text(config.qap_database);
    if (!database || database !== environment.AZURE_KUSTO_EXPERIMENT_DATABASE) {
      throw new Error(`qap_database must be set and equal the generated experiment database; run \`${setupCommand}\`.`);
    }
    const endpoint = new URL(requireValue(text(config.qap_endpoint), 'qap_endpoint'));
    if (endpoint.protocol !== 'https:' || endpoint.hostname !== `${clusterName}.${selectedStamp?.location}.kusto.windows.net` ||
        endpoint.username || endpoint.password || endpoint.search || endpoint.hash || endpoint.pathname !== '/') {
      throw new Error('QAP endpoint must match the exact selected stamp and region.');
    }
    output('getent', ['ahosts', endpoint.hostname]);
    const query = await request(new URL('/v1/rest/query', endpoint).href, 'https://kusto.kusto.windows.net', {
      method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ db: database, csl: 'print OatPreflight = 1' }),
    });
    if (!isRecord(query) || !Array.isArray(query.Tables)) throw new Error('QAP query returned an invalid response.');
    const principals = await request(new URL('/v1/rest/mgmt', endpoint).href, 'https://kusto.kusto.windows.net', {
      method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify({ db: database, csl: '.show database principals' }),
    });
    const tables = isRecord(principals) ? records(principals.Tables) : [];
    const admin = tables.some((table) => {
      const columns = records(table.Columns).map((column) => column.ColumnName);
      const rows = Array.isArray(table.Rows) ? table.Rows : [];
      return rows.some((row) => {
        if (!Array.isArray(row)) return false;
        const values = Object.fromEntries(columns.map((name, index) => [String(name), row[index]]));
        return typeof values.Role === 'string' && values.Role.toLowerCase() === `database ${database} admin`.toLowerCase() &&
          typeof values.PrincipalObjectId === 'string' && Boolean(principalId) &&
          values.PrincipalObjectId.toLowerCase() === principalId!.toLowerCase();
      });
    });
    if (!admin) throw new Error('Selected workload UAMI lacks verified database Admin permission for per-run external-table bindings.');
    return 'Selected experiment database is queryable through the workload UAMI and has per-run binding permission. This is not a data-flow verdict.';
  });

  await check('Network-policy enforcement proof', async () => {
    const live = aks ?? az(['aks', 'show', '-g', resourceGroup, '-n', requireValue(clusterName, 'cluster name')]);
    const profile = object(live.networkProfile, 'Live networkProfile');
    const dataplane = at(profile, 'networkDataplane')?.toLowerCase();
    const policy = at(profile, 'networkPolicy')?.toLowerCase();
    if (dataplane !== 'azure' && dataplane !== 'cilium') throw new Error('Live cluster network dataplane is missing or unsupported.');
    network.provider = dataplane === 'cilium' || policy === 'cilium' ? 'cilium' : policy && policy !== 'none' ? policy : 'none';
    network.enforcing = network.provider !== 'none';
    if (!network.enforcing) {
      warnings.push("No network-policy provider enforces the router-only fence: another workload inside the cluster could reach the dataflow engine's passwordless port. There is no public ingress.");
      return 'Live cluster has no enforcing provider; explicit risk warning applies. Run-scoped policies are rendered by the runner.';
    }
    if (network.provider !== 'cilium') throw new Error('The live enforcing provider is not qualified by this preflight.');
    const cilium = kube(['-n', 'kube-system', 'get', 'ds', 'cilium', '-o', 'json']);
    const status = object(cilium.status, 'Cilium status');
    if (typeof status.desiredNumberScheduled !== 'number' || status.desiredNumberScheduled < 1 ||
        status.numberReady !== status.desiredNumberScheduled) throw new Error('Live Cilium agents are not all ready.');
    const proofPath = requireValue(environment.ARROWCOL_NETWORK_POLICY_RECEIPT, 'ARROWCOL_NETWORK_POLICY_RECEIPT');
    let proof: JsonRecord;
    try { proof = object(JSON.parse(await readFile(proofPath, 'utf8')), 'Network proof'); }
    catch { throw new Error('Network-policy proof receipt is missing or invalid.'); }
    const recorded = typeof proof.recordedAt === 'string' ? Date.parse(proof.recordedAt) : NaN;
    if (!apiServer || proof.clusterServer !== apiServer || proof.ownerApproved !== true ||
        proof.positivePath !== true || proof.negativePath !== true || !Number.isFinite(recorded) ||
        recorded > now() || now() - recorded > 30 * 24 * 60 * 60_000) {
      throw new Error('A current exact-cluster owner-approved positive/negative policy proof is required.');
    }
    return 'Live Cilium agents and exact-cluster behavioral proof verified; policies follow the per-run lifecycle.';
  }, 'Obtain an owner-approved exact-cluster behavioral proof receipt; preflight never creates traffic/policies.');

  await check('Storage and website publication', () => {
    const accountName = `arrowcollector${stamp}`;
    const account = az(['storage', 'account', 'show', '-n', accountName]);
    const accountId = requireValue(at(account, 'id'), 'storage account ID');
    for (const container of ['experiments', '$web']) {
      az(['storage', 'container', 'show', '-n', container, '--account-name', accountName, '--auth-mode', 'login']);
      output('az', azArgs(['storage', 'blob', 'list', '--container-name', container, '--account-name', accountName, '--auth-mode', 'login', '--num-results', '1']));
      const scope = `${accountId}/blobServices/default/containers/${container}`;
      const assignments = records(json('az', azArgs(['role', 'assignment', 'list', '--scope', scope, '--include-inherited'])));
      if (!principalId || !assignments.some((assignment) => assignment.principalId === principalId &&
          assignment.roleDefinitionName === 'Storage Blob Data Contributor' && typeof assignment.scope === 'string' &&
          (scope.toLowerCase() === assignment.scope.toLowerCase() || scope.toLowerCase().startsWith(`${assignment.scope.toLowerCase()}/`)))) {
        throw new Error('Producer UAMI storage write-role evidence is missing.');
      }
    }
    const website = az(['storage', 'blob', 'service-properties', 'show', '--account-name', accountName, '--auth-mode', 'login']);
    if (!isRecord(website.staticWebsite) || website.staticWebsite.enabled !== true) throw new Error('Static website is not enabled.');
    return 'Container metadata/read access, root static website and producer UAMI write-role evidence verified. No write probe performed.';
  });

  await check('Run federated-credential permissions', async () => {
    const id = requireValue(identityId, 'UAMI resource ID');
    const page = object(json('az', ['rest', '--method', 'GET', '--url',
      `https://management.azure.com${id}/providers/Microsoft.Authorization/permissions?api-version=2022-04-01`,
      '--subscription', requireValue(subscription, 'subscription')], 30_000),
    'Operator effective permissions');
    if (page.nextLink) throw new Error('Effective permissions response is paginated; complete permission evidence is required.');
    for (const action of ['write', 'delete']) {
      if (!permitsAction(page.value, `Microsoft.ManagedIdentity/userAssignedIdentities/federatedIdentityCredentials/${action}`)) {
        throw new Error(`The operator Azure CLI context lacks effective run FIC ${action} permission at the selected UAMI scope.`);
      }
    }
    return 'Operator Azure CLI permissions allow creating and deleting run federated credentials on the selected UAMI.';
  });
  await check('Run federated-credential headroom', () => {
    const inventory = json('az', azArgs(['identity', 'federated-credential', 'list', '-g', resourceGroup, '--identity-name', identityName]));
    if (!Array.isArray(inventory) || !inventory.every(isRecord)) throw new Error('Federated-credential inventory is invalid; headroom cannot be established.');
    const credentials = inventory;
    const leaked = credentials.filter((credential) => at(credential, 'name')?.startsWith('arrowcol-run-'));
    if (leaked.length) warnings.push(`${leaked.length} run FIC(s) remain; investigate possible leaked credentials. No deletion attempted.`);
    if (credentials.length >= 20) throw new Error('UAMI federated credentials are at the limit of 20; no run credential headroom remains.');
    return `${20 - credentials.length} FIC slot(s) remain across native and external trusts.`;
  });
  if (assertion && assertionPath) {
    try {
      if (await publisherAssertion(assertionPath, now) !== assertion) throw new Error('Publisher assertion changed during preflight');
    } catch {
      const credentials = checks.find((entry) => entry.name === 'Kubernetes context and credentials')!;
      credentials.ready = false;
      credentials.detail = `Federated assertion expired, changed or became invalid during preflight; run \`${setupCommand}\`, then retry.`;
    }
  }
  const ready = checks.every((entry) => entry.ready);
  return { ready, checks, nextCommand: ready ? 'npx --no-install arrowcol-oat --mode aks' : nextCommand, warnings, network };
}

export function formatPreflightReport(report: PreflightReport): string {
  const lines = [`AKS preflight: ${report.ready ? 'READY' : 'NOT READY'}`,
    `Network provider: ${report.network.provider}; enforcing: ${report.network.enforcing}`];
  for (const warning of report.warnings) lines.push(`WARNING: ${warning}`);
  for (const check of report.checks) {
    lines.push(`${check.ready ? 'READY' : 'NOT READY'}: ${check.name} - ${check.detail}`);
    if (!check.ready) lines.push(`  Next: ${check.next}`);
  }
  lines.push(`Next command: ${report.nextCommand}`);
  return lines.join('\n');
}
