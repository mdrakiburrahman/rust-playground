import { execFileSync } from 'node:child_process';
import { readFile, stat } from 'node:fs/promises';
import { isAbsolute, resolve } from 'node:path';

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
}

interface CommandResult {
  stdout: string;
  exitCode: number;
}

interface PreflightOptions {
  workspaceRoot?: string;
  environment?: NodeJS.ProcessEnv;
  runCommand?: (command: string, args: string[]) => CommandResult;
  fetcher?: typeof fetch;
  now?: () => number;
}

type JsonRecord = Record<string, unknown>;

const publisherNamespace = 'arrow-collector';
const publisherServiceAccount = 'arrow-collector';
const resourceGroup = 'arrow-collector-rg';
const identityName = 'arrow-collector';
const secretRequirements = [
  ['arrowcol-sql-admin', ['password']],
  ['arrowcol-sql-runtime', ['password']],
  ['arrowcol-sql-migration', ['password']],
  ['arrowcol-router-auth', ['edge-hmac', 'internal-hmac']],
  ['arrowcol-generation', ['generation']],
] as const;

function isRecord(value: unknown): value is JsonRecord {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function readString(value: unknown): string | undefined {
  return typeof value === 'string' && value.trim() ? value : undefined;
}

function readStringAt(value: unknown, key: string): string | undefined {
  return isRecord(value) ? readString(value[key]) : undefined;
}

function runLocalCommand(command: string, args: string[]): CommandResult {
  try {
    return {
      stdout: execFileSync(command, args, { encoding: 'utf8', timeout: 30_000, maxBuffer: 8 * 1024 * 1024 }),
      exitCode: 0,
    };
  } catch (error) {
    const status = isRecord(error) && typeof error.status === 'number' ? error.status : 1;
    return { stdout: '', exitCode: status };
  }
}

function commandOutput(
  runCommand: NonNullable<PreflightOptions['runCommand']>,
  command: string,
  args: string[],
): string {
  const result = runCommand(command, args);
  if (result.exitCode !== 0) throw new Error(`${command} exited with status ${result.exitCode}`);
  return result.stdout.trim();
}

function commandJsonValue(
  runCommand: NonNullable<PreflightOptions['runCommand']>,
  command: string,
  args: string[],
): unknown {
  const output = commandOutput(runCommand, command, args);
  try {
    return JSON.parse(output);
  } catch {
    throw new Error(command + ' returned invalid JSON');
  }
}

function commandJson(
  runCommand: NonNullable<PreflightOptions['runCommand']>,
  command: string,
  args: string[],
): JsonRecord {
  const value = commandJsonValue(runCommand, command, args);
  if (!isRecord(value)) throw new Error(command + ' returned an unexpected JSON shape');
  return value;
}

function kubectlArgs(context: string, args: string[]): string[] {
  return ['--context', context, '--request-timeout=30s', ...args];
}

function azArgs(subscription: string, args: string[]): string[] {
  return [...args, '--subscription', subscription, '-o', 'json'];
}

function addCheck(
  checks: PreflightCheck[],
  name: string,
  ready: boolean,
  detail: string,
  next: string,
): void {
  checks.push({ name, ready, detail, next });
}

function recordArray(value: unknown): JsonRecord[] {
  return Array.isArray(value) ? value.filter(isRecord) : [];
}

function checkCondition(value: unknown, type: string): boolean {
  return recordArray(value).some((condition) => condition.type === type && condition.status === 'True');
}

function parseImage(image: string): { repository: string; digest: string } | undefined {
  const match = /^[a-z0-9]+\.azurecr\.io\/([a-z0-9][a-z0-9/_.-]*)@(sha256:[a-f0-9]{64})$/.exec(image);
  return match ? { repository: match[1]!, digest: match[2]! } : undefined;
}

export async function runAksPreflight(configPath: string, options: PreflightOptions = {}): Promise<PreflightReport> {
  const workspaceRoot = resolve(options.workspaceRoot ?? process.cwd());
  const environment = options.environment ?? process.env;
  const runCommand = options.runCommand ?? runLocalCommand;
  const fetcher = options.fetcher ?? fetch;
  const now = options.now ?? Date.now;
  const checks: PreflightCheck[] = [];
  const nextCommand = 'npx --no-install arrowcol-oat --mode aks --preflight-only';
  const configFile = resolve(workspaceRoot, configPath);
  let config: JsonRecord;
  let platform: JsonRecord;
  let runtime: JsonRecord;
  let runtimeFile: string;
  try {
    const parsed: unknown = JSON.parse(await readFile(configFile, 'utf8'));
    if (!isRecord(parsed) || !isRecord(parsed.platform)) throw new Error('OAT config must contain a platform object');
    config = parsed;
    platform = config.platform as JsonRecord;
    if (platform.kind !== 'aks') throw new Error('AKS preflight requires platform.kind=aks');
    const stamp = readString(config.stamp);
    if (!stamp || !/^\d{5}$/.test(stamp)) throw new Error('AKS config requires a five-digit stamp');
    if (!readString(config.qap_database)) throw new Error('Set the selected QAP database explicitly');
    const resultUrl = readString(config.result_url);
    const websiteUrl = readString(config.website_url);
    if (!resultUrl || !websiteUrl ||
        !resultUrl.startsWith(`abfss://experiments@arrowcollector${stamp}.dfs.core.windows.net/`) ||
        !websiteUrl.startsWith(`abfss://$web@arrowcollector${stamp}.dfs.core.windows.net/`) || resultUrl === websiteUrl) {
      throw new Error('AKS results must target separate experiments and root $web paths for the selected stamp');
    }
    const runtimeRelative = readStringAt(platform, 'runtime_config_file');
    const bootstrapRelative = readStringAt(platform, 'bootstrap_env_file');
    if (!runtimeRelative || !bootstrapRelative) throw new Error('AKS config requires runtime_config_file and bootstrap_env_file');
    runtimeFile = resolve(workspaceRoot, runtimeRelative);
    const bootstrapFile = resolve(workspaceRoot, bootstrapRelative);
    const [runtimeInfo, bootstrapInfo] = await Promise.all([stat(runtimeFile), stat(bootstrapFile)]);
    if (!runtimeInfo.isFile() || !bootstrapInfo.isFile()) throw new Error('AKS runtime and bootstrap files must be regular files');
    const runtimeValue: unknown = JSON.parse(await readFile(runtimeFile, 'utf8'));
    if (!isRecord(runtimeValue) || !readString(runtimeValue.subscriptionId) ||
        !readString(runtimeValue.workloadIdentityClientId) || runtimeValue.stamp !== stamp || !isRecord(runtimeValue.images)) {
      throw new Error('Runtime config must identify the selected subscription, stamp, UAMI, and immutable images');
    }
    runtime = runtimeValue;
    const validation = runCommand('cargo', [
      'run', '--locked', '--package', 'arrowcol-oat-rust', '--', '--validate-config', configFile,
    ]);
    if (validation.exitCode !== 0) throw new Error('Rust OAT config validation failed; inspect the validator output and correct the config');
  } catch (error) {
    const detail = error instanceof Error ? error.message : 'configuration could not be read';
    addCheck(checks, 'Configuration, stamp, and destinations', false, detail,
      'Correct the AKS config/runtime files, then rerun the preflight.');
    for (const name of [
      'Kubernetes context and credentials', 'Fixed publisher identity', 'Federated credential', 'Required secrets',
      'AKS deployments', 'Router service and endpoints', 'Immutable ACR images', 'Fresh pod metrics',
      'Selected QAP endpoint/database', 'Network-policy enforcement proof', 'Storage and website publication',
    ]) addCheck(checks, name, false, 'Blocked until configuration and destination validation pass.',
      'Fix the configuration gate above, then rerun the preflight.');
    return { ready: false, checks, nextCommand };
  }

  addCheck(checks, 'Configuration, stamp, and destinations', true,
    'Rust validation passed; the selected five-digit stamp and separate experiments/$web destinations match.', nextCommand);

  const subscription = readString(runtime.subscriptionId)!;
  const clientId = readString(runtime.workloadIdentityClientId)!;
  const stamp = readString(config.stamp)!;
  const expectedContext = readString(config.kubectl_context) ?? `arrow-collector-${stamp}`;
  const clusterName = `arrow-collector-${stamp}`;
  const kube = (args: string[]) => commandJson(runCommand, 'kubectl', kubectlArgs(expectedContext, args));
  const az = (args: string[]) => commandJson(runCommand, 'az', azArgs(subscription, args));
  let aks: JsonRecord | undefined;
  let apiServer: string | undefined;
  let issuer: string | undefined;
  let principalId: string | undefined;

  try {
    const currentContext = commandOutput(runCommand, 'kubectl', ['config', 'current-context']);
    const kubeConfig = commandJson(runCommand, 'kubectl', ['config', 'view', '--minify', '--context', expectedContext, '-o', 'json']);
    const clusters = recordArray(kubeConfig.clusters);
    const users = recordArray(kubeConfig.users);
    apiServer = readStringAt(clusters[0]?.cluster, 'server');
    const user = isRecord(users[0]?.user) ? users[0]!.user as JsonRecord : undefined;
    const execConfig = isRecord(user?.exec) ? user!.exec as JsonRecord : undefined;
    const execArgs = Array.isArray(execConfig?.args) ? execConfig!.args.filter((value): value is string => typeof value === 'string') : [];
    const kubeloginVersion = commandOutput(runCommand, 'kubelogin', ['--version']);
    commandOutput(runCommand, 'kubectl', kubectlArgs(expectedContext, ['get', '--raw=/version']));
    const account = az(['account', 'show']);
    const tenantId = readStringAt(account, 'tenantId');
    const accountSubscription = readStringAt(account, 'id');
    const publisherEnvSet = Boolean(environment.AZURE_TENANT_ID || environment.AZURE_SUBSCRIPTION_ID);
    const hasSafeExec = execArgs.includes('--disable-environment-override');
    const subscriptionEnvReady = environment.AZURE_SUBSCRIPTION_ID === undefined || environment.AZURE_SUBSCRIPTION_ID === subscription;
    const envReady = environment.ARROWCOL_MODE === 'azure' && environment.AZURE_CLIENT_ID === clientId &&
      Boolean(environment.AZURE_TENANT_ID) && (!tenantId || environment.AZURE_TENANT_ID === tenantId) && subscriptionEnvReady;
    const assertionPath = environment.AZURE_FEDERATED_TOKEN_FILE;
    let assertionReady = false;
    if (assertionPath && isAbsolute(assertionPath)) {
      try {
        const assertion = await stat(assertionPath);
        assertionReady = assertion.isFile() && assertion.size > 0 && (assertion.mode & 0o777) === 0o600;
      } catch {
        assertionReady = false;
      }
    }
    aks = az(['aks', 'show', '-g', resourceGroup, '-n', clusterName]);
    const fqdn = readStringAt(aks, 'fqdn');
    issuer = readStringAt(aks.oidcIssuerProfile, 'issuerUrl');
    const endpointHost = apiServer ? new URL(apiServer).hostname.toLowerCase() : '';
    const ready = currentContext === expectedContext && Boolean(endpointHost) && Boolean(fqdn) &&
      endpointHost === fqdn!.toLowerCase() && accountSubscription?.toLowerCase() === subscription.toLowerCase() &&
      hasSafeExec && (!publisherEnvSet || hasSafeExec) && envReady && assertionReady;
    addCheck(checks, 'Kubernetes context and credentials', ready,
      ready ? `Context ${expectedContext} is reachable; ${kubeloginVersion.trim()} uses the expected API endpoint and private assertion.` :
        `Context/API/auth mismatch or required Azure environment/assertion is missing; server reported ${endpointHost || 'no API endpoint'}.`,
      'Use the selected stamp kubeconfig, prepare the documented 0600 assertion file, set the documented Azure variables, and add --disable-environment-override to stored kubelogin exec args.');
  } catch (error) {
    addCheck(checks, 'Kubernetes context and credentials', false,
      error instanceof Error ? error.message : 'Kubernetes/Azure read-only checks failed.',
      'Prepare local kubeconfig/auth as documented; do not run az login or get-credentials automatically.');
  }

  try {
    const namespace = kube(['get', 'ns', publisherNamespace, '-o', 'json']);
    const namespaceReady = readStringAt(namespace.status, 'phase') === 'Active';
    const serviceAccount = kube(['-n', publisherNamespace, 'get', 'sa', publisherServiceAccount, '-o', 'json']);
    const annotations = isRecord(serviceAccount.metadata) && isRecord(serviceAccount.metadata.annotations)
      ? serviceAccount.metadata.annotations : {};
    const ready = namespaceReady && annotations['azure.workload.identity/client-id'] === clientId;
    addCheck(checks, 'Fixed publisher identity', ready,
      ready ? 'The fixed arrow-collector namespace/ServiceAccount is active and annotated with the selected UAMI.' :
        'The fixed publisher namespace is missing/terminating or its ServiceAccount annotation does not match the selected UAMI. Run-scoped future namespaces are intentionally not required here.',
      'Request C2 identity approval; render the identity phase and have the cluster owner apply it.');
  } catch (error) {
    addCheck(checks, 'Fixed publisher identity', false,
      error instanceof Error ? error.message : 'Could not inspect the fixed publisher identity.',
      'Request C2 identity approval; no namespace or ServiceAccount was created by preflight.');
  }

  try {
    if (!aks || !issuer) throw new Error('Could not establish the selected cluster OIDC issuer.');
    const identity = az(['identity', 'show', '-g', resourceGroup, '-n', identityName]);
    principalId = readStringAt(identity, 'principalId');
    const identityClientId = readStringAt(identity, 'clientId');
    const credentials = recordArray(commandJsonValue(runCommand, 'az', azArgs(subscription, ['identity', 'federated-credential', 'list', '-g', resourceGroup, '--identity-name', identityName])));
    const subject = `system:serviceaccount:${publisherNamespace}:${publisherServiceAccount}`;
    const ready = Boolean(principalId) && identityClientId === clientId && credentials.some((credential) =>
      credential.issuer === issuer && credential.subject === subject &&
      Array.isArray(credential.audiences) && credential.audiences.includes('api://AzureADTokenExchange'));
    addCheck(checks, 'Federated credential', ready,
      ready ? 'The existing UAMI FIC exactly matches the selected issuer, fixed ServiceAccount subject, and audience; run-scoped FIC preparation remains runner-owned.' :
        'No exact existing UAMI FIC matched issuer, fixed subject, and audience; future run-scoped namespaces remain pending runner preparation.',
      'Request owner approval to prepare the exact fixed-subject FIC; do not create a wildcard or a new identity.');
  } catch (error) {
    addCheck(checks, 'Federated credential', false,
      error instanceof Error ? error.message : 'Could not inspect the UAMI federated credentials.',
      'Verify the selected subscription, cluster OIDC issuer, and existing UAMI FIC with the cluster owner.');
  }

  try {
    const missing: string[] = [];
    for (const [name, keys] of secretRequirements) {
      const secret = kube(['-n', publisherNamespace, 'get', 'secret', name, '-o', 'json']);
      const data = isRecord(secret.data) ? secret.data : {};
      for (const key of keys) {
        const encoded = data[key];
        if (typeof encoded !== 'string' || !/^[A-Za-z0-9+/]+={0,2}$/.test(encoded) || Buffer.from(encoded, 'base64').length === 0) missing.push(`${name}/${key}`);
      }
    }
    const ready = missing.length === 0;
    addCheck(checks, 'Required secrets', ready,
      ready ? 'All five required Kubernetes Secrets exist with nonempty required keys; secret values were not logged.' :
        `Missing Secret metadata or required keys: ${missing.join(', ')}. Secret contents were not logged.`,
      'Request approved C5 secret preparation; never print Secret JSON/data.');
  } catch (error) {
    addCheck(checks, 'Required secrets', false,
      error instanceof Error ? error.message : 'Could not inspect required Secret metadata.',
      'Request approved C5 secret preparation; preflight does not create Kubernetes Secrets.');
  }

  try {
    const deployments = kube(['-n', publisherNamespace, 'get', 'deploy', 'arrowcol-sql', 'arrowcol-catalog', 'arrowcol', 'arrowcol-router', '-o', 'json']);
    const items = recordArray(deployments.items);
    const names = ['arrowcol-sql', 'arrowcol-catalog', 'arrowcol', 'arrowcol-router'];
    const states = names.map((name) => {
      const item = items.find((entry) => readStringAt(entry.metadata, 'name') === name);
      const spec = isRecord(item?.spec) ? item!.spec as JsonRecord : {};
      const status = isRecord(item?.status) ? item!.status as JsonRecord : {};
      const metadata = isRecord(item?.metadata) ? item!.metadata as JsonRecord : {};
      const desired = typeof spec.replicas === 'number' ? spec.replicas : 1;
      const generation = typeof metadata.generation === 'number' ? metadata.generation : Number.POSITIVE_INFINITY;
      return Boolean(item) && desired > 0 && typeof status.observedGeneration === 'number' &&
        status.observedGeneration >= generation && status.availableReplicas === desired &&
        status.readyReplicas === desired && status.updatedReplicas === desired;
    });
    const ready = states.every(Boolean);
    addCheck(checks, 'AKS deployments', ready,
      ready ? 'All four deployments have observed the current generation and matching desired/available/ready/updated replicas.' :
        'One or more required deployments are missing, scaled to zero, stale, or not fully available.',
      'Request approved C5 workload preparation; preflight does not apply manifests or change replicas.');
  } catch (error) {
    addCheck(checks, 'AKS deployments', false,
      error instanceof Error ? error.message : 'Could not inspect required deployment status.',
      'Request approved C5 workload preparation; no rollout or scaling command was run.');
  }

  try {
    const service = kube(['-n', publisherNamespace, 'get', 'svc', 'arrowcol-router', '-o', 'json']);
    const spec = isRecord(service.spec) ? service.spec : {};
    const ports = recordArray(spec.ports);
    const serviceReady = spec.type === 'ClusterIP' && !spec.externalIPs &&
      ports.some((port) => port.port === 50050 && !port.nodePort);
    const slices = kube(['-n', publisherNamespace, 'get', 'endpointslices', '-l', 'kubernetes.io/service-name=arrowcol-router', '-o', 'json']);
    const endpoints = recordArray(slices.items).flatMap((slice) => recordArray(slice.endpoints));
    const readyEndpoints = endpoints.filter((endpoint) => isRecord(endpoint.conditions) && endpoint.conditions.ready === true);
    const ready = serviceReady && readyEndpoints.length > 0;
    addCheck(checks, 'Router service and endpoints', ready,
      ready ? 'Router is ClusterIP-only on 50050 and has ready EndpointSlice endpoints.' :
        'Router is not ClusterIP-only on 50050 or has no ready EndpointSlice endpoints.',
      'Request approved C5 router/service preparation; do not expose a public or node port.');
  } catch (error) {
    addCheck(checks, 'Router service and endpoints', false,
      error instanceof Error ? error.message : 'Could not inspect router service/endpoints.',
      'Request approved C5 router/service preparation; preflight does not create or expose services.');
  }

  try {
    const runtimeImages = isRecord(runtime.images) ? runtime.images : {};
    if (Object.keys(runtimeImages).sort().join(',') !== 'catalog,dataflow,router,sqlserver') throw new Error('Runtime config must list exactly the sqlserver, catalog, router, and dataflow images.');
    const images = [...Object.values(runtimeImages), config.dmv_client_image]
      .filter((image): image is string => typeof image === 'string');
    if (images.length < 5) throw new Error('Runtime config and OAT config do not list all four service images plus dmv_client_image.');
    const parsedImages = images.map(parseImage);
    if (parsedImages.some((image) => !image)) {
      throw new Error('Every service image and dmv_client_image must be pinned to an immutable ACR sha256 digest; tags/placeholders are not ready.');
    }
    for (const image of parsedImages) {
      const result = az(['acr', 'repository', 'show', '-n', `arrowcollector${stamp}`, '--image', `${image!.repository}@${image!.digest}`]);
      const foundDigest = readStringAt(result, 'digest');
      if (foundDigest !== image!.digest) throw new Error(`ACR did not confirm the requested manifest digest for ${image!.repository}.`);
    }
    addCheck(checks, 'Immutable ACR images', true,
      'All four runtime images and dmv_client_image are exact digests present in the selected stamp ACR.', nextCommand);
  } catch (error) {
    addCheck(checks, 'Immutable ACR images', false,
      error instanceof Error ? error.message : 'Could not verify immutable ACR image digests.',
      'Request C3 approval to publish and pin every image by registry manifest digest; tags and unpublished placeholders do not pass.');
  }

  try {
    const apiService = kube(['get', 'apiservice', 'v1beta1.metrics.k8s.io', '-o', 'json']);
    const apiStatus = isRecord(apiService.status) ? apiService.status : {};
    if (!checkCondition(apiStatus.conditions, 'Available')) throw new Error('metrics.k8s.io APIService is not Available=True.');
    const pods = kube(['-n', publisherNamespace, 'get', 'pods', '-o', 'json']);
    const workloads = recordArray(pods.items).filter((pod) => {
      const metadata = isRecord(pod.metadata) ? pod.metadata : {};
      const labels = isRecord(metadata.labels) ? metadata.labels : {};
      const status = isRecord(pod.status) ? pod.status : {};
      return ['arrowcol-sql', 'arrowcol-catalog', 'arrowcol', 'arrowcol-router'].includes(String(labels.app ?? '')) &&
        status.phase === 'Running' && checkCondition(status.conditions, 'Ready');
    });
    const metricsPath = `/apis/metrics.k8s.io/v1beta1/namespaces/${publisherNamespace}/pods`;
    const metricList: unknown = JSON.parse(commandOutput(runCommand, 'kubectl', kubectlArgs(expectedContext, ['get', `--raw=${metricsPath}`])));
    const metricItems = isRecord(metricList) ? recordArray(metricList.items) : [];
    const metricByName = new Map(metricItems.map((item) => [readStringAt(item.metadata, 'name') ?? '', item]));
    const timestampFresh = (item: JsonRecord): boolean => {
      const stampText = readStringAt(item, 'timestamp');
      const stampTime = stampText ? Date.parse(stampText) : Number.NaN;
      return Number.isFinite(stampTime) && now() - stampTime <= 5 * 60_000 && now() >= stampTime;
    };
    const ready = workloads.length > 0 && workloads.every((pod) => {
      const name = readStringAt(pod.metadata, 'name');
      const metric = name ? metricByName.get(name) : undefined;
      return Boolean(metric && timestampFresh(metric));
    });
    addCheck(checks, 'Fresh pod metrics', ready,
      ready ? `Fresh metrics cover all ${workloads.length} current ArrowCol stack pods.` :
        'Metrics API is available, but current workload pod coverage is empty, incomplete, or stale.',
      'Restore metrics-server coverage for every current workload pod; zero/fallback metrics do not pass.');
  } catch (error) {
    addCheck(checks, 'Fresh pod metrics', false,
      error instanceof Error ? error.message : 'Could not verify current pod metrics.',
      'Repair metrics-server coverage and rerun the read-only preflight.');
  }

  try {
    const qap = new URL(readString(config.qap_endpoint)!);
    const database = readString(config.qap_database)!;
    if (qap.protocol !== 'https:') throw new Error('The selected QAP endpoint must use HTTPS.');
    commandOutput(runCommand, 'getent', ['ahosts', qap.hostname]);
    const token = commandOutput(runCommand, 'az', [
      'account', 'get-access-token', '--subscription', subscription,
      '--resource', 'https://kusto.kusto.windows.net', '--query', 'accessToken', '-o', 'tsv',
    ]);
    const response = await fetcher(new URL('/v1/rest/query', qap), {
      method: 'POST',
      headers: { authorization: `Bearer ${token}`, 'content-type': 'application/json' },
      body: JSON.stringify({ db: database, csl: 'print OatPreflight = 1' }),
      signal: AbortSignal.timeout(30_000),
    });
    if (!response.ok) throw new Error(`Selected QAP query returned HTTP ${response.status}.`);
    addCheck(checks, 'Selected QAP endpoint/database', true,
      `DNS and a bounded read-only query succeeded for the configured database ${database}.`, nextCommand);
  } catch (error) {
    addCheck(checks, 'Selected QAP endpoint/database', false,
      error instanceof Error ? error.message : 'Could not query the configured QAP endpoint/database.',
      'Set the selected stamp QAP endpoint and database and obtain owner-approved QAP qualification; DNS alone is insufficient.');
  }

  try {
    const networkProfile = isRecord(aks?.networkProfile) ? aks!.networkProfile as JsonRecord : {};
    const policy = readStringAt(networkProfile, 'networkPolicy');
    const cilium = kube(['-n', 'kube-system', 'get', 'ds', 'cilium', '-o', 'json']);
    const ciliumStatus = isRecord(cilium.status) ? cilium.status : {};
    const agentsReady = typeof ciliumStatus.desiredNumberScheduled === 'number' && ciliumStatus.desiredNumberScheduled > 0 &&
      ciliumStatus.numberReady === ciliumStatus.desiredNumberScheduled;
    const policies = kube(['-n', publisherNamespace, 'get', 'networkpolicies', '-o', 'json']);
    const proofPath = environment.ARROWCOL_NETWORK_POLICY_RECEIPT;
    let proofReady = false;
    if (proofPath && apiServer) {
      try {
        const proof: unknown = JSON.parse(await readFile(proofPath, 'utf8'));
        proofReady = isRecord(proof) && proof.clusterServer === apiServer && proof.namespace === publisherNamespace &&
          proof.ownerApproved === true && proof.positivePath === true && proof.negativePath === true &&
          typeof proof.recordedAt === 'string' && Number.isFinite(Date.parse(proof.recordedAt)) &&
          now() - Date.parse(proof.recordedAt) <= 30 * 24 * 60 * 60_000;
      } catch {
        proofReady = false;
      }
    }
    const ready = Boolean(policy && policy.toLowerCase() !== 'none') && agentsReady && recordArray(policies.items).length > 0 && proofReady;
    addCheck(checks, 'Network-policy enforcement proof', ready,
      ready ? 'Enforcing dataplane, healthy agents, namespace policies, and a current owner-approved positive/negative receipt match this API server.' :
        'Enforcing dataplane/healthy agents, namespace policy topology, or an exact-cluster owner-approved positive/negative traffic receipt is missing. Read-only preflight cannot create behavioral proof.',
      'Request C1 approval and an owner-approved positive/negative traffic proof receipt for this exact cluster; do not infer enforcement from policy objects alone.');
  } catch (error) {
    addCheck(checks, 'Network-policy enforcement proof', false,
      error instanceof Error ? error.message : 'Could not inspect network-policy enforcement.',
      'Request C1 approval and exact-cluster behavioral proof; preflight performs no traffic or policy mutation.');
  }

  try {
    const accountName = `arrowcollector${stamp}`;
    const account = az(['storage', 'account', 'show', '-n', accountName]);
    const accountId = readStringAt(account, 'id');
    if (!accountId) throw new Error('Selected storage account metadata is missing.');
    for (const container of ['experiments', '$web']) {
      az(['storage', 'container', 'show', '-n', container, '--account-name', accountName, '--auth-mode', 'login']);
      commandOutput(runCommand, 'az', [
        'storage', 'blob', 'list', '--container-name', container, '--account-name', accountName,
        '--auth-mode', 'login', '--num-results', '1', '--subscription', subscription, '-o', 'json',
      ]);
    }
    const website = az(['storage', 'blob', 'service-properties', 'show', '--account-name', accountName]);
    const staticWebsite = isRecord(website.staticWebsite) ? website.staticWebsite : {};
    const roleScopes = ['experiments', '$web'].map((container) => `${accountId}/blobServices/default/containers/${container}`);
    const assignments = roleScopes.map((scope) => recordArray(commandJsonValue(runCommand, 'az', azArgs(subscription, ['role', 'assignment', 'list', '--scope', scope, '--include-inherited']))));
    const writerReady = Boolean(principalId) && roleScopes.every((scope, index) => assignments[index]!.some((assignment) =>
      assignment.principalId === principalId && assignment.roleDefinitionName === 'Storage Blob Data Contributor' &&
      typeof assignment.scope === 'string' && scope.toLowerCase().startsWith(assignment.scope.toLowerCase())));
    const ready = staticWebsite.enabled === true && writerReady;
    addCheck(checks, 'Storage and website publication', ready,
      ready ? 'experiments and root $web metadata/list checks passed; static website is enabled and the workload UAMI has write-role evidence.' :
        'Container/list access, static website configuration, or producer-principal write-role evidence is missing. No write probe was performed.',
      'Request C5 storage approval and producer-principal RBAC evidence; preflight performs no write probe.');
  } catch (error) {
    addCheck(checks, 'Storage and website publication', false,
      error instanceof Error ? error.message : 'Could not verify selected storage destinations.',
      'Request C5 storage approval and verify read/list plus producer write-role evidence; do not run a write probe.');
  }

  return { ready: checks.every((check) => check.ready), checks, nextCommand };
}

export function formatPreflightReport(report: PreflightReport): string {
  const lines = [`AKS preflight: ${report.ready ? 'READY' : 'NOT READY'}`];
  for (const check of report.checks) {
    lines.push(`${check.ready ? 'READY' : 'NOT READY'}: ${check.name} — ${check.detail}`);
    if (!check.ready) lines.push(`  Next: ${check.next}`);
  }
  lines.push(`Next command: ${report.nextCommand}`);
  return lines.join('\n');
}
