import { spawn } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { parse as parseYaml, stringify } from 'yaml';
import { Config, Failure, ROOT, run_tree } from '../../../infra/azure/bootstrap.js';
import type { Options, Profile } from './profile.js';

const CLEANUP_SECONDS = 5 * 60;
const JOB_START_SECONDS = 120;
const RESOURCE_NAME = /^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$/;
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

export interface KubernetesConfig {
  namespace: string;
  serviceAccount: string;
  identityClientId: string;
  image: string;
  context?: string;
  resources: { requests: { cpu: string; memory: string }; limits: { cpu: string; memory: string } };
  nodeSelector: Record<string, string>;
}

function record(value: unknown, name: string): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error(`${name} must be a mapping.`);
  return value as Record<string, unknown>;
}

function exactKeys(value: Record<string, unknown>, allowed: string[], name: string): void {
  const extra = Object.keys(value).filter(key => !allowed.includes(key));
  if (extra.length) throw new Error(`${name} contains unsupported key(s): ${extra.join(', ')}.`);
}

function stringValue(value: unknown, name: string, pattern: RegExp, maxLength = 255): string {
  if (typeof value !== 'string' || value.length > maxLength || !pattern.test(value)) throw new Error(`${name} is invalid.`);
  return value;
}

function resourceQuantity(value: unknown, name: string, memory: boolean): string {
  const pattern = memory ? /^\d+(?:Ki|Mi|Gi|Ti)$/ : /^(?:\d+(?:m)?|\d+\.\d+)$/;
  const normalized = !memory && typeof value === 'number' && Number.isFinite(value) ? String(value) : value;
  return stringValue(normalized, name, pattern);
}

function quantityValue(value: string, memory: boolean): number {
  if (!memory) return value.endsWith('m') ? Number(value.slice(0, -1)) / 1000 : Number(value);
  const match = /^(\d+)(Ki|Mi|Gi|Ti)$/.exec(value)!;
  const factors = { Ki: 1024, Mi: 1024 ** 2, Gi: 1024 ** 3, Ti: 1024 ** 4 };
  return Number(match[1]) * factors[match[2] as keyof typeof factors];
}

export function parseKubernetesConfig(text: string): KubernetesConfig {
  let value: unknown;
  try { value = parseYaml(text); }
  catch (error) { throw new Error('Kubernetes configuration is not valid YAML.', { cause: error }); }
  const raw = record(value, 'Kubernetes configuration');
  exactKeys(raw, ['namespace', 'serviceAccount', 'identityClientId', 'image', 'context', 'resources', 'nodeSelector'],
    'Kubernetes configuration');
  const namespace = stringValue(raw.namespace, 'namespace', RESOURCE_NAME, 63);
  const serviceAccount = stringValue(raw.serviceAccount, 'serviceAccount', RESOURCE_NAME, 63);
  const identityClientId = stringValue(raw.identityClientId, 'identityClientId', UUID);
  const image = stringValue(raw.image, 'image', /^[a-z0-9][a-z0-9.-]*(?::\d+)?\/[a-z0-9][a-z0-9._/-]*@sha256:[a-f0-9]{64}$/);
  const context = raw.context === undefined ? undefined : stringValue(raw.context, 'context', /^[A-Za-z0-9_.:/-]+$/);
  const resources = record(raw.resources, 'resources');
  exactKeys(resources, ['requests', 'limits'], 'resources');
  const requests = record(resources.requests, 'resources.requests');
  const limits = record(resources.limits, 'resources.limits');
  exactKeys(requests, ['cpu', 'memory'], 'resources.requests');
  exactKeys(limits, ['cpu', 'memory'], 'resources.limits');
  const nodeSelectorRaw = raw.nodeSelector === undefined ? {} : record(raw.nodeSelector, 'nodeSelector');
  const nodeSelector: Record<string, string> = {};
  for (const [key, selectorValue] of Object.entries(nodeSelectorRaw)) {
    if (!/^[A-Za-z0-9](?:[-A-Za-z0-9_.]*[A-Za-z0-9])?(?:\/[A-Za-z0-9](?:[-A-Za-z0-9_.]*[A-Za-z0-9])?)?$/.test(key) ||
        typeof selectorValue !== 'string' || selectorValue.length > 63)
      throw new Error(`nodeSelector entry ${key} is invalid.`);
    nodeSelector[key] = selectorValue;
  }
  const requestCpu = resourceQuantity(requests.cpu, 'resources.requests.cpu', false);
  const requestMemory = resourceQuantity(requests.memory, 'resources.requests.memory', true);
  const limitCpu = resourceQuantity(limits.cpu, 'resources.limits.cpu', false);
  const limitMemory = resourceQuantity(limits.memory, 'resources.limits.memory', true);
  if (quantityValue(requestCpu, false) <= 0 || quantityValue(requestMemory, true) <= 0 ||
      quantityValue(requestCpu, false) > quantityValue(limitCpu, false) ||
      quantityValue(requestMemory, true) > quantityValue(limitMemory, true))
    throw new Error('Resource requests must be positive and no greater than their limits.');
  return { namespace, serviceAccount, identityClientId, image, ...(context ? { context } : {}),
    resources: { requests: { cpu: requestCpu, memory: requestMemory },
      limits: { cpu: limitCpu, memory: limitMemory } }, nodeSelector };
}

export function loadKubernetesConfig(filename: string): KubernetesConfig {
  return parseKubernetesConfig(fs.readFileSync(path.resolve(ROOT, filename), 'utf8'));
}

export function kubernetesRuntimeArgs(argv: readonly string[]): string[] {
  const result: string[] = [];
  for (let index = 0; index < argv.length; index++) {
    const argument = argv[index]!;
    if (argument === '--kubernetes') continue;
    if (argument === '--kubernetes-config' || argument === '--env' || argument === '--out') { index++; continue; }
    if (argument.startsWith('--kubernetes-config=') || argument.startsWith('--env=') || argument.startsWith('--out=')) continue;
    result.push(argument);
  }
  return ['--out', '/artifacts', ...result];
}

function jsonPatch(target: Record<string, string>, operations: Array<{ op: string; path: string; value: unknown }>) {
  return { target, patch: stringify(operations) };
}

function writeKustomizeOverlay(config: KubernetesConfig, jobName: string, args: string[], environment: Record<string, string>,
  generation: string, activeDeadlineSeconds: number): string {
  const tempRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'azure-qap-kustomize-'));
  const baseSource = path.join(ROOT, 'infra/kubernetes/base');
  const overlaySource = path.join(ROOT, 'infra/kubernetes/overlays/azure-qap');
  const base = path.join(tempRoot, 'infra/kubernetes/base');
  const overlay = path.join(tempRoot, 'infra/kubernetes/overlays/azure-qap');
  fs.cpSync(baseSource, base, { recursive: true });
  fs.cpSync(overlaySource, overlay, { recursive: true });
  const kustomizationFile = path.join(overlay, 'kustomization.yaml');
  const kustomization = record(parseYaml(fs.readFileSync(kustomizationFile, 'utf8')), 'Kustomize overlay');
  const envName = `${jobName}-env`;
  const secretName = `${jobName}-secret`;
  const patches = [
    jsonPatch({ version: 'v1', kind: 'Namespace', name: 'azure-qap' }, [
      { op: 'replace', path: '/metadata/name', value: config.namespace },
    ]),
    jsonPatch({ version: 'v1', kind: 'ServiceAccount', name: 'azure-qap' }, [
      { op: 'replace', path: '/metadata/name', value: config.serviceAccount },
      { op: 'add', path: '/metadata/annotations/azure.workload.identity~1client-id', value: config.identityClientId },
    ]),
    jsonPatch({ version: 'v1', kind: 'ConfigMap', name: 'azure-qap-env' }, [
      { op: 'replace', path: '/metadata/name', value: envName },
      { op: 'replace', path: '/data', value: environment },
    ]),
    jsonPatch({ version: 'v1', kind: 'Secret', name: 'azure-qap-secret' }, [
      { op: 'replace', path: '/metadata/name', value: secretName },
      { op: 'replace', path: '/stringData/ARROWCOL_RESET_GENERATION', value: generation },
    ]),
    jsonPatch({ group: 'batch', version: 'v1', kind: 'Job', name: 'azure-qap' }, [
      { op: 'replace', path: '/metadata/name', value: jobName },
      { op: 'add', path: '/metadata/labels/run', value: jobName },
      { op: 'add', path: '/spec/activeDeadlineSeconds', value: activeDeadlineSeconds },
      { op: 'add', path: '/spec/template/metadata/labels/run', value: jobName },
      { op: 'replace', path: '/spec/template/spec/serviceAccountName', value: config.serviceAccount },
      { op: 'replace', path: '/spec/template/spec/containers/0/image', value: config.image },
      { op: 'replace', path: '/spec/template/spec/containers/0/args', value: args },
      { op: 'replace', path: '/spec/template/spec/containers/0/envFrom/0/configMapRef/name', value: envName },
      { op: 'replace', path: '/spec/template/spec/containers/0/env/0/valueFrom/secretKeyRef/name', value: secretName },
      { op: 'replace', path: '/spec/template/spec/containers/0/resources', value: config.resources },
      { op: 'add', path: '/spec/template/spec/nodeSelector', value: config.nodeSelector },
    ]),
  ];
  kustomization.namespace = config.namespace;
  kustomization.patches = patches;
  fs.writeFileSync(kustomizationFile, stringify(kustomization), { mode: 0o600 });
  return tempRoot;
}

function kubectlArgs(config: KubernetesConfig, args: string[]): string[] {
  return [...(config.context ? ['--context', config.context] : []), ...args];
}

function commandFailure(name: string, result: { returncode: number; stdout: string; stderr: string }, config: Config): Failure {
  return new Failure(`${name} failed: ${config.redact((result.stderr || result.stdout || `exit ${result.returncode}`).trim()).slice(0, 2000)}`);
}

async function runKubectl(config: KubernetesConfig, azure: Config, args: string[], timeoutSeconds = 120) {
  const result = await run_tree(['kubectl', ...kubectlArgs(config, args)], timeoutSeconds);
  if (result.returncode) throw commandFailure('kubectl', result, azure);
  return result.stdout.trim();
}

function streamLogs(config: KubernetesConfig, pod: string, redact: (text: string) => string): () => void {
  const child = spawn('kubectl', kubectlArgs(config, ['logs', '--follow', '--container', 'experiment', '-n', config.namespace, pod]),
    { detached: true, stdio: ['ignore', 'pipe', 'pipe'] });
  const output = (chunk: Buffer) => process.stdout.write(redact(chunk.toString()));
  child.stdout.on('data', output);
  child.stderr.on('data', output);
  return () => {
    if (!child.pid) return;
    try { process.kill(-child.pid, 'SIGTERM'); }
    catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ESRCH') throw error; }
  };
}

const sleep = (ms: number) => new Promise(resolve => setTimeout(resolve, ms));

export async function runKubernetes(input: { options: Options; argv: readonly string[]; profile: Profile; azureConfig: Config;
  generation?: string; secrets: string[] }): Promise<number> {
  if (!input.generation) throw new Failure('ARROWCOL_RESET_GENERATION is required for the Kubernetes QAP Job.');
  const config = loadKubernetesConfig(input.options.kubernetesConfig);
  const suffix = randomBytes(4).toString('hex');
  const jobName = `azure-qap-${new Date().toISOString().replace(/[-:TZ.]/g, '').slice(0, 14)}-${suffix}`;
  const args = kubernetesRuntimeArgs(input.argv);
  const activeDeadlineSeconds = Math.ceil((input.profile.budgetMs + CLEANUP_SECONDS * 1000 + JOB_START_SECONDS * 1000) / 1000);
  const environment = { ...input.azureConfig.generated(), AZURE_SUBSCRIPTION_ID: input.azureConfig.subscription,
    AZURE_RESOURCE_GROUP: input.azureConfig.group, ARROWCOL_QAP_STAGE: '/tmp/qap-stage' };
  const tempRoot = writeKustomizeOverlay(config, jobName, args, environment, input.generation, activeDeadlineSeconds);
  const overlay = path.join(tempRoot, 'infra/kubernetes/overlays/azure-qap');
  const manifest = path.join(tempRoot, 'job.yaml');
  const output = path.resolve(ROOT, input.options.out ?? `.temp/experiments/azure-qap/kubernetes/${jobName}`);
  let jobApplied = false;
  let pod: string | undefined;
  let logsStop: (() => void) | undefined;
  let interrupted = false;
  let primaryError: unknown;
  let exitCode = 1;
  const onSignal = () => { interrupted = true; };
  process.on('SIGINT', onSignal);
  process.on('SIGTERM', onSignal);
  const secrets = input.secrets.filter(Boolean);
  const redact = (text: string) => secrets.reduce((value, secret) => value.replaceAll(secret, '******'), input.azureConfig.redact(text));
  const artifactsCopied = { value: false };
  const copyArtifacts = async () => {
    if (!pod || artifactsCopied.value) return;
    if (fs.existsSync(output) && fs.readdirSync(output).length)
      throw new Failure(`Refusing to overwrite non-empty Kubernetes evidence directory ${path.relative(ROOT, output)}.`);
    fs.mkdirSync(output, { recursive: true, mode: 0o700 });
    await runKubectl(config, input.azureConfig,
      ['cp', '--container', 'experiment', '-n', config.namespace, `${pod}:/artifacts/.`, output], 120);
    artifactsCopied.value = true;
  };
  const waitForDone = async (timeoutSeconds: number, ignoreInterrupt = false): Promise<boolean> => {
    const deadline = Date.now() + timeoutSeconds * 1000;
    while ((ignoreInterrupt || !interrupted) && Date.now() < deadline) {
      const marker = await run_tree(['kubectl', ...kubectlArgs(config, [
        'exec', '-n', config.namespace, pod!, '--', 'test', '-f', '/artifacts/done'])], 15);
      if (marker.returncode === 0) return true;
      const status = await runKubectl(config, input.azureConfig,
        ['get', 'job', jobName, '-n', config.namespace, '-o', 'json'], 15).then(JSON.parse) as { status?: { failed?: number } };
      if ((status.status?.failed ?? 0) > 0) return false;
      await sleep(5_000);
    }
    return false;
  };
  try {
    const built = await run_tree(['kustomize', 'build', overlay], 60);
    if (built.returncode) throw commandFailure('kustomize build', built, input.azureConfig);
    fs.writeFileSync(manifest, built.stdout, { mode: 0o600 });
    const applied = await run_tree(['kubectl', ...kubectlArgs(config, ['apply', '-f', manifest])], 120);
    if (applied.returncode) throw commandFailure('kubectl apply', applied, input.azureConfig);
    jobApplied = true;
    await runKubectl(config, input.azureConfig,
      ['wait', '--for=condition=Ready', 'pod', '--selector', `job-name=${jobName}`, '-n', config.namespace,
        `--timeout=${JOB_START_SECONDS}s`], JOB_START_SECONDS + 15);
    const pods = JSON.parse(await runKubectl(config, input.azureConfig,
      ['get', 'pods', '--selector', `job-name=${jobName}`, '-n', config.namespace, '-o', 'json'], 30)) as {
        items?: Array<{ metadata?: { name?: string } }> };
    pod = pods.items?.[0]?.metadata?.name;
    if (!pod) throw new Failure('Kubernetes Job became ready without an owned Pod.');
    logsStop = streamLogs(config, pod, redact);
    const completed = await waitForDone(activeDeadlineSeconds);
    if (!completed) throw new Failure(interrupted ? 'Kubernetes QAP Job was interrupted before writing its completion marker.' :
      'Kubernetes QAP Job ended without writing its completion marker.');
    await copyArtifacts();
    const result = await runKubectl(config, input.azureConfig,
      ['exec', '-n', config.namespace, pod, '--', 'cat', '/artifacts/exit-code'], 30);
    if (!/^\d{1,3}$/.test(result) || Number(result) > 255) throw new Failure('Kubernetes QAP Job wrote an invalid exit code.');
    exitCode = Number(result);
  } catch (error) {
    primaryError = error;
  }
  if (jobApplied && pod && !artifactsCopied.value) {
    try {
      await runKubectl(config, input.azureConfig, ['exec', '-n', config.namespace, pod, '--', 'kill', '-TERM', '1'], 30);
      if (await waitForDone(CLEANUP_SECONDS, true)) await copyArtifacts();
      else throw new Failure('Kubernetes QAP cancellation did not finish cleanup before the five-minute deadline.');
    } catch (error) {
      primaryError ??= error;
    }
  }
  if (jobApplied) {
    try { await runKubectl(config, input.azureConfig,
      ['delete', 'job', jobName, '-n', config.namespace, '--grace-period=10', '--wait=true', '--timeout=30s'], 75); }
    catch (error) { primaryError ??= error; }
  }
  if (logsStop) {
    try { logsStop(); } catch (error) { primaryError ??= error; }
  }
  process.off('SIGINT', onSignal);
  process.off('SIGTERM', onSignal);
  fs.rmSync(tempRoot, { recursive: true, force: true });
  if (primaryError) throw primaryError;
  if (interrupted) return 130;
  console.log(`Kubernetes evidence and report copied to ${path.relative(ROOT, output)}.`);
  return exitCode;
}