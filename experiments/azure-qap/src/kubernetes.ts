import { spawn } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { parse as parseYaml, stringify } from 'yaml';
import { Config, Failure, ROOT, run_tree } from '../../../infra/azure/bootstrap.js';
import type { Options, Profile } from './profile.js';
import { cleanupRetainedJob, validateOwnedMetadata, type RetainedJobReceipt } from './retained-job.js';
import { runtimeSignalCommand } from './runtime-signal.js';
export { cleanupRetainedJob };

const CLEANUP_SECONDS = 5 * 60;
const JOB_START_SECONDS = 120;

export interface KubernetesConfig {
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
  exactKeys(raw, ['image', 'context', 'resources', 'nodeSelector'], 'Kubernetes configuration');
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
  return { image, ...(context ? { context } : {}),
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

export function writeKustomizeOverlay(config: KubernetesConfig, namespace: string, serviceAccount: string, identityClientId: string,
  jobName: string, args: string[], environment: Record<string, string>, generation: string, activeDeadlineSeconds: number): string {
  const scratch = path.join(ROOT, '.temp/azure-qap-kustomize');
  fs.mkdirSync(scratch, { recursive: true, mode: 0o700 });
  const tempRoot = fs.mkdtempSync(path.join(scratch, 'run-'));
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
      { op: 'replace', path: '/metadata/name', value: namespace },
    ]),
    jsonPatch({ version: 'v1', kind: 'ServiceAccount', name: 'azure-qap' }, [
      { op: 'replace', path: '/metadata/name', value: serviceAccount },
      { op: 'add', path: '/metadata/annotations/azure.workload.identity~1client-id', value: identityClientId },
    ]),
    jsonPatch({ version: 'v1', kind: 'ConfigMap', name: 'azure-qap-env' }, [
      { op: 'replace', path: '/metadata/name', value: envName },
      { op: 'add', path: '/metadata/labels', value: { 'app.kubernetes.io/name': 'azure-qap', run: jobName } },
      { op: 'replace', path: '/data', value: environment },
    ]),
    jsonPatch({ version: 'v1', kind: 'Secret', name: 'azure-qap-secret' }, [
      { op: 'replace', path: '/metadata/name', value: secretName },
      { op: 'add', path: '/metadata/labels', value: { 'app.kubernetes.io/name': 'azure-qap', run: jobName } },
      { op: 'replace', path: '/stringData/ARROWCOL_RESET_GENERATION', value: generation },
    ]),
    jsonPatch({ group: 'batch', version: 'v1', kind: 'Job', name: 'azure-qap' }, [
      { op: 'replace', path: '/metadata/name', value: jobName },
      { op: 'add', path: '/metadata/labels/run', value: jobName },
      { op: 'add', path: '/spec/activeDeadlineSeconds', value: activeDeadlineSeconds },
      { op: 'add', path: '/spec/template/metadata/labels/run', value: jobName },
      { op: 'replace', path: '/spec/template/spec/serviceAccountName', value: serviceAccount },
      { op: 'replace', path: '/spec/template/spec/containers/0/image', value: config.image },
      { op: 'replace', path: '/spec/template/spec/containers/0/args', value: args },
      { op: 'replace', path: '/spec/template/spec/containers/0/envFrom/0/configMapRef/name', value: envName },
      { op: 'replace', path: '/spec/template/spec/containers/0/env/0/valueFrom/secretKeyRef/name', value: secretName },
      { op: 'replace', path: '/spec/template/spec/containers/0/resources', value: config.resources },
      { op: 'add', path: '/spec/template/spec/nodeSelector', value: config.nodeSelector },
    ]),
  ];
  kustomization.namespace = namespace;
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

function streamLogs(config: KubernetesConfig, namespace: string, pod: string, redact: (text: string) => string): () => void {
  const child = spawn('kubectl', kubectlArgs(config, ['logs', '--follow', '--container', 'experiment', '-n', namespace, pod]),
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

export function jobDeadlineSeconds(profile: Pick<Profile, 'budgetMs'>, options: Pick<Options, 'keepRunning' | 'inspectionMs'>): number {
  return Math.ceil(profile.budgetMs / 1000) + (options.keepRunning ? Math.ceil(options.inspectionMs / 1000) + 120 : 0) +
    CLEANUP_SECONDS + JOB_START_SECONDS;
}

export async function runKubernetes(input: { options: Options; argv: readonly string[]; profile: Profile; azureConfig: Config;
  generation?: string; secrets: string[] }): Promise<number> {
  if (!input.generation) throw new Failure('ARROWCOL_RESET_GENERATION is required for the Kubernetes QAP Job.');
  const config = loadKubernetesConfig(input.options.kubernetesConfig);
  const namespace = input.azureConfig.kubernetesNamespace;
  const serviceAccount = input.azureConfig.kubernetesServiceAccount;
  const identityClientId = input.azureConfig.identity_client_id;
  if (!identityClientId) throw new Failure('Generated AZURE_UAMI_CLIENT_ID is missing; provision the stamped Azure infrastructure first.');
  const suffix = randomBytes(4).toString('hex');
  const jobName = `azure-qap-${new Date().toISOString().replace(/[-:TZ.]/g, '').slice(0, 14)}-${suffix}`;
  console.log(`Kubernetes job: ${jobName} namespace: ${namespace}`);
  const args = kubernetesRuntimeArgs(input.argv);
  const activeDeadlineSeconds = jobDeadlineSeconds(input.profile, input.options);
  console.log(`Job activeDeadlineSeconds: ${activeDeadlineSeconds}; inspection limit: ${input.options.keepRunning ? input.options.inspectionMs / 1000 : 0}s.`);
  const environment = { ...input.azureConfig.generated(), AZURE_SUBSCRIPTION_ID: input.azureConfig.subscription,
    AZURE_RESOURCE_GROUP: input.azureConfig.group, ARROWCOL_QAP_STAGE: '/app/.temp/qap-stage' };
  const tempRoot = writeKustomizeOverlay(config, namespace, serviceAccount, identityClientId, jobName, args, environment,
    input.generation, activeDeadlineSeconds);
  const overlay = path.join(tempRoot, 'infra/kubernetes/overlays/azure-qap');
  const manifest = path.join(tempRoot, 'job.yaml');
  const output = path.resolve(ROOT, input.options.out ?? `.temp/experiments/azure-qap/kubernetes/${jobName}`);
  let jobApplied = false;
  let pod: string | undefined;
  let logsStop: (() => void) | undefined;
  let interrupted = false;
  let primaryError: unknown;
  let exitCode = 1;
  let retained = false;
  const onSignal = () => { interrupted = true; };
  process.on('SIGINT', onSignal);
  process.on('SIGTERM', onSignal);
  const secrets = input.secrets.filter(Boolean);
  const redact = (text: string) => secrets.reduce((value, secret) => value.replaceAll(secret, '******'), input.azureConfig.redact(text));
  const artifactsCopied = { value: false };
  const copyArtifacts = async (final = false) => {
    if (!pod || (artifactsCopied.value && !final)) return;
    const destination = final ? path.join(output, 'final') : output;
    if (fs.existsSync(destination) && fs.readdirSync(destination).length)
      throw new Failure(`Refusing to overwrite non-empty Kubernetes evidence directory ${path.relative(ROOT, destination)}.`);
    fs.mkdirSync(destination, { recursive: true, mode: 0o700 });
    await runKubectl(config, input.azureConfig,
      ['cp', '--container', 'experiment', '-n', namespace, `${pod}:/artifacts/.`, destination], 120);
    fs.writeFileSync(path.join(destination, 'kubernetes-job.json'), JSON.stringify({ jobName, namespace }) + '\n', { mode: 0o600 });
    artifactsCopied.value = true;
  };
  const waitForDone = async (timeoutSeconds: number, ignoreInterrupt = false, allowInspection = false): Promise<boolean> => {
    const deadline = Date.now() + timeoutSeconds * 1000;
    while ((ignoreInterrupt || !interrupted) && Date.now() < deadline) {
      const marker = await run_tree(['kubectl', ...kubectlArgs(config, [
        'exec', '--container', 'experiment', '-n', namespace, pod!, '--', 'sh', '-c',
        allowInspection ? 'test -f /artifacts/inspection-ready || test -f /artifacts/done' : 'test -f /artifacts/done'])], 15);
      if (marker.returncode === 0) return true;
      const status = await runKubectl(config, input.azureConfig,
        ['get', 'job', jobName, '-n', namespace, '-o', 'json'], 15).then(JSON.parse) as { status?: { failed?: number } };
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
      ['wait', '--for=condition=Ready', 'pod', '--selector', `job-name=${jobName}`, '-n', namespace,
        `--timeout=${JOB_START_SECONDS}s`], JOB_START_SECONDS + 15);
    const pods = JSON.parse(await runKubectl(config, input.azureConfig,
      ['get', 'pods', '--selector', `job-name=${jobName}`, '-n', namespace, '-o', 'json'], 30)) as {
        items?: Array<{ metadata?: { name?: string } }> };
    pod = pods.items?.[0]?.metadata?.name;
    if (!pod) throw new Failure('Kubernetes Job became ready without an owned Pod.');
    logsStop = streamLogs(config, namespace, pod, redact);
    const completed = await waitForDone(activeDeadlineSeconds, false, input.options.keepRunning);
    if (!completed) throw new Failure(interrupted ? 'Kubernetes QAP Job was interrupted before writing its completion marker.' :
      'Kubernetes QAP Job ended without writing its completion marker.');
    await copyArtifacts();
    const report = JSON.parse(fs.readFileSync(path.join(output, 'report.json'), 'utf8'));
    if (input.options.keepRunning && report.cleanup?.status === 'retained') {
      if (report.exitCode !== 0 || report.inspection?.mode !== 'read-only' || report.control?.status !== 'detected')
        throw new Failure('Job did not produce a successful read-only inspection handoff.');
      const resources: RetainedJobReceipt['resources'] = {};
      for (const name of [`job/${jobName}`, `configmap/${jobName}-env`, `secret/${jobName}-secret`]) {
        const live = JSON.parse(await runKubectl(config, input.azureConfig, ['get', name, '-n', namespace, '-o', 'json']));
        resources[name] = validateOwnedMetadata(live, name.split('/')[1]!, namespace, jobName);
      }
      const livePod = JSON.parse(await runKubectl(config, input.azureConfig, ['get', 'pod', pod, '-n', namespace, '-o', 'json']));
      const podUid = validateOwnedMetadata(livePod, pod, namespace, jobName);
      if (!livePod.metadata.ownerReferences?.some((owner: { uid: string; kind: string }) =>
        owner.kind === 'Job' && owner.uid === resources[`job/${jobName}`]))
        throw new Failure('Inspection Pod is not owned by the retained Job.');
      await runKubectl(config, input.azureConfig,
        ['exec', '--container', 'experiment', '-n', namespace, pod, '--', 'test', '-f', '/artifacts/inspection-ready'], 15);
      if (interrupted) throw new Failure('Inspection handoff interrupted; stopping the owned workload for cleanup.');
      const receipt: RetainedJobReceipt = { version: 1, mode: 'read-only-inspection', jobName, namespace,
        context: config.context ?? null, pod, podUid, resources, runId: report.runId,
        inspectionDeadline: report.inspection.deadline };
      fs.writeFileSync(path.join(output, 'kubernetes-job.json'), JSON.stringify(receipt, null, 2) + '\n', { mode: 0o600 });
      retained = true;
      exitCode = 0;
      console.log(`READ-ONLY INSPECTION retained until ${receipt.inspectionDeadline}; no more trial mutations.`);
      console.log(`Observe: kubectl ${config.context ? `--context ${config.context} ` : ''}logs --follow --container experiment -n ${namespace} ${pod}`);
      console.log(`Scoped cleanup: npx --no-install tsx tools/scripts/src/qap-rehearsal.ts cleanup --receipt ${path.relative(ROOT, path.join(output, 'kubernetes-job.json'))}`);
    } else {
      const result = await runKubectl(config, input.azureConfig,
        ['exec', '-n', namespace, pod, '--', 'cat', '/artifacts/exit-code'], 30);
      if (!/^\d{1,3}$/.test(result) || Number(result) > 255) throw new Failure('Kubernetes QAP Job wrote an invalid exit code.');
      exitCode = Number(result);
    }
  } catch (error) {
    primaryError = error;
  }
  if (jobApplied && pod && (!artifactsCopied.value || primaryError)) {
    try {
      await runKubectl(config, input.azureConfig,
        ['exec', '--container', 'experiment', '-n', namespace, pod, '--', ...runtimeSignalCommand('SIGTERM')], 30);
      if (await waitForDone(CLEANUP_SECONDS + 30, true)) await copyArtifacts(artifactsCopied.value);
      else throw new Failure('Kubernetes QAP cancellation did not finish cleanup before the five-minute deadline.');
    } catch (error) {
      primaryError ??= error;
    }
  }
  if (jobApplied && !retained) {
    if (pod && artifactsCopied.value) {
      try { await runKubectl(config, input.azureConfig,
        ['exec', '--container', 'experiment', '-n', namespace, pod, '--', ...runtimeSignalCommand('SIGUSR1')], 15); }
      catch (error) { primaryError ??= error; }
    }
    try { await runKubectl(config, input.azureConfig,
      ['delete', `job/${jobName}`, `configmap/${jobName}-env`, `secret/${jobName}-secret`,
        '-n', namespace, '--ignore-not-found=true', '--grace-period=10', '--wait=true', '--timeout=30s'], 75); }
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