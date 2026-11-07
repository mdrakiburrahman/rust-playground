import { createHash } from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { parseArgs } from 'node:util';
import { fileURLToPath } from 'node:url';
import { parseDocument } from 'yaml';
import { runStreamingTree } from './process.js';
import { bodyReads, type ReadGroup, type ReadSummary } from '../../../experiments/azure-qap/src/storage-reads.js';

const GROUP = 'arrow-collector-rg';
const OPERATIONS = ['optimize', 'purge', 'vacuum', 'reload'] as const;
const OUTPUT = '.temp/experiments/azure-qap';
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;
const JOB = /^azure-qap-[0-9]{14}-[a-f0-9]{8}$/;
const SHA = /^[a-f0-9]{40}$/;
let interrupted = false;

async function command(program: string, args: string[], timeoutMs: number, capture = true,
  onLine?: (line: string) => void): Promise<string> {
  return runStreamingTree(program, args, { timeoutMs, capture, onLine, onInterrupt: () => { interrupted = true; } });
}
function json(text: string, label: string): unknown {
  try { return JSON.parse(text) as unknown; }
  catch { throw new Error(`Invalid JSON from ${label}.`); }
}

function record(value: unknown, label: string): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error(`Invalid ${label}.`);
  return value as Record<string, unknown>;
}

interface Trial {
  trial: string;
  operation: string;
  reads: { warmUp: ReadSummary; afterMutation: ReadSummary; unchanged: ReadSummary };
}
interface Report { runId: string; trials: Trial[] }
function summary(value: unknown): ReadSummary {
  const raw = record(value, 'read summary');
  if (!Number.isSafeInteger(raw.files) || Number(raw.files) < 0 || !Number.isSafeInteger(raw.bytes) ||
      Number(raw.bytes) < 0 || !Number.isSafeInteger(raw.requests) || Number(raw.requests) < 0 ||
      !Array.isArray(raw.paths) || raw.paths.some(path => typeof path !== 'string') ||
      new Set(raw.paths).size !== raw.paths.length || raw.paths.length !== raw.files)
    throw new Error('Invalid read summary.');
  return { files: Number(raw.files), bytes: Number(raw.bytes), requests: Number(raw.requests), paths: raw.paths as string[] };
}

function compareReads(value: unknown, trial: Trial): void {
  if (!Array.isArray(value)) throw new Error('Invalid saved read query output.');
  const rows: ReadGroup[] = value.map(item => {
    const row = record(item, 'read row');
    if ((row.phase !== 'warm-up' && row.phase !== 'after-mutation' && row.phase !== 'late') ||
        typeof row.path !== 'string' || typeof row.operation !== 'string' ||
        typeof row.status !== 'number' || !Number.isInteger(row.status) ||
        typeof row.bytes !== 'number' || !Number.isSafeInteger(row.bytes) || row.bytes < 0 ||
        typeof row.requests !== 'number' || !Number.isSafeInteger(row.requests) || row.requests < 0 ||
        typeof row.last !== 'string' || typeof row.userAgent !== 'string')
      throw new Error('Invalid saved read query row.');
    return { phase: row.phase, path: row.path, operation: row.operation, status: row.status, bytes: row.bytes,
      requests: row.requests, last: row.last, userAgent: row.userAgent };
  });
  for (const [phase, expected] of [['warm-up', trial.reads.warmUp], ['after-mutation', trial.reads.afterMutation]] as const) {
    const observed = bodyReads(rows, phase);
    if (observed.bytes !== expected.bytes || observed.files !== expected.files ||
        JSON.stringify(observed.paths) !== JSON.stringify([...expected.paths].sort()))
      throw new Error(`Read recheck mismatch for ${trial.operation}/${phase}.`);
  }
}

function requireFile(file: string): void {
  const stat = fs.lstatSync(file);
  if (!stat.isFile() || stat.isSymbolicLink()) throw new Error(`Expected regular file: ${file}`);
}

function stamp(subscription: string): { namespace: string; stamp: number; document: ReturnType<typeof parseDocument> } {
  const file = 'infra/azure/stamp.yaml';
  requireFile(file);
  const document = parseDocument(fs.readFileSync(file, 'utf8'));
  if (document.errors.length) throw new Error('Invalid private stamp YAML.');
  const value = record(document.toJS() as unknown, 'private stamp');
  const aks = record(value.aks, 'AKS stamp configuration');
  if (typeof value.subscription !== 'string' || value.subscription.toLowerCase() !== subscription.toLowerCase() ||
      value.resourceGroup !== GROUP || !Number.isInteger(value.stamp) || Number(value.stamp) < 1 || Number(value.stamp) > 999999 ||
      typeof aks.namespace !== 'string' || !/^[a-z0-9](?:[-a-z0-9]*[a-z0-9])?$/.test(aks.namespace))
    throw new Error('Private stamp does not match the authorized subscription and sandbox.');
  return { namespace: aks.namespace, stamp: Number(value.stamp), document };
}

function createEvidenceDir(value: string): string {
  if (!path.isAbsolute(value) || value.split(/[\\/]/).includes('..'))
    throw new Error('--evidence-dir must be an absolute path without traversal.');
  const root = fs.realpathSync('.');
  const parent = fs.realpathSync(path.dirname(value));
  if (parent === root || parent.startsWith(root + path.sep))
    throw new Error('Evidence must be outside the checkout.');
  const result = path.join(parent, path.basename(value));
  fs.mkdirSync(result, { mode: 0o700 });
  return result;
}

function report(directory: string): Report {
  for (const filename of ['report.json', 'report.md', 'evidence.jsonl']) requireFile(path.join(directory, filename));
  const value = record(json(fs.readFileSync(path.join(directory, 'report.json'), 'utf8'), 'report'), 'report');
  const control = record(value.control, 'positive control');
  const cleanup = record(value.cleanup, 'cleanup');
  if (typeof value.runId !== 'string' || !/^[a-z0-9_-]+$/.test(value.runId) || value.exitCode !== 0 ||
      value.interrupted || value.error || control.status !== 'detected' ||
      cleanup.status !== 'done' || !Array.isArray(cleanup.errors) || cleanup.errors.length ||
      !Array.isArray(value.trials) || value.trials.length !== OPERATIONS.length)
    throw new Error(`Incomplete report or cleanup: ${directory}`);
  const trials: Trial[] = [];
  for (const operation of OPERATIONS) {
    const matches = value.trials.map(item => record(item, 'trial')).filter(trial => trial.operation === operation);
    const trial = matches[0];
    if (!trial) throw new Error(`Missing ${operation} trial in ${directory}.`);
    const files = record(trial.files, 'trial files');
    const reads = record(trial.reads, 'trial reads');
    const unchanged = summary(reads.unchanged);
    const warmUp = summary(reads.warmUp);
    const afterMutation = summary(reads.afterMutation);
    if (matches.length !== 1 || trial.trial !== operation || trial.status !== 'complete' || trial.telemetry !== 'complete' ||
        !Number.isSafeInteger(files.unchanged) || Number(files.unchanged) <= 0 ||
        (operation === 'reload' && unchanged.files < Number(files.unchanged)))
      throw new Error(`Incomplete ${operation} trial in ${directory}.`);
    if (operation === 'reload' && (new Set(unchanged.paths).size < Number(files.unchanged) ||
        unchanged.paths.some(p => !afterMutation.paths.includes(p))))
      throw new Error(`Reload control is missing observed file paths in ${directory}.`);
    requireFile(path.join(directory, 'queries', `${operation}.yaml`));
    trials.push({ trial: operation, operation, reads: { unchanged, warmUp, afterMutation } });
  }
  if (!fs.statSync(path.join(directory, 'http-traces')).isDirectory()) throw new Error('Missing HTTP traces.');
  return { runId: value.runId, trials };
}

function copySafe(source: string, destination: string): void {
  const st = fs.lstatSync(source);
  if (st.isSymbolicLink()) throw new Error(`Evidence contains a symlink: ${source}`);
  if (st.isDirectory()) {
    fs.mkdirSync(destination, { mode: 0o700 });
    for (const entry of fs.readdirSync(source)) copySafe(path.join(source, entry), path.join(destination, entry));
  } else if (st.isFile()) fs.copyFileSync(source, destination, fs.constants.COPYFILE_EXCL);
  else throw new Error(`Evidence contains a non-file: ${source}`);
}

function validateTree(source: string): void {
  const stat = fs.lstatSync(source);
  if (/^\.env(?:\.|$)/.test(path.basename(source)) || stat.isSymbolicLink() || (!stat.isDirectory() && !stat.isFile()))
    throw new Error('Evidence contains a symlink or non-file.');
  if (stat.isDirectory()) for (const entry of fs.readdirSync(source)) validateTree(path.join(source, entry));
}

async function collect(subscription: string, destination: string, legacy: boolean, jobOverride?: string): Promise<{
  devcontainerRunId: string; aksRunId: string; jobName?: string; cleanupVerification: string; namespace: string; archiveSha256: string;
}> {
  const info = stamp(subscription);
  const directories = ['devcontainer', 'aks'].map(mode => path.join(OUTPUT, mode));
  for (const directory of directories) {
    if (fs.realpathSync(directory) !== path.resolve(directory)) throw new Error('Report path contains a symlink.');
    for (const name of ['report.json', 'report.md', 'evidence.jsonl', 'queries', 'http-traces']) validateTree(path.join(directory, name));
  }
  const reports = directories.map(report);
  const sidecar = path.join(directories[1], 'kubernetes-job.json');
  if (legacy && fs.existsSync(sidecar)) throw new Error('Legacy cleanup inventory is only for reports without a Kubernetes job receipt.');
  let owned: Record<string, unknown> = {};
  if (jobOverride) owned = { jobName: jobOverride, namespace: info.namespace };
  else if (!legacy) {
    requireFile(sidecar);
    owned = record(json(fs.readFileSync(sidecar, 'utf8'), 'Kubernetes job receipt'), 'Kubernetes job receipt');
  }
  if (!legacy && (typeof owned.jobName !== 'string' || !JOB.test(owned.jobName) || owned.namespace !== info.namespace))
    throw new Error('Provide the exact run-owned Kubernetes Job name and namespace.');
  const job = typeof owned.jobName === 'string' ? owned.jobName : undefined;
  const account = record(json(await command('az', ['account', 'show', '--subscription', subscription, '-o', 'json'], 60_000), 'Azure account'), 'Azure account');
  if (typeof account.id !== 'string' || account.id.toLowerCase() !== subscription.toLowerCase())
    throw new Error('Azure account does not match the authorized subscription.');
  await command('az', ['aks', 'get-credentials', '--subscription', subscription, '-g', GROUP,
    '-n', `arrow-collector-${info.stamp}`, '--overwrite-existing'], 120_000);
  await command('kubelogin', ['convert-kubeconfig', '-l', 'azurecli'], 60_000);
  for (let i = 0; i < directories.length; i++) {
    const mode = i === 0 ? 'devcontainer' : 'aks';
    const rechecks = path.join(destination, 'rechecks', mode);
    fs.mkdirSync(rechecks, { recursive: true, mode: 0o700 });
    for (const trial of reports[i].trials) {
      const output = await command('npx', ['--no-install', 'tsx', 'experiments/azure-qap/src/read-logs.ts',
        '--query', path.join(directories[i], 'queries', `${trial.trial}.yaml`)], 180_000);
      compareReads(json(output, 'saved read query'), trial);
      fs.writeFileSync(path.join(rechecks, `${trial.trial}.json`), output, { mode: 0o600 });
    }
  }
  if (legacy) {
    for (const resource of ['jobs', 'pods', 'configmaps', 'secrets']) {
      const inventory = record(json(await command('kubectl', ['get', resource, '-n', info.namespace, '-o', 'json'], 60_000),
        `Kubernetes ${resource}`), `Kubernetes ${resource}`);
      if (!Array.isArray(inventory.items)) throw new Error(`Invalid Kubernetes ${resource} inventory.`);
      for (const item of inventory.items) {
        const metadata = record(record(item, 'Kubernetes object').metadata, 'Kubernetes metadata');
        if (typeof metadata.name !== 'string') throw new Error('Kubernetes inventory item has no name.');
        if (metadata.name.startsWith('azure-qap-')) throw new Error(`Kubernetes ${resource} inventory contains an azure-qap object.`);
      }
    }
  } else {
    for (const resource of [`job/${job}`, `configmap/${job}-env`, `secret/${job}-secret`]) {
      const result = await command('kubectl', ['get', resource, '-n', info.namespace, '--ignore-not-found=true', '-o', 'name'], 60_000);
      if (result.trim()) throw new Error(`Run-owned Kubernetes object still exists: ${resource}`);
    }
    const pods = record(json(await command('kubectl', ['get', 'pods', '-n', info.namespace, '--selector', `job-name=${job}`, '-o', 'json'], 60_000), 'Kubernetes pods'), 'Kubernetes pods');
    if (!Array.isArray(pods.items) || pods.items.length) throw new Error('Run-owned Kubernetes pods remain or response is invalid.');
  }
  for (let i = 0; i < directories.length; i++) {
    const target = path.join(destination, i === 0 ? 'devcontainer' : 'aks');
    fs.mkdirSync(target, { mode: 0o700 });
    for (const name of ['report.json', 'report.md', 'evidence.jsonl', 'queries', 'http-traces',
      'kubernetes-job.json', 'done', 'exit-code']) {
      const source = path.join(directories[i], name);
      if (fs.existsSync(source)) copySafe(source, path.join(target, name));
    }
  }
  const tar = path.join(destination, 'evidence.tar');
  await command('tar', ['-cf', tar, '-C', destination, 'devcontainer', 'aks', 'rechecks'], 120_000);
  const hash = await new Promise<string>((resolve, reject) => {
    const digest = createHash('sha256');
    const input = fs.createReadStream(tar);
    input.on('data', chunk => digest.update(chunk));
    input.on('error', reject);
    input.on('end', () => resolve(digest.digest('hex')));
  });
  fs.writeFileSync(path.join(destination, 'SHA256SUMS'), `${hash}  evidence.tar\n`, { mode: 0o600 });
  return { devcontainerRunId: reports[0].runId, aksRunId: reports[1].runId,
    ...(job ? { jobName: job } : {}), cleanupVerification: legacy ? 'legacy-prefix-inventory' : 'exact-job',
    namespace: info.namespace, archiveSha256: hash };
}

async function main(): Promise<void> {
  const { positionals, values } = parseArgs({ allowPositionals: true, options: {
    subscription: { type: 'string' }, 'expected-sha': { type: 'string' },
    'evidence-dir': { type: 'string' }, 'job-name': { type: 'string' }, 'legacy-cleanup-inventory': { type: 'boolean' },
  } });
  const mode = positionals[0];
  if (positionals.length !== 1 || (mode !== 'run' && mode !== 'collect') ||
      !values.subscription || !UUID.test(values.subscription) || !values['evidence-dir'] ||
      (mode === 'run' && (!values['expected-sha'] || !SHA.test(values['expected-sha']))) ||
      (mode === 'collect' && values['expected-sha']) || (mode === 'run' && (values['job-name'] || values['legacy-cleanup-inventory'])) ||
      (values['job-name'] && values['legacy-cleanup-inventory']))
    throw new Error('Usage: qap-rehearsal.ts run --subscription <UUID> --expected-sha <sha> --evidence-dir <absolute-new-dir> | collect --subscription <UUID> --evidence-dir <absolute-new-dir> [--job-name <owned-job> | --legacy-cleanup-inventory]');
  if (process.platform === 'win32') throw new Error('Run and collect must execute in the Linux devcontainer.');
  const subscription = values.subscription;
  const destination = createEvidenceDir(values['evidence-dir']);
  const receipt: Record<string, unknown> = { mode, subscription, stage: 'preflight', startedAt: new Date().toISOString() };
  const save = () => fs.writeFileSync(path.join(destination, 'receipt.json'), JSON.stringify(receipt, null, 2) + '\n', { mode: 0o600 });
  const stage = (name: string) => { receipt.stage = name; save(); console.log(`QAP rehearsal: ${name}`); };
  try {
    save();
    const info = stamp(subscription);
    if (mode === 'run') {
      requireFile('.env');
      if (fs.existsSync('.env.azure')) throw new Error('Fresh checkout must not contain generated .env.azure.');
      for (const name of ['devcontainer', 'aks'])
        if (fs.existsSync(path.join(OUTPUT, name))) throw new Error('Refusing to overwrite an existing report directory.');
      const actualSha = (await command('git', ['rev-parse', 'HEAD'], 30_000)).trim();
      const dirty = (await command('git', ['status', '--porcelain'], 30_000)).trim();
      if (actualSha !== values['expected-sha'] || dirty) throw new Error('Checkout is not clean at the expected SHA.');
      stage('npm-ci');
      await command('npm', ['ci'], 30 * 60_000, false);
      const newStamp = Math.floor(Date.now() / 1000) % 999999 + 1;
      if (newStamp === info.stamp) throw new Error('Generated stamp equals the previous stamp; start a new run later.');
      info.document.set('stamp', newStamp);
      info.document.set('storageReadLogDestination', 'storage');
      info.document.setIn(['aks', 'systemPool', 'vmSize'], 'Standard_D8s_v5');
      fs.writeFileSync('infra/azure/stamp.yaml', info.document.toString(), { mode: 0o600 });
      receipt.stamp = newStamp;
      stage('account');
      const account = record(json(await command('az', ['account', 'show', '--subscription', subscription, '-o', 'json'], 60_000), 'Azure account'), 'Azure account');
      if (typeof account.id !== 'string' || account.id.toLowerCase() !== subscription.toLowerCase())
        throw new Error('Azure account does not match the authorized subscription.');
      await command('az', ['account', 'set', '--subscription', subscription], 60_000);
      const provision = ['nx', 'run', 'azure-infra:provision', '--', '--stamp-config', 'infra/azure/stamp.yaml', '--wait-seconds', '600'];
      stage('provision-1'); await command('npx', provision, 60 * 60_000, false);
      stage('provision-2'); await command('npx', provision, 60 * 60_000, false);
      stage('credentials');
      await command('az', ['aks', 'get-credentials', '--subscription', subscription, '-g', GROUP,
        '-n', `arrow-collector-${newStamp}`, '--overwrite-existing'], 120_000);
      await command('kubelogin', ['convert-kubeconfig', '-l', 'azurecli'], 60_000);
      stage('image');
      const registry = `arrowcollector${newStamp}`;
      const publish = await command('npx', ['nx', 'run', 'azure-qap-experiment:image', '--', '--federated',
        '--registry', registry, '--stamp-config', 'infra/azure/stamp.yaml'], 60 * 60_000);
      const matches = [...publish.matchAll(/^Digest-pinned runtime image: ([a-z0-9.-]+\/[a-z0-9._/-]+@sha256:[a-f0-9]{64})\r?$/gm)];
      if (matches.length !== 1 || !matches[0][1].startsWith(`${registry}.azurecr.io/`))
        throw new Error('Image publisher did not return exactly one expected digest-pinned image.');
      const image = parseDocument(fs.readFileSync('infra/kubernetes/azure-qap.example.yaml', 'utf8'));
      if (image.errors.length) throw new Error('Invalid Kubernetes example configuration.');
      image.set('image', matches[0][1]);
      fs.writeFileSync('infra/kubernetes/azure-qap.yaml', image.toString(), { mode: 0o600 });
      receipt.image = matches[0][1];
      const common = ['--operations', OPERATIONS.join(','), '--tail', '1m', '--log-wait', '15m', '--budget', '30m'];
      stage('devcontainer');
      await command('npx', ['nx', 'run', 'azure-qap-experiment:run', '--', ...common, '--out', `${OUTPUT}/devcontainer`], 60 * 60_000, false);
      stage('aks');
      await command('npx', ['nx', 'run', 'azure-qap-experiment:run', '--', '--kubernetes', '--kubernetes-config',
        'infra/kubernetes/azure-qap.yaml', ...common, '--out', `${OUTPUT}/aks`], 60 * 60_000, false, line => {
        const match = /Kubernetes job: (azure-qap-[0-9]{14}-[a-f0-9]{8}) namespace: ([a-z0-9-]+)/.exec(line);
        if (match && match[2] === info.namespace) {
          receipt.kubernetesJob = match[1]; save();
          console.log(`Kubernetes job: ${match[1]} namespace: ${match[2]}`);
        }
      });
    }
    stage('collect');
    receipt.evidence = await collect(subscription, destination, Boolean(values['legacy-cleanup-inventory']), values['job-name']);
    receipt.stage = 'complete';
    receipt.finishedAt = new Date().toISOString(); receipt.exitCode = 0; save();
    const receiptHash = createHash('sha256').update(fs.readFileSync(path.join(destination, 'receipt.json'))).digest('hex');
    fs.appendFileSync(path.join(destination, 'SHA256SUMS'), `${receiptHash}  receipt.json\n`);
    console.log('QAP rehearsal: complete');
  } catch (error) {
    if (receipt.stage === 'complete') receipt.stage = 'finalization';
    receipt.failedAt = new Date().toISOString();
    receipt.exitCode = interrupted ? 130 : 1;
    receipt.error = error instanceof Error ? error.message : String(error);
    save();
    throw error;
  }
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch(error => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = interrupted ? 130 : 1;
  });
}
