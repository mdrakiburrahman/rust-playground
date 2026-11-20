import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import { parseArgs, stripVTControlCharacters } from 'node:util';
import { fileURLToPath } from 'node:url';
import { createProjectGraphAsync, readProjectsConfigurationFromProjectGraph } from '@nx/devkit';
import { runChecked, SpawnCommandRunner } from './process.js';
import { Azure, Config, is_schema_evolution_group, load_env, run_tree, write_private } from '../../../infra/azure/bootstrap.js';
import { PublisherControl } from '../../../infra/azure/publisher_control.js';
import { privateEnvironment } from '../../../experiments/schema-evolution/src/private-nx.js';
import { requireNoLiveAttemptWriters, requireSealedAttemptEvidence } from '../../../projects/loadtest/src/attempt-evidence.js';

const REPO = 'mdrakiburrahman/rust-playground';
const SHA = /^[a-f0-9]{40}$/;
const DIGEST = /^sha256:[a-f0-9]{64}$/;
const UUID = /^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/i;
const LOADTEST = 'projects/loadtest/src/main.ts';
const RESULTS = 'onelake/benchmark/latest/results';
const QUALIFICATION_GROUP = 'rust-playground-schema-evo-qualification-20261108';
const MODES = ['verify', 'local', 'azure', 'reset'] as const;
type Mode = typeof MODES[number];
type JsonRecord = Record<string, unknown>;
export interface Options {
  mode: Mode;
  evidenceDir: string;
  expectedSha?: string;
  verifiedReceipt?: string;
  resetReceipt?: string;
  subscription?: string;
  resourceGroup?: string;
  envFile?: string;
  readyEnv?: string;
  confirmReset?: string;
}
export interface ImageIdentity {
  project: string;
  repository: string;
  alias: string;
  configDigest: string;
  revision: string;
}
interface Stage {
  name: string; command: string[]; startedAt: string; finishedAt?: string;
  status: 'running' | 'passed' | 'failed'; tests?: string[];
}

function record(value: unknown, label: string): JsonRecord {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error(`Invalid ${label}.`);
  return value as JsonRecord;
}
function records(value: unknown, label: string): JsonRecord[] {
  if (!Array.isArray(value)) throw new Error(`Invalid ${label}.`);
  return value.map(item => record(item, label));
}
function readJson(file: string): JsonRecord {
  regularFile(file);
  return record(JSON.parse(fs.readFileSync(file, 'utf8')) as unknown, file);
}
function regularFile(file: string): void {
  const stat = fs.lstatSync(file);
  if (!stat.isFile() || stat.isSymbolicLink()) throw new Error(`Expected a regular file: ${file}`);
}
function hash(file: string): string {
  regularFile(file);
  return createHash('sha256').update(fs.readFileSync(file)).digest('hex');
}
function saveJson(file: string, value: unknown): void {
  fs.writeFileSync(file, JSON.stringify(value, null, 2) + '\n', { mode: 0o600 });
}
function string(value: unknown, label: string): string {
  if (typeof value !== 'string') throw new Error(`Invalid ${label}.`);
  return value;
}
async function hashArchive(file: string): Promise<string> {
  const digest = createHash('sha256');
  for await (const chunk of fs.createReadStream(file)) digest.update(chunk);
  return digest.digest('hex');
}
export function parseOptions(argv: string[]): Options {
  const { positionals, values } = parseArgs({ args: argv, allowPositionals: true, options: {
    'expected-sha': { type: 'string' }, 'evidence-dir': { type: 'string' },
    'verified-receipt': { type: 'string' }, subscription: { type: 'string' },
    'reset-receipt': { type: 'string' },
    'resource-group': { type: 'string' }, 'env-file': { type: 'string' },
    'ready-env': { type: 'string' }, 'confirm-reset': { type: 'string' },
  } });
  const mode = MODES.find(item => item === positionals[0]);
  if (positionals.length !== 1 || !mode || !values['evidence-dir'])
    throw new Error('Use verify|local|azure|reset with --evidence-dir <new absolute directory>.');
  if (mode === 'verify' ? (values['expected-sha'] && !SHA.test(values['expected-sha'])) || values['verified-receipt'] || values['reset-receipt'] :
    values['expected-sha'] || (mode === 'azure' ? Boolean(values['verified-receipt']) === Boolean(values['reset-receipt']) :
      !values['verified-receipt'] || values['reset-receipt']))
    throw new Error('verify accepts optional --expected-sha <full SHA>; azure requires --reset-receipt or --verified-receipt; local/reset require --verified-receipt.');
  const cloud = mode === 'azure' || mode === 'reset';
  if (cloud ? values.subscription && !UUID.test(values.subscription) :
    values.subscription || values['resource-group'] || values['env-file'] || values['ready-env'] || values['confirm-reset'])
    throw new Error('Only azure/reset accept cloud inputs and --subscription <UUID>.');
  const group = cloud ? values['resource-group'] ?? QUALIFICATION_GROUP : undefined;
  if (cloud && (group === 'rust-playground-schema-evo' || !is_schema_evolution_group(group!)))
    throw new Error('azure/reset require a dedicated schema-evolution group; the old default group is refused.');
  if (mode === 'azure') {
    if (values['confirm-reset'] || (values['reset-receipt'] && (values['env-file'] || values['ready-env'])) ||
        (!values['reset-receipt'] && !values['ready-env']) ||
        (values['env-file'] && values['ready-env'] === values['env-file']))
      throw new Error('azure consumes --reset-receipt, or a separate --ready-env override with --verified-receipt; it never provisions or resets.');
  } else if (values['ready-env'] || (mode === 'reset' && values['confirm-reset'] !== group)) {
    throw new Error('reset is destructive and requires --confirm-reset exactly equal to --resource-group.');
  }
  return { mode, evidenceDir: values['evidence-dir'], expectedSha: values['expected-sha'],
    verifiedReceipt: values['verified-receipt'], resetReceipt: values['reset-receipt'], subscription: values.subscription,
    resourceGroup: group, envFile: values['env-file'], readyEnv: values['ready-env'],
    confirmReset: values['confirm-reset'] };
}

export function createEvidenceDir(value: string, workspace: string): string {
  if (!path.isAbsolute(value) || value.split(/[\\/]/).includes('..'))
    throw new Error('--evidence-dir must be absolute without traversal.');
  const parent = fs.realpathSync(path.dirname(value)), root = fs.realpathSync(workspace);
  if (parent === root || parent.startsWith(root + path.sep))
    throw new Error('Evidence must be outside the checkout so a reset cannot erase it.');
  const destination = path.join(parent, path.basename(value));
  fs.mkdirSync(destination, { mode: 0o700 });
  return destination;
}

export function serviceImages(value: unknown): Record<string, string> {
  const services = record(record(value, 'Compose configuration').services, 'Compose services');
  const images: Record<string, string> = {};
  for (const [service, value] of Object.entries(services)) {
    const image = record(value, service).image;
    if (typeof image !== 'string' || !image || /hive|arrowcol-direct|catalog-sql/i.test(service))
      throw new Error(`Invalid or removed ArrowCol topology service: ${service}`);
    images[service] = image;
  }
  for (const service of ['arrowcol-catalog', 'dataflow-1', 'router', 'validator'])
    if (!images[service]) throw new Error(`Missing DFE/catalog service: ${service}`);
  return images;
}
export function loadtestImageProjects(value: unknown): string[] {
  const targets = record(record(record(value, 'loadtest project').targets, 'loadtest targets').build, 'loadtest build target');
  if (!Array.isArray(targets.dependsOn)) throw new Error('loadtest:build must declare its image dependencies.');
  const projects = targets.dependsOn.flatMap((dependency: unknown) => {
    if (!dependency || typeof dependency !== 'object' || Array.isArray(dependency)) return [];
    const target = record(dependency, 'loadtest build dependency');
    if (target.target !== 'image') return [];
    if (!Array.isArray(target.projects)) throw new Error('loadtest:build image dependencies must list projects.');
    return target.projects.map((project: unknown) => string(project, 'loadtest image project'));
  });
  if (!projects.length || new Set(projects).size !== projects.length)
    throw new Error('loadtest:build must declare a non-empty, unique image project list.');
  return projects.sort();
}
export function validateComposeImageCoverage(images: Record<string, string>, projects: string[],
  repositories: Record<string, string>): void {
  const ownedPrefix = `ghcr.io/${REPO}/`;
  const projectSet = new Set(projects);
  for (const [service, image] of Object.entries(images)) {
    if (!image.startsWith(ownedPrefix) || !image.endsWith(':dev')) continue;
    const project = image.slice(ownedPrefix.length, -':dev'.length);
    if (!projectSet.has(project)) throw new Error(`Compose service ${service} uses an unbuilt project :dev image: ${image}`);
  }
  const required = { 'arrowcol-catalog': 'arrowcol-catalog-rust', 'dataflow-1': 'arrowcol-dataflow-rust',
    router: 'arrowcol-router-rust', validator: 'arrowcol-router-rust' };
  for (const [service, project] of Object.entries(required)) {
    if (!projectSet.has(project) || images[service] !== `${repositories[project]}:dev`)
      throw new Error(`Compose service ${service} is not covered by loadtest:build project ${project}.`);
  }
}
export function validateImage(value: unknown, identity: ImageIdentity, sha: string): void {
  const image = record(value, 'Docker image'), config = record(image.Config, 'Docker config');
  const labels = record(config.Labels, 'OCI labels');
  assert.equal(image.Id, identity.configDigest, `Config mismatch for ${identity.alias}`);
  assert.equal(labels['org.opencontainers.image.revision'], sha, `Revision mismatch for ${identity.alias}`);
  assert.equal(identity.revision, sha);
}
export function resolveServiceImages(images: Record<string, string>, inspect: (image: string) => unknown,
  builtAliases?: ReadonlySet<string>): JsonRecord {
  const map: JsonRecord = {};
  for (const [service, image] of Object.entries(images)) {
    if (builtAliases && !builtAliases.has(image)) {
      map[service] = { image, status: 'not-inspected-before-compose' };
      continue;
    }
    const configId = string(record(inspect(image), `Docker image for ${service}`).Id, `${service} config ID`);
    assert.match(configId, DIGEST, `Invalid ${service} config ID`);
    map[service] = { image, configId };
  }
  return map;
}
export function validateCiRuns(value: unknown, sha: string): JsonRecord[] {
  const runs = records(value, 'CI runs').filter(run => run.workflowName === 'CI');
  if (!runs.length) throw new Error(`No CI workflow run exists for exact HEAD ${sha}.`);
  for (const run of runs)
    if (run.headSha !== sha || run.status !== 'completed' || run.conclusion !== 'success')
      throw new Error(`Exact-head CI run ${run.databaseId} is not completed successfully.`);
  return runs;
}
export function checkVerifiedReceipt(file: string, expectedProjects: string[]): JsonRecord {
  const receipt = readJson(file);
  const sums = fs.readFileSync(path.join(path.dirname(file), 'SHA256SUMS'), 'utf8');
  if (!sums.split('\n').includes(`${hash(file)}  receipt.json`) || receipt.mode !== 'verify' ||
      receipt.exitCode !== 0 || receipt.stage !== 'complete' || typeof receipt.source !== 'string' || !SHA.test(receipt.source))
    throw new Error('Expected a successful, checksummed verify receipt for this source.');
  const images = records(receipt.images, 'verified images');
  assert.deepEqual(images.map(image => string(image.project, 'image project')).sort(), [...expectedProjects].sort());
  for (const image of images) {
    const project = string(image.project, 'image project');
    const repository = string(image.repository, 'image repository');
    assert.equal(repository, `ghcr.io/${REPO}/${project}`);
    assert.equal(string(image.alias, 'image alias'), `${repository}:dev`);
    assert.match(string(image.configDigest, 'image config digest'), DIGEST);
    assert.equal(image.revision, receipt.source, `Image revision mismatch for ${project}`);
  }
  return receipt;
}
export function derivedCloudInput(values: Record<string, string>, group: string): Record<string, string> {
  if (group === 'rust-playground-schema-evo' || !is_schema_evolution_group(group))
    throw new Error('A dedicated schema-evolution resource group is required.');
  const base = { AZURE_SUBSCRIPTION_ID: values.AZURE_SUBSCRIPTION_ID ?? '', AZURE_RESOURCE_GROUP: group,
    AZURE_KUSTO_CLUSTER_TAGS: values.AZURE_KUSTO_CLUSTER_TAGS ?? '{}', ARROWCOL_AZURE_MODE: 'schema-evolution' };
  new Config(base, 'schema-evolution');
  return base;
}
export function azureAccountCommand(subscription: string): string[] {
  if (!UUID.test(subscription)) throw new Error('Azure subscription must be a UUID.');
  return ['az', 'account', 'show', '--subscription', subscription, '--query', '{id:id,state:state}', '--output', 'json'];
}
export function validateAzureAccount(value: unknown, subscription: string): void {
  const account = record(value, 'Azure account');
  if (typeof account.id !== 'string' || account.id.toLowerCase() !== subscription.toLowerCase() || account.state !== 'Enabled')
    throw new Error('The approved Azure subscription is unavailable, disabled, or mismatched.');
}
export async function verifyAzureAccount(subscription: string, lookup: (command: string[]) => Promise<string>): Promise<void> {
  const account = JSON.parse(await lookup(azureAccountCommand(subscription))) as unknown;
  validateAzureAccount(account, subscription);
}
export function checkResetReceipt(file: string, workspace: string, group: string): JsonRecord {
  if (group === 'rust-playground-schema-evo' || !is_schema_evolution_group(group))
    throw new Error('Reset receipt must identify a dedicated schema-evolution group.');
  const receipt = readJson(file);
  const sums = fs.readFileSync(path.join(path.dirname(file), 'SHA256SUMS'), 'utf8');
  if (!sums.split('\n').includes(`${hash(file)}  receipt.json`) || receipt.mode !== 'reset' ||
      receipt.exitCode !== 0 || receipt.stage !== 'complete' || receipt.resourceGroup !== group)
    throw new Error('Expected a successful, checksummed reset receipt for this dedicated group.');
  const reset = record(receipt.reset, 'reset custody');
  assert.equal(reset.explicitlyConfirmedGroup, group); assert.equal(reset.executed, true); assert.equal(reset.provisioned, true);
  const root = fs.realpathSync(workspace);
  for (const name of ['base', 'ready', 'manifest']) {
    const input = record(record(receipt.managedInputs, 'managed inputs')[name], name);
    const file = string(input.path, 'managed input path');
    regularFile(file);
    const resolved = fs.realpathSync(file), stat = fs.statSync(file);
    assert.ok(path.isAbsolute(file) && resolved !== root && !resolved.startsWith(root + path.sep), 'Managed input must be outside checkout');
    assert.equal(stat.mode & 0o777, 0o600, 'Managed inputs must have mode 0600');
    assert.equal(stat.uid, process.getuid!(), 'Managed input belongs to another user');
    assert.equal(hash(file), input.sha256, 'Managed input changed since provisioning');
  }
  const ready = load_env(string(record(record(receipt.managedInputs, 'managed inputs').ready, 'ready').path, 'ready path'));
  assert.equal(ready.ARROWCOL_RESET_GENERATION, receipt.resetGeneration, 'Ready generation changed since provisioning');
  assert.ok(ready.ARROWCOL_RESET_GENERATION && UUID.test(ready.ARROWCOL_RESET_GENERATION));
  const imageProjects = loadtestImageProjects(readJson(path.join(workspace, 'projects/loadtest/project.json')));
  const verified = checkVerifiedReceipt(string(receipt.verifiedReceipt, 'verify receipt path'), imageProjects);
  assert.equal(receipt.source, verified.source, 'Reset and verify sources differ');
  return receipt;
}
export function redactLog(text: string): string {
  return text.split('\n').map(line =>
    /password|secret|credential|authorization|(?:accountkey|sharedaccesssignature|token|sig)\s*[:=]/i.test(line) ?
      '[private diagnostic line withheld]' : line).join('\n');
}
export function passedTests(text: string): string[] {
  return stripVTControlCharacters(text).split('\n').flatMap(line => {
    const rust = /\btest ([\w:]+) \.\.\. ok$/.exec(line.trim());
    const node = /^(?:ok \d+ - |✔ )(.+?)(?: \([\d.]+ms\))?$/.exec(line.trim());
    return rust ? [rust[1]!] : node && !/^\s*nx run(?:\s|$)/.test(node[1]!) && !/# (?:SKIP|TODO)/i.test(line) ? [node[1]!] : [];
  });
}
export function positiveDfeSample(sample: JsonRecord, configDigest: string): boolean {
  if (sample.service !== 'dataflow-1') return false;
  if (sample.identity && record(sample.identity, 'collector identity').ok === false) return false;
  const container = record(sample.container, 'container sample');
  if (container.ok === false) return false;
  assert.equal(container.ok, true);
  const state = record(container.value, 'container state');
  if (state.imageId !== configDigest || state.running !== true) return false;
  const readiness = record(record(sample.admin, 'admin sample').readiness, 'readiness sample');
  if (readiness.ok === false) return false;
  assert.equal(readiness.ok, true);
  const value = record(readiness.value, 'readiness');
  return value.ready === true && value.status === 200;
}
export function copyEvidence(source: string, destination: string): void {
  const stat = fs.lstatSync(source), name = path.basename(source);
  if (stat.isSymbolicLink() || (!stat.isFile() && !stat.isDirectory()) ||
      /^\.env|^secrets?$|azure-cli|stamp\.yaml|seal\.json|password|credential|private-key|\.(?:pem|key)$/i.test(name))
    throw new Error(`Private or unsafe evidence member: ${name}`);
  if (name.endsWith('.log')) return;
  if (stat.isDirectory()) {
    fs.mkdirSync(destination, { mode: 0o700 });
    for (const child of fs.readdirSync(source)) copyEvidence(path.join(source, child), path.join(destination, child));
  } else fs.copyFileSync(source, destination, fs.constants.COPYFILE_EXCL);
}
export function copyEvolutionEvidence(source: string, destination: string): void {
  fs.mkdirSync(destination, { mode: 0o700 });
  for (const name of ['report.json', 'report.md', 'evidence.jsonl', 'catalog-v1', 'catalog-v2', 'sdk-v1', 'sdk-v2'])
    if (fs.existsSync(path.join(source, name))) copyEvidence(path.join(source, name), path.join(destination, name));
}

class Rehearsal {
  readonly stages: Stage[] = [];
  readonly receipt: JsonRecord;
  readonly destination: string;
  readonly env: NodeJS.ProcessEnv;
  private cloud?: Config;
  constructor(readonly options: Options, readonly workspace: string) {
    this.destination = createEvidenceDir(options.evidenceDir, workspace);
    fs.mkdirSync(path.join(this.destination, 'logs'), { mode: 0o700 });
    this.receipt = { mode: options.mode, stage: 'preflight', startedAt: new Date().toISOString(), stages: this.stages };
    this.env = { ...process.env, NX_LOAD_DOT_ENV_FILES: 'false' };
    for (const key of Object.keys(this.env))
      if (/^(AZURE_|ARROWCOL_)/.test(key) && !['AZURE_CONFIG_DIR', 'AZURE_EXTENSION_DIR'].includes(key)) delete this.env[key];
    this.env.ARROWCOL_MODE = 'local';
  }
  save(): void { saveJson(path.join(this.destination, 'receipt.json'), this.receipt); }
  prepareCloudInputs(): void {
    if (this.options.mode !== 'azure' && this.options.mode !== 'reset') return;
    if (this.options.resetReceipt) {
      const reset = checkResetReceipt(this.options.resetReceipt, this.workspace, this.options.resourceGroup!);
      const inputs = record(reset.managedInputs, 'managed inputs');
      this.options.envFile = string(record(inputs.base, 'base').path, 'base path');
      this.options.readyEnv = string(record(inputs.ready, 'ready').path, 'ready path');
      this.options.verifiedReceipt = string(reset.verifiedReceipt, 'verify receipt');
      this.receipt.resetReceipt = { path: path.resolve(this.options.resetReceipt), sha256: hash(this.options.resetReceipt) };
      this.receipt.managedInputs = inputs;
    }
    if (!this.options.envFile || this.options.mode === 'reset') {
      const state = fs.mkdtempSync(path.join(path.dirname(this.destination), 'schema-catalog-private-'));
      fs.chmodSync(state, 0o700);
      const base = this.options.envFile ? load_env(this.options.envFile) :
        derivedCloudInput(load_env(path.join(this.workspace, '.env')), this.options.resourceGroup!);
      const config = new Config(base, 'schema-evolution');
      assert.equal(config.group, this.options.resourceGroup, 'Private resource-group mismatch');
      const file = path.join(state, 'base.env');
      write_private(file, base);
      assert.deepEqual(load_env(file), base, 'Derived private input failed round-trip');
      this.options.envFile = file;
    }
    this.receipt.verifiedReceipt = path.resolve(this.options.verifiedReceipt!);
    this.save();
  }
  async command(name: string, argv: string[], seconds = 5400, privateEnv = this.env): Promise<string> {
    const stage: Stage = { name, command: argv, startedAt: new Date().toISOString(), status: 'running' };
    this.stages.push(stage); this.receipt.stage = name; this.save();
    console.log(`Schema catalog bug bash: ${name}`);
    try {
      const result = await run_tree(argv, seconds, this.workspace, privateEnv);
      const text = result.stdout + '\n' + result.stderr;
      const safe = redactLog(this.cloud ? text.split('\n').map(line => this.cloud!.redact(line)).join('\n') : text);
      const log = path.join(this.destination, 'logs', `${this.stages.length}-${name.replace(/[^a-z0-9-]/gi, '-')}.log`);
      fs.writeFileSync(log, safe, { mode: 0o600 });
      stage.tests = passedTests(safe);
      if (result.returncode !== 0) throw new Error(`${name} exited ${result.returncode}; inspect ${log}.`);
      stage.status = 'passed';
      return result.stdout;
    } catch (error) { stage.status = 'failed'; throw error; }
    finally { stage.finishedAt = new Date().toISOString(); this.save(); }
  }
  assertions(stageName: string, names: string[], claim: string): void {
    const stage = this.stages.find(item => item.name === stageName);
    assert.equal(stage?.status, 'passed');
    const observed = stage.tests ?? [];
    assert.ok(observed.length, `${stageName} observed zero completed tests (Nx task summaries are not tests)`);
    for (const name of names)
      assert.ok(observed.some(test => test === name || test.endsWith(`::${name}`)), `Missing executed acceptance test: ${name}`);
    const acceptance = this.receipt.acceptance ?? {};
    record(acceptance, 'acceptance')[claim] = { stage: stageName, tests: names.length ? names : observed };
    this.receipt.acceptance = acceptance; this.save();
  }
  docker(args: string[]): string {
    return runChecked(new SpawnCommandRunner(), 'docker', args, { stdout: 'capture', stderr: 'capture' }).stdout;
  }
  inspect(reference: string): JsonRecord {
    return record(JSON.parse(this.docker(['image', 'inspect', reference, '--format', '{{json .}}'])) as unknown, 'Docker image');
  }
  async source(sha: string): Promise<void> {
    const head = (await this.command('source-head', ['git', 'rev-parse', 'HEAD'], 30)).trim();
    const dirty = (await this.command('source-clean', ['git', 'status', '--porcelain'], 30)).trim();
    if (head !== sha || dirty) throw new Error('Checkout must be clean at the exact expected full SHA.');
    this.receipt.source = sha; this.save();
  }
  composeImages(override = false): Record<string, string> {
    const file = 'onelake/benchmark/latest/router-state/compose-runtime.json';
    const env = { ...this.env, LOADTEST_UID: String(process.getuid!()), LOADTEST_GID: String(process.getgid!()),
      LOADTEST_WORKSPACE: this.workspace, ONELAKE_HOST_PATH: path.join(this.workspace, 'onelake/benchmark/latest'),
      LOADTEST_BIN_HOST_PATH: path.join(this.workspace, 'projects/loadtest/.temp/bin'),
      LOADTEST_SCRATCH_HOST_PATH: path.join(this.workspace, 'projects/loadtest/.temp') };
    const files = ['--file', 'projects/loadtest/compose.yaml', ...(override ? ['--file', file] : [])];
    const output = runChecked(new SpawnCommandRunner(), 'docker',
      ['compose', ...files, '--profile', 'benchmark', 'config', '--format', 'json'],
      { env, stdout: 'capture', stderr: 'capture' }).stdout;
    return serviceImages(JSON.parse(output) as unknown);
  }
  imageMap(images: Record<string, string>, builtAliases?: ReadonlySet<string>): JsonRecord {
    return resolveServiceImages(images, image => this.inspect(image), builtAliases);
  }
  async ci(sha: string): Promise<void> {
    const runs = JSON.parse(await this.command('ci-inventory', ['gh', 'run', 'list', '-R', REPO,
      '--workflow', 'CI', '--commit', sha, '--limit', '100',
      '--json', 'databaseId,workflowName,status,conclusion,headSha,url'], 60)) as unknown;
    const exactRuns = validateCiRuns(runs, sha);
    const inventory: JsonRecord[] = [];
    for (const run of exactRuns) {
      const detail = record(JSON.parse(await this.command(`ci-${run.databaseId}`, ['gh', 'run', 'view',
        String(run.databaseId), '-R', REPO, '--json', 'headSha,status,conclusion,jobs,url'], 60)) as unknown, 'CI workflow');
      assert.equal(detail.headSha, sha); assert.equal(detail.status, 'completed'); assert.equal(detail.conclusion, 'success');
      const jobs = records(detail.jobs, 'CI jobs');
      if (!jobs.length || jobs.some(job => job.status !== 'completed' || !['success', 'skipped'].includes(String(job.conclusion))))
        throw new Error(`Incomplete or failed CI job inventory for run ${run.databaseId}.`);
      inventory.push({ id: run.databaseId, ...detail });
    }
    const evidence = { CI: inventory };
    saveJson(path.join(this.destination, 'github.json'), evidence);
    this.receipt.workflows = evidence;
  }
  async verify(sha: string): Promise<void> {
    await this.ci(sha);
    const npx = ['npx', '--no-install'];
    const imageProjects = loadtestImageProjects(readJson(path.join(this.workspace, 'projects/loadtest/project.json')));
    const graph = readProjectsConfigurationFromProjectGraph(await createProjectGraphAsync()).projects;
    const repositories: Record<string, string> = {};
    for (const project of imageProjects) {
      const targets = record(record(graph[project], `${project} project`).targets, `${project} targets`);
      const options = record(record(targets.image, `${project} image target`).options, `${project} image options`);
      if (typeof options.image !== 'string' || !Array.isArray(options.tags) || !options.tags.includes('dev'))
        throw new Error(`Invalid local image contract for ${project}.`);
      repositories[project] = options.image;
    }
    await this.command('runtime-build', [...npx, 'nx', 'run', 'loadtest:build', '--parallel=2', '--outputStyle=static', '--skip-nx-cache'],
      5400, { ...this.env, REGISTRY_SHA: sha });
    await this.command('initializer-typescript', [...npx, 'nx', 'run', 'azure-infra:verify', '--skip-nx-cache']);
    await this.command('initializer-fence', ['cargo', 'test', '--locked', '-p', 'arrow-deltalake', '--lib', 'azure_fence::tests']);
    this.assertions('initializer-fence', [], 'Dedicated resource-group Rust ownership fence');
    await this.command('initializer-control', ['cargo', 'test', '--locked', '-p', 'arrowcol-loadgen', '--test', 'initialize_control']);
    this.assertions('initializer-control', [], 'Parent/CLI/initializer ownership contract');
    await this.command('catalog-evolution-regressions', [...npx, 'nx', 'run-many', '-t', 'verify',
      '--projects=tools-scripts,arrowcol-props,arrowcol-sparksql,arrowcol-catalog,arrowcol-catalog-rust,arrowcol-catalog-client,arrowcol-sdk-gen,schema-evolution-experiment,arrow-deltalake,delta-maintenance',
      '--parallel=2', '--outputStyle=static', '--skip-nx-cache']);
    this.assertions('catalog-evolution-regressions', ['flight_sql_and_warm_cache_count_actual_catalog_calls',
      'reserved_properties_failed_ddl_and_retained_gap', 'committed_delta_restart_and_concurrent_evolution',
      'commit_publication_crash_and_old_seed_do_not_rollback_delta', 'concurrent_same_ddl_coalesces_to_one_version',
      'protobuf_allocation_skips_reserved_numbers_and_rejects_exhaustion', 'nullable_tail_append_is_wire_json_and_layout_compatible',
      'dry_run_then_apply_then_noop'],
    '100 warm streams make zero catalog RPCs; CES disconnect/reconnect/missed ALTER/epoch and retained gap; DDL crash/restart/concurrency; stable protobuf IDs/reserved range; maintenance no-op');
    await this.command('flight-history-and-rejection', ['cargo', 'test', '--locked', '-p', 'arrowcol-flight', '--test', 'runtime']);
    this.assertions('flight-history-and-rejection', ['registered_older_schema_is_null_filled_to_current',
      'every_unregistered_breaking_mutation_is_rejected_before_admission'], 'Historical schema NULL fill and wrong-schema rejection');
    await this.command('dfe-partition-replay', ['cargo', 'test', '--locked', '-p', 'arrowcol-dataflow-components',
      'catalog_partitions_publish_all_rows_and_replay_across_commit_waves']);
    this.assertions('dfe-partition-replay', ['catalog_partitions_publish_all_rows_and_replay_across_commit_waves'],
      'Partitioned DFE commit/replay row conservation');
    await this.command('dfe-only-topology', [...npx, 'tsx', '--test', 'projects/loadtest/test/compose.test.ts']);
    this.assertions('dfe-only-topology', [], 'DFE-only topology without Direct collector, Hive or SQL catalog');
    await this.command('sdk-compatibility', [...npx, 'nx', 'run', 'arrowcol-sdk-gen:compatibility', '--skip-nx-cache']);
    await this.command('evolution-fixtures', [...npx, 'nx', 'run', 'schema-evolution-experiment:build-fixtures', '--skip-nx-cache']);
    await this.command('reader-bundle', ['bash', 'projects/loadtest/scripts/build-reader-bundle.sh']);
    const sdk = path.join(this.destination, 'sdk');
    fs.mkdirSync(sdk, { mode: 0o700 });
    for (const name of ['catalog', 'catalog-v2', 'generated', 'generated-v2'])
      copyEvidence(`crates/arrowcol-sdk-gen/.temp/${name}`, path.join(sdk, name));
    const services = this.composeImages();
    validateComposeImageCoverage(services, imageProjects, repositories);
    const images: ImageIdentity[] = [];
    for (const project of imageProjects) {
      const repository = repositories[project]!;
      if (repository !== `ghcr.io/${REPO}/${project}`) throw new Error(`Invalid image repository for ${project}.`);
      const aliases = [...new Set(Object.values(services).filter(image => image.startsWith(repository + ':')))];
      if (aliases.length > 1 || (aliases.length && aliases[0] !== `${repository}:dev`)) throw new Error(`Ambiguous Compose alias: ${project}`);
      const alias = `${repository}:dev`;
      const identity: ImageIdentity = { project, repository, alias, configDigest: '', revision: sha };
      const local = this.inspect(alias);
      identity.configDigest = string(local.Id, `${project} config digest`);
      assert.match(identity.configDigest, DIGEST);
      validateImage(local, identity, sha);
      images.push(identity);
      this.receipt.images = images; this.save();
    }
    this.receipt.serviceImages = services;
    saveJson(path.join(this.destination, 'compose-images.json'), this.imageMap(services, new Set(images.map(image => image.alias))));
    for (const image of images) {
      const targets = record(record(graph[image.project], `${image.project} project`).targets, `${image.project} targets`);
      if (targets['image-smoke'])
        await this.command(`${image.project}-smoke`, [...npx, 'nx', 'run', `${image.project}:image-smoke`,
          '--excludeTaskDependencies', '--skip-nx-cache'], 600, { ...this.env,
          ...(image.project === 'arrowcol-catalog-rust' ? { CATALOG_IMAGE: image.alias } : {}) });
    }
    for (const image of images) validateImage(this.inspect(image.alias), image, sha);
    await this.ci(sha);
    await this.source(sha);
  }
  verified(): { sha: string; images: ImageIdentity[]; services: Record<string, string> } {
    const imageProjects = loadtestImageProjects(readJson(path.join(this.workspace, 'projects/loadtest/project.json')));
    const value = checkVerifiedReceipt(this.options.verifiedReceipt!, imageProjects);
    const images = records(value.images, 'images').map(item => {
      const image: ImageIdentity = { project: string(item.project, 'project'), repository: string(item.repository, 'repository'),
        alias: string(item.alias, 'alias'), configDigest: string(item.configDigest, 'config digest'),
        revision: string(item.revision, 'revision') };
      assert.match(image.configDigest, DIGEST);
      assert.equal(image.alias, `${image.repository}:dev`);
      assert.equal(image.revision, value.source);
      return image;
    });
    const services = record(value.serviceImages, 'verified Compose images');
    if (Object.values(services).some(image => typeof image !== 'string')) throw new Error('Invalid verified Compose names.');
    const resolvedServices = services as Record<string, string>;
    validateComposeImageCoverage(resolvedServices, imageProjects,
      Object.fromEntries(images.map(image => [image.project, image.repository])));
    return { sha: String(value.source), images, services: resolvedServices };
  }
  producer(name: string, images: ImageIdentity[], sha: string): void {
    const provenance = readJson(`${RESULTS}/provenance.json`);
    assert.equal(record(provenance.source, 'producer source').head, sha);
    const containers = records(provenance.containers, 'producer containers');
    const services = this.composeImages(true), samples = fs.readFileSync(`${RESULTS}/resource-samples.jsonl`, 'utf8')
      .trim().split('\n').map(line => record(JSON.parse(line) as unknown, 'resource sample'));
    const runtimeImages = this.imageMap(services);
    for (const [service, value] of Object.entries(runtimeImages)) {
      const observed = record(value, 'resolved runtime image'), configId = string(observed.configId, `${service} config ID`);
      assert.match(configId, DIGEST);
      const expected = images.find(image => image.alias === services[service]);
      if (expected) assert.equal(configId, expected.configDigest, `Full resolved image/config map mismatch: ${service}`);
      for (const container of containers.filter(item => item.service === service))
        assert.equal(container.imageId, configId, `Container differs from resolved service-image/config map: ${service}`);
    }
    if (this.receipt.runtimeImages)
      assert.deepEqual(runtimeImages, this.receipt.runtimeImages, 'Complete normal/Quiver service-image/config map changed');
    this.receipt.runtimeImages = runtimeImages;
    for (const service of ['arrowcol-catalog', 'router', 'dataflow-1', 'kusto-explorer']) {
      const expected = images.find(image => image.alias === services[service]);
      assert.ok(expected, `Unverified runtime image: ${service}`);
      const matches = containers.filter(container => container.service === service);
      assert.equal(matches.length, 1); assert.equal(matches[0]!.imageId, expected.configDigest);
      assert.ok(matches[0]!.id);
      if (service !== 'kusto-explorer') {
        assert.equal(matches[0]!.running, true); assert.equal(matches[0]!.healthStatus, 'healthy');
        const started = Date.parse(String(matches[0]!.startedAt)), observed = Date.parse(String(matches[0]!.observedAt));
        assert.ok(Number.isFinite(started) && Number.isFinite(observed) && started <= observed &&
          observed <= Date.parse(String(provenance.stackStartedAt)));
      }
    }
    const dfe = images.find(image => image.alias === services['dataflow-1'])!;
    const ready = samples.flatMap(sample => records(sample.collectors, 'collectors')).filter(sample => positiveDfeSample(sample, dfe.configDigest));
    assert.ok(ready.length, 'No positive live DFE readiness/config observation');
    requireSealedAttemptEvidence(this.workspace, path.join(this.workspace, 'onelake/benchmark/latest'));
    const destination = path.join(this.destination, name);
    copyEvidence(RESULTS, destination);
    const metadata = readJson(`${RESULTS}/run-metadata.json`), runId = metadata.run_id;
    assert.ok(typeof runId === 'string' && /^run-[0-9]+-[a-f0-9]{32}$/.test(runId));
    const root = `projects/loadtest/.temp/loadgen/latest/${runId}`;
    const tables = fs.readdirSync(`${root}/artifacts`).sort();
    assert.equal(tables.length, 32); assert.ok(tables.includes('sqlserver_active_sessions'));
    assert.deepEqual(fs.readdirSync(`${root}/generated`).sort(), tables);
    for (const table of tables) {
      for (const file of ['manifest.json', 'schema.arrow', 'descriptor.pb', 'schema.proto']) {
        regularFile(`${root}/artifacts/${table}/${file}`);
        assert.ok(fs.statSync(`${root}/artifacts/${table}/${file}`).size > 0);
      }
      assert.ok(fs.readdirSync(`${root}/generated/${table}`).length);
    }
    copyEvidence(`${root}/artifacts`, path.join(destination, 'catalog-artifacts'));
    copyEvidence(`${root}/generated`, path.join(destination, 'generated-sdk'));
    saveJson(path.join(destination, 'compose-images.json'), runtimeImages);
    const recovery = name === 'quiver' ? readJson(`${RESULTS}/quiver-recovery.json`) : undefined;
    if (recovery) {
      assert.equal(recovery.passed, true); assert.equal(record(recovery.recovered, 'Quiver recovered').dataflow, true);
      const before = record(record(recovery.beforeCrash, 'before crash').dataflow, 'before crash metrics');
      const after = record(record(recovery.afterRecovery, 'after recovery').dataflow, 'after recovery metrics');
      assert.ok(typeof before.backlog === 'number' && before.backlog > 0, 'No persisted pre-crash Quiver backlog');
      assert.ok(typeof after.recovered === 'number' && after.recovered > 0, 'No post-crash Quiver replay');
    }
    this.receipt[name] = { runId, tables: tables.length, positiveDfeSamples: ready.length, privateSealVerified: true,
      ...(recovery ? { quiverRecovery: recovery } : {}) };
    this.save();
  }
  async local(images: ImageIdentity[], expectedServices: Record<string, string>, sha: string): Promise<void> {
    assert.deepEqual(this.composeImages(), expectedServices, 'Complete Compose service/image map changed since verify');
    for (const image of images) validateImage(this.inspect(image.alias), image, sha);
    let failure: unknown;
    try {
      for (const [name, duration, extra] of [['normal', '30s', []], ['quiver', '60s', ['--quiver', '--crash-after', '15s']]] as const) {
        await this.command(name, ['npx', '--no-install', 'tsx', LOADTEST, '--mode', 'local', '--warmup', '5s',
          '--measure', duration, '--batch-rows', '1024', '--validate', ...extra]);
        assert.deepEqual(this.composeImages(true), expectedServices, 'Resolved runtime override changed service/image identities');
        this.producer(name, images, sha);
        await this.command(`${name}-stop`, ['npx', '--no-install', 'tsx', LOADTEST, '--stop'], 900);
        requireNoLiveAttemptWriters(this.workspace, path.join(this.workspace, 'onelake/benchmark/latest'), args => this.docker(args));
      }
      await this.evolution('local');
    } catch (error) {
      failure = error;
      try {
        if (fs.existsSync(RESULTS)) copyEvidence(RESULTS, path.join(this.destination, 'failed-current-results'));
      } catch (copyError) { failure = new AggregateError([error, copyError], 'Local run and diagnostic export failed'); }
    } finally {
      try {
        await this.command('final-owned-stop', ['npx', '--no-install', 'tsx', LOADTEST, '--stop'], 900);
        if (fs.existsSync('onelake/benchmark/latest'))
          requireNoLiveAttemptWriters(this.workspace, path.join(this.workspace, 'onelake/benchmark/latest'), args => this.docker(args));
        this.receipt.cleanup = { stopped: true, dataPreserved: true, noLiveAttemptWriters: true }; this.save();
      } catch (error) { failure = failure ? new AggregateError([failure, error], 'Local run and cleanup failed') : error; }
    }
    if (failure) throw failure;
  }
  async evolution(target: 'local' | 'azure'): Promise<void> {
    const out = `.temp/experiments/schema-evolution/bugbash-${target}-${Date.now()}`;
    const args = target === 'local' ? ['npx', '--no-install', 'nx', 'run', 'schema-evolution-experiment:run', '--',
      '--target', 'local', '--out', out] : ['npx', '--no-install', 'tsx', 'experiments/schema-evolution/src/private-nx.ts',
      'run', '--mode', 'schema-evolution', '--env-file', this.options.envFile!, '--ready-env', this.options.readyEnv!, '--out', out];
    let failure: unknown;
    try {
      await this.command(`${target}-native-evolution`, args);
      const report = readJson(`${out}/report.json`);
      assert.equal(report.status, 'passed'); assert.equal(record(report.cleanup, 'experiment cleanup').status, 'ok');
      const evidence = fs.readFileSync(`${out}/evidence.jsonl`, 'utf8').trim().split('\n')
        .map(line => record(JSON.parse(line) as unknown, 'experiment evidence'));
      const started = evidence.filter(item => item.kind === 'owned-runtime-start').map(item => record(item.data, 'owned runtime'));
      const closed = evidence.filter(item => item.kind === 'owned-runtime-close').map(item => record(item.data, 'closed runtime'));
      assert.deepEqual(started.map(item => item.role).sort(), ['catalog', 'dfe', 'v1', 'v2']);
      assert.equal(closed.length, started.length);
      const namespace = fs.readlinkSync('/proc/self/ns/pid'), boot = fs.readFileSync('/proc/sys/kernel/random/boot_id', 'utf8').trim();
      for (const processIdentity of started) {
        assert.equal(processIdentity.pidNamespace, namespace); assert.equal(processIdentity.bootId, boot);
        assert.ok(typeof processIdentity.pid === 'number' && Number.isSafeInteger(processIdentity.pid) && processIdentity.pid > 1);
        const matches = closed.filter(item => item.role === processIdentity.role && item.pid === processIdentity.pid &&
          item.pidNamespace === namespace && item.bootId === boot);
        assert.equal(matches.length, 1); assert.equal(matches[0]!.exitCode, 0); assert.equal(matches[0]!.signalCode, null);
        assert.throws(() => process.kill(processIdentity.pid as number, 0), (error: unknown) =>
          error instanceof Error && 'code' in error && error.code === 'ESRCH', 'Owned process is still live or PID reused');
      }
      assert.equal(fs.existsSync('.temp/experiments/schema-evolution/runner.lock'), false);
      assert.ok(evidence.some(item => item.kind === 'native-hydration'), 'Missing row-level native hydration observations');
      const provenance = record(evidence.find(item => item.kind === 'provenance')?.data, 'experiment provenance');
      assert.equal(provenance.target, target);
      if (target === 'local') {
        assert.ok(typeof provenance.runId === 'string' && /^evo[0-9]+_[a-f0-9]{8}$/.test(provenance.runId));
        assert.equal(this.docker(['container', 'ls', '--all', '--no-trunc', '--filter',
          `name=^/w6-${provenance.runId}$`, '--format', '{{.ID}}']).trim(), '', 'Owned Kustainer remains');
      } else await this.ready(provenance);
      this.receipt.evolution = { target, runId: report.runId, passed: true, cleanup: 'ok', ownedPidsAbsent: true };
    } catch (error) { failure = error; }
    try {
      if (fs.existsSync(out)) copyEvolutionEvidence(out, path.join(this.destination, `${target}-evolution`));
    } catch (error) { failure = failure ? new AggregateError([failure, error], 'Evolution and diagnostic export failed') : error; }
    if (failure) throw failure;
  }
  cloudInput(): Config {
    const base = load_env(this.options.envFile!);
    const config = new Config(base, 'schema-evolution');
    if (this.options.subscription)
      assert.equal(config.subscription.toLowerCase(), this.options.subscription.toLowerCase(), 'Private subscription mismatch');
    assert.equal(config.group, this.options.resourceGroup, 'Private resource-group mismatch');
    if (this.options.readyEnv) {
      if (fs.realpathSync(this.options.envFile!) === fs.realpathSync(this.options.readyEnv))
        throw new Error('Base and ready inputs must be distinct files.');
      const ready = load_env(this.options.readyEnv);
      assert.ok(ready.ARROWCOL_RESET_GENERATION && UUID.test(ready.ARROWCOL_RESET_GENERATION),
        'Ready input must contain a genuine reset generation UUID');
      const merged = new Config({ ...base, ...ready }, 'schema-evolution');
      assert.equal(merged.subscription, config.subscription); assert.equal(merged.group, config.group);
      this.cloud = merged;
      this.receipt.readyInputSha256 = hash(this.options.readyEnv);
      this.receipt.generationSha256 = createHash('sha256').update(ready.ARROWCOL_RESET_GENERATION ?? '').digest('hex');
    } else this.cloud = config;
    this.receipt.resourceGroup = config.group;
    this.receipt.subscription = config.subscription;
    this.save();
    return config;
  }
  async ready(provenance?: JsonRecord): Promise<void> {
    const config = this.cloud!, azure = new Azure(config);
    const [metadata, registry] = await new PublisherControl(azure).read();
    const values = load_env(this.options.readyEnv!);
    assert.equal(metadata.sandboxid, config.sandbox); assert.equal(metadata.resetstate, 'ready-v1');
    assert.equal(metadata.resetgeneration, values.ARROWCOL_RESET_GENERATION);
    assert.equal(metadata.occupancy, 'released'); assert.equal(Object.keys(registry).length, 0);
    if (provenance) {
      assert.equal(provenance.group, config.group); assert.equal(provenance.stamp, config.sandbox);
      assert.ok(typeof provenance.runId === 'string' && /^evo[0-9]+_[a-f0-9]{8}$/.test(provenance.runId));
      const blobs = await azure.blobs(config.experimentContainer);
      assert.equal(blobs.some(blob => blob.name.startsWith(`runs/${provenance.runId}/`)), false, 'Owned retained run blobs remain');
      const result = record(await azure.kql('.show external tables | project TableName', config.experimentDatabase), 'Kusto inventory');
      const tables = records(result.Tables, 'Kusto tables'), rows = tables[0]?.Rows;
      assert.ok(Array.isArray(rows));
      assert.equal(rows.some(row => Array.isArray(row) && row[0] === `foo_${provenance.runId}`), false, 'Owned external table remains');
    }
    this.receipt.azureReady = { liveGenerationMatched: true, occupancy: 'released', publishers: 0, runDataAbsent: Boolean(provenance) };
    this.save();
  }
  async azure(reset: boolean): Promise<void> {
    const config = this.cloudInput(), env = privateEnvironment(load_env(this.options.envFile!), this.env, 'schema-evolution');
    await verifyAzureAccount(config.subscription, argv => this.command('azure-account', argv, 60, env));
    const common = ['npx', '--no-install', 'tsx', 'infra/azure/bootstrap.ts'];
    await this.command('azure-preflight', [...common, 'preflight', '--mode', 'schema-evolution', '--env-file', this.options.envFile!], 900, env);
    if (reset) {
      const state = fs.mkdtempSync(path.join(path.dirname(this.destination), 'schema-catalog-ready-'));
      fs.chmodSync(state, 0o700);
      const ready = path.join(state, 'ready.env'), manifest = path.join(state, 'tables.json');
      await this.command('reset-plan', [...common, 'reset', '--mode', 'schema-evolution', '--env-file', this.options.envFile!], 900, env);
      await this.command('reset-explicit-execute', [...common, 'reset', '--mode', 'schema-evolution',
        '--env-file', this.options.envFile!, '--execute'], 5400, env);
      this.receipt.reset = { explicitlyConfirmedGroup: this.options.confirmReset, executed: true, provisioned: false };
      this.save();
      await this.command('reset-provision', [...common, 'provision', '--mode', 'schema-evolution',
        '--env-file', this.options.envFile!, '--output', ready, '--tables', manifest], 10800, env);
      const inputs: JsonRecord = {};
      for (const [name, input] of Object.entries({ base: this.options.envFile!, ready, manifest })) {
        regularFile(input);
        const resolved = fs.realpathSync(input);
        assert.ok(resolved !== this.workspace && !resolved.startsWith(this.workspace + path.sep),
          'Reset private inputs must be outside checkout');
        fs.chmodSync(input, 0o600);
        inputs[name] = { path: resolved, sha256: hash(input) };
      }
      this.options.readyEnv = ready;
      this.cloudInput();
      await this.ready();
      this.receipt.managedInputs = inputs;
      this.receipt.resetGeneration = load_env(ready).ARROWCOL_RESET_GENERATION;
      assert.ok(typeof this.receipt.resetGeneration === 'string' && UUID.test(this.receipt.resetGeneration));
      record(this.receipt.reset, 'reset').provisioned = true;
      this.save();
    } else {
      await this.ready();
      await this.evolution('azure');
    }
  }
  async finish(error?: unknown): Promise<void> {
    this.receipt.stage = error ? 'failed' : 'complete';
    this.receipt.exitCode = error ? 1 : 0;
    this.receipt.finishedAt = new Date().toISOString();
    if (error) this.receipt.error = redactLog(error instanceof Error ? error.message : String(error));
    this.save();
    const members = fs.readdirSync(this.destination).filter(name => name !== 'evidence.tar' && name !== 'SHA256SUMS').sort();
    try {
      const archive = await run_tree(['tar', '-cf', path.join(this.destination, 'evidence.tar'), '-C', this.destination, ...members], 120, this.workspace);
      if (archive.returncode !== 0) throw new Error('Evidence archive creation failed. Original directory retained.');
      fs.writeFileSync(path.join(this.destination, 'SHA256SUMS'),
        `${await hashArchive(path.join(this.destination, 'evidence.tar'))}  evidence.tar\n${hash(path.join(this.destination, 'receipt.json'))}  receipt.json\n`,
        { mode: 0o600 });
    } catch (exportError) {
      this.receipt.stage = 'failed'; this.receipt.exitCode = 1;
      this.receipt.exportError = redactLog(exportError instanceof Error ? exportError.message : String(exportError));
      this.save();
      fs.writeFileSync(path.join(this.destination, 'SHA256SUMS'), `${hash(path.join(this.destination, 'receipt.json'))}  receipt.json\n`,
        { mode: 0o600 });
      throw exportError;
    }
    console.log(`Evidence: ${this.destination}/{evidence.tar,receipt.json,SHA256SUMS}`);
  }
}

export async function main(argv = process.argv.slice(2)): Promise<void> {
  const options = parseOptions(argv);
  if (process.platform !== 'linux' || !fs.existsSync('/.dockerenv'))
    throw new Error('Run inside the Linux repository devcontainer, not on the host.');
  const workspace = fs.realpathSync(path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../..'));
  process.chdir(workspace);
  const rehearsal = new Rehearsal(options, workspace);
  let failure: unknown;
  try {
    rehearsal.prepareCloudInputs();
    const verified = options.mode === 'verify' ? undefined : rehearsal.verified();
    const sha = options.mode === 'verify' ? options.expectedSha ??
      (await rehearsal.command('expected-checkout-head', ['git', 'rev-parse', 'HEAD'], 30)).trim() : verified!.sha;
    assert.match(sha, SHA);
    await rehearsal.source(sha);
    if (options.mode === 'verify') await rehearsal.verify(sha);
    else if (options.mode === 'local') await rehearsal.local(verified!.images, verified!.services, sha);
    else await rehearsal.azure(options.mode === 'reset');
    await rehearsal.source(sha);
  } catch (error) { failure = error; }
  try { await rehearsal.finish(failure); }
  catch (error) { failure = failure ? new AggregateError([failure, error], 'Bug bash and evidence export failed') : error; }
  if (failure) throw failure;
  console.log('Schema catalog bug bash: passed');
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url))
  main().catch(error => { console.error(error instanceof Error ? error.message : String(error)); process.exitCode = 1; });
