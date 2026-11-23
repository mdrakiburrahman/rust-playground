import { spawn } from 'node:child_process';
import { createHash, randomUUID } from 'node:crypto';
import {
  constants, copyFileSync, existsSync, linkSync, lstatSync, mkdirSync, openSync,
  closeSync, fsyncSync, readFileSync, readdirSync, renameSync, unlinkSync, writeFileSync,
} from 'node:fs';
import { dirname, isAbsolute, join, relative, resolve, sep, win32 } from 'node:path';
import { exportCompiler, generateWithProtoc, protocProvenance, validateCompiler } from './protoc.js';

export const contractRoot = '.temp/contract-builder';
export const uuidPattern = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
export interface Registry {
  id: string;
  label: string;
  authority: string;
  endpoint: string;
  mode: 'owned' | 'external';
}
export interface ContractConfig {
  registries: Registry[];
  hostWorkspace: string;
}
export type BuildRequest = (
  | { registry: string; mode: 'create'; ddl: string; definitionsDigest?: string }
  | { registry: string; mode: 'rebuild'; table: string; version: number }
) & { codegen?: 'protoc'; protoEntry?: string };
export interface Pin {
  table: string;
  tableId: string;
  version: number;
  fingerprint: string;
}
export interface ContractRun {
  id: string;
  registry: Registry;
  request: BuildRequest;
  status: 'queued' | 'running' | 'succeeded' | 'failed';
  phase: string;
  registration: 'not-attempted' | 'unknown' | 'committed' | 'existing';
  createdAt: string;
  startedAt?: string;
  finishedAt?: string;
  pid?: number;
  pin?: Pin;
  definitionsDigest?: string;
  staged?: { sha256: string; bytes: number; candidateSha256?: string };
  artifact?: { path: string; hostPath: string; sha256: string; bytes: number };
  error?: string;
  steps?: { phase: string; status: 'running' | 'succeeded' | 'failed'; startedAt: string; finishedAt?: string; error?: string }[];
}
export function setRunPhase(workspace: string, run: ContractRun, phase: string): void {
  const now = new Date().toISOString();
  const active = run.steps?.at(-1);
  if (active?.status === 'running') {
    active.status = 'succeeded';
    active.finishedAt = now;
  }
  run.steps ??= [];
  run.phase = phase;
  if (phase !== 'complete') run.steps.push({ phase, status: 'running', startedAt: now });
  saveRun(workspace, run);
}
export function failRunStep(run: ContractRun): void {
  const active = run.steps?.at(-1);
  if (active?.status === 'running') {
    active.status = 'failed';
    active.finishedAt = new Date().toISOString();
    active.error = run.error;
  }
}
export function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : String(error);
}
export function digest(data: string | Buffer): string {
  return createHash('sha256').update(data).digest('hex');
}
export function ensureDirectory(workspace: string, directory: string): void {
  const root = resolve(workspace);
  const target = resolve(workspace, directory);
  const parts = relative(root, target).split(sep);
  if (parts.some((part) => part === '..') || !target.startsWith(`${root}${sep}`)) {
    throw new Error('Storage must be inside the workspace');
  }
  let current = root;
  for (const part of parts) {
    current = join(current, part);
    if (!existsSync(current)) mkdirSync(current);
    if (lstatSync(current).isSymbolicLink() || !lstatSync(current).isDirectory()) {
      throw new Error(`Storage is not a regular directory: ${current}`);
    }
  }
}
export function writeJson(path: string, value: unknown): void {
  const stage = `${path}.${randomUUID()}.tmp`;
  const file = openSync(stage, 'wx');
  try {
    writeFileSync(file, `${JSON.stringify(value, null, 2)}\n`);
    fsyncSync(file);
  } finally { closeSync(file); }
  renameSync(stage, path);
}
export function loadConfig(workspace = process.cwd()): ContractConfig {
  ensureDirectory(workspace, contractRoot);
  const path = resolve(workspace, contractRoot, 'config.json');
  if (!existsSync(path)) {
    const config: ContractConfig = {
      hostWorkspace: process.env.CONTRACT_HOST_WORKSPACE || resolve(workspace),
      registries: [
        { id: 'local', label: 'Local catalogue', authority: randomUUID(), endpoint: 'http://127.0.0.1:18817', mode: 'owned' },
        { id: 'hosted', label: 'Hosted simulation', authority: randomUUID(), endpoint: 'http://127.0.0.1:18818', mode: 'owned' },
      ],
    };
    try {
      writeFileSync(path, `${JSON.stringify(config, null, 2)}\n`, { flag: 'wx' });
    } catch (error) {
      if (!isCode(error, 'EEXIST')) throw error;
    }
  }
  const config: ContractConfig = JSON.parse(readFileSync(path, 'utf8'));
  if (!config || !Array.isArray(config.registries) || !config.registries.length ||
      typeof config.hostWorkspace !== 'string' ||
      (!isAbsolute(config.hostWorkspace) && !win32.isAbsolute(config.hostWorkspace))) {
    throw new Error('Invalid contract-builder config');
  }
  const ids = new Set<string>();
  const authorities = new Set<string>();
  const endpoints = new Set<string>();
  for (const registry of config.registries) {
    const url = new URL(registry.endpoint);
    if (!/^[a-z][a-z0-9-]{0,40}$/.test(registry.id) || ids.has(registry.id) ||
        typeof registry.label !== 'string' || !registry.label ||
        typeof registry.authority !== 'string' || !registry.authority || authorities.has(registry.authority) ||
        !['http:', 'https:'].includes(url.protocol) || url.username || url.password ||
        url.search || url.hash || url.pathname !== '/' ||
        !['owned', 'external'].includes(registry.mode) || endpoints.has(url.origin)) {
      throw new Error(`Invalid or duplicate registry configuration: ${registry.id}`);
    }
    if (registry.mode === 'owned' &&
        (url.hostname !== '127.0.0.1' || url.protocol !== 'http:' || !url.port)) {
      throw new Error('Owned registries require distinct http://127.0.0.1:port endpoints');
    }
    ids.add(registry.id);
    authorities.add(registry.authority);
    endpoints.add(url.origin);
  }
  return config;
}
export function isCode(error: unknown, code: string): boolean {
  return error instanceof Error && 'code' in error && error.code === code;
}
export function parseRequest(value: unknown): BuildRequest {
  if (!value || typeof value !== 'object' || !('registry' in value) ||
      typeof value.registry !== 'string' || !('mode' in value)) {
    throw new Error('A registry and create/rebuild mode are required');
  }
  if ('codegen' in value && value.codegen !== 'direct' && value.codegen !== 'protoc') {
    throw new Error('Unknown codegen backend; use direct or protoc');
  }
  let codegen: { codegen?: 'protoc'; protoEntry?: string } = {};
  if ('codegen' in value && value.codegen === 'protoc') {
    const entry = 'protoEntry' in value ? value.protoEntry : 'schema.proto';
    if (typeof entry !== 'string' || !/^[a-zA-Z0-9_][a-zA-Z0-9_.-]*\.proto$/.test(entry) ||
        ['options.proto', 'arrowcol_codegen.proto', 'descriptor.proto'].includes(entry)) {
      throw new Error('protoEntry must be a non-reserved .proto basename');
    }
    codegen = { codegen: 'protoc', protoEntry: entry };
  } else if ('protoEntry' in value) {
    throw new Error('protoEntry requires the protoc backend');
  }
  if (value.mode === 'create' && 'ddl' in value && typeof value.ddl === 'string' &&
      value.ddl.trim().length > 0 && Buffer.byteLength(value.ddl) <= 64 * 1024) {
    if ('definitionsDigest' in value &&
        (typeof value.definitionsDigest !== 'string' || !/^sha256:[0-9a-f]{64}$/.test(value.definitionsDigest))) {
      throw new Error('Invalid compiled option definitions digest');
    }
    return {
      registry: value.registry, mode: 'create', ddl: value.ddl,
      ...codegen,
      ...('definitionsDigest' in value ? { definitionsDigest: String(value.definitionsDigest) } : {}),
    };
  }
  if (value.mode === 'rebuild' && 'table' in value && typeof value.table === 'string' &&
      /^[a-zA-Z_][\w]*\.[a-zA-Z_][\w]*\.[a-zA-Z_][\w]*$/.test(value.table) &&
      'version' in value && typeof value.version === 'number' &&
      Number.isSafeInteger(value.version) && value.version > 0) {
    return { registry: value.registry, mode: 'rebuild', table: value.table, version: value.version, ...codegen };
  }
  throw new Error('Use CREATE TABLE DDL (max 64 KiB), or a table and positive committed version');
}
export function runDirectory(workspace: string, id: string): string {
  if (!uuidPattern.test(id)) throw new Error('Invalid run ID');
  return resolve(workspace, contractRoot, 'runs', id);
}
export function saveRun(workspace: string, run: ContractRun): void {
  writeJson(join(runDirectory(workspace, run.id), 'run.json'), run);
}
export function readRun(workspace: string, id: string): ContractRun {
  const path = join(runDirectory(workspace, id), 'run.json');
  for (let attempt = 0; ; attempt++) {
    try { return JSON.parse(readFileSync(path, 'utf8')); } catch (error) {
      if (!isCode(error, 'ENOENT') || attempt >= 5) throw error;
      // WSL Windows mounts can briefly hide the destination during replacement.
      Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 20);
    }
  }
}
export function listRuns(workspace: string): ContractRun[] {
  const directory = join(contractRoot, 'runs');
  ensureDirectory(workspace, directory);
  return readdirSync(resolve(workspace, directory)).filter((id) => uuidPattern.test(id))
    .map((id) => readRun(workspace, id))
    .sort((a, b) => b.createdAt.localeCompare(a.createdAt));
}
export function enqueue(workspace: string, input: unknown): ContractRun {
  const request = parseRequest(input);
  const registry = loadConfig(workspace).registries.find((item) => item.id === request.registry);
  if (!registry) throw new Error('Unknown registry');
  const run: ContractRun = {
    id: randomUUID(), registry, request, status: 'queued', phase: 'queued',
    registration: 'not-attempted', createdAt: new Date().toISOString(), steps: [],
  };
  ensureDirectory(workspace, relative(workspace, runDirectory(workspace, run.id)));
  saveRun(workspace, run);
  return run;
}

export type Execute = (command: string, args: string[], log?: string, env?: NodeJS.ProcessEnv) => Promise<string>;
export function commandRunner(workspace: string): Execute {
  return (command, args, log, environment) => new Promise((resolvePromise, reject) => {
    if (log) writeFileSync(log, `$ ${JSON.stringify([command, ...args])}\n`, { flag: 'a' });
    const child = spawn(command, args, {
      cwd: workspace, env: { ...process.env, ...environment }, shell: false, stdio: ['ignore', 'pipe', 'pipe'],
    });
    let output = '';
    let tail = '';
    child.stdout.on('data', (data: Buffer) => {
      output += data.toString();
      if (output.length > 16 * 1024 * 1024) {
        child.kill('SIGTERM');
        reject(new Error('Command exceeded the 16 MiB output limit'));
      }
      if (log) writeFileSync(log, data, { flag: 'a' });
    });
    child.stderr.on('data', (data: Buffer) => {
      tail = (tail + data.toString()).slice(-8000);
      if (log) writeFileSync(log, data, { flag: 'a' });
    });
    child.on('error', reject);
    child.on('close', (code, signal) => {
      if (code === 0) resolvePromise(output);
      else reject(new Error(`${command} exited ${code ?? signal}: ${tail || output.slice(-8000)}`));
    });
  });
}
export function generatorPath(workspace: string): string {
  return resolve(workspace, 'target', 'debug', process.platform === 'win32' ? 'arrowcol-sdk-gen.exe' : 'arrowcol-sdk-gen');
}
export function registryEnvironment(workspace: string, registry: Registry): NodeJS.ProcessEnv {
  return registry.mode === 'owned' ? {
    ARROWCOL_CATALOG_DDL_IDENTITY_SECRET_FILE: resolve(workspace, contractRoot, 'registries', digest(registry.authority), 'identity.key'),
  } : {};
}
export function hostPath(config: ContractConfig, path: string): string {
  return /^[a-zA-Z]:[\\/]/.test(config.hostWorkspace)
    ? win32.join(config.hostWorkspace, ...path.split(/[\\/]/))
    : resolve(config.hostWorkspace, path);
}
export function publishArchive(candidate: string, destination: string): void {
  const stage = join(dirname(destination), `.${randomUUID()}.stage`);
  copyFileSync(candidate, stage, constants.COPYFILE_EXCL);
  try {
    linkSync(stage, destination);
  } catch (error) {
    if (!isCode(error, 'EEXIST')) throw error;
    if (lstatSync(destination).isSymbolicLink() ||
        !readFileSync(stage).equals(readFileSync(destination))) {
      throw new Error('Immutable release collision: existing bytes differ');
    }
  } finally {
    unlinkSync(stage);
  }
}
export async function buildContract(
  workspace: string, id: string, execute: Execute = commandRunner(workspace),
): Promise<ContractRun> {
  const run = readRun(workspace, id);
  if (run.status !== 'queued') throw new Error('Only a queued run can execute; create a rebuild run to regenerate');
  const directory = runDirectory(workspace, id);
  const claim = join(directory, 'claim');
  const fd = openSync(claim, 'wx');
  closeSync(fd);
  const log = join(directory, 'build.log');
  const relativePath = (path: string) => relative(workspace, path);
  const generator = async (args: string[]) => execute(generatorPath(workspace), args, log, registryEnvironment(workspace, run.registry));
  const phase = (value: string) => setRunPhase(workspace, run, value);
  run.status = 'running';
  run.startedAt = new Date().toISOString();
  run.pid = process.pid;
  try {
    phase('validating');
    const provenance = {
      generatorSha256: digest(readFileSync(generatorPath(workspace))),
      workflowSha256: digest(readFileSync(resolve(workspace, 'tools/libs/arrowcol-sdk-gen/src/contract.ts'))),
      dependencyLockSha256: digest(readFileSync(resolve(workspace, 'package-lock.json'))),
      optionDefinitionsSha256: '',
      ...(run.request.codegen === 'protoc'
        ? { codegen: await protocProvenance(workspace, execute, log) } : {}),
    };
    const definitions: { definitions_sha256: string } = JSON.parse(await generator(['catalog', 'properties']));
    if (!/^sha256:[0-9a-f]{64}$/.test(definitions.definitions_sha256)) throw new Error('Invalid compiled option definitions');
    if (run.request.mode === 'create' && run.request.definitionsDigest &&
        run.request.definitionsDigest !== definitions.definitions_sha256) {
      throw new Error('Property definitions changed since the form was loaded; refresh and review the new defaults');
    }
    run.definitionsDigest = definitions.definitions_sha256;
    provenance.optionDefinitionsSha256 = definitions.definitions_sha256;
    const config = loadConfig(workspace);
    const currentRegistry = config.registries.find((item) => item.id === run.registry.id);
    if (JSON.stringify(currentRegistry) !== JSON.stringify(run.registry)) {
      throw new Error('Registry configuration changed since queueing; submit a new run');
    }
    const payload = join(directory, 'payload');
    ensureDirectory(workspace, relativePath(payload));
    const artifacts = join(payload, 'contract');
    const sdk = join(payload, 'sdk');
    const compiler = join(payload, 'compiler');
    const entry = run.request.protoEntry ?? 'schema.proto';
    let pin: Pin;
    if (run.request.mode === 'create') {
      const ddl = join(directory, 'definition.sql');
      writeFileSync(ddl, run.request.ddl, { flag: 'wx' });
      await generator(['catalog', 'validate', '--ddl', relativePath(ddl)]);
      phase('preparing');
      pin = JSON.parse(await generator([
        'catalog', 'prepare', '--endpoint', run.registry.endpoint,
        '--ddl', relativePath(ddl), '--out', relativePath(artifacts),
      ]));
      renameSync(join(artifacts, 'candidate.pb'), join(directory, 'candidate.pb'));
    } else {
      phase('pinning');
      pin = JSON.parse(await generator([
        'catalog', 'export', '--endpoint', run.registry.endpoint, '--table', run.request.table,
        '--version', String(run.request.version), '--out', relativePath(artifacts),
      ]));
      if (pin.table !== run.request.table || pin.version !== run.request.version) {
        throw new Error('Catalog returned a different committed version');
      }
    }
    if (!uuidPattern.test(pin.tableId) || !Number.isSafeInteger(pin.version) || pin.version < 1 ||
        (run.request.mode === 'create' && pin.version !== 1) ||
        typeof pin.fingerprint !== 'string' || !pin.fingerprint.length) {
      throw new Error('Catalog returned an invalid committed pin');
    }
    run.pin = pin;
    if (run.request.mode === 'rebuild') run.registration = 'existing';
    if (run.request.codegen === 'protoc') {
      phase('exporting-proto');
      await exportCompiler(workspace, artifacts, compiler, entry, execute, log);
      phase('validating-proto');
      await validateCompiler(workspace, compiler, entry, join(directory, 'validated-descriptor.pb'), execute, log);
    }
    phase('generating');
    const generate = () => run.request.codegen === 'protoc'
      ? generateWithProtoc(workspace, artifacts, compiler, sdk, entry, execute, log)
      : generator(['generate', '--artifacts', relativePath(artifacts), '--out', relativePath(sdk)]);
    await generate();
    phase('verifying');
    await generator(['check', '--artifacts', relativePath(artifacts), '--out', relativePath(sdk)]);
    await execute(process.execPath, [
      'node_modules/typescript/bin/tsc', '--project', join(sdk, 'typescript', 'tsconfig.json'), '--noEmit',
    ], log);
    await execute(process.execPath, [
      '--import', 'tsx', '--test', join(sdk, 'typescript', 'test', 'generated.test.ts'),
    ], log);
    await execute('cargo', ['test', '--manifest-path', join(sdk, 'rust', 'Cargo.toml')], log, {
      CARGO_TARGET_DIR: resolve(workspace, 'target/arrowcol-sdk-gen-validation-cargo-target'),
    });
    // Cargo creates a lockfile in the generated crate. Package only canonical SDK output.
    if (run.request.codegen === 'protoc' && existsSync(join(sdk, 'rust', 'Cargo.lock'))) {
      unlinkSync(join(sdk, 'rust', 'Cargo.lock'));
    }
    await generate();
    await generator(['check', '--artifacts', relativePath(artifacts), '--out', relativePath(sdk)]);
    const manifest: { schema: { metadata: { properties: Record<string, string> } } } =
      JSON.parse(readFileSync(join(artifacts, 'manifest.json'), 'utf8'));
    writeJson(join(payload, 'release.json'), {
      formatVersion: 1,
      authority: run.registry.authority,
      endpoint: run.registry.endpoint,
      ...pin,
      client: { transport: 'Arrow Flight', languages: ['rust', 'typescript'], sdk: 'sdk/sdk-manifest.json' },
      backendPolicy: {
        properties: manifest.schema.metadata.properties,
        note: 'Recorded catalog metadata; this workflow does not add policy enforcement.',
      },
      provenance,
    });
    if (digest(readFileSync(generatorPath(workspace))) !== provenance.generatorSha256) {
      throw new Error('Generator changed during the build; the candidate has not been promoted');
    }
    if (provenance.codegen && JSON.stringify(await protocProvenance(workspace, execute, log)) !== JSON.stringify(provenance.codegen)) {
      throw new Error('Protoc toolchain changed during the build; the candidate has not been promoted');
    }
    phase('packaging');
    const candidate = join(directory, 'contract-sdk.tar.gz');
    const packaged: { sha256: string; bytes: number } = JSON.parse(await generator([
      'package-artifact', '--input', relativePath(payload), '--out', relativePath(candidate),
    ]));
    const sha256 = digest(readFileSync(candidate));
    if (packaged.sha256 !== `sha256:${sha256}`) throw new Error('Packaged digest mismatch');
    run.staged = {
      sha256, bytes: packaged.bytes,
      ...(run.request.mode === 'create'
        ? { candidateSha256: digest(readFileSync(join(directory, 'candidate.pb'))) } : {}),
    };
    saveRun(workspace, run);
    await finalizeContract(workspace, run, execute);
  } catch (error) {
    run.status = 'failed';
    run.error = errorMessage(error);
    failRunStep(run);
    writeFileSync(log, `ERROR: ${run.error}\n`, { flag: 'a' });
  } finally {
    run.finishedAt = new Date().toISOString();
    saveRun(workspace, run);
  }
  return run;
}

async function finalizeContract(workspace: string, run: ContractRun, execute: Execute): Promise<void> {
  if (!run.staged || !run.pin) throw new Error('No verified staged release to finalize');
  setRunPhase(workspace, run, 'finalizing');
  const config = loadConfig(workspace);
  if (JSON.stringify(config.registries.find((item) => item.id === run.registry.id)) !== JSON.stringify(run.registry)) {
    throw new Error('Registry changed since preparation; cannot promote this candidate');
  }
  const directory = runDirectory(workspace, run.id);
  const definitions: { definitions_sha256: string } = JSON.parse(await execute(
    generatorPath(workspace), ['catalog', 'properties'], join(directory, 'build.log'),
  ));
  if (definitions.definitions_sha256 !== run.definitionsDigest) {
    throw new Error('Compiled option definitions changed after preparation; restore the pinned definitions before finalizing');
  }
  const candidateArchive = join(directory, 'contract-sdk.tar.gz');
  if (digest(readFileSync(candidateArchive)) !== run.staged.sha256) throw new Error('Staged archive digest mismatch');
  if (run.request.mode === 'create') {
    const candidate = join(directory, 'candidate.pb');
    if (digest(readFileSync(candidate)) !== run.staged.candidateSha256) throw new Error('Prepared candidate digest mismatch');
    run.registration = 'unknown';
    setRunPhase(workspace, run, 'promoting');
    await execute(generatorPath(workspace), [
      'catalog', 'promote', '--endpoint', run.registry.endpoint, '--candidate', relative(workspace, candidate),
    ], join(directory, 'build.log'), registryEnvironment(workspace, run.registry));
    run.registration = 'committed';
  }
  setRunPhase(workspace, run, 'publishing');
  const releaseDirectory = join(
    contractRoot, 'releases', digest(run.registry.authority), run.pin.tableId,
    `v${run.pin.version}`, run.staged.sha256,
  );
  ensureDirectory(workspace, releaseDirectory);
  const archive = join(releaseDirectory, 'contract-sdk.tar.gz');
  publishArchive(candidateArchive, resolve(workspace, archive));
  run.artifact = {
    path: archive, hostPath: hostPath(config, archive),
    sha256: run.staged.sha256, bytes: run.staged.bytes,
  };
  run.status = 'succeeded';
  setRunPhase(workspace, run, 'complete');
}

export async function resumePromotion(
  workspace: string, id: string, execute: Execute = commandRunner(workspace),
): Promise<ContractRun> {
  const run = readRun(workspace, id);
  if (run.status !== 'failed' || !run.staged || !['packaging', 'finalizing', 'promoting', 'publishing'].includes(run.phase)) {
    throw new Error('Only a failed promotion/publication with a verified staged archive can resume');
  }
  const claim = join(runDirectory(workspace, id), 'resume-claim');
  closeSync(openSync(claim, 'wx'));
  run.status = 'running';
  run.pid = process.pid;
  delete run.error;
  delete run.finishedAt;
  try {
    await finalizeContract(workspace, run, execute);
  } catch (error) {
    run.status = 'failed';
    run.error = errorMessage(error);
    failRunStep(run);
  } finally {
    run.finishedAt = new Date().toISOString();
    saveRun(workspace, run);
    unlinkSync(claim);
  }
  return run;
}

export function recoverRuns(workspace: string): void {
  for (const observed of listRuns(workspace)) {
    if (observed.status !== 'running') continue;
    if (observed.pid) {
      try {
        process.kill(observed.pid, 0);
        continue;
      } catch (error) {
        if (!isCode(error, 'ESRCH')) throw error;
      }
    }
    // The worker may have persisted its final result before exiting during the probe.
    const run = readRun(workspace, observed.id);
    if (run.status !== 'running' || run.pid !== observed.pid) continue;
    run.status = 'failed';
    run.error = run.staged
      ? 'Build interrupted; verified staged bytes are retained. Explicitly resume promotion/publication for this run; no DDL is replayed.'
      : 'Build interrupted before publication. No automatic replay; inspect the run phase and catalogue.';
    run.finishedAt = new Date().toISOString();
    failRunStep(run);
    saveRun(workspace, run);
  }
}
