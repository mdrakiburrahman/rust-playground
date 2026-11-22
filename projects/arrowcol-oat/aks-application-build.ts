import { createHash, randomUUID } from 'node:crypto';
import { lstat, mkdir, mkdtemp, readFile, realpath, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { isAbsolute, join, relative, resolve } from 'node:path';
import { parseArgs } from 'node:util';
import { pathToFileURL } from 'node:url';
import { imageBuilds } from './publish-images.js';
import { readOutput, snapshotSource, type BuildOutput, type SourceMaterial } from './aks-candidate-build.js';
import { ROOT, run_tree } from '../../infra/azure/bootstrap.js';

type Role = typeof imageBuilds[number][0];
type Runner = typeof run_tree;
type Json = Record<string, unknown>;
export const installedExecutables: Readonly<Record<Role, readonly string[]>> = {
  sqlserver: [], catalog: ['/usr/local/bin/arrowcol-catalog-rust'],
  router: ['/usr/local/bin/arrowcol-router-rust'], dataflow: ['/usr/local/bin/arrowcol-dataflow-rust'],
  client: ['/usr/local/bin/arrowcol-dmv', '/usr/local/bin/arrowcol-oat-pin-init'],
  maintenance: ['/usr/local/bin/delta-maintenance'],
};
export interface ApplicationEvidence {
  schema_version: 1;
  source_commit: string;
  roles: Array<{
    role: Role; image_digest: string; platform_digest: string; config_digest: string;
    build_invocation_sha256: string;
    comparison: 'changed-code' | 'source-only';
    executables: Array<{ path: string; sha256: string; baseline_sha256?: string }>;
  }>;
}
export interface LocalApplicationBuild {
  schema_version: 1; scope: 'local-application-proof' | 'local-fixture';
  source_commit: string; materials: SourceMaterial[];
  builds: Array<{ output: BuildOutput; invocation: string[]; archive: string }>;
}
function object(value: unknown, label: string): Json {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error(`Invalid ${label}.`);
  return value as Json;
}
function sha(bytes: Buffer | string): string { return createHash('sha256').update(bytes).digest('hex'); }
function digest(value: unknown): string {
  if (typeof value !== 'string' || !/^sha256:[a-f0-9]{64}$/.test(value)) throw new Error('Invalid immutable application image digest.');
  return value;
}
function hash(value: unknown): string {
  if (typeof value !== 'string' || !/^[a-f0-9]{64}$/.test(value)) throw new Error('Invalid executable or invocation SHA-256.');
  return value;
}
async function checked(runner: Runner, args: string[], cwd: string, seconds = 30): Promise<string> {
  const result = await runner(args, seconds, cwd);
  if (result.returncode) throw new Error(`${args[0]} ${args[1]} failed (${result.returncode}); inspect the local build diagnostics.`);
  return result.stdout;
}

export async function isolatedBuild(args: string[], cwd: string, runner: Runner = run_tree): Promise<string[]> {
  if (args[0] !== 'docker' || args[1] !== 'buildx' || args[2] !== 'build' ||
      args.some(arg => ['--builder', '--cache-from', '--cache-to', '--secret', '--ssh', '--allow', '--build-context'].includes(arg.split('=')[0]!))) {
    throw new Error('Application build requires a fresh builder without imported caches, secrets or external contexts.');
  }
  const builder = 'arrowcol-application-' + randomUUID();
  await checked(runner, ['docker', 'buildx', 'create', '--name', builder, '--driver', 'docker-container'], cwd);
  const invocation = [...args.slice(0, 3), '--builder', builder, ...args.slice(3)];
  try {
    await checked(runner, invocation, cwd, 4 * 3600);
    return invocation;
  } finally {
    await checked(runner, ['docker', 'buildx', 'rm', builder], cwd, 120);
  }
}

export async function captureInstalledExecutables(role: Role, platform: BuildOutput['platforms'][number],
  invocation: string[], revision: string, cwd: string, runner: Runner = run_tree): Promise<NonNullable<BuildOutput['application']>> {
  const config = digest(platform.config_digest), manifest = digest(platform.digest);
  const inspect = object(JSON.parse(await checked(runner, ['docker', 'image', 'inspect', '--format', '{{json .}}', config], cwd)), 'built image');
  if (inspect.Id !== config || inspect.Os !== platform.os || inspect.Architecture !== platform.architecture ||
      platform.os !== 'linux' || platform.architecture !== 'amd64') throw new Error('Loaded image is not the exact completed platform config.');
  const result: NonNullable<BuildOutput['application']> = {
    schema_version: 1, source_commit: revision, build_invocation_sha256: sha(JSON.stringify(invocation)),
    isolation: 'fresh_docker_container_builder', executables: [],
  };
  if (!installedExecutables[role].length) {
    result.unavailable_reason = 'Role has no repository-owned installed executable; no changed-code qualification.';
    return result;
  }
  const directory = await mkdtemp(join(tmpdir(), 'arrowcol-executable-'));
  let container: string | undefined;
  try {
    container = (await checked(runner, ['docker', 'create', '--pull', 'never', '--network', 'none',
      '--entrypoint', '/bin/true', config], cwd)).trim();
    if (!/^[a-f0-9]{64}$/.test(container)) throw new Error('Invalid stopped extraction container identity.');
    for (const [index, path] of installedExecutables[role].entries()) {
      const file = join(directory, String(index));
      await checked(runner, ['docker', 'cp', container + ':' + path, file], cwd);
      const stat = await lstat(file);
      if (!stat.isFile() || stat.isSymbolicLink() || stat.size === 0) throw new Error('Installed executable is missing, empty or not a regular file.');
      result.executables.push({ path, sha256: sha(await readFile(file)), platform_digest: manifest, config_digest: config });
    }
    return result;
  } finally {
    try {
      if (container && /^[a-f0-9]{64}$/.test(container)) await checked(runner, ['docker', 'rm', '--volumes', container], cwd);
    } finally { await rm(directory, { recursive: true, force: true }); }
  }
}

export function assertApplicationEvidence(output: unknown, rawExpected: unknown, revision: string): ApplicationEvidence['roles'][number] {
  const actual = object(output, 'application output'), expected = object(rawExpected, 'independent application evidence');
  const role = imageBuilds.find(([name]) => name === actual.role)?.[0];
  if (!role || expected.schema_version !== 1 || expected.source_commit !== revision || !Array.isArray(expected.roles)) {
    throw new Error('Independent application evidence does not bind the requested source and role.');
  }
  const matches = expected.roles.filter(value => object(value, 'expected role').role === role);
  if (matches.length !== 1) throw new Error('Expected application role must be unique and complete.');
  const wanted = object(matches[0], 'expected role');
  const application = object(actual.application, 'installed executable binding');
  const platforms = actual.platforms;
  if (!Array.isArray(platforms) || platforms.length !== 1) throw new Error('Application must bind one exact platform.');
  const platform = object(platforms[0], 'application platform');
  if (application.schema_version !== 1 || application.source_commit !== revision ||
      application.isolation !== 'fresh_docker_container_builder' ||
      digest(wanted.image_digest) !== digest(actual.digest) ||
      digest(wanted.platform_digest) !== digest(platform.digest) ||
      digest(wanted.config_digest) !== digest(platform.config_digest) ||
      hash(wanted.build_invocation_sha256) !== hash(application.build_invocation_sha256) ||
      platform.os !== 'linux' || platform.architecture !== 'amd64' ||
      !['changed-code', 'source-only'].includes(String(wanted.comparison)) ||
      !Array.isArray(wanted.executables) || !Array.isArray(application.executables)) {
    throw new Error('Independent application evidence differs from the isolated source/build/image binding.');
  }
  const paths = installedExecutables[role];
  if (wanted.executables.length !== paths.length || application.executables.length !== paths.length) {
    throw new Error('Installed executable evidence is incomplete for the publisher role.');
  }
  let changed = false;
  for (const path of paths) {
    const observed = application.executables.filter(value => object(value, 'observed executable').path === path);
    const required = wanted.executables.filter(value => object(value, 'expected executable').path === path);
    if (observed.length !== 1 || required.length !== 1) throw new Error('Installed executable path is missing or duplicated.');
    const a = object(observed[0], 'observed executable'), e = object(required[0], 'expected executable');
    if (hash(a.sha256) !== hash(e.sha256) || a.platform_digest !== platform.digest || a.config_digest !== platform.config_digest) {
      throw new Error('Installed executable SHA-256 does not match independent expected evidence.');
    }
    if (wanted.comparison === 'changed-code') {
      const baseline = hash(e.baseline_sha256);
      changed ||= baseline !== a.sha256;
    }
  }
  if (wanted.comparison === 'changed-code' && !changed) {
    throw new Error('Changed-code comparison has identical or unavailable executable bytes; refusing qualification.');
  }
  return {
    role, image_digest: digest(wanted.image_digest), platform_digest: digest(wanted.platform_digest),
    config_digest: digest(wanted.config_digest), build_invocation_sha256: hash(wanted.build_invocation_sha256),
    comparison: wanted.comparison === 'changed-code' ? 'changed-code' : 'source-only',
    executables: wanted.executables.map(value => {
      const executable = object(value, 'expected executable');
      return { path: String(executable.path), sha256: hash(executable.sha256),
        ...(executable.baseline_sha256 !== undefined ? { baseline_sha256: hash(executable.baseline_sha256) } : {}) };
    }),
  };
}

async function archiveObject(archive: string, directory: string, path: string, runner: Runner): Promise<Buffer> {
  if (!/^(?:index\.json|blobs\/sha256\/[a-f0-9]{64})$/.test(path)) throw new Error('Unsafe OCI metadata path.');
  await checked(runner, ['tar', '--extract', '--no-same-owner', '--file', archive, '--directory', directory, path], directory);
  const file = join(directory, path), stat = await lstat(file);
  const canonical = await realpath(file), r = relative(await realpath(directory), canonical);
  if (!stat.isFile() || stat.isSymbolicLink() || isAbsolute(r) || r.startsWith('..') || stat.size > 16 * 1024 * 1024) {
    throw new Error('Unsafe or oversized OCI metadata.');
  }
  return readFile(file);
}

export async function buildLocalApplications(options: { sourceRoot: string; revision: string; output: string;
  roles: Role[]; fixture?: boolean }, runner: Runner = run_tree): Promise<LocalApplicationBuild> {
  if (!options.roles.length || new Set(options.roles).size !== options.roles.length ||
      options.roles.some(role => !imageBuilds.some(([name]) => name === role))) throw new Error('Select unique known publisher roles.');
  const output = resolve(options.output), source = await realpath(options.sourceRoot);
  const location = relative(source, output);
  if (!isAbsolute(location) && location !== '..' && !location.startsWith('../')) throw new Error('Application proof output must be outside candidate source.');
  await mkdir(output, { recursive: false, mode: 0o700 });
  const directory = await mkdtemp(join(tmpdir(), 'arrowcol-application-source-'));
  try {
    const materials = await snapshotSource({ ...options, scope: 'local-fixture' }, directory, runner);
    const bundle: LocalApplicationBuild = { schema_version: 1, scope: options.fixture ? 'local-fixture' : 'local-application-proof',
      source_commit: options.revision, materials, builds: [] };
    for (const role of options.roles) {
      const definition = imageBuilds.find(([name]) => name === role)!;
      const root = join(directory, 'context'), file = await realpath(join(root, definition[1])), context = await realpath(join(root, definition[2]));
      if ([file, context].some(path => { const r = relative(root, path); return isAbsolute(r) || r.startsWith('..'); })) {
        throw new Error('Application recipe escapes immutable context.');
      }
      const archive = join(output, role + '.oci.tar'), metadataPath = join(output, role + '.metadata.json');
      const invocation = await isolatedBuild(['docker', 'buildx', 'build', '--platform', 'linux/amd64',
        '--provenance=false', '--sbom=false', '--load', '--output', 'type=oci,dest=' + archive,
        '--metadata-file', metadataPath, '--build-arg', 'VCS_REF=' + options.revision, '--file', file, context], root, runner);
      const metadata = object(JSON.parse(await readFile(metadataPath, 'utf8')), 'completed application build');
      const index = object(JSON.parse((await archiveObject(archive, output, 'index.json', runner)).toString('utf8')), 'OCI archive index');
      if (!Array.isArray(index.manifests) || index.manifests.length !== 1) throw new Error('OCI archive must contain one completed output.');
      const rootDigest = digest(object(index.manifests[0], 'OCI root descriptor').digest);
      if (metadata['containerimage.digest'] !== rootDigest) throw new Error('OCI output differs from completed BuildKit metadata.');
      const read = async (_repository: string, _kind: string, id: string) => {
        const bytes = await archiveObject(archive, output, 'blobs/sha256/' + digest(id).slice(7), runner);
        if ('sha256:' + sha(bytes) !== id) throw new Error('OCI output content digest mismatch.');
        return bytes;
      };
      const manifest = object(JSON.parse((await read('', '', rootDigest)).toString('utf8')), 'OCI output');
      const buildOutput: BuildOutput = { role, repository: 'local/' + role, digest: rootDigest,
        media_type: String(manifest.mediaType), platforms: await readOutput('local/' + role, rootDigest, metadata, read) };
      buildOutput.application = await captureInstalledExecutables(role, buildOutput.platforms[0]!, invocation, options.revision, root, runner);
      bundle.builds.push({ output: buildOutput, invocation, archive });
      await writeFile(join(output, 'application-build.json'), JSON.stringify(bundle, null, 2) + '\n', { mode: 0o600 });
    }
    return bundle;
  } finally { await rm(directory, { recursive: true, force: true }); }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const { values } = parseArgs({ options: { 'source-root': { type: 'string' }, revision: { type: 'string' },
    output: { type: 'string' }, roles: { type: 'string' } } });
  Promise.resolve().then(async () => {
    if (!values.revision || !values.output || !values.roles) throw new Error('Provide --revision SHA --roles dataflow[,client] --output EXTERNAL_NEW_DIRECTORY.');
    const roles = values.roles.split(',').map(name => {
      const role = imageBuilds.find(([role]) => role === name)?.[0];
      if (!role) throw new Error('Unknown publisher role: ' + name);
      return role;
    });
    const result = await buildLocalApplications({ sourceRoot: values['source-root'] ?? ROOT, revision: values.revision,
      output: values.output, roles });
    console.log(JSON.stringify(result, null, 2));
  }).catch((error: unknown) => { console.error(error instanceof Error ? error.message : 'Application build failed'); process.exitCode = 1; });
}
