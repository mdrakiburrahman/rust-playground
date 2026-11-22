import { createHash, createPrivateKey, createPublicKey, sign, verify } from 'node:crypto';
import { lstat, mkdir, mkdtemp, readFile, realpath, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, isAbsolute, join, relative, resolve } from 'node:path';
import { parseArgs } from 'node:util';
import { pathToFileURL } from 'node:url';
import { clientRepository, imageBuilds, repositories } from './publish-images.js';
import { parseImageOptions, withPublisherIdentity } from '../../experiments/azure-qap/src/publish-image.js';
import { ROOT, run_tree } from '../../infra/azure/bootstrap.js';

export const PRIMARY_REPOSITORY = 'git+https://github.com/mdrakiburrahman/rust-playground';
export const PRODUCER_ID = 'urn:arrowcol:oat:immutable-candidate-builder:v1';
const PAYLOAD_TYPE = 'application/vnd.in-toto+json';
type Json = Record<string, unknown>;
type Role = typeof imageBuilds[number][0];
export interface SourceMaterial {
  path: string; uri: string; digest: { sha1: string; gitTree: string }; archive_sha256: string; repository_verified: boolean;
}
export interface BuildOutput {
  role: Role; repository: string; digest: string; media_type: string;
  platforms: Array<{ os: string; architecture: string; digest: string; config_digest: string }>;
}
export interface BuildEnvelope { payloadType: string; payload: string; signatures: Array<{ keyid: string; sig: string }> }
export interface CandidateBuildBundle {
  schema_version: 1; scope: 'candidate' | 'local-fixture'; source_commit: string;
  outputs: BuildOutput[]; envelopes: BuildEnvelope[];
}
export interface CandidateBuildOptions {
  sourceRoot: string; revision: string; registry: string; signingKey: string; output: string;
  scope?: 'candidate' | 'local-fixture';
  allowRemotePublishing?: boolean;
}
type Runner = typeof run_tree;
export type RegistryRead = (repository: string, kind: 'manifests' | 'blobs', digest: string) => Promise<Buffer>;

function registryReader(registry: string, credential?: string): RegistryRead {
  const origin = (credential ? 'https://' : 'http://') + registry;
  return async (repository, kind, id) => {
    if (!repository.startsWith(registry + '/')) throw new Error('Registry read escaped the selected publisher.');
    const url = origin + '/v2/' + repository.slice(registry.length + 1) + '/' + kind + '/' + id;
    const headers: Record<string, string> = { Accept: 'application/vnd.oci.image.index.v1+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json, application/vnd.docker.distribution.manifest.list.v2+json' };
    let response = await fetch(url, { headers, signal: AbortSignal.timeout(30_000) });
    if (response.status === 401 && credential) {
      const challenge = response.headers.get('www-authenticate') ?? '';
      const realm = /realm="([^"]+)"/.exec(challenge)?.[1];
      const service = /service="([^"]+)"/.exec(challenge)?.[1];
      const scope = /scope="([^"]+)"/.exec(challenge)?.[1];
      if (!realm || !service || !scope || new URL(realm).origin !== origin) throw new Error('Registry returned an unsafe authentication challenge.');
      const tokenUrl = new URL(realm);
      tokenUrl.searchParams.set('service', service); tokenUrl.searchParams.set('scope', scope);
      const tokenResponse = await fetch(tokenUrl, { headers: { Authorization: 'Basic ' +
        Buffer.from('00000000-0000-0000-0000-000000000000:' + credential).toString('base64') }, signal: AbortSignal.timeout(30_000) });
      if (!tokenResponse.ok) throw new Error('Registry token exchange failed.');
      const token = object(await tokenResponse.json(), 'registry token');
      const value = token.token ?? token.access_token;
      if (typeof value !== 'string' || !value) throw new Error('Registry token response lacks an access token.');
      headers.Authorization = 'Bearer ' + value;
      response = await fetch(url, { headers, signal: AbortSignal.timeout(30_000) });
    }
    if (!response.ok) throw new Error(`Registry content read failed (${response.status}).`);
    return Buffer.from(await response.arrayBuffer());
  };
}

function object(value: unknown, label: string): Json {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) throw new Error(`Invalid ${label}.`);
  return value as Json;
}
function sha(bytes: Buffer | string): string { return createHash('sha256').update(bytes).digest('hex'); }
function gitHash(value: string): string {
  if (!/^[a-f0-9]{40}$/.test(value)) throw new Error('Expected an immutable full Git object ID.');
  return value;
}
function digest(value: unknown): string {
  if (typeof value !== 'string' || !/^sha256:[a-f0-9]{64}$/.test(value)) throw new Error('Invalid build output digest.');
  return value;
}
function inside(root: string, path: string): boolean {
  const r = relative(root, path);
  return !isAbsolute(r) && r !== '..' && !r.startsWith('..' + (process.platform === 'win32' ? '\\' : '/'));
}
function sourceUri(raw: string, fixture: boolean, parent?: string): string {
  const normalized = raw.replace(/^git\+/, '').replace(/^git@github\.com:/, 'https://github.com/');
  if (!fixture && isAbsolute(normalized)) throw new Error('Candidate Git material cannot use a local filesystem origin.');
  let url: URL;
  try { url = new URL(normalized, parent ? parent.replace(/^git\+/, '') + '/' : undefined); }
  catch { throw new Error('Source repository has no canonical credential-free URI.'); }
  if (url.username || url.password || url.search || url.hash ||
      !(url.protocol === 'https:' || (fixture && url.protocol === 'file:'))) {
    throw new Error('Source repository URI must not contain credentials or mutable parameters.');
  }
  return 'git+' + url.href.replace(/\/$/, '').replace(/\.git$/, '');
}
async function checked(runner: Runner, args: string[], cwd: string, seconds = 30): Promise<string> {
  const result = await runner(args, seconds, cwd);
  if (result.returncode) throw new Error(`${args[0]} ${args[1]} failed (${result.returncode}); raw build diagnostics are not published.`);
  return result.stdout;
}
function pae(payload: Buffer): Buffer {
  return Buffer.concat([Buffer.from(`DSSEv1 ${Buffer.byteLength(PAYLOAD_TYPE)} ${PAYLOAD_TYPE} ${payload.length} `), payload]);
}
function publicKeyId(key: ReturnType<typeof createPublicKey>): string {
  return sha(key.export({ format: 'der', type: 'spki' }));
}
function seal(statement: Json, key: ReturnType<typeof createPrivateKey>): BuildEnvelope {
  const payload = Buffer.from(JSON.stringify(statement));
  return { payloadType: PAYLOAD_TYPE, payload: payload.toString('base64'),
    signatures: [{ keyid: publicKeyId(createPublicKey(key)), sig: sign(null, pae(payload), key).toString('base64') }] };
}

export function verifyBuildEnvelope(envelope: unknown, publicKeyPem: Buffer): Json {
  const e = object(envelope, 'DSSE envelope');
  const key = createPublicKey(publicKeyPem);
  if (key.asymmetricKeyType !== 'ed25519' || e.payloadType !== PAYLOAD_TYPE || typeof e.payload !== 'string' ||
      !Array.isArray(e.signatures) || e.signatures.length !== 1) throw new Error('Unsupported build envelope or trust anchor.');
  const payload = Buffer.from(e.payload, 'base64');
  const signature = object(e.signatures[0], 'DSSE signature');
  if (payload.toString('base64') !== e.payload || signature.keyid !== publicKeyId(key) ||
      typeof signature.sig !== 'string' || !verify(null, pae(payload), key, Buffer.from(signature.sig, 'base64'))) {
    throw new Error('Build signature does not verify under the independently supplied trust anchor.');
  }
  return object(JSON.parse(payload.toString('utf8')), 'signed build statement');
}

export function assertProducedBuildStatement(statement: unknown, imageDigest: string, commit: string,
  repository: string, scope: 'candidate' | 'local-fixture' = 'candidate', expectedUri = PRIMARY_REPOSITORY): void {
  const s = object(statement, 'build statement'), p = object(s.predicate, 'build predicate');
  const invocation = object(p.invocation, 'build invocation'), source = object(invocation.configSource, 'primary source');
  const params = object(invocation.parameters, 'build parameters'), builder = object(p.builder, 'builder');
  const sourceDigest = object(source.digest, 'source digest'), output = object(p.output, 'build output');
  const build = imageBuilds.find(([role]) => (role === 'client' ? clientRepository : repositories[role]) === repository.split('/').at(-1));
  if (s._type !== 'https://in-toto.io/Statement/v0.1' || s.predicateType !== 'https://slsa.dev/provenance/v0.2' ||
      builder.id !== PRODUCER_ID || !build || p.scope !== scope || source.uri !== expectedUri ||
      sourceDigest.sha1 !== commit || typeof sourceDigest.gitTree !== 'string' || !/^[a-f0-9]{40}$/.test(sourceDigest.gitTree) ||
      source.entryPoint !== 'projects/arrowcol-oat/aks-candidate-build.ts#buildCandidateImages' ||
      params.revision !== commit || params.role !== build[0] || params.dockerfile !== build[1] || params.context !== build[2] ||
      params.platform !== 'linux/amd64' || output.digest !== imageDigest || output.repository !== repository ||
      !Array.isArray(s.subject) || s.subject.length !== 1 ||
      object(s.subject[0], 'subject').name !== repository ||
      object(object(s.subject[0], 'subject').digest, 'subject digest').sha256 !== imageDigest.slice(7)) {
    throw new Error('Produced build does not bind primary source/tree, role, context, scope and final registry digest.');
  }
  if (!Array.isArray(p.materials) || !p.materials.length) throw new Error('Produced build lacks immutable source materials.');
  const paths = new Set<string>();
  for (const raw of p.materials) {
    const material = object(raw, 'source material'), d = object(material.digest, 'material digest');
    if (typeof material.path !== 'string' || paths.has(material.path) || typeof material.uri !== 'string' ||
        typeof d.sha1 !== 'string' || !/^[a-f0-9]{40}$/.test(d.sha1) ||
        typeof d.gitTree !== 'string' || !/^[a-f0-9]{40}$/.test(d.gitTree) ||
        typeof material.archive_sha256 !== 'string' || !/^[a-f0-9]{64}$/.test(material.archive_sha256) ||
        (scope === 'candidate' && material.repository_verified !== true)) {
      throw new Error('Invalid recursive Git material binding.');
    }
    paths.add(material.path);
  }
  const primary = object(p.materials[0], 'primary material');
  if (primary.path !== '' || primary.uri !== expectedUri ||
      object(primary.digest, 'primary material digest').sha1 !== commit ||
      object(primary.digest, 'primary material digest').gitTree !== sourceDigest.gitTree) {
    throw new Error('An unrelated material cannot substitute for the primary source.');
  }
  if (typeof output.media_type !== 'string' || !Array.isArray(output.platforms) || output.platforms.length !== 1) {
    throw new Error('Build output must identify the exact linux/amd64 platform manifest.');
  }
  const platform = object(output.platforms[0], 'platform output');
  digest(platform.digest); digest(platform.config_digest);
  if (platform.os !== 'linux' || platform.architecture !== 'amd64') throw new Error('Unexpected built platform.');
  const index = output.media_type === 'application/vnd.oci.image.index.v1+json' ||
    output.media_type === 'application/vnd.docker.distribution.manifest.list.v2+json';
  if (!index && (!(output.media_type === 'application/vnd.oci.image.manifest.v1+json' ||
      output.media_type === 'application/vnd.docker.distribution.manifest.v2+json') || platform.digest !== imageDigest)) {
    throw new Error('Manifest vs index digest binding is inconsistent.');
  }
}

async function snapshotSource(options: CandidateBuildOptions, directory: string, runner: Runner): Promise<SourceMaterial[]> {
  const original = await realpath(options.sourceRoot);
  const fixture = options.scope === 'local-fixture';
  const revision = gitHash(options.revision);
  if ((await checked(runner, ['git', 'rev-parse', 'HEAD'], original)).trim() !== revision ||
      (await checked(runner, ['git', 'status', '--porcelain', '--untracked-files=all', '--ignore-submodules=none'], original)).trim()) {
    throw new Error('Build source must be a clean checkout of the exact requested commit, including submodules.');
  }
  const rootUri = sourceUri((await checked(runner, ['git', 'remote', 'get-url', 'origin'], original)).trim(), fixture);
  if (!fixture && rootUri !== PRIMARY_REPOSITORY) throw new Error('Candidate primary repository is not the sanctioned source.');
  const materials: SourceMaterial[] = [];
  async function archive(repo: string, commit: string, path: string, uri: string): Promise<void> {
    if ((await checked(runner, ['git', 'rev-parse', 'HEAD'], repo)).trim() !== commit ||
        (await checked(runner, ['git', 'status', '--porcelain', '--untracked-files=all', '--ignore-submodules=none'], repo)).trim()) {
      throw new Error('Submodule checkout differs from its immutable gitlink or is dirty.');
    }
    await checked(runner, ['git', 'fsck', '--no-dangling', commit], repo, 120);
    const tree = gitHash((await checked(runner, ['git', 'rev-parse', commit + '^{tree}'], repo)).trim());
    const store = await mkdtemp(join(directory, 'git-'));
    await checked(runner, ['git', 'init', '--bare', '--quiet', store], directory);
    const objects = await realpath(resolve(repo, (await checked(runner, ['git', 'rev-parse', '--git-path', 'objects'], repo)).trim()));
    await mkdir(join(store, 'objects', 'info'), { recursive: true });
    if (!fixture) {
      // Verify the exact object against the policy repository, not a mutable origin label.
      await checked(runner, ['git', '--git-dir', store, 'fetch', '--depth=1', '--no-tags', '--no-write-fetch-head',
        uri.replace(/^git\+/, '') + '.git', commit], directory, 120);
      await checked(runner, ['git', '--git-dir', store, 'fsck', '--no-dangling', commit], directory, 120);
    } else await writeFile(join(store, 'objects', 'info', 'alternates'), objects + '\n');
    // Ignore export attributes, not tracked files: the build sees the exact immutable tree.
    await writeFile(join(store, 'info', 'attributes'), '* -export-ignore -export-subst\n');
    const tar = join(store, 'source.tar');
    await checked(runner, ['git', '--git-dir', store, 'archive', '--format=tar', '--output', tar, commit], directory, 120);
    const destination = join(directory, 'context', path);
    await mkdir(destination, { recursive: true });
    await checked(runner, ['tar', '--extract', '--no-same-owner', '--file', tar, '--directory', destination], directory, 120);
    materials.push({ path, uri, digest: { sha1: commit, gitTree: tree }, archive_sha256: sha(await readFile(tar)), repository_verified: !fixture });
    const entries = (await checked(runner, ['git', 'ls-tree', '-r', '-z', commit], repo)).split('\0').filter(Boolean);
    for (const entry of entries) {
      const match = /^160000 commit ([a-f0-9]{40})\t([\s\S]+)$/.exec(entry);
      if (!match) continue;
      const subPath = match[2]!;
      if (isAbsolute(subPath) || /[\x00-\x1f\\]/.test(subPath) || subPath.split('/').some(part => !part || part === '..' || part === '.')) {
        throw new Error('Unsafe gitlink path.');
      }
      const paths = await checked(runner, ['git', 'config', '--blob', commit + ':.gitmodules', '--get-regexp', '^submodule\\..*\\.path$'], repo);
      const setting = paths.split('\n').find(line => line.slice(line.indexOf(' ') + 1) === subPath)?.split(' ')[0];
      if (!setting) throw new Error('Gitlink has no immutable .gitmodules mapping.');
      const url = (await checked(runner, ['git', 'config', '--blob', commit + ':.gitmodules', '--get', setting.replace(/\.path$/, '.url')], repo)).trim();
      await archive(join(repo, subPath), match[1]!, path ? path + '/' + subPath : subPath, sourceUri(url, fixture, uri));
    }
  }
  await archive(original, revision, '', rootUri);
  return materials;
}

async function readOutput(repository: string, rootDigest: string, metadata: Json, read: RegistryRead): Promise<BuildOutput['platforms']> {
  async function record(kind: 'manifests' | 'blobs', id: string): Promise<Json> {
    const bytes = await read(repository, kind, id);
    if ('sha256:' + sha(bytes) !== id) throw new Error('Registry content does not match the completed build digest.');
    return object(JSON.parse(bytes.toString('utf8')), 'registry object');
  }
  const root = await record('manifests', rootDigest);
  let childDigest = rootDigest, manifest = root;
  if (Array.isArray(root.manifests)) {
    if (root.manifests.length !== 1) throw new Error('Unexpected extra platform or attestation manifests.');
    const child = object(root.manifests[0], 'platform descriptor'), platform = object(child.platform, 'platform descriptor');
    if (platform.os !== 'linux' || platform.architecture !== 'amd64') throw new Error('Build index has the wrong platform.');
    childDigest = digest(child.digest); manifest = await record('manifests', childDigest);
  }
  const configDigest = digest(object(manifest.config, 'image config descriptor').digest);
  if (metadata['containerimage.config.digest'] !== configDigest) throw new Error('Registry platform config differs from BuildKit output.');
  const config = await record('blobs', configDigest);
  if (config.os !== 'linux' || config.architecture !== 'amd64') throw new Error('Actual built image is not linux/amd64.');
  return [{ os: 'linux', architecture: 'amd64', digest: childDigest, config_digest: configDigest }];
}

export async function buildCandidateImages(options: CandidateBuildOptions, runner: Runner = run_tree,
  registryRead?: RegistryRead): Promise<CandidateBuildBundle> {
  gitHash(options.revision);
  if (!/^(?:localhost|127\.0\.0\.1):[1-9]\d{0,4}$/.test(options.registry) &&
      !(options.allowRemotePublishing && /^[a-z0-9]+\.azurecr\.io$/.test(options.registry))) {
    throw new Error('This offline producer is loopback-registry-only; production publishing is separately gated.');
  }
  const source = await realpath(options.sourceRoot), keyPath = await realpath(options.signingKey);
  if (inside(source, keyPath) || inside(source, resolve(options.output))) throw new Error('Signing key and build bundle must be outside candidate source.');
  const scope = options.scope ?? 'candidate';
  const directory = await mkdtemp(join(tmpdir(), 'arrowcol-candidate-'));
  try {
    const materials = await snapshotSource(options, directory, runner);
    const read = registryRead ?? registryReader(options.registry);
    const bundle: CandidateBuildBundle = { schema_version: 1, scope, source_commit: options.revision, outputs: [], envelopes: [] };
    const statements: Json[] = [];
    for (const [role, dockerfile, context] of imageBuilds) {
      const root = join(directory, 'context'), file = await realpath(join(root, dockerfile)), cwd = await realpath(join(root, context));
      if (!inside(root, file) || !inside(root, cwd) || !(await lstat(file)).isFile() || !(await lstat(cwd)).isDirectory()) {
        throw new Error('Build definition escapes the immutable source context.');
      }
      const repository = options.registry + '/' + (role === 'client' ? clientRepository : repositories[role]);
      const metadataPath = join(directory, role + '.metadata.json');
      const tag = repository + ':candidate-' + options.revision;
      await checked(runner, ['docker', 'buildx', 'build', '--platform', 'linux/amd64', '--provenance=false', '--sbom=false',
        '--push', '--metadata-file', metadataPath, '--tag', tag, '--label', 'org.opencontainers.image.revision=' + options.revision,
        '--file', file, cwd], root, 3600);
      const metadata = object(JSON.parse(await readFile(metadataPath, 'utf8')), 'completed BuildKit metadata');
      const rootDigest = digest(metadata['containerimage.digest']);
      const rootBytes = await read(repository, 'manifests', rootDigest);
      if ('sha256:' + sha(rootBytes) !== rootDigest) throw new Error('Final registry digest differs from the actual build.');
      const rootManifest = object(JSON.parse(rootBytes.toString('utf8')), 'registry manifest');
      const output: BuildOutput = { role, repository, digest: rootDigest, media_type: String(rootManifest.mediaType),
        platforms: await readOutput(repository, rootDigest, metadata, read) };
      const statement: Json = { _type: 'https://in-toto.io/Statement/v0.1', predicateType: 'https://slsa.dev/provenance/v0.2',
        subject: [{ name: repository, digest: { sha256: rootDigest.slice(7) } }],
        predicate: { builder: { id: PRODUCER_ID }, scope,
          invocation: { configSource: { uri: materials[0]!.uri, digest: materials[0]!.digest,
            entryPoint: 'projects/arrowcol-oat/aks-candidate-build.ts#buildCandidateImages' },
            parameters: { revision: options.revision, role, dockerfile, context, platform: 'linux/amd64' } },
          materials, output } };
      assertProducedBuildStatement(statement, rootDigest, options.revision, repository, scope, materials[0]!.uri);
      bundle.outputs.push(output); statements.push(statement);
    }
    const key = createPrivateKey(await readFile(keyPath));
    if (key.asymmetricKeyType !== 'ed25519') throw new Error('An independently provisioned Ed25519 signer is required.');
    bundle.envelopes = statements.map(statement => seal(statement, key));
    await writeFile(options.output, JSON.stringify(bundle, null, 2) + '\n', { flag: 'wx', mode: 0o600 });
    return bundle;
  } finally { await rm(directory, { recursive: true, force: true }); }
}

export async function writeCandidateImageBinding(bundle: CandidateBuildBundle, templatePath: string, output: string): Promise<void> {
  const template = object(JSON.parse(await readFile(templatePath, 'utf8')), 'image binding template');
  if (bundle.scope !== 'candidate' || template.schemaVersion !== 1 || template.sourceCommit !== bundle.source_commit ||
      typeof template.workloadConfigSha256 !== 'string' || !/^[a-f0-9]{64}$/.test(template.workloadConfigSha256)) {
    throw new Error('Image template does not bind this candidate source; fixture builds cannot populate production bindings.');
  }
  const images: Record<string, string> = {};
  for (const reference of Object.keys(object(template.images, 'template image references'))) {
    const outputImage = bundle.outputs.find(output => output.repository.split('/').at(-1) === reference.split('@')[0]);
    if (!outputImage || !/^[a-z0-9-]+@sha256:[a-f0-9]{64}$/.test(reference)) throw new Error('Template references an unknown OAT role.');
    images[reference] = outputImage.digest;
  }
  if (!Object.keys(images).length) throw new Error('Empty image binding template.');
  await writeFile(output, JSON.stringify({ ...template, images }, null, 2) + '\n', { flag: 'wx', mode: 0o600 });
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const { values } = parseArgs({ options: { 'source-root': { type: 'string' }, revision: { type: 'string' },
    registry: { type: 'string' }, 'signing-key': { type: 'string' }, output: { type: 'string' },
    publish: { type: 'boolean' }, federated: { type: 'boolean' }, 'azure-env': { type: 'string' },
    'stamp-config': { type: 'string' }, env: { type: 'string' }, 'image-template': { type: 'string' }, 'image-binding': { type: 'string' } } });
  Promise.resolve().then(async () => {
    if (!values.revision || !values.registry || !values['signing-key'] || !values.output) {
      throw new Error('Usage: aks-candidate-build.ts --source-root PATH --revision SHA --registry localhost:PORT --signing-key EXTERNAL_KEY --output EXTERNAL_BUNDLE');
    }
    const options: CandidateBuildOptions = { sourceRoot: resolve(values['source-root'] ?? ROOT), revision: values.revision,
      registry: values.registry, signingKey: resolve(values['signing-key']), output: resolve(values.output) };
    if (!!values['image-template'] !== !!values['image-binding']) throw new Error('Provide both --image-template and --image-binding.');
    let bundle: CandidateBuildBundle;
    if (values.publish) {
      if (!values.federated) throw new Error('Production publication requires the existing federated publisher identity and separate campaign approval.');
      const args = ['--federated', '--registry', values.registry];
      for (const name of ['azure-env', 'stamp-config', 'env'] as const) {
        if (values[name]) args.push('--' + name, values[name]);
      }
      const imageOptions = parseImageOptions(args);
      bundle = await withPublisherIdentity(imageOptions, async session => {
        const token = object(JSON.parse(session.clean('Expose scoped publisher registry token', await session.run(
          ['az', 'acr', 'login', '--name', imageOptions.registry, '--subscription', session.subscription, '--expose-token', '--output', 'json'], 120))), 'publisher registry token');
        if (typeof token.accessToken !== 'string' || token.loginServer !== session.loginServer) throw new Error('Publisher registry token binding failed.');
        const publisherRunner: Runner = (args, timeout, cwd, ...rest) => args[0] === 'docker'
          ? session.run(args, timeout) : run_tree(args, timeout, cwd, ...rest);
        return buildCandidateImages({ ...options, registry: session.loginServer, allowRemotePublishing: true }, publisherRunner,
          registryReader(session.loginServer, token.accessToken));
      }, run_tree, true);
    } else bundle = await buildCandidateImages(options);
    if (values['image-template'] && values['image-binding']) {
      await writeCandidateImageBinding(bundle, values['image-template'], values['image-binding']);
    }
    console.log(JSON.stringify({ scope: bundle.scope, source_commit: bundle.source_commit, outputs: bundle.outputs }, null, 2));
  }).catch((error: unknown) => { console.error(error instanceof Error ? error.message : 'Candidate build failed'); process.exitCode = 1; });
}
