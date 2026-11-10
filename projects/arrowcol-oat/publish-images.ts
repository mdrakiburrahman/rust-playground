import { randomUUID } from 'node:crypto';
import { readFile, rename, rm, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { load_env, ROOT, run_tree } from '../../infra/azure/bootstrap.js';
import { loadStampConfig } from '../../infra/azure/stamp.js';
import { parseImageOptions, withPublisherIdentity, type ImageOptions, type PublisherSession } from '../../experiments/azure-qap/src/publish-image.js';
import { validateRuntime, type RuntimeConfig } from './runtime.js';

const repositories = {
  sqlserver: 'arrowcol-sqlserver', catalog: 'arrowcol-catalog-rust',
  router: 'arrowcol-router-rust', dataflow: 'arrowcol-dataflow-rust',
} as const;
type Options = ImageOptions & { runtimeConfig: string };

const clientRepository = 'arrowcol-oat-client';
const revisionLabel = 'org.opencontainers.image.revision';
const localImages = {
  sqlserver: 'arrowcol-oat/sqlserver:local', catalog: 'arrowcol-oat/catalog:local',
  router: 'arrowcol-oat/router:local', dataflow: 'arrowcol-oat/dataflow:local', client: 'arrowcol-oat/client:local',
} as const;
const imageBuilds = [
  ['sqlserver', 'projects/arrowcol-infra/docker/sqlserver/Dockerfile', 'projects/arrowcol-infra'],
  ['catalog', 'projects/arrowcol-catalog-rust/Dockerfile', '.'],
  ['router', 'projects/arrowcol-router-rust/Dockerfile', '.'],
  ['dataflow', 'projects/arrowcol-dataflow-rust/Dockerfile', '.'],
  ['client', 'projects/arrowcol-oat/Dockerfile', '.'],
] as const;

export class MissingPublishedImagesError extends Error {
  readonly missingRoles: string[];

  constructor(missingRoles: string[]) {
    super(`No published revision tag for: ${missingRoles.join(', ')}.`);
    this.name = 'MissingPublishedImagesError';
    this.missingRoles = [...missingRoles];
  }
}

function requireRevision(revision: string): string {
  if (!/^[a-f0-9]{40}$/i.test(revision)) throw new Error('Image revision must be a full 40-character commit SHA.');
  return revision.toLowerCase();
}

function revisionTag(revision: string): string { return `oat-${requireRevision(revision)}`; }

async function getSourceRevision(runner: typeof run_tree): Promise<string> {
  const result = await runner(['git', 'rev-parse', 'HEAD'], 15, ROOT);
  if (result.returncode) throw new Error(`git rev-parse failed: ${result.stderr.trim() || 'no diagnostic output'}`);
  return requireRevision(result.stdout.trim());
}

function inspectLocalImage(stdout: string, role: string, revision: string): void {
  let image: { Os?: string; Architecture?: string; Config?: { Labels?: Record<string, string> | null } };
  try { image = JSON.parse(stdout); } catch { throw new Error(`Docker returned invalid ${role} image metadata.`); }
  if (`${image.Os}/${image.Architecture}` !== 'linux/amd64') throw new Error(`Local ${role} image must be linux/amd64.`);
  if (image.Config?.Labels?.[revisionLabel] !== revision) throw new Error(`Local ${role} image revision provenance does not match ${revision}.`);
}

async function verifyLocalImage(role: string, reference: string, revision: string, runner: typeof run_tree): Promise<void> {
  const result = await runner(['docker', 'image', 'inspect', '--format', '{{json .}}', reference], 30, ROOT);
  if (result.returncode) throw new Error(`Missing local ${role} image ${reference}; build it before publishing.`);
  inspectLocalImage(result.stdout.trim(), role, revision);
}

function missingManifest(result: { stdout: string; stderr: string }): boolean {
  return /(?:Error:\s*the specified tag does not exist\.|Error:\s*manifest unknown\.)/i.test(`${result.stderr}\n${result.stdout}`);
}

function parseStringList(output: string, label: string): string[] {
  let value: unknown;
  try { value = JSON.parse(output); } catch { throw new Error(`${label} returned invalid JSON.`); }
  if (!Array.isArray(value) || !value.every((item): item is string => typeof item === 'string')) {
    throw new Error(`${label} returned an invalid list.`);
  }
  return value;
}

async function listRepositories(session: PublisherSession, options: ImageOptions): Promise<Set<string>> {
  const output = session.clean('ACR repository list', await session.run(['az', 'acr', 'repository', 'list',
    '--name', options.registry, '--subscription', session.subscription, '--output', 'json'], 120));
  return new Set(parseStringList(output, 'ACR repository list'));
}

async function listRepositoryTags(session: PublisherSession, options: ImageOptions, repository: string): Promise<Set<string>> {
  const output = session.clean(`ACR tags ${repository}`, await session.run(['az', 'acr', 'repository', 'show-tags',
    '--name', options.registry, '--repository', repository, '--subscription', session.subscription, '--output', 'json'], 120));
  return new Set(parseStringList(output, `ACR tags ${repository}`));
}

async function publishedDigest(session: PublisherSession, options: ImageOptions, repository: string, tag: string): Promise<string> {
  const digest = session.clean(`ACR manifest ${repository}`, await session.run(['az', 'acr', 'manifest', 'show-metadata',
    '--registry', options.registry, '--name', `${repository}:${tag}`, '--subscription', session.subscription,
    '--query', 'digest', '--output', 'tsv'], 120));
  if (!/^sha256:[a-f0-9]{64}$/.test(digest)) throw new Error(`ACR returned an invalid ${repository} manifest digest.`);
  return digest;
}

export async function buildLocalImages({ revision }: { revision: string }, runner: typeof run_tree = run_tree): Promise<void> {
  const source = requireRevision(revision);
  if (await getSourceRevision(runner) !== source) throw new Error('Image revision does not match the checked-out source.');
  const status = await runner(['git', 'status', '--porcelain'], 15, ROOT);
  if (status.returncode || status.stdout.trim()) throw new Error('Commit the source changes before building revision-proven images.');
  for (const [role, dockerfile, context] of imageBuilds) {
    const result = await runner(['docker', 'buildx', 'build', '--pull', '--platform', 'linux/amd64', '--load',
      '--tag', localImages[role], '--label', `${revisionLabel}=${source}`, '--file', dockerfile, context], 3600, ROOT);
    if (result.returncode) throw new Error(`Docker build failed for ${role} (${result.returncode}): ${result.stderr.trim() || 'no diagnostic output'}`);
  }
}

export async function publishClientImage(options: ImageOptions, runner: typeof run_tree = run_tree): Promise<string> {
  if (!options.federated) throw new Error('OAT publishing requires --federated; never use operator credentials for ACR publication.');
  const source = await getSourceRevision(runner);
  await verifyLocalImage('client', localImages.client, source, runner);
  return withPublisherIdentity(options, async session => {
    if (!source.startsWith(session.revision.toLowerCase())) throw new Error('Source revision changed during image publication.');
    const tag = options.tag ?? revisionTag(source);
    const tagged = `${session.loginServer}/${clientRepository}:${tag}`;
    session.clean('docker tag client', await session.run(['docker', 'tag', localImages.client, tagged], 30));
    session.clean('docker push client', await session.run(['docker', 'push', tagged], 3600));
    const digest = await publishedDigest(session, options, clientRepository, tag);
    return `${session.loginServer}/${clientRepository}@${digest}`;
  }, runner, true);
}

export async function readPublishedImages(
  options: ImageOptions & { revision: string }, runner: typeof run_tree = run_tree,
): Promise<{ images: RuntimeConfig['images']; client: string }> {
  if (!options.federated) throw new Error('OAT image verification requires --federated.');
  const source = requireRevision(options.revision);
  const current = await getSourceRevision(runner);
  if (current !== source) throw new Error(`Requested image revision ${source} is not the checked-out source revision ${current}.`);
  const tag = options.tag ?? revisionTag(source);
  return withPublisherIdentity(options, async session => {
    if (!source.startsWith(session.revision.toLowerCase())) throw new Error('Source revision changed during image verification.');
    const images = {} as RuntimeConfig['images'];
    let client = '';
    const missing: string[] = [];
    const allRepositories: Array<[keyof typeof repositories | 'client', string]> = [
      ...(Object.entries(repositories) as Array<[keyof typeof repositories, string]>), ['client', clientRepository],
    ];
    const repositoryNames = await listRepositories(session, options);
    for (const [role, repository] of allRepositories) {
      if (!repositoryNames.has(repository)) { missing.push(role); continue; }
      let tags = await listRepositoryTags(session, options, repository);
      if (!tags.has(tag)) { missing.push(role); continue; }
      const metadata = await session.run(['az', 'acr', 'manifest', 'show-metadata', '--registry', options.registry,
        '--name', `${repository}:${tag}`, '--subscription', session.subscription, '--query', 'digest', '--output', 'tsv'], 120);
      if (metadata.returncode) {
        if (missingManifest(metadata)) {
          tags = await listRepositoryTags(session, options, repository);
          if (!tags.has(tag)) { missing.push(role); continue; }
        }
        session.clean(`ACR manifest ${repository}`, metadata);
      }
      const digest = session.clean(`ACR manifest ${repository}`, metadata);
      if (!/^sha256:[a-f0-9]{64}$/.test(digest)) throw new Error(`ACR returned an invalid ${repository} manifest digest.`);
      const tagged = `${session.loginServer}/${repository}:${tag}`;
      session.clean(`docker pull ${role}`, await session.run(['docker', 'pull', tagged], 3600));
      const inspect = session.clean(`docker inspect ${role}`, await session.run(['docker', 'image', 'inspect', '--format', '{{json .}}', tagged], 30));
      let image: { Os?: string; Architecture?: string; Config?: { Labels?: Record<string, string> | null }; RepoDigests?: string[] };
      try { image = JSON.parse(inspect); } catch { throw new Error(`Docker returned invalid ${role} image metadata.`); }
      if (`${image.Os}/${image.Architecture}` !== 'linux/amd64') throw new Error(`Published ${role} image must be linux/amd64.`);
      if (image.Config?.Labels?.[revisionLabel] !== source) throw new Error(`Published ${role} image revision provenance does not match ${source}.`);
      if (!image.RepoDigests?.includes(`${session.loginServer}/${repository}@${digest}`))
        throw new Error(`Published ${role} image digest changed while it was being verified.`);
      if (role === 'client') client = `${session.loginServer}/${repository}@${digest}`;
      else images[role] = `${session.loginServer}/${repository}@${digest}`;
    }
    if (missing.length) throw new MissingPublishedImagesError(missing);
    return { images, client };
  }, runner, true);
}

export async function publishImages(options: Options, runner: typeof run_tree = run_tree): Promise<RuntimeConfig> {
  if (!options.federated) throw new Error('OAT publishing requires --federated; never use operator credentials for ACR publication.');
  const file = resolve(ROOT, options.runtimeConfig);
  const ignored = await runner(['git', 'check-ignore', '--quiet', file], 15);
  if (ignored.returncode) throw new Error('Runtime configuration must be an ignored workspace file.');
  const original = await readFile(file, 'utf8');
  const config = validateRuntime(JSON.parse(original), true);
  const stamp = loadStampConfig(options.stampConfig);
  const generated = load_env(resolve(ROOT, options.azureEnv));
  if (config.stamp !== String(stamp.stamp) || config.subscriptionId.toLowerCase() !== stamp.subscription.toLowerCase() ||
      config.workloadIdentityClientId.toLowerCase() !== generated.AZURE_UAMI_CLIENT_ID?.toLowerCase() ||
      config.namespace !== stamp.aks.namespace || (config.serviceAccount ?? 'arrowcol-oat') !== stamp.aks.serviceAccount) {
    throw new Error('Runtime configuration must reuse the selected stamp namespace, ServiceAccount and UAMI.');
  }
  const source = await getSourceRevision(runner);
  for (const role of Object.keys(repositories) as Array<keyof typeof repositories>) {
    await verifyLocalImage(role, localImages[role], source, runner);
  }
  const images = await withPublisherIdentity(options, async ({ run, clean, revision, loginServer, subscription }) => {
    if (!source.startsWith(revision.toLowerCase())) throw new Error('Source revision changed during image publication.');
    const published = {} as RuntimeConfig['images'];
    const tag = options.tag ?? revisionTag(source);
    for (const [role, repository] of Object.entries(repositories) as Array<[keyof typeof repositories, string]>) {
      const tagged = `${loginServer}/${repository}:${tag}`;
      clean(`docker tag ${role}`, await run(['docker', 'tag', `arrowcol-oat/${role}:local`, tagged], 30));
      clean(`docker push ${role}`, await run(['docker', 'push', tagged], 3600));
      const digest = clean(`ACR manifest ${role}`, await run(['az', 'acr', 'manifest', 'show-metadata',
        '--registry', options.registry, '--name', `${repository}:${tag}`, '--subscription', subscription,
        '--query', 'digest', '--output', 'tsv'], 120));
      if (!/^sha256:[a-f0-9]{64}$/.test(digest)) throw new Error(`ACR returned an invalid ${role} manifest digest.`);
      published[role] = `${loginServer}/${repository}@${digest}`;
    }
    return published;
  }, runner, true);
  const published = validateRuntime({ ...config, images });
  if (await readFile(file, 'utf8') !== original) throw new Error('Runtime configuration changed during publication; refusing to overwrite it.');
  const pending = `${file}.published-${randomUUID()}`;
  try {
    await writeFile(pending, `${JSON.stringify(published, null, 2)}\n`, { flag: 'wx', mode: 0o600 });
    await rename(pending, file);
  } finally { await rm(pending, { force: true }); }
  return published;
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const [runtimeConfig, ...args] = process.argv.slice(2);
  Promise.resolve().then(async () => {
    if (!runtimeConfig || runtimeConfig.startsWith('--')) throw new Error('Usage: publish-images.ts <ignored-runtime.json> --federated --registry <acr> --azure-env <env> --stamp-config <stamp> [--env <env>] [--tag <tag>]');
    const result = await publishImages({ ...parseImageOptions(args), runtimeConfig });
    console.log(JSON.stringify({ images: result.images, runtimeConfig }, null, 2));
  }).catch((error: unknown) => { console.error(error instanceof Error ? error.message : 'Image publication failed'); process.exitCode = 1; });
}
