import fs from 'node:fs';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import { assertCampaignEnvironment, createBlobCampaignStore, validateCampaignState, type CampaignStore } from '../../projects/arrowcol-oat/aks-campaign.js';
import { loadCustodyConfig } from '../../projects/arrowcol-oat/aks-custody-config.js';
import { armDirectBenchmark, type DirectRelease } from './benchmark-direct-arm.js';
import { assertDirectInventory, publishDirectHandoff } from './benchmark-direct-handoff.js';
import { evidenceObject, requireEvidence } from './benchmark-evidence-json.js';
import { directProducerAuthority, type DirectCampaignScope } from './benchmark-run-authority.js';
import type { DirectStopOrigin } from './benchmark-stop-patch.js';
import { directArmRecordPath, finishDirectTerminalCustody, type DirectTerminalArm } from './benchmark-terminal-custody.js';
import { Azure, canonical_json } from './bootstrap.js';
import { PublisherControl } from './publisher_control.js';
import { read_run_fic_record } from './run_fic.js';

const uuid = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/;
const digest = /^[^\s@]+@sha256:[a-f0-9]{64}$/;
const roles = ['writer', 'client', 'maintenance', 'sql', 'router', 'catalog'] as const;
const common = ['env-file', 'stamp-config', 'fic-record', 'release-file', 'kubeconfig', 'context'] as const;

type Action = 'arm' | 'finish';
type Options = {
  action: Action;
  'env-file': string;
  'stamp-config': string;
  'fic-record': string;
  'release-file': string;
  kubeconfig: string;
  context: string;
  clients?: number;
  'run-id'?: string;
  'baseline-directory'?: string;
  'terminal-directory'?: string;
  'delta-directory'?: string;
};
type LoadedConfig = ReturnType<typeof loadCustodyConfig>;
export interface DirectCliDependencies {
  loadConfig?: (args: string[], ambient: NodeJS.ProcessEnv) => LoadedConfig;
  azure?: Azure;
  store?: CampaignStore;
  log?: (value: string) => void;
}
export type DirectCliResult =
  | { action: 'arm'; arm: DirectTerminalArm }
  | { action: 'finish'; qualification: unknown; sealPath: string; handoffEtag: string };

function parseOptions(args: string[]): Options {
  const action = args[0];
  if (action !== 'arm' && action !== 'finish') throw new Error('Select exactly one direct command: arm or finish.');
  const required = [...common, ...(action === 'arm' ? ['clients', 'baseline-directory'] :
    ['run-id', 'terminal-directory', 'delta-directory'])];
  const allowed = new Set<string>(required);
  const values = new Map<string, string>();
  for (let index = 1; index < args.length; index++) {
    const token = args[index]!;
    if (!token.startsWith('--') || token.length < 3) throw new Error('Direct command arguments must be named options.');
    const name = token.slice(2);
    if (!allowed.has(name)) throw new Error('Direct command contains an unknown or inapplicable option.');
    if (values.has(name)) throw new Error('Direct command option was provided more than once.');
    const value = args[++index];
    if (!value || value.startsWith('--') || /[\u0000-\u001f]/.test(value))
      throw new Error('Direct command option requires one non-empty value.');
    values.set(name, value);
  }
  if (required.some(name => !values.has(name))) throw new Error('Direct command is missing a required option.');
  const clientsText = values.get('clients');
  if (action === 'arm' && !/^[1-8]$/.test(clientsText ?? '')) throw new Error('--clients must be an integer from 1 through 8.');
  if (action === 'finish' && !uuid.test(values.get('run-id') ?? '')) throw new Error('--run-id must identify the original direct run.');
  const context = values.get('context')!;
  if (context.length > 512 || !context.trim() || context !== context.trim()) throw new Error('--context is invalid.');
  return {
    action,
    'env-file': values.get('env-file')!,
    'stamp-config': values.get('stamp-config')!,
    'fic-record': values.get('fic-record')!,
    'release-file': values.get('release-file')!,
    kubeconfig: values.get('kubeconfig')!,
    context,
    ...(action === 'arm' ? { clients: Number(clientsText), 'baseline-directory': values.get('baseline-directory')! } : {
      'run-id': values.get('run-id')!, 'terminal-directory': values.get('terminal-directory')!,
      'delta-directory': values.get('delta-directory')!,
    }),
  };
}

function privateFile(filename: string, label: string, maxBytes = 1_048_576): string {
  const path = resolve(filename), stat = fs.lstatSync(path);
  if (!stat.isFile() || stat.isSymbolicLink() || stat.size > maxBytes || (stat.mode & 0o077) !== 0)
    throw new Error(`${label} must be a bounded private regular file, not a symlink.`);
  return path;
}

function privateDirectory(filename: string, label: string): string {
  const path = resolve(filename), stat = fs.lstatSync(path);
  if (!stat.isDirectory() || stat.isSymbolicLink() || (stat.mode & 0o077) !== 0)
    throw new Error(`${label} must be an existing private directory, not a symlink.`);
  return path;
}

function exactKeys(value: Record<string, unknown>, expected: readonly string[]): boolean {
  const actual = Object.keys(value).sort(), sorted = [...expected].sort();
  return actual.length === sorted.length && actual.every((key, index) => key === sorted[index]);
}

export function readRelease(filename: string): DirectRelease {
  const path = privateFile(filename, 'Immutable release file', 16_384);
  let value: unknown;
  try { value = JSON.parse(fs.readFileSync(path, 'utf8')); }
  catch { throw new Error('Immutable release file is not readable JSON.'); }
  const release = evidenceObject(value), images = evidenceObject(release.images);
  const revision = typeof release.revision === 'string' ? release.revision : '';
  const schemaFingerprint = typeof release.schemaFingerprint === 'string' ? release.schemaFingerprint : '';
  const image = (role: typeof roles[number]) => {
    const reference = images[role];
    requireEvidence(typeof reference === 'string' && digest.test(reference), 'Immutable image digest is invalid');
    return reference;
  };
  requireEvidence(exactKeys(release, ['revision', 'schemaFingerprint', 'images']) && exactKeys(images, roles) &&
    /^[a-f0-9]{40}$/.test(revision) && schemaFingerprint.length > 0 && schemaFingerprint.length < 129,
  'Immutable release pins are invalid or incomplete');
  return { revision, schemaFingerprint, images: { writer: image('writer'), client: image('client'),
    maintenance: image('maintenance'), sql: image('sql'), router: image('router'), catalog: image('catalog') } };
}

function sameOrigin(value: unknown, origin: DirectStopOrigin): boolean {
  try { return canonical_json(evidenceObject(value)) === canonical_json(origin); }
  catch { return false; }
}

function isDirectTerminalArm(value: unknown, base: Omit<DirectStopOrigin, 'clients'>,
  release: DirectRelease): value is DirectTerminalArm {
  try {
    const arm = evidenceObject(value), provenance = evidenceObject(arm.provenance);
    const records = (items: unknown): items is unknown[] => Array.isArray(items) && items.every(item => {
      try { evidenceObject(item); return true; } catch { return false; }
    });
    if (!exactKeys(arm, ['origin', 'startMs', 'endMs', 'baseline', 'clients', 'writer', 'writerClaim', 'maintenance',
        'sql', 'catalogLifetime', 'routers', 'registration', 'routerUid', 'provenance']) ||
        !exactKeys(provenance, ['clocks', 'release', 'catalog', 'catalogLog', 'schedule', 'transport', 'parquet'])) return false;
    const armOrigin = evidenceObject(arm.origin), clients = armOrigin.clients;
    if (typeof clients !== 'number' || !Number.isInteger(clients) || clients < 1 || clients > 8) return false;
    const origin: DirectStopOrigin = { ...base, clients };
    return sameOrigin(armOrigin, origin) && typeof arm.startMs === 'number' && Number.isSafeInteger(arm.startMs) &&
      typeof arm.endMs === 'number' && Number.isSafeInteger(arm.endMs) && arm.startMs > 0 && arm.endMs > arm.startMs &&
      canonical_json(provenance.release) === canonical_json(release) && records(arm.clients) && arm.clients.length === clients &&
      records(arm.routers) && arm.routers.length > 0 && records(provenance.clocks) && provenance.clocks.length > 0 &&
      evidenceObject(arm.baseline) !== undefined && evidenceObject(arm.writer) !== undefined && evidenceObject(arm.writerClaim) !== undefined &&
      evidenceObject(arm.maintenance) !== undefined && evidenceObject(arm.sql) !== undefined &&
      evidenceObject(arm.catalogLifetime) !== undefined && evidenceObject(arm.registration) !== undefined &&
      typeof arm.routerUid === 'string' && evidenceObject(provenance.catalog) !== undefined &&
      typeof provenance.catalogLog === 'string' && records(provenance.schedule) &&
      evidenceObject(provenance.transport) !== undefined && evidenceObject(provenance.parquet) !== undefined;
  } catch { return false; }
}

export function validateDurableArm(value: unknown, base: Omit<DirectStopOrigin, 'clients'>, release: DirectRelease): DirectTerminalArm {
  if (!isDirectTerminalArm(value, base, release)) throw new Error('Original off-node arm record does not match the authorized run and immutable release.');
  return value;
}

export async function loadDurableArm(azure: Azure, base: Omit<DirectStopOrigin, 'clients'>, release: DirectRelease): Promise<DirectTerminalArm> {
  const path = directArmRecordPath({ ...base, clients: 1 });
  const [bytes, headers] = await azure.blob('GET', azure.c.checkpoints, path, undefined, undefined, {}, false, true);
  requireEvidence(headers[':status'] === '200' && typeof headers.etag === 'string' && /^"[^"\r\n]+"$/.test(headers.etag) && bytes && bytes.length <= 8 * 1024 ** 2,
    'Original off-node arm record is missing or its read outcome is unknown');
  let value: unknown;
  try { value = JSON.parse(bytes.toString('utf8')); }
  catch { throw new Error('Original off-node arm record is not readable JSON.'); }
  return validateDurableArm(value, base, release);
}

function resolveOptionsConfig(options: Options, loadConfig: DirectCliDependencies['loadConfig']): LoadedConfig {
  privateFile(options['env-file'], 'Bootstrap environment file');
  privateFile(options['stamp-config'], 'Stamp configuration file');
  const selected = (loadConfig ?? ((args, ambient) => loadCustodyConfig(args, false, ambient)))([
    '--env-file', options['env-file'], '--stamp-config', options['stamp-config'],
  ], process.env);
  if (selected.positionals.length !== 0) throw new Error('Custody configuration received unexpected positional arguments.');
  return selected;
}

async function resolveProducer(options: Options, selected: LoadedConfig, azure: Azure, store: CampaignStore,
  release: DirectRelease): Promise<{ campaign: DirectCampaignScope; base: Omit<DirectStopOrigin, 'clients'> }> {
  const current = await store.read();
  if (!current) throw new Error('No active campaign state exists.');
  const state = validateCampaignState(current.state), inventory = state.runInventory;
  if (state.status !== 'active' || state.mode !== 'armed' || !inventory || state.campaignId !== inventory.campaign_id ||
      state.ownerId !== inventory.owner_id || state.fencingToken !== inventory.fencing_token ||
      inventory.load_admission !== 'authorized' || (options.action === 'finish' && options['run-id'] !== inventory.cell_run_id) ||
      !inventory.namespace.uid ||
      inventory.namespace.owner_token !== state.ownerId || !uuid.test(inventory.cell_run_id) ||
      !uuid.test(inventory.namespace.uid) || !uuid.test(state.campaignId) || !uuid.test(state.ownerId))
    throw new Error('Durable campaign state does not authorize this direct producer run.');
  assertCampaignEnvironment(selected.env, state);
  const fic = read_run_fic_record(options['fic-record'], selected.config);
  requireEvidence(fic.runId === inventory.cell_run_id && fic.namespace === inventory.namespace.name &&
    fic.namespaceUid === inventory.namespace.uid && fic.namespaceOwner === inventory.namespace.owner_token &&
    (fic.phase === 'ready' || options.action === 'finish' && fic.phase === 'deleted'),
  'Run FIC record does not match the durable producer inventory');
  assertDirectInventory(azure, inventory, fic, options.action);
  const [publisher] = await new PublisherControl(azure).read();
  requireEvidence(typeof publisher.resetgeneration === 'string' && uuid.test(publisher.resetgeneration),
    'Publisher generation is unavailable');
  const base: Omit<DirectStopOrigin, 'clients'> = { runId: inventory.cell_run_id, owner: inventory.namespace.owner_token,
    namespaceUid: inventory.namespace.uid, namespace: inventory.namespace.name, cloudGeneration: publisher.resetgeneration,
    sourceRevision: release.revision, fencingToken: state.fencingToken };
  return { campaign: { store, campaignId: state.campaignId, ownerId: state.ownerId }, base };
}

export async function runDirectBenchmarkCli(args: string[], dependencies: DirectCliDependencies = {}): Promise<DirectCliResult> {
  const options = parseOptions(args), release = readRelease(options['release-file']);
  privateFile(options.kubeconfig, 'Kubeconfig');
  const expectedKubeconfig = resolve(options.kubeconfig);
  if (process.env.KUBECONFIG && resolve(process.env.KUBECONFIG) !== expectedKubeconfig)
    throw new Error('Process kubeconfig conflicts with the explicit --kubeconfig path.');
  const selected = resolveOptionsConfig(options, dependencies.loadConfig);
  if (selected.env.KUBECONFIG && resolve(selected.env.KUBECONFIG) !== expectedKubeconfig)
    throw new Error('Configured kubeconfig conflicts with the explicit --kubeconfig path.');
  const azure = dependencies.azure ?? new Azure(selected.config);
  const store = dependencies.store ?? createBlobCampaignStore({ account: selected.config.account,
    container: selected.config.checkpoints, subscriptionId: selected.config.subscription });
  const previousKubeconfig = process.env.KUBECONFIG;
  process.env.KUBECONFIG = expectedKubeconfig;
  try {
    const { campaign, base } = await resolveProducer(options, selected, azure, store, release);
    if (options.action === 'arm') {
      if (options.clients === undefined || options['baseline-directory'] === undefined)
        throw new Error('Arm command is missing its workload shape or baseline directory.');
      const origin: DirectStopOrigin = { ...base, clients: options.clients };
      await directProducerAuthority(campaign, origin).assertCurrent();
      const arm = await armDirectBenchmark(azure, campaign, origin, release, options.context, options['fic-record'],
        privateDirectory(options['baseline-directory'], 'Baseline directory'), async record => {
          await directProducerAuthority(campaign, origin).assertCurrent();
          const current = await store.read();
          requireEvidence(current?.state.runInventory, 'Original direct inventory is missing at arm');
          assertDirectInventory(azure, current.state.runInventory, read_run_fic_record(options['fic-record'], selected.config), 'arm', record);
        });
      (dependencies.log ?? console.log)(JSON.stringify({ ok: true, action: 'arm', runId: origin.runId,
        startMs: arm.startMs, endMs: arm.endMs, armRecord: directArmRecordPath(origin) }));
      return { action: 'arm', arm };
    }
    const arm = await loadDurableArm(azure, base, release);
    await directProducerAuthority(campaign, arm.origin, 'finish').assertCurrent();
    const fresh = await store.read();
    requireEvidence(fresh?.state.runInventory, 'Original direct inventory is missing at finish');
    assertDirectInventory(azure, fresh.state.runInventory, read_run_fic_record(options['fic-record'], selected.config), 'finish', arm);
    if (options['terminal-directory'] === undefined || options['delta-directory'] === undefined)
      throw new Error('Finish command is missing an evidence directory.');
    const result = await finishDirectTerminalCustody(azure, campaign, arm, options.context,
      privateDirectory(options['terminal-directory'], 'Terminal evidence directory'),
      privateDirectory(options['delta-directory'], 'Delta evidence directory'), options['fic-record']);
    const handoffEtag = await publishDirectHandoff(azure, campaign, arm.origin, result);
    (dependencies.log ?? console.log)(JSON.stringify({ ok: true, action: 'finish', runId: arm.origin.runId,
      qualification: result.qualification, seal: result.seal.path, handoffEtag }));
    return { action: 'finish', qualification: result.qualification, sealPath: result.seal.path, handoffEtag };
  } finally {
    if (previousKubeconfig === undefined) delete process.env.KUBECONFIG;
    else process.env.KUBECONFIG = previousKubeconfig;
  }
}

async function main(): Promise<void> {
  try { await runDirectBenchmarkCli(process.argv.slice(2)); }
  catch (error) {
    console.error(error instanceof Error ? error.message : 'Direct benchmark command failed with an unknown error.');
    process.exitCode = 1;
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) void main();
