import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { parseArgs } from 'node:util';
import { Azure, canonical_json } from '../../infra/azure/bootstrap.js';
import { EffectBoundary, readEffectEnvelope, ResponseWithholdingSupervisor, withEffectBoundary,
  type ResponseWithholdingTarget } from '../../infra/azure/effect-boundary.js';
import { loadCustodyConfig } from './aks-custody-config.js';
import { assertCampaignEnvironment, createBlobCampaignStore, campaignMain, validateInitialRunInventory,
  type CampaignStore } from './aks-campaign.js';
import { CELL_CLEANUP_SELECTORS } from './aks-cell-cleanup.js';
import { watchdogOnce } from './aks-watchdog.js';

export interface BoundedEffectRehearsalDependencies {
  store?: CampaignStore;
  azure?: Azure;
  campaignMain?: typeof campaignMain;
  watchdogOnce?: typeof watchdogOnce;
}

export async function boundedEffectRehearsal(args: string[], dependencies: BoundedEffectRehearsalDependencies = {}):
  Promise<Record<string, unknown> | undefined> {
  const parsed = parseArgs({ args, allowPositionals: false, options: {
    mode: { type: 'string' }, selector: { type: 'string' }, 'env-file': { type: 'string' },
    'stamp-config': { type: 'string' }, 'effect-envelope': { type: 'string' },
    'inventory-file': { type: 'string' }, 'initial-inventory-file': { type: 'string' },
    'receipt-file': { type: 'string' }, 'withhold-response': { type: 'string' }, resume: { type: 'boolean' },
  } });
  const { mode, selector, 'env-file': envFile, 'stamp-config': stampConfig,
    'effect-envelope': effectFile, 'inventory-file': inventoryFile, 'initial-inventory-file': initialInventoryFile,
    'receipt-file': receiptFile, 'withhold-response': withholdResponseFile, resume } = parsed.values;
  if (!['producer-cleanup-step', 'watchdog-once', 'setup-baseline'].includes(mode ?? '') || !envFile || !stampConfig || !effectFile || !receiptFile ||
      mode === 'producer-cleanup-step' && (!selector || !Object.hasOwn(CELL_CLEANUP_SELECTORS, selector) || !inventoryFile || initialInventoryFile !== undefined) ||
      mode === 'watchdog-once' && (selector !== undefined || inventoryFile !== undefined || initialInventoryFile !== undefined || withholdResponseFile !== undefined || resume) ||
      mode === 'setup-baseline' && (selector !== undefined || inventoryFile !== undefined || !initialInventoryFile) ||
      mode !== 'setup-baseline' && (initialInventoryFile !== undefined || resume) ||
      resume && withholdResponseFile !== undefined ||
      withholdResponseFile !== undefined && !(mode === 'setup-baseline' || mode === 'producer-cleanup-step' && selector === 'terminal_capture'))
    throw new Error('Usage: aks-effect-rehearsal.ts --mode producer-cleanup-step --selector SELECTOR --inventory-file PATH --env-file PATH --stamp-config PATH --effect-envelope PATH --receipt-file PATH | --mode watchdog-once with the same identity and effect arguments.');
  const selected = loadCustodyConfig(['--env-file', envFile, '--stamp-config', stampConfig], false);
  const envelope = readEffectEnvelope(effectFile);
  if (envelope.subscription_id !== selected.config.subscription || envelope.resource_group !== selected.config.group ||
      envelope.storage_account !== selected.config.account || envelope.state_container !== selected.config.checkpoints)
    throw new Error('Effect envelope does not match the explicitly selected custody stamp.');
  if (fs.existsSync(receiptFile) && fs.realpathSync(receiptFile) === fs.realpathSync(effectFile))
    throw new Error('Effect receipt and allocation envelope must be separate files.');
  const boundary = new EffectBoundary(envelope, receiptFile);
  const responseWithholding = withholdResponseFile ? readResponseWithholdingPlan(withholdResponseFile, envelope, effectFile!, receiptFile!) : undefined;
  if (responseWithholding && (mode === 'setup-baseline' ? responseWithholding.transport !== 'campaign-blob' : responseWithholding.transport !== 'process'))
    throw new Error('Response-withholding target does not match the selected rehearsal stage.');
  const azure = dependencies.azure ?? new Azure(selected.config);
  const store = dependencies.store ?? createBlobCampaignStore({ account: selected.config.account, container: selected.config.checkpoints,
    subscriptionId: selected.config.subscription });
  const runCampaign = dependencies.campaignMain ?? campaignMain;
  const evaluateWatchdog = dependencies.watchdogOnce ?? watchdogOnce;
  const cleanupEnvelope = mode === 'producer-cleanup-step' ? readPrivateJson(inventoryFile!) : undefined;
  const initialInventory = mode === 'setup-baseline' ? readPrivateJson(initialInventoryFile!) : undefined;
  const result = await withEffectBoundary(boundary, async () => {
    if (mode === 'setup-baseline') {
      const ownerId = selected.env.CAMPAIGN_OWNER_ID;
      if (!ownerId || selected.env.CAMPAIGN_ID !== envelope.campaign_id || selected.env.CAMPAIGN_FENCING_TOKEN !== '1')
        throw new Error('Initial RunInventory and explicit campaign environment do not match the allocated row-free run.');
      const inventory = validateInitialRunInventory(initialInventory, envelope.campaign_id, ownerId);
      if (inventory.cell_run_id !== envelope.run_id || inventory.fencing_token !== 1)
        throw new Error('Initial RunInventory identity does not match the exact effect allocation.');
      const current = await store.read();
      if (resume) {
        if (!current || current.state.status !== 'active' || current.state.mode !== 'observe' ||
            current.state.campaignId !== envelope.campaign_id || current.state.ownerId !== ownerId ||
            current.state.fencingToken !== 1 || canonical_json(current.state.runInventory) !== canonical_json(inventory))
          throw new Error('Baseline restart requires the exact original active campaign and immutable initial RunInventory.');
        assertCampaignEnvironment(selected.env, current.state);
        await runCampaign(['capture-baseline', ownerId, '1', '--env-file', envFile!, '--stamp-config', stampConfig!],
          { store, azure, env: selected.env });
        return undefined;
      }
      if (current) throw new Error('Setup-baseline refuses to overwrite an existing campaign; use the exact resume state only.');
      await runCampaign(['start', envelope.campaign_id, ownerId,
        '--env-file', envFile!, '--stamp-config', stampConfig!], { store, azure, env: selected.env });
      await runCampaign(['publish-inventory', '--env-file', envFile!, '--stamp-config', stampConfig!],
        { store, azure, env: selected.env, runInventoryInput: inventory });
      await runCampaign(['capture-baseline', ownerId, '1',
        '--env-file', envFile!, '--stamp-config', stampConfig!], { store, azure, env: selected.env });
      return undefined;
    }
    const current = await store.read();
    if (!current || current.state.campaignId !== envelope.campaign_id ||
        current.state.runInventory?.cell_run_id !== envelope.run_id)
      throw new Error('Durable campaign/run identity does not match the exact effect allocation.');
    if (mode === 'producer-cleanup-step') {
      await runCampaign(['cleanup-step', selector!, '--env-file', envFile!, '--stamp-config', stampConfig!],
        { store, azure, env: selected.env, cleanupEnvelope });
      return undefined;
    }
    return evaluateWatchdog(selected.env, undefined, dependencies.store, azure);
  }, responseWithholding);
  responseWithholding?.assertConsumed();
  return result;
}

function isObject(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function exactKeys(value: Record<string, unknown>, expected: string[]): boolean {
  return Object.keys(value).sort().join(',') === expected.slice().sort().join(',');
}

function readResponseWithholdingPlan(filename: string, envelope: ReturnType<typeof readEffectEnvelope>,
  effectFile: string, receiptFile: string): ResponseWithholdingSupervisor {
  const value = readPrivateJson(filename);
  if (!isObject(value) || !exactKeys(value, ['schema_version', 'campaign_id', 'run_id', 'target', 'evidence_file']) ||
      value.schema_version !== 1 || value.campaign_id !== envelope.campaign_id || value.run_id !== envelope.run_id ||
      typeof value.evidence_file !== 'string' || !value.evidence_file || path.isAbsolute(value.evidence_file) ||
      path.basename(value.evidence_file) !== value.evidence_file || ['.', '..'].includes(value.evidence_file) || !isObject(value.target))
    throw new Error('Response-withholding plan is malformed or bound to another campaign/run.');
  const target = value.target;
  const targetKeys = ['transport', 'method', 'resource', 'path'];
  if ((!exactKeys(target, targetKeys) && !exactKeys(target, [...targetKeys, 'body_sha256'])) ||
      typeof target.transport !== 'string' || !['azure-rest', 'azure-auth', 'campaign-blob', 'process'].includes(target.transport) ||
      typeof target.method !== 'string' || typeof target.resource !== 'string' || typeof target.path !== 'string' ||
      target.body_sha256 !== undefined && (typeof target.body_sha256 !== 'string' || !/^[0-9a-f]{64}$/.test(target.body_sha256)))
    throw new Error('Response-withholding target has unexpected or invalid fields.');
  const typedTarget: ResponseWithholdingTarget = { transport: target.transport as ResponseWithholdingTarget['transport'],
    method: target.method, resource: target.resource, path: target.path,
    ...(typeof target.body_sha256 === 'string' ? { body_sha256: target.body_sha256 } : {}) };
  const allocation = envelope.effects.find(effect => effect.transport === typedTarget.transport &&
    effect.method === typedTarget.method && effect.resource === typedTarget.resource && effect.path === typedTarget.path &&
    effect.body_sha256 === typedTarget.body_sha256);
  if (!allocation) throw new Error('Response-withholding target must match one exact effect allocation.');
  const evidencePath = path.resolve(path.dirname(filename), value.evidence_file);
  if ([filename, effectFile, receiptFile].some(file => path.resolve(file) === evidencePath))
    throw new Error('Response-withholding evidence must be separate from its plan, allocation and receipt journal.');
  return new ResponseWithholdingSupervisor(envelope.campaign_id, envelope.run_id, typedTarget, evidencePath);
}

function readPrivateJson(filename: string): unknown {
  const stat = fs.lstatSync(filename);
  if (!stat.isFile() || stat.isSymbolicLink() || (stat.mode & 0o077) !== 0)
    throw new Error('Cleanup inventory must be a private regular file.');
  try { return JSON.parse(fs.readFileSync(filename, 'utf8')); }
  catch { throw new Error('Cleanup inventory is not valid JSON.'); }
}

if (process.argv[1] && ['aks-effect-rehearsal.ts', 'aks-effect-rehearsal.js'].includes(path.basename(process.argv[1])) &&
    import.meta.url === pathToFileURL(process.argv[1]).href) {
  boundedEffectRehearsal(process.argv.slice(2)).then(evaluation => {
    if (evaluation) console.log(JSON.stringify(evaluation));
  }).catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : 'Bounded effect rehearsal failed'); process.exitCode = 1;
  });
}
