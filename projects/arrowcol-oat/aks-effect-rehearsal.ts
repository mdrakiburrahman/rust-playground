import fs from 'node:fs';
import path from 'node:path';
import { pathToFileURL } from 'node:url';
import { parseArgs } from 'node:util';
import { Azure } from '../../infra/azure/bootstrap.js';
import { EffectBoundary, readEffectEnvelope, withEffectBoundary } from '../../infra/azure/effect-boundary.js';
import { loadCustodyConfig } from './aks-custody-config.js';
import { createBlobCampaignStore, campaignMain, type CampaignStore } from './aks-campaign.js';
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
    'inventory-file': { type: 'string' }, 'receipt-file': { type: 'string' },
  } });
  const { mode, selector, 'env-file': envFile, 'stamp-config': stampConfig,
    'effect-envelope': effectFile, 'inventory-file': inventoryFile, 'receipt-file': receiptFile } = parsed.values;
  if (!['producer-cleanup-step', 'watchdog-once'].includes(mode ?? '') || !envFile || !stampConfig || !effectFile || !receiptFile ||
      mode === 'producer-cleanup-step' && (!selector || !Object.hasOwn(CELL_CLEANUP_SELECTORS, selector) || !inventoryFile) ||
      mode === 'watchdog-once' && (selector !== undefined || inventoryFile !== undefined))
    throw new Error('Usage: aks-effect-rehearsal.ts --mode producer-cleanup-step --selector SELECTOR --inventory-file PATH --env-file PATH --stamp-config PATH --effect-envelope PATH --receipt-file PATH | --mode watchdog-once with the same identity and effect arguments.');
  const selected = loadCustodyConfig(['--env-file', envFile, '--stamp-config', stampConfig], false);
  const envelope = readEffectEnvelope(effectFile);
  if (envelope.subscription_id !== selected.config.subscription || envelope.resource_group !== selected.config.group ||
      envelope.storage_account !== selected.config.account || envelope.state_container !== selected.config.checkpoints)
    throw new Error('Effect envelope does not match the explicitly selected custody stamp.');
  if (fs.existsSync(receiptFile) && fs.realpathSync(receiptFile) === fs.realpathSync(effectFile))
    throw new Error('Effect receipt and allocation envelope must be separate files.');
  const boundary = new EffectBoundary(envelope, receiptFile);
  const azure = dependencies.azure ?? new Azure(selected.config);
  const store = dependencies.store ?? createBlobCampaignStore({ account: selected.config.account, container: selected.config.checkpoints,
    subscriptionId: selected.config.subscription });
  const runCampaign = dependencies.campaignMain ?? campaignMain;
  const evaluateWatchdog = dependencies.watchdogOnce ?? watchdogOnce;
  const cleanupEnvelope = mode === 'producer-cleanup-step' ? readPrivateJson(inventoryFile!) : undefined;
  return await withEffectBoundary(boundary, async () => {
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
  });
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
