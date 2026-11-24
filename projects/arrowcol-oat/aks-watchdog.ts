import { setTimeout as delay } from 'node:timers/promises';
import { pathToFileURL } from "node:url";
import { createBlobCampaignStore, readArmOperationStatus, type CleanupStepName, type CampaignState, type CampaignStore } from './aks-campaign.js';
import { reconcileCleanupOnce, reconcileOnce, reconcilePendingArmOperations, type CleanupProgress } from './aks-reconcile.js';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { Azure } from '../../infra/azure/bootstrap.js';
import { loadCustodyConfig } from './aks-custody-config.js';
import { WATCHDOG_INTERVAL_MS } from './aks-cleanup-budget.js';
import { DEFAULT_EFFECTIVE_STOP_BUDGETS, parseEffectiveStopBudgets, type EffectiveStopBudgets } from './stop-budgets.js';
export { WATCHDOG_INTERVAL_MS };

export type CleanupExecutor = (step: CleanupStepName, state: CampaignState, idempotencyKey: string) => Promise<'pending' | 'complete'>;
export interface WatchdogReadOnlyAzure { readArmStatus(url: string): Promise<{ status: number; body?: unknown }> }

type StopBudgetSource = EffectiveStopBudgets & { budget_source: 'effective_stop_budgets' | 'legacy_ceiling' };
function stopBudgetsForInventory(inventory: NonNullable<CampaignState['runInventory']>): StopBudgetSource {
  const persisted = inventory.effective_stop_budgets;
  if (!persisted) return { ...DEFAULT_EFFECTIVE_STOP_BUDGETS, budget_source: 'legacy_ceiling' };
  return { ...parseEffectiveStopBudgets(persisted), budget_source: 'effective_stop_budgets' };
}

export async function watchdogOnce(env: NodeJS.ProcessEnv = process.env, executeCleanup?: CleanupExecutor,
  storeOverride?: CampaignStore, selectedAzure?: Azure | WatchdogReadOnlyAzure): Promise<Record<string, unknown>> {
  if (!storeOverride && (!(selectedAzure instanceof Azure) || env.ARM_SUBSCRIPTION_ID !== selectedAzure.c.subscription ||
      env.CAMPAIGN_STORAGE_ACCOUNT !== selectedAzure.c.account || env.CAMPAIGN_STATE_CONTAINER !== selectedAzure.c.checkpoints))
    throw new Error('Watchdog requires explicit validated stamp configuration and matching custody subscription/account/container.');
  const subscriptionId = env.ARM_SUBSCRIPTION_ID ?? '';
  const store = storeOverride ?? createBlobCampaignStore({ account: env.CAMPAIGN_STORAGE_ACCOUNT ?? '',
    container: env.CAMPAIGN_STATE_CONTAINER ?? '', subscriptionId });
  const armStatusReader = selectedAzure && 'readArmStatus' in selectedAzure
    ? (url: string) => selectedAzure.readArmStatus(url)
    : (url: string) => readArmOperationStatus(url, subscriptionId);
  const operations = await reconcilePendingArmOperations(store, armStatusReader, subscriptionId);
  const result = await reconcileOnce(store);
  const current = await store.read();
  let cleanupProgress: CleanupProgress | undefined;
  if (current?.state.status === 'complete_unsettled') cleanupProgress = 'complete_unsettled';
  else if (current?.state.status === 'complete') cleanupProgress = 'complete';
  if (current && (current.state.status === 'fenced' || current.state.status === 'cleaning')) {
    if (!executeCleanup && storeOverride) throw new Error('Fenced campaign requires an external cleanup executor; refusing to claim safe idle.');
    if (executeCleanup) cleanupProgress = await reconcileCleanupOnce(store, executeCleanup);
    else {
      if (!(selectedAzure instanceof Azure)) throw new Error('Default cleanup providers require the selected Azure client.');
      const azure = selectedAzure;
      const { createDefaultCleanupProviders } = await import('./aks-default-providers.js');
      const { createExternalCleanupExecutor } = await import('./aks-cleanup-executor.js');
      const directory = fs.mkdtempSync(path.join(os.tmpdir(), 'oat-watchdog-cleanup-'));
      try {
        const actions = createDefaultCleanupProviders(store, { azure, workDirectory: directory });
        if (!current.state.runInventory) throw new Error('Cleanup requires a persisted RunInventory.');
        cleanupProgress = await reconcileCleanupOnce(store, createExternalCleanupExecutor(store, actions,
          stopBudgetsForInventory(current.state.runInventory)));
      } finally { fs.rmSync(directory, { recursive: true }); }
    }
  }
  const latest = await store.read();
  return { event: 'campaign_custody_evaluation', ...result, ...(cleanupProgress ? { cleanupProgress } : {}),
    ...(latest ? { campaignStatus: latest.state.status, cleanupStatus: latest.state.cleanupStatus } : {}),
    ...(latest?.state.runInventory ? { maintenanceStopBudget: {
      maintenance_stop_timeout_ms: stopBudgetsForInventory(latest.state.runInventory).maintenanceStopTimeoutMs,
      budget_source: stopBudgetsForInventory(latest.state.runInventory).budget_source } } : {}),
    pendingArmOperations: operations?.operations.filter(operation => operation.status === 'pending' || operation.status === 'indeterminate').length ?? 0,
    observedAtMs: Date.now() };
}

export async function runWatchdog(signal: AbortSignal, evaluate: () => Promise<Record<string, unknown>> = () => watchdogOnce()):
  Promise<'aborted' | 'complete_unsettled'> {
  let consecutiveFailures = 0;
  while (!signal.aborted) {
    const started = Date.now();
    try {
      const result = await evaluate();
      console.log(JSON.stringify(result));
      if (result.cleanupProgress === 'complete_unsettled') return 'complete_unsettled';
      consecutiveFailures = 0;
    } catch (error) {
      consecutiveFailures++;
      console.error(JSON.stringify({ event: 'campaign_custody_watchdog_failure', observedAtMs: Date.now(),
        consecutiveFailures, error: error instanceof Error ? error.message : 'Unexpected watchdog failure' }));
    }
    try {
      await delay(Math.max(0, WATCHDOG_INTERVAL_MS - (Date.now() - started)), undefined, { signal });
    } catch (error) {
      if (!signal.aborted && error instanceof Error && error.name !== 'AbortError') throw error;
    }
  }
  return 'aborted';
}

async function main(args: string[]): Promise<void> {
  const selected = loadCustodyConfig(args, true), azure = new Azure(selected.config);
  const evaluate = () => watchdogOnce(selected.env, undefined, undefined, azure);
  if (selected.once) {
    const result = await evaluate();
    console.log(JSON.stringify(result));
    if (result.cleanupProgress === 'complete_unsettled') process.exitCode = 2;
    return;
  }
  const controller = new AbortController();
  const stop = () => controller.abort();
  process.once('SIGINT', stop);
  process.once('SIGTERM', stop);
  if (await runWatchdog(controller.signal, evaluate) === 'complete_unsettled') process.exitCode = 2;
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main(process.argv.slice(2)).catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : 'Campaign watchdog failed'); process.exitCode = 1;
  });
}
