import { setTimeout as delay } from 'node:timers/promises';
import { pathToFileURL } from "node:url";
import { createBlobCampaignStore, readArmOperationStatus, type CleanupStepName, type CampaignState, type CampaignStore } from './aks-campaign.js';
import { reconcileCleanupOnce, reconcileOnce, reconcilePendingArmOperations, type CleanupProgress } from './aks-reconcile.js';

export const WATCHDOG_INTERVAL_MS = 60_000;

export type CleanupExecutor = (step: CleanupStepName, state: CampaignState, idempotencyKey: string) => Promise<'pending' | 'complete'>;

export async function watchdogOnce(env: NodeJS.ProcessEnv = process.env, executeCleanup?: CleanupExecutor,
  storeOverride?: CampaignStore): Promise<Record<string, unknown>> {
  const subscriptionId = env.ARM_SUBSCRIPTION_ID ?? '';
  const store = storeOverride ?? createBlobCampaignStore({ account: env.CAMPAIGN_STORAGE_ACCOUNT ?? '',
    container: env.CAMPAIGN_STATE_CONTAINER ?? '', subscriptionId });
  const operations = await reconcilePendingArmOperations(store,
    url => readArmOperationStatus(url, subscriptionId), subscriptionId);
  const result = await reconcileOnce(store);
  const current = await store.read();
  let cleanupProgress: CleanupProgress | undefined;
  if (current && (current.state.status === 'fenced' || current.state.status === 'cleaning')) {
    if (!executeCleanup) throw new Error('Fenced campaign requires an external cleanup executor; refusing to claim safe idle.');
    cleanupProgress = await reconcileCleanupOnce(store, executeCleanup);
  }
  return { event: 'campaign_custody_evaluation', ...result, ...(cleanupProgress ? { cleanupProgress } : {}),
    pendingArmOperations: operations?.operations.filter(operation => operation.status === 'pending' || operation.status === 'indeterminate').length ?? 0,
    observedAtMs: Date.now() };
}

export async function runWatchdog(signal: AbortSignal, evaluate: () => Promise<Record<string, unknown>> = () => watchdogOnce()): Promise<void> {
  let consecutiveFailures = 0;
  while (!signal.aborted) {
    const started = Date.now();
    try {
      console.log(JSON.stringify(await evaluate()));
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
}

async function main(args: string[]): Promise<void> {
  if (args.length === 1 && args[0] === '--once') {
    console.log(JSON.stringify(await watchdogOnce()));
    return;
  }
  if (args.length !== 1 || args[0] !== '--watch') throw new Error('Usage: aks-watchdog.ts --once | --watch');
  const controller = new AbortController();
  const stop = () => controller.abort();
  process.once('SIGINT', stop);
  process.once('SIGTERM', stop);
  await runWatchdog(controller.signal);
}
if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main(process.argv.slice(2)).catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : 'Campaign watchdog failed'); process.exitCode = 1;
  });
}
