/** Offline driver of the real Azure bootstrap parent's initializer environment and reconciliation. */
import { readFileSync, statSync } from 'node:fs';
import path from 'node:path';
import { Config, initializer_env, NotReady, reconcile } from '../../../../../infra/azure/bootstrap.js';
import type { Azure } from '../../../../../infra/azure/bootstrap.js';

class StopBeforeKusto extends Error {}

class OfflineAzure {
  reads = 0;
  kql_calls = 0;
  constructor(readonly c: Config, readonly store: string) {}

  async blob(method: string, container: string, name = '', query?: unknown,
    _body?: unknown, _headers: unknown = {}, missing_ok = false): Promise<[Buffer | null, null]> {
    if (method !== 'GET' || container !== this.c.data || query) throw new Error('unexpected storage request');
    const file = path.join(this.store, name);
    try {
      if (!statSync(file).isFile()) throw Object.assign(new Error('not a file'), { code: 'ENOENT' });
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
      if (missing_ok) return [null, null];
      throw new Error('missing blob without missing_ok');
    }
    this.reads++;
    return [readFileSync(file), null];
  }
  async arm(): Promise<never> { throw new StopBeforeKusto(); }
  async kql(): Promise<never> { this.kql_calls++; throw new Error('Kusto attempted'); }
}

async function main(): Promise<void> {
  const [mode, config, manifest, store] = process.argv.slice(2);
  const values = JSON.parse(readFileSync(config, 'utf8'));
  const c = new Config(values);
  if (mode === 'child-env') {
    console.log(JSON.stringify(initializer_env(values, c, manifest, store)));
    return;
  }
  if (mode !== 'reconcile') throw new Error('unknown mode');
  const azure = new OfflineAzure(c, store);
  let outcome: { outcome: string; detail?: string };
  try {
    await reconcile(azure as unknown as Azure, manifest);
    outcome = { outcome: 'completed' };
  } catch (error) {
    if (error instanceof StopBeforeKusto) outcome = { outcome: 'stopped-before-kusto' };
    else if (error instanceof NotReady) outcome = { outcome: 'not-ready', detail: error.message };
    else throw error;
  }
  console.log(JSON.stringify({ ...outcome, validated: azure.reads, kql: azure.kql_calls }));
}

await main();
