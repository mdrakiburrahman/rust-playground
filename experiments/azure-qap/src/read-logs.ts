import fs from 'node:fs';
import path from 'node:path';
import { parseArgs } from 'node:util';
import { fileURLToPath } from 'node:url';
import { parse as parseYaml } from 'yaml';
import { Azure, Config, ROOT, identity_principal, load_env } from '../../../infra/azure/bootstrap.ts';
import { object } from './kusto.js';
import { parseReads, validateScope, type ReadScope, type ReadWindow } from './storage-reads.js';
import { storageExportQuery, type StorageReadQuery } from './storage-export.js';
import { ownedReads } from './owned-reads.js';

export function parseStorageReadQuery(value: unknown): StorageReadQuery {
  const raw = object(value);
  const scopeValue = object(raw.scope);
  const owned = object(scopeValue.owned);
  const windowValue = object(raw.window);
  if (raw.source !== 'storage' || typeof raw.logAccount !== 'string' || !/^[a-z0-9]{3,24}$/.test(raw.logAccount) ||
      (raw.raw !== undefined && typeof raw.raw !== 'boolean')) throw new Error('Invalid diagnostic storage query.');
  const text = (value: unknown): string => {
    if (typeof value !== 'string') throw new Error('Diagnostic storage query field must be a string.');
    return value;
  };
  if (!Array.isArray(owned.requestIds) || owned.requestIds.some(value => typeof value !== 'string'))
    throw new Error('Diagnostic storage query has no valid owned-request receipts.');
  const scope: ReadScope = { workspace: text(scopeValue.workspace), account: text(scopeValue.account),
    container: text(scopeValue.container), requester: text(scopeValue.requester), prefix: text(scopeValue.prefix),
    owned: { marker: text(owned.marker), requestIds: owned.requestIds.map(text) } };
  validateScope(scope);
  const time = (value: unknown): number => {
    if (typeof value !== 'number' || !Number.isSafeInteger(value) || value <= 0)
      throw new Error('Invalid diagnostic storage time window.');
    return value;
  };
  const window: ReadWindow = { from: time(windowValue.from), mutationAt: time(windowValue.mutationAt),
    windowEnd: time(windowValue.windowEnd) };
  if (window.from > window.mutationAt || window.mutationAt > window.windowEnd)
    throw new Error('Diagnostic storage time window is reversed.');
  return { source: 'storage', logAccount: raw.logAccount, scope, window, ...(raw.raw === true ? { raw: true } : {}) };
}

async function main(): Promise<void> {
  const { values } = parseArgs({ options: { query: { type: 'string' }, env: { type: 'string', default: '.env' } } });
  if (!values.query) throw new Error('Usage: read-logs.ts --query <saved-query.yaml> [--env <private-env-file>]');
  const query = parseStorageReadQuery(parseYaml(fs.readFileSync(path.resolve(values.query), 'utf8')));
  const [, runId, trial] = query.scope.owned!.marker.split('/');
  const report = path.dirname(path.dirname(path.resolve(values.query)));
  const verified = ownedReads(path.join(report, 'http-traces', runId!, trial + '.jsonl'), query.scope.owned!.marker);
  if (JSON.stringify(verified.requestIds) !== JSON.stringify([...query.scope.owned!.requestIds].sort()))
    throw new Error('Saved query exclusions do not match the preserved owned HTTP receipts.');
  const env = path.resolve(ROOT, values.env);
  const generated = path.join(path.dirname(env), '.env.azure');
  const config = new Config({ ...load_env(env), ...(fs.existsSync(generated) ? load_env(generated) : {}), ...process.env });
  const azure = new Azure(config);
  if (config.readLogAccount !== query.logAccount || config.account !== query.scope.account ||
      config.experimentContainer !== query.scope.container || await identity_principal(azure) !== query.scope.requester)
    throw new Error('Saved read query does not match the exact configured stamp, container and shared identity.');
  const rows: Record<string, unknown>[] = [];
  const response = await storageExportQuery(azure, query.logAccount, (_scope, events) => rows.push(...events))(
    '', AbortSignal.timeout(120_000), { scope: query.scope, window: query.window });
  console.log(JSON.stringify(query.raw ? rows : parseReads(response, query.scope), null, 2));
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url))
  main().catch(error => { console.error(error instanceof Error ? error.message : String(error)); process.exitCode = 1; });
