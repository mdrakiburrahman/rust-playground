import { existsSync, readFileSync } from 'node:fs';
import path from 'node:path';
import { parseEnv } from 'node:util';

export type LoadtestMode = 'local' | 'azure';

export function parseMode(value: string): LoadtestMode {
  if (value !== 'local' && value !== 'azure') throw new Error('--mode must be local or azure');
  return value;
}

/** Private files are inputs, never run evidence. Explicit environment wins. */
export function modeEnvironment(workspace: string, mode: LoadtestMode, inherited: NodeJS.ProcessEnv): NodeJS.ProcessEnv {
  const files: NodeJS.ProcessEnv = {};
  if (mode === 'azure') {
    for (const name of ['.env', '.env.azure']) {
      const file = path.join(workspace, name);
      if (existsSync(file)) Object.assign(files, parseEnv(readFileSync(file, 'utf8')));
    }
  }
  return { ...files, ...inherited, ARROWCOL_MODE: mode };
}

export interface AzureTargets {
  root: string;
  directRoot: string;
  dataflowRoot: string;
  kustoEndpoint: string;
  database: string;
}

/** Validate before any local reset or resource mutation; never fall back to local. */
export function azureTargets(env: NodeJS.ProcessEnv, run: string): AzureTargets {
  for (const key of ['ARROWCOL_AZURE_TABLE_ROOT', 'AZURE_STORAGE_ACCOUNT', 'AZURE_STORAGE_CONTAINER',
    'AZURE_STORAGE_CHECKPOINT_CONTAINER', 'AZURE_KUSTO_URI', 'AZURE_KUSTO_DATABASE']) {
    if (!env[key]) throw new Error(`Azure mode requires private configuration ${key}`);
  }
  if (!/^[a-z0-9]{3,24}$/u.test(env.AZURE_STORAGE_ACCOUNT!)) throw new Error('Invalid AZURE_STORAGE_ACCOUNT');
  if (!/^[a-z0-9](?:[a-z0-9-]{1,61}[a-z0-9])$/u.test(env.AZURE_STORAGE_CONTAINER!)) {
    throw new Error('Invalid AZURE_STORAGE_CONTAINER');
  }
  if (!/^[A-Za-z0-9_-]+$/u.test(run)) throw new Error('Invalid isolated Azure run name');
  if (!/^[A-Za-z][A-Za-z0-9_-]{0,127}$/u.test(env.AZURE_KUSTO_DATABASE!)) throw new Error('Invalid AZURE_KUSTO_DATABASE');
  let endpoint: URL;
  try {
    endpoint = new URL(env.AZURE_KUSTO_URI!);
  } catch {
    throw new Error('Invalid AZURE_KUSTO_URI');
  }
  if (endpoint.protocol !== 'https:' || endpoint.username || endpoint.password || endpoint.search || endpoint.hash ||
      endpoint.pathname !== '/' || endpoint.port || !endpoint.hostname.endsWith('.kusto.windows.net')) {
    throw new Error('AZURE_KUSTO_URI must be an Azure Kusto HTTPS origin without credentials');
  }
  let base: URL;
  try {
    base = new URL(env.ARROWCOL_AZURE_TABLE_ROOT!);
  } catch {
    throw new Error('Invalid ARROWCOL_AZURE_TABLE_ROOT');
  }
  if (base.protocol !== 'abfss:' || base.username !== env.AZURE_STORAGE_CONTAINER ||
      base.hostname !== `${env.AZURE_STORAGE_ACCOUNT}.dfs.core.windows.net` || base.password || base.port ||
      base.search || base.hash || /[%\\]/u.test(env.ARROWCOL_AZURE_TABLE_ROOT!) ||
      env.ARROWCOL_AZURE_TABLE_ROOT!.split('/').some(part => part === '.' || part === '..')) {
    throw new Error('ARROWCOL_AZURE_TABLE_ROOT must remain inside the configured account/container without credentials or traversal');
  }
  const root = `${base.href.replace(/\/+$/u, '')}/loadtest/${run}`;
  return {
    root,
    directRoot: `${root}/direct`,
    dataflowRoot: `${root}/dataflow`,
    kustoEndpoint: endpoint.origin,
    database: env.AZURE_KUSTO_DATABASE!,
  };
}
