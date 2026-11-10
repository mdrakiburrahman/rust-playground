import { run_tree } from '../../infra/azure/bootstrap.js';
import type { LiveStamp, SetupOptions, SetupResult } from './aks-types.js';

export async function setup(_options: SetupOptions, _runner: typeof run_tree = run_tree): Promise<SetupResult> {
  throw new Error('aks setup is not implemented yet');
}

export async function discoverLiveStamp(_options: SetupOptions, _runner: typeof run_tree = run_tree): Promise<LiveStamp> {
  throw new Error('aks setup is not implemented yet');
}

export async function main(_args: string[], _runner: typeof run_tree = run_tree): Promise<void> {
  throw new Error('aks setup is not implemented yet');
}
