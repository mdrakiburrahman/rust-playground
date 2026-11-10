import { run_tree } from '../../infra/azure/bootstrap.js';
import type { DeployOptions, DeployResult } from './aks-types.js';

export async function deploy(_options: DeployOptions, _runner: typeof run_tree = run_tree): Promise<DeployResult> {
  throw new Error('aks deploy is not implemented yet');
}

export async function main(_args: string[], _runner: typeof run_tree = run_tree): Promise<void> {
  throw new Error('aks deploy is not implemented yet');
}
