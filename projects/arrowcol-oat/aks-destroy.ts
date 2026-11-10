import { run_tree } from '../../infra/azure/bootstrap.js';
import type { DestroyOptions, DestroyResult } from './aks-types.js';

export async function destroy(_options: DestroyOptions, _runner: typeof run_tree = run_tree): Promise<DestroyResult> {
  throw new Error('aks destroy is not implemented yet');
}

export async function main(_args: string[], _runner: typeof run_tree = run_tree): Promise<void> {
  throw new Error('aks destroy is not implemented yet');
}
