import { pathToFileURL } from 'node:url';
import { run_tree } from '../../infra/azure/bootstrap.js';
import * as launch from './launch.js';

export async function main(argv: string[], runner: typeof run_tree = run_tree): Promise<void> {
  if (argv[0] !== 'aks') return launch.main(argv, runner);
  switch (argv[1]) {
    case 'destroy':
      return (await import('./aks-destroy.js')).main(argv.slice(2), runner);
    case 'deploy':
      return (await import('./aks-deploy.js')).main(argv.slice(2), runner);
    case 'setup':
      return (await import('./aks-setup.js')).main(argv.slice(2), runner);
    default:
      console.error('Usage: arrowcol-oat aks <destroy|deploy|setup> [options]');
      process.exitCode = 2;
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main(process.argv.slice(2)).catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
