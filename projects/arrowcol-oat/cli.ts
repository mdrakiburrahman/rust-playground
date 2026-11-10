import { pathToFileURL } from 'node:url';
import { run_tree } from '../../infra/azure/bootstrap.js';
import * as launch from './launch.js';

type AksSubcommand = 'destroy' | 'deploy' | 'setup';
type CommandLoader = (subcommand: AksSubcommand) => Promise<{
  main(args: string[], runner: typeof run_tree): Promise<void>;
}>;

export async function main(
  argv: string[],
  runner: typeof run_tree = run_tree,
  loadCommand: CommandLoader = (subcommand) => import(`./aks-${subcommand}.js`),
): Promise<void> {
  if (argv[0] !== 'aks') return launch.main(argv, runner);
  switch (argv[1]) {
    case 'destroy':
    case 'deploy':
    case 'setup':
      return (await loadCommand(argv[1])).main(argv.slice(2), runner);
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
