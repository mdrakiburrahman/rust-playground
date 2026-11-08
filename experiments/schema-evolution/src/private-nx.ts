import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';
import { Config, load_env, run_tree } from '../../../infra/azure/bootstrap.ts';

const WORKSPACE = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../..');
const CLI_CONTEXT = new Set(['AZURE_CONFIG_DIR', 'AZURE_EXTENSION_DIR']);

export function privateEnvironment(values: Record<string, string>, ambient: NodeJS.ProcessEnv, mode: string): NodeJS.ProcessEnv {
  if (mode !== 'schema-evolution') throw new Error('Private Nx requires explicit --mode schema-evolution.');
  const config = new Config(values, 'schema-evolution');
  if (config.group !== 'rust-playground-schema-evo')
    throw new Error('Private Nx execution requires the isolated schema-evo resource group.');
  const env = { ...ambient };
  for (const key of Object.keys(env)) if (/^(AZURE_|ARROWCOL_)/.test(key) && !CLI_CONTEXT.has(key)) delete env[key];
  return { ...env, ...values, ...config.generated(), NX_LOAD_DOT_ENV_FILES: 'false' };
}

export function privateCommand(argv: string[]): { command: string[]; envFile: string } {
  const { values, positionals } = parseArgs({ args: argv, allowPositionals: true, options: {
    mode: { type: 'string' }, 'env-file': { type: 'string' }, 'ready-env': { type: 'string' }, tables: { type: 'string' },
    output: { type: 'string' }, out: { type: 'string' }, 'wait-seconds': { type: 'string' },
  } });
  if (positionals.length !== 1 || argv[0] !== positionals[0] ||
      !['provision', 'run'].includes(positionals[0]!) || !values['env-file'] || values.mode !== 'schema-evolution')
    throw new Error('Use private-nx.ts provision|run --mode schema-evolution --env-file <approved-private-input> with target options.');
  const provision = positionals[0] === 'provision';
  if (provision ? !values.tables || !values.output || values['ready-env'] || values.out :
    !values['ready-env'] || values.tables || values.output || values['wait-seconds'])
    throw new Error('Provision requires fresh --tables/--output; run requires --ready-env.');
  return { envFile: values['env-file'], command: ['npx', 'nx', 'run',
    provision ? 'azure-infra:provision' : 'schema-evolution-experiment:run', '--',
    ...(!provision ? ['--target', 'azure'] : []), ...argv.slice(1)] };
}

export async function main(argv = process.argv.slice(2)): Promise<number> {
  const { command, envFile } = privateCommand(argv);
  const values = load_env(path.resolve(WORKSPACE, envFile));
  const config = new Config(values, 'schema-evolution');
  const result = await run_tree(command, 5400, WORKSPACE, privateEnvironment(values, process.env, 'schema-evolution'));
  const redacted = (text: string) => text.split('\n').map(line => config.redact(line)).join('\n');
  process.stdout.write(redacted(result.stdout));
  process.stderr.write(redacted(result.stderr));
  return result.returncode;
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url))
  process.exitCode = await main();
