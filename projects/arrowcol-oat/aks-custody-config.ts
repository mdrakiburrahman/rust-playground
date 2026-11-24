import { parseArgs } from 'node:util';
import { Config, load_env } from '../../infra/azure/bootstrap.js';
import { loadStampConfig } from '../../infra/azure/stamp.js';

export function loadCustodyConfig(args: string[], watchdog = false, ambient: NodeJS.ProcessEnv = process.env) {
  const parsed = parseArgs({ args, allowPositionals: !watchdog, options: {
    'env-file': { type: 'string' }, 'stamp-config': { type: 'string' },
    ...(watchdog ? { once: { type: 'boolean' as const }, watch: { type: 'boolean' as const } } : {}),
  } });
  if (!parsed.values['env-file'] || !parsed.values['stamp-config'])
    throw new Error('Custody requires explicit --env-file and --stamp-config paths.');
  if (watchdog && Boolean(parsed.values.once) === Boolean(parsed.values.watch))
    throw new Error('Select exactly one watchdog mode: --once or --watch.');
  const env = { ...load_env(parsed.values['env-file']), ...ambient };
  const config = new Config(env, loadStampConfig(parsed.values['stamp-config']));
  if (env.ARM_SUBSCRIPTION_ID !== config.subscription || env.CAMPAIGN_STORAGE_ACCOUNT !== config.account ||
      env.CAMPAIGN_STATE_CONTAINER !== config.checkpoints)
    throw new Error('Custody subscription/account/container must exactly match the selected stamp configuration.');
  return { config, env, positionals: parsed.positionals, once: parsed.values.once === true };
}
