import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { dirname, isAbsolute, relative, resolve } from 'node:path';
import { parseArgs } from 'node:util';
import { Azure, ROOT, runtime, type run_tree } from '../../infra/azure/bootstrap.js';
import { dispatchExternalEffect, processEffectRequest } from '../../infra/azure/effect-boundary.js';
import { loadCustodyConfig } from './aks-custody-config.js';
import { acquireFleetAuthority } from './aks-fleet-custody.js';
import { createFleetAdapter, observeFleetBinding, observeFleetImageArchitectures } from './aks-fleet-provider.js';
import { cleanupAksFleet, runAksFleet } from './aks-fleet.js';
import { main as launch, readOatConfigForMode } from './launch.js';
import { assertSafeOutputParents, main as prepare } from './prepare-experiments.js';
import { parseFleetSizing } from './fleet-config.js';
import { validateRuntime } from './runtime.js';

interface FleetCliDependencies {
  custody: typeof loadCustodyConfig;
  binding: typeof observeFleetBinding;
  acquire: typeof acquireFleetAuthority;
  adapter: typeof createFleetAdapter;
  images: typeof observeFleetImageArchitectures;
  launch: typeof launch;
  prepare: typeof prepare;
}

function seconds(value: string | undefined, fallback: number, label: string): number {
  const parsed = value === undefined ? fallback : Number(value);
  if (!Number.isSafeInteger(parsed) || parsed < 60 || parsed > 21_600)
    throw new Error(`${label} must be an integer from 60 through 21600 seconds`);
  return parsed;
}

export async function main(
  args: string[], runner: typeof run_tree = runtime.run_tree, overrides: Partial<FleetCliDependencies> = {},
): Promise<void> {
  const { values, positionals } = parseArgs({ args, allowPositionals: true, options: {
    'env-file': { type: 'string' }, 'stamp-config': { type: 'string' }, config: { type: 'string' },
    'collector-pool': { type: 'string' }, 'owner-id': { type: 'string' }, out: { type: 'string' },
    'scale-seconds': { type: 'string' }, 'run-seconds': { type: 'string' }, 'cleanup-seconds': { type: 'string' },
    help: { type: 'boolean' },
  } });
  if (values.help) {
    console.log('Usage: arrowcol-oat aks fleet <run|kill> --env-file <custody.env> --stamp-config <stamp.yaml> ' +
      '--collector-pool <pool> --owner-id <UUID> [--config <auto-config.json> --out <new out/arrowcol-oat/directory>] ' +
      '[--scale-seconds 7200 --run-seconds 7200 --cleanup-seconds 3600]');
    return;
  }
  const [action] = positionals;
  if (positionals.length !== 1 || !['run', 'kill'].includes(action ?? ''))
    throw new Error('fleet requires exactly one action: run or kill');
  for (const name of ['env-file', 'stamp-config', 'collector-pool', 'owner-id'] as const) {
    if (!values[name]?.trim()) throw new Error(`fleet requires --${name}`);
  }
  if (!/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/i.test(values['owner-id']!))
    throw new Error('fleet --owner-id must be a UUID retained for recovery');
  const scaleSeconds = seconds(values['scale-seconds'], 7200, '--scale-seconds');
  const runSeconds = seconds(values['run-seconds'], 7200, '--run-seconds');
  const cleanupSeconds = seconds(values['cleanup-seconds'], 3600, '--cleanup-seconds');
  if (action === 'run' && scaleSeconds + runSeconds + cleanupSeconds > 21_600)
    throw new Error('fleet scale, run and cleanup budgets must fit six hours');
  if (action === 'run' && (!values.config || !values.out))
    throw new Error('fleet run requires --config and a new private --out directory');
  const deps: FleetCliDependencies = {
    custody: loadCustodyConfig,
    binding: observeFleetBinding, acquire: acquireFleetAuthority, adapter: createFleetAdapter,
    images: observeFleetImageArchitectures, launch, prepare, ...overrides,
  };
  const { config } = deps.custody([
    '--env-file', values['env-file']!, '--stamp-config', values['stamp-config']!,
  ]);
  const azure = new Azure(config);
  const input = action === 'run' ? await (async () => {
    const source = await readOatConfigForMode(values.config!, 'aks');
    const platform = source.platform;
    if (!platform || typeof platform !== 'object' || !('fleet' in platform) ||
        parseFleetSizing(platform.fleet).collector_pool !== values['collector-pool'] ||
        source.stamp !== config.stamp || source.campaign_profile !== 'scale_out_single_table' ||
        !Array.isArray(source.tables) || source.tables.length !== 1)
      throw new Error('fleet config must select the approved collector, stamp and single-table scale-out topology');
    if (!('runtime_config_file' in platform) || typeof platform.runtime_config_file !== 'string')
      throw new Error('fleet requires generated private runtime configuration');
    const runtimeConfig = validateRuntime(JSON.parse(await readFile(platform.runtime_config_file, 'utf8')));
    if (runtimeConfig.stamp !== config.stamp || typeof source.dmv_client_image !== 'string' ||
        source.maintenance_image !== undefined && typeof source.maintenance_image !== 'string')
      throw new Error('fleet requires matching runtime stamp and pinned client/maintenance images');
    const images = await deps.images(azure, [...Object.values(runtimeConfig.images)
      .filter((image): image is string => typeof image === 'string'), source.dmv_client_image,
      ...(typeof source.maintenance_image === 'string' ? [source.maintenance_image] : [])]);
    const out = resolve(ROOT, values.out!);
    const child = relative(resolve(ROOT, 'out/arrowcol-oat'), out);
    if (!child || child.startsWith('..') || isAbsolute(child)) throw new Error('fleet --out must be a new private out/arrowcol-oat subdirectory');
    await assertSafeOutputParents(ROOT, out);
    await mkdir(dirname(out), { recursive: true, mode: 0o700 });
    await mkdir(out, { mode: 0o700 });
    return { out, images };
  })() : undefined;
  const binding = await deps.binding(azure, values['collector-pool']!);
  const started = Date.now();
  const authority = await deps.acquire(azure, binding, { ownerId: values['owner-id']!, resume: action === 'kill' });
  try {
    const observedRunner: typeof run_tree = runner === runtime.run_tree ? runner : (...params) =>
      dispatchExternalEffect(processEffectRequest(params[0]), () => runner(...params));
    const adapter = deps.adapter(azure, binding, authority, observedRunner);
    if (action === 'kill') {
      if (authority.baselineEstablished === false) await adapter.establishBaseline();
      const cleanup = await cleanupAksFleet(adapter, authority, {
        deadlineAtMs: Date.now() + cleanupSeconds * 1000, includePoolNames: binding.pools.map(pool => pool.name),
        expectedScaleCounts: Object.fromEntries(binding.pools.map(pool => [pool.name, pool.count])),
      });
      if (!cleanup.verified) throw new Error(`Fleet kill cannot verify zero selected user nodes: ${cleanup.failed.join(', ')}`);
      await authority.complete();
      console.log(JSON.stringify({ owner_id: values['owner-id'], ranking_metrics: null, cleanup }));
      return;
    }
    if (!input) throw new Error('Fleet run inputs are missing');
    const { out, images } = input;
    await adapter.establishBaseline();
    const controller = new AbortController();
    const interrupted = () => controller.abort(new Error('Fleet execution interrupted'));
    process.once('SIGINT', interrupted);
    process.once('SIGTERM', interrupted);
    let fenceError: unknown;
    let heartbeatRunning = false;
    const heartbeat = setInterval(() => {
      if (heartbeatRunning) return;
      heartbeatRunning = true;
      void authority.assertCurrent().catch(error => {
        fenceError = error;
        controller.abort(error);
      }).finally(() => { heartbeatRunning = false; });
    }, 20_000);
    const request = {
      enabled: true,
      collector: binding.pools.filter(pool => pool.role === 'arrowcol').map(pool =>
        ({ pool: pool.name, sku: pool.sku, vcpu: pool.vcpu, count: pool.count }))[0]!,
      clients: binding.pools.filter(pool => pool.role === 'client').map(pool =>
        ({ pool: pool.name, sku: pool.sku, vcpu: pool.vcpu, count: pool.count })),
      collectorSupportReservation: { cpu_millicores: 9000, memory_bytes: 12 * 1024 ** 3 },
      deadlineAtMs: started + scaleSeconds * 1000,
      cleanupDeadlineAtMs: started + (scaleSeconds + runSeconds + cleanupSeconds) * 1000,
      signal: controller.signal,
    };
    try {
      const result = await runAksFleet(request, adapter, authority, async (nodes, pools) => {
        controller.signal.throwIfAborted();
        const observation = resolve(out, 'fleet-observation.json');
        await writeFile(observation, JSON.stringify({ nodes, pools, images }, null, 2) + '\n', { flag: 'wx', mode: 0o600 });
        await deps.prepare(['--base', values.config!, '--fleet-observation', observation,
          '--stamp-config', values['stamp-config']!, '--out', resolve(out, 'resolved')]);
        const timeout = setTimeout(() => controller.abort(new Error('Fleet run deadline exceeded')), runSeconds * 1000);
        try {
          controller.signal.throwIfAborted();
          await deps.launch(['--mode', 'aks', '--stamp-config', values['stamp-config']!,
            '--config', resolve(out, 'resolved/full-fleet.config.json')], runner,
          undefined, undefined, undefined, undefined, controller.signal);
          if (fenceError !== undefined) throw fenceError;
          if (controller.signal.aborted) throw controller.signal.reason;
          if (process.exitCode) throw new Error('Fleet OAT run failed; retained native receipts remain authoritative');
          return { config_file: resolve(out, 'resolved/full-fleet.config.json') };
        } finally {
          clearTimeout(timeout);
        }
      });
      await authority.complete();
      await writeFile(resolve(out, 'fleet-result.json'), JSON.stringify({
        ...result, ...(result.resolved === null ? { ranking_metrics: null } : {}),
      }, null, 2) + '\n', { flag: 'wx', mode: 0o600 });
      console.log(JSON.stringify({ owner_id: values['owner-id'], result_file: resolve(out, 'fleet-result.json'),
        collector_ready: result.resolved !== null, cleanup: result.cleanup }));
      if (result.resolved === null) throw new Error('Collector pool failed; ranking is null; fleet teardown was verified');
    } finally {
      clearInterval(heartbeat);
      process.removeListener('SIGINT', interrupted);
      process.removeListener('SIGTERM', interrupted);
    }
  } finally {
    await authority.close();
  }
}
