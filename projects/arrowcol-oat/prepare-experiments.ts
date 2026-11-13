import { lstat, mkdir, readFile, stat, writeFile } from 'node:fs/promises';
import { dirname, isAbsolute, relative, resolve, sep } from 'node:path';
import { pathToFileURL } from 'node:url';
import { parseArgs } from 'node:util';
import { readOatConfigForMode } from './launch.js';
import { validateRuntime } from './runtime.js';

import { parseStampConfig, type StampConfig } from '../../infra/azure/stamp.js';
export const DAEMONSET_POD_ALLOWANCE_PER_NODE = 32;
const MAX_PODS_PER_NODE = 250;
const FIXED_ARROWCOL_PODS = 3;
const PIN_INIT_JOB_PODS = 1;

export interface ExperimentPoolPodCount {
  name: string;
  clientPods: number;
  arrowcolPods: number;
}

export function experimentPoolPodCount(name: string, config: Record<string, unknown>): ExperimentPoolPodCount {
  if (!Array.isArray(config.tables) || config.tables.length === 0) throw new Error(`${name} must have at least one table`);
  const clientsPerTable = integer(config.clients_per_table, 1, 128, `${name} clients_per_table`);
  if (!Array.isArray(config.replica_counts) || config.replica_counts.length === 0) {
    throw new Error(`${name} must have at least one ArrowCol replica count`);
  }
  const maximumReplicas = Math.max(...config.replica_counts.map((value) => integer(value, 1, 16, `${name} replica_counts`)));
  return {
    name,
    clientPods: config.tables.length * clientsPerTable,
    arrowcolPods: FIXED_ARROWCOL_PODS + maximumReplicas + PIN_INIT_JOB_PODS,
  };
}

export function assertExperimentPoolCapacity(
  userPools: StampConfig['aks']['userPools'], experiments: ExperimentPoolPodCount[],
): void {
  for (const experiment of experiments) {
    for (const [role, podCount] of [['client', experiment.clientPods], ['arrowcol', experiment.arrowcolPods]] as const) {
      if (!Number.isSafeInteger(podCount) || podCount < 0) throw new Error(`${experiment.name} has an invalid ${role} pod count`);
      const matchingPools = userPools.filter((pool) => pool.role === role);
      if (matchingPools.length === 0) throw new Error(`AKS stamp has no ${role} user pool; ${experiment.name} capacity cannot be verified`);
      const workloadSlots = matchingPools.reduce((total, pool) =>
        total + Math.max(0, pool.maxPods - DAEMONSET_POD_ALLOWANCE_PER_NODE) * pool.maxCount, 0);
      if (podCount <= workloadSlots) continue;
      const poolNames = matchingPools.map((pool) => `"${pool.name}"`).join(', ');
      const recommendation = capacityRecommendation(userPools, matchingPools, podCount, workloadSlots);
      throw new Error(
        `${experiment.name} requires ${podCount} ${role} pods, but pools ${poolNames} have only ${workloadSlots} combined workload slots ` +
        `after reserving ${DAEMONSET_POD_ALLOWANCE_PER_NODE} pods per node for DaemonSets; ${recommendation}.`,
      );
    }
  }
}

function capacityRecommendation(
  userPools: StampConfig['aks']['userPools'],
  matchingPools: StampConfig['aks']['userPools'],
  podCount: number,
  workloadSlots: number,
): string {
  const maxWorkloadSlotsPerNode = MAX_PODS_PER_NODE - DAEMONSET_POD_ALLOWANCE_PER_NODE;
  for (const pool of matchingPools) {
    if (pool.maxCount === 0) continue;
    const poolWorkloadSlots = Math.max(0, pool.maxPods - DAEMONSET_POD_ALLOWANCE_PER_NODE) * pool.maxCount;
    const otherWorkloadSlots = workloadSlots - poolWorkloadSlots;
    const minimumMaxPods = DAEMONSET_POD_ALLOWANCE_PER_NODE + Math.ceil((podCount - otherWorkloadSlots) / pool.maxCount);
    if (minimumMaxPods <= MAX_PODS_PER_NODE) {
      return `raise aks.userPools[${userPools.indexOf(pool)}].maxPods to at least ${minimumMaxPods}`;
    }
  }
  const pool = matchingPools[0]!;
  const poolWorkloadSlots = Math.max(0, pool.maxPods - DAEMONSET_POD_ALLOWANCE_PER_NODE) * pool.maxCount;
  const otherWorkloadSlots = workloadSlots - poolWorkloadSlots;
  const minimumMaxCount = Math.ceil((podCount - otherWorkloadSlots) / maxWorkloadSlotsPerNode);
  return `raise aks.userPools[${userPools.indexOf(pool)}].maxCount to at least ${minimumMaxCount} with maxPods=${MAX_PODS_PER_NODE}`;
}

export interface PilotBinding {
  warmupSeconds: number;
  measureSeconds: number;
  higherDfeConcurrency: number;
}

function integer(value: unknown, min: number, max: number, name: string): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < min || value > max) {
    throw new Error(`${name} must be an integer from ${min} through ${max}`);
  }
  return value;
}

export function fixtureTables(ddl: string): string[] {
  const tables = [...ddl.matchAll(/^CREATE TABLE telemetry\.sqlserver\.(\w+) \(/gm)].map((match) => match[1]);
  if (tables.length !== 32 || new Set(tables).size !== 32) throw new Error('expected exactly 32 distinct fixture tables');
  return tables;
}

export function prepareExperiments(
  base: Record<string, unknown>, tables: string[], binding: PilotBinding,
): Record<string, Record<string, unknown>> {
  if (tables.length !== 32 || new Set(tables).size !== 32 ||
      tables.some((table) => !/^sqlserver_[a-z0-9_]+$/.test(table))) {
    throw new Error('expected 32 distinct supported fixture table names');
  }
  const warmup = integer(binding.warmupSeconds, 0, 300, 'pilot warmup seconds');
  const measure = integer(binding.measureSeconds, 60, 600, 'pilot measurement seconds');
  if (measure % 30 || warmup + measure > 600) {
    throw new Error('pilot binding requires full 30-second measurement windows within the ten-minute pilot budget');
  }
  if (base.clients_per_table !== 1) throw new Error('baseline must have one client per table so E4 fits the 128-client limit');
  if (!Array.isArray(base.dfe_worker_concurrency) || base.dfe_worker_concurrency.length !== 1) {
    throw new Error('baseline must select exactly one DFE concurrency');
  }
  const baselineDfe = integer(base.dfe_worker_concurrency[0], 1, 256, 'baseline DFE concurrency');
  const higherDfe = integer(binding.higherDfeConcurrency, 1, 256, 'higher DFE concurrency');
  if (higherDfe <= baselineDfe) throw new Error('E5 DFE concurrency must exceed the baseline');
  integer(base.rows_per_table, 1, 1_000_000, 'rows per table');
  integer(base.dmv_concurrency, 1, 64, 'DMV concurrency');
  if (typeof base.qap_database !== 'string' || !base.qap_database.trim()) {
    throw new Error('generated AKS config must supply its QAP observation database');
  }
  const common = { ...base, tables: [...tables], warmup_seconds: warmup, measure_seconds: measure, pilot_mode: false };
  return Object.fromEntries([4, 8, 16, 16, 16].map((replicas, index) => [
    `E${index + 1}`,
    structuredClone({
      ...common, replica_counts: [replicas], clients_per_table: index === 3 ? 4 : 1,
      dfe_worker_concurrency: [index === 4 ? higherDfe : baselineDfe],
    }),
  ]));
}

async function assertSafeOutputParents(workspaceRoot: string, out: string): Promise<void> {
  const root = resolve(workspaceRoot);
  const relativeParent = relative(root, dirname(out));
  let current = root;
  for (const component of ['', ...relativeParent.split(sep)]) {
    if (component) current = resolve(current, component);
    try {
      const info = await lstat(current);
      if (info.isSymbolicLink() || !info.isDirectory()) {
        throw new Error(`--out path has a symlink or non-directory component: ${current}`);
      }
    } catch (error) {
      if (error instanceof Error && 'code' in error && error.code === 'ENOENT') break;
      throw error;
    }
  }
  try {
    await lstat(out);
    throw new Error('EEXIST: --out must be a new subdirectory of out/arrowcol-oat');
  } catch (error) {
    if (!(error instanceof Error && 'code' in error && error.code === 'ENOENT')) throw error;
  }
}

export async function main(args: string[], workspaceRoot = resolve('.')): Promise<void> {
  const { values } = parseArgs({ args, options: {
    base: { type: 'string' }, out: { type: 'string' }, 'stamp-config': { type: 'string' },
    'warmup-seconds': { type: 'string' }, 'measure-seconds': { type: 'string' },
    'higher-dfe': { type: 'string' }, help: { type: 'boolean' },
  } });
  if (values.help) {
    console.log('Offline only: --base <private AKS config> --out <new out/arrowcol-oat/subdirectory> ' +
      '[--stamp-config <selected private stamp YAML>] --warmup-seconds <measured> ' +
      '--measure-seconds <measured> --higher-dfe <approved>');
    return;
  }
  for (const key of ['base', 'out', 'warmup-seconds', 'measure-seconds', 'higher-dfe'] as const) {
    if (!values[key]?.trim()) throw new Error(`--${key} is required; pilot values have no defaults`);
  }
  const root = resolve(workspaceRoot);
  const out = resolve(root, values.out!);
  const privateRoot = resolve(root, 'out/arrowcol-oat');
  const child = relative(privateRoot, out);
  if (!child || child.startsWith('..') || isAbsolute(child)) throw new Error('--out must be a new subdirectory of out/arrowcol-oat');
  await assertSafeOutputParents(root, out);
  const base = await readOatConfigForMode(values.base!, 'aks');
  const platform = base.platform as Record<string, unknown>;
  if (typeof platform.runtime_config_file !== 'string' || typeof platform.bootstrap_env_file !== 'string') {
    throw new Error('generated AKS runtime_config_file and bootstrap_env_file are required');
  }
  const runtime = validateRuntime(JSON.parse(await readFile(platform.runtime_config_file, 'utf8')));
  if (runtime.stamp !== base.stamp || !((await stat(platform.bootstrap_env_file)).isFile())) {
    throw new Error('generated runtime stamp must match the AKS config and its bootstrap file must exist');
  }
  if (typeof base.dmv_client_image !== 'string' ||
      !/^[a-z0-9]+\.azurecr\.io\/[a-z0-9][a-z0-9/_.-]*@sha256:[a-f0-9]{64}$/.test(base.dmv_client_image)) {
    throw new Error('generated AKS config must supply a digest-pinned DMV client image');
  }
  const tables = fixtureTables(await readFile(resolve(root, 'projects/loadtest/schemas/sqlserver.sql'), 'utf8'));
  const configs = prepareExperiments(base, tables, {
    warmupSeconds: Number(values['warmup-seconds']),
    measureSeconds: Number(values['measure-seconds']),
    higherDfeConcurrency: Number(values['higher-dfe']),
  });
  const stampConfigPath = resolve(root, values['stamp-config'] ?? 'infra/azure/stamp.yaml');
  let stampSource: string;
  try {
    stampSource = await readFile(stampConfigPath, 'utf8');
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') {
      throw new Error(`Configured AKS pool limits are required; selected stamp config is missing at ${stampConfigPath}.`, { cause: error });
    }
    throw error;
  }
  const stamp = parseStampConfig(stampSource);
  if (stamp.stamp !== base.stamp) {
    throw new Error(`Selected AKS stamp config stamp ${stamp.stamp} does not match baseline config stamp ${String(base.stamp)}.`);
  }
  assertExperimentPoolCapacity(stamp.aks.userPools, Object.entries(configs).map(([name, config]) =>
    experimentPoolPodCount(name, config)));
  await mkdir(privateRoot, { recursive: true, mode: 0o700 });
  await mkdir(out, { mode: 0o700 });
  for (const [name, config] of Object.entries(configs)) {
    await writeFile(resolve(out, `${name}.config.json`), `${JSON.stringify(config, null, 2)}\n`, { flag: 'wx', mode: 0o600 });
  }
  console.log(`Prepared E1-E5 in ${out}; no benchmark or cloud operation was performed. Pilot evidence requires operator review.`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main(process.argv.slice(2)).catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
