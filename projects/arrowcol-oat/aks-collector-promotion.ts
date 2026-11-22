import { createHash } from 'node:crypto';
import { parseAllDocuments } from 'yaml';

type ObjectValue = Record<string, unknown>;
export interface CollectorContractRequest {
  config: ObjectValue;
  selected_tunables: ObjectValue;
  pipeline_yaml: string;
  readback_logs?: string;
}
export interface CollectorContract {
  schema_version: 1;
  canonical_tunables_json: string;
  effective_tunables_json: string;
  environment: Record<string, string>;
  observed: ObjectValue | null;
}
export type ProjectCollector = (request: CollectorContractRequest) => Promise<CollectorContract>;
export interface CollectorPromotion {
  schema_version: 1;
  qualified_source_commit: string;
  qualified_config_sha256: string;
  qualified_config_json: string;
  won_coordinates: string[];
}
export interface PromotionBinding {
  schema_version: 1;
  qualified_source_commit: string;
  qualified_config_sha256: string;
  won_coordinates: string[];
  canonical_tunables_json: string;
  projected_environment: Record<string, string>;
  delta_sha256: string;
}

function object(value: unknown, name: string): ObjectValue {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error(name + ' must be an object.');
  return value as ObjectValue;
}
export function configDigest(value: unknown): string {
  const canonical = (item: unknown): unknown => Array.isArray(item) ? item.map(canonical)
    : item && typeof item === 'object'
      ? Object.fromEntries(Object.entries(item).sort(([a], [b]) => a.localeCompare(b)).map(([key, child]) => [key, canonical(child)]))
      : item;
  return createHash('sha256').update(JSON.stringify(canonical(value))).digest('hex');
}
export const qualifiedConfigDigest = (json: string): string => createHash('sha256').update(json).digest('hex');
function documents(manifest: string) {
  const result = parseAllDocuments(manifest);
  if (result.some(document => document.errors.length)) throw new Error('Invalid promotion manifest YAML.');
  return result;
}
function collector(manifest: string) {
  const docs = documents(manifest);
  const writers = docs.filter(doc => doc.get('kind') === 'Deployment' && doc.getIn(['metadata', 'name']) === 'arrowcol');
  const configs = docs.filter(doc => doc.get('kind') === 'ConfigMap' && doc.getIn(['metadata', 'name']) === 'arrowcol-config');
  if (writers.length !== 1 || configs.length !== 1) throw new Error('Promotion requires exactly one collector and config.');
  const path = ['spec', 'template', 'spec', 'containers', 0];
  const containers = object(object(object(object(writers[0]!.toJSON(), 'Writer').spec, 'Writer spec').template, 'Writer template').spec, 'Writer pod').containers;
  if (!Array.isArray(containers) || containers.length !== 1) throw new Error('Collector must have exactly one container.');
  const container = object(containers[0], 'Collector');
  if (container.name !== 'dataflow') throw new Error('Unknown collector container.');
  const pipeline = configs[0]!.getIn(['data', 'dataflow.yaml']);
  if (typeof pipeline !== 'string') throw new Error('Missing collector pipeline.');
  const parsed = documents(pipeline);
  if (parsed.length !== 1) throw new Error('Collector pipeline must contain exactly one document.');
  return { docs, writer: writers[0]!, config: configs[0]!, path, container, pipeline, parsed: parsed[0]! };
}
export function collectorSettings(manifest: string) {
  const value = collector(manifest);
  const pipeline = object(value.parsed.toJSON(), 'Pipeline');
  const args = value.container.args;
  const env = value.container.env;
  const settings = { schema_version: 1 as const, args, env, envFrom: value.container.envFrom,
    resources: value.container.resources, pipeline };
  return {
    ...settings,
    dfe_core_allocation_count: value.parsed.getIn(['policies', 'resources', 'core_allocation', 'count']),
    receiver_rss_limit_bytes: value.parsed.getIn(['groups', 'arrowcol', 'pipelines', 'ingest', 'nodes', 'flight', 'config', 'registration', 'rss_limit_bytes']),
    sha256: configDigest(settings),
  };
}
function selectedTunables(promotion: CollectorPromotion, qualifiedConfig: ObjectValue): ObjectValue {
  const selected: ObjectValue = {};
  const tunables = object(qualifiedConfig.tunables, 'Qualified tunables');
  if (!Array.isArray(promotion.won_coordinates) || !promotion.won_coordinates.length ||
      new Set(promotion.won_coordinates).size !== promotion.won_coordinates.length) throw new Error('Won coordinates must be nonempty and unique.');
  for (const coordinate of promotion.won_coordinates) {
    if (typeof coordinate !== 'string' ||
        !/^(collector\.(batching|commit|runtime)|parquet_writer\.defaults)\.[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)*$/.test(coordinate)) {
      throw new Error('Unmappable or non-collector promotion coordinate: ' + coordinate);
    }
    const parts = coordinate.split('.');
    if (parts.some(part => ['__proto__', 'prototype', 'constructor'].includes(part))) throw new Error('Unsafe promotion coordinate.');
    let source: unknown = tunables;
    let destination = selected;
    for (const [index, part] of parts.entries()) {
      source = object(source, 'Qualified coordinate')[part];
      if (source === undefined || source === null) throw new Error('Won coordinate is missing: ' + coordinate);
      if (index === parts.length - 1) {
        if (typeof source === 'object') throw new Error('Only explicitly won leaf coordinates may be promoted.');
        if (typeof source === 'number' && (!Number.isFinite(source) || (Number.isInteger(source) && !Number.isSafeInteger(source)))) {
          throw new Error('Won numeric coordinate cannot be represented exactly: ' + coordinate);
        }
        destination[part] = source;
      } else {
        destination[part] ??= {};
        destination = object(destination[part], 'Selected coordinate');
      }
    }
  }
  return selected;
}
const projectedTargets: Record<string, string[]> = {
  ARROWCOL_COLLECTOR_BATCHING_CONFIG: ['batching'],
  ARROWCOL_COLLECTOR_COMMIT_CONFIG: ['commit'],
};
const runtimeVariables = new Set([
  'ARROWCOL_COLLECTOR_RUNTIME_WORKERS', 'ARROWCOL_COLLECTOR_BLOCKING_WORKERS',
  'ARROWCOL_COLLECTOR_UPLOAD_LIMIT', 'ARROWCOL_COLLECTOR_TASK_LIMIT',
  'ARROWCOL_COLLECTOR_READ_AHEAD', 'ARROWCOL_COLLECTOR_BATCH_ROWS', 'ARROWCOL_COLLECTOR_CPU_WORKERS',
]);
export async function applyCollectorPromotion(manifest: string, input: unknown, sourceCommit: string, project: ProjectCollector) {
  if (input === undefined) return { manifest, binding: undefined, request: undefined };
  const rawPromotion = object(input, 'Collector promotion');
  const keys = ['schema_version', 'qualified_source_commit', 'qualified_config_sha256', 'qualified_config_json', 'won_coordinates'];
  if (Object.keys(rawPromotion).length !== keys.length || keys.some(key => !Object.hasOwn(rawPromotion, key)) ||
      rawPromotion.schema_version !== 1 || rawPromotion.qualified_source_commit !== sourceCommit ||
      !/^[0-9a-f]{40}$/.test(sourceCommit) || typeof rawPromotion.qualified_config_sha256 !== 'string' ||
      !/^[0-9a-f]{64}$/.test(rawPromotion.qualified_config_sha256) ||
      typeof rawPromotion.qualified_config_json !== 'string' ||
      qualifiedConfigDigest(rawPromotion.qualified_config_json) !== rawPromotion.qualified_config_sha256 ||
      !Array.isArray(rawPromotion.won_coordinates) || !rawPromotion.won_coordinates.every((coordinate): coordinate is string => typeof coordinate === 'string')) {
    throw new Error('Promotion does not bind the qualified source/config digest.');
  }
  const promotion: CollectorPromotion = {
    schema_version: 1, qualified_source_commit: sourceCommit,
    qualified_config_sha256: rawPromotion.qualified_config_sha256,
    qualified_config_json: rawPromotion.qualified_config_json,
    won_coordinates: rawPromotion.won_coordinates,
  };
  const qualifiedConfig = object(JSON.parse(promotion.qualified_config_json), 'Qualified config');
  const value = collector(manifest);
  const request: CollectorContractRequest = { config: qualifiedConfig,
    selected_tunables: selectedTunables(promotion, qualifiedConfig), pipeline_yaml: value.pipeline };
  const projected = await project(request);
  if (projected.schema_version !== 1 || typeof projected.canonical_tunables_json !== 'string') throw new Error('Invalid strict Rust projection.');
  const environment = object(projected.environment, 'Projected environment');
  if (!Object.keys(environment).length) throw new Error('Promotion has no mapped collector settings.');
  const args = value.container.args;
  if (!Array.isArray(args) || args.length !== 4 || args[0] !== '--config' || args[1] !== '/etc/oat/dataflow.yaml' ||
      args[2] !== '--num-cores') throw new Error('Explicit collector flags conflict with promotion.');
  const env = value.container.env ?? [];
  if (!Array.isArray(env)) throw new Error('Unknown collector environment.');
  const existing = new Map<string, ObjectValue>();
  for (const entry of env) {
    const variable = object(entry, 'Collector env');
    if (typeof variable.name !== 'string' || existing.has(variable.name)) throw new Error('Duplicate/unknown collector environment.');
    existing.set(variable.name, variable);
  }
  const nodePath = ['groups', 'arrowcol', 'pipelines', 'ingest', 'nodes', 'delta', 'config'];
  const node = value.parsed.getIn(nodePath);
  if (!node || typeof node !== 'object') throw new Error('Collector exporter config is unmappable.');
  for (const [name, raw] of Object.entries(environment)) {
    if (typeof raw !== 'string' || (!projectedTargets[name] && name !== 'ARROWCOL_PARQUET_CONFIG' && !runtimeVariables.has(name))) {
      throw new Error('Strict projection has no checked promotion/readback mapping: ' + name);
    }
    const old = existing.get(name);
    if (old && (old.value !== raw || old.valueFrom !== undefined)) throw new Error('Promotion collides with explicit collector env: ' + name);
    const target = projectedTargets[name];
    if (target) {
      const fragment = object(JSON.parse(raw), 'Rust projected fragment');
      const merge = (parts: string[], item: ObjectValue) => {
        for (const [key, child] of Object.entries(item)) {
          if (child === null) continue;
          if (child && typeof child === 'object' && !Array.isArray(child)) merge([...parts, key], object(child, 'Projected fragment'));
          else value.parsed.setIn([...nodePath, ...parts, key], child);
        }
      };
      merge(target, fragment);
    }
    existing.set(name, { name, value: raw });
  }
  if (Object.hasOwn(environment, 'ARROWCOL_PARQUET_CONFIG')) {
    for (const doc of value.docs) {
      if (doc.get('kind') === 'ConfigMap' && doc.getIn(['metadata', 'name']) === 'arrowcol-config') continue;
      if (/parquet_writer|parquet_config|writer_properties|table_policy/.test(doc.toString())) {
        throw new Error('Explicit table/writer policy prevents process-writer promotion.');
      }
    }
    if (value.parsed.getIn([...nodePath, 'parquet_writer']) !== undefined || value.parsed.getIn([...nodePath, 'table_policy']) !== undefined) {
      throw new Error('Explicit collector table/writer policy collides with promotion.');
    }
  }
  value.writer.setIn([...value.path, 'env'], [...existing.values()]);
  value.config.setIn(['data', 'dataflow.yaml'], value.parsed.toString());
  const rendered = value.docs.map(doc => doc.toString()).join('');
  const binding: PromotionBinding = {
    schema_version: 1, qualified_source_commit: sourceCommit, qualified_config_sha256: promotion.qualified_config_sha256,
    won_coordinates: [...promotion.won_coordinates], canonical_tunables_json: projected.canonical_tunables_json,
    projected_environment: projected.environment, delta_sha256: configDigest({ coordinates: promotion.won_coordinates, tunables: projected.canonical_tunables_json }),
  };
  return { manifest: rendered, binding, request: { ...request, pipeline_yaml: String(value.config.getIn(['data', 'dataflow.yaml'])) } };
}

export function assertPromotionEnvironment(expected: Record<string, string>, actual: unknown): void {
  if (!Array.isArray(actual)) throw new Error('Promoted collector env readback is missing.');
  for (const [name, value] of Object.entries(expected)) {
    const matches = actual.filter(entry => object(entry, 'Actual collector env').name === name);
    if (matches.length !== 1 || object(matches[0], 'Actual collector env').value !== value ||
        object(matches[0], 'Actual collector env').valueFrom !== undefined) throw new Error('Promoted collector env drift: ' + name);
  }

}

export function collectorCapacityReadback(observed: ObjectValue | null | undefined) {
  const runtime = observed?.runtime;
  const value = runtime && typeof runtime === 'object' && !Array.isArray(runtime) ? object(runtime, 'Runtime').cpu_workers : undefined;
  const cpuWorkers = typeof value === 'number' && Number.isSafeInteger(value) && value > 0 ? value : null;
  return {
    dfe_core_allocation_count: null,
    dfe_unavailable_reason: 'DFE startup emits the drain deadline, not resolved core allocation; pod args/config are checked separately.',
    cpu_workers: cpuWorkers,
    cpu_workers_unavailable_reason: cpuWorkers === null ? 'Consumer startup did not expose the process-wide CPU worker count.' : null,
  };
}
