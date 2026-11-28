import { execFileSync } from 'node:child_process';
import { copyFileSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join, relative } from 'node:path';

export const catalogFileName = 'arrowcol-dmv-catalog.json';
const legacyFixture = 'projects/loadtest/schemas/sqlserver.sql';
const catalogSource = 'tools/libs/arrowcol-sdk-gen/catalog/dmv-catalog.json';
const legacyFamily = 'sqlserver_dmv_v2';
const viewFamily = 'dmv';
const identifier = /^[a-z_][a-z0-9_]*$/;
const catalogType = /^(?:STRING|BOOLEAN|SMALLINT|INT|BIGINT|FLOAT|DOUBLE|DATE|TIMESTAMP|DECIMAL\((\d{1,2}),(\d{1,2})\))$/;
const interval = /^\d{2}:\d{2}:\d{2}$/;

const fixedScaleMetrics = new Set([
  'avg_cpu_percent',
  'data_size_used_mb',
  'data_size_allocated_mb',
  'log_size_used_mb',
  'log_size_allocated_mb',
  'persistent_version_store_size_mb',
  'online_index_version_store_size_mb',
  'memory_size_mb',
  'virtual_memory_committed_mb',
  'file_size_mb',
  'file_max_size_mb',
]);

export interface DmvColumn { name: string; type: string; nullable: boolean; source?: string }

export interface DmvDataset {
  family: string;
  dataset: string;
  table: string;
  source?: string;
  query?: string;
  top?: number;
  orderBy?: string;
  watermark?: string;
  interval: string;
  optional: boolean;
  targets: string[];
  provenance: string[];
  columns: DmvColumn[];
}

interface ViewEntry {
  view: string;
  interval: string;
  provenance: string[];
  targets: string[];
  columns: string[];
  aliases?: Record<string, string>;
  top?: number;
  orderBy?: string;
  watermark?: string;
  optional?: boolean;
  query?: string;
}

export interface DmvCatalogSource {
  version: number;
  sources: Record<string, string>;
  identity: string[];
  families: Record<string, string[]>;
  views: ViewEntry[];
}

function ddl(table: string, columns: DmvColumn[]): string {
  return `CREATE TABLE ${table} (\n    ${columns.map(column =>
    `${column.name} ${column.type}${column.nullable ? '' : ' NOT NULL'}`).join(',\n    ')}\n) USING DELTA;\n`;
}

export function datasetDdl(dataset: DmvDataset): string {
  return ddl(dataset.table, dataset.columns);
}

function legacyTables(source: string): Map<string, DmvColumn[]> {
  const tables = new Map<string, DmvColumn[]>();
  const sql = source.replace(/^\s*--[^\n]*$/gm, '').trim();
  for (const statement of sql.split(';').map(text => text.trim()).filter(Boolean)) {
    const match = /^CREATE TABLE telemetry\.sqlserver\.(sqlserver_[a-z0-9_]+) \(\s*([\s\S]+?)\s*\) USING DELTA$/.exec(statement);
    if (!match) throw new Error('Unsupported statement in the public SQL Server fixture');
    const [, table, body] = match;
    if (tables.has(table)) throw new Error(`Duplicate public DMV: ${table}`);
    const seen = new Set<string>();
    const columns = body.split(',').map(column => {
      const field = /^([a-z][a-z0-9_]*) ([A-Z]+) NOT NULL$/.exec(column.trim());
      if (!field) throw new Error(`Unsupported public DMV column in ${table}: ${column.trim()}`);
      const [, name, type] = field;
      if (seen.has(name)) throw new Error(`Duplicate public DMV column ${table}.${name}`);
      seen.add(name);
      const payloadType = type === 'DOUBLE' && fixedScaleMetrics.has(name) ? 'DECIMAL(38,12)' : type;
      return { name, type: payloadType, nullable: name !== 'customer_id' };
    });
    if (!seen.has('customer_id')) throw new Error(`Public DMV ${table} is missing customer_id`);
    tables.set(table, columns);
  }
  if (tables.size === 0) throw new Error('The public SQL Server fixture has no tables');
  return tables;
}

export function dmvDefinitions(source: string): Map<string, string> {
  return new Map([...legacyTables(source)].map(([table, columns]) =>
    [table, ddl(`telemetry.${legacyFamily}.${table}`, columns)]));
}

function parseColumn(view: string, text: string, alias?: string): DmvColumn {
  const [name, type, ...rest] = text.split(' ');
  if (rest.length || !identifier.test(name) || !catalogType.test(type ?? '')) {
    throw new Error(`Unsupported column in ${view}: ${text}`);
  }
  return { name, type, nullable: true, ...(alias ? { source: alias } : {}) };
}

function checkDataset(dataset: DmvDataset): DmvDataset {
  const id = `${dataset.family}.${dataset.dataset}`;
  const names = new Set<string>();
  for (const column of dataset.columns) {
    if (names.has(column.name)) throw new Error(`Duplicate column ${id}.${column.name}`);
    names.add(column.name);
  }
  if (!interval.test(dataset.interval)) throw new Error(`${id}: interval must be hh:mm:ss`);
  if (dataset.query && (dataset.top || dataset.watermark || dataset.orderBy)) {
    throw new Error(`${id}: a verbatim query carries its own bounds`);
  }
  if (dataset.query && !/^SELECT SYSUTCDATETIME\(\) AS sample_time_utc, /.test(dataset.query)) {
    throw new Error(`${id}: a verbatim query must start with the sample time`);
  }
  for (const column of [dataset.watermark, ...(dataset.orderBy?.split(', ').map(term => term.split(' ')[0]) ?? [])]) {
    if (column && !names.has(column)) throw new Error(`${id}: ordering column ${column} is not in the table`);
  }
  if (!dataset.targets.length || !dataset.provenance.length) throw new Error(`${id}: targets and provenance are required`);
  return dataset;
}

/** Every packaged dataset: the legacy family, the PaaS families and one table per public view. */
export function dmvCatalog(fixture: string, source: DmvCatalogSource): DmvDataset[] {
  if (source.version !== 1) throw new Error(`Unsupported DMV catalog source version ${source.version}`);
  const legacy = legacyTables(fixture);
  const provenance = [legacyFixture];
  const identity = source.identity.map(text => parseColumn('identity', text));
  const datasets: DmvDataset[] = [...legacy].map(([table, columns]) => ({
    family: legacyFamily, dataset: table, table: `telemetry.${legacyFamily}.${table}`, interval: '00:05:00',
    optional: false, targets: ['sqlserver'], provenance, columns,
  }));
  for (const [family, names] of Object.entries(source.families)) {
    for (const name of names) {
      const columns = legacy.get(`sqlserver_${name}`);
      if (!columns) throw new Error(`${family}.${name} has no public SQL Server fixture table`);
      datasets.push({
        family, dataset: name, table: `telemetry.${family}.${name}`, interval: '00:05:00', optional: false,
        targets: [family], provenance, columns: [...columns, ...identity],
      });
    }
  }
  const keys: DmvColumn[] = [
    { name: 'customer_id', type: 'STRING', nullable: false }, { name: 'sample_time_utc', type: 'TIMESTAMP', nullable: true },
    ...identity,
  ];
  for (const view of source.views) {
    if (!identifier.test(view.view) || !view.view.startsWith('dm_')) throw new Error(`Unsupported view ${view.view}`);
    const aliases = view.aliases ?? {};
    const columns = view.columns.map(text => parseColumn(view.view, text, aliases[text.split(' ')[0]]));
    for (const alias of Object.keys(aliases)) {
      if (!columns.some(column => column.name === alias)) throw new Error(`${view.view}: alias ${alias} has no column`);
    }
    datasets.push({
      family: viewFamily, dataset: view.view, table: `telemetry.${viewFamily}.${view.view}`, source: `sys.${view.view}`,
      ...(view.query ? { query: view.query } : {}),
      ...(view.top ? { top: view.top } : {}),
      ...(view.orderBy ? { orderBy: view.orderBy } : {}),
      ...(view.watermark ? { watermark: view.watermark } : {}),
      interval: view.interval, optional: view.optional ?? false, targets: view.targets, provenance: view.provenance,
      columns: [...keys, ...columns],
    });
  }
  const ids = new Set<string>();
  for (const dataset of datasets.map(checkDataset)) {
    const id = `${dataset.family}.${dataset.dataset}`;
    if (ids.has(id)) throw new Error(`Duplicate DMV dataset ${id}`);
    ids.add(id);
  }
  return datasets;
}

export function generateDmvPackageInputs(generator: string, root: string): void {
  const source = JSON.parse(readFileSync(catalogSource, 'utf8')) as DmvCatalogSource;
  const datasets = dmvCatalog(readFileSync(legacyFixture, 'utf8'), source);
  const assets = join(root, 'assets');
  const ddlRoot = join(assets, 'arrow-ddl');
  const models = join(root, 'models');
  mkdirSync(ddlRoot, { recursive: true });
  mkdirSync(models);
  let hasOptions = false;
  const entries = datasets.map(dataset => {
    const legacy = dataset.family === legacyFamily;
    const name = legacy ? dataset.dataset : `${dataset.family}.${dataset.dataset}`;
    const definition = join(ddlRoot, `${name}.sql`);
    const bundle = join(root, 'bundles', name);
    const contract = join(assets, 'arrow-contracts', name);
    writeFileSync(definition, datasetDdl(dataset));
    execFileSync(generator, ['fixture', '--ddl', definition, '--out', relative(process.cwd(), bundle)]);
    mkdirSync(contract, { recursive: true });
    for (const file of ['manifest.json', 'descriptor.pb', 'schema.arrow']) {
      copyFileSync(join(bundle, file), join(contract, file));
    }
    if (legacy) {
      copyFileSync(join(bundle, 'schema.proto'), join(models, `${name}.proto`));
      if (!hasOptions) {
        copyFileSync(join(bundle, 'options.proto'), join(models, 'options.proto'));
        hasOptions = true;
      }
    }
    return { ...dataset, contract: `arrow-contracts/${name}` };
  });
  writeFileSync(join(assets, catalogFileName),
    `${JSON.stringify({ version: 1, sources: source.sources, datasets: entries }, null, 2)}\n`);
}