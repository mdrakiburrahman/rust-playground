import fs from 'node:fs';
import path from 'node:path';
import { object } from '../../azure-qap/src/kusto.ts';

export interface Snapshot {
  version: number;
  rows: number;
  files: string[];
  schema: Record<string, unknown>;
}

export function snapshot(directory: string): Snapshot {
  const log = path.join(directory, '_delta_log');
  const names = fs.readdirSync(log).filter(name => /^\d{20}\.json$/.test(name)).sort();
  if (!names.length) throw new Error('Delta has no initial commit.');
  const files = new Map<string, number>();
  let schema: Record<string, unknown> | undefined;
  for (const [version, name] of names.entries()) {
    if (Number(name.slice(0, 20)) !== version) throw new Error('Delta log has a gap; refusing a partial snapshot.');
    for (const line of fs.readFileSync(path.join(log, name), 'utf8').split('\n').filter(line => line.trim())) {
      const action = object(JSON.parse(line));
      if (action.metaData) {
        const metadata = object(action.metaData);
        if (typeof metadata.schemaString !== 'string') throw new Error('Delta schema is absent.');
        schema = object(JSON.parse(metadata.schemaString));
        const properties = object(metadata.configuration);
        if (properties['delta.columnMapping.mode'] !== 'none' || properties['delta.enableDeletionVectors'] !== 'false')
          throw new Error('Fixture unexpectedly enables column mapping or deletion vectors.');
        if (!Array.isArray(metadata.partitionColumns) || !metadata.partitionColumns.includes('tenant'))
          throw new Error('Catalog did not preserve the tenant partition.');
      }
      for (const kind of ['add', 'remove'] as const) {
        if (!action[kind]) continue;
        const entry = object(action[kind]);
        if (typeof entry.path !== 'string') throw new Error('Delta action has no file path.');
        const relative = decodeURIComponent(entry.path);
        const absolute = path.resolve(directory, relative);
        const confined = path.relative(directory, absolute);
        if (!confined || confined.startsWith('..') || path.isAbsolute(confined) || relative.includes('\\'))
          throw new Error('Delta file escaped the owned table.');
        if (kind === 'remove') files.delete(absolute);
        else {
          if (typeof entry.stats !== 'string') throw new Error('Delta file has no row statistics.');
          const rows = object(JSON.parse(entry.stats)).numRecords;
          if (typeof rows !== 'number' || !Number.isSafeInteger(rows) || rows < 0)
            throw new Error('Delta file row statistics are invalid.');
          if (entry.deletionVector) throw new Error('Fixture unexpectedly uses a deletion vector.');
          files.set(absolute, rows);
        }
      }
    }
  }
  if (!schema) throw new Error('Delta has no committed schema.');
  return { version: names.length - 1, rows: [...files.values()].reduce((sum, rows) => sum + rows, 0),
    files: [...files.keys()], schema };
}

export function nullableBar(schema: Record<string, unknown>): boolean {
  if (!Array.isArray(schema.fields)) throw new Error('Delta schema fields are absent.');
  const fields = schema.fields.map(object).filter(field => field.name === 'bar');
  if (!fields.length) return false;
  if (fields.length !== 1 || fields[0]!.type !== 'string' || fields[0]!.nullable !== true)
    throw new Error('ADD COLUMNS did not commit exactly one nullable STRING bar.');
  return true;
}

export function physicalCounts(value: unknown, rows: number): void {
  const result = object(value);
  if (!Array.isArray(result.counts) || result.counts.length !== 3) throw new Error('Physical row cohorts are incomplete.');
  const expected = new Set(['old:before', 'old:after', 'new:after']);
  for (const raw of result.counts) {
    const count = object(raw), key = `${count.client}:${count.phase}`;
    if (!expected.delete(key) || count.total !== rows || count.badSentinel !== 0 ||
        !Array.isArray(count.values) || count.values.length !== rows || count.values.some((value, index) => value !== index) ||
        count.nulls !== (count.client === 'old' ? rows : 0) ||
        count.filled !== (count.client === 'old' ? 0 : rows) ||
        (key === 'old:after' && count.missingColumn !== 0))
      throw new Error('Physical old/new client null or sentinel proof failed.');
  }
}
