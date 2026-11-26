import { execFileSync } from 'node:child_process';
import { copyFileSync, mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { join, relative } from 'node:path';

export function dmvDefinitions(source: string): Map<string, string> {
  const definitions = new Map<string, string>();
  const sql = source.replace(/^\s*--[^\n]*$/gm, '').trim();
  for (const statement of sql.split(';').map(text => text.trim()).filter(Boolean)) {
    const match = /^CREATE TABLE telemetry\.sqlserver\.(sqlserver_[a-z0-9_]+) \(\s*([\s\S]+?)\s*\) USING DELTA$/.exec(statement);
    if (!match) throw new Error('Unsupported statement in the public SQL Server fixture');
    const [, table, body] = match;
    if (definitions.has(table)) throw new Error(`Duplicate public DMV: ${table}`);
    const seen = new Set<string>();
    const columns = body.split(',').map(column => {
      const field = /^([a-z][a-z0-9_]*) ([A-Z]+) NOT NULL$/.exec(column.trim());
      if (!field) throw new Error(`Unsupported public DMV column in ${table}: ${column.trim()}`);
      const [, name, type] = field;
      if (seen.has(name)) throw new Error(`Duplicate public DMV column ${table}.${name}`);
      seen.add(name);
      return name === 'customer_id' ? `${name} ${type} NOT NULL` : `${name} ${type === 'DOUBLE' ? 'DECIMAL(38,12)' : type}`;
    });
    if (!seen.has('customer_id')) throw new Error(`Public DMV ${table} is missing customer_id`);
    definitions.set(table, `CREATE TABLE telemetry.sqlserver_dmv.${table} (\n    ${columns.join(',\n    ')}\n) USING DELTA;\n`);
  }
  if (definitions.size === 0) throw new Error('The public SQL Server fixture has no tables');
  return definitions;
}

export function generateDmvPackageInputs(generator: string, root: string): void {
  const definitions = dmvDefinitions(readFileSync('projects/loadtest/schemas/sqlserver.sql', 'utf8'));
  const ddlRoot = join(root, 'assets/arrow-ddl');
  const models = join(root, 'models');
  mkdirSync(ddlRoot, { recursive: true });
  mkdirSync(models);
  let hasOptions = false;
  for (const [table, definition] of definitions) {
    const ddl = join(ddlRoot, `${table}.sql`);
    const bundle = join(root, 'bundles', table);
    const contract = join(root, 'assets/arrow-contracts', table);
    writeFileSync(ddl, definition);
    execFileSync(generator, ['fixture', '--ddl', ddl, '--out', relative(process.cwd(), bundle)]);
    mkdirSync(contract, { recursive: true });
    for (const name of ['manifest.json', 'descriptor.pb', 'schema.arrow']) {
      copyFileSync(join(bundle, name), join(contract, name));
    }
    copyFileSync(join(bundle, 'schema.proto'), join(models, `${table}.proto`));
    if (!hasOptions) {
      copyFileSync(join(bundle, 'options.proto'), join(models, 'options.proto'));
      hasOptions = true;
    }
  }
}
