import { lstatSync, readFileSync } from 'node:fs';
import { parseArgs } from 'node:util';
import { buildContract, enqueue, errorMessage, resumePromotion } from './contract.js';

try {
  const { values } = parseArgs({
    options: {
      'run-id': { type: 'string' },
      'resume-run': { type: 'string' },
      registry: { type: 'string' },
      definition: { type: 'string' },
      table: { type: 'string' },
      version: { type: 'string' },
      'schema-version': { type: 'string' },
      codegen: { type: 'string' },
      'proto-entry': { type: 'string' },
    },
    strict: true,
  });
  if (values.version && values['schema-version']) throw new Error('Use only one schema version argument');
  const version = values['schema-version'] ?? values.version;
  if ((values['run-id'] || values['resume-run']) && Object.keys(values).length !== 1) {
    throw new Error('--run-id/--resume-run cannot be combined with other options');
  }
  if (!values['run-id'] && !values['resume-run'] && (!values.registry ||
      (values.definition ? values.table || version : !values.table || !version))) {
    throw new Error('Use --registry ID --definition file.sql, or --registry ID --table catalog.schema.table --schema-version N');
  }
  if (values.definition) {
    const metadata = lstatSync(values.definition);
    if (!metadata.isFile() || metadata.isSymbolicLink() || !values.definition.endsWith('.sql')) {
      throw new Error('--definition must be a regular non-symlink .sql file');
    }
  }
  const id = values['resume-run'] ?? values['run-id'] ?? enqueue(process.cwd(), values.definition
    ? {
      registry: values.registry, mode: 'create', ddl: readFileSync(values.definition, 'utf8'),
      ...(values.codegen ? { codegen: values.codegen } : {}),
      ...(values['proto-entry'] ? { protoEntry: values['proto-entry'] } : {}),
    }
    : {
      registry: values.registry, mode: 'rebuild', table: values.table, version: Number(version),
      ...(values.codegen ? { codegen: values.codegen } : {}),
      ...(values['proto-entry'] ? { protoEntry: values['proto-entry'] } : {}),
    }).id;
  const result = await (values['resume-run'] ? resumePromotion(process.cwd(), id) : buildContract(process.cwd(), id));
  console.log(JSON.stringify(result, null, 2));
  if (result.status !== 'succeeded') process.exitCode = 1;
} catch (error) {
  console.error(`contract-builder: ${errorMessage(error)}`);
  process.exitCode = 1;
}
