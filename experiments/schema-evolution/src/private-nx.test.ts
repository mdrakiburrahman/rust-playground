import assert from 'node:assert/strict';
import test from 'node:test';
import { privateCommand, privateEnvironment } from './private-nx.ts';

const values = { AZURE_SUBSCRIPTION_ID: '00000000-0000-0000-0000-000000000001',
  AZURE_RESOURCE_GROUP: 'rust-playground-schema-evo' };

test('private Nx environment removes shared Azure roots and generations while preserving approved subscription', () => {
  const env = privateEnvironment(values, { PATH: 'tools', AZURE_CONFIG_DIR: 'selected-cli-context',
    AZURE_EXTENSION_DIR: 'selected-cli-extensions', AZURE_RESOURCE_GROUP: 'rust-playground',
    AZURE_STAMP: '1', AZURE_STORAGE_ACCOUNT: 'shared', ARROWCOL_RESET_GENERATION: 'stale',
    ARROWCOL_AZURE_TABLE_ROOT: 'shared-root' }, 'schema-evolution');
  assert.equal(env.PATH, 'tools');
  assert.equal(env.AZURE_CONFIG_DIR, 'selected-cli-context');
  assert.equal(env.AZURE_EXTENSION_DIR, 'selected-cli-extensions');
  assert.equal(env.AZURE_SUBSCRIPTION_ID, values.AZURE_SUBSCRIPTION_ID);
  assert.equal(env.AZURE_RESOURCE_GROUP, values.AZURE_RESOURCE_GROUP);
  assert.equal(env.AZURE_STORAGE_ACCOUNT, 'arrowcol582e3df61d82');
  assert.equal(env.ARROWCOL_RESET_GENERATION, undefined);
  assert.match(env.ARROWCOL_AZURE_TABLE_ROOT!, /arrowcol582e3df61d82/);
  assert.equal(env.NX_LOAD_DOT_ENV_FILES, 'false');
  assert.equal(env.ARROWCOL_AZURE_MODE, 'schema-evolution');
  const dedicatedValues = { ...values, AZURE_RESOURCE_GROUP: 'rust-playground-schema-evo-qualification-20261108' };
  const dedicatedEnv = privateEnvironment(dedicatedValues, {}, 'schema-evolution');
  assert.equal(dedicatedEnv.AZURE_RESOURCE_GROUP, dedicatedValues.AZURE_RESOURCE_GROUP);
  assert.notEqual(dedicatedEnv.AZURE_STORAGE_ACCOUNT, env.AZURE_STORAGE_ACCOUNT);
  assert.notEqual(dedicatedEnv.ARROWCOL_AZURE_CHECKPOINT_ROOT, env.ARROWCOL_AZURE_CHECKPOINT_ROOT);
  assert.equal(env.AZURE_STAMP, undefined);
  assert.throws(() => privateEnvironment(values, {}, 'stamped'), /explicit/);
  assert.throws(() => privateEnvironment({ ...values, AZURE_RESOURCE_GROUP: 'rust-playground' }, {}, 'schema-evolution'), /dedicated/);
  assert.throws(() => privateEnvironment({ ...values, AZURE_STAMP: '1' }, {}, 'schema-evolution'), /rejects/);
});

test('both private entrypoints invoke public Nx targets without infra-only or direct tsx substitution', () => {
  const provision = privateCommand(['provision', '--mode', 'schema-evolution', '--env-file', '.env.private', '--tables', '.env.tables',
    '--output', '.env.ready']);
  assert.deepEqual(provision.command, ['npx', 'nx', 'run', 'azure-infra:provision', '--',
    '--mode', 'schema-evolution', '--env-file', '.env.private', '--tables', '.env.tables', '--output', '.env.ready']);
  const run = privateCommand(['run', '--mode', 'schema-evolution', '--env-file', '.env.private', '--ready-env', '.env.ready']);
  assert.deepEqual(run.command, ['npx', 'nx', 'run', 'schema-evolution-experiment:run', '--', '--target', 'azure',
    '--mode', 'schema-evolution', '--env-file', '.env.private', '--ready-env', '.env.ready']);
  assert.throws(() => privateCommand(['provision', '--env-file', '.env.private', '--infra-only']), /Unknown option/);
  assert.throws(() => privateCommand(['run', '--mode', 'schema-evolution', '--env-file', '.env.private']), /requires --ready-env/);
  assert.throws(() => privateCommand(['run', '--env-file', '.env.private', '--ready-env', '.env.ready']), /--mode/);
});
