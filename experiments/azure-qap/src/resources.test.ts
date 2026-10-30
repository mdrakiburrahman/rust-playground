import assert from 'node:assert/strict';
import { test } from 'node:test';
import { Config } from '../../../infra/azure/bootstrap.ts';
import { environmentHash, names, validateManifest, type Manifest } from './resources.js';

const config = new Config({ AZURE_SUBSCRIPTION_ID: '11111111-1111-1111-1111-111111111111' });
const manifest: Manifest = {
  format: 1, runId: '22222222-2222-2222-2222-222222222222',
  generation: '33333333-3333-3333-3333-333333333333',
  environmentHash: environmentHash(config), createdAt: '2026-01-01T00:00:00Z', phase: 'ready',
};

test('all destructive names derive from a UUID, never caller supplied prefixes', () => {
  const owned = names(manifest.runId);
  assert.equal(owned.database, 'qapexp-22222222222222222222222222222222');
  assert.notEqual(owned.data, config.data);
  assert.notEqual(owned.checkpoints, config.checkpoints);
  assert.throws(() => names('../arrowcol'), /UUID/);
  assert.throws(() => names(manifest.runId + '/arrowcol'), /UUID/);
});
test('ownership manifest is strict and bound to the exact private Azure environment', () => {
  assert.deepEqual(validateManifest(manifest, config), manifest);
  assert.throws(() => validateManifest({ ...manifest, database: 'arrowcol' }, config), /invalid/);
  assert.throws(() => validateManifest({ ...manifest, generation: '../' }, config), /invalid/);
  assert.throws(() => validateManifest({ ...manifest, phase: 'assume-owned' }, config), /invalid/);
  assert.throws(() => validateManifest(manifest,
    new Config({ AZURE_SUBSCRIPTION_ID: '44444444-4444-4444-4444-444444444444' })), /different/);
  assert.throws(() => validateManifest(null, config), /Malformed/);
});
test('cleanup resume state can be persisted without weakening run identity', () => {
  assert.equal(validateManifest({ ...manifest, phase: 'cleaning' }, config).phase, 'cleaning');
  assert.throws(() => validateManifest({ ...manifest, environmentHash: '' }, config), /invalid/);
});
