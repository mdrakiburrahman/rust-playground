import assert from 'node:assert/strict';
import { test } from 'node:test';
import { shellQuote, validateConfig } from './lab.js';

const config = {
  subscription: '11111111-1111-1111-1111-111111111111',
  resourceGroup: 'rg-spike-example', owner: 'example', prefix: 'example', location: 'centralus', stagingLocation: 'eastus2',
  vmSize: 'Standard_D8ads_v6', sshPublicKeyPath: '.temp/key.pub',
  uploaderObjectId: '22222222-2222-2222-2222-222222222222', uploaderIp: '192.0.2.1',
  resourceTags: {}, outboundIpTags: [], azureCli: { program: 'az', prefix: [] },
  buildContainer: 'example-container', outDir: '.temp/example',
};

test('configuration prevents accidental deployment into another owner group', () => {
  assert.equal(validateConfig(config).resourceGroup, 'rg-spike-example');
  assert.throws(() => validateConfig({ ...config, resourceGroup: 'rg-production' }), /owner suffix/u);
  assert.throws(() => validateConfig({ ...config, unexpected: true }), /unknown/u);
  assert.throws(() => validateConfig({ ...config, uploaderIp: '0.0.0.0/0' }), /single uploader/u);
  assert.throws(() => validateConfig({ ...config, uploaderIp: '999.1.1.1' }), /single uploader/u);
});

test('remote shell arguments cannot inject a command', () => {
  assert.equal(shellQuote("a'; echo surprise"), "'a'\\''; echo surprise'");
  assert.throws(() => shellQuote('line\nbreak'), /Invalid/u);
});
