import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mountOptions } from './job.js';
import { bootstrapScript } from './bootstrap.js';

test('all cache profiles retain server locks and explicit NFS version', () => {
  for (const profile of ['default', 'none', 'positive', 'recommended'] as const) {
    const options = mountOptions(profile).split(',');
    assert.ok(options.includes('vers=4') && options.includes('minorversion=1'));
    assert.ok(!options.some(o => o === 'nolock' || o.startsWith('local_lock=')));
  }
  assert.ok(mountOptions('none').includes('lookupcache=none'));
  assert.ok(mountOptions('positive').includes('actimeo=1'));
  assert.throws(() => mountOptions('injected,nolock' as 'none'));
});

test('bootstrap formats only the explicit provisioned LUN and verifies artifacts', () => {
  const script = bootstrapScript({ name: 'artifacts/example/lab.tgz', sha256: 'a'.repeat(64), binarySha256: 'b'.repeat(64) }, 'https://example.invalid/');
  assert.ok(script.includes('/dev/disk/azure/data/by-lun/0'));
  assert.ok(script.includes('if test -z "$TYPE"; then mkfs.ext4'));
  assert.ok(script.includes('test "$(blkid -s LABEL -o value "$DEVICE")" = storage-spike-oracle'));
  assert.equal((script.match(/sha256sum --check/gu) ?? []).length, 3);
  assert.ok(!script.includes('set -x'));
});
