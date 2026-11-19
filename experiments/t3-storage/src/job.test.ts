import assert from 'node:assert/strict';
import { test } from 'node:test';
import { mkdtemp, rename, rm, stat, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { directoryListing, mountOptions } from './job.js';
import { bootstrapScript, startJob } from './bootstrap.js';
import type { Lab } from './lab.js';

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

test('directory evidence records files renamed between enumeration and stat', async context => {
  const root = await mkdtemp(path.join(tmpdir(), 'storage-listing-'));
  context.after(() => rm(root, { recursive: true, force: true }));
  await writeFile(path.join(root, 'segment.qseg.tmp'), 'data');
  await writeFile(path.join(root, 'stable.qseg'), 'data');
  const entries = await directoryListing(root, async file => {
    if (path.basename(file) === 'segment.qseg.tmp') await rename(file, path.join(root, 'segment.qseg'));
    return stat(file);
  });
  assert.deepEqual(entries[0], { name: 'segment.qseg.tmp', disappeared: true });
  assert.equal(entries[1].name, 'stable.qseg');
  assert.equal(entries[1].bytes, 4);
  assert.equal(entries[1].disappeared, undefined);
});

test('directory evidence propagates missing directories and other stat errors', async context => {
  const root = await mkdtemp(path.join(tmpdir(), 'storage-listing-'));
  context.after(() => rm(root, { recursive: true, force: true }));
  await assert.rejects(directoryListing(path.join(root, 'absent')), { code: 'ENOENT' });
  await writeFile(path.join(root, 'segment.qseg'), 'data');
  await assert.rejects(directoryListing(root, async () => { throw Object.assign(new Error('Denied'), { code: 'EACCES' }); }), { code: 'EACCES' });
});

test('bootstrap formats only the explicit provisioned LUN and verifies artifacts', () => {
  const script = bootstrapScript({ name: 'artifacts/example/lab.tgz', sha256: 'a'.repeat(64), binarySha256: 'b'.repeat(64), quiverCommit: 'c'.repeat(40) }, 'https://example.invalid/');
  assert.ok(script.includes('/dev/disk/azure/data/by-lun/0'));
  assert.ok(script.includes('if test -z "$TYPE"; then mkfs.ext4'));
  assert.ok(script.includes('test "$(blkid -s LABEL -o value "$DEVICE")" = storage-spike-oracle'));
  assert.equal((script.match(/sha256sum --check/gu) ?? []).length, 3);
  assert.ok(!script.includes('set -x'));
});

test('source bootstrap verifies the pinned Phase A checkout and recovery gate capability', () => {
  const commit = 'c'.repeat(40);
  const script = bootstrapScript({ name: 'artifacts/example/source.tgz', sha256: 'a'.repeat(64), binarySha256: 'b'.repeat(64), quiverCommit: commit, transport: 'source-arm' }, 'https://example.invalid/');
  assert.ok(script.includes(`-name ${commit.slice(0, 7)}`));
  assert.ok(script.includes(`rev-parse HEAD)" = ${commit}`));
  assert.ok(script.includes('git clone --shared'));
  assert.ok(script.includes('cargo clean --release --package storage-spike'));
  assert.ok(script.includes('recover --help | grep -q -- --pause-before-drain'));
  assert.throws(() => bootstrapScript({ name: 'x', sha256: 'a'.repeat(64), binarySha256: 'b'.repeat(64), quiverCommit: 'moving-branch', transport: 'source-arm' }, 'https://example.invalid/'));
});

test('guest jobs isolate mounts so a prior job cannot block their remount', async context => {
  const root = await mkdtemp(path.join(tmpdir(), 'storage-job-'));
  context.after(() => rm(root, { recursive: true, force: true }));
  await writeFile(path.join(root, 'artifact.json'), JSON.stringify({ sha256: 'a'.repeat(64), quiverCommit: 'c'.repeat(40) }));
  await writeFile(path.join(root, 'guest-binaries.json'), JSON.stringify({ 'test-vm': 'b'.repeat(64) }));
  let script = '';
  const lab = {
    config: { outDir: root },
    inventory: async () => ({ nfsSource: 'example.invalid:/share', storageEndpoints: [{}, {}, {}, { blob: 'https://example.invalid/' }] }),
    save: async () => {},
    remote: async (_vm: string, value: string) => { script = value; return 'STORAGE_SPIKE_OK'; },
  } as unknown as Lab;
  await startJob(lab, 'test-vm', { id: 'isolated-test', mode: 'fault-b', cache: 'none', pairId: 'isolated', scenario: 'pause' });
  assert.ok(script.includes('--property=PrivateMounts=yes'));
  assert.ok(script.includes('--property=User=spike'));
  assert.ok(script.includes('--property=RuntimeMaxSec=10800'));
});
