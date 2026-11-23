import assert from 'node:assert/strict';
import { mkdir, mkdtemp, readFile, readdir, rm, stat, utimes, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { performance } from 'node:perf_hooks';
import test from 'node:test';
import { main, OatLauncherDeadline } from './launch.js';

test('fresh diagnostic prelaunch and timed launch use the same local gate without reconciliation', async () => {
  const directory = await mkdtemp(path.join(process.cwd(), 'oat-diagnostic-test-'));
  const receiptRoot = path.join(directory, 'receipts');
  const configPath = path.join(directory, 'config.json');
  const previousParity = process.env.OAT_DARWIN_PARITY_PRECHECK;
  const previousFetch = globalThis.fetch;
  const previousExitCode = process.exitCode;
  process.env.OAT_DARWIN_PARITY_PRECHECK = '1';
  try {
    await writeFile(configPath, JSON.stringify({ platform: { kind: 'local' } }));
    const args = ['--mode', 'local', '--no-viewer', '--config', configPath];
    await main([...args, '--diagnostic-prelaunch-only'], undefined, undefined, receiptRoot);
    await assert.rejects(readdir(receiptRoot), { code: 'ENOENT' });
    await mkdir(receiptRoot);
    await main([...args, '--diagnostic-prelaunch-only'], undefined, undefined, receiptRoot);
    assert.deepEqual(await readdir(receiptRoot), []);

    let posts = 0;
    globalThis.fetch = async (input, init) => {
      assert.equal(String(input), 'http://127.0.0.1:8088/runs');
      assert.equal(init?.method, 'POST');
      posts++;
      return new Response('synthetic rejection', { status: 422 });
    };
    await main([...args, '--server', 'http://127.0.0.1:8088', '--deadline-seconds', '60'], undefined, undefined, receiptRoot);
    assert.equal(process.exitCode, 1);
    assert.equal(posts, 1);
    const names = await readdir(receiptRoot);
    assert.equal(names.length, 1);
    const receipt = JSON.parse(await readFile(path.join(receiptRoot, names[0]!), 'utf8'));
    assert.equal(receipt.creation_state, 'rejected');
    assert.equal(receipt.deadline_seconds, 60);
  } finally {
    globalThis.fetch = previousFetch;
    process.exitCode = previousExitCode;
    if (previousParity === undefined) delete process.env.OAT_DARWIN_PARITY_PRECHECK;
    else process.env.OAT_DARWIN_PARITY_PRECHECK = previousParity;
    await rm(directory, { recursive: true, force: true });
  }
});

for (const kind of ['unsettled', 'settled', 'unknown filename', 'malformed disposition'] as const) {
  test(`fresh diagnostic rejects ${kind} without reading or changing it`, async () => {
    const directory = await mkdtemp(path.join(process.cwd(), 'oat-diagnostic-test-'));
    const receiptRoot = path.join(directory, 'receipts');
    const configPath = path.join(directory, 'config.json');
    const previousParity = process.env.OAT_DARWIN_PARITY_PRECHECK;
    const previousFetch = globalThis.fetch;
    process.env.OAT_DARWIN_PARITY_PRECHECK = '1';
    try {
      await writeFile(configPath, JSON.stringify({ platform: { kind: 'local' } }));
      let entry: string;
      if (kind === 'settled' || kind === 'unsettled') {
        const receipt = await OatLauncherDeadline.create(undefined, performance.now(), receiptRoot);
        if (kind === 'unsettled') await receipt.markCreationPending('http://127.0.0.1:8088/');
        else await receipt.markNoRun();
        receipt.dispose();
        entry = receipt.receiptPath;
      } else {
        await mkdir(receiptRoot);
        entry = path.join(receiptRoot, kind === 'unknown filename' ? 'unexpected-file' : 'orphan.json.failed-aks-disposition');
        await writeFile(entry, 'not a valid receipt or disposition');
      }
      const original = await readFile(entry);
      globalThis.fetch = async () => { throw new Error('unexpected network request'); };
      const args = ['--mode', 'local', '--no-viewer', '--config', configPath];
      for (const suffix of [['--diagnostic-prelaunch-only'], ['--server', 'http://127.0.0.1:8088', '--deadline-seconds', '60']]) {
        await utimes(entry, 1000, 1000);
        const before = await stat(entry);
        await assert.rejects(main([...args, ...suffix], undefined, undefined, receiptRoot), /Fresh diagnostic requires an empty launcher receipt ledger/);
        const after = await stat(entry);
        assert.equal(after.atimeMs, before.atimeMs);
        assert.equal(after.mtimeMs, before.mtimeMs);
        assert.deepEqual(await readdir(receiptRoot), [path.basename(entry)]);
        assert.deepEqual(await readFile(entry), original);
      }
    } finally {
      globalThis.fetch = previousFetch;
      if (previousParity === undefined) delete process.env.OAT_DARWIN_PARITY_PRECHECK;
      else process.env.OAT_DARWIN_PARITY_PRECHECK = previousParity;
      await rm(directory, { recursive: true, force: true });
    }
  });
}

test('diagnostic prelaunch rejects non-parity and non-launch modes before side effects', async () => {
  const previousParity = process.env.OAT_DARWIN_PARITY_PRECHECK;
  try {
    delete process.env.OAT_DARWIN_PARITY_PRECHECK;
    await assert.rejects(main(['--diagnostic-prelaunch-only']), /requires local diagnostic parity/);
    process.env.OAT_DARWIN_PARITY_PRECHECK = '1';
    for (const options of [['--mode', 'aks'], ['--viewer-only'], ['--preflight-only'], ['--cleanup-local', '123']]) {
      await assert.rejects(main(['--diagnostic-prelaunch-only', ...options]), /requires local diagnostic parity/);
    }
  } finally {
    if (previousParity === undefined) delete process.env.OAT_DARWIN_PARITY_PRECHECK;
    else process.env.OAT_DARWIN_PARITY_PRECHECK = previousParity;
  }
});
