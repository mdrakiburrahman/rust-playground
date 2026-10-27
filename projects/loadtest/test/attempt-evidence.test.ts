import assert from 'node:assert/strict';
import fs from 'node:fs';
import { mkdirSync, mkdtempSync, readFileSync, readdirSync, rmSync, statSync, symlinkSync, writeFileSync } from 'node:fs';
import { syncBuiltinESMExports } from 'node:module';
import net from 'node:net';
import { tmpdir } from 'node:os';
import path from 'node:path';
import { test } from 'node:test';

import { beginAttemptEvidence, requireSealedAttemptEvidence, sealAttemptEvidence } from '../src/attempt-evidence.js';
import { resetBenchmark } from '../src/lifecycle.js';
import { attemptFixture } from './attempt-fixture.js';

function fixture(t: { after: (callback: () => void) => void }, shortSocketPath = false) {
  const workspace = mkdtempSync(shortSocketPath ? path.join(tmpdir(), 'a48-') : path.resolve('projects/loadtest/test/.fixture-evidence-'));
  t.after(() => rmSync(workspace, { recursive: true, force: true }));
  return { workspace, ...attemptFixture(workspace) };
}

function archive(workspace: string): string {
  const root = path.join(workspace, '.temp/loadtest-attempt-evidence');
  return path.join(root, readdirSync(root)[0]);
}

function patch(t: { after: (callback: () => void) => void }, name: 'fsyncSync' | 'writeSync' | 'readSync' | 'linkSync',
  replacement: typeof fs[typeof name]): void {
  const previous = fs[name];
  // Node's filesystem exports are writable; the live ESM binding is restored after every fault injection.
  Object.assign(fs, { [name]: replacement }); syncBuiltinESMExports();
  t.after(() => { Object.assign(fs, { [name]: previous }); syncBuiltinESMExports(); });
}

test('qualified success seals exact files, helpers and images privately before the next reset can destroy anything', async t => {
  const f = fixture(t);
  mkdirSync(path.join(f.directory, 'direct/table'), { recursive: true });
  writeFileSync(path.join(f.directory, 'direct/table/data.parquet'), 'actual-parquet-fixture');
  writeFileSync(path.join(f.directory, 'results/grafana-validation.json'), '{"passed":true}');
  await f.seal();
  requireSealedAttemptEvidence(f.workspace, f.directory);
  const root = archive(f.workspace);
  const manifest = JSON.parse(readFileSync(path.join(root, 'manifest.json'), 'utf8'));
  assert.equal(manifest.nativeCachePreserved, false);
  assert.equal(manifest.images.length, 7);
  assert.equal(statSync(root).mode & 0o777, 0o700);
  assert.equal(statSync(path.join(root, 'manifest.json')).mode & 0o777, 0o600);
  assert.equal(readFileSync(path.join(root, 'corpus/direct/table/data.parquet'), 'utf8'), 'actual-parquet-fixture');
  assert.equal(manifest.entries.filter((entry: { target: string }) => entry.target.startsWith('helpers/')).length, 3);
  assert.deepEqual(f.commands.filter(args => args[0] === 'stop'), [['stop', '--time', '300', 'router-id']]);
  // No real Docker invocation: this is the exact second-start reset with an empty stub daemon.
  await resetBenchmark(f.workspace, () => '');
  assert.equal(readFileSync(path.join(root, 'corpus/results/grafana-validation.json'), 'utf8'), '{"passed":true}');
});

for (const name of ['arrowcol-loadgen', 'arrowcol-sdk-gen', 'delta-inspect']) {
  test(`rebuilding ${name} after sealing does not invalidate preserved evidence or block the next reset`, async t => {
    const f = fixture(t);
    await f.seal();
    const original = path.join(f.helperDirectory, name);
    const previous = `${original}.previous`;
    const bytes = readFileSync(original);
    fs.renameSync(original, previous);
    writeFileSync(original, bytes, { mode: 0o700 });
    assert.notEqual(statSync(original).ino, statSync(previous).ino);
    const copied = path.join(archive(f.workspace), 'helpers', name);
    requireSealedAttemptEvidence(f.workspace, f.directory);
    await resetBenchmark(f.workspace, () => '');
    assert.deepEqual(readFileSync(copied), bytes);
  });
}

test('external lock and diagnostic changes do not rewrite their immutable archived evidence', async t => {
  const f = fixture(t);
  const diagnostic = path.join(f.workspace, 'projects/loadtest/.temp/loadgen/latest/diagnostics/stress-private-one/reader-stderr.private.log');
  mkdirSync(path.dirname(diagnostic), { recursive: true });
  writeFileSync(diagnostic, 'original diagnostic');
  await f.seal();
  const root = archive(f.workspace);
  const manifest = JSON.parse(readFileSync(path.join(root, 'manifest.json'), 'utf8')) as {
    entries: Array<{ source: string; target: string }>;
  };
  for (const original of [path.join(f.workspace, 'Cargo.lock'), path.join(f.workspace, 'package-lock.json'), diagnostic]) {
    const entry = manifest.entries.find(value => value.source === original);
    assert.ok(entry);
    const archived = path.join(root, entry.target);
    const bytes = readFileSync(archived);
    writeFileSync(original, 'new external output');
    requireSealedAttemptEvidence(f.workspace, f.directory);
    assert.deepEqual(readFileSync(archived), bytes);
  }
  await resetBenchmark(f.workspace, () => '');
  assert.equal(readFileSync(path.join(root, 'source/Cargo.lock'), 'utf8'), 'lock');
});

for (const state of ['unsealed', 'legacy', 'tampered-manifest', 'tampered-copy', 'tampered-helper', 'source-drift', 'missing-seal', 'archive-permissions']) {
  test(`${state} blocks reset before any Docker call or file deletion`, async t => {
    const f = fixture(t);
    const keep = path.join(f.directory, 'results/keep.json');
    writeFileSync(keep, 'keep');
    if (state !== 'unsealed' && state !== 'legacy') await f.seal();
    if (state === 'legacy') fs.unlinkSync(path.join(f.directory, '.attempt-evidence.json'));
    if (state === 'tampered-manifest') writeFileSync(path.join(archive(f.workspace), 'manifest.json'), '{}');
    if (state === 'tampered-copy') writeFileSync(path.join(archive(f.workspace), 'corpus/results/keep.json'), 'fake');
    if (state === 'tampered-helper') writeFileSync(path.join(archive(f.workspace), 'helpers/arrowcol-loadgen'), 'fake');
    if (state === 'source-drift') writeFileSync(keep, 'late');
    if (state === 'missing-seal') fs.unlinkSync(path.join(archive(f.workspace), 'seal.json'));
    if (state === 'archive-permissions') fs.chmodSync(archive(f.workspace), 0o755);
    const commands: string[][] = [];
    await assert.rejects(resetBenchmark(f.workspace, args => { commands.push(args); return ''; }), /reset refused/u);
    assert.deepEqual(commands, []);
    assert.ok(fs.existsSync(keep));
  });
}

for (const target of ['..', 'seal.json', 'manifest.json', 'helpers', 'helpers/delta-inspect', 'source/Cargo.lock', 'corpus', 'corpus/results/provenance.json']) {
  test(`post-seal permission drift at ${target} blocks reset before any Docker call`, async t => {
    const f = fixture(t);
    await f.seal();
    const entry = path.join(archive(f.workspace), target);
    fs.chmodSync(entry, statSync(entry).isDirectory() ? 0o755 : 0o644);
    const commands: string[][] = [];
    await assert.rejects(resetBenchmark(f.workspace, args => { commands.push(args); return ''; }), /reset refused/u);
    assert.deepEqual(commands, []);
    assert.ok(fs.existsSync(path.join(f.directory, 'results/provenance.json')));
  });
}

for (const state of ['foreign', 'router-exit', 'router-oom', 'mutator-restarted', 'rw-mount', 'wrong-socket-config']) {
  test(`${state} cannot produce a seal or enable a destructive reset`, async t => {
    const f = fixture(t);
    if (state === 'foreign') f.values[2].labels['com.docker.compose.project.config_files'] = '/foreign/compose.yaml';
    if (state === 'router-exit') f.values[2].state.ExitCode = 137;
    if (state === 'router-oom') f.values[2].state.OOMKilled = true;
    if (state === 'mutator-restarted') f.values[0].state.Running = true;
    if (state === 'rw-mount') f.values.push({
      ...f.values[2], id: 'foreign-running', labels: { ...f.values[2].labels, 'com.docker.compose.project': 'foreign' },
      mounts: [{ Type: 'bind', Source: f.directory, Destination: '/other', RW: true }],
    });
    if (state === 'wrong-socket-config') writeFileSync(path.join(f.directory, 'router-state/config.yaml'), 'server:\n  state_directory: /other\n');
    await assert.rejects(f.seal, /seal failed/u);
    await assert.rejects(resetBenchmark(f.workspace, () => { throw new Error('MUST NOT CALL DOCKER'); }), /reset refused/u);
    assert.equal(f.commands.some(args => args.includes('rm')), false);
  });
}

test('the exact configured, stopped router socket is metadata only; unknown sockets are refused', async t => {
  const f = fixture(t, true);
  const file = path.join(f.directory, 'router-state/arrowcol-router-upgrade.sock');
  const server = net.createServer();
  await new Promise<void>(resolve => server.listen(path.relative(process.cwd(), file), resolve));
  t.after(() => server.close());
  await f.seal();
  const manifest = JSON.parse(readFileSync(path.join(archive(f.workspace), 'manifest.json'), 'utf8'));
  assert.equal(manifest.entries.find((entry: { target: string }) => entry.target.endsWith('.sock')).kind, 'socket');
  assert.ok(!fs.existsSync(path.join(archive(f.workspace), 'corpus/router-state/arrowcol-router-upgrade.sock')));
  const other = fixture(t, true);
  const unknown = net.createServer();
  await new Promise<void>(resolve => unknown.listen(path.relative(process.cwd(), path.join(other.directory, 'unknown.sock')), resolve));
  t.after(() => unknown.close());
  await assert.rejects(other.seal, /seal failed/u);
});

test('symlink roots, foreign markers, archive aliases and duplicate seals are refused', async t => {
  const f = fixture(t);
  assert.throws(() => beginAttemptEvidence(f.workspace, f.directory), /initialization failed/u);
  await f.seal();
  await assert.rejects(f.seal, /seal failed/u);
  requireSealedAttemptEvidence(f.workspace, f.directory);
  const alias = path.join(f.workspace, 'alias');
  symlinkSync(f.directory, alias);
  assert.throws(() => requireSealedAttemptEvidence(f.workspace, alias), /reset refused/u);
  const other = fixture(t);
  writeFileSync(path.join(other.directory, '.attempt-evidence.json'), readFileSync(path.join(f.directory, '.attempt-evidence.json')));
  await assert.rejects(other.seal, /seal failed/u);
  const linked = fixture(t);
  symlinkSync(linked.directory, path.join(linked.workspace, '.temp'));
  await assert.rejects(linked.seal, /seal failed/u);
});

test('only explicit run diagnostics and selected helper bytes are copied, never secret scratch or env files', async t => {
  const f = fixture(t);
  const temp = path.join(f.workspace, 'projects/loadtest/.temp');
  mkdirSync(path.join(temp, 'secrets/latest'), { recursive: true });
  writeFileSync(path.join(temp, 'secrets/latest/password'), 'NEVER-COPY');
  const diagnostic = path.join(temp, 'loadgen/latest/diagnostics/stress-private-one');
  mkdirSync(diagnostic, { recursive: true });
  writeFileSync(path.join(diagnostic, 'reader-stderr.private.log'), 'PRIVATE-DIAGNOSTIC');
  writeFileSync(path.join(f.workspace, '.env'), 'NEVER-COPY');
  await f.seal();
  const manifest = readFileSync(path.join(archive(f.workspace), 'manifest.json'), 'utf8');
  assert.ok(!manifest.includes('secrets/latest'));
  assert.ok(!manifest.includes('/.env'));
  assert.equal(readFileSync(path.join(archive(f.workspace), 'diagnostics/stress-private-one/reader-stderr.private.log'), 'utf8'), 'PRIVATE-DIAGNOSTIC');
  assert.equal(statSync(path.join(archive(f.workspace), 'diagnostics/stress-private-one/reader-stderr.private.log')).mode & 0o777, 0o600);
});

test('the main path awaits qualification and observer/sampler completion, never seals a failed or nonvalidated run', () => {
  const main = readFileSync(new URL('../src/main.ts', import.meta.url), 'utf8');
  const finish = main.indexOf('await sealAttemptEvidence(');
  for (const before of ['await runLocalLoadgen(context, \'validate\'', 'await runUiGate(', 'await stopQapObserver?.()',
    'await stopSampler?.()', "step('Capturing Compose logs and diagnostics')", 'if (failure) {']) {
    assert.ok(main.lastIndexOf(before, finish) >= 0);
  }
  assert.ok(main.lastIndexOf('if (options.validate && !azure)', finish) > main.lastIndexOf('if (failure) {', finish));
  assert.ok(main.includes('requirePreviousAttemptEvidence(workspace);\n  const controller = await acquireBenchmarkController(workspace)'));
});

for (const state of ['symlink-file', 'credential-file', 'socket-wrong-type', 'private-permissions', 'helper-mismatch', 'image-mismatch']) {
  test(`${state} is fail-closed without removing original or partial evidence`, async t => {
    const f = fixture(t);
    if (state === 'symlink-file') symlinkSync(path.join(f.workspace, 'Cargo.lock'), path.join(f.directory, 'link'));
    if (state === 'credential-file') writeFileSync(path.join(f.directory, '.env'), 'NEVER-COPY');
    if (state === 'socket-wrong-type') writeFileSync(path.join(f.directory, 'router-state/arrowcol-router-upgrade.sock'), 'not-a-socket');
    if (state === 'private-permissions') {
      const root = path.join(f.workspace, '.temp/loadtest-attempt-evidence');
      mkdirSync(root, { recursive: true });
      fs.chmodSync(root, 0o755);
    }
    if (state === 'helper-mismatch') writeFileSync(path.join(f.helperDirectory, 'delta-inspect'), 'different');
    const docker = state === 'image-mismatch' ? (args: string[]) => args[0] === 'image'
      ? JSON.stringify({ id: `sha256:${'b'.repeat(64)}`, revision: 'c'.repeat(40) }) : f.docker(args) : f.docker;
    await assert.rejects(() => sealAttemptEvidence(f.workspace, f.directory, f.helperDirectory, docker, f.stopped), /seal failed/u);
    await assert.rejects(resetBenchmark(f.workspace, () => { throw new Error('MUST NOT CALL DOCKER'); }), /reset refused/u);
  });
}

test('a writable mount appearing after a successful seal blocks the next reset before signals or deletion', async t => {
  const f = fixture(t);
  await f.seal();
  const resetStart = f.commands.length;
  f.values[2].state.Running = true;
  await assert.rejects(resetBenchmark(f.workspace, f.docker), /reset refused/u);
  assert.equal(f.commands.slice(resetStart).some(args => args.includes('rm') || args[0] === 'stop'), false);
  assert.ok(fs.existsSync(path.join(archive(f.workspace), 'seal.json')));
});

test('host-mapped corpus mounts and an unrelated unlabelled builder are handled without a workspace path guess', async t => {
  const f = fixture(t);
  const hostDirectory = '/owned-host/source/onelake/benchmark/latest';
  f.values[2].mounts[0].Source = path.join(hostDirectory, 'router-state');
  const docker = (args: string[]) => {
    if (args[0] === 'ps') return `${f.docker(args)}\nbuilder`;
    if (args.at(-1) === 'builder') return JSON.stringify({
      id: 'builder', labels: null, image: 'builder-image',
      state: { Running: true, ExitCode: 0, OOMKilled: false }, mounts: [],
    });
    return f.docker(args);
  };
  await sealAttemptEvidence(f.workspace, f.directory, f.helperDirectory, docker, f.stopped, hostDirectory);
  requireSealedAttemptEvidence(f.workspace, f.directory);
  const manifest = JSON.parse(readFileSync(path.join(archive(f.workspace), 'manifest.json'), 'utf8'));
  assert.equal(manifest.hostDirectory, hostDirectory);
});

test('copy cancellation is observed between chunks, leaves an unsealed private partial and closes its descriptors', async t => {
  const f = fixture(t);
  const file = path.join(f.directory, 'large.bin');
  writeFileSync(file, Buffer.alloc(3 * 1024 * 1024));
  const controller = new AbortController();
  const read = fs.readSync;
  patch(t, 'readSync', ((...args: Parameters<typeof fs.readSync>) => {
    const result = Reflect.apply(read, fs, args);
    if (fs.readlinkSync(`/proc/self/fd/${args[0]}`) === file) {
      assert.ok(args[1].byteLength <= 1024 * 1024);
      setImmediate(() => controller.abort());
    }
    return result;
  }) as typeof fs.readSync);
  await assert.rejects(() => sealAttemptEvidence(f.workspace, f.directory, f.helperDirectory, f.docker, f.stopped, f.directory, controller.signal),
    /seal failed/u);
  assert.ok(fs.existsSync(file));
  assert.ok(!fs.existsSync(path.join(archive(f.workspace), 'seal.json')));
});

test('cancellation during the final source rehash cannot publish a seal or enable reset', async t => {
  const f = fixture(t);
  const file = path.join(f.directory, 'results/cancel-rehash');
  writeFileSync(file, Buffer.alloc(1024 * 1024 + 1));
  const controller = new AbortController();
  const read = fs.readSync;
  let reads = 0;
  patch(t, 'readSync', ((...args: Parameters<typeof fs.readSync>) => {
    const count = read(...args);
    if (count > 0 && fs.readlinkSync(`/proc/self/fd/${args[0]}`) === file && ++reads === 3) {
      setImmediate(() => controller.abort());
    }
    return count;
  }) as typeof fs.readSync);
  await assert.rejects(sealAttemptEvidence(f.workspace, f.directory, f.helperDirectory, f.docker, f.stopped, f.directory, controller.signal),
    /seal failed/u);
  assert.ok(controller.signal.aborted);
  assert.ok(!fs.existsSync(path.join(archive(f.workspace), 'seal.json')));
  const commands: string[][] = [];
  await assert.rejects(resetBenchmark(f.workspace, args => { commands.push(args); return ''; }), /reset refused/u);
  assert.deepEqual(commands, []);
});

for (const parent of ['.temp', '.']) {
  test(`failure to synchronize the ${parent} archive ancestor prevents publication and reset`, async t => {
    const f = fixture(t);
    const sync = fs.fsyncSync;
    let injected = false;
    patch(t, 'fsyncSync', (fd: number) => {
      if (fs.readlinkSync(`/proc/self/fd/${fd}`) === path.resolve(f.workspace, parent)) {
        injected = true;
        throw new Error('injected-parent-sync');
      }
      return sync(fd);
    });
    await assert.rejects(f.seal, /seal failed during durability/u);
    assert.ok(injected);
    assert.ok(!fs.existsSync(path.join(archive(f.workspace), 'seal.json')));
    const commands: string[][] = [];
    await assert.rejects(resetBenchmark(f.workspace, args => { commands.push(args); return ''; }), /reset refused/u);
    assert.deepEqual(commands, []);
    assert.ok(fs.existsSync(path.join(f.directory, 'results/provenance.json')));
  });
}

test('a final directory fsync failure revokes the published seal and cannot enable reset', async t => {
  const f = fixture(t);
  const sync = fs.fsyncSync;
  patch(t, 'fsyncSync', (fd: number) => {
    const root = path.join(f.workspace, '.temp/loadtest-attempt-evidence');
    if (fs.existsSync(root) && readdirSync(root).some(name => fs.existsSync(path.join(root, name, 'seal.json')))) {
      throw new Error('injected-final-sync');
    }
    return sync(fd);
  });
  await assert.rejects(f.seal, /seal failed/u);
  assert.ok(!fs.existsSync(path.join(archive(f.workspace), 'seal.json')));
  await assert.rejects(resetBenchmark(f.workspace, () => { throw new Error('MUST NOT CALL DOCKER'); }), /reset refused/u);
});

test('manifest accounting refuses the fixed 256MiB bound before copying', async t => {
  const f = fixture(t);
  const length = Buffer.byteLength;
  const mocked = t.mock.method(Buffer, 'byteLength', (...args: Parameters<typeof Buffer.byteLength>) =>
    typeof args[0] === 'string' && args[0].includes('"target":"corpus"') ? 256 * 1024 * 1024 : length(...args));
  t.after(() => mocked.mock.restore());
  await assert.rejects(f.seal, /seal failed/u);
  assert.ok(!fs.existsSync(path.join(archive(f.workspace), 'seal.json')));
});

test('the fixed entry bound prevents an oversized directory traversal without allocating its entire listing', async t => {
  const f = fixture(t);
  const open = fs.opendirSync;
  const entry = fs.readdirSync(f.directory, { withFileTypes: true }).find(entry => entry.name === '.attempt-evidence.json');
  assert.ok(entry);
  let count = 0;
  const mocked = t.mock.method(fs, 'opendirSync', (...args: Parameters<typeof fs.opendirSync>) => {
    const dir = open(...args);
    if (args[0] === f.directory) dir.readSync = () => { count++; return entry; };
    return dir;
  });
  syncBuiltinESMExports();
  t.after(() => { mocked.mock.restore(); syncBuiltinESMExports(); });
  await assert.rejects(f.seal, /seal failed/u);
  assert.equal(count, 100_000);
  assert.ok(!fs.existsSync(path.join(archive(f.workspace), 'seal.json')));
});

for (const fault of ['ENOSPC', 'fsync', 'publish', 'cancel', 'timeout', 'oversize', 'copy-drift']) {
  test(`${fault} leaves original data and private partials unsealed; next reset makes zero destructive calls`, async t => {
    const f = fixture(t);
    const keep = path.join(f.directory, 'results/keep');
    writeFileSync(keep, 'evidence');
    const controller = new AbortController();
    if (fault === 'cancel') controller.abort();
    if (fault === 'ENOSPC') patch(t, 'writeSync', (() => { throw Object.assign(new Error('private-path'), { code: 'ENOSPC' }); }) as typeof fs.writeSync);
    if (fault === 'fsync') patch(t, 'fsyncSync', () => { throw new Error('private-path'); });
    if (fault === 'publish') patch(t, 'linkSync', () => { throw new Error('private-path'); });
    if (fault === 'oversize') fs.truncateSync(keep, 16 * 1024 ** 3 + 1);
    if (fault === 'timeout') {
      const original = Date.now;
      let calls = 0;
      Date.now = () => original() + (++calls > 2 ? 300_001 : 0);
      t.after(() => { Date.now = original; });
    }
    if (fault === 'copy-drift') {
      const read = fs.readSync;
      let changed = false;
      patch(t, 'readSync', ((...args: Parameters<typeof fs.readSync>) => {
        const result = Reflect.apply(read, fs, args);
        if (!changed && fs.readlinkSync(`/proc/self/fd/${args[0]}`) === keep) {
          changed = true; writeFileSync(keep, 'late-write');
        }
        return result;
      }) as typeof fs.readSync);
    }
    await assert.rejects(() => sealAttemptEvidence(f.workspace, f.directory, f.helperDirectory, f.docker, f.stopped, f.directory, controller.signal),
      error => error instanceof Error && /seal failed/u.test(error.message) && !error.message.includes('private-path'));
    await assert.rejects(resetBenchmark(f.workspace, () => { throw new Error('MUST NOT CALL DOCKER'); }), /reset refused/u);
    assert.ok(fs.existsSync(keep));
    assert.ok(!f.commands.some(args => args.includes('rm')));
  });
}
