import assert from 'node:assert/strict';
import { execFileSync, spawn } from 'node:child_process';
import { once } from 'node:events';
import { mkdir, mkdtemp, readFile, readdir, rm, symlink, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { performance } from 'node:perf_hooks';
import test from 'node:test';
import { pathToFileURL } from 'node:url';
import {
  assertHistoryPin, assertSettledHistory, handoverDestination, historyRequiredSuffix, historySha256, importSettledHistory, settledHistorySuffix,
} from './aks-settled-history.js';
import { acquireAksSubmitterLock, assertLaunchLedgersSettled, OatLauncherDeadline, validateSettledHistoryReceipt } from './launch.js';

const subscription = '123e4567-e89b-42d3-a456-426614174000';
const invocation = '123e4567-e89b-42d3-a456-426614174001';
const run = '123e4567-e89b-42d3-a456-426614174002';
const stamp = 'fixture';
const file = (content: string) => ({ content_base64: Buffer.from(content).toString('base64'), sha256: historySha256(content) });

async function fixture() {
  const root = await mkdtemp(join(tmpdir(), 'oat-handover-'));
  const workspace = join(root, 'destination');
  await mkdir(workspace);
  execFileSync('git', ['init', '--quiet', workspace]);
  const ledger = join(workspace, 'out/arrowcol-oat/deadline-receipts');
  const destination = await handoverDestination(workspace, ledger);
  const now = Date.now();
  const started = now - 20_000;
  const body = JSON.stringify({ stamp, platform: { kind: 'aks' } });
  const receipt = {
    version: 1, invocation_id: invocation, started_at_unix_ms: started, started_at_monotonic_ms: 10,
    deadline_seconds: 5, deadline_at_unix_ms: started + 5_000, deadline_at_monotonic_ms: 5_010,
    submitted_config_body: body, submitted_config_sha256: historySha256(body), source_config_sha256: 'a'.repeat(64),
    cleanup_deadline_at_unix_ms: started + 10_000, config_binding_error: null,
    overrun: true, overrun_at_unix_ms: started + 5_000,
    creation_state: 'accepted', server_url: 'http://127.0.0.1:1', server_pid: 123,
    server_start_ticks: '456', server_executable: '/original/server', run_id: run, run_state: 'failed',
    terminal_observation: {
      observed_at_unix_ms: started + 8_000,
      status: { id: run, config_sha256: historySha256(body), state: 'failed', progress: 'failed', result_count: 0,
        error: 'synthetic failure', finished_at_unix_ms: started + 7_000, aks_cleanup_confirmed: true },
    },
    cancellation_attempts: 0, last_cancel_result: null, cleanup_state: 'confirmed', error: 'synthetic failure',
  };
  const bundle = {
    version: 1, kind: 'aks-settled-history', stamp, subscription_id: subscription,
    source: {
      host: 'synthetic-old-host', operator: 'synthetic-old-client', observed_at_unix_ms: now - 1000,
      lease_ids: ['old-lease', 'export-lease'], export_lease_id: 'export-lease', ledgers: ['/original/ledger'],
      files: [{ ledger: '/original/ledger', name: `${invocation}.json`, ...file(JSON.stringify(receipt, null, 2) + '\n') }],
      before_sha256: '', after_sha256: '',
    },
    destination: { ...destination, lease_id: 'new-lease', client_id: 'new-client' },
    approval: {
      observed_at_unix_ms: now, exclusive_campaign_ownership: true, source_inventory_sha256: '',
      retirement: {
        observed_at_unix_ms: now, owned_launchers: 0, owned_jobs: 0, renewals: 0,
        final_census: { observed_at_unix_ms: now - 750, sha256: '', ledgers: ['/original/ledger'],
          inventory: [] as { ledger: string; name: string; sha256: string }[] },
        leases: ['old-lease', 'export-lease'].map(lease_id => ({
          lease_id, client_id: 'old-client', state: 'released', released_at_unix_ms: now - 500,
        })),
        evidence: file('synthetic, not a live operator retirement receipt'),
      },
      safe_idle: {
        observed_at_unix_ms: now, stamp, subscription_id: subscription, run_namespaces: 0, run_fics: 0,
        benchmark_pools: [{ name: 'fixture-only', count: 0, autoscaling: true, autoscale_min: 0, autoscale_max: 1 }],
        evidence: file('synthetic, not a live cloud observation'),
      },
    },
  };
  const path = join(root, 'bundle');
  const save = async () => {
    const inventory = bundle.source.files.map(({ ledger, name, sha256 }) => ({ ledger, name, sha256 }))
      .sort((a, b) => `${a.ledger}/${a.name}`.localeCompare(`${b.ledger}/${b.name}`, 'en'));
    bundle.source.before_sha256 = bundle.source.after_sha256 = bundle.approval.source_inventory_sha256 = historySha256(JSON.stringify(inventory));
    bundle.approval.retirement.final_census.inventory = inventory;
    bundle.approval.retirement.final_census.sha256 = bundle.source.after_sha256;
    const bytes = JSON.stringify(bundle, null, 2) + '\n';
    await writeFile(path, bytes);
    return historySha256(bytes);
  };
  return { root, workspace, ledger, destination, now, receipt, bundle, path, save };
}

test('one-time handover preserves exact historical bytes/deadlines and never reconciles them', async () => {
  const f = await fixture();
  const original = Buffer.from(f.bundle.source.files[0]!.content_base64, 'base64');
  const sha = await f.save();
  const lock = await acquireAksSubmitterLock(f.ledger);
  try {
    await importSettledHistory(f.path, sha, f.destination, validateSettledHistoryReceipt, () => lock.assertHeld(), f.now);
    await assert.rejects(assertHistoryPin(f.ledger), /requires --aks-history-sha256/);
    assert.deepEqual(await readFile(f.ledger + settledHistorySuffix), await readFile(f.path));
    assert.deepEqual(await readdir(f.ledger), []);
    await assertSettledHistory(f.ledger, f.destination, validateSettledHistoryReceipt, sha, stamp, subscription);
    await assertLaunchLedgersSettled(f.ledger);
    const next = await OatLauncherDeadline.create(undefined, performance.now(), f.ledger);
    await next.markNoRun();
    next.dispose();
    await assertSettledHistory(f.ledger, f.destination, validateSettledHistoryReceipt, sha, stamp, subscription);
    const archived = JSON.parse(await readFile(f.ledger + settledHistorySuffix, 'utf8'));
    assert.deepEqual(Buffer.from(archived.source.files[0].content_base64, 'base64'), original);
    assert.equal(JSON.parse(original.toString()).cleanup_deadline_at_unix_ms, f.receipt.cleanup_deadline_at_unix_ms);
    await assert.rejects(importSettledHistory(f.path, sha, f.destination, validateSettledHistoryReceipt, () => lock.assertHeld(), f.now), /existing destination ledger/);
    await writeFile(join(f.ledger, `${invocation}.json`), original);
    await assert.rejects(assertSettledHistory(f.ledger, f.destination, validateSettledHistoryReceipt, sha, stamp, subscription), /never become replayable/);
  } finally {
    await lock.release();
    await rm(f.root, { recursive: true, force: true });
  }
});

test('interrupted import cannot become a blank ledger or grant a fresh admission', async () => {
  const f = await fixture();
  const sha = await f.save();
  let checks = 0;
  try {
    await assert.rejects(importSettledHistory(f.path, sha, f.destination, validateSettledHistoryReceipt, () => {
      if (++checks === 3) throw new Error('synthetic lock loss before publication');
    }, f.now), /lock loss/);
    assert.equal(await readFile(f.ledger + historyRequiredSuffix, 'utf8'), `${sha}\n`);
    await assert.rejects(readFile(f.ledger + settledHistorySuffix), { code: 'ENOENT' });
    await assert.rejects(assertHistoryPin(f.ledger), /requires --aks-history-sha256/);
    await assert.rejects(assertSettledHistory(f.ledger, f.destination, validateSettledHistoryReceipt, sha, stamp, subscription), { code: 'ENOENT' });
    await assert.rejects(importSettledHistory(f.path, sha, f.destination, validateSettledHistoryReceipt, () => {}, f.now), { code: 'EEXIST' });
    assert.deepEqual(await readdir(f.ledger), []);
  } finally { await rm(f.root, { recursive: true, force: true }); }
});

test('history rejects ambiguous admission, wrong stamp, missing cleanup and incomplete evidence before writing', async () => {
  const mutations: [string, (f: Awaited<ReturnType<typeof fixture>>) => void, RegExp][] = [
    ['active', f => { f.receipt.creation_state = 'pending'; }, /historical|Historical/],
    ['ambiguous', f => { f.receipt.creation_state = 'unknown'; }, /historical|Historical/],
    ['cleanup', f => { f.receipt.terminal_observation.status.aks_cleanup_confirmed = false; }, /historical|Historical/],
    ['terminal', f => { f.receipt.run_state = 'running'; }, /historical|Historical/],
    ['stamp', f => { f.bundle.stamp = 'different'; }, /different stamp/],
    ['stale', f => { f.bundle.approval.safe_idle.observed_at_unix_ms -= 600_001; }, /stale/],
    ['lease', f => { f.bundle.approval.retirement.leases[0]!.state = 'active'; }, /not released/],
    ['missing lease', f => { f.bundle.approval.retirement.leases.pop(); }, /not been retired/],
    ['premature release', f => { f.bundle.approval.retirement.final_census.observed_at_unix_ms = f.now; }, /follows source lease release/],
    ['stale final census', f => { f.bundle.approval.retirement.final_census.observed_at_unix_ms = f.now - 2000; }, /predates export/],
    ['incomplete final census', f => { f.bundle.approval.retirement.final_census.ledgers = []; }, /different source directories/],
    ['renewal', f => { f.bundle.approval.retirement.renewals = 1; }, /renewal still active/],
    ['owner', f => { f.bundle.destination.uid += 1; }, /destination uid mismatch/],
    ['capacity', f => { f.bundle.approval.safe_idle.benchmark_pools[0]!.count = 1; }, /not idle/],
    ['idle evidence', f => { f.bundle.approval.safe_idle.evidence.sha256 = 'f'.repeat(64); }, /evidence hash mismatch/],
  ];
  for (const [label, mutate, reason] of mutations) {
    const f = await fixture();
    try {
      mutate(f);
      Object.assign(f.bundle.source.files[0]!, file(JSON.stringify(f.receipt)));
      const sha = await f.save();
      await assert.rejects(importSettledHistory(f.path, sha, f.destination, validateSettledHistoryReceipt, () => {}, f.now), reason, label);
      await assert.rejects(readFile(f.ledger + settledHistorySuffix), { code: 'ENOENT' });
    } finally { await rm(f.root, { recursive: true, force: true }); }
  }
});

test('a newly settled file in the final census still invalidates the entire export', async () => {
  const f = await fixture();
  try {
    await f.save();
    f.bundle.approval.retirement.final_census.inventory.push({
      ledger: '/original/ledger', name: `${run}.json`, sha256: 'b'.repeat(64),
    });
    const bytes = JSON.stringify(f.bundle);
    await writeFile(f.path, bytes);
    await assert.rejects(importSettledHistory(f.path, historySha256(bytes), f.destination, validateSettledHistoryReceipt, () => {}, f.now), /frontier changed/);
    f.bundle.approval.retirement.final_census.inventory.pop();
    f.bundle.approval.retirement.final_census.sha256 = 'f'.repeat(64);
    const wrongDigest = JSON.stringify(f.bundle);
    await writeFile(f.path, wrongDigest);
    await assert.rejects(importSettledHistory(f.path, historySha256(wrongDigest), f.destination, validateSettledHistoryReceipt, () => {}, f.now), /Final census digest/);
    await assert.rejects(readFile(f.ledger + historyRequiredSuffix), { code: 'ENOENT' });
  } finally { await rm(f.root, { recursive: true, force: true }); }
});
test('empty, truncated, tampered, symlinked and duplicate handovers fail closed', async () => {
  const f = await fixture();
  const sha = await f.save();
  const lock = await acquireAksSubmitterLock(f.ledger);
  try {
    await assert.rejects(importSettledHistory(f.path, 'f'.repeat(64), f.destination, validateSettledHistoryReceipt, () => lock.assertHeld(), f.now), /bundle hash mismatch/);
    const alias = join(f.root, 'alias');
    await symlink(f.path, alias);
    await assert.rejects(importSettledHistory(alias, sha, f.destination, validateSettledHistoryReceipt, () => lock.assertHeld(), f.now), /symlinks/);
    const saved = await readFile(f.path);
    await writeFile(f.path, '{');
    await assert.rejects(importSettledHistory(f.path, historySha256('{'), f.destination, validateSettledHistoryReceipt, () => lock.assertHeld(), f.now), SyntaxError);
    await writeFile(f.path, saved);
    f.bundle.source.files = [];
    const emptySha = await f.save();
    await assert.rejects(importSettledHistory(f.path, emptySha, f.destination, validateSettledHistoryReceipt, () => lock.assertHeld(), f.now), /Empty history/);
    await writeFile(f.path, saved);
    await importSettledHistory(f.path, sha, f.destination, validateSettledHistoryReceipt, () => lock.assertHeld(), f.now);
    await assert.rejects(importSettledHistory(f.path, sha, f.destination, validateSettledHistoryReceipt, () => lock.assertHeld(), f.now), { code: 'EEXIST' });
    await writeFile(f.ledger + settledHistorySuffix, '{');
    await assert.rejects(assertSettledHistory(f.ledger, f.destination, validateSettledHistoryReceipt, sha, stamp, subscription), /bundle hash mismatch/);
    await rm(f.ledger + settledHistorySuffix);
    await assert.rejects(assertSettledHistory(f.ledger, f.destination, validateSettledHistoryReceipt, sha, stamp, subscription), { code: 'ENOENT' });
  } finally {
    await lock.release();
    await rm(f.root, { recursive: true, force: true });
  }
});

test('pre-submission failed build receipts are inert, but any possible effect is refused', async () => {
  const f = await fixture();
  try {
    const noRun = { ...f.receipt, creation_state: 'not_started', run_state: 'not_started', run_id: null,
      submitted_config_body: null, submitted_config_sha256: null, source_config_sha256: null,
      server_url: null, server_pid: null, server_start_ticks: null, server_executable: null, terminal_observation: null };
    assert.deepEqual(validateSettledHistoryReceipt(noRun, stamp, subscription), { invocationId: invocation, runId: null });
    assert.throws(() => validateSettledHistoryReceipt({ ...noRun, server_url: 'http://127.0.0.1:1' }, stamp, subscription), /active, uncertain/);
    assert.throws(() => validateSettledHistoryReceipt({ ...noRun, creation_state: 'unknown' }, stamp, subscription), /active, uncertain/);
  } finally { await rm(f.root, { recursive: true, force: true }); }
});

test('history survives restart and concurrent independent processes share the destination lock', { timeout: 60_000 }, async () => {
  const f = await fixture();
  const sha = await f.save();
  const historyModule = new URL('./aks-settled-history.ts', import.meta.url).href;
  const launchModule = new URL('./launch.ts', import.meta.url).href;
  const childCode = `
    import { acquireAksSubmitterLock, validateSettledHistoryReceipt } from ${JSON.stringify(launchModule)};
    import { importSettledHistory, assertSettledHistory } from ${JSON.stringify(historyModule)};
    const destination = ${JSON.stringify(f.destination)};
    const lock = await acquireAksSubmitterLock(destination.ledger);
    await importSettledHistory(${JSON.stringify(f.path)}, ${JSON.stringify(sha)}, destination, validateSettledHistoryReceipt, () => lock.assertHeld());
    console.log('installed');
    setInterval(() => {}, 1000);
  `;
  const child = spawn(process.execPath, ['--import', 'tsx', '--input-type=module', '-e', childCode], { stdio: ['ignore', 'pipe', 'pipe'] });
  let output = '';
  let stderr = '';
  child.stderr.setEncoding('utf8').on('data', chunk => { stderr += chunk; });
  try {
    await new Promise<void>((resolve, reject) => {
      child.once('exit', code => reject(new Error(`handover child exited ${code}: ${stderr}`)));
      child.stdout.setEncoding('utf8').on('data', chunk => { output += chunk; if (output.includes('installed')) resolve(); });
    });
    const other = join(f.root, 'independent');
    execFileSync('git', ['init', '--quiet', other]);
    assert.throws(() => execFileSync(process.execPath, ['--import', pathToFileURL(join(process.cwd(), 'node_modules/tsx/dist/loader.mjs')).href,
      '--input-type=module', '-e', `import { acquireAksSubmitterLock } from ${JSON.stringify(launchModule)}; await acquireAksSubmitterLock(${JSON.stringify(f.ledger)});`],
    { cwd: other, stdio: 'pipe' }), /Another AKS launcher holds/);
    const exited = once(child, 'exit');
    child.kill('SIGKILL');
    await exited;
    let lock: Awaited<ReturnType<typeof acquireAksSubmitterLock>> | undefined;
    for (let attempt = 0; !lock; attempt++) {
      try { lock = await acquireAksSubmitterLock(f.ledger); }
      catch (error) {
        if (attempt === 50 || !(error instanceof Error) || !/Another AKS launcher holds/.test(error.message)) throw error;
        await new Promise(resolve => setTimeout(resolve, 100));
      }
    }
    try {
      await assertSettledHistory(f.ledger, f.destination, validateSettledHistoryReceipt, sha, stamp, subscription);
      await assert.rejects(importSettledHistory(f.path, sha, f.destination, validateSettledHistoryReceipt, () => lock!.assertHeld()), { code: 'EEXIST' });
      assert.equal((await readdir(f.ledger)).length, 0);
    } finally { await lock.release(); }
  } finally {
    if (child.exitCode === null && child.signalCode === null) { child.kill('SIGKILL'); await once(child, 'exit'); }
    await rm(f.root, { recursive: true, force: true });
  }
});
