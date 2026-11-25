import assert from 'node:assert/strict';
import { createHash, randomUUID } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { constants } from 'node:fs';
import { link, lstat, mkdir, open, readdir, realpath, unlink } from 'node:fs/promises';
import { hostname } from 'node:os';
import { dirname, isAbsolute, resolve } from 'node:path';

const digest = /^[0-9a-f]{64}$/;
const uuid = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;
const maxEvidenceAgeMs = 10 * 60 * 1000;
const maxBundleBytes = 16 * 1024 * 1024;
export const settledHistorySuffix = '.settled-history';
export const historyRequiredSuffix = '.history-required';
export const historySha256 = (bytes: string | Buffer): string => createHash('sha256').update(bytes).digest('hex');
export type HistoryReceiptValidator = (value: unknown, stamp: string, subscription: string) => { invocationId: string; runId: string | null };
export interface HandoverDestination {
  hostname: string;
  workspace: string;
  ledger: string;
  git_common_dir: string;
  uid: number;
}

function object(value: unknown): Record<string, unknown> {
  assert.ok(value !== null && typeof value === 'object' && !Array.isArray(value), 'Invalid handover object');
  return value as Record<string, unknown>;
}
function text(value: unknown): string {
  assert.ok(typeof value === 'string' && value.length > 0, 'Missing handover identity');
  return value;
}
function time(value: unknown): number {
  assert.ok(typeof value === 'number' && Number.isSafeInteger(value) && value > 0, 'Invalid handover timestamp');
  return value;
}
function evidence(value: unknown): void {
  const file = object(value);
  const bytes = Buffer.from(text(file.content_base64), 'base64');
  assert.equal(bytes.toString('base64'), file.content_base64, 'Noncanonical evidence bytes');
  assert.equal(historySha256(bytes), file.sha256, 'Handover evidence hash mismatch');
}
function fresh(value: unknown, now: number): void {
  const observed = time(value);
  assert.ok(observed <= now && now - observed <= maxEvidenceAgeMs, 'Handover evidence is stale or future-dated');
}

export async function handoverDestination(workspace: string, ledger: string): Promise<HandoverDestination> {
  const uid = process.getuid?.();
  assert.ok(uid !== undefined, 'Operator handover requires a Unix owner');
  assert.equal(await realpath(workspace), workspace, 'Handover workspace must be canonical');
  const metadata = await lstat(workspace);
  assert.ok(metadata.isDirectory() && metadata.uid === uid, 'Handover workspace is not owned');
  assert.equal(ledger, resolve(workspace, 'out/arrowcol-oat/deadline-receipts'), 'Handover requires an independent checkout ledger, not the held primary');
  const dotGit = await lstat(resolve(workspace, '.git'));
  assert.ok(dotGit.isDirectory() && !dotGit.isSymbolicLink() && dotGit.uid === uid, 'Handover destination must be an independently owned clone');
  const env = Object.fromEntries(Object.entries(process.env).filter(([name]) => !name.startsWith('GIT_')));
  const git = (...args: string[]) => execFileSync('git', ['-C', workspace, ...args], { encoding: 'utf8', env, stdio: ['ignore', 'pipe', 'pipe'] }).trim();
  assert.equal(git('rev-parse', '--show-toplevel'), workspace, 'Wrong destination Git workspace');
  const common = git('rev-parse', '--path-format=absolute', '--git-common-dir');
  assert.equal(await realpath(common), resolve(workspace, '.git'), 'Wrong destination Git identity');
  return { hostname: hostname(), workspace, ledger, git_common_dir: common, uid };
}

async function readOwned(path: string): Promise<Buffer> {
  assert.equal(await realpath(path), path, 'Handover path must not use symlinks');
  const file = await open(path, constants.O_RDONLY | constants.O_NOFOLLOW);
  try {
    const metadata = await file.stat();
    assert.ok(metadata.isFile() && metadata.uid === process.getuid?.() && metadata.size <= maxBundleBytes, 'Invalid handover file owner, type or size');
    return await file.readFile();
  } finally { await file.close(); }
}

export async function assertHistoryPin(ledger: string, expectedSha?: string): Promise<void> {
  const present = async (path: string) => lstat(path).then(() => true).catch((error: unknown) => {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return false;
    throw error;
  });
  if (expectedSha !== undefined) {
    assert.match(expectedSha, digest, 'Expected a reviewed handover SHA-256');
    assert.equal((await readOwned(ledger + historyRequiredSuffix)).toString('utf8'), `${expectedSha}\n`, 'Required handover digest mismatch');
  } else if (await present(ledger + historyRequiredSuffix) || await present(ledger + settledHistorySuffix)) {
    throw new Error('AKS handover requires --aks-history-sha256; refusing an unpinned admission');
  }
}

function validateBundle(bytes: Buffer, expectedSha: string, destination: HandoverDestination, validateReceipt: HistoryReceiptValidator, importing: boolean, now: number) {
  assert.match(expectedSha, digest, 'Expected a reviewed handover SHA-256');
  assert.equal(historySha256(bytes), expectedSha, 'Handover bundle hash mismatch');
  const bundle = object(JSON.parse(bytes.toString('utf8')));
  assert.equal(bundle.version, 1);
  assert.equal(bundle.kind, 'aks-settled-history');
  const stamp = text(bundle.stamp);
  const subscription = text(bundle.subscription_id);
  assert.match(subscription, uuid, 'Invalid handover subscription');
  const target = object(bundle.destination);
  for (const [key, value] of Object.entries(destination)) assert.equal(target[key], value, `Handover destination ${key} mismatch`);
  const targetLease = text(target.lease_id);
  text(target.client_id);
  const source = object(bundle.source);
  text(source.host);
  text(source.operator);
  time(source.observed_at_unix_ms);
  assert.ok(Array.isArray(source.ledgers) && source.ledgers.length > 0, 'Missing complete source-ledger census');
  const roots = source.ledgers.map(text);
  assert.equal(new Set(roots).size, roots.length, 'Duplicate source ledger');
  assert.ok(roots.every(isAbsolute), 'Source ledger paths must be absolute');
  assert.ok(Array.isArray(source.files) && source.files.length > 0, 'Empty history cannot establish a handover');
  const ids = new Set<string>();
  const runs = new Set<string>();
  const inventory: { ledger: string; name: string; sha256: string }[] = [];
  for (const entry of source.files) {
    const file = object(entry);
    const ledger = text(file.ledger);
    const name = text(file.name);
    assert.ok(roots.includes(ledger) && /^[0-9a-f-]{36}\.json$/i.test(name), 'Unsupported source ledger entry');
    evidence(file);
    const original: unknown = JSON.parse(Buffer.from(text(file.content_base64), 'base64').toString('utf8'));
    const identity = validateReceipt(original, stamp, subscription);
    const receipt = object(original);
    assert.ok(time(receipt.started_at_unix_ms) <= time(source.observed_at_unix_ms), 'Historical receipt follows its source census');
    if (receipt.terminal_observation != null) {
      assert.ok(time(object(receipt.terminal_observation).observed_at_unix_ms) <= time(source.observed_at_unix_ms), 'Terminal proof follows its source census');
    }
    assert.equal(name, `${identity.invocationId}.json`, 'Historical filename/producer mismatch');
    assert.ok(!ids.has(identity.invocationId), 'Duplicate historical invocation');
    ids.add(identity.invocationId);
    if (identity.runId !== null) {
      assert.ok(!runs.has(identity.runId), 'Duplicate historical run');
      runs.add(identity.runId);
    }
    inventory.push({ ledger, name, sha256: text(file.sha256) });
  }
  inventory.sort((a, b) => `${a.ledger}/${a.name}`.localeCompare(`${b.ledger}/${b.name}`, 'en'));
  const inventorySha = historySha256(JSON.stringify(inventory));
  assert.equal(source.before_sha256, inventorySha, 'Incomplete or mismatched before census');
  assert.equal(source.after_sha256, inventorySha, 'Source history changed during export');
  assert.ok(Array.isArray(source.lease_ids) && source.lease_ids.length > 0, 'Missing source operator leases');
  const sourceLeases = source.lease_ids.map(text);
  assert.equal(new Set(sourceLeases).size, sourceLeases.length, 'Duplicate source operator lease');
  const exportLease = text(source.export_lease_id);
  assert.ok(sourceLeases.includes(exportLease), 'Export lease missing from source custody');
  const approval = object(bundle.approval);
  assert.equal(approval.exclusive_campaign_ownership, true, 'An explicit exclusive-campaign handover is required');
  assert.equal(approval.source_inventory_sha256, inventorySha, 'Approval does not bind source census');
  assert.ok(time(approval.observed_at_unix_ms) >= time(source.observed_at_unix_ms), 'Approval predates source census');
  const retirement = object(approval.retirement);
  const finalCensus = object(retirement.final_census);
  assert.deepEqual(finalCensus.ledgers, source.ledgers, 'Final census covers different source directories');
  assert.deepEqual(finalCensus.inventory, inventory, 'Source frontier changed before retirement');
  assert.equal(finalCensus.sha256, inventorySha, 'Final census digest does not bind exported history');
  assert.ok(time(finalCensus.observed_at_unix_ms) >= time(source.observed_at_unix_ms), 'Final census predates export');
  assert.ok(time(retirement.observed_at_unix_ms) <= time(approval.observed_at_unix_ms), 'Retirement observation follows approval');
  assert.equal(retirement.owned_launchers, 0, 'Predecessor launcher still active');
  assert.equal(retirement.owned_jobs, 0, 'Predecessor job still active');
  assert.equal(retirement.renewals, 0, 'Predecessor renewal still active');
  assert.ok(Array.isArray(retirement.leases) && retirement.leases.length > 0, 'Missing predecessor lease retirement');
  const leases = new Set<string>();
  for (const item of retirement.leases) {
    const lease = object(item);
    const id = text(lease.lease_id);
    assert.ok(id !== targetLease && !leases.has(id), 'Conflicting predecessor lease');
    leases.add(id);
    text(lease.client_id);
    assert.equal(lease.state, 'released', 'Predecessor lease not released');
    assert.ok(time(lease.released_at_unix_ms) <= time(retirement.observed_at_unix_ms), 'Release follows retirement observation');
    if (id === exportLease) {
      const gap = time(lease.released_at_unix_ms) - time(finalCensus.observed_at_unix_ms);
      assert.ok(gap >= 0 && gap <= maxEvidenceAgeMs, 'Final census is stale at release or follows source lease release');
    }
  }
  assert.ok(sourceLeases.every(id => leases.has(id)), 'A source operator lease has not been retired');
  evidence(retirement.evidence);
  const idle = object(approval.safe_idle);
  assert.ok(time(idle.observed_at_unix_ms) <= time(approval.observed_at_unix_ms), 'Idle observation follows approval');
  assert.equal(idle.stamp, stamp, 'Idle witness stamp mismatch');
  assert.equal(idle.subscription_id, subscription, 'Idle witness subscription mismatch');
  assert.equal(idle.run_namespaces, 0, 'Run namespaces still exist');
  assert.equal(idle.run_fics, 0, 'Run identities still exist');
  assert.ok(Array.isArray(idle.benchmark_pools) && idle.benchmark_pools.length > 0, 'Missing benchmark pool census');
  const pools = new Set<string>();
  for (const item of idle.benchmark_pools) {
    const pool = object(item);
    const name = text(pool.name);
    assert.ok(!pools.has(name), 'Duplicate benchmark pool observation');
    pools.add(name);
    assert.equal(pool.count, 0, 'Benchmark capacity is not idle');
    assert.equal(pool.autoscale_min, 0, 'Benchmark pool has a paid minimum');
    assert.ok(typeof pool.autoscaling === 'boolean' && typeof pool.autoscale_max === 'number' &&
      Number.isSafeInteger(pool.autoscale_max) && pool.autoscale_max >= 0, 'Missing bounded benchmark autoscaling observation');
  }
  evidence(idle.evidence);
  if (importing) {
    fresh(approval.observed_at_unix_ms, now);
    fresh(retirement.observed_at_unix_ms, now);
    fresh(idle.observed_at_unix_ms, now);
  } else {
    time(retirement.observed_at_unix_ms);
    time(idle.observed_at_unix_ms);
  }
  return { stamp, subscription, ids, runs };
}

export async function assertSettledHistory(
  ledger: string, destination: HandoverDestination, validateReceipt: HistoryReceiptValidator,
  expectedSha: string, stamp: string, subscription: string,
): Promise<void> {
  await assertHistoryPin(ledger, expectedSha);
  const bytes = await readOwned(ledger + settledHistorySuffix);
  const history = validateBundle(bytes, expectedSha, destination, validateReceipt, false, Date.now());
  assert.equal(history.stamp, stamp, 'Requested stamp does not match handed-over history');
  assert.equal(history.subscription, subscription, 'Requested subscription does not match handed-over history');
  assert.equal(await realpath(ledger), ledger, 'Destination ledger was replaced');
  for (const name of await readdir(ledger)) {
    if (!name.endsWith('.json')) continue;
    const receipt = object(JSON.parse((await readOwned(resolve(ledger, name))).toString('utf8')));
    assert.ok(!history.ids.has(text(receipt.invocation_id)), 'Historical invocation must never become replayable work');
    assert.ok(receipt.run_id === null || (typeof receipt.run_id === 'string' && !history.runs.has(receipt.run_id)), 'Historical run must never be replayed');
  }
}

export async function importSettledHistory(
  bundlePath: string, expectedSha: string, destination: HandoverDestination, validateReceipt: HistoryReceiptValidator,
  assertLockHeld: () => void, now = Date.now(),
): Promise<void> {
  assertLockHeld();
  const bytes = await readOwned(bundlePath);
  validateBundle(bytes, expectedSha, destination, validateReceipt, true, now);
  const ledger = destination.ledger;
  await mkdir(ledger, { recursive: true, mode: 0o700 });
  assert.equal(await realpath(ledger), ledger, 'Destination ledger must be canonical');
  assert.equal((await lstat(ledger)).uid, destination.uid, 'Destination ledger has another owner');
  assert.equal((await readdir(ledger)).length, 0, 'Handover cannot replace an existing destination ledger');
  const archive = ledger + settledHistorySuffix;
  assertLockHeld();
  const required = await open(ledger + historyRequiredSuffix, 'wx', 0o600);
  try {
    await required.writeFile(`${expectedSha}\n`);
    await required.sync();
  } finally { await required.close(); }
  const parent = await open(dirname(archive), 'r');
  try { await parent.sync(); } finally { await parent.close(); }
  const temporary = `${archive}.${randomUUID()}.tmp`;
  const file = await open(temporary, 'wx', 0o600);
  try {
    try {
      await file.writeFile(bytes);
      await file.sync();
    } finally { await file.close(); }
    assertLockHeld();
    // link is atomic and refuses an existing archive; an uncertain completion is never retried.
    await link(temporary, archive);
    const directory = await open(dirname(archive), 'r');
    try { await directory.sync(); } finally { await directory.close(); }
  } finally { await unlink(temporary); }
}
