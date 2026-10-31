import test from 'node:test';
import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { limited, main, makeRunId, manualCleanup, resetGeneration, secretsFrom } from './main.js';

const REPO = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../..');

test('run IDs sort by time and are safe as Kusto table names and storage paths', () => {
  assert.equal(makeRunId(new Date(Date.UTC(2025, 0, 2, 3, 4, 5)), 'ab12'), '20250102t030405_ab12');
  assert.match(makeRunId(new Date()), /^\d{8}t\d{6}_[0-9a-f]{4}$/);
  assert.throws(() => makeRunId(new Date(), 'XYZ!'), /four hex digits/);
});

test('secrets come from identifying env values, never from short or public ones', () => {
  assert.deepEqual(secretsFrom({ AZURE_SUBSCRIPTION_ID: 'sub-1234567890', AZURE_TENANT_ID: 'tenant-123456', AZURE_LOCATION: 'westus3',
    ARROWCOL_RESET_GENERATION: 'gen-12345678', SOME_TOKEN: 'short', AZURE_OPERATOR_OBJECT_ID: undefined }),
  ['sub-1234567890', 'tenant-123456', 'gen-12345678']);
});

test('the reset generation falls back to .env.azure beside the env file', () => {
  fs.mkdirSync(path.join(REPO, '.temp'), { recursive: true });
  const dir = fs.mkdtempSync(path.join(REPO, '.temp', 'qap-main-test-'));
  try {
    assert.equal(resetGeneration({ ARROWCOL_RESET_GENERATION: 'from-env' }, path.join(dir, '.env')), 'from-env');
    assert.equal(resetGeneration({}, path.join(dir, '.env')), undefined);
    fs.writeFileSync(path.join(dir, '.env.azure'), 'ARROWCOL_RESET_GENERATION=from-generated\n');
    assert.equal(resetGeneration({}, path.join(dir, '.env')), 'from-generated');
  } finally {
    fs.rmSync(dir, { recursive: true, force: true });
  }
});

test('limited never runs more than `parallel` trials at once and runs them all', async () => {
  let running = 0;
  let peak = 0;
  const done: number[] = [];
  await limited([1, 2, 3, 4, 5], 2, async item => {
    peak = Math.max(peak, ++running);
    await new Promise(resolve => setTimeout(resolve, 5));
    running--;
    done.push(item);
  });
  assert.equal(peak, 2);
  assert.deepEqual(done.sort(), [1, 2, 3, 4, 5]);
});

test('manual cleanup instructions drop every table and the run directory', () => {
  const text = manualCleanup('20250102t030405_ab12', ['qap_20250102t030405_ab12_optimize'],
    { account: 'acct', experimentContainer: 'experimentqap', experimentDatabase: 'experimentqap' });
  assert.match(text, /\.drop external table \['qap_20250102t030405_ab12_optimize'\] ifexists/);
  assert.match(text, /--file-system experimentqap --name runs\/20250102t030405_ab12 /);
});

async function capture(argv: string[]): Promise<{ code: number; out: string; err: string }> {
  const log = console.log;
  const error = console.error;
  let out = '';
  let err = '';
  console.log = (...items: unknown[]) => { out += items.join(' ') + '\n'; };
  console.error = (...items: unknown[]) => { err += items.join(' ') + '\n'; };
  try { return { code: await main(argv), out, err }; }
  finally { console.log = log; console.error = error; }
}

test('main prints help without touching Azure and rejects bad flags with exit 2', async () => {
  const help = await capture(['--help']);
  assert.equal(help.code, 0);
  assert.match(help.out, /Azure Kusto QAP demo/);
  assert.match(help.out, /--budget 1h/);
  const keep = await capture(['--keep']);
  assert.equal(keep.code, 2);
  assert.match(keep.err, /Unknown option '--keep'[\s\S]*Run with --help/);
  assert.equal((await capture(['--log-wait', '0'])).code, 2);
});
