import assert from 'node:assert/strict';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { cleanupRetainedJob, parseRetainedReceipt, type RetainedJobReceipt } from './retained-job.js';

const job = 'azure-qap-20261107123456-1234abcd';
const uid = '11111111-2222-3333-4444-555555555555';
const receipt: RetainedJobReceipt = { version: 1, mode: 'read-only-inspection', jobName: job,
  namespace: 'azure-qap', context: null, pod: `${job}-abcde`, podUid: uid,
  resources: { [`job/${job}`]: uid, [`configmap/${job}-env`]: uid, [`secret/${job}-secret`]: uid },
  runId: '20261107t123456_abcd', inspectionDeadline: '2026-11-07T20:00:00Z' };
const metadata = (name: string) => ({ name, namespace: receipt.namespace, uid,
  labels: { 'app.kubernetes.io/name': 'azure-qap', run: job }, ownerReferences: [{ kind: 'Job', uid }] });
const report = (status: string) => ({ runId: receipt.runId, cleanup: { status, errors: [] },
  inspection: { mode: 'read-only', deadline: receipt.inspectionDeadline } });

test('receipt rejects generic names, namespace/context changes, incomplete ownership and traversal', () => {
  assert.deepEqual(parseRetainedReceipt(receipt, receipt.namespace), receipt);
  for (const changed of [{ jobName: 'unrelated' }, { namespace: 'other' }, { context: 'other' },
    { pod: '../unrelated' }, { resources: { [`job/${job}`]: uid } }, { runId: '../other' }])
    assert.throws(() => parseRetainedReceipt({ ...receipt, ...changed }, receipt.namespace));
});

test('scoped teardown signals only the verified Pod, waits for cleanup, copies final evidence, then deletes exact UID-verified objects', async () => {
  const output = path.resolve(`.temp/retained-job-test-${process.pid}`);
  fs.rmSync(output, { recursive: true, force: true });
  const calls: string[][] = [];
  try {
    await cleanupRetainedJob({ receipt, namespace: receipt.namespace, output,
      kubectl: async args => {
        calls.push(args);
        if (args[0] === 'get') return JSON.stringify({ metadata: metadata(args[1]!.split('/')[1]!) });
        if (args.includes('/artifacts/report.json')) return JSON.stringify(report('retained'));
        if (args.includes('sh')) return 'done';
        if (args[0] === 'cp') fs.writeFileSync(path.join(output, 'report.json'), JSON.stringify(report('done')));
        return '';
      } });
    const signal = calls.findIndex(call => call.includes('SIGTERM'));
    const copied = calls.findIndex(call => call[0] === 'cp');
    const deleted = calls.findIndex(call => call[0] === 'delete');
    assert.ok(signal > 3 && copied > signal && deleted > copied);
    assert.deepEqual(calls[signal]!.slice(0, 7), ['exec', '--container', 'experiment', '-n', 'azure-qap', receipt.pod, '--']);
    assert.deepEqual(calls[deleted]!.slice(1, 4), Object.keys(receipt.resources));
    const release = calls.findIndex(call => call.includes('SIGUSR1'));
    assert.ok(release > copied && deleted > release);
    assert.ok(calls.every(call => !call.includes('--all') && !call.includes('--selector')));
  } finally { fs.rmSync(output, { recursive: true, force: true }); }
});

test('signal, copy, release and revalidated ownership failures preserve exact owned resources', async () => {
  for (const failure of ['SIGTERM', 'cp', 'SIGUSR1', 'revalidation']) {
    const output = path.resolve(`.temp/retained-job-error-${failure}-${process.pid}`);
    fs.rmSync(output, { recursive: true, force: true });
    let copied = false;
    const calls: string[][] = [];
    try {
      await assert.rejects(cleanupRetainedJob({ receipt, namespace: receipt.namespace, output,
        kubectl: async args => {
          calls.push(args);
          if (args.includes(failure)) throw new Error(`simulated ${failure} failure`);
          if (args[0] === 'get') return JSON.stringify({ metadata: { ...metadata(args[1]!.split('/')[1]!),
            ...(failure === 'revalidation' && copied ? { uid: 'aaaaaaaa-2222-3333-4444-555555555555' } : {}) } });
          if (args.includes('/artifacts/report.json')) return JSON.stringify(report('retained'));
          if (args.includes('sh')) return 'done';
          if (args[0] === 'cp') {
            fs.writeFileSync(path.join(output, 'report.json'), JSON.stringify(report('done')));
            copied = true;
          }
          return '';
        } }));
      assert.equal(calls.some(call => call[0] === 'delete'), false);
      if (failure !== 'SIGUSR1') assert.equal(calls.some(call => call.includes('SIGUSR1')), false);
    } finally { fs.rmSync(output, { recursive: true, force: true }); }
  }
});

test('already-completed deadline cleanup skips TERM but still verifies evidence before releasing the holder', async () => {
  const output = path.resolve(`.temp/retained-job-completed-${process.pid}`);
  fs.rmSync(output, { recursive: true, force: true });
  const calls: string[][] = [];
  try {
    await cleanupRetainedJob({ receipt, namespace: receipt.namespace, output,
      kubectl: async args => {
        calls.push(args);
        if (args[0] === 'get') return JSON.stringify({ metadata: metadata(args[1]!.split('/')[1]!) });
        if (args.includes('/artifacts/report.json')) return JSON.stringify(report('done'));
        if (args.includes('sh')) return 'done';
        if (args[0] === 'cp') fs.writeFileSync(path.join(output, 'report.json'), JSON.stringify(report('done')));
        return '';
      } });
    assert.equal(calls.some(call => call.includes('SIGTERM')), false);
    assert.ok(calls.findIndex(call => call.includes('SIGUSR1')) > calls.findIndex(call => call[0] === 'cp'));
    assert.equal(calls.at(-1)?.[0], 'delete');
  } finally { fs.rmSync(output, { recursive: true, force: true }); }
});

test('ownership mismatch, cleanup timeout and failed cleanup never delete objects', async () => {
  for (const failure of ['ownership', 'run', 'timeout', 'cleanup']) {
    const output = path.resolve(`.temp/retained-job-${failure}-${process.pid}`);
    fs.rmSync(output, { recursive: true, force: true });
    let time = 0;
    const calls: string[][] = [];
    try {
      await assert.rejects(cleanupRetainedJob({ receipt, namespace: receipt.namespace, output,
        now: () => time, sleep: async ms => { time += ms; },
        kubectl: async args => {
          calls.push(args);
          if (args[0] === 'get') return JSON.stringify({ metadata: { ...metadata(args[1]!.split('/')[1]!),
            ...(failure === 'ownership' ? { uid: 'aaaaaaaa-2222-3333-4444-555555555555' } : {}) } });
          if (args.includes('/artifacts/report.json')) return JSON.stringify({ ...report('retained'),
            ...(failure === 'run' ? { runId: 'another-run' } : {}) });
          if (args.includes('sh')) return failure === 'timeout' ? '' : 'done';
          if (args[0] === 'cp') fs.writeFileSync(path.join(output, 'report.json'), JSON.stringify(report('failed')));
          return '';
        } }), failure === 'ownership' ? /Ownership mismatch/ : failure === 'run' ? /report does not match/ :
        failure === 'timeout' ? /within 6m/ : /cleanup failed/);
      assert.equal(calls.some(call => call[0] === 'delete'), false);
      if (failure === 'ownership' || failure === 'run') assert.equal(calls.some(call => call.includes('SIGTERM')), false);
    } finally { fs.rmSync(output, { recursive: true, force: true }); }
  }
});
