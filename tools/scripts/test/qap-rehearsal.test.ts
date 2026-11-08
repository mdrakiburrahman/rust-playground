import assert from 'node:assert/strict';
import { spawn, spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import fs from 'node:fs';
import path from 'node:path';
import test from 'node:test';
import { rehearsalAksArgs } from '../src/qap-rehearsal.js';

const script = path.resolve('tools/scripts/src/qap-rehearsal.ts');
const subscription = '11111111-2222-3333-4444-555555555555';
const sha = 'a'.repeat(40);
const job = 'azure-qap-20261107123456-1234abcd';

test('real CLI runs ordered rehearsal and rechecks both reports; failures stop immediately', { skip: process.platform === 'win32' }, async () => {
  const fixture = path.resolve(`qap-cli-fixture-${process.pid}`);
  const root = path.join(fixture, 'repo');
  const bin = path.join(fixture, 'bin');
  const output = path.join(fixture, 'evidence');
  const log = path.join(fixture, 'calls.jsonl');
  fs.mkdirSync(path.join(root, 'infra/azure'), { recursive: true });
  fs.mkdirSync(path.join(root, 'infra/kubernetes'), { recursive: true });
  fs.mkdirSync(bin);
  fs.writeFileSync(path.join(fixture, 'package.json'), '{"type":"commonjs"}');
  fs.writeFileSync(path.join(root, '.env'), 'private-test-value');
  fs.writeFileSync(path.join(root, 'infra/azure/stamp.yaml'), `subscription: ${subscription}\nstamp: 1\nresourceGroup: arrow-collector-rg\naks:\n  namespace: arrow-collector\n  systemPool:\n    vmSize: old\n`);
  fs.writeFileSync(path.join(root, 'infra/kubernetes/azure-qap.example.yaml'),
    'image: example\nresources:\n  requests:\n    cpu: 1\n    memory: 2Gi\n  limits:\n    cpu: 2\n    memory: 4Gi\n');
  const fake = `#!/usr/bin/env node
const fs = require('node:fs'), path = require('node:path');
const cmd = path.basename(process.argv[1]), args = process.argv.slice(2);
fs.appendFileSync(process.env.QAP_FAKE_LOG, JSON.stringify([cmd,...args])+'\\n');
if (process.env.QAP_FAIL && [cmd,...args].join(' ').includes(process.env.QAP_FAIL)) {
  if(args.includes('--kubernetes')) console.log('Kubernetes job: '+process.env.QAP_FAKE_JOB+' namespace: arrow-collector');
  console.error('private-test-value'); process.exit(19);
}
if (process.env.QAP_HANG && [cmd,...args].join(' ').includes(process.env.QAP_HANG)) {
  process.on('SIGTERM',()=>{ fs.writeFileSync(process.env.QAP_SIGNAL_FILE,'terminated'); process.exit(130); });
  setInterval(()=>{},1000); return;
}
if (cmd === 'git' && args[0] === 'rev-parse') console.log(process.env.QAP_FAKE_SHA);
if (cmd === 'az' && args[0] === 'account' && args[1] === 'show') console.log(JSON.stringify({id:process.env.QAP_FAKE_SUB}));
if (cmd === 'npx' && args.includes('azure-infra:provision')) fs.writeFileSync('.env.azure','generated');
if (cmd === 'npx' && args.includes('azure-qap-experiment:image')) console.log('Digest-pinned runtime image: '+args[args.indexOf('--registry')+1]+'.azurecr.io/azure-qap@sha256:'+(process.env.QAP_BAD_IMAGE ? 'bad' : '1'.repeat(64)));
if (cmd === 'npx' && args.includes('azure-qap-experiment:run')) {
  if(args.includes('--kubernetes')) console.log('Kubernetes job: '+process.env.QAP_FAKE_JOB+' namespace: arrow-collector');
  const dir=args[args.indexOf('--out')+1]; fs.mkdirSync(path.join(dir,'queries'),{recursive:true});
  const warm={files:2,bytes:20,requests:2,paths:['a.parquet','b.parquet']};
  const after={files:2,bytes:20,requests:2,paths:['a.parquet','b.parquet']};
  const trials=['optimize','purge','vacuum','reload'].map(operation=>({trial:operation,operation,status:'complete',telemetry:'complete',files:{unchanged:2},reads:{warmUp:warm,afterMutation:after,unchanged:operation==='reload'?after:{files:0,bytes:0,requests:0,paths:[]}}}));
  const retained=args.includes('--keep-running');
  const deadline='2026-11-07T20:00:00Z';
  fs.writeFileSync(path.join(dir,'report.json'),JSON.stringify({runId:'20261107t123456_abcd',trials,control:{status:'detected'},cleanup:{status:retained?'retained':'done',errors:[]},exitCode:0,...(retained?{inspection:{mode:'read-only',deadline}}:{})}));
  fs.writeFileSync(path.join(dir,'report.md'),'report');
  fs.writeFileSync(path.join(dir,'evidence.jsonl'),'{}');
  for(const t of trials) fs.writeFileSync(path.join(dir,'queries',t.trial+'.yaml'),'query');
  fs.mkdirSync(path.join(dir,'http-traces'));
  if(args.includes('--kubernetes')) fs.writeFileSync(path.join(dir,'kubernetes-job.json'),JSON.stringify({jobName:process.env.QAP_FAKE_JOB,namespace:'arrow-collector',...(retained?{version:1,mode:'read-only-inspection',context:null,pod:process.env.QAP_FAKE_JOB+'-abcde',podUid:process.env.QAP_FAKE_SUB,resources:Object.fromEntries(['job/'+process.env.QAP_FAKE_JOB,'configmap/'+process.env.QAP_FAKE_JOB+'-env','secret/'+process.env.QAP_FAKE_JOB+'-secret'].map(name=>[name,process.env.QAP_FAKE_SUB])),runId:'20261107t123456_abcd',inspectionDeadline:deadline}:{})}));
}
if (cmd === 'npx' && args.includes('experiments/azure-qap/src/read-logs.ts')) {
  const rows=['warm-up','after-mutation'].flatMap(phase=>['a.parquet','b.parquet'].map(path=>({phase,path,operation:'GetBlob',status:206,bytes:10,requests:1,last:'2026-01-01',userAgent:''})));
  if(process.env.QAP_BAD_RELOAD && args.at(-1).endsWith('reload.yaml')) rows.filter(r=>r.phase==='after-mutation').forEach(r=>{r.operation='GetBlobProperties';r.bytes=0});
  if(process.env.QAP_BAD_OPTIMIZE && args.at(-1).endsWith('optimize.yaml')) rows[0].bytes++;
  console.log(JSON.stringify(rows));
}
if (cmd === 'kubectl' && args.includes('-o') && args.includes('json')) {
  if(args[1].includes('/')) {
    console.log(JSON.stringify({metadata:{name:args[1].split('/')[1],namespace:'arrow-collector',uid:process.env.QAP_FAKE_SUB,labels:{'app.kubernetes.io/name':'azure-qap',run:process.env.QAP_FAKE_JOB},ownerReferences:[{kind:'Job',uid:process.env.QAP_FAKE_SUB}]}})); return;
  }
  const legacy=!args.includes('--selector');
  console.log(JSON.stringify({items:legacy && process.env.QAP_LEGACY_OBJECT && args.includes(process.env.QAP_LEGACY_OBJECT) ? [{metadata:{name:'azure-qap-unrelated-active'}}] : !legacy && process.env.QAP_POD ? [{}] : []}));
}
if (cmd === 'kubectl' && args.includes('job/'+process.env.QAP_FAKE_JOB) && process.env.QAP_JOB) console.log('job/'+process.env.QAP_FAKE_JOB);
`;
  for (const command of ['git', 'npm', 'npx', 'az', 'kubectl', 'kubelogin']) {
    fs.writeFileSync(path.join(bin, command), fake, { mode: 0o755 });
  }
  const env = { ...process.env, PATH: `${bin}:${process.env.PATH}`, QAP_FAKE_LOG: log, QAP_FAKE_SHA: sha,
    QAP_FAKE_SUB: subscription, QAP_FAKE_JOB: job };
  const cli = (args: string[], extra: NodeJS.ProcessEnv = {}) => spawnSync(process.execPath, ['--import', 'tsx', script, ...args],
    { cwd: root, env: { ...env, ...extra }, encoding: 'utf8', timeout: 60_000 });
  const args = ['run', '--subscription', subscription, '--expected-sha', sha, '--evidence-dir', output];
  try {
    const missing = cli(args, { PATH: fixture });
    assert.notEqual(missing.status, 0);
    assert.match(missing.stderr, /git failed \(unable to start: ENOENT\)/);
    assert.equal(JSON.parse(fs.readFileSync(path.join(output, 'receipt.json'), 'utf8')).stage, 'preflight');
    fs.rmSync(output, { recursive: true });
    const first = cli(args, { QAP_FAIL: 'azure-infra:provision' });
    assert.notEqual(first.status, 0);
    assert.doesNotMatch(first.stderr, /private-test-value/);
    assert.ok(fs.existsSync(log), first.stderr);
    let calls = fs.readFileSync(log, 'utf8').trim().split('\n').map(line => JSON.parse(line) as string[]);
    assert.equal(calls.filter(call => call.includes('azure-infra:provision')).length, 1);
    assert.equal(calls.some(call => call.includes('azure-qap-experiment:image')), false);
    assert.equal(JSON.parse(fs.readFileSync(path.join(output, 'receipt.json'), 'utf8')).stage, 'provision-1');
    fs.rmSync(output, { recursive: true });
    fs.rmSync(log);
    fs.writeFileSync(path.join(root, 'infra/azure/stamp.yaml'), `subscription: ${subscription}\nstamp: 1\nresourceGroup: arrow-collector-rg\naks:\n  namespace: arrow-collector\n  systemPool:\n    vmSize: old\n`);
    const marker = path.join(fixture, 'signal');
    const running = spawn(process.execPath, ['--import', 'tsx', script, ...args], {
      cwd: root, env: { ...env, QAP_HANG: 'azure-infra:provision', QAP_SIGNAL_FILE: marker }, stdio: 'ignore',
    });
    const exited = new Promise<number | null>(resolve => running.on('close', resolve));
    const deadline = Date.now() + 10_000;
    while ((!fs.existsSync(log) || !fs.readFileSync(log, 'utf8').includes('azure-infra:provision')) && Date.now() < deadline)
      await new Promise(resolve => setTimeout(resolve, 50));
    assert.ok(fs.existsSync(log) && fs.readFileSync(log, 'utf8').includes('azure-infra:provision'));
    await new Promise(resolve => setTimeout(resolve, 100));
    running.kill('SIGTERM');
    const code = await Promise.race([exited, new Promise<never>((_, reject) => {
      setTimeout(() => reject(new Error('Cancellation timed out')), 10_000).unref();
    })]);
    assert.equal(code, 130);
    assert.equal(fs.readFileSync(marker, 'utf8'), 'terminated');
    assert.equal(JSON.parse(fs.readFileSync(path.join(output, 'receipt.json'), 'utf8')).stage, 'provision-1');
    assert.equal(JSON.parse(fs.readFileSync(path.join(output, 'receipt.json'), 'utf8')).exitCode, 130);
    fs.rmSync(output, { recursive: true });
    fs.rmSync(log);
    fs.writeFileSync(path.join(root, 'infra/azure/stamp.yaml'), `subscription: ${subscription}\nstamp: 1\nresourceGroup: arrow-collector-rg\naks:\n  namespace: arrow-collector\n  systemPool:\n    vmSize: old\n`);
    const badImage = cli(args, { QAP_BAD_IMAGE: '1' });
    assert.notEqual(badImage.status, 0);
    assert.equal(JSON.parse(fs.readFileSync(path.join(output, 'receipt.json'), 'utf8')).stage, 'image');
    assert.equal(fs.readFileSync(log, 'utf8').includes('azure-qap-experiment:run'), false);
    fs.rmSync(output, { recursive: true });
    fs.rmSync(log);
    fs.rmSync(path.join(root, '.env.azure'));
    fs.writeFileSync(path.join(root, 'infra/azure/stamp.yaml'), `subscription: ${subscription}\nstamp: 1\nresourceGroup: arrow-collector-rg\naks:\n  namespace: arrow-collector\n  systemPool:\n    vmSize: old\n`);
    const failedAks = cli(args, { QAP_FAIL: '--kubernetes' });
    assert.notEqual(failedAks.status, 0);
    const failedReceipt = JSON.parse(fs.readFileSync(path.join(output, 'receipt.json'), 'utf8'));
    assert.equal(failedReceipt.stage, 'aks');
    assert.equal(failedReceipt.kubernetesJob, job);
    assert.doesNotMatch(failedAks.stderr, /private-test-value/);
    fs.rmSync(output, { recursive: true });
    fs.rmSync(log);
    fs.rmSync(path.join(root, '.env.azure'));
    fs.rmSync(path.join(root, '.temp'), { recursive: true });
    fs.writeFileSync(path.join(root, 'infra/azure/stamp.yaml'), `subscription: ${subscription}\nstamp: 1\nresourceGroup: arrow-collector-rg\naks:\n  namespace: arrow-collector\n  systemPool:\n    vmSize: old\n`);
    const success = cli(args);
    assert.equal(success.status, 0, success.stderr);
    calls = fs.readFileSync(log, 'utf8').trim().split('\n').map(line => JSON.parse(line) as string[]);
    assert.equal(calls.filter(call => call.includes('azure-infra:provision')).length, 2);
    assert.equal(calls.filter(call => call.includes('azure-qap-experiment:run')).length, 2);
    assert.equal(calls.filter(call => call.includes('experiments/azure-qap/src/read-logs.ts')).length, 8);
    const provisions = calls.filter(call => call.includes('azure-infra:provision'));
    assert.deepEqual(provisions[0], provisions[1]);
    assert.deepEqual(provisions[0].slice(-4), ['--stamp-config', 'infra/azure/stamp.yaml', '--wait-seconds', '600']);
    const runs = calls.filter(call => call.includes('azure-qap-experiment:run'));
    assert.ok(!runs[0].includes('--kubernetes') && runs[1].includes('--kubernetes'));
    assert.ok(runs.every(call => call.includes('optimize,purge,vacuum,reload') && call.includes('15m')));
    const index = (name: string) => calls.findIndex(call => call.includes(name));
    assert.ok(index('ci') < index('azure-infra:provision'));
    assert.ok(index('azure-infra:provision') < index('azure-qap-experiment:image'));
    assert.ok(index('azure-qap-experiment:image') < index('azure-qap-experiment:run'));
    assert.ok(index('azure-qap-experiment:run') < index('experiments/azure-qap/src/read-logs.ts'));
    const stamp = fs.readFileSync(path.join(root, 'infra/azure/stamp.yaml'), 'utf8');
    assert.match(stamp, /vmSize: Standard_D8s_v5/);
    assert.match(stamp, /storageReadLogDestination: storage/);
    assert.match(fs.readFileSync(path.join(root, 'infra/kubernetes/azure-qap.yaml'), 'utf8'), /@sha256:1{64}/);
    const receipt = fs.readFileSync(path.join(output, 'receipt.json'));
    assert.equal(JSON.parse(receipt.toString()).stage, 'complete');
    assert.equal(JSON.parse(receipt.toString()).evidence.jobName, job);
    assert.equal(JSON.parse(receipt.toString()).kubernetesJob, job);
    const digest = createHash('sha256').update(fs.readFileSync(path.join(output, 'evidence.tar'))).digest('hex');
    const receiptHash = createHash('sha256').update(receipt).digest('hex');
    assert.equal(fs.readFileSync(path.join(output, 'SHA256SUMS'), 'utf8'),
      `${digest}  evidence.tar\n${receiptHash}  receipt.json\n`);
    const collectArgs = ['collect', '--subscription', subscription, '--evidence-dir'];
    const invalidPath = cli([...collectArgs, path.join(root, 'evidence')]);
    assert.notEqual(invalidPath.status, 0);
    assert.match(invalidPath.stderr, /outside the checkout/);
    const queries = path.join(root, '.temp/experiments/azure-qap/aks/queries');
    fs.renameSync(queries, `${queries}-actual`);
    fs.symlinkSync(`${queries}-actual`, queries);
    const symlinked = cli([...collectArgs, `${output}-symlink`]);
    assert.notEqual(symlinked.status, 0);
    fs.unlinkSync(queries);
    fs.renameSync(`${queries}-actual`, queries);
    const badReload = cli([...collectArgs, `${output}-reload`], { QAP_BAD_RELOAD: '1' });
    assert.notEqual(badReload.status, 0);
    assert.match(badReload.stderr, /Read recheck mismatch for reload\/after-mutation/);
    const badOptimize = cli([...collectArgs, `${output}-optimize`], { QAP_BAD_OPTIMIZE: '1' });
    assert.notEqual(badOptimize.status, 0);
    assert.match(badOptimize.stderr, /Read recheck mismatch for optimize\/warm-up/);
    const sidecar = path.join(root, '.temp/experiments/azure-qap/aks/kubernetes-job.json');
    const sidecarData = fs.readFileSync(sidecar);
    const invalidLegacy = cli([...collectArgs, `${output}-legacy-invalid`, '--legacy-cleanup-inventory']);
    assert.notEqual(invalidLegacy.status, 0);
    assert.match(invalidLegacy.stderr, /only for reports without/);
    fs.unlinkSync(sidecar);
    const legacy = cli([...collectArgs, `${output}-legacy`, '--legacy-cleanup-inventory']);
    assert.equal(legacy.status, 0, legacy.stderr);
    assert.equal(JSON.parse(fs.readFileSync(path.join(`${output}-legacy`, 'receipt.json'), 'utf8')).evidence.cleanupVerification, 'legacy-prefix-inventory');
    for (const resource of ['jobs', 'pods', 'configmaps', 'secrets']) {
      const blocked = cli([...collectArgs, `${output}-legacy-${resource}`, '--legacy-cleanup-inventory'], { QAP_LEGACY_OBJECT: resource });
      assert.notEqual(blocked.status, 0);
      assert.match(blocked.stderr, /inventory contains an azure-qap object/);
    }
    fs.writeFileSync(sidecar, sidecarData);
    const lingering = cli([...collectArgs, `${output}-pod`, '--job-name', job], { QAP_POD: '1' });
    assert.notEqual(lingering.status, 0);
    assert.match(lingering.stderr, /pods remain/);
    const lingeringJob = cli([...collectArgs, `${output}-job`], { QAP_JOB: '1' });
    assert.notEqual(lingeringJob.status, 0);
    assert.match(lingeringJob.stderr, /object still exists/);
    const report = path.join(root, '.temp/experiments/azure-qap/aks/report.json');
    const value = JSON.parse(fs.readFileSync(report, 'utf8'));
    value.control.status = 'not-detected';
    fs.writeFileSync(report, JSON.stringify(value));
    const failedCollect = cli(['collect', '--subscription', subscription, '--evidence-dir', `${output}-failed`]);
    assert.notEqual(failedCollect.status, 0);
    assert.notEqual(JSON.parse(fs.readFileSync(path.join(`${output}-failed`, 'receipt.json'), 'utf8')).stage, 'complete');
    fs.rmSync(path.join(root, '.env.azure'));
    fs.rmSync(path.join(root, '.temp'), { recursive: true });
    fs.rmSync(log);
    const retained = cli(['run', '--subscription', subscription, '--expected-sha', sha,
      '--evidence-dir', `${output}-retained`, '--keep-running', '--inspection-duration', '10m']);
    assert.equal(retained.status, 0, retained.stderr);
    const retainedReceipt = JSON.parse(fs.readFileSync(path.join(`${output}-retained`, 'receipt.json'), 'utf8'));
    assert.equal(retainedReceipt.evidence.cleanupVerification, 'retained-read-only-not-cleaned');
    assert.equal(retainedReceipt.retention.cleanup, 'not-yet-verified');
    assert.match(retainedReceipt.retention.cleanupCommand, /cleanup --receipt .*aks.*kubernetes-job.json/);
    calls = fs.readFileSync(log, 'utf8').trim().split('\n').map(line => JSON.parse(line) as string[]);
    const retainedRuns = calls.filter(call => call.includes('azure-qap-experiment:run'));
    assert.equal(retainedRuns[0].includes('--keep-running'), false);
    assert.equal(retainedRuns[1].includes('--keep-running'), true);
    assert.ok(retainedRuns[1].includes('10m'));
    assert.equal(calls.some(call => call.includes('--ignore-not-found=true')), false);
    assert.match(retained.stdout, /Cleanup verification intentionally skipped/);
  } finally {
    fs.rmSync(fixture, { recursive: true, force: true });
  }
});

test('rehearsal forwards inspection only to the AKS phase and leaves default arguments unchanged', () => {
  const common = ['--operations', 'optimize,purge,vacuum,reload'];
  assert.deepEqual(rehearsalAksArgs(common, false), common);
  assert.deepEqual(rehearsalAksArgs(common, true, '10m'), [...common, '--keep-running', '--inspection-duration', '10m']);
});
