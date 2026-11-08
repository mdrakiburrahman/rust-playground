import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { test } from 'node:test';
import { azureAccountCommand, checkResetReceipt, checkVerifiedReceipt, copyEvidence, copyEvolutionEvidence, createEvidenceDir, derivedCloudInput,
  loadtestImageProjects, parseOptions, passedTests, positiveDfeSample, redactLog, resolveServiceImages, serviceImages, validateCiRuns,
  validateAzureAccount, validateComposeImageCoverage, validateImage, verifyAzureAccount, type ImageIdentity } from '../src/schema-catalog-bugbash.js';

const sha = 'a'.repeat(40), digest = `sha256:${'b'.repeat(64)}`;
const cloud = ['--subscription', '11111111-1111-1111-1111-111111111111',
  '--resource-group', 'rust-playground-schema-evo-qualification-20261108', '--env-file', '/private/base'];
const receipt = ['--verified-receipt', '/tmp/verify/receipt.json'];
const evidence = ['--evidence-dir', '/tmp/new-evidence'];
const imageProjects = loadtestImageProjects({ targets: { build: { dependsOn: [
  'reader-build', { projects: ['arrowcol-catalog-rust', 'arrowcol-dataflow-rust', 'arrowcol-router-rust',
    'arrow-tenant-stream', 'delta-maintenance', 'kusto-explorer'], target: 'image' },
] } } });

test('Azure account lookup is scoped to the approved subscription and rejects an unavailable target', async () => {
  const target = cloud[1]!;
  assert.deepEqual(azureAccountCommand(target), ['az', 'account', 'show', '--subscription', target,
    '--query', '{id:id,state:state}', '--output', 'json']);
  validateAzureAccount({ id: target, state: 'Enabled' }, target);
  assert.throws(() => validateAzureAccount({ id: '22222222-2222-2222-2222-222222222222', state: 'Enabled' }, target), /mismatched/);
  assert.throws(() => validateAzureAccount({ id: target, state: 'Disabled' }, target), /disabled/);
  assert.throws(() => validateAzureAccount(null, target));
  assert.throws(() => azureAccountCommand(''), /UUID/);
  await verifyAzureAccount(target, async args => {
    assert.equal(args[args.indexOf('--subscription') + 1], target);
    return JSON.stringify({ id: target, state: 'Enabled' });
  });
  await assert.rejects(verifyAzureAccount(target, async () => { throw new Error('target unavailable'); }), /target unavailable/);
});

function imageIdentity(project: string): ImageIdentity {
  const repository = `ghcr.io/mdrakiburrahman/rust-playground/${project}`;
  return { project, repository, alias: `${repository}:dev`, configDigest: digest, revision: sha };
}

test('CLI separates verification, receipt-chained Azure and dedicated explicitly confirmed reset', () => {
  assert.equal(parseOptions(['verify', '--expected-sha', sha, ...evidence]).mode, 'verify');
  assert.equal(parseOptions(['verify', ...evidence]).expectedSha, undefined);
  assert.equal(parseOptions(['local', ...receipt, ...evidence]).mode, 'local');
  assert.equal(parseOptions(['azure', '--reset-receipt', '/tmp/reset/receipt.json', ...evidence]).resourceGroup, cloud[3]);
  assert.equal(parseOptions(['reset', ...receipt, '--confirm-reset', cloud[3]!, ...evidence]).resourceGroup, cloud[3]);
  assert.throws(() => parseOptions(['azure', '--reset-receipt', '/tmp/reset/receipt.json', ...receipt, ...evidence]), /reset-receipt/);
  assert.throws(() => parseOptions(['azure', '--reset-receipt', '/tmp/reset/receipt.json', '--ready-env', '/private/ready', ...evidence]), /override/);
  assert.equal(parseOptions(['azure', ...receipt, ...cloud, '--ready-env', '/private/ready', ...evidence]).mode, 'azure');
  assert.equal(parseOptions(['azure', ...receipt, ...cloud.slice(2), '--ready-env', '/private/ready', ...evidence]).subscription, undefined);
  assert.throws(() => parseOptions(['azure', ...receipt, '--subscription', 'not-a-uuid', ...cloud.slice(2),
    '--ready-env', '/private/ready', ...evidence]), /subscription/);
  assert.throws(() => parseOptions(['verify', '--expected-sha', sha.slice(0, 7), ...evidence]), /full SHA/);
  assert.throws(() => parseOptions(['local', ...receipt, ...cloud, ...evidence]), /Only azure/);
  assert.throws(() => parseOptions(['azure', ...receipt, ...cloud, '--ready-env', '/private/base', ...evidence]), /separate/);
  assert.throws(() => parseOptions(['reset', ...receipt, ...cloud, ...evidence]), /confirm-reset/);
  assert.throws(() => parseOptions(['reset', ...receipt, ...cloud, '--confirm-reset', 'wrong-group', ...evidence]), /confirm-reset/);
  assert.equal(parseOptions(['reset', ...receipt, ...cloud, '--confirm-reset', cloud[3]!, ...evidence]).mode, 'reset');
  const old = [...cloud]; old[3] = 'rust-playground-schema-evo';
  assert.throws(() => parseOptions(['azure', ...receipt, ...old, '--ready-env', '/private/ready', ...evidence]), /dedicated/);
  assert.throws(() => parseOptions(['reset', ...receipt, ...old, '--confirm-reset', old[3]!, ...evidence]), /dedicated/);
});

test('synced env projection excludes stamped identities and preserves only validated subscription/tags and selected group', () => {
  const values = { AZURE_SUBSCRIPTION_ID: cloud[1]!, AZURE_RESOURCE_GROUP: 'unrelated-stamped-group',
    AZURE_KUSTO_CLUSTER_TAGS: '{"NRMS.KustoPlatform.Classification.1P":"Corp"}', AZURE_STAMP: '4', AZURE_CLIENT_ID: 'ignored',
    ARROWCOL_RESET_GENERATION: 'stale', AZURE_STORAGE_ACCOUNT: 'old-account' };
  assert.deepEqual(derivedCloudInput(values, cloud[3]!), {
    AZURE_SUBSCRIPTION_ID: cloud[1], AZURE_RESOURCE_GROUP: cloud[3],
    AZURE_KUSTO_CLUSTER_TAGS: values.AZURE_KUSTO_CLUSTER_TAGS, ARROWCOL_AZURE_MODE: 'schema-evolution',
  });
  assert.throws(() => derivedCloudInput(values, 'rust-playground-schema-evo'), /dedicated/);
  assert.throws(() => derivedCloudInput({ ...values, AZURE_SUBSCRIPTION_ID: '' }, cloud[3]!), /subscription/);
  assert.throws(() => derivedCloudInput({ ...values, AZURE_KUSTO_CLUSTER_TAGS: 'not-json' }, cloud[3]!), /tags/);
});

test('loadtest image projects come from build dependencies and Compose :dev images are covered', () => {
  assert.deepEqual(imageProjects, ['arrow-tenant-stream', 'arrowcol-catalog-rust', 'arrowcol-dataflow-rust',
    'arrowcol-router-rust', 'delta-maintenance', 'kusto-explorer']);
  const repositories = Object.fromEntries(imageProjects.map(project =>
    [project, `ghcr.io/mdrakiburrahman/rust-playground/${project}`]));
  const services = { 'arrowcol-catalog': `${repositories['arrowcol-catalog-rust']}:dev`,
    'dataflow-1': `${repositories['arrowcol-dataflow-rust']}:dev`, router: `${repositories['arrowcol-router-rust']}:dev`,
    validator: `${repositories['arrowcol-router-rust']}:dev`, 'third-party': 'vendor/image:dev' };
  assert.doesNotThrow(() => validateComposeImageCoverage(services, imageProjects, repositories));
  assert.throws(() => validateComposeImageCoverage({ ...services,
    extra: 'ghcr.io/mdrakiburrahman/rust-playground/not-built:dev' }, imageProjects, repositories), /unbuilt/);
  assert.throws(() => validateComposeImageCoverage({ ...services, validator: 'vendor/router:dev' },
    imageProjects, repositories), /validator/);
  assert.throws(() => loadtestImageProjects({ targets: { build: { dependsOn: [
    { projects: ['duplicate', 'duplicate'], target: 'image' },
  ] } } }), /unique/);
});

test('CI gate requires every exact-head CI run to complete successfully', () => {
  const runs = [
    { workflowName: 'CI', headSha: sha, status: 'completed', conclusion: 'success', databaseId: 1 },
    { workflowName: 'CI', headSha: sha, status: 'completed', conclusion: 'success', databaseId: 2 },
    { workflowName: 'Publish runtime images', headSha: 'b'.repeat(40), status: 'queued', conclusion: null, databaseId: 3 },
  ];
  assert.equal(validateCiRuns(runs, sha).length, 2);
  assert.throws(() => validateCiRuns([], sha), /No CI/);
  assert.throws(() => validateCiRuns([...runs, { ...runs[0], status: 'in_progress' }], sha), /not completed successfully/);
  assert.throws(() => validateCiRuns([{ ...runs[0], headSha: 'b'.repeat(40) }], sha), /not completed successfully/);
});

test('reset custody rejects modified private inputs, stale generation, failed reset and wrong source/group', t => {
  if (process.platform === 'win32') { t.skip('Unix private-file ownership guard'); return; }
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'sc-reset-custody-'));
  t.after(() => fs.rmSync(root, { recursive: true }));
  const workspace = path.join(root, 'checkout'); fs.mkdirSync(workspace);
  const loadtestDir = path.join(workspace, 'projects', 'loadtest'); fs.mkdirSync(loadtestDir, { recursive: true });
  fs.writeFileSync(path.join(loadtestDir, 'project.json'), JSON.stringify({ targets: { build: { dependsOn: [
    { projects: imageProjects, target: 'image' },
  ] } } }));
  const verify = path.join(root, 'verify'); fs.mkdirSync(verify);
  const reset = path.join(root, 'reset'); fs.mkdirSync(reset);
  const writeReceipt = (directory: string, value: unknown) => {
    const file = path.join(directory, 'receipt.json');
    fs.writeFileSync(file, JSON.stringify(value));
    fs.writeFileSync(path.join(directory, 'SHA256SUMS'),
      `${createHash('sha256').update(fs.readFileSync(file)).digest('hex')}  receipt.json\n`);
    return file;
  };
  const verifiedReceipt = writeReceipt(verify, { mode: 'verify', source: sha, stage: 'complete', exitCode: 0,
    images: imageProjects.map(imageIdentity) });
  const generation = '22222222-2222-2222-2222-222222222222';
  const managedInputs = Object.fromEntries(['base', 'ready', 'manifest'].map(name => {
    const file = path.join(root, `${name}.private`);
    fs.writeFileSync(file, name === 'ready' ? `ARROWCOL_RESET_GENERATION=${generation}\n` : '{}', { mode: 0o600 });
    return [name, { path: file, sha256: createHash('sha256').update(fs.readFileSync(file)).digest('hex') }];
  }));
  const value = { mode: 'reset', source: sha, stage: 'complete', exitCode: 0, resourceGroup: cloud[3],
    verifiedReceipt, managedInputs, resetGeneration: generation,
    reset: { explicitlyConfirmedGroup: cloud[3], executed: true, provisioned: true } };
  let file = writeReceipt(reset, value);
  assert.equal(checkResetReceipt(file, workspace, cloud[3]!).source, sha);
  assert.throws(() => checkResetReceipt(file, workspace, 'rust-playground-schema-evo-other'), /successful/);
  file = writeReceipt(reset, { ...value, exitCode: 1 });
  assert.throws(() => checkResetReceipt(file, workspace, cloud[3]!), /successful/);
  file = writeReceipt(reset, { ...value, source: 'c'.repeat(40) });
  assert.throws(() => checkResetReceipt(file, workspace, cloud[3]!), /sources differ/);
  file = writeReceipt(reset, { ...value, resetGeneration: cloud[1] });
  assert.throws(() => checkResetReceipt(file, workspace, cloud[3]!), /generation changed/);
  file = writeReceipt(reset, value);
  fs.chmodSync(managedInputs.ready!.path, 0o644);
  assert.throws(() => checkResetReceipt(file, workspace, cloud[3]!), /0600/);
  fs.chmodSync(managedInputs.ready!.path, 0o600);
  fs.appendFileSync(managedInputs.ready!.path, '# modified\n');
  assert.throws(() => checkResetReceipt(file, workspace, cloud[3]!), /changed since provisioning/);
});

test('evidence paths reject checkout aliases, traversal and overwrite', t => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'sc-evidence-'));
  t.after(() => fs.rmSync(root, { recursive: true }));
  const workspace = path.join(root, 'workspace'); fs.mkdirSync(workspace);
  assert.throws(() => createEvidenceDir(path.join(workspace, 'evidence'), workspace), /outside/);
  assert.throws(() => createEvidenceDir(path.join(root, 'x') + '/../evidence', workspace), /traversal/);
  const output = createEvidenceDir(path.join(root, 'evidence'), workspace);
  assert.throws(() => createEvidenceDir(output, workspace));
  if (process.platform !== 'win32') {
    fs.symlinkSync(workspace, path.join(root, 'alias'));
    assert.throws(() => createEvidenceDir(path.join(root, 'alias', 'evidence'), workspace), /outside/);
  }
});

test('local image identity checks config digest and exact source revision', () => {
  const image = imageIdentity('arrowcol-catalog-rust');
  const docker = { Id: digest, Config: { Labels: { 'org.opencontainers.image.revision': sha } } };
  validateImage(docker, image, sha);
  assert.throws(() => validateImage({ ...docker, Id: `sha256:${'c'.repeat(64)}` }, image, sha), /Config mismatch/);
  assert.throws(() => validateImage(docker, image, 'd'.repeat(40)), /Revision mismatch/);
  const repositories = Object.fromEntries(imageProjects.map(project =>
    [project, `ghcr.io/mdrakiburrahman/rust-playground/${project}`]));
  const map = { 'arrowcol-catalog': { image: `${repositories['arrowcol-catalog-rust']}:dev` },
    'dataflow-1': { image: `${repositories['arrowcol-dataflow-rust']}:dev` },
    router: { image: `${repositories['arrowcol-router-rust']}:dev` },
    validator: { image: `${repositories['arrowcol-router-rust']}:dev` } };
  assert.equal(serviceImages({ services: map })['arrowcol-catalog'], image.alias);
  assert.throws(() => serviceImages({ services: { ...map, hive: { image: 'hive:dev' } } }), /removed/);
  assert.throws(() => serviceImages({ services: { ...map, router: {} } }), /Invalid/);
  const ref = `ghcr.io/google/cadvisor:v0.60.6@${digest}`;
  const seen: string[] = [];
  const inspect = (image: string) => { seen.push(image); return { Id: digest }; };
  assert.deepEqual(resolveServiceImages({ cadvisor: ref, catalog: image.alias }, inspect), {
    cadvisor: { image: ref, configId: digest }, catalog: { image: image.alias, configId: digest },
  });
  assert.deepEqual(seen, [ref, image.alias]);
  assert.throws(() => resolveServiceImages({ cadvisor: ref }, () => { throw new Error('image not found'); }), /image not found/);
  assert.throws(() => resolveServiceImages({ cadvisor: ref }, () => ({ Id: 'invalid' })), /Invalid cadvisor config ID/);
  assert.deepEqual(resolveServiceImages({ cadvisor: ref, catalog: image.alias }, inspect, new Set([image.alias])), {
    cadvisor: { image: ref, status: 'not-inspected-before-compose' }, catalog: { image: image.alias, configId: digest },
  });
});

test('evidence excludes private inputs and symlinks; verification receipts reject failure and tampering', t => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'sc-receipt-'));
  t.after(() => fs.rmSync(root, { recursive: true }));
  const privateFile = path.join(root, '.env'); fs.writeFileSync(privateFile, 'PASSWORD=not-for-export');
  assert.throws(() => copyEvidence(privateFile, path.join(root, 'copy')), /Private/);
  const file = path.join(root, 'receipt.json');
  const value = { mode: 'verify', source: sha, stage: 'complete', exitCode: 0,
    images: imageProjects.map(imageIdentity) };
  const write = (input: unknown) => {
    fs.writeFileSync(file, JSON.stringify(input));
    fs.writeFileSync(path.join(root, 'SHA256SUMS'),
      `${createHash('sha256').update(fs.readFileSync(file)).digest('hex')}  receipt.json\n`);
  };
  write(value); assert.equal(checkVerifiedReceipt(file, imageProjects).source, sha);
  fs.appendFileSync(file, ' '); assert.throws(() => checkVerifiedReceipt(file, imageProjects), /checksummed/);
  write({ ...value, exitCode: 1 }); assert.throws(() => checkVerifiedReceipt(file, imageProjects), /successful/);
  write({ ...value, images: value.images.slice(1) }); assert.throws(() => checkVerifiedReceipt(file, imageProjects));
  write({ ...value, images: value.images.map(image => ({ ...image, revision: 'c'.repeat(40) })) });
  assert.throws(() => checkVerifiedReceipt(file, imageProjects), /revision mismatch/);
  const trigger = ['pass', 'word'].join('');
  assert.equal(redactLog('SQL ' + trigger + ': harmless\nstage passed'), '[private diagnostic line withheld]\nstage passed');
  if (process.platform !== 'win32') {
    fs.symlinkSync(privateFile, path.join(root, 'link'));
    assert.throws(() => copyEvidence(path.join(root, 'link'), path.join(root, 'copy')), /unsafe/);
  }
});

test('acceptance receipts count only real completed tests and export bounded evolution evidence, not runtime state', t => {
  assert.deepEqual(passedTests('test service::tests::warm_cache ... ok\nrunning 0 tests\nok 3 - skipped # SKIP\n✔ actual node assertion (1.23ms)'),
    ['service::tests::warm_cache', 'actual node assertion']);
  assert.deepEqual(passedTests('test required ... FAILED\n﹣ unavailable (1ms)\n'), []);
  assert.equal(positiveDfeSample({ service: 'dataflow-1', identity: { ok: false } }, digest), false);
  assert.equal(positiveDfeSample({ service: 'dataflow-1', container: { ok: false } }, digest), false);
  const collector = { service: 'dataflow-1', container: { ok: true, value: { running: true, imageId: digest } },
    admin: { readiness: { ok: true, value: { ready: true, status: 200 } } } };
  assert.equal(positiveDfeSample(collector, digest), true);
  assert.equal(positiveDfeSample(collector, `sha256:${'f'.repeat(64)}`), false);
  assert.equal(positiveDfeSample({ ...collector, admin: { readiness: { ok: false } } }, digest), false);
  assert.throws(() => positiveDfeSample({ ...collector, admin: {} }, digest), /readiness/);
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'sc-evolution-export-'));
  t.after(() => fs.rmSync(root, { recursive: true }));
  const input = path.join(root, 'input'); fs.mkdirSync(input);
  for (const name of ['report.json', 'evidence.jsonl', 'catalog.log']) fs.writeFileSync(path.join(input, name), '{}');
  for (const name of ['catalog-state', 'warehouse', 'sdk-v1']) fs.mkdirSync(path.join(input, name));
  fs.writeFileSync(path.join(input, 'sdk-v1', 'sdk-manifest.json'), '{}');
  const output = path.join(root, 'output'); copyEvolutionEvidence(input, output);
  assert.deepEqual(fs.readdirSync(output).sort(), ['evidence.jsonl', 'report.json', 'sdk-v1']);
});

test('acceptance output ignores Nx task summaries but retains Node and Rust tests', () => {
  assert.deepEqual(passedTests('✔  nx run arrowcol-catalog:test (2s)\n✔ real Node test (1.2ms)\ntest actual_rust_test ... ok\n'),
    ['real Node test', 'actual_rust_test']);
  assert.deepEqual(passedTests('✔  nx run arrowcol-catalog:test\n'), []);
});

test('real wrong-head invocation fails before GitHub/build/Docker and archives a terminal failure receipt', {
  skip: process.platform !== 'linux' || !fs.existsSync('/.dockerenv') ? 'CLI execution requires the repository devcontainer' : false,
}, t => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'sc-failure-'));
  t.after(() => fs.rmSync(root, { recursive: true }));
  const output = path.join(root, 'evidence');
  const result = spawnSync(process.execPath, ['--import', import.meta.resolve('tsx'),
    'tools/scripts/src/schema-catalog-bugbash.ts', 'verify', '--expected-sha', '0'.repeat(40), '--evidence-dir', output],
  { encoding: 'utf8', timeout: 60_000 });
  assert.ifError(result.error);
  assert.equal(result.status, 1, result.stdout + result.stderr);
  const value = JSON.parse(fs.readFileSync(path.join(output, 'receipt.json'), 'utf8')) as {
    stage: string; exitCode: number; stages: { command: string[] }[];
  };
  assert.equal(value.stage, 'failed'); assert.equal(value.exitCode, 1);
  assert.deepEqual(value.stages.map(stage => stage.command[0]), ['git', 'git']);
  const sums = fs.readFileSync(path.join(output, 'SHA256SUMS'), 'utf8');
  for (const name of ['evidence.tar', 'receipt.json'])
    assert.ok(sums.includes(`${createHash('sha256').update(fs.readFileSync(path.join(output, name))).digest('hex')}  ${name}`));
  const bin = path.join(root, 'bin'); fs.mkdirSync(bin);
  fs.writeFileSync(path.join(bin, 'tar'), '#!/bin/sh\nexit 9\n', { mode: 0o700 });
  const failedOutput = path.join(root, 'failed-export');
  const failed = spawnSync(process.execPath, ['--import', import.meta.resolve('tsx'),
    'tools/scripts/src/schema-catalog-bugbash.ts', 'verify', '--expected-sha', '0'.repeat(40), '--evidence-dir', failedOutput],
  { encoding: 'utf8', timeout: 60_000, env: { ...process.env, PATH: `${bin}:${process.env.PATH}` } });
  assert.ifError(failed.error); assert.equal(failed.status, 1);
  const failedReceipt = JSON.parse(fs.readFileSync(path.join(failedOutput, 'receipt.json'), 'utf8')) as {
    stage: string; exitCode: number; exportError: string;
  };
  assert.equal(failedReceipt.stage, 'failed'); assert.equal(failedReceipt.exitCode, 1);
  assert.match(failedReceipt.exportError, /archive creation failed/);
  assert.equal(fs.readFileSync(path.join(failedOutput, 'SHA256SUMS'), 'utf8'),
    `${createHash('sha256').update(fs.readFileSync(path.join(failedOutput, 'receipt.json'))).digest('hex')}  receipt.json\n`);
});
