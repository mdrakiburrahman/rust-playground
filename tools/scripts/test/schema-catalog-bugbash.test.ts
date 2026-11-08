import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { test } from 'node:test';
import { checkVerifiedReceipt, copyEvidence, copyEvolutionEvidence, createEvidenceDir, parseOptions, passedTests, positiveDfeSample, redactLog, selectPlatform,
  serviceImages, validateImage, type ImageIdentity } from '../src/schema-catalog-bugbash.js';
import { PUBLISHED_RUNTIME_IMAGE_PROJECTS } from '../src/image-plan.js';

const sha = 'a'.repeat(40), digest = `sha256:${'b'.repeat(64)}`;
const cloud = ['--subscription', '11111111-1111-1111-1111-111111111111',
  '--resource-group', 'rust-playground-schema-evo-qualification-20261108', '--env-file', '/private/base'];
const receipt = ['--verified-receipt', '/tmp/verify/receipt.json'];
const evidence = ['--evidence-dir', '/tmp/new-evidence'];

test('CLI separates verification, ready-only Azure and explicitly confirmed destructive reset', () => {
  assert.equal(parseOptions(['verify', '--expected-sha', sha, ...evidence]).mode, 'verify');
  assert.equal(parseOptions(['local', ...receipt, ...evidence]).mode, 'local');
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
  assert.equal(parseOptions(['reset', ...receipt, ...old, '--confirm-reset', old[3]!, ...evidence]).mode, 'reset');
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

test('OCI index selection distinguishes manifest and config identities and rejects wrong revisions', () => {
  const platform = `sha256:${'c'.repeat(64)}`;
  assert.equal(selectPlatform({ manifests: [
    { digest: platform, platform: { os: 'linux', architecture: 'amd64' } },
    { digest, platform: { os: 'unknown', architecture: 'unknown' } },
  ] }, digest), platform);
  assert.throws(() => selectPlatform({ manifests: [] }, digest), /exactly one/);
  const image: ImageIdentity = { project: 'arrowcol-catalog-rust', repository: 'ghcr.io/mdrakiburrahman/rust-playground/arrowcol-catalog-rust',
    alias: 'ghcr.io/mdrakiburrahman/rust-playground/arrowcol-catalog-rust:dev',
    reference: `ghcr.io/mdrakiburrahman/rust-playground/arrowcol-catalog-rust@${digest}`,
    configDigest: digest, platformDigest: platform, revision: sha };
  const docker = { Id: digest, Config: { Labels: { 'org.opencontainers.image.revision': sha } } };
  validateImage(docker, image, sha);
  assert.throws(() => validateImage({ ...docker, Id: platform }, image, sha), /Config mismatch/);
  assert.throws(() => validateImage(docker, image, 'd'.repeat(40)), /Revision mismatch/);
  const map = { 'arrowcol-catalog': { image: image.alias }, 'dataflow-1': { image: 'registry/dfe:dev' },
    router: { image: 'registry/router:dev' }, validator: { image: 'registry/router:dev' } };
  assert.equal(serviceImages({ services: map })['arrowcol-catalog'], image.alias);
  assert.throws(() => serviceImages({ services: { ...map, hive: { image: 'hive:dev' } } }), /removed/);
  assert.throws(() => serviceImages({ services: { ...map, router: {} } }), /Invalid/);
});

test('evidence excludes private inputs and symlinks; verification receipts reject failure and tampering', t => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'sc-receipt-'));
  t.after(() => fs.rmSync(root, { recursive: true }));
  const privateFile = path.join(root, '.env'); fs.writeFileSync(privateFile, 'PASSWORD=not-for-export');
  assert.throws(() => copyEvidence(privateFile, path.join(root, 'copy')), /Private/);
  const file = path.join(root, 'receipt.json');
  const value = { mode: 'verify', source: sha, stage: 'complete', exitCode: 0,
    images: PUBLISHED_RUNTIME_IMAGE_PROJECTS.map(project => ({ project })) };
  const write = (input: unknown) => {
    fs.writeFileSync(file, JSON.stringify(input));
    fs.writeFileSync(path.join(root, 'SHA256SUMS'),
      `${createHash('sha256').update(fs.readFileSync(file)).digest('hex')}  receipt.json\n`);
  };
  write(value); assert.equal(checkVerifiedReceipt(file).source, sha);
  fs.appendFileSync(file, ' '); assert.throws(() => checkVerifiedReceipt(file), /checksummed/);
  write({ ...value, exitCode: 1 }); assert.throws(() => checkVerifiedReceipt(file), /successful/);
  write({ ...value, images: value.images.slice(1) }); assert.throws(() => checkVerifiedReceipt(file));
  assert.equal(redactLog('SQL password: forbidden\nstage passed'), '[private diagnostic line withheld]\nstage passed');
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
