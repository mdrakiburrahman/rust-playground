import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import test from 'node:test';
import { ROOT, run_tree } from '../../infra/azure/bootstrap.js';
import { assertApplicationEvidence, captureInstalledExecutables, isolatedBuild,
  type ApplicationEvidence } from './aks-application-build.js';
import type { LocalApplicationBuild } from './aks-application-build.js';
import type { BuildOutput } from './aks-candidate-build.js';

const revision = 'a'.repeat(40), config = 'sha256:' + 'b'.repeat(64), platform = 'sha256:' + 'c'.repeat(64);
const path = '/usr/local/bin/arrowcol-dataflow-rust';
const invocation = ['docker', 'buildx', 'build', '--builder', 'arrowcol-application-test', '--load', '.'];
const invocationHash = createHash('sha256').update(JSON.stringify(invocation)).digest('hex');
const actualHash = 'd'.repeat(64);
function evidence() {
  const output: BuildOutput = { role: 'dataflow', repository: 'local/dataflow', digest: platform,
    media_type: 'application/vnd.oci.image.manifest.v1+json',
    platforms: [{ os: 'linux', architecture: 'amd64', digest: platform, config_digest: config }],
    application: { schema_version: 1, source_commit: revision, isolation: 'fresh_docker_container_builder',
      build_invocation_sha256: invocationHash,
      executables: [{ path, sha256: actualHash, platform_digest: platform, config_digest: config }] } };
  const expected: ApplicationEvidence = { schema_version: 1, source_commit: revision,
    roles: [{ role: 'dataflow', image_digest: platform, platform_digest: platform, config_digest: config,
      build_invocation_sha256: invocationHash, comparison: 'changed-code',
      executables: [{ path, sha256: actualHash, baseline_sha256: 'e'.repeat(64) }] }] };
  return { output, expected };
}

test('changed-code admission binds source, invocation, exact image platform and independently expected executable', () => {
  const { output, expected } = evidence();
  assert.equal(assertApplicationEvidence(output, expected, revision).comparison, 'changed-code');
  for (const mutate of [
    (e: ApplicationEvidence) => { e.source_commit = 'f'.repeat(40); },
    (e: ApplicationEvidence) => { e.roles[0]!.image_digest = config; },
    (e: ApplicationEvidence) => { e.roles[0]!.platform_digest = config; },
    (e: ApplicationEvidence) => { e.roles[0]!.config_digest = platform; },
    (e: ApplicationEvidence) => { e.roles[0]!.build_invocation_sha256 = 'f'.repeat(64); },
    (e: ApplicationEvidence) => { e.roles[0]!.executables[0]!.sha256 = 'f'.repeat(64); },
    (e: ApplicationEvidence) => { e.roles[0]!.executables[0]!.baseline_sha256 = actualHash; },
    (e: ApplicationEvidence) => { e.roles[0]!.executables = []; },
    (e: ApplicationEvidence) => { e.roles.push(e.roles[0]!); },
  ]) {
    const changed = structuredClone(expected); mutate(changed);
    assert.throws(() => assertApplicationEvidence(output, changed, revision));
  }
  const absent = structuredClone(output); delete absent.application;
  assert.throws(() => assertApplicationEvidence(absent, expected, revision), /binding/);
  assert.throws(() => assertApplicationEvidence({ ...output,
    application: { ...output.application, isolation: 'shared' } }, expected, revision), /isolated/);
  assert.throws(() => assertApplicationEvidence(output, { ...expected,
    roles: [{ ...expected.roles[0], comparison: ['changed-code'] }] }, revision), /binding/);
});

test('source-only edits allow unchanged executable bytes but cannot be mislabeled changed-code', () => {
  const { output, expected } = evidence();
  expected.roles[0]!.executables[0]!.baseline_sha256 = actualHash;
  assert.throws(() => assertApplicationEvidence(output, expected, revision), /identical/);
  expected.roles[0]!.comparison = 'source-only';
  assert.equal(assertApplicationEvidence(output, expected, revision).comparison, 'source-only');
});

test('isolation uses a fresh builder per build and removes only its builder after failure', async () => {
  const calls: string[][] = [];
  const runner: typeof run_tree = async args => {
    calls.push(args);
    return { returncode: args[2] === 'build' ? 1 : 0, stdout: '', stderr: 'deliberate failure' };
  };
  await assert.rejects(isolatedBuild(['docker', 'buildx', 'build', '--load', '.'], tmpdir(), runner), /failed/);
  const name = calls[0]![calls[0]!.indexOf('--name') + 1]!;
  assert.ok(name.startsWith('arrowcol-application-'));
  assert.equal(calls[1]![calls[1]!.indexOf('--builder') + 1], name);
  assert.deepEqual(calls[2], ['docker', 'buildx', 'rm', name]);
  for (const flag of ['--cache-from=x', '--builder=x', '--secret=x', '--ssh=x', '--build-context=x']) {
    await assert.rejects(isolatedBuild(['docker', 'buildx', 'build', flag, '.'], tmpdir(), runner), /fresh builder/);
  }
  assert.equal(calls.length, 3);
});

test('extraction uses the immutable platform config, never starts a container and retains installed byte digest', async () => {
  const calls: string[][] = [];
  const bytes = Buffer.from('installed fixture bytes'), id = 'f'.repeat(64);
  const runner: typeof run_tree = async args => {
    calls.push(args);
    if (args[1] === 'image') return { returncode: 0, stdout: JSON.stringify({ Id: config, Os: 'linux', Architecture: 'amd64' }), stderr: '' };
    if (args[1] === 'create') return { returncode: 0, stdout: id + '\n', stderr: '' };
    if (args[1] === 'cp') await writeFile(args[3]!, bytes);
    return { returncode: 0, stdout: '', stderr: '' };
  };
  const result = await captureInstalledExecutables('dataflow', { os: 'linux', architecture: 'amd64',
    digest: platform, config_digest: config }, invocation, revision, tmpdir(), runner);
  assert.equal(result.executables[0]!.sha256, createHash('sha256').update(bytes).digest('hex'));
  assert.ok(calls.every(args => !['run', 'start', 'exec'].includes(args[1]!)));
  assert.deepEqual(calls.at(-1), ['docker', 'rm', '--volumes', id]);
});

test('real isolated local OCI export and installed-file extraction, without registry or signing', {
  skip: process.env.ARROWCOL_APPLICATION_FIXTURE_PROOF !== '1',
}, async () => {
  const root = await mkdtemp(join(tmpdir(), 'arrowcol-application-fixture-'));
  const source = join(root, 'source'); await mkdir(source);
  const file = join(source, 'projects/arrowcol-dataflow-rust/Dockerfile');
  await mkdir(dirname(file), { recursive: true });
  await writeFile(file, 'FROM scratch\nCOPY marker /usr/local/bin/arrowcol-dataflow-rust\n');
  await writeFile(join(source, 'marker'), 'LOCAL FIXTURE ONLY\n');
  async function git(...args: string[]) {
    const result = await run_tree(['git', ...args], 30, source);
    assert.equal(result.returncode, 0, result.stderr); return result.stdout.trim();
  }
  let image: string | undefined;
  try {
    await git('init', '--quiet');
    await git('config', 'user.name', 'Local application fixture');
    await git('config', 'user.email', 'fixture@example.invalid');
    await git('remote', 'add', 'origin', 'https://github.com/mdrakiburrahman/rust-playground.git');
    await git('add', '.'); await git('commit', '--quiet', '-m', 'local-only fixture');
    const sourceCommit = await git('rev-parse', 'HEAD');
    const command = await run_tree(['npx', '--no-install', 'tsx',
      resolve(ROOT, 'projects/arrowcol-oat/aks-application-build.ts'), '--source-root', source,
      '--revision', sourceCommit, '--output', join(root, 'output'), '--roles', 'dataflow'], 120, ROOT);
    assert.equal(command.returncode, 0, command.stderr);
    const bundle: LocalApplicationBuild = JSON.parse(command.stdout);
    const output = bundle.builds[0]!.output;
    image = output.platforms[0]!.config_digest;
    assert.equal(output.application!.executables[0]!.sha256,
      createHash('sha256').update('LOCAL FIXTURE ONLY\n').digest('hex'));
    assert.equal(bundle.scope, 'local-application-proof');
    const retained = JSON.parse(await readFile(join(root, 'output/application-build.json'), 'utf8'));
    assert.equal(retained.builds[0].output.digest, output.digest);
  } finally {
    if (image) await run_tree(['docker', 'image', 'rm', image], 30, source);
    await rm(root, { recursive: true, force: true });
  }
});
