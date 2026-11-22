import assert from 'node:assert/strict';
import { createHash, generateKeyPairSync, randomUUID } from 'node:crypto';
import { mkdir, mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { pathToFileURL } from 'node:url';
import test from 'node:test';
import { run_tree } from '../../infra/azure/bootstrap.js';
import { imageBuilds } from './publish-images.js';
import { assertProducedBuildStatement, buildCandidateImages, verifyBuildEnvelope, writeCandidateImageBinding,
  type RegistryRead } from './aks-candidate-build.js';

async function command(args: string[], cwd: string): Promise<string> {
  const result = await run_tree(args, 120, cwd);
  assert.equal(result.returncode, 0, `${args[0]} ${args[1]} failed: ${result.stderr}`);
  return result.stdout.trim();
}
async function fixture() {
  const root = await mkdtemp(join(tmpdir(), 'arrowcol-build-test-'));
  const source = join(root, 'source'); await mkdir(source);
  for (const [, file, context] of imageBuilds) {
    await mkdir(dirname(join(source, file)), { recursive: true });
    await writeFile(join(source, file), 'FROM scratch\nCOPY marker /marker\n');
    await writeFile(join(source, context, 'marker'), 'immutable tiny fixture\n');
  }
  await writeFile(join(source, '.gitignore'), '.env\n');
  await writeFile(join(source, '.gitattributes'), '* export-ignore\n');
  await command(['git', 'init', '--quiet'], source);
  await command(['git', 'config', 'user.name', 'Offline fixture'], source);
  await command(['git', 'config', 'user.email', 'fixture@example.invalid'], source);
  await command(['git', 'remote', 'add', 'origin', pathToFileURL(source).href], source);
  const dependency = join(root, 'dependency'); await mkdir(dependency);
  await command(['git', 'init', '--quiet'], dependency);
  await command(['git', 'config', 'user.name', 'Offline fixture'], dependency);
  await command(['git', 'config', 'user.email', 'fixture@example.invalid'], dependency);
  await writeFile(join(dependency, 'pinned'), 'pinned dependency\n');
  await command(['git', 'add', '.'], dependency);
  await command(['git', 'commit', '--quiet', '-m', 'pinned dependency'], dependency);
  await command(['git', '-c', 'protocol.file.allow=always', 'submodule', 'add', '--quiet', dependency, 'modules/dependency'], source);
  await command(['git', 'add', '.'], source);
  await command(['git', 'commit', '--quiet', '-m', 'tiny fixture'], source);
  const revision = await command(['git', 'rev-parse', 'HEAD'], source);
  await writeFile(join(source, '.env'), 'TEST_ONLY_PRIVATE_VALUE=not-in-build-context\n');
  const keys = generateKeyPairSync('ed25519');
  const signingKey = join(root, 'test-only.key');
  await writeFile(signingKey, keys.privateKey.export({ type: 'pkcs8', format: 'pem' }), { mode: 0o600 });
  const publicKey = Buffer.from(keys.publicKey.export({ type: 'spki', format: 'pem' }));
  return { root, source, revision, signingKey, publicKey, uri: 'git+' + pathToFileURL(source).href,
    options: { sourceRoot: source, revision, registry: 'localhost:5001', signingKey,
      output: join(root, 'bundle.json'), scope: 'local-fixture' as const } };
}

test('producer signs actual output from immutable exact trees; ignores ignored inputs and export-ignore', async () => {
  const f = await fixture();
  const objects = new Map<string, Buffer>();
  let builds = 0;
  const bytes = (value: unknown) => {
    const b = Buffer.from(JSON.stringify(value)), id = 'sha256:' + createHash('sha256').update(b).digest('hex');
    objects.set(id, b); return id;
  };
  const runner: typeof run_tree = async (args, seconds, cwd, ...rest) => {
    if (args[0] !== 'docker') return run_tree(args, seconds, cwd, ...rest);
    builds++;
    const context = args.at(-1)!;
    assert.equal(await readFile(join(context, 'marker'), 'utf8'), 'immutable tiny fixture\n');
    await assert.rejects(readFile(join(context, '.env')));
    const config = bytes({ os: 'linux', architecture: 'amd64', ordinal: builds });
    const child = bytes({ mediaType: 'application/vnd.oci.image.manifest.v1+json', config: { digest: config }, layers: [] });
    const root = bytes({ mediaType: 'application/vnd.oci.image.index.v1+json', manifests: [
      { digest: child, platform: { os: 'linux', architecture: 'amd64' } }] });
    await writeFile(args[args.indexOf('--metadata-file') + 1]!, JSON.stringify({
      'containerimage.digest': root, 'containerimage.config.digest': config }));
    return { returncode: 0, stdout: '', stderr: '' };
  };
  const read: RegistryRead = async (_repository, _kind, id) => {
    const bytes = objects.get(id); assert.ok(bytes); return bytes;
  };
  try {
    const bundle = await buildCandidateImages(f.options, runner, read);
    assert.equal(builds, imageBuilds.length);
    for (const [index, envelope] of bundle.envelopes.entries()) {
      const output = bundle.outputs[index]!;
      const statement = verifyBuildEnvelope(envelope, f.publicKey);
      const materials = (statement.predicate as { materials: Array<{ path: string; digest: { sha1: string } }> }).materials;
      assert.equal(materials.length, 2);
      assert.equal(materials[1]!.path, 'modules/dependency');
      assert.equal(materials[1]!.digest.sha1, await command(['git', 'rev-parse', 'HEAD'], join(f.source, 'modules/dependency')));
      assertProducedBuildStatement(statement, output.digest, f.revision, output.repository, 'local-fixture', f.uri);
      assert.notEqual(output.digest, output.platforms[0]!.digest);
      assert.throws(() => assertProducedBuildStatement(statement, output.platforms[0]!.digest, f.revision,
        output.repository, 'local-fixture', f.uri), /final registry digest/);
      assert.throws(() => assertProducedBuildStatement(statement, output.digest, f.revision, output.repository), /scope/);
      const changed = structuredClone(envelope);
      changed.payload = Buffer.from(JSON.stringify({ ...statement, subject: [] })).toString('base64');
      assert.throws(() => verifyBuildEnvelope(changed, f.publicKey), /signature/);
      const other = generateKeyPairSync('ed25519').publicKey.export({ type: 'spki', format: 'pem' });
      assert.throws(() => verifyBuildEnvelope(envelope, Buffer.from(other)), /signature/);
    }
    await assert.rejects(writeCandidateImageBinding(bundle, join(f.root, 'absent-template'), join(f.root, 'binding')));
  } finally { await rm(f.root, { recursive: true, force: true }); }
});

test('producer refuses dirty source, revision mismatch and keys in candidate contexts before building', async () => {
  const f = await fixture();
  let builds = 0;
  const runner: typeof run_tree = async (args, ...rest) => {
    if (args[0] === 'docker') builds++;
    return run_tree(args, ...rest);
  };
  try {
    await assert.rejects(buildCandidateImages({ ...f.options, revision: 'f'.repeat(40) }, runner), /clean checkout/);
    await writeFile(join(f.source, 'modules/dependency', 'extra'), 'dirty submodule');
    await assert.rejects(buildCandidateImages(f.options, runner), /clean checkout/);
    await rm(join(f.source, 'modules/dependency', 'extra'));
    await writeFile(join(f.source, 'marker'), 'dirty');
    await assert.rejects(buildCandidateImages(f.options, runner), /clean checkout/);
    await assert.rejects(buildCandidateImages({ ...f.options, signingKey: join(f.source, '.env') }, runner), /outside candidate/);
    assert.equal(builds, 0);
  } finally { await rm(f.root, { recursive: true, force: true }); }
});

test('opt-in application producer signs fresh-builder invocation and extracted bytes, never supplied executable claims', async () => {
  const f = await fixture(), objects = new Map<string, Buffer>(), builders = new Set<string>(), calls: string[][] = [];
  let builds = 0, currentConfig = '';
  const bytes = (value: unknown) => {
    const content = Buffer.from(JSON.stringify(value)), id = 'sha256:' + createHash('sha256').update(content).digest('hex');
    objects.set(id, content); return id;
  };
  const runner: typeof run_tree = async (args, seconds, cwd, ...rest) => {
    if (args[0] !== 'docker') return run_tree(args, seconds, cwd, ...rest);
    calls.push(args);
    if (args[1] === 'buildx' && args[2] === 'create') builders.add(args[args.indexOf('--name') + 1]!);
    if (args[1] === 'buildx' && args[2] === 'build') {
      builds++;
      assert.ok(builders.has(args[args.indexOf('--builder') + 1]!));
      currentConfig = bytes({ os: 'linux', architecture: 'amd64', build: builds });
      const manifest = bytes({ mediaType: 'application/vnd.oci.image.manifest.v1+json',
        config: { digest: currentConfig }, layers: [] });
      await writeFile(args[args.indexOf('--metadata-file') + 1]!, JSON.stringify({
        'containerimage.digest': manifest, 'containerimage.config.digest': currentConfig }));
    }
    if (args[1] === 'image') return { returncode: 0, stdout: JSON.stringify({ Id: currentConfig, Os: 'linux', Architecture: 'amd64' }), stderr: '' };
    if (args[1] === 'create') return { returncode: 0, stdout: 'a'.repeat(64), stderr: '' };
    if (args[1] === 'cp') await writeFile(args[3]!, 'extracted image bytes:' + currentConfig);
    return { returncode: 0, stdout: '', stderr: '' };
  };
  const read: RegistryRead = async (_repository, _kind, id) => {
    const content = objects.get(id); assert.ok(content); return content;
  };
  try {
    const bundle = await buildCandidateImages({ ...f.options, application: true }, runner, read);
    assert.equal(builders.size, imageBuilds.length);
    assert.equal(calls.filter(args => args[1] === 'buildx' && args[2] === 'rm').length, imageBuilds.length);
    assert.ok(!calls.some(args => args.includes('--secret') || args.includes(f.signingKey)));
    for (const [index, output] of bundle.outputs.entries()) {
      const statement = verifyBuildEnvelope(bundle.envelopes[index], f.publicKey);
      assertProducedBuildStatement(statement, output.digest, f.revision, output.repository, 'local-fixture', f.uri);
      const invocation = (statement.predicate as { invocation: { parameters: { build_invocation: string[] } } }).invocation.parameters.build_invocation;
      assert.equal(output.application!.build_invocation_sha256,
        createHash('sha256').update(JSON.stringify(invocation)).digest('hex'));
      for (const executable of output.application!.executables) {
        assert.equal(executable.sha256, createHash('sha256').update('extracted image bytes:' + executable.config_digest).digest('hex'));
      }
    }
  } finally { await rm(f.root, { recursive: true, force: true }); }
});

test('immutable source admission ignores Git replacement refs and rejects a replacement-clean working tree', async () => {
  const f = await fixture();
  let builds = 0;
  try {
    await writeFile(join(f.source, 'marker'), 'replacement tree input\n');
    await command(['git', 'add', 'marker'], f.source);
    const tree = await command(['git', 'write-tree'], f.source);
    const replacement = await command(['git', 'commit-tree', tree, '-m', 'fixture replacement'], f.source);
    await command(['git', 'replace', f.revision, replacement], f.source);
    assert.equal(await command(['git', 'status', '--porcelain'], f.source), '');
    const runner: typeof run_tree = async (args, ...rest) => {
      if (args[0] === 'docker') builds++;
      return run_tree(args, ...rest);
    };
    await assert.rejects(buildCandidateImages(f.options, runner), /clean checkout/);
    assert.equal(builds, 0);
  } finally { await rm(f.root, { recursive: true, force: true }); }
});

test('real small-image publisher and verifier chain with a disposable signer and owned loopback registry', {
  skip: process.env.ARROWCOL_LOCAL_BUILD_PROOF !== '1',
}, async () => {
  const f = await fixture(), container = 'arrowcol-proof-' + randomUUID();
  const port = 20000 + Math.floor(Math.random() * 20000);
  const registry = 'localhost:' + port;
  const tags: string[] = [];
  try {
    await command(['docker', 'run', '--detach', '--name', container, '--publish', `127.0.0.1:${port}:5000`, 'registry:2.8.3'], f.root);
    let ready = false;
    for (let attempt = 0; attempt < 30; attempt++) {
      const r = await run_tree(['docker', 'exec', container, 'wget', '-q', '-O', '-', 'http://127.0.0.1:5000/v2/'], 10, f.root);
      if (!r.returncode) { ready = true; break; }
      await new Promise(resolve => setTimeout(resolve, 1000));
    }
    assert.ok(ready, 'Owned loopback registry did not become ready');
    const read: RegistryRead = async (repository, kind, id) => {
      const result = await run_tree(['docker', 'exec', container, 'wget', '-q', '-O', '-',
        '--header=Accept: application/vnd.oci.image.index.v1+json, application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json',
        'http://127.0.0.1:5000/v2/' + repository.slice(repository.indexOf('/') + 1) + '/' + kind + '/' + id], 30, f.root);
      assert.equal(result.returncode, 0, 'Registry raw content read failed');
      return Buffer.from(result.stdout);
    };
    const bundle = await buildCandidateImages({ ...f.options, registry }, run_tree, read);
    for (const [index, envelope] of bundle.envelopes.entries()) {
      const output = bundle.outputs[index]!;
      tags.push(output.repository + ':candidate-' + f.revision);
      const statement = verifyBuildEnvelope(envelope, f.publicKey);
      assertProducedBuildStatement(statement, output.digest, f.revision, output.repository, 'local-fixture', f.uri);
      assert.throws(() => assertProducedBuildStatement(statement, 'sha256:' + 'f'.repeat(64), f.revision,
        output.repository, 'local-fixture', f.uri), /digest/);
      assert.throws(() => assertProducedBuildStatement(statement, output.digest, f.revision, output.repository), /scope/);
    }
    const proof = { scope: 'LOCAL_FIXTURE_ONLY_NOT_DARWIN_IMAGES', real_builds: bundle.outputs.length, signature_verified: true,
      wrong_digest_rejected: true, production_scope_rejected: true, outputs: bundle.outputs };
    console.log('OFFLINE_BUILD_PROOF ' + JSON.stringify(proof));
    if (process.env.ARROWCOL_BUILD_PROOF_OUTPUT) await writeFile(process.env.ARROWCOL_BUILD_PROOF_OUTPUT, JSON.stringify(proof, null, 2) + '\n');
  } finally {
    await run_tree(['docker', 'rm', '--force', '--volumes', container], 30, f.root);
    for (const tag of tags) await run_tree(['docker', 'image', 'rm', tag], 30, f.root);
    await rm(f.root, { recursive: true, force: true });
  }
});
