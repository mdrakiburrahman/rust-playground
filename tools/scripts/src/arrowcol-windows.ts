import assert from 'node:assert/strict';
import { spawn, spawnSync, type ChildProcess } from 'node:child_process';
import { createHash, randomBytes } from 'node:crypto';
import { once } from 'node:events';
import { copyFile, mkdir, mkdtemp, readFile, readdir, rm, utimes, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { pathToFileURL } from 'node:url';

const tag = 'arrowcol-windows-latest';
const releaseTitle = 'ArrowCol Windows x64 (rolling)';
const managed = '<!-- arrowcol-windows-runtime:v1 -->';
const ownedAsset = /^arrowcol-windows-x64-[0-9a-f]{40}\.zip$/;
const target = 'x86_64-pc-windows-msvc';
const executables = ['arrowcol-catalog-rust.exe', 'arrowcol-dataflow-rust.exe'] as const;
const configName = 'config.windows.yaml';
const payload = [...executables, configName] as const;
const manifestName = 'manifest.json';

function run(program: string, args: string[], env = process.env): string {
  const result = spawnSync(program, args, { encoding: 'utf8', env, timeout: 120_000, windowsHide: true });
  if (result.error) throw result.error;
  assert.equal(result.status, 0, `${program} ${args.join(' ')} failed: ${result.stderr}\n${result.stdout}`);
  return result.stdout.trim();
}

async function sha256(path: string): Promise<string> {
  return createHash('sha256').update(await readFile(path)).digest('hex');
}

function launch(program: string, args: string[], env: NodeJS.ProcessEnv) {
  const child = spawn(program, args, { env, windowsHide: true, stdio: ['pipe', 'inherit', 'inherit'] });
  const exit = new Promise<number | null>((resolve, reject) => {
    child.once('error', reject);
    child.once('close', resolve);
  });
  // Observe spawn failure even if it occurs before a readiness probe is run.
  void exit.catch(() => {});
  return { child, exit };
}

async function waitUntilReady(child: ChildProcess, probe: () => Promise<boolean>) {
  for (let i = 0; i < 75; i++) {
    assert.equal(child.exitCode, null, 'service exited before readiness');
    if (await probe()) return;
    await delay(500);
  }
  throw new Error('service did not become ready');
}

async function ready(url: string): Promise<boolean> {
  try {
    return (await fetch(url, { signal: AbortSignal.timeout(1000) })).ok;
  } catch (error) {
    if (error instanceof TypeError || (error instanceof DOMException && error.name === 'TimeoutError')) return false;
    throw error;
  }
}

function assertListenerOwner(child: ChildProcess, ports: number[]) {
  assert.ok(child.pid && child.exitCode === null, 'expected a running owned service');
  const listeners = run('netstat.exe', ['-ano', '-p', 'TCP']).split('\n')
    .map(line => line.trim().split(/\s+/))
    .filter(([protocol, , remote]) => protocol === 'TCP' && (remote === '0.0.0.0:0' || remote === '[::]:0'));
  for (const port of ports) {
    const matching = listeners.filter(([, address]) => address.endsWith(`:${port}`));
    assert.deepEqual(matching.map(([, address, , , pid]) => [address, Number(pid)]),
      [[`127.0.0.1:${port}`, child.pid]], `listener ${port} must belong only to the owned loopback service`);
  }
}

async function checkExit(exit: Promise<number | null>, service: string) {
  const timeout = new AbortController();
  try {
    const code = await Promise.race([exit, delay(85_000, undefined, { signal: timeout.signal })]);
    assert.equal(code, 0, `${service} did not drain and exit cleanly`);
  } finally {
    timeout.abort();
  }
}

async function smoke(build: string, archive: string, sourceSha: string) {
  const root = await mkdtemp(join(tmpdir(), 'arrowcol-windows-'));
  const processes: ChildProcess[] = [];
  try {
    const staging = join(root, 'staging');
    const extracted = join(root, 'extracted');
    const ddl = join(root, 'arrow-ddl');
    const warehouse = join(root, 'warehouse with spaces');
    const key = join(root, 'identity.key');
    const state = join(root, 'catalog');
    await Promise.all([staging, extracted, ddl, warehouse, state].map(path => mkdir(path)));
    await writeFile(key, randomBytes(32).toString('hex'), { flag: 'wx' });
    await writeFile(join(ddl, 'windows_probe.sql'),
      'CREATE TABLE arrowcol.fixture.windows_probe (id BIGINT NOT NULL) USING DELTA\n');
    const files = Object.fromEntries(await Promise.all(payload.map(async name => {
      const dest = join(staging, name);
      await copyFile(name === configName ? resolve('projects/arrowcol-dataflow-rust', name) : join(build, name), dest);
      return [name, await sha256(dest)];
    })));
    const manifest = { manifestVersion: 1, sourceSha, target, compatibility: 'Windows x64; static MSVC CRT', files };
    await writeFile(join(staging, manifestName), JSON.stringify(manifest, null, 2) + '\n');
    const archiveTime = new Date('2000-01-01T00:00:00Z');
    await Promise.all([...payload, manifestName].map(name => utimes(join(staging, name), archiveTime, archiveTime)));
    run('tar.exe', ['-a', '-cf', archive, '-C', staging, ...payload, manifestName]);
    run('tar.exe', ['-xf', archive, '-C', extracted]);
    assert.deepEqual((await readdir(extracted)).sort(), [...payload, manifestName].sort());
    assert.deepEqual(JSON.parse(await readFile(join(extracted, manifestName), 'utf8')), manifest);
    for (const name of payload) assert.equal(await sha256(join(extracted, name)), files[name]);

    const env = {
      ...process.env,
      ARROWCOL_CATALOG_DDL_AUTH_MODE: 'signed_identity',
      ARROWCOL_CATALOG_DDL_IDENTITY_SECRET_FILE: key,
      ARROWCOL_DELTA_ROOT: warehouse.replaceAll('\\', '/'),
      ARROWCOL_PARTITION_GRANULARITY: 'hour',
    };
    const catalog = launch(join(extracted, executables[0]), [
      '--listen', '127.0.0.1:8817', '--state-dir', state, '--ddl-dir', ddl,
      '--warehouse-root', pathToFileURL(warehouse).href, '--ingestion-layout', '--shutdown-on-stdin-eof',
    ], env);
    processes.push(catalog.child);
    await waitUntilReady(catalog.child, async () => spawnSync(join(extracted, executables[0]),
      ['--listen', '127.0.0.1:8817', '--healthcheck'], { env, timeout: 4000, windowsHide: true }).status === 0);
    assertListenerOwner(catalog.child, [8817]);
    const collector = launch(join(extracted, executables[1]), [
      '--config', join(extracted, configName),
    ], env);
    processes.push(collector.child);
    await waitUntilReady(collector.child, async () =>
      await ready('http://127.0.0.1:32311/readyz') && await ready('http://127.0.0.1:32312/api/v1/status'));
    assertListenerOwner(collector.child, [32310, 32311, 32312]);
    run(join(build, 'examples', 'windows-runtime-probe.exe'), [], env);
    const drain = await fetch('http://127.0.0.1:32312/api/v1/groups/shutdown?wait=false&timeout_secs=60', {
      method: 'POST', signal: AbortSignal.timeout(5000),
    });
    assert.equal(drain.status, 202, await drain.text());
    await checkExit(collector.exit, 'collector');
    catalog.child.stdin?.end();
    await checkExit(catalog.exit, 'catalog');

    const table = join(warehouse, 'arrowcol', 'fixture', 'windows_probe');
    const log = join(table, '_delta_log');
    const entries = await readdir(log);
    const actions = (await Promise.all(entries.filter(name => /^\d{20}\.json$/.test(name))
      .map(name => readFile(join(log, name), 'utf8')))).flatMap(commit =>
      commit.trim().split('\n').map(line => JSON.parse(line) as { add?: { path: string; stats?: string } }));
    const added = actions.flatMap(action => action.add ? [action.add] : []);
    assert.equal(added.length, 1, 'one committed Delta add expected');
    const { path, stats } = added[0];
    assert.ok(stats, 'Delta add does not contain statistics');
    const { numRecords, minValues, maxValues } = JSON.parse(stats);
    assert.equal(numRecords, 2);
    assert.equal(minValues.id, 41);
    assert.equal(maxValues.id, 42);
    assert.equal((await readFile(join(table, decodeURIComponent(path)))).subarray(0, 4).toString(), 'PAR1');
    console.log('Signed DDL accepted, unsigned DDL rejected, and Flight rows committed to Delta after ordered graceful drain');
    console.log(`${archive}: sha256:${await sha256(archive)}`);
  } finally {
    for (const child of processes.reverse()) {
      if (child.exitCode === null && child.signalCode === null && child.pid) {
        child.kill();
        await once(child, 'close');
      }
    }
    await rm(root, { recursive: true, force: true });
  }
}

interface Asset { id: number; name: string; state: string; digest: string | null; browser_download_url: string }
interface Release { id: number; tag_name: string; name: string; body: string | null; draft: boolean }

function publishRelease(repo: string, archive: string, name: string, sourceSha: string, digest: string) {
  const releases = `repos/${repo}/releases`;
  const gh = (...args: string[]) => run('gh', args);
  const find = () => (JSON.parse(gh('api', '--paginate', '--slurp', `${releases}?per_page=100`)) as Release[][])
    .flat().find(release => release.tag_name === tag);
  const note = (sha: string) => `${managed}\nCurrent asset: \`${name}\`\n\nSource: \`${sourceSha}\`\n\nZIP SHA-256: \`${sha}\`\n`;
  let release = find();
  if (!release) {
    gh('release', 'create', tag, '--repo', repo, '--target', sourceSha, '--title', releaseTitle,
      '--notes', note(digest), '--draft', '--latest=false');
    release = find();
  }
  assert.ok(release?.name === releaseTitle && release.body?.startsWith(`${managed}\n`),
    `Refusing to modify an unmanaged ${tag} release`);
  const id = release.id;
  const assets = () => (JSON.parse(gh('api', '--paginate', '--slurp', `${releases}/${id}/assets?per_page=100`)) as Asset[][]).flat();
  const existing = assets().find(asset => asset.name === name);
  if (existing?.state === 'starter') gh('api', '--method', 'DELETE', `${releases}/assets/${existing.id}`);
  if (!existing || existing.state === 'starter') gh('release', 'upload', tag, archive, '--repo', repo);
  const all = assets();
  const current = all.find(asset => asset.name === name);
  assert.ok(current?.state === 'uploaded' && current.digest && /^sha256:[a-f0-9]{64}$/.test(current.digest),
    'GitHub did not confirm the candidate asset SHA-256; previous download was not removed');
  const approvedDigest = current.digest.slice('sha256:'.length);
  const alreadyPublished = !release.draft && release.body?.trimEnd() === note(approvedDigest).trimEnd();
  assert.ok(approvedDigest === digest || alreadyPublished,
    'Candidate ZIP differs from the GitHub asset; previous download was not removed');
  if (!alreadyPublished) {
    gh('release', 'edit', tag, '--repo', repo, '--notes', note(approvedDigest), '--draft=false', '--latest=false');
  }
  for (const old of all) {
    if (old.id !== current.id && ownedAsset.test(old.name)) gh('api', '--method', 'DELETE', `${releases}/assets/${old.id}`);
  }
  assert.deepEqual(assets().filter(asset => ownedAsset.test(asset.name)).map(asset => asset.id), [current.id]);
  console.log(`Published ${current.browser_download_url} (sha256:${approvedDigest})`);
}

async function main() {
  const [command, directory] = process.argv.slice(2);
  assert.ok(directory && (command === 'smoke' || command === 'publish'), 'Usage: arrowcol-windows.ts smoke|publish BUILD_DIRECTORY');
  assert.equal(process.platform, 'win32');
  const build = resolve(directory);
  const sourceSha = run('git', ['rev-parse', 'HEAD']);
  assert.match(sourceSha, /^[0-9a-f]{40}$/);
  if (process.env.GITHUB_SHA) assert.equal(sourceSha, process.env.GITHUB_SHA);
  const name = `arrowcol-windows-x64-${sourceSha}.zip`;
  const archive = join(build, name);
  if (command === 'smoke') {
    await smoke(build, archive, sourceSha);
  } else {
    const repo = process.env.GITHUB_REPOSITORY;
    assert.ok(process.env.GITHUB_ACTIONS === 'true' && repo === 'mdrakiburrahman/rust-playground',
      'Release writes are only supported by the upstream GitHub Actions workflow');
    publishRelease(repo, archive, name, sourceSha, await sha256(archive));
  }
}

await main();
