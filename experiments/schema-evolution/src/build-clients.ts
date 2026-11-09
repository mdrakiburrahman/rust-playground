import fs from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';
import { runProcess } from '../../../tools/libs/experiment-kit/src/experiment.js';
import { object } from '../../azure-qap/src/kusto.ts';

const WORKSPACE = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../..');
const ROOT = path.join(WORKSPACE, '.temp/experiments/schema-evolution/clients');

function safeParents(filename: string): void {
  const relative = path.relative(WORKSPACE, filename);
  if (relative.startsWith('..') || path.isAbsolute(relative)) throw new Error('Client path escaped the workspace.');
  let parent = WORKSPACE;
  for (const segment of relative.split(path.sep)) {
    parent = path.join(parent, segment);
    if (fs.existsSync(parent) && fs.lstatSync(parent).isSymbolicLink())
      throw new Error('Client paths must not traverse symlinks.');
  }
}

function packageName(directory: string, version: number): string {
  safeParents(directory);
  const manifest = object(JSON.parse(fs.readFileSync(path.join(directory, 'sdk-manifest.json'), 'utf8')));
  const table = object(manifest.table), packages = object(manifest.packages), rust = object(packages.rust);
  if (table.catalog !== 'schema_evolution' || table.schema !== 'public' || table.table !== 'foo' ||
      table.version !== version || typeof table.fingerprint !== 'string' ||
      !/^sha256:[a-f0-9]{64}$/.test(table.fingerprint) || typeof rust.name !== 'string' ||
      !/^arrowcol-sdk-foo-[a-f0-9]{12}$/.test(rust.name) || rust.root !== 'rust')
    throw new Error(`Expected immutable foo SDK version ${version}, not a regenerated or unrelated package.`);
  return rust.name;
}

async function command(executable: string, args: string[], signal: AbortSignal): Promise<string> {
  const result = await runProcess(executable, args, {
    cwd: WORKSPACE, signal, env: { ...process.env, CARGO_TARGET_DIR: path.join(ROOT, 'target') },
  });
  if (result.exitCode !== 0)
    throw new Error(`${executable} ${args.join(' ')} failed: ${result.stderr || result.stdout}`);
  return result.stdout;
}

export async function buildClients(v1: string, v2: string, signal: AbortSignal): Promise<{ old: string; new: string }> {
  const directories = [path.resolve(WORKSPACE, v1), path.resolve(WORKSPACE, v2)];
  const names = directories.map((directory, index) => packageName(directory, index + 1));
  if (names[0] === names[1]) throw new Error('Old and new client SDK fingerprints are not distinct.');
  const old = await buildClient(v1, 1, signal);
  const current = await buildClient(v2, 2, signal);
  return { old, new: current };
}

export async function buildClient(generated: string, version: 1 | 2, signal: AbortSignal): Promise<string> {
    const directory = path.resolve(WORKSPACE, generated);
    const name = packageName(directory, version);
    const client = path.join(ROOT, `v${version}`);
    safeParents(client);
    fs.mkdirSync(path.join(client, 'src'), { recursive: true });
    fs.copyFileSync(path.join(WORKSPACE, 'experiments/schema-evolution/client.rs'), path.join(client, 'src/main.rs'));
    const manifest = [
      '[package]', `name = "schema-evolution-client-v${version}"`, 'version = "0.1.0"', 'edition = "2024"',
      '[workspace]', '[features]', 'v2 = []', `[dependencies.sdk]`, `package = ${JSON.stringify(name)}`,
      `path = ${JSON.stringify(path.join(directory, 'rust'))}`, '[dependencies]',
      'arrow-array = "=58.4.0"', 'arrow-schema = "=58.4.0"',
      'arrow-flight = { version = "=58.4.0", default-features = false }', 'futures = "=0.3.34"',
      'serde_json = "=1.0.154"', 'tonic = { version = "=0.14.6", features = ["transport"] }',
      'tokio = { version = "=1.53.2", features = ["macros", "rt-multi-thread"] }', '',
    ].join('\n');
    fs.writeFileSync(path.join(client, 'Cargo.toml'), manifest);
    const manifestArg = ['--manifest-path', path.join(client, 'Cargo.toml')];
    await command('cargo', ['generate-lockfile', ...manifestArg], signal);
    await command('cargo', ['build', '--locked', ...manifestArg, ...(version === 2 ? ['--features', 'v2'] : [])], signal);
    const binary = path.join(ROOT, 'target/debug', `schema-evolution-client-v${version}`);
    const description = object(JSON.parse((await command(binary, ['describe'], signal)).trim()));
    if (description.version !== version || description.client !== (version === 1 ? 'old' : 'new') ||
        !Array.isArray(description.fields) || description.fields.includes('bar') !== (version === 2))
      throw new Error('Compiled client does not preserve its historical generated SDK.');
    return binary;
}

export async function buildCatalogProof(signal: AbortSignal): Promise<string> {
  const client = path.join(ROOT, 'catalog-proof');
  safeParents(client);
  fs.mkdirSync(path.join(client, 'src'), { recursive: true });
  fs.copyFileSync(path.join(WORKSPACE, 'experiments/schema-evolution/catalog-proof.rs'), path.join(client, 'src/main.rs'));
  fs.copyFileSync(path.join(WORKSPACE, 'experiments/schema-evolution/inspect-parquet.rs'), path.join(client, 'src/inspect-parquet.rs'));
  fs.writeFileSync(path.join(client, 'Cargo.toml'), [
    '[package]', 'name = "schema-evolution-catalog-proof"', 'version = "0.1.0"', 'edition = "2024"',
    '[[bin]]', 'name = "schema-evolution-inspect-parquet"', 'path = "src/inspect-parquet.rs"',
    '[workspace]', '[dependencies.arrowcol-catalog-client]',
    `path = ${JSON.stringify(path.join(WORKSPACE, 'crates/arrowcol-catalog-client'))}`, '[dependencies]',
    'serde_json = "=1.0.154"', 'tokio = { version = "=1.53.2", features = ["macros", "rt-multi-thread"] }', '',
    'arrow-array = "=58.4.0"', 'parquet = { version = "=58.4.0", default-features = false, features = ["arrow", "snap", "zstd"] }', '',
    '[dev-dependencies]', 'tempfile = "=3.27.0"', '',
  ].join('\n'));
  const manifestArg = ['--manifest-path', path.join(client, 'Cargo.toml')];
  await command('cargo', ['generate-lockfile', ...manifestArg], signal);
  await command('cargo', ['build', '--locked', ...manifestArg], signal);
  await command('cargo', ['test', '--locked', ...manifestArg, '--bin', 'schema-evolution-inspect-parquet'], signal);
  return path.join(ROOT, 'target/debug/schema-evolution-catalog-proof');
}

async function buildFixtures(signal: AbortSignal): Promise<{ old: string; new: string }> {
  const base = '.temp/experiments/schema-evolution';
  const generate = async (args: string[]) => command('cargo', ['run', '--locked', '--quiet', '-p', 'arrowcol-sdk-gen', '--', ...args], signal);
  await generate(['fixture', '--ddl', 'experiments/schema-evolution/fixtures/create.sql', '--out', `${base}/catalog-v1`]);
  await generate(['fixture', '--ddl', 'experiments/schema-evolution/fixtures/alter.sql',
    '--previous', `${base}/catalog-v1`, '--out', `${base}/catalog-v2`]);
  for (const version of [1, 2])
    await generate(['generate', '--artifacts', `${base}/catalog-v${version}`, '--out', `${base}/sdk-v${version}`]);
  const clients = await buildClients(`${base}/sdk-v1`, `${base}/sdk-v2`, signal);
  await buildCatalogProof(signal);
  return clients;
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  const { values } = parseArgs({ options: { v1: { type: 'string' }, v2: { type: 'string' }, fixtures: { type: 'boolean' } } });
  if (values.fixtures && (values.v1 || values.v2)) throw new Error('--fixtures cannot be combined with catalog SDK directories.');
  const signal = AbortSignal.timeout(20 * 60_000);
  if (values.fixtures) console.log(JSON.stringify(await buildFixtures(signal)));
  else {
    if (!values.v1 || !values.v2) throw new Error('build-clients requires --v1 and --v2 immutable generated SDK directories.');
    console.log(JSON.stringify(await buildClients(values.v1, values.v2, signal)));
  }
}
