import { execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { copyFileSync, existsSync, mkdirSync, mkdtempSync, readdirSync, readFileSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { basename, join, resolve } from 'node:path';
import { loadEnvFile } from 'node:process';
import { parseArgs } from 'node:util';
import { generateDmvPackageInputs } from './nuget-dmv.js';

const root = process.cwd();
const { positionals, values } = parseArgs({
  allowPositionals: true,
  options: { 'package-version': { type: 'string' }, 'windows-runtime': { type: 'string' } },
});
const [action] = positionals;
const version = values['package-version'];
const windowsRelease = 'arrowcol-windows-latest';
const windowsAsset = /^arrowcol-windows-x64-([0-9a-f]{40})\.zip$/;
const windowsExecutables = ['arrowcol-catalog-rust.exe', 'arrowcol-dataflow-rust.exe'];
const sdkConfig = 'config.sdk.yaml';
// One local collector serves every DMV table, so widen the smoke-sized admission limits.
const sdkLimits: [string, string, string][] = [
  ['max_concurrent_streams', '2', '128'],
  ['registration_ttl', '5s', '30s'],
  ['buffered_bytes_limit', '1048576', '134217728'],
  ['buffered_rows_limit', '10000', '1000000'],
  ['buffered_batches_limit', '16', '256'],
  ['admission_timeout', '2s', '30s'],
  ['max_stream_duration', '10s', '5m'],
  ['authority_timeout', '1s', '10s'],
];
const packageId = 'Rust.Playground';
const out = resolve(root, 'target/arrowcol-nuget');
const source = 'ArrowcolRelease';

function command(program: string, args: string[], options: { cwd?: string; env?: NodeJS.ProcessEnv } = {}): void {
  execFileSync(program, args, { stdio: 'inherit', ...options });
}

function sha256(path: string): string {
  return createHash('sha256').update(readFileSync(path)).digest('hex');
}

/** Stage the published Windows catalog and collector, verified against their release digest and manifest. */
function stageWindowsRuntime(dir: string, archive?: string): string {
  mkdirSync(dir);
  if (!archive) {
    const repo = process.env.GITHUB_REPOSITORY ?? 'mdrakiburrahman/rust-playground';
    const release = execFileSync('gh', ['release', 'view', windowsRelease, '--repo', repo, '--json', 'assets'], { encoding: 'utf8' });
    const owned = (JSON.parse(release).assets as { name: string; digest?: string }[]).filter(asset => windowsAsset.test(asset.name));
    if (owned.length !== 1 || !/^sha256:[0-9a-f]{64}$/.test(owned[0].digest ?? '')) {
      throw new Error(`Release ${windowsRelease} must carry exactly one digested Windows runtime`);
    }
    command('gh', ['release', 'download', windowsRelease, '--repo', repo, '--pattern', owned[0].name, '--dir', dir]);
    archive = join(dir, owned[0].name);
    if (`sha256:${sha256(archive)}` !== owned[0].digest) throw new Error(`${owned[0].name} does not match its release digest`);
  }
  const sourceSha = windowsAsset.exec(basename(archive))?.[1];
  if (!sourceSha) throw new Error('The Windows runtime must be an arrowcol-windows-x64-<commit>.zip release asset');
  const extracted = join(dir, 'extracted');
  command('unzip', ['-q', archive, '-d', extracted]);
  const manifest = JSON.parse(readFileSync(join(extracted, 'manifest.json'), 'utf8')) as
    { manifestVersion: number; sourceSha: string; target: string; files: Record<string, string> };
  const payload = [...windowsExecutables, 'config.windows.yaml'].sort();
  if (manifest.manifestVersion !== 1 || manifest.sourceSha !== sourceSha || manifest.target !== 'x86_64-pc-windows-msvc'
    || JSON.stringify(Object.keys(manifest.files).sort()) !== JSON.stringify(payload)
    || JSON.stringify(readdirSync(extracted).sort()) !== JSON.stringify([...payload, 'manifest.json'].sort())) {
    throw new Error('Unexpected Windows runtime manifest');
  }
  for (const name of payload) {
    if (sha256(join(extracted, name)) !== manifest.files[name]) throw new Error(`Windows runtime ${name} does not match its manifest`);
  }
  const runtime = join(dir, 'runtime');
  mkdirSync(runtime);
  let config = readFileSync(join(extracted, 'config.windows.yaml'), 'utf8');
  for (const [key, from, to] of sdkLimits) {
    const line = new RegExp(`^(\\s+${key}: )${from}$`, 'gm');
    if ((config.match(line) ?? []).length !== 1) throw new Error(`Expected one ${key}: ${from} in config.windows.yaml`);
    config = config.replace(line, `$1${to}`);
  }
  writeFileSync(join(runtime, sdkConfig), config);
  for (const name of windowsExecutables) copyFileSync(join(extracted, name), join(runtime, name));
  const files = Object.fromEntries([...windowsExecutables, sdkConfig].map(name => [name, sha256(join(runtime, name))]));
  writeFileSync(join(runtime, 'runtime.json'), `${JSON.stringify({ version: 1, sourceSha, files }, null, 2)}\n`);
  return runtime;
}

const linuxTarget = 'x86_64-unknown-linux-gnu';
const windowsTarget = 'x86_64-pc-windows-gnu';

/** Build the native runtimes and generator, generate the DMV inputs into a temporary directory and run `use` on them. */
function withDmvInputs(targets: string[], use: (generated: string, native: string) => void): void {
  for (const target of targets) {
    command('cargo', ['build', '--locked', '--release', '--package', 'arrowcol-sdk-ffi', '--target', target], target === windowsTarget
      ? { env: { ...process.env, CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER: 'x86_64-w64-mingw32-gcc-posix' } } : {});
  }
  command('cargo', ['build', '--locked', '--release', '--package', 'arrowcol-sdk-gen']);
  const native = resolve(process.env.CARGO_TARGET_DIR ?? join(root, 'target'));
  mkdirSync(out, { recursive: true });
  const generated = mkdtempSync(join(out, 'dmv-'));
  try {
    generateDmvPackageInputs(join(native, 'release/arrowcol-sdk-gen'), generated);
    use(generated, native);
  } finally {
    rmSync(generated, { recursive: true, force: true });
  }
}

function pack(version: string, windowsArchive?: string): void {
  withDmvInputs([linuxTarget, windowsTarget], (generated, native) => {
    const windowsRuntime = stageWindowsRuntime(join(generated, 'windows'), windowsArchive && resolve(windowsArchive));
    command('dotnet', [
      'pack', 'crates/arrowcol-sdk-runtime/bindings/dotnet/Rust.Playground.csproj', '--configuration', 'Release',
      '--output', out, '--artifacts-path', join(generated, 'build'), `-p:PackageVersion=${version}`,
      `-p:ArrowcolDmvRoot=${generated}`,
      `-p:ArrowcolLinuxNative=${join(native, linuxTarget, 'release/libarrowcol_sdk_ffi.so')}`,
      `-p:ArrowcolWindowsNative=${join(native, windowsTarget, 'release/arrowcol_sdk_ffi.dll')}`,
      `-p:ArrowcolWindowsRuntime=${windowsRuntime}`,
    ]);
  });
}

/** Run the .NET binding tests on Linux x64; set ARROWCOL_TEST_SQLSERVER to include the SQL Server tests. */
function test(): void {
  withDmvInputs([linuxTarget], (generated, native) => {
    const build = join(generated, 'build');
    command('dotnet', [
      'build', 'crates/arrowcol-sdk-runtime/bindings/dotnet-tests/Arrowcol.Sdk.Tests.csproj',
      '--artifacts-path', build, `-p:ArrowcolDmvRoot=${generated}`,
      `-p:ArrowcolNative=${join(native, linuxTarget, 'release/libarrowcol_sdk_ffi.so')}`,
    ]);
    command('dotnet', [join(build, 'bin/Arrowcol.Sdk.Tests/debug/Arrowcol.Sdk.Tests.dll')]);
  });
}

function publish(version: string): void {
  const packagePath = join(out, `${packageId}.${version}.nupkg`);
  if (!existsSync(packagePath) || !statSync(packagePath).isFile()) {
    throw new Error('Pack the exact version before publishing it');
  }
  if (existsSync(join(root, '.env'))) loadEnvFile(join(root, '.env'));
  const { NUGET_URL, NUGET_PAT } = process.env;
  if (!NUGET_URL || !NUGET_PAT) throw new Error('Configure NUGET_URL and NUGET_PAT outside source control');
  let url: URL;
  try {
    url = new URL(NUGET_URL);
  } catch {
    throw new Error('NUGET_URL must be an Azure Artifacts NuGet v3 HTTPS endpoint');
  }
  if (url.protocol !== 'https:' || url.username || url.password || url.search || url.hash
    || !/^(?:\/[\w.%-]+)*\/_packaging\/[\w.%-]+\/nuget\/v3\/index\.json$/.test(url.pathname)
    || !(url.hostname === 'pkgs.dev.azure.com' || url.hostname.endsWith('.pkgs.visualstudio.com'))) {
    throw new Error('NUGET_URL must be an Azure Artifacts NuGet v3 HTTPS endpoint');
  }
  const tmp = mkdtempSync(join(tmpdir(), 'arrowcol-nuget-'));
  try {
    writeFileSync(join(tmp, 'NuGet.Config'), `<configuration><packageSources><clear /><add key="${source}" value="${url.href}" /></packageSources></configuration>`);
    command('dotnet', ['nuget', 'push', packagePath, '--source', source, '--api-key', 'AzureArtifacts'], {
      cwd: tmp,
      env: {
        ...process.env,
        [`NuGetPackageSourceCredentials_${source}`]: `Username=AzureArtifacts;Password=${NUGET_PAT};ValidAuthenticationTypes=Basic`,
      },
    });
  } finally {
    rmSync(tmp, { recursive: true, force: true });
  }
}

try {
  const versioned = action === 'pack' || action === 'publish';
  if (positionals.length !== 1 || (!versioned && action !== 'test') || (versioned !== Boolean(version)) ||
      (version && !/^(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)(?:-[a-z][a-z0-9]*(?:\.[a-z0-9]+)*)?$/.test(version))) {
    throw new Error('Use nuget.ts test, or nuget.ts pack|publish --package-version MAJOR.MINOR.PATCH[-prerelease] [--windows-runtime <zip>]');
  }
  if (action === 'test') test();
  else if (action === 'pack') pack(version!, values['windows-runtime']);
  else publish(version!);
} catch (error) {
  console.error(`arrowcol-nuget: ${error instanceof Error ? error.message : String(error)}`);
  process.exitCode = 1;
}
