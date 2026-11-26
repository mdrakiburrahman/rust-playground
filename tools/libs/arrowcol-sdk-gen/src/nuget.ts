import { execFileSync } from 'node:child_process';
import { existsSync, mkdtempSync, rmSync, statSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { loadEnvFile } from 'node:process';
import { parseArgs } from 'node:util';

const root = process.cwd();
const { positionals, values } = parseArgs({
  allowPositionals: true,
  options: { 'package-version': { type: 'string' } },
});
const [action] = positionals;
const version = values['package-version'];
const packageId = 'Rust.Playground';
const out = resolve(root, 'target/arrowcol-nuget');
const source = 'ArrowcolRelease';

function command(program: string, args: string[], options: { cwd?: string; env?: NodeJS.ProcessEnv } = {}): void {
  execFileSync(program, args, { stdio: 'inherit', ...options });
}

function pack(version: string): void {
  command('cargo', ['build', '--locked', '--release', '--package', 'arrowcol-sdk-ffi', '--target', 'x86_64-unknown-linux-gnu']);
  const windowsTarget = 'x86_64-pc-windows-gnu';
  command('cargo', ['build', '--locked', '--release', '--package', 'arrowcol-sdk-ffi', '--target', windowsTarget], {
    env: { ...process.env, CARGO_TARGET_X86_64_PC_WINDOWS_GNU_LINKER: 'x86_64-w64-mingw32-gcc-posix' },
  });
  const native = resolve(process.env.CARGO_TARGET_DIR ?? join(root, 'target'));
  command('dotnet', [
    'pack', 'crates/arrowcol-sdk-runtime/bindings/dotnet/Rust.Playground.csproj', '--configuration', 'Release',
    '--output', out, '--artifacts-path', join(out, 'build'), `-p:PackageVersion=${version}`,
    `-p:ArrowcolLinuxNative=${join(native, 'x86_64-unknown-linux-gnu/release/libarrowcol_sdk_ffi.so')}`,
    `-p:ArrowcolWindowsNative=${join(native, windowsTarget, 'release/arrowcol_sdk_ffi.dll')}`,
  ]);
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
    || !/^\/[\w./%-]+\/_packaging\/[\w.%-]+\/nuget\/v3\/index\.json$/.test(url.pathname)
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
  if (positionals.length !== 1 || (action !== 'pack' && action !== 'publish') ||
      !version || !/^(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)\.(?:0|[1-9]\d*)(?:-[a-z][a-z0-9]*(?:\.[a-z0-9]+)*)?$/.test(version)) {
    throw new Error('Use nuget.ts pack|publish --package-version MAJOR.MINOR.PATCH[-prerelease]');
  }
  if (action === 'pack') pack(version);
  else publish(version);
} catch (error) {
  console.error(`arrowcol-nuget: ${error instanceof Error ? error.message : String(error)}`);
  process.exitCode = 1;
}
