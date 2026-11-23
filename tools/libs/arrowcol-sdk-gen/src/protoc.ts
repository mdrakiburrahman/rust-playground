import { copyFileSync, readFileSync } from 'node:fs';
import { join, relative, resolve } from 'node:path';
import { digest, ensureDirectory, type Execute } from './contract.js';

export function protocPaths(workspace: string) {
  const suffix = process.platform === 'win32' ? '.exe' : '';
  return {
    exporter: resolve(workspace, `target/debug/arrowcol-protoc${suffix}`),
    plugin: resolve(workspace, `target/debug/protoc-gen-arrowcol-sdk${suffix}`),
  };
}

export async function protocProvenance(workspace: string, execute: Execute, log?: string) {
  const paths = protocPaths(workspace);
  return {
    backend: 'protoc' as const,
    exporterSha256: digest(readFileSync(paths.exporter)),
    pluginSha256: digest(readFileSync(paths.plugin)),
    driverSha256: digest(readFileSync(resolve(workspace, 'tools/libs/arrowcol-sdk-gen/src/protoc.ts'))),
    protocVersion: (await execute('protoc', ['--version'], log)).trim(),
  };
}

export async function exportCompiler(
  workspace: string, artifacts: string, compiler: string, entry: string, execute: Execute, log: string,
): Promise<void> {
  await execute(protocPaths(workspace).exporter, [
    '--artifacts', relative(workspace, artifacts), '--out', relative(workspace, compiler), '--entry', entry,
  ], log);
}

export async function generateWithProtoc(
  workspace: string, artifacts: string, compiler: string, sdk: string, entry: string,
  execute: Execute, log: string,
): Promise<void> {
  ensureDirectory(workspace, relative(workspace, sdk));
  await execute('protoc', [
    `--proto_path=${compiler}`,
    `--plugin=protoc-gen-arrowcol-sdk=${protocPaths(workspace).plugin}`,
    `--arrowcol-sdk_out=${sdk}`, entry,
  ], log);
  // protoc returns UTF-8 source files. Keep the two binary artifacts byte-exact.
  for (const name of ['descriptor.pb', 'schema.arrow']) {
    copyFileSync(join(artifacts, name), join(sdk, 'descriptors', name));
  }

}

export async function validateCompiler(
  workspace: string, compiler: string, entry: string, output: string, execute: Execute, log: string,
): Promise<void> {
  await execute('protoc', [
    `--proto_path=${compiler}`, '--include_imports', `--descriptor_set_out=${relative(workspace, output)}`, entry,
  ], log);
}
