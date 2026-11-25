import { copyFileSync, lstatSync, readFileSync, readdirSync } from 'node:fs';
import { join, relative, resolve } from 'node:path';
import { digest, ensureDirectory, writeJson, type Execute } from './contract.js';

export interface Distribution {
  id: string;
  kind: 'contract' | 'models' | 'sdk';
  language?: 'rust' | 'cpp' | 'csharp';
  sha256: string;
  bytes: number;
}

function copyTree(workspace: string, source: string, target: string): void {
  if (lstatSync(source).isSymbolicLink() || !lstatSync(source).isDirectory()) {
    throw new Error(`Package input must be a regular directory: ${source}`);
  }
  ensureDirectory(workspace, target);
  for (const entry of readdirSync(source, { withFileTypes: true })) {
    const from = join(source, entry.name), to = join(target, entry.name);
    if (entry.isDirectory()) copyTree(workspace, from, to);
    else if (entry.isFile() && !entry.isSymbolicLink()) copyFileSync(from, to);
    else throw new Error(`Package input must be a regular file: ${from}`);
  }
}

export async function stageDistributions(
  workspace: string, directory: string, generator: string, execute: Execute, log: string, typed: boolean,
): Promise<Distribution[]> {
  const payload = join(directory, 'payload');
  const packages = join(directory, 'packages');
  const result: Distribution[] = [];
  async function stage(
    id: string, kind: Distribution['kind'], sources: [string, string][], language?: Distribution['language'],
  ): Promise<void> {
    const root = join(packages, id);
    for (const [source, target] of sources) copyTree(workspace, join(payload, source), join(root, target));
    const archive = join(packages, `${id}.tar.gz`);
    const packaged: { sha256: string; bytes: number } = JSON.parse(await execute(generator, [
      'package-artifact', '--input', relative(workspace, root), '--out', relative(workspace, archive),
    ], log));
    const bytes = readFileSync(archive);
    const sha256 = digest(bytes);
    if (packaged.sha256 !== `sha256:${sha256}` || packaged.bytes !== bytes.length) {
      throw new Error(`Package digest/size mismatch: ${id}`);
    }
    result.push({ id, kind, ...(language ? { language } : {}), sha256, bytes: bytes.length });
  }
  await stage('contract', 'contract', [['contract', 'contract']]);
  // The models embed byte-identical neutral files, not a second schema authority.
  for (const language of typed ? ['rust', 'cpp', 'csharp'] as const : []) {
    for (const file of readdirSync(join(payload, 'contract'), { recursive: true, withFileTypes: true })) {
      if (!file.isFile()) continue;
      const source = join(file.parentPath, file.name);
      const model = join(payload, 'sdk/models', language, 'contract', relative(join(payload, 'contract'), source));
      if (!readFileSync(source).equals(readFileSync(model))) {
        throw new Error(`Embedded ${language} contract differs from neutral bundle: ${file.name}`);
      }
    }
    await stage(`models-${language}`, 'models', [[`sdk/models/${language}`, `models/${language}`]], language);
    const sources: [string, string][] = [['sdk/runtime/arrowcol-sdk-runtime', 'runtime/arrowcol-sdk-runtime']];
    if (language !== 'rust') {
      sources.push(['sdk/runtime/arrowcol-sdk-ffi', 'runtime/arrowcol-sdk-ffi'],
        [`sdk/runtime/${language}`, `runtime/${language}`]);
    }
    await stage(`sdk-${language}`, 'sdk', sources, language);
  }
  writeJson(resolve(payload, 'packages.json'), { formatVersion: 1, contractSha256: result[0].sha256, packages: result });
  return result;
}
