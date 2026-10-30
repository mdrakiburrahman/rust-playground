import { existsSync, readFileSync, readdirSync, statSync } from 'node:fs';
import { dirname, extname, join, relative, resolve } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { parse as parseToml } from 'smol-toml';

export const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), '..');
export const DOCUMENTS = [
  'README.md',
  'docs/architecture/arrowcol-platform/README.md',
  'docs/architecture/arrowcol-platform/routing.md',
  'docs/architecture/arrowcol-platform/catalog.md',
  'docs/architecture/arrowcol-platform/delivery-guarantees.md',
  'docs/architecture/arrowcol-platform/sdk-generation.md',
  'docs/architecture/arrowcol-platform/storage.md',
  'docs/architecture/arrowcol-platform/observability.md',
  'docs/architecture/loadtest/README.md',
  'infra/azure/README.md',
  'docs/delivery-verification.md',
  'docs/quiver/README.md',
  'docs/containers/publishing.md',
  'projects/arrowcol-infra/README.md',
  'projects/arrowcol-rust/README.md',
  'projects/arrowcol-router-rust/README.md',
  'projects/arrowcol-dataflow-rust/README.md',
  'projects/loadtest/README.md',
  'projects/kusto-bridge/README.md',
  'projects/otelcol-rust/README.md',
  'bin/quiver-cli/README.md',
  'crates/arrow-deltalake/README.md',
  'crates/arrow-tenant-stream/README.md',
  'crates/delta-maintenance/README.md',
  'crates/arrowcol-buffer/README.md',
  'crates/arrowcol-catalog/README.md',
  'crates/arrowcol-dataflow-components/README.md',
  'crates/arrowcol-flight/README.md',
  'crates/arrowcol-protocol/README.md',
  'crates/arrowcol-receipts/README.md',
  'crates/arrowcol-routing/README.md',
  'crates/arrowcol-sdk-gen/README.md',
  'crates/otel-deltalake-exporter/README.md',
  'tools/libs/arrowcol-sdk-gen/README.md',
];
const RUNTIME_ROOTS = [
  'onelake/', 'target/', '.temp/', 'crates/arrowcol-sdk-gen/.temp/',
  'tools/libs/arrowcol-sdk-gen/.temp/', 'projects/loadtest/.temp/',
  'projects/arrowcol-rust/.data/',
];
const PATH_PREFIXES = ['.devcontainer/', '.github/', 'bin/', 'crates/', 'docs/', 'projects/', 'submodules/', 'tools/'];
const EXCLUDED = new Set(['node_modules', 'target', 'submodules', 'onelake', '.temp', '.git']);
const PROHIBITED: Record<string, RegExp> = {
  'absolute workspace path': new RegExp('/' + 'workspaces' + '/'),
  'pull request URL': /https?:\/\/github\.com\/[^)\s]+\/pull\/\d+/i,
  'issue URL': /https?:\/\/github\.com\/[^)\s]+\/issues\/\d+/i,
  'private development branch': new RegExp('dev/' + 'mdrrahman' + '/', 'i'),
};

export function githubSlug(value: string): string {
  return value.trim().toLowerCase().replace(/[^\p{L}\p{N}_\-\s]/gu, '').replace(/\s+/g, '-');
}

export function headingAnchors(text: string): Set<string> {
  const anchors = new Set<string>();
  const counts = new Map<string, number>();
  let fenced = false;
  for (const line of text.split(/\r?\n/)) {
    if (line.trimStart().startsWith('```')) { fenced = !fenced; continue; }
    if (fenced) continue;
    const match = /^#{1,6}\s+(.+?)\s*#*$/.exec(line);
    if (!match) continue;
    const base = githubSlug(match[1]);
    const count = counts.get(base) ?? 0;
    counts.set(base, count + 1);
    anchors.add(count === 0 ? base : `${base}-${count}`);
  }
  return anchors;
}

function* manifests(root: string, name: string): Generator<string> {
  for (const entry of readdirSync(root, { withFileTypes: true })) {
    if (entry.isDirectory() && !EXCLUDED.has(entry.name)) yield* manifests(join(root, entry.name), name);
    else if (entry.isFile() && entry.name === name) yield join(root, name);
  }
}

export function loadNxProjects(root = ROOT): Map<string, Set<string>> {
  const projects = new Map<string, Set<string>>();
  for (const path of manifests(root, 'project.json')) {
    const data: { name?: unknown; targets?: Record<string, unknown> } = JSON.parse(readFileSync(path, 'utf8'));
    if (typeof data.name === 'string') projects.set(data.name, new Set(Object.keys(data.targets ?? {})));
  }
  return projects;
}

export function loadCargoPackages(root = ROOT): Set<string> {
  const packages = new Set<string>();
  for (const path of manifests(root, 'Cargo.toml')) {
    const data = parseToml(readFileSync(path, 'utf8'));
    const pkg = data.package;
    if (pkg && typeof pkg === 'object' && !Array.isArray(pkg) && 'name' in pkg && typeof pkg.name === 'string') {
      packages.add(pkg.name);
    }
  }
  return packages;
}

export function markdownLinks(text: string): [string, string | undefined][] {
  return [...text.matchAll(/(?<!!)\[[^\]]+\]\(([^)]+)\)/g)].flatMap((match) => {
    const target = match[1].trim().split(/\s+/, 1)[0].replace(/^<|>$/g, '');
    if (/^(?:https?|mailto):/.test(target)) return [];
    const decoded = decodeURIComponent(target);
    const hash = decoded.indexOf('#');
    return [[hash < 0 ? decoded : decoded.slice(0, hash), hash < 0 ? undefined : decoded.slice(hash + 1)]];
  });
}

export function validateLink(source: string, path: string, anchor?: string, root = ROOT): void {
  const target = path ? resolve(dirname(source), path) : source;
  if (!existsSync(target)) throw new Error(`${relative(root, source)}: missing link target ${path}`);
  if (anchor === undefined || !statSync(target).isFile() || extname(target).toLowerCase() !== '.md') return;
  if (!headingAnchors(readFileSync(target, 'utf8')).has(anchor)) {
    throw new Error(`${relative(root, source)}: missing anchor #${anchor} in ${relative(root, target)}`);
  }
}

export function shellBlocks(text: string): string[] {
  return [...text.matchAll(/```(?:bash|sh)\n([\s\S]*?)```/g)].map((match) => match[1]);
}

// Tokenize examples without executing them; preserve quoted paths and reject incomplete quotes.
export function shellWords(text: string): string[] {
  const words: string[] = [];
  let word = '', quote = '', started = false;
  for (let i = 0; i < text.length; i++) {
    const char = text[i];
    if (char === '\\' && quote !== "'") {
      const next = text[++i];
      if (next === undefined) throw new Error('invalid shell example: dangling escape');
      if (quote === '"' && !['$', '`', '"', '\\', '\n'].includes(next)) word += '\\';
      if (next !== '\n') word += next;
      started = true;
    } else if (quote) {
      if (char === quote) quote = ''; else word += char;
    } else if (char === "'" || char === '"') {
      quote = char; started = true;
    } else if (char === '#') {
      while (i < text.length && text[i] !== '\n') i++;
      if (started) words.push(word);
      word = ''; started = false;
    } else if (/\s/.test(char)) {
      if (started) words.push(word);
      word = ''; started = false;
    } else { word += char; started = true; }
  }
  if (quote) throw new Error('invalid shell example: unclosed quote');
  if (started) words.push(word);
  return words;
}

export function staticPathTokens(text: string): Set<string> {
  const tokens = new Set<string>();
  const accept = (candidate: string): boolean =>
    PATH_PREFIXES.some((prefix) => candidate.startsWith(prefix)) &&
    !/[$<>*{}]/.test(candidate) && !RUNTIME_ROOTS.some((prefix) => candidate.startsWith(prefix)) &&
    candidate !== '.devcontainer/.env' && !candidate.includes('/secrets/') && !candidate.endsWith('/secrets');
  for (const block of shellBlocks(text)) {
    for (const word of shellWords(block.replace(/\\\n\s*/g, ' '))) {
      const candidate = word.replace(/[,:;]+$/, '');
      if (accept(candidate)) tokens.add(candidate);
    }
  }
  for (const match of text.matchAll(/`([^`\n]+)`/g)) {
    const candidate = match[1].replace(/\/+$/, '');
    if (!candidate.includes(' ') && accept(candidate)) tokens.add(candidate);
  }
  return tokens;
}

export function validateCommands(
  path: string, text: string, nxProjects: Map<string, Set<string>>, cargoPackages: Set<string>, root = ROOT,
): [number, number, number] {
  let nxCount = 0, cargoCount = 0, pathCount = 0;
  const checkNx = (project: string, target: string): void => {
    if (!nxProjects.get(project)?.has(target)) throw new Error(`${relative(root, path)}: unknown Nx target ${project}:${target}`);
    nxCount++;
  };
  for (const block of shellBlocks(text)) {
    const flat = block.replace(/\\\n\s*/g, ' ');
    for (const match of flat.matchAll(/\bnpx(?:\s+--no-install)?\s+nx\s+run\s+([A-Za-z0-9_.-]+):([A-Za-z0-9_.-]+)/g)) {
      checkNx(match[1], match[2]);
    }
    for (const match of flat.matchAll(/\bnpx(?:\s+--no-install)?\s+nx\s+run-many\b[^;\n]*/g)) {
      const projects = /--projects(?:=|\s+)([A-Za-z0-9_.,-]+)/.exec(match[0]);
      const target = /(?:^|\s)(?:-t|--target)(?:=|\s+)([A-Za-z0-9_.-]+)/.exec(match[0]);
      if (projects && target) for (const project of projects[1].split(',')) checkNx(project, target[1]);
    }
    for (const match of flat.matchAll(/\bcargo\b[^\n;]*(?:--package|-p)\s+([A-Za-z0-9_.-]+)/g)) {
      if (!cargoPackages.has(match[1])) throw new Error(`${relative(root, path)}: unknown Cargo package ${match[1]}`);
      cargoCount++;
    }
  }
  for (const candidate of staticPathTokens(text)) {
    if (!existsSync(join(root, candidate))) throw new Error(`${relative(root, path)}: missing documented path ${candidate}`);
    pathCount++;
  }
  return [nxCount, cargoCount, pathCount];
}

export function validateMarkdown(path: string, text: string, root = ROOT): void {
  const label = relative(root, path);
  if (!text.endsWith('\n')) throw new Error(`${label}: missing final newline`);
  let fenced = false, previousHeading = 0;
  for (const [index, line] of text.split(/\r?\n/).entries()) {
    if (line !== line.trimEnd()) throw new Error(`${label}:${index + 1}: trailing whitespace`);
    if (line.includes('\t')) throw new Error(`${label}:${index + 1}: tab character`);
    if (line.trimStart().startsWith('```')) { fenced = !fenced; continue; }
    if (fenced) continue;
    const heading = /^(#{1,6})\s+/.exec(line);
    if (heading) {
      if (previousHeading && heading[1].length > previousHeading + 1) {
        throw new Error(`${label}:${index + 1}: skipped heading level`);
      }
      previousHeading = heading[1].length;
    }
  }
  if (fenced) throw new Error(`${label}: unclosed fenced code block`);
  for (const [name, pattern] of Object.entries(PROHIBITED)) {
    if (pattern.test(text)) throw new Error(`${label}: prohibited ${name}`);
  }
}

export function main(): number {
  try {
    const projects = loadNxProjects(), packages = loadCargoPackages();
    let links = 0, nx = 0, cargo = 0, paths = 0;
    for (const document of DOCUMENTS) {
      const path = join(ROOT, document), text = readFileSync(path, 'utf8');
      validateMarkdown(path, text);
      for (const [target, anchor] of markdownLinks(text)) { validateLink(path, target, anchor); links++; }
      const counts = validateCommands(path, text, projects, packages);
      nx += counts[0]; cargo += counts[1]; paths += counts[2];
    }
    console.log(`documentation validation passed: ${DOCUMENTS.length} files, ${links} links, ${nx} Nx targets, ${cargo} Cargo package references, ${paths} paths`);
    return 0;
  } catch (error) {
    console.error(`documentation validation failed: ${error instanceof Error ? error.message : String(error)}`);
    return 1;
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) process.exitCode = main();
