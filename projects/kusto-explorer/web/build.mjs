// Bundles the browser assets for kusto-explorer into a static directory served by the Rust server.
//
// Usage: node web/build.mjs --pkg <wasm-bindgen output dir> [--out <dir>] [--no-compress]
import { constants as zlibConstants, brotliCompressSync, gzipSync } from 'node:zlib';
import { copyFile, mkdir, readFile, readdir, rm, stat, writeFile } from 'node:fs/promises';
import { dirname, join, relative, resolve } from 'node:path';
import { fileURLToPath } from 'node:url';
import { parseArgs } from 'node:util';
import * as esbuild from 'esbuild';

const projectDir = resolve(dirname(fileURLToPath(import.meta.url)), '..');
const webDir = join(projectDir, 'web');
const { values } = parseArgs({
  options: {
    pkg: { type: 'string' },
    out: { type: 'string', default: join(projectDir, 'dist') },
    'no-compress': { type: 'boolean', default: false },
  },
});
if (!values.pkg) {
  console.error('error: --pkg <wasm-bindgen output dir> is required');
  process.exit(2);
}

const pkgDir = resolve(values.pkg);
const outDir = resolve(values.out);
const assetsDir = join(outDir, 'assets');
const uiFiles = ['kusto_explorer_ui.js', 'kusto_explorer_ui_bg.wasm'];
const compressible = /\.(css|html|js|json|svg|ttf|txt|wasm)$/;

// The language service imports Monaco through the legacy `monaco-editor/esm/vs/...` paths, which
// Monaco 0.56+ no longer exposes through its `exports` map, and Monaco's ESM sources import some
// modules without a file extension. Resolve both by file path.
const monacoEsmPaths = {
  name: 'monaco-esm-paths',
  setup(build) {
    build.onResolve({ filter: /^monaco-editor\/esm\/vs\/.+/ }, (args) => {
      const file = join(projectDir, 'node_modules', args.path);
      return build.resolve(/\.(js|css)$/.test(file) ? file : `${file}.js`, { kind: args.kind, resolveDir: args.resolveDir });
    });
  },
};

// The language service carries optional Node.js code paths that never run in a browser.
const nodeBuiltinStub = {
  name: 'node-builtin-stub',
  setup(build) {
    build.onResolve({ filter: /^(fs|path|crypto|os|vm)$/ }, (args) => ({ path: args.path, namespace: 'node-stub' }));
    build.onLoad({ filter: /.*/, namespace: 'node-stub' }, () => ({ contents: 'module.exports = {};', loader: 'js' }));
  },
};

async function filesUnder(directory) {
  const entries = await readdir(directory, { withFileTypes: true, recursive: true });
  return entries.filter((entry) => entry.isFile()).map((entry) => join(entry.parentPath, entry.name));
}

async function precompress(directory) {
  for (const file of await filesUnder(directory)) {
    if (!compressible.test(file)) continue;
    const data = await readFile(file);
    if (data.length < 1024) continue;
    await writeFile(`${file}.gz`, gzipSync(data, { level: 9 }));
    await writeFile(
      `${file}.br`,
      brotliCompressSync(data, {
        params: {
          [zlibConstants.BROTLI_PARAM_QUALITY]: 11,
          [zlibConstants.BROTLI_PARAM_SIZE_HINT]: data.length,
        },
      }),
    );
  }
}

async function thirdPartyNotices(metafile) {
  const packages = new Set();
  for (const input of Object.keys(metafile.inputs)) {
    const match = /node_modules\/((?:@[^/]+\/)?[^/]+)\//.exec(input);
    if (match) packages.add(match[1]);
  }
  const sections = [];
  for (const name of [...packages].sort()) {
    const root = join(projectDir, 'node_modules', name);
    const manifest = JSON.parse(await readFile(join(root, 'package.json'), 'utf8'));
    const files = (await readdir(root)).filter((file) => /^(licen[cs]e|thirdpartynotices)/i.test(file)).sort();
    const texts = await Promise.all(files.map((file) => readFile(join(root, file), 'utf8')));
    sections.push(
      [`${manifest.name} ${manifest.version}`, `License: ${manifest.license ?? 'see package'}`, ...texts.map((text) => text.trim())].join('\n\n'),
    );
  }
  return `kusto-explorer bundles the following third-party packages.\n\n${sections.join(`\n\n${'-'.repeat(78)}\n\n`)}\n`;
}

for (const file of uiFiles) {
  await stat(join(pkgDir, file)).catch(() => {
    console.error(`error: ${join(pkgDir, file)} is missing; run wasm-bindgen --target web first`);
    process.exit(2);
  });
}

await rm(outDir, { recursive: true, force: true });
await mkdir(assetsDir, { recursive: true });

const result = await esbuild.build({
  absWorkingDir: projectDir,
  entryPoints: {
    main: 'web/src/main.ts',
    'kusto.worker': 'web/src/kusto.worker.ts',
    'editor.worker': 'web/src/editor.worker.ts',
  },
  outdir: assetsDir,
  entryNames: '[name]',
  assetNames: '[name]-[hash]',
  publicPath: '/assets',
  bundle: true,
  format: 'esm',
  platform: 'browser',
  target: ['es2022'],
  minify: true,
  legalComments: 'linked',
  loader: { '.ttf': 'file' },
  // The language service runtime treats any CommonJS wrapper as Node.js and reads `global`.
  define: { global: 'globalThis' },
  plugins: [monacoEsmPaths, nodeBuiltinStub],
  logLevel: 'warning',
  metafile: true,
});

await copyFile(join(webDir, 'index.html'), join(outDir, 'index.html'));
await copyFile(join(webDir, 'public', 'favicon.svg'), join(outDir, 'favicon.svg'));
for (const file of uiFiles) await copyFile(join(pkgDir, file), join(assetsDir, file));

await writeFile(join(outDir, 'THIRD_PARTY_NOTICES.txt'), await thirdPartyNotices(result.metafile));

if (!values['no-compress']) await precompress(outDir);

const sizes = await Promise.all(
  Object.keys(result.metafile.outputs).map(async (file) => [relative(projectDir, file), (await stat(join(projectDir, file))).size]),
);
for (const [file, size] of sizes.sort()) console.log(`${(size / 1024).toFixed(0).padStart(8)} KiB  ${file}`);
console.log(`wrote ${relative(process.cwd(), outDir) || '.'}`);
