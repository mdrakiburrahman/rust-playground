import { spawn, type ChildProcess } from 'node:child_process';
import { randomBytes } from 'node:crypto';
import { closeSync, existsSync, openSync, readFileSync, unlinkSync, writeFileSync } from 'node:fs';
import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';
import { createConnection } from 'node:net';
import { createRequire } from 'node:module';
import { dirname, join, resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import {
  commandRunner, contractRoot, digest, enqueue, ensureDirectory, errorMessage, generatorPath,
  isCode, listRuns, loadConfig, readRun, recoverRuns, runDirectory, saveRun,
} from './contract.js';

const workspace = process.cwd();
const execute = commandRunner(workspace);
const nxPackagePath = createRequire(import.meta.url).resolve('nx/package.json');
const nxPackage: { bin: { nx: string } } = JSON.parse(readFileSync(nxPackagePath, 'utf8'));
const nxBinary = resolve(dirname(nxPackagePath), nxPackage.bin.nx);
const children: ChildProcess[] = [];
const token = randomBytes(32).toString('hex');
const port = Number(process.env.CONTRACT_UI_PORT ?? '4173');
const host = process.env.CONTRACT_UI_BIND ?? '127.0.0.1';
const allowedHosts = new Set([`localhost:${port}`, `127.0.0.1:${port}`]);
const assets = resolve(workspace, 'tools/libs/arrowcol-sdk-gen/ui');
let stopping = false;
let pumping = false;
let ownsLock = false;
const lock = resolve(workspace, contractRoot, 'server.lock');

async function listening(endpoint: string): Promise<boolean> {
  const url = new URL(endpoint);
  return new Promise((resolvePromise, reject) => {
    const socket = createConnection({ host: url.hostname, port: Number(url.port) });
    socket.setTimeout(1000);
    socket.once('connect', () => { socket.destroy(); resolvePromise(true); });
    socket.once('timeout', () => { socket.destroy(); reject(new Error(`Probe timed out: ${endpoint}`)); });
    socket.once('error', (error) => {
      if (isCode(error, 'ECONNREFUSED')) resolvePromise(false);
      else reject(error);
    });
  });
}
function acquireLock(): void {
  if (existsSync(lock)) {
    const pid = Number(readFileSync(lock, 'utf8'));
    if (!Number.isSafeInteger(pid) || pid <= 0) throw new Error('Invalid server.lock; inspect it before removing');
    let alive = true;
    try { process.kill(pid, 0); } catch (error) {
      if (!isCode(error, 'ESRCH')) throw error;
      alive = false;
    }
    if (alive) throw new Error(`Catalogue UI is already owned by process ${pid}`);
    unlinkSync(lock);
  }
  writeFileSync(lock, String(process.pid), { flag: 'wx' });
  ownsLock = true;
}
function json(response: ServerResponse, status: number, body: unknown): void {
  response.writeHead(status, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' });
  response.end(JSON.stringify(body));
}
async function body(request: IncomingMessage): Promise<unknown> {
  if (request.headers['content-type'] !== 'application/json' || request.headers['x-contract-token'] !== token) {
    throw new Error('Expected same-origin JSON request with session token');
  }
  const chunks: Buffer[] = [];
  let bytes = 0;
  for await (const chunk of request) {
    const buffer = Buffer.isBuffer(chunk) ? chunk : Buffer.from(chunk);
    bytes += buffer.length;
    if (bytes > 80 * 1024) throw new Error('Request exceeds 80 KiB');
    chunks.push(buffer);
  }
  return JSON.parse(Buffer.concat(chunks).toString('utf8'));
}
async function pump(): Promise<void> {
  if (pumping || stopping) return;
  pumping = true;
  try {
    for (;;) {
      const run = listRuns(workspace).reverse().find((item) => item.status === 'queued');
      if (!run || stopping) break;
      try {
        await execute(process.execPath, [
          nxBinary, 'run', 'arrowcol-sdk-gen:build-contract', `--run-id=${run.id}`,
        ], join(runDirectory(workspace, run.id), 'nx.log'), {
          NX_DAEMON: 'false', NX_TUI: 'false', NX_TASKS_RUNNER_DYNAMIC_OUTPUT: 'false', FORCE_COLOR: '0',
        });
      } catch (error) {
        const latest = readRun(workspace, run.id);
        if (latest.status === 'queued' || latest.status === 'running') {
          latest.status = 'failed';
          latest.error = `Nx workflow failed: ${errorMessage(error)}`;
          latest.finishedAt = new Date().toISOString();
          saveRun(workspace, latest);
        }
        console.error(`Run ${run.id}: ${errorMessage(error)}`);
      }
    }
  } finally { pumping = false; }
}
async function route(request: IncomingMessage, response: ServerResponse): Promise<void> {
  const requestHost = request.headers.host ?? '';
  if (!allowedHosts.has(requestHost) ||
      (request.headers.origin && request.headers.origin !== `http://${requestHost}`)) {
    json(response, 403, { error: 'Local same-origin access only' });
    return;
  }
  response.setHeader('X-Content-Type-Options', 'nosniff');
  response.setHeader('Content-Security-Policy', "default-src 'self'; script-src 'self'; style-src 'self'; frame-ancestors 'none'; base-uri 'none'");
  const url = new URL(request.url ?? '/', `http://${requestHost}`);
  if (request.method === 'GET' && url.pathname === '/api/config') {
    json(response, 200, { registries: loadConfig(workspace).registries, token });
  } else if (request.method === 'GET' && url.pathname === '/api/properties') {
    json(response, 200, JSON.parse(await execute(generatorPath(workspace), ['catalog', 'properties'])));
  } else if (request.method === 'GET' && url.pathname === '/api/tables') {
    const registry = loadConfig(workspace).registries.find((item) => item.id === url.searchParams.get('registry'));
    if (!registry) { json(response, 400, { error: 'Unknown registry' }); return; }
    const snapshot = await execute(generatorPath(workspace), ['catalog', 'list', '--endpoint', registry.endpoint]);
    json(response, 200, JSON.parse(snapshot));
  } else if (request.method === 'GET' && url.pathname === '/api/runs') {
    recoverRuns(workspace);
    json(response, 200, listRuns(workspace));
  } else if (request.method === 'POST' && url.pathname === '/api/runs') {
    const run = enqueue(workspace, await body(request));
    json(response, 202, run);
    void pump().catch((error) => console.error(`Run dispatcher: ${errorMessage(error)}`));
  } else if (request.method === 'GET' && /^\/api\/runs\/[^/]+\/log$/.test(url.pathname)) {
    const id = url.pathname.split('/')[3];
    readRun(workspace, id);
    const directory = runDirectory(workspace, id);
    const text = ['build.log', 'nx.log'].map((name) =>
      existsSync(join(directory, name)) ? readFileSync(join(directory, name), 'utf8').slice(-64 * 1024) : '',
    ).join('\n');
    json(response, 200, { text });
  } else if (request.method === 'GET' && /^\/api\/runs\/[^/]+\/artifact$/.test(url.pathname)) {
    const run = readRun(workspace, url.pathname.split('/')[3]);
    if (run.status !== 'succeeded' || !run.artifact) { json(response, 404, { error: 'No completed artifact' }); return; }
    const expected = join(
      contractRoot, 'releases', digest(run.registry.authority), run.pin!.tableId,
      `v${run.pin!.version}`, run.artifact.sha256, 'contract-sdk.tar.gz',
    );
    if (run.artifact.path !== expected) throw new Error('Invalid release path');
    const bytes = readFileSync(resolve(workspace, expected));
    if (digest(bytes) !== run.artifact.sha256) throw new Error('Release integrity check failed');
    response.writeHead(200, {
      'Content-Type': 'application/gzip',
      'Content-Disposition': 'attachment; filename="contract-sdk.tar.gz"',
      'Content-Length': bytes.length,
      'Cache-Control': 'no-store',
    });
    response.end(bytes);
  } else if (request.method === 'GET' && ['/', '/app.js', '/style.css'].includes(url.pathname)) {
    const file = url.pathname === '/' ? join(assets, 'index.html')
      : url.pathname === '/style.css' ? join(assets, 'style.css')
        : resolve(workspace, contractRoot, 'public/app.js');
    response.writeHead(200, {
      'Content-Type': url.pathname === '/' ? 'text/html' : url.pathname.endsWith('.css') ? 'text/css' : 'text/javascript',
      'Cache-Control': 'no-store',
    });
    response.end(readFileSync(file));
  } else json(response, 404, { error: 'Not found' });
}
const server = createServer((request, response) => {
  void route(request, response).catch((error) => {
    console.error(errorMessage(error));
    if (!response.headersSent) json(response, 400, { error: errorMessage(error) });
    else response.destroy();
  });
});
async function shutdown(): Promise<void> {
  if (stopping) return;
  stopping = true;
  server.close();
  // Let an admitted build finish rather than interrupt a possibly committed CREATE.
  while (pumping) await delay(250);
  for (const child of children) child.kill('SIGTERM');
  await Promise.all(children.map((child) => child.exitCode !== null || child.signalCode !== null
    ? Promise.resolve() : new Promise<void>((done) => child.once('exit', () => done()))));
  if (ownsLock) unlinkSync(lock);
}
process.once('SIGINT', () => { void shutdown().catch((error) => { console.error(error); process.exitCode = 1; }); });
process.once('SIGTERM', () => { void shutdown().catch((error) => { console.error(error); process.exitCode = 1; }); });

try {
  if (!Number.isSafeInteger(port) || port < 1024 || port > 65535 ||
      !['127.0.0.1', '0.0.0.0'].includes(host)) throw new Error('Invalid UI port or bind address');
  const config = loadConfig(workspace);
  acquireLock();
  await execute('cargo', ['build', '--locked', '--quiet', '-p', 'arrowcol-sdk-gen', '-p', 'arrowcol-catalog-rust']);
  await execute(process.execPath, [
    'node_modules/typescript/bin/tsc', '--project', 'tools/libs/arrowcol-sdk-gen/ui/tsconfig.json',
  ]);
  for (const registry of config.registries.filter((item) => item.mode === 'owned')) {
    if (await listening(registry.endpoint)) throw new Error(`Refusing to adopt unowned service at ${registry.endpoint}`);
    const root = join(contractRoot, 'registries', digest(registry.authority));
    ensureDirectory(workspace, root);
    const output = openSync(resolve(workspace, root, 'service.log'), 'a');
    const binary = resolve(workspace, 'target/debug', process.platform === 'win32' ? 'arrowcol-catalog-rust.exe' : 'arrowcol-catalog-rust');
    const child = spawn(binary, [
      '--listen', new URL(registry.endpoint).host,
      '--state-dir', resolve(workspace, root, 'state'),
      '--warehouse-root', resolve(workspace, root, 'warehouse'),
    ], { cwd: workspace, stdio: ['ignore', output, output], shell: false });
    closeSync(output);
    children.push(child);
    let spawnError: Error | undefined;
    child.on('error', (error) => { spawnError = error; });
    child.on('exit', (code, signal) => {
      if (!stopping) console.error(`${registry.label} exited ${code ?? signal}; see ${root}/service.log`);
    });
    let ready = false;
    let lastError = '';
    for (let attempt = 0; attempt < 60; attempt++) {
      if (spawnError) throw spawnError;
      if (child.exitCode !== null || child.signalCode !== null) throw new Error(`${registry.label} exited; see ${root}/service.log`);
      try {
        await execute(binary, ['--healthcheck', '--listen', new URL(registry.endpoint).host]);
        ready = true;
        break;
      } catch (error) { lastError = errorMessage(error); await delay(500); }
    }
    if (!ready) throw new Error(`${registry.label} did not become ready: ${lastError}`);
  }
  recoverRuns(workspace);
  await new Promise<void>((done, reject) => {
    server.once('error', reject);
    server.listen(port, host, done);
  });
  console.log(`Catalogue Explorer ready: http://localhost:${port}`);
  void pump().catch((error) => console.error(`Run dispatcher: ${errorMessage(error)}`));
} catch (error) {
  console.error(`catalog-ui: ${errorMessage(error)}`);
  await shutdown();
  process.exitCode = 1;
}
