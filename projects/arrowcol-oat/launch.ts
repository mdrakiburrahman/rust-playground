import { spawn, type ChildProcess } from 'node:child_process';
import { createServer as createHttpServer } from 'node:http';
import { readFile, realpath, stat } from 'node:fs/promises';
import { createServer as createTcpServer } from 'node:net';
import { extname, isAbsolute, relative, resolve, sep } from 'node:path';
import { fileURLToPath, pathToFileURL } from 'node:url';

export interface OatRunStatus {
  id: string;
  state: 'queued' | 'running' | 'succeeded' | 'failed';
  progress: string;
  result_count: number;
  error: string | null;
}

type FetchLike = typeof fetch;
type ProgressHandler = (status: OatRunStatus) => void;

export interface LocalViewer {
  url: string;
  close(): Promise<void>;
}

interface LocalViewerConfig {
  resultsPath: string;
  siteRoot: string;
}

export async function launchOat(
  configPath: string,
  serverUrl = 'http://127.0.0.1:8088',
  pollIntervalMs = 1000,
  fetcher: FetchLike = fetch,
  onProgress: ProgressHandler = (status) => console.log(`${status.state}: ${status.progress}`),
  mode?: 'local' | 'aks',
): Promise<OatRunStatus> {
  if (!Number.isSafeInteger(pollIntervalMs) || pollIntervalMs < 1) {
    throw new Error('poll interval must be a positive integer');
  }
  const server = new URL(serverUrl);
  if (!['http:', 'https:'].includes(server.protocol)) {
    throw new Error('server URL must use HTTP or HTTPS');
  }
  const configText = await readFile(configPath, 'utf8');
  const parsedConfig: unknown = JSON.parse(configText);
  const config = mode && parsedConfig !== null && typeof parsedConfig === 'object' && !Array.isArray(parsedConfig)
    ? {
        ...parsedConfig,
        platform: {
          ...(Object.hasOwn(parsedConfig, 'platform') && 'platform' in parsedConfig && typeof parsedConfig.platform === 'object' && parsedConfig.platform !== null
            ? parsedConfig.platform
            : {}),
          kind: mode,
        },
      }
    : parsedConfig;
  if (config === null || typeof config !== 'object' || Array.isArray(config)) {
    throw new Error('OAT config must be a JSON object');
  }

  const startResponse = await fetcher(new URL('/runs', server), {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(config),
    signal: AbortSignal.timeout(30_000),
  });
  let status = await readStatus(startResponse, 'start OAT run');
  onProgress(status);
  while (status.state === 'queued' || status.state === 'running') {
    await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));
    const response = await fetcher(new URL(`/runs/${encodeURIComponent(status.id)}`, server), {
      signal: AbortSignal.timeout(30_000),
    });
    status = await readStatus(response, 'poll OAT run');
    onProgress(status);
  }
  if (status.state === 'failed') {
    throw new Error(status.error ?? `OAT run ${status.id} failed`);
  }
  return status;
}

async function readStatus(response: Response, action: string): Promise<OatRunStatus> {
  const body: unknown = await response.json().catch(() => undefined);
  if (!response.ok) {
    const error = body !== null && typeof body === 'object' && 'error' in body && typeof body.error === 'string'
      ? body.error
      : `${response.status} ${response.statusText}`;
    throw new Error(`Could not ${action}: ${error}`);
  }
  if (
    body === null || typeof body !== 'object' ||
    !('id' in body) || typeof body.id !== 'string' ||
    !('state' in body) || !['queued', 'running', 'succeeded', 'failed'].includes(String(body.state)) ||
    !('progress' in body) || typeof body.progress !== 'string' ||
    !('result_count' in body) || typeof body.result_count !== 'number'
  ) {
    throw new Error(`Server returned an invalid status while trying to ${action}`);
  }
  return body as OatRunStatus;
}

export async function main(args: string[]): Promise<void> {
  const option = (name: string): string | undefined => {
    const index = args.indexOf(name);
    if (index < 0) return undefined;
    const value = args[index + 1];
    if (!value || value.startsWith('--')) throw new Error(`Expected a value after ${name}`);
    return value;
  };
  const valuedOptions = new Set(['--config', '--mode', '--server', '--poll-ms']);
  const positionalConfig = args.find((value, index) =>
    !value.startsWith('--') && !valuedOptions.has(args[index - 1] ?? ''),
  );
  const configPath = option('--config') ?? positionalConfig;
  if (!configPath) {
    throw new Error('Usage: npx arrowcol-oat --config <oat-config.json> [--mode local|aks] [--server URL] [--poll-ms N]; or --viewer-only --config <local-oat-config.json>');
  }
  const requestedServerUrl = option('--server');
  const mode = option('--mode');
  const viewerOnly = args.includes('--viewer-only');
  if (viewerOnly) {
    if (requestedServerUrl || mode !== undefined || args.includes('--poll-ms')) {
      throw new Error('--viewer-only only supports --config');
    }
    await keepLocalViewerOpen(await startLocalViewer(configPath));
    return;
  }
  const pollValue = Number(option('--poll-ms') ?? 1000);
  if (mode !== undefined && mode !== 'local' && mode !== 'aks') throw new Error('--mode must be local or aks');
  if (!Number.isSafeInteger(pollValue) || pollValue < 1) {
    throw new Error('Expected values after --server and --poll-ms');
  }
  if (requestedServerUrl) {
    const status = await launchOat(configPath, requestedServerUrl, pollValue, fetch, undefined, mode);
    console.log(`Completed ${status.id}: published ${status.result_count} result rows`);
    await maybeServeLocalViewer(configPath, mode);
    return;
  }

  const server = await startLocalServer();
  let status: OatRunStatus;
  try {
    status = await launchOat(configPath, server.url, pollValue, fetch, undefined, mode);
    console.log(`Completed ${status.id}: published ${status.result_count} result rows`);
  } finally {
    await stopLocalServer(server.child);
  }
  await maybeServeLocalViewer(configPath, mode);
}

export async function startLocalViewer(configPath: string): Promise<LocalViewer> {
  const config = await readLocalViewerConfig(configPath);
  if (!config) {
    throw new Error('--viewer-only requires platform.kind=local and website_url to be a file:// URL');
  }
  try {
    const resultFile = await stat(config.resultsPath);
    if (!resultFile.isFile()) throw new Error(`Local OAT results URL is not a file: ${config.resultsPath}`);
  } catch (error) {
    if (error instanceof Error && 'code' in error && error.code === 'ENOENT') {
      throw new Error(`Local OAT results file does not exist: ${config.resultsPath}`);
    }
    throw error;
  }
  return createLocalViewer(config);
}

async function maybeServeLocalViewer(configPath: string, mode?: 'local' | 'aks'): Promise<void> {
  const config = await readLocalViewerConfig(configPath, mode);
  if (!config) return;
  await keepLocalViewerOpen(await createLocalViewer(config));
}

async function readLocalViewerConfig(
  configPath: string,
  mode?: 'local' | 'aks',
): Promise<LocalViewerConfig | undefined> {
  const parsed: unknown = JSON.parse(await readFile(configPath, 'utf8'));
  if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) return;
  const config = parsed as { platform?: { kind?: string }; website_url?: string };
  const platform = mode ?? config.platform?.kind;
  const websiteUrl = config.website_url;
  if (platform !== 'local' || !websiteUrl?.startsWith('file://')) return undefined;
  const resultsPath = fileURLToPath(new URL(websiteUrl));
  const workspace = resolve(fileURLToPath(new URL('../..', import.meta.url)));
  const siteRoot = await realpath(resolve(workspace, 'docs/experiments/arrowcol-oat'));
  return { resultsPath, siteRoot };
}

async function createLocalViewer(config: LocalViewerConfig): Promise<LocalViewer> {
  const { resultsPath, siteRoot } = config;
  const server = createHttpServer(async (request, response) => {
    try {
      if (request.method !== 'GET' && request.method !== 'HEAD') {
        response.writeHead(405, { allow: 'GET, HEAD' }).end();
        return;
      }
      const requestUrl = new URL(request.url ?? '/', 'http://127.0.0.1');
      if (requestUrl.pathname === '/results.parquet') {
        await sendViewerFile(request, response, await readFile(resultsPath), 'application/vnd.apache.parquet');
        return;
      }
      let pathname: string;
      try {
        pathname = decodeURIComponent(requestUrl.pathname === '/' ? '/index.html' : requestUrl.pathname);
      } catch {
        response.writeHead(400).end('Invalid path');
        return;
      }
      const file = resolve(siteRoot, `.${pathname}`);
      if (!isPathWithin(siteRoot, file)) {
        response.writeHead(400).end('Invalid path');
        return;
      }
      const actualFile = await realpath(file);
      if (!isPathWithin(siteRoot, actualFile)) {
        response.writeHead(403).end('Forbidden');
        return;
      }
      const contentTypes: Record<string, string> = {
        '.css': 'text/css; charset=utf-8',
        '.html': 'text/html; charset=utf-8',
        '.js': 'text/javascript; charset=utf-8',
        '.json': 'application/json; charset=utf-8',
        '.png': 'image/png',
        '.svg': 'image/svg+xml',
      };
      await sendViewerFile(
        request,
        response,
        await readFile(actualFile),
        contentTypes[extname(actualFile).toLowerCase()] ?? 'application/octet-stream',
      );
    } catch (error) {
      const code = error instanceof Error && 'code' in error && error.code === 'ENOENT' ? 404 : 500;
      response.writeHead(code).end(code === 404 ? 'Not found' : 'Could not serve local OAT viewer');
      if (code === 500) console.error(error);
    }
  });
  await new Promise<void>((resolvePromise, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolvePromise);
  });
  const address = server.address();
  if (!address || typeof address === 'string') throw new Error('Could not allocate a local viewer port');
  return {
    url: `http://127.0.0.1:${address.port}/`,
    close: () => new Promise<void>((resolvePromise, reject) => {
      server.close((error) => error ? reject(error) : resolvePromise());
    }),
  };
}

async function keepLocalViewerOpen(viewer: LocalViewer): Promise<void> {
  console.log(`Local results viewer: ${viewer.url}`);
  console.log('Press Ctrl+C to stop the viewer.');
  try {
    await new Promise<void>((resolvePromise) => {
      const stop = (): void => {
        process.off('SIGINT', stop);
        process.off('SIGTERM', stop);
        resolvePromise();
      };
      process.once('SIGINT', stop);
      process.once('SIGTERM', stop);
    });
  } finally {
    await viewer.close();
  }
}

function isPathWithin(root: string, target: string): boolean {
  const path = relative(root, target);
  return path !== '..' && !path.startsWith(`..${sep}`) && !isAbsolute(path);
}

export async function sendViewerFile(
  request: import('node:http').IncomingMessage,
  response: import('node:http').ServerResponse,
  data: Buffer,
  contentType: string,
): Promise<void> {
  const range = request.headers.range;
  const headers = {
    'accept-ranges': 'bytes',
    'cache-control': 'no-store',
    'content-type': contentType,
  };
  if (!range) {
    response.writeHead(200, { ...headers, 'content-length': data.length });
    response.end(request.method === 'HEAD' ? undefined : data);
    return;
  }
  const match = /^bytes=(\d*)-(\d*)$/.exec(range);
  if (!match || (!match[1] && !match[2])) {
    response.writeHead(416, { 'content-range': `bytes */${data.length}` }).end();
    return;
  }
  const suffixLength = match[1] ? undefined : Number(match[2]);
  const start = match[1] ? Number(match[1]) : Math.max(0, data.length - (suffixLength ?? 0));
  const end = match[2] && match[1] ? Math.min(Number(match[2]), data.length - 1) : data.length - 1;
  if (!Number.isSafeInteger(start) || !Number.isSafeInteger(end) || start < 0 || start > end || end >= data.length) {
    response.writeHead(416, { 'content-range': `bytes */${data.length}` }).end();
    return;
  }
  const body = data.subarray(start, end + 1);
  response.writeHead(206, {
    ...headers,
    'content-length': body.length,
    'content-range': `bytes ${start}-${end}/${data.length}`,
  });
  response.end(request.method === 'HEAD' ? undefined : body);
}

async function startLocalServer(): Promise<{ child: ChildProcess; url: string }> {
  const port = await availablePort();
  const url = `http://127.0.0.1:${port}`;
  const workspace = resolve(fileURLToPath(new URL('../..', import.meta.url)));
  const child = spawn('cargo', [
    'run', '--locked', '--package', 'arrowcol-oat-rust', '--',
    '--listen', `127.0.0.1:${port}`,
  ], { cwd: workspace, stdio: 'inherit' });
  let exit: { code: number | null; signal: NodeJS.Signals | null } | undefined;
  let spawnError: Error | undefined;
  child.once('exit', (code, signal) => { exit = { code, signal }; });
  child.once('error', (error) => { spawnError = error; });
  const deadline = Date.now() + 10 * 60 * 1000;
  while (Date.now() < deadline) {
    if (spawnError) throw new Error(`Could not start OAT server: ${spawnError.message}`);
    if (exit) throw new Error(`OAT server exited before becoming ready (${exit.signal ?? exit.code})`);
    try {
      const response = await fetch(new URL('/healthz', url), { signal: AbortSignal.timeout(2000) });
      if (response.ok) return { child, url };
    } catch {
      // The server may still be compiling or starting.
    }
    await new Promise((resolve) => setTimeout(resolve, 500));
  }
  await stopLocalServer(child);
  throw new Error('OAT server did not become ready within ten minutes');
}

async function availablePort(): Promise<number> {
  const server = createTcpServer();
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', () => resolve());
  });
  const address = server.address();
  if (!address || typeof address === 'string') {
    server.close();
    throw new Error('Could not allocate a local OAT server port');
  }
  const port = address.port;
  await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  return port;
}

async function stopLocalServer(child: ChildProcess): Promise<void> {
  if (child.exitCode !== null || child.signalCode !== null) return;
  const exited = new Promise<void>((resolve) => child.once('exit', () => resolve()));
  child.kill('SIGTERM');
  let timeout: NodeJS.Timeout | undefined;
  const stopped = await Promise.race([
    exited.then(() => true),
    new Promise<boolean>((resolve) => { timeout = setTimeout(() => resolve(false), 5000); }),
  ]);
  if (timeout) clearTimeout(timeout);
  if (!stopped) throw new Error('OAT server did not stop after SIGTERM; check its process before retrying');
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main(process.argv.slice(2)).catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
