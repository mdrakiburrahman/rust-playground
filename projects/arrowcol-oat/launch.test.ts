import assert from 'node:assert/strict';
import { mkdtemp, rm, unlink, writeFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { pathToFileURL } from 'node:url';
import { launchOat, readOatConfigForMode, resolveOatConfigPath, sendViewerFile, startLocalViewer } from './launch.js';

const status = (state: 'queued' | 'running' | 'succeeded') => ({
  id: 'run-a', state, progress: state, result_count: state === 'succeeded' ? 2 : 0, error: null,
});

test('posts the JSON config and polls until the server publishes results', async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'arrowcol-oat-'));
  const configPath = path.join(directory, 'oat.json');
  await writeFile(configPath, JSON.stringify({ replica_counts: [1, 2] }));
  const calls: Array<{ url: string; method: string }> = [];
  const responses = [status('queued'), status('running'), status('succeeded')];
  const fetcher: typeof fetch = async (input, init) => {
    calls.push({ url: String(input), method: init?.method ?? 'GET' });
    const next = responses.shift();
    assert.ok(next);
    return new Response(JSON.stringify(next), { status: 202 });
  };
  const progress: string[] = [];
  try {
    const result = await launchOat(configPath, 'http://127.0.0.1:8088', 1, fetcher, (value) => progress.push(value.state));
    assert.equal(result.result_count, 2);
    assert.deepEqual(progress, ['queued', 'running', 'succeeded']);
    assert.deepEqual(calls.map((call) => call.method), ['POST', 'GET', 'GET']);
    assert.equal(calls[0]?.url, 'http://127.0.0.1:8088/runs');
    assert.equal(calls[1]?.url, 'http://127.0.0.1:8088/runs/run-a');
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test('reports validation errors returned by the server', async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'arrowcol-oat-'));
  const configPath = path.join(directory, 'oat.json');
  await writeFile(configPath, '{}');
  const fetcher: typeof fetch = async () => new Response(JSON.stringify({ error: 'invalid matrix' }), { status: 400 });
  try {
    await assert.rejects(launchOat(configPath, undefined, 1, fetcher), /invalid matrix/);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test('allows the executable to select the platform mode without editing the config file', async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'arrowcol-oat-'));
  const configPath = path.join(directory, 'oat.json');
  await writeFile(configPath, JSON.stringify({ platform: { kind: 'aks' } }));
  let posted: unknown;
  const fetcher: typeof fetch = async (_input, init) => {
    posted = JSON.parse(String(init?.body));
    return new Response(JSON.stringify(status('succeeded')), { status: 202 });
  };
  try {
    await launchOat(configPath, undefined, 1, fetcher, () => {}, 'local');
    assert.deepEqual(posted, { platform: { kind: 'local' } });
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test('selects a per-mode quick config when no path is supplied', () => {
  assert.equal(path.basename(resolveOatConfigPath('local')), 'oat-config.quick.json');
  assert.equal(path.basename(resolveOatConfigPath('aks')), 'oat-config.aks.quick.json');
  assert.equal(resolveOatConfigPath('aks', 'custom.json'), 'custom.json');
});

test('fills missing local Docker bridge hosts while preserving explicit values', async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'arrowcol-oat-mode-'));
  const configPath = path.join(directory, 'oat.json');
  let gatewayCalls = 0;
  try {
    await writeFile(configPath, JSON.stringify({
      platform: { kind: 'aks', local: { bind_host: null, endpoint_host: null } },
    }));
    const config = await readOatConfigForMode(configPath, 'local', () => {
      gatewayCalls += 1;
      return '172.18.0.1';
    });
    const platform = config.platform as { kind: string; local: { bind_host: string; endpoint_host: string } };
    assert.equal(platform.kind, 'local');
    assert.equal(platform.local.bind_host, '172.18.0.1');
    assert.equal(platform.local.endpoint_host, '172.18.0.1');
    assert.equal(gatewayCalls, 1);

    await writeFile(configPath, JSON.stringify({
      platform: { kind: 'local', local: { bind_host: '192.0.2.1', endpoint_host: 'oat.example.test' } },
    }));
    const explicit = await readOatConfigForMode(configPath, 'local', () => {
      throw new Error('bridge lookup should not run when both hosts are explicit');
    });
    const explicitPlatform = explicit.platform as { local: { bind_host: string; endpoint_host: string } };
    assert.equal(explicitPlatform.local.bind_host, '192.0.2.1');
    assert.equal(explicitPlatform.local.endpoint_host, 'oat.example.test');
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test('serves local viewer byte ranges, suffix ranges, and HEAD requests', async () => {
  const server = createServer((request, response) => {
    void sendViewerFile(request, response, Buffer.from('abcdef'), 'application/vnd.apache.parquet');
  });
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  const address = server.address();
  assert.ok(address && typeof address !== 'string');
  const url = `http://127.0.0.1:${address.port}/`;
  try {
    const partial = await fetch(url, { headers: { range: 'bytes=1-3' } });
    assert.equal(partial.status, 206);
    assert.equal(partial.headers.get('content-range'), 'bytes 1-3/6');
    assert.equal(await partial.text(), 'bcd');

    const suffix = await fetch(url, { headers: { range: 'bytes=-2' } });
    assert.equal(suffix.status, 206);
    assert.equal(suffix.headers.get('content-range'), 'bytes 4-5/6');
    assert.equal(await suffix.text(), 'ef');

    const head = await fetch(url, { method: 'HEAD' });
    assert.equal(head.status, 200);
    assert.equal(head.headers.get('content-length'), '6');
    assert.equal(await head.text(), '');

    const invalid = await fetch(url, { headers: { range: 'bytes=8-' } });
    assert.equal(invalid.status, 416);
    assert.equal(invalid.headers.get('content-range'), 'bytes */6');
  } finally {
    await new Promise<void>((resolve, reject) => {
      server.close((error) => error ? reject(error) : resolve());
    });
  }
});

test('opens the existing local results in viewer-only mode without losing missing-file errors', async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'arrowcol-oat-viewer-'));
  const configPath = path.join(directory, 'oat.json');
  const resultsPath = path.join(directory, 'results.parquet');
  await writeFile(resultsPath, Buffer.from('PAR1data'));
  await writeFile(configPath, JSON.stringify({
    platform: { kind: 'local' },
    website_url: pathToFileURL(resultsPath).href,
  }));
  let viewer: Awaited<ReturnType<typeof startLocalViewer>> | undefined;
  try {
    viewer = await startLocalViewer(configPath);
    assert.equal(new URL(viewer.url).hostname, '127.0.0.1');

    const page = await fetch(viewer.url);
    assert.equal(page.status, 200);
    assert.match(await page.text(), /ArrowCol OAT benchmark/);

    const resultsUrl = new URL('/results.parquet', viewer.url);
    const range = await fetch(resultsUrl, { headers: { range: 'bytes=0-3' } });
    assert.equal(range.status, 206);
    assert.equal(range.headers.get('content-range'), 'bytes 0-3/8');
    assert.equal(await range.text(), 'PAR1');

    const head = await fetch(resultsUrl, { method: 'HEAD' });
    assert.equal(head.status, 200);
    assert.equal(head.headers.get('content-length'), '8');
    assert.equal(await head.text(), '');

    await unlink(resultsPath);
    const missing = await fetch(resultsUrl);
    assert.equal(missing.status, 404);
    assert.equal(await missing.text(), 'Not found');
    await assert.rejects(startLocalViewer(configPath), /Local OAT results file does not exist/);
  } finally {
    if (viewer) await viewer.close();
    await rm(directory, { recursive: true, force: true });
  }
});
