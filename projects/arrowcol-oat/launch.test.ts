import assert from 'node:assert/strict';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { launchOat } from './launch.js';

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
