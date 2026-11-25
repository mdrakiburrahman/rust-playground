import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { contractRoot, digest, ensureDirectory, writeJson, type ContractRun, type Registry } from '../src/contract.js';

const mode = process.argv[2];
if (mode !== 'capture' && mode !== 'verify') throw new Error('Use capture, restart the UI, then verify');
const baseURL = process.env.CONTRACT_UI_URL ?? 'http://localhost:4173';
const workspace = process.cwd();
const output = resolve(workspace, contractRoot, 'evidence/restart.json');
async function get(path: string): Promise<unknown> {
  const response = await fetch(`${baseURL}${path}`);
  assert.equal(response.status, 200);
  return response.json();
}
const config = await get('/api/config') as { registries: Registry[] };
const runs = await get('/api/runs') as ContractRun[];
assert.ok(runs.every((run) => run.status === 'succeeded' || run.status === 'failed'), 'restart check requires drained builds');
const snapshots: Record<string, unknown> = {};
for (const registry of config.registries) snapshots[registry.id] = await get(`/api/tables?registry=${registry.id}`);
const current = {
  serverPid: readFileSync(resolve(workspace, contractRoot, 'server.lock'), 'utf8'),
  registries: config.registries, runs, snapshots,
};
if (mode === 'capture') {
  ensureDirectory(workspace, `${contractRoot}/evidence`);
  writeJson(output, current);
  console.log('Captured both registry snapshots, run history and server PID. Restart the owned UI before verify.');
} else {
  const expected: typeof current = JSON.parse(readFileSync(output, 'utf8'));
  assert.notEqual(current.serverPid, expected.serverPid, 'server process was actually replaced');
  assert.deepEqual(current.registries, expected.registries, 'authority identities persist');
  assert.deepEqual(current.snapshots, expected.snapshots, 'table IDs, versions, fields and properties persist');
  assert.deepEqual(current.runs, expected.runs, 'run history and release references persist');
  let downloads = 0;
  for (const run of runs.filter((item) => item.status === 'succeeded')) {
    assert.ok(run.artifact);
    const response = await fetch(`${baseURL}/api/runs/${run.id}/artifact`);
    assert.equal(response.status, 200);
    assert.equal(digest(Buffer.from(await response.arrayBuffer())), run.artifact.sha256);
    assert.equal(digest(readFileSync(resolve(workspace, run.artifact.path))), run.artifact.sha256);
    downloads++;
  }
  writeJson(resolve(workspace, contractRoot, 'evidence/restart-result.json'), { result: 'passed', registries: config.registries.length, runs: runs.length, downloads });
  console.log(`Restart verification passed: ${config.registries.length} registries, ${runs.length} runs, ${downloads} intact downloads.`);
}
