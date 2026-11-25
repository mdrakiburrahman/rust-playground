import assert from 'node:assert/strict';
import { mkdirSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { nativeManager } from '../src/native-catalogues.js';
import type { ContractRun } from '../src/contract.js';

const baseURL = process.env.CONTRACT_UI_URL ?? 'http://localhost:4173';
const manager = nativeManager(process.cwd());
const evidence = resolve('.temp/contract-builder/evidence', `native-restart-${Date.now()}`);
mkdirSync(evidence, { recursive: true });
const results: unknown[] = [];

async function snapshot(registry: string) {
  const response = await fetch(`${baseURL}/api/tables?registry=${registry}`, { signal: AbortSignal.timeout(90_000) });
  assert.equal(response.status, 200, await response.clone().text());
  return response.json();
}

for (const provider of ['unity', 'hive'] as const) {
  const runs: ContractRun[] = await (await fetch(`${baseURL}/api/runs`)).json();
  assert.ok(!runs.some((run) => ['queued', 'running'].includes(run.status)), 'Run restart proof only with builds idle');
  const before = await snapshot(provider);
  assert.ok(before.tables.length > 0, 'First create a real native-backed contract');
  try {
    await manager.stop(provider);
    const unavailable = await fetch(`${baseURL}/api/tables?registry=${provider}`, { signal: AbortSignal.timeout(90_000) });
    assert.notEqual(unavailable.status, 200, 'A stopped authority must not serve a successful cached snapshot');
    await snapshot('local');
  } finally {
    await manager.up(provider);
  }
  const after = await snapshot(provider);
  assert.deepEqual(after, before, 'Native table identity, nullability, properties and receipt survive native restart');
  results.push({ provider, tables: after.tables.map((table: { table: string; registrationReceipt: unknown }) =>
    ({ table: table.table, receipt: table.registrationReceipt })) });
}
writeFileSync(resolve(evidence, 'result.json'), JSON.stringify({ result: 'passed', results }, null, 2));
console.log(JSON.stringify({ result: 'passed', evidence, results }, null, 2));
