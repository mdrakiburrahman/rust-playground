import assert from 'node:assert/strict';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { chromium } from 'playwright';
import { digest, type ContractRun } from '../src/contract.js';
import { nativeManager } from '../src/native-catalogues.js';

const baseURL = process.env.CONTRACT_UI_URL ?? 'http://localhost:4173';
const table = `native_${Date.now()}`;
const evidence = resolve('.temp/contract-builder/evidence', table);
mkdirSync(evidence, { recursive: true });
const browser = await chromium.launch({ args: ['--no-sandbox'] });
const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
page.setDefaultTimeout(90_000);
const errors: string[] = [];
page.on('pageerror', (error) => errors.push(error.message));
const results: ContractRun[] = [];
const manager = nativeManager(process.cwd());

async function complete(id: string): Promise<ContractRun> {
  const deadline = Date.now() + 15 * 60_000;
  while (Date.now() < deadline) {
    const response = await page.request.get(`${baseURL}/api/runs`, { timeout: 90_000, maxRetries: 2 });
    assert.equal(response.status(), 200);
    const runs: ContractRun[] = await response.json();
    const run = runs.find((item) => item.id === id);
    assert.ok(run);
    if (run.status === 'failed') throw new Error(`${run.phase}: ${run.error}`);
    if (run.status === 'succeeded') return run;
    await delay(1000);
  }
  throw new Error(`Native build did not finish: ${id}`);
}

try {
  await page.goto(`${baseURL}/lakehouse`);
  for (const provider of ['unity', 'hive'] as const) {
    await page.locator(`#registry option[value="${provider}"]`).waitFor({ state: 'attached' });
    await page.locator('#registry').selectOption(provider);
    await page.locator('#catalog-tab').click();
    await page.locator('#add-table').click();
    await page.locator('#catalog-name').fill(provider === 'hive' ? 'hive' : 'telemetry');
    await page.getByLabel('Table', { exact: true }).fill(table);
    const accepted = page.waitForResponse((response) =>
      response.url().endsWith('/api/runs') && response.request().method() === 'POST');
    await page.locator('#create-submit').click();
    const response = await accepted;
    assert.equal(response.status(), 202);
    const queued: ContractRun = await response.json();
    assert.equal(queued.request.codegen, 'protoc');
    const created = await complete(queued.id);
    results.push(created);
    assert.ok(created.artifact && created.pin && created.registrationReceipt);
    assert.equal(created.registration, 'committed');
    assert.equal(digest(readFileSync(resolve(created.artifact.path))), created.artifact.sha256);
    const native = await manager.inspect(provider, provider === 'hive' ? 'hive' : 'telemetry', 'prod', table);
    assert.ok(JSON.stringify(native).includes(table), 'actual native service contains the registered table');
    writeFileSync(resolve(evidence, `${provider}-native.json`), JSON.stringify(native, null, 2));
    const listed = await page.request.get(`${baseURL}/api/tables?registry=${provider}`);
    assert.equal(listed.status(), 200);
    const snapshot = await listed.json();
    const registered = snapshot.tables.find((item: { table: string }) => item.table === created.pin!.table);
    assert.deepEqual(registered.registrationReceipt, created.registrationReceipt);
    assert.deepEqual(registered.schema.fields.map((field: { nullable: boolean }) => field.nullable), [false, false, true]);
    for (const rust of ['local', 'hosted']) {
      const other = await (await page.request.get(`${baseURL}/api/tables?registry=${rust}`)).json();
      assert.ok(!other.tables.some((item: { table: string }) => item.table === created.pin!.table), 'no dual registration');
    }
    await page.reload();
    await page.locator('#catalog-tab').click();
    await page.locator('#filter').fill(table);
    await page.locator(`[data-table="${created.pin.table}"]`).click();
    assert.equal(await page.locator('#table-tab-name').innerText(), table);
    assert.ok((await page.locator('#lakehouse-schema').innerText()).includes('event_id'));
    await page.screenshot({ path: resolve(evidence, `${provider}.png`), fullPage: true });
    const config = await (await page.request.get(`${baseURL}/api/config`)).json();
    const rebuild = await page.request.post(`${baseURL}/api/runs`, {
      headers: { 'x-contract-token': config.token },
      data: { registry: provider, mode: 'rebuild', table: created.pin.table, version: 1, codegen: 'protoc', protoEntry: 'telemetry.proto' },
    });
    assert.equal(rebuild.status(), 202);
    const rebuilt = await complete((await rebuild.json()).id);
    assert.equal(rebuilt.registration, 'existing');
    assert.equal(rebuilt.artifact?.sha256, created.artifact.sha256, 'rebuild preserves immutable SDK bytes');
    const duplicate = await page.request.post(`${baseURL}/api/runs`, {
      headers: { 'x-contract-token': config.token }, data: queued.request,
    });
    assert.equal(duplicate.status(), 202);
    const duplicateId = (await duplicate.json()).id;
    await assert.rejects(complete(duplicateId), /already exists|already has/);
    assert.deepEqual(await manager.inspect(provider, provider === 'hive' ? 'hive' : 'telemetry', 'prod', table), native);
  }
  assert.deepEqual(errors, []);
  writeFileSync(resolve(evidence, 'result.json'), JSON.stringify({ table, results }, null, 2));
  console.log(JSON.stringify({ result: 'passed', evidence, runs: results.map((run) => ({ provider: run.registry.id, id: run.id })) }, null, 2));
} catch (error) {
  await page.screenshot({ path: resolve(evidence, 'failure.png'), fullPage: true });
  writeFileSync(resolve(evidence, 'failure.json'), JSON.stringify({ error: String(error), results, errors }, null, 2));
  throw error;
} finally {
  await browser.close();
}
