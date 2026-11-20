import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { chromium, type Page } from 'playwright';
import { digest, generatorPath, type ContractRun } from '../src/contract.js';

const baseURL = process.env.CONTRACT_UI_URL ?? 'http://localhost:4173';
const table = `verified_${Date.now()}`;
const qualified = `telemetry.prod.${table}`;
const evidence = resolve('.temp/contract-builder/evidence', table);
mkdirSync(evidence, { recursive: true });
const browser = await chromium.launch({ args: ['--no-sandbox'] });
const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
const browserErrors: string[] = [];
let option: { key: string; enum_values: string[]; declared_default: string };
let definitionsDigest: string;
page.on('pageerror', (error) => browserErrors.push(error.message));

async function runs(): Promise<ContractRun[]> {
  const response = await page.request.get(`${baseURL}/api/runs`);
  assert.equal(response.status(), 200);
  return response.json();
}
async function tables(registry: string): Promise<{ tables: { table: string; version: number; properties: Record<string, string> }[] }> {
  const response = await page.request.get(`${baseURL}/api/tables?registry=${registry}`);
  assert.equal(response.status(), 200);
  return response.json();
}
async function complete(id: string, registry: string, expectAbsentBeforePromotion: boolean): Promise<ContractRun> {
  let observedUnregistered = false;
  const deadline = Date.now() + 15 * 60_000;
  while (Date.now() < deadline) {
    const run = (await runs()).find((item) => item.id === id)!;
    if (expectAbsentBeforePromotion && run.status === 'running' && run.registration === 'not-attempted') {
      const snapshot = await tables(registry);
      if (!snapshot.tables.some((item) => item.table === qualified)) observedUnregistered = true;
    }
    if (run.status === 'failed') {
      const log = await page.request.get(`${baseURL}/api/runs/${id}/log`);
      throw new Error(`${run.phase}: ${run.error}\n${await log.text()}`);
    }
    if (run.status === 'succeeded') {
      assert.ok(run.artifact);
      if (expectAbsentBeforePromotion) assert.ok(observedUnregistered, 'observed the candidate build before authoritative registration');
      return run;
    }
    await delay(1000);
  }
  throw new Error(`Build timed out: ${id}`);
}
async function submitForm(page: Page, registry: string): Promise<ContractRun> {
  await page.getByLabel('Registry', { exact: true }).selectOption(registry);
  await page.getByRole('button', { name: '+ Add table', exact: true }).click();
  await page.getByLabel('Table', { exact: true }).fill(table);
  const enumControl = page.locator('.property-editor').filter({
    has: page.locator(`[data-key][value=${JSON.stringify(option.key)}]`),
  }).getByLabel('Property value', { exact: true });
  assert.equal(await enumControl.inputValue(), option.declared_default, 'UI reads the compiled native default');
  if (registry === 'hosted') await enumControl.selectOption(option.enum_values.find((value) => value !== option.declared_default)!);
  await page.getByLabel('Property to add', { exact: true }).selectOption('custom');
  await page.getByRole('button', { name: '+ Add property', exact: true }).click();
  await page.getByLabel('Property key', { exact: true }).last().fill('vendor.owner');
  await page.getByLabel('Property value', { exact: true }).last().fill('portable contract');
  assert.match(await page.getByLabel('DDL preview', { exact: true }).inputValue(), /vendor\.owner.*portable contract/);
  const accepted = page.waitForResponse((response) =>
    response.url().endsWith('/api/runs') && response.request().method() === 'POST');
  await page.getByRole('button', { name: 'Create and build', exact: true }).click();
  const response = await accepted;
  assert.equal(response.status(), 202);
  const run: ContractRun = await response.json();
  assert.equal(run.request.mode === 'create' ? run.request.definitionsDigest : undefined, definitionsDigest);
  return run;
}
try {
  await page.goto(baseURL);
  await page.getByLabel('Registry', { exact: true }).locator('option[value="hosted"]').waitFor({ state: 'attached' });
  const definitions = await (await page.request.get(`${baseURL}/api/properties`)).json();
  definitionsDigest = definitions.definitions_sha256;
  option = definitions.options.find((item: { kind: string; declared_default: string | null }) => item.kind === 'enum' && item.declared_default !== null);
  assert.ok(option && option.enum_values.length > 1, 'compiled option definition exposes native default and alternatives');
  const unauthorized = await page.request.post(`${baseURL}/api/runs`, { data: { registry: 'local', mode: 'create', ddl: 'bad' } });
  assert.equal(unauthorized.status(), 400, 'mutation without session token rejected');
  const foreignOrigin = await page.request.get(`${baseURL}/api/config`, { headers: { Origin: 'https://untrusted.invalid' } });
  assert.equal(foreignOrigin.status(), 403);
  const localQueued = await submitForm(page, 'local');
  await page.reload();
  await page.getByRole('button', { name: /^Workflow runs/ }).click();
  await page.locator(`[data-run="${localQueued.id}"]`).waitFor();
  const local = await complete(localQueued.id, 'local', true);
  const localSnapshot = await tables('local');
  assert.equal(localSnapshot.tables.find((item) => item.table === qualified)?.properties['vendor.owner'], 'portable contract');
  assert.equal(localSnapshot.tables.find((item) => item.table === qualified)?.properties[option.key], option.declared_default);
  assert.ok(!(await tables('hosted')).tables.some((item) => item.table === qualified), 'hosted service has independent state');

  await page.getByRole('button', { name: 'Overview', exact: true }).click();
  await page.getByRole('button', { name: 'Refresh', exact: true }).click();
  await page.locator(`[data-table="${qualified}"]`).click();
  const rebuildAccepted = page.waitForResponse((response) => response.url().endsWith('/api/runs') && response.request().method() === 'POST');
  await page.getByRole('button', { name: 'Build committed v1', exact: true }).click();
  const rebuildQueued: ContractRun = await (await rebuildAccepted).json();
  const rebuilt = await complete(rebuildQueued.id, 'local', false);
  assert.equal(rebuilt.registration, 'existing');
  assert.equal(rebuilt.artifact?.sha256, local.artifact?.sha256, 'same pinned version packages identically');
  assert.equal(rebuilt.artifact?.path, local.artifact?.path, 'immutable release reused without overwrite');

  const hostedQueued = await submitForm(page, 'hosted');
  const hosted = await complete(hostedQueued.id, 'hosted', true);
  assert.notEqual(hosted.registry.authority, local.registry.authority);
  assert.notEqual(hosted.artifact?.path, local.artifact?.path);
  assert.equal(hosted.pin?.fingerprint, local.pin?.fingerprint, 'same physical schema independent of authority/policy location');

  await page.reload();
  await page.getByRole('button', { name: /^Workflow runs/ }).click();
  const runCard = page.locator(`[data-run="${hosted.id}"]`);
  await runCard.getByRole('link', { name: 'Download SDK + contract' }).waitFor();
  const downloadEvent = page.waitForEvent('download');
  await runCard.getByRole('link', { name: 'Download SDK + contract' }).click();
  const download = await downloadEvent;
  const downloaded = resolve(evidence, 'contract-sdk.tar.gz');
  await download.saveAs(downloaded);
  assert.equal(digest(readFileSync(downloaded)), hosted.artifact!.sha256);
  const entries = execFileSync('tar', ['-tzf', downloaded], { encoding: 'utf8' });
  for (const entry of ['contract/manifest.json', 'contract/schema.proto', 'contract/options.proto', 'contract/google/protobuf/descriptor.proto', 'contract/descriptor.pb', 'sdk/rust/Cargo.toml', 'sdk/typescript/package.json', 'release.json']) {
    assert.ok(entries.split('\n').includes(entry), `archive includes ${entry}`);
  }
  execFileSync('tar', ['-xzf', downloaded, '-C', evidence, 'contract/descriptor.pb']);
  const portable = JSON.parse(execFileSync(generatorPath(process.cwd()), [
    'catalog', 'metadata', '--descriptor', resolve(evidence, 'contract/descriptor.pb'),
  ], { encoding: 'utf8' }));
  assert.equal(portable.properties['vendor.owner'], 'portable contract', 'descriptor-only recovery preserves arbitrary metadata');
  assert.equal(portable.properties[option.key], option.enum_values.find((value) => value !== option.declared_default), 'explicit enum override survives portable descriptor export');
  const release = JSON.parse(execFileSync('tar', ['-xOf', downloaded, 'release.json'], { encoding: 'utf8' }));
  assert.equal(release.provenance.optionDefinitionsSha256, definitionsDigest, 'release pins compiled option definitions');
  await runCard.getByRole('button', { name: 'View logs' }).click();
  await page.getByLabel('Workflow logs').waitFor();
  assert.match(await page.getByLabel('Workflow logs').innerText(), /promote/);
  await page.screenshot({ path: resolve(evidence, 'workflow-runs.png'), fullPage: true });
  await page.getByRole('button', { name: 'Overview', exact: true }).click();
  await page.getByRole('button', { name: 'Refresh', exact: true }).click();
  await page.locator(`[data-table="${qualified}"]`).click();
  await page.screenshot({ path: resolve(evidence, 'catalogue.png'), fullPage: true });
  const { token } = await (await page.request.get(`${baseURL}/api/config`)).json();
  const rejectedTable = `telemetry.prod.${table}_rejected`;
  const rejectedResponse = await page.request.post(`${baseURL}/api/runs`, {
    headers: { 'X-Contract-Token': token },
    data: {
      registry: 'local', mode: 'create', definitionsDigest,
      ddl: `CREATE TABLE ${rejectedTable} (\`a-b\` STRING, a_b STRING) USING DELTA`,
    },
  });
  assert.equal(rejectedResponse.status(), 202);
  const rejected: ContractRun = await rejectedResponse.json();
  let failure: ContractRun | undefined;
  for (let attempt = 0; attempt < 120; attempt++) {
    const current = (await runs()).find((run) => run.id === rejected.id)!;
    if (['failed', 'succeeded'].includes(current.status)) { failure = current; break; }
    await delay(500);
  }
  assert.equal(failure?.status, 'failed');
  assert.equal(failure?.phase, 'preparing', failure?.error ?? '');
  assert.match(failure?.error ?? '', /protobuf field-name collision/);
  assert.equal(failure?.registration, 'not-attempted');
  assert.equal(failure?.artifact, undefined);
  assert.ok(!(await tables('local')).tables.some((item) => item.table === rejectedTable), 'candidate descriptor rejection leaves no authoritative table');
  assert.deepEqual(browserErrors, []);
  const report = { baseURL, qualified, local, rebuilt, hosted, expectedPreparationFailure: failure, evidence, archiveEntries: entries.split('\n') };
  writeFileSync(resolve(evidence, 'result.json'), JSON.stringify(report, null, 2));
  console.log(JSON.stringify({ result: 'passed', qualified, local: local.id, rebuilt: rebuilt.id, hosted: hosted.id, expectedPreparationFailure: failure?.id, artifact: hosted.artifact, evidence }, null, 2));
} finally {
  await browser.close();
}
