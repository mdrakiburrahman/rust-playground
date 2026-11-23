import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { chromium, type Page } from 'playwright';
import { digest, generatorPath, type ContractRun } from '../src/contract.js';

const baseURL = process.env.CONTRACT_UI_URL ?? 'http://localhost:4173';
const fabric = process.env.CONTRACT_UI_FRONTEND === 'fabric';
const lakehouse = process.env.CONTRACT_UI_FRONTEND === 'lakehouse';
const protoc = fabric || lakehouse;
const frontendURL = `${baseURL}${lakehouse ? '/lakehouse' : fabric ? '/fabric' : '/'}`;
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
  const response = await page.request.get(`${baseURL}/api/runs`, { maxRetries: 2 });
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
  await page.locator('#catalog-tab').click();
  await page.locator('#add-table').click();
  await page.getByLabel('Table', { exact: true }).fill(table);
  const enumControl = page.locator('.property-editor').filter({
    has: page.locator(`[data-key][value=${JSON.stringify(option.key)}]`),
  }).getByLabel('Property value', { exact: true });
  assert.equal(await enumControl.inputValue(), option.declared_default, 'UI reads the compiled native default');
  if (registry === 'hosted') await enumControl.selectOption(option.enum_values.find((value) => value !== option.declared_default)!);
  await page.getByLabel('Property to add', { exact: true }).selectOption('custom');
  await page.locator('#add-property').click();
  await page.getByLabel('Property key', { exact: true }).last().fill('vendor.owner');
  await page.getByLabel('Property value', { exact: true }).last().fill('portable contract');
  assert.match(await page.getByLabel('DDL preview', { exact: true }).inputValue(), /vendor\.owner.*portable contract/);
  const accepted = page.waitForResponse((response) =>
    response.url().endsWith('/api/runs') && response.request().method() === 'POST');
  await page.locator('#create-submit').click();
  const response = await accepted;
  assert.equal(response.status(), 202);
  const run: ContractRun = await response.json();
  assert.equal(run.request.codegen, protoc ? 'protoc' : undefined, 'frontends retain their independent compiler defaults');
  if (protoc) assert.equal(run.request.protoEntry, 'telemetry.proto');
  assert.equal(run.request.mode === 'create' ? run.request.definitionsDigest : undefined, definitionsDigest);
  return run;
}
try {
  await page.goto(frontendURL);
  await page.getByLabel('Registry', { exact: true }).locator('option[value="hosted"]').waitFor({ state: 'attached' });
  const config = await (await page.request.get(`${baseURL}/api/config`)).json();
  const unsignedEnvironment = { ...process.env };
  delete unsignedEnvironment.ARROWCOL_CATALOG_DDL_IDENTITY_SECRET_FILE;
  assert.throws(() => execFileSync(generatorPath(process.cwd()), [
    'catalog', 'list', '--endpoint', config.registries.find((registry: { id: string }) => registry.id === 'local').endpoint,
  ], { encoding: 'utf8', env: unsignedEnvironment, stdio: 'pipe' }), /requires a valid signed internal identity/,
  'owned catalogues require signed identity, not merely a listening port');
  const definitions = await (await page.request.get(`${baseURL}/api/properties`)).json();
  definitionsDigest = definitions.definitions_sha256;
  option = definitions.options.find((item: { kind: string; declared_default: string | null }) => item.kind === 'enum' && item.declared_default !== null);
  assert.ok(option && option.enum_values.length > 1, 'compiled option definition exposes native default and alternatives');
  const unauthorized = await page.request.post(`${baseURL}/api/runs`, { data: { registry: 'local', mode: 'create', ddl: 'bad' } });
  assert.equal(unauthorized.status(), 400, 'mutation without session token rejected');
  const foreignOrigin = await page.request.get(`${baseURL}/api/config`, { headers: { Origin: 'https://untrusted.invalid' } });
  assert.equal(foreignOrigin.status(), 403);
  const localQueued = await submitForm(page, 'local');
  const [completed, inspected] = await Promise.allSettled([
    complete(localQueued.id, 'local', true),
    (async () => {
      await page.getByRole('button', { name: /^Workflow runs/ }).click();
      await page.locator(`[data-run="${localQueued.id}"]`).waitFor();
      await page.locator(`[data-pipeline="${localQueued.id}"]`).click();
      await page.getByRole('dialog', { name: 'Workflow pipeline', exact: true }).waitFor();
      await page.locator('.pipeline-node.running').first().waitFor({ timeout: 60_000 });
      await page.screenshot({ path: resolve(evidence, 'pipeline-running.png'), fullPage: true });
      await page.reload();
      await page.getByRole('dialog', { name: 'Workflow pipeline', exact: true }).waitFor();
      assert.ok(page.url().endsWith(`#run=${localQueued.id}`), 'selected workflow survives reload');
    })(),
  ]);
  if (completed.status === 'rejected') throw completed.reason;
  if (inspected.status === 'rejected') throw inspected.reason;
  const local = completed.value;
  await page.locator('.pipeline-node.succeeded [data-phase="publishing"]').waitFor({ timeout: 10_000 });
  await page.locator('[data-phase="preparing"]').click();
  await page.waitForFunction(() => document.getElementById('pipeline-output')?.textContent?.includes('syntax = "proto2"'));
  await page.locator('#pipeline-document').selectOption('ddl');
  await page.waitForFunction((name) => document.getElementById('pipeline-output')?.textContent?.includes(name), table);
  if (protoc) {
    await page.locator('[data-phase="validating-proto"]').click();
    await page.waitForFunction(() => document.getElementById('pipeline-output')?.textContent?.includes('arrowcol_codegen'));
    assert.equal(await page.locator('.pipeline-node.succeeded [data-phase="validating-proto"]').count(), 1);
  } else {
    assert.equal(await page.locator('.pipeline-node.skipped [data-phase="validating-proto"]').count(), 1);
  }
  await page.screenshot({ path: resolve(evidence, 'pipeline-complete.png'), fullPage: true });
  await page.getByRole('button', { name: 'Close workflow pipeline', exact: true }).click();
  const localSnapshot = await tables('local');
  assert.equal(localSnapshot.tables.find((item) => item.table === qualified)?.properties['vendor.owner'], 'portable contract');
  assert.equal(localSnapshot.tables.find((item) => item.table === qualified)?.properties[option.key], option.declared_default);
  assert.ok(!(await tables('hosted')).tables.some((item) => item.table === qualified), 'hosted service has independent state');

  await page.locator('#catalog-tab').click();
  await page.locator('#refresh').click();
  await page.locator(`[data-table="${qualified}"]`).click();
  if (lakehouse) {
    assert.equal(await page.locator('#table-tab-name').innerText(), table);
    assert.equal(await page.locator('.schema-grid tbody tr').count(), 3);
    assert.equal(await page.locator('.suite-rail button:not(:disabled)').count(), 0, 'surrounding Fabric navigation is visual only');
    assert.equal(await page.locator('.suite-rail a[href="/fabric"]').count(), 1, 'OneLake catalog remains functional');
    await page.locator('#properties-view').click();
    assert.match(await page.locator('#lakehouse-properties').innerText(), /portable contract/);
    await page.locator('#contract-view').click();
    assert.ok((await page.locator('#lakehouse-contract').innerText()).includes(local.pin!.fingerprint));
    await page.locator('#schema-view').click();
    const schemaBranch = page.locator('[data-branch="schema:telemetry.prod"] > summary');
    await schemaBranch.click();
    assert.equal(await page.locator(`[data-table="${qualified}"]`).isVisible(), false);
    await schemaBranch.click();
    await page.locator('#filter').fill(table);
    assert.equal(await page.locator('[data-table]').count(), 1);
    await page.locator('#filter').fill('');
    await page.locator('#toggle-explorer').click();
    assert.equal(await page.locator('#explorer-content').isVisible(), false);
    await page.locator('#toggle-explorer').click();
    assert.equal(await page.locator('#explorer-content').isVisible(), true);
    await page.reload();
    await page.locator(`[data-table="${qualified}"][aria-pressed=true]`).waitFor();
    assert.equal(new URL(page.url()).searchParams.get('table'), qualified, 'table selection survives reload and supports catalog deep links');
    await page.screenshot({ path: resolve(evidence, 'lakehouse-explorer.png'), fullPage: true });
    await page.setViewportSize({ width: 820, height: 944 });
    assert.equal(await page.evaluate(() => document.documentElement.scrollWidth <= innerWidth), true, 'narrow shell keeps scrolling inside the table pane');
    await page.screenshot({ path: resolve(evidence, 'lakehouse-narrow.png'), fullPage: true });
    await page.setViewportSize({ width: 1440, height: 1000 });
  }
  if (fabric) {
    await page.locator('#detail-panel').waitFor();
    await page.getByRole('button', { name: 'Close table details', exact: true }).click();
    await page.locator(`[data-favorite="${qualified}"]`).click();
    await page.locator('#favorites').click();
    assert.equal(await page.locator(`[data-table="${qualified}"]`).count(), 1);
    await page.reload();
    await page.locator('#favorites').click();
    await page.locator(`[data-table="${qualified}"]`).click();
    await page.locator('#detail-panel').waitFor();
  }
  const rebuildAccepted = page.waitForResponse((response) => response.url().endsWith('/api/runs') && response.request().method() === 'POST');
  if (lakehouse) await page.locator('#build-selected').click();
  else await page.getByRole('button', { name: 'Build committed v1', exact: true }).click();
  const rebuildQueued: ContractRun = await (await rebuildAccepted).json();
  const rebuilt = await complete(rebuildQueued.id, 'local', false);
  assert.equal(rebuilt.request.codegen, protoc ? 'protoc' : undefined);
  assert.equal(rebuilt.registration, 'existing');
  assert.equal(rebuilt.artifact?.sha256, local.artifact?.sha256, 'same pinned version packages identically');
  assert.equal(rebuilt.artifact?.path, local.artifact?.path, 'immutable release reused without overwrite');
  await page.goto(`${frontendURL}#run=${rebuilt.id}`);
  await page.locator('.pipeline-node.skipped [data-phase="promoting"]').waitFor();
  await page.getByRole('button', { name: 'Close workflow pipeline', exact: true }).click();

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
  if (protoc) assert.ok(entries.split('\n').includes('compiler/telemetry.proto'), 'configured protoc entry is packaged');
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
  await page.locator('#catalog-tab').click();
  if (fabric) await page.locator('#all-items').click();
  await page.locator('#refresh').click();
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
  await page.goto(`${frontendURL}#run=${rejected.id}`);
  await page.locator('.pipeline-node.failed [data-phase="preparing"]').waitFor();
  assert.match(await page.locator('#pipeline-attempts').innerText(), /protobuf field-name collision/);
  assert.equal(await page.locator('.pipeline-node.succeeded [data-phase="promoting"]').count(), 0);
  await page.screenshot({ path: resolve(evidence, 'pipeline-failed.png'), fullPage: true });
  await page.getByRole('button', { name: 'Close workflow pipeline', exact: true }).click();
  const legacy = (await runs()).find((run) => !run.steps);
  if (legacy) {
    await page.goto(`${frontendURL}#run=${legacy.id}`);
    await page.locator('#pipeline-legacy').waitFor();
    assert.equal(await page.locator('.pipeline-node.succeeded [data-phase="verifying"]').count(), 0);
  }
  const invalidDocument = await page.request.get(`${baseURL}/api/runs/${local.id}/documents/identity.key`);
  assert.equal(invalidDocument.status(), 400);
  assert.deepEqual(browserErrors, []);
  const report = { frontendURL, qualified, local, rebuilt, hosted, expectedPreparationFailure: failure, evidence, archiveEntries: entries.split('\n') };
  writeFileSync(resolve(evidence, 'result.json'), JSON.stringify(report, null, 2));
  console.log(JSON.stringify({ result: 'passed', qualified, local: local.id, rebuilt: rebuilt.id, hosted: hosted.id, expectedPreparationFailure: failure?.id, artifact: hosted.artifact, evidence }, null, 2));
} catch (error) {
  await page.screenshot({ path: resolve(evidence, 'failure.png'), fullPage: true });
  writeFileSync(resolve(evidence, 'failure.json'), JSON.stringify({
    error: String(error), url: page.url(), browserErrors, text: await page.locator('body').innerText(),
  }, null, 2));
  throw error;
} finally {
  await browser.close();
}
