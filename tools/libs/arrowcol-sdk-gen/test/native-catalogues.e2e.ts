import assert from 'node:assert/strict';
import { mkdirSync, readdirSync, readFileSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { chromium } from 'playwright';

const baseURL = process.env.CONTRACT_UI_URL ?? 'http://localhost:4173';
const root = resolve('.temp/contract-builder/native');
const evidence = resolve('.temp/contract-builder/evidence', `native-inspection-${Date.now()}`);
mkdirSync(evidence, { recursive: true });
const browser = await chromium.launch({ args: ['--no-sandbox'] });
const page = await browser.newPage({ viewport: { width: 1440, height: 1000 } });
page.setDefaultTimeout(90_000);
const errors: string[] = [];
const invalidRequests: string[] = [];
page.on('pageerror', (error) => errors.push(error.message));
page.on('request', (request) => {
  const url = new URL(request.url());
  if ((url.pathname === '/api/runs' && request.method() === 'POST') ||
    (url.pathname === '/api/tables' && url.searchParams.get('registry')?.startsWith('native:'))) {
    invalidRequests.push(request.url());
  }
});
try {
  const config: { registries: { id: string; native?: { provider: string } }[] } =
    await (await page.request.get(`${baseURL}/api/config`)).json();
  for (const frontend of ['/', '/fabric', '/lakehouse']) {
    await page.goto(`${baseURL}${frontend}`);
    await page.locator('#registry option[value=hosted]').waitFor({ state: 'attached' });
    await page.locator('#inspect-connections').click();
    await page.locator('#inspect-connection option[value="native:unity"]').waitFor({ state: 'attached' });
    await page.waitForFunction(() => document.getElementById('connection-metadata')?.textContent?.includes('"tables"'));
    await page.locator('#close-connections').click();
    for (const provider of ['unity', 'hive']) {
      const file = readdirSync(root).filter((name) => name.startsWith(`${provider}-arrowcol_check_`) && name.endsWith('.json')).sort().at(-1);
      assert.ok(file, `Run native-catalogues --action=verify --provider=${provider} first`);
      const receipt: { namespace: string; persistence: string; table?: unknown } = JSON.parse(readFileSync(resolve(root, file), 'utf8'));
      assert.equal(receipt.persistence, 'verified');
      assert.ok(receipt.table, 'native table persistence was verified, not just namespace creation');
      const buildRegistry = config.registries.find((registry) => registry.native?.provider === provider);
      const selectedRegistry = buildRegistry?.id ?? `native:${provider}`;
      await page.locator('#registry').selectOption(selectedRegistry);
      if (buildRegistry) {
        assert.equal(await page.locator('#add-table').isEnabled(), true);
        await page.locator('#inspect-connections').click();
        await page.locator('#inspect-connection').selectOption(`native:${provider}`);
      }
      await page.waitForFunction((provider) =>
        (document.getElementById('inspect-connection') as HTMLSelectElement)?.value === `native:${provider}`, provider);
      await page.waitForFunction(() => {
        const text = document.getElementById('connection-metadata')?.textContent;
        return text && !text.includes('Reading catalogue metadata...') &&
          /Native builds enabled|Enable builds with/.test(document.getElementById('connection-status')?.textContent ?? '');
      }, undefined, { timeout: 90_000 });
      assert.equal(await page.locator('#add-table').isEnabled(), !!buildRegistry);
      if (frontend === '/fabric') assert.equal(await page.locator('#create-nav').isEnabled(), !!buildRegistry);
      if (!buildRegistry) {
        if (frontend === '/lakehouse') assert.equal(await page.locator('#build-selected').isDisabled(), true);
        assert.equal(await page.locator('[data-table]').count(), 0, 'inspection-only selection must not show Rust tables');
      }
      assert.match(await page.locator('#connection-status').innerText(), /Native builds enabled|Enable builds with/);
      assert.doesNotMatch(await page.locator('#connection-metadata').innerText(), /Inspection failed/);
      await page.getByLabel('Native catalogue', { exact: true }).fill(provider === 'unity' ? receipt.namespace : 'hive');
      await page.getByLabel('Native schema / database', { exact: true }).fill(provider === 'unity' ? 'contracts' : receipt.namespace);
      await page.getByLabel('Native table', { exact: true }).fill('example_events');
      const started = Date.now();
      const [response] = await Promise.all([
        page.waitForResponse((response) =>
          response.url().includes(`/api/native-catalogues/${provider}/inspect?`) && response.url().includes('example_events')),
        page.locator('#inspect-metadata').click(),
      ]);
      assert.equal(response.status(), 200);
      await page.waitForFunction(() => document.getElementById('connection-metadata')?.textContent?.includes('example_events'));
      const metadata = await page.locator('#connection-metadata').innerText();
      assert.match(metadata, /payload/);
      assert.match(metadata, /bigint/);
      console.log(JSON.stringify({ frontend, provider, metadataMs: Date.now() - started }));
      assert.match(await page.locator(`#inspect-connection option[value="native:${provider}"]`).innerText(), /metadata inspector/);
      await page.screenshot({ path: resolve(evidence, `${frontend === '/' ? 'original' : frontend.slice(1)}-${provider}.png`), fullPage: true });
      await page.locator('#close-connections').click();
      await page.reload();
      await page.waitForFunction((selectedRegistry) =>
        (document.getElementById('registry') as HTMLSelectElement)?.value === selectedRegistry, selectedRegistry);
      assert.equal(await page.locator('#add-table').isEnabled(), !!buildRegistry, 'native build capability survives reload');
      await page.locator('#inspect-connections').click();
      if (buildRegistry) await page.locator('#inspect-connection').selectOption(`native:${provider}`);
      await page.waitForFunction((provider) =>
        (document.getElementById('inspect-connection') as HTMLSelectElement)?.value === `native:${provider}`, provider);
      await page.locator('#close-connections').click();
    }
    for (const registry of ['local', 'hosted']) {
      await page.locator('#registry').selectOption(registry);
      await page.waitForFunction(() => document.querySelectorAll('[data-table]').length > 0);
      assert.equal(await page.locator('#add-table').isEnabled(), true);
      await page.locator('#add-table').click();
      assert.match(await page.locator('#create-registry').innerText(), registry === 'local' ? /Local catalogue/ : /Hosted simulation/);
      await page.locator('#close-dialog').click();
    }
    if (frontend === '/fabric') {
      const table = page.locator('[data-table]').first();
      const qualified = await table.getAttribute('data-table');
      await table.click();
      const link = page.getByRole('link', { name: 'Open in Lakehouse Explorer', exact: true });
      assert.equal(new URL((await link.getAttribute('href'))!, baseURL).searchParams.get('table'), qualified);
      await link.click();
      await page.waitForFunction((qualified) =>
        document.querySelector('[data-table][aria-pressed=true]')?.getAttribute('data-table') === qualified, qualified);
      assert.equal(new URL(page.url()).searchParams.get('registry'), 'hosted');
      assert.equal(await page.locator('.schema-grid tbody tr').count() > 0, true, 'OneLake catalog opens the actual selected table in Explorer');
    }
  }
  const injection = await page.request.get(`${baseURL}/api/native-catalogues/hive/inspect?catalog=hive&schema=bad%3BDROP`);
  assert.equal(injection.status(), 400);
  assert.deepEqual(errors, []);
  assert.deepEqual(invalidRequests, [], 'inspection must never submit builds or route native IDs through the Rust table API');
  writeFileSync(resolve(evidence, 'result.json'), JSON.stringify({ result: 'passed', baseURL, evidence }, null, 2));
  console.log(JSON.stringify({ result: 'passed', baseURL, evidence }, null, 2));
} catch (error) {
  await page.screenshot({ path: resolve(evidence, 'failure.png'), fullPage: true });
  writeFileSync(resolve(evidence, 'failure.json'), JSON.stringify({
    error: String(error), url: page.url(), errors, text: await page.locator('body').innerText(),
  }, null, 2));
  throw error;
} finally { await browser.close(); }
