import { spawnSync } from 'node:child_process';
import { readFileSync, rmSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';

import { chromium, type Browser, type Locator, type Page } from 'playwright';

import { safeKustoError } from './kusto.js';
import { metricRangeEvidence, panelDiagnostics, panelFrameEvidence } from './metrics-gate.js';
import { requiredMetrics } from './metric-contract.js';

export const kustoExplorerStartArguments = [
  '--profile', 'benchmark', 'up', '--detach', '--no-build', '--no-deps', '--wait', '--wait-timeout', '120', 'kusto-explorer',
];

/** The UI and its upstream as addressed inside the Compose network. */
export const kustoExplorerUrl = 'http://kusto-explorer:8080/';
export const kustoExplorerClusterUri = 'http://kusto:8080';

export function tableSummaryQuery(names: Iterable<string>): string {
  const tables = [...names].sort();
  if (tables.length === 0 || tables.some(name => !/^[A-Za-z_][A-Za-z0-9_]*$/u.test(name))) {
    throw new Error('External-table summary requires safe validated table names');
  }
  return [
    `union ${tables.map(name =>
      `(external_table('${name}') | summarize Rows=count() | extend TableName='${name}')`).join(',\n')}`,
    '| project TableName, Rows',
    '| order by TableName asc',
  ].join('\n');
}

const browserProfile = ['--profile', 'ui-gate'];
const viewport = { width: 1600, height: 960 };
const queryTimeoutMs = 120_000;

export interface UiGateOptions {
  workspace: string;
  results: string;
  /** Playwright run-server endpoint of the `playwright` Compose service. */
  browserEndpoint: string;
  compose: (args: string[], timeout?: number) => string;
  log: (message: string) => void;
  metricScope?: 'source-sdk' | 'service-qualification';
  bridgeEnabled?: boolean;
}

export interface UiGateResult {
  tables: number;
  rows: number;
}

interface Evidence {
  passed: boolean;
  url: string;
  cluster_uri: string;
  started_at: string;
  finished_at?: string;
  checks: Record<string, unknown>;
  screenshots: string[];
  console_errors: string[];
  error?: string;
}

/** Reads the per-table row counts that the loadgen validator proved are in Kusto. */
export function expectedTableRows(validation: unknown): Map<string, number> {
  if (!validation || typeof validation !== 'object') throw new Error('kusto-validation.json is not an object');
  const document = validation as { passed?: unknown; tables?: unknown };
  if (document.passed !== true) throw new Error('kusto-validation.json did not pass; the UI gate needs validated Kusto data');
  if (!document.tables || typeof document.tables !== 'object' || Array.isArray(document.tables)) {
    throw new Error('kusto-validation.json has no tables');
  }
  const expected = new Map<string, number>();
  for (const [key, table] of Object.entries(document.tables)) {
    const name = /^\['(.+)'\]$/u.exec(key)?.[1] ?? key;
    const rows = (table as { expected_rows?: unknown } | null)?.expected_rows;
    if (!/^[A-Za-z_][A-Za-z0-9_]*$/u.test(name) || typeof rows !== 'number' || !Number.isSafeInteger(rows) || rows < 0) {
      throw new Error(`kusto-validation.json has an invalid entry for ${JSON.stringify(key)}`);
    }
    expected.set(name, rows);
  }
  if (expected.size === 0) throw new Error('kusto-validation.json lists no tables');
  return expected;
}

/** Compares a JSON export of {@link tableSummaryQuery} with the validated row counts. */
export function tableRowMismatches(expected: ReadonlyMap<string, number>, exported: unknown): string[] {
  if (!Array.isArray(exported)) return ['the JSON export is not an array of rows'];
  const actual = new Map<string, unknown>();
  for (const row of exported) {
    const record = row as { TableName?: unknown; Rows?: unknown } | null;
    if (typeof record?.TableName !== 'string') return ['the JSON export has a row without a TableName'];
    actual.set(record.TableName, record.Rows);
  }
  const problems: string[] = [];
  for (const [name, rows] of expected) {
    if (!actual.has(name)) problems.push(`${name}: missing from the UI results`);
    else if (actual.get(name) !== rows) problems.push(`${name}: the UI shows ${JSON.stringify(actual.get(name))} rows, expected ${rows}`);
  }
  for (const name of actual.keys()) {
    if (!expected.has(name)) problems.push(`${name}: returned by the UI but absent from kusto-validation.json`);
  }
  return problems;
}

function docker(args: string[], cwd: string): void {
  const result = spawnSync('docker', args, { cwd, encoding: 'utf8', timeout: 300_000 });
  if (result.error || result.status !== 0) {
    throw new Error(`docker ${args[0]} failed: ${result.error?.message ?? result.status}\n${result.stderr}`);
  }
}

async function connectBrowser(endpoint: string, timeoutMs: number): Promise<Browser> {
  const deadline = Date.now() + timeoutMs;
  let lastError: unknown = new Error('deadline elapsed');
  while (Date.now() < deadline) {
    try {
      return await chromium.connect(endpoint, { timeout: 10_000 });
    } catch (error) {
      lastError = error;
      await delay(1_000);
    }
  }
  throw new Error(`Playwright browser server ${endpoint} did not accept connections: ${safeKustoError(lastError)}`);
}

/**
 * Creates the browser container, installs this workspace's playwright-core into it (so client and
 * server always match) and connects to its run-server over the Compose network.
 */
async function startBrowser(options: UiGateOptions): Promise<Browser> {
  options.compose([...browserProfile, 'create', '--force-recreate', 'playwright'], 900_000);
  const container = options.compose([...browserProfile, 'ps', '--all', '--quiet', 'playwright']).trim();
  if (!/^[0-9a-f]{12,64}$/u.test(container)) throw new Error('The Playwright browser container was not created');
  docker(['cp', '--follow-link', path.join(options.workspace, 'node_modules/playwright-core'), `${container}:/opt/playwright-core`],
    options.workspace);
  options.compose([...browserProfile, 'start', 'playwright']);
  return connectBrowser(options.browserEndpoint, 120_000);
}

async function runQuery(page: Page, query: string): Promise<void> {
  await page.locator('.monaco-editor').first().click();
  await page.keyboard.press('ControlOrMeta+A');
  await page.keyboard.press('Delete');
  await page.keyboard.insertText(query);
  await page.waitForTimeout(300);
  await page.keyboard.press('Escape');
  await page.locator('.suggest-widget.visible').waitFor({ state: 'hidden', timeout: 5_000 });
  const response = page.waitForResponse(
    (candidate) => new URL(candidate.url()).pathname === '/api/query' && candidate.request().method() === 'POST',
    { timeout: queryTimeoutMs },
  );
  await page.keyboard.press('F5');
  const completed = await response;
  if (!completed.ok()) throw new Error(`Query ${JSON.stringify(query.split('\n')[0])} returned HTTP ${completed.status()}`);
  await page.locator('[data-testid=running]').waitFor({ state: 'detached', timeout: queryTimeoutMs });
  if (await page.locator('[data-testid=query-error]').count() > 0) {
    throw new Error(`Query failed in the UI: ${await page.locator('[data-testid=query-error]').first().innerText()}`);
  }
  await page.locator('[data-testid=status-rows]').waitFor({ timeout: 10_000 });
}

function count(text: string): number {
  return Number(text.replace(/[^0-9]/gu, ''));
}

export async function expandExternalTableFolder(folder: Pick<Locator, 'waitFor' | 'getAttribute' | 'click'>): Promise<void> {
  await folder.waitFor({ state: 'visible', timeout: 60_000 });
  const expanded = await folder.getAttribute('aria-expanded');
  if (expanded === 'false') await folder.click();
  else if (expanded !== 'true') throw new Error('External tables folder has no expansion state');
}

/** Browser-only checks, also usable against an existing stack without changing its lifecycle. */
export async function exerciseExplorer(page: Page, expected: ReadonlyMap<string, number>, options: Pick<UiGateOptions, 'results'>, evidence: Evidence): Promise<UiGateResult> {
  const screenshot = async (name: string): Promise<void> => {
    await page.screenshot({ path: path.join(options.results, name) });
    evidence.screenshots.push(name);
  };

  await page.goto(kustoExplorerUrl, { waitUntil: 'domcontentloaded', timeout: 60_000 });
  const config = await page.evaluate(async () => {
    const [configResponse, readyResponse] = await Promise.all([fetch('/api/config'), fetch('/readyz')]);
    return { config: await configResponse.json() as unknown, ready: await readyResponse.json() as unknown };
  }) as { config: { connections?: Array<{ name?: string; clusterUri?: string }> }; ready: { ready?: boolean } };
  const connection = config.config.connections?.[0];
  evidence.checks.config = { connection: connection?.name, cluster_uri: connection?.clusterUri, ready: config.ready.ready };
  if (config.config.connections?.length !== 1 || connection?.clusterUri !== kustoExplorerClusterUri) {
    throw new Error(`kusto-explorer must reach Kusto only through ${kustoExplorerClusterUri}; /api/config reported ` +
      JSON.stringify(config.config.connections?.map((item) => item.clusterUri)));
  }
  if (config.ready.ready !== true) throw new Error('kusto-explorer /readyz reports Kusto as unreachable');

  await page.locator('[data-testid=status-connection][data-state=connected]').waitFor({ timeout: 60_000 });
  await expandExternalTableFolder(page.locator('[data-testid=tree-node][data-kind=folder][data-name="External tables"]'));
  await page.waitForFunction(
    (minimum) => document.querySelectorAll('[data-testid=tree-node][data-kind=external-table]').length >= minimum,
    expected.size,
    { timeout: 60_000 },
  );
  const treeTables = await page.locator('[data-testid=tree-node][data-kind=external-table]')
    .evaluateAll((nodes) => nodes.map((node) => node.getAttribute('data-name') ?? ''));
  const missing = [...expected.keys()].filter((name) => !treeTables.includes(name));
  evidence.checks.connection_tree = { tables: treeTables.length, missing };
  if (missing.length > 0) throw new Error(`The connection tree is missing ${missing.join(', ')}`);

  const summaryQuery = tableSummaryQuery(expected.keys());
  await runQuery(page, summaryQuery);
  const statusRows = count(await page.locator('[data-testid=status-rows]').innerText());
  if (statusRows !== expected.size) throw new Error(`The status bar shows ${statusRows} result rows, expected ${expected.size}`);
  await screenshot('kusto-explorer-results-dark.png');
  const exportFile = path.join(options.results, 'kusto-explorer-export.json');
  await page.locator('[data-testid=export-menu]').click();
  await page.locator('[data-testid=context-menu]').waitFor();
  const [download] = await Promise.all([
    page.waitForEvent('download', { timeout: 30_000 }),
    page.locator('[data-testid=menu-item]', { hasText: 'Export as JSON' }).click(),
  ]);
  await download.saveAs(exportFile);
  const exported = JSON.parse(readFileSync(exportFile, 'utf8')) as unknown;
  const mismatches = tableRowMismatches(expected, exported);
  const rows = [...expected.values()].reduce((total, value) => total + value, 0);
  evidence.checks.table_rows = { query: summaryQuery, status_bar_rows: statusRows, tables: expected.size, rows, mismatches };
  if (mismatches.length > 0) throw new Error(`UI results differ from kusto-validation.json: ${mismatches.join('; ')}`);
  for (const toast of await page.locator('[data-testid=toast] button[aria-label=Dismiss]').all()) await toast.click();
  await page.locator('[data-testid=toast]').first().waitFor({ state: 'detached', timeout: 10_000 });

  const management = '.show external tables | count';
  await runQuery(page, management);
  const tableCount = count(await page.locator('[data-testid=grid-row] .kx-grid-cell').first().innerText());
  evidence.checks.management_command = { query: management, count: tableCount };
  if (tableCount !== treeTables.length) {
    throw new Error(`${management} returned ${tableCount}, but the connection tree lists ${treeTables.length} tables`);
  }

  const chart = `${summaryQuery}\n| render columnchart`;
  await runQuery(page, chart);
  await page.locator('[data-testid=chart] rect.kx-bar').first().waitFor({ timeout: 30_000 });
  const bars = await page.locator('[data-testid=chart] rect.kx-bar').count();
  evidence.checks.chart = { query: chart, bars };
  if (bars !== expected.size) throw new Error(`The column chart drew ${bars} bars, expected ${expected.size}`);
  await screenshot('kusto-explorer-chart-dark.png');

  const themes = [await page.evaluate(() => document.documentElement.dataset.theme)];
  await page.locator('[data-testid=theme-toggle]').click();
  await page.waitForFunction(() => document.documentElement.dataset.theme === 'light', null, { timeout: 10_000 });
  themes.push(await page.evaluate(() => document.documentElement.dataset.theme));
  evidence.checks.themes = themes;
  await page.locator('[data-testid=result-tab]').first().click();
  await screenshot('kusto-explorer-results-light.png');
  return { tables: expected.size, rows };
}

/**
 * Proves the validated loadtest data is queryable through kusto-explorer: a browser on the Compose
 * network opens http://kusto-explorer:8080 (which reaches Kusto at http://kusto:8080), checks the
 * connection tree, runs data, management and chart queries, compares an exported result with
 * kusto-validation.json and saves screenshots plus kusto-explorer-validation.json.
 */
export async function runUiGate(options: UiGateOptions): Promise<UiGateResult> {
  const evidence: Evidence = {
    passed: false,
    url: kustoExplorerUrl,
    cluster_uri: kustoExplorerClusterUri,
    started_at: new Date().toISOString(),
    checks: {},
    screenshots: [],
    console_errors: [],
  };
  for (const stale of ['kusto-explorer-failure.png', 'kusto-explorer-browser.log']) {
    rmSync(path.join(options.results, stale), { force: true });
  }

  let browser: Browser | undefined;
  let page: Page | undefined;
  try {
    const expected = expectedTableRows(JSON.parse(readFileSync(path.join(options.results, 'kusto-validation.json'), 'utf8')));
    options.log('Starting the Playwright browser on the Compose network');
    browser = await startBrowser(options);
    const context = await browser.newContext({ viewport, deviceScaleFactor: 1, colorScheme: 'dark', acceptDownloads: true });
    page = await context.newPage();
    page.on('console', (message) => {
      if (message.type() === 'error') evidence.console_errors.push(message.text().slice(0, 500));
    });
    page.on('pageerror', (error) => evidence.console_errors.push(`page error: ${error.message}`.slice(0, 500)));
    const result = await exerciseExplorer(page, expected, options, evidence);
    if (evidence.console_errors.length > 0) {
      throw new Error(`The page logged errors: ${evidence.console_errors.join(' | ')}`);
    }
    evidence.passed = true;
    return result;
  } catch (error) {
    evidence.error = safeKustoError(error);
    if (page) {
      await page.screenshot({ path: path.join(options.results, 'kusto-explorer-failure.png') })
        .then(() => evidence.screenshots.push('kusto-explorer-failure.png'), () => undefined);
    }
    try {
      writeFileSync(path.join(options.results, 'kusto-explorer-browser.log'),
        options.compose([...browserProfile, 'logs', '--no-color', '--timestamps', 'playwright']));
    } catch {
      // The browser container may never have been created.
    }
    throw new Error(`kusto-explorer UI gate failed: ${evidence.error}`);
  } finally {
    evidence.finished_at = new Date().toISOString();
    writeFileSync(path.join(options.results, 'kusto-explorer-validation.json'), `${JSON.stringify(evidence, null, 2)}\n`);
    await browser?.close().catch(() => undefined);
    try {
      options.compose([...browserProfile, 'rm', '--force', '--stop', 'playwright']);
    } catch (error) {
      options.log(`Unable to remove the Playwright browser container: ${safeKustoError(error)}`);
    }
  }
}

/** Checks actual Grafana datasource samples and rendered plots on the same dashboard. */
export async function runGrafanaGate(options: UiGateOptions): Promise<void> {
  const started = Date.now();
  const scope = options.metricScope ?? 'source-sdk';
  const end = Math.floor(started / 1000);
  const start = end - 300;
  const evidence: Record<string, unknown> = {
    passed: false, scope, started_at: new Date(started).toISOString(), start, end, metrics: {}, panels: [],
  };
  let browser: Browser | undefined;
  try {
    browser = await startBrowser(options);
    const context = await browser.newContext({ viewport, colorScheme: 'dark' });
    const page = await context.newPage();
    await page.goto('http://lgtm:3000/d/arrowcol-delta-operations?from=now-15m&to=now&refresh=5s', { timeout: 60_000 });
    const login = page.locator('input[name="user"]');
    if (await login.isVisible()) {
      await login.fill('admin');
      await page.locator('input[name="password"]').fill('admin');
      await page.getByRole('button', { name: /log in|sign in/iu }).click();
    }
    await page.getByText('Delta SDK and service operation counters', { exact: true }).waitFor({ timeout: 60_000 });
    const queryRange = async (query: string): Promise<unknown> =>
      page.evaluate(async ({ query, start, end }) => {
        const parameters = new URLSearchParams({ query, start: String(start), end: String(end), step: '2' });
        const response = await fetch(`/api/datasources/proxy/uid/prometheus/api/v1/query_range?${parameters}`);
        if (!response.ok) throw new Error(`Grafana datasource HTTP ${response.status}`);
        return response.json() as Promise<unknown>;
      }, { query, start, end });
    for (const metric of requiredMetrics(scope, options.bridgeEnabled ?? false)) {
      const result = await queryRange(metric.query);
      const measurement: Record<string, unknown> = { query: metric.query, allow_zero: metric.allowZero };
      (evidence.metrics as Record<string, unknown>)[metric.name] = measurement;
      try {
        Object.assign(measurement, metricRangeEvidence(result, 3, metric.allowZero));
      } catch (error) {
        throw new Error(`Missing or invalid required live metric ${metric.name}`, { cause: error });
      }
    }
    const dashboard = JSON.parse(readFileSync(path.join(options.workspace, 'projects/loadtest/observability/delta-operations.json'), 'utf8')) as {
      panels: Array<{ id: number; title: string; type: string; datasource: { uid: string }; targets: Array<{ expr: string }> }>;
    };
    const panelIds = scope === 'source-sdk' ? [1, 2, 6, 10, 11] : [1, 2, 3, 4, 5, 6, 7, 8, 10, 11, 12, 13, 14, 15];
    for (const id of panelIds) {
      evidence.active_panel = { id, status: 'pending' };
      const panel = dashboard.panels.find(panel => panel.id === id);
      if (!panel || panel.type !== 'timeseries' || panel.datasource?.uid !== 'prometheus' || !panel.targets.length) {
        throw new Error(`Missing required Grafana operation panel ${id} with the qualified datasource`);
      }
      const samples = [];
      for (const target of panel.targets) {
        if (typeof target.expr !== 'string' || !target.expr) throw new Error(`Missing panel ${id} datasource query`);
        samples.push({ query: target.expr, ...metricRangeEvidence(await queryRange(target.expr), 3, true) });
      }
      const [response] = await Promise.all([
        page.waitForResponse(response => {
          if (!response.url().includes('/api/ds/query') || response.request().method() !== 'POST') return false;
          const request: unknown = response.request().postDataJSON();
          return Boolean(request && typeof request === 'object' && 'queries' in request && Array.isArray(request.queries) &&
            request.queries.some((query: unknown) => query && typeof query === 'object' && 'expr' in query &&
              panel.targets.some(target => target.expr === query.expr)));
        }, { timeout: 30_000 }),
        page.goto(`http://lgtm:3000/d-solo/arrowcol-delta-operations?from=${start * 1000}&to=${end * 1000}&panelId=${id}`,
          { timeout: 60_000 }),
      ]);
      evidence.active_panel = { id, http_status: response.status() };
      if (!response.ok()) throw new Error(`Grafana panel ${id} datasource HTTP ${response.status()}`);
      const responseDocument: unknown = await response.json();
      evidence.active_panel = {
        id, http_status: response.status(), ...panelDiagnostics(responseDocument, response.request().postDataJSON()),
      };
      const delivered = panelFrameEvidence(responseDocument);
      await page.getByText(panel.title, { exact: true }).waitFor({ state: 'visible', timeout: 30_000 });
      await page.waitForFunction(() => [...document.querySelectorAll('canvas')].some(canvas => canvas.width > 100 && canvas.height > 100),
        null, { timeout: 30_000 });
      if (await page.getByText('No data', { exact: true }).isVisible()) throw new Error(`Grafana panel ${id} rendered no data`);
      const screenshot = `grafana-delta-panel-${id}.png`;
      await page.screenshot({ path: path.join(options.results, screenshot), fullPage: true });
      (evidence.panels as unknown[]).push({ id, title: panel.title, samples, delivered, screenshot });
      delete evidence.active_panel;
    }
    await page.goto(`http://lgtm:3000/d/arrowcol-delta-operations?from=${start * 1000}&to=${end * 1000}`, { timeout: 60_000 });
    await page.screenshot({ path: path.join(options.results, 'grafana-delta-operations.png'), fullPage: true });
    evidence.screenshot = 'grafana-delta-operations.png';
    evidence.passed = true;
  } catch (error) {
    evidence.error = safeKustoError(error);
    throw new Error(`Grafana live telemetry gate failed: ${evidence.error}`);
  } finally {
    evidence.finished_at = new Date().toISOString();
    writeFileSync(path.join(options.results, 'grafana-validation.json'), `${JSON.stringify(evidence, null, 2)}\n`);
    await browser?.close();
    options.compose([...browserProfile, 'rm', '--force', '--stop', 'playwright']);
  }
}
