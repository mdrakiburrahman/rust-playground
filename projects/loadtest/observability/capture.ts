import { mkdir, readFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { setTimeout as delay } from 'node:timers/promises';
import type { Page, Response } from 'playwright';

interface Capture {
  name: 'baseline' | 'diagnosed-bottleneck' | 'best-sustained-candidate';
  run: string;
  variant: string;
  fromMs: number;
  toMs: number;
}

export interface CapturePlan {
  component: string;
  outputDir: string;
  captures: Capture[];
}

const allowedArgs = new Set([
  '--baseline-run', '--baseline-variant', '--baseline-from', '--baseline-to',
  '--bottleneck-run', '--bottleneck-variant', '--bottleneck-from', '--bottleneck-to',
  '--best-run', '--best-variant', '--best-from', '--best-to', '--component', '--output-dir',
]);
const labelPattern = /^[A-Za-z0-9_-]{1,64}$/u;

export function parseCaptureArgs(args: string[]): CapturePlan {
  const values = new Map<string, string>();
  for (const arg of args) {
    const equals = arg.indexOf('=');
    if (equals < 0) throw new Error(`Expected --name=value, got ${arg}`);
    const name = arg.slice(0, equals);
    const value = arg.slice(equals + 1);
    if (!allowedArgs.has(name) || !value || values.has(name)) throw new Error(`Invalid or duplicate option: ${name}`);
    values.set(name, value);
  }

  const keys = (['baseline', 'bottleneck', 'best'] as const).filter(key =>
    ['run', 'variant', 'from', 'to'].some(field => values.has(`--${key}-${field}`)));
  if (!keys.length) throw new Error('At least one capture is required');
  const required = keys.flatMap(key => ['run', 'variant', 'from', 'to'].map(field => `--${key}-${field}`));
  const missing = required.filter(name => !values.has(name));
  if (missing.length) throw new Error(`Missing required options: ${missing.join(', ')}`);

  const parseTimestamp = (name: string): number => {
    const value = values.get(name)!;
    if (!/(?:Z|[+-]\d{2}:\d{2})$/u.test(value)) throw new Error(`${name} must include an explicit timezone`);
    const timestamp = Date.parse(value);
    if (!Number.isFinite(timestamp)) throw new Error(`${name} is not a valid timestamp`);
    return timestamp;
  };
  const captures = keys.map((key): Capture => {
    const option = `--${key}-run`;
    const run = values.get(option)!;
    if (!labelPattern.test(run)) throw new Error(`${option} must be a fixed low-cardinality run label`);
    const variant = values.get(`--${key}-variant`)!;
    if (!labelPattern.test(variant)) throw new Error(`--${key}-variant must be a fixed low-cardinality variant label`);
    const fromMs = parseTimestamp(`--${key}-from`);
    const toMs = parseTimestamp(`--${key}-to`);
    if (fromMs >= toMs) throw new Error(`--${key}-from must precede --${key}-to`);
    return {
      name: { baseline: 'baseline', bottleneck: 'diagnosed-bottleneck', best: 'best-sustained-candidate' }[key] as Capture['name'],
      run, variant, fromMs, toMs,
    };
  });
  const windowDurations = new Set(captures.map(({ fromMs, toMs }) => toMs - fromMs));
  if (windowDurations.size !== 1) throw new Error('All captures must use the same measurement-window duration');
  const component = values.get('--component') ?? 'all';
  if (component !== 'all' && !labelPattern.test(component)) {
    throw new Error('--component must be all or a fixed low-cardinality component label');
  }

  return {
    component: component === 'all' ? '.*' : component,
    outputDir: path.resolve(values.get('--output-dir') ?? path.join(os.tmpdir(), 'arrowcol-grafana-captures')),
    captures,
  };
}

export function dashboardUrl(baseUrl: string, plan: CapturePlan, capture: Capture): URL {
  const base = new URL(baseUrl);
  if (!['http:', 'https:'].includes(base.protocol) || base.username || base.password || base.search || base.hash) {
    throw new Error('Grafana URL must be an HTTP(S) origin without credentials, query, or fragment');
  }
  const url = new URL('/d/arrowcol-throughput/arrowcol-throughput', base);
  url.searchParams.set('orgId', '1');
  url.searchParams.set('from', String(capture.fromMs));
  url.searchParams.set('to', String(capture.toMs));
  url.searchParams.set('var-run', capture.run);
  url.searchParams.set('var-variant', capture.variant);
  url.searchParams.set('var-component', plan.component);
  url.searchParams.set('refresh', '');
  return url;
}

type QueryState = 'data' | 'unavailable';

export function queryResultState(result: unknown): QueryState {
  if (!result || typeof result !== 'object') throw new Error('Missing Grafana query result');
  const query = result as { error?: unknown; status?: number; frames?: Array<{ data?: { values?: unknown[][] } }> };
  if (query.error || (query.status !== undefined && query.status >= 400)) {
    throw new Error(`Grafana query failed: ${query.error ?? query.status}`);
  }
  if (!Array.isArray(query.frames)) throw new Error('Grafana query did not return completed frames');
  for (const frame of query.frames) {
    if (!Array.isArray(frame.data?.values) || !frame.data.values.every(Array.isArray)) {
      throw new Error('Grafana returned malformed frame data');
    }
  }
  return query.frames.some(frame => frame.data!.values!.some(column => column.some(value => value !== null)))
    ? 'data' : 'unavailable';
}

interface DashboardPanel {
  title: string;
  type: string;
  targets?: Array<{ expr?: string; refId: string; hide?: boolean }>;
}

async function waitForPanels(
  page: Page, panels: DashboardPanel[], states: Map<string, QueryState | 'pending'>,
  failures: string[], plan: CapturePlan, capture: Capture,
): Promise<void> {
  for (const definition of panels) {
    const panel = page.getByRole('region', { name: definition.title, exact: true });
    await panel.scrollIntoViewIfNeeded({ timeout: 60_000 });
    const expressions = (definition.targets ?? []).filter(target => !target.hide && target.expr)
      .map(target => target.expr!.replace(/\$run\b/gu, capture.run)
        .replace(/\$variant\b/gu, capture.variant).replace(/\$component\b/gu, plan.component));
    const deadline = Date.now() + 60_000;
    while (expressions.some(expr => !states.has(expr) || states.get(expr) === 'pending')) {
      if (failures.length) throw new Error(failures.join('; '));
      if (Date.now() >= deadline) throw new Error(`Timed out waiting for query data: ${definition.title}`);
      await delay(100);
    }
    if (failures.length) throw new Error(failures.join('; '));
    await panel.getByLabel('Panel loading bar', { exact: true }).waitFor({ state: 'hidden', timeout: 60_000 });
    const content = panel.locator('[data-testid="data-testid Panel content"]');
    if (expressions.length && expressions.every(expr => states.get(expr) === 'unavailable')) {
      await content.getByText(/no data|unavailable|unknown|n\/a/iu).first()
        .waitFor({ state: 'visible', timeout: 60_000 });
    } else {
      await page.waitForFunction(element =>
        !!element && (!!element.querySelector('canvas, svg, table') || !!element.textContent?.trim()),
      await content.elementHandle(), { timeout: 60_000 });
    }
  }
}

async function captureScreenshots(plan: CapturePlan, baseUrl: string): Promise<void> {
  const { chromium } = await import('playwright');
  const dashboard = JSON.parse(await readFile(new URL('./throughput-dashboard.json', import.meta.url), 'utf8')) as {
    panels: DashboardPanel[];
  };
  await mkdir(plan.outputDir, { recursive: true });
  let browser;
  try {
    browser = await chromium.launch({ headless: true });
  } catch (error) {
    const detail = error instanceof Error ? `: ${error.message}` : '';
    throw new Error(
      `Could not launch Playwright Chromium; install its browser and OS dependencies with ` +
      '`npx --no-install playwright install --with-deps chromium`' + detail,
      { cause: error },
    );
  }
  try {
    for (const capture of plan.captures) {
      const page = await browser.newPage({ viewport: { width: 1920, height: 1080 }, deviceScaleFactor: 1 });
      const states = new Map<string, QueryState | 'pending'>();
      const failures: string[] = [];
      page.on('request', request => {
        if (new URL(request.url()).pathname !== '/api/ds/query') return;
        const body = request.postDataJSON() as { queries?: Array<{ expr?: string }> };
        for (const query of body.queries ?? []) if (query.expr) states.set(query.expr, 'pending');
      });
      page.on('requestfailed', request => {
        if (new URL(request.url()).pathname === '/api/ds/query') {
          failures.push(`Grafana query request failed: ${request.failure()?.errorText}`);
        }
      });
      const onResponse = async (response: Response): Promise<void> => {
        if (new URL(response.url()).pathname !== '/api/ds/query') return;
        try {
          if (!response.ok()) throw new Error(`Grafana query HTTP ${response.status()}`);
          const body = response.request().postDataJSON() as { queries: Array<{ expr?: string; refId: string }> };
          const result = await response.json() as { results?: Record<string, unknown> };
          for (const query of body.queries) {
            if (query.expr) states.set(query.expr, queryResultState(result.results?.[query.refId]));
          }
        } catch (error) {
          failures.push(error instanceof Error ? error.message : String(error));
        }
      };
      page.on('response', response => { void onResponse(response); });
      const url = dashboardUrl(baseUrl, plan, capture);
      const response = await page.goto(url.href, { waitUntil: 'domcontentloaded', timeout: 60_000 });
      if (!response?.ok()) throw new Error(`Grafana returned ${response?.status() ?? 'no response'} for ${capture.name}`);

      if (new URL(page.url()).pathname.endsWith('/login')) {
        const username = process.env.GRAFANA_USER;
        const password = process.env.GRAFANA_PASSWORD;
        if (!username || !password) throw new Error('Grafana login required; set GRAFANA_USER and GRAFANA_PASSWORD');
        await page.locator('input[name="user"]').fill(username);
        await page.locator('input[name="password"]').fill(password);
        await page.getByRole('button', { name: /log in/i }).click();
        await page.goto(url.href, { waitUntil: 'domcontentloaded', timeout: 60_000 });
      }

      await waitForPanels(page, dashboard.panels, states, failures, plan, capture);
      const file = path.join(plan.outputDir, `${capture.name}-${capture.run}.png`);
      await page.screenshot({ path: file, fullPage: true, animations: 'disabled' });
      console.log(`Captured ${capture.name}: ${file}`);
      await page.close();
    }
  } finally {
    await browser.close();
  }
}

async function main(): Promise<void> {
  const plan = parseCaptureArgs(process.argv.slice(2));
  const baseUrl = process.env.GRAFANA_URL ?? 'http://127.0.0.1:3000';
  const durationMinutes = (plan.captures[0].toMs - plan.captures[0].fromMs) / 60_000;
  console.log(`Capturing ${plan.captures.length} ${durationMinutes}-minute measurement window(s)`);
  for (const capture of plan.captures) {
    console.log(`${capture.name}: ${new Date(capture.fromMs).toISOString()} .. ${new Date(capture.toMs).toISOString()}`);
  }
  await captureScreenshots(plan, baseUrl);
}

if (process.argv[1] && path.resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  main().catch(error => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
