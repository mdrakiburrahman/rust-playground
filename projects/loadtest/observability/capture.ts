import { mkdir } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

interface Capture {
  name: 'baseline' | 'diagnosed-bottleneck' | 'best-sustained-candidate';
  run: string;
  fromMs: number;
  toMs: number;
}

export interface CapturePlan {
  component: string;
  outputDir: string;
  captures: Capture[];
}

const allowedArgs = new Set([
  '--baseline-run', '--baseline-from', '--baseline-to',
  '--bottleneck-run', '--bottleneck-from', '--bottleneck-to',
  '--best-run', '--best-from', '--best-to', '--component', '--output-dir',
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

  const required = [
    '--baseline-run', '--baseline-from', '--baseline-to',
    '--bottleneck-run', '--bottleneck-from', '--bottleneck-to',
    '--best-run', '--best-from', '--best-to',
  ];
  const missing = required.filter(name => !values.has(name));
  if (missing.length) throw new Error(`Missing required options: ${missing.join(', ')}`);

  const parseTimestamp = (name: string): number => {
    const value = values.get(name)!;
    if (!/(?:Z|[+-]\d{2}:\d{2})$/u.test(value)) throw new Error(`${name} must include an explicit timezone`);
    const timestamp = Date.parse(value);
    if (!Number.isFinite(timestamp)) throw new Error(`${name} is not a valid timestamp`);
    return timestamp;
  };
  const captures = (['baseline', 'bottleneck', 'best'] as const).map((key, index): Capture => {
    const option = `--${key}-run`;
    const run = values.get(option)!;
    if (!labelPattern.test(run)) throw new Error(`${option} must be a fixed low-cardinality run label`);
    const fromMs = parseTimestamp(`--${key}-from`);
    const toMs = parseTimestamp(`--${key}-to`);
    if (fromMs >= toMs) throw new Error(`--${key}-from must precede --${key}-to`);
    return {
      name: ['baseline', 'diagnosed-bottleneck', 'best-sustained-candidate'][index] as Capture['name'],
      run, fromMs, toMs,
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
  url.searchParams.set('var-variant', capture.name);
  url.searchParams.set('var-component', plan.component);
  return url;
}

async function captureScreenshots(plan: CapturePlan, baseUrl: string): Promise<void> {
  const { chromium } = await import('playwright');
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

      await page.getByText('Committed new rows / s', { exact: true }).waitFor({ state: 'visible', timeout: 60_000 });
      await page.waitForTimeout(1_500);
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
  console.log(`Capturing three equal ${durationMinutes}-minute measurement windows`);
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
