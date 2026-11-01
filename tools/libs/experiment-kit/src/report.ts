import { randomBytes } from 'node:crypto';
import { open, rename, rm } from 'node:fs/promises';
import path from 'node:path';

import type { ExperimentReport, StageReport } from './experiment.js';

export type JsonValue =
  | null
  | boolean
  | number
  | string
  | readonly JsonValue[]
  | { readonly [key: string]: JsonValue };

export type CellValue = string | number | boolean | null | undefined;

export interface Column<TRow> {
  readonly header: string;
  /** Rendered as `Header (unit)`. Units are never inferred. */
  readonly unit?: string;
  readonly value: (row: TRow) => CellValue;
  readonly format?: (value: number) => string;
  readonly align?: 'left' | 'right' | 'center';
}

export interface TableOptions {
  /** Text for null/undefined cells. Defaults to `n/a`; missing values are never rendered as zero. */
  readonly missing?: string;
  /** Literal secret values to mask in rendered text. */
  readonly secrets?: readonly string[];
}

export const REDACTED = '[REDACTED]';

export class JsonSafetyError extends TypeError {
  readonly path: string;

  constructor(at: string, detail: string) {
    super(`${at}: ${detail}`);
    this.name = 'JsonSafetyError';
    this.path = at;
  }
}

const SECRET_KEY_SUFFIXES = [
  'password',
  'passwd',
  'secret',
  'token',
  'apikey',
  'accesskey',
  'accountkey',
  'credential',
  'credentials',
  'authorization',
  'connectionstring',
  'sig',
] as const;

const SECRET_PATTERNS: readonly (readonly [RegExp, string])[] = [
  [/\b(Bearer|Basic)\s+[A-Za-z0-9._~+/=-]+/gi, `$1 ${REDACTED}`],
  [
    /\b(AccountKey|SharedAccessKey|SharedAccessSignature|Password|Pwd|ClientSecret|sig)=([^;&\s"']+)/gi,
    `$1=${REDACTED}`,
  ],
  [/-----BEGIN [A-Z ]*PRIVATE KEY-----[\s\S]*?-----END [A-Z ]*PRIVATE KEY-----/g, REDACTED],
];

/** True for keys such as `password`, `accessToken`, `client_secret`, `connectionString`. */
export function isSecretKey(key: string): boolean {
  const normalized = key.toLowerCase().replace(/[^a-z0-9]/g, '');
  return SECRET_KEY_SUFFIXES.some((suffix) => normalized.endsWith(suffix));
}

/** Masks caller-supplied literal secrets (4+ chars) and common credential shapes. */
export function redactString(value: string, secrets: readonly string[] = []): string {
  let out = value;
  for (const secret of secrets) {
    if (secret.length >= 4) out = out.split(secret).join(REDACTED);
  }
  for (const [pattern, replacement] of SECRET_PATTERNS) out = out.replace(pattern, replacement);
  return out;
}

/**
 * Returns a redacted deep copy that is exactly representable as JSON.
 * Rejects non-finite numbers, bigint, undefined, functions, symbols, cycles,
 * sparse arrays and non-plain objects (Date, Map, class instances, ...).
 */
export function toSafeJsonValue(value: unknown, secrets: readonly string[] = [], at = '$'): JsonValue {
  return visit(value, at, new Set<object>(), secrets);
}

export function stringifySafeJson(value: unknown, secrets: readonly string[] = []): string {
  return `${JSON.stringify(toSafeJsonValue(value, secrets), null, 2)}\n`;
}

function visit(value: unknown, at: string, ancestors: Set<object>, secrets: readonly string[]): JsonValue {
  if (value === null) return null;
  if (typeof value === 'string') return redactString(value, secrets);
  if (typeof value === 'boolean') return value;
  if (typeof value === 'number') {
    if (!Number.isFinite(value)) throw new JsonSafetyError(at, `non-finite number ${String(value)}`);
    return Object.is(value, -0) ? 0 : value;
  }
  if (typeof value !== 'object') throw new JsonSafetyError(at, `unsupported ${typeof value} value`);
  if (ancestors.has(value)) throw new JsonSafetyError(at, 'circular reference');
  ancestors.add(value);
  try {
    if (Array.isArray(value)) {
      const items: JsonValue[] = [];
      for (let index = 0; index < value.length; index += 1) {
        if (!(index in value)) throw new JsonSafetyError(`${at}[${index}]`, 'sparse array hole');
        const item: unknown = value[index];
        items.push(visit(item, `${at}[${index}]`, ancestors, secrets));
      }
      return items;
    }
    const proto: unknown = Object.getPrototypeOf(value);
    if (proto !== Object.prototype && proto !== null) {
      throw new JsonSafetyError(at, `unsupported object ${value.constructor?.name ?? 'instance'}`);
    }
    if (Object.getOwnPropertySymbols(value).length > 0) throw new JsonSafetyError(at, 'symbol keys');
    const out: Record<string, JsonValue> = {};
    for (const key of Object.keys(value)) {
      const child: unknown = Reflect.get(value, key);
      const childAt = `${at}.${key}`;
      if (child === undefined) throw new JsonSafetyError(childAt, 'undefined value');
      const safe = isSecretKey(key) ? REDACTED : visit(child, childAt, ancestors, secrets);
      Object.defineProperty(out, key, { value: safe, enumerable: true, writable: true, configurable: true });
    }
    return out;
  } finally {
    ancestors.delete(value);
  }
}

/** Escapes text for a single GitHub-flavoured Markdown table cell. */
export function escapeCell(text: string): string {
  return text
    .replace(/\\/g, '\\\\')
    .replace(/\|/g, '\\|')
    .replace(/\r\n|\r|\n/g, '<br>');
}

function columnTitle<TRow>(column: Column<TRow>): string {
  return column.unit === undefined || column.unit === '' ? column.header : `${column.header} (${column.unit})`;
}

const ALIGN = { left: ':---', right: '---:', center: ':---:' } as const;

export function renderTable<TRow>(
  columns: readonly Column<TRow>[],
  rows: readonly TRow[],
  options: TableOptions = {},
): string {
  if (columns.length === 0) throw new RangeError('renderTable requires at least one column');
  const missing = options.missing ?? 'n/a';
  const secrets = options.secrets ?? [];
  const cell = (text: string): string => escapeCell(redactString(text, secrets));
  const lines = [
    `| ${columns.map((column) => cell(columnTitle(column))).join(' | ')} |`,
    `| ${columns.map((column) => ALIGN[column.align ?? 'left']).join(' | ')} |`,
  ];
  rows.forEach((row, rowIndex) => {
    const cells = columns.map((column) => {
      const value = column.value(row);
      if (value === null || value === undefined) return cell(missing);
      if (typeof value === 'number') {
        if (!Number.isFinite(value)) {
          throw new RangeError(`row ${rowIndex}, column "${columnTitle(column)}": non-finite number ${String(value)}`);
        }
        return cell(column.format ? column.format(value) : String(value));
      }
      return cell(String(value));
    });
    lines.push(`| ${cells.join(' | ')} |`);
  });
  return `${lines.join('\n')}\n`;
}

const STAGE_COLUMNS: readonly Column<StageReport>[] = [
  { header: 'Stage', value: (stage) => stage.name },
  { header: 'Status', value: (stage) => stage.status },
  { header: 'Duration', unit: 'ms', value: (stage) => stage.durationMs, align: 'right' },
  { header: 'Rows', value: (stage) => stage.rowCount, align: 'right' },
  { header: 'Summary', value: (stage) => stage.summary },
];

const SUMMARY_COLUMNS: readonly Column<ExperimentReport>[] = [
  { header: 'Experiment', value: (report) => report.experimentId },
  { header: 'Status', value: (report) => report.status },
  { header: 'Started', unit: 'UTC', value: (report) => report.startedAt },
  { header: 'Duration', unit: 'ms', value: (report) => report.durationMs, align: 'right' },
  { header: 'Evidence records', value: (report) => report.evidence.records, align: 'right' },
];

export type ResultsSection = { readonly table: string } | { readonly error: string };

/** Renders the final Markdown summary; the same text is written to file and console. */
export function renderReportMarkdown(
  report: ExperimentReport,
  results: ResultsSection,
  secrets: readonly string[] = [],
): string {
  const text = (value: string): string => escapeCell(redactString(value, secrets));
  const parts = [
    `## ${text(report.title)}`,
    renderTable(SUMMARY_COLUMNS, [report], { secrets }),
    '### Stages',
    renderTable(STAGE_COLUMNS, report.stages, { secrets }),
    '### Results',
  ];
  if ('error' in results) parts.push(`**Results table could not be rendered:** ${text(results.error)}`);
  else if (report.rows.length === 0) parts.push('_No rows._');
  else parts.push(results.table);

  const notes: string[] = [];
  if (report.abortReason !== null) notes.push(`Aborted: ${text(report.abortReason)}`);
  for (const stage of report.stages) {
    if (!stage.settled) {
      notes.push(`Stage ${text(stage.name)} did not settle after abort; its effects may still be running.`);
    }
  }
  if (report.cleanup.status !== 'none') {
    const detail = report.cleanup.detail === null ? '' : `: ${text(report.cleanup.detail)}`;
    notes.push(`Cleanup ${report.cleanup.status}${detail}`);
  }
  if (report.evidence.truncated) {
    notes.push(`Evidence truncated: ${report.evidence.dropped} record(s) dropped after the configured limit.`);
  }
  if (report.evidence.writeError !== null) notes.push(`Evidence write error: ${text(report.evidence.writeError)}`);
  if (notes.length > 0) parts.push('### Notes', notes.map((note) => `- ${note}`).join('\n'));
  return `${parts.map((part) => part.trimEnd()).join('\n\n')}\n`;
}

/**
 * Writes via a same-directory temp file, fsync, rename, then a best-effort
 * directory fsync. Readers observe either the old or the complete new file.
 */
export async function writeFileAtomic(file: string, data: string): Promise<void> {
  const dir = path.dirname(file);
  const tmp = path.join(dir, `.${path.basename(file)}.${process.pid}.${randomBytes(6).toString('hex')}.tmp`);
  try {
    const handle = await open(tmp, 'wx', 0o600);
    try {
      await handle.writeFile(data, 'utf8');
      await handle.sync();
    } finally {
      await handle.close();
    }
    await rename(tmp, file);
  } catch (error) {
    await rm(tmp, { force: true });
    throw error;
  }
  await syncDirectory(dir);
}

async function syncDirectory(dir: string): Promise<void> {
  try {
    const handle = await open(dir, 'r');
    try {
      await handle.sync();
    } finally {
      await handle.close();
    }
  } catch {
    // Directory fsync is unsupported on some platforms (for example Windows); rename is still atomic.
  }
}