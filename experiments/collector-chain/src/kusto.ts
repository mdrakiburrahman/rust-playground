export type Row = Record<string, unknown>;

interface Column { readonly ColumnName: string }
interface Frame {
  readonly FrameType?: string;
  readonly TableKind?: string;
  readonly HasErrors?: boolean;
  readonly OneApiErrors?: readonly unknown[];
  readonly Columns?: readonly Column[];
  readonly Rows?: readonly (readonly unknown[])[];
}

function rows(columns: readonly Column[], values: readonly (readonly unknown[])[]): Row[] {
  return values.map(value => Object.fromEntries(columns.map((column, i) => [column.ColumnName, value[i]])));
}

/** Rows of the primary result in a v2 query response; any error frame throws. */
export function queryRows(body: string): Row[] {
  const frames = JSON.parse(body) as Frame[];
  const failed = frames.find(frame => frame.HasErrors || (frame.OneApiErrors?.length ?? 0) > 0);
  if (failed) throw new Error(`Kusto query failed: ${JSON.stringify(failed.OneApiErrors ?? failed)}`);
  const primary = frames.find(frame => frame.FrameType === 'DataTable' && frame.TableKind === 'PrimaryResult');
  if (!primary?.Columns || !primary.Rows) throw new Error('Kusto response has no primary result');
  return rows(primary.Columns, primary.Rows);
}

/** Rows of the first table in a v1 management response. */
export function commandRows(body: string): Row[] {
  const table = (JSON.parse(body) as { Tables?: Frame[] }).Tables?.[0];
  if (!table?.Columns || !table.Rows) throw new Error('Kusto response has no table');
  return rows(table.Columns, table.Rows);
}

function safeName(name: string): string {
  if (!/^[A-Za-z_][A-Za-z0-9_]{0,127}$/.test(name)) throw new Error(`Unsafe Kusto identifier: ${name}`);
  return name;
}

export function identifier(name: string): string {
  return `['${safeName(name)}']`;
}

function tableRef(name: string): string {
  return `external_table('${safeName(name)}')`;
}

/** The exact KQL the experiment runs, so readers can paste it into Kusto themselves. */
export const kql = {
  externalTable(name: string, location: string): string {
    if (!/^\/[A-Za-z0-9_/.-]+$/.test(location)) throw new Error(`Unsafe Delta location: ${location}`);
    return `.create-or-alter external table ${identifier(name)} kind=delta (h@'${location}')`;
  },
  dmvClients(table: string): string {
    return `${tableRef(table)} | summarize rows=count() by client=customer_id`;
  },
  otapClients(prefix: string): string {
    const signal = (name: string, table: string) =>
      `(${tableRef(`${prefix}${table}`)} | project signal='${name}', rid=tolong(resource.id), ns=_otel_join_namespace)`;
    return [
      `let clients = ${tableRef(`${prefix}resource_attrs`)}`,
      `| where key == 'client.id' | project rid=parent_id, ns=_otel_join_namespace, client=str;`,
      `union ${signal('logs', 'logs')}, ${signal('spans', 'spans')}, ${signal('metrics', 'univariate_metrics')}`,
      '| join kind=inner clients on rid, ns',
      '| summarize rows=count() by signal, client',
    ].join('\n');
  },
};

export class Kusto {
  constructor(readonly endpoint: string, readonly database = 'NetDefaultDB') {}

  async command(csl: string, signal: AbortSignal): Promise<Row[]> {
    return commandRows(await this.post('/v1/rest/mgmt', csl, signal));
  }

  async query(csl: string, signal: AbortSignal): Promise<Row[]> {
    return queryRows(await this.post('/v2/rest/query', csl, signal));
  }

  private async post(route: string, csl: string, signal: AbortSignal): Promise<string> {
    const response = await fetch(`${this.endpoint}${route}`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json', Accept: 'application/json' },
      body: JSON.stringify({ db: this.database, csl }),
      signal,
    });
    const body = await response.text();
    if (!response.ok) throw new Error(`Kusto returned HTTP ${response.status}: ${body.slice(0, 500)}`);
    return body;
  }
}