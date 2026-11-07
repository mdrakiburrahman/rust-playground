import { parse_xml, type Azure } from '../../../infra/azure/bootstrap.ts';
import { object } from './kusto.js';
import { isAdxUserAgent, validateScope, type LogsQuery, type ReadGroup, type ReadScope, type ReadWindow } from './storage-reads.js';

export interface StorageReadQuery {
  source: 'storage';
  logAccount: string;
  scope: ReadScope;
  window: ReadWindow;
  raw?: boolean;
}

/** Normalize genuine diagnostic NDJSON, not aggregate storage metrics or fixture responses. */
export function exportedReads(lines: readonly string[], scope: ReadScope, window: ReadWindow,
  record?: (row: Record<string, unknown>) => void): ReadGroup[] {
  validateScope(scope);
  const ownedIds = new Set(scope.owned?.requestIds.map(id => id.toLowerCase()) ?? []);
  const root = `/${scope.container}/${scope.prefix}`;
  const hosts = [`${scope.account}.blob.core.windows.net`, `${scope.account}.dfs.core.windows.net`];
  const groups = new Map<string, ReadGroup>();
  const seen = new Set<string>();
  for (const line of lines) {
    if (!line.trim()) continue;
    const row = object(JSON.parse(line));
    const properties = object(row.properties);
    if (properties.accountName !== scope.account || row.category !== 'StorageRead') continue;
    const identity = object(row.identity);
    const requester = identity.requester == null ? {} : object(identity.requester);
    if (typeof requester.objectId !== 'string' || requester.objectId.toLowerCase() !== scope.requester.toLowerCase()) continue;
    if (typeof row.uri !== 'string' || typeof row.time !== 'string' || typeof row.operationName !== 'string' ||
        typeof row.correlationId !== 'string') throw new Error('Attributed storage diagnostic record is malformed.');
    const uri = new URL(row.uri);
    const pathname = decodeURIComponent(uri.pathname);
    if (!hosts.includes(uri.hostname) || !pathname.startsWith(root)) continue;
    const time = Date.parse(row.time);
    const status = Number(row.statusCode);
    const bytes = properties.responseBodySize ?? 0;
    const userAgent = properties.userAgentHeader ?? '';
    if (!Number.isFinite(time) || !Number.isInteger(status) || typeof bytes !== 'number' || bytes < 0 ||
        typeof userAgent !== 'string') throw new Error('Attributed storage diagnostic values are malformed.');
    if (time < window.from) continue;
    if (scope.owned) {
      if (userAgent === scope.owned.marker || ownedIds.has(row.correlationId.toLowerCase()) ||
          (typeof properties.clientRequestId === 'string' && ownedIds.has(properties.clientRequestId.toLowerCase()))) continue;
    } else if (!isAdxUserAgent(userAgent)) continue;
    const event = `${row.time}/${row.correlationId}/${row.operationName}/${uri.pathname}/${status}/${bytes}`;
    if (seen.has(event)) continue;
    seen.add(event);
    record?.({ TimeGenerated: row.time, CorrelationId: row.correlationId, OperationName: row.operationName,
      Uri: row.uri, StatusCode: status, ResponseBodySize: bytes, UserAgentHeader: userAgent,
      RequesterObjectId: requester.objectId, ClientRequestId: properties.clientRequestId ?? '' });
    const phase = time < window.mutationAt ? 'warm-up' : time < window.windowEnd ? 'after-mutation' : 'late';
    const path = pathname.slice(root.length);
    const key = JSON.stringify([phase, path, row.operationName, status, userAgent]);
    const previous = groups.get(key);
    if (previous) {
      previous.requests++;
      previous.bytes += bytes;
      if (time > Date.parse(previous.last)) previous.last = row.time;
    } else {
      groups.set(key, { phase, path, operation: row.operationName, status, requests: 1, bytes,
        last: row.time, userAgent });
    }
  }
  return [...groups.values()].sort((a, b) => JSON.stringify(a).localeCompare(JSON.stringify(b)));
}

export function readGroupsResponse(groups: readonly ReadGroup[], scope: ReadScope): unknown {
  return { tables: [{ columns: ['Phase', 'Path', 'OperationName', 'StatusCode', 'Requests', 'Bytes', 'Last', 'UserAgent']
    .map(name => ({ name })), rows: groups.map(group =>
    [group.phase, `https://${scope.account}.blob.core.windows.net/${scope.container}/${scope.prefix}${group.path}`,
      group.operation, group.status, group.requests, group.bytes, group.last, group.userAgent]) }] };
}

/** Read a distinct, stamped Azure Monitor diagnostic destination using Entra credentials. */
export function storageExportQuery(azure: Pick<Azure, 'request'> & { c: { subscription: string; group: string } },
  logAccount: string, record?: (scope: ReadScope, rows: Record<string, unknown>[]) => void): LogsQuery {
  if (!/^[a-z0-9]{3,24}$/.test(logAccount)) throw new Error('Invalid diagnostic storage account.');
  const endpoint = `https://${logAccount}.blob.core.windows.net/insights-logs-storageread`;
  const headers = { 'x-ms-version': '2023-11-03', 'User-Agent': 'arrowcol-qap/read-evidence' };
  const cache = new Map<string, { etag: string; lines: string[] }>();
  return async (_query, signal, context) => {
    signal.throwIfAborted();
    if (!context?.scope.owned) throw new Error('Storage export evidence requires complete owned-request receipts.');
    const { scope, window } = context;
    validateScope(scope);
    if (scope.account === logAccount) throw new Error('Diagnostic storage must be distinct from the monitored source.');
    const resource = `/subscriptions/${azure.c.subscription}/resourceGroups/${azure.c.group}` +
      `/providers/Microsoft.Storage/storageAccounts/${scope.account}/blobServices/default`;
    const prefix = 'resourceId=' + resource + '/';
    const active = new Set<string>();
    const allLines: string[] = [];
    let marker = '';
    for (let page = 0; ; page++) {
      signal.throwIfAborted();
      if (page >= 1000) throw new Error('Diagnostic export listing exceeded 1000 pages.');
      const query = new URLSearchParams({ restype: 'container', comp: 'list', prefix, marker });
      const [data] = await azure.request('GET', `${endpoint}?${query}`, 'https://storage.azure.com/', undefined, headers, true);
      if (!data) return readGroupsResponse([], scope);
      const root = parse_xml(data.toString());
      if (root.name !== 'EnumerationResults') throw new Error('Diagnostic export listing is malformed.');
      const text = (node: typeof root, name: string) => node.children.find(child => child.name === name)?.text ?? '';
      for (const blob of root.children.find(child => child.name === 'Blobs')?.children ?? []) {
        if (blob.name !== 'Blob') continue;
        const name = text(blob, 'Name');
        const properties = blob.children.find(child => child.name === 'Properties');
        if (!properties || !name.startsWith(prefix) || !name.endsWith('.json'))
          throw new Error('Diagnostic export blob is malformed or outside the expected source.');
        const match = /\/y=(\d{4})\/m=(\d{2})\/d=(\d{2})\/h=(\d{2})\//.exec(name);
        if (!match) throw new Error('Diagnostic export blob has no supported hourly timestamp.');
        const hour = Date.parse(`${match[1]}-${match[2]}-${match[3]}T${match[4]}:00:00Z`);
        if (!Number.isFinite(hour)) throw new Error('Diagnostic export hourly timestamp is invalid.');
        if (hour + 3_600_000 < window.from || hour > Date.now()) continue;
        if (Number(text(properties, 'Content-Length')) > 16 * 1024 * 1024)
          throw new Error('Diagnostic export blob exceeds the bounded 16 MiB reader.');
        active.add(name);
        const etag = text(properties, 'Etag');
        if (!etag) throw new Error('Diagnostic export blob has no ETag.');
        let entry = cache.get(name);
        if (entry?.etag !== etag) {
          const [content] = await azure.request('GET', endpoint + '/' + name.split('/').map(encodeURIComponent).join('/'),
            'https://storage.azure.com/', undefined, headers);
          if (!content) throw new Error('Diagnostic export blob disappeared while being read.');
          entry = { etag, lines: content.toString().split('\n') };
          cache.set(name, entry);
        }
        allLines.push(...entry.lines);
      }
      marker = text(root, 'NextMarker');
      if (!marker) break;
    }
    for (const key of cache.keys()) if (!active.has(key)) cache.delete(key);
    const rows: Record<string, unknown>[] = [];
    const groups = exportedReads(allLines, scope, window, row => rows.push(row));
    record?.(scope, rows);
    return readGroupsResponse(groups, scope);
  };
}
