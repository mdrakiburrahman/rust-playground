import fs from 'node:fs';
import { object } from './kusto.js';

export interface OwnedReads {
  marker: string;
  requestIds: string[];
}

const UUID = /^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$/i;

/** Require receipts for every owned request, including responses whose user-agent was lost in export. */
export function ownedReads(filename: string, marker: string): OwnedReads {
  if (!/^arrowcol-qap\/[a-z0-9_]+\/[a-z0-9_]+$/.test(marker))
    throw new Error('Invalid owned-read marker.');
  const pending = new Map<string, Record<string, unknown>>();
  const complete = new Set<string>();
  const ids = new Set<string>();
  const lines = fs.readFileSync(filename, 'utf8').trim().split('\n').filter(Boolean);
  if (!lines.length) throw new Error('Owned HTTP receipts are empty; read attribution is unavailable.');
  for (const line of lines) {
    const row = object(JSON.parse(line));
    if (row.marker !== marker || typeof row.clientRequestId !== 'string' || !UUID.test(row.clientRequestId) ||
        typeof row.time !== 'string' || !Number.isFinite(Date.parse(row.time)))
      throw new Error('Owned HTTP receipt is malformed or belongs to another trial.');
    const id = row.clientRequestId;
    if (row.event === 'request') {
      if (pending.has(id) || complete.has(id) || typeof row.url !== 'string' || typeof row.method !== 'string')
        throw new Error('Owned HTTP request receipt is duplicate or malformed.');
      const url = new URL(row.url);
      if (url.search || url.hash || url.username || url.password)
        throw new Error('Owned HTTP receipt URL must not contain credentials or query parameters.');
      pending.set(id, row);
      ids.add(id);
    } else if (row.event === 'response') {
      const request = pending.get(id);
      if (!request || complete.has(id) || typeof row.status !== 'number' || !Number.isInteger(row.status))
        throw new Error('Owned HTTP response receipt has no matching request.');
      pending.delete(id);
      const host = new URL(String(request.url)).hostname;
      if ((host.endsWith('.blob.core.windows.net') || host.endsWith('.dfs.core.windows.net')) &&
          (request.method === 'GET' || request.method === 'HEAD') && typeof row.serverRequestId !== 'string')
        throw new Error('Owned storage read has no server request ID; attribution is unavailable.');
      if (typeof row.serverRequestId === 'string') {
        if (!UUID.test(row.serverRequestId)) throw new Error('Owned HTTP server request ID is malformed.');
        ids.add(row.serverRequestId);
      }
      complete.add(id);
    } else {
      throw new Error('Owned HTTP request did not produce a definitive response; attribution is unavailable.');
    }
  }
  if (pending.size) throw new Error('Owned HTTP requests are incomplete; refusing to attribute unidentified reads to ADX.');
  return { marker, requestIds: [...ids].sort() };
}
