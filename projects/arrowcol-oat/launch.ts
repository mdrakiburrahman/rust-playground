import { readFile } from 'node:fs/promises';
import { pathToFileURL } from 'node:url';

export interface OatRunStatus {
  id: string;
  state: 'queued' | 'running' | 'succeeded' | 'failed';
  progress: string;
  result_count: number;
  error: string | null;
}

type FetchLike = typeof fetch;
type ProgressHandler = (status: OatRunStatus) => void;

export async function launchOat(
  configPath: string,
  serverUrl = 'http://127.0.0.1:8088',
  pollIntervalMs = 1000,
  fetcher: FetchLike = fetch,
  onProgress: ProgressHandler = (status) => console.log(`${status.state}: ${status.progress}`),
): Promise<OatRunStatus> {
  if (!Number.isSafeInteger(pollIntervalMs) || pollIntervalMs < 1) {
    throw new Error('poll interval must be a positive integer');
  }
  const server = new URL(serverUrl);
  if (!['http:', 'https:'].includes(server.protocol)) {
    throw new Error('server URL must use HTTP or HTTPS');
  }
  const configText = await readFile(configPath, 'utf8');
  const config: unknown = JSON.parse(configText);
  if (config === null || typeof config !== 'object' || Array.isArray(config)) {
    throw new Error('OAT config must be a JSON object');
  }

  const startResponse = await fetcher(new URL('/runs', server), {
    method: 'POST',
    headers: { 'content-type': 'application/json' },
    body: JSON.stringify(config),
    signal: AbortSignal.timeout(30_000),
  });
  let status = await readStatus(startResponse, 'start OAT run');
  onProgress(status);
  while (status.state === 'queued' || status.state === 'running') {
    await new Promise((resolve) => setTimeout(resolve, pollIntervalMs));
    const response = await fetcher(new URL(`/runs/${encodeURIComponent(status.id)}`, server), {
      signal: AbortSignal.timeout(30_000),
    });
    status = await readStatus(response, 'poll OAT run');
    onProgress(status);
  }
  if (status.state === 'failed') {
    throw new Error(status.error ?? `OAT run ${status.id} failed`);
  }
  return status;
}

async function readStatus(response: Response, action: string): Promise<OatRunStatus> {
  const body: unknown = await response.json().catch(() => undefined);
  if (!response.ok) {
    const error = body !== null && typeof body === 'object' && 'error' in body && typeof body.error === 'string'
      ? body.error
      : `${response.status} ${response.statusText}`;
    throw new Error(`Could not ${action}: ${error}`);
  }
  if (
    body === null || typeof body !== 'object' ||
    !('id' in body) || typeof body.id !== 'string' ||
    !('state' in body) || !['queued', 'running', 'succeeded', 'failed'].includes(String(body.state)) ||
    !('progress' in body) || typeof body.progress !== 'string' ||
    !('result_count' in body) || typeof body.result_count !== 'number'
  ) {
    throw new Error(`Server returned an invalid status while trying to ${action}`);
  }
  return body as OatRunStatus;
}

async function main(args: string[]): Promise<void> {
  const configPath = args[0];
  if (!configPath || configPath.startsWith('--')) {
    throw new Error('Usage: tsx projects/arrowcol-oat/launch.ts <oat-config.json> [--server URL] [--poll-ms N]');
  }
  const serverIndex = args.indexOf('--server');
  const pollIndex = args.indexOf('--poll-ms');
  const serverUrl = serverIndex >= 0 ? args[serverIndex + 1] : undefined;
  const pollValue = pollIndex >= 0 ? Number(args[pollIndex + 1]) : 1000;
  if ((serverIndex >= 0 && !serverUrl) || !Number.isSafeInteger(pollValue)) {
    throw new Error('Expected values after --server and --poll-ms');
  }
  const status = await launchOat(configPath, serverUrl, pollValue);
  console.log(`Completed ${status.id}: published ${status.result_count} result rows`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  main(process.argv.slice(2)).catch((error: unknown) => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
