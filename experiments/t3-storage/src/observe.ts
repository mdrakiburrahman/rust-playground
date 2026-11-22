import { mkdir, writeFile } from 'node:fs/promises';
import path from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { createHash } from 'node:crypto';
import { brotliDecompressSync } from 'node:zlib';
import { Lab, privateDirectory, shellQuote } from './lab.js';
import type { Request } from './job.js';

const chunkSize = 2560;

export function evidenceArchiveScript(root: string, files: string[]): string {
  return `const fs=require('node:fs'),z=require('node:zlib'),c=require('node:crypto');const root=${JSON.stringify(root)},files=${JSON.stringify(files)},data={},unavailable=[];for(const name of files){try{data[name]=fs.readFileSync(root+'/'+name).toString('base64')}catch(e){if(e.code!=='ENOENT')throw e;unavailable.push(name)}}const b=z.brotliCompressSync(Buffer.from(JSON.stringify({data,unavailable})));fs.writeFileSync(root+'/collected.json.br',b);console.log('STORAGE_SPIKE_ARCHIVE:'+JSON.stringify({count:Math.ceil(b.length/${chunkSize}),total:b.length,sha256:c.createHash('sha256').update(b).digest('hex'),part:b.subarray(0,${chunkSize}).toString('base64')}));`;
}

export async function blobJson(lab: Lab, name: string): Promise<unknown | null> {
  if (!/^[a-zA-Z0-9_./-]+$/u.test(name) || name.includes('..')) throw new Error('Invalid evidence blob name.');
  const inventory = await lab.inventory();
  try {
    const result = await lab.az(['rest', '--method', 'get', '--url', `${inventory.storageEndpoints[3].blob}spike/${name}`, '--resource', 'https://storage.azure.com/', '--headers', 'x-ms-version=2021-12-02']);
    return typeof result === 'string' ? JSON.parse(result) : result;
  } catch (error) {
    if (/BlobNotFound|404/u.test(String(error))) return null;
    throw error;
  }
}

export async function collectJob(lab: Lab, request: Request): Promise<void> {
  if (!request.vm || !/^[a-z0-9-]{1,100}$/u.test(request.id)) throw new Error('Saved job identity and VM are required.');
  const root = await privateDirectory(lab.config.outDir);
  const dir = path.join(root, 'jobs', request.id);
  await mkdir(dir, { recursive: true, mode: 0o700 });
  const files = ['environment.json', 'status.json'];
  if (request.mode === 'smoke') files.push('raw.jsonl', 'write.jsonl', 'recover.jsonl', 'oracle.jsonl', 'observed.json');
  if (request.mode === 'fault-a') files.push('engine.jsonl', 'oracle.jsonl', 'segments-after-writer.json');
  if (request.mode === 'fault-b') files.push('engine.jsonl', 'oracle.jsonl', 'observed.json', 'verification.jsonl', 'takeover.json', 'control-ready.json', 'control-fault.json', 'original-writer-finished.json');
  if (request.mode === 'fault-b' && request.restartBeforeDrain) files.push('pre-drain.jsonl', 'restart.json', 'segments-before-restart.json', ...(request.trace ? ['pre-drain-syscalls.log'] : []));
  if (request.trace) files.push('syscalls.log');
  if (request.mode === 'performance') {
    for (const mib of request.sizesMiB ?? [1, 2, 4, 8, 16]) {
      const cores = request.cores ?? 1;
      if (cores === 1) files.push(`raw-${mib}.jsonl`);
      for (const interval of request.intervals ?? [0, 1, 5, 10, 25, 50, 100, 250, 1000]) {
        for (let cpu = 0; cpu < cores; cpu++) files.push(`quiver-${mib}-${interval}${cores === 1 ? '' : `-core-${cpu}`}.jsonl`);
      }
    }
  }
  if (request.mode === 'blob') {
    for (const mib of request.sizesMiB ?? [1, 2, 4, 8, 16]) files.push(`blob-${mib}.jsonl`);
    files.push('blob-fence.jsonl');
  }
  if (request.mode === 'phase-a') files.push('quiver-tests.log', 'durable-buffer-tests.log', 'df-engine-build.log', 'df-engine.yaml', 'df-engine-validation.log', 'df-engine-smoke.log', 'ingest-bench-build.log', 'ingest-bench.log');
  const guest = `/var/lib/storage-spike-oracle/jobs/${request.id}`;
  const archive = `${guest}/collected.json.br`;
  const build = evidenceArchiveScript(guest, files);
  const parse = (message: string): { count: number; total: number; sha256: string; part: string } => {
    const line = message.split(/\r?\n/u).find(s => s.startsWith('STORAGE_SPIKE_ARCHIVE:'));
    if (!line) throw new Error('Missing complete archive response.');
    return JSON.parse(line.slice('STORAGE_SPIKE_ARCHIVE:'.length));
  };
  const first = parse(await lab.remote(request.vm, `node -e ${shellQuote(build)}\necho STORAGE_SPIKE_OK`, `collect-${request.id}-0`));
  if (!Number.isSafeInteger(first.count) || first.count < 1 || first.count > 5000) throw new Error('Invalid bounded evidence archive.');
  const parts = [Buffer.from(first.part, 'base64')];
  for (let index = 1; index < first.count; index++) {
    const script = `const b=require('node:fs').readFileSync(${JSON.stringify(archive)});console.log('STORAGE_SPIKE_ARCHIVE:'+JSON.stringify({part:b.subarray(${index * chunkSize},${(index + 1) * chunkSize}).toString('base64')}));`;
    const part = parse(await lab.remote(request.vm, `node -e ${shellQuote(script)}\necho STORAGE_SPIKE_OK`, `collect-${request.id}-${index}`));
    parts.push(Buffer.from(part.part, 'base64'));
  }
  const compressed = Buffer.concat(parts);
  if (compressed.length !== first.total || createHash('sha256').update(compressed).digest('hex') !== first.sha256) throw new Error('Evidence archive digest mismatch.');
  const contents = JSON.parse(brotliDecompressSync(compressed).toString()) as { data: Record<string, string>; unavailable: string[] };
  for (const [file, value] of Object.entries(contents.data)) {
    if (!files.includes(file)) throw new Error('Unexpected archive member.');
    await writeFile(path.join(dir, file), Buffer.from(value, 'base64'), { mode: 0o600 });
  }
  await writeFile(path.join(dir, 'collection.json'), JSON.stringify({ files, unavailable: contents.unavailable, sha256: first.sha256, compression: 'brotli', chunks: first.count }, null, 2));
}

export async function waitJob(lab: Lab, request: Request, timeoutMs = 3 * 60 * 60_000): Promise<Record<string, unknown>> {
  const until = Date.now() + timeoutMs;
  if (!request.vm || !/^[a-z0-9-]{1,100}$/u.test(request.id)) throw new Error('Saved job identity and VM are required.');
  while (Date.now() < until) {
    const script = `const fs=require('node:fs/promises');(async()=>{const until=Date.now()+1080000;let status={status:'pending'};while(Date.now()<until){try{status=JSON.parse(await fs.readFile(${JSON.stringify(`/var/lib/storage-spike-oracle/jobs/${request.id}/status.json`)},'utf8'))}catch(e){if(e.code!=='ENOENT')throw e}if(['succeeded','failed'].includes(status.status))break;await new Promise(r=>setTimeout(r,2000))}console.log('STORAGE_SPIKE_STATUS:'+JSON.stringify(status))})().catch(e=>{console.error(e);process.exit(1)});`;
    const message = await lab.remote(request.vm, `node -e ${shellQuote(script)}\necho STORAGE_SPIKE_OK`, `wait-${request.id}`);
    const line = message.split(/\r?\n/u).find(s => s.startsWith('STORAGE_SPIKE_STATUS:'));
    if (!line) throw new Error('Missing complete guest status response.');
    const status = JSON.parse(line.slice('STORAGE_SPIKE_STATUS:'.length)) as Record<string, unknown>;
    if (status?.status === 'succeeded' || status?.status === 'failed') {
      await collectJob(lab, request);
      if (status.status === 'failed') throw new Error(`Guest job ${request.id} failed: ${String(status.error)}. Evidence preserved privately.`);
      return status;
    }
    await delay(10_000);
  }
  await collectJob(lab, request);
  throw new Error(`Guest job ${request.id} exceeded its observation deadline; reconcile its live service before retrying.`);
}
