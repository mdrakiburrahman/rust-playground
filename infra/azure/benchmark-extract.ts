import { createReadStream } from 'node:fs';
import { open } from 'node:fs/promises';
import { createInterface } from 'node:readline';
import { pathToFileURL } from 'node:url';

const MAX_LINE_BYTES = 8 * 1024 * 1024;
const message = 'Delta logical publication audit receipt';

function stringEnd(json: string, start: number): number {
  if (json[start] !== '"') throw new Error('Expected JSON string');
  for (let i = start + 1; i < json.length; i++) {
    if (json[i] === '\\') i++;
    else if (json[i] === '"') return i + 1;
  }
  throw new Error('Unterminated JSON string');
}

function valueEnd(json: string, start: number): number {
  if (json[start] === '"') return stringEnd(json, start);
  if (json[start] !== '{' && json[start] !== '[') {
    let i = start;
    while (i < json.length && !/[,}\]\s]/.test(json[i])) i++;
    if (i === start) throw new Error('Missing JSON value');
    return i;
  }
  const stack: string[] = [];
  for (let i = start; i < json.length; i++) {
    const ch = json[i];
    if (ch === '"') { i = stringEnd(json, i) - 1; continue; }
    if (ch === '{' || ch === '[') stack.push(ch);
    if (ch === '}' || ch === ']') {
      if (stack.pop() !== (ch === '}' ? '{' : '[')) throw new Error('Mismatched JSON structure');
      if (stack.length === 0) return i + 1;
    }
  }
  throw new Error('Unterminated JSON structure');
}

export function rawField(json: string, field: string): string | undefined {
  let i = 0;
  const ws = () => { while (i < json.length && /\s/.test(json[i])) i++; };
  ws();
  if (json[i++] !== '{') throw new Error('Expected JSON object');
  let found: string | undefined;
  for (;;) {
    ws();
    if (json[i] === '}') { i++; break; }
    const keyEnd = stringEnd(json, i);
    const key: unknown = JSON.parse(json.slice(i, keyEnd));
    i = keyEnd; ws();
    if (json[i++] !== ':') throw new Error('Missing JSON colon');
    ws();
    const end = valueEnd(json, i);
    if (key === field) {
      if (found !== undefined) throw new Error(`Duplicate ${field}`);
      found = json.slice(i, end);
    }
    i = end; ws();
    if (json[i] === ',') { i++; continue; }
    if (json[i] !== '}') throw new Error('Invalid JSON object');
  }
  ws();
  if (i !== json.length) throw new Error('Trailing JSON content');
  return found;
}

export function terminalManifest(line: string): string | undefined {
  const prefix = 'OAT_CLIENT_METRICS ';
  if (!line.startsWith(prefix)) return undefined;
  const json = line.slice(prefix.length);
  const phase = rawField(json, 'phase');
  if (phase !== '"completed"' && phase !== '"failed"') return undefined;
  const manifest = rawField(json, 'logical_batch_manifest');
  if (!manifest || manifest[0] !== '{') throw new Error('Terminal client metrics lack a manifest');
  return manifest;
}

export function collectorReceipt(line: string): boolean {
  if (!line.startsWith('{') || !line.includes(message)) return false;
  const fields = rawField(line, 'fields');
  return fields !== undefined && rawField(fields, 'message') === JSON.stringify(message);
}

async function extract(mode: string, input: string, output: string): Promise<void> {
  if (mode !== 'manifest' && mode !== 'collector') throw new Error('Expected manifest or collector mode');
  const lines = createInterface({ input: createReadStream(input, { encoding: 'utf8' }), crlfDelay: Infinity });
  const out = await open(output, 'wx', 0o600);
  let matches = 0;
  try {
    for await (const line of lines) {
      if (Buffer.byteLength(line) > MAX_LINE_BYTES) throw new Error('Input log line exceeds 8 MiB');
      const result = mode === 'manifest' ? terminalManifest(line) : collectorReceipt(line) ? line : undefined;
      if (result !== undefined) {
        matches++;
        if (mode === 'manifest' && matches > 1) throw new Error('Multiple terminal client manifests');
        await out.writeFile(`${result}\n`);
      }
    }
    if (matches === 0) throw new Error(`No ${mode} evidence found`);
  } finally {
    await out.close();
  }
  console.log(`Extracted ${matches} ${mode} record(s); original logs remain authoritative.`);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  const [mode, input, output, ...extra] = process.argv.slice(2);
  if (!mode || !input || !output || extra.length) throw new Error('Usage: tsx benchmark-extract.ts manifest|collector INPUT_LOG NEW_OUTPUT');
  await extract(mode, input, output);
}
