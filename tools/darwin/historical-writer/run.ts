import { spawnSync } from 'node:child_process';
import { mkdirSync, writeFileSync } from 'node:fs';
import { resolve } from 'node:path';

const args = process.argv.slice(2);
if (args.length !== 4 || args[0] !== '--client' || args[2] !== '--out') {
  throw new Error('Usage: npx --no-install tsx tools/darwin/historical-writer/run.ts --client <pinned-binary> --out <new-directory>');
}
const client = resolve(args[1]);
const output = resolve(args[3]);
mkdirSync(output);

const env: NodeJS.ProcessEnv = {};
for (const name of ['PATH', 'HOME', 'CARGO_HOME', 'RUSTUP_HOME', 'CARGO_TARGET_DIR', 'TMPDIR']) {
  if (process.env[name] !== undefined) env[name] = process.env[name];
}
Object.assign(env, {
  DARWIN_HISTORICAL_CLIENT: client,
  ARROWCOL_LAYOUT_ROW_GROUP_ROWS: '262144',
  ARROWCOL_LAYOUT_MAX_INPUT_BYTES: '1073741824',
  ARROWCOL_LAYOUT_MAX_ROWS: '2000000',
  ARROWCOL_PARTITION_GRANULARITY: 'hour',
});
const candidate = '{"columns":{"last_connect_error_number":{"dictionary_enabled":false}}}';
const testArgs = ['test', '--locked', '--release', '-p', 'arrowcol-dataflow-components', '--test', 'darwin_historical_writer'];

function execute(name: string, command: string, commandArgs: string[], runEnv: NodeJS.ProcessEnv): string {
  const result = spawnSync(command, commandArgs, { env: runEnv, encoding: 'utf8', timeout: 600_000, maxBuffer: 8 * 1024 * 1024 });
  writeFileSync(`${output}/${name}.stdout`, result.stdout ?? '');
  writeFileSync(`${output}/${name}.stderr`, result.stderr ?? '');
  writeFileSync(`${output}/${name}.json`, JSON.stringify({
    command, args: commandArgs, status: result.status, signal: result.signal, error: result.error?.message ?? null,
  }) + '\n');
  if (result.error || result.status !== 0) throw new Error(`${name} failed: ${result.error?.message ?? result.status}`);
  return result.stdout;
}

execute('build', 'cargo', [...testArgs, '--no-run'], env);
let corpus: string | undefined;
let layout: string | undefined;
let otherFooters: string | undefined;
const observations: { pair: number; arm: string; result: string }[] = [];
for (let pair = 1; pair <= 3; pair++) {
  for (const arm of pair === 2 ? ['candidate', 'baseline'] : ['baseline', 'candidate']) {
    const runEnv = { ...env };
    if (arm === 'candidate') runEnv.ARROWCOL_PARQUET_CONFIG = candidate;
    const name = `${pair}-${arm}`;
    const stdout = execute(name, 'cargo', [...testArgs, '--', '--exact', 'historical_delta_writer', '--nocapture'], runEnv);
    const lines = stdout.split('\n');
    const one = (prefix: string) => {
      const matches = lines.filter(line => line.startsWith(prefix));
      if (matches.length !== 1) throw new Error(`${name}: expected one ${prefix} line`);
      return matches[0];
    };
    const currentCorpus = one('HISTORICAL_CORPUS ');
    const currentLayout = one('HISTORICAL_LAYOUT ');
    const footers = lines.filter(line => line.startsWith('HISTORICAL_FOOTER ') && !line.startsWith('HISTORICAL_FOOTER column=last_connect_error_number ')).join('\n');
    if (!footers || (corpus !== undefined && corpus !== currentCorpus) ||
        (layout !== undefined && layout !== currentLayout) ||
        (otherFooters !== undefined && otherFooters !== footers)) {
      throw new Error(`${name}: corpus, layout, or noncandidate footer differs`);
    }
    corpus = currentCorpus;
    layout = currentLayout;
    otherFooters = footers;
    const result = one('HISTORICAL_RESULT ');
    if (!result.includes('row_groups=1') || !result.includes('codecs={"UNCOMPRESSED"}') ||
        !result.includes('decoded_ordered_values=true')) throw new Error(`${name}: invalid physical output`);
    observations.push({ pair, arm, result });
  }
}
writeFileSync(`${output}/summary.json`, JSON.stringify({ corpus, layout, candidate, observations }, null, 2) + '\n');
console.log(JSON.stringify({ corpus, layout, candidate, observations }, null, 2));
