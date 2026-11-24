import fs from 'node:fs';
import { createHash } from 'node:crypto';

type Cell = { id: string; grid: string; x: string; y: string; env: Record<string, string> };
const cells: Cell[] = [];
const common: Record<string, string> = {
  BWA_CORES: '24', BWA_NCLIENTS: '2', BWA_PD: '16', BWA_MCF: '24',
  BWA_CLIENT_SPW: '1',
  BWA_BUF_BYTES: '17200000000', BWA_GCAP_BYTES: '17200000000', BWA_TCAP_BYTES: '16000000000',
  BWA_BUF_ROWS: '100000000', BWA_GCAP_ROWS: '100000000', BWA_TCAP_ROWS: '100000000',
  ARROWCOL_COLLECTOR_PARQUET_COLUMN_WORKERS: '1',
};
const control = (id: string) => cells.push({ id, grid: 'control', x: 'reference', y: id, env: {} });
const cell = (grid: string, x: string | number, y: string | number, env: Record<string, string>) =>
  ({ id: `${grid}-${x}-${y}`, grid, x: String(x), y: String(y), env });
const capacity = (gb: number) => ({
  BWA_TCAP_BYTES: String(gb * 1e9), BWA_GCAP_BYTES: String(gb * 1.075e9), BWA_BUF_BYTES: String(gb * 1.075e9),
});
function grid(name: string, xs: (string | number)[], ys: (string | number)[],
  overrides: (x: string, y: string) => Record<string, string>): void {
  const batch = xs.flatMap((x) => ys.map((y) => cell(name, x, y, overrides(String(x), String(y)))));
  batch.sort((a, b) => createHash('sha256').update(a.id).digest('hex').localeCompare(createHash('sha256').update(b.id).digest('hex')));
  cells.push(...batch);
  control(`${name}-reference`);
}

const kind = process.argv[2];
if (kind === 'control') {
  control('control-a1');
  grid('offered', [1, 2, 4], [1, 4], (x, y) => ({ BWA_NCLIENTS: x, BWA_CLIENT_SPW: y }));
  control('control-a2');
  control('control-a3');
} else if (kind === 'screen') {
  control('screen-reference-start');
  grid('depth-flush', [1, 8, 32], [4, 12, 32], (x, y) => ({ BWA_PD: x, BWA_MCF: y }));
  grid('window-files', ['100ms', '500ms', '2s'], [8, 32, 128], (x, y) => ({ BWA_BW: x, BWA_MFC: y }));
  grid('rows-target', [262144, 1048576, 2000000], [8388608, 33554432, 134217728],
    (x, y) => ({ BWA_MRPF: x, BWA_TARGET: y }));
  grid('capacity-flush', [4, 8, 16], [4, 12, 32], (x, y) => ({ ...capacity(Number(x)), BWA_MCF: y }));
  grid('capacity-handoff', [4, 8, 16], [0, 4, 16], (x, y) => ({
    ...capacity(Number(x)), ARROWCOL_COLLECTOR_BATCHING_CONFIG: JSON.stringify({ max_pending_commit_bytes: Number(y) * 1e9 }),
  }));
  grid('capacity-stage', [4, 8, 16], [1, 4, 8], (x, y) => ({ ...capacity(Number(x)), BWA_SFC: y }));
  grid('cpu-blocking', [4, 12, 24], [16, 48, 128], (x, y) =>
    ({ ARROWCOL_COLLECTOR_CPU_WORKERS: x, ARROWCOL_COLLECTOR_BLOCKING_WORKERS: y }));
  grid('runtime-upload', [2, 6, 12], [8, 32, 64], (x, y) =>
    ({ ARROWCOL_COLLECTOR_RUNTIME_WORKERS: x, ARROWCOL_COLLECTOR_UPLOAD_LIMIT: y }));
  grid('streams-clients', [32, 128, 256], [1, 2, 4], (x, y) => ({ BWA_STREAMS: x, BWA_NCLIENTS: y }));
  grid('txn-pending', ['per_id', 'batched', 'batched_cached'], [128, 512, 1024],
    (x, y) => ({ BWA_TXN: x, BWA_MPF: y }));
} else if (kind === 'checkpoint') {
  grid('txn-checkpoint', ['per_id', 'batched', 'batched_cached'], ['off', '1', '5'],
    (x, y): Record<string, string> => y === 'off' ? { BWA_TXN: x } : { BWA_TXN: x, BWA_CHECKPOINT_EVERY_COMMITS: y });
} else if (kind === 'capacity') {
  grid('capacity-sink', ['l0', 'l1'], [1, 2, 4], (x, y) => ({ BWA_ISOLATION: x, BWA_NCLIENTS: y }));
} else {
  throw new Error('Expected plan kind: control, screen, checkpoint or capacity');
}
fs.writeFileSync(process.argv[3], JSON.stringify({
  duration: kind === 'capacity' ? 20 : kind === 'screen' ? 40 : 60,
  warmup: kind === 'capacity' ? 5 : 20, common, cells,
}, null, 2) + '\n');
