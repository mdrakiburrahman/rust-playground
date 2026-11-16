import { mkdir, readFile, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { pathToFileURL } from 'node:url';

export interface CampaignMeasurement {
  cohort: string;
  candidate: string;
  factorX: string;
  valueX: string;
  factorY: string;
  valueY: string;
  repeat: number;
  committedRowsPerSecond: number | null;
  diedFirst?: string | null;
  comment?: string | null;
}

type Cell = { x: string; y: string; rates: number[] };
type Summary = { candidate: string; rates: number[]; mean: number; min: number; max: number };

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function requiredText(value: unknown, field: string): string {
  if (typeof value !== 'string' || value.trim() === '') throw new Error(field + ' must be a non-empty string');
  return value;
}

function escapeXml(value: string): string {
  const entities: Record<string, string> = { '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&apos;' };
  return value.replace(/[&<>"']/g, character => entities[character]!);
}

function numberLabel(value: number): string {
  return value.toLocaleString('en-US', { maximumFractionDigits: 2 });
}

export function validateCampaignMeasurements(value: unknown): CampaignMeasurement[] {
  if (!Array.isArray(value) || value.length === 0) throw new Error('results must be a non-empty array');
  return value.map((raw, index) => {
    if (!isRecord(raw)) throw new Error('result ' + index + ' must be an object');
    const rate = raw.committedRowsPerSecond;
    if (rate !== null && (typeof rate !== 'number' || !Number.isFinite(rate) || rate < 0)) {
      throw new Error('result ' + index + ' committedRowsPerSecond must be a non-negative number or null');
    }
    const diedFirst = raw.diedFirst == null ? null : requiredText(raw.diedFirst, 'result ' + index + ' diedFirst');
    const comment = raw.comment == null ? null : requiredText(raw.comment, 'result ' + index + ' comment');
    if (diedFirst?.toLowerCase() === 'kustainer' && (rate !== null || !comment)) {
      throw new Error('a dead Kustainer must have a null committed rate and a comment');
    }
    if (!Number.isSafeInteger(raw.repeat) || Number(raw.repeat) < 1) throw new Error('result ' + index + ' repeat must be a positive integer');
    return {
      cohort: requiredText(raw.cohort, 'result ' + index + ' cohort'),
      candidate: requiredText(raw.candidate, 'result ' + index + ' candidate'),
      factorX: requiredText(raw.factorX, 'result ' + index + ' factorX'),
      valueX: requiredText(raw.valueX, 'result ' + index + ' valueX'),
      factorY: requiredText(raw.factorY, 'result ' + index + ' factorY'),
      valueY: requiredText(raw.valueY, 'result ' + index + ' valueY'),
      repeat: Number(raw.repeat),
      committedRowsPerSecond: rate as number | null,
      diedFirst,
      comment,
    };
  });
}

function svgText(x: number, y: number, value: string, size = 13, weight = 'normal'): string {
  return '<text x="' + x + '" y="' + y + '" font-size="' + size + '" font-weight="' + weight + '">' + escapeXml(value) + '</text>';
}

function cellColor(value: number, minimum: number, maximum: number): string {
  const ratio = maximum === minimum ? 0.65 : (value - minimum) / (maximum - minimum);
  const red = Math.round(239 - ratio * 170);
  const green = Math.round(247 - ratio * 80);
  const blue = Math.round(255 - ratio * 15);
  return 'rgb(' + red + ',' + green + ',' + blue + ')';
}

function average(values: number[]): number {
  return values.reduce((sum, value) => sum + value, 0) / values.length;
}

export function renderCampaignResults(input: unknown, cohort: string): string {
  const allRows = validateCampaignMeasurements(input);
  const rows = allRows.filter(row => row.cohort === cohort);
  if (rows.length === 0) throw new Error('no results for cohort ' + cohort);
  const factorX = rows[0]!.factorX;
  const factorY = rows[0]!.factorY;
  if (rows.some(row => row.factorX !== factorX || row.factorY !== factorY)) {
    throw new Error('a cohort picture must use exactly one pair of factors');
  }
  const xValues = [...new Set(rows.map(row => row.valueX))].sort();
  const yValues = [...new Set(rows.map(row => row.valueY))].sort();
  const cells = new Map<string, Cell>();
  for (const row of rows) {
    const key = JSON.stringify([row.valueX, row.valueY]);
    const cell = cells.get(key) ?? { x: row.valueX, y: row.valueY, rates: [] };
    if (row.committedRowsPerSecond !== null) cell.rates.push(row.committedRowsPerSecond);
    cells.set(key, cell);
  }
  const rates = rows.flatMap(row => row.committedRowsPerSecond === null ? [] : [row.committedRowsPerSecond]);
  const minimum = rates.length ? Math.min(...rates) : 0;
  const maximum = rates.length ? Math.max(...rates) : 0;
  const groups = new Map<string, number[]>();
  for (const row of rows) {
    if (row.committedRowsPerSecond === null) continue;
    const values = groups.get(row.candidate) ?? [];
    values.push(row.committedRowsPerSecond);
    groups.set(row.candidate, values);
  }
  const leaderboard: Summary[] = [...groups].map(([candidate, values]) => ({
    candidate, rates: values, mean: average(values), min: Math.min(...values), max: Math.max(...values),
  })).sort((a, b) => b.mean - a.mean || a.candidate.localeCompare(b.candidate));
  const maxMean = Math.max(1, ...leaderboard.map(item => item.mean));
  const width = 1220;
  const heatX = 45;
  const heatY = 130;
  const cellWidth = Math.max(76, Math.min(150, 500 / Math.max(1, xValues.length)));
  const cellHeight = 42;
  const leaderboardX = 610;
  const height = Math.max(540, 230 + yValues.length * 38 + 55 + leaderboard.length * 38 + Math.min(rows.length, 8) * 25);
  const parts: string[] = [
    '<svg xmlns="http://www.w3.org/2000/svg" role="img" aria-labelledby="title desc" width="' + width + '" height="' + height + '" viewBox="0 0 ' + width + ' ' + height + '">',
    '<title id="title">' + escapeXml(cohort + ' throughput heatmap and repeat leaderboard') + '</title>',
    '<desc id="desc">Committed new rows per second; repeat ranges show observed error bars. Null rates are not plotted as zero.</desc>',
    '<rect width="100%" height="100%" fill="#ffffff"/>',
    '<g fill="#1f2328" font-family="Segoe UI, Arial, sans-serif">',
    svgText(40, 42, cohort + ' | committed new rows/s', 22, 'bold'),
    svgText(40, 76, factorX + ' × ' + factorY + ' heatmap', 16, 'bold'),
    svgText(leaderboardX, 76, 'Leaderboard | mean and repeat range', 16, 'bold'),
    svgText(heatX, heatY - 18, factorX + ' →', 12, 'bold'),
  ];
  xValues.forEach((value, index) => parts.push(svgText(heatX + 126 + index * cellWidth, heatY - 18, value, 11)));
  yValues.forEach((value, rowIndex) => {
    const y = heatY + rowIndex * cellHeight;
    parts.push(svgText(heatX, y + 25, value, 11));
    xValues.forEach((x, columnIndex) => {
      const cell = cells.get(JSON.stringify([x, value]));
      const xPosition = heatX + 120 + columnIndex * cellWidth;
      if (!cell || !cell.rates.length) {
        parts.push('<rect x="' + xPosition + '" y="' + y + '" width="' + (cellWidth - 4) + '" height="34" rx="4" fill="#f6f8fa" stroke="#d0d7de"/>');
        parts.push(svgText(xPosition + 8, y + 22, 'null', 11));
      } else {
        const mean = average(cell.rates);
        parts.push('<rect x="' + xPosition + '" y="' + y + '" width="' + (cellWidth - 4) + '" height="34" rx="4" fill="' + cellColor(mean, minimum, maximum) + '" stroke="#8c959f"/>');
        parts.push(svgText(xPosition + 8, y + 22, numberLabel(mean), 11, 'bold'));
      }
    });
  });
  parts.push(svgText(heatX, heatY + yValues.length * cellHeight + 20, factorY + ' ↓ | repeat means; null = unavailable', 11));
  const barStart = leaderboardX + 190;
  const barWidth = 300;
  leaderboard.forEach((item, index) => {
    const y = 112 + index * 38;
    parts.push(svgText(leaderboardX, y + 13, item.candidate, 12));
    const meanWidth = item.mean / maxMean * barWidth;
    const minX = barStart + item.min / maxMean * barWidth;
    const maxX = barStart + item.max / maxMean * barWidth;
    parts.push('<rect x="' + barStart + '" y="' + y + '" width="' + meanWidth + '" height="18" rx="3" fill="#218bff" opacity="0.75"/>');
    parts.push('<line x1="' + minX + '" y1="' + (y + 9) + '" x2="' + maxX + '" y2="' + (y + 9) + '" stroke="#1f2328" stroke-width="2"/>');
    parts.push('<line x1="' + minX + '" y1="' + (y + 4) + '" x2="' + minX + '" y2="' + (y + 14) + '" stroke="#1f2328"/>');
    parts.push('<line x1="' + maxX + '" y1="' + (y + 4) + '" x2="' + maxX + '" y2="' + (y + 14) + '" stroke="#1f2328"/>');
    parts.push(svgText(barStart + barWidth + 12, y + 13, numberLabel(item.mean) + ' (' + numberLabel(item.min) + '–' + numberLabel(item.max) + ')', 11));
  });
  const deaths = rows.filter(row => row.diedFirst);
  const noteY = Math.max(heatY + yValues.length * cellHeight + 70, 145 + leaderboard.length * 38);
  parts.push(svgText(40, noteY, 'What died first', 16, 'bold'));
  if (!deaths.length) parts.push(svgText(40, noteY + 25, 'No failure recorded in these rows.', 12));
  deaths.slice(0, 8).forEach((row, index) => {
    const rate = row.committedRowsPerSecond === null ? 'committed rows/s: null' : 'committed rows/s: ' + numberLabel(row.committedRowsPerSecond);
    const comment = row.comment ? ' — ' + row.comment : '';
    parts.push(svgText(40, noteY + 25 + index * 23, row.candidate + ' repeat ' + row.repeat + ': ' + row.diedFirst + ' (' + rate + ')' + comment, 11));
  });
  parts.push('</g></svg>');
  return parts.join('\n');
}

async function main(args: string[]): Promise<void> {
  let inputPath: string | undefined;
  let outputPath: string | undefined;
  for (let index = 0; index < args.length; index++) {
    const flag = args[index];
    if (flag === '--input') inputPath = args[++index];
    else if (flag === '--out') outputPath = args[++index];
    else throw new Error('unknown option: ' + flag);
    if (!args[index] || args[index].startsWith('--')) throw new Error('expected a value after ' + flag);
  }
  if (!inputPath || !outputPath) throw new Error('usage: campaign-results --input <results.json> --out <directory>');
  const rows = validateCampaignMeasurements(JSON.parse(await readFile(inputPath, 'utf8')));
  await mkdir(outputPath, { recursive: true });
  for (const cohort of [...new Set(rows.map(row => row.cohort))]) {
    if (!/^[A-Za-z0-9][A-Za-z0-9_-]{0,39}$/.test(cohort)) throw new Error('unsafe cohort name: ' + cohort);
    await writeFile(resolve(outputPath, cohort + '.svg'), renderCampaignResults(rows, cohort), 'utf8');
  }
}

if (process.argv[1] && import.meta.url === pathToFileURL(resolve(process.argv[1])).href) {
  main(process.argv.slice(2)).catch(error => {
    console.error(error instanceof Error ? error.message : String(error));
    process.exitCode = 1;
  });
}
