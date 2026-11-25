// Derives the Lane P runner fixture from the pinned BC6 harness. Deterministic: same inputs give byte-identical output.
import fs from 'node:fs';
import path from 'node:path';
import { createHash } from 'node:crypto';
import { fileURLToPath } from 'node:url';

const dir = path.dirname(fileURLToPath(import.meta.url));
const PINNED = {
  'score.ts': '01485307262090ba46b7871083730e73fce04d83b975c7255d8a0463b41a0263',
  'measure.ts': 'f3dd1d6f2d58ae93174bc4a1424a537c3145df8e33176b61269a63b0f4d38cd4',
  'base.yaml': '3beace90a22e64ba09b22b5cee4895cb8f9758c5433addace097a479e45316a6',
};
const [reference, out] = process.argv.slice(2);
if (!out || !reference) throw Error('Usage: node prepare-harness.mjs <reference-dir> <fresh-output-dir>');
const hash = buf => createHash('sha256').update(buf).digest('hex');
const sources = Object.fromEntries(Object.keys(PINNED).map(file => [file, fs.readFileSync(path.join(reference, 'harness', file))]));
for (const [file, sha] of Object.entries(PINNED)) if (hash(sources[file]) !== sha) throw Error(`Unrecognized reference ${file}`);
let runner = sources['score.ts'].toString('utf8');
function replace(a, b) {
  if (runner.split(a).length !== 2) throw Error('Lost unique runner anchor: ' + a.slice(0, 90));
  runner = runner.replace(a, () => b);
}
function replaceBetween(left, right, b) {
  const start = runner.indexOf(left), end = runner.indexOf(right);
  if (start < 0 || end < start || runner.indexOf(left, start + 1) >= 0) throw Error('Lost runner block: ' + left);
  replace(runner.slice(start, end), b);
}

replace("import os from 'node:os';", `import os from 'node:os';
import { audit } from './audit.mjs';
import { validatePlan, identityProblems, resumeProblems, HARNESS_FILES, BINARIES } from './policy.mjs';`);
replaceBetween('function environment(value: unknown)', 'function start(binary', `function parsePlan(file: string): Plan {
  return validatePlan(JSON.parse(fs.readFileSync(file, 'utf8'))) as Plan;
}

function hostMemory(): Record<string, number> {
  const counters: Record<string, number> = {};
  for (const line of fs.readFileSync('/proc/vmstat', 'utf8').split('\\n')) {
    const [name, value] = line.split(' ');
    if (/^(allocstall_|pgscan_direct|compact_stall|thp_fault_alloc)/.test(name)) counters[name] = Number(value);
  }
  for (const line of fs.readFileSync('/proc/meminfo', 'utf8').split('\\n')) {
    const m = /^(MemFree|MemAvailable|Cached):\\s+(\\d+) kB/.exec(line);
    if (m) counters[m[1] + '_kb'] = Number(m[2]);
  }
  return counters;
}

`);
replace("    kustainer_comment: 'Not started: collector-only local filesystem experiment, no Kustainer claim',",
`    kustainer_comment: 'Not started: collector-only local filesystem experiment, no Kustainer claim',
    table_creation: 'arrowcol-loadgen initialize-local (the OAT local initializer), then catalog DDL adoption',`);
replace("    const catalog = start(path.join(bin, 'arrowcol-catalog-rust'),",
`    if ((env.BWA_TABLE ?? 'sqlserver_availability_replica_states') !== 'sqlserver_availability_replica_states')
      throw new Error('The table initializer supports only the campaign hot table');
    const initializer = start(path.join(bin, 'arrowcol-loadgen'), ['initialize-local', '--partition-by-ingestion-date',
      '--table', 'telemetry.sqlserver.sqlserver_availability_replica_states', '--dataflow-root', root],
      env, path.join(run, 'initialize.log'), kcpus);
    if (await initializer.done !== 0) throw new Error(\`Table initializer failed: \${fs.readFileSync(initializer.log, 'utf8').slice(-700)}\`);
    const catalog = start(path.join(bin, 'arrowcol-catalog-rust'),`);
replace('    const before = snapshot(root), cpuBefore = proc(collector.child.pid!);',
  '    const before = snapshot(root), cpuBefore = proc(collector.child.pid!), memoryBefore = hostMemory();');
replace('    const after = snapshot(root), cpuAfter = proc(collector.child.pid!);',
  '    const after = snapshot(root), cpuAfter = proc(collector.child.pid!), memoryAfter = hostMemory();');
replace('      effective_startup: startup,', `      effective_startup: startup,
      table_initializer: fs.readFileSync(path.join(run, 'initialize.log'), 'utf8').split('\\n')
        .filter((line) => line.startsWith('arrowcol_delta_stats_config '))
        .map((line) => JSON.parse(line.slice('arrowcol_delta_stats_config '.length))),
      host_memory: { start: memoryBefore, end: memoryAfter,
        note: 'Non-scoring diagnostic: host-wide reclaim, compaction and THP counters at the window bounds' },`);
replace("      binaries: Object.fromEntries(['arrowcol-dataflow-rust', 'arrowcol-dmv', 'arrowcol-catalog-rust'].map((name) =>",
  '      binaries: Object.fromEntries(BINARIES.map((name: string) =>');
replaceBetween('  if (valid && !diagnostic) {', '  fs.appendFileSync(results, JSON.stringify(result)', `  if (valid && !diagnostic) {
    try {
      const r = result as any;
      const problems = identityProblems(r);
      if (problems.length || r.retry_offered_rps !== 0 || !(r.committed_rps > 0))
        throw Error('Identity, retry or empty-window check failed: ' + [...problems,
          ...(r.retry_offered_rps !== 0 ? [\`retry_offered_rps=\${r.retry_offered_rps}\`] : []),
          ...(r.committed_rps > 0 ? [] : [\`committed_rps=\${r.committed_rps}\`])].join('; '));
      const footer = path.join(here, 'footer');
      if (createHash('sha256').update(fs.readFileSync(footer)).digest('hex') !== r.footer_sha256)
        throw Error('Footer tool changed after startup');
      r.physical = audit(r, root, run, footer);
    } catch (e) {
      valid = false;
      result = {...result, status:'failed_physical_audit', committed_rps:null,
        comment: e instanceof Error ? e.message : String(e)};
    }
  }
`);
replace('  fs.rmSync(root, { recursive: true });', '');
replace('  const offset = children.length;', `  const offset = children.length;
  const free = () => { const v = fs.statfsSync(root); return v.bavail * v.bsize; };
  const floor = Math.max(24 * 2**30, 0.15 * fs.statfsSync(root).blocks * fs.statfsSync(root).bsize);
  if (free() < floor + 12 * 2**30) throw Error('Insufficient storage above preserved fixture reserve; no arm launched');
  let storageStop = false;
  const guard = setInterval(() => {
    if (!storageStop && free() < floor) {
      storageStop = true;
      for (const own of children.slice(offset).reverse()) if (own.code === null && own.child.pid) own.child.kill('SIGTERM');
    }
  }, 500);`);
replace('    for (const child of children.slice(offset).reverse()) await stop(child);',
`    clearInterval(guard);
    for (const child of children.slice(offset).reverse()) await stop(child);
    if (storageStop) { valid = false; result = { ...metadata, status:'failed_storage_reserve', committed_rps:null,
      comment:'Storage reserve crossed; stopped this fixture only. No existing data was removed.' }; }`);
replace('  const plan = parsePlan(process.argv[2]);', `  for (const name of Object.keys(process.env)) {
    if (/^(ARROWCOL_|BWA_|MIMALLOC_)/.test(name) && name !== 'BWA_BIN') throw Error('Untracked ambient override: '+name);
  }
  const plan = parsePlan(process.argv[2]);`);
replace('    const identity = {', `    const sha = (file: string) => createHash('sha256').update(fs.readFileSync(file)).digest('hex');
    const identity = {`);
replace("      harness_sha256: createHash('sha256').update(fs.readFileSync(process.argv[1])).digest('hex'),",
`      harness_sha256: sha(path.join(here, 'score.ts')),
      harness_files: Object.fromEntries(HARNESS_FILES.map((file: string) => [file, sha(path.join(here, file))])),
      footer_sha256: sha(path.join(here, 'footer')),
      plan_sha256: sha(process.argv[2]),
      node_version: process.version,
      allocator_env: { GLIBC_TUNABLES: process.env.GLIBC_TUNABLES ?? null, LD_PRELOAD: process.env.LD_PRELOAD ?? null },
      storage: 'local_filesystem',`);
replaceBetween('    const done = new Set(', '      console.log(`CELL_START', `    const problems = identityProblems(identity);
    if (problems.length) throw Error('Refusing to run without a complete identity: ' + problems.join('; '));
    const prior = fs.existsSync(results)
      ? fs.readFileSync(results, 'utf8').split('\\n').filter(Boolean).map((line) => JSON.parse(line)) : [];
    const refused = resumeProblems(prior, plan, identity, output);
    if (refused.length) throw Error('Refusing incompatible or replayed output: ' + refused.join('; '));
    const probeOnly = plan.cells.every((cell) => cell.grid === 'probe');
    for (const cell of plan.cells.slice(prior.length)) {
`);
replace('if (!await runCell(cell, plan, identity)) process.exitCode = 1;',
  'if (!await runCell(cell, plan, identity)) { process.exitCode = 1; if (!probeOnly) break; }');
for (const stale of ['/tmp/bw-bc6-67a58eba-tools', 'readFileSync(process.argv[1])', 'done.has(', 'function environment(']) {
  if (runner.includes(stale)) throw Error('Stale BC6 runner logic survived: ' + stale);
}

fs.mkdirSync(out, { recursive: false });
for (const file of ['base.yaml', 'measure.ts']) fs.writeFileSync(path.join(out, file), sources[file], { flag: 'wx' });
for (const file of ['audit.mjs', 'policy.mjs']) fs.copyFileSync(path.join(dir, file), path.join(out, file), fs.constants.COPYFILE_EXCL);
fs.writeFileSync(path.join(out, 'score.ts'), runner, { flag: 'wx' });
