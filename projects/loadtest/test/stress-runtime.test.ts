import assert from 'node:assert/strict';
import { mkdirSync, mkdtempSync, readFileSync, rmSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { test } from 'node:test';
import { LocalStressRuntime } from '../src/stress-runtime.js';

for (const foreign of [true, false]) {
  test(`failed launcher ${foreign ? 'never stops a foreign' : 'stops only its positively owned'} maintenance container`, async t => {
    const directory = mkdtempSync(path.resolve('projects/loadtest/test/.fixture-stress-runtime-'));
    t.after(() => rmSync(directory, { recursive: true, force: true }));
    mkdirSync(path.join(directory, 'results'));
    const reader = path.join(directory, 'reader');
    writeFileSync(reader, `#!${process.execPath}
console.log(JSON.stringify(process.argv[2] === 'summarize'
  ? {tables: {'telemetry.sqlserver.sqlserver_cpu_utilization': {rows: 2, files: [{rows: 2, path: 'part.parquet'}]}}}
  : {rows: 2, row_groups: [{}, {}]}));
`, { mode: 0o755 });
    const log = path.join(directory, 'commands.jsonl');
    const state = path.join(directory, 'container.json');
    const id = 'a'.repeat(64);
    writeFileSync(path.join(directory, 'docker'), `#!${process.execPath}
import {appendFileSync, readFileSync, writeFileSync} from 'node:fs';
const args = process.argv.slice(2);
const log = ${JSON.stringify(log)}, state = ${JSON.stringify(state)};
appendFileSync(log, JSON.stringify(args) + '\\n');
if (args[0] === 'compose') {
  const label = args[args.indexOf('--label') + 1].split('=')[1];
  writeFileSync(state, JSON.stringify({
    id: ${JSON.stringify(id)}, owner: ${foreign ? "'foreign-owner'" : 'label'},
    project: 'fixture-stress', service: 'delta-maintenance', state: {Running: true}
  }));
  process.exit(2);
}
const container = JSON.parse(readFileSync(state, 'utf8'));
if (args[0] === 'inspect') console.log(JSON.stringify(container));
else if (args[0] === 'stop') {
  container.state.Running = false;
  writeFileSync(state, JSON.stringify(container));
} else process.exit(3);
`, { mode: 0o755 });
    const runtime = new LocalStressRuntime({
      workspace: process.cwd(), directory, project: 'fixture-stress', composeFiles: [],
      env: { ...process.env, PATH: `${directory}:${process.env.PATH}` }, reader,
      directAdmin: 'http://127.0.0.1:1', dataflowAdmin: 'http://127.0.0.1:1',
      quiver: false, physicalRoots: {}, loadEnded: new AbortController().signal, checkRunning: () => {},
    });
    await assert.rejects(runtime.operate(1, Date.now() + 10_000), error => {
      if (foreign) {
        assert(error instanceof AggregateError);
        assert.match(String(error.errors[1]), /ownership is unverified/u);
      } else {
        assert(error instanceof Error && !(error instanceof AggregateError), 'preserve the actual launcher failure');
      }
      return true;
    });
    const commands: string[][] = readFileSync(log, 'utf8').trim().split('\n').map(line => JSON.parse(line));
    const mutations = commands.filter(args => ['stop', 'rm'].includes(args[0]));
    assert.deepEqual(mutations, foreign ? [] : [['stop', '--time', '300', id]]);
  });
}
