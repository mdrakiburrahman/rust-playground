import assert from 'node:assert/strict';
import { spawn } from 'node:child_process';
import { chmod, mkdtemp, readFile, readdir, rm, writeFile } from 'node:fs/promises';
import { createServer } from 'node:http';
import os from 'node:os';
import path from 'node:path';
import { test } from 'node:test';

import { startQapObserver } from '../src/qap-observer.js';

test('real helper processes isolate provenance from the concurrently appending loadgen', { timeout: 20_000 }, async t => {
  const root = await mkdtemp(path.join(os.tmpdir(), 'qap-provenance-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const fixturePath = path.join(root, 'fixture.json');
  await writeFile(fixturePath, JSON.stringify({
    observed_unix_ms: 1_800_000_000_000, backend: 'native-qap', mode: 'local',
    tables: Object.fromEntries(['direct', 'dataflow'].flatMap(pool => Array.from({ length: 13 }, (_, index) => [
      `${pool}_sqlserver_fixture_${index}`, { available: false, error: 'fixture unavailable' },
    ]))),
  }));
  const binary = path.join(root, 'observer.py');
  await writeFile(binary, `#!/usr/bin/env python3
import json, os, pathlib, sys, time
args = sys.argv[1:]
results = pathlib.Path(args[args.index('--results') + 1])
output = pathlib.Path(args[args.index('--output') + 1])
(results / 'arguments.json').write_text(json.dumps(args))
with (results / 'kusto-queries.jsonl').open('a') as log:
    for i in range(10):
        for char in json.dumps({'source': 'observer', 'index': i}) + '\\n':
            log.write(char)
            log.flush()
            time.sleep(0.0001)
with output.open('x') as out:
    out.write(pathlib.Path(os.environ['QAP_TEST_FIXTURE']).read_text())
`);
  await chmod(binary, 0o700);
  const writer = spawn(process.execPath, ['-e', `
    const fs = require('node:fs');
    (async () => {
      for (let i = 0; i < 10; i++) {
        for (const char of JSON.stringify({source:'loadgen',index:i})+'\\n') {
          fs.appendFileSync(process.argv[1], char);
          await new Promise(resolve => setTimeout(resolve, 1));
        }
      }
    })().catch(() => process.exit(1));
  `, path.join(root, 'kusto-queries.jsonl')], { stdio: 'ignore' });
  t.after(() => { writer.kill('SIGKILL'); });
  const writerEnded = new Promise<void>((resolve, reject) => {
    writer.once('error', reject);
    writer.once('exit', code => code === 0 ? resolve() : reject(new Error('Fixture writer failed')));
  });
  let exported = 0;
  let resolveExports: (() => void) | undefined;
  const twoExports = new Promise<void>(resolve => { resolveExports = resolve; });
  const server = createServer((request, response) => {
    request.resume().on('end', () => {
      response.writeHead(200, { 'content-type': 'application/json' }).end('{}');
      if (++exported >= 2) resolveExports?.();
    });
  });
  await new Promise<void>(resolve => server.listen(0, '127.0.0.1', resolve));
  t.after(() => new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve())));
  const address = server.address();
  assert(address && typeof address !== 'string');
  const stop = startQapObserver({
    binary, cwd: root, env: { ...process.env, QAP_TEST_FIXTURE: fixturePath }, results: root,
    endpoint: 'http://fixture.invalid', otlpEndpoint: `http://127.0.0.1:${address.port}`, intervalMs: 1,
  });
  t.after(stop);
  await twoExports;
  await stop();
  await writerEnded;
  const directories = await readdir(path.join(root, 'qap-observations'));
  assert(directories.length >= 2);
  for (const directory of directories) {
    const provenance = path.join(root, 'qap-observations', directory);
    const args = JSON.parse(await readFile(path.join(provenance, 'arguments.json'), 'utf8')) as string[];
    assert.equal(args[args.indexOf('--results') + 1], provenance);
    assert.notEqual(provenance, root);
    const lines = (await readFile(path.join(provenance, 'kusto-queries.jsonl'), 'utf8')).trim().split('\n');
    assert.equal(lines.length, 10);
    for (const line of lines) assert.equal((JSON.parse(line) as { source: string }).source, 'observer');
  }
  const lines = (await readFile(path.join(root, 'kusto-queries.jsonl'), 'utf8')).trim().split('\n');
  assert.equal(lines.length, 10);
  for (const line of lines) assert.equal((JSON.parse(line) as { source: string }).source, 'loadgen');
});
