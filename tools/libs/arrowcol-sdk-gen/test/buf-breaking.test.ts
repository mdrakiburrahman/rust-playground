import assert from 'node:assert/strict';
import { mkdtempSync, mkdirSync, rmSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import { spawnSync } from 'node:child_process';
import test from 'node:test';

test('Buf WIRE_JSON accepts optional append and rejects field rename, renumber and type change', () => {
  const directory = mkdtempSync(join(tmpdir(), 'arrowcol-buf-'));
  try {
    const previous = join(directory, 'previous');
    const current = join(directory, 'current');
    mkdirSync(previous);
    mkdirSync(current);
    const schema = (fields: string) =>
      `syntax = "proto2";\npackage arrowcol.telemetry.prod;\nmessage Events {\n${fields}\n}\n`;
    writeFileSync(join(previous, 'schema.proto'), schema('optional int64 id = 1;'));
    for (const [fields, expected] of [
      ['optional int64 id = 1;\noptional string status = 2;', 0],
      ['optional int64 renamed_id = 1;', 100],
      ['optional int64 id = 2;', 100],
      ['optional string id = 1;', 100],
    ] as const) {
      writeFileSync(join(current, 'schema.proto'), schema(fields));
      const result = spawnSync('npx', [
        '--no-install', 'buf', 'breaking', join(current, 'schema.proto'),
        '--against', join(previous, 'schema.proto'),
        '--config', resolve('buf.yaml'),
      ], { encoding: 'utf8' });
      assert.equal(result.status, expected, `${fields}\n${result.stdout}\n${result.stderr}`);
      assert.equal(result.error, undefined);
    }
  } finally {
    rmSync(directory, { recursive: true, force: true });
  }
});
