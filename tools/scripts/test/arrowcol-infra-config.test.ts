import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import fs from 'node:fs';
import os from 'node:os';
import path from 'node:path';
import { test } from 'node:test';

const source = path.resolve('projects/arrowcol-infra/scripts');

for (const scenario of [
  { name: 'accepts eleven migrations with scalar digest conversions', count: 11,
    sql: "SELECT CONVERT(varbinary(max), N'key');", succeeds: true },
  { name: 'accepts case-insensitive spaced scalar conversions', count: 11,
    sql: "SELECT convert ( varbinary ( max ) , N'key');", succeeds: true },
  { name: 'rejects the old ten-migration layout', count: 10,
    sql: 'SELECT 1;', succeeds: false, error: 'Expected exactly 11' },
  { name: 'rejects binary payload columns', count: 11,
    sql: 'CREATE TABLE payloads (bytes varbinary(max));', succeeds: false, error: 'payload byte storage' },
  { name: 'does not hide a payload column beside a scalar conversion', count: 11,
    sql: "SELECT CONVERT(varbinary(max), N'key'); CREATE TABLE payloads (bytes varbinary(max));",
    succeeds: false, error: 'payload byte storage' },
  { name: 'rejects image payload columns', count: 11,
    sql: 'CREATE TABLE payloads (bytes image);', succeeds: false, error: 'payload byte storage' },
]) {
  test(`SQL configuration guard ${scenario.name}`, () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'arrowcol-config-'));
    try {
      for (const dir of ['scripts', 'migrations', 'docker/sqlserver', 'bin']) {
        fs.mkdirSync(path.join(root, dir), { recursive: true });
      }
      for (const name of ['lib.sh', 'config-check.sh']) {
        fs.copyFileSync(path.join(source, name), path.join(root, 'scripts', name));
      }
      for (const name of ['docker', 'git']) {
        fs.writeFileSync(path.join(root, 'bin', name), '#!/usr/bin/env bash\nexit 0\n', { mode: 0o755 });
      }
      fs.writeFileSync(path.join(root, '.env.example'), 'COMPOSE_PROJECT_NAME=guard-test\n');
      fs.writeFileSync(path.join(root, '.dockerignore'), '.env\nsecrets/\n');
      fs.writeFileSync(path.join(root, 'docker/sqlserver/Dockerfile'), 'FROM fixture@sha256:fixture\n');
      for (let version = 1; version <= scenario.count; version++) {
        fs.writeFileSync(path.join(root, 'migrations', `V${String(version).padStart(4, '0')}__fixture.sql`),
          `-- $(MIGRATION_CHECKSUM)\n${version === scenario.count ? scenario.sql : 'SELECT 1;'}\n`);
      }
      const result = spawnSync('bash', [path.join(root, 'scripts/config-check.sh')], {
        encoding: 'utf8',
        timeout: 10_000,
        env: { ...process.env, PATH: `${path.join(root, 'bin')}:${process.env.PATH ?? ''}` },
      });
      assert.ifError(result.error);
      assert.equal(result.signal, null);
      const output = result.stdout + result.stderr;
      assert.equal(result.status, scenario.succeeds ? 0 : 1, output);
      if (scenario.error) assert.ok(output.includes(scenario.error), output);
    } finally {
      fs.rmSync(root, { recursive: true, force: true });
    }
  });
}
