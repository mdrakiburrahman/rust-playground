import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtemp, rm, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import test from 'node:test';

test('real Compose merge removes fixed networking and preserves daemon-selected port bindings', async () => {
  const repo = fileURLToPath(new URL('../..', import.meta.url));
  const temporary = await mkdtemp(path.join(os.tmpdir(), 'oat-compose-config-'));
  const secret = path.join(temporary, 'empty-test-secret');
  await writeFile(secret, '');
  try {
    const result = spawnSync('docker', [
      'compose', '--env-file', '/dev/null',
      '--file', 'projects/loadtest/compose.yaml', '--file', 'projects/loadtest/compose.oat.yaml',
      '--project-name', 'oat-compose-config-test', '--profile', 'benchmark', '--profile', 'oat',
      'config', '--format', 'json',
    ], {
      cwd: repo,
      stdio: ['ignore', 'pipe', 'pipe'],
      encoding: 'utf8',
      env: {
        ...process.env,
        LOADTEST_UID: '10001', LOADTEST_GID: '10001', LOADTEST_WORKSPACE: repo, LOADTEST_BIN_HOST_PATH: repo,
        ONELAKE_HOST_PATH: temporary, OAT_CONTROL_HOST_PATH: temporary, OAT_PIN_INIT_CONFIG_HOST_PATH: secret,
        SQLSERVER_SA_PASSWORD_FILE: secret, ARROWCOL_APP_PASSWORD_FILE: secret, ARROWCOL_MIGRATOR_PASSWORD_FILE: secret,
        LOADTEST_EDGE_HMAC_FILE: secret, LOADTEST_INTERNAL_HMAC_FILE: secret,
        LOADTEST_BIND_HOST: '172.18.0.1',
        SQLSERVER_PORT: '0', LOADTEST_CATALOG_PORT: '0', KUSTO_PORT: '0', GRAFANA_PORT: '0', LOADTEST_ROUTER_PORT: '0',
      },
    });
    assert.equal(result.error, undefined);
    assert.equal(result.status, 0, result.stderr);
    const resolved = JSON.parse(result.stdout) as {
      services: Record<string, {
        environment?: Record<string, string>;
        networks: Record<string, { ipv4_address?: string }>;
        ports?: Array<{ host_ip: string; published: string }>;
      }>;
      networks: Record<string, { ipam?: { config?: unknown[] } }>;
    };
    for (const service of ['sqlserver', 'arrowcol-catalog', 'dataflow-1', 'kusto', 'lgtm', 'router']) {
      assert.equal(resolved.services[service]?.networks.default?.ipv4_address, undefined, service);
      for (const port of resolved.services[service]?.ports ?? []) {
        assert.equal(port.host_ip, '172.18.0.1', service);
        assert.equal(port.published, '0', service);
      }
    }
    assert.equal(
      resolved.services['arrowcol-catalog']?.environment?.ARROWCOL_CATALOG_DDL_IDENTITY_SECRET_FILE,
      '/run/secrets/internal-hmac',
    );
    assert.equal(resolved.networks.default?.ipam?.config?.length ?? 0, 0);
  } finally {
    await rm(temporary, { recursive: true, force: true });
  }
});
