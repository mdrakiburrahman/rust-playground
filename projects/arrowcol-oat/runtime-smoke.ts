import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { randomBytes, randomUUID } from 'node:crypto';
import { mkdir, readFile, rm, writeFile } from 'node:fs/promises';
import { resolve } from 'node:path';
import { parse, stringify } from 'yaml';

// This rehearsal has no Kubernetes/Azure calls or credential mounts.
const prefix = `oat-smoke-${randomUUID().slice(0, 8)}`;
const root = resolve('out/arrowcol-oat-smoke', prefix);
const network = prefix;
const containers: string[] = [];
const volumes = ['secrets', 'catalog', 'warehouse'].map((name) => `${prefix}-${name}`);
const images = {
  sql: 'arrowcol-oat/sqlserver:local', catalog: 'arrowcol-oat/catalog:local',
  dataflow: 'arrowcol-oat/dataflow:local', router: 'arrowcol-oat/router:local', client: 'arrowcol-oat/client:local',
};
function docker(args: string[], input?: string): string {
  const result = spawnSync('docker', args, { encoding: 'utf8', input, maxBuffer: 8 * 1024 * 1024 });
  if (result.status !== 0) throw new Error(`docker ${args[0]} failed: ${result.stderr}`);
  return result.stdout.trim();
}
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));
async function waitReady(name: string, command: string[]): Promise<void> {
  for (let i = 0; i < 120; i++) {
    const result = spawnSync('docker', ['exec', name, ...command], { encoding: 'utf8' });
    if (result.status === 0) return;
    if (docker(['inspect', '-f', '{{.State.Running}}', name]) !== 'true') break;
    await sleep(1000);
  }
  throw new Error(`${name} not ready:\n${docker(['logs', name])}`);
}
function run(name: string, args: string[], image: string, command: string[] = []): void {
  containers.push(name);
  docker(['run', '-d', '--name', name, '--network', network, ...args, image, ...command]);
}
function stop(name: string): void {
  docker(['stop', '--time', '120', name]);
  const code = docker(['inspect', '-f', '{{.State.ExitCode}}', name]);
  assert.equal(code, '0', `${name} must drain cleanly, not be killed`);
  docker(['rm', name]);
  containers.splice(containers.indexOf(name), 1);
}
const http = (port: number, path: string) => ['curl', '-fsS', '--max-time', '3', `http://127.0.0.1:${port}${path}`];
const secretMount = ['-v', `${volumes[0]}:/run/oat:ro`];

async function main(): Promise<void> {
  await mkdir(root, { recursive: true, mode: 0o700 });
  docker(['network', 'create', '--internal', network]);
  for (const volume of volumes) docker(['volume', 'create', volume]);
  const helper = `${prefix}-files`;
  run(helper, ['--user', '0', '-v', `${volumes[0]}:/run/oat`, '-v', `${volumes[1]}:/data`,
    '-v', `${volumes[2]}:/onelake`, '--entrypoint', 'sleep'], images.catalog, ['infinity']);
  for (const key of ['sa-password', 'app-password', 'migration-password', 'edge-hmac', 'internal-hmac']) {
    const file = resolve(root, key);
    await writeFile(file, `Aa1!${randomBytes(32).toString('hex')}`, { mode: 0o600 });
    docker(['cp', file, `${helper}:/run/oat/${key}`]);
  }
  const dataflow = parse(await readFile('projects/arrowcol-dataflow-rust/config.yaml', 'utf8'));
  delete dataflow.policies.resources;
  const router = parse(await readFile('projects/arrowcol-router-rust/config.docker.yaml', 'utf8'));
  router.auth.authenticator = 'dev_only_allow_all';
  router.sql_server.host = `${prefix}-sql`;
  router.sql_server.trust_server_certificate = true;
  router.sql_server.password_file = '/run/oat/app-password';
  router.auth.edge_secret_file = '/run/oat/edge-hmac';
  router.auth.internal_signing_secret_file = '/run/oat/internal-hmac';
  router.upstream.tls = false;
  router.upstream.sni = '';
  router.upstream.max_h2_streams = 1;
  router.upstream.idle_timeout_seconds = 0;
  router.routing.distinct_table_owners = false;
  router.routing.warmup_ms = 1000;
  for (const [name, data] of [['dataflow.yaml', dataflow], ['router.yaml', router]] as const) {
    const file = resolve(root, name);
    await writeFile(file, stringify(data), { mode: 0o600 });
    docker(['cp', file, `${helper}:/run/oat/${name}`]);
  }
  docker(['exec', helper, 'sh', '-ec', 'chown -R 10001:10001 /run/oat /data /onelake; chmod 750 /run/oat /data /onelake; chmod 440 /run/oat/*']);
  docker(['rm', '-f', helper]); containers.splice(containers.indexOf(helper), 1);
  const sql = `${prefix}-sql`;
  run(sql, [...secretMount, '-e', 'ACCEPT_EULA=Y', '-e', 'MSSQL_PID=Developer', '-e', 'MSSQL_MEMORY_LIMIT_MB=2048',
    '-e', 'MSSQL_SA_PASSWORD_FILE=/run/oat/sa-password'], images.sql);
  await waitReady(sql, ['/usr/local/bin/sqlserver-healthcheck']);
  const bootstrap = ['run', '--rm', '--network', network, ...secretMount, '-e', `SQLSERVER_HOST=${sql}`];
  docker([...bootstrap, '-e', 'SQLSERVER_SA_PASSWORD_FILE=/run/oat/sa-password',
    '-e', 'ARROWCOL_APP_PASSWORD_FILE=/run/oat/app-password', '-e', 'ARROWCOL_MIGRATOR_PASSWORD_FILE=/run/oat/migration-password',
    '--entrypoint', '/usr/local/bin/database-bootstrap', images.sql]);
  for (let i = 0; i < 2; i++) docker([...bootstrap, '-e', 'ARROWCOL_MIGRATOR_PASSWORD_FILE=/run/oat/migration-password',
    '--entrypoint', '/usr/local/bin/arrowcol-migrate', images.sql]);
  const catalog = `${prefix}-catalog`;
  const startCatalog = () => run(catalog, ['-v', `${volumes[0]}:/run/oat:ro`, '-v', `${volumes[1]}:/data`,
    '-v', `${volumes[2]}:/onelake`], images.catalog,
  ['--warehouse-root', '/onelake/dataflow', '--ingestion-layout',
    '--ddl-identity-secret-file', '/run/oat/internal-hmac']);
  startCatalog();
  await waitReady(catalog, ['/usr/local/bin/arrowcol-catalog-rust', '--healthcheck']);
  const routerName = `${prefix}-router`;
  run(routerName, secretMount, images.router, ['--config', '/run/oat/router.yaml']);
  const tables = ['sqlserver_wait_stats', 'sqlserver_cpu_utilization'];
  const fileCounts = new Map<string, number>();
  let upstreamConnections = 0;
  for (const replicas of [1, 2]) {
    for (const cores of [1, 2]) {
      const collectors: string[] = [];
      for (let i = 0; i < replicas; i++) {
        const name = `${prefix}-r${replicas}-c${cores}-${i}`;
        collectors.push(name);
        run(name, [...secretMount, '-v', `${volumes[2]}:/onelake`,
          '-e', `ARROWCOL_SQL_HOST=${sql}`, '-e', 'ARROWCOL_SQL_PASSWORD_FILE=/run/oat/app-password',
          '-e', `ARROWCOL_CATALOG_ENDPOINT=http://${catalog}:8817`,
          '-e', `ARROWCOL_INSTANCE_KEY=${name}`, '-e', 'ARROWCOL_DELTA_ROOT=/onelake/dataflow',
          '-e', 'ARROWCOL_PARTITION_GRANULARITY=hour',
          '--entrypoint', '/bin/sh'], images.dataflow,
        ['-ec', `export ARROWCOL_ADVERTISED_FLIGHT_ENDPOINT="$(hostname -i):32010"; exec /usr/bin/tini -- /usr/local/bin/arrowcol-dataflow-rust --config /run/oat/dataflow.yaml --num-cores ${cores}`]);
        await waitReady(name, http(32011, '/readyz'));
        const listeners = docker(['exec', name, 'sh', '-c', 'cat /proc/net/tcp /proc/net/tcp6']);
        // 32010=7D0A, 32011=7D0B. Every worker must bind both SO_REUSEPORT listeners.
        for (const port of ['7D0A', '7D0B']) {
          assert.equal(listeners.split('\n').filter((line) => new RegExp(`:${port} .* 0A `).test(line)).length, cores,
            `expected ${cores} active ${port} worker listeners`);
        }
      }
      await waitReady(routerName, http(50050, '/readyz'));
      await sleep(32_000);
      for (const table of tables.slice(0, replicas)) {
        const args = ['run', '--rm', '--network', network, ...secretMount,
          '-e', 'LOADTEST_INTERNAL_SECRET_FILE=/run/oat/internal-hmac', images.client,
          '--endpoint', `http://${routerName}:50050`, '--table', table, '--rows', '100', '--concurrency', '4'];
        const unsigned = spawnSync('docker', args, { encoding: 'utf8' });
        assert.notEqual(unsigned.status, 0, 'router must reject unsigned requests');
        docker([...args, '--edge-secret-file', '/run/oat/edge-hmac']);
      }
      await sleep(8_000);
      const assignments = docker(['exec', routerName, ...http(50050, '/metrics')]);
      assert.match(assignments, /arrowcol_router_distinct_table_owners 0/);
      const connections = Number(/^arrowcol_router_upstream_connections_total\{reused="false"\} (\d+)$/m.exec(assignments)?.[1]);
      assert.ok(connections - upstreamConnections >= replicas * 4, 'router must preserve independent upstream connections');
      upstreamConnections = connections;
      const owners = tables.slice(0, replicas).map((table) => {
        const lines = assignments.split('\n').filter((line) => line.startsWith('arrowcol_router_table_owners{') &&
          line.includes(`table="${table}"`) && line.includes('state="active"') && line.endsWith(' 1'));
        assert.equal(lines.length, 1, `${table} must have exactly one active SQL lease`);
        return /collector="([^"]+)"/.exec(lines[0])?.[1];
      });
      assert.ok(owners.every((owner) => collectors.includes(owner ?? '')), 'every table must have one active owner');
      for (const name of collectors) stop(name);
      // Check physical output, not merely admission acknowledgements.
      const files = docker(['exec', catalog, 'sh', '-c', 'find /onelake/dataflow -name "*.parquet" -type f']);
      for (const table of tables.slice(0, replicas)) {
        const count = files.split('\n').filter((file) => file.includes(`/${table}/`)).length;
        assert.ok(count > (fileCounts.get(table) ?? 0), `no newly persisted ${table}`);
        fileCounts.set(table, count);
      }
      console.log(`PASS replicas=${replicas} cores=${cores}: authenticated Flight, independent upstream connections, SQL routing, per-core listeners and Delta files`);
    }
  }
  stop(catalog); startCatalog();
  await waitReady(catalog, ['/usr/local/bin/arrowcol-catalog-rust', '--healthcheck']);
  stop(catalog); stop(routerName);
  console.log('PASS catalog persistence/restart and repeated SQL migrations; no cloud benchmark evidence produced.');
}

try { await main(); }
catch (error) {
  for (const name of containers) {
    const logs = spawnSync('docker', ['logs', '--tail', '30', name], { encoding: 'utf8' });
    console.error(`${name}:\n${logs.stdout}\n${logs.stderr}`);
  }
  throw error;
}
finally {
  for (const name of containers.reverse()) spawnSync('docker', ['rm', '-f', name], { stdio: 'ignore' });
  for (const volume of volumes) spawnSync('docker', ['volume', 'rm', volume], { stdio: 'ignore' });
  spawnSync('docker', ['network', 'rm', network], { stdio: 'ignore' });
  await rm(root, { recursive: true, force: true });
}
