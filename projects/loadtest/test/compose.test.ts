import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';
import path from 'node:path';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';

import YAML from 'yaml';

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../..');
const compose = YAML.parse(readFileSync(path.join(root, 'projects/loadtest/compose.yaml'), 'utf8')) as {
  services: Record<string, {
    profiles?: string[];
    image?: string;
    cpus?: string;
    mem_limit?: string;
    environment?: Record<string, string>;
    command?: string[];
    ports?: string[];
    secrets?: Array<string | { source: string; target?: string }>;
    tmpfs?: string[];
    volumes?: Array<string | { type?: string; source?: string; target?: string; read_only?: boolean; bind?: { create_host_path?: boolean } }>;
    depends_on?: Record<string, unknown>;
    entrypoint?: string[];
    networks?: { default?: { ipv4_address?: string } };
    network_mode?: string;
    extra_hosts?: unknown;
    pull_policy?: string;
    read_only?: boolean;
    cap_drop?: string[];
    user?: string;
    init?: boolean;
  }>;
  volumes: Record<string, { labels?: Record<string, string> } | null>;
};

test('real publisher services use an init and source layout overrides reach both collectors', () => {
  for (const service of ['direct-1', 'dataflow-1', 'tenant-stream', 'delta-maintenance']) {
    assert.equal(compose.services[service].init, true, `${service} must reap Azure CLI descendants`);
  }
  for (const service of ['direct-1', 'dataflow-1']) {
    assert.equal(compose.services[service].environment?.ARROWCOL_LAYOUT_ROW_GROUP_ROWS, '${ARROWCOL_LAYOUT_ROW_GROUP_ROWS:-16384}');
    assert.equal(compose.services[service].environment?.ARROWCOL_LAYOUT_PAGE_ROWS, '${ARROWCOL_LAYOUT_PAGE_ROWS:-2048}');
  }
});

test('real maintenance and tenant images are optional, build-covered, and use isolated writable roots', () => {
  const project = JSON.parse(readFileSync(path.join(root, 'projects/loadtest/project.json'), 'utf8'));
  const images = project.targets.build.dependsOn.find((dependency: unknown) =>
    typeof dependency === 'object' && dependency !== null && 'target' in dependency && dependency.target === 'image').projects;
  for (const [service, profile, image] of [
    ['tenant-stream', 'tenant', 'arrow-tenant-stream'],
    ['delta-maintenance', 'maintenance', 'delta-maintenance'],
  ]) {
    assert.deepEqual(compose.services[service].profiles, [profile]);
    assert.equal(compose.services[service].image, `ghcr.io/mdrakiburrahman/rust-playground/${image}:dev`);
    assert(images.includes(image));
    assert(project.implicitDependencies.includes(image));
  }
  const tenant = compose.services['tenant-stream'];
  assert(tenant.volumes?.some(volume => typeof volume === 'object' && volume.target === '/delta/direct' && volume.read_only));
  assert(tenant.volumes?.some(volume => typeof volume === 'object' && volume.target === '/checkpoints' && !volume.read_only));
  const maintenance = compose.services['delta-maintenance'];
  assert(maintenance.volumes?.some(volume => typeof volume === 'object' && volume.target === '/tmp/arrowcol-stress/direct'));
  assert(!maintenance.command?.includes('--unsafe-short-vacuum-for-tests'));
});

test('stack includes SQL, Hive, router, one collector per pool, Kusto, monitoring and validator', () => {
  const names = Object.keys(compose.services);
  for (const service of ['sqlserver', 'hive-metastore', 'router', 'direct-1', 'dataflow-1', 'kusto', 'lgtm', 'cadvisor', 'portainer', 'validator']) {
    assert(names.includes(service), `missing ${service}`);
    assert.deepEqual(compose.services[service].profiles, ['benchmark']);
  }
  for (const gone of ['direct-2', 'dataflow-2', 'otelcol-rust', 'hello-world', 'load-driver', 'prometheus']) {
    assert(!names.includes(gone), `unexpected ${gone}`);
  }
});

test('Kusto is pinned, memory-bounded, loopback-only and keeps its documented default startup', () => {
  const kusto = compose.services.kusto;
  assert.match(kusto.image ?? '', /^mcr\.microsoft\.com\/azuredataexplorer\/kustainer-linux@sha256:[0-9a-f]{64}$/u);
  assert.equal(kusto.mem_limit, '${KUSTO_MEMORY_LIMIT:-4g}');
  assert.deepEqual(kusto.environment, { ACCEPT_EULA: 'Y' });
  assert.deepEqual(kusto.ports, ['127.0.0.1:${KUSTO_PORT:-8080}:8080']);
  assert.equal(kusto.entrypoint, undefined);
  assert.equal(kusto.command, undefined);
  assert.equal(kusto.networks?.default?.ipv4_address, '${LOADTEST_KUSTO_IP:-10.89.0.50}');
  const devcontainer = JSON.parse(readFileSync(path.join(root, '.devcontainer/devcontainer.json'), 'utf8'));
  assert.equal(devcontainer.portsAttributes['8080'].onAutoForward, 'ignore');
});

test('Kusto sees only read-only Delta roots plus owned persistent native storage', () => {
  const kusto = compose.services.kusto;
  assert.deepEqual(kusto.volumes, [
    'kusto-data:/kustodata',
    ...['direct', 'dataflow'].map(pool => ({
      type: 'bind', source: `\${ONELAKE_HOST_PATH:?Run through Nx}/${pool}`,
      target: `/delta/${pool}`, read_only: true, bind: { create_host_path: false },
    })),
  ]);
  assert.deepEqual(compose.volumes['kusto-data']?.labels, {
    'io.arrowcol.loadtest.workspace': '${LOADTEST_WORKSPACE:?Run through Nx}',
  });
  assert.equal(kusto.secrets, undefined);
});

test('Kusto cannot become a required collector dependency and the validator uses its internal address', () => {
  for (const [name, service] of Object.entries(compose.services)) {
    assert(!Object.hasOwn(service.depends_on ?? {}, 'kusto'), `${name} must not require Kusto startup`);
  }
  assert.equal(compose.services.kusto.depends_on, undefined);
  assert.equal(compose.services.validator.environment?.LOADTEST_KUSTO_ENDPOINT,
    'http://${LOADTEST_KUSTO_IP:-10.89.0.50}:8080');
});

test('direct and dataflow pools use separate Delta roots', () => {
  const root = (service: string): string | undefined =>
    compose.services[service].volumes?.flatMap((volume) =>
      typeof volume !== 'string' && volume.target?.startsWith('/delta/') ? [volume.source] : [])[0];
  assert(root('direct-1'));
  assert.notEqual(root('direct-1'), root('dataflow-1'));
});

test('SQL Server publishes an overridable loopback port and retains its data and secret mounts', () => {
  const sqlserver = compose.services.sqlserver;
  assert.deepEqual(sqlserver.ports, ['127.0.0.1:${SQLSERVER_PORT:-14333}:1433']);
  assert(sqlserver.volumes?.includes('sqlserver-data:/var/opt/mssql'));
  assert(sqlserver.secrets?.includes('sqlserver_sa_password'));
  const devcontainer = JSON.parse(readFileSync(path.join(root, '.devcontainer/devcontainer.json'), 'utf8'));
  assert.equal(devcontainer.portsAttributes['14333'].onAutoForward, 'ignore');
});

test('SQL Server has an overridable 16 GiB memory budget for continuous benchmarks', () => {
  assert.equal(compose.services.sqlserver.environment?.MSSQL_MEMORY_LIMIT_MB,
    '${SQLSERVER_MEMORY_LIMIT_MB:-16384}');
});

test('both collectors have equal CPU budgets and explicit matching runtime thread/core counts', () => {
  assert.equal(compose.services['direct-1'].cpus, '${LOADTEST_CPUS:-2}');
  assert.equal(compose.services['dataflow-1'].cpus, compose.services['direct-1'].cpus);
  assert.equal(compose.services['direct-1'].environment?.TOKIO_WORKER_THREADS, '${LOADTEST_CPUS:-2}');
  assert.deepEqual(compose.services['dataflow-1'].command?.slice(0, 2), ['--num-cores', '${LOADTEST_CPUS:-2}']);
});

test('portainer is pinned, loopback-only, seeded from the SA secret and stateless', () => {
  const portainer = compose.services.portainer;
  assert.match(portainer.image ?? '', /^portainer\/portainer-ce:[0-9.]+@sha256:[0-9a-f]{64}$/u);
  assert(portainer.ports?.length, 'portainer must publish its UI');
  for (const port of portainer.ports) {
    assert(port.startsWith('127.0.0.1:') && port.endsWith(':9443'), `portainer port ${port} must be loopback-only`);
  }
  assert(portainer.command?.includes('--admin-password-file=/run/secrets/sqlserver_sa_password'));
  assert(portainer.secrets?.includes('sqlserver_sa_password'));
  assert(portainer.volumes?.some((volume) =>
    typeof volume !== 'string' && volume.source === '/var/run/docker.sock' && volume.target === '/var/run/docker.sock'));
  assert(!portainer.volumes?.some((volume) =>
    typeof volume === 'string' ? volume.split(':')[1] === '/data' : volume.target === '/data'),
  'admin must be re-seeded on every start');
  assert(portainer.tmpfs?.some((mount) => mount.startsWith('/data:')));
  assert(portainer.networks?.default?.ipv4_address, 'a dynamic address could take a static service IP');
});

test('kusto-explorer reaches Kusto only through the Compose network and publishes a loopback UI port', () => {
  const explorer = compose.services['kusto-explorer'];
  assert.deepEqual(explorer.profiles, ['benchmark']);
  assert.equal(explorer.image, 'ghcr.io/mdrakiburrahman/rust-playground/kusto-explorer:dev');
  assert.equal(explorer.pull_policy, 'never');
  assert.deepEqual(explorer.environment, {
    KUSTO_EXPLORER_CONNECTIONS: 'Loadtest emulator=http://kusto:8080',
    KUSTO_EXPLORER_DEFAULT_DATABASE: 'loadtest',
  });
  for (const value of Object.values(explorer.environment ?? {})) {
    assert.doesNotMatch(value, /localhost|127\.0\.0\.1|host\.docker\.internal|KUSTO_PORT|LOADTEST_KUSTO_IP/u);
  }
  assert.equal(explorer.network_mode, undefined);
  assert.equal(explorer.extra_hosts, undefined);
  assert.equal(explorer.depends_on, undefined, 'kusto-explorer must tolerate a cold Kusto instead of blocking on it');
  assert.equal(explorer.networks?.default?.ipv4_address, '${LOADTEST_KUSTO_EXPLORER_IP:-10.89.0.51}');
  assert.deepEqual(explorer.ports, ['127.0.0.1:${KUSTO_EXPLORER_PORT:-8090}:8080']);
  assert.equal(explorer.read_only, true);
  assert.deepEqual(explorer.cap_drop, ['ALL']);
  const devcontainer = JSON.parse(readFileSync(path.join(root, '.devcontainer/devcontainer.json'), 'utf8'));
  assert.equal(devcontainer.portsAttributes['8090'].onAutoForward, 'ignore');
});

test('the UI gate browser is pinned to the installed Playwright release and stays off the host network', () => {
  const browser = compose.services.playwright;
  assert.deepEqual(browser.profiles, ['ui-gate']);
  const image = /^mcr\.microsoft\.com\/playwright:v([0-9.]+)-noble@sha256:[0-9a-f]{64}$/u.exec(browser.image ?? '');
  assert(image, 'the Playwright image must be version-tagged and digest-pinned');
  const lock = JSON.parse(readFileSync(path.join(root, 'package-lock.json'), 'utf8')) as {
    packages: Record<string, { version?: string }>;
  };
  assert.equal(image[1], lock.packages['node_modules/playwright-core']?.version,
    'bump the Playwright image together with the playwright npm package');
  assert.equal(browser.user, 'pwuser');
  assert.equal(browser.ports, undefined);
  assert.equal(browser.network_mode, undefined);
  assert.deepEqual(browser.command?.slice(0, 3), ['node', '/opt/playwright-core/cli.js', 'run-server']);
  assert.equal(browser.networks?.default?.ipv4_address, '${LOADTEST_PLAYWRIGHT_IP:-10.89.0.52}');
});
