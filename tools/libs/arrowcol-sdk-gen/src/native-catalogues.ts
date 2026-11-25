import { randomUUID } from 'node:crypto';
import { existsSync, readFileSync, writeFileSync, openSync, closeSync, unlinkSync } from 'node:fs';
import { join, relative, resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { commandRunner, contractRoot, digest, ensureDirectory, listRuns, loadConfig, writeJson, uuidPattern, type Execute, type Registry } from './contract.js';
import { dockerExec, localDockerSocket } from './docker-exec.js';

export type NativeProvider = 'unity' | 'hive';
export const nativeImages = {
  unity: 'unitycatalog/unitycatalog:v0.7.0@sha256:cdacdc5e7624795e7b2a18ece0d7357c5a1ec640db17fccb4d0fb208dd0d335b',
  hive: 'apache/hive:4.1.0@sha256:b957bb0e698e560d24e162b7549d79f191d58703e92899a886618c317f6e8f30',
  postgres: 'postgres:16-alpine@sha256:1d5b02d3a9941fbd52437db4107f8c52d6297dac553ca1e5c299ebb3756b4857',
};
const label = 'io.arrowcol.native.owner';
const jdbcSha256 = '2a32a9dcbc42d67a50ad3a0de5efd102c8d2be46720045f2cbd6689f160ab7c7';
interface NativeState { id: string }
interface Container {
  Id: string; Config: { Image: string; Hostname?: string; Labels: Record<string, string> };
  State: { Running: boolean; Status: string };
  Mounts?: { Type: string; Source: string; Destination: string }[];
  NetworkSettings?: { Networks: Record<string, unknown> };
}

export function nativeNames(id: string) {
  if (!uuidPattern.test(id)) throw new Error('Invalid native catalogue ownership identity');
  const prefix = `arrowcol-native-${digest(id).slice(0, 12)}`;
  return { unity: `${prefix}-unity`, hive: `${prefix}-hive`, postgres: `${prefix}-postgres`, network: `${prefix}-net` };
}
export function nativeState(workspace: string): NativeState | undefined {
  const path = resolve(workspace, contractRoot, 'native', 'state.json');
  if (!existsSync(path)) return undefined;
  const value: NativeState = JSON.parse(readFileSync(path, 'utf8'));
  nativeNames(value.id);
  return value;
}
export function nativeManager(workspace: string, execute: Execute = commandRunner(workspace)) {
  const root = resolve(workspace, contractRoot, 'native');
  let localEndpoint: Promise<string> | undefined;
  function endpoint(): Promise<string> {
    return localEndpoint ??= (async () => {
      const value = (!process.env.DOCKER_CONTEXT && process.env.DOCKER_HOST) ||
        (await execute('docker', ['context', 'inspect', '--format', '{{.Endpoints.docker.Host}}'])).trim();
      localDockerSocket(value);
      return value;
    })();
  }
  const docker = async (args: string[]) => execute('docker', ['--host', await endpoint(), ...args],
    undefined, { DOCKER_CONTEXT: undefined, DOCKER_HOST: undefined });
  async function execContainer(name: string, command: string[]) {
    return dockerExec(await endpoint(), name, command);
  }
  function buildRegistry(provider: NativeProvider): Registry | undefined {
    if (!existsSync(resolve(workspace, contractRoot, 'config.json'))) return undefined;
    return loadConfig(workspace).registries.find((registry) => registry.native?.provider === provider);
  }
  async function sharedWarehouse(state: NativeState, registry: Registry) {
    const hostname = process.env.HOSTNAME;
    if (process.platform !== 'linux' || !hostname) throw new Error('Enable native builds from the Linux development container');
    const [self]: Container[] = JSON.parse(await docker(['container', 'inspect', hostname]));
    const mount = self.Mounts?.find((item) => item.Type === 'bind' && item.Destination === resolve(workspace));
    if (!self.State.Running || self.Config.Hostname !== hostname || !mount) {
      throw new Error('Cannot prove the current development container and its workspace bind mount');
    }
    const target = resolve(workspace, contractRoot, 'registries', digest(registry.authority), 'warehouse');
    ensureDirectory(workspace, relative(workspace, target));
    const source = `${mount.Source}/${relative(workspace, target)}`;
    const network = nativeNames(state.id).network;
    if (!self.NetworkSettings?.Networks[network]) await docker(['network', 'connect', network, self.Id]);
    return { source, target };
  }
  async function warehouseArguments(state: NativeState, provider: NativeProvider): Promise<string[]> {
    const registry = buildRegistry(provider);
    if (!registry) return [];
    if (registry.authority !== `${state.id}:${provider}`) throw new Error('Native authority does not match the owned service');
    const { source, target } = await sharedWarehouse(state, registry);
    const found = await container(state, provider);
    if (found && !found.Mounts?.some((mount) => mount.Type === 'bind' && mount.Source === source && mount.Destination === target)) {
      throw new Error(`The owned ${provider} container needs its shared warehouse mount; run --action=enable-build`);
    }
    return ['--mount', `type=bind,source=${source},target=${target}`];
  }
  async function container(state: NativeState, service: keyof typeof nativeImages): Promise<Container | undefined> {
    const name = nativeNames(state.id)[service];
    if (!(await docker(['container', 'ls', '-aq', '--filter', `name=^/${name}$`])).trim()) return undefined;
    const [found]: Container[] = JSON.parse(await docker(['container', 'inspect', name]));
    if (found.Config.Labels?.[label] !== state.id || found.Config.Image !== nativeImages[service]) {
      throw new Error(`Refusing foreign or changed container: ${name}`);
    }
    return found;
  }
  async function resource(state: NativeState, kind: 'network' | 'volume', name: string) {
    const ids = await docker([kind, 'ls', '-q', '--filter', `name=^${name}$`]);
    if (ids.trim()) {
      const [found]: { Labels?: Record<string, string>; Internal?: boolean }[] =
        JSON.parse(await docker([kind, 'inspect', name]));
      if (found.Labels?.[label] !== state.id || (kind === 'network' && found.Internal)) {
        throw new Error(`Refusing foreign or incompatible ${kind}: ${name}`);
      }
    } else await docker([kind, 'create', '--label', `${label}=${state.id}`, name]);
  }
  async function create(state: NativeState, service: keyof typeof nativeImages, args: string[], command: string[] = []) {
    const found = await container(state, service);
    if (found) return false;
    const names = nativeNames(state.id);
    await docker(['create', '--name', names[service], '--label', `${label}=${state.id}`,
      '--network', names.network, '--init', ...args, nativeImages[service], ...command]);
    return true;
  }
  async function start(state: NativeState, service: keyof typeof nativeImages) {
    const found = await container(state, service);
    if (!found) throw new Error(`Missing ${service} container`);
    if (!found.State.Running) await docker(['start', found.Id]);
  }
  async function ready(state: NativeState, service: keyof typeof nativeImages, probe: () => Promise<unknown>) {
    let failure: unknown;
    for (let attempt = 0; attempt < 60; attempt++) {
      const found = await container(state, service);
      if (!found?.State.Running) throw new Error(`${service} stopped during startup. Inspect logs for ${nativeNames(state.id)[service]}.`);
      try { await probe(); return; } catch (error) { failure = error; }
      await delay(2000);
    }
    throw new Error(`${service} readiness failed: ${String(failure)}`);
  }
  async function unityRequest(state: NativeState, path: string, body?: unknown): Promise<unknown> {
    const args = ['wget', '-qO-', '-T', '15'];
    if (body !== undefined) args.push('--header=Content-Type: application/json', `--post-data=${JSON.stringify(body)}`);
    args.push(`http://127.0.0.1:8080/api/2.1/unity-catalog/${path}`);
    return JSON.parse(await execContainer(nativeNames(state.id).unity, args));
  }
  async function hiveQuery(state: NativeState, statement: string): Promise<string> {
    return execContainer(nativeNames(state.id).hive, ['java', '-Xmx512m',
      '--add-opens=java.base/java.net=ALL-UNNAMED', '-Dlog4j2.statusLoggerLevel=ERROR', '-Dhive.log.level=ERROR',
      '-cp', ['/opt/hive/conf', '/opt/hive/lib/*', '/opt/hive/hcatalog/share/hcatalog/*',
        '/opt/hadoop/share/hadoop/common/*', '/opt/hadoop/share/hadoop/common/lib/*',
        '/opt/hadoop/share/hadoop/mapreduce/*', '/opt/hadoop/share/hadoop/hdfs/*', '/opt/hadoop/share/hadoop/hdfs/lib/*'].join(':'),
      'org.apache.hive.hcatalog.cli.HCatCli', '-e', statement]);
  }
  async function up(provider: NativeProvider) {
    await endpoint();
    ensureDirectory(workspace, join(contractRoot, 'native'));
    const lock = join(root, 'lifecycle.lock');
    closeSync(openSync(lock, 'wx'));
    try {
      let state = nativeState(workspace);
      if (!state) {
        state = { id: randomUUID() };
        writeFileSync(join(root, 'state.json'), `${JSON.stringify(state, null, 2)}\n`, { flag: 'wx' });
      }
      const names = nativeNames(state.id);
      await resource(state, 'network', names.network);
      const shared = await warehouseArguments(state, provider);
      if (provider === 'unity') {
        const volume = `${names.unity}-metadata`;
        await resource(state, 'volume', volume);
        await create(state, 'unity', [
          '--publish', '127.0.0.1:18080:8080', '--env', 'JAVA_TOOL_OPTIONS=-Xmx768m',
          '--mount', `type=volume,source=${volume},target=/home/unitycatalog/etc/db`,
          ...shared,
          '--entrypoint', '/bin/sh',
        ], ['-c', 'exec java -cp "$(cat server/target/classpath)" io.unitycatalog.server.UnityCatalogServer']);
        const configuration = join(root, 'hibernate.properties');
        writeFileSync(configuration, [
          'hibernate.connection.driver_class=org.h2.Driver',
          'hibernate.connection.url=jdbc:h2:file:./etc/db/h2db;DB_CLOSE_DELAY=-1;WRITE_DELAY=0',
          'hibernate.hikari.maximumPoolSize=20', 'hibernate.hikari.minimumIdle=2',
          'hibernate.hbm2ddl.auto=update', 'hibernate.show_sql=false', 'hibernate.archive.autodetection=class', '',
        ].join('\n'));
        if (!(await container(state, 'unity'))?.State.Running) {
          await docker(['cp', configuration, `${names.unity}:/home/unitycatalog/etc/conf/hibernate.properties`]);
        }
        await start(state, 'unity');
        await ready(state, 'unity', () => unityRequest(state, 'catalogs'));
      } else {
        const database = `${names.hive}-metadata`;
        const warehouse = `${names.hive}-warehouse`;
        await resource(state, 'volume', database);
        await resource(state, 'volume', warehouse);
        await create(state, 'postgres', [
          '--env', 'POSTGRES_DB=metastore', '--env', 'POSTGRES_USER=arrowcol',
          '--env', 'POSTGRES_HOST_AUTH_METHOD=trust',
          '--mount', `type=volume,source=${database},target=/var/lib/postgresql/data`,
        ]);
        await start(state, 'postgres');
        await ready(state, 'postgres', () => execContainer(names.postgres, ['pg_isready', '-U', 'arrowcol', '-d', 'metastore']));
        const jdbc = join(root, 'postgresql-42.7.8.jar');
        if (!existsSync(jdbc)) {
          const response = await fetch('https://repo.maven.apache.org/maven2/org/postgresql/postgresql/42.7.8/postgresql-42.7.8.jar',
            { signal: AbortSignal.timeout(60_000) });
          if (!response.ok) throw new Error(`JDBC download failed: HTTP ${response.status}`);
          const bytes = Buffer.from(await response.arrayBuffer());
          if (digest(bytes) !== jdbcSha256) throw new Error('JDBC download checksum mismatch');
          writeFileSync(jdbc, bytes, { flag: 'wx' });
        }
        if (digest(readFileSync(jdbc)) !== jdbcSha256) throw new Error('Cached JDBC checksum mismatch');
        const configuration = join(root, 'hive-site.xml');
        const properties = {
          'javax.jdo.option.ConnectionDriverName': 'org.postgresql.Driver',
          'javax.jdo.option.ConnectionURL': `jdbc:postgresql://${names.postgres}:5432/metastore`,
          'javax.jdo.option.ConnectionUserName': 'arrowcol',
          'javax.jdo.option.ConnectionPassword': '',
          'hive.metastore.uris': 'thrift://localhost:9083',
          'hive.metastore.warehouse.dir': 'file:/opt/hive/data/warehouse',
          'hive.metastore.warehouse.external.dir': 'file:/opt/hive/data/warehouse/external',
        };
        writeFileSync(configuration, `<configuration>${Object.entries(properties)
          .map(([name, value]) => `<property><name>${name}</name><value>${value}</value></property>`).join('')}</configuration>\n`);
        await create(state, 'hive', [
          '--publish', '127.0.0.1:19083:9083', '--env', 'SERVICE_NAME=metastore', '--env', 'DB_DRIVER=postgres',
          '--mount', `type=volume,source=${warehouse},target=/opt/hive/data/warehouse`,
          ...shared,
        ]);
        const found = await container(state, 'hive');
        if (!found?.State.Running) {
          await docker(['cp', jdbc, `${names.hive}:/opt/hive/lib/postgres.jar`]);
          await docker(['cp', configuration, `${names.hive}:/opt/hive/conf/hive-site.xml`]);
        }
        await start(state, 'hive');
        await ready(state, 'hive', async () => {
          const output = await hiveQuery(state, 'SHOW DATABASES;');
          if (!/^default\r?$/m.test(output)) throw new Error('Hive readiness did not return its default database');
        });
      }
      return status(provider);
    } finally { unlinkSync(lock); }
  }
  async function status(provider: NativeProvider) {
    const state = nativeState(workspace);
    if (!state) return { provider, installed: false, buildSupported: false };
    const found = await container(state, provider);
    return { provider, installed: !!found, running: found?.State.Running ?? false,
      container: nativeNames(state.id)[provider], image: nativeImages[provider],
      endpoint: provider === 'unity' ? 'http://localhost:18080/api/2.1/unity-catalog' : 'thrift://localhost:19083',
      buildSupported: !!buildRegistry(provider),
      note: buildRegistry(provider) ? 'Native registration-last authority; restart catalog-ui after enabling its CES bridge.'
        : 'Run --action=enable-build from the devcontainer to enable native registration and SDK builds.' };
  }
  async function enableBuild(provider: NativeProvider) {
    if (listRuns(workspace).some((run) => (run.registry.native?.provider === provider || run.registry.id === provider) &&
      ['queued', 'running'].includes(run.status))) throw new Error('Wait for native builds to finish before changing the service');
    let state = nativeState(workspace);
    if (!state || !await container(state, provider)) {
      await up(provider);
      state = nativeState(workspace);
    }
    if (!state) throw new Error('Native service did not persist its ownership identity');
    const lock = join(root, 'lifecycle.lock');
    closeSync(openSync(lock, 'wx'));
    try {
      const config = loadConfig(workspace);
      const names = nativeNames(state.id);
      const registry: Registry = {
        id: provider, label: provider === 'unity' ? 'Unity Catalog' : 'Hive Metastore',
        authority: `${state.id}:${provider}`, endpoint: `http://127.0.0.1:${provider === 'unity' ? 18819 : 18820}`, mode: 'owned',
        native: { provider, endpoint: provider === 'unity'
          ? `http://${names.unity}:8080/api/2.1/unity-catalog` : `thrift://${names.hive}:9083` },
      };
      const existing = config.registries.find((item) => item.id === provider || item.native?.provider === provider);
      if (existing && JSON.stringify(existing) !== JSON.stringify(registry)) {
        throw new Error('Refusing to overwrite an existing different native registry configuration');
      }
      if (!existing && config.registries.some((item) => item.endpoint === registry.endpoint || item.authority === registry.authority)) {
        throw new Error('Native bridge endpoint or authority is already configured for another registry');
      }
      const { source, target } = await sharedWarehouse(state, registry);
      const found = await container(state, provider);
      if (found && !found.Mounts?.some((mount) => mount.Type === 'bind' && mount.Source === source && mount.Destination === target)) {
        // Replace only the proved owned container, never its persistent volumes.
        if (found.State.Running) await docker(['stop', '--time', '45', found.Id]);
        await docker(['container', 'rm', found.Id]);
      }
      if (!existing) {
        config.registries.push(registry);
        writeJson(resolve(workspace, contractRoot, 'config.json'), config);
      }
    } finally { unlinkSync(lock); }
    return up(provider);
  }
  async function inspect(provider: NativeProvider, catalog?: string, schema?: string, table?: string): Promise<unknown> {
    for (const name of [catalog, schema, table]) {
      if (name !== undefined && !/^[a-zA-Z_][a-zA-Z0-9_]{0,127}$/.test(name)) throw new Error('Expected a simple native namespace identifier');
    }
    if (table && !schema || schema && !catalog) throw new Error('Provide parent namespaces for native inspection');
    const state = nativeState(workspace);
    if (!state || !(await container(state, provider))?.State.Running) throw new Error(`Start the ${provider} native service first`);
    if (provider === 'unity') {
      const path = table ? `tables/${catalog}.${schema}.${table}` : schema
        ? `tables?catalog_name=${catalog}&schema_name=${schema}` : catalog ? `schemas?catalog_name=${catalog}` : 'catalogs';
      return unityRequest(state, path);
    }
    if (catalog && catalog !== 'hive') throw new Error('The local Hive inspector supports the default hive catalog');
    const registry = buildRegistry('hive');
    if (table && registry) {
      const config = resolve(workspace, contractRoot, 'registries', digest(registry.authority), 'native-config.json');
      if (!existsSync(config)) writeJson(config, registry.native);
      else if (JSON.stringify(JSON.parse(readFileSync(config, 'utf8'))) !== JSON.stringify(registry.native)) {
        throw new Error('Native inspector configuration differs from the selected authority');
      }
      return JSON.parse(await execute(resolve(workspace, 'target/debug/arrowcol-catalog-rust'), [
        '--native-config', config, '--inspect-native-table', `${catalog}.${schema}.${table}`,
      ]));
    }
    return { text: await hiveQuery(state, table ? `DESCRIBE FORMATTED \`${schema}\`.\`${table}\`;`
      : schema ? `SHOW TABLES IN \`${schema}\`;` : 'SHOW DATABASES;') };
  }
  async function stop(provider: NativeProvider) {
    const state = nativeState(workspace);
    if (!state) throw new Error('Native catalogues have not been initialized');
    for (const service of provider === 'hive' ? ['hive', 'postgres'] as const : ['unity'] as const) {
      const found = await container(state, service);
      if (found?.State.Running) await docker(['stop', '--time', '30', found.Id]);
    }
    return status(provider);
  }
  async function verify(provider: NativeProvider) {
    await up(provider);
    const state = nativeState(workspace)!;
    const namespace = `arrowcol_check_${Date.now()}`;
    if (provider === 'unity') {
      await unityRequest(state, 'catalogs', { name: namespace, comment: 'Local Arrowcol integration rehearsal' });
      await unityRequest(state, 'schemas', { name: 'contracts', catalog_name: namespace });
      await unityRequest(state, 'tables', {
        name: 'example_events', catalog_name: namespace, schema_name: 'contracts',
        table_type: 'EXTERNAL', data_source_format: 'PARQUET',
        comment: 'Native metadata example only; no data files or Arrowcol SDK release.',
        storage_location: `file:///home/unitycatalog/etc/db/demo-tables/${namespace}/example_events`,
        columns: [
          { name: 'id', type_name: 'LONG', type_text: 'bigint', position: 0, nullable: true,
            type_json: JSON.stringify({ name: 'id', type: 'long', nullable: true, metadata: {} }) },
          { name: 'payload', type_name: 'STRING', type_text: 'string', position: 1, nullable: true,
            type_json: JSON.stringify({ name: 'payload', type: 'string', nullable: true, metadata: {} }) },
        ],
      });
    } else {
      await hiveQuery(state, `CREATE DATABASE \`${namespace}\`;`);
      await hiveQuery(state, `CREATE EXTERNAL TABLE \`${namespace}\`.example_events (id BIGINT, payload STRING)
        COMMENT 'Native metadata example only without data files or Arrowcol SDK release.'
        STORED AS PARQUET LOCATION 'file:/opt/hive/data/warehouse/external/${namespace}/example_events';`);
    }
    const before = await inspect(provider, provider === 'unity' ? namespace : undefined);
    if (!JSON.stringify(before).includes(provider === 'unity' ? 'contracts' : namespace)) {
      throw new Error('Native CREATE was not visible before restart');
    }
    const tableBefore = await inspect(provider, provider === 'unity' ? namespace : 'hive',
      provider === 'unity' ? 'contracts' : namespace, 'example_events');
    await stop(provider);
    await up(provider);
    const metadata = await inspect(provider, provider === 'unity' ? namespace : undefined);
    if (!JSON.stringify(metadata).includes(provider === 'unity' ? 'contracts' : namespace)) {
      throw new Error('Native metadata did not survive service restart');
    }
    if (provider === 'unity' && JSON.stringify(before) !== JSON.stringify(metadata)) {
      throw new Error('Unity namespace identity changed across restart');
    }
    const table = await inspect(provider, provider === 'unity' ? namespace : 'hive',
      provider === 'unity' ? 'contracts' : namespace, 'example_events');
    if (provider === 'unity' && JSON.stringify(tableBefore) !== JSON.stringify(table)) {
      throw new Error('Unity table identity or metadata changed across restart');
    }
    if (provider === 'hive') {
      if (!table || typeof table !== 'object') throw new Error('Hive returned no table metadata');
      if ('text' in table) {
        if (typeof table.text !== 'string' || !/^id\s+bigint/m.test(table.text) || !/^payload\s+string/m.test(table.text)) {
          throw new Error('Hive table schema was not retained across restart');
        }
      } else if (!('columns' in table) || !Array.isArray(table.columns) ||
          !table.columns.some((column: unknown) => column && typeof column === 'object' &&
            'name' in column && column.name === 'id' && 'type' in column && column.type === 'bigint') ||
          !table.columns.some((column: unknown) => column && typeof column === 'object' &&
            'name' in column && column.name === 'payload' && 'type' in column && column.type === 'string') ||
          JSON.stringify(tableBefore) !== JSON.stringify(table)) {
        throw new Error('Hive structured identity/schema was not retained across restart');
      }
    }
    const result = { provider, namespace, persistence: 'verified', metadata, table, buildSupported: !!buildRegistry(provider) };
    writeFileSync(join(root, `${provider}-${namespace}.json`), `${JSON.stringify(result, null, 2)}\n`, { flag: 'wx' });
    return result;
  }
  return { up, stop, status, inspect, verify, enableBuild };
}
