import assert from 'node:assert/strict';
import { existsSync, readFileSync } from 'node:fs';
import { join, relative, resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { errorMessage, writeJson, type Execute } from './contract.js';

const image = 'mcr.microsoft.com/azuredataexplorer/kustainer-linux@sha256:51e2250da2faf292de903f871a073eb37d3f2641cf63381b183879d21ebb1013';
const label = 'arrowcol.stream-demo';
type Container = {
  Id: string; Config: { Hostname: string; Labels?: Record<string, string> }; State: { Running: boolean };
  Mounts: { Type: string; Source: string; Destination: string; RW: boolean }[];
  NetworkSettings: { Networks: Record<string, unknown> };
};
async function request(endpoint: string, csl: string, management = false): Promise<unknown> {
  const response = await fetch(`${endpoint}/${management ? 'v1/rest/mgmt' : 'v2/rest/query'}`, {
    method: 'POST', headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify({ db: 'NetDefaultDB', csl }), signal: AbortSignal.timeout(120_000),
  });
  const text = await response.text();
  assert.ok(response.ok, `Kusto HTTP ${response.status}: ${text.slice(0, 4000)}`);
  const value = JSON.parse(text);
  assert.ok(!value.error && !(Array.isArray(value) && value.some((frame) => frame.HasErrors)), `Kusto query failed: ${text.slice(0, 4000)}`);
  return value;
}

export async function kustoDemo(
  action: string, workspace: string, root: string, id: string, warehouse: string,
  table: string, port: number, execute: Execute,
): Promise<void> {
  assert.ok(Number.isInteger(port) && port > 1024 && port <= 65535, 'Kusto port must be 1025..65535');
  const dockerHost = process.env.DOCKER_HOST ||
    (await execute('docker', ['context', 'inspect', '--format', '{{.Endpoints.docker.Host}}'])).trim();
  assert.ok(dockerHost.startsWith('unix://') || dockerHost.startsWith('npipe://'), 'The demo requires a local Docker daemon');
  const name = `arrowcol-stream-${id.slice(0, 12)}-kusto`;
  const record = join(root, 'kusto.json');
  const matches = (await execute('docker', ['ps', '-a', '--filter', `name=^/${name}$`, '--format', '{{.ID}}'])).trim();
  let container: Container | undefined;
  if (matches) {
    [container] = JSON.parse(await execute('docker', ['inspect', matches]));
    assert.equal(container?.Config.Labels?.[label], id, 'Refusing to use a foreign Kusto container');
  }
  if (action === 'kusto-stop') {
    assert.ok(container, 'No owned Kusto container to stop');
    await execute('docker', ['stop', '--time', '30', container.Id]);
    console.log('Stopped the owned Kusto emulator; its container, database volume and Delta data are retained.');
    return;
  }
  if (action === 'kusto-query') {
    assert.ok(container?.State.Running && existsSync(record), 'Run kusto-up first');
    const saved = JSON.parse(readFileSync(record, 'utf8'));
    assert.equal(saved.containerId, container.Id);
    const result = await request(saved.endpoint, 'external_table("ArrowcolEvents") | project event_id, observed_at, payload | order by event_id asc');
    writeJson(join(root, 'kusto-rows.json'), result);
    console.log(JSON.stringify(result, null, 2));
    return;
  }
  let source = warehouse, endpoint = `http://127.0.0.1:${port}`;
  const networkArgs: string[] = [];
  if (existsSync('/.dockerenv')) {
    assert.ok(process.env.HOSTNAME, 'Missing development container hostname');
    const [self]: Container[] = JSON.parse(await execute('docker', ['inspect', process.env.HOSTNAME]));
    assert.equal(self.Config.Hostname, process.env.HOSTNAME);
    const mount = self.Mounts.find((item) => item.Type === 'bind' && item.Destination === resolve(workspace));
    assert.ok(mount && self.State.Running, 'Cannot prove the development workspace bind mount');
    let networks = Object.keys(self.NetworkSettings.Networks);
    if (networks.length > 1) {
      const project = self.Config.Labels?.['com.docker.compose.project'];
      assert.ok(project, 'Multiple networks without an identifiable development Compose project');
      const inspected: { Name: string; Labels?: Record<string, string> }[] =
        JSON.parse(await execute('docker', ['network', 'inspect', ...networks]));
      networks = inspected.filter((network) => network.Labels?.['com.docker.compose.project'] === project &&
        network.Labels?.['com.docker.compose.network'] === 'default').map((network) => network.Name);
    }
    assert.equal(networks.length, 1, 'Cannot identify one development Docker network');
    source = `${mount.Source}/${relative(workspace, warehouse).replaceAll('\\', '/')}`;
    networkArgs.push('--network', networks[0]);
    endpoint = `http://${name}:8080`;
  }
  assert.ok(!source.includes(',') && !source.includes('\n'), 'Unsupported Docker mount path');
  if (container) {
    assert.ok(container.Mounts.some((mount) => mount.Source === source && mount.Destination === '/delta' && !mount.RW),
      'Existing owned Kusto container has a different warehouse mount');
    if (!container.State.Running) await execute('docker', ['start', container.Id]);
  } else {
    const volume = `${name}-data`;
    const volumes = (await execute('docker', ['volume', 'ls', '--filter', `name=^${volume}$`, '--format', '{{.Name}}'])).trim();
    if (volumes) {
      const [found] = JSON.parse(await execute('docker', ['volume', 'inspect', volume]));
      assert.equal(found.Labels?.[label], id, 'Refusing to use a foreign Kusto volume');
    } else await execute('docker', ['volume', 'create', '--label', `${label}=${id}`, volume]);
    const containerId = (await execute('docker', ['run', '--detach', '--init', '--name', name, '--label', `${label}=${id}`,
      '--memory', '6g', '--publish', `127.0.0.1:${port}:8080`, '--env', 'ACCEPT_EULA=Y',
      '--mount', `type=volume,source=${volume},target=/kustodata`,
      '--mount', `type=bind,source=${source},target=/delta,readonly`, ...networkArgs, image])).trim();
    [container] = JSON.parse(await execute('docker', ['inspect', containerId]));
  }
  assert.ok(container);
  writeJson(record, { containerId: container.Id, name, endpoint, hostEndpoint: `http://localhost:${port}`, source, image });
  let ready = false, lastError = '';
  for (let attempt = 0; attempt < 120; attempt++) {
    try { await request(endpoint, '.show version', true); ready = true; break; }
    catch (error) {
      lastError = errorMessage(error);
      const [current]: Container[] = JSON.parse(await execute('docker', ['inspect', container.Id]));
      assert.ok(current.State.Running, `Kusto exited: ${lastError}. Inspect docker logs ${container.Id}`);
    }
    await delay(1000);
  }
  assert.ok(ready, `Kusto not ready: ${lastError}. Inspect docker logs ${container.Id}`);
  assert.ok(table.split('.').every((part) => /^[a-zA-Z_][a-zA-Z0-9_]*$/.test(part)), 'Unsupported demo table identifier');
  await request(endpoint, `.create-or-alter external table ArrowcolEvents kind=delta (h@'/delta/${table.split('.').join('/')}')`, true);
  console.log(JSON.stringify({ endpoint, hostEndpoint: `http://localhost:${port}`, containerId: container.Id,
    database: 'NetDefaultDB', query: 'external_table("ArrowcolEvents") | take 100',
    storage: 'Native Delta external table; no rows copied into Kusto', record }, null, 2));
}
