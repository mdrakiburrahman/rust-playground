import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join, resolve } from 'node:path';
import test from 'node:test';
import { parseAllDocuments } from 'yaml';
import { Config, load_env, ROOT } from '../../infra/azure/bootstrap.js';
import { loadStampConfig } from '../../infra/azure/stamp.js';
import { main, renderManifest, type BenchmarkDependencies } from './aks-direct-benchmark.js';
import { collectorShapes, effectiveWorkloadConfig, renderScaledProfileManifest } from './aks-scaled-profile.js';

interface Resource {
  kind: string;
  metadata: { name: string; uid?: string; labels: Record<string, string>; annotations?: Record<string, string> };
  spec: { template: { metadata: { labels: Record<string, string> }; spec: {
    containers: { name: string; image: string; args: string[]; resources: Record<string, Record<string, string>> }[];
    [key: string]: unknown;
  } } };
}

async function fixture(scaled = true, failure?: 'missing-client' | 'failed-client' | 'partial-start' | 'collector-create' | 'pool-drift' | 'untrusted') {
  const directory = await mkdtemp(join(tmpdir(), 'scaled-cli-'));
  const runId = randomUUID();
  const runHex = runId.replaceAll('-', '');
  const envFile = join(directory, 'env');
  const stampFile = join(directory, 'stamp.yaml');
  const imageFile = join(directory, 'images.json');
  const publicKey = join(directory, 'trusted.pub');
  const commit = 'a'.repeat(40);
  await writeFile(envFile, 'AZURE_SUBSCRIPTION_ID=00000000-0000-0000-0000-000000000000\nAZURE_UAMI_CLIENT_ID=11111111-1111-4111-8111-111111111111\n');
  await writeFile(stampFile, await readFile(resolve(ROOT, 'infra/azure/stamp.example.yaml')));
  await writeFile(publicKey, 'offline mock trust root');
  const config = new Config({ ...load_env(envFile), ...process.env }, loadStampConfig(stampFile));
  const source = await readFile(resolve(ROOT, 'experiments/aks-direct-benchmark/arrowcollector-aks-benchmark.yaml'), 'utf8');
  const placeholderRun = '00000000-0000-4000-8000-000000000001';
  const base = renderManifest(source, {
    BENCHRUNUUID: placeholderRun, BENCHRUNHEX: placeholderRun.replaceAll('-', ''), BENCHOWNER: 'a'.repeat(48),
    BENCHCLIENTID: config.identity_client_id, BENCHREGISTRY: config.registryLoginServer,
    BENCHDELTA_ROOT: `abfss://${config.data}@${config.account}.dfs.core.windows.net/direct-baseline-1/runs/${placeholderRun}/dataflow`,
    BENCHSTAMP: config.stamp, BENCHSUBSCRIPTION: config.subscription, BENCHRESOURCEGROUP: config.group,
    BENCHSTORAGEACCOUNT: config.account, BENCHCHECKPOINTS: config.checkpoints, BENCHDURATIONSECS: '600',
  });
  const workload = renderScaledProfileManifest(base, placeholderRun, 2);
  const images: Record<string, string> = {};
  let ordinal = 0;
  for (const match of base.matchAll(/image: \S+\/([^/@]+@sha256:[0-9a-f]{64})/g)) {
    images[match[1]!] ??= 'sha256:' + String(++ordinal).padStart(64, '0');
  }
  await writeFile(imageFile, JSON.stringify({ schemaVersion: 1, sourceCommit: commit,
    workloadConfigSha256: effectiveWorkloadConfig(workload).sha256, images }));

  const commands: string[][] = [];
  const events: string[] = [];
  let time = 1_800_000_000_000;
  let namespace: Resource | undefined;
  let resources: Resource[] = [];
  let collectorPool: { name: string; vmSize: string; tags: Record<string, string> } | undefined;
  let clientMax = 1;
  let schedule = 0;
  let clientNodes = 0;
  let pendingClient = true;
  const named = (name: string) => resources.find(value => value.metadata?.name === name);
  const loadJobs = () => resources.filter(value => value.kind === 'Job' && value.metadata.name.startsWith('direct-load-'));
  const pool = () => ({ enableAutoScaling: true, minCount: 0, maxCount: clientMax, count: clientNodes, vmSize: 'Standard_D64ds_v6' });
  const pods = () => {
    const writer = named('arrowcol');
    if (!writer || !namespace) return [];
    const measured = [writer, ...loadJobs()];
    return measured.filter((value, index) => failure !== 'missing-client' || index !== 2).map((value, index) => ({
      metadata: { ...value.spec.template.metadata, name: 'pod-' + index, uid: 'uid-' + index,
        labels: { ...value.spec.template.metadata.labels, ...(index ? { 'job-name': value.metadata.name } : {}) } },
      spec: { ...value.spec.template.spec, nodeName: index ? 'client-node-' + index : 'collector-node' },
      status: { phase: index && schedule ? 'Succeeded' : 'Running', containerStatuses: [{
        name: index ? 'generator' : 'dataflow', ready: true, restartCount: 0,
        imageID: 'docker-pullable://' + value.spec.template.spec.containers[0].image,
      }] },
    }));
  };
  const arg = (args: string[], flag: string) => args[args.indexOf(flag) + 1]!;
  const dependencies: BenchmarkDependencies = {
    now: () => time,
    sleep: async ms => { time += ms; },
    generation: async () => '22222222-2222-4222-8222-222222222222',
    readDelta: async (_azure, _path, start, end) => {
      assert.equal(end - start, 600_000);
      if (scaled) assert.ok(events.includes('observer-stop'));
      events.push('delta-read');
      return { total_rows: 1200, total_files: 2, total_bytes: 2400, total_commits: 1,
        window_rows: 1200, window_files: 2, window_bytes: 2400, window_commits: 1,
        first_version: 0, last_version: 0, delta_version: 0, active_rows: 1200 };
    },
    startObservation: options => {
      assert.equal(options.targets.length, 3);
      assert.equal(options.measurement_end_unix_ms - options.measurement_start_unix_ms, 600_000);
      assert.equal(schedule, 0);
      events.push('observer-start');
      return { async stop() { events.push('observer-stop'); return {
        schema_version: 1, source: 'pod_procfs_cgroup_v2', scope: 'target_pods_aggregated_by_node_and_role',
        sample_interval_ms: 10000, measurement_start_unix_ms: options.measurement_start_unix_ms,
        measurement_end_unix_ms: options.measurement_end_unix_ms, nodes: [], pods: [],
        unavailable_reason: 'offline_mock_not_measured',
      }; } };
    },
    runTree: async args => {
      commands.push(args);
      const ok = (value: unknown = '') => ({ returncode: 0, stdout: typeof value === 'string' ? value : JSON.stringify(value), stderr: '' });
      const failed = () => ({ returncode: 1, stdout: '', stderr: 'deterministic partial startup failure' });
      if (args[0] === 'bash') return ok();
      if (args[0] === 'cosign') {
        const digest = args.at(-1)!.split('@')[1]!;
        const statement = { _type: 'https://in-toto.io/Statement/v0.1', predicateType: 'https://slsa.dev/provenance/v0.2',
          subject: [{ digest: { sha256: digest.slice(7) } }],
          predicate: { materials: [{ uri: 'git+https://github.com/mdrakiburrahman/rust-playground',
            digest: { sha1: failure === 'untrusted' ? 'b'.repeat(40) : commit } }] } };
        return ok({ payload: Buffer.from(JSON.stringify(statement)).toString('base64') });
      }
      if (args[0] === 'kubelogin') return ok(args.includes('--help') ? '--disable-environment-override' : '');
      if (args[0] === 'npx') {
        if (args.includes('prepare-run-fic')) await writeFile(arg(args, '--record'), '{}', { mode: 0o600 });
        if (args.includes('cleanup-run-fic')) { events.push('fic-cleanup'); await rm(arg(args, '--record')); }
        return ok();
      }
      if (args[0] === 'az') {
        if (args.includes('get-credentials')) return ok();
        if (args[1] === 'account') return ok(arg(args, '--subscription'));
        if (args[1] === 'storage') return ok({ exists: false });
        if (args[1] === 'vm') {
          if (args.includes('list-skus')) return ok([
            { name: 'Standard_E192ids_v6', family: 'E', restrictions: [], capabilities: [{ name: 'vCPUs', value: '192' }, { name: 'MemoryGB', value: '1832' }] },
            { name: 'Standard_D64ds_v6', family: 'D', restrictions: [], capabilities: [{ name: 'vCPUs', value: '64' }, { name: 'MemoryGB', value: '256' }] },
          ]);
          return ok(['E', 'D', 'cores'].map(value => ({ name: { value }, currentValue: 0, limit: 2000 })));
        }
        if (args[1] === 'deployment') {
          const parameters = JSON.parse(await readFile(arg(args, '--parameters').slice(1), 'utf8'));
          collectorPool = { name: parameters.poolName.value, vmSize: parameters.vmSize.value,
            tags: { 'arrowcol-oat-run-id': parameters.runId.value, 'arrowcol-oat-owner-token': parameters.ownerToken.value } };
          if (failure === 'collector-create') return failed();
          return ok();
        }
        if (args.includes('nodepool')) {
          if (args.includes('list')) return ok(collectorPool ? [collectorPool] : []);
          const name = arg(args, '--name');
          if (args.includes('show')) return ok(name === 'benchd64' ? pool() : { enableAutoScaling: true, minCount: 0, maxCount: 1, count: 0 });
          if (args.includes('delete')) {
            assert.equal(name, collectorPool?.name);
            assert.equal(namespace, undefined);
            events.push('collector-delete'); collectorPool = undefined; return ok();
          }
          if (args.includes('scale')) { assert.equal(name, 'benchd64'); clientNodes = 0; return ok(); }
          if (args.includes('--max-count')) clientMax = Number(arg(args, '--max-count'));
          return ok();
        }
        if (args[1] === 'aks' && args.includes('show')) return ok({ agentPoolProfiles: [{ vnetSubnetId: '/fixture/subnet' }] });
      }
      if (args[0] === 'kubectl') {
        if (args.includes('apply')) {
          if (args.some(value => value.startsWith('--dry-run'))) return ok();
          resources = parseAllDocuments(await readFile(arg(args, '-f'), 'utf8')).map(document => document.toJSON()).filter(Boolean);
          const stage = arg(args, '-l').split('=')[1];
          events.push('apply-' + stage);
          if (stage === 'identity') namespace = { ...resources.find(value => value.kind === 'Namespace')!, metadata: {
            ...resources.find(value => value.kind === 'Namespace')!.metadata, uid: 'namespace-uid',
          } };
          if (stage === 'sql' && failure === 'partial-start') return failed();
          if (stage === 'load') clientNodes = scaled ? 2 : 1;
          return ok();
        }
        if (args.includes('patch')) {
          assert.equal(loadJobs().length, scaled ? 2 : 1);
          schedule = Number(JSON.parse(arg(args, '-p')).data.start);
          assert.ok(schedule - time >= 125000);
          events.push('schedule'); return ok();
        }
        if (args.includes('delete')) {
          events.push('namespace-delete'); namespace = undefined;
          if (failure === 'pool-drift') clientMax = 3;
          return ok();
        }
        if (args.includes('logs')) {
          const name = args[args.indexOf('logs') + 1]!.slice(4);
          if (name === 'direct-catalog-verified') {
            return ok(JSON.stringify({ kind: 'oat_run_table_registered', schema_version: 1, catalog_table_count: 1,
              run_id: runId, table: 'telemetry.sqlserver.sqlserver_availability_replica_states',
              location: `abfss://${config.data}@${config.account}.dfs.core.windows.net/direct-baseline-1/runs/${runId}/dataflow/telemetry/sqlserver/sqlserver_availability_replica_states`,
              provider: 'delta', schema_fingerprint: 'sha256:' + 'a'.repeat(64) }));
          }
          const job = named(name);
          const generatorArgs = job!.spec.template.spec.containers[0]!.args;
          const clientId = generatorArgs[generatorArgs.indexOf('--client-id') + 1];
          return ok(`OAT_CLIENT_READY client_id=${clientId}\nOAT_CLIENT_SCHEDULE_RECEIVED client_id=${clientId} run_start_unix_ms=${schedule}\nmeasured_rows=600 measured_seconds=600.000 rows_per_second=1.000 measurement_window_start_unix_ms=${schedule + 60000} measurement_window_end_unix_ms=${schedule + 660000}`);
        }
        if (args.includes('get')) {
          const kind = args[args.indexOf('get') + 1];
          const name = args[args.indexOf('get') + 2]!;
          if (kind === 'namespace') return ok(namespace ?? '');
          if (kind === 'deployment') return ok({ status: { availableReplicas: 1 } });
          if (kind === 'configmap') return ok(named(name));
          if (kind === 'job') {
            if (failure === 'missing-client' && name.endsWith('client-02')) return ok();
            if (name.startsWith('direct-load-') && schedule) time = schedule + 660000;
            return ok({ status: { ...(failure === 'failed-client' && name.endsWith('client-02') ? { failed: 1 } : {
              succeeded: name.startsWith('direct-load-') && !schedule ? 0 : 1,
            }) } });
          }
          if (kind === 'pods') {
            if (args.includes('--all-namespaces')) return ok({ items: [] });
            if (args.includes('-l') && arg(args, '-l').startsWith('job-name=')) {
              const jobName = arg(args, '-l').slice('job-name='.length);
              const selected = pods().filter(pod => pod.metadata.labels['job-name'] === jobName);
              if (pendingClient && selected.length) {
                pendingClient = false;
                events.push('pending-client');
                return ok({ items: [{ ...selected[0], status: { phase: 'Pending' } }] });
              }
              return ok({ items: selected });
            }
            return ok({ items: args.includes('-l') ? pods().slice(0, 1) : pods() });
          }
          if (kind === 'nodes') return ok({ items: Array.from({ length: arg(args, '-l').endsWith('benchd64') ? clientNodes : 0 }, (_, index) => ({ metadata: { name: 'node-' + index } })) });
          if (kind === 'node') return ok({ metadata: { labels: {
            'kubernetes.azure.com/agentpool': name === 'collector-node' ? collectorPool!.name : 'benchd64',
            'node.kubernetes.io/instance-type': name === 'collector-node' ? 'Standard_E192ids_v6' : 'Standard_D64ds_v6',
          } },
            status: { allocatable: { cpu: name === 'collector-node' ? '190' : '62', memory: name === 'collector-node' ? '1800Gi' : '250Gi' } } });
          if (name?.startsWith('--raw')) return ok('version');
        }
      }
      throw new Error('Unmocked command: ' + args.join(' '));
    },
  };
  const args = ['run', '--env-file', envFile, '--stamp-config', stampFile, '--run-id', runId, ...(scaled ? [
    '--profile', 'scaled', '--clients', '2', '--collector-shape', 'e192', '--candidate-commit', commit,
    '--image-manifest', imageFile, '--provenance-key', publicKey,
  ] : [])];
  const reportPath = resolve(ROOT, 'out/arrowcol-oat/direct-benchmark', runHex, 'report.json');
  return { args, dependencies, commands, events, reportPath, imageFile, directory,
    state: () => ({ namespace, collectorPool, clientMax, clientNodes }),
    async dispose() { await rm(directory, { recursive: true, force: true }); await rm(join(reportPath, '..'), { recursive: true, force: true }); } };
}

test('actual opt-in CLI renders and runs all clients, binds candidate pods/large collector and restores original pools', async () => {
  const f = await fixture();
  try {
    const output = join(f.directory, 'rendered.yaml');
    const renderArgs = f.args.filter((_value, index) => index !== 0 && index !== f.args.indexOf('--run-id') && index !== f.args.indexOf('--run-id') + 1);
    assert.equal(await main(['render', ...renderArgs, '--output', output], f.dependencies), 0);
    assert.equal(f.commands.length, 0);
    const templateOutput = join(f.directory, 'template.yaml');
    const imageIndex = renderArgs.indexOf('--image-manifest');
    const templateArgs = renderArgs.filter((_value, index) => index !== imageIndex && index !== imageIndex + 1);
    assert.equal(await main(['render', ...templateArgs, '--output', templateOutput], f.dependencies), 0);
    const template = JSON.parse(await readFile(templateOutput + '.images.json', 'utf8'));
    assert.equal(template.workloadConfigSha256, JSON.parse(await readFile(f.imageFile, 'utf8')).workloadConfigSha256);
    assert.ok(Object.values(template.images).every(value => value === 'sha256:' + '0'.repeat(64)));
    assert.equal(f.commands.length, 0);
    for (const [shape, clients] of [['e64', '1'], ['e192', '8']] as const) {
      const boundaryArgs = [...templateArgs];
      boundaryArgs[boundaryArgs.indexOf('--clients') + 1] = clients;
      boundaryArgs[boundaryArgs.indexOf('--collector-shape') + 1] = shape;
      const boundaryPath = join(f.directory, shape + '-boundary.yaml');
      assert.equal(await main(['render', ...boundaryArgs, '--output', boundaryPath], f.dependencies), 0);
      const documents = parseAllDocuments(await readFile(boundaryPath, 'utf8'));
      assert.equal(documents.filter(document => String(document.getIn(['metadata', 'name'])).startsWith('direct-load-')).length, Number(clients));
      const writer = documents.find(document => document.getIn(['metadata', 'name']) === 'arrowcol')!;
      assert.equal(writer.getIn(['spec', 'template', 'spec', 'containers', 0, 'args', 3]), String(collectorShapes[shape].runtimeCores));
    }
    assert.equal(f.commands.length, 0);
    const rendered = parseAllDocuments(await readFile(output, 'utf8'));
    const collector = rendered.find(document => document.getIn(['metadata', 'name']) === 'arrowcol')!;
    assert.equal(collector.getIn(['spec', 'template', 'spec', 'containers', 0, 'args', 3]), '176');
    assert.equal(collector.getIn(['spec', 'template', 'spec', 'containers', 0, 'resources', 'limits', 'memory']), '1536Gi');
    for (const document of rendered) {
      if (document.getIn(['spec', 'template', 'spec', 'nodeSelector', 'arrow-collector/role']) === 'arrowcol') {
        assert.equal(document.getIn(['spec', 'template', 'spec', 'nodeSelector', 'kubernetes.azure.com/agentpool']),
          collector.getIn(['spec', 'template', 'spec', 'nodeSelector', 'kubernetes.azure.com/agentpool']));
      }
    }
    assert.equal(await main(f.args, f.dependencies), 0);
    const report = JSON.parse(await readFile(f.reportPath, 'utf8'));
    assert.equal(report.scaled_profile.client_results.length, 2);
    assert.equal(report.scaled_profile.placement.clients.length, 2);
    assert.equal(report.scaled_profile.trusted_build_provenance_verified, true);
    assert.deepEqual(report.scaled_profile.hardware, collectorShapes.e192);
    assert.deepEqual(f.state(), { namespace: undefined, collectorPool: undefined, clientMax: 1, clientNodes: 0 });
    assert.ok(f.events.indexOf('observer-start') < f.events.indexOf('schedule'));
    assert.ok(f.events.includes('pending-client'));
    assert.ok(f.events.indexOf('observer-stop') < f.events.indexOf('delta-read'));
    assert.ok(f.events.indexOf('namespace-delete') < f.events.indexOf('collector-delete'));
    const commandCount = f.commands.length;
    assert.equal(await main(f.args, f.dependencies), 1);
    assert.equal(f.commands.length, commandCount);
  } finally { await f.dispose(); }
});

for (const failure of ['missing-client', 'failed-client', 'partial-start', 'collector-create'] as const) {
  test('actual scaled CLI fails closed and cleans up after ' + failure, async () => {
    const f = await fixture(true, failure);
    try {
      assert.equal(await main(f.args, f.dependencies), 1);
      assert.equal(f.events.includes('schedule'), false);
      assert.ok(f.events.includes(failure === 'collector-create' ? 'apply-identity' : failure === 'partial-start' ? 'apply-sql' : 'apply-load'));
      assert.deepEqual(f.state(), { namespace: undefined, collectorPool: undefined, clientMax: 1, clientNodes: 0 });
      assert.ok(f.events.includes('namespace-delete'));
      assert.ok(f.events.includes('collector-delete'));
    } finally { await f.dispose(); }
  });
}

test('actual CLI rejects self-declared provenance before any workload or pool write', async () => {
  const f = await fixture(true, 'untrusted');
  try {
    assert.equal(await main(f.args, f.dependencies), 1);
    assert.equal(f.events.includes('apply-identity'), false);
    assert.equal(f.commands.some(command => command[1] === 'deployment' || command.includes('update') || command.includes('scale')), false);
  } finally { await f.dispose(); }
});

test('actual CLI cleanup preserves externally drifted client pool settings rather than overwriting them', async () => {
  const f = await fixture(true, 'pool-drift');
  try {
    assert.equal(await main(f.args, f.dependencies), 1);
    assert.equal(f.state().clientMax, 3);
    assert.equal(f.state().collectorPool, undefined);
    assert.equal(f.commands.some(command => command.includes('--max-count') && command[command.indexOf('--max-count') + 1] === '1'), false);
  } finally { await f.dispose(); }
});

test('default CLI retains the original one-client runner and does not provision scaled capacity or verify images', async () => {
  const f = await fixture(false);
  try {
    assert.equal(await main(f.args, f.dependencies), 0);
    const report = JSON.parse(await readFile(f.reportPath, 'utf8'));
    assert.equal(report.scaled_profile, undefined);
    assert.equal(f.commands.some(command => command[0] === 'cosign' || command[1] === 'deployment'), false);
    assert.equal(f.events.includes('observer-start'), false);
    assert.equal(f.commands.filter(command => command.includes('patch')).length, 1);
  } finally { await f.dispose(); }
});
