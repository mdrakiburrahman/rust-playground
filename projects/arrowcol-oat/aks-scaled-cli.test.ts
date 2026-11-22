import assert from 'node:assert/strict';
import { createHash, generateKeyPairSync, randomUUID, sign } from 'node:crypto';
import { mkdtemp, readFile, rm, writeFile } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { dirname, join, resolve } from 'node:path';
import test, { after, before } from 'node:test';
import { parseAllDocuments } from 'yaml';
import { Config, load_env, ROOT } from '../../infra/azure/bootstrap.js';
import { loadStampConfig } from '../../infra/azure/stamp.js';
import { main, projectCollectorContract, renderManifest, type BenchmarkDependencies } from './aks-direct-benchmark.js';
import { applyCollectorPromotion, collectorSettings, qualifiedConfigDigest } from './aks-collector-promotion.js';
import { collectorShapes, effectiveWorkloadConfig, renderScaledProfileManifest } from './aks-scaled-profile.js';
import { clientRepository, imageBuilds, repositories } from './publish-images.js';
import { POD_RESOURCE_SAMPLE_COMMAND } from './aks-node-observation.js';
import { PRIMARY_REPOSITORY, PRODUCER_ID } from './aks-candidate-build.js';
import { installedExecutables, type ApplicationEvidence } from './aks-application-build.js';

const azureEnvironment = Object.fromEntries(Object.entries(process.env).filter(([key]) =>
  key.startsWith('AZURE_') || key.startsWith('ARROWCOL_AZURE_')));
before(() => { for (const key of Object.keys(azureEnvironment)) delete process.env[key]; });
after(() => { Object.assign(process.env, azureEnvironment); });

interface Resource {
  kind: string;
  data?: Record<string, string>;
  metadata: { name: string; uid?: string; labels: Record<string, string>; annotations?: Record<string, string> };
  spec: { template: { metadata: { labels: Record<string, string> }; spec: {
    containers: { name: string; image: string; args: string[]; resources: Record<string, Record<string, string>> }[];
    [key: string]: unknown;
  } } };
}

const qualifiedConfig = {
  platform: { kind: 'local' }, warmup_seconds: 60, measure_seconds: 600, pilot_mode: false,
  arrowcol_endpoint: 'http://127.0.0.1:32010', router_metrics_endpoint: 'http://127.0.0.1:9101',
  tables: ['telemetry.sqlserver.sqlserver_availability_replica_states'], rows_per_table: 65536,
  dmv_concurrency: 32, qap_endpoint: 'fixture', qap_timeout_seconds: 60,
  replica_counts: [1], dfe_worker_concurrency: [32],
  tunables: {
    client: { data_profile: 'realistic', batch_rows: 4096 },
    collector: { batching: { pipeline_depth: 16, coalesce_publications: true }, runtime: { cpu_workers: 7 } },
    parquet_writer: { defaults: { compression: 'zstd', compression_level: 3 } },
  },
};
const collectorProof = process.env.ARROWCOL_COLLECTOR_CONTRACT_PROOF === '1';
const applicationInvocation = ['docker', 'buildx', 'build', '--builder', 'arrowcol-application-offline-mock', '--load', '.'];
const applicationInvocationHash = createHash('sha256').update(JSON.stringify(applicationInvocation)).digest('hex');
function applicationOutput(role: typeof imageBuilds[number][0], repository: string, image: string, commit: string) {
  return { role, repository, digest: image, media_type: 'application/vnd.docker.distribution.manifest.v2+json',
    platforms: [{ os: 'linux', architecture: 'amd64', digest: image, config_digest: 'sha256:' + 'd'.repeat(64) }],
    application: { schema_version: 1, source_commit: commit, isolation: 'fresh_docker_container_builder',
      build_invocation_sha256: applicationInvocationHash,
      executables: installedExecutables[role].map(path => ({ path, sha256: 'e'.repeat(64),
        platform_digest: image, config_digest: 'sha256:' + 'd'.repeat(64) })) } };
}

async function fixture(scaled = true, failure?: 'missing-client' | 'failed-client' | 'partial-start' | 'collector-create' | 'pool-drift' | 'untrusted' | 'unknown-quota',
  promotion?: 'valid' | 'readback-drift') {
  const directory = await mkdtemp(join(tmpdir(), 'scaled-cli-'));
  const runId = randomUUID();
  const runHex = runId.replaceAll('-', '');
  const envFile = join(directory, 'env');
  const stampFile = join(directory, 'stamp.yaml');
  const imageFile = join(directory, 'images.json');
  const applicationFile = join(directory, 'independent-application.json');
  const promotionFile = join(directory, 'promotion.json');
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
  const roleImages = new Map<string, string>();
  let ordinal = 0;
  for (const match of base.matchAll(/image: \S+\/([^/@]+@sha256:[0-9a-f]{64})/g)) {
    const reference = match[1]!, repository = reference.split('@')[0]!;
    if (!roleImages.has(repository)) roleImages.set(repository, 'sha256:' + String(++ordinal).padStart(64, '0'));
    images[reference] = roleImages.get(repository)!;
  }
  await writeFile(imageFile, JSON.stringify({ schemaVersion: 1, sourceCommit: commit,
    workloadConfigSha256: effectiveWorkloadConfig(workload).sha256, images }));
  const application: ApplicationEvidence = { schema_version: 1, source_commit: commit, roles: [] };
  for (const [reference, image] of Object.entries(images)) {
    const role = imageBuilds.find(([name]) => (name === 'client' ? clientRepository : repositories[name]) === reference.split('@')[0])![0];
    if (application.roles.some(value => value.role === role)) continue;
    application.roles.push({ role, image_digest: image, platform_digest: image, config_digest: 'sha256:' + 'd'.repeat(64),
      build_invocation_sha256: applicationInvocationHash, comparison: 'source-only',
      executables: installedExecutables[role].map(path => ({ path, sha256: 'e'.repeat(64) })) });
  }
  await writeFile(applicationFile, JSON.stringify(application));
  if (promotion) await writeFile(promotionFile, JSON.stringify({
    schema_version: 1, qualified_source_commit: commit, qualified_config_sha256: qualifiedConfigDigest(JSON.stringify(qualifiedConfig)),
    qualified_config_json: JSON.stringify(qualifiedConfig), won_coordinates: ['collector.batching.pipeline_depth'],
  }));

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
  const observationAbort = new AbortController();
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
    readDelta: async (_azure, _path, start, end, _guard, options) => {
      assert.equal(end - start, 600_000);
      if (scaled) { assert.ok(events.includes('observer-stop')); assert.equal(options?.retainCommitSummaries, true); }
      events.push('delta-read');
      return { total_rows: 1200, total_files: 2, total_bytes: 2400, total_commits: 1,
        window_rows: 1200, window_files: 2, window_bytes: 2400, window_commits: 1,
        first_version: 0, last_version: 0, delta_version: 0, active_rows: 1200,
        ...(scaled ? { table_id: 'table-id', commit_summaries: [
          { version: 0, commit_info_timestamp_unix_ms: start, rows: 1200, files: 2, bytes: 2400 },
        ] } : {}) };
    },
    startObservation: options => {
      assert.equal(options.targets.length, 3);
      assert.equal(options.measurement_end_unix_ms - options.measurement_start_unix_ms, 600_000);
      assert.equal(schedule, 0);
      events.push('observer-start');
      const sample = options.execute(options.targets[0]!, POD_RESOURCE_SAMPLE_COMMAND, observationAbort.signal);
      return { async stop() { await sample; events.push('observer-stop'); return {
        schema_version: 2, source: 'pod_procfs_cgroup_v2', scope: 'target_pods_aggregated_by_node_and_role',
        node_total_source: 'unavailable', node_total_unavailable_reason: 'offline_mock_not_measured',
        sample_interval_ms: 10000, sample_timeout_ms: 5000, measurement_start_unix_ms: options.measurement_start_unix_ms,
        measurement_end_unix_ms: options.measurement_end_unix_ms, nodes: [], node_totals: [], pods: [],
        unavailable_reason: 'offline_mock_not_measured',
      }; } };
    },
    runTree: async (args, _timeout, _cwd, _env, _stderr, _maxBytes, signal) => {
      commands.push(args);
      const ok = (value: unknown = '') => ({ returncode: 0, stdout: typeof value === 'string' ? value : JSON.stringify(value), stderr: '' });
      const failed = () => ({ returncode: 1, stdout: '', stderr: 'deterministic partial startup failure' });
      if (args[0] === 'bash') return ok();
      if (args[0] === 'cosign') {
        const digest = args.at(-1)!.split('@')[1]!;
        const image = args.at(-1)!.split('@')[0]!;
        const build = imageBuilds.find(([role]) => (role === 'client' ? clientRepository : repositories[role]) === image.split('/').at(-1))!;
        const statement = { _type: 'https://in-toto.io/Statement/v0.1', predicateType: 'https://slsa.dev/provenance/v0.2',
          subject: [{ name: image, digest: { sha256: digest.slice(7) } }],
          predicate: { builder: { id: 'urn:arrowcol:oat:publish-images:buildLocalImages:v1' },
            invocation: { configSource: { uri: 'git+https://github.com/mdrakiburrahman/rust-playground',
              digest: { sha1: failure === 'untrusted' ? 'b'.repeat(40) : commit },
              entryPoint: 'projects/arrowcol-oat/publish-images.ts#buildLocalImages' },
              parameters: { revision: commit, role: build[0], dockerfile: build[1], context: build[2],
                build_invocation: applicationInvocation } },
            output: applicationOutput(build[0], image, digest, commit),
            materials: [{ uri: 'git+https://github.com/mdrakiburrahman/rust-playground', digest: { sha1: commit } }] } };
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
          return ok(['E', 'D', 'cores'].map(value => ({ name: { value }, currentValue: failure === 'unknown-quota' ? null : 0, limit: 2000 })));
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
        if (args.includes('exec')) {
          assert.equal(signal, observationAbort.signal);
          events.push('abortable-observer-read');
          return ok('{}');
        }
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
          if (name === 'pod-0') {
            if (promotion) {
              events.push('collector-startup-readback');
              const configs = named('arrowcol-config');
              assert.ok(configs?.data);
              const pipeline = parseAllDocuments(configs.data['dataflow.yaml']!)[0]!.toJSON();
              const batching = pipeline.groups.arrowcol.pipelines.ingest.nodes.delta.config.batching;
              const commit = pipeline.groups.arrowcol.pipelines.ingest.nodes.delta.config.commit;
              batching.max_age = '1m';
              if (promotion === 'readback-drift') batching.pipeline_depth = 8;
              const startup = JSON.stringify({ component: 'receiver', config: {
                batching, commit, runtime: { cpu_workers: 32 },
              } }).replace('"max_parquet_ratio":4', '"max_parquet_ratio":4.0');
              return ok('arrowcol_effective_config ' + startup + '\narrowcol_data_commit ' + JSON.stringify({ schema_version: 1, outcome: 'committed', table_id: 'table-id',
                delta_version: 0, rows: 1200, files: 2, parquet_bytes: 2400, finished_at_unix_ms: schedule + 60001 }));
            }
            events.push('collector-window-log');
            return ok('arrowcol_data_commit ' + JSON.stringify({ schema_version: 1, outcome: 'committed', table_id: 'table-id',
              delta_version: 0, rows: 1200, files: 2, parquet_bytes: 2400, finished_at_unix_ms: schedule + 60001 }));
          }
          if (name.startsWith('pod-')) events.push('client-window-log');
          if (name === 'direct-catalog-verified') {
            return ok(JSON.stringify({ kind: 'oat_run_table_registered', schema_version: 1, catalog_table_count: 1,
              run_id: runId, table: 'telemetry.sqlserver.sqlserver_availability_replica_states',
              location: `abfss://${config.data}@${config.account}.dfs.core.windows.net/direct-baseline-1/runs/${runId}/dataflow/telemetry/sqlserver/sqlserver_availability_replica_states`,
              provider: 'delta', schema_fingerprint: 'sha256:' + 'a'.repeat(64) }));
          }
          const jobName = name.startsWith('pod-') ? pods().find(pod => pod.metadata.name === name)!.metadata.labels['job-name'] : name;
          if (typeof jobName !== 'string') throw new Error('Mock client pod has no owning Job.');
          const job = named(jobName);
          const generatorArgs = job!.spec.template.spec.containers[0]!.args;
          const clientId = generatorArgs[generatorArgs.indexOf('--client-id') + 1];
          return ok(`OAT_CLIENT_READY client_id=${clientId}\nOAT_CLIENT_SCHEDULE_RECEIVED client_id=${clientId} run_start_unix_ms=${schedule}\nmeasured_rows=600 measured_seconds=600.000 rows_per_second=1.000 measurement_window_start_unix_ms=${schedule + 60000} measurement_window_end_unix_ms=${schedule + 660000}`);
        }
        if (args.includes('get')) {
          const kind = args[args.indexOf('get') + 1];
          const name = args[args.indexOf('get') + 2]!;
          if (kind === 'namespace') return ok(namespace ?? '');
          if (kind === 'pod') return ok(pods().find(pod => pod.metadata.name === name) ?? '');
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
    '--image-manifest', imageFile, '--provenance-key', publicKey, '--application-evidence', applicationFile,
  ] : []), ...(promotion ? ['--collector-promotion', promotionFile] : [])];
  const reportPath = resolve(ROOT, 'out/arrowcol-oat/direct-benchmark', runHex, 'report.json');
  return { args, dependencies, commands, events, reportPath, imageFile, promotionFile, base, directory,
    state: () => ({ namespace, collectorPool, clientMax, clientNodes }),
    async dispose() { await rm(directory, { recursive: true, force: true }); await rm(join(reportPath, '..'), { recursive: true, force: true }); } };
}

test('historical no-promotion render is byte-identical and rejects non-collector coordinates before projection', async () => {
  const f = await fixture();
  try {
    const rejectProject = async () => { throw new Error('Projection must not run'); };
    assert.equal((await applyCollectorPromotion(f.base, undefined, 'a'.repeat(40), rejectProject)).manifest, f.base);
    const control = collectorSettings(f.base);
    assert.equal(control.dfe_core_allocation_count, 32);
    assert.equal(control.args instanceof Array && control.args.at(-1), '48');
    await assert.rejects(applyCollectorPromotion(f.base, {
      schema_version: 1, qualified_source_commit: 'a'.repeat(40), qualified_config_sha256: qualifiedConfigDigest(JSON.stringify(qualifiedConfig)),
      qualified_config_json: JSON.stringify(qualifiedConfig), won_coordinates: ['client.batch_rows'],
    }, 'a'.repeat(40), rejectProject), /Unmappable/);
    const unsafe = { ...qualifiedConfig, tunables: { collector: { commit: { max_pending_files: Number.MAX_SAFE_INTEGER + 1 } } } };
    await assert.rejects(applyCollectorPromotion(f.base, {
      schema_version: 1, qualified_source_commit: 'a'.repeat(40), qualified_config_sha256: qualifiedConfigDigest(JSON.stringify(unsafe)),
      qualified_config_json: JSON.stringify(unsafe), won_coordinates: ['collector.commit.max_pending_files'],
    }, 'a'.repeat(40), rejectProject), /represented exactly/);
  } finally { await f.dispose(); }
});

test('actual promoted CLI uses strict Rust projection, changes only won depth and retains UID-bound startup readback', { skip: !collectorProof }, async () => {
  const f = await fixture(true, undefined, 'valid');
  try {
    const output = join(f.directory, 'promoted.yaml');
    const renderArgs = f.args.filter((_value, index) => index !== 0 && index !== f.args.indexOf('--run-id') && index !== f.args.indexOf('--run-id') + 1);
    assert.equal(await main(['render', ...renderArgs, '--output', output], f.dependencies), 0);
    assert.equal(f.commands.length, 0);
    const rendered = await readFile(output, 'utf8');
    assert.equal(effectiveWorkloadConfig(rendered).sha256, JSON.parse(await readFile(f.imageFile, 'utf8')).workloadConfigSha256);
    const settings = JSON.parse(await readFile(output + '.collector.json', 'utf8'));
    assert.equal(settings.historical_collector_control.dfe_core_allocation_count, 32);
    assert.equal(settings.expected_collector.dfe_core_allocation_count, 176);
    assert.deepEqual(settings.collector_promotion.projected_environment, { ARROWCOL_COLLECTOR_BATCHING_CONFIG: '{"pipeline_depth":16}' });
    assert.equal(await main(f.args, f.dependencies), 0);
    const report = JSON.parse(await readFile(f.reportPath, 'utf8'));
    assert.equal(report.scaled_profile.collector_startup_readback.batching.pipeline_depth, 16);
    assert.equal(report.scaled_profile.collector_startup_readback.runtime.cpu_workers, 32);
    assert.equal(report.scaled_profile.collector_capacity_readback.cpu_workers, 32);
    assert.equal(report.scaled_profile.collector_capacity_readback.dfe_core_allocation_count, null);
    assert.equal(report.scaled_profile.workload.dataProfile, 'diagnostic');
    assert.ok(f.events.indexOf('collector-startup-readback') < f.events.indexOf('schedule'));
    for (const phase of ['startup', 'finished']) {
      const retained = JSON.parse(await readFile(join(dirname(f.reportPath), 'collector-promotion-' + phase + '.json'), 'utf8'));
      assert.equal(retained.pod_uid, 'uid-0');
      assert.equal(retained.observed.batching.pipeline_depth, 16);
      assert.equal(retained.expected_collector_sha256, report.scaled_profile.expected_collector.sha256);
    }
    const input = JSON.parse(await readFile(f.promotionFile, 'utf8'));
    const runtime = await applyCollectorPromotion(f.base, { ...input, won_coordinates: ['collector.runtime.cpu_workers'] },
      'a'.repeat(40), projectCollectorContract);
    assert.deepEqual(runtime.binding?.projected_environment, { ARROWCOL_COLLECTOR_CPU_WORKERS: '7' });
    assert.equal(collectorSettings(runtime.manifest).dfe_core_allocation_count, 32);
  } finally { await f.dispose(); }
});

test('strict promoted CLI rejects invalid enum/config binding and live drift before scheduling with owned cleanup', { skip: !collectorProof }, async () => {
  const f = await fixture(true, undefined, 'readback-drift');
  try {
    assert.equal(await main(f.args, f.dependencies), 1);
    assert.ok(f.events.includes('collector-startup-readback'));
    assert.ok(!f.events.includes('schedule'));
    assert.deepEqual(f.state(), { namespace: undefined, collectorPool: undefined, clientMax: 1, clientNodes: 0 });
    const invalid = { ...qualifiedConfig, tunables: { parquet_writer: { defaults: { compression: 'not-a-codec' } } } };
    await assert.rejects(applyCollectorPromotion(f.base, {
      schema_version: 1, qualified_source_commit: 'a'.repeat(40), qualified_config_sha256: qualifiedConfigDigest(JSON.stringify(invalid)),
      qualified_config_json: JSON.stringify(invalid), won_coordinates: ['parquet_writer.defaults.compression'],
    }, 'a'.repeat(40), projectCollectorContract), /parse collector contract|unknown variant/);
    await assert.rejects(applyCollectorPromotion(f.base, {
      schema_version: 1, qualified_source_commit: 'a'.repeat(40), qualified_config_sha256: '0'.repeat(64),
      qualified_config_json: JSON.stringify(qualifiedConfig), won_coordinates: ['collector.batching.pipeline_depth'],
    }, 'a'.repeat(40), projectCollectorContract), /qualified source\/config digest/);
    const input = JSON.parse(await readFile(f.promotionFile, 'utf8'));
    const documents = parseAllDocuments(f.base);
    const writer = documents.find(doc => doc.getIn(['metadata', 'name']) === 'arrowcol')!;
    writer.setIn(['spec', 'template', 'spec', 'containers', 0, 'env'], [
      { name: 'ARROWCOL_COLLECTOR_BATCHING_CONFIG', value: '{"pipeline_depth":8}' },
    ]);
    await assert.rejects(applyCollectorPromotion(documents.map(doc => doc.toString()).join(''), input,
      'a'.repeat(40), projectCollectorContract), /collides with explicit collector env/);
    await assert.rejects(applyCollectorPromotion(f.base.replace('--num-cores', '--unmapped-core-flag'), input,
      'a'.repeat(40), projectCollectorContract), /Explicit collector flags/);
    const policyInput = { ...input, won_coordinates: ['parquet_writer.defaults.compression'] };
    await assert.rejects(applyCollectorPromotion(f.base + '\n---\nkind: ConfigMap\nmetadata:\n  name: policy\ndata:\n  table_policy: explicit\n',
      policyInput, 'a'.repeat(40), projectCollectorContract), /table\/writer policy/);
  } finally { await f.dispose(); }
});

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
    for (const client of report.scaled_profile.client_results) {
      const summary = JSON.parse(await readFile(join(dirname(f.reportPath), client.job_name + '.measurement.json'), 'utf8'));
      assert.deepEqual(summary, client.final_measurement_summary);
      assert.equal(summary.pod_uid, client.pod_uid);
      assert.equal(summary.configured_transport_slots, 16);
      assert.equal(summary.measurement, null);
      assert.match(summary.measurement_unavailable_reason, /not substituted/);
    }
    assert.ok(f.events.lastIndexOf('client-window-log') < f.events.indexOf('namespace-delete'));
    assert.equal(report.scaled_profile.placement.clients.length, 2);
    assert.equal(report.scaled_profile.trusted_build_provenance_verified, true);
    const collectorWindow = JSON.parse(await readFile(join(dirname(f.reportPath), 'collector-window.json'), 'utf8'));
    assert.deepEqual(collectorWindow, report.scaled_profile.collector_receipt_window);
    assert.equal(collectorWindow.pod_uid, 'uid-0');
    assert.equal(collectorWindow.joined_rows, 1200);
    assert.deepEqual(collectorWindow.receipt_window_versions, [0]);
    assert.deepEqual(collectorWindow.delta_window_versions, [0]);
    assert.ok(f.events.indexOf('delta-read') < f.events.indexOf('collector-window-log'));
    assert.ok(f.events.indexOf('collector-window-log') < f.events.indexOf('namespace-delete'));
    assert.equal(report.node_observation.schema_version, 2);
    assert.ok(f.events.includes('abortable-observer-read'));
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

test('actual CLI verifies independently anchored DSSE bundles, rejects fixture scope and records signed platform digests', async () => {
  for (const scope of ['local-fixture', 'candidate']) {
    const f = await fixture();
    try {
      const keys = generateKeyPairSync('ed25519');
      const keyPath = f.args[f.args.indexOf('--provenance-key') + 1]!;
      await writeFile(keyPath, keys.publicKey.export({ type: 'spki', format: 'pem' }));
      const keyId = createHash('sha256').update(keys.publicKey.export({ type: 'spki', format: 'der' })).digest('hex');
      const binding = JSON.parse(await readFile(f.imageFile, 'utf8'));
      const commit = f.args[f.args.indexOf('--candidate-commit') + 1]!;
      const references = new Map(Object.entries(binding.images as Record<string, string>).map(([reference, digest]) =>
        [reference.split('@')[0]!, digest]));
      const envelopes = [...references.entries()].map(([repo, imageDigest]) => {
        const build = imageBuilds.find(([role]) => (role === 'client' ? clientRepository : repositories[role]) === repo)!;
        const repository = 'arrowcollector1.azurecr.io/' + repo;
        const material = { path: '', uri: PRIMARY_REPOSITORY, digest: { sha1: commit, gitTree: 'e'.repeat(40) },
          archive_sha256: 'f'.repeat(64), repository_verified: true };
        const statement = { _type: 'https://in-toto.io/Statement/v0.1', predicateType: 'https://slsa.dev/provenance/v0.2',
          subject: [{ name: repository, digest: { sha256: imageDigest.slice(7) } }],
          predicate: { builder: { id: PRODUCER_ID }, scope: 'candidate',
            invocation: { configSource: { uri: PRIMARY_REPOSITORY, digest: material.digest,
              entryPoint: 'projects/arrowcol-oat/aks-candidate-build.ts#buildCandidateImages' },
            parameters: { revision: commit, role: build[0], dockerfile: build[1], context: build[2], platform: 'linux/amd64',
              build_invocation: applicationInvocation } },
            materials: [material], output: applicationOutput(build[0], repository, imageDigest, commit) } };
        const payload = Buffer.from(JSON.stringify(statement));
        const type = 'application/vnd.in-toto+json';
        const pae = Buffer.concat([Buffer.from(`DSSEv1 ${Buffer.byteLength(type)} ${type} ${payload.length} `), payload]);
        return { payloadType: type, payload: payload.toString('base64'), signatures: [{ keyid: keyId, sig: sign(null, pae, keys.privateKey).toString('base64') }] };
      });

      test('scaled live gate refuses missing or mismatched independent executable evidence before workload writes', async () => {
        for (const mode of ['absent', 'wrong-bytes', 'stale-code']) {
          const f = await fixture();
          try {
            const index = f.args.indexOf('--application-evidence');
            if (mode === 'absent') {
              f.args.splice(index, 2);
            } else {
              const file = f.args[index + 1]!;
              const expected: ApplicationEvidence = JSON.parse(await readFile(file, 'utf8'));
              const role = expected.roles.find(value => value.role === 'dataflow')!;
              if (mode === 'wrong-bytes') role.executables[0]!.sha256 = 'f'.repeat(64);
              else {
                role.comparison = 'changed-code';
                role.executables[0]!.baseline_sha256 = role.executables[0]!.sha256;
              }
              await writeFile(file, JSON.stringify(expected));
            }
            assert.equal(await main(f.args, f.dependencies), 1);
            assert.ok(!f.events.some(event => event.startsWith('apply-')));
            assert.ok(!f.commands.some(args => args.includes('update') || args.includes('create')));
            if (mode === 'absent') assert.equal(f.commands.length, 0);
          } finally { await f.dispose(); }
        }
      });
      const bundlePath = join(f.directory, 'build-bundle.json');
      await writeFile(bundlePath, JSON.stringify({ schema_version: 1, scope, source_commit: commit, envelopes }));
      const args = [...f.args, '--provenance-bundle', bundlePath];
      assert.equal(await main(args, f.dependencies), scope === 'candidate' ? 0 : 1);
      if (scope === 'local-fixture') {
        assert.ok(!f.events.some(event => event.startsWith('apply-')));
        continue;
      }
      assert.ok(!f.commands.some(command => command[0] === 'cosign'));
      const report = JSON.parse(await readFile(f.reportPath, 'utf8'));
      assert.equal(Object.keys(report.scaled_profile.attested_platform_digests).length, references.size);
    } finally { await f.dispose(); }
  }
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

test('scaled CLI rejects unknown quota usage before any workload or pool write', async () => {
  const f = await fixture(true, 'unknown-quota');
  try {
    assert.equal(await main(f.args, f.dependencies), 1);
    assert.ok(!f.commands.some(args => args[1] === 'deployment' || args.includes('apply') || args.includes('update')));
    assert.deepEqual(f.state(), { namespace: undefined, collectorPool: undefined, clientMax: 1, clientNodes: 0 });
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
