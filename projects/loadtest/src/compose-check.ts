import { spawnSync } from 'node:child_process';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { benchmarkQuiver, buildBenchmarkComposeOverride, buildBenchmarkRuntimeConfigs, dataflowConfigFile } from './runtime-config.js';

const workspace = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '../../..');
for (const quiver of [false, true]) {
  const state = path.join(workspace, 'projects/loadtest/.temp', quiver ? 'compose-check-quiver' : 'compose-check');
  for (const directory of [
    'direct', 'dataflow', 'tenant', 'tenant-checkpoints', 'results', 'router-state', 'direct-state-1', 'dataflow-state-1',
    ...(quiver ? [benchmarkQuiver.directHostDirectory, benchmarkQuiver.dataflowHostDirectory] : []),
  ]) {
    mkdirSync(path.join(state, directory), { recursive: true });
  }
  const runtime = buildBenchmarkRuntimeConfigs(
    readFileSync(path.join(workspace, 'projects/loadtest/config/direct.yaml.tmpl'), 'utf8'),
    readFileSync(path.join(workspace, dataflowConfigFile(quiver)), 'utf8'),
    { instanceKey: 'loadtest-direct-1', advertisedEndpoint: '10.89.0.21:50051', generation: 1 },
    { quiver },
  );
  writeFileSync(path.join(state, 'direct-state-1/direct.yaml'), runtime.directYaml);
  writeFileSync(path.join(state, 'dataflow-state-1/dataflow.yaml'), runtime.dataflowYaml);
  const overrideFile = path.join(state, 'router-state/compose-runtime.json');
  writeFileSync(overrideFile, buildBenchmarkComposeOverride(state, { quiver }));
  checkCompose(state, overrideFile);
}

function checkCompose(state: string, overrideFile: string): void {
  const args = [
    'compose',
    '--project-name', 'loadtest-config-check',
    '--file', path.join(workspace, 'projects/loadtest/compose.yaml'),
    '--file', overrideFile,
  ];
  args.push('--profile', 'benchmark', '--profile', 'ui-gate', '--profile', 'tenant', '--profile', 'maintenance', 'config', '--quiet');
  const result = spawnSync('docker', args, {
    cwd: workspace,
    env: {
      ...process.env,
      LOADTEST_UID: String(process.getuid?.() ?? 10001),
      LOADTEST_GID: String(process.getgid?.() ?? 10001),
      LOADTEST_WORKSPACE: workspace,
      ONELAKE_HOST_PATH: state,
      LOADTEST_SCRATCH_HOST_PATH: path.join(workspace, 'projects/loadtest/.temp'),
      LOADTEST_BIN_HOST_PATH: path.join(workspace, 'target/debug'),
    },
    encoding: 'utf8',
  });
  if (result.error || result.status !== 0) {
    throw new Error(`Compose config failed: ${result.error?.message ?? result.status}\n${result.stdout}\n${result.stderr}`);
  }
}
