import { createHash } from 'node:crypto';
import { mkdirSync, readFileSync, writeFileSync } from 'node:fs';
import path from 'node:path';

import { BENCHMARK_PROJECT } from '../src/cli.js';
import { beginAttemptEvidence, sealAttemptEvidence, stoppedAttemptMutators } from '../src/attempt-evidence.js';

export function attemptFixture(workspace: string, directory = path.join(workspace, 'onelake/benchmark/latest')) {
  mkdirSync(path.join(directory, 'results'), { recursive: true });
  mkdirSync(path.join(directory, 'router-state'), { recursive: true });
  const helperDirectory = path.join(workspace, 'artifacts/debug');
  mkdirSync(helperDirectory, { recursive: true });
  const host = Object.fromEntries(['arrowcol-loadgen', 'arrowcol-sdk-gen', 'delta-inspect'].map(name => {
    const file = path.join(helperDirectory, name);
    writeFileSync(file, name, { mode: 0o700 });
    const bytes = readFileSync(file);
    return [name, { bytes: bytes.length, sha256: createHash('sha256').update(bytes).digest('hex') }];
  }));
  const head = 'a'.repeat(40);
  writeFileSync(path.join(directory, 'results/provenance.json'), JSON.stringify({ helpers: { host }, source: { head } }));
  writeFileSync(path.join(directory, 'router-state/config.yaml'), 'server:\n  state_directory: /state\n');
  writeFileSync(path.join(workspace, 'Cargo.lock'), 'lock');
  writeFileSync(path.join(workspace, 'package-lock.json'), '{}');
  beginAttemptEvidence(workspace, directory);
  const values = ['direct-1', 'dataflow-1', 'router'].map(service => ({
    id: `${service}-id`, name: service, image: `sha256:${'b'.repeat(64)}`,
    labels: { 'com.docker.compose.project': BENCHMARK_PROJECT, 'com.docker.compose.service': service,
      'com.docker.compose.project.config_files': path.join(workspace, 'projects/loadtest/compose.yaml') },
    state: { Running: service === 'router', ExitCode: 0, OOMKilled: false },
    mounts: service === 'router'
      ? [{ Type: 'bind', Source: path.join(directory, 'router-state'), Destination: '/state', RW: true }] : [],
  }));
  writeFileSync(path.join(directory, 'results/provenance.json'), JSON.stringify({ helpers: { host }, source: { head },
    containers: values.map(value => ({ id: value.id, imageId: value.image, service: value.name })) }));
  const commands: string[][] = [];
  const docker = (args: string[]): string => {
    commands.push(args);
    if (args[0] === 'ps') return values.map(value => value.id).join('\n');
    if (args[1] === 'ls') return '';
    if (args[0] === 'image') return JSON.stringify({ id: `sha256:${'b'.repeat(64)}`, revision: head });
    const value = values.find(value => value.id === args.at(-1));
    if (!value) throw new Error('Unknown fixture container');
    if (args[0] === 'stop') { value.state.Running = false; return ''; }
    if (args[0] === 'inspect') return JSON.stringify(value);
    throw new Error('Unexpected fixture command');
  };
  const stopped = stoppedAttemptMutators(workspace, docker, ['direct-1', 'dataflow-1']);
  return { directory, helperDirectory, commands, docker, values, stopped,
    seal: () => sealAttemptEvidence(workspace, directory, helperDirectory, docker, stopped) };
}
