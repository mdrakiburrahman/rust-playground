import { type ChildProcess } from 'node:child_process';
import {
  closeSync, existsSync, lstatSync, mkdirSync, openSync, readFileSync, readlinkSync,
  readdirSync, rmSync, statSync, unlinkSync, writeFileSync,
} from 'node:fs';
import { hostname } from 'node:os';
import path from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';

import { BENCHMARK_PROJECT } from './cli.js';
import { requireNoLiveAttemptWriters, requireSealedAttemptEvidence } from './attempt-evidence.js';

type Docker = (args: string[]) => string;

export function stopReceipt(resetGeneration?: string): {
  stopped: true; scope: 'workspace-owned-loadtest'; dataPreserved: true; resetGeneration?: string;
} {
  if (resetGeneration !== undefined && (resetGeneration.length !== 36 ||
      !/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/u.test(resetGeneration))) {
    throw new Error('Stop receipt requires canonical lowercase ARROWCOL_RESET_GENERATION when provided');
  }
  return { stopped: true, scope: 'workspace-owned-loadtest', dataPreserved: true,
    ...(resetGeneration === undefined ? {} : { resetGeneration }) };
}

interface ProcessIdentity {
  pid: number;
  parent: number;
  started: string;
  executable: string;
  arguments: string[];
  cwd: string;
}

function processIdentity(pid: number, relevant?: (args: string[]) => boolean): ProcessIdentity | undefined {
  try {
    const directory = `/proc/${pid}`;
    if (statSync(directory).uid !== process.getuid?.()) return undefined;
    const stat = readFileSync(`${directory}/stat`, 'utf8');
    const fields = stat.slice(stat.lastIndexOf(')') + 2).split(' ');
    if (fields[0] === 'Z') return undefined;
    const args = readFileSync(`${directory}/cmdline`, 'utf8').split('\0').filter(Boolean);
    if (relevant && !relevant(args)) return undefined;
    return {
      pid, parent: Number(fields[1]), started: fields[19],
      executable: readlinkSync(`${directory}/exe`).replace(/ \(deleted\)$/u, ''),
      arguments: args,
      cwd: readlinkSync(`${directory}/cwd`),
    };
  } catch (error) {
    if (['ENOENT', 'ESRCH'].includes((error as NodeJS.ErrnoException).code ?? '')) return undefined;
    throw error;
  }
}

async function waitForExit(identity: ProcessIdentity): Promise<void> {
  const deadline = Date.now() + 120_000;
  while (processIdentity(identity.pid)?.started === identity.started) {
    if (Date.now() >= deadline) throw new Error(`Previous loadtest process ${identity.pid} did not stop; nothing will be reset`);
    await delay(100);
  }
}

function signal(identity: ProcessIdentity, value: NodeJS.Signals): void {
  if (processIdentity(identity.pid)?.started !== identity.started) return;
  process.kill(identity.pid, value);
}

function contained(directory: string, root: string): boolean {
  const relative = path.relative(root, directory);
  return relative === '' || (!relative.startsWith(`..${path.sep}`) && relative !== '..' && !path.isAbsolute(relative));
}

export function checkedBenchmarkPath(workspace: string, target: string): string {
  const resolved = path.resolve(target);
  if (resolved === workspace || !contained(resolved, workspace)) throw new Error('Refusing cleanup outside the workspace');
  let current = workspace;
  for (const component of path.relative(workspace, resolved).split(path.sep)) {
    current = path.join(current, component);
    try {
      if (lstatSync(current).isSymbolicLink()) throw new Error(`Refusing cleanup through a symlink: ${current}`);
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
    }
  }
  return resolved;
}

export interface BenchmarkController {
  checkRunning(): void;
  setChild(child: ChildProcess | undefined): void;
  close(): void;
}

export async function acquireBenchmarkController(workspace: string): Promise<BenchmarkController> {
  const directory = checkedBenchmarkPath(workspace, path.join(workspace, 'projects/loadtest/.temp'));
  mkdirSync(directory, { recursive: true });
  const file = checkedBenchmarkPath(workspace, path.join(directory, 'controller.json'));
  const current = processIdentity(process.pid);
  if (!current) throw new Error('Cannot identify the loadtest controller');
  for (let attempt = 0; ; attempt++) {
    try {
      const fd = openSync(file, 'wx', 0o600);
      try {
        writeFileSync(fd, JSON.stringify({ pid: current.pid, started: current.started }));
      } finally {
        closeSync(fd);
      }
      break;
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'EEXIST') throw error;
      if (attempt >= 2) throw new Error('Another loadtest is starting; retry after it stops');
      let previous: { pid: number; started: string };
      try {
        previous = JSON.parse(readFileSync(file, 'utf8')) as typeof previous;
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code === 'ENOENT') continue;
        throw new Error('Invalid loadtest controller file; inspect it before restarting', { cause: error });
      }
      if (!Number.isSafeInteger(previous.pid) || previous.pid <= 0 || !/^\d+$/u.test(previous.started)) {
        throw new Error('Invalid loadtest controller identity');
      }
      const identity = processIdentity(previous.pid);
      if (identity?.started === previous.started) {
        const main = path.join(workspace, 'projects/loadtest/src/main.ts');
        if (identity.pid === process.pid || !identity.arguments.some(arg => path.resolve(identity.cwd, arg) === main)) {
          throw new Error('Refusing to stop an unrelated process from the controller file');
        }
        signal(identity, 'SIGTERM');
        await waitForExit(identity);
      }
      if (existsSync(file)) {
        const latest = JSON.parse(readFileSync(file, 'utf8')) as typeof previous;
        if (latest.pid !== previous.pid || latest.started !== previous.started) continue;
        unlinkSync(file);
      }
    }
  }
  let stopped = false;
  let child: ChildProcess | undefined;
  const terminate = (): void => {
    stopped = true;
    child?.kill('SIGINT');
  };
  const interrupt = (): void => {
    if (child) child.kill('SIGINT');
    else stopped = true;
  };
  process.on('SIGTERM', terminate);
  process.on('SIGINT', interrupt);
  return {
    checkRunning: () => {
      if (stopped) throw new Error('Loadtest was stopped before startup completed');
    },
    setChild: value => {
      child = value;
      if (stopped) child?.kill('SIGINT');
    },
    close: () => {
      process.removeListener('SIGTERM', terminate);
      process.removeListener('SIGINT', interrupt);
      const owner = JSON.parse(readFileSync(file, 'utf8')) as { pid: number; started: string };
      if (owner.pid !== current.pid || owner.started !== current.started) throw new Error('Loadtest controller ownership changed');
      unlinkSync(file);
    },
  };
}

export function benchmarkDirectories(workspace: string): string[] {
  const root = checkedBenchmarkPath(workspace, path.join(workspace, 'onelake/benchmark'));
  const latest = checkedBenchmarkPath(workspace, path.join(root, 'latest'));
  if (!existsSync(root)) return [];
  return readdirSync(root, { withFileTypes: true }).flatMap(entry => {
    const directory = checkedBenchmarkPath(workspace, path.join(root, entry.name));
    if (!entry.isDirectory()) return [];
    return directory === latest || existsSync(path.join(directory, 'router-state/network.json'))
      || existsSync(path.join(directory, 'results/provenance.json')) ? [directory] : [];
  });
}

async function stopBenchmarkProcesses(workspace: string, directories: string[]): Promise<void> {
  const generators: ProcessIdentity[] = [];
  const samplers: ProcessIdentity[] = [];
  const owners = new Map<number, ProcessIdentity>();
  const main = path.join(workspace, 'projects/loadtest/src/main.ts');
  const sampler = path.join(workspace, 'projects/loadtest/src/resource-sampler.ts');
  for (const entry of readdirSync('/proc')) {
    if (!/^[1-9]\d*$/u.test(entry)) continue;
    const identity = processIdentity(Number(entry), args =>
      (path.basename(args[0] ?? '') === 'arrowcol-loadgen' && ['run', 'validate'].includes(args[1]))
      || args.some(arg => path.basename(arg) === 'resource-sampler.ts'));
    if (!identity || identity.pid === process.pid) continue;
    if (path.basename(identity.executable) === 'arrowcol-loadgen'
        && contained(identity.executable, workspace)
        && ['run', 'validate'].includes(identity.arguments[1])) {
      let values: string;
      try {
        values = readFileSync(`/proc/${identity.pid}/environ`, 'utf8');
      } catch (error) {
        if (['ENOENT', 'ESRCH'].includes((error as NodeJS.ErrnoException).code ?? '')) continue;
        throw error;
      }
      const environment = new Map(values.split('\0').map(value => {
        const separator = value.indexOf('=');
        return [value.slice(0, separator), value.slice(separator + 1)];
      }));
      const results = environment.get('LOADTEST_RESULTS');
      if (!results || !directories.some(directory => contained(path.resolve(identity.cwd, results), directory))) continue;
      generators.push(identity);
      const parent = processIdentity(identity.parent);
      if (parent?.arguments.some(arg => path.resolve(parent.cwd, arg) === main)) owners.set(parent.pid, parent);
      const record = path.join(results, 'continuation.json');
      if (parent && existsSync(record)) {
        const continuation = JSON.parse(readFileSync(record, 'utf8')) as { generatorPid: number; ownerPid: number };
        if (continuation.generatorPid === identity.pid && continuation.ownerPid === parent.pid) owners.set(parent.pid, parent);
      }
    } else if (identity.arguments.some(arg => path.resolve(identity.cwd, arg) === sampler)) {
      const resultsIndex = identity.arguments.indexOf('--results');
      const results = identity.arguments[resultsIndex + 1];
      if (resultsIndex >= 0 && results && directories.some(directory => contained(path.resolve(identity.cwd, results), directory))) {
        samplers.push(identity);
      }
    }
  }
  for (const generator of generators) signal(generator, 'SIGINT');
  for (const generator of generators) await waitForExit(generator);
  for (const owner of owners.values()) await waitForExit(owner);
  for (const sampler of samplers) signal(sampler, 'SIGTERM');
  for (const sampler of samplers) await waitForExit(sampler);
}

interface Resource {
  id: string;
  name: string;
  labels: Record<string, string>;
  containers?: Record<string, { Name: string }>;
}

function resources(docker: Docker, kind: 'container' | 'network' | 'volume'): Resource[] {
  const list = kind === 'container' ? ['ps', '--all'] : [kind, 'ls'];
  const ids = docker([...list, '--quiet', '--filter', 'label=com.docker.compose.project']).trim().split('\n').filter(Boolean);
  const labels = kind === 'container' ? '.Config.Labels' : '.Labels';
  const name = kind === 'volume' ? '.Name' : '.Id';
  const format = `{"id":{{json ${name}}},"name":{{json .Name}},"labels":{{json ${labels}}}${kind === 'network' ? ',"containers":{{json .Containers}}' : ''}}`;
  return ids.map(id => {
    const args = kind === 'container' ? ['inspect'] : [kind, 'inspect'];
    const resource = JSON.parse(docker([...args, '--format', format, id])) as Resource;
    if (typeof resource.id !== 'string' || typeof resource.name !== 'string'
        || !resource.labels || typeof resource.labels !== 'object') throw new Error(`Invalid Docker ${kind} identity`);
    return resource;
  });
}

/** Seconds a cloud mutator gets to drain before Docker kills it; collector drain budgets must fit inside. */
export const MUTATOR_STOP_SECONDS = 300;

/** Stops only verified resources owned by this workspace, retaining data for inspection. */
export async function stopBenchmark(
  workspace: string, docker: Docker, options: { requireCleanMutators?: boolean; mutatorsOnly?: boolean } = {},
): Promise<string[]> {
  if (options.mutatorsOnly && !options.requireCleanMutators) throw new Error('Final validation requires clean mutator drain verification');
  const directories = benchmarkDirectories(workspace);
  const projects = new Set([BENCHMARK_PROJECT, ...directories.map(directory => `rust-playground-loadtest-${path.basename(directory)}`)]);
  const containers = resources(docker, 'container').filter(resource => {
    if (!projects.has(resource.labels['com.docker.compose.project'])) return false;
    if (!resource.labels['com.docker.compose.project.config_files']?.split(',')
      .includes(path.join(workspace, 'projects/loadtest/compose.yaml'))) {
      throw new Error('Refusing to stop a Docker project owned by another workspace');
    }
    return !options.mutatorsOnly ||
      ['direct-1', 'dataflow-1', 'tenant-stream', 'delta-maintenance'].includes(resource.labels['com.docker.compose.service']);
  });
  if (options.mutatorsOnly && ['direct-1', 'dataflow-1'].some(service =>
    !containers.some(container => container.labels['com.docker.compose.service'] === service))) {
    throw new Error('Cannot freeze validation without both positively identified collectors');
  }
  await stopBenchmarkProcesses(workspace, directories);
  if (containers.length) docker(['stop', '--time', options.requireCleanMutators ? String(MUTATOR_STOP_SECONDS) : '30', ...containers.map(container => container.id)]);
  const cleanMutators: string[] = [];
  if (options.requireCleanMutators) {
    for (const container of containers) {
      const service = container.labels['com.docker.compose.service'];
      if (!service) throw new Error('Cannot verify cloud drain without Compose service identity; retain occupancy');
      if (!['direct-1', 'dataflow-1', 'tenant-stream', 'delta-maintenance'].includes(service)) continue;
      const state: unknown = JSON.parse(docker(['inspect', '--format', '{{json .State}}', container.id]));
      if (!state || typeof state !== 'object' || !('Running' in state) || state.Running !== false ||
          !('ExitCode' in state) || state.ExitCode !== 0 || !('OOMKilled' in state) || state.OOMKilled !== false) {
        throw new Error(`Cloud mutator ${service} did not exit cleanly; retain occupancy and publisher registration`);
      }
      cleanMutators.push(service);
    }
  }
  return cleanMutators;
}

export function requirePreviousAttemptEvidence(workspace: string): void {
  for (const directory of benchmarkDirectories(workspace)) requireSealedAttemptEvidence(workspace, directory);
}

export async function resetBenchmark(workspace: string, docker: Docker): Promise<string> {
  const directories = benchmarkDirectories(workspace);
  for (const directory of directories) requireSealedAttemptEvidence(workspace, directory);
  const scratchDirectories = [...new Set(['latest', ...directories.map(directory => path.basename(directory))])]
    .flatMap(name => ['secrets', 'bin', 'loadgen'].map(folder => checkedBenchmarkPath(workspace,
      path.join(workspace, 'projects/loadtest/.temp', folder, name))));
  const projects = new Set([BENCHMARK_PROJECT, ...directories.map(directory => `rust-playground-loadtest-${path.basename(directory)}`)]);
  const containers = resources(docker, 'container').filter(resource => {
    if (!projects.has(resource.labels['com.docker.compose.project'])) return false;
    const files = resource.labels['com.docker.compose.project.config_files']?.split(',') ?? [];
    if (!files.includes(path.join(workspace, 'projects/loadtest/compose.yaml'))) {
      throw new Error('Refusing to reset a Docker project owned by another workspace');
    }
    return true;
  });
  const volumes = resources(docker, 'volume').filter(resource => {
    const project = resource.labels['com.docker.compose.project'];
    const volume = resource.labels['com.docker.compose.volume'];
    if (!projects.has(project) || !['sqlserver-data', 'hive-warehouse', 'kusto-data', 'kusto-bridge-state'].includes(volume)
        || resource.name !== `${project}_${volume}`) return false;
    if (['kusto-data', 'kusto-bridge-state'].includes(volume) && resource.labels['io.arrowcol.loadtest.workspace'] !== workspace) {
      throw new Error('Refusing to reset Kusto storage owned by another or unverifiable workspace');
    }
    return true;
  });
  const networks = resources(docker, 'network').filter(resource =>
    projects.has(resource.labels['com.docker.compose.project'])
      && resource.name === `${resource.labels['com.docker.compose.project']}_default`);
  const ownedIds = new Set(containers.map(container => container.id));
  for (const network of networks) {
    for (const id of Object.keys(network.containers ?? {})) {
      if (!ownedIds.has(id) && !id.startsWith(hostname())) {
        throw new Error(`Another container is using the previous loadtest network ${network.name}`);
      }
    }
  }
  for (const directory of directories) requireNoLiveAttemptWriters(workspace, directory, docker);
  await stopBenchmarkProcesses(workspace, directories);
  if (containers.length) {
    docker(['stop', '--time', '30', ...containers.map(container => container.id)]);
    docker(['rm', ...containers.map(container => container.id)]);
  }
  for (const network of networks) {
    const workspaceId = Object.keys(network.containers ?? {}).find(id => id.startsWith(hostname()));
    if (workspaceId) docker(['network', 'disconnect', network.id, workspaceId]);
    docker(['network', 'rm', network.id]);
  }
  for (const volume of volumes) docker(['volume', 'rm', volume.name]);
  for (const directory of directories) {
    checkedBenchmarkPath(workspace, directory);
    rmSync(directory, { recursive: true });
  }
  for (const scratch of scratchDirectories) {
    checkedBenchmarkPath(workspace, scratch);
    if (existsSync(scratch)) rmSync(scratch, { recursive: true });
  }
  const latest = checkedBenchmarkPath(workspace, path.join(workspace, 'onelake/benchmark/latest'));
  mkdirSync(latest, { recursive: true });
  return latest;
}
