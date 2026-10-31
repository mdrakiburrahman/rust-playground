import { createHash, randomUUID } from 'node:crypto';
import {
  closeSync, constants, existsSync, fstatSync, fsyncSync, linkSync, lstatSync, mkdirSync,
  openSync, opendirSync, readSync, realpathSync, unlinkSync, writeFileSync, writeSync,
} from 'node:fs';
import path from 'node:path';
import { hostname } from 'node:os';
import { setImmediate } from 'node:timers/promises';

import YAML from 'yaml';

import { BENCHMARK_PROJECT } from './cli.js';
import { checkedBenchmarkPath } from './lifecycle.js';

const markerName = '.attempt-evidence.json';
const socketName = 'router-state/arrowcol-router-upgrade.sock';
const helpers = ['arrowcol-loadgen', 'arrowcol-sdk-gen', 'delta-inspect'] as const;
const imageNames = ['arrowcol-rust', 'arrowcol-dataflow-rust', 'arrowcol-router-rust',
  'kusto-explorer', 'kusto-bridge', 'arrow-tenant-stream', 'delta-maintenance'];
const serviceImages: Record<string, string> = {
  'direct-1': imageNames[0], 'dataflow-1': imageNames[1], router: imageNames[2],
  'kusto-explorer': imageNames[3], 'kusto-bridge': imageNames[4], 'tenant-stream': imageNames[5],
  'delta-maintenance': imageNames[6],
};
const mutators = ['direct-1', 'dataflow-1', 'tenant-stream', 'delta-maintenance'];
const limits = { files: 100_000, bytes: 16 * 1024 ** 3, manifest: 256 * 1024 ** 2, milliseconds: 300_000 };
const digest = (value: string | Buffer): string => createHash('sha256').update(value).digest('hex');
const within = (root: string, value: string): boolean => value === root || value.startsWith(`${root}${path.sep}`);
const fail = (): never => { throw new Error('Attempt evidence is incomplete or unsafe; original data retained, reset refused'); };
type Docker = (args: string[], timeoutMs?: number) => string;
export interface StoppedMutator { id: string; service: string }
interface Container {
  id: string; labels: Record<string, string>; image: string;
  state: { Running: boolean; ExitCode: number; OOMKilled: boolean };
  mounts: Array<{ Type: string; Source: string; Destination: string; RW: boolean }>;
}
interface Marker { version: 1; id: string; workspace: string }
interface Entry {
  source: string; target: string; kind: 'file' | 'directory' | 'socket'; bytes: number;
  dev: number; ino: number; mtime: number; ctime: number; sha256?: string;
}
interface Manifest {
  version: 1; marker: Marker; outcome: 'validated-success'; nativeCachePreserved: false;
  hostDirectory: string;
  entries: Entry[]; images: Array<{ name: string; id: string; revision: string }>;
  writers: { router: string; mutators: StoppedMutator[]; clean: true };
}

function boundedText(file: string, maximum: number): string {
  const fd = openSync(file, constants.O_RDONLY | constants.O_NOFOLLOW);
  try {
    const stat = fstatSync(fd);
    if (!stat.isFile() || !Number.isSafeInteger(stat.size) || stat.size > maximum) fail();
    const data = Buffer.alloc(stat.size);
    let offset = 0;
    while (offset < data.length) {
      const count = readSync(fd, data, offset, data.length - offset, null);
      if (count === 0) fail();
      offset += count;
    }
    const after = fstatSync(fd);
    if (after.size !== stat.size || after.ctimeMs !== stat.ctimeMs || after.mtimeMs !== stat.mtimeMs) fail();
    return data.toString('utf8');
  } finally { closeSync(fd); }
}

function boundedJson(file: string, maximum = limits.manifest): unknown {
  return JSON.parse(boundedText(file, maximum)) as unknown;
}

function privateDirectory(workspace: string, directory: string): void {
  checkedBenchmarkPath(workspace, directory);
  mkdirSync(directory, { recursive: true, mode: 0o700 });
  const stat = lstatSync(directory);
  if (!stat.isDirectory() || stat.uid !== process.getuid?.() || (stat.mode & 0o077)) fail();
}

function privateArchivePath(workspace: string, target: string): void {
  const root = archiveRoot(workspace);
  checkedBenchmarkPath(workspace, target);
  if (!within(root, target)) fail();
  for (let item = target; ; item = path.dirname(item)) {
    const stat = lstatSync(item);
    if (stat.uid !== process.getuid?.() || (stat.mode & 0o077)) fail();
    if (item === root) break;
  }
}

function archiveRoot(workspace: string): string {
  return checkedBenchmarkPath(workspace, path.join(workspace, '.temp/loadtest-attempt-evidence'));
}

function marker(workspace: string, directory: string): Marker {
  checkedBenchmarkPath(workspace, directory);
  const value = boundedJson(path.join(directory, markerName), 4096) as Marker;
  if (value?.version !== 1 || !/^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/u.test(value.id)
      || value.workspace !== digest(realpathSync(workspace))) fail();
  return value;
}

export function beginAttemptEvidence(workspace: string, directory: string): void {
  try {
    checkedBenchmarkPath(workspace, directory);
    const value: Marker = { version: 1, id: randomUUID(), workspace: digest(realpathSync(workspace)) };
    writeFileSync(path.join(directory, markerName), JSON.stringify(value), { flag: 'wx', mode: 0o600 });
  } catch {
    throw new Error('Attempt evidence initialization failed; original data retained, reset blocked');
  }
}

function containers(docker: Docker): Container[] {
  const ids = docker(['ps', '--all', '--quiet']).trim().split('\n').filter(Boolean);
  return ids.map(id => {
    const value = JSON.parse(docker(['inspect', '--format',
      '{"id":{{json .Id}},"labels":{{json .Config.Labels}},"image":{{json .Image}},"state":{{json .State}},"mounts":{{json .Mounts}}}', id])) as Container;
    value.labels ??= {};
    if (!value.id || typeof value.labels !== 'object' || !value.state || !Array.isArray(value.mounts)) fail();
    return value;
  });
}

function owned(workspace: string, value: Container): boolean {
  if (value.labels['com.docker.compose.project'] !== BENCHMARK_PROJECT) return false;
  if (!value.labels['com.docker.compose.project.config_files']?.split(',')
    .includes(path.join(workspace, 'projects/loadtest/compose.yaml'))) fail();
  return true;
}

function clean(value: Container): void {
  if (value.state.Running !== false || value.state.ExitCode !== 0 || value.state.OOMKilled !== false) fail();
}

function noLiveWriters(values: Container[], hostRoots: string[], allowedId?: string): void {
  for (const value of values) {
    if (!value.state.Running || value.id === allowedId || value.id.startsWith(hostname())) continue;
    for (const mount of value.mounts) {
      if (mount.Type === 'bind' && mount.RW && hostRoots.some(root =>
        within(root, path.resolve(mount.Source)) || within(path.resolve(mount.Source), root))) fail();
    }
  }
}

export function requireNoLiveAttemptWriters(workspace: string, directory: string, docker: Docker): void {
  try {
    const attempt = marker(workspace, directory);
    const manifest = boundedJson(path.join(archiveRoot(workspace), attempt.id, 'manifest.json')) as Manifest;
    if (!path.isAbsolute(manifest.hostDirectory)) fail();
    const values = containers(docker);
    const routers = values.filter(value => owned(workspace, value) && value.labels['com.docker.compose.service'] === 'router');
    const source = routers.flatMap(value => value.mounts.filter(mount => mount.Type === 'bind' && mount.Destination === '/state'))
      .map(mount => path.dirname(mount.Source));
    noLiveWriters(values, [directory, manifest.hostDirectory, ...source]);
  } catch { fail(); }
}

export function stoppedAttemptMutators(workspace: string, docker: Docker, services: string[]): StoppedMutator[] {
  try {
    const selected = containers(docker).filter(value => owned(workspace, value)
      && mutators.includes(value.labels['com.docker.compose.service']));
    if (['direct-1', 'dataflow-1'].some(service => !selected.some(value => value.labels['com.docker.compose.service'] === service))
        || selected.length !== services.length) fail();
    return selected.map(value => {
      clean(value);
      const service = value.labels['com.docker.compose.service'];
      if (!services.includes(service)) fail();
      return { id: value.id, service };
    });
  } catch { return fail(); }
}

/** This is after qualification, never inside the measured or native validation window. */
export function stopAttemptRouter(
  workspace: string, directory: string, docker: Docker, expected: StoppedMutator[], hostDirectory = directory,
): { router: string; mutators: StoppedMutator[]; clean: true } {
  const before = containers(docker);
  const selected = before.filter(value => owned(workspace, value));
  const actual = selected.filter(value => mutators.includes(value.labels['com.docker.compose.service']));
  if (actual.length !== expected.length || !expected.some(value => value.service === 'direct-1')
      || !expected.some(value => value.service === 'dataflow-1')) fail();
  for (const value of actual) {
    if (!expected.some(previous => previous.id === value.id && previous.service === value.labels['com.docker.compose.service'])) fail();
    clean(value);
  }
  const routers = selected.filter(value => value.labels['com.docker.compose.service'] === 'router');
  if (routers.length !== 1) fail();
  const router = routers[0];
  const hostWorkspace = path.resolve(hostDirectory, path.relative(directory, workspace));
  const hostRoots = [hostDirectory, path.join(hostWorkspace, 'projects/loadtest/.temp/loadgen/latest/diagnostics')];
  const config = YAML.parse(boundedText(checkedBenchmarkPath(workspace, path.join(directory, 'router-state/config.yaml')), 16 * 1024)) as {
    server?: { state_directory?: string };
  };
  if (config?.server?.state_directory !== '/state'
      || !router.mounts.some(mount => mount.Type === 'bind' && mount.Destination === '/state' && mount.RW
        && path.resolve(mount.Source) === path.join(hostDirectory, 'router-state'))) fail();
  noLiveWriters(before, hostRoots, router.id);
  docker(['stop', '--time', '300', router.id]);
  const after = containers(docker);
  const stopped = after.find(value => value.id === router.id);
  if (!stopped || !owned(workspace, stopped)) return fail();
  clean(stopped);
  const afterMutators = after.filter(value => owned(workspace, value) && mutators.includes(value.labels['com.docker.compose.service']));
  if (afterMutators.length !== expected.length) fail();
  for (const value of afterMutators) {
    if (!expected.some(previous => previous.id === value.id && previous.service === value.labels['com.docker.compose.service'])) fail();
    clean(value);
  }
  noLiveWriters(after, hostRoots);
  return { router: router.id, mutators: expected, clean: true };
}

function stamp(file: string, target: string, allowSocket: boolean): Entry {
  const stat = lstatSync(file);
  if (allowSocket && !stat.isSocket()) fail();
  const kind = stat.isFile() ? 'file' : stat.isDirectory() ? 'directory' : allowSocket && stat.isSocket() ? 'socket' : fail();
  if (!Number.isSafeInteger(stat.size)) fail();
  return { source: file, target, kind, bytes: kind === 'file' ? stat.size : 0,
    dev: stat.dev, ino: stat.ino, mtime: stat.mtimeMs, ctime: stat.ctimeMs };
}

function budget(signal?: AbortSignal, checkRunning?: () => void): { check: () => void; remaining: () => number } {
  const deadline = Date.now() + limits.milliseconds;
  return { check: () => { checkRunning?.(); if (signal?.aborted || Date.now() >= deadline) fail(); },
    remaining: () => Math.max(1, deadline - Date.now()) };
}

function inventory(workspace: string, directory: string, check: () => void): Entry[] {
  const entries: Entry[] = [];
  let bytes = 0, manifestBytes = 0;
  const visit = (root: string, target: string): void => {
    check();
    checkedBenchmarkPath(workspace, root);
    if (/^\.env(?:\.|$)|(?:password|credential|private-key)|\.(?:pem|key)$/iu.test(path.basename(root))
        || target.startsWith('corpus/azure-cli/')) fail();
    // The router's one configured endpoint is metadata, not a copied database or socket.
    const entry = stamp(root, target, target === `corpus/${socketName}`);
    entries.push(entry); bytes += entry.bytes;
    manifestBytes += Buffer.byteLength(JSON.stringify(entry)) + 100;
    if (entries.length > limits.files || bytes > limits.bytes || manifestBytes > limits.manifest - 1024 * 1024) fail();
    if (entry.kind === 'directory') {
      const dir = opendirSync(root);
      try {
        let child;
        while ((child = dir.readSync()) !== null) visit(path.join(root, child.name), path.posix.join(target, child.name));
      } finally { dir.closeSync(); }
    }
  };
  visit(directory, 'corpus');
  return entries.sort((a, b) => a.target.localeCompare(b.target));
}

function* transfer(entry: Entry, check: () => void, destination?: string): Generator<void, string> {
  if (realpathSync(entry.source) !== entry.source) fail();
  const source = openSync(entry.source, constants.O_RDONLY | constants.O_NOFOLLOW);
  let output: number | undefined;
  try {
    const stat = fstatSync(source);
    if (!stat.isFile() || stat.dev !== entry.dev || stat.ino !== entry.ino || stat.size !== entry.bytes
        || stat.mtimeMs !== entry.mtime || stat.ctimeMs !== entry.ctime) fail();
    if (destination) output = openSync(destination, constants.O_CREAT | constants.O_EXCL | constants.O_WRONLY | constants.O_NOFOLLOW, 0o600);
    const hash = createHash('sha256'), buffer = Buffer.allocUnsafe(1024 * 1024);
    let total = 0, count: number;
    while ((count = readSync(source, buffer, 0, buffer.length, null)) !== 0) {
      check(); total += count;
      if (total > entry.bytes) fail();
      hash.update(buffer.subarray(0, count));
      if (output !== undefined) {
        let written = 0;
        while (written < count) {
          const value = writeSync(output, buffer, written, count - written);
          if (value <= 0) fail();
          written += value;
        }
      }
      yield;
    }
    const after = fstatSync(source);
    if (total !== entry.bytes || after.size !== stat.size || after.mtimeMs !== stat.mtimeMs || after.ctimeMs !== stat.ctimeMs) fail();
    const current = lstatSync(entry.source);
    if (current.dev !== stat.dev || current.ino !== stat.ino || realpathSync(entry.source) !== entry.source) fail();
    if (output !== undefined) fsyncSync(output);
    return hash.digest('hex');
  } finally { closeSync(source); if (output !== undefined) closeSync(output); }
}

function hashFile(entry: Entry, check: () => void): string {
  const iterator = transfer(entry, check);
  let value = iterator.next();
  while (!value.done) value = iterator.next();
  return value.value;
}

async function copyOrHash(entry: Entry, check: () => void, destination?: string): Promise<string> {
  const iterator = transfer(entry, check, destination);
  try {
    let value = iterator.next();
    while (!value.done) {
      await setImmediate();
      check();
      value = iterator.next();
    }
    return value.value;
  } finally { iterator.return(''); }
}

function syncDirectory(directory: string): void {
  const fd = openSync(directory, constants.O_RDONLY | constants.O_NOFOLLOW);
  try { fsyncSync(fd); } finally { closeSync(fd); }
}

function identity(file: string): Entry {
  // Only the three explicitly selected, provenance-bound helpers may be outside the workspace.
  const resolved = realpathSync(file);
  if (lstatSync(file).isSymbolicLink()) fail();
  return stamp(resolved, `helpers/${path.basename(file)}`, false);
}

function receipt(workspace: string, directory: string): {
  helpers: { host: Record<string, { bytes: number; sha256: string }> }; source: { head: string };
  containers: Array<{ id: string; service: string; imageId: string }>;
} {
  return boundedJson(checkedBenchmarkPath(workspace, path.join(directory, 'results/provenance.json'))) as ReturnType<typeof receipt>;
}

/** Private artifact custody only. This never exports or claims a consistent Kusto named volume. */
export async function sealAttemptEvidence(
  workspace: string, directory: string, helperDirectory: string, docker: Docker, stopped: StoppedMutator[],
  hostDirectory = directory, signal?: AbortSignal, checkRunning?: () => void,
): Promise<void> {
  let sealPath: string | undefined;
  let phase = 'preflight';
  try {
    const { check, remaining } = budget(signal, checkRunning);
    const boundedDocker = (args: string[]): string => { check(); return docker(args, remaining()); };
    const attempt = marker(workspace, directory);
    const provenance = receipt(workspace, directory);
    if (!/^[a-f0-9]{40}$/u.test(provenance.source?.head)) fail();
    check();
    phase = 'router-stop';
    const writers = stopAttemptRouter(workspace, directory, boundedDocker, stopped, hostDirectory);
    check();
    const root = archiveRoot(workspace);
    privateDirectory(workspace, root);
    const destination = checkedBenchmarkPath(workspace, path.join(root, attempt.id));
    mkdirSync(destination, { mode: 0o700 }); // A partial archive is never overwritten or retried.
    sealPath = path.join(destination, 'seal.json');
    phase = 'inventory';
    const entries = inventory(workspace, directory, check);
    for (const name of helpers) entries.push(identity(path.join(helperDirectory, name)));
    for (const name of ['Cargo.lock', 'package-lock.json']) {
      entries.push(stamp(checkedBenchmarkPath(workspace, path.join(workspace, name)), `source/${name}`, false));
    }
    const diagnostics = checkedBenchmarkPath(workspace, path.join(workspace, 'projects/loadtest/.temp/loadgen/latest/diagnostics'));
    if (existsSync(diagnostics)) {
      const dir = opendirSync(diagnostics);
      try {
        let child;
        while ((child = dir.readSync()) !== null) {
          check();
          const name = child.name;
          if (!/^stress-private-[a-zA-Z0-9-]+$/u.test(name) || !child.isDirectory()) fail();
          const file = checkedBenchmarkPath(workspace, path.join(diagnostics, name, 'reader-stderr.private.log'));
          if (existsSync(file)) entries.push(stamp(file, `diagnostics/${name}/reader-stderr.private.log`, false));
          if (entries.length > limits.files) fail();
        }
      } finally { dir.closeSync(); }
    }
    if (entries.length > limits.files || entries.reduce((sum, entry) => sum + entry.bytes, 0) > limits.bytes) fail();
    const images = imageNames.map(name => {
      check();
      const value = JSON.parse(boundedDocker(['image', 'inspect', '--format',
        '{"id":{{json .Id}},"revision":{{json (index .Config.Labels "org.opencontainers.image.revision")}}}',
        `ghcr.io/mdrakiburrahman/rust-playground/${name}:dev`])) as { id: string; revision: string };
      if (!/^sha256:[a-f0-9]{64}$/u.test(value.id) || value.revision !== provenance.source.head) fail();
      return { name, ...value };
    });
    if (!Array.isArray(provenance.containers)) fail();
    for (const value of containers(boundedDocker).filter(value => owned(workspace, value))) {
      const service = value.labels['com.docker.compose.service'];
      const selected = serviceImages[service];
      if (!selected) continue;
      const image = images.find(image => image.name === selected);
      if (!image || image.id !== value.image
          || !provenance.containers.some(record => record.service === service && record.imageId === value.image)) fail();
    }
    phase = 'copy';
    const directories = new Set([destination]);
    for (const entry of entries) {
      check();
      if (entry.kind === 'socket') continue;
      const output = checkedBenchmarkPath(workspace, path.join(destination, entry.target));
      if (entry.kind === 'directory') {
        mkdirSync(output, { recursive: true, mode: 0o700 });
        directories.add(output);
        continue;
      }
      let parent = path.dirname(output);
      while (parent !== destination) { directories.add(parent); parent = path.dirname(parent); }
      mkdirSync(path.dirname(output), { recursive: true, mode: 0o700 });
      entry.sha256 = await copyOrHash(entry, check, output);
      const copied = stamp(output, entry.target, false);
      if (await copyOrHash(copied, check) !== entry.sha256) fail();
      if (entry.target.startsWith('helpers/')) {
        const expected = provenance.helpers?.host[path.basename(entry.source)];
        if (!expected || entry.sha256 !== expected.sha256 || entry.bytes !== expected.bytes) fail();
      }
    }
    const current = inventory(workspace, directory, check);
    const original = entries.filter(entry => entry.target === 'corpus' || entry.target.startsWith('corpus/'))
      .map(({ sha256: _, ...entry }) => entry);
    if (JSON.stringify(current) !== JSON.stringify(original)) fail();
    for (const entry of entries) if (entry.kind === 'file' && await copyOrHash(entry, check) !== entry.sha256) fail();
    phase = 'durability';
    const manifest: Manifest = { version: 1, marker: attempt, outcome: 'validated-success', nativeCachePreserved: false,
      hostDirectory, entries, images, writers };
    const data = JSON.stringify(manifest);
    if (Buffer.byteLength(data) > limits.manifest) fail();
    const pending = path.join(destination, 'manifest.pending');
    const fd = openSync(pending, 'wx', 0o600);
    try { writeFileSync(fd, data); fsyncSync(fd); } finally { closeSync(fd); }
    for (const item of [...directories].sort((a, b) => b.length - a.length)) syncDirectory(item);
    check();
    linkSync(pending, path.join(destination, 'manifest.json'));
    syncDirectory(destination);
    unlinkSync(pending);
    syncDirectory(destination); syncDirectory(root);
    syncDirectory(path.dirname(root)); syncDirectory(workspace);
    const seal = path.join(destination, 'seal.pending');
    const sealFd = openSync(seal, 'wx', 0o600);
    try { writeFileSync(sealFd, JSON.stringify({ manifestSha256: digest(data) })); fsyncSync(sealFd); }
    finally { closeSync(sealFd); }
    check();
    linkSync(seal, sealPath);
    syncDirectory(destination);
  } catch {
    try { if (sealPath && existsSync(sealPath)) unlinkSync(sealPath); }
    catch { throw new Error('Attempt evidence seal revocation failed; original data retained, operator inspection required'); }
    throw new Error(`Attempt evidence seal failed during ${phase}; private partial evidence and original data retained, reset blocked`);
  }
}

/** Runs before any process signal, Docker mutation or filesystem deletion. */
export function requireSealedAttemptEvidence(workspace: string, directory: string): void {
  try {
    const { check } = budget();
    const attempt = marker(workspace, directory);
    const destination = checkedBenchmarkPath(workspace, path.join(archiveRoot(workspace), attempt.id));
    privateArchivePath(workspace, destination);
    privateArchivePath(workspace, path.join(destination, 'seal.json'));
    const seal = boundedJson(path.join(destination, 'seal.json'), 4096) as { manifestSha256?: string };
    const manifestFile = checkedBenchmarkPath(workspace, path.join(destination, 'manifest.json'));
    privateArchivePath(workspace, manifestFile);
    if (seal.manifestSha256 !== hashFile(stamp(manifestFile, 'manifest.json', false), check)) fail();
    const manifest = boundedJson(manifestFile) as Manifest;
    if (manifest.version !== 1 || manifest.outcome !== 'validated-success' || manifest.nativeCachePreserved !== false
        || JSON.stringify(manifest.marker) !== JSON.stringify(attempt) || !Array.isArray(manifest.entries)
        || manifest.entries.length > limits.files
        || manifest.entries.reduce((sum, entry) => sum + entry.bytes, 0) > limits.bytes) fail();
    const entries = inventory(workspace, directory, check);
    const archived = manifest.entries.filter(entry => entry.target === 'corpus' || entry.target.startsWith('corpus/'));
    if (JSON.stringify(entries) !== JSON.stringify(archived.map(({ sha256: _, ...entry }) => entry))) fail();
    for (const entry of manifest.entries) {
      check();
      if (entry.kind === 'socket') {
        if (entry.target !== `corpus/${socketName}`) fail();
        continue;
      }
      if (entry.kind === 'directory') {
        const output = checkedBenchmarkPath(workspace, path.join(destination, entry.target));
        privateArchivePath(workspace, output);
        if (!within(destination, output) || !lstatSync(output).isDirectory()) fail();
        continue;
      }
      if (entry.kind !== 'file' || !/^[a-f0-9]{64}$/u.test(entry.sha256 ?? '')) fail();
      const output = checkedBenchmarkPath(workspace, path.join(destination, entry.target));
      privateArchivePath(workspace, output);
      if (!within(destination, output)) fail();
      if (hashFile(stamp(output, entry.target, false), check) !== entry.sha256) fail();
      if (entry.target.startsWith('corpus/') && hashFile(entry, check) !== entry.sha256) fail();
    }
  } catch {
    fail();
  }
}
