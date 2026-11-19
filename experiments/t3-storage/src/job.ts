import { spawn, spawnSync, execFileSync } from 'node:child_process';
import { createHash } from 'node:crypto';
import { createWriteStream, type Stats } from 'node:fs';
import { mkdir, readFile, writeFile, access, readdir, stat, open } from 'node:fs/promises';
import path from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { parseArgs } from 'node:util';

export type CacheProfile = 'default' | 'none' | 'positive' | 'recommended';
export interface Request {
  id: string;
  mode: 'smoke' | 'performance' | 'blob' | 'phase-a' | 'writer' | 'recover' | 'collect' | 'fault-a' | 'fault-b';
  stagingEndpoint: string;
  nfsSource: string;
  cache: CacheProfile;
  binarySha256: string;
  args?: string[];
  seconds?: number;
  intervals?: number[];
  sizesMiB?: number[];
  endpoint?: string;
  blobMode?: 'block' | 'append';
  local?: boolean;
  pairId?: string;
  scenario?: 'kill' | 'pause' | 'partition' | 'power';
  vm?: string;
  cores?: number;
  sourceArchiveSha256?: string;
  quiverCommit?: string;
  intervalMs?: number;
  restartBeforeDrain?: boolean;
  trace?: boolean;
}

export function mountOptions(profile: CacheProfile): string {
  const cache = { default: [], none: ['lookupcache=none'], positive: ['lookupcache=positive', 'actimeo=1'], recommended: ['actimeo=30'] }[profile];
  if (!cache) throw new Error('Unknown cache profile.');
  return ['vers=4', 'minorversion=1', 'sec=sys', 'nconnect=4', 'rsize=1048576', 'wsize=1048576', ...cache].join(',');
}

export async function directoryListing(directory: string, inspect: (file: string) => Promise<Stats> = stat) {
  const entries = await readdir(directory);
  return Promise.all(entries.sort().map(async name => {
    try {
      const info = await inspect(path.join(directory, name));
      return { name, bytes: info.size, mode: info.mode & 0o777, modifiedAt: info.mtime.toISOString() };
    } catch (error) {
      if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error;
      return { name, disappeared: true as const };
    }
  }));
}

let cachedToken: { value: string; until: number } | undefined;
async function token(): Promise<string> {
  if (cachedToken && cachedToken.until > Date.now()) return cachedToken.value;
  const response = await fetch('http://169.254.169.254/metadata/identity/oauth2/token?api-version=2018-02-01&resource=https%3A%2F%2Fstorage.azure.com%2F', { headers: { Metadata: 'true' }, signal: AbortSignal.timeout(10_000) });
  if (!response.ok) throw new Error(`Managed identity token failed: ${response.status}`);
  const result = await response.json() as { access_token: string };
  cachedToken = { value: result.access_token, until: Date.now() + 300_000 };
  return result.access_token;
}

async function upload(endpoint: string, name: string, data: Buffer | string): Promise<void> {
  const bytes = typeof data === 'string' ? Buffer.from(data) : data;
  const response = await fetch(`${endpoint}spike/${name}`, {
    method: 'PUT', body: Uint8Array.from(bytes),
    headers: { Authorization: `Bearer ${await token()}`, 'x-ms-version': '2021-12-02', 'x-ms-date': new Date().toUTCString(), 'x-ms-blob-type': 'BlockBlob', 'Content-MD5': createHash('md5').update(bytes).digest('base64') },
    signal: AbortSignal.timeout(120_000),
  });
  if (!response.ok) throw new Error(`Evidence upload failed: ${response.status}, ${(await response.text()).slice(0, 512)}`);
}

async function command(program: string, args: string[], file: string, onLine?: (line: string) => void): Promise<void> {
  await new Promise<void>((resolve, reject) => {
    const log = createWriteStream(file, { flags: 'wx' });
    const child = spawn(program, args, { stdio: ['ignore', 'pipe', 'pipe'] });
    let tail = '';
    let pending = '';
    child.stdout.on('data', (b: Buffer) => {
      log.write(b);
      pending += b.toString();
      const lines = pending.split('\n');
      pending = lines.pop()!.slice(-16_384);
      for (const line of lines) onLine?.(line);
    });
    child.stderr.on('data', (b: Buffer) => { log.write(b); tail = (tail + b.toString()).slice(-8192); });
    log.on('error', error => { if (child.pid) process.kill(child.pid, 'SIGTERM'); reject(error); });
    child.on('error', reject);
    child.on('close', (code, signal) => log.end(() => code === 0 ? resolve() : reject(Object.assign(new Error(`${program} exited ${code ?? signal}: ${tail}`), { exitCode: code, signal }))));
  });
}

async function waitFor<T>(check: () => Promise<T | null>, timeoutMs = 300_000): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const result = await check();
    if (result !== null) return result;
    await delay(50);
  }
  throw new Error('Coordination deadline exceeded.');
}

async function fault(request: Request, out: string, binary: string): Promise<void> {
  const pair = request.pairId;
  if (!pair || !/^[a-z0-9-]{1,80}$/u.test(pair) || !request.scenario) throw new Error('Fault pair and scenario required.');
  const interval = request.intervalMs ?? 5;
  if (!Number.isSafeInteger(interval) || interval < 0 || interval > 1000) throw new Error('Invalid fault WAL interval.');
  if (request.restartBeforeDrain && request.scenario !== 'pause') throw new Error('Pre-drain restart requires a pause scenario.');
  const control = `control/${pair}`;
  const put = async (name: string, value: unknown) => upload(request.stagingEndpoint, `${control}/${name}.json`, JSON.stringify(value));
  const get = async (name: string): Promise<Record<string, unknown> | null> => {
    const response = await fetch(`${request.stagingEndpoint}spike/${control}/${name}.json`, { headers: { Authorization: `Bearer ${await token()}`, 'x-ms-version': '2021-12-02', 'x-ms-date': new Date().toUTCString() }, signal: AbortSignal.timeout(10_000) });
    if (response.status === 404) return null;
    if (!response.ok) throw new Error(`Control read failed: ${response.status}`);
    return await response.json() as Record<string, unknown>;
  };
  const controlState = async (name: string) => {
    const abort = await get('abort');
    if (abort) throw new Error(`Other actor aborted: ${String(abort.error)}`);
    return get(name);
  };
  const data = `/mnt/storage-spike/crash-${pair}`;
  const oracle = `${out}/oracle.jsonl`;
  const log = `${out}/engine.jsonl`;
  const save = async (name: string) => upload(request.stagingEndpoint, `results/${request.id}/${name}`, await readFile(`${out}/${name}`));
  const engine = (args: string[], onLine?: (line: string) => void, label = 'engine') => request.trace
    ? command('strace', ['-f', '-ttt', '-T', '-y', '-s', '256', '-e', 'trace=openat,openat2,close,statx,newfstatat,fsync,fdatasync,renameat2,link,linkat,unlink,unlinkat,write,ftruncate,flock', '-o', `${out}/${label === 'engine' ? 'syscalls' : `${label}-syscalls`}.log`, binary, ...args], `${out}/${label}.jsonl`, onLine)
    : command(binary, args, `${out}/${label}.jsonl`, onLine);
  try {
    if (request.mode === 'fault-a') {
      const barrier = `${out}/continue`;
      let pid: number | undefined;
      let writerOutcome: string | null | undefined;
      let writerExitCode: number | null = null;
      const resumeArgs = request.scenario === 'pause' || request.scenario === 'partition' ? ['--barrier-before-ingest'] : [];
      const writer = engine(['write', '--dir', data, '--run', pair, '--oracle', oracle, '--barrier', barrier, '--seconds', '600', '--interval-ms', String(interval), '--fence-samples', request.trace ? '0' : '1000', '--warmup-seconds', request.cache === 'default' ? '55' : '2', ...resumeArgs], line => {
        try { const record = JSON.parse(line) as { kind?: string; pid?: number }; if (record.kind === 'ready') pid = record.pid; } catch { /* Diagnostics remain in the file. */ }
      }).then(() => { writerExitCode = 0; writerOutcome = null; return null; }, (error: Error & { exitCode?: number | null }) => { writerExitCode = error.exitCode ?? null; writerOutcome = String(error); return writerOutcome; });
      await waitFor(async () => {
        if (writerOutcome !== undefined) throw new Error(writerOutcome ?? 'Writer exited before its barrier.');
        try { await access(`${barrier}.ready`); return true; } catch { return null; }
      });
      if (!pid || !Number.isSafeInteger(pid) || pid <= 1) throw new Error('Writer did not report its PID.');
      const writerPid = pid;
      const signal = (value: NodeJS.Signals) => {
        try { process.kill(writerPid, value); }
        catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ESRCH') throw error; }
      };
      await put('ready', { oracle: await readFile(oracle, 'utf8'), ownerMarker: await readFile(`${data}/quiver.owner`, 'utf8'), bootId: (await readFile('/proc/sys/kernel/random/boot_id', 'utf8')).trim(), pid });
      const at = Date.now();
      let heal: (() => void) | undefined;
      if (request.scenario === 'pause') signal('SIGSTOP');
      if (request.scenario === 'kill') signal('SIGKILL');
      if (request.scenario === 'partition') {
        const host = request.nfsSource.split(':')[0];
        const ip = execFileSync('getent', ['ahostsv4', host], { encoding: 'utf8' }).trim().split(/\s/u)[0];
        if (!/^10\.88\.0\.\d+$/u.test(ip)) throw new Error('Files endpoint did not resolve inside this lab.');
        const rule = ['OUTPUT', '-d', ip, '-m', 'comment', '--comment', `storage-spike-${pair}`, '-j', 'DROP'];
        execFileSync('sudo', ['iptables', '-I', ...rule]);
        heal = () => { execFileSync('sudo', ['iptables', '-D', ...rule]); };
        await writeFile(barrier, 'continue\n');
      }
      await put('fault', { at, scenario: request.scenario, timestampKind: request.scenario === 'power' ? 'before-abrupt-reboot' : 'signal-or-rule-bracket-start' });
      if (request.scenario === 'power') {
        execFileSync('sudo', ['sysctl', '-w', 'kernel.sysrq=1']);
        execFileSync('sudo', ['/bin/sh', '-c', 'echo b > /proc/sysrq-trigger']);
        throw new Error('Abrupt reboot command unexpectedly returned.');
      }
      try {
        let forcedTermination = false;
        if (request.scenario !== 'kill') {
          await waitFor(async () => {
            if (writerOutcome !== undefined) throw new Error(writerOutcome ?? 'Writer exited before the replacement claimed.');
            return controlState('claimed');
          });
          if (heal) { heal(); heal = undefined; }
          if (request.scenario === 'pause') {
            await writeFile(barrier, 'continue\n');
            signal('SIGCONT');
          }
          const outcome = await Promise.race([writer.then(value => ({ value })), delay(request.scenario === 'partition' ? 120_000 : 10_000).then(() => null)]);
          if (outcome === null) { forcedTermination = true; signal('SIGKILL'); }
        }
        const outcome = await writer;
        const events = (await readFile(log, 'utf8')).split('\n').flatMap(line => { try { return [JSON.parse(line) as Record<string, unknown>]; } catch { return []; } });
        const segments = await directoryListing(`${data}/segments`);
        await writeFile(`${out}/segments-after-writer.json`, JSON.stringify(segments, null, 2));
        await put('finished', { oracle: await readFile(oracle, 'utf8'), outcome, exitCode: writerExitCode, forcedTermination, fenced: events.some(e => e.kind === 'error' && e.fenced === true), segments, events });
      } finally { heal?.(); }
    } else {
      const ready = await waitFor(() => controlState('ready'));
      await writeFile(oracle, String(ready.oracle));
      const observed = `${out}/observed.json`;
      const release = `${out}/release`;
      const beforeDrain = `${out}/before-drain`;
      let claim: Record<string, unknown> | undefined;
      let claimSaved: Promise<void> | undefined;
      let claimSaveError: unknown;
      let recoveryOutcome: string | null | undefined;
      let pausedPid: number | undefined;
      const recoveryArgs = ['recover', '--dir', data, '--run', pair, '--oracle', oracle, '--interval-ms', String(interval), '--retry-seconds', '180'];
      const recovery = engine([...recoveryArgs, '--observed', request.restartBeforeDrain ? `${out}/pre-drain-observed.json` : observed, ...(request.restartBeforeDrain ? ['--pause-before-drain', beforeDrain] : ['--hold-open-seconds', '180', '--hold-open-until', release])], line => {
        try {
          const event = JSON.parse(line) as Record<string, unknown>;
          if (event.kind === 'claim') {
            claim = event;
            if (!request.restartBeforeDrain) claimSaved = put('claimed', event).catch(error => { claimSaveError = error; });
          }
          if (request.restartBeforeDrain && event.kind === 'barrier' && event.path === beforeDrain && claim) {
            pausedPid = Number(event.pid);
            claimSaved = put('claimed', { ...claim, pausedBeforeDrain: true }).catch(error => { claimSaveError = error; });
          }
        } catch { /* Diagnostics remain in the file. */ }
      }, request.restartBeforeDrain ? 'pre-drain' : 'engine').then(() => { recoveryOutcome = null; return null; }, error => { recoveryOutcome = String(error); return recoveryOutcome; });
      const finish = request.scenario === 'power' ? ready : await waitFor(async () => {
        if (recoveryOutcome !== undefined) throw new Error(recoveryOutcome ?? 'Recovery exited before the original writer finished.');
        if (claimSaveError) throw claimSaveError;
        return controlState('finished');
      });
      await writeFile(oracle, String(finish.oracle));
      await claimSaved;
      if (claimSaveError) throw claimSaveError;
      if (request.restartBeforeDrain) {
        if (!pausedPid || !Number.isSafeInteger(pausedPid) || pausedPid <= 1) throw new Error('Recovery did not pause before its drain.');
        const before = await directoryListing(`${data}/wal`);
        const rotated = before.filter(entry => !entry.disappeared && /^quiver\.wal\.\d+$/u.test(entry.name));
        if (rotated.length === 0) throw new Error('Pre-drain restart is untested: moved WAL already absent.');
        try { await access(`${out}/pre-drain-observed.json`); throw new Error('Recovery drained before its restart gate.'); }
        catch (error) { if ((error as NodeJS.ErrnoException).code !== 'ENOENT') throw error; }
        const segments = await directoryListing(`${data}/segments`);
        await writeFile(`${out}/segments-before-restart.json`, JSON.stringify(segments, null, 2));
        const restart: Record<string, unknown> = { pausedPid, before, segments, observedBeforeRestart: false, stoppedWith: 'SIGKILL', at: new Date().toISOString() };
        await writeFile(`${out}/restart.json`, JSON.stringify(restart, null, 2));
        process.kill(pausedPid, 'SIGKILL');
        const stopped = await recovery;
        if (stopped === null) throw new Error('Pre-drain recovery exited normally instead of being terminated.');
        const afterKill = await directoryListing(`${data}/wal`);
        restart.afterKill = afterKill;
        restart.initialRecoveryOutcome = stopped;
        await writeFile(`${out}/restart.json`, JSON.stringify(restart, null, 2));
        if (rotated.some(entry => !afterKill.some(current => !current.disappeared && current.name === entry.name))) throw new Error('Moved WAL disappeared before the restart.');
        try {
          await engine([...recoveryArgs, '--observed', observed]);
          restart.result = 'passed';
        } catch (error) { restart.result = 'failed'; restart.error = String(error); throw error; }
        finally { await writeFile(`${out}/restart.json`, JSON.stringify(restart, null, 2)); }
      } else {
        await writeFile(release, 'release\n');
        const recoveryError = await recovery;
        if (recoveryError) throw new Error(recoveryError);
      }
      const faultEvent = await waitFor(() => controlState('fault'));
      const ids = (text: unknown): number[] => String(text).split('\n').slice(0, -1).flatMap(line => (JSON.parse(line) as { ids: number[] }).ids);
      const originalIds = new Set(ids(ready.oracle));
      const lateAcks = ids(finish.oracle).filter(id => !originalIds.has(id));
      if (!claim || !Number.isFinite(Number(claim.at_unix_ms))) throw new Error('Claim timestamp was not observed.');
      await writeFile(`${out}/takeover.json`, JSON.stringify({ claim, fault: faultEvent, takeoverMs: Number(claim.at_unix_ms) - Number(faultEvent.at), previousOwnerFenced: finish.fenced, forcedTermination: finish.forcedTermination, postClaimAcknowledgedIds: lateAcks, readyBootId: ready.bootId }, null, 2));
      await writeFile(`${out}/control-ready.json`, JSON.stringify(ready));
      await writeFile(`${out}/control-fault.json`, JSON.stringify(faultEvent));
      await writeFile(`${out}/original-writer-finished.json`, JSON.stringify(finish));
      await command(binary, ['verify-oracle', '--run', pair, '--oracle', oracle, '--observed', observed], `${out}/verification.jsonl`);
      await save('verification.jsonl');
      await save('takeover.json');
      if (lateAcks.length > 0) throw new Error('FENCING CONTRACT VIOLATION: original owner acknowledged new ingests after the replacement claimed the directory.');
      if (finish.forcedTermination) throw new Error('INCONCLUSIVE: original writer required forced termination after the fault was healed.');
      if (request.scenario === 'pause' && (finish.fenced !== true || finish.exitCode !== 20)) throw new Error('Original writer did not exit fenced with code 20.');
    }
  } catch (error) {
    try { await put('abort', { actor: request.id, error: String(error) }); } catch { /* Local status still records the original failure. */ }
    throw error;
  } finally {
    for (const name of ['oracle.jsonl', 'engine.jsonl', 'observed.json', 'verification.jsonl', 'takeover.json', 'syscalls.log', 'control-ready.json', 'control-fault.json', 'original-writer-finished.json', 'pre-drain.jsonl', 'pre-drain-syscalls.log', 'restart.json', 'segments-after-writer.json', 'segments-before-restart.json']) {
      try { await save(name); } catch { /* Preserve every available artifact even when the fault kills a guest. */ }
    }
  }
}

async function run(request: Request): Promise<void> {
  if (!/^[a-z0-9-]{1,100}$/u.test(request.id)) throw new Error('Invalid run ID.');
  const binary = '/var/lib/storage-spike/storage-spike';
  if (createHash('sha256').update(await readFile(binary)).digest('hex') !== request.binarySha256) throw new Error('Binary digest mismatch.');
  const out = `/var/lib/storage-spike-oracle/jobs/${request.id}`;
  await mkdir(out, { recursive: false, mode: 0o700 });
  const evidencePrefix = `results/${request.id}`;
  const save = async (name: string) => upload(request.stagingEndpoint, `${evidencePrefix}/${name}`, await readFile(path.join(out, name)));
  const invoke = async (name: string, args: string[], cpu = 0) => {
    const file = `${name}.jsonl`;
    await command('taskset', ['-c', String(cpu), binary, ...args], path.join(out, file));
    await save(file);
  };
  let status: Record<string, unknown> = { id: request.id, mode: request.mode, startedAt: new Date().toISOString(), status: 'running' };
  await writeFile(path.join(out, 'status.json'), JSON.stringify(status, null, 2), { mode: 0o600, flush: true });
  try {
    await upload(request.stagingEndpoint, `${evidencePrefix}/status.json`, JSON.stringify(status));
    const mountNamespace = execFileSync('readlink', ['/proc/self/ns/mnt'], { encoding: 'utf8' }).trim();
    const hostMountNamespace = execFileSync('sudo', ['readlink', '/proc/1/ns/mnt'], { encoding: 'utf8' }).trim();
    if (mountNamespace === hostMountNamespace) throw new Error('Guest job must run in its own mount namespace.');
    if (!request.local && ['smoke', 'performance', 'phase-a', 'fault-a', 'fault-b'].includes(request.mode)) {
      const mounted = spawnSync('mountpoint', ['-q', '/mnt/storage-spike']);
      if (mounted.error || (mounted.status !== 0 && mounted.status !== 1 && mounted.status !== 32)) throw new Error('Unable to inspect the existing mount.');
      if (mounted.status === 0) execFileSync('sudo', ['umount', '/mnt/storage-spike'], { stdio: 'pipe', timeout: 60_000 });
      execFileSync('sudo', ['mount', '-t', 'aznfs', request.nfsSource, '/mnt/storage-spike', '-o', mountOptions(request.cache)], { stdio: 'pipe', timeout: 120_000 });
      const actual = execFileSync('findmnt', ['-n', '-o', 'OPTIONS', '/mnt/storage-spike'], { encoding: 'utf8' }).trim().split(',');
      if (actual.some(option => ['nolock', 'local_lock=all', 'local_lock=flock'].includes(option))) throw new Error('Effective mount options disable server locks.');
    }
    await writeFile(path.join(out, 'environment.json'), JSON.stringify({
      request, mountNamespace, hostMountNamespace, uname: execFileSync('uname', ['-a'], { encoding: 'utf8' }),
      mount: spawnSync('findmnt', ['--json', '/mnt/storage-spike'], { encoding: 'utf8' }).stdout,
      cpu: execFileSync('lscpu', ['--json'], { encoding: 'utf8' }),
      aznfs: execFileSync('dpkg-query', ['-W', 'aznfs'], { encoding: 'utf8' }),
      time: execFileSync('timedatectl', ['show', '-p', 'NTPSynchronized'], { encoding: 'utf8' }),
    }, null, 2), { mode: 0o600, flush: true });
    for (const directory of [out, path.dirname(out)]) {
      const handle = await open(directory, 'r');
      try { await handle.sync(); } finally { await handle.close(); }
    }
    await save('environment.json');
    const root = `${request.local ? '/var/lib/storage-spike-oracle/local' : '/mnt/storage-spike'}/${request.id}`;
    if (['smoke', 'performance', 'phase-a'].includes(request.mode)) await mkdir(root, { mode: 0o700 });
    switch (request.mode) {
      case 'fault-a':
      case 'fault-b':
        await fault(request, out, binary);
        break;
      case 'smoke':
        await invoke('raw', ['raw', '--path', `${root}/raw`, '--bytes', '1048576', '--iterations', '100']);
        await invoke('write', ['write', '--dir', `${root}/quiver`, '--run', request.id, '--seconds', String(request.seconds ?? 3), '--interval-ms', String(request.intervalMs ?? 5), '--oracle', `${out}/oracle.jsonl`]);
        await invoke('recover', ['recover', '--dir', `${root}/quiver`, '--run', request.id, '--oracle', `${out}/oracle.jsonl`, '--observed', `${out}/observed.json`]);
        await save('oracle.jsonl');
        await save('observed.json');
        break;
      case 'performance': {
        const cores = request.cores ?? 1;
        if (!Number.isSafeInteger(cores) || cores < 1 || cores > 8) throw new Error('Core count must be between 1 and 8.');
        for (const mib of request.sizesMiB ?? [1, 2, 4, 8, 16]) {
          const bytes = String(mib * 1_048_576);
          if (cores === 1) await invoke(`raw-${mib}`, ['raw', '--path', `${root}/raw-${mib}`, '--bytes', bytes, '--iterations', '1000']);
          for (const interval of request.intervals ?? [0, 1, 5, 10, 25, 50, 100, 250, 1000]) {
            await Promise.all(Array.from({ length: cores }, async (_, cpu) => {
              const name = `quiver-${mib}-${interval}${cores === 1 ? '' : `-core-${cpu}`}`;
              await invoke(name, ['write', '--dir', `${root}/${name}`, '--run', `${request.id}-${name}`, '--bundle-bytes', bytes, '--interval-ms', String(interval), '--seconds', String(request.seconds ?? 15), '--drain'], cpu);
            }));
          }
        }
        break;
      }
      case 'blob':
        if (!request.endpoint || !request.blobMode) throw new Error('Blob endpoint and mode required.');
        for (const mib of request.sizesMiB ?? [1, 2, 4, 8, 16]) {
          await invoke(`blob-${mib}`, ['blob', '--endpoint', request.endpoint, '--mode', request.blobMode, '--bytes', String(mib * 1_048_576), '--seconds', String(request.seconds ?? 30)]);
        }
        await invoke('blob-fence', ['blob', '--endpoint', request.endpoint, '--mode', request.blobMode, '--fence']);
        break;
      case 'phase-a': {
        const source = '/var/lib/storage-spike/otel-arrow/rust/otap-dataflow';
        if (!request.quiverCommit || !/^[0-9a-f]{40}$/u.test(request.quiverCommit) || execFileSync('git', ['-C', source, 'rev-parse', 'HEAD'], { encoding: 'utf8' }).trim() !== request.quiverCommit) throw new Error('Phase A source differs from the driver pin.');
        const env = { ...process.env, TMPDIR: `${root}/tmp`, CARGO_TARGET_DIR: '/var/lib/storage-spike/phase-a-target' };
        await mkdir(env.TMPDIR, { recursive: true });
        const previous = process.cwd();
        process.chdir(source);
        for (const [label, args] of [
          ['quiver-tests', ['test', '--locked', '-p', 'otel-arrow-dfe-quiver']],
          ['durable-buffer-tests', ['test', '--locked', '-p', 'otel-arrow-dfe-core-nodes', 'durable_buffer']],
          ['df-engine-build', ['build', '--locked', '--release', '--bin', 'df_engine']],
        ] as const) {
          await command('env', Object.entries(env).filter((entry): entry is [string, string] => entry[1] !== undefined).map(([k, v]) => `${k}=${v}`).concat(['/home/spike/.cargo/bin/cargo', ...args]), path.join(out, `${label}.log`));
          await save(`${label}.log`);
        }
        const sample = await readFile(`${source}/configs/trafficgen-durable-buffer-noop.yaml`, 'utf8');
        const config = sample.replace('/var/lib/otap/trafficgen-durable-buffer-noop-example/buffer', `${root}/df-engine`).replace('poll_interval: 10ms', 'poll_interval: 10ms\n              upstream_ack: after_wal_sync\n              wal_flush_interval: 5ms');
        if (config === sample || !config.includes(`${root}/df-engine`)) throw new Error('Sample configuration was not redirected to this job.');
        const configFile = `${out}/df-engine.yaml`;
        await writeFile(configFile, config);
        const dfe = `${env.CARGO_TARGET_DIR}/release/df_engine`;
        const dfeArgs = ['--config', configFile, '--num-cores', '1', '--http-admin-bind', '127.0.0.1:0'];
        await command(dfe, [...dfeArgs, '--validate-and-exit'], `${out}/df-engine-validation.log`);
        await save('df-engine-validation.log');
        await command('timeout', ['--preserve-status', '--signal=INT', '--kill-after=30s', '30s', dfe, ...dfeArgs], `${out}/df-engine-smoke.log`);
        await save('df-engine-smoke.log');
        let bench: string | undefined;
        const buildEnv = Object.entries(env).filter((entry): entry is [string, string] => entry[1] !== undefined).map(([k, v]) => `${k}=${v}`);
        await command('env', [...buildEnv, '/home/spike/.cargo/bin/cargo', 'bench', '--locked', '-p', 'otel-arrow-dfe-quiver', '--bench', 'ingest', '--no-run', '--message-format=json'], `${out}/ingest-bench-build.log`, line => {
          try {
            const record = JSON.parse(line) as { reason?: string; target?: { name?: string }; executable?: string };
            if (record.reason === 'compiler-artifact' && record.target?.name === 'ingest' && record.executable) bench = record.executable;
          } catch { /* Cargo diagnostics stay in the build log. */ }
        });
        await save('ingest-bench-build.log');
        if (!bench?.startsWith(`${env.CARGO_TARGET_DIR}/release/deps/ingest-`)) throw new Error('Cargo did not identify the ingest benchmark executable.');
        const home = `${root}/benchmark-home`;
        await mkdir(home, { recursive: false, mode: 0o700 });
        await command('env', [`HOME=${home}`, bench, '--bench'], `${out}/ingest-bench.log`);
        await save('ingest-bench.log');
        process.chdir(previous);
        break;
      }
      case 'writer': {
        const oracle = `${out}/oracle.jsonl`;
        let active = true;
        const witness = (async () => {
          while (active) {
            try { await save('oracle.jsonl'); } catch { /* The local fsynced oracle remains authoritative. */ }
            await delay(1000);
          }
        })();
        try {
          await command(binary, ['write', '--run', request.id, '--oracle', oracle, ...request.args ?? []], path.join(out, 'writer.jsonl'), line => {
            try {
              const record = JSON.parse(line) as { kind?: string; pid?: number };
              if (record.kind === 'ready') void writeFile(path.join(out, 'writer.pid'), String(record.pid));
            } catch { /* Non-JSON diagnostics stay in the raw log. */ }
          });
        } finally { active = false; await witness; await save('oracle.jsonl'); await save('writer.jsonl'); }
        break;
      }
      case 'recover':
        await invoke('recover', ['recover', ...request.args ?? []]);
        break;
      case 'collect':
        for (const file of request.args ?? []) await upload(request.stagingEndpoint, `${evidencePrefix}/${path.basename(file)}`, await readFile(file));
        break;
    }
    status = { ...status, status: 'succeeded', finishedAt: new Date().toISOString() };
  } catch (error) {
    status = { ...status, status: 'failed', finishedAt: new Date().toISOString(), error: String(error) };
    if (['fault-a', 'fault-b'].includes(request.mode) && request.pairId && /^[a-z0-9-]{1,80}$/u.test(request.pairId)) {
      try { await upload(request.stagingEndpoint, `control/${request.pairId}/abort.json`, JSON.stringify({ actor: request.id, error: String(error) })); }
      catch { /* Local status preserves startup failures even if control storage is unavailable. */ }
    }
    throw error;
  } finally {
    await writeFile(path.join(out, 'status.json'), JSON.stringify(status, null, 2), { mode: 0o600, flush: true });
    try { await upload(request.stagingEndpoint, `${evidencePrefix}/status.json`, JSON.stringify(status)); }
    catch (error) {
      status = { ...status, evidenceUploadError: String(error) };
      await writeFile(path.join(out, 'status.json'), JSON.stringify(status, null, 2), { mode: 0o600, flush: true });
    }
  }
}

if (process.argv.includes('--request')) {
  const { values } = parseArgs({ options: { request: { type: 'string' } } });
  if (!values.request) throw new Error('Request required.');
  await run(JSON.parse(await readFile(values.request, 'utf8')) as Request);
}
