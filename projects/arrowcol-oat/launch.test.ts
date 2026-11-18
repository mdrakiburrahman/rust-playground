import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { execFileSync, spawn } from 'node:child_process';
import { once } from 'node:events';
import { readFileSync, readdirSync, writeFileSync } from 'node:fs';
import { mkdir, mkdtemp, open, readFile, readdir, readlink, realpath, rm, stat, unlink, writeFile, type FileHandle } from 'node:fs/promises';
import { createServer, request as createHttpRequest } from 'node:http';
import { connect as createTcpConnection } from 'node:net';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { performance } from 'node:perf_hooks';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { run_tree } from '../../infra/azure/bootstrap.js';
import { tableFromArrays, tableToIPC } from 'apache-arrow';
import { Table as WasmTable, writeParquet } from 'parquet-wasm';
import { collectDataFlow } from './dataflow-verdict.js';
import {
  buildOwnedServer, cleanupLocalComposeProjects, finishOwnedLocalRun, launchOat, main, OatLauncherDeadline, assertNoUnsettledDeadlineReceipts, readOatConfigForMode,
  processStartTicks, resolveCliConfigPath, resolveOatConfigPath, sendViewerFile,
  recordLegacyAksDisposition, startLocalServer, startLocalViewer, stopLocalServer, type OatRunStatus,
} from './launch.js';

const status = (state: OatRunStatus['state']) => ({
  id: '123e4567-e89b-42d3-a456-426614174000',
  config_sha256: createHash('sha256').update('{"platform":{"kind":"local"}}').digest('hex'),
  state, progress: state, result_count: state === 'succeeded' ? 2 : 0, error: null,
});

async function bindLocalReceipt(deadline: OatLauncherDeadline): Promise<void> {
  const submitted_config_body = '{"platform":{"kind":"local"}}';
  await deadline.setSubmissionBinding({
    submitted_config_body,
    submitted_config_sha256: status('running').config_sha256,
    source_config_sha256: 'a'.repeat(64),
  });
}

test('procfs ESRCH race is treated as an exited process', { skip: process.platform !== 'linux' }, async () => {
  const error = Object.assign(new Error('process disappeared during procfs read'), { code: 'ESRCH' });
  const readProcStat = async (procPath: string): Promise<string> => {
    assert.equal(procPath, '/proc/12345/stat');
    throw error;
  };
  assert.equal(await processStartTicks(12345, readProcStat), undefined);
});

async function ownedBuildFixture(options: {
  dirty?: string; afterStatus?: string; afterSha?: string; noArtifact?: boolean;
  duplicateArtifact?: boolean; buildCode?: number;
  rejectRun?: boolean;
  succeedRun?: boolean;
  stopDelayMs?: number;
  configHash?: string | null;
  invalidRunId?: boolean;
  initialState?: OatRunStatus['state'];
  cancelState?: 'cancelled' | 'failed';
} = {}) {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'oat-owned-build-'));
  const executable = path.join(directory, 'built-server');
  const pidPath = path.join(directory, 'server.pid');
  await writeFile(executable, '#!/usr/bin/env node\nprocess.exit(83);\n', { mode: 0o700 });
  const sha = 'a'.repeat(40);
  const commands: string[][] = [];
  let headReads = 0, statusReads = 0;
  const runner = async (command: string[], _timeout: number, cwd?: string) => {
    commands.push([...command]);
    if (command[0] === 'git') {
      const stdout = command[1] === 'rev-parse'
        ? (headReads++ ? options.afterSha ?? sha : sha)
        : (statusReads++ ? options.afterStatus ?? options.dirty ?? '' : options.dirty ?? '');
      return { returncode: 0, stdout, stderr: '' };
    }
    assert.equal(command[0], 'cargo');
    await writeFile(executable, `#!/usr/bin/env node
const http = require('node:http');
const { createHash } = require('node:crypto');
require('node:fs').writeFileSync(${JSON.stringify(pidPath)}, String(process.pid));
const address = process.argv[process.argv.indexOf('--listen') + 1];
const [host, port] = address.split(':');
if (${options.stopDelayMs ?? 0} > 0) process.on('SIGTERM', () => setTimeout(() => process.exit(0), ${options.stopDelayMs ?? 0}));
let configHash;
let cancelled = false;
const runStatus = (state) => ({
  id: ${JSON.stringify(options.invalidRunId ? 'invalid-run-id' : '123e4567-e89b-42d3-a456-426614174000')}, state,
  progress: state === 'succeeded' ? 'published' : state, result_count: state === 'succeeded' ? 1 : 0,
  error: state === 'failed' ? 'synthetic cleanup failure' : null, config_sha256: configHash,
  started_at_unix_ms: Date.now() - 1,
  finished_at_unix_ms: state === 'queued' || state === 'running' ? null : Date.now(),
});
http.createServer((request, response) => {
  response.setHeader('content-type', 'application/json');
  if (request.method === 'POST' && request.url === '/runs') {
    const chunks = [];
    request.on('data', (chunk) => chunks.push(Buffer.from(chunk)));
    request.on('end', () => {
      const body = Buffer.concat(chunks);
      if (${options.rejectRun ?? false}) {
        response.writeHead(422).end(JSON.stringify({ error: 'invalid matrix fixture' }));
        return;
      }
      if (${options.succeedRun ?? false}) {
        configHash = ${options.configHash === undefined ? "createHash('sha256').update(body).digest('hex')" : JSON.stringify(options.configHash)};
        response.end(JSON.stringify(runStatus(${JSON.stringify(options.initialState ?? 'succeeded')})));
        return;
      }
      response.end(JSON.stringify({ status: 'ok', git_sha: process.env.GIT_SHA,
        stamp_config: process.env.ARROWCOL_AKS_STAMP_CONFIG_FILE ?? null }));
    });
    return;
  }
  if (request.method === 'POST' && request.url === '/runs/123e4567-e89b-42d3-a456-426614174000/cancel') {
    cancelled = true;
    response.writeHead(202).end();
    return;
  }
  if (request.method === 'GET' && request.url === '/runs/123e4567-e89b-42d3-a456-426614174000') {
    response.end(JSON.stringify(runStatus(${options.cancelState ? `cancelled ? ${JSON.stringify(options.cancelState)} : 'running'` : "'succeeded'"})));
    return;
  }
  response.end(JSON.stringify({ status: 'ok', git_sha: process.env.GIT_SHA,
    stamp_config: process.env.ARROWCOL_AKS_STAMP_CONFIG_FILE ?? null }));
}).listen(Number(port), host);
`, { mode: 0o700 });
    const artifact = {
      reason: 'compiler-artifact', manifest_path: path.join(cwd!, 'projects/arrowcol-oat-rust/Cargo.toml'),
      target: { name: 'arrowcol-oat-rust', kind: ['bin'] }, executable, fresh: false,
    };
    return {
      returncode: options.buildCode ?? 0,
      stdout: options.noArtifact ? '' : `${JSON.stringify(artifact)}\n`.repeat(options.duplicateArtifact ? 2 : 1),
      stderr: options.buildCode ? 'synthetic build failure' : '',
    };
  };
  const stop = async (): Promise<void> => {
    let pid: number;
    try { pid = Number(await readFile(pidPath, 'utf8')); }
    catch (error) {
      if (error instanceof Error && 'code' in error && error.code === 'ENOENT') return;
      throw error;
    }
    if (await processStartTicks(pid) === undefined) return;
    process.kill(pid, 'SIGTERM');
    const until = performance.now() + 5000;
    while (await processStartTicks(pid) !== undefined && performance.now() < until) {
      await new Promise((resolveWait) => setTimeout(resolveWait, 10));
    }
    assert.equal(await processStartTicks(pid), undefined, 'fixture server must exit');
  };
  return { directory, executable, sha, commands, runner, stop };
}

async function passingMainFixture(options: Parameters<typeof ownedBuildFixture>[0] = {}) {
  const fixture = await ownedBuildFixture({ succeedRun: true, ...options });
  const stateRoot = path.join(fixture.directory, 'state');
  const receiptRoot = path.join(fixture.directory, 'receipts');
  const configPath = path.join(fixture.directory, 'config.json');
  const resultPath = path.join(fixture.directory, 'results.parquet');
  const table = tableFromArrays({
    schema_version: [2], run_id: ['22222222-2222-4222-8222-222222222222'], mode: ['local'], stamp: ['00000'],
    tunables_json: [JSON.stringify({ orchestration_run_id: status('succeeded').id })], git_sha: [fixture.sha],
    status: ['succeeded'], pinning_ok: [true], table_owner: ['fixture-owner'], rows: [100n],
    duration_ms: [1000n], rows_per_second: [100], replicas: [1n], dfe_worker_concurrency: [8n],
    table: ['sqlserver_active_sessions'], tables: [1],
  });
  await writeFile(resultPath, writeParquet(WasmTable.fromIPCStream(tableToIPC(table, 'stream'))));
  await writeFile(configPath, JSON.stringify({
    platform: { kind: 'local', compose_state_root: stateRoot }, stamp: '00000',
    tables: ['sqlserver_active_sessions'], replica_counts: [1], dfe_worker_concurrency: [8],
    result_url: pathToFileURL(resultPath).href,
  }));
  return { ...fixture, stateRoot, receiptRoot, configPath };
}

test('owned server starts the exact successful Cargo artifact, not a stale prebuilt path', async () => {
  const fixture = await ownedBuildFixture();
  let server: Awaited<ReturnType<typeof startLocalServer>> | undefined;
  const callerSha = process.env.GIT_SHA;
  process.env.GIT_SHA = 'unverified-caller-revision';
  try {
    server = await startLocalServer(fixture.runner, {
      ARROWCOL_AKS_STAMP_CONFIG_FILE: '.temp/stamp-87513.yaml',
    });
    assert.deepEqual(await (await fetch(`${server.url}/healthz`)).json(), {
      status: 'ok', git_sha: fixture.sha, stamp_config: '.temp/stamp-87513.yaml',
    });
    assert.deepEqual(fixture.commands.find((command) => command[0] === 'cargo'), [
      'cargo', 'build', '--locked', '--package', 'arrowcol-oat-rust',
      '--bin', 'arrowcol-oat-rust', '--message-format=json-render-diagnostics',
    ]);
  } finally {
    if (callerSha === undefined) delete process.env.GIT_SHA;
    else process.env.GIT_SHA = callerSha;
    if (server) await stopLocalServer(server.child);
    await rm(fixture.directory, { recursive: true, force: true });
  }
});

test('dirty owned builds retain unknown provenance instead of asserting a clean commit', async () => {
  const fixture = await ownedBuildFixture({ dirty: ' M projects/arrowcol-oat/launch.ts\n' });
  try {
    const build = await buildOwnedServer(process.cwd(), fixture.runner);
    assert.equal(build.gitSha, 'unknown');
    assert.equal(build.executable, fixture.executable);
  } finally {
    await rm(fixture.directory, { recursive: true, force: true });
  }
});

test('changed source, failed builds and missing artifact metadata cannot start an attributed server', async () => {
  for (const [options, expected] of [
    [{ afterSha: 'b'.repeat(40) }, /source changed/],
    [{ afterStatus: ' M projects/arrowcol-oat/launch.ts\n' }, /source changed/],
    [{ noArtifact: true }, /exactly one built OAT server/],
    [{ duplicateArtifact: true }, /exactly one built OAT server/],
    [{ buildCode: 101 }, /synthetic build failure/],
  ] as const) {
    const fixture = await ownedBuildFixture(options);
    try {
      await assert.rejects(startLocalServer(fixture.runner), expected);
    } finally {
      await rm(fixture.directory, { recursive: true, force: true });
    }
  }
});

test('executable help resolves its tsx loader outside the repository', async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'oat-cli-cwd-'));
  try {
    const executable = fileURLToPath(new URL('./cli.mjs', import.meta.url));
    const output = execFileSync(process.platform === 'win32' ? process.execPath : executable,
      process.platform === 'win32' ? [executable, '--help'] : ['--help'],
      { cwd: directory, encoding: 'utf8', timeout: 15_000 });
    assert.match(output, /Usage: arrowcol-oat/);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test('failed local runs exit without serving absent or stale results', { skip: process.platform !== 'linux' }, async (t) => {
  for (const [scenario, owned] of [
    ['rejected', true], ['rejected', false], ['failed-verdict', true], ['failed-verdict', false],
  ] as const) {
    await t.test(`${scenario}: ${owned ? 'owned server' : 'external server'}`, async () => {
      const fixture = await ownedBuildFixture({ rejectRun: scenario === 'rejected', succeedRun: scenario === 'failed-verdict' });
      const configPath = path.join(fixture.directory, 'config.json');
      const resultsPath = path.join(fixture.directory, 'results.parquet');
      if (scenario === 'failed-verdict') await writeFile(resultsPath, 'PAR1stalePAR1');
      await writeFile(configPath, JSON.stringify({
        platform: { kind: 'local', compose_state_root: path.join(fixture.directory, 'state') },
        website_url: pathToFileURL(resultsPath).href,
        result_url: pathToFileURL(resultsPath).href,
      }));
      const server = owned ? undefined : await startLocalServer(fixture.runner);
      const previousExitCode = process.exitCode;
      const log = console.log;
      const lines: string[] = [];
      console.log = (...values: unknown[]) => {
        const line = values.map(String).join(' ');
        lines.push(line);
        if (line.startsWith('Local results viewer:')) {
          setImmediate(() => process.emit('SIGTERM'));
        }
      };
      try {
        await main(['--mode', 'local', '--config', configPath,
          ...(server ? ['--server', server.url] : [])], fixture.runner, undefined, path.join(fixture.directory, 'receipts'));
        assert.equal(process.exitCode, 1);
        assert.ok(lines.some((line) => line.includes(
          scenario === 'rejected' ? 'invalid matrix fixture' : 'DATA FLOW: FAIL')));
        assert.equal(lines.some((line) => line.startsWith('Local results viewer:')), false);
      } finally {
        console.log = log;
        process.exitCode = previousExitCode;
        if (server) await stopLocalServer(server.child);
        await rm(fixture.directory, { recursive: true, force: true });
      }
    });
  }
});

test('deadline-enabled main path fails if a successful report crosses the deadline during server shutdown', { skip: process.platform !== 'linux' }, async () => {
  const fixture = await passingMainFixture({ stopDelayMs: 2500 });
  const previousExitCode = process.exitCode;
  let scored = false;
  const collect: typeof collectDataFlow = async (...args) => {
    const verdict = await collectDataFlow(...args);
    assert.equal(verdict.pass, true, verdict.reasons.join('; '));
    scored = true;
    return verdict;
  };
  try {
    process.exitCode = undefined;
    await main(['--deadline-seconds', '2', '--no-viewer', '--config', fixture.configPath], fixture.runner, undefined, fixture.receiptRoot, collect);
    assert.equal(scored, true);
    assert.equal(process.exitCode, 1);
    const [receiptName] = await readdir(fixture.receiptRoot);
    assert.ok(receiptName);
    const receipt = JSON.parse(await readFile(path.join(fixture.receiptRoot, receiptName), 'utf8')) as {
      overrun: boolean; run_state: string; cleanup_state: string; submitted_config_sha256: string;
    };
    assert.equal(receipt.overrun, true);
    assert.equal(receipt.run_state, 'succeeded');
    assert.equal(receipt.cleanup_state, 'confirmed');
    assert.match(receipt.submitted_config_sha256, /^[0-9a-f]{64}$/);
  } finally {
    process.exitCode = previousExitCode;
    await fixture.stop();
    await rm(fixture.directory, { recursive: true, force: true });
  }
});

test('successful deadline main exposes one frozen durable identity without waiting on supplementary work', { skip: process.platform !== 'linux' }, async () => {
  const fixture = await passingMainFixture({ initialState: 'running' });
  const previousExitCode = process.exitCode;
  let calls = 0;
  let observerError: unknown;
  try {
    process.exitCode = undefined;
    await main(['--deadline-seconds', '10', '--no-viewer', '--config', fixture.configPath], fixture.runner, undefined, fixture.receiptRoot, undefined,
      (identity, receiptPath) => {
        calls++;
        try {
        assert.ok(Object.isFrozen(identity));
        const receipt = JSON.parse(readFileSync(receiptPath, 'utf8'));
        const markerName = readdirSync(fixture.stateRoot).find((name) => name.startsWith('.arrowcol-oat-owner-'));
        assert.ok(markerName);
        const marker = JSON.parse(readFileSync(path.join(fixture.stateRoot, markerName), 'utf8'));
        assert.deepEqual(identity, {
          invocationId: receipt.invocation_id, runId: receipt.run_id, serverUrl: marker.serverUrl,
          serverPid: marker.serverPid, serverStartTicks: marker.serverStartTicks, serverExecutable: marker.serverExecutable,
        });
        assert.equal(receipt.creation_state, 'accepted');
        assert.equal(receipt.config_binding_error, null);
        assert.equal(receipt.run_id, marker.runId);
        assert.equal(receipt.server_url, identity.serverUrl);
        assert.equal(receipt.submitted_config_body, marker.submitted_config_body);
        assert.equal(receipt.submitted_config_sha256, marker.submitted_config_sha256);
        assert.equal(marker.ownerPid, process.pid);
        } catch (error) {
          observerError = error;
        }
        return new Promise<void>(() => {});
      });
    assert.equal(calls, 1);
    assert.ifError(observerError);
    assert.equal(process.exitCode, undefined);
    assert.deepEqual(await readdir(fixture.stateRoot), []);
    const [name] = await readdir(fixture.receiptRoot);
    const receipt = JSON.parse(await readFile(path.join(fixture.receiptRoot, name!), 'utf8'));
    assert.equal(receipt.overrun, false);
    assert.equal(receipt.cleanup_state, 'confirmed');
  } finally {
    process.exitCode = previousExitCode;
    await fixture.stop();
    await rm(fixture.directory, { recursive: true, force: true });
  }
});

test('terminal receipt remains readable after the owned server stops before observer polling', { skip: process.platform !== 'linux', timeout: 15_000 }, async () => {
  const fixture = await passingMainFixture({ initialState: 'running' });
  const previousExitCode = process.exitCode;
  let observerPromise: Promise<void> | undefined;
  try {
    process.exitCode = undefined;
    await main(['--deadline-seconds', '10', '--no-viewer', '--config', fixture.configPath], fixture.runner, undefined, fixture.receiptRoot, undefined,
      (identity, receiptPath) => {
        const markerName = readdirSync(fixture.stateRoot).find((name) => name.startsWith('.arrowcol-oat-owner-'));
        assert.ok(markerName);
        const marker = JSON.parse(readFileSync(path.join(fixture.stateRoot, markerName), 'utf8')) as {
          serverUrl: string; serverPid: number; serverStartTicks: string; serverExecutable: string; runId: string;
          submitted_config_sha256: string;
        };
        const activeReceipt = JSON.parse(readFileSync(receiptPath, 'utf8')) as {
          invocation_id: string; run_id: string; server_url: string; server_pid: number;
          server_start_ticks: string; server_executable: string; submitted_config_sha256: string;
        };
        assert.equal(path.basename(receiptPath), identity.invocationId + '.json');
        assert.equal(activeReceipt.invocation_id, identity.invocationId);
        assert.equal(activeReceipt.run_id, identity.runId);
        assert.equal(activeReceipt.server_url, identity.serverUrl);
        assert.equal(activeReceipt.server_pid, identity.serverPid);
        assert.equal(activeReceipt.server_start_ticks, identity.serverStartTicks);
        assert.equal(activeReceipt.server_executable, identity.serverExecutable);
        assert.equal(activeReceipt.submitted_config_sha256, marker.submitted_config_sha256);
        observerPromise = (async () => {
          const stopDeadline = performance.now() + 5000;
          while (await processStartTicks(identity.serverPid) !== undefined && performance.now() < stopDeadline) {
            await new Promise((resolveWait) => setTimeout(resolveWait, 10));
          }
          assert.equal(await processStartTicks(identity.serverPid), undefined, 'owned server must stop before observer reads the receipt');
          assert.equal(path.basename(receiptPath), identity.invocationId + '.json');
          const receipt = JSON.parse(await readFile(receiptPath, 'utf8')) as {
            invocation_id: string; run_id: string; server_url: string; server_pid: number;
            server_start_ticks: string; server_executable: string; submitted_config_sha256: string;
            terminal_observation: { observed_at_unix_ms: number; status: OatRunStatus } | null;
          };
          assert.equal(receipt.invocation_id, identity.invocationId);
          assert.equal(receipt.run_id, identity.runId);
          assert.equal(receipt.server_url, identity.serverUrl);
          assert.equal(receipt.server_pid, identity.serverPid);
          assert.equal(receipt.server_start_ticks, identity.serverStartTicks);
          assert.equal(receipt.server_executable, identity.serverExecutable);
          assert.equal(receipt.server_url, marker.serverUrl);
          assert.equal(receipt.server_pid, marker.serverPid);
          assert.equal(receipt.server_start_ticks, marker.serverStartTicks);
          assert.equal(receipt.server_executable, marker.serverExecutable);
          assert.equal(receipt.run_id, marker.runId);
          assert.equal(receipt.submitted_config_sha256, marker.submitted_config_sha256);
          assert.ok(receipt.terminal_observation);
          const observation = receipt.terminal_observation;
          assert.equal(observation.status.id, identity.runId);
          assert.equal(observation.status.config_sha256, marker.submitted_config_sha256);
          assert.equal(observation.status.state, 'succeeded');
          assert.equal(observation.status.progress, 'published');
          assert.equal(observation.status.result_count, 1);
          assert.equal(observation.status.error, null);
          assert.equal(typeof observation.status.started_at_unix_ms, 'number');
          assert.equal(typeof observation.status.finished_at_unix_ms, 'number');
          assert.ok(observation.observed_at_unix_ms >= observation.status.finished_at_unix_ms!);
        })();
      });
    assert.ok(observerPromise);
    await observerPromise;
    assert.equal(process.exitCode, undefined);
    assert.deepEqual(await readdir(fixture.stateRoot), []);
  } finally {
    process.exitCode = previousExitCode;
    await fixture.stop();
    await rm(fixture.directory, { recursive: true, force: true });
  }
});

test('terminal observations require exact accepted run, config and server identity', async () => {
  const runId = '123e4567-e89b-42d3-a456-426614174020';
  const serverUrl = 'http://127.0.0.1:8088';
  const submittedConfigBody = '{"platform":{"kind":"local"}}';
  const submittedConfigSha256 = createHash('sha256').update(submittedConfigBody).digest('hex');
  const scenarios = [
    { name: 'missing server identity', bindIdentity: false, statusId: runId, statusSha: submittedConfigSha256 },
    { name: 'different run ID', bindIdentity: true, statusId: '123e4567-e89b-42d3-a456-426614174021', statusSha: submittedConfigSha256 },
    { name: 'different config hash', bindIdentity: true, statusId: runId, statusSha: 'b'.repeat(64) },
  ];
  for (const scenario of scenarios) {
    const directory = await mkdtemp(path.join(os.tmpdir(), 'oat-terminal-binding-'));
    const deadline = await OatLauncherDeadline.create(600, performance.now(), directory);
    try {
      await deadline.setSubmissionBinding({
        submitted_config_body: submittedConfigBody,
        submitted_config_sha256: submittedConfigSha256,
        source_config_sha256: 'a'.repeat(64),
      });
      await deadline.markCreationPending(serverUrl);
      await deadline.recordRunStarted({ ...status('running'), id: runId });
      if (scenario.bindIdentity) {
        await deadline.recordOwnedServerIdentity({
          invocationId: deadline.invocationId, runId, serverUrl, serverPid: process.pid,
          serverStartTicks: 'fixture-start-ticks', serverExecutable: process.execPath,
        });
      }
      await deadline.recordTerminal({
        ...status('succeeded'), id: scenario.statusId, config_sha256: scenario.statusSha,
        started_at_unix_ms: Date.now() - 1, finished_at_unix_ms: Date.now(),
      });
      const receipt = JSON.parse(await readFile(deadline.receiptPath, 'utf8')) as { terminal_observation: unknown };
      assert.equal(receipt.terminal_observation, null, scenario.name);
    } finally {
      deadline.dispose();
      await rm(directory, { recursive: true, force: true });
    }
  }
});

test('failed terminal response is retained as termination evidence without confirming cleanup', async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'oat-terminal-failed-'));
  const runId = '123e4567-e89b-42d3-a456-426614174022';
  const serverUrl = 'http://127.0.0.1:8088';
  const submittedConfigBody = '{"platform":{"kind":"local"}}';
  const submittedConfigSha256 = createHash('sha256').update(submittedConfigBody).digest('hex');
  const deadline = await OatLauncherDeadline.create(600, performance.now(), directory);
  try {
    await deadline.setSubmissionBinding({
      submitted_config_body: submittedConfigBody,
      submitted_config_sha256: submittedConfigSha256,
      source_config_sha256: 'a'.repeat(64),
    });
    await deadline.markCreationPending(serverUrl);
    await deadline.recordRunStarted({ ...status('running'), id: runId });
    await deadline.recordOwnedServerIdentity({
      invocationId: deadline.invocationId, runId, serverUrl, serverPid: process.pid,
      serverStartTicks: 'fixture-start-ticks', serverExecutable: process.execPath,
    });
    await deadline.recordTerminal({
      ...status('failed'), id: runId, state: 'failed', error: 'synthetic terminal failure',
      started_at_unix_ms: Date.now() - 1, finished_at_unix_ms: Date.now(),
    });
    const receipt = JSON.parse(await readFile(deadline.receiptPath, 'utf8')) as {
      cleanup_state: string; terminal_observation: { status: OatRunStatus } | null;
    };
    assert.equal(receipt.cleanup_state, 'unsettled');
    assert.equal(receipt.terminal_observation?.status.state, 'failed');
    assert.equal(receipt.terminal_observation?.status.error, 'synthetic terminal failure');
  } finally {
    deadline.dispose();
    await rm(directory, { recursive: true, force: true });
  }
});

test('AKS failure stays a benchmark failure, and only bound cleanup evidence clears the receipt', async () => {
  const scenarios = [
    { name: 'confirmed', prove: true, own: true, mismatch: false, allowed: true },
    { name: 'no proof', prove: undefined, own: true, mismatch: false, allowed: false },
    { name: 'malformed proof', prove: 'true', own: true, mismatch: false, allowed: false },
    { name: 'external server', prove: true, own: false, mismatch: false, allowed: false },
    { name: 'wrong digest', prove: true, own: true, mismatch: true, allowed: false },
  ];
  for (const { name, prove, own, mismatch, allowed } of scenarios) {
    const directory = await mkdtemp(path.join(os.tmpdir(), 'oat-aks-cleanup-'));
    const submitted_config_body = '{"platform":{"kind":"aks"}}';
    const submitted_config_sha256 = createHash('sha256').update(submitted_config_body).digest('hex');
    const server = 'http://127.0.0.1:8088/';
    const deadline = await OatLauncherDeadline.create(undefined, performance.now(), directory);
    try {
      const accepted = { ...status('running'), config_sha256: submitted_config_sha256 };
      const failed = {
        ...accepted, state: 'failed', progress: 'failed', result_count: 0,
        error: 'synthetic AKS failure', finished_at_unix_ms: Date.now(),
        config_sha256: mismatch ? '0'.repeat(64) : submitted_config_sha256,
        aks_cleanup_confirmed: prove,
      };
      const requests: string[] = [];
      await assert.rejects(launchOat('unused.json', server, 1, async (url, init) => {
        requests.push(`${init?.method ?? 'GET'} ${new URL(String(url)).pathname}`);
        return Response.json(init?.method === 'POST' ? accepted : failed);
      }, () => {}, 'aks', undefined, undefined, own ? async (started) => {
        await deadline.recordOwnedServerIdentity({
          invocationId: deadline.invocationId, runId: started.id, serverUrl: server,
          serverPid: process.pid, serverStartTicks: 'fixture-start-ticks', serverExecutable: process.execPath,
        });
      } : undefined, undefined, deadline, {
        submitted_config_body, submitted_config_sha256, source_config_sha256: 'a'.repeat(64),
      }), mismatch ? /config_sha256 mismatch/ : /synthetic AKS failure/, name);
      assert.deepEqual(requests, ['POST /runs', `GET /runs/${accepted.id}`], name);
      const bytes = await readFile(deadline.receiptPath, 'utf8');
      const receipt = JSON.parse(bytes);
      assert.equal(receipt.run_state, 'failed', name);
      assert.equal(receipt.cleanup_state, allowed ? 'confirmed' : 'unsettled', name);
      if (allowed) {
        assert.equal(receipt.terminal_observation?.status.aks_cleanup_confirmed, true, name);
        assert.equal(receipt.terminal_observation?.status.error, failed.error, name);
        await assertNoUnsettledDeadlineReceipts(directory);
      } else {
        await assert.rejects(assertNoUnsettledDeadlineReceipts(directory), /Unsettled AKS launcher receipt .*operator/, name);
      }
      assert.equal(await readFile(deadline.receiptPath, 'utf8'), bytes, name);
    } finally {
      deadline.dispose();
      await rm(directory, { recursive: true, force: true });
    }
  }
});

test('accepted receipts with unverified platforms refuse recovery without any request', async () => {
  const requests: string[] = [];
  const server = createServer((request, response) => {
    requests.push(`${request.method} ${request.url}`);
    response.setHeader('content-type', 'application/json');
    response.end(JSON.stringify(status('succeeded')));
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  assert.ok(address && typeof address !== 'string');
  try {
    for (const body of [null, 'not JSON', '{}', '{"platform":{"kind":"other"}}']) {
      const directory = await mkdtemp(path.join(os.tmpdir(), 'oat-unknown-recovery-platform-'));
      const deadline = await OatLauncherDeadline.create(undefined, performance.now(), directory);
      try {
        if (body !== null) {
          await deadline.setSubmissionBinding({
            submitted_config_body: body,
            submitted_config_sha256: createHash('sha256').update(body).digest('hex'),
            source_config_sha256: 'a'.repeat(64),
          });
        }
        await deadline.markCreationPending(`http://127.0.0.1:${address.port}`);
        await deadline.recordRunStarted(status('running'));
        const original = await readFile(deadline.receiptPath, 'utf8');
        await assert.rejects(assertNoUnsettledDeadlineReceipts(directory), /platform unverified.*operator/);
        assert.deepEqual(requests, []);
        assert.equal(await readFile(deadline.receiptPath, 'utf8'), original);
      } finally {
        deadline.dispose();
        await rm(directory, { recursive: true, force: true });
      }
    }
  } finally {
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
  }
});

test('legacy AKS disposition is one-time, exact, and does not rewrite the failed receipt', {
  skip: process.platform !== 'linux',
}, async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'oat-legacy-disposition-'));
  const root = path.join(directory, 'receipts');
  const deadline = await OatLauncherDeadline.create(undefined, performance.now(), root);
  const manifestPath = path.join(directory, 'reviewed-evidence');
  const dispositionPath = deadline.receiptPath + '.failed-aks-disposition';
  const digest = (body: Buffer | string) => createHash('sha256').update(body).digest('hex');
  const error = 'maintenance omitted effective per-table operationTimeout before load admission';
  const body = JSON.stringify({
    platform: { kind: 'aks' }, tables: ['test'], replica_counts: [1], dfe_worker_concurrency: [16],
  });
  const listener = createServer();
  await new Promise<void>((resolve, reject) => {
    listener.once('error', reject);
    listener.listen(0, '127.0.0.1', resolve);
  });
  const address = listener.address();
  assert.ok(address && typeof address !== 'string');
  const serverUrl = 'http://127.0.0.1:' + address.port + '/';
  const now = Date.now();
  const terminal = {
    ...status('failed'), config_sha256: digest(body), error, started_at_unix_ms: now, finished_at_unix_ms: now,
  };
  try {
    await deadline.setSubmissionBinding({ submitted_config_body: body,
      submitted_config_sha256: digest(body), source_config_sha256: digest(body) });
    await deadline.markCreationPending(serverUrl);
    await deadline.recordRunStarted(terminal);
    const original = await readFile(deadline.receiptPath);
    const cellId = 'c0bfca05-28f2-4e16-a4cc-b3f28a351fb9';
    const raw = digest('fixture');
    const manifest = {
      schema_version: 1, disposition: 'failed-before-load-cleanup-verified', invocation_id: deadline.invocationId,
      original_receipt_sha256: digest(original), server_url: serverUrl, raw_evidence_sha256: raw,
      verifier_sha256: raw, runner_source_sha256: raw, terminal_get: terminal,
      raw_observation_sha256: Object.fromEntries([
        'terminal_get', 'single_cell_trace', 'fic_ready', 'workload_stop_trace', 'process_exit',
        'namespace_get', 'fic_inventory',
      ].map((name) => [name, raw])),
      cell: {
        id: cellId, namespace: 'arrow-collector-' + cellId.replaceAll('-', ''), namespace_uid: cellId,
        aks_id: '/fake/cluster', api_origin: 'https://test.invalid', owner_token: 'fixture',
        fic_id: '/fake/fic', fic_subject: 'fixture', qap: 'not-attempted', result_artifact: 'retained-unranked',
        namespace_absent_at_unix_ms: now, fic_absent_at_unix_ms: now,
      },
      writer_fence: {
        boot_id: (await readFile('/proc/sys/kernel/random/boot_id', 'utf8')).trim(),
        pid_namespace: await readlink('/proc/self/ns/pid'), net_namespace: await readlink('/proc/self/ns/net'),
        launcher: { pid: 2147483646, start_ticks: '16', executable: '/opt/example/node' },
        server: { pid: 2147483647, ppid: 2147483646, start_ticks: '17', executable: '/opt/example/oat' },
        fenced_at_unix_ms: now,
      },
    };
    const bytes = JSON.stringify(manifest);
    const approval = [{ invocationId: deadline.invocationId, receiptSha256: digest(original), evidenceSha256: digest(bytes) }];
    await writeFile(manifestPath, bytes);
    await assert.rejects(main(['--recover-failed-aks', deadline.invocationId, manifestPath], undefined, undefined, root),
      /No reviewed legacy AKS disposition/);
    await assert.rejects(assertNoUnsettledDeadlineReceipts(root, undefined, approval), /Unsettled AKS/);
    const badStatus = JSON.stringify({ ...manifest, terminal_get: { ...terminal, config_sha256: '0'.repeat(64) } });
    await writeFile(manifestPath, badStatus);
    await assert.rejects(recordLegacyAksDisposition(root, deadline.invocationId, manifestPath, [
      { ...approval[0]!, evidenceSha256: digest(badStatus) },
    ]), /lacks coherent single-cell/);
    await writeFile(manifestPath, bytes);
    await assert.rejects(recordLegacyAksDisposition(root, deadline.invocationId, manifestPath, approval), /listener is still present/);
    await new Promise<void>((resolve, reject) => listener.close((error) => error ? reject(error) : resolve()));
    await Promise.all([1, 2].map(() => recordLegacyAksDisposition(root, deadline.invocationId, manifestPath, approval)));
    await assertNoUnsettledDeadlineReceipts(root, undefined, approval);
    assert.deepEqual(await readFile(deadline.receiptPath), original);
    assert.equal(await readFile(dispositionPath, 'utf8'), bytes);
    await assert.rejects(assertNoUnsettledDeadlineReceipts(root), /Unapproved or orphaned/);
    await writeFile(dispositionPath, bytes + '\n');
    await assert.rejects(assertNoUnsettledDeadlineReceipts(root, undefined, approval), /reviewed receipt and evidence digests/);
    await assert.rejects(recordLegacyAksDisposition(root, deadline.invocationId, manifestPath, approval), /Conflicting/);
    assert.equal(await readFile(dispositionPath, 'utf8'), bytes + '\n');
    await writeFile(dispositionPath, bytes);
    await writeFile(deadline.receiptPath, Buffer.concat([original, Buffer.from('\n')]));
    await assert.rejects(assertNoUnsettledDeadlineReceipts(root, undefined, approval), /reviewed receipt and evidence digests/);
    await unlink(deadline.receiptPath);
    await assert.rejects(assertNoUnsettledDeadlineReceipts(root, undefined, approval), /Unapproved or orphaned/);
  } finally {
    if (listener.listening) await new Promise<void>((resolve) => listener.close(() => resolve()));
    deadline.dispose();
    await rm(directory, { recursive: true, force: true });
  }
});

test('legacy deadline receipts remain readable without new terminal identity fields', async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'oat-terminal-legacy-'));
  const deadline = await OatLauncherDeadline.create(600, performance.now(), directory);
  try {
    const legacy = JSON.parse(await readFile(deadline.receiptPath, 'utf8')) as Record<string, unknown>;
    delete legacy.server_pid;
    delete legacy.server_start_ticks;
    delete legacy.server_executable;
    delete legacy.terminal_observation;
    await writeFile(deadline.receiptPath, JSON.stringify(legacy));
    const restored = await OatLauncherDeadline.restore(deadline.receiptPath);
    await restored.markNoRun();
    const receipt = JSON.parse(await readFile(deadline.receiptPath, 'utf8')) as {
      server_pid: number | null; server_start_ticks: string | null; server_executable: string | null;
      terminal_observation: unknown;
    };
    assert.equal(receipt.server_pid, null);
    assert.equal(receipt.server_start_ticks, null);
    assert.equal(receipt.server_executable, null);
    assert.equal(receipt.terminal_observation, null);
    restored.dispose();
  } finally {
    deadline.dispose();
    await rm(directory, { recursive: true, force: true });
  }
});

test('restoring an expired receipt persists overrun on the normalized receipt', async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'oat-terminal-expired-'));
  const deadline = await OatLauncherDeadline.create(600, performance.now(), directory);
  try {
    deadline.dispose();
    const expired = JSON.parse(await readFile(deadline.receiptPath, 'utf8')) as Record<string, unknown>;
    expired.deadline_at_unix_ms = Date.now() - 1;
    expired.overrun = false;
    expired.overrun_at_unix_ms = null;
    await writeFile(deadline.receiptPath, JSON.stringify(expired));
    const restored = await OatLauncherDeadline.restore(deadline.receiptPath);
    const receipt = JSON.parse(await readFile(deadline.receiptPath, 'utf8')) as {
      deadline_at_unix_ms: number; overrun: boolean; overrun_at_unix_ms: number | null;
    };
    assert.equal(receipt.overrun, true);
    assert.ok(receipt.overrun_at_unix_ms !== null && receipt.overrun_at_unix_ms >= receipt.deadline_at_unix_ms);
    restored.dispose();
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test('malformed terminal observation identity is rejected during receipt restore', async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'oat-terminal-malformed-'));
  const runId = '123e4567-e89b-42d3-a456-426614174023';
  const serverUrl = 'http://127.0.0.1:8088';
  const submittedConfigBody = '{"platform":{"kind":"local"}}';
  const submittedConfigSha256 = createHash('sha256').update(submittedConfigBody).digest('hex');
  const deadline = await OatLauncherDeadline.create(600, performance.now(), directory);
  try {
    await deadline.setSubmissionBinding({
      submitted_config_body: submittedConfigBody,
      submitted_config_sha256: submittedConfigSha256,
      source_config_sha256: 'a'.repeat(64),
    });
    await deadline.markCreationPending(serverUrl);
    await deadline.recordRunStarted({ ...status('running'), id: runId });
    await deadline.recordOwnedServerIdentity({
      invocationId: deadline.invocationId, runId, serverUrl, serverPid: process.pid,
      serverStartTicks: 'fixture-start-ticks', serverExecutable: process.execPath,
    });
    await deadline.recordTerminal({
      ...status('succeeded'), id: runId, started_at_unix_ms: Date.now() - 1, finished_at_unix_ms: Date.now(),
    });
    const receipt = JSON.parse(await readFile(deadline.receiptPath, 'utf8')) as {
      terminal_observation: { status: { id: string } };
    };
    receipt.terminal_observation.status.id = '123e4567-e89b-42d3-a456-426614174024';
    await writeFile(deadline.receiptPath, JSON.stringify(receipt));
    await assert.rejects(OatLauncherDeadline.restore(deadline.receiptPath), /Invalid launcher deadline receipt/);
  } finally {
    deadline.dispose();
    await rm(directory, { recursive: true, force: true });
  }
});

test('supplementary observer throw and rejection preserve the actual main scorer and cleanup result', { skip: process.platform !== 'linux' }, async (t) => {
  for (const kind of ['throw', 'reject'] as const) {
    await t.test(kind, async (t) => {
      const fixture = await passingMainFixture({ initialState: 'running' });
      const previousExitCode = process.exitCode;
      const diagnostics: unknown[][] = [];
      t.mock.method(console, 'error', (...args: unknown[]) => { diagnostics.push(args); });
      let calls = 0;
      try {
        process.exitCode = undefined;
        await main(['--deadline-seconds', '10', '--no-viewer', '--config', fixture.configPath], fixture.runner, undefined, fixture.receiptRoot, undefined, () => {
          calls++;
          if (kind === 'throw') throw new Error('synthetic supplementary throw');
          return Promise.reject(new Error('synthetic supplementary rejection'));
        });
        assert.equal(calls, 1);
        assert.equal(process.exitCode, undefined);
        assert.equal(diagnostics.length, 1);
        assert.match(String(diagnostics[0]?.[0]), /Supplementary owned-run observer failed/);
        assert.deepEqual(await readdir(fixture.stateRoot), []);
      } finally {
        process.exitCode = previousExitCode;
        await fixture.stop();
        await rm(fixture.directory, { recursive: true, force: true });
      }
    });
  }
});

test('owned main skips supplementary identity for initial non-running statuses', { skip: process.platform !== 'linux' }, async (t) => {
  for (const initialState of ['succeeded', 'failed', 'cancelled'] as const) {
    await t.test(initialState, async (t) => {
      const fixture = await passingMainFixture({ initialState });
      const previousExitCode = process.exitCode;
      const diagnostics: unknown[][] = [];
      t.mock.method(console, 'error', (...args: unknown[]) => { diagnostics.push(args); });
      let calls = 0;
      try {
        process.exitCode = undefined;
        await main(['--deadline-seconds', '10', '--no-viewer', '--config', fixture.configPath], fixture.runner, undefined,
          fixture.receiptRoot, undefined, () => { calls++; });
        assert.equal(calls, 0);
        assert.equal(process.exitCode, initialState === 'succeeded' ? undefined : 1);
        assert.ok(diagnostics.some(([message]) => /Supplementary owned-run identity skipped/.test(String(message))));
      } finally {
        process.exitCode = previousExitCode;
        await fixture.stop();
        await rm(fixture.directory, { recursive: true, force: true });
      }
    });
  }
});

test('owned main rechecks supplementary eligibility after cancellation during marker persistence', { skip: process.platform !== 'linux', timeout: 15_000 }, async (t) => {
  const fixture = await passingMainFixture({ initialState: 'running', cancelState: 'failed' });
  const previousExitCode = process.exitCode;
  const diagnostics: unknown[][] = [];
  t.mock.method(console, 'error', (...args: unknown[]) => { diagnostics.push(args); });
  const probe = await open(path.join(fixture.directory, 'probe'), 'w');
  const handles = Object.getPrototypeOf(probe) as FileHandle;
  const sync = handles.sync;
  await probe.close();
  let releaseMarker!: () => void;
  const terminalRecorded = new Promise<void>((resolve) => { releaseMarker = resolve; });
  let markerHeld = false;
  const recordTerminal = OatLauncherDeadline.prototype.recordTerminal;
  t.mock.method(OatLauncherDeadline.prototype, 'recordTerminal', async function (this: OatLauncherDeadline, run: OatRunStatus) {
    await recordTerminal.call(this, run);
    if (run.state === 'failed') {
      assert.equal(markerHeld, true);
      assert.equal(this.cleanupState, 'unsettled');
      releaseMarker();
    }
  });
  t.mock.method(handles, 'sync', async function (this: FileHandle) {
    await sync.call(this);
    const file = await readlink(`/proc/self/fd/${this.fd}`);
    if (path.dirname(file) === fixture.stateRoot && path.basename(file).startsWith('.arrowcol-oat-owner-')) {
      markerHeld = true;
      process.emit('SIGTERM');
      await terminalRecorded;
    }
  });
  let calls = 0;
  try {
    process.exitCode = undefined;
    await main(['--deadline-seconds', '10', '--no-viewer', '--config', fixture.configPath], fixture.runner, undefined,
      fixture.receiptRoot, undefined, () => { calls++; });
    assert.equal(markerHeld, true);
    assert.equal(calls, 0);
    assert.equal(process.exitCode, 143);
    const [name] = await readdir(fixture.receiptRoot);
    const receipt = JSON.parse(await readFile(path.join(fixture.receiptRoot, name!), 'utf8'));
    assert.equal(receipt.run_state, 'failed');
    assert.equal(receipt.cleanup_state, 'unsettled');
    assert.equal(receipt.cancellation_attempts, 1);
    assert.ok(diagnostics.some(([message]) => /Supplementary owned-run identity skipped/.test(String(message))));
  } finally {
    releaseMarker();
    process.exitCode = previousExitCode;
    await fixture.stop();
    await rm(fixture.directory, { recursive: true, force: true });
  }
});

test('owned main does not emit supplementary identity for rejected, unknown or unattested runs', { skip: process.platform !== 'linux' }, async (t) => {
  for (const options of [{ rejectRun: true }, { invalidRunId: true }, { configHash: null }, { configHash: '0'.repeat(64) }]) {
    await t.test(JSON.stringify(options), async () => {
      const fixture = await passingMainFixture(options);
      const previousExitCode = process.exitCode;
      let calls = 0;
      try {
        process.exitCode = undefined;
        await main(['--deadline-seconds', '10', '--no-viewer', '--config', fixture.configPath], fixture.runner, undefined, fixture.receiptRoot, undefined, () => { calls++; });
        assert.equal(calls, 0);
        assert.equal(process.exitCode, 1);
        if ('configHash' in options) {
          const [name] = await readdir(fixture.receiptRoot);
          const receipt = JSON.parse(await readFile(path.join(fixture.receiptRoot, name!), 'utf8'));
          assert.equal(receipt.cleanup_state, 'unsettled');
          assert.match(receipt.config_binding_error, /config_sha256/);
          const markers = await readdir(fixture.stateRoot);
          assert.equal(markers.length, 1, 'an unresolved binding must retain the actual server ownership marker');
        }
      } finally {
        process.exitCode = previousExitCode;
        await fixture.stop();
        await rm(fixture.directory, { recursive: true, force: true });
      }
    });
  }
});

test('owned main cannot pass when real scoring crosses the original deadline', { skip: process.platform !== 'linux' }, async (t) => {
  const fixture = await passingMainFixture();
  const previousExitCode = process.exitCode;
  const now = performance.now.bind(performance);
  let offset = 0;
  t.mock.method(performance, 'now', () => now() + offset);
  const collect: typeof collectDataFlow = async (...args) => {
    const verdict = await collectDataFlow(...args);
    assert.equal(verdict.pass, true, verdict.reasons.join('; '));
    offset = 20_000;
    return verdict;
  };
  try {
    process.exitCode = undefined;
    await main(['--deadline-seconds', '10', '--no-viewer', '--config', fixture.configPath], fixture.runner, undefined, fixture.receiptRoot, collect);
    assert.equal(process.exitCode, 1);
    const [name] = await readdir(fixture.receiptRoot);
    const receipt = JSON.parse(await readFile(path.join(fixture.receiptRoot, name!), 'utf8'));
    assert.equal(receipt.overrun, true);
    assert.equal(receipt.run_state, 'succeeded');
    assert.equal(receipt.cancellation_attempts, 0);
  } finally {
    process.exitCode = previousExitCode;
    await fixture.stop();
    await rm(fixture.directory, { recursive: true, force: true });
  }
});

test('external --server submits unchanged configuration without building or attributing that server', async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'oat-external-server-'));
  const configPath = path.join(directory, 'config.json');
  const config = { platform: { kind: 'local' } };
  const receiptRoot = path.join(directory, 'receipts');
  await writeFile(configPath, JSON.stringify(config));
  let posted: unknown;
  let prePostReceipt: Record<string, unknown> | undefined;
  let callbacks = 0;
  const server = createServer(async (request, response) => {
    let body = '';
    for await (const chunk of request) body += chunk;
    posted = JSON.parse(body);
    const [name] = await readdir(receiptRoot);
    prePostReceipt = JSON.parse(await readFile(path.join(receiptRoot, name!), 'utf8'));
    response.setHeader('content-type', 'application/json');
    response.end(JSON.stringify({ ...status('succeeded'), config_sha256: createHash('sha256').update(body).digest('hex') }));
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  assert.ok(address && typeof address !== 'string');
  const previousExitCode = process.exitCode;
  try {
    await main(['--config', configPath, '--server', `http://127.0.0.1:${address.port}`], async () => {
      throw new Error('must not build or inspect source for an independently running server');
    }, undefined, receiptRoot, undefined, () => { callbacks++; });
    assert.equal(process.exitCode, 1);
    assert.deepEqual(posted, config);
    assert.equal(await readFile(configPath, 'utf8'), JSON.stringify(config));
    assert.equal(callbacks, 0);
    assert.equal(prePostReceipt?.submitted_config_body, JSON.stringify(config));
    assert.equal(prePostReceipt?.submitted_config_sha256, createHash('sha256').update(JSON.stringify(config)).digest('hex'));
    assert.equal(prePostReceipt?.deadline_seconds, null);
    assert.equal(prePostReceipt?.deadline_at_monotonic_ms, null);
    assert.equal(prePostReceipt?.deadline_at_unix_ms, null);
  } finally {
    process.exitCode = previousExitCode;
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
    await rm(directory, { recursive: true, force: true });
  }
});

test('external main rechecks the original deadline after final receipt work', { skip: process.platform !== 'linux' }, async (t) => {
  const fixture = await passingMainFixture();
  const server = await startLocalServer(fixture.runner);
  const previousExitCode = process.exitCode;
  const now = performance.now.bind(performance);
  const markNoRun = OatLauncherDeadline.prototype.markNoRun;
  let offset = 0;
  t.mock.method(performance, 'now', () => now() + offset);
  t.mock.method(OatLauncherDeadline.prototype, 'markNoRun', async function (this: OatLauncherDeadline, error?: unknown) {
    await markNoRun.call(this, error);
    offset = 20_000;
  });
  let callbacks = 0;
  const collect: typeof collectDataFlow = (status, config, mode, _sha, runner) => collectDataFlow(status, config, mode, fixture.sha, runner);
  try {
    process.exitCode = undefined;
    await main(['--deadline-seconds', '10', '--no-viewer', '--server', server.url, '--config', fixture.configPath],
      fixture.runner, undefined, fixture.receiptRoot, collect, () => { callbacks++; });
    assert.equal(process.exitCode, 1);
    assert.equal(callbacks, 0);
    const [name] = await readdir(fixture.receiptRoot);
    const receipt = JSON.parse(await readFile(path.join(fixture.receiptRoot, name!), 'utf8'));
    assert.equal(receipt.overrun, true);
    assert.equal(receipt.run_state, 'succeeded');
    assert.equal(receipt.cancellation_attempts, 0);
  } finally {
    process.exitCode = previousExitCode;
    await stopLocalServer(server.child);
    await fixture.stop();
    await rm(fixture.directory, { recursive: true, force: true });
  }
});

test('posts the JSON config and polls until the server publishes results', async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'arrowcol-oat-'));
  const configPath = path.join(directory, 'oat.json');
  await writeFile(configPath, JSON.stringify({ replica_counts: [1, 2] }));
  const calls: Array<{ url: string; method: string }> = [];
  const responses = [
    status('queued'), status('queued'), status('running'), status('running'),
    { ...status('running'), progress: 'measuring' }, status('succeeded'),
  ];
  const fetcher: typeof fetch = async (input, init) => {
    calls.push({ url: String(input), method: init?.method ?? 'GET' });
    const next = responses.shift();
    assert.ok(next);
    return new Response(JSON.stringify(next), { status: 202 });
  };
  const progress: string[] = [];
  try {
    const result = await launchOat(configPath, 'http://127.0.0.1:8088', 1, fetcher, (value) => progress.push(`${value.state}: ${value.progress}`));
    assert.equal(result.result_count, 2);
    assert.deepEqual(progress, ['queued: queued', 'running: running', 'running: measuring', 'succeeded: succeeded']);
    assert.deepEqual(calls.map((call) => call.method), ['POST', 'GET', 'GET', 'GET', 'GET', 'GET']);
    assert.equal(calls[0]?.url, 'http://127.0.0.1:8088/runs');
    assert.equal(calls[1]?.url, 'http://127.0.0.1:8088/runs/123e4567-e89b-42d3-a456-426614174000');
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test('build progress streams before process exit while preserving captured diagnostics', async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'oat-build-progress-'));
  const gate = path.join(directory, 'progress-seen');
  const chunks: string[] = [];
  try {
    const result = await run_tree([process.execPath, '--input-type=module', '-e', `
      import { existsSync } from 'node:fs';
      process.stdout.write('{"reason":"compiler-artifact"}\\n');
      process.stderr.write('Compiling OAT\\n');
      const timer = setInterval(() => {
        if (existsSync(process.argv[1])) {
          clearInterval(timer);
          process.exitCode = 7;
        }
      }, 10);
    `, gate], 10, directory, undefined, (chunk) => {
      chunks.push(chunk);
      writeFileSync(gate, 'seen');
    });
    assert.equal(result.returncode, 7);
    assert.equal(result.stdout, '{"reason":"compiler-artifact"}\n');
    assert.equal(result.stderr, 'Compiling OAT\n');
    assert.equal(chunks.join(''), result.stderr);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test('reports validation errors returned by the server', async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'arrowcol-oat-'));
  const configPath = path.join(directory, 'oat.json');
  await writeFile(configPath, '{}');
  const fetcher: typeof fetch = async () => new Response(JSON.stringify({ error: 'invalid matrix' }), { status: 400 });
  try {
    await assert.rejects(launchOat(configPath, undefined, 1, fetcher), /invalid matrix/);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test('ownership persistence failures cancel the accepted run before propagating the disk error', async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'oat-owner-enospc-'));
  const configPath = path.join(directory, 'config.json');
  await writeFile(configPath, '{}');
  try {
    for (const cleanupFails of [false, true]) {
      const calls: string[] = [];
      let cleanupFinished = false;
      const fetcher: typeof fetch = async (input, init) => {
        const url = new URL(String(input));
        calls.push(`${init?.method ?? 'GET'} ${url.pathname}`);
        if (url.pathname === '/runs') return new Response(JSON.stringify(status('queued')), { status: 202 });
        if (url.pathname.endsWith('/cancel')) {
          return new Response(JSON.stringify(cleanupFails ? { error: 'synthetic cleanup failure' } : {}), { status: cleanupFails ? 500 : 202 });
        }
        return new Response(JSON.stringify({
          ...status('running'), state: 'cancelled', error: 'run cancelled by request',
        }));
      };
      await assert.rejects(launchOat(configPath, undefined, 1, fetcher, () => {}, undefined, undefined, undefined,
        async () => { throw new Error('ENOSPC writing ownership marker'); },
        () => { cleanupFinished = true; }),
      cleanupFails ? /ENOSPC writing ownership marker.*synthetic cleanup failure/ : /ENOSPC writing ownership marker/);
      assert.deepEqual(calls, cleanupFails
        ? ['POST /runs', 'POST /runs/123e4567-e89b-42d3-a456-426614174000/cancel']
        : ['POST /runs', 'POST /runs/123e4567-e89b-42d3-a456-426614174000/cancel', 'GET /runs/123e4567-e89b-42d3-a456-426614174000']);
      assert.equal(cleanupFinished, !cleanupFails);
    }
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test('allows the executable to select the platform mode without editing the config file', async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'arrowcol-oat-'));
  const configPath = path.join(directory, 'oat.json');
  await writeFile(configPath, JSON.stringify({ platform: { kind: 'aks' } }));
  let posted: unknown;
  const fetcher: typeof fetch = async (_input, init) => {
    posted = JSON.parse(String(init?.body));
    return new Response(JSON.stringify(status('succeeded')), { status: 202 });
  };
  try {
    await launchOat(configPath, undefined, 1, fetcher, () => {}, 'local');
    assert.deepEqual(posted, { platform: { kind: 'local' } });
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test('selects a per-mode quick config when no path is supplied', async t => {
  const root = await mkdtemp(path.join(os.tmpdir(), 'oat-quick-config-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  assert.equal(resolveOatConfigPath('local', undefined, root),
    path.join(root, 'docs/experiments/arrowcol-oat/oat-config.quick.json'));
  assert.equal(resolveOatConfigPath('aks', undefined, root),
    path.join(root, 'docs/experiments/arrowcol-oat/oat-config.aks.quick.json'));
  assert.equal(resolveOatConfigPath('aks', 'custom.json', root), 'custom.json');
});

test('fills missing local Docker bridge hosts while preserving explicit values', async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'arrowcol-oat-mode-'));
  const configPath = path.join(directory, 'oat.json');
  let gatewayCalls = 0;
  try {
    await writeFile(configPath, JSON.stringify({
      platform: { kind: 'aks', local: { bind_host: null, endpoint_host: null } },
    }));
    const config = await readOatConfigForMode(configPath, 'local', () => {
      gatewayCalls += 1;
      return '172.18.0.1';
    });
    const platform = config.platform as { kind: string; local: { bind_host: string; endpoint_host: string } };
    assert.equal(platform.kind, 'local');
    assert.equal(platform.local.bind_host, '172.18.0.1');
    assert.equal(platform.local.endpoint_host, '172.18.0.1');
    assert.equal(gatewayCalls, 1);

    await writeFile(configPath, JSON.stringify({
      platform: { kind: 'local', local: { bind_host: '192.0.2.1', endpoint_host: 'oat.example.test' } },
    }));
    const explicit = await readOatConfigForMode(configPath, 'local', () => {
      throw new Error('bridge lookup should not run when both hosts are explicit');
    });
    const explicitPlatform = explicit.platform as { local: { bind_host: string; endpoint_host: string } };
    assert.equal(explicitPlatform.local.bind_host, '192.0.2.1');
    assert.equal(explicitPlatform.local.endpoint_host, 'oat.example.test');
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test('serves local viewer byte ranges, suffix ranges, and HEAD requests', async () => {
  const server = createServer((request, response) => {
    void sendViewerFile(request, response, Buffer.from('abcdef'), 'application/vnd.apache.parquet');
  });
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  const address = server.address();
  assert.ok(address && typeof address !== 'string');
  const url = `http://127.0.0.1:${address.port}/`;
  try {
    const partial = await fetch(url, { headers: { range: 'bytes=1-3' } });
    assert.equal(partial.status, 206);
    assert.equal(partial.headers.get('content-range'), 'bytes 1-3/6');
    assert.equal(await partial.text(), 'bcd');

    const suffix = await fetch(url, { headers: { range: 'bytes=-2' } });
    assert.equal(suffix.status, 206);
    assert.equal(suffix.headers.get('content-range'), 'bytes 4-5/6');
    assert.equal(await suffix.text(), 'ef');

    const head = await fetch(url, { method: 'HEAD' });
    assert.equal(head.status, 200);
    assert.equal(head.headers.get('content-length'), '6');
    assert.equal(await head.text(), '');

    const invalid = await fetch(url, { headers: { range: 'bytes=8-' } });
    assert.equal(invalid.status, 416);
    assert.equal(invalid.headers.get('content-range'), 'bytes */6');
  } finally {
    await new Promise<void>((resolve, reject) => {
      server.close((error) => error ? reject(error) : resolve());
    });
  }
});

test('opens the existing local results in viewer-only mode without losing missing-file errors', async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'arrowcol-oat-viewer-'));
  const configPath = path.join(directory, 'oat.json');
  const resultsPath = path.join(directory, 'results.parquet');
  await writeFile(resultsPath, Buffer.from('PAR1data'));
  await writeFile(configPath, JSON.stringify({
    platform: { kind: 'local' },
    website_url: pathToFileURL(resultsPath).href,
  }));
  let viewer: Awaited<ReturnType<typeof startLocalViewer>> | undefined;
  try {
    viewer = await startLocalViewer(configPath);
    assert.equal(new URL(viewer.url).hostname, '127.0.0.1');

    const viewerUrl = viewer.url;
    const port = new URL(viewerUrl).port;
    const target = new URL(viewerUrl);
    const requestViewer = (headers: Record<string, string>, requestPath = target.pathname) => new Promise<{
      status: number | undefined;
      text: string;
    }>((resolve, reject) => {
      const request = createHttpRequest({
        hostname: target.hostname,
        port: Number(target.port),
        path: requestPath,
        method: 'GET',
        headers,
        agent: false,
      }, (response) => {
        const chunks: Buffer[] = [];
        response.on('data', (chunk: Buffer) => chunks.push(chunk));
        response.on('end', () => resolve({
          status: response.statusCode,
          text: Buffer.concat(chunks).toString('utf8'),
        }));
      });
      request.once('error', reject);
      request.end();
    });
    const localhost = await requestViewer({ host: `localhost:${port}`, origin: `http://localhost:${port}` });
    assert.equal(localhost.status, 200);

    const uppercaseHostAndOrigin = await requestViewer({
      host: `LOCALHOST:${port}`, origin: `HTTP://127.0.0.1:${port}`,
    });
    assert.equal(uppercaseHostAndOrigin.status, 200);

    const duplicateHost = await new Promise<{ status: number | undefined; text: string }>((resolve, reject) => {
      const socket = createTcpConnection({ host: '127.0.0.1', port: Number(port) }, () => {
        socket.write('GET / HTTP/1.1\r\nHost: 127.0.0.1:' + port + '\r\nHost: localhost:' + port + '\r\nConnection: close\r\n\r\n');
      });
      const chunks: string[] = [];
      socket.setTimeout(5000, () => socket.destroy(new Error('Duplicate Host request timed out')));
      socket.on('data', (chunk: Buffer | string) => chunks.push(typeof chunk === 'string' ? chunk : chunk.toString('utf8')));
      socket.once('error', reject);
      socket.once('end', () => {
        const response = chunks.join('');
        const [headers, body = ''] = response.split('\r\n\r\n');
        resolve({ status: Number(headers.match(/^HTTP\/1\.1 (\d{3})/m)?.[1]), text: body.trim() });
      });
    });
    assert.equal(duplicateHost.status, 400);
    assert.match(duplicateHost.text, /Invalid Host header/);

    const notDirectory = await requestViewer({}, '/index.html/child');
    assert.equal(notDirectory.status, 404);
    assert.equal(notDirectory.text, 'Not found');

    const tooLongPath = await requestViewer({}, '/' + 'a'.repeat(12_000));
    assert.equal(tooLongPath.status, 414);

    const invalidHost = await requestViewer({ host: `example.com:${port}` });
    assert.equal(invalidHost.status, 400);
    assert.equal(invalidHost.text, 'Invalid Host header');

    const invalidOrigin = await requestViewer({ origin: 'https://example.com' });
    assert.equal(invalidOrigin.status, 403);
    assert.equal(invalidOrigin.text, 'Origin not allowed');

    const page = await fetch(viewer.url);
    assert.equal(page.status, 200);
    assert.match(await page.text(), /ArrowCol OAT benchmark/);

    const resultsUrl = new URL('/results.parquet', viewer.url);
    const range = await fetch(resultsUrl, { headers: { range: 'bytes=0-3' } });
    assert.equal(range.status, 206);
    assert.equal(range.headers.get('content-range'), 'bytes 0-3/8');
    assert.equal(await range.text(), 'PAR1');

    const head = await fetch(resultsUrl, { method: 'HEAD' });
    assert.equal(head.status, 200);
    assert.equal(head.headers.get('content-length'), '8');
    assert.equal(await head.text(), '');

    await unlink(resultsPath);
    const missing = await fetch(resultsUrl);
    assert.equal(missing.status, 404);
    assert.equal(await missing.text(), 'Not found');
    await assert.rejects(startLocalViewer(configPath), /Local OAT results file does not exist/);
  } finally {
    if (viewer) await viewer.close();
    await rm(directory, { recursive: true, force: true });
  }
});

test('CLI help is available and unknown options fail as usage errors', async () => {
  const originalLog = console.log;
  const messages: string[] = [];
  console.log = (message?: unknown) => messages.push(String(message ?? ''));
  try {
    await main(['--help', '--config', '/missing-oat-config.json']);
  } finally {
    console.log = originalLog;
  }
  assert.match(messages.join('\n'), /Usage:.*arrowcol-oat/s);
  assert.match(messages.join('\n'), /--no-viewer/);
  await assert.rejects(
    main(['--not-a-real-option', '--config', '/missing-oat-config.json']),
    (error: unknown) => error instanceof Error && error.message === 'Unknown option: --not-a-real-option' && 'exitCode' in error && error.exitCode === 2,
  );
  const directory = await mkdtemp(path.join(os.tmpdir(), 'arrowcol-oat-no-viewer-'));
  const previousExitCode = process.exitCode;
  try {
    await assert.rejects(
      main(['--mode', 'local', '--no-viewer', '--config', path.join(directory, 'missing.json')]),
      (error: unknown) => error instanceof Error && 'code' in error && error.code === 'ENOENT',
    );
    await assert.rejects(
      main(['--mode', 'local', '--viewer-only', '--no-viewer', '--config', path.join(directory, 'missing.json')]),
      (error: unknown) => error instanceof Error && error.message.includes('--no-viewer requires a local benchmark run') && 'exitCode' in error && error.exitCode === 2,
    );
  } finally {
    process.exitCode = previousExitCode;
    await rm(directory, { recursive: true, force: true });
  }
});


test('local cleanup removes only the Compose project derived from the supplied cell UUID', async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'oat-cleanup-'));
  const runId = '123e4567-e89b-42d3-a456-426614174000';
  const stateRoot = path.join(directory, 'state');
  const stateDir = path.join(stateRoot, runId);
  await mkdir(stateDir, { recursive: true });
  const project = 'arrowcol-oat-qu-123e4567e89b42d3a456426614174000';
  const otherProject = 'arrowcol-oat-qu-123e4567e89b42d3a456426614174001';
  const calls: string[][] = [];
  const docker = (args: string[]): string => {
    calls.push(args);
    const joined = args.join(' ');
    if (joined === 'ps -aq --filter label=com.docker.compose.project') return 'container-target\ncontainer-other\n';
    if (joined === 'network ls -q --filter label=com.docker.compose.project') return 'network-target\nnetwork-other\n';
    if (joined === 'volume ls -q --filter label=com.docker.compose.project') return 'volume-target\nvolume-other\n';
    if (args[0] === 'inspect' || args[0] === 'network' && args[1] === 'inspect' || args[0] === 'volume' && args[1] === 'inspect') {
      const id = args.at(-1);
      return id?.endsWith('target') ? project : otherProject;
    }
    if (joined.includes('label=com.docker.compose.project=' + project)) return joined.startsWith('ps ') ? 'container-target\n' : joined.startsWith('network ls') ? 'network-target\n' : 'volume-target\n';
    return '';
  };
  try {
    const projects = await cleanupLocalComposeProjects({
      platform: { compose_project: 'arrowcol-oat-quick', compose_state_root: stateRoot },
    }, runId, docker);
    assert.deepEqual(projects, [project]);
    assert.ok(calls.some(args => args[0] === 'rm' && args.includes('container-target')));
    assert.ok(calls.some(args => args[0] === 'network' && args.includes('network-target')));
    assert.ok(calls.some(args => args[0] === 'volume' && args.includes('volume-target')));
    assert.ok(calls.filter(args => args[0] === 'rm' || args[0] === 'network' && args[1] === 'rm' || args[0] === 'volume' && args[1] === 'rm').every(args => !args.includes('container-other') && !args.includes('network-other') && !args.includes('volume-other')));
    await assert.rejects(stat(stateDir), { code: 'ENOENT' });
    await assert.rejects(cleanupLocalComposeProjects({ platform: { compose_project: 'arrowcol-oat-quick' } }, 'not-a-uuid', docker), /canonical OAT cell run UUID/);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test('local cleanup refuses ambiguous legacy project before removing resources or state', async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'oat-legacy-cleanup-'));
  const runId = '123e4567-e89b-42d3-a456-426614174000';
  const stateDir = path.join(directory, runId);
  const calls: string[][] = [];
  await mkdir(stateDir, { recursive: true });
  const docker = (args: string[]): string => {
    calls.push(args);
    if (args.join(' ') === 'ps -aq --filter label=com.docker.compose.project') return 'legacy-container\n';
    if (args[0] === 'inspect') return 'arrowcol-oat-quick-123e4567';
    return '';
  };
  try {
    await assert.rejects(cleanupLocalComposeProjects({ platform: { compose_project: 'arrowcol-oat-quick', compose_state_root: directory } }, runId, docker),
      /Legacy Compose project .* ownership is ambiguous/);
    assert.ok((await stat(stateDir)).isDirectory());
    assert.ok(!calls.some(args => args[0] === 'rm' || args[1] === 'rm'));
  } finally { await rm(directory, { recursive: true, force: true }); }
});


test('local recovery and cleanup resolve relative state beside the owned server, not the caller cwd', { skip: process.platform !== 'linux' }, async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'oat-recovery-cwd-'));
  const workspace = fileURLToPath(new URL('../..', import.meta.url));
  const runId = '123e4567-e89b-42d3-a456-426614174000';
  const stateRoot = path.join(directory, 'state');
  const cellState = path.join(stateRoot, runId);
  const markerPath = path.join(stateRoot, '.arrowcol-oat-owner-2147483647.json');
  const configPath = path.join(directory, 'config.json');
  const config = {
    platform: {
      kind: 'local', compose_project: 'oat-recovery-test',
      compose_state_root: path.relative(workspace, stateRoot),
    },
  };
  await mkdir(cellState, { recursive: true });
  await writeFile(configPath, JSON.stringify(config));
  await writeFile(markerPath, JSON.stringify({
    version: 1, runId, ownerPid: 2147483647, ownerStartTicks: '0',
    serverPid: 2147483647, serverStartTicks: '0',
    serverUrl: 'http://127.0.0.1:12345', serverExecutable: process.execPath,
    submitted_config_body: '{}', submitted_config_sha256: '44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a', source_config_sha256: 'b'.repeat(64),
  }));
  const cwd = process.cwd();
  const callerDirectory = path.join(directory, 'caller', 'nested');
  await mkdir(callerDirectory, { recursive: true });
  process.chdir(callerDirectory);
  try {
    const runner = async () => { throw new Error('startup must remain blocked'); };
    await assert.rejects(main(['--config', configPath], runner, undefined, path.join(directory, 'receipts')), /Interrupted OAT server is gone/);
    await main(['--config', configPath, '--cleanup-local', runId], runner,
      (value, id) => cleanupLocalComposeProjects(value, id, () => ''));
    await assert.rejects(stat(cellState), { code: 'ENOENT' });
    await assert.rejects(stat(markerPath), { code: 'ENOENT' });
  } finally {
    process.chdir(cwd);
    await rm(directory, { recursive: true, force: true });
  }
});

test('local interruption requests cancellation and waits for the server to finish cleanup', async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'oat-cancel-'));
  const configPath = path.join(directory, 'oat.json');
  await writeFile(configPath, JSON.stringify({
    platform: { kind: 'local', local: { bind_host: '127.0.0.1', endpoint_host: '127.0.0.1' } },
  }));
  const requests: string[] = [];
  let status: { id: string; state: string; progress: string; result_count: number; error: string | null } = { id: '123e4567-e89b-42d3-a456-426614174000', state: 'running', progress: 'starting', result_count: 0, error: null };
  const server = createServer((request, response) => {
    requests.push(request.method + ' ' + request.url);
    response.setHeader('content-type', 'application/json');
    if (request.method === 'POST' && request.url === '/runs') {
      response.writeHead(202).end(JSON.stringify(status));
    } else if (request.method === 'POST' && request.url === '/runs/123e4567-e89b-42d3-a456-426614174000/cancel') {
      status = { ...status, state: 'cancelled', progress: 'local resources cleaned', error: 'run cancelled by request' };
      response.writeHead(202).end();
    } else {
      response.end(JSON.stringify(status));
    }
  });
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  const address = server.address();
  assert.ok(address && typeof address !== 'string');
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), 10);
  try {
    await assert.rejects(
      launchOat(configPath, 'http://127.0.0.1:' + address.port, 1, fetch, () => {}, 'local', () => '127.0.0.1', controller.signal),
      /run cancelled by request/,
    );
    assert.ok(requests.includes('POST /runs/123e4567-e89b-42d3-a456-426614174000/cancel'));
    assert.ok(requests.some(request => request === 'GET /runs/123e4567-e89b-42d3-a456-426614174000'));
    assert.equal(status.state, 'cancelled');
  } finally {
    clearTimeout(timer);
    await new Promise<void>((resolve, reject) => server.close(error => error ? reject(error) : resolve()));
    await rm(directory, { recursive: true, force: true });
  }
});

test('owned server is isolated from the launcher foreground process group on Linux', { skip: process.platform !== 'linux' }, async () => {
  const fixture = await ownedBuildFixture();
  let server: Awaited<ReturnType<typeof startLocalServer>> | undefined;
  try {
    server = await startLocalServer(fixture.runner);
    const serverGroup = execFileSync('ps', ['-o', 'pgid=', '-p', String(server.child.pid)], { encoding: 'utf8' }).trim();
    const launcherGroup = execFileSync('ps', ['-o', 'pgid=', '-p', String(process.pid)], { encoding: 'utf8' }).trim();
    assert.equal(serverGroup, String(server.child.pid));
    assert.notEqual(serverGroup, launcherGroup);
  } finally {
    if (server) await stopLocalServer(server.child);
    await rm(fixture.directory, { recursive: true, force: true });
  }
});

test('a signal received during owned-server teardown prevents viewer startup', async () => {
  let signal: NodeJS.Signals | undefined;
  let releaseStop!: () => void;
  let disposed = false;
  let viewerStarted = false;
  const previousExitCode = process.exitCode;
  const stopping = new Promise<void>((resolve) => { releaseStop = resolve; });
  const interrupt = {
    get signal() { return signal; },
    get exitCode() { return signal === 'SIGTERM' ? 143 : 0; },
    dispose() { disposed = true; },
  };
  try {
    const finishing = finishOwnedLocalRun(
      async () => stopping,
      async () => {},
      interrupt,
      async () => { viewerStarted = true; },
    );
    signal = 'SIGTERM';
    releaseStop();
    await finishing;
    assert.equal(process.exitCode, 143);
    assert.equal(viewerStarted, false);
    assert.equal(disposed, true);
  } finally {
    process.exitCode = previousExitCode;
  }
});

test('cleanup-local config resolution skips its UUID and honors positional and explicit config paths', async t => {
  const root = await mkdtemp(path.join(os.tmpdir(), 'oat-cleanup-config-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const runId = '123e4567-e89b-42d3-a456-426614174000';
  assert.equal(resolveCliConfigPath(['--cleanup-local', runId], 'local', root),
    path.join(root, 'docs/experiments/arrowcol-oat/oat-config.quick.json'));
  assert.equal(resolveCliConfigPath(['--cleanup-local', runId, './after.json'], 'local', root), './after.json');
  assert.equal(resolveCliConfigPath(['--cleanup-local', runId, '--config', './explicit.json'], 'local', root), './explicit.json');
});

test('immediately propagates cancellation HTTP and transport errors with a long poll interval', async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'oat-cancel-error-'));
  const configPath = path.join(directory, 'oat.json');
  await writeFile(configPath, JSON.stringify({ platform: { kind: 'local' } }));
  try {
    for (const failure of ['http', 'transport'] as const) {
      const controller = new AbortController();
      const timer = setTimeout(() => controller.abort(), 10);
      const fetcher: typeof fetch = async (input) => {
        const url = new URL(String(input));
        if (url.pathname === '/runs') return new Response(JSON.stringify(status('running')), { status: 202 });
        if (url.pathname.endsWith('/cancel')) {
          if (failure === 'transport') throw new Error('synthetic cancel transport failure');
          return new Response(JSON.stringify({ error: 'synthetic cancel HTTP failure' }), { status: 500 });
        }
        return new Response(JSON.stringify(status('running')), { status: 200 });
      };
      const started = Date.now();
      try {
        await assert.rejects(
          launchOat(configPath, undefined, 10_000, fetcher, () => {}, 'local', () => '127.0.0.1', controller.signal),
          failure === 'transport' ? /synthetic cancel transport failure/ : /synthetic cancel HTTP failure/,
        );
        assert.ok(Date.now() - started < 1_000, 'cancellation failure should not wait for the polling interval');
      } finally {
        clearTimeout(timer);
      }
    }
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test('failed interrupted cleanup blocks startup and retains evidence until explicit local cleanup', { skip: process.platform !== 'linux' }, async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'oat-recovery-failed-'));
  const stateRoot = path.join(directory, 'state');
  const cellRunId = '123e4567-e89b-42d3-a456-426614174000';
  const cellState = path.join(stateRoot, cellRunId);
  const markerPath = path.join(stateRoot, '.arrowcol-oat-owner-123e4567-e89b-42d3-a456-426614174000.json');
  const configPath = path.join(directory, 'oat.json');
  const serverScript = path.join(directory, 'server.cjs');
  const reservation = createServer();
  await new Promise<void>((resolve, reject) => {
    reservation.once('error', reject);
    reservation.listen(0, '127.0.0.1', resolve);
  });
  const address = reservation.address();
  assert.ok(address && typeof address !== 'string');
  await new Promise<void>((resolve, reject) => reservation.close((error) => error ? reject(error) : resolve()));
  await mkdir(cellState, { recursive: true });
  await writeFile(configPath, JSON.stringify({
    platform: {
      kind: 'local', compose_project: 'arrowcol-oat-test', compose_state_root: stateRoot,
      local: { bind_host: '127.0.0.1', endpoint_host: '127.0.0.1' },
    },
  }));
  await writeFile(serverScript, `
const http = require('node:http');
let state = 'running';
const server = http.createServer((request, response) => {
  response.setHeader('content-type', 'application/json');
  if (request.method === 'POST' && request.url === '/runs/123e4567-e89b-42d3-a456-426614174000/cancel') {
    state = 'failed';
    response.writeHead(202).end(JSON.stringify({ id: '123e4567-e89b-42d3-a456-426614174000', state, progress: 'cleanup failed', result_count: 0, error: 'synthetic Compose cleanup failure' }));
    return;
  }
  response.end(JSON.stringify({ id: '123e4567-e89b-42d3-a456-426614174000', config_sha256: '44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a', state, progress: state, result_count: 0, error: state === 'failed' ? 'synthetic Compose cleanup failure' : null }));
});
server.listen(Number(process.argv[2]), '127.0.0.1');
`);
  const child = spawn(process.execPath, [serverScript, String(address.port)], { stdio: 'ignore' });
  const childExit = once(child, 'exit');
  try {
    assert.ok(child.pid);
    const serverUrl = `http://127.0.0.1:${address.port}`;
    const readyBy = Date.now() + 5_000;
    while (Date.now() < readyBy) {
      try {
        const response = await fetch(new URL('/runs/123e4567-e89b-42d3-a456-426614174000', serverUrl));
        if (response.ok) break;
      } catch {}
      await new Promise((resolve) => setTimeout(resolve, 20));
    }
    const processStat = await readFile(`/proc/${child.pid}/stat`, 'utf8');
    const serverStartTicks = processStat.slice(processStat.lastIndexOf(')') + 2).trim().split(/\s+/)[19];
    assert.ok(serverStartTicks);
    await writeFile(markerPath, JSON.stringify({
      version: 1, runId: '123e4567-e89b-42d3-a456-426614174000', ownerPid: 2147483647, ownerStartTicks: '0',
      serverPid: child.pid, serverStartTicks, serverUrl, serverExecutable: await realpath(process.execPath),
      submitted_config_body: '{}', submitted_config_sha256: '44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a', source_config_sha256: 'b'.repeat(64),
    }));
    const runner = async () => { throw new Error('startup must remain blocked'); };
    await assert.rejects(main(['--config', configPath], runner, undefined, path.join(directory, 'receipts')), /synthetic Compose cleanup failure/);
    assert.ok((await stat(markerPath)).isFile());
    assert.ok((await stat(cellState)).isDirectory());
    assert.equal((await fetch(new URL('/runs/123e4567-e89b-42d3-a456-426614174000', serverUrl))).ok, true);

    const cleaned: string[] = [];
    await main(['--config', configPath, '--cleanup-local', cellRunId], runner, async (_config, runId) => {
      cleaned.push(runId);
      await rm(path.join(stateRoot, runId), { recursive: true, force: true });
      return [];
    });
    assert.deepEqual(cleaned, [cellRunId]);
    await assert.rejects(stat(markerPath), { code: 'ENOENT' });
    await assert.rejects(stat(cellState), { code: 'ENOENT' });
    await childExit;
  } finally {
    if (child.exitCode === null && child.signalCode === null && child.pid) child.kill('SIGTERM');
    if (child.exitCode === null && child.signalCode === null) await childExit;
    await rm(directory, { recursive: true, force: true });
  }
});

test('deadline strictly validates its CLI values and preserves default behavior when absent', async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'oat-deadline-cli-'));
  const configPath = path.join(directory, 'config.json');
  await writeFile(configPath, JSON.stringify({ platform: { kind: 'local', local: { bind_host: '127.0.0.1', endpoint_host: '127.0.0.1' } } }));
  try {
    for (const value of ['0', '01', '1.5', '+1', '601', '9999']) {
      await assert.rejects(main(['--deadline-seconds', value, '--no-viewer', '--config', configPath]), /--deadline-seconds must be an integer/);
    }
    await assert.rejects(main(['--deadline-seconds', '1', '--config', configPath]), /requires a local benchmark run with --no-viewer/);
    await assert.rejects(main(['--deadline-seconds', '1', '--deadline-seconds', '2', '--no-viewer', '--config', configPath]), /may be specified only once/);
    await assert.rejects(main(['--deadline-seconds', '1', '--no-viewer', '--server', 'http://example.test', '--config', configPath]), /requires a loopback --server URL/);
    await assert.rejects(main(['--deadline-seconds', '1', '--no-viewer', '--mode', 'aks', '--config', configPath]), /requires a local benchmark run/);
  } finally {
    await rm(directory, { recursive: true, force: true });
  }
});

test('expired original monotonic deadline is not reset and prevents POST /runs', async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'oat-deadline-original-'));
  const configPath = path.join(directory, 'config.json');
  const receiptRoot = path.join(directory, 'receipts');
  await writeFile(configPath, JSON.stringify({ platform: { kind: 'local' } }));
  const startedAt = performance.now() - 1500;
  const deadline = await OatLauncherDeadline.create(1, startedAt, receiptRoot);
  let posts = 0;
  const fetcher: typeof fetch = async () => {
    posts++;
    throw new Error('POST must not start after the deadline');
  };
  try {
    await assert.rejects(
      launchOat(configPath, 'http://127.0.0.1:8088', 1, fetcher, () => {}, 'local', () => '127.0.0.1', undefined, undefined, undefined, deadline),
      /Launcher-start deadline expired before OAT run creation/,
    );
    const receipt = JSON.parse(await readFile(deadline.receiptPath, 'utf8')) as {
      started_at_monotonic_ms: number; deadline_at_monotonic_ms: number; overrun: boolean; creation_state: string;
    };
    assert.equal(posts, 0);
    assert.equal(receipt.started_at_monotonic_ms, startedAt);
    assert.equal(receipt.deadline_at_monotonic_ms, startedAt + 1000);
    assert.equal(receipt.overrun, true);
    assert.equal(receipt.creation_state, 'not_started');
  } finally {
    deadline.dispose();
    await rm(directory, { recursive: true, force: true });
  }
});

test('abort during POST pins the accepted running identity before once-only cancellation', async (t) => {
  for (const seconds of [600, undefined]) {
    await t.test(seconds === undefined ? 'receipt without deadline' : 'before original deadline', async () => {
      const directory = await mkdtemp(path.join(os.tmpdir(), 'oat-aborted-post-'));
      const configPath = path.join(directory, 'config.json');
      await writeFile(configPath, JSON.stringify({ platform: { kind: 'local' } }));
      const deadline = await OatLauncherDeadline.create(seconds, performance.now(), path.join(directory, 'receipts'));
      const controller = new AbortController();
      const runId = '123e4567-e89b-42d3-a456-426614174019';
      const requests: string[] = [];
      let configHash: string;
      let cancelCalls = 0;
      const server = createServer(async (request, response) => {
        requests.push(`${request.method} ${request.url}`);
        response.setHeader('content-type', 'application/json');
        if (request.method === 'POST' && request.url === '/runs') {
          const chunks: Buffer[] = [];
          for await (const chunk of request) chunks.push(Buffer.from(chunk));
          configHash = createHash('sha256').update(Buffer.concat(chunks)).digest('hex');
          assert.equal(deadline.isExpired(), false);
          controller.abort();
          response.writeHead(202).end(JSON.stringify({ ...status('running'), id: runId, config_sha256: configHash }));
        } else if (request.method === 'POST' && request.url === `/runs/${runId}/cancel`) {
          cancelCalls++;
          response.writeHead(202).end();
        } else if (request.method === 'GET' && request.url === `/runs/${runId}`) {
          response.end(JSON.stringify({ ...status(cancelCalls ? 'cancelled' : 'running'), id: runId, config_sha256: configHash,
            error: cancelCalls ? 'cancelled by request' : null }));
        } else {
          response.writeHead(404).end();
        }
      });
      await new Promise<void>((resolve, reject) => {
        server.once('error', reject);
        server.listen(0, '127.0.0.1', resolve);
      });
      const address = server.address();
      assert.ok(address && typeof address !== 'string');
      try {
        let failure: unknown;
        try {
          await launchOat(configPath, `http://127.0.0.1:${address.port}`, 1, fetch, () => {}, 'local',
            () => '127.0.0.1', controller.signal, undefined, undefined, deadline);
        } catch (error) {
          failure = error;
        }
        assert.equal(cancelCalls, 1);
        assert.ok(failure instanceof Error && /cancelled by request/.test(failure.message));
        assert.deepEqual(requests, ['POST /runs', `GET /runs/${runId}`, `POST /runs/${runId}/cancel`, `GET /runs/${runId}`]);
        const receipt = JSON.parse(await readFile(deadline.receiptPath, 'utf8')) as Record<string, unknown>;
        assert.equal(receipt.run_id, runId);
        assert.equal(receipt.run_state, 'cancelled');
        assert.equal(receipt.cleanup_state, 'confirmed');
        assert.equal(receipt.cancellation_attempts, 1);
        assert.equal(receipt.config_binding_error, null);
        assert.equal(receipt.overrun, false);
        assert.equal(deadline.isExpired(), false);
        if (seconds === undefined) assert.equal(receipt.deadline_at_unix_ms, null);
      } finally {
        deadline.dispose();
        await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
        await rm(directory, { recursive: true, force: true });
      }
    });
  }
});

test('launcher deadline cancels the exact active run and reconciles a lost cancel response to terminal state', async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'oat-deadline-cancel-'));
  const configPath = path.join(directory, 'config.json');
  const receiptRoot = path.join(directory, 'receipts');
  const runId = '123e4567-e89b-42d3-a456-426614174000';
  await writeFile(configPath, JSON.stringify({ platform: { kind: 'local' } }));
  const deadline = await OatLauncherDeadline.create(1, performance.now(), receiptRoot);
  let state: OatRunStatus['state'] = 'running';
  let cancelCalls = 0;
  const server = createServer((request, response) => {
    response.setHeader('content-type', 'application/json');
    if (request.method === 'POST' && request.url === '/runs') {
      response.writeHead(202).end(JSON.stringify({ ...status('running'), id: runId }));
    } else if (request.method === 'POST' && request.url === `/runs/${runId}/cancel`) {
      cancelCalls++;
      state = 'cancelled';
      response.writeHead(500).end(JSON.stringify({ error: 'acceptance response lost after cancel' }));
    } else {
      response.end(JSON.stringify({ ...status(state === 'succeeded' ? 'succeeded' : 'running'), id: runId, state,
        error: state === 'cancelled' ? 'cancelled by request' : null }));
    }
  });
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  const address = server.address();
  assert.ok(address && typeof address !== 'string');
  try {
    await assert.rejects(
      launchOat(configPath, `http://127.0.0.1:${address.port}`, 10_000, fetch, () => {}, 'local', () => '127.0.0.1', undefined, undefined, undefined, deadline),
      /Launcher-start deadline expired/,
    );
    const receipt = JSON.parse(await readFile(deadline.receiptPath, 'utf8')) as {
      run_id: string; run_state: string; overrun: boolean; cleanup_state: string; cancellation_attempts: number; last_cancel_result: string;
    };
    assert.equal(cancelCalls, 1);
    assert.deepEqual({
      run_id: receipt.run_id, run_state: receipt.run_state, overrun: receipt.overrun,
      cleanup_state: receipt.cleanup_state, cancellation_attempts: receipt.cancellation_attempts,
      last_cancel_result: receipt.last_cancel_result,
    }, {
      run_id: runId, run_state: 'cancelled', overrun: true,
      cleanup_state: 'confirmed', cancellation_attempts: 1, last_cancel_result: 'response_unknown',
    });
  } finally {
    deadline.dispose();
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
    await rm(directory, { recursive: true, force: true });
  }
});

test('deadline does not dispatch cancellation when cleanup budget cannot be persisted', async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'oat-deadline-receipt-retry-'));
  const configPath = path.join(directory, 'config.json');
  const receiptRoot = path.join(directory, 'receipts');
  const runId = '123e4567-e89b-42d3-a456-426614174004';
  await writeFile(configPath, JSON.stringify({ platform: { kind: 'local' } }));
  const deadline = await OatLauncherDeadline.create(1, performance.now(), receiptRoot);
  let cancelCalls = 0;
  const server = createServer((request, response) => {
    response.setHeader('content-type', 'application/json');
    if (request.method === 'POST' && request.url === '/runs') {
      response.writeHead(202).end(JSON.stringify({ ...status('running'), id: runId }));
      setTimeout(() => {
        void (async () => {
          await rm(receiptRoot, { recursive: true, force: true });
          await writeFile(receiptRoot, 'temporary persistence failure');
        })();
      }, 100);
    } else if (request.method === 'POST' && request.url === `/runs/${runId}/cancel`) {
      cancelCalls++;
      response.writeHead(202).end();
    } else {
      response.end(JSON.stringify({ ...status('running'), id: runId }));
    }
  });
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  const address = server.address();
  assert.ok(address && typeof address !== 'string');
  try {
    await assert.rejects(
      launchOat(configPath, `http://127.0.0.1:${address.port}`, 10_000, fetch, () => {}, 'local', () => '127.0.0.1', undefined, undefined, undefined, deadline),
      /EEXIST|cleanup deadline|receipt/i,
    );
    assert.equal(cancelCalls, 0);
  } finally {
    deadline.dispose();
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
    await rm(directory, { recursive: true, force: true });
  }
});
test('launcher never reports success when a run wins the race after the original deadline', async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'oat-deadline-no-success-'));
  const configPath = path.join(directory, 'config.json');
  const receiptRoot = path.join(directory, 'receipts');
  const runId = '123e4567-e89b-42d3-a456-426614174001';
  await writeFile(configPath, JSON.stringify({ platform: { kind: 'local' } }));
  const deadline = await OatLauncherDeadline.create(1, performance.now(), receiptRoot);
  let state: OatRunStatus['state'] = 'running';
  const server = createServer((request, response) => {
    response.setHeader('content-type', 'application/json');
    if (request.method === 'POST' && request.url === '/runs') {
      response.writeHead(202).end(JSON.stringify({ ...status('running'), id: runId }));
    } else if (request.method === 'POST' && request.url === `/runs/${runId}/cancel`) {
      state = 'succeeded';
      response.writeHead(202).end();
    } else {
      response.end(JSON.stringify({ ...status(state === 'succeeded' ? 'succeeded' : 'running'), id: runId, state }));
    }
  });
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  const address = server.address();
  assert.ok(address && typeof address !== 'string');
  try {
    await assert.rejects(
      launchOat(configPath, `http://127.0.0.1:${address.port}`, 10_000, fetch, () => {}, 'local', () => '127.0.0.1', undefined, undefined, undefined, deadline),
      /Launcher-start deadline expired/,
    );
    const receipt = JSON.parse(await readFile(deadline.receiptPath, 'utf8')) as { run_state: string; overrun: boolean; cleanup_state: string };
    assert.equal(receipt.run_state, 'succeeded');
    assert.equal(receipt.overrun, true);
    assert.equal(receipt.cleanup_state, 'confirmed');
  } finally {
    deadline.dispose();
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
    await rm(directory, { recursive: true, force: true });
  }
});

test('lost POST /runs response leaves an unknown owned receipt and blocks the next cell without retrying', async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'oat-deadline-lost-create-'));
  const configPath = path.join(directory, 'config.json');
  await writeFile(configPath, JSON.stringify({ platform: { kind: 'local', local: { bind_host: '127.0.0.1', endpoint_host: '127.0.0.1' } } }));
  const deadline = await OatLauncherDeadline.create(1);
  let posts = 0;
  let cancels = 0;
  const server = createServer((request, response) => {
    if (request.method === 'POST' && request.url === '/runs') {
      posts++;
      setTimeout(() => {
        response.writeHead(202, { 'content-type': 'application/json' });
        response.flushHeaders();
        response.socket?.destroy();
      }, 1100);
      return;
    }
    if (request.url?.endsWith('/cancel')) cancels++;
    response.writeHead(404).end();
  });
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  const address = server.address();
  assert.ok(address && typeof address !== 'string');
  try {
    await assert.rejects(
      launchOat(configPath, `http://127.0.0.1:${address.port}`, 10, fetch, () => {}, 'local', () => '127.0.0.1', undefined, undefined, undefined, deadline),
    );
    const receipt = JSON.parse(await readFile(deadline.receiptPath, 'utf8')) as {
      run_id: string | null; creation_state: string; cleanup_state: string; overrun: boolean;
    };
    assert.equal(posts, 1);
    assert.equal(cancels, 0);
    assert.equal(receipt.run_id, null);
    assert.equal(receipt.creation_state, 'unknown');
    assert.equal(receipt.cleanup_state, 'unsettled');
    assert.equal(receipt.overrun, true);
    await assert.rejects(
      main(['--deadline-seconds', '5', '--no-viewer', '--config', configPath], async () => {
        throw new Error('must not build or create a next cell');
      }),
      /Unsettled launcher deadline receipt .*run identity UNKNOWN/,
    );
  } finally {
    deadline.dispose();
    await unlink(deadline.receiptPath).catch((error: unknown) => {
      if (!(error instanceof Error && 'code' in error && error.code === 'ENOENT')) throw error;
    });
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
    await rm(directory, { recursive: true, force: true });
  }
});

test('lost cancel response uses only GET reconciliation while the exact run remains active', async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'oat-deadline-retry-'));
  const configPath = path.join(directory, 'config.json');
  const receiptRoot = path.join(directory, 'receipts');
  const runId = '123e4567-e89b-42d3-a456-426614174002';
  await writeFile(configPath, JSON.stringify({ platform: { kind: 'local' } }));
  const deadline = await OatLauncherDeadline.create(1, performance.now(), receiptRoot);
  let state: OatRunStatus['state'] = 'running';
  let cancelCalls = 0;
  let readsAfterCancel = 0;
  const requests: string[] = [];
  const server = createServer((request, response) => {
    requests.push(`${request.method} ${request.url}`);
    response.setHeader('content-type', 'application/json');
    if (request.method === 'POST' && request.url === '/runs') {
      response.writeHead(202).end(JSON.stringify({ ...status('running'), id: runId }));
    } else if (request.method === 'POST' && request.url === `/runs/${runId}/cancel`) {
      cancelCalls++;
      response.writeHead(500).end(JSON.stringify({ error: 'temporary response failure' }));
    } else {
      if (cancelCalls && ++readsAfterCancel === 3) state = 'cancelled';
      response.end(JSON.stringify({ ...status(state === 'succeeded' ? 'succeeded' : 'running'), id: runId, state,
        error: state === 'cancelled' ? 'cancelled by request' : null }));
    }
  });
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  const address = server.address();
  assert.ok(address && typeof address !== 'string');
  try {
    await assert.rejects(
      launchOat(configPath, `http://127.0.0.1:${address.port}`, 1, fetch, () => {}, 'local', () => '127.0.0.1', undefined, undefined, undefined, deadline),
      /Launcher-start deadline expired/,
    );
    const firstCancel = requests.indexOf(`POST /runs/${runId}/cancel`);
    const confirmingRead = requests.indexOf(`GET /runs/${runId}`, firstCancel + 1);
    assert.equal(cancelCalls, 1);
    assert.ok(confirmingRead > firstCancel);
    assert.equal(readsAfterCancel, 3);
  } finally {
    deadline.dispose();
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
    await rm(directory, { recursive: true, force: true });
  }
});

test('known-ID deadline receipt reconciles terminal status before allowing a next cell', async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'oat-deadline-recover-'));
  const runId = '123e4567-e89b-42d3-a456-426614174003';
  const deadline = await OatLauncherDeadline.create(600, performance.now(), directory);
  await bindLocalReceipt(deadline);
  const server = createServer((request, response) => {
    response.setHeader('content-type', 'application/json');
    if (request.method === 'GET' && request.url === `/runs/${runId}`) {
      response.end(JSON.stringify({ ...status('succeeded'), id: runId }));
    } else {
      response.writeHead(404).end();
    }
  });
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  const address = server.address();
  assert.ok(address && typeof address !== 'string');
  try {
    await deadline.markCreationPending(`http://127.0.0.1:${address.port}`);
    await deadline.recordRunStarted({ ...status('running'), id: runId });
    deadline.dispose();
    await assertNoUnsettledDeadlineReceipts(directory);
    const receipt = JSON.parse(await readFile(deadline.receiptPath, 'utf8')) as { run_state: string; cleanup_state: string };
    assert.deepEqual({ run_state: receipt.run_state, cleanup_state: receipt.cleanup_state }, { run_state: 'succeeded', cleanup_state: 'confirmed' });
  } finally {
    deadline.dispose();
    await unlink(deadline.receiptPath).catch((error: unknown) => {
      if (!(error instanceof Error && 'code' in error && error.code === 'ENOENT')) throw error;
    });
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
    await rm(directory, { recursive: true, force: true });
  }
});

test('requires server config attestation and cancels the exact known run on missing or mismatched hashes', async (t) => {
  for (const [label, hash] of [['missing', undefined], ['mismatched', '0'.repeat(64)]] as const) {
    await t.test(label, async () => {
      const directory = await mkdtemp(path.join(os.tmpdir(), 'oat-config-attestation-'));
      const configPath = path.join(directory, 'config.json');
      const receiptRoot = path.join(directory, 'receipts');
      const runId = '123e4567-e89b-42d3-a456-426614174010';
      await writeFile(configPath, JSON.stringify({ platform: { kind: 'local' } }));
      const deadline = await OatLauncherDeadline.create(600, performance.now(), receiptRoot);
      const submitted_config_body = '{"platform":{"kind":"local"}}';
      const binding = {
        submitted_config_body,
        submitted_config_sha256: createHash('sha256').update(submitted_config_body).digest('hex'),
        source_config_sha256: createHash('sha256').update(await readFile(configPath)).digest('hex'),
      };
      let state: OatRunStatus['state'] = 'running';
      let cancelCalls = 0;
      let activeReads = 0;
      const server = createServer((request, response) => {
        response.setHeader('content-type', 'application/json');
        if (request.method === 'POST' && request.url === '/runs') {
          const initial = { ...status('running'), id: runId, config_sha256: hash };
          response.writeHead(202).end(JSON.stringify(initial));
        } else if (request.method === 'POST' && request.url === `/runs/${runId}/cancel`) {
          cancelCalls++;
          state = 'cancelled';
          response.writeHead(202).end();
        } else {
          if (state !== 'cancelled' && ++activeReads >= 3) state = 'succeeded';
          response.end(JSON.stringify({ ...status(state === 'cancelled' ? 'succeeded' : state), id: runId, state }));
        }
      });
      await new Promise<void>((resolve, reject) => {
        server.once('error', reject);
        server.listen(0, '127.0.0.1', resolve);
      });
      const address = server.address();
      assert.ok(address && typeof address !== 'string');
      try {
        await assert.rejects(
          launchOat(configPath, `http://127.0.0.1:${address.port}`, 1, fetch, () => {}, 'local', () => '127.0.0.1', undefined, undefined, undefined, deadline, binding),
          /config_sha256/,
        );
        assert.equal(cancelCalls, 1);
        assert.equal(deadline.runId, runId);
        assert.equal(deadline.cleanupState, 'unsettled');
        await assert.rejects(assertNoUnsettledDeadlineReceipts(receiptRoot), /config hash mismatch|unsettled/i);
      } finally {
        deadline.dispose();
        await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
        await rm(directory, { recursive: true, force: true });
      }
    });
  }
});

test('persists exact submitted and source bytes before POST /runs', async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'oat-submission-receipt-'));
  const configPath = path.join(directory, 'config.json');
  const receiptRoot = path.join(directory, 'receipts');
  const sourceBytes = Buffer.from('{\n  "platform": { "kind": "local" }\n}\n');
  await writeFile(configPath, sourceBytes);
  const deadline = await OatLauncherDeadline.create(600, performance.now(), receiptRoot);
  const runId = '123e4567-e89b-42d3-a456-426614174018';
  let bodyAtPost: string | undefined;
  let receiptAtPost: Record<string, unknown> | undefined;
  const server = createServer(async (request, response) => {
    if (request.method === 'POST' && request.url === '/runs') {
      const chunks: Buffer[] = [];
      for await (const chunk of request) chunks.push(Buffer.from(chunk));
      const bytes = Buffer.concat(chunks);
      bodyAtPost = bytes.toString('utf8');
      receiptAtPost = JSON.parse(await readFile(deadline.receiptPath, 'utf8')) as Record<string, unknown>;
      response.setHeader('content-type', 'application/json');
      response.end(JSON.stringify({ ...status('succeeded'), id: runId, config_sha256: createHash('sha256').update(bytes).digest('hex') }));
    }
  });
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  const address = server.address();
  assert.ok(address && typeof address !== 'string');
  try {
    await launchOat(configPath, `http://127.0.0.1:${address.port}`, 1, fetch, () => {}, 'local', () => '127.0.0.1', undefined, undefined, undefined, deadline);
    assert.ok(bodyAtPost);
    assert.equal(receiptAtPost?.submitted_config_body, bodyAtPost);
    assert.equal(receiptAtPost?.submitted_config_sha256, createHash('sha256').update(bodyAtPost!).digest('hex'));
    assert.equal(receiptAtPost?.source_config_sha256, createHash('sha256').update(sourceBytes).digest('hex'));
  } finally {
    deadline.dispose();
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
    await rm(directory, { recursive: true, force: true });
  }
});

test('failed terminal cleanup remains unsettled and blocks the next cell', async (t) => {
  for (const phase of ['initial', 'poll', 'cancel'] as const) {
    await t.test(phase, async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'oat-failed-cleanup-gate-'));
  const configPath = path.join(directory, 'config.json');
  const receiptRoot = path.join(directory, 'receipts');
  const runId = '123e4567-e89b-42d3-a456-426614174011';
  await writeFile(configPath, JSON.stringify({ platform: { kind: 'local' } }));
  const deadline = await OatLauncherDeadline.create(600, performance.now(), receiptRoot);
  const controller = new AbortController();
  let cancelCalls = 0;
  const server = createServer((request, response) => {
    response.setHeader('content-type', 'application/json');
    if (request.method === 'POST' && request.url === '/runs') {
      response.writeHead(202).end(JSON.stringify({ ...status(phase === 'initial' ? 'failed' : 'running'), id: runId, error: 'cleanup failed' }));
    } else if (request.method === 'POST' && request.url === `/runs/${runId}/cancel`) {
      cancelCalls++;
      response.writeHead(202).end();
    } else {
      response.end(JSON.stringify({ ...status(phase === 'cancel' && !cancelCalls ? 'running' : 'failed'), id: runId, error: 'cleanup failed' }));
    }
  });
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  const address = server.address();
  assert.ok(address && typeof address !== 'string');
  try {
    await assert.rejects(
      launchOat(configPath, `http://127.0.0.1:${address.port}`, 1, fetch, () => {}, 'local', () => '127.0.0.1', controller.signal,
        async () => { if (phase === 'cancel') controller.abort(); }, undefined, deadline),
      /failed/,
    );
    assert.equal(cancelCalls, phase === 'cancel' ? 1 : 0);
    let builds = 0;
    await assert.rejects(main(['--no-viewer', '--config', configPath], async () => {
      builds++;
      throw new Error('unexpected build');
    }, undefined, receiptRoot), /failed|unsettled/i);
    assert.equal(builds, 0);
    assert.equal(deadline.cleanupState, 'unsettled');
    await assert.rejects(assertNoUnsettledDeadlineReceipts(receiptRoot), /failed|unsettled/i);
  } finally {
    deadline.dispose();
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
    await rm(directory, { recursive: true, force: true });
  }
    });
  }
});

test('restoring durable cancellation intent permits GETs but never another POST', async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'oat-once-only-recovery-'));
  const deadline = await OatLauncherDeadline.create(600, performance.now(), directory);
  await bindLocalReceipt(deadline);
  const runId = status('running').id;
  let reads = 0;
  let cancels = 0;
  const server = createServer((request, response) => {
    response.setHeader('content-type', 'application/json');
    if (request.method === 'POST') {
      cancels++;
      response.writeHead(202).end();
    } else {
      reads++;
      response.end(JSON.stringify(status(reads < 2 ? 'running' : 'cancelled')));
    }
  });
  await new Promise<void>((resolve) => server.listen(0, '127.0.0.1', resolve));
  const address = server.address();
  assert.ok(address && typeof address !== 'string');
  try {
    await deadline.markCreationPending(`http://127.0.0.1:${address.port}`);
    await deadline.recordRunStarted(status('running'));
    const cleanupDeadline = await deadline.ensureCleanupDeadline();
    await deadline.markCancelAttempt();
    await deadline.markCancelResult('response_unknown');
    await assertNoUnsettledDeadlineReceipts(directory);
    const receipt = JSON.parse(await readFile(deadline.receiptPath, 'utf8'));
    assert.equal(receipt.run_id, runId);
    assert.equal(receipt.cancellation_attempts, 1);
    assert.equal(receipt.cleanup_deadline_at_unix_ms, cleanupDeadline);
    assert.equal(receipt.cleanup_state, 'confirmed');
    assert.equal(cancels, 0);
    assert.equal(reads, 2);
  } finally {
    deadline.dispose();
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
    await rm(directory, { recursive: true, force: true });
  }
});

test('malformed initial run identity is unknown and never becomes a cancellation target', async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'oat-invalid-run-id-'));
  const configPath = path.join(directory, 'config.json');
  const receiptRoot = path.join(directory, 'receipts');
  await writeFile(configPath, JSON.stringify({ platform: { kind: 'local' } }));
  const deadline = await OatLauncherDeadline.create(600, performance.now(), receiptRoot);
  const requests: string[] = [];
  const server = createServer((request, response) => {
    requests.push(`${request.method} ${request.url}`);
    response.setHeader('content-type', 'application/json');
    if (request.method === 'POST' && request.url === '/runs') {
      response.writeHead(202).end(JSON.stringify({ ...status('running'), id: 'not-a-uuid' }));
    } else {
      response.end(JSON.stringify({ ...status('succeeded'), id: 'not-a-uuid' }));
    }
  });
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  const address = server.address();
  assert.ok(address && typeof address !== 'string');
  try {
    await assert.rejects(
      launchOat(configPath, `http://127.0.0.1:${address.port}`, 1, fetch, () => {}, 'local', () => '127.0.0.1', undefined, undefined, undefined, deadline),
      /run ID|identity/i,
    );
    assert.deepEqual(requests, ['POST /runs']);
    assert.equal(deadline.runId, null);
  } finally {
    deadline.dispose();
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
    await rm(directory, { recursive: true, force: true });
  }
});

test('poll identity mismatch never redirects status or cancellation to another run', async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'oat-poll-id-mismatch-'));
  const configPath = path.join(directory, 'config.json');
  const receiptRoot = path.join(directory, 'receipts');
  const runA = '123e4567-e89b-42d3-a456-426614174012';
  const runB = '123e4567-e89b-42d3-a456-426614174013';
  await writeFile(configPath, JSON.stringify({ platform: { kind: 'local' } }));
  const deadline = await OatLauncherDeadline.create(600, performance.now(), receiptRoot);
  const requests: string[] = [];
  const server = createServer((request, response) => {
    requests.push(`${request.method} ${request.url}`);
    response.setHeader('content-type', 'application/json');
    if (request.method === 'POST' && request.url === '/runs') {
      response.writeHead(202).end(JSON.stringify({ ...status('running'), id: runA }));
    } else if (request.url === `/runs/${runA}`) {
      response.end(JSON.stringify({ ...status('running'), id: runB }));
    } else if (request.url === `/runs/${runB}`) {
      response.end(JSON.stringify({ ...status('succeeded'), id: runB }));
    } else {
      response.writeHead(404).end();
    }
  });
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  const address = server.address();
  assert.ok(address && typeof address !== 'string');
  try {
    await assert.rejects(
      launchOat(configPath, `http://127.0.0.1:${address.port}`, 1, fetch, () => {}, 'local', () => '127.0.0.1', undefined, undefined, undefined, deadline),
      /identity|run ID/i,
    );
    assert.ok(requests.every((request) => !request.includes(runB)));
    assert.equal(deadline.cleanupState, 'unsettled');
  } finally {
    deadline.dispose();
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
    await rm(directory, { recursive: true, force: true });
  }
});

test('receipt reconciliation rejects a terminal status for a different run ID', async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'oat-reconcile-id-mismatch-'));
  const runA = '123e4567-e89b-42d3-a456-426614174014';
  const runB = '123e4567-e89b-42d3-a456-426614174015';
  const deadline = await OatLauncherDeadline.create(600, performance.now(), directory);
  await bindLocalReceipt(deadline);
  const server = createServer((_request, response) => {
    response.setHeader('content-type', 'application/json');
    response.end(JSON.stringify({ ...status('succeeded'), id: runB }));
  });
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  const address = server.address();
  assert.ok(address && typeof address !== 'string');
  try {
    await deadline.markCreationPending(`http://127.0.0.1:${address.port}`);
    await deadline.recordRunStarted({ ...status('running'), id: runA });
    await assert.rejects(assertNoUnsettledDeadlineReceipts(directory), /identity|run ID|Unsettled launcher deadline receipt/i);
    assert.equal(deadline.runId, runA);
    const receipt = JSON.parse(await readFile(deadline.receiptPath, 'utf8')) as { cleanup_state: string };
    assert.equal(receipt.cleanup_state, 'unsettled');
  } finally {
    deadline.dispose();
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
    await rm(directory, { recursive: true, force: true });
  }
});

test('expired persisted cleanup budget does not grant another cancellation allowance', async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'oat-cleanup-budget-exhausted-'));
  const runId = '123e4567-e89b-42d3-a456-426614174016';
  const deadline = await OatLauncherDeadline.create(600, performance.now(), directory);
  await bindLocalReceipt(deadline);
  let cancelCalls = 0;
  let state: OatRunStatus['state'] = 'running';
  const server = createServer((request, response) => {
    response.setHeader('content-type', 'application/json');
    if (request.method === 'POST' && request.url === `/runs/${runId}/cancel`) {
      cancelCalls++;
      state = 'cancelled';
      response.writeHead(202).end();
    } else {
      response.end(JSON.stringify({ ...status(state === 'cancelled' ? 'succeeded' : 'running'), id: runId, state }));
    }
  });
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  const address = server.address();
  assert.ok(address && typeof address !== 'string');
  try {
    await deadline.markCreationPending(`http://127.0.0.1:${address.port}`);
    await deadline.recordRunStarted({ ...status('running'), id: runId });
    const receipt = JSON.parse(await readFile(deadline.receiptPath, 'utf8')) as Record<string, unknown>;
    receipt.cleanup_deadline_at_unix_ms = Date.now() - 1;
    await writeFile(deadline.receiptPath, JSON.stringify(receipt));
    await assert.rejects(assertNoUnsettledDeadlineReceipts(directory), /budget|deadline|unsettled/i);
    assert.equal(cancelCalls, 0);
  } finally {
    deadline.dispose();
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
    await rm(directory, { recursive: true, force: true });
  }
});

test('receipt write crossing cleanup deadline prevents cancellation dispatch', async () => {
  const directory = await mkdtemp(path.join(os.tmpdir(), 'oat-cleanup-budget-write-'));
  const runId = '123e4567-e89b-42d3-a456-426614174017';
  const deadline = await OatLauncherDeadline.create(600, performance.now(), directory);
  await bindLocalReceipt(deadline);
  let cancelCalls = 0;
  let state: OatRunStatus['state'] = 'running';
  const server = createServer((request, response) => {
    response.setHeader('content-type', 'application/json');
    if (request.method === 'POST' && request.url === `/runs/${runId}/cancel`) {
      cancelCalls++;
      state = 'cancelled';
      response.writeHead(202).end();
    } else {
      response.end(JSON.stringify({ ...status(state === 'cancelled' ? 'succeeded' : 'running'), id: runId, state }));
    }
  });
  await new Promise<void>((resolve, reject) => {
    server.once('error', reject);
    server.listen(0, '127.0.0.1', resolve);
  });
  const address = server.address();
  assert.ok(address && typeof address !== 'string');
  try {
    await deadline.markCreationPending(`http://127.0.0.1:${address.port}`);
    await deadline.recordRunStarted({ ...status('running'), id: runId });
    const receipt = JSON.parse(await readFile(deadline.receiptPath, 'utf8')) as Record<string, unknown>;
    receipt.cleanup_deadline_at_unix_ms = Date.now() + 100;
    await writeFile(deadline.receiptPath, JSON.stringify(receipt));
    const slowWriter = async (target: string, value: unknown): Promise<void> => {
      await new Promise((resolve) => setTimeout(resolve, 150));
      await writeFile(target, JSON.stringify(value), { mode: 0o600 });
    };
    await assert.rejects(assertNoUnsettledDeadlineReceipts(directory, slowWriter), /budget|deadline|unsettled/i);
    assert.equal(cancelCalls, 0);
  } finally {
    deadline.dispose();
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
    await rm(directory, { recursive: true, force: true });
  }
});