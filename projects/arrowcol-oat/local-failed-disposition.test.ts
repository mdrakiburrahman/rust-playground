import assert from 'node:assert/strict';
import { createHash } from 'node:crypto';
import { writeFileSync } from 'node:fs';
import { chmod, cp, mkdir, mkdtemp, readFile, rm, symlink, unlink, writeFile } from 'node:fs/promises';
import os from 'node:os';
import path from 'node:path';
import test from 'node:test';
import { fileURLToPath, pathToFileURL } from 'node:url';
import { tableFromArrays, tableFromIPC, tableToIPC } from 'apache-arrow';
import { Table as WasmTable, readParquet, writeParquet } from 'parquet-wasm';
import { assertLocalFailedDisposition, inspectPinnedPreloadFailure, localDispositionSuffix, recordLocalFailedDisposition, reconcileAndQuarantineLocalFailedReceipt } from './local-failed-disposition.js';
import { assertNoUnsettledDeadlineReceipts, main as launchMain } from './launch.js';
import { readReconciledOatResult } from '../../tools/darwin/src/oat-result.js';
import { runOat } from '../../tools/darwin/src/replay.js';

for (const resultStatus of ['failed', 'pinning_violation']) test(`local ${resultStatus} disposition preserves evidence and refuses changed or live custody`, async t => {
  const root = await mkdtemp(path.join(os.tmpdir(), 'oat-local-disposition-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const receiptRoot = path.join(root, 'receipts');
  const runRoot = path.join(root, 'run');
  const stateRoot = path.join(runRoot, 'state');
  await mkdir(receiptRoot);
  await mkdir(stateRoot, { recursive: true });
  const invocation = '123e4567-e89b-42d3-a456-426614174001';
  const run = '123e4567-e89b-42d3-a456-426614174002';
  const cell = '123e4567-e89b-42d3-a456-426614174003';
  const resultPath = path.join(runRoot, 'results.parquet');
  const markerPath = path.join(stateRoot, '.arrowcol-oat-owner-123.json');
  const logPath = path.join(runRoot, 'run.log');
  const receiptPath = path.join(receiptRoot, `${invocation}.json`);
  const project = 'arrowcol-oat-hi-123e4567e89b42d3a456426614174003';
  const body = JSON.stringify({
    platform: { kind: 'local', compose_project: 'arrowcol-oat-historical-parity', compose_state_root: stateRoot },
    result_url: pathToFileURL(resultPath).href, tables: ['test_table'], replica_counts: [1], dfe_worker_concurrency: [32],
  });

  if (resultStatus === 'failed') await t.test('pinned pre-load inspection is read-only and rejects mismatched or post-load evidence', async t => {
    const root = await mkdtemp(path.join(os.tmpdir(), 'oat-preload-proof-'));
    t.after(() => rm(root, { recursive: true, force: true }));
    const receiptRoot = path.join(root, '.darwin', 'pressure-diagnostic', 'trusted', 'runtime', 'out', 'arrowcol-oat', 'deadline-receipts');
    const runRoot = path.join(root, 'run');
    const stateRoot = path.join(runRoot, 'state');
    await mkdir(receiptRoot, { recursive: true });
    await mkdir(stateRoot, { recursive: true });
    const invocation = '123e4567-e89b-42d3-a456-426614174001';
    const run = '123e4567-e89b-42d3-a456-426614174002';
    const cell = '123e4567-e89b-42d3-a456-426614174003';
    const resultPath = path.join(runRoot, 'results.parquet');
    const markerPath = path.join(stateRoot, '.arrowcol-oat-owner-123.json');
    const logPath = path.join(runRoot, 'run.log');
    const receiptPath = path.join(receiptRoot, `${invocation}.json`);
    const project = 'arrowcol-oat-hi-123e4567e89b42d3a456426614174003';
    const collectorImage = `sha256:${'b'.repeat(64)}`;
    const collectorRevision = 'c'.repeat(40);
    const body = JSON.stringify({
      platform: { kind: 'local', compose_project: 'arrowcol-oat-historical-parity', compose_state_root: stateRoot,
        local: { collector_image_override: { image_id: collectorImage, source_revision: collectorRevision } } },
      result_url: pathToFileURL(resultPath).href, tables: ['test_table'], replica_counts: [1], dfe_worker_concurrency: [32],
    });
    const hash = (bytes: Uint8Array | string) => createHash('sha256').update(bytes).digest('hex');
    const configHash = hash(body);
    const marker = {
      version: 1, runId: run, ownerPid: 2147483611, ownerStartTicks: '100', serverPid: 2147483612, serverStartTicks: '200',
      serverUrl: 'http://127.0.0.1:1234/', serverExecutable: '/tmp/oat',
      submitted_config_body: body, submitted_config_sha256: configHash, source_config_sha256: 'a'.repeat(64),
    };
    const startupError = `prepare isolated OAT matrix-cell platform: start remaining local OAT stack failed: dependency failed to start: container ${project}-dataflow-1-1 exited (127)`;
    const receipt = {
      version: 1, invocation_id: invocation, run_id: run, creation_state: 'accepted', run_state: 'failed',
      started_at_unix_ms: 1, started_at_monotonic_ms: 1, deadline_at_unix_ms: null,
      deadline_seconds: null, deadline_at_monotonic_ms: null, cleanup_deadline_at_unix_ms: null,
      overrun: false, overrun_at_unix_ms: null, cancellation_attempts: 0, last_cancel_result: null, error: null,
      cleanup_state: 'unsettled', config_binding_error: null,
      server_pid: 2147483612, server_start_ticks: '200', server_executable: '/tmp/oat', server_url: marker.serverUrl,
      submitted_config_body: body, submitted_config_sha256: configHash, source_config_sha256: marker.source_config_sha256,
      terminal_observation: {
        observed_at_unix_ms: 10, status: { id: run, state: 'failed', config_sha256: configHash, progress: 'failed', result_count: 1,
          error: startupError, started_at_unix_ms: 2, finished_at_unix_ms: 9 },
      },
    };
    const evidence = {
      overload_guard: null, overload_stop: null, preload_delta: null, durable_rows_per_second: null,
      client_schedule: null, client_phase_reports: null, durable_commits: null, durable_bytes: null,
      rows: { delta_committed: null },
    };
    const writeResult = async (resultEvidence: object, orchestration = run) => {
      const table = tableFromArrays({
        schema_version: [3], git_sha: [collectorRevision], pinning_ok: [true],
        run_id: [cell], mode: ['local'], status: ['failed'], table: ['test_table'], replicas: [1], dfe_worker_concurrency: [32],
        tunables_json: [JSON.stringify({ orchestration_run_id: orchestration, dfe_worker_concurrency: 32 })],
        evidence_json: [JSON.stringify(resultEvidence)],
      });
      await writeFile(resultPath, writeParquet(WasmTable.fromIPCStream(tableToIPC(table, 'stream'))));
    };
    await writeResult(evidence);
    await writeFile(receiptPath, JSON.stringify(receipt));
    await writeFile(markerPath, JSON.stringify(marker));
    await writeFile(logPath, `${run} ${project} ${startupError}\n[oat] Preserving OAT server for unresolved run receipt ${receiptPath}`);
    const files = [receiptPath, markerPath, resultPath, logPath];
    const original = await Promise.all(files.map(file => readFile(file)));
    const pins = {
      invocation, run, cell, table: 'test_table', collectorImage, collectorRevision,
      receiptSha256: hash(original[0]!), markerSha256: hash(original[1]!),
      resultSha256: hash(original[2]!), logSha256: hash(original[3]!),
    };
    const env = {
      docker: (_args: string[]) => '',
      processStartTicks: async (_pid: number) => undefined,
      bootId: async () => '123e4567-e89b-42d3-a456-426614174004',
      listeningSockets: (_port: number) => '',
    };
    const proof = await inspectPinnedPreloadFailure(receiptPath, markerPath, pins, env);
    assert.equal(proof.kind, 'verified-preload-failed-cleanup-read-only');
    assert.equal(proof.disposition, 'not-recorded');
    assert.equal(proof.underlying_error, 'not-established-from-retained-artifacts');
    assert.deepEqual(await Promise.all(files.map(file => readFile(file))), original);
    await recordLocalFailedDisposition(receiptRoot, invocation, markerPath, env);
    await unlink(receiptPath + localDispositionSuffix);
    const sourcePath = fileURLToPath(new URL('./local-failed-disposition.ts', import.meta.url));
    const source = await readFile(sourcePath, 'utf8');
    const pinDeclaration = /const reviewedPreloadPins: PreloadProofPins = Object\.freeze\(\{[\s\S]*?\n\}\);/g;
    assert.equal([...source.matchAll(pinDeclaration)].length, 1);
    const fixtureSource = source.replace(pinDeclaration,
      `const reviewedPreloadPins: PreloadProofPins = Object.freeze(${JSON.stringify(pins)});`);
    const moduleRoot = await mkdtemp(path.join(path.dirname(sourcePath), '.preload-fixture-'));
    t.after(() => rm(moduleRoot, { recursive: true, force: true }));
    const modulePath = path.join(moduleRoot, 'local-failed-disposition.ts');
    await writeFile(modulePath, fixtureSource.replace("from './campaign-results.js'", "from '../campaign-results.js'"));
    const fixture: typeof import('./local-failed-disposition.js') = await import(pathToFileURL(modulePath).href);
    const readonlyCommands: string[][] = [];
    const recordingEnv = { ...env, bootId: process.platform === 'linux'
      ? async () => (await readFile('/proc/sys/kernel/random/boot_id', 'utf8')).trim() : env.bootId,
      docker: (args: string[]) => {
      assert.ok(args[0] === 'ps' || (args[0] === 'network' && args[1] === 'ls') ||
        (args[0] === 'volume' && args[1] === 'ls'));
      readonlyCommands.push(args);
      return '';
    } };
    const dispositionPath = receiptPath + localDispositionSuffix;
    const conflictingPath = receiptPath + '.failed-aks-disposition';
    await writeFile(conflictingPath, 'conflict');
    await assert.rejects(fixture.recordLocalFailedDisposition(receiptRoot, invocation, markerPath, recordingEnv),
      /Conflicting AKS disposition/);
    await unlink(conflictingPath);
    let queries = 0;
    await assert.rejects(fixture.recordLocalFailedDisposition(receiptRoot, invocation, markerPath, {
      ...recordingEnv, docker: args => {
        if (++queries === 8) writeFileSync(logPath, 'changed during inspection');
        return recordingEnv.docker(args);
      },
    }), /hash mismatch|evidence changed/);
    await assert.rejects(readFile(dispositionPath), { code: 'ENOENT' });
    await writeFile(logPath, original[3]!);
    let runs = 0;
    await launchMain(['--recover-failed-local', invocation, markerPath], async () => {
      runs++;
      throw new Error('unexpected workload launch');
    }, undefined, receiptRoot, undefined, undefined, undefined,
    (root, id, marker) => fixture.recordLocalFailedDisposition(root, id, marker, recordingEnv));
    assert.equal(runs, 0);
    await fixture.assertLocalFailedDisposition(receiptPath, dispositionPath, recordingEnv);
    const savedDisposition = await readFile(dispositionPath);
    const disposition = JSON.parse(savedDisposition.toString('utf8'));
    assert.equal(disposition.kind, 'verified-preload-failed-cleanup');
    assert.equal(disposition.disposition, 'recorded');
    assert.equal(disposition.qualification, 'not-evaluated');
    assert.ok(readonlyCommands.length > 0);
    assert.deepEqual(await Promise.all(files.map(file => readFile(file))), original);
    const savedRow = tableFromIPC(readParquet(await readFile(resultPath)).intoIPCStream()).toArray()[0];
    assert.equal(savedRow.status, 'failed');
    assert.equal(JSON.parse(savedRow.evidence_json).durable_rows_per_second, null);
    await t.test('actual diagnostic launcher checks the original nonempty ledger before workloads', { skip: process.platform !== 'linux' }, async () => {
      const sourceRoot = path.resolve(path.dirname(sourcePath), '../..');
      const trusted = path.join(root, '.darwin', 'pressure-diagnostic', 'trusted');
      const runtime = path.join(root, '.darwin', 'pressure-diagnostic-v2', 'trusted', 'runtime');
      const toolDir = path.join(root, 'tools');
      const home = path.join(root, 'home');
      await mkdir(path.join(runtime, 'projects'), { recursive: true });
      await mkdir(path.join(runtime, 'infra'), { recursive: true });
      await mkdir(toolDir);
      await mkdir(home);
      await cp(path.join(sourceRoot, 'projects', 'arrowcol-oat'), path.join(runtime, 'projects', 'arrowcol-oat'), { recursive: true });
      await cp(path.join(sourceRoot, 'infra', 'azure'), path.join(runtime, 'infra', 'azure'), { recursive: true });
      await writeFile(path.join(runtime, 'package.json'), '{"type":"module"}\n');
      await symlink(path.resolve(fileURLToPath(import.meta.resolve('tsx')), '../../..'), path.join(runtime, 'node_modules'));
      await writeFile(path.join(runtime, 'projects', 'arrowcol-oat', 'local-failed-disposition.ts'), fixtureSource);
      const launcherPath = path.join(runtime, 'projects', 'arrowcol-oat', 'launch.ts');
      const launcherSource = await readFile(launcherPath, 'utf8');
      await writeFile(launcherPath, launcherSource.replaceAll('89fdefa5-855d-46ea-93ea-481fd77cbd2e', invocation));
      const revision = '86eb8ebe07d876c925e027c0c656a69917269f22';
      await writeFile(path.join(trusted, 'manifest.json'), JSON.stringify({ commit: revision }));
      await writeFile(path.join(trusted, 'producer.json'), JSON.stringify({ revision }));
      const git = path.join(toolDir, 'git');
      await writeFile(git, `#!/bin/sh\ncase "$3 $4" in\n  "rev-parse --show-toplevel") echo "$2";;\n  "rev-parse HEAD") echo ${revision};;\n  "status --porcelain"|"submodule status") :;;\n  *) exit 1;;\nesac\n`);
      const docker = path.join(toolDir, 'docker');
      await writeFile(docker, '#!/bin/sh\nexit 0\n');
      await chmod(git, 0o755);
      await chmod(docker, 0o755);
      const config = path.join(root, 'config.json');
      await writeFile(config, JSON.stringify({ platform: { kind: 'local', local: { bind_host: '127.0.0.1', endpoint_host: '127.0.0.1' } } }));
      const check = (name: string) => runOat(runtime, config, home, toolDir, path.join(root, `${name}.log`), 8 * 1024 * 1024, true, undefined, true);
      const v2 = path.join(root, '.darwin', 'pressure-diagnostic-v2');
      const v2Trusted = path.join(v2, 'trusted');
      const seedDir = path.join(v2Trusted, 'src', 'experiments', 'darwin');
      await mkdir(seedDir, { recursive: true });
      await cp(toolDir, path.join(v2Trusted, 'bin'), { recursive: true });
      const seed = await readFile(path.join(sourceRoot, 'experiments', 'darwin', 'historical-parity.oat.json'));
      await writeFile(path.join(seedDir, 'historical-parity.oat.json'), seed);
      const newRevision = 'd'.repeat(40);
      await writeFile(path.join(v2Trusted, 'manifest.json'), JSON.stringify({
        commit: newRevision, files: { 'experiments/darwin/historical-parity.oat.json': { sha256: hash(seed) } },
      }));
      await writeFile(path.join(v2Trusted, 'producer.json'), JSON.stringify({ revision: newRevision }));
      const tools = path.join(runtime, 'tools', 'darwin', 'src');
      await mkdir(path.dirname(tools), { recursive: true });
      await cp(path.join(sourceRoot, 'tools', 'darwin', 'src'), tools, { recursive: true });
      const parityPath = path.join(tools, 'parity-replay.ts');
      let paritySource = await readFile(parityPath, 'utf8');
      const substitutions: Array<[string, string]> = [
        ['const { runtime, binaryHash } = await prepareReplayRuntime(repo, home, stackBytes);',
          `const runtime = ${JSON.stringify(runtime)}; const binaryHash = "fixture";`],
        ['const footer = await buildFooterInspector(runtime, join(trusted, "prepare-home"), stackBytes);',
          'const footer = { path: "unused", sha256: "unused" };'],
        ['const host = await inspectParityHost(Number(client.max_memory_bytes), Number(local.collector_memory_limit_bytes));',
          'const host = { fixture: true };'],
        ['return execFileSync("git", ["-C", runtime, ...args], { encoding: "utf8", env }).trimEnd();',
          `return args[0] === "rev-parse" ? ${JSON.stringify(newRevision)} : "";`],
        ['|| !hasPinnedSubmodules(runtime, env)) throw new Error("trusted parity runtime changed while building");',
          ') throw new Error("trusted parity runtime changed while building");'],
      ];
      for (const [from, to] of substitutions) {
        assert.ok(paritySource.includes(from), `fixture substitution not found: ${from}`);
        paritySource = paritySource.replace(from, to);
      }
      await writeFile(parityPath, paritySource);
      const parity: typeof import('../../tools/darwin/src/parity-replay.js') = await import(pathToFileURL(parityPath).href);
      const preflight = () => parity.replayParityArm(sourceRoot, v2, 1, '127.0.0.1', '127.0.0.1',
        8 * 1024 * 1024, undefined, undefined, undefined, undefined, true);
      await check('intact');
      assert.match(await readFile(path.join(root, 'intact.log'), 'utf8'), /ledger prelaunch gate passed/);
      const parityResult = await preflight();
      assert.equal(parityResult.prelaunch_only, true);
      assert.equal(parityResult.qualification, 'not-evaluated');
      const other = path.join(receiptRoot, '123e4567-e89b-42d3-a456-426614174099.json');
      const otherReceipt = JSON.stringify({ ...receipt, invocation_id: '123e4567-e89b-42d3-a456-426614174099' });
      await writeFile(other, otherReceipt);
      await assert.rejects(check('other-unsettled'), /OAT run failed/);
      assert.match(await readFile(path.join(root, 'other-unsettled.log'), 'utf8'), /refusing diagnostic transition without modifying it/);
      assert.equal(await readFile(other, 'utf8'), otherReceipt);
      await unlink(other);
      await assert.rejects(readFile(path.join(runtime, 'out', 'arrowcol-oat', 'deadline-receipts', `${invocation}.json`)), { code: 'ENOENT' });
      await unlink(dispositionPath);
      await assert.rejects(check('missing'), /OAT run failed/);
      assert.match(await readFile(path.join(root, 'missing.log'), 'utf8'), /ENOENT/);
      await assert.rejects(runOat(runtime, config, home, toolDir, path.join(root, 'normal-missing.log'),
        8 * 1024 * 1024, true), /OAT run failed/);
      assert.match(await readFile(path.join(root, 'normal-missing.log'), 'utf8'), /ENOENT/);
      await assert.rejects(preflight(), /OAT run failed/);
      await writeFile(dispositionPath, JSON.stringify({ ...disposition, qualification: 'qualified' }));
      await assert.rejects(check('changed-proof'), /OAT run failed/);
      assert.match(await readFile(path.join(root, 'changed-proof.log'), 'utf8'), /evidence changed/);
      await assert.rejects(preflight(), /OAT run failed/);
      await writeFile(dispositionPath, savedDisposition);
      await writeFile(logPath, 'changed');
      await assert.rejects(check('changed-original'), /OAT run failed/);
      assert.match(await readFile(path.join(root, 'changed-original.log'), 'utf8'), /hash mismatch/);
      await assert.rejects(preflight(), /OAT run failed/);
      await writeFile(logPath, original[3]!);
      assert.deepEqual(await Promise.all(files.map(file => readFile(file))), original);
      assert.deepEqual(await readFile(dispositionPath), savedDisposition);
    });
    await assert.rejects(fixture.recordLocalFailedDisposition(receiptRoot, invocation, markerPath, recordingEnv), { code: 'EEXIST' });
    assert.deepEqual(await readFile(dispositionPath), savedDisposition);
    await writeFile(dispositionPath, JSON.stringify({ ...disposition, qualification: 'qualified' }));
    await assert.rejects(fixture.assertLocalFailedDisposition(receiptPath, dispositionPath, recordingEnv), /evidence changed/);
    await writeFile(dispositionPath, savedDisposition);
    await writeFile(conflictingPath, 'conflict');
    await assert.rejects(fixture.assertLocalFailedDisposition(receiptPath, dispositionPath, recordingEnv), /Conflicting AKS disposition/);
    await unlink(conflictingPath);
    await writeFile(logPath, 'changed');
    await assert.rejects(fixture.assertLocalFailedDisposition(receiptPath, dispositionPath, recordingEnv), /hash mismatch/);
    await assert.rejects(fixture.recordLocalFailedDisposition(receiptRoot, invocation, markerPath, recordingEnv), /hash mismatch/);
    await writeFile(logPath, original[3]!);
    await assert.rejects(readReconciledOatResult(resultPath, runRoot, 'test_table', collectorRevision, null, 'diagnostic',
      { config: {}, decodeFile: async () => 0 }), /status is invalid/);
    await assert.rejects(inspectPinnedPreloadFailure(receiptPath, markerPath, { ...pins, run: cell }, env), /pinned identity/);
    await assert.rejects(inspectPinnedPreloadFailure(receiptPath, markerPath, pins, {
      ...env, docker: () => 'residual-container',
    }), /resources remain/);
    await assert.rejects(inspectPinnedPreloadFailure(receiptPath, markerPath, pins, {
      ...env, processStartTicks: async pid => pid === 2147483611 ? '100' : undefined,
    }), /still active/);
    await assert.rejects(inspectPinnedPreloadFailure(receiptPath, markerPath, {
      ...pins, collectorImage: `sha256:${'d'.repeat(64)}`,
    }, env), /collector image/);
    await writeResult({ ...evidence, client_schedule: { started: true } });
    const postLoadHash = hash(await readFile(resultPath));
    await assert.rejects(inspectPinnedPreloadFailure(receiptPath, markerPath, {
      ...pins, resultSha256: postLoadHash,
    }, env), /Pre-load failed cell/);
    await writeResult(evidence, cell);
    const wrongRunHash = hash(await readFile(resultPath));
    await assert.rejects(inspectPinnedPreloadFailure(receiptPath, markerPath, {
      ...pins, resultSha256: wrongRunHash,
    }, env), /Pre-load failed cell/);
    await assert.rejects(inspectPinnedPreloadFailure(receiptPath, markerPath, pins, env), /result or log hash mismatch/);
    await writeFile(resultPath, original[2]!);
    await writeFile(logPath, `${run} ${project} ${startupError}`);
    await assert.rejects(inspectPinnedPreloadFailure(receiptPath, markerPath, {
      ...pins, logSha256: hash(await readFile(logPath)),
    }, env), /Run log does not bind/);
    await writeFile(logPath, original[3]!);
    await writeFile(path.join(stateRoot, 'unexpected'), 'residual');
    await assert.rejects(inspectPinnedPreloadFailure(receiptPath, markerPath, pins, env), /unresolved cell or owner/);
  });
  const hash = createHash('sha256').update(body).digest('hex');
  const marker = {
    version: 1, runId: run, ownerPid: 123, ownerStartTicks: '100', serverPid: 124, serverStartTicks: '200',
    serverUrl: 'http://127.0.0.1:1234/', serverExecutable: '/tmp/oat',
    submitted_config_body: body, submitted_config_sha256: hash, source_config_sha256: 'a'.repeat(64),
  };
  const error = resultStatus === 'failed' ? 'guard unavailable' : 'pinning_violation: no active owner';
  const receipt = {
    version: 1, invocation_id: invocation, run_id: run, creation_state: 'accepted', run_state: 'failed',
    started_at_unix_ms: 1,
    cleanup_state: 'unsettled', config_binding_error: null, server_pid: 124, server_start_ticks: '200',
    server_executable: '/tmp/oat', server_url: marker.serverUrl, submitted_config_body: body,
    submitted_config_sha256: hash, source_config_sha256: marker.source_config_sha256,
    terminal_observation: {
      observed_at_unix_ms: 10, status: { id: run, state: 'failed', config_sha256: hash, error, started_at_unix_ms: 2, finished_at_unix_ms: 9 },
    },
  };
  const table = tableFromArrays({
    run_id: [cell], mode: ['local'], status: [resultStatus], pinning_ok: [false], table: ['test_table'],
    evidence_json: [JSON.stringify({ overload_guard: { run_id: cell }, rows: { delta_committed: null } })],
  });
  await writeFile(resultPath, writeParquet(WasmTable.fromIPCStream(tableToIPC(table, 'stream'))));
  await writeFile(receiptPath, JSON.stringify(receipt));
  await writeFile(markerPath, JSON.stringify(marker));
  await writeFile(logPath, `${run} ${project} ${error}`);
  const originals = await Promise.all([receiptPath, markerPath, resultPath, logPath].map(file => readFile(file)));
  const env = {
    docker: (_args: string[]) => '',
    processStartTicks: async (_pid: number) => undefined,
    bootId: async () => '123e4567-e89b-42d3-a456-426614174004',
      listeningSockets: (_port: number) => '',
  };
  if (resultStatus === 'failed') {
    const dispositionPath = receiptPath + localDispositionSuffix;
    for (const quota of [0.01, 2, 'unlimited', 2.000000001, 0.0100000001]) {
      const config = JSON.parse(body);
      config.platform.local = { collector_cpu_quota: quota };
      const submitted = JSON.stringify(config);
      const digest = createHash('sha256').update(submitted).digest('hex');
      await writeFile(markerPath, JSON.stringify({ ...marker, submitted_config_body: submitted, submitted_config_sha256: digest }));
      await writeFile(receiptPath, JSON.stringify({ ...receipt, submitted_config_body: submitted, submitted_config_sha256: digest,
        terminal_observation: { ...receipt.terminal_observation,
          status: { ...receipt.terminal_observation.status, config_sha256: digest } } }));
      const quotaEnv = { ...env, docker: (args: string[]) => args[0] === 'info' ? '2\n' : '' };
      if (quota === 2) {
        for (const host of ['', '0', 'invalid']) {
          await assert.rejects(recordLocalFailedDisposition(receiptRoot, invocation, markerPath,
            { ...env, docker: () => host }), /platform\.local\.collector_cpu_quota/);
        }
        await assert.rejects(recordLocalFailedDisposition(receiptRoot, invocation, markerPath,
          { ...env, docker: () => { throw new Error('docker failed'); } }), /platform\.local\.collector_cpu_quota/);
      }
      if (quota === 2.000000001 || quota === 0.0100000001) {
        await assert.rejects(recordLocalFailedDisposition(receiptRoot, invocation, markerPath, quotaEnv), /platform\.local\.collector_cpu_quota/);
      } else {
        await recordLocalFailedDisposition(receiptRoot, invocation, markerPath, quotaEnv);
        const proof = JSON.parse(await readFile(dispositionPath, 'utf8'));
        assert.deepEqual(proof.collector_cpu_quota, { source: 'platform.local.collector_cpu_quota', requested_quota: quota });
        await assertLocalFailedDisposition(receiptPath, dispositionPath, quotaEnv);
        await unlink(dispositionPath);
      }
    }
    for (const [selection, settings, metric, valid] of [
      ['working_set', 'working_set', 'container_working_set_bytes', true],
      ['working_set', 'usage', 'container_working_set_bytes', false],
      ['working_set', 'working_set', 'container_memory_bytes', false],
      ['working-set', 'working-set', 'container_working_set_bytes', false],
      ['usage', 'usage', 'container_memory_bytes', true],
    ] as const) {
      const config = JSON.parse(body);
      config.overload_guard = { container_memory_metric: selection };
      const submitted = JSON.stringify(config);
      const digest = createHash('sha256').update(submitted).digest('hex');
      await writeFile(markerPath, JSON.stringify({ ...marker, submitted_config_body: submitted, submitted_config_sha256: digest }));
      await writeFile(receiptPath, JSON.stringify({ ...receipt, submitted_config_body: submitted, submitted_config_sha256: digest,
        terminal_observation: { ...receipt.terminal_observation,
          status: { ...receipt.terminal_observation.status, config_sha256: digest } } }));
      const guard = { run_id: cell, settings: { container_memory_metric: settings },
        container_memory_metric: settings, last_caps: [{ metric }] };
      const metricTable = tableFromArrays({ run_id: [cell], mode: ['local'], status: [resultStatus], pinning_ok: [false],
        table: ['test_table'], evidence_json: [JSON.stringify({ overload_guard: guard, rows: { delta_committed: null } })] });
      await writeFile(resultPath, writeParquet(WasmTable.fromIPCStream(tableToIPC(metricTable, 'stream'))));
      if (valid) {
        await recordLocalFailedDisposition(receiptRoot, invocation, markerPath, env);
        await assertLocalFailedDisposition(receiptPath, dispositionPath, env);
        await unlink(dispositionPath);
      } else {
        await assert.rejects(recordLocalFailedDisposition(receiptRoot, invocation, markerPath, env), /overload_guard\.container_memory_metric/);
      }
    }
    await writeFile(receiptPath, originals[0]!);
    await writeFile(markerPath, originals[1]!);
    await writeFile(resultPath, originals[2]!);
  }
  if (resultStatus === 'pinning_violation') {
    await writeFile(receiptPath, JSON.stringify({
      ...receipt, terminal_observation: { ...receipt.terminal_observation,
        status: { ...receipt.terminal_observation.status, error: 'guard unavailable' } },
    }));
    await assert.rejects(recordLocalFailedDisposition(receiptRoot, invocation, markerPath, env), /Failed cell identity mismatch/);
    await writeFile(receiptPath, originals[0]!);
    const invalid = tableFromArrays({
      run_id: [cell], mode: ['local'], status: [resultStatus], pinning_ok: [true], table: ['test_table'],
      evidence_json: [JSON.stringify({ overload_guard: { run_id: cell }, rows: { delta_committed: null } })],
    });
    await writeFile(resultPath, writeParquet(WasmTable.fromIPCStream(tableToIPC(invalid, 'stream'))));
    await assert.rejects(recordLocalFailedDisposition(receiptRoot, invocation, markerPath, env), /Failed cell identity mismatch/);
    await writeFile(resultPath, originals[2]!);
  }
  await recordLocalFailedDisposition(receiptRoot, invocation, markerPath, env);
  const dispositionPath = receiptPath + localDispositionSuffix;
  await assertLocalFailedDisposition(receiptPath, dispositionPath, env);
  assert.deepEqual(JSON.parse(await readFile(dispositionPath, 'utf8')).collector_cpu_quota,
    { source: 'coupled_to_dfe_worker_concurrency', requested_quota: null });
  const currentProof = await readFile(dispositionPath);
  const legacyProof = JSON.parse(currentProof.toString('utf8'));
  delete legacyProof.collector_cpu_quota;
  await writeFile(dispositionPath, JSON.stringify(legacyProof));
  await assertLocalFailedDisposition(receiptPath, dispositionPath, env);
  await writeFile(dispositionPath, currentProof);
  assert.deepEqual(await Promise.all([receiptPath, markerPath, resultPath, logPath].map(file => readFile(file))), originals);
  await assert.rejects(recordLocalFailedDisposition(receiptRoot, invocation, markerPath, env), { code: 'EEXIST' });
  await assert.rejects(assertLocalFailedDisposition(receiptPath, dispositionPath, {
    ...env, docker: () => 'live-container',
  }), /resources remain/);
  await assert.rejects(assertLocalFailedDisposition(receiptPath, dispositionPath, {
    ...env, processStartTicks: async pid => pid === 123 ? '100' : undefined,
  }), /still active/);
  await assert.rejects(assertLocalFailedDisposition(receiptPath, dispositionPath, {
    ...env, bootId: async () => '123e4567-e89b-42d3-a456-426614174005',
  }), /disposition or evidence changed/);
  await writeFile(receiptPath, JSON.stringify({ ...receipt, run_id: cell }));
  await assert.rejects(assertLocalFailedDisposition(receiptPath, dispositionPath, env), /ownership marker do not match/);
  await writeFile(receiptPath, originals[0]!);
  await writeFile(logPath, 'altered');
  await assert.rejects(assertLocalFailedDisposition(receiptPath, dispositionPath, env), /Run log does not bind/);
});

test('operator quarantine archives only failed pre-measurement receipts with verified zero-resource custody', async t => {
  const root = await mkdtemp(path.join(os.tmpdir(), 'oat-operator-quarantine-'));
  t.after(() => rm(root, { recursive: true, force: true }));
  const receiptRoot = path.join(root, 'deadline-receipts');
  const runRoot = path.join(root, 'run');
  const stateRoot = path.join(runRoot, 'state');
  await mkdir(receiptRoot);
  await mkdir(stateRoot, { recursive: true });
  const invocation = '123e4567-e89b-42d3-a456-426614174101';
  const run = '123e4567-e89b-42d3-a456-426614174102';
  const cell = '123e4567-e89b-42d3-a456-426614174103';
  const resultPath = path.join(runRoot, 'result.parquet');
  const markerPath = path.join(stateRoot, '.arrowcol-oat-owner-123.json');
  const logPath = path.join(runRoot, 'run.log');
  const receiptPath = path.join(receiptRoot, `${invocation}.json`);
  const project = `arrowcol-oat-hi-${cell.replace(/-/g, '').slice(0, 32)}`;
  const body = JSON.stringify({
    platform: { kind: 'local', compose_project: 'arrowcol-oat-historical-parity', compose_state_root: stateRoot },
    result_url: pathToFileURL(resultPath).href, tables: ['test_table'], replica_counts: [1], dfe_worker_concurrency: [32],
  });
  const hash = (bytes: Uint8Array | string) => createHash('sha256').update(bytes).digest('hex');
  const configHash = hash(body);
  const marker = {
    version: 1, runId: run, ownerPid: 2147483601, ownerStartTicks: '100', serverPid: 2147483602, serverStartTicks: '200',
    serverUrl: 'http://127.0.0.1:1234/', serverExecutable: '/tmp/oat',
    submitted_config_body: body, submitted_config_sha256: configHash, source_config_sha256: 'a'.repeat(64),
  };
  const error = 'collector failed before measurement';
  const receipt = {
    version: 1, invocation_id: invocation, run_id: run, creation_state: 'accepted', run_state: 'failed',
    started_at_unix_ms: 1, started_at_monotonic_ms: 1, deadline_at_unix_ms: null, deadline_seconds: null,
    deadline_at_monotonic_ms: null, cleanup_deadline_at_unix_ms: null, overrun: false, overrun_at_unix_ms: null,
    cancellation_attempts: 0, last_cancel_result: null, error: null, cleanup_state: 'unsettled', config_binding_error: null,
    server_pid: marker.serverPid, server_start_ticks: marker.serverStartTicks, server_executable: marker.serverExecutable,
    server_url: marker.serverUrl, submitted_config_body: body, submitted_config_sha256: configHash,
    source_config_sha256: marker.source_config_sha256,
    terminal_observation: {
      observed_at_unix_ms: 10,
      status: { id: run, state: 'failed', config_sha256: configHash, progress: 'failed', result_count: 1,
        error, started_at_unix_ms: 2, finished_at_unix_ms: 9 },
    },
  };
  const evidence = {
    overload_guard: null, overload_stop: null, preload_delta: null, durable_rows_per_second: null,
    client_schedule: null, client_phase_reports: null, durable_commits: null, durable_bytes: null,
    rows: { delta_committed: null },
  };
  const table = tableFromArrays({
    run_id: [cell], mode: ['local'], status: ['failed'], table: ['test_table'],
    evidence_json: [JSON.stringify(evidence)],
  });
  const resultBytes = writeParquet(WasmTable.fromIPCStream(tableToIPC(table, 'stream')));
  await writeFile(resultPath, resultBytes);
  await writeFile(receiptPath, JSON.stringify(receipt));
  await writeFile(markerPath, JSON.stringify(marker));
  await writeFile(logPath, `${run} ${project} ${error}`);
  const env = {
    docker: (_args: string[]) => '',
    processStartTicks: async (_pid: number) => undefined,
    bootId: async () => '123e4567-e89b-42d3-a456-426614174104',
    listeningSockets: (_port: number) => '',
  };
  await assert.rejects(reconcileAndQuarantineLocalFailedReceipt(receiptRoot, invocation, markerPath, {
    ...env, docker: () => 'owned-container',
  }), /resources remain/);
  assert.equal(await readFile(receiptPath, 'utf8'), JSON.stringify(receipt));
  assert.equal((await readFile(markerPath, 'utf8')), JSON.stringify(marker));
  await assert.rejects(readFile(path.join(root, 'operator-quarantine', invocation)), { code: 'ENOENT' });

  let cliInvoked = false;
  await launchMain(['--reconcile-failed-local', invocation, markerPath], undefined, undefined, receiptRoot,
    undefined, undefined, undefined, undefined, async (actualRoot, actualInvocation, actualMarker) => {
      cliInvoked = true;
      assert.equal(actualRoot, receiptRoot);
      assert.equal(actualInvocation, invocation);
      assert.equal(actualMarker, path.resolve(markerPath));
      return { invocation_id: invocation, disposition: 'tested' };
    });
  assert.equal(cliInvoked, true);

  const record = await reconcileAndQuarantineLocalFailedReceipt(receiptRoot, invocation, markerPath, env);
  const quarantineDir = path.join(root, 'operator-quarantine', invocation);
  assert.equal(record.disposition, 'failed_before_measurement_quarantined_not_settled');
  assert.equal(record.receipt_sha256, hash(JSON.stringify(receipt)));
  assert.deepEqual(JSON.parse(await readFile(path.join(quarantineDir, `${invocation}.json`), 'utf8')), receipt);
  await assert.rejects(readFile(receiptPath), { code: 'ENOENT' });
  await assert.rejects(readFile(markerPath), { code: 'ENOENT' });
  await assertNoUnsettledDeadlineReceipts(receiptRoot);

  const archivedEvidence = record.archived_evidence_files;
  assert.ok(Array.isArray(archivedEvidence));
  const archivedResultEntry = archivedEvidence.find((item: unknown) => typeof item === 'object' && item !== null &&
    'name' in item && item.name === 'result' && 'path' in item && typeof item.path === 'string');
  assert.ok(archivedResultEntry && typeof archivedResultEntry === 'object' && 'path' in archivedResultEntry &&
    typeof archivedResultEntry.path === 'string');
  const archivedResult = path.join(quarantineDir, archivedResultEntry.path);
  await writeFile(archivedResult, 'tampered');
  await assert.rejects(assertNoUnsettledDeadlineReceipts(receiptRoot), /archive digest mismatch/);
  await writeFile(archivedResult, resultBytes);
  await assertNoUnsettledDeadlineReceipts(receiptRoot);
});
