import assert from 'node:assert/strict';
import { cpSync, readFileSync } from 'node:fs';
import { createRequire } from 'node:module';
import { dirname, join, resolve } from 'node:path';
import { pathToFileURL } from 'node:url';
import {
  commandRunner, digest, enqueue, ensureDirectory, generatorPath, readRun, runDirectory, writeJson,
} from '../src/contract.js';
import { protocPaths } from '../src/protoc.js';

const workspace = process.cwd();
const execute = commandRunner(workspace);
const name = `protoc_${Date.now()}`;
const queued = enqueue(workspace, {
  registry: 'hosted', mode: 'create', codegen: 'protoc', protoEntry: 'telemetry.proto',
  ddl: `CREATE TABLE telemetry.prod.${name} (event_id BIGINT NOT NULL, payload STRING) USING DELTA`,
});
const nxPackagePath = createRequire(import.meta.url).resolve('nx/package.json');
const nxPackage: { bin: { nx: string } } = JSON.parse(readFileSync(nxPackagePath, 'utf8'));
const nxBinary = resolve(dirname(nxPackagePath), nxPackage.bin.nx);
await execute(process.execPath, [nxBinary, 'run', 'arrowcol-sdk-gen:build-contract', `--run-id=${queued.id}`],
  join(runDirectory(workspace, queued.id), 'nx.log'),
  { NX_DAEMON: 'false', NX_TUI: 'false', NX_TASKS_RUNNER_DYNAMIC_OUTPUT: 'false' });
const run = readRun(workspace, queued.id);
assert.equal(run.status, 'succeeded', run.error ?? '');
assert.equal(run.registration, 'committed');
assert.ok(run.artifact && run.pin);

const payload = join(runDirectory(workspace, queued.id), 'payload');
const release = JSON.parse(readFileSync(join(payload, 'release.json'), 'utf8'));
assert.equal(release.provenance.codegen.backend, 'protoc');
const text = readFileSync(join(payload, 'compiler/telemetry.proto'), 'utf8');
assert.match(text, /operation: ARROW_FLIGHT_DO_PUT/);
assert.match(text, /x-arrowcol-implementation-pool/);
const snapshot = JSON.parse(await execute(generatorPath(workspace), ['catalog', 'list', '--endpoint', run.registry.endpoint]));
assert.ok(snapshot.tables.some((table: { table: string }) => table.table === `telemetry.prod.${name}`));

const evidence = resolve(workspace, '.temp/contract-builder/evidence', name);
ensureDirectory(workspace, evidence);
const extendedSdk = join(evidence, 'extended-sdk');
cpSync(join(payload, 'sdk'), extendedSdk, { recursive: true, errorOnExist: true, force: false });
const suffix = process.platform === 'win32' ? '.exe' : '';
await execute('protoc', [
  `--proto_path=${join(payload, 'compiler')}`,
  `--plugin=protoc-gen-arrowcol-sdk=${protocPaths(workspace).plugin}`,
  `--plugin=protoc-gen-team=${resolve(workspace, `target/debug/protoc-gen-arrowcol-example${suffix}`)}`,
  `--arrowcol-sdk_out=${extendedSdk}`, `--team_out=${extendedSdk}`, 'telemetry.proto',
], join(evidence, 'composition.log'));
await execute(process.execPath, [
  'node_modules/typescript/bin/tsc', '--ignoreConfig', '--strict', '--target', 'ES2022', '--module', 'NodeNext',
  '--moduleResolution', 'NodeNext', '--skipLibCheck', '--noEmit', join(extendedSdk, 'typescript/team.ts'),
], join(evidence, 'composition.log'));
const extension: {
  planTeamTelemetry(rows: []): { grpcPath: string; headers: Record<string, string>; descriptorPath: string[] };
} = await import(pathToFileURL(join(extendedSdk, 'typescript/team.ts')).href);
const plan = extension.planTeamTelemetry([]);
assert.equal(plan.grpcPath, '/arrow.flight.protocol.FlightService/DoPut');
assert.equal(plan.headers['x-arrowcol-table'], name);
assert.equal(plan.headers['x-arrowcol-implementation-pool'], 'dataflow');
assert.equal(plan.headers['x-arrowcol-schema-fingerprint'], run.pin.fingerprint);
assert.deepEqual(plan.descriptorPath, ['telemetry', 'prod', name, 'v1']);
const response = await fetch(`${process.env.CONTRACT_UI_URL ?? 'http://localhost:4173'}/api/runs/${run.id}/artifact`);
assert.equal(response.status, 200);
assert.equal(digest(Buffer.from(await response.arrayBuffer())), run.artifact.sha256);
writeJson(join(evidence, 'result.json'), {
  result: 'passed', run,
  plan: { grpcPath: plan.grpcPath, headers: plan.headers, descriptorPath: plan.descriptorPath },
});
console.log(JSON.stringify({ result: 'passed', run: run.id, artifact: run.artifact, evidence }, null, 2));
