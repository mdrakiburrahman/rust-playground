import { isIP } from 'node:net';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import type { ArmIdentity, WriterLifetime, observeWriterExit } from './benchmark-arm.js';
import { evidenceList, evidenceObject, evidenceText, parseEvidence } from './benchmark-evidence-json.js';
import type { RunCleanupAuthority } from './run_fic.js';

type Writer = WriterLifetime;
type WriterExit = ReturnType<typeof observeWriterExit>;
const uuid = /^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/;
const sqlUtc = /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{7}Z$/;
const exec = promisify(execFile);

export interface WriterRegistration {
  instance_id: string;
  instance_key: string;
  implementation_pool: string;
  flight_endpoint: string;
  state: string;
  started_at_utc: string;
  row_version: string;
}

export async function readWriterRegistrations(authority: RunCleanupAuthority, run: ArmIdentity, generation: string,
  context: string, server: string, sqlPod: { name: string; uid: string }, assertSqlLifetime: () => Promise<void>): Promise<WriterRegistration[]> {
  if (!uuid.test(run.runId) || !uuid.test(generation) || !uuid.test(sqlPod.uid) ||
      !/^arrowcol-sql-[a-z0-9-]+$/.test(sqlPod.name) || !/^https:\/\/[a-z0-9.-]+$/.test(server)) {
    throw new Error('SQL observation does not identify the original run and TLS server');
  }
  const key = `direct-single-writer-${run.runId}|cloud=${generation}`;
  const query = `SET NOCOUNT ON;
    SELECT CAST((SELECT TOP (2)
      LOWER(CONVERT(char(36), instance_id)) AS instance_id, instance_key, implementation_pool, flight_endpoint, state,
      CONCAT(FORMAT(started_at_utc, 'yyyy-MM-ddTHH:mm:ss.fffffff', 'en-US'), 'Z') AS started_at_utc,
      CONVERT(varchar(18), row_version, 1) AS row_version
      FROM arrowcol.routing_collectors WHERE instance_key = '${key}' FOR JSON PATH) AS nvarchar(2000));`;
  await authority.assertCurrent();
  await assertSqlLifetime();
  const args = ['--context', context, '--server', server, '--insecure-skip-tls-verify=false', '--request-timeout=16s',
    '-n', run.namespace, 'exec', sqlPod.name, '-c', 'sqlserver', '--', '/usr/local/bin/sqlcmd-secret', '-S', 'localhost,1433',
    '-U', 'sa', '-d', 'arrowcol', '-Q', query, '-b', '-h', '-1', '-W', '-w', '4096', '-l', '5', '-t', '6'];
  let stdout: string;
  try { ({ stdout } = await exec('kubectl', args, { timeout: 20_000, maxBuffer: 8_192 })); }
  catch { throw new Error('Original SQL collector observation failed; terminal state is unknown'); }
  await authority.assertCurrent();
  await assertSqlLifetime();
  return evidenceList(parseEvidence(stdout)).map(value => {
    const row = evidenceObject(value);
    return {
      instance_id: evidenceText(row.instance_id), instance_key: evidenceText(row.instance_key),
      implementation_pool: evidenceText(row.implementation_pool), flight_endpoint: evidenceText(row.flight_endpoint),
      state: evidenceText(row.state), started_at_utc: evidenceText(row.started_at_utc), row_version: evidenceText(row.row_version),
    };
  });
}

function originalRegistration(run: ArmIdentity, generation: string, writer: Writer,
  observations: readonly WriterRegistration[]): WriterRegistration {
  const [row] = observations;
  if (!uuid.test(run.runId) || !uuid.test(generation) || isIP(writer.podIp) !== 4 ||
      observations.length !== 1 || !row || !uuid.test(row.instance_id) ||
      row.instance_key !== `direct-single-writer-${run.runId}|cloud=${generation}` ||
      row.implementation_pool !== 'dataflow' || row.flight_endpoint !== `${writer.podIp}:32010` ||
      !/^0x[0-9a-f]{16}$/i.test(row.row_version) || !sqlUtc.test(row.started_at_utc) ||
      !Number.isFinite(Date.parse(row.started_at_utc))) {
    throw new Error('No unique SQL registration for the original writer, endpoint and cloud generation');
  }
  return row;
}

export function pinWriterRegistration(run: ArmIdentity, generation: string, writer: Writer,
  observations: readonly WriterRegistration[]): WriterRegistration {
  const row = originalRegistration(run, generation, writer, observations);
  if (row.state !== 'ready') throw new Error('Original writer SQL registration is not ready');
  return { ...row };
}

export function observeStoppedWriterRegistration(run: ArmIdentity, generation: string, writer: WriterExit,
  pinned: WriterRegistration, observations: readonly WriterRegistration[]): WriterRegistration {
  const before = pinWriterRegistration(run, generation, writer, [pinned]);
  const row = originalRegistration(run, generation, writer, observations);
  if (row.instance_id !== before.instance_id || row.started_at_utc !== before.started_at_utc ||
      row.state !== 'stopped' || BigInt(row.row_version) <= BigInt(before.row_version)) {
    throw new Error('Original writer never produced a newer SQL stopped observation');
  }
  return { ...row };
}
