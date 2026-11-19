import { isIP } from 'node:net';
import type { ArmIdentity, WriterLifetime, observeWriterExit } from './benchmark-arm.js';

type Writer = WriterLifetime;
type WriterExit = ReturnType<typeof observeWriterExit>;
const uuid = /^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/;
const sqlUtc = /^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{7}Z$/;

export interface WriterRegistration {
  instance_id: string;
  instance_key: string;
  implementation_pool: string;
  flight_endpoint: string;
  state: string;
  started_at_utc: string;
  row_version: string;
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
