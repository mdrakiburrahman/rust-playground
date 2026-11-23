import assert from 'node:assert/strict';
import test from 'node:test';
import { requireMatchedStartupAccounting, requireStartupAccounting } from './startup-accounting.js';

const liveBatchBytes = 152_920_064;
const liveBatchReservationBytes = liveBatchBytes * 28;
const maxMemoryBytes = 5_368_709_120;

function client(clientId: string, corpusReservationBytes = 1_000) {
  return {
    client_id: clientId,
    admitted_live_slots: 28,
    configured_live_slots: 28,
    live_identity_batch_bytes: liveBatchBytes,
    live_identity_batch_reservation_bytes: liveBatchReservationBytes,
    corpus_memory_reservation_bytes: corpusReservationBytes,
    memory_reservation_bytes: corpusReservationBytes + liveBatchReservationBytes,
    max_memory_bytes: maxMemoryBytes,
  };
}

function evidence(clients: unknown = [client('oat-run-table-c1')]) {
  return {
    schema_version: 1,
    capture_point: 'client_ready_before_arm',
    clients,
  };
}

test('accepts and retains complete per-client accounting captured before arm', () => {
  const expectedClientIds = ['oat-run-table-c1', 'oat-run-table-c2'];
  const input = evidence(expectedClientIds.map(id => client(id, 9_876)));
  const result = requireStartupAccounting(input, expectedClientIds);

  assert.equal(result.capture_point, 'client_ready_before_arm');
  assert.equal(result.validation_point, 'before_arm');
  assert.equal(result.clients[0]?.corpus_memory_reservation_bytes, 9_876);
  assert.equal(result.clients[0]?.live_identity_batch_reservation_bytes, liveBatchReservationBytes);
  assert.equal(result.clients[0]?.memory_reservation_bytes, 9_876 + liveBatchReservationBytes);
  assert.equal(result.clients[0]?.max_memory_bytes, maxMemoryBytes);
  assert.deepEqual(result.expected, {
    admitted_live_slots: 28,
    configured_live_slots: 28,
    live_identity_batch_bytes: liveBatchBytes,
    max_memory_bytes: maxMemoryBytes,
  });
});

test('rejects live-slot, batch-byte, reservation, and memory-limit mismatches', () => {
  const valid = client('oat-run-table-c1');
  for (const invalid of [
    { ...valid, admitted_live_slots: 27 },
    { ...valid, configured_live_slots: 29 },
    { ...valid, live_identity_batch_bytes: liveBatchBytes - 1 },
    { ...valid, live_identity_batch_reservation_bytes: liveBatchReservationBytes - 1 },
    { ...valid, memory_reservation_bytes: valid.memory_reservation_bytes + 1 },
    { ...valid, max_memory_bytes: maxMemoryBytes - 1 },
    client('oat-run-table-c1', maxMemoryBytes - liveBatchReservationBytes + 1),
  ]) {
    assert.throws(() => requireStartupAccounting(evidence([invalid]), ['oat-run-table-c1']));
  }
});

test('fails closed on missing, unparseable, zero, extra, or unexpected client evidence', () => {
  const valid = client('oat-run-table-c1');
  const missingCorpus = Object.fromEntries(
    Object.entries(valid).filter(([key]) => key !== 'corpus_memory_reservation_bytes'),
  );
  assert.throws(
    () => requireStartupAccounting(evidence([missingCorpus]), ['oat-run-table-c1']),
    /missing, malformed, or extra client field/,
  );
  assert.throws(
    () => requireStartupAccounting(
      evidence([{ ...valid, corpus_memory_reservation_bytes: '1000' }]),
      ['oat-run-table-c1'],
    ),
    /safe unsigned corpus_memory_reservation_bytes/,
  );
  assert.throws(
    () => requireStartupAccounting(
      evidence([{ ...valid, corpus_memory_reservation_bytes: 0 }]),
      ['oat-run-table-c1'],
    ),
    /requires nonzero corpus_memory_reservation_bytes/,
  );
  assert.throws(
    () => requireStartupAccounting(
      evidence([{ ...valid, unexpected_counter: 1 }]),
      ['oat-run-table-c1'],
    ),
    /missing, malformed, or extra client field/,
  );
  assert.throws(
    () => requireStartupAccounting(evidence([valid, client('oat-run-extra-c1')]), ['oat-run-table-c1']),
    /missing clients or contains extra clients/,
  );
  assert.throws(
    () => requireStartupAccounting(evidence([valid]), ['oat-run-table-c1', 'oat-run-table-c2']),
    /missing clients or contains extra clients/,
  );
  assert.throws(
    () => requireStartupAccounting(evidence([valid]), ['oat-run-table-c1', 'oat-run-table-c1']),
    /malformed or contains duplicates/,
  );
});

test('rejects missing, duplicate, or after-measurement startup evidence', () => {
  assert.throws(
    () => requireStartupAccounting(undefined, ['oat-run-table-c1']),
    /missing, malformed, or was not captured before arm/,
  );
  assert.throws(
    () => requireStartupAccounting(
      evidence([client('oat-run-table-c1'), client('oat-run-table-c1')]),
      ['oat-run-table-c1', 'oat-run-table-c1'],
    ),
  );
  assert.throws(
    () => requireStartupAccounting(
      { ...evidence(), capture_point: 'after_measurement' },
      ['oat-run-table-c1'],
    ),
    /missing, malformed, or was not captured before arm/,
  );
});

test('allows checking the retained pre-arm snapshot at warmup end, before measurement', () => {
  const result = requireStartupAccounting(
    evidence(),
    ['oat-run-table-c1'],
    'warmup_end_before_measurement',
  );
  assert.equal(result.validation_point, 'warmup_end_before_measurement');
  assert.throws(
    () => requireStartupAccounting(
      evidence([{ ...client('oat-run-table-c1'), memory_reservation_bytes: maxMemoryBytes + 1 }]),
      ['oat-run-table-c1'],
      'warmup_end_before_measurement',
    ),
  );
});

test('requires corpus reservations to match per client across matched arms without pinning a value', () => {
  const controlId = 'control-run:table:0';
  const candidateId = 'candidate-run:table:0';
  const arms = [
    { arm_id: 'control', expected_client_ids: [controlId], evidence: evidence([client(controlId, 77_777)]) },
    { arm_id: 'candidate', expected_client_ids: [candidateId], evidence: evidence([client(candidateId, 77_777)]) },
  ];
  assert.equal(requireMatchedStartupAccounting(arms)[0]?.evidence.clients[0]?.corpus_memory_reservation_bytes, 77_777);
  assert.throws(
    () => requireMatchedStartupAccounting([
      arms[0]!,
      {
        ...arms[1]!,
        evidence: evidence([client(candidateId, 77_778)]),
      },
    ]),
    /corpus reservation differs/,
  );
});

test('rejects malformed or extra matched-arm evidence', () => {
  const expectedClientIds = ['control-run:table:0'];
  const validArm = {
    arm_id: 'control',
    expected_client_ids: expectedClientIds,
    evidence: evidence([client(expectedClientIds[0]!)]),
  };
  const extraFieldArm = { ...validArm, arm_id: 'candidate', unexpected_arm_field: true };
  assert.throws(() => requireMatchedStartupAccounting([validArm]));
  assert.throws(
    () => requireMatchedStartupAccounting([validArm, extraFieldArm]),
    /missing, malformed, or extra arm field/,
  );
  assert.throws(
    () => requireMatchedStartupAccounting([
      validArm,
      {
        ...validArm,
        arm_id: 'candidate',
        expected_client_ids: ['candidate-run:other:0'],
        evidence: evidence([client('candidate-run:other:0')]),
      },
    ]),
    /different client roster/,
  );
  assert.throws(
    () => requireMatchedStartupAccounting([
      validArm,
      {
        arm_id: 'candidate',
        expected_client_ids: ['invalid'],
        evidence: evidence([client('invalid')]),
      },
    ]),
    /invalid OAT client ID/,
  );
  assert.throws(
    () => requireMatchedStartupAccounting([
      validArm,
      {
        arm_id: 'candidate',
        expected_client_ids: ['run-a:table:0', 'run-b:table:0'],
        evidence: evidence([client('run-a:table:0'), client('run-b:table:0')]),
      },
    ]),
    /duplicate logical client/,
  );
});
