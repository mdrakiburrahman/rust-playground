export const STARTUP_ACCOUNTING_EXPECTED = Object.freeze({
  admitted_live_slots: 28,
  configured_live_slots: 28,
  live_identity_batch_bytes: 152_920_064,
  max_memory_bytes: 5_368_709_120,
});

export type StartupAccountingValidationPoint = 'before_arm' | 'warmup_end_before_measurement';

export interface StartupAccountingClient {
  client_id: string;
  admitted_live_slots: number;
  configured_live_slots: number;
  live_identity_batch_bytes: number;
  live_identity_batch_reservation_bytes: number;
  corpus_memory_reservation_bytes: number;
  memory_reservation_bytes: number;
  max_memory_bytes: number;
}

export interface StartupAccountingEvidence {
  schema_version: 1;
  capture_point: 'client_ready_before_arm';
  validation_point: StartupAccountingValidationPoint;
  expected: typeof STARTUP_ACCOUNTING_EXPECTED;
  clients: readonly StartupAccountingClient[];
}

export interface StartupAccountingArmInput {
  arm_id: string;
  expected_client_ids: readonly string[];
  evidence: unknown;
}

export interface MatchedStartupAccountingArm {
  arm_id: string;
  evidence: StartupAccountingEvidence;
}

const EVIDENCE_KEYS = ['capture_point', 'clients', 'schema_version'];
const CLIENT_KEYS = [
  'admitted_live_slots',
  'client_id',
  'configured_live_slots',
  'corpus_memory_reservation_bytes',
  'live_identity_batch_bytes',
  'live_identity_batch_reservation_bytes',
  'max_memory_bytes',
  'memory_reservation_bytes',
];
const ARM_KEYS = ['arm_id', 'evidence', 'expected_client_ids'];

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value);
}

function hasExactKeys(value: Record<string, unknown>, expected: readonly string[]): boolean {
  const actual = Object.keys(value).sort();
  return actual.length === expected.length && actual.every((key, index) => key === expected[index]);
}

function requireSafeUnsignedInteger(value: unknown, name: string, clientId: string): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0) {
    throw new Error(`Startup accounting for ${clientId} is missing a safe unsigned ${name}.`);
  }
  return value;
}

function requirePositiveSafeUnsignedInteger(value: unknown, name: string, clientId: string): number {
  const parsed = requireSafeUnsignedInteger(value, name, clientId);
  if (parsed === 0) {
    throw new Error(`Startup accounting for ${clientId} requires nonzero ${name}.`);
  }
  return parsed;
}

function requireExpectedClientIds(value: readonly string[]): Set<string> {
  if (!Array.isArray(value) || value.length === 0) {
    throw new Error('Startup accounting requires a nonempty expected client roster.');
  }
  const expected = new Set<string>();
  for (const clientId of value) {
    if (typeof clientId !== 'string' || clientId.trim() !== clientId || clientId === '' || expected.has(clientId)) {
      throw new Error('Startup accounting expected client roster is malformed or contains duplicates.');
    }
    expected.add(clientId);
  }
  return expected;
}

export function requireStartupAccounting(
  value: unknown,
  expectedClientIds: readonly string[],
  validationPoint: StartupAccountingValidationPoint = 'before_arm',
): StartupAccountingEvidence {
  if (validationPoint !== 'before_arm' && validationPoint !== 'warmup_end_before_measurement') {
    throw new Error('Startup accounting must be checked before arming or before measurement.');
  }
  if (!isRecord(value) || !hasExactKeys(value, EVIDENCE_KEYS)
    || value.schema_version !== 1 || value.capture_point !== 'client_ready_before_arm') {
    throw new Error('Startup accounting evidence is missing, malformed, or was not captured before arm.');
  }

  const expected = requireExpectedClientIds(expectedClientIds);
  if (!Array.isArray(value.clients) || value.clients.length !== expected.size) {
    throw new Error('Startup accounting client roster is missing clients or contains extra clients.');
  }

  const seen = new Set<string>();
  const clients = value.clients.map((candidate): StartupAccountingClient => {
    if (!isRecord(candidate) || !hasExactKeys(candidate, CLIENT_KEYS)
      || typeof candidate.client_id !== 'string'
      || candidate.client_id.trim() !== candidate.client_id || candidate.client_id === '') {
      throw new Error('Startup accounting contains a missing, malformed, or extra client field.');
    }
    const clientId = candidate.client_id;
    if (!expected.has(clientId) || seen.has(clientId)) {
      throw new Error(`Startup accounting contains an unexpected or duplicate client ${clientId}.`);
    }
    seen.add(clientId);

    const admitted = requireSafeUnsignedInteger(candidate.admitted_live_slots, 'admitted_live_slots', clientId);
    const configured = requireSafeUnsignedInteger(candidate.configured_live_slots, 'configured_live_slots', clientId);
    const liveBatchBytes = requirePositiveSafeUnsignedInteger(
      candidate.live_identity_batch_bytes,
      'live_identity_batch_bytes',
      clientId,
    );
    const liveBatchReservationBytes = requirePositiveSafeUnsignedInteger(
      candidate.live_identity_batch_reservation_bytes,
      'live_identity_batch_reservation_bytes',
      clientId,
    );
    const corpusReservationBytes = requirePositiveSafeUnsignedInteger(
      candidate.corpus_memory_reservation_bytes,
      'corpus_memory_reservation_bytes',
      clientId,
    );
    const memoryReservationBytes = requirePositiveSafeUnsignedInteger(
      candidate.memory_reservation_bytes,
      'memory_reservation_bytes',
      clientId,
    );
    const maxMemoryBytes = requirePositiveSafeUnsignedInteger(
      candidate.max_memory_bytes,
      'max_memory_bytes',
      clientId,
    );
    const expectedLiveBatchReservation = liveBatchBytes * admitted;
    const expectedTotalReservation = corpusReservationBytes + liveBatchReservationBytes;

    if (admitted !== STARTUP_ACCOUNTING_EXPECTED.admitted_live_slots
      || configured !== STARTUP_ACCOUNTING_EXPECTED.configured_live_slots
      || liveBatchBytes !== STARTUP_ACCOUNTING_EXPECTED.live_identity_batch_bytes
      || maxMemoryBytes !== STARTUP_ACCOUNTING_EXPECTED.max_memory_bytes
      || !Number.isSafeInteger(expectedLiveBatchReservation)
      || liveBatchReservationBytes !== expectedLiveBatchReservation
      || !Number.isSafeInteger(expectedTotalReservation)
      || memoryReservationBytes !== expectedTotalReservation
      || memoryReservationBytes > maxMemoryBytes) {
      throw new Error(
        `Startup accounting mismatch for ${clientId}: admitted=${admitted}, configured=${configured}, `
        + `live_identity_batch_bytes=${liveBatchBytes}, corpus_memory_reservation_bytes=${corpusReservationBytes}, `
        + `memory_reservation_bytes=${memoryReservationBytes}, max_memory_bytes=${maxMemoryBytes}.`,
      );
    }
    return Object.freeze({
      client_id: clientId,
      admitted_live_slots: admitted,
      configured_live_slots: configured,
      live_identity_batch_bytes: liveBatchBytes,
      live_identity_batch_reservation_bytes: liveBatchReservationBytes,
      corpus_memory_reservation_bytes: corpusReservationBytes,
      memory_reservation_bytes: memoryReservationBytes,
      max_memory_bytes: maxMemoryBytes,
    });
  });

  return Object.freeze({
    schema_version: 1,
    capture_point: 'client_ready_before_arm',
    validation_point: validationPoint,
    expected: STARTUP_ACCOUNTING_EXPECTED,
    clients: Object.freeze(clients),
  });
}

export function requireMatchedStartupAccounting(
  arms: readonly StartupAccountingArmInput[],
  validationPoint: StartupAccountingValidationPoint = 'before_arm',
): readonly MatchedStartupAccountingArm[] {
  if (!Array.isArray(arms) || arms.length < 2) {
    throw new Error('Matched startup accounting requires at least two arms.');
  }

  const seenArms = new Set<string>();
  const validatedArms = arms.map((arm): MatchedStartupAccountingArm => {
    if (!isRecord(arm) || !hasExactKeys(arm, ARM_KEYS)
      || typeof arm.arm_id !== 'string' || arm.arm_id.trim() !== arm.arm_id || arm.arm_id === '') {
      throw new Error('Matched startup accounting contains a missing, malformed, or extra arm field.');
    }
    if (seenArms.has(arm.arm_id)) {
      throw new Error(`Matched startup accounting contains duplicate arm ${arm.arm_id}.`);
    }
    seenArms.add(arm.arm_id);
    return {
      arm_id: arm.arm_id,
      evidence: requireStartupAccounting(arm.evidence, arm.expected_client_ids, validationPoint),
    };
  });

  const referenceClients = validatedArms[0]!.evidence.clients;
  const referenceIds = referenceClients.map(client => client.client_id).sort();
  for (const arm of validatedArms.slice(1)) {
    const clients = arm.evidence.clients;
    if (clients.map(client => client.client_id).sort().join('\0') !== referenceIds.join('\0')) {
      throw new Error(`Matched startup accounting arm ${arm.arm_id} has a different client roster.`);
    }
    for (const reference of referenceClients) {
      const current = clients.find(client => client.client_id === reference.client_id)!;
      if (current.corpus_memory_reservation_bytes !== reference.corpus_memory_reservation_bytes) {
        throw new Error(
          `Matched startup accounting corpus reservation differs for ${reference.client_id} in arm ${arm.arm_id}.`,
        );
      }
    }
  }

  return Object.freeze(validatedArms.map(arm => Object.freeze(arm)));
}
