import { canonical_json, Failure, type Azure, type HttpReceipt } from './bootstrap.js';
import { assert_namespace_delete_response, parse_run_namespace_identity, type NamespaceDeletionIntent,
  type NamespaceDeletionJournal, type NamespaceStopReceipt, type RunCleanupAuthority, type RunNamespaceIdentity } from './run_fic.js';

interface Phase {
  schema_version: 1;
  phase: 'intent' | 'responded' | 'complete';
  intent: NamespaceDeletionIntent<RunNamespaceIdentity>;
  response: unknown | null;
  receipt: NamespaceStopReceipt | null;
}
function object(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Failure('Invalid durable namespace journal.');
  return value as Record<string, unknown>;
}
export function create_namespace_deletion_journal(azure: Azure, input: RunNamespaceIdentity, authority: RunCleanupAuthority):
  NamespaceDeletionJournal<RunNamespaceIdentity, NamespaceStopReceipt> & {
    complete(receipt: NamespaceStopReceipt): Promise<void>;
    stopped(): Promise<NamespaceStopReceipt>;
  } {
  const identity = parse_run_namespace_identity(input, azure.c);
  const blob = `_arrowcol-custody/namespaces/${identity.runId}/${identity.namespaceUid}.json`;
  const intent: NamespaceDeletionIntent<RunNamespaceIdentity> = { workload_record: identity,
    deletion: { apiVersion: 'v1', kind: 'DeleteOptions', preconditions: { uid: identity.namespaceUid } } };
  const read = async (): Promise<{ value: Phase; etag: string } | undefined> => {
    await authority.assertCurrent();
    const [bytes, headers] = await azure.blob('GET', azure.c.checkpoints, blob, undefined, undefined, {}, true, true);
    if (bytes === null) return undefined;
    if (bytes.length > 65_536 || !headers.etag) throw new Failure('Namespace journal has no bounded versioned response.');
    const value = object(JSON.parse(bytes.toString()) as unknown);
    if (Object.keys(value).sort().join(',') !== ['schema_version', 'phase', 'intent', 'response', 'receipt'].sort().join(',') ||
        value.schema_version !== 1 || canonical_json(value.intent) !== canonical_json(intent) ||
        !['intent', 'responded', 'complete'].includes(String(value.phase)))
      throw new Failure('Namespace journal identity or phase differs.');
    if (value.phase === 'intent') {
      if (value.response !== null || value.receipt !== null) throw new Failure('Namespace intent cannot claim a response.');
      return { value: { schema_version: 1, phase: 'intent', intent, response: null, receipt: null }, etag: headers.etag };
    }
    assert_namespace_delete_response(value.response, identity);
    let receipt: NamespaceStopReceipt | null = null;
    if (value.phase === 'complete') {
      const observedAt = object(value.receipt).observed_at_unix_ms;
      if (typeof observedAt !== 'number' || !Number.isSafeInteger(observedAt) || observedAt < 1 || observedAt > Date.now())
        throw new Failure('Namespace stop observation time is invalid.');
      receipt = { schema_version: 1, namespace_record: identity, delete_succeeded: true,
        absence_confirmed: true, observed_at_unix_ms: observedAt };
      if (canonical_json(receipt) !== canonical_json(value.receipt)) throw new Failure('Namespace stop proof differs from its original identity.');
    } else if (value.receipt !== null) throw new Failure('Namespace response alone is not absence proof.');
    return { value: { schema_version: 1, phase: value.phase === 'complete' ? 'complete' : 'responded',
      intent, response: value.response, receipt }, etag: headers.etag };
  };
  const save = async (value: Phase) => {
    const prior = await read(), rank = { intent: 0, responded: 1, complete: 2 };
    if (canonical_json(value.intent) !== canonical_json(intent)) throw new Failure('Namespace journal write identity changed.');
    if (prior && rank[prior.value.phase] >= rank[value.phase]) {
      if (value.response !== null && canonical_json(value.response) !== canonical_json(prior.value.response) ||
          value.receipt !== null && canonical_json(value.receipt) !== canonical_json(prior.value.receipt))
        throw new Failure('Namespace journal proof is immutable.');
      return;
    }
    await authority.assertCurrent();
    let response: HttpReceipt | undefined;
    const [, headers] = await azure.blob('PUT', azure.c.checkpoints, blob, undefined, canonical_json(value),
      { 'x-ms-blob-type': 'BlockBlob', ...(prior ? { 'If-Match': prior.etag } : { 'If-None-Match': '*' }) },
      false, false, original => { response = original; });
    if (headers[':status'] !== '201' || !headers.etag || response?.status !== 201)
      throw new Failure('Namespace journal write outcome is UNKNOWN; no DELETE replay.');
    const confirmed = await read();
    if (!confirmed || confirmed.etag !== headers.etag || canonical_json(confirmed.value) !== canonical_json(value))
      throw new Failure('Namespace journal write postcondition is UNKNOWN.');
  };
  return {
    load: async () => {
      const saved = await read();
      return saved ? { intent: saved.value.intent, response: saved.value.response,
        ...(saved.value.receipt ? { receipt: saved.value.receipt } : {}) } : undefined;
    },
    persist: (requested, response) => save({ schema_version: 1, phase: response === null ? 'intent' : 'responded',
      intent: requested, response, receipt: null }),
    complete: async receipt => {
      const saved = await read();
      if (!saved || saved.value.response === null) throw new Failure('No positive namespace DELETE response exists.');
      if (!Number.isSafeInteger(receipt.observed_at_unix_ms) || receipt.observed_at_unix_ms < 1 ||
          receipt.observed_at_unix_ms > Date.now() ||
          canonical_json(receipt) !== canonical_json({ schema_version: 1, namespace_record: identity,
            delete_succeeded: true, absence_confirmed: true, observed_at_unix_ms: receipt.observed_at_unix_ms }))
        throw new Failure('Namespace stop proof differs from its original identity or observation time.');
      await save({ ...saved.value, phase: 'complete', receipt });
    },
    stopped: async () => {
      const saved = await read();
      if (!saved?.value.receipt) throw new Failure('Positive original namespace stop proof is missing.');
      return saved.value.receipt;
    },
  };
}
