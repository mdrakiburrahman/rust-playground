import { createHash } from 'node:crypto';
import { canonical_json, type Azure } from '../../infra/azure/bootstrap.js';
import { CampaignConflictError, CampaignWriteOutcomeUnknownError, hasVerifiedUnsettledDrain, type CampaignStore, type RunInventory } from './aks-campaign.js';
import { readCellRunInventory, type CellCleanupActor } from './aks-cell-cleanup.js';
import { baselineDelta, immutableBlob, readBlob } from './aks-data-safety.js';
import { parseStop } from './aks-lifecycle.js';
import { CleanupDeadlineExpiredError } from './aks-cleanup-budget.js';
import type { CleanupMutationAuthority } from './aks-cleanup-executor.js';
import { readFrozenDeltaCommits } from './aks-delta-preservation.js';
import { COLLECTOR_TERMINAL_LOG_MAX_BYTES, parseTerminalSummary, terminalSummaryClear, terminalTableDigest, type BoundTerminalSummary } from './aks-terminal-evidence.js';

interface Ref { blob: string; etag: string; sha256: string }
interface Input { schema_version: 1; campaign_id: string; orchestration_run_id: string; cell_run_id: string;
  expected_etag: string; actor: CellCleanupActor }
type Classification = 'exit_0' | 'exit_143_terminal_reconciliation_unavailable' | 'exit_143_without_drain_marker' |
  'exit_oom_killed' | 'exit_nonzero' | 'terminal_status_unavailable';
interface Verdict { pod_name: string; pod_uid: string; container: string; restart_count: number | null; classification: Classification }
const object = (value: unknown): value is Record<string, unknown> => !!value && typeof value === 'object' && !Array.isArray(value);
const exact = (value: unknown, keys: string[]): value is Record<string, unknown> => object(value) &&
  Object.keys(value).length === keys.length && keys.every(key => Object.hasOwn(value, key));
const digest = (value: unknown) => createHash('sha256').update(canonical_json(value)).digest('hex');
const origin = (inventory: RunInventory) => ({ campaign_id: inventory.campaign_id, owner_id: inventory.owner_id,
  fencing_token: inventory.fencing_token, orchestration_run_id: inventory.orchestration_run_id, cell_run_id: inventory.cell_run_id,
  namespace: inventory.namespace });
const cellBlob = (inventory: RunInventory, name: string) => `_arrowcol-custody/campaigns/${inventory.campaign_id}` +
  `/runs/${inventory.orchestration_run_id}/cells/${inventory.cell_run_id}/${name}.json`;
function parseInput(value: unknown, extra: string[] = []): Input {
  if (!exact(value, ['schema_version', 'campaign_id', 'orchestration_run_id', 'cell_run_id', 'expected_etag', 'actor', ...extra]) ||
      value.schema_version !== 1 || typeof value.campaign_id !== 'string' || typeof value.orchestration_run_id !== 'string' ||
      typeof value.cell_run_id !== 'string' || typeof value.expected_etag !== 'string' ||
      !exact(value.actor, ['mode', 'owner_id', 'fencing_token']) ||
      (value.actor.mode !== 'producer' && value.actor.mode !== 'controller') || typeof value.actor.owner_id !== 'string' ||
      typeof value.actor.fencing_token !== 'number' || !Number.isSafeInteger(value.actor.fencing_token))
    throw new Error('Invalid exact shutdown finalization request.');
  return { schema_version: 1, campaign_id: value.campaign_id, orchestration_run_id: value.orchestration_run_id,
    cell_run_id: value.cell_run_id, expected_etag: value.expected_etag,
    actor: { mode: value.actor.mode, owner_id: value.actor.owner_id, fencing_token: value.actor.fencing_token } };
}
function classify(inventory: RunInventory, capture: Record<string, unknown>): Verdict[] {
  if (capture.schema_version !== 1 || capture.field !== 'terminal_capture' ||
      canonical_json(capture.origin) !== canonical_json(origin(inventory)) ||
      capture.scope !== 'exact_kubernetes_status_and_sanitized_log_receipts' || !Array.isArray(capture.workloads))
    throw new Error('Terminal capture origin or schema is UNKNOWN.');
  const verdicts: Verdict[] = [];
  for (const resource of inventory.resources.filter(value => value.role === 'collector' && value.state !== 'absent')) {
    const identity = { api_version: resource.api_version, kind: resource.kind, name: resource.name, namespace: resource.namespace, uid: resource.uid };
    const matches = capture.workloads.filter(value => object(value) && canonical_json(value.identity) === canonical_json(identity));
    if (matches.length !== 1 || !object(matches[0]) || !matches[0].original_stop)
      throw new Error('Exact collector terminal stop evidence is UNKNOWN.');
    const stop = parseStop(matches[0].original_stop, inventory, 'bounded_drain', resource);
    const observed = matches[0].pods === undefined ? [] :
      parseStop({ ...stop, terminal_pods: matches[0].pods }, inventory, 'bounded_drain', resource).terminal_pods;
    const contradictory = observed.some(pod => {
      const ended = stop.terminal_pods.find(value => value.uid === pod.uid && value.name === pod.name);
      return !ended || pod.containers.length !== ended.containers.length || pod.containers.some(container => {
        const original = ended.containers.find(value => value.name === container.name);
        return !original || container.exit_code !== original.exit_code || container.oom_killed !== original.oom_killed ||
          original.restart_count !== undefined && container.restart_count !== original.restart_count;
      });
    });
    if (stop.initial_pods.length === 0 && (stop.initial_desired > 0 || inventory.load_admission !== 'not_authorized'))
      throw new Error('Collector original pod coverage is UNKNOWN.');
    if (new Set(stop.initial_pods.map(pod => pod.uid)).size !== stop.initial_pods.length ||
        new Set(stop.initial_pods.map(pod => pod.name)).size !== stop.initial_pods.length ||
        new Set(stop.terminal_pods.map(pod => pod.uid)).size !== stop.terminal_pods.length ||
        stop.terminal_pods.some(ended => !stop.initial_pods.some(pod => pod.uid === ended.uid && pod.name === ended.name)) ||
        [...stop.initial_pods, ...stop.terminal_pods].some(pod => new Set(pod.containers.map(value => value.name)).size !== pod.containers.length))
      throw new Error('Collector terminal pod/container identity coverage is UNKNOWN.');
    for (const pod of stop.initial_pods) for (const initial of pod.containers) {
      const ended = stop.terminal_pods.find(value => value.uid === pod.uid && value.name === pod.name);
      const terminal = ended?.containers.find(value => value.name === initial.name);
      const legacyZero = terminal?.exit_code === 0 && initial.restart_count === undefined && terminal.restart_count === undefined;
      const sameInstance = ended?.containers.length === pod.containers.length &&
        ended.containers.every(container => pod.containers.some(value => value.name === container.name)) &&
        (legacyZero || initial.restart_count !== undefined && terminal?.restart_count === initial.restart_count);
      const marker = sameInstance && stop.logs.some(log => log.phase === 'terminal' && log.pod_uid === pod.uid &&
        log.pod_name === pod.name && log.container === initial.name && log.drain_complete_marker === true);
      const classification: Classification = contradictory || !sameInstance || terminal?.exit_code === null || terminal?.exit_code === undefined ?
        'terminal_status_unavailable' : terminal.oom_killed ? 'exit_oom_killed' : terminal.exit_code === 0 ? 'exit_0' :
          terminal.exit_code === 143 ? marker ? 'exit_143_terminal_reconciliation_unavailable' : 'exit_143_without_drain_marker' : 'exit_nonzero';
      verdicts.push({ pod_name: pod.name, pod_uid: pod.uid, container: initial.name,
        restart_count: initial.restart_count ?? null, classification });
    }
  }
  return verdicts.sort((a, b) => canonical_json([a.pod_uid, a.container]).localeCompare(canonical_json([b.pod_uid, b.container])));
}
type SourceReason = 'no_baseline' | 'terminal_capture_unavailable' | 'terminal_delta_unavailable' | 'terminal_identity_unavailable';
type Reason = SourceReason | 'reconciliation_unavailable' | 'reconciliation_mismatch' | 'exit_143_unavailable' | 'collector_exit_failed';
interface Reconciliation {
  profile: 'single_hot_table' | 'other';
  proof: null | { status: 'matched' | 'mismatch' | 'unavailable';
    post_teardown_verification: 'matched_final_delta_log' | 'mismatch' | 'unavailable';
    expected_rows: number | null; observed_rows: number | null;
    settled_zero_ambiguity: boolean | null; teardown_admissions_clear: boolean | null };
}
function parseReconciliation(raw: unknown): Reconciliation {
  if (!exact(raw, ['profile', 'proof']) || (raw.profile !== 'single_hot_table' && raw.profile !== 'other'))
    throw new Error('Invalid exact reconciliation profile/proof.');
  if (raw.proof === null) return { profile: raw.profile, proof: null };
  const proof = raw.proof;
  if (!exact(proof, ['status', 'post_teardown_verification', 'expected_rows', 'observed_rows',
    'settled_zero_ambiguity', 'teardown_admissions_clear']) ||
      !['matched', 'mismatch', 'unavailable'].includes(String(proof.status)) ||
      !['matched_final_delta_log', 'mismatch', 'unavailable'].includes(String(proof.post_teardown_verification)) ||
      ![proof.expected_rows, proof.observed_rows].every(value => value === null ||
        typeof value === 'number' && Number.isSafeInteger(value) && value >= 0) ||
      ![proof.settled_zero_ambiguity, proof.teardown_admissions_clear].every(value => value === null || typeof value === 'boolean'))
    throw new Error('Invalid exact measured reconciliation proof.');
  return raw as unknown as Reconciliation;
}
async function refs(azure: Azure, inventory: RunInventory, guard: () => Promise<void>, deadlineUnixMs: number) {
  const destination = inventory.evidence_destination;
  if (destination && (destination.account !== azure.c.account || destination.container !== azure.c.checkpoints))
    throw new Error('Finalization evidence destination differs from the selected stamp.');
  const names = { terminal_capture: destination ?
    destination.blob_prefix.replace(/\/$/, '') + `/custody/${inventory.cell_run_id}/terminal_capture.json` : null,
    delta_before: cellBlob(inventory, 'delta-before'), delta_terminal: cellBlob(inventory, 'delta-terminal') };
  const [capture, before, terminal] = await Promise.all([
    names.terminal_capture ? readBlob(azure, names.terminal_capture, guard) : Promise.resolve(undefined),
    readBlob(azure, names.delta_before, guard), readBlob(azure, names.delta_terminal, guard)]);
  await guard();
  const initial = before ? baselineDelta(before.value, inventory, azure) : null;
  const final = terminal ? baselineDelta(terminal.value, inventory, azure, 'terminal_after_writers_stopped') : null;
  if (before && terminal && initial && final && (terminal.value.baseline_etag !== before.etag ||
      initial.table_root !== final.table_root || initial.observation === 'table' &&
      (final.observation !== 'table' || initial.table_id !== final.table_id || final.version < initial.version)))
    throw new Error('Terminal Delta source differs from its immutable original baseline.');
  let verdicts: Verdict[] = [], identityUnavailable = false;
  if (capture) {
    if (capture.value.schema_version !== 1 || capture.value.field !== 'terminal_capture' ||
        canonical_json(capture.value.origin) !== canonical_json(origin(inventory)) ||
        capture.value.scope !== 'exact_kubernetes_status_and_sanitized_log_receipts' || !Array.isArray(capture.value.workloads))
      throw new Error('Terminal capture origin or schema differs from the exact original run.');
    try { verdicts = classify(inventory, capture.value); }
    catch { identityUnavailable = true; }
  }
  const unavailable_reason: SourceReason | null = !before ? 'no_baseline' :
    !capture || inventory.cleanup.terminal_capture.state !== 'complete' ? 'terminal_capture_unavailable' :
      !terminal ? 'terminal_delta_unavailable' :
        identityUnavailable || verdicts.length === 0 || verdicts.some(value => value.classification === 'terminal_status_unavailable') ?
          'terminal_identity_unavailable' : null;
  const make = (blob: string | null, value: { etag: string; value: Record<string, unknown> } | undefined): Ref | null =>
    blob && value ? { blob, etag: value.etag, sha256: digest(value.value) } : null;
  let terminal_summaries: BoundTerminalSummary[] | null = null;
  let summary_required = false;
  if (capture && Array.isArray(capture.value.workloads)) {
    summary_required = capture.value.workloads.some(workload => object(workload) && object(workload.original_stop) &&
      (workload.original_stop.terminal_evidence_version === 1 ||
        Array.isArray(workload.original_stop.logs) && workload.original_stop.logs.some(log =>
          object(log) && Object.hasOwn(log, 'terminal_summary_status'))));
    if (!identityUnavailable && verdicts.length > 0 && verdicts.every(value => value.classification === 'exit_0') && initial && final &&
        final.observation === 'table' && final.row_count !== null && (initial.observation === 'observed_absent' || initial.row_count !== null)) {
      try {
        const candidates: BoundTerminalSummary[] = [];
        for (const resource of inventory.resources.filter(value => value.role === 'collector' && value.state !== 'absent')) {
          const workload = capture.value.workloads.find(value => object(value) && canonical_json(value.identity) ===
            canonical_json({ api_version: resource.api_version, kind: resource.kind, name: resource.name, namespace: resource.namespace, uid: resource.uid }));
          if (!object(workload)) throw new Error('Original terminal summary workload is unavailable.');
          const stop = parseStop(workload.original_stop, inventory, 'bounded_drain', resource);
          if (stop.initial_desired !== stop.initial_pods.length)
            throw new Error('Original terminal summary replica coverage is unavailable.');
          for (const pod of stop.initial_pods) for (const container of pod.containers) {
            const ended = stop.terminal_pods.find(value => value.uid === pod.uid && value.name === pod.name);
            const terminal = ended?.containers.find(value => value.name === container.name);
            const logs = stop.logs.filter(log => log.phase === 'terminal' && log.pod_uid === pod.uid &&
              log.pod_name === pod.name && log.container === container.name);
            if (container.restart_count !== 0 || terminal?.restart_count !== 0 || terminal.exit_code !== 0 ||
                logs.length !== 1 || logs[0]!.full_log_read !== true || logs[0]!.restart_count !== 0 ||
                logs[0]!.collector_startup_observed !== true ||
                logs[0]!.terminal_summary_status !== 0 || logs[0]!.bytes >= COLLECTOR_TERMINAL_LOG_MAX_BYTES)
              throw new Error('Original terminal summary log completeness/lifetime is unavailable.');
            const summary = parseTerminalSummary(logs[0]!.terminal_summary);
            if (!terminalSummaryClear(summary)) throw new Error('Terminal summary accounting is incomplete or ambiguous.');
            candidates.push({ pod_name: pod.name, pod_uid: pod.uid, container: container.name,
              restart_count: 0, exit_code: 0, summary });
          }
        }
        const expectedTable = terminalTableDigest(azure.c.account, azure.c.data, final.table_root);
        const tuples = candidates.flatMap(value => value.summary.commits);
        if (new Set(candidates.map(value => `${value.pod_uid}:${value.container}`)).size !== candidates.length ||
            tuples.some(value => value.table_id !== expectedTable) ||
            new Set(tuples.map(value => value.version)).size !== tuples.length)
          throw new Error('Terminal summary table/version identities differ or duplicate.');
        const frozen = await readFrozenDeltaCommits(azure, initial, final, guard, deadlineUnixMs);
        const numeric = tuples.map(({ version, rows, files, parquet_bytes }) => ({ version, rows, files, parquet_bytes }))
          .sort((a, b) => a.version - b.version);
        const publicationVersions = candidates.flatMap(value => value.summary.publication_evidence.flatMap(call =>
          call.outcomes.flatMap(outcome => outcome.delta_version === null ? [] : [outcome.delta_version])))
          .sort((a, b) => a - b);
        const publicationIdentities = candidates.flatMap(value => value.summary.publication_evidence.flatMap(call =>
          call.outcomes.map(outcome => JSON.stringify([outcome.app_id, outcome.app_version]))));
        const frozenVersions = frozen.map(value => value.version).sort((a, b) => a - b);
        const rows = candidates.reduce((sum, value) => sum + value.summary.committed_rows, 0);
        if (canonical_json(numeric) !== canonical_json(frozen) ||
            new Set(publicationIdentities).size !== publicationIdentities.length ||
            canonical_json(publicationVersions) !== canonical_json(frozenVersions) || !Number.isSafeInteger(rows) ||
            rows !== final.row_count - (initial.observation === 'table' ? initial.row_count! : 0))
          throw new Error('Terminal summary tuples differ from independently frozen Delta.');
        terminal_summaries = candidates;
      } catch (error) {
        if (error instanceof CampaignConflictError || error instanceof CleanupDeadlineExpiredError) throw error;
      }
    }
  }
  await guard();
  return { terminal_capture: make(names.terminal_capture, capture), delta_before: make(names.delta_before, before),
    delta_terminal: make(names.delta_terminal, terminal), verdicts, initial, final, unavailable_reason, terminal_summaries, summary_required };
}
async function context(store: CampaignStore, azure: Azure, input: Input, env: NodeJS.ProcessEnv) {
  const request = { schema_version: 1, campaign_id: input.campaign_id, orchestration_run_id: input.orchestration_run_id,
    cell_run_id: input.cell_run_id, actor: input.actor };
  const current = await readCellRunInventory(store, request, env), deadline = Date.now() + 30_000;
  if (current.etag !== input.expected_etag) throw new CampaignConflictError();
  const guard = async () => {
    if (Date.now() >= deadline) throw new CleanupDeadlineExpiredError('Shutdown journal storage action budget expired.');
    const next = await readCellRunInventory(store, request, env);
    if (next.etag !== input.expected_etag || canonical_json(next.inventory) !== canonical_json(current.inventory))
      throw new CampaignConflictError();
  };
  return { inventory: current.inventory, guard, deadline, evidence: await refs(azure, current.inventory, guard, deadline) };
}
export async function shutdownFinalizationInputs(store: CampaignStore, azure: Azure, raw: unknown, env: NodeJS.ProcessEnv) {
  const input = parseInput(raw), current = await context(store, azure, input, env);
  return { schema_version: 1, inventory_etag: input.expected_etag, terminal_capture: current.evidence.terminal_capture,
    delta_before: current.evidence.delta_before, delta_terminal: current.evidence.delta_terminal,
    unavailable_reason: current.evidence.unavailable_reason, terminal_summaries: current.evidence.terminal_summaries };
}
function valueFor(input: Input, inventory: RunInventory, evidence: Awaited<ReturnType<typeof refs>>, reconciliation: Reconciliation) {
  const { proof } = reconciliation;
  let reason_code: Reason | null = evidence.unavailable_reason;
  let status: 'validated' | 'unavailable' | 'failed' = 'unavailable';
  if (!reason_code && evidence.verdicts.some(value => value.classification.includes('143')))
    reason_code = 'exit_143_unavailable';
  if (!reason_code && evidence.verdicts.some(value => value.classification !== 'exit_0')) {
    reason_code = 'collector_exit_failed'; status = 'failed';
  }
  if (!reason_code) {
    if (proof?.status === 'mismatch' || proof?.post_teardown_verification === 'mismatch') {
      status = 'failed'; reason_code = 'reconciliation_mismatch';
    } else if (reconciliation.profile !== 'single_hot_table' || !proof || proof.status !== 'matched' ||
        proof.post_teardown_verification !== 'matched_final_delta_log' || proof.expected_rows === null ||
        proof.observed_rows === null || proof.settled_zero_ambiguity !== true || proof.teardown_admissions_clear !== true ||
        evidence.summary_required && evidence.terminal_summaries === null ||
        !evidence.initial || evidence.initial.observation === 'table' && evidence.initial.row_count === null ||
        !evidence.final || evidence.final.observation !== 'table' || evidence.final.row_count === null)
      reason_code = 'reconciliation_unavailable';
    else {
      const observed = evidence.final.row_count - (evidence.initial.observation === 'table' ? evidence.initial.row_count! : 0);
      if (observed < 0 || observed !== proof.expected_rows || observed !== proof.observed_rows) {
        status = 'failed'; reason_code = 'reconciliation_mismatch';
      } else status = 'validated';
    }
  }
  return { schema_version: 1, inventory_etag: input.expected_etag, actor: input.actor, origin: origin(inventory),
    terminal_capture: evidence.terminal_capture, delta_before: evidence.delta_before, delta_terminal: evidence.delta_terminal,
    verdicts: evidence.verdicts, reconciliation, status, reason_code, release_allowed: status === 'validated' };
}
async function readValidated(azure: Azure, inventory: RunInventory, evidence: Awaited<ReturnType<typeof refs>>, guard: () => Promise<void>) {
  const blob = cellBlob(inventory, 'shutdown-finalization'), saved = await readBlob(azure, blob, guard);
  await guard();
  if (!saved || !exact(saved.value, ['schema_version', 'inventory_etag', 'actor', 'origin', 'terminal_capture',
    'delta_before', 'delta_terminal', 'verdicts', 'reconciliation', 'status', 'reason_code', 'release_allowed']) ||
      typeof saved.value.inventory_etag !== 'string' || !saved.value.inventory_etag || saved.value.inventory_etag.length > 512 ||
      saved.value.inventory_etag === '*' || saved.value.inventory_etag.startsWith('W/') ||
      /[\u0000-\u001f]/.test(saved.value.inventory_etag) || !exact(saved.value.actor, ['mode', 'owner_id', 'fencing_token']) ||
      !['producer', 'controller'].includes(String(saved.value.actor.mode)) ||
      typeof saved.value.actor.owner_id !== 'string' || typeof saved.value.actor.fencing_token !== 'number' ||
      !Number.isSafeInteger(saved.value.actor.fencing_token) ||
      (saved.value.actor.mode === 'producer' ?
        saved.value.actor.owner_id !== inventory.owner_id || saved.value.actor.fencing_token !== inventory.fencing_token :
        saved.value.actor.fencing_token <= inventory.fencing_token))
    throw new Error('Matching immutable shutdown finalization is unavailable.');
  const actor = saved.value.actor as unknown as CellCleanupActor, reconciliation = parseReconciliation(saved.value.reconciliation);
  const expected = valueFor({ schema_version: 1, campaign_id: inventory.campaign_id,
    orchestration_run_id: inventory.orchestration_run_id, cell_run_id: inventory.cell_run_id,
    expected_etag: saved.value.inventory_etag, actor }, inventory, evidence, reconciliation);
  if (canonical_json(expected) !== canonical_json(saved.value))
    throw new Error('Shutdown finalization differs from independently read sources and reconciliation.');
  return { ...expected, blob, etag: saved.etag, sha256: digest(saved.value) };
}
export async function shutdownFinalize(store: CampaignStore, azure: Azure, raw: unknown, env: NodeJS.ProcessEnv) {
  const input = parseInput(raw, ['terminal_capture', 'delta_before', 'delta_terminal', 'reconciliation']);
  if (!object(raw)) throw new Error('Invalid finalization request.');
  const reconciliation = parseReconciliation(raw.reconciliation), current = await context(store, azure, input, env);
  for (const key of ['terminal_capture', 'delta_before', 'delta_terminal'] as const)
    if (canonical_json(raw[key]) !== canonical_json(current.evidence[key])) throw new Error(`Finalization ${key} reference changed.`);
  const verifySources = async () => {
    const observed = await refs(azure, current.inventory, current.guard, current.deadline);
    if (canonical_json(observed) !== canonical_json(current.evidence)) throw new CampaignConflictError();
    await current.guard();
  };
  const value = valueFor(input, current.inventory, current.evidence, reconciliation);
  const blob = cellBlob(current.inventory, 'shutdown-finalization');
  await verifySources();
  const previous = await readBlob(azure, blob, current.guard);
  await current.guard();
  let etag: string;
  if (previous) {
    if (canonical_json(previous.value) !== canonical_json(value)) throw new CampaignWriteOutcomeUnknownError();
    etag = previous.etag;
  } else {
    try { etag = await immutableBlob(azure, blob, value, current.guard); }
    catch {
      const saved = await readBlob(azure, blob, current.guard);
      await current.guard();
      if (!saved || canonical_json(saved.value) !== canonical_json(value)) throw new CampaignWriteOutcomeUnknownError();
      etag = saved.etag;
    }
  }
  await verifySources();
  return { ...value, blob, etag, sha256: digest(value) };
}
export async function shutdownFinalizationRead(store: CampaignStore, azure: Azure, raw: unknown, env: NodeJS.ProcessEnv) {
  const input = parseInput(raw), current = await context(store, azure, input, env);
  const result = await readValidated(azure, current.inventory, current.evidence, current.guard);
  if (canonical_json(await refs(azure, current.inventory, current.guard, current.deadline)) !== canonical_json(current.evidence))
    throw new CampaignConflictError();
  await current.guard();
  return result;
}
export async function requireFinalizedCollectorStop(store: CampaignStore, azure: Azure, inventory: RunInventory,
  authority: CleanupMutationAuthority): Promise<void> {
  await authority.assertCurrent();
  const snapshot = await store.read();
  if (!snapshot || canonical_json(snapshot.state.runInventory) !== canonical_json(inventory)) throw new CampaignConflictError();
  const deadline = Date.now() + 35_000;
  const guard = async () => {
    if (Date.now() >= deadline) throw new CleanupDeadlineExpiredError('Shutdown source verification shared deadline expired.');
    await authority.assertCurrent();
    const current = await store.read();
    if (!current || current.etag !== snapshot.etag || canonical_json(current.state.runInventory) !== canonical_json(inventory))
      throw new CampaignConflictError();
  };
  const evidence = await refs(azure, inventory, guard, deadline);
  if (evidence.unavailable_reason || evidence.verdicts.some(value => value.classification.includes('143')))
    throw new Error('Collector shutdown finalization is unavailable or failed; release and handoff withheld.');
  const saved = await readValidated(azure, inventory, evidence, guard);
  if (!saved.release_allowed && !(authority.mode === 'controller' && hasVerifiedUnsettledDrain(snapshot.state) &&
      saved.status === 'failed' && saved.reason_code === 'collector_exit_failed' && evidence.delta_before !== null &&
      saved.verdicts.length > 0 && saved.verdicts.every(value =>
        ['exit_0', 'exit_nonzero', 'exit_oom_killed'].includes(value.classification))))
    throw new Error('Collector shutdown finalization is unavailable or failed; release and handoff withheld.');
  if (canonical_json(await refs(azure, inventory, guard, deadline)) !== canonical_json(evidence)) throw new CampaignConflictError();
  await guard();
}
