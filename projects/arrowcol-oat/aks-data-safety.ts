import { createHash } from 'node:crypto';
import { canonical_json, type Azure } from '../../infra/azure/bootstrap.js';
import { assertExactConnectionStrings, managementRows, parseKustoQueryResult, parseQualifiedDeltaDefinition } from '../../infra/azure/oat_qap.js';
import { CampaignConflictError, CampaignFencedError, CampaignWriteOutcomeUnknownError, hasVerifiedUnsettledDrain, validateCampaignState,
  type CampaignState, type CampaignStore, type RunInventory } from './aks-campaign.js';
import { readDeltaReference, parseDeltaReplayReference, verifyDeltaPreservation, type DeltaReplayReference } from './aks-delta-preservation.js';
import type { CleanupMutationAuthority } from './aks-cleanup-executor.js';

const digest = (value: unknown) => createHash('sha256').update(canonical_json(value)).digest('hex');
function object(value: unknown): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Data preservation evidence is UNKNOWN.');
  return value as Record<string, unknown>;
}
function integer(value: unknown): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0) throw new Error('Data preservation count is UNKNOWN.');
  return value;
}
function identifier(value: unknown): string {
  if (typeof value !== 'string' || !/^[A-Za-z_][A-Za-z0-9_]{0,127}$/.test(value)) throw new Error('Data preservation Kusto identity is unsupported.');
  return value;
}
function strong(value: unknown): value is string {
  return typeof value === 'string' && value.length > 0 && value.length <= 512 && value !== '*' && !value.startsWith('W/') && !/[\u0000-\u001f]/.test(value);
}
function origin(inventory: RunInventory) {
  return { campaign_id: inventory.campaign_id, owner_id: inventory.owner_id, fencing_token: inventory.fencing_token,
    orchestration_run_id: inventory.orchestration_run_id, cell_run_id: inventory.cell_run_id, namespace: inventory.namespace };
}
const campaignBlob = (state: CampaignState) => `_arrowcol-custody/campaigns/${state.campaignId}/baseline.json`;
const cellBlob = (inventory: RunInventory, phase: string) => `_arrowcol-custody/campaigns/${inventory.campaign_id}` +
  `/runs/${inventory.orchestration_run_id}/cells/${inventory.cell_run_id}/${phase}.json`;
export interface KustoCountReceipt {
  database: string; table: string; kind: 'regular' | 'external'; rows: number;
  query_sha256: string; result_sha256: string; observed_at_unix_ms: number;
}
export interface StampKustoTable {
  database: string; name: string; kind: 'regular' | 'external'; schema_sha256: string; definition_sha256: string | null;
  before: KustoCountReceipt; qap_before: QapStateReceipt | null;
}
export interface QapStateReceipt {
  observed_at_unix_ms: number; query_sha256: string; is_healthy: boolean; not_healthy_reason_present: boolean;
  pending_files: number; row_count: number; accelerated_row_count: number; table_version: number;
}
export interface StampKustoBaseline {
  schema_version: 1; scope: 'configured_stamp_databases_non_current_run_tables'; databases: string[];
  tables: StampKustoTable[]; source: '.show tables and .show external tables with exact current run ownership exclusion';
  empty_reason: 'positive_inventory_contains_no_non_run_tables' | null; observed_at_unix_ms: number;
}
export interface CaptureCellBaselineEnvelope {
  schema_version: 1; campaign_id: string; orchestration_run_id: string; cell_run_id: string;
  expected_etag: string; hot_delta_relative_path: string; actor: { mode: 'producer'; owner_id: string; fencing_token: number };
}
export interface CellBaselineReceipt {
  status: 'captured'; campaign_id: string; owner_id: string; fencing_token: number; orchestration_run_id: string;
  cell_run_id: string; namespace_uid: string; hot_delta_relative_path: string; blob: string; etag: string;
  delta_state: 'table' | 'observed_absent'; table_id: string | null; delta_version: number | null; rows: number | null;
}
export interface DataPreservationReceipt {
  schema_version: 1; scope: 'delta_and_non_run_kusto_preservation'; campaign_id: string; cell_run_id: string;
  status: 'preserved' | 'unknown' | 'failed'; reason: string | null; observed_at_unix_ms: number; evidence_etag: string;
  delta: Awaited<ReturnType<typeof verifyDeltaPreservation>>;
  non_run_kusto: Array<{ identity: StampKustoTable; after: KustoCountReceipt; qap_after: QapStateReceipt | null }>;
  run_owned_qap_after: { rows: null; reason: 'run_owned_qap_removed_by_design' };
}
function validateQapState(value: unknown): QapStateReceipt {
  const record = object(value);
  if (Object.keys(record).sort().join() !== ['observed_at_unix_ms', 'query_sha256', 'is_healthy', 'not_healthy_reason_present',
    'pending_files', 'row_count', 'accelerated_row_count', 'table_version'].sort().join() ||
      typeof record.is_healthy !== 'boolean' || typeof record.not_healthy_reason_present !== 'boolean' ||
      !/^[a-f0-9]{64}$/.test(String(record.query_sha256)) || integer(record.observed_at_unix_ms) > Date.now())
    throw new Error('Stored exact QAP state proof is malformed.');
  return { observed_at_unix_ms: integer(record.observed_at_unix_ms), query_sha256: String(record.query_sha256),
    is_healthy: record.is_healthy, not_healthy_reason_present: record.not_healthy_reason_present,
    pending_files: integer(record.pending_files), row_count: integer(record.row_count),
    accelerated_row_count: integer(record.accelerated_row_count), table_version: integer(record.table_version) };
}
function validateCampaignBaseline(value: Record<string, unknown>, state: CampaignState) {
  if (value.schema_version !== 1 || value.campaign_id !== state.campaignId || value.owner_id !== state.ownerId ||
      value.producer_fencing_token !== state.runInventory?.fencing_token || value.started_at_unix_ms !== state.startedAtMs ||
      integer(value.captured_at_unix_ms) < state.startedAtMs || integer(value.captured_at_unix_ms) > (state.fencedAtMs ?? Date.now()))
    throw new Error('Original immutable campaign baseline belongs to another campaign/origin/time.');
}
async function readBlob(azure: Azure, blob: string, guard: () => Promise<void>) {
  await guard();
  const [bytes, headers] = await azure.blob('GET', azure.c.checkpoints, blob, undefined, undefined, {}, true, true);
  if (bytes === null) return undefined;
  if (bytes.length > 16_777_216 || !strong(headers.etag)) throw new Error('Preservation Blob is unbounded or lacks a strong version.');
  return { value: object(JSON.parse(bytes.toString()) as unknown), etag: headers.etag };
}
async function immutableBlob(azure: Azure, blob: string, value: unknown, guard: () => Promise<void>) {
  await guard();
  try {
    const [, headers] = await azure.blob('PUT', azure.c.checkpoints, blob, undefined, canonical_json(value),
      { 'x-ms-blob-type': 'BlockBlob', 'If-None-Match': '*' });
    const confirmed = await readBlob(azure, blob, guard);
    if (headers[':status'] !== '201' || !strong(headers.etag) || confirmed?.etag !== headers.etag ||
        canonical_json(confirmed.value) !== canonical_json(value)) throw new CampaignWriteOutcomeUnknownError();
    return headers.etag;
  } catch { throw new CampaignWriteOutcomeUnknownError(); }
}
async function count(azure: Azure, database: string, table: string, kind: StampKustoTable['kind'],
  guard: () => Promise<void>): Promise<KustoCountReceipt> {
  identifier(database); identifier(table);
  const query = (kind === 'external' ? `external_table('${table}')` : `['${table}']`) + ' | summarize Count=count()';
  await guard();
  const [bytes, headers] = await azure.request('POST', azure.c.uri + '/v1/rest/query', 'https://kusto.kusto.windows.net',
    canonical_json({ db: database, csl: query }), { 'Content-Type': 'application/json' }, false, true);
  if (!bytes || bytes.length > 1_048_576 || headers[':status'] !== '200') throw new Error('Kusto preservation count has no positive response.');
  const parsed = parseKustoQueryResult(JSON.parse(bytes.toString()) as unknown);
  if (parsed.rows.length !== 1 || Object.keys(parsed.rows[0]!).join() !== 'Count')
    throw new Error('Kusto preservation count result shape differs.');
  await guard();
  return { database, table, kind, rows: integer(parsed.rows[0]!.Count), query_sha256: digest(query), result_sha256: digest(parsed),
    observed_at_unix_ms: Date.now() };
}
async function schema(azure: Azure, database: string, table: string, kind: StampKustoTable['kind'], guard: () => Promise<void>) {
  await guard();
  const result = managementRows(await azure.kql(`.show ${kind === 'external' ? 'external ' : ''}table ['${identifier(table)}'] schema as json`, identifier(database), true));
  if (result.length !== 1 || result[0].TableName !== table || result[0].DatabaseName !== database)
    throw new Error('Kusto preservation schema belongs to another table.');
  const parsed = typeof result[0].Schema === 'string' ? JSON.parse(result[0].Schema) as unknown : result[0].Schema;
  if (!Array.isArray(object(parsed).OrderedColumns)) throw new Error('Kusto preservation schema is incomplete.');
  return digest(parsed);
}
async function qapState(azure: Azure, database: string, table: string, guard: () => Promise<void>): Promise<QapStateReceipt | null> {
  await guard();
  const policyCommand = `.show external table ['${identifier(table)}'] policy query_acceleration`;
  const policyRows = managementRows(await azure.kql(policyCommand, identifier(database), true));
  if (policyRows.length !== 1 || policyRows[0].EntityName !== `[${database}].[${table}]` || policyRows[0].EntityType !== 'ExternalTable')
    throw new Error('Exact QAP policy identity is UNKNOWN.');
  if (policyRows[0].Policy === null) return null;
  const policy = object(typeof policyRows[0].Policy === 'string' ? JSON.parse(policyRows[0].Policy) as unknown : policyRows[0].Policy);
  if (policy.IsEnabled === false) return null;
  if (policy.IsEnabled !== true) throw new Error('Exact QAP enabled state is UNKNOWN.');
  const detailsCommand = `.show external table ['${table}'] details`;
  await guard();
  const details = managementRows(await azure.kql(detailsCommand, database, true));
  if (details.length !== 1) throw new Error('Exact QAP details are ambiguous.');
  const raw = details[0].QueryAccelerationState;
  const state = object(typeof raw === 'string' ? JSON.parse(raw) as unknown : raw);
  const statisticsCommand = `.show external tables operations query_acceleration statistics | where ExternalTableName == '${table}'` +
    ' | project ExternalTableName, AccelerationPendingDataFilesCount';
  await guard();
  const stats = managementRows(await azure.kql(statisticsCommand, database, true));
  if (stats.length !== 1 || stats[0].ExternalTableName !== table || typeof state.IsHealthy !== 'boolean' ||
      !(state.NotHealthyReason === null || typeof state.NotHealthyReason === 'string'))
    throw new Error('Exact QAP health/backlog is UNKNOWN.');
  return { observed_at_unix_ms: Date.now(), query_sha256: digest([policyCommand, detailsCommand, statisticsCommand]),
    is_healthy: state.IsHealthy, not_healthy_reason_present: typeof state.NotHealthyReason === 'string' && state.NotHealthyReason.length > 0,
    pending_files: integer(stats[0].AccelerationPendingDataFilesCount), row_count: integer(state.RowCount),
    accelerated_row_count: integer(state.AcceleratedRowCount), table_version: integer(state.TableVersion) };
}
export async function captureStampKustoBaseline(azure: Azure, state: CampaignState, guard: () => Promise<void>): Promise<StampKustoBaseline> {
  const databases = [...new Set([identifier(azure.c.database), identifier(azure.c.experimentDatabase)])], tables: StampKustoTable[] = [];
  for (const database of databases) for (const kind of ['regular', 'external'] as const) {
    await guard();
    const listing = managementRows(await azure.kql(kind === 'regular' ? '.show tables' : '.show external tables', database, true));
    if (listing.length > 256) throw new Error('Stamp Kusto baseline exceeds its bounded table scope.');
    const seen = new Set<string>();
    for (const item of listing) {
      const name = identifier(item.TableName);
      if (seen.has(name)) throw new Error('Stamp Kusto inventory has duplicate table identities.');
      seen.add(name);
      const qap = state.runInventory?.qap;
      if (kind === 'external' && database === qap?.database && qap.reference &&
          qap.bindings.some(binding => binding.external_table === name)) {
        const owner = `arrowcol-oat:${qap.reference.run_id}:${qap.reference.nonce}:${qap.reference.manifest_sha256}`;
        if (parseQualifiedDeltaDefinition(item, name).docString !== owner) throw new Error('Current run QAP exclusion lacks exact original ownership.');
        continue;
      }
      tables.push({ database, name, kind, schema_sha256: await schema(azure, database, name, kind, guard),
        definition_sha256: kind === 'external' ? digest(parseQualifiedDeltaDefinition(item, name).definition) : null,
        before: await count(azure, database, name, kind, guard),
        qap_before: kind === 'external' ? await qapState(azure, database, name, guard) : null });
    }
  }
  return { schema_version: 1, scope: 'configured_stamp_databases_non_current_run_tables', databases, tables,
    source: '.show tables and .show external tables with exact current run ownership exclusion',
    empty_reason: tables.length === 0 ? 'positive_inventory_contains_no_non_run_tables' : null, observed_at_unix_ms: Date.now() };
}
function validateOrigin(value: Record<string, unknown>, inventory: RunInventory) {
  if (value.schema_version !== 1 || canonical_json(value.origin) !== canonical_json(origin(inventory)))
    throw new Error('Preservation reference belongs to another immutable original run/namespace.');
}
function baselineDelta(value: Record<string, unknown>, inventory: RunInventory, azure: Azure,
  phase: 'pre_load' | 'terminal_after_writers_stopped' = 'pre_load') {
  validateOrigin(value, inventory);
  if (value.observed_phase !== phase || phase === 'pre_load' &&
      (value.load_admission !== 'not_authorized' || !strong(value.inventory_etag)))
    throw new Error('Delta preservation reference lacks its original phase/admission/version proof.');
  if (typeof value.hot_delta_relative_path !== 'string' ||
      inventory.qap?.bindings.filter(binding => binding.relative_path === value.hot_delta_relative_path).length !== 1)
    throw new Error('Preservation hot table does not exactly match one planned QAP binding.');
  return parseDeltaReplayReference(value.delta, azure, value.hot_delta_relative_path);
}
export async function captureCellBaseline(store: CampaignStore, azure: Azure, input: unknown): Promise<CellBaselineReceipt> {
  const value = object(input), actor = object(value.actor);
  if (Object.keys(value).sort().join() !== ['schema_version', 'campaign_id', 'orchestration_run_id', 'cell_run_id',
    'expected_etag', 'hot_delta_relative_path', 'actor'].sort().join() ||
      Object.keys(actor).sort().join() !== ['mode', 'owner_id', 'fencing_token'].sort().join() ||
      value.schema_version !== 1 || actor.mode !== 'producer' || !strong(value.expected_etag))
    throw new Error('Malformed exact capture-cell-baseline envelope.');
  const original = await store.read();
  if (!original) throw new CampaignFencedError();
  const state = validateCampaignState(original.state), inventory = state.runInventory;
  if (!inventory || inventory.campaign_id !== value.campaign_id || inventory.cell_run_id !== value.cell_run_id ||
      inventory.orchestration_run_id !== value.orchestration_run_id || inventory.owner_id !== actor.owner_id ||
      inventory.fencing_token !== actor.fencing_token || state.fencingToken !== actor.fencing_token)
    throw new CampaignFencedError();
  if (original.etag !== value.expected_etag) throw new CampaignConflictError();
  const hot = value.hot_delta_relative_path;
  if (!inventory.namespace.uid || typeof hot !== 'string' || inventory.qap?.bindings.filter(binding => binding.relative_path === hot).length !== 1)
    throw new Error('Select exactly one explicit planned hot Delta table; missing/ambiguous paths never default.');
  const guard = async () => {
    const current = await store.read(), now = Date.now();
    if (!current || current.state.status !== 'active' || current.state.campaignId !== state.campaignId ||
        current.state.ownerId !== inventory.owner_id || current.state.fencingToken !== inventory.fencing_token ||
        canonical_json(current.state.runInventory) !== canonical_json(inventory) || now < current.state.heartbeatAtMs ||
        now - current.state.heartbeatAtMs >= 180_000 || now >= current.state.expiresAtMs) throw new CampaignFencedError();
  };
  await guard();
  const campaign = await readBlob(azure, campaignBlob(state), guard);
  if (!campaign || !object(campaign.value).non_run_kusto) throw new Error('Capture the immutable campaign capacity/Kusto baseline before cell load.');
  validateCampaignBaseline(campaign.value, state);
  const { baselineSystemCount } = await import('./aks-default-providers.js');
  await baselineSystemCount(azure, state, campaign.etag);
  const blob = cellBlob(inventory, 'delta-before'), previous = await readBlob(azure, blob, guard);
  let delta: DeltaReplayReference, etag: string;
  if (previous) { delta = baselineDelta(previous.value, inventory, azure); etag = previous.etag; }
  else {
    if (inventory.load_admission !== 'not_authorized')
      throw new Error('A new pre-load reference requires positively persisted not_authorized admission.');
    delta = await readDeltaReference(azure, hot, guard);
    etag = await immutableBlob(azure, blob, { schema_version: 1, origin: origin(inventory), inventory_etag: original.etag,
      observed_phase: 'pre_load', load_admission: 'not_authorized',
      hot_delta_relative_path: hot, delta }, guard);
  }
  if (delta.table_id !== null && delta.row_count === null)
    throw new Error('Pre-load row coverage is UNKNOWN; the persisted reference is not load authorization.');
  await guard();
  return { status: 'captured', campaign_id: inventory.campaign_id, owner_id: inventory.owner_id, fencing_token: inventory.fencing_token,
    orchestration_run_id: inventory.orchestration_run_id, cell_run_id: inventory.cell_run_id, namespace_uid: inventory.namespace.uid,
    hot_delta_relative_path: hot, blob, etag, delta_state: delta.table_id === null ? 'observed_absent' : 'table',
    table_id: delta.table_id, delta_version: delta.version, rows: delta.row_count };
}

export async function captureTerminalData(store: CampaignStore, azure: Azure, authority: CleanupMutationAuthority,
  assertWritersStopped: () => Promise<void>): Promise<void> {
  const current = await store.read();
  if (!current?.state.runInventory) throw new Error('Terminal data capture requires exact durable inventory.');
  const inventory = current.state.runInventory;
  await authority.assertCurrent();
  if (!(inventory.cleanup.bounded_drain.state === 'complete' || hasVerifiedUnsettledDrain(current.state)) ||
      inventory.cleanup.stop_maintenance.state !== 'complete') throw new Error('Freeze terminal Delta only after writer and maintenance stops.');
  await assertWritersStopped();
  const before = await readBlob(azure, cellBlob(inventory, 'delta-before'), authority.assertCurrent);
  if (!before) throw new Error('The exact original pre-load Delta reference is absent.');
  const initial = baselineDelta(before.value, inventory, azure), blob = cellBlob(inventory, 'delta-terminal');
  const existing = await readBlob(azure, blob, authority.assertCurrent);
  if (existing) { baselineDelta(existing.value, inventory, azure, 'terminal_after_writers_stopped'); return; }
  const delta = await readDeltaReference(azure, initial.table_root, authority.assertCurrent);
  if (initial.observation === 'table' && (delta.observation !== 'table' || delta.table_id !== initial.table_id ||
      delta.version < initial.version) || delta.observation === 'table' && delta.row_count === null)
    throw new Error('Terminal Delta identity/version/row coverage is UNKNOWN or regressed.');
  const qap = inventory.qap;
  const observations: unknown[] = [];
  if (qap) for (const binding of qap.bindings) {
    try {
      const rows = await count(azure, qap.database, binding.external_table, 'external', authority.assertCurrent);
      await authority.assertCurrent();
      const listing = managementRows(await azure.kql('.show external tables', identifier(qap.database), true));
      const matching = listing.filter(row => row.TableName === binding.external_table);
      const reference = qap.reference;
      if (!reference || matching.length !== 1) throw new Error('Run QAP current original identity is UNKNOWN.');
      const definition = parseQualifiedDeltaDefinition(matching[0], binding.external_table);
      if (definition.docString !== `arrowcol-oat:${reference.run_id}:${reference.nonce}:${reference.manifest_sha256}`)
        throw new Error('Run QAP owner differs.');
      assertExactConnectionStrings(definition.connectionStrings,
        `abfss://${azure.c.data}@${azure.c.account}.dfs.core.windows.net/${binding.relative_path};managed_identity=${azure.c.identity_id}`);
      const measured = await qapState(azure, qap.database, binding.external_table, authority.assertCurrent);
      observations.push({ database: qap.database, table: binding.external_table, relative_path: binding.relative_path,
        observed_phase: 'pre_remove_qap', observed_at_unix_ms: Date.now(), query_sha256: measured?.query_sha256 ?? rows.query_sha256,
        rows, results: measured, reason: measured === null ? 'qap_not_enabled' : null });
    } catch (error) {
      if (error instanceof CampaignFencedError || error instanceof CampaignConflictError || error instanceof CampaignWriteOutcomeUnknownError) throw error;
      console.error(JSON.stringify({ event: 'custody_pre_remove_qap_unknown', cell_run_id: inventory.cell_run_id }));
      observations.push({ database: qap.database, table: binding.external_table, relative_path: binding.relative_path,
        observed_phase: 'pre_remove_qap', observed_at_unix_ms: Date.now(), rows: null, reason: 'qap_observation_unavailable' });
    }
    await assertWritersStopped();
  }
  await immutableBlob(azure, blob, { schema_version: 1, origin: origin(inventory), hot_delta_relative_path: initial.table_root,
    observed_phase: 'terminal_after_writers_stopped',
    baseline_etag: before.etag, delta, run_qap: observations }, authority.assertCurrent);
}

export async function observeDataPreservation(store: CampaignStore, azure: Azure, state: CampaignState,
  authority: CleanupMutationAuthority): Promise<DataPreservationReceipt> {
  const inventory = state.runInventory;
  if (!inventory || inventory.resources.some(resource => !['removed', 'absent'].includes(resource.state)) ||
      inventory.qap && !['removed', 'absent'].includes(inventory.qap.state) || inventory.fic && !['removed', 'absent'].includes(inventory.fic.state))
    throw new Error('Final independent preservation observation requires positively completed exact removals.');
  await authority.assertCurrent();
  const before = await readBlob(azure, cellBlob(inventory, 'delta-before'), authority.assertCurrent);
  const terminal = await readBlob(azure, cellBlob(inventory, 'delta-terminal'), authority.assertCurrent);
  const campaign = await readBlob(azure, campaignBlob(state), authority.assertCurrent);
  if (!before || !terminal || !campaign || terminal.value.baseline_etag !== before.etag)
    throw new Error('Original pre-load/terminal/stamp preservation references are absent or unversioned.');
  validateCampaignBaseline(campaign.value, state);
  const delta = await verifyDeltaPreservation(azure, baselineDelta(before.value, inventory, azure),
    baselineDelta(terminal.value, inventory, azure, 'terminal_after_writers_stopped'), authority.assertCurrent);
  const kusto = object(campaign.value.non_run_kusto);
  if (kusto.schema_version !== 1 || kusto.scope !== 'configured_stamp_databases_non_current_run_tables' ||
      !Array.isArray(kusto.tables) || kusto.tables.length > 512 ||
      canonical_json(kusto.databases) !== canonical_json([...new Set([azure.c.database, azure.c.experimentDatabase])]) ||
      kusto.source !== '.show tables and .show external tables with exact current run ownership exclusion' ||
      integer(kusto.observed_at_unix_ms) < state.startedAtMs || integer(kusto.observed_at_unix_ms) > Number(campaign.value.captured_at_unix_ms) ||
      (kusto.tables.length === 0 && kusto.empty_reason !== 'positive_inventory_contains_no_non_run_tables'))
    throw new Error('Non-run stamp Kusto baseline is incomplete; absence cannot become an inferred empty set.');
  const nonRun: DataPreservationReceipt['non_run_kusto'] = [];
  let status = delta.status, reason = delta.reason;
  for (const raw of kusto.tables) {
    const target = object(raw), database = identifier(target.database), name = identifier(target.name), kind = target.kind;
    if (!['regular', 'external'].includes(String(kind))) throw new Error('Kusto preservation table kind differs.');
    const typedKind = kind === 'regular' ? 'regular' : 'external';
    const original = object(target.before);
    if (original.database !== database || original.table !== name || original.kind !== kind ||
        !/^[a-f0-9]{64}$/.test(String(target.schema_sha256)) || !/^[a-f0-9]{64}$/.test(String(original.query_sha256)) ||
        !/^[a-f0-9]{64}$/.test(String(original.result_sha256))) throw new Error('Original Kusto count identity/provenance is incomplete.');
    if (await schema(azure, database, name, typedKind, authority.assertCurrent) !== target.schema_sha256)
      throw new Error('Original preserved Kusto schema identity changed.');
    if (typedKind === 'external') {
      await authority.assertCurrent();
      const listing = managementRows(await azure.kql('.show external tables', database, true));
      const matching = listing.filter(row => row.TableName === name);
      if (matching.length !== 1 || digest(parseQualifiedDeltaDefinition(matching[0], name).definition) !== target.definition_sha256)
        throw new Error('Original preserved Kusto external definition changed.');
    }
    const after = await count(azure, database, name, typedKind, authority.assertCurrent), rows = integer(original.rows);
    if (after.rows < rows) { status = 'failed'; reason = 'non_run_kusto_rows_decreased'; }
    let qapAfter: QapStateReceipt | null = null;
    if (typedKind === 'external') {
      const first = await qapState(azure, database, name, authority.assertCurrent);
      qapAfter = await qapState(azure, database, name, authority.assertCurrent);
      if ((target.qap_before === null) !== (qapAfter === null)) throw new Error('Preserved QAP enabled policy changed.');
      if (first && qapAfter && (!qapAfter.is_healthy || qapAfter.not_healthy_reason_present || qapAfter.pending_files > first.pending_files)) {
        status = 'failed'; reason = 'non_run_qap_unhealthy_or_backlog_growing';
      }
    }
    nonRun.push({ identity: { database, name, kind: typedKind, schema_sha256: String(target.schema_sha256),
      definition_sha256: target.definition_sha256 === null ? null : String(target.definition_sha256),
      before: { database, table: name, kind: typedKind, rows, query_sha256: String(original.query_sha256),
        result_sha256: String(original.result_sha256), observed_at_unix_ms: integer(original.observed_at_unix_ms) },
      qap_before: target.qap_before === null ? null : validateQapState(target.qap_before) }, after, qap_after: qapAfter });
  }
  await authority.assertCurrent();
  const result = { schema_version: 1 as const, scope: 'delta_and_non_run_kusto_preservation' as const, campaign_id: state.campaignId,
    cell_run_id: inventory.cell_run_id, status, reason, observed_at_unix_ms: Date.now(),
    delta, non_run_kusto: nonRun, run_owned_qap_after: { rows: null, reason: 'run_owned_qap_removed_by_design' } };
  const etag = await immutableBlob(azure, cellBlob(inventory, 'data-final/' + digest(result)), result, authority.assertCurrent);
  return { ...result, run_owned_qap_after: { rows: null, reason: 'run_owned_qap_removed_by_design' }, evidence_etag: etag };
}
