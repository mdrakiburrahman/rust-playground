import fs from 'node:fs';
import { createHash, randomUUID } from 'node:crypto';
import { Azure, Config, Failure, WorkloadIdentityPropagationPending, exchange_workload_identity_assertion,
  runtime, verify_aks_native_credential, canonical_json, owned, publish_private, reset_lock, HttpFailure,
  type HttpReceipt } from './bootstrap.js';

const ARM_API = '2023-01-31';
const AKS_API = '2024-10-01';
const AUDIENCE = 'api://AzureADTokenExchange';
const RUN_LABEL = 'arrowcol-oat/run-id';
const OWNER_LABEL = 'arrowcol-oat/owner-token';
const UUID = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/;
const DNS = /^[a-z0-9](?:[a-z0-9-]*[a-z0-9])?$/;

export interface RunFicRecord {
  version: 1;
  runId: string;
  ownershipNonce: string;
  namespace: string;
  namespaceUid: string;
  namespaceOwner: string;
  serviceAccount: string;
  serviceAccountUid: string;
  identityId: string;
  clientId: string;
  tenantId: string;
  aksId: string;
  server: string;
  credentialName: string;
  issuer: string;
  subject: string;
  audience: string;
  phase: 'create-submitted' | 'ready' | 'delete-submitted' | 'deleted';
}

function object(value: unknown, label: string): Record<string, unknown> {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Failure(`${label} must be an object.`);
  return value as Record<string, unknown>;
}
function string(value: Record<string, unknown>, key: string): string {
  if (typeof value[key] !== 'string' || !value[key]) throw new Failure(`Run FIC ${key} is missing or invalid.`);
  return value[key];
}
function uuid(value: string): string {
  if (!UUID.test(value) || value === '00000000-0000-0000-0000-000000000000')
    throw new Failure('Run FIC identifiers must be nonzero canonical lowercase UUIDs.');
  return value;
}
export function validate_run_identity(runId: string, namespace: string, serviceAccount: string): void {
  uuid(runId);
  const suffix = '-' + runId.replaceAll('-', '');
  if (namespace.length > 63 || !DNS.test(namespace) || !namespace.endsWith(suffix) ||
      namespace.length === suffix.length || !DNS.test(namespace.slice(0, -suffix.length)))
    throw new Failure('Run FIC namespace must be a DNS-1123 name with the exact canonical run UUID suffix.');
  if (serviceAccount.length > 63 || !DNS.test(serviceAccount))
    throw new Failure('Run FIC ServiceAccount must be an explicit DNS-1123 name.');
}
function https_url(value: string, label: string): URL {
  let url: URL;
  try { url = new URL(value); } catch { throw new Failure(`${label} must be an HTTPS URL.`); }
  if (url.protocol !== 'https:' || !url.hostname || url.username || url.password || url.search || url.hash)
    throw new Failure(`${label} must be an HTTPS URL without credentials, query, or fragment.`);
  return url;
}
export function parse_run_fic_record(value: unknown, c: Config): RunFicRecord {
  if (c.mode !== 'stamped') throw new Failure('Run FIC helpers require stamped infrastructure.');
  const raw = object(value, 'Run FIC record');
  const keys = ['version', 'runId', 'ownershipNonce', 'namespace', 'namespaceUid', 'namespaceOwner', 'serviceAccount',
    'serviceAccountUid', 'identityId', 'clientId', 'tenantId', 'aksId', 'server', 'credentialName',
    'issuer', 'subject', 'audience', 'phase'];
  if (raw.version !== 1 || Object.keys(raw).sort().join(',') !== keys.sort().join(','))
    throw new Failure('Run FIC record has an unsupported version or fields.');
  const runId = uuid(string(raw, 'runId')), namespace = string(raw, 'namespace'), serviceAccount = string(raw, 'serviceAccount');
  validate_run_identity(runId, namespace, serviceAccount);
  const namespaceOwner = string(raw, 'namespaceOwner');
  if (!/^[A-Za-z0-9_-]{16,128}$/.test(namespaceOwner)) throw new Failure('Run FIC namespace ownership token is invalid.');
  const identityId = string(raw, 'identityId'), aksId = string(raw, 'aksId');
  if (identityId.toLowerCase() !== c.identity_id.toLowerCase() ||
      aksId.toLowerCase() !== (c.base + '/providers/Microsoft.ContainerService/managedClusters/' + c.cluster).toLowerCase())
    throw new Failure('Run FIC record targets a different stamp or UAMI.');
  const credentialName = string(raw, 'credentialName'), subject = string(raw, 'subject');
  if (credentialName !== 'arrowcol-run-' + runId || subject !== `system:serviceaccount:${namespace}:${serviceAccount}` ||
      raw.audience !== AUDIENCE)
    throw new Failure('Run FIC credential name or subject is not owned by the exact run.');
  const phase = raw.phase;
  if (phase !== 'create-submitted' && phase !== 'ready' && phase !== 'delete-submitted' && phase !== 'deleted')
    throw new Failure('Run FIC record phase is invalid.');
  const server = string(raw, 'server'), issuer = string(raw, 'issuer');
  const parsedServer = https_url(server, 'AKS server');
  if (parsedServer.pathname !== '/' || (parsedServer.port && parsedServer.port !== '443'))
    throw new Failure('AKS server must be its exact HTTPS origin.');
  https_url(issuer, 'AKS issuer');
  return { version: 1, runId, ownershipNonce: uuid(string(raw, 'ownershipNonce')),
    namespace, namespaceUid: uuid(string(raw, 'namespaceUid')), namespaceOwner,
    serviceAccount, serviceAccountUid: uuid(string(raw, 'serviceAccountUid')), identityId,
    clientId: uuid(string(raw, 'clientId')), tenantId: uuid(string(raw, 'tenantId')),
    aksId, server, credentialName, issuer, subject, audience: AUDIENCE, phase };
}
export function read_run_fic_record(filename: string, c: Config): RunFicRecord {
  const stat = fs.lstatSync(filename);
  if (!stat.isFile() || stat.isSymbolicLink() || stat.size > 16_384 || (stat.mode & 0o077) !== 0)
    throw new Failure('Run FIC custody must be a bounded private regular file, not a symlink.');
  let value: unknown;
  try { value = JSON.parse(fs.readFileSync(filename, 'utf8')); }
  catch { throw new Failure('Run FIC custody record is not readable JSON.'); }
  return parse_run_fic_record(value, c);
}
function credential_matches(value: unknown, record: RunFicRecord): boolean {
  const resource = object(value, 'Federated credential'), properties = object(resource.properties, 'Federated credential properties');
  const id = record.identityId + '/federatedIdentityCredentials/' + record.credentialName;
  return typeof resource.id === 'string' && resource.id.toLowerCase() === id.toLowerCase() &&
    resource.name === record.credentialName && properties.issuer === record.issuer &&
    properties.subject === record.subject && Array.isArray(properties.audiences) &&
    properties.audiences.length === 1 && properties.audiences[0] === AUDIENCE;
}
async function kubectl(record: Pick<RunFicRecord, 'server'>, args: string[], timeout = 15): Promise<string> {
  const result = await runtime.run_tree(['kubectl', '--server', record.server, '--insecure-skip-tls-verify=false',
    '--request-timeout', `${timeout}s`, ...args], timeout);
  if (result.returncode !== 0) throw new Failure('Run FIC Kubernetes request failed; credentials and command output withheld.');
  return result.stdout.trim();
}
async function kubernetes_object(record: Pick<RunFicRecord, 'server'>, args: string[]): Promise<Record<string, unknown>> {
  const output = await kubectl(record, [...args, '--output', 'json']);
  try { return object(JSON.parse(output), 'Kubernetes response'); }
  catch { throw new Failure('Run FIC Kubernetes response was not a valid object.'); }
}
async function verify_run_resources(azure: Azure, record: RunFicRecord): Promise<void> {
  const cluster = await azure.arm('GET', record.aksId, undefined, false, AKS_API);
  const identity = await azure.arm('GET', record.identityId, undefined, false, ARM_API);
  await verify_aks_native_credential(azure, cluster, identity);
  if (cluster?.properties?.oidcIssuerProfile?.issuerURL !== record.issuer ||
      `https://${cluster?.properties?.fqdn}` !== record.server ||
      identity?.properties?.clientId !== record.clientId || identity?.properties?.tenantId !== record.tenantId)
    throw new Failure('Run FIC cluster or UAMI identity changed; refusing.');
  const credential = await azure.arm('GET', record.identityId + '/federatedIdentityCredentials/' + record.credentialName,
    undefined, false, ARM_API);
  if (!credential_matches(credential, record)) throw new Failure('The live run FIC differs from its owned record.');
}
export async function verify_run_fic(azure: Azure, record: RunFicRecord): Promise<void> {
  await verify_run_resources(azure, record);
  const namespace = await kubernetes_object(record, ['get', 'namespace', record.namespace]);
  const metadata = object(namespace.metadata, 'Namespace metadata');
  const labels = object(metadata.labels, 'Namespace labels'), annotations = object(metadata.annotations, 'Namespace annotations');
  if (metadata.name !== record.namespace || metadata.uid !== record.namespaceUid || metadata.deletionTimestamp ||
      labels[RUN_LABEL] !== record.runId || annotations[RUN_LABEL] !== record.runId ||
      labels[OWNER_LABEL] !== record.namespaceOwner || annotations[OWNER_LABEL] !== record.namespaceOwner)
    throw new Failure('Run FIC namespace custody changed or deletion has begun.');
  const account = await kubernetes_object(record, ['get', 'serviceaccount', record.serviceAccount, '--namespace', record.namespace]);
  const sa = object(account.metadata, 'ServiceAccount metadata');
  const saAnnotations = object(sa.annotations, 'ServiceAccount annotations');
  if (sa.name !== record.serviceAccount || sa.namespace !== record.namespace || sa.uid !== record.serviceAccountUid ||
      sa.deletionTimestamp || saAnnotations['azure.workload.identity/client-id'] !== record.clientId)
    throw new Failure('Run FIC ServiceAccount identity or UAMI annotation changed.');
}
function validate_assertion(assertion: string, record: RunFicRecord): void {
  if (!assertion || assertion.length > 16_384 || assertion.split('.').length !== 3)
    throw new Failure('Run FIC TokenRequest returned an invalid JWT.');
  let claims: Record<string, unknown>;
  try { claims = object(JSON.parse(Buffer.from(assertion.split('.')[1]!, 'base64url').toString('utf8')), 'Token claims'); }
  catch { throw new Failure('Run FIC TokenRequest claims could not be decoded.'); }
  const audiences = Array.isArray(claims.aud) ? claims.aud : [claims.aud];
  const kubernetes = object(claims['kubernetes.io'], 'Token Kubernetes claims');
  const account = object(kubernetes.serviceaccount, 'Token ServiceAccount claims');
  if (claims.iss !== record.issuer || claims.sub !== record.subject || audiences.length !== 1 || audiences[0] !== AUDIENCE ||
      kubernetes.namespace !== record.namespace || account.name !== record.serviceAccount || account.uid !== record.serviceAccountUid ||
      typeof claims.exp !== 'number' || claims.exp <= Date.now() / 1000)
    throw new Failure('Run FIC TokenRequest claims do not match the recorded run identity.');
}
export async function probe_fic_exchange(record: RunFicRecord, waitSeconds: number): Promise<void> {
  if (!Number.isInteger(waitSeconds) || waitSeconds < 1 || waitSeconds > 300)
    throw new Failure('Run FIC propagation wait must be in 1..300 seconds.');
  const deadline = runtime.now() + waitSeconds;
  while (runtime.now() < deadline) {
    const remaining = () => Math.max(0, deadline - runtime.now());
    const assertion = await kubectl(record, ['create', 'token', record.serviceAccount, '--namespace', record.namespace,
      '--audience', AUDIENCE, '--duration', '10m'], Math.min(15, remaining()));
    validate_assertion(assertion, record);
    if (remaining() <= 0) break;
    try {
      await exchange_workload_identity_assertion('https://storage.azure.com/', record.clientId, record.tenantId,
        assertion, Math.max(1, Math.floor(Math.min(15, remaining()) * 1000)));
      return;
    } catch (error) {
      if (!(error instanceof WorkloadIdentityPropagationPending)) throw error;
      if (remaining() > 0) await runtime.sleep(Math.min(5, remaining()));
    }
  }
  throw new Failure('Run FIC propagation was not confirmed within the bounded deadline; no workload is authorized.');
}
export async function probe_run_fic(azure: Azure, runId: string, namespace: string, serviceAccount: string,
  recordPath: string, waitSeconds: number): Promise<void> {
  validate_run_identity(runId, namespace, serviceAccount);
  const record = read_run_fic_record(recordPath, azure.c);
  if (record.runId !== runId || record.namespace !== namespace || record.serviceAccount !== serviceAccount || record.phase !== 'ready')
    throw new Failure('Run FIC probe requires this exact run, namespace, ServiceAccount and a confirmed ready record.');
  const snapshot = await read_ledger(azure);
  if (snapshot?.value.transaction?.phase !== 'complete')
    throw new Failure('Run FIC shared coordination is unresolved; probe cannot authorize workloads.');
  assert_owned_entry(snapshot.value.entries[runId], record);
  await verify_run_fic(azure, record);
  await probe_fic_exchange(record, waitSeconds);
  console.log('Exact run ServiceAccount token exchange succeeded; tokens withheld.');
}

interface OperationIntent { requestId: string; startedAt: string }
interface LeaseIntent extends OperationIntent {
  leaseId: string;
  action: 'acquire' | 'release';
  account: string;
  container: string;
  blob: string;
}
interface CoordinationTransaction {
  kind: 'mutation' | 'admission-refusal';
  leaseId: string;
  runId: string;
  ownershipNonce: string;
  operation: 'prepare' | 'cleanup';
  phase: 'acquiring' | 'active' | 'release-pending' | 'complete';
  acquireIntent: LeaseIntent;
  acquireReceipt?: HttpReceipt;
  releaseIntent?: LeaseIntent;
  releaseReceipt?: HttpReceipt;
  refusal?: {
    record: RunFicRecord;
    code: 'capacity' | 'collision' | 'foreign' | 'drift' | 'workload';
    beforeEntriesSha256: string;
    afterEntriesSha256: string;
  };
}
class AdmissionRejected extends Failure {
  constructor(readonly code: NonNullable<CoordinationTransaction['refusal']>['code'], message: string) { super(message); }
}
interface LedgerEntry {
  record: RunFicRecord;
  createIntent: OperationIntent;
  createReceipt?: HttpReceipt;
  createPostcondition?: HttpReceipt;
  createdResource?: Record<string, unknown>;
  createCompletion?: CoordinationTransaction;
  deleteIntent?: OperationIntent;
  deleteReceipt?: HttpReceipt;
  deletePostcondition?: HttpReceipt;
  deleteCompletion?: CoordinationTransaction;
}
interface Ledger {
  version: 1;
  identityId: string;
  entries: Record<string, LedgerEntry>;
  refusals: Record<string, CoordinationTransaction>;
  transaction: CoordinationTransaction | null;
}
interface LedgerSnapshot { value: Ledger; etag: string }
export function run_fic_ledger_path(c: Config): string {
  return 'arrowcol-oat/federation/' + createHash('sha256').update(c.identity_id.toLowerCase()).digest('hex') + '/ledger.json';
}
function intent(): OperationIntent { return { requestId: randomUUID(), startedAt: new Date().toISOString() }; }
function parse_receipt(value: unknown): HttpReceipt {
  const raw = object(value, 'ARM receipt'), headers = object(raw.headers, 'ARM receipt headers');
  if (typeof raw.status !== 'number' || !Number.isInteger(raw.status) || raw.status < 100 || raw.status > 599 ||
      typeof raw.bodyBase64 !== 'string' || Buffer.from(raw.bodyBase64, 'base64').toString('base64') !== raw.bodyBase64 ||
      Object.values(headers).some(item => typeof item !== 'string'))
    throw new Failure('ARM receipt is malformed; reconciliation required.');
  return { status: raw.status, bodyBase64: raw.bodyBase64, headers: headers as Record<string, string> };
}
function parse_intent(value: unknown): OperationIntent {
  const raw = object(value, 'ARM operation intent'), requestId = uuid(string(raw, 'requestId')), startedAt = string(raw, 'startedAt');
  if (!Number.isFinite(Date.parse(startedAt))) throw new Failure('ARM operation intent has no valid UTC timestamp.');
  return { requestId, startedAt };
}
function lease_intent(c: Config, leaseId: string, action: 'acquire' | 'release'): LeaseIntent {
  return { ...intent(), leaseId, action, account: c.account, container: c.checkpoints, blob: run_fic_ledger_path(c) };
}
function parse_lease_intent(value: unknown): LeaseIntent {
  const raw = object(value, 'Coordination lease intent');
  if ((raw.action !== 'acquire' && raw.action !== 'release') ||
      Object.keys(raw).sort().join(',') !== 'account,action,blob,container,leaseId,requestId,startedAt')
    throw new Failure('Coordination lease intent is missing its exact request scope.');
  return { ...parse_intent(raw), leaseId: uuid(string(raw, 'leaseId')), action: raw.action,
    account: string(raw, 'account'), container: string(raw, 'container'), blob: string(raw, 'blob') };
}
function entries_digest(entries: Ledger['entries']): string {
  return createHash('sha256').update(canonical_json(entries)).digest('hex');
}
function parse_transaction(value: unknown, c: Config): CoordinationTransaction {
  const raw = object(value, 'Run FIC transaction');
  const { phase, operation, kind } = raw;
  if ((kind !== 'mutation' && kind !== 'admission-refusal') ||
      (phase !== 'acquiring' && phase !== 'active' && phase !== 'release-pending' && phase !== 'complete') ||
      (operation !== 'prepare' && operation !== 'cleanup') ||
      Object.keys(raw).some(key => !['kind', 'leaseId', 'runId', 'ownershipNonce', 'operation', 'phase',
        'acquireIntent', 'acquireReceipt', 'releaseIntent', 'releaseReceipt', 'refusal'].includes(key)))
    throw new Failure('Run FIC transaction has invalid phase, operation or fields.');
  let refusal: CoordinationTransaction['refusal'];
  if (kind === 'admission-refusal') {
    const proof = object(raw.refusal, 'Admission refusal'), code = proof.code;
    if ((code !== 'capacity' && code !== 'collision' && code !== 'foreign' && code !== 'drift' && code !== 'workload') ||
        Object.keys(proof).sort().join(',') !== 'afterEntriesSha256,beforeEntriesSha256,code,record')
      throw new Failure('Admission refusal has no exact deterministic reason or unchanged-authority evidence.');
    const beforeEntriesSha256 = string(proof, 'beforeEntriesSha256'), afterEntriesSha256 = string(proof, 'afterEntriesSha256');
    if (!/^[0-9a-f]{64}$/.test(beforeEntriesSha256) || afterEntriesSha256 !== beforeEntriesSha256)
      throw new Failure('Admission refusal changed the resource authority instead of remaining pre-ARM.');
    refusal = { record: parse_run_fic_record(proof.record, c), code, beforeEntriesSha256, afterEntriesSha256 };
  } else if (raw.refusal !== undefined) throw new Failure('A mutation transaction cannot claim an admission-only refusal.');
  return { kind, leaseId: uuid(string(raw, 'leaseId')), runId: uuid(string(raw, 'runId')),
    ownershipNonce: uuid(string(raw, 'ownershipNonce')), operation, phase,
    acquireIntent: parse_lease_intent(raw.acquireIntent),
    ...(raw.acquireReceipt === undefined ? {} : { acquireReceipt: parse_receipt(raw.acquireReceipt) }),
    ...(raw.releaseIntent === undefined ? {} : { releaseIntent: parse_lease_intent(raw.releaseIntent) }),
    ...(raw.releaseReceipt === undefined ? {} : { releaseReceipt: parse_receipt(raw.releaseReceipt) }),
    ...(refusal === undefined ? {} : { refusal }) };
}
function require_receipt(receipt: HttpReceipt | undefined, status: number, label: string): asserts receipt is HttpReceipt {
  if (!receipt || receipt.status !== status ||
      !(receipt.headers['x-ms-request-id'] || receipt.headers['x-ms-correlation-request-id']))
    throw new Failure(`${label} lacks its original definitive HTTP status and correlation receipt.`);
}
function receipt_resource(receipt: HttpReceipt): Record<string, unknown> {
  try { return object(JSON.parse(Buffer.from(receipt.bodyBase64, 'base64').toString('utf8')), 'ARM response'); }
  catch { throw new Failure('Original ARM resource response is missing or malformed.'); }
}
function require_creation_proof(entry: LedgerEntry): void {
  require_receipt(entry.createReceipt, 201, 'FIC creation');
  require_receipt(entry.createPostcondition, 200, 'FIC creation postcondition');
  if (!entry.createdResource || !credential_matches(receipt_resource(entry.createReceipt), entry.record) ||
      !credential_matches(entry.createdResource, entry.record) ||
      canonical_json(receipt_resource(entry.createPostcondition)) !== canonical_json(entry.createdResource))
    throw new Failure('Original FIC creation intent, response and verified resource postcondition disagree.');
}
function validate_completion(ledger: Ledger, tx: CoordinationTransaction, operation: 'prepare' | 'cleanup', c: Config,
  current = false): void {
  const entry = ledger.entries[tx.runId];
  const owner = tx.kind === 'admission-refusal' ? tx.refusal?.record : entry?.record;
  if (tx.phase !== 'complete' || tx.operation !== operation || !owner ||
      tx.ownershipNonce !== owner.ownershipNonce || tx.runId !== owner.runId)
    throw new Failure('Run FIC completion does not identify its original operation and owner generation.');
  require_receipt(tx.acquireReceipt, 201, 'Coordination lease acquisition');
  require_receipt(tx.releaseReceipt, 200, 'Coordination lease release');
  if (!tx.releaseIntent || tx.acquireIntent.action !== 'acquire' || tx.releaseIntent.action !== 'release' ||
      [tx.acquireIntent, tx.releaseIntent].some(request => request.leaseId !== tx.leaseId ||
        request.account !== c.account || request.container !== c.checkpoints || request.blob !== run_fic_ledger_path(c)) ||
      tx.acquireReceipt.headers['x-ms-lease-id'] !== tx.leaseId ||
      tx.acquireReceipt.headers['x-ms-client-request-id'] !== tx.acquireIntent.requestId ||
      tx.releaseReceipt.headers['x-ms-client-request-id'] !== tx.releaseIntent.requestId ||
      tx.releaseIntent.requestId === tx.acquireIntent.requestId ||
      Date.parse(tx.releaseIntent.startedAt) < Date.parse(tx.acquireIntent.startedAt))
    throw new Failure('Run FIC completion lacks definitive release proof for the exact acquired lease.');
  if (tx.kind === 'admission-refusal') {
    if (!tx.refusal || owner.phase !== (tx.operation === 'prepare' ? 'create-submitted' : 'ready') ||
        tx.refusal.beforeEntriesSha256 !== tx.refusal.afterEntriesSha256 ||
        (current && entries_digest(ledger.entries) !== tx.refusal.beforeEntriesSha256))
      throw new Failure('Admission refusal is not an unchanged pre-ARM transaction.');
    return;
  }
  if (!entry || !entry.createIntent) throw new Failure('Mutation completion has no original resource intent.');
  require_creation_proof(entry);
  if (operation === 'cleanup') {
    if (!entry.deleteIntent || entry.record.phase !== 'deleted')
      throw new Failure('Run FIC cleanup completion has no original delete intent and tombstone.');
    require_receipt(entry.deleteReceipt, entry.deleteReceipt?.status === 200 ? 200 : 204, 'FIC deletion');
    require_receipt(entry.deletePostcondition, 404, 'FIC deletion postcondition');
  }
}
function parse_ledger(value: unknown, c: Config): Ledger {
  const raw = object(value, 'Run FIC ledger');
  if (raw.version !== 1 || raw.identityId !== c.identity_id ||
      Object.keys(raw).sort().join(',') !== 'entries,identityId,refusals,transaction,version')
    throw new Failure('Run FIC ledger belongs to a different authority or schema.');
  const entries = object(raw.entries, 'Run FIC ledger entries');
  if (Object.keys(entries).length > 4096) throw new Failure('Run FIC history limit reached; explicit archival review required.');
  const parsed: Record<string, LedgerEntry> = {};
  for (const [runId, value] of Object.entries(entries)) {
    const entry = object(value, 'Run FIC ledger entry'), record = parse_run_fic_record(entry.record, c);
    if (Object.keys(entry).some(key => !['record', 'createIntent', 'createReceipt', 'createPostcondition', 'createdResource',
      'createCompletion', 'deleteIntent', 'deleteReceipt', 'deletePostcondition', 'deleteCompletion'].includes(key)))
      throw new Failure('Run FIC ledger entry has unknown fields.');
    if (runId !== record.runId) throw new Failure('Run FIC ledger key disagrees with its record.');
    const createReceipt = entry.createReceipt === undefined ? undefined : parse_receipt(entry.createReceipt);
    const deleteReceipt = entry.deleteReceipt === undefined ? undefined : parse_receipt(entry.deleteReceipt);
    if ((record.phase !== 'create-submitted' && createReceipt?.status !== 201) ||
        (record.phase === 'deleted' && ![200, 204].includes(deleteReceipt?.status ?? 0)))
      throw new Failure('Run FIC ledger lacks the original definitive ARM receipt.');
    parsed[runId] = { record, createIntent: parse_intent(entry.createIntent),
      ...(createReceipt === undefined ? {} : { createReceipt }),
      ...(deleteReceipt === undefined ? {} : { deleteReceipt }),
      ...(entry.createPostcondition === undefined ? {} : { createPostcondition: parse_receipt(entry.createPostcondition) }),
      ...(entry.deletePostcondition === undefined ? {} : { deletePostcondition: parse_receipt(entry.deletePostcondition) }),
      ...(entry.createCompletion === undefined ? {} : { createCompletion: parse_transaction(entry.createCompletion, c) }),
      ...(entry.deleteCompletion === undefined ? {} : { deleteCompletion: parse_transaction(entry.deleteCompletion, c) }),
      ...(entry.createdResource === undefined ? {} : { createdResource: object(entry.createdResource, 'Created FIC resource') }),
      ...(entry.deleteIntent === undefined ? {} : { deleteIntent: parse_intent(entry.deleteIntent) }) };
  }
  const refusals: Ledger['refusals'] = {};
  const rawRefusals = object(raw.refusals, 'Admission refusal history');
  if (Object.keys(rawRefusals).length > 4096) throw new Failure('Admission refusal history requires explicit archival review.');
  for (const [leaseId, value] of Object.entries(rawRefusals)) {
    const refusal = parse_transaction(value, c);
    if (leaseId !== refusal.leaseId || refusal.kind !== 'admission-refusal')
      throw new Failure('Admission refusal history has a different lease or transaction kind.');
    refusals[leaseId] = refusal;
  }
  const transaction = raw.transaction === null ? null : parse_transaction(raw.transaction, c);
  const ledger: Ledger = { version: 1, identityId: c.identity_id, entries: parsed, refusals, transaction };
  for (const entry of Object.values(parsed)) {
    if ((entry.createCompletion && entry.createCompletion.kind !== 'mutation') ||
        (entry.deleteCompletion && entry.deleteCompletion.kind !== 'mutation'))
      throw new Failure('Resource ownership requires a mutation completion, never an admission refusal.');
    if (entry.createCompletion) validate_completion(ledger, entry.createCompletion, 'prepare', c);
    if (entry.deleteCompletion) validate_completion(ledger, entry.deleteCompletion, 'cleanup', c);
  }
  for (const refusal of Object.values(refusals)) validate_completion(ledger, refusal, refusal.operation, c);
  if (transaction?.phase === 'complete') {
    validate_completion(ledger, transaction, transaction.operation, c, true);
    const entry = parsed[transaction.runId];
    if (transaction.kind === 'mutation' && entry?.record.phase !== (transaction.operation === 'prepare' ? 'ready' : 'deleted'))
      throw new Failure('Run FIC completion disagrees with its terminal operation state.');
    const archived = transaction.kind === 'admission-refusal' ? refusals[transaction.leaseId]
      : transaction.operation === 'prepare' ? entry?.createCompletion : entry?.deleteCompletion;
    if (!archived || canonical_json(archived) !== canonical_json(transaction))
      throw new Failure('Run FIC completion is missing its immutable receipt-chain tombstone.');
  }
  return ledger;
}
async function read_ledger(azure: Azure, leaseId?: string): Promise<LedgerSnapshot | null> {
  const [data, headers] = await azure.blob('GET', azure.c.checkpoints, run_fic_ledger_path(azure.c), undefined,
    undefined, leaseId ? { 'x-ms-lease-id': leaseId } : {}, true);
  if (data === null) return null;
  if (!headers.etag || data.length > 8 * 1024 * 1024) throw new Failure('Run FIC ledger has no ETag or exceeds the custody bound.');
  return { value: parse_ledger(JSON.parse(data.toString('utf8')), azure.c), etag: headers.etag };
}
async function write_ledger(azure: Azure, value: Ledger, etag: string | null, leaseId?: string,
  capture?: (receipt: HttpReceipt) => void): Promise<LedgerSnapshot> {
  const bytes = canonical_json(value);
  const [, headers] = await azure.blob('PUT', azure.c.checkpoints, run_fic_ledger_path(azure.c), undefined, bytes,
    { 'x-ms-blob-type': 'BlockBlob', 'Content-Type': 'application/json',
      ...(etag ? { 'If-Match': etag } : { 'If-None-Match': '*' }),
      ...(leaseId ? { 'x-ms-lease-id': leaseId } : {}) }, false, false, capture);
  if (headers[':status'] !== '201' || !headers.etag)
    throw new Failure('Run FIC ledger write receipt is incomplete; never replay.');
  const confirmed = await read_ledger(azure, leaseId);
  if (!confirmed || confirmed.etag !== headers.etag || canonical_json(confirmed.value) !== bytes)
    throw new Failure('Run FIC ledger postcondition changed or is unknown; never replay.');
  return confirmed;
}
async function require_owned_storage(azure: Azure): Promise<void> {
  owned(await azure.arm('GET', azure.c.storage_id, undefined, false, '2023-05-01'), azure.c);
  const container = await azure.arm('GET', azure.c.storage_id + '/blobServices/default/containers/' + azure.c.checkpoints,
    undefined, false, '2023-05-01');
  if (container?.properties?.metadata?.sandboxid !== azure.c.sandbox)
    throw new Failure('Run FIC coordinator requires the existing owned checkpoint container.');
}
async function admit_run_fic(azure: Azure, record: RunFicRecord, ledger: Ledger | null): Promise<void> {
  const credentials = await azure.collection(record.identityId + '/federatedIdentityCredentials', ARM_API);
  if (credentials.some(item => typeof item?.name !== 'string' || !/^[A-Za-z0-9][A-Za-z0-9_-]{2,119}$/.test(item.name)) ||
      new Set(credentials.map(item => item.name)).size !== credentials.length)
    throw new Failure('Live UAMI credential inventory is malformed or contains duplicate names.');
  if (!credentials.some(item => item.name === `azure-qap-${azure.c.cluster}`))
    throw new Failure('Live UAMI credential inventory lost the required native trust; capacity cannot be established.');
  if (credentials.some(item => item.name === record.credentialName) || ledger?.entries[record.runId])
    throw new AdmissionRejected('collision', 'Run credential name or tombstone already exists; no adoption.');
  const names = new Set<string>(credentials.map(item => item.name));
  for (const credential of credentials.filter(item => item.name.startsWith('arrowcol-run-'))) {
    const runId = credential.name.slice('arrowcol-run-'.length), entry = ledger?.entries[runId];
    if (!entry || entry.record.phase !== 'ready')
      throw new AdmissionRejected('foreign', 'A foreign or unresolved live run credential blocks admission.');
  }
  for (const entry of Object.values(ledger?.entries ?? {})) {
    if (entry.record.phase === 'deleted') continue;
    if (entry.record.phase !== 'ready')
      throw new Failure('An unresolved run resource intent blocks admission.');
    assert_owned_entry(entry, entry.record);
    if (!names.has(entry.record.credentialName))
      throw new AdmissionRejected('drift', 'A proven ready run credential disappeared; admission is blocked.');
    const current = await azure.arm('GET', entry.record.identityId + '/federatedIdentityCredentials/' + entry.record.credentialName,
      undefined, true, ARM_API);
    if (!current || !credential_matches(current, entry.record) || canonical_json(current) !== canonical_json(entry.createdResource))
      throw new AdmissionRejected('drift', 'A proven ready run credential changed; admission is blocked.');
    names.add(entry.record.credentialName);
  }
  if (names.size >= 20) throw new AdmissionRejected('capacity', 'Shared UAMI federated credential capacity is exhausted.');
}
async function with_coordinator(azure: Azure, record: RunFicRecord, recordPath: string, operation: 'prepare' | 'cleanup',
  work: (value: Ledger, save: () => Promise<void>) => Promise<void>,
  admit?: (value: Ledger | null) => Promise<void>): Promise<void> {
  await require_owned_storage(azure);
  const localIntent = recordPath + '.' + operation + '-mutex.json';
  if (fs.existsSync(localIntent)) throw new Failure('Prior run FIC mutex attempt exists; no automatic replay or takeover.');
  let snapshot = await read_ledger(azure);
  if (snapshot && snapshot.value.transaction?.phase !== 'complete')
    throw new Failure('A prior run FIC transaction is unresolved; explicit reconciliation required.');
  await admit?.(snapshot?.value ?? null);
  const leaseId = randomUUID();
  const transaction: CoordinationTransaction = { kind: 'mutation', leaseId, runId: record.runId, ownershipNonce: record.ownershipNonce,
    operation, phase: 'acquiring', acquireIntent: lease_intent(azure.c, leaseId, 'acquire') };
  publish_private(localIntent, { identityId: record.identityId, blob: run_fic_ledger_path(azure.c), transaction });
  let initialized = false;
  if (!snapshot) {
    try {
      snapshot = await write_ledger(azure, { version: 1, identityId: azure.c.identity_id, entries: {}, refusals: {}, transaction }, null);
      initialized = true;
    }
    catch (error) {
      if (!(error instanceof HttpFailure) || error.code !== 412) throw error;
      snapshot = await read_ledger(azure);
      if (!snapshot || snapshot.value.transaction?.phase !== 'complete')
        throw new Failure('A concurrent coordinator initialization is unresolved; no acquisition.');
      await admit?.(snapshot.value);
    }
  }
  if (!snapshot || (!initialized && snapshot.value.transaction?.phase !== 'complete'))
    throw new Failure('A prior run FIC transaction is unresolved; explicit reconciliation required.');
  const eligible = canonical_json(snapshot.value);
  const acquisition: { receipt?: HttpReceipt } = {};
  const [, acquired] = await azure.blob('PUT', azure.c.checkpoints, run_fic_ledger_path(azure.c), { comp: 'lease' },
    Buffer.alloc(0), { 'x-ms-lease-action': 'acquire', 'x-ms-lease-duration': '-1',
      'x-ms-proposed-lease-id': leaseId, 'If-Match': snapshot.etag,
      'x-ms-client-request-id': transaction.acquireIntent.requestId }, false, false, response => {
      publish_private(localIntent + '.acquired.json', { leaseId, intent: transaction.acquireIntent, response });
      acquisition.receipt = response;
    });
  require_receipt(acquisition.receipt, 201, 'Coordination lease acquisition');
  if (acquired[':status'] !== '201' || acquired['x-ms-lease-id'] !== leaseId ||
      acquisition.receipt.headers['x-ms-lease-id'] !== leaseId ||
      acquisition.receipt.headers['x-ms-client-request-id'] !== transaction.acquireIntent.requestId)
    throw new Failure('Infinite run FIC mutex acquisition is ambiguous; no replay, release or lease break.');
  const held = await read_ledger(azure, leaseId);
  if (!held || held.etag !== snapshot.etag || canonical_json(held.value) !== eligible)
    throw new Failure('Run FIC eligible state changed during conditional acquisition; retain custody and reconcile.');
  snapshot = held;
  const value = snapshot.value;
  transaction.acquireReceipt = acquisition.receipt;
  const save = async () => { snapshot = await write_ledger(azure, value, snapshot!.etag, leaseId); };
  const finish = async () => {
    transaction.phase = 'release-pending';
    const releaseIntent = lease_intent(azure.c, leaseId, 'release');
    transaction.releaseIntent = releaseIntent;
    await save();
    const release: { receipt?: HttpReceipt } = {};
    const [, released] = await azure.blob('PUT', azure.c.checkpoints, run_fic_ledger_path(azure.c), { comp: 'lease' },
      Buffer.alloc(0), { 'x-ms-lease-action': 'release', 'x-ms-lease-id': leaseId,
        'x-ms-client-request-id': releaseIntent.requestId }, false, false, response => {
        publish_private(localIntent + '.released.json', { leaseId, intent: releaseIntent, response });
        release.receipt = response;
      });
    require_receipt(release.receipt, 200, 'Coordination lease release');
    if (released[':status'] !== '200' || release.receipt.headers['x-ms-client-request-id'] !== releaseIntent.requestId)
      throw new Failure('Run FIC mutex release receipt is unknown; no replay.');
    transaction.releaseReceipt = release.receipt;
    transaction.phase = 'complete';
    validate_completion(value, transaction, operation, azure.c, true);
    if (transaction.kind === 'admission-refusal') value.refusals[leaseId] = { ...transaction };
    else {
      const entry = value.entries[record.runId]!;
      if (operation === 'prepare') entry.createCompletion = { ...transaction };
      else entry.deleteCompletion = { ...transaction };
    }
    // Only this final metadata acknowledgement may be reconciled from the full committed proof chain.
    try {
      await write_ledger(azure, value, snapshot!.etag, undefined, response =>
        publish_private(localIntent + '.completion-http.json', { leaseId, response }));
    } catch (error) {
      publish_private(localIntent + '.completion-unknown.json', {
        identityId: record.identityId, runId: record.runId, ownershipNonce: record.ownershipNonce,
        leaseId, operation, releasePendingEtag: snapshot!.etag, observedAt: new Date().toISOString(),
        outcome: 'final-coordination-commit-acknowledgement-unknown',
      });
      throw error;
    }
  };
  // Admission has no resource-write callback; only this pre-ARM branch may finish a definite refusal.
  const beforeAdmission = entries_digest(value.entries);
  try { await admit?.(value); }
  catch (error) {
    if (!(error instanceof AdmissionRejected) || !admit) throw error;
    const afterAdmission = entries_digest(value.entries);
    if (afterAdmission !== beforeAdmission)
      throw new Failure('Admission changed resource authority; retain custody without a terminal refusal.');
    transaction.kind = 'admission-refusal';
    transaction.refusal = { record, code: error.code,
      beforeEntriesSha256: beforeAdmission, afterEntriesSha256: afterAdmission };
    transaction.phase = 'active';
    value.transaction = transaction;
    await save();
    await finish();
    throw error;
  }
  transaction.phase = 'active';
  value.transaction = transaction;
  await save();
  // Unknown read/write outcomes retain the infinite lease; this is deliberately not a finally release.
  await work(value, save);
  await finish();
}
function assert_owned_entry(entry: LedgerEntry | undefined, record: RunFicRecord): asserts entry is LedgerEntry {
  if (!entry || canonical_json(entry.record) !== canonical_json(record) || !entry.createCompletion)
    throw new Failure('No original definitive creation/ownership record exists for this run FIC.');
  require_creation_proof(entry);
}
async function arm_mutation(azure: Azure, record: RunFicRecord, operation: 'create' | 'delete',
  operationIntent: OperationIntent, recordPath: string): Promise<HttpReceipt> {
  const captured: { receipt?: HttpReceipt } = {};
  await azure.request(operation === 'create' ? 'PUT' : 'DELETE',
    'https://management.azure.com' + record.identityId + '/federatedIdentityCredentials/' + record.credentialName + '?api-version=' + ARM_API,
    'https://management.azure.com/', operation === 'create' ? JSON.stringify({
      properties: { issuer: record.issuer, subject: record.subject, audiences: [AUDIENCE] },
    }) : undefined, { 'Content-Type': 'application/json', 'x-ms-client-request-id': operationIntent.requestId },
    false, false, response => {
      publish_private(recordPath + '.' + operation + '-receipt.json', { intent: operationIntent, response });
      captured.receipt = response;
    });
  if (!captured.receipt) throw new Failure('ARM mutation has no durable HTTP receipt; stop and reconcile without replay.');
  return captured.receipt;
}
async function arm_postcondition(azure: Azure, record: RunFicRecord, operation: 'create' | 'delete',
  recordPath: string): Promise<HttpReceipt> {
  const captured: { receipt?: HttpReceipt } = {};
  await azure.request('GET', 'https://management.azure.com' + record.identityId +
    '/federatedIdentityCredentials/' + record.credentialName + '?api-version=' + ARM_API,
    'https://management.azure.com/', undefined, {}, operation === 'delete', false, response => {
      publish_private(recordPath + '.' + operation + '-postcondition.json', {
        runId: record.runId, ownershipNonce: record.ownershipNonce, identityId: record.identityId,
        credentialName: record.credentialName, response,
      });
      captured.receipt = response;
    });
  require_receipt(captured.receipt, operation === 'create' ? 200 : 404, 'FIC verified postcondition');
  return captured.receipt;
}
async function new_record(azure: Azure, runId: string, namespace: string, serviceAccount: string): Promise<RunFicRecord> {
  validate_run_identity(runId, namespace, serviceAccount);
  if (azure.c.mode !== 'stamped') throw new Failure('Run FIC preparation requires stamped infrastructure.');
  const aksId = azure.c.base + '/providers/Microsoft.ContainerService/managedClusters/' + azure.c.cluster;
  const cluster = await azure.arm('GET', aksId, undefined, false, AKS_API);
  const identity = await azure.arm('GET', azure.c.identity_id, undefined, false, ARM_API);
  await verify_aks_native_credential(azure, cluster, identity);
  const fqdn = cluster?.properties?.fqdn;
  if (typeof fqdn !== 'string' || !/^[a-zA-Z0-9.-]+$/.test(fqdn)) throw new Failure('AKS public FQDN is unavailable.');
  const server = `https://${fqdn}`;
  const ns = object((await kubernetes_object({ server }, ['get', 'namespace', namespace])).metadata, 'Namespace metadata');
  const labels = object(ns.labels, 'Namespace labels'), annotations = object(ns.annotations, 'Namespace annotations');
  const owner = string(labels, OWNER_LABEL);
  if (ns.name !== namespace || ns.deletionTimestamp || labels[RUN_LABEL] !== runId ||
      annotations[RUN_LABEL] !== runId || annotations[OWNER_LABEL] !== owner)
    throw new Failure('Run FIC preparation requires this run-owned namespace before workloads start.');
  const sa = object((await kubernetes_object({ server }, ['get', 'serviceaccount', serviceAccount,
    '--namespace', namespace])).metadata, 'ServiceAccount metadata');
  if (sa.name !== serviceAccount || sa.namespace !== namespace || sa.deletionTimestamp ||
      object(sa.annotations, 'ServiceAccount annotations')['azure.workload.identity/client-id'] !== identity.properties.clientId)
    throw new Failure('Run ServiceAccount does not reference the exact shared UAMI.');
  return parse_run_fic_record({ version: 1, runId, ownershipNonce: randomUUID(), namespace, namespaceUid: ns.uid,
    namespaceOwner: owner, serviceAccount, serviceAccountUid: sa.uid, identityId: azure.c.identity_id,
    clientId: identity.properties.clientId, tenantId: identity.properties.tenantId, aksId, server,
    credentialName: 'arrowcol-run-' + runId, issuer: cluster.properties.oidcIssuerProfile.issuerURL,
    subject: `system:serviceaccount:${namespace}:${serviceAccount}`, audience: AUDIENCE, phase: 'create-submitted' }, azure.c);
}
export async function prepare_run_fic(azure: Azure, runId: string, namespace: string, serviceAccount: string,
  recordPath: string): Promise<void> {
  validate_run_identity(runId, namespace, serviceAccount);
  await reset_lock(recordPath, async () => {
    if (fs.existsSync(recordPath)) {
      const record = read_run_fic_record(recordPath, azure.c), snapshot = await read_ledger(azure);
      if (record.runId !== runId || record.namespace !== namespace || record.serviceAccount !== serviceAccount ||
          record.phase !== 'ready' || snapshot?.value.transaction?.phase !== 'complete')
        throw new Failure('Existing run FIC attempt is not definitively complete; no replay.');
      assert_owned_entry(snapshot.value.entries[runId], record);
      await verify_run_fic(azure, record);
      return;
    }
    const record = await new_record(azure, runId, namespace, serviceAccount);
    publish_private(recordPath, record);
    await with_coordinator(azure, record, recordPath, 'prepare', async (ledger, save) => {
      const createIntent = intent();
      const entry: LedgerEntry = { record, createIntent };
      ledger.entries[runId] = entry;
      await save();
      publish_private(recordPath + '.create-intent.json', { record, intent: createIntent });
      const receipt = await arm_mutation(azure, record, 'create', createIntent, recordPath);
      if (receipt.status !== 201)
        throw new Failure('ARM did not return create-only status 201; retain custody and reconcile without replay.');
      const original = JSON.parse(Buffer.from(receipt.bodyBase64, 'base64').toString('utf8'));
      if (!credential_matches(original, record)) throw new Failure('ARM creation receipt has an unexpected credential identity.');
      const postcondition = await arm_postcondition(azure, record, 'create', recordPath);
      const current = receipt_resource(postcondition);
      if (!credential_matches(current, record)) throw new Failure('Run FIC creation postcondition does not match the intent.');
      entry.createReceipt = receipt;
      entry.createPostcondition = postcondition;
      entry.createdResource = current;
      entry.record = { ...record, phase: 'ready' };
      await save();
      publish_private(recordPath, entry.record, record);
    }, ledger => admit_run_fic(azure, record, ledger));
  });
  console.log('Run FIC ready with definitive creation receipt and exact identity; token probe is still required.');
}
export async function cleanup_run_fic(azure: Azure, runId: string, recordPath: string): Promise<void> {
  uuid(runId);
  await reset_lock(recordPath, async () => {
    const record = read_run_fic_record(recordPath, azure.c), snapshot = await read_ledger(azure);
    if (record.runId !== runId || snapshot?.value.transaction?.phase !== 'complete')
      throw new Failure('Run FIC custody is unresolved or belongs to a different run.');
    assert_owned_entry(snapshot.value.entries[runId], record);
    if (record.phase === 'deleted') return;
    if (record.phase !== 'ready') throw new Failure('Run FIC cleanup never replays an interrupted mutation.');
    await verify_run_resources(azure, record);
    const namespace = await kubectl(record, ['get', 'namespace', record.namespace, '--ignore-not-found', '--output', 'name']);
    if (namespace) throw new Failure('Run namespace must be absent before removing its federated trust.');
    await with_coordinator(azure, record, recordPath, 'cleanup', async (ledger, save) => {
      const entry = ledger.entries[runId];
      assert_owned_entry(entry, record);
      entry.deleteIntent = intent();
      entry.record = { ...record, phase: 'delete-submitted' };
      await save();
      publish_private(recordPath, entry.record, record);
      publish_private(recordPath + '.delete-intent.json', { record: entry.record, intent: entry.deleteIntent });
      const receipt = await arm_mutation(azure, record, 'delete', entry.deleteIntent, recordPath);
      if (![200, 204].includes(receipt.status)) throw new Failure('Run FIC deletion is not definitive; retain custody.');
      const postcondition = await arm_postcondition(azure, record, 'delete', recordPath);
      const previous = entry.record;
      entry.deleteReceipt = receipt;
      entry.deletePostcondition = postcondition;
      entry.record = { ...record, phase: 'deleted' };
      await save();
      publish_private(recordPath, entry.record, previous);
    }, async ledger => {
      const entry = ledger?.entries[runId];
      if (!entry || canonical_json(entry.record) !== canonical_json(record))
        throw new AdmissionRejected('drift', 'Cleanup authority changed before admission; no deletion.');
      assert_owned_entry(entry, record);
      if (await kubectl(record, ['get', 'namespace', record.namespace, '--ignore-not-found', '--output', 'name']))
        throw new AdmissionRejected('workload', 'Run namespace reappeared during cleanup; retain its trust and reconcile.');
      const current = await azure.arm('GET', record.identityId + '/federatedIdentityCredentials/' + record.credentialName,
        undefined, true, ARM_API);
      if (!current || !credential_matches(current, record) || canonical_json(current) !== canonical_json(entry.createdResource))
        throw new AdmissionRejected('drift', 'Live run FIC changed since definitive creation; stop without deleting it.');
    });
  });
  console.log('Only the recorded run FIC was removed; immutable ownership receipts retained.');
}
