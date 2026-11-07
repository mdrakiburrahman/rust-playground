import fs from 'node:fs';
import path from 'node:path';

const JOB = /^azure-qap-[0-9]{14}-[a-f0-9]{8}$/;
const UID = /^[a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12}$/;
type ObjectRecord = Record<string, unknown>;
function object(value: unknown): ObjectRecord {
  if (!value || typeof value !== 'object' || Array.isArray(value)) throw new Error('Invalid owned Kubernetes object.');
  return value as ObjectRecord;
}

export interface RetainedJobReceipt {
  version: 1;
  mode: 'read-only-inspection';
  jobName: string;
  namespace: string;
  context: string | null;
  pod: string;
  podUid: string;
  resources: Record<string, string>;
  runId: string;
  inspectionDeadline: string;
}

export function parseRetainedReceipt(value: unknown, namespace: string, context?: string): RetainedJobReceipt {
  const raw = object(value);
  if (raw.version !== 1 || raw.mode !== 'read-only-inspection' || typeof raw.jobName !== 'string' ||
      !JOB.test(raw.jobName) || raw.namespace !== namespace || raw.context !== (context ?? null) ||
      typeof raw.pod !== 'string' || !new RegExp(`^${raw.jobName}-[a-z0-9]{5}$`).test(raw.pod) ||
      typeof raw.podUid !== 'string' || !UID.test(raw.podUid) ||
      typeof raw.runId !== 'string' || !/^\d{8}t\d{6}_[a-f0-9]{4}$/.test(raw.runId) ||
      typeof raw.inspectionDeadline !== 'string' || !Number.isFinite(Date.parse(raw.inspectionDeadline)))
    throw new Error('Receipt does not match the configured namespace/context or a retained run-owned Job.');
  const resources = object(raw.resources);
  const names = [`job/${raw.jobName}`, `configmap/${raw.jobName}-env`, `secret/${raw.jobName}-secret`];
  if (Object.keys(resources).length !== names.length || names.some(name => typeof resources[name] !== 'string' || !UID.test(resources[name])))
    throw new Error('Receipt must identify exactly the owned Job, ConfigMap and Secret UIDs.');
  return raw as unknown as RetainedJobReceipt;
}

export function validateOwnedMetadata(value: unknown, name: string, namespace: string, job: string, uid?: string): string {
  const metadata = object(object(value).metadata);
  const labels = object(metadata.labels);
  if (metadata.name !== name || metadata.namespace !== namespace || typeof metadata.uid !== 'string' ||
      !UID.test(metadata.uid) || (uid !== undefined && metadata.uid !== uid) ||
      labels['app.kubernetes.io/name'] !== 'azure-qap' || labels.run !== job)
    throw new Error(`Ownership mismatch for ${name}; refusing to signal or delete.`);
  return metadata.uid;
}

export type Kubectl = (args: string[], timeoutSeconds?: number) => Promise<string>;

export async function cleanupRetainedJob(input: {
  receipt: unknown; namespace: string; context?: string; output: string; kubectl: Kubectl;
  now?: () => number; sleep?: (ms: number) => Promise<void>;
}): Promise<void> {
  const receipt = parseRetainedReceipt(input.receipt, input.namespace, input.context);
  const { kubectl } = input;
  const get = async (name: string) => JSON.parse(await kubectl(['get', name, '-n', receipt.namespace, '-o', 'json'], 30));
  for (const [name, uid] of Object.entries(receipt.resources))
    validateOwnedMetadata(await get(name), name.split('/')[1]!, receipt.namespace, receipt.jobName, uid);
  const pod = await get(`pod/${receipt.pod}`);
  validateOwnedMetadata(pod, receipt.pod, receipt.namespace, receipt.jobName, receipt.podUid);
  const owners = object(pod.metadata).ownerReferences;
  if (!Array.isArray(owners) || !owners.some(owner =>
    object(owner).kind === 'Job' && object(owner).uid === receipt.resources[`job/${receipt.jobName}`]))
    throw new Error('Pod owner does not match the receipt Job UID.');
  const exec = (args: string[], timeout = 30) => kubectl([
    'exec', '--container', 'experiment', '-n', receipt.namespace, receipt.pod, '--', ...args], timeout);
  const liveReport = object(JSON.parse(await exec(['cat', '/artifacts/report.json'])));
  const inspection = object(liveReport.inspection);
  const cleanup = object(liveReport.cleanup);
  if (liveReport.runId !== receipt.runId || inspection.mode !== 'read-only' ||
      inspection.deadline !== receipt.inspectionDeadline ||
      !['retained', 'done', 'failed'].includes(String(cleanup.status)))
    throw new Error('Live workload report does not match the retained receipt.');
  if (fs.existsSync(input.output)) throw new Error('Refusing to overwrite final cleanup artifacts.');
  console.log(`Stopping read-only inspection for exact owned Job ${receipt.jobName}; waiting up to 6m for scoped cleanup.`);
  if (cleanup.status === 'retained') await exec(['kill', '-TERM', '1']);
  const now = input.now ?? Date.now;
  const sleep = input.sleep ?? (ms => new Promise(resolve => setTimeout(resolve, ms)));
  const deadline = now() + 360_000;
  let done = false;
  while (now() < deadline) {
    const marker = await exec(['sh', '-c', 'if test -f /artifacts/done; then echo done; fi'], 15);
    if (marker.trim() === 'done') { done = true; break; }
    await sleep(2000);
  }
  if (!done) throw new Error('Scoped runtime cleanup did not finish within 6m; owned resources were NOT deleted.');
  fs.mkdirSync(input.output, { recursive: true, mode: 0o700 });
  await kubectl(['cp', '--container', 'experiment', '-n', receipt.namespace,
    `${receipt.pod}:/artifacts/.`, input.output], 120);
  const final = object(JSON.parse(fs.readFileSync(path.join(input.output, 'report.json'), 'utf8')));
  const finalCleanup = object(final.cleanup);
  if (final.runId !== receipt.runId || finalCleanup.status !== 'done' ||
      !Array.isArray(finalCleanup.errors) || finalCleanup.errors.length)
    throw new Error('Final scoped table/blob cleanup failed; artifacts copied but owned resources were NOT deleted.');
  for (const [name, uid] of Object.entries(receipt.resources))
    validateOwnedMetadata(await get(name), name.split('/')[1]!, receipt.namespace, receipt.jobName, uid);
  await exec(['kill', '-USR1', '1']);
  await kubectl(['delete', ...Object.keys(receipt.resources), '-n', receipt.namespace,
    '--wait=true', '--timeout=60s'], 90);
  console.log(`Scoped cleanup verified; final artifacts: ${input.output}`);
}
