import { createHash } from 'node:crypto';
import fs from 'node:fs';
import { pathToFileURL } from 'node:url';
import { parseArgs } from 'node:util';
import { validateCampaignState } from './aks-campaign.js';
import { loadCustodyConfig } from './aks-custody-config.js';
import { validateEffects, validateRecoveryTargets } from './aks-scoped-watchdog.js';
import { canonical_json, type Config } from '../../infra/azure/bootstrap.js';
import { readRelease, validateDurableArm } from '../../infra/azure/benchmark-direct-cli.js';
import { assertDirectInventory } from '../../infra/azure/benchmark-direct-handoff.js';
import { directEvidencePrefix } from '../../infra/azure/benchmark-evidence-custody.js';
import { evidenceObject, parseEvidence, requireEvidence } from '../../infra/azure/benchmark-evidence-json.js';
import { directConditionalStopArgv } from '../../infra/azure/benchmark-stop-patch.js';
import { directArmRecordPath } from '../../infra/azure/benchmark-terminal-custody.js';
import { parseEnvelope, processEffectRequest, type EffectEnvelope, type EffectRule } from '../../infra/azure/effect-boundary.js';
import { assert_run_fic_plan, parse_run_fic_record, run_fic_ledger_path } from '../../infra/azure/run_fic.js';

const storageAudience = 'https://storage.azure.com/';
const managementAudience = 'https://management.azure.com/';
const uuid = /^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/;
const hex = /^[a-f0-9]{64}$/;
const sha = (bytes: string) => createHash('sha256').update(bytes).digest('hex');
const isText = (value: unknown, pattern: RegExp): value is string => typeof value === 'string' && pattern.test(value);

function privateRecord(filename: string): unknown {
  const fd = fs.openSync(filename, fs.constants.O_RDONLY | fs.constants.O_NOFOLLOW | fs.constants.O_NONBLOCK);
  try {
    const stat = fs.fstatSync(fd), limit = 8 * 1024 ** 2;
    requireEvidence(stat.isFile() && !(stat.mode & 0o077) && stat.size <= limit, 'Expected a bounded private durable record');
    const bytes = Buffer.alloc(stat.size + 1);
    let size = 0;
    while (size < bytes.length) {
      const read = fs.readSync(fd, bytes, size, bytes.length - size, null);
      if (!read) break;
      size += read;
    }
    requireEvidence(size === stat.size, 'Durable record changed while reading');
    const text = new TextDecoder('utf-8', { fatal: true }).decode(bytes.subarray(0, size));
    parseEvidence(text); // Reject duplicate keys before using the stock JSON representation.
    return JSON.parse(text) as unknown;
  } finally { fs.closeSync(fd); }
}

function readSet(value: unknown, armHash: string, baselineVersion: number) {
  const row = evidenceObject(value);
  const { schema_version, arm_sha256, max_delta_version, parquet_paths, delta_markers } = row;
  requireEvidence(Object.keys(row).length === 5 && schema_version === 1 && arm_sha256 === armHash &&
    typeof max_delta_version === 'number' && Number.isSafeInteger(max_delta_version) &&
    max_delta_version >= baselineVersion && max_delta_version < 256,
  'Delta read set requires the original arm hash and a bounded commit ceiling covering the baseline');
  const list = (items: unknown, max: number, valid: (item: unknown) => item is string) => {
    requireEvidence(Array.isArray(items) && items.length <= max && items.every(valid) &&
      new Set(items).size === items.length, 'Delta read set is missing, ambiguous or exceeds its bound');
    return [...items].sort();
  };
  const files = list(parquet_paths, 512, (p): p is string => typeof p === 'string' && p.length <= 2048 &&
    p.endsWith('.parquet') && p.split('/').every(part => /^[A-Za-z0-9_.=+-]+$/.test(part) && !['.', '..'].includes(part)));
  const markers = list(delta_markers, 99, (m): m is string => isText(m, /^[!-~]{1,2048}$/));
  return { maxVersion: max_delta_version, files, markers };
}

function generate(config: Config, campaignRecord: unknown, ficRecord: unknown, campaignReads: number,
  recovery?: { arm: unknown; release: ReturnType<typeof readRelease>; context: string; reads: unknown }) {
  const state = validateCampaignState(campaignRecord), inventory = state.runInventory;
  const fic = parse_run_fic_record(ficRecord, config);
  requireEvidence(state.mode === 'armed' && state.status === 'active' && state.fencingToken === 1 && inventory &&
    inventory.campaign_id === state.campaignId && inventory.owner_id === state.ownerId && inventory.fencing_token === 1 &&
    fic.phase === 'ready' && fic.runId === inventory.cell_run_id && fic.namespace === inventory.namespace.name &&
    fic.namespaceUid === inventory.namespace.uid && fic.namespaceOwner === inventory.namespace.owner_token &&
    inventory.fic?.resource_id === `${fic.identityId}/federatedIdentityCredentials/${fic.credentialName}` &&
    inventory.fic.name === fic.credentialName && inventory.fic.plan,
  'Expected the original armed producer inventory and exact ready FIC identity');
  assert_run_fic_plan(inventory.fic.plan, fic);
  requireEvidence(Number.isSafeInteger(campaignReads) && campaignReads >= 3 && campaignReads <= 9999,
    'Campaign GET budget must be an integer from 3 to 9999');

  const effects: EffectRule[] = [];
  function add(id: string, transport: EffectRule['transport'], method: string, resource: string, path: string,
    max_calls: number, body_sha256?: string) {
    effects.push({ id, transport, method, resource, path, max_calls, ...(body_sha256 ? { body_sha256 } : {}) });
  }
  const blobOrigin = `https://${config.account}.blob.core.windows.net`;
  const checkpoint = '/' + config.checkpoints;
  add('campaign-read', 'campaign-blob', 'GET', blobOrigin + checkpoint, checkpoint + '/active.json', campaignReads);
  add('campaign-fence', 'campaign-blob', 'PUT', blobOrigin + checkpoint, checkpoint + '/active.json', 1);
  const token = (id: string, audience: string, count: number, tail: string[]) =>
    add(id, 'process', 'AZURE_ACCOUNT_GET-ACCESS-TOKEN', 'az',
      JSON.stringify(['account', 'get-access-token', '--subscription', config.subscription, '--resource', audience, ...tail]), count);
  token('campaign-token', storageAudience, campaignReads + 1,
    ['--query', 'accessToken', '--output', 'tsv', '--only-show-errors']);

  const shape = () => parseEnvelope({ schema_version: 1, campaign_id: state.campaignId, run_id: fic.runId,
    subscription_id: config.subscription, resource_group: config.group, storage_account: config.account,
    state_container: config.checkpoints, effects });
  if (!recovery) {
    const envelope = shape();
    validateEffects(envelope, false, false);
    return envelope;
  }

  const { context, release } = recovery;
  requireEvidence(/^[A-Za-z0-9._:-]{1,128}$/.test(context) && !['get', 'logs', 'patch', 'delete', 'apply', 'create',
    'replace', 'exec'].includes(context), 'Recovery requires an unambiguous exact Kubernetes context');
  const generation = evidenceObject(evidenceObject(recovery.arm).origin).cloudGeneration;
  requireEvidence(isText(generation, uuid), 'Original publisher generation is missing or invalid');
  const arm = validateDurableArm(recovery.arm, { runId: fic.runId, namespace: fic.namespace, namespaceUid: fic.namespaceUid,
    owner: fic.namespaceOwner, sourceRevision: release.revision, cloudGeneration: generation, fencingToken: inventory.fencing_token }, release);
  assertDirectInventory({ c: config }, inventory, fic, 'finish', arm);
  const { origin, baseline } = arm;
  const root = baseline.table_root;
  requireEvidence(isText(root, /^[A-Za-z0-9_.\/-]+$/) && root.startsWith(`direct-baseline-1/runs/${origin.runId}/dataflow/`) &&
    root.split('/').every(part => part && part !== '.' && part !== '..') &&
    isText(baseline.table_id, uuid) && Number.isSafeInteger(baseline.version) && baseline.version >= 0 &&
    [baseline.active_files_sha256, baseline.history_sha256, baseline.last_commit_sha256].every(value => isText(value, hex)),
  'Original physical Delta baseline is missing or invalid');
  const delta = readSet(recovery.reads, sha(canonical_json(arm)), baseline.version);
  const lifetimes = [arm.writer, arm.maintenance, arm.sql];
  requireEvidence([arm.routerUid, ...lifetimes.flatMap(pod => [pod.deploymentUid, pod.podUid])].every(value => isText(value, uuid)) &&
    lifetimes.every(pod => isText(pod.podName, /^[a-z0-9]([a-z0-9-]{0,251}[a-z0-9])?$/)) &&
    new Set(lifetimes.map(pod => pod.podName)).size === lifetimes.length &&
    new Set([arm.routerUid, ...lifetimes.flatMap(pod => [pod.deploymentUid, pod.podUid])]).size === 7,
  'Original workload lifetimes are missing or ambiguous');

  const namespace = origin.namespace, server = fic.server;
  const base = ['--server', server, '--insecure-skip-tls-verify=false'];
  const selected = ['--context', context, ...base, '--request-timeout=15s', '-n', namespace];
  const kube = (id: string, method: string, value: unknown, count: number, bodyHash?: string) =>
    add(id, 'process', method, 'kubectl', JSON.stringify(value), count, bodyHash);
  const get = (id: string, args: string[], count: number) => {
    const request = processEffectRequest(['kubectl', ...args]);
    requireEvidence(request.method === 'KUBECTL_GET', 'Ambiguous Kubectl read target');
    add(id, request.transport, request.method, request.resource, request.path, count);
  };
  const observe = (id: string, args: string[], count = 12) => get(id, [...selected, 'get', ...args, '-o', 'json'], count);
  observe('namespace-read', ['namespace', namespace]);
  observe('router-read', ['daemonset', 'arrowcol-router'], 450);
  observe('router-pods', ['pods', '-l', 'app=arrowcol-router'], 450);
  for (const service of ['arrowcol-router', 'arrowcol-router-node-local'])
    observe('endpoints-' + service, ['endpointslices', '-l', `kubernetes.io/service-name=${service}`], 450);
  observe('sql-deployment', ['deployment', 'arrowcol-sql']);
  observe('sql-replicasets', ['replicasets', '-l', 'app=arrowcol-sql']);
  observe('sql-pods', ['pods', '-l', 'app=arrowcol-sql']);
  for (const [role, uid, pod] of [
    ['arrowcol-router', arm.routerUid, undefined], ['arrowcol', arm.writer.deploymentUid, arm.writer],
    ['delta-maintenance', arm.maintenance.deploymentUid, arm.maintenance],
  ] as const) {
    kube('stop-' + role, 'KUBECTL_DIRECT_STOP', { argv: directConditionalStopArgv(context, server, origin, role), uid }, 1);
    if (!pod) continue;
    observe('deployment-' + role, ['deployment', role]);
    observe('pod-' + role, ['pod', pod.podName]);
    observe('pods-' + role, ['pods', '-l', `app=${role}`]);
    kube('watch-' + role, 'KUBECTL_DIRECT_EXIT_WATCH', { context, server, namespace, pod: pod.podName, uid: pod.podUid }, 3);
  }
  kube('sql-read', 'KUBECTL_DIRECT_SQL_READ', { context, server, namespace, pod: arm.sql.podName, uid: arm.sql.podUid,
    run: origin.runId, generation }, 4);
  const cleanup = [...base, '--request-timeout', '15s'];
  get('cleanup-namespace', [...cleanup, 'get', 'namespace', namespace, '--output', 'json'], 8);
  get('cleanup-namespace-absence', [...cleanup, 'get', 'namespace', namespace, '--ignore-not-found', '--output', 'name'], 12);
  kube('namespace-delete', 'KUBECTL_DELETE', [...cleanup, 'delete', '--raw', '/api/v1/namespaces/' + namespace, '-f', '-'],
    1, sha(canonical_json({ apiVersion: 'v1', kind: 'DeleteOptions', preconditions: { uid: fic.namespaceUid } })));

  for (const [id, audience] of [['storage-token', storageAudience], ['management-token', managementAudience]])
    token(id!, audience!, 120, ['--only-show-errors', '--output', 'json']);
  const azure = (id: string, method: string, resource: string, path: string, count: number) =>
    add(id, 'azure-rest', method, resource, path, count);
  const armRule = (id: string, method: string, path: string, api: string, count: number) =>
    azure(id, method, `https://management.azure.com|${managementAudience}`, `${path}?api-version=${api}`, count);
  const ficCollection = fic.identityId + '/federatedIdentityCredentials', credential = ficCollection + '/' + fic.credentialName;
  armRule('aks-get', 'GET', fic.aksId, '2024-10-01', 12);
  armRule('identity-get', 'GET', fic.identityId, '2023-01-31', 12);
  armRule('fic-list', 'GET', ficCollection, '2023-01-31', 12);
  armRule('fic-get', 'GET', credential, '2023-01-31', 20);
  armRule('fic-delete', 'DELETE', credential, '2023-01-31', 1);
  armRule('storage-get', 'GET', config.storage_id, '2023-05-01', 8);
  armRule('checkpoint-container-get', 'GET', `${config.storage_id}/blobServices/default/containers/${config.checkpoints}`, '2023-05-01', 8);

  const blob = (id: string, method: string, container: string, name: string, count: number, query?: Record<string, string>) =>
    azure(id, method, `${blobOrigin}|${storageAudience}`, '/' + container +
      (name ? '/' + name.split('/').map(encodeURIComponent).join('/') : '') + (query ? '?' + new URLSearchParams(query) : ''), count);
  const cp = (id: string, method: string, path: string, count: number, query?: Record<string, string>) =>
    blob(id, method, config.checkpoints, path, count, query);
  cp('publisher-head', 'HEAD', '', 40, { restype: 'container' });
  cp('publisher-lease', 'PUT', '', 2, { comp: 'lease', restype: 'container' });
  cp('publisher-compare-and-set', 'PUT', '', 1, { restype: 'container', comp: 'metadata' });
  cp('ledger-lease', 'PUT', run_fic_ledger_path(config), 2, { comp: 'lease' });
  cp('original-arm', 'GET', directArmRecordPath(origin), 6);
  cp('original-producer-seal', 'GET', `${directEvidencePrefix(origin)}/seal.json`, 6);
  const direct = `_arrowcol-custody/direct/${origin.runId}/${origin.namespaceUid}`;
  const pair = (id: string, path: string, writes = 1) => {
    cp(id + '-get', 'GET', path, 12);
    cp(id + '-put', 'PUT', path, writes);
  };
  pair('ledger', run_fic_ledger_path(config), 6);
  pair('namespace-journal', `_arrowcol-custody/namespaces/${origin.runId}/${origin.namespaceUid}.json`, 3);
  for (const role of ['arrowcol-router', 'arrowcol', 'delta-maintenance']) pair('journal-stop-' + role, `${direct}/stop-${role}.json`, 2);
  for (const role of ['arrowcol', 'delta-maintenance']) pair('journal-exit-' + role, `${direct}/exit-${role}.json`);
  pair('interrupted-seal', `${direct}/interrupted/seal.json`);
  const prefix = `${root}/_delta_log/`;
  for (const [i, marker] of ['', ...delta.markers].entries())
    blob(`delta-list-${i}`, 'GET', config.data, '', 12, { restype: 'container', comp: 'list', prefix, marker, maxresults: '1000' });
  blob('delta-checkpoint-pointer', 'GET', config.data, `${prefix}_last_checkpoint`, 12);
  for (let version = 0; version <= delta.maxVersion; version++) {
    const filename = `${String(version).padStart(20, '0')}.json`;
    blob(`delta-commit-${version}`, 'GET', config.data, prefix + filename, 12);
    pair(`interrupted-commit-${version}`, `${direct}/interrupted/delta-${filename}`);
  }
  delta.files.forEach((file, i) => blob(`delta-file-${i}`, 'HEAD', config.data, `${root}/${file}`, 12));
  const envelope = shape();
  validateEffects(envelope, true, true);
  validateRecoveryTargets(envelope, config, arm, fic, context);
  return envelope;
}

export function watchdogEnvelopeMain(args: string[]): EffectEnvelope {
  const { values, tokens } = parseArgs({ args, tokens: true, options: {
    mode: { type: 'string' }, 'env-file': { type: 'string' }, 'stamp-config': { type: 'string' },
    'campaign-record': { type: 'string' }, 'fic-record': { type: 'string' }, 'campaign-reads': { type: 'string' },
    'arm-record': { type: 'string' }, 'release-file': { type: 'string' },
    'delta-read-set': { type: 'string' }, context: { type: 'string' },
  } });
  requireEvidence(new Set(tokens.map(token => token.kind === 'option' ? token.name : '')).size === tokens.length,
    'Duplicate watchdog envelope option');
  const { mode, context, 'env-file': env, 'stamp-config': stamp, 'campaign-record': campaign,
    'fic-record': fic, 'campaign-reads': budget, 'arm-record': arm, 'release-file': release, 'delta-read-set': reads } = values;
  requireEvidence(env && stamp && campaign && fic && isText(budget, /^[1-9][0-9]{0,3}$/) &&
    (mode === 'fence' && [context, arm, release, reads].every(value => value === undefined) ||
      mode === 'recovery' && context && arm && release && reads), 'Missing or incompatible watchdog envelope options');
  const { config } = loadCustodyConfig(['--env-file', env, '--stamp-config', stamp], false, {});
  return generate(config, privateRecord(campaign), privateRecord(fic), Number(budget),
    context && arm && release && reads ? { context, arm: privateRecord(arm), release: readRelease(release), reads: privateRecord(reads) } : undefined);
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
  try { process.stdout.write(JSON.stringify(watchdogEnvelopeMain(process.argv.slice(2)), null, 2) + '\n'); }
  catch (error) {
    console.error(error instanceof Error ? error.message : 'Watchdog envelope generation failed');
    process.exitCode = 1;
  }
}
