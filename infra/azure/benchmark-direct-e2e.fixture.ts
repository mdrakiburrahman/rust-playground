// Executed in a separate process by the real kubectl adapters, including sync probes and streamed watches.
export function fakeKubectl() {
  const fs = require('node:fs') as typeof import('node:fs');
  const assert: typeof import('node:assert/strict') = require('node:assert/strict');
  const crypto = require('node:crypto') as typeof import('node:crypto');
  const file = process.env.DIRECT_E2E_STATE!;
  const s = JSON.parse(fs.readFileSync(file, 'utf8'));
  const a = process.argv.slice(2);
  const event = (event: string, extra = {}) => fs.appendFileSync(s.trace, JSON.stringify({ event, ...extra }) + '\n');
  const save = () => fs.writeFileSync(file, JSON.stringify(s));
  const emit = (value: unknown) => process.stdout.write(typeof value === 'string' ? value : JSON.stringify(value));
  assert.equal(a[a.indexOf('--server') + 1], 'https://fixture.azmk8s.io');
  assert.ok(a.includes('--insecure-skip-tls-verify=false'));
  const verb = a.find(v => ['get', 'patch', 'exec', 'logs', 'create'].includes(v))!;
  const at = a.indexOf(verb), kind = a[at + 1], name = a[at + 2];
  event('kubectl', { verb, kind, name });
  if (a.includes('--raw')) {
    const url = new URL(a.at(-1)!, 'https://fixture.azmk8s.io');
    const pod = s.objects['pod/' + url.searchParams.get('fieldSelector')!.split('=')[1]];
    assert.equal(url.searchParams.get('resourceVersion'), '17');
    assert.ok(pod.status.containerStatuses[0].state.terminated, 'watch must follow the actual stop PATCH');
    event('exit-watch', { name: pod.metadata.name });
    if (s.fault === 'exit-unknown') return emit(JSON.stringify({ type: 'DELETED', object: pod }) + '\n');
    return emit(JSON.stringify({ type: 'MODIFIED', object: pod }) + '\n');
  }
  if (verb === 'get') {
    if (kind === 'pods' || kind === 'replicasets') {
      const selector = a[a.indexOf('-l') + 1];
      const role = selector.includes('direct-load-confirmwide') ? 'direct-load-confirmwide'
        : selector.includes('direct-catalog-verified') ? 'direct-catalog-verified' : selector.split('=')[1];
      const items = Object.values(s.objects).filter((o: any) => o.kind === (kind === 'pods' ? 'Pod' : 'ReplicaSet') &&
        (o.metadata.labels.app === role || o.metadata.labels['batch.kubernetes.io/job-name'] === role));
      return emit({ kind: kind === 'pods' ? 'PodList' : 'ReplicaSetList', items });
    }
    if (kind === 'endpointslices') return emit({ kind: 'EndpointSliceList', items: [] });
    const object = s.objects[kind + '/' + name];
    assert.ok(object, 'unhandled fake Kubernetes GET ' + kind + '/' + name);
    return emit(object);
  }
  if (verb === 'logs') return emit(s.catalogLog);
  if (verb === 'create') {
    const pod = JSON.parse(fs.readFileSync(0, 'utf8'));
    assert.ok(pod.metadata.name.endsWith('-log-export'));
    pod.metadata.uid = s.helperUid;
    pod.spec.nodeName = pod.spec.affinity.nodeAffinity.requiredDuringSchedulingIgnoredDuringExecution.nodeSelectorTerms[0].matchFields[0].values[0];
    pod.status = { phase: 'Running', containerStatuses: [{ name: 'export', restartCount: 0,
      imageID: s.image, containerID: 'containerd://export-' + pod.metadata.name, state: { running: {} } }] };
    s.objects['pod/' + pod.metadata.name] = pod;
    save(); event('export-create', { name: pod.metadata.name }); return emit(pod);
  }
  if (verb === 'patch') {
    const ops = JSON.parse(a[a.indexOf('--patch') + 1]);
    const object = s.objects[kind + '/' + name];
    if (kind === 'configmap') {
      event('barrier-attempt');
      const events = fs.readFileSync(s.trace, 'utf8').trim().split('\n').map((line: string) => JSON.parse(line));
      assert.ok(events.some((e: any) => e.event === 'arm-readback'), 'barrier cannot mutate before arm bytes read back');
    }
    for (const op of ops) {
      const parts = op.path.split('/').slice(1).map((v: string) => v.replaceAll('~1', '/').replaceAll('~0', '~'));
      let parent = object;
      for (const part of parts.slice(0, -1)) parent = parent[part];
      const key = parts.at(-1);
      if (op.op === 'test') assert.deepEqual(parent[key], op.value, 'real JSON PATCH precondition');
      else { assert.ok(['replace', 'add'].includes(op.op)); parent[key] = op.value; }
    }
    object.metadata.resourceVersion = '28';
    if (kind === 'configmap') {
      event('release');
      save();
      if (s.fault === 'release-unknown') process.exit(1);
      if (s.fault === 'release-replaced') object.metadata.uid = s.helperUid;
    } else {
      object.metadata.generation++;
      object.status.observedGeneration = object.metadata.generation;
      if (kind === 'daemonset') {
        Object.assign(object.status, { desiredNumberScheduled: 0, currentNumberScheduled: 0, numberReady: 0, numberMisscheduled: 0 });
        for (const [key, pod] of Object.entries(s.objects) as [string, any][]) {
          if (pod.kind === 'Pod' && pod.metadata.labels.app === 'arrowcol-router') delete s.objects[key];
        }
      } else {
        const pod = s.objects['pod/' + s.roles[name]];
        const runtime = pod.status.containerStatuses[0];
        runtime.state = { terminated: { startedAt: runtime.state.running.startedAt,
          finishedAt: new Date(s.now).toISOString(), containerID: runtime.containerID, exitCode: 0, reason: 'Completed' } };
        pod.metadata.resourceVersion = '28'; pod.status.phase = 'Succeeded';
      }
      event('stop', { name }); save();
      if (s.fault === 'stop-unknown') process.exit(1);
    }
    return emit(object);
  }
  assert.equal(verb, 'exec');
  const cmd = a.slice(a.indexOf('--') + 1);
  if (cmd[0] === 'readlink' || cmd[0] === '/usr/bin/readlink') {
    assert.equal(cmd.at(-1), '/proc/self/ns/time');
    event('time-namespace', { name: kind, loaded: s.loaded });
    return emit(s.loaded && s.fault === 'clock-namespace' ? 'time:[2]\n' : 'time:[1]\n');
  }
  if (cmd[0] === 'date') {
    if (s.loaded && s.fault === 'clock-missing') process.exit(1);
    const offset = s.fault === 'clock-boundary'
      ? (s.objects['pod/' + kind].spec.nodeName === 'writer-node' ? -50 : 50)
      : s.loaded && s.fault === 'clock-skew' ? 101 : 0;
    return emit(String(BigInt(s.now + offset) * 1_000_000n) + '\n');
  }
  if (cmd[0] === '/usr/local/bin/sqlcmd-secret') {
    const stopped = !!s.objects['pod/' + s.roles.arrowcol].status.containerStatuses[0].state.terminated;
    return emit([{ ...s.registration, state: stopped ? 'stopped' : 'ready', row_version: stopped ? '0x0000000000000002' : '0x0000000000000001' }]);
  }
  if (cmd[0] === '/bin/sh') {
    if (kind === s.roles.arrowcol) return emit('writer-capture-active\n');
    return emit('OAT_CLIENT_READY client_id=direct-' + s.runId + '-0 preflight_tables=1\n');
  }
  const stream = cmd.at(-1)!.split('/').at(-1);
  const owner = kind === 'direct-writer-log-export' ? 'writer' : 'client';
  let bytes = s.logs[owner + '-' + stream];
  if (s.fault === 'audit-invalid' && owner === 'client' && stream === 'stdout') bytes = bytes.replace('"completed"', '"failed"');
  if (cmd[0] === '/usr/bin/sha256sum') return emit(crypto.createHash('sha256').update(bytes).digest('hex') + '  /evidence/' + stream + '\n');
  assert.equal(cmd[0], '/bin/cat');
  event('export-bytes', { owner, stream }); return emit(bytes);
}
