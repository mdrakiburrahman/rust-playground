import { appendFileSync, existsSync, readFileSync, unlinkSync, writeFileSync } from 'node:fs';
import { createServer } from 'node:http';
import { join } from 'node:path';

const root = process.env.OAT_CLEANUP_FIXTURE_ROOT!;
const [tool, ...args] = process.argv.slice(2);
appendFileSync(join(root, 'commands'), `${tool} ${args.join(' ')}\n`);
const mode = readFileSync(join(root, 'mode'), 'utf8');
const fail = (message: string): never => {
  console.error(message);
  process.exit(17);
};

if (tool === 'docker') {
  if (args[0] === 'ps') {
    if (args.includes('--all')) fail('fixture stop listing failed');
    process.exit(0);
  }
  if (args[0] === 'compose' && args.includes('down') && args.includes('--volumes')) {
    writeFileSync(join(root, 'compose-destroyed'), '');
    process.exit(0);
  }
  fail('unexpected Docker fixture command');
}

if (args.includes('proxy')) {
  const port = Number(args[args.indexOf('--port') + 1]);
  createServer((request, response) => {
    if (request.method === 'GET' && request.url === '/api') return response.end('{}');
    if (request.method !== 'DELETE' || request.url !== '/api/v1/namespaces/fixture-owned') {
      return response.writeHead(400).end();
    }
    let body = '';
    request.on('data', chunk => { body += chunk; });
    request.on('end', () => {
      const options = JSON.parse(body);
      if (options.preconditions?.uid !== 'fixture-uid') return response.writeHead(409).end();
      writeFileSync(join(root, 'delete-options.json'), body);
      unlinkSync(join(root, 'namespace.json'));
      response.end('{}');
    });
  }).listen(port, '127.0.0.1');
} else if (args.includes('get') && args.includes('namespace')) {
  const path = join(root, 'namespace.json');
  if (existsSync(path)) process.stdout.write(readFileSync(path));
  else if (!args.includes('--ignore-not-found=true')) fail('unexpected missing namespace');
} else if (args.includes('get') && args.includes('deployment')) {
  if (mode === 'stop_failure') fail('fixture deployment stop failed');
  if (mode === 'rollout_hang') {
    setInterval(() => {}, 1000);
  } else if (mode === 'absent_deployment') {
    if (!args.includes('--ignore-not-found')) fail('NotFound');
  } else {
    const rollout = mode.startsWith('rollout_');
    const recovering = mode === 'rollout_recovers';
    const ready = recovering && existsSync(join(root, 'deployment-read'));
    if (recovering) writeFileSync(join(root, 'deployment-read'), '');
    console.log(JSON.stringify({
      metadata: { name: 'fixture', uid: 'fixture-deployment', generation: 1,
        annotations: { 'deployment.kubernetes.io/revision': '1' } },
      spec: { replicas: rollout ? 1 : 0, selector: { matchLabels: { app: 'fixture' } } },
      status: { replicas: rollout ? 1 : 0, updatedReplicas: rollout ? 1 : 0,
        availableReplicas: ready ? 1 : 0, observedGeneration: 1 },
    }));
  }
} else if (args.includes('get') && args.includes('replicasets')) {
  console.log(JSON.stringify({ items: [{
    metadata: { annotations: { 'deployment.kubernetes.io/revision': '1' },
      ownerReferences: [{ kind: 'Deployment', name: 'fixture', uid: 'fixture-deployment' }] },
    spec: { selector: { matchLabels: { 'pod-template-hash': 'fixture-hash' } } },
  }] }));
} else if (args.includes('get') && args.includes('pods')) {
  const marker = join(root, 'pods-read');
  if (mode.startsWith('rollout_')) {
    console.log(JSON.stringify({ items: [{
      metadata: { name: 'fixture-pod', labels: { 'pod-template-hash': 'fixture-hash' } },
      status: { containerStatuses: [{
        name: 'dataflow', restartCount: mode === 'rollout_persistent' ? 4 : 1,
        ready: false, state: { waiting: { reason: 'CrashLoopBackOff' } },
      }] },
    }] }));
  } else if (existsSync(marker)) console.log('{"items":[]}');
  else {
    writeFileSync(marker, '');
    console.log(JSON.stringify({ items: [{
      metadata: { name: 'fixture-pod' },
      status: { containerStatuses: [{
        name: 'dataflow',
        state: mode === 'exit_137'
          ? { terminated: { exitCode: 137, reason: 'Error', finishedAt: '2026-01-01T00:00:00Z' } }
          : { running: {} },
      }] },
    }] }));
  }
} else if (args.includes('logs')) {
  console.log('arrowcol_effective_config {"component":"receiver"}');
} else if (!args.includes('scale')) {
  fail('unexpected kubectl fixture command');
}
