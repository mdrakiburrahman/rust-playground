import assert from 'node:assert/strict';
import test from 'node:test';
import { clientPids, networkAddresses, parseContainers, portConflicts, publishedPorts } from './stacks.js';

const first = 'collector-chain-20261104t175805-1128';
const second = 'collector-chain-20261104t220743-c0b0';

test('publishedPorts reads host TCP ports and ignores exposed-only and UDP ones', () => {
  assert.deepEqual(publishedPorts('0.0.0.0:8080->8080/tcp, [::]:8080->8080/tcp, :::8090->80/tcp'), [8080, 8090]);
  assert.deepEqual(publishedPorts('0.0.0.0:32768-32770->8000-8002/tcp'), [32768, 32769, 32770]);
  assert.deepEqual(publishedPorts('4317-4318/tcp, 8080/tcp, 0.0.0.0:53->53/udp'), []);
});

test('portConflicts finds the earlier runs holding the ports, including a half-started one', () => {
  const containers = parseContainers([
    `${first}-kusto-1\t${first}\t127.0.0.1:8080->8080/tcp`,
    `${first}-otapcol-gateway-1\t${first}\t4317-4318/tcp, 8080/tcp`,
    `${second}-kusto-1\t${second}\t`,
    `${second}-kusto-explorer-1\t${second}\t127.0.0.1:8090->8080/tcp`,
    'rust-playground_devcontainer-workspace-1\trust-playground_devcontainer\t',
  ].join('\n'));
  assert.deepEqual(portConflicts(containers, [8080, 8090]), { earlier: new Map([[first, [8080]], [second, [8090]]]), foreign: [] });
  assert.deepEqual(portConflicts(containers, [8081, 8091]), { earlier: new Map(), foreign: [] });
});

test('portConflicts never treats containers outside this experiment as replaceable', () => {
  const containers = parseContainers([
    'web\t\t0.0.0.0:8080->80/tcp, :::8080->80/tcp',
    'collector-chain-demo-kusto-1\tcollector-chain-demo\t127.0.0.1:8090->8080/tcp',
  ].join('\n'));
  assert.deepEqual(portConflicts(containers, [8080, 8090]), {
    earlier: new Map(),
    foreign: [{ name: 'web', port: 8080 }, { name: 'collector-chain-demo-kusto-1', port: 8090 }],
  });
});

test('clientPids selects only the clients that send to the stack, whatever namespace started them', () => {
  const client = (arrow: string, otlp: string) =>
    `/workspaces/rust-playground/target/release/collector-chain --arrow-endpoint http://${arrow}:32310 --otlp-endpoint http://${otlp}:4318 ` +
    '--rows 100 --signals 10 --client-id cfa09f4a --interval 10';
  const ps = [
    `  47166 ${client('172.19.0.7', '172.19.0.10')}`,
    ` 126474 ${client('172.19.0.6', '172.19.0.8')}`,
    `  47167 ${client('172.20.0.6', '172.20.0.8')}`,
    '  47200 [collector-chain] <defunct>',
    `  47300 sh -c '${client('172.19.0.7', '172.19.0.10')}'`,
    '  47400 node node_modules/.bin/tsx experiments/collector-chain/src/main.ts --stream',
  ].join('\n');
  const stack = networkAddresses('172.19.0.2/16\n172.19.0.6/16\n172.19.0.7/16\n172.19.0.8/16\n172.19.0.10/16\n');
  assert.deepEqual(clientPids(ps, 'collector-chain', stack), [47166, 126474]);
});
