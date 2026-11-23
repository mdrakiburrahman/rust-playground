import { resolve } from 'node:path';
import { inspectPinnedPreloadFailure } from './local-failed-disposition.js';

const pins = {
  invocation: '89fdefa5-855d-46ea-93ea-481fd77cbd2e',
  run: '8d46570b-791e-43c6-a25a-4832c5cbee81',
  cell: '241b7533-1025-441c-b8b2-ae9bbc547c1e',
  table: 'sqlserver_availability_replica_states',
  collectorImage: 'sha256:c6ed6724f3323ca120fe50f93fccf505c48eb593cd3b7c2e5c8ce85b92a8a93e',
  collectorRevision: '3630d3e4301bc3ba8f9f69a308d077cebee6d12a',
  receiptSha256: '9c288590a651a8fcdbe1ba9fa242ed235b565923416392e5bbbb6c9701d359a8',
  markerSha256: '9dafe9bf79d8dd0f3c7398a2abcbe851a0a931fec4f319d8bfe5fa0facf9d12e',
  resultSha256: 'b6dc74b976de44d22f4ee328f4add64cdf36060e05bb7b268191cac3eeea7ee1',
  logSha256: '04fd38d33e9ac82af23be0d5fcefb078128a0bc939329e50f7bb3a19a7663503',
};

async function main(): Promise<void> {
  if (process.argv.length !== 4) throw new Error('Usage: inspect-preload-failure.ts RECEIPT MARKER');
  const result = await inspectPinnedPreloadFailure(resolve(process.argv[2]!), resolve(process.argv[3]!), pins);
  console.log(JSON.stringify(result));
}

main().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
