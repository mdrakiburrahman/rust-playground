import { appendFileSync, readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';

const root = process.env.OAT_ENDPOINT_FIXTURE_ROOT!;
const args = process.argv.slice(2);
appendFileSync(join(root, 'commands'), `${args.join(' ')}\n`);
const state = JSON.parse(readFileSync(join(root, 'state.json'), 'utf8')) as {
  scaled: boolean;
  fail?: string;
  routerExited?: boolean;
  before: Record<string, number>;
  after: Record<string, number>;
};
if (args[0] === 'inspect') {
  console.log(args.includes('{{json .HostConfig.NanoCpus}}') ? '1000000000' : state.routerExited ? 'exited|17' : 'running|0');
} else if (args[0] === 'exec') {
  console.log('100000 100000\nnr_periods 10\nnr_throttled 0\nthrottled_usec 0');
} else if (args[0] === 'logs') {
  console.log('fixture router failed');
} else if (args.includes('ps')) {
  console.log('a'.repeat(64));
} else if (args.includes('up')) {
  state.scaled = true;
  writeFileSync(join(root, 'state.json'), JSON.stringify(state));
} else if (args.includes('port')) {
  const service = args[args.indexOf('port') + 1]!;
  if (state.fail === service) {
    console.error(`fixture: ${service} rediscovery failed`);
    process.exit(17);
  }
  console.log(`127.0.0.1:${(state.scaled ? state.after : state.before)[service]}`);
} else {
  throw new Error(`unexpected fixture command: ${args.join(' ')}`);
}
