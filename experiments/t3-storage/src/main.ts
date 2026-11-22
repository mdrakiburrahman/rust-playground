import { readFile } from 'node:fs/promises';
import { parseArgs } from 'node:util';
import { Lab, validateConfig } from './lab.js';
import { bootstrap, startJob } from './bootstrap.js';
import { waitJob } from './observe.js';
import type { Request } from './job.js';

const { values, positionals } = parseArgs({ options: { config: { type: 'string' }, vm: { type: 'string' }, script: { type: 'string' }, label: { type: 'string' }, request: { type: 'string' } }, allowPositionals: true });
if (!values.config) throw new Error('Pass --config with an ignored private lab configuration.');
const lab = new Lab(validateConfig(JSON.parse(await readFile(values.config, 'utf8'))));
switch (positionals[0]) {
  case 'deploy': await lab.deploy(); console.log('Deployment complete; inventory saved privately.'); break;
  case 'stop': await lab.stop(); console.log('Owned VMs deallocated; resources retained.'); break;
  case 'bootstrap': await bootstrap(lab); console.log('Owned guests bootstrapped.'); break;
  case 'refresh': await bootstrap(lab, true); console.log('Verified source and guest binaries refreshed.'); break;
  case 'start-job': {
    if (!values.vm || !values.request) throw new Error('start-job requires --vm and --request.');
    console.log(await startJob(lab, values.vm, JSON.parse(await readFile(values.request, 'utf8'))));
    break;
  }
  case 'wait-job': {
    if (!values.request) throw new Error('wait-job requires --request.');
    const request = JSON.parse(await readFile(values.request, 'utf8')) as Request;
    console.log(JSON.stringify(await waitJob(lab, request)));
    break;
  }
  case 'remote':
    if (!values.vm || !values.script || !values.label) throw new Error('remote requires --vm, --script and --label.');
    console.log(await lab.remote(values.vm, await readFile(values.script, 'utf8'), values.label));
    break;
  default: throw new Error('Expected deploy, bootstrap, start-job, wait-job, remote or stop.');
}
