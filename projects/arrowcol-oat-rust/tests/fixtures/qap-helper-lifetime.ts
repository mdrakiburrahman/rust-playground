import { fork } from 'node:child_process';
import { existsSync, writeFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';

const [mode, ready, release, receipt] = process.argv.slice(2);
if (!ready || !release || !receipt || !['helper', 'worker'].includes(mode)) {
  throw new Error('Expected fixture mode, ready, release and receipt paths.');
}

const expiry = setTimeout(() => {
  console.error('Local QAP lifetime fixture expired.');
  process.exit(2);
}, 10_000);

if (mode === 'worker') {
  const poll = setInterval(() => {
    if (!existsSync(release)) return;
    writeFileSync(receipt, JSON.stringify({ writerPid: process.pid, writtenAt: Date.now() }), { flag: 'wx' });
    clearInterval(poll);
    clearTimeout(expiry);
    if (!process.disconnect) throw new Error('Fixture worker IPC channel unavailable.');
    process.disconnect();
  }, 10);
  if (!process.send) throw new Error('Fixture worker IPC channel unavailable.');
  process.send({ ready: true });
} else {
  const child = fork(fileURLToPath(import.meta.url), ['worker', ready, release, receipt], {
    execArgv: ['--import', 'tsx'],
    stdio: ['ignore', 'ignore', 'inherit', 'ipc'],
  });
  child.once('error', error => {
    console.error(error);
    process.exit(2);
  });
  child.once('message', () => {
    if (!child.pid) throw new Error('Fixture descendant PID unavailable.');
    writeFileSync(ready, JSON.stringify({ helperPid: process.pid, workerPid: child.pid }), { flag: 'wx' });
  });
  child.once('exit', (code, signal) => {
    clearTimeout(expiry);
    if (signal || code !== 0) {
      console.error(`Fixture descendant failed: code=${code} signal=${signal}`);
      process.exitCode = 2;
    } else {
      console.log('Local QAP fixture completed.');
    }
  });
}
