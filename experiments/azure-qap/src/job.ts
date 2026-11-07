import fs from 'node:fs';
import { main } from './main.js';

const envFile = '/tmp/azure-qap.env';
fs.writeFileSync(envFile, '', { mode: 0o600 });
let artifactsReady = false;
const stopped = new Promise<void>(resolve => process.on('SIGTERM', () => {
  if (artifactsReady) resolve();
}));
// An unresolved Promise alone does not keep Node alive for kubectl cp.
const keepAlive = setInterval(() => {}, 60_000);
let code = 1;
try { code = await main([...process.argv.slice(2), '--env', envFile]); }
catch (error) { console.error(error instanceof Error ? error.message : String(error)); }
fs.writeFileSync('/artifacts/exit-code', `${code}\n`, { mode: 0o600 });
fs.writeFileSync('/artifacts/done', '', { mode: 0o600 });
process.exitCode = code;
artifactsReady = true;
await stopped;
clearInterval(keepAlive);
