/** Bounded private stderr tail for the reader, supervised as part of the owned process group. */
import { spawn } from 'node:child_process';
import { closeSync, ftruncateSync, openSync, writeSync } from 'node:fs';

const [reader, command, input, output, diagnostic, ...extra] = process.argv.slice(2);
const out = openSync(output, 'w', 0o600);
const err = openSync(diagnostic, 'w', 0o600);
let tail = Buffer.alloc(0);
const record = (data: Buffer): void => {
  tail = Buffer.concat([tail, data]).subarray(-65536);
  writeSync(err, tail, 0, tail.length, 0);
  ftruncateSync(err, tail.length);
};
try {
  const child = spawn(reader, [command, input, ...extra], { stdio: ['ignore', out, 'pipe'] });
  child.stderr!.on('data', record);
  await new Promise<void>((resolve, reject) => {
    child.once('error', error => { record(Buffer.from(String(error))); reject(error); });
    child.once('close', (code, signal) => {
      process.exitCode = signal || code === null ? 1 : code;
      resolve();
    });
  });
} finally {
  closeSync(out);
  closeSync(err);
}
