#!/usr/bin/env node
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';

const launch = fileURLToPath(new URL('./cli.ts', import.meta.url));
const result = spawnSync(process.execPath, ['--import', import.meta.resolve('tsx'), launch, ...process.argv.slice(2)], {
  stdio: 'inherit',
});

if (result.error) {
  console.error(`Could not start ArrowCol OAT: ${result.error.message}`);
  process.exitCode = 1;
} else if (result.signal) {
  process.kill(process.pid, result.signal);
} else {
  process.exitCode = result.status ?? 1;
}
