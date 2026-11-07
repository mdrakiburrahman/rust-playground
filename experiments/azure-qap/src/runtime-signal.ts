// tsx is PID 1 in the published slim image; the signal handlers live in its Node child.
export function runtimeSignalCommand(signal: 'SIGTERM' | 'SIGUSR1', rootPid = 1,
  entryPoint = 'experiments/azure-qap/src/job.ts'): string[] {
  return ['node', '-e', String.raw`
const fs = require('node:fs');
const path = require('node:path');
const [signal, root, entry] = process.argv.slice(1);
if (!['SIGTERM', 'SIGUSR1'].includes(signal) || !/^[1-9]\d*$/.test(root))
  throw new Error('Invalid runtime signal.');
const read = (pid, file) => fs.readFileSync('/proc/' + pid + '/' + file, 'utf8');
const startTime = pid => read(pid, 'stat').split(') ').at(-1).split(' ')[19];
const children = read(root, 'task/' + root + '/children').trim().split(/\s+/).filter(Boolean);
const candidates = [root, ...children].flatMap(pid => {
  try {
    const identity = startTime(pid);
    const cmdline = read(pid, 'cmdline');
    const args = cmdline.split('\0').filter(Boolean);
    return path.basename(fs.readlinkSync('/proc/' + pid + '/exe')) === 'node' &&
      args.includes(entry) && !args.some(arg => /(?:^|\/)(?:\.bin\/tsx|tsx\/dist\/cli\.mjs)$/.test(arg))
      ? [{ pid, identity, cmdline }] : [];
  } catch { return []; }
});
if (candidates.length !== 1) throw new Error('Expected exactly one owned QAP Node runtime; refusing to signal.');
const { pid, identity, cmdline } = candidates[0];
if (identity !== startTime(pid) || cmdline !== read(pid, 'cmdline'))
  throw new Error('Runtime identity changed; refusing to signal.');
process.kill(Number(pid), signal);
`, signal, String(rootPid), entryPoint];
}
