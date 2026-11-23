import assert from 'node:assert/strict';
import { randomUUID } from 'node:crypto';
import { mkdtempSync, rmSync } from 'node:fs';
import { createServer } from 'node:http';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import test from 'node:test';
import { decodeDockerOutput, dockerExec } from '../src/docker-exec.js';

function frame(stream: number, text: string) {
  const body = Buffer.from(text);
  const header = Buffer.alloc(8);
  header[0] = stream; header.writeUInt32BE(body.length, 4);
  return Buffer.concat([header, body]);
}
test('Docker output keeps streams separate and rejects incomplete frames', () => {
  assert.deepEqual(decodeDockerOutput(Buffer.concat([frame(1, 'db\n'), frame(2, 'warning\n'), frame(1, 'table\n')])),
    { stdout: 'db\ntable\n', stderr: 'warning\n' });
  assert.throws(() => decodeDockerOutput(frame(1, 'db').subarray(0, 7)), /Truncated/);
  assert.throws(() => decodeDockerOutput(frame(1, 'db').subarray(0, 9)), /Invalid/);
});
test('Docker exec waits for complete output and checks actual exit state', async () => {
  const directory = mkdtempSync(join(tmpdir(), 'native-exec-'));
  const socket = process.platform === 'win32' ? `\\\\.\\pipe\\native-exec-${randomUUID()}` : join(directory, 'docker.sock');
  const endpoint = process.platform === 'win32' ? `npipe://${socket.replaceAll('\\', '/')}` : `unix://${socket}`;
  let running = false;
  let exitCode = 0;
  const server = createServer((request, response) => {
    request.resume();
    request.on('end', () => {
      if (request.url === '/containers/owned/exec') response.end(JSON.stringify({ Id: 'operation' }));
      else if (request.url === '/exec/operation/start') {
        response.write(frame(1, 'before\n').subarray(0, 5));
        setTimeout(() => {
          response.end(Buffer.concat([frame(1, 'before\n').subarray(5), frame(1, 'after\n'), frame(2, 'rejected')]));
        }, 20);
      } else response.end(JSON.stringify({ Running: running, ExitCode: exitCode }));
    });
  });
  await new Promise<void>((resolve) => server.listen(socket, resolve));
  try {
    assert.equal(await dockerExec(endpoint, 'owned', ['inspect']), 'before\nafter\n');
    exitCode = 1;
    await assert.rejects(() => dockerExec(endpoint, 'owned', ['inspect']), /exited 1: rejected/);
    exitCode = 0; running = true;
    await assert.rejects(() => dockerExec(endpoint, 'owned', ['inspect']), /outcome unknown/);
    await assert.rejects(() => dockerExec('tcp://remote:2375', 'owned', ['inspect']), /local Docker/);
    await assert.rejects(() => dockerExec('npipe:////remote/pipe/docker_engine', 'owned', ['inspect']), /local Docker/);
  } finally {
    await new Promise<void>((resolve, reject) => server.close((error) => error ? reject(error) : resolve()));
    rmSync(directory, { recursive: true, force: true });
  }
});
