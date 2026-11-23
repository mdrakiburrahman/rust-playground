import { request } from 'node:http';

export function localDockerSocket(endpoint: string): string {
  if (endpoint.startsWith('unix:///')) return endpoint.slice(7);
  if (/^npipe:\/{4}\.\/pipe\/[a-zA-Z0-9._-]+$/.test(endpoint)) return endpoint.slice(8).replaceAll('/', '\\');
  throw new Error('Native catalogues require a local Docker socket (Unix or Windows named pipe); remote Docker provisioning is not supported');
}
export function decodeDockerOutput(bytes: Buffer): { stdout: string; stderr: string } {
  const stdout: Buffer[] = [];
  const stderr: Buffer[] = [];
  for (let offset = 0; offset < bytes.length;) {
    if (offset + 8 > bytes.length) throw new Error('Truncated Docker stream header');
    const stream = bytes[offset];
    const length = bytes.readUInt32BE(offset + 4);
    if (![1, 2].includes(stream) || offset + 8 + length > bytes.length) throw new Error('Invalid Docker stream frame');
    (stream === 1 ? stdout : stderr).push(bytes.subarray(offset + 8, offset + 8 + length));
    offset += 8 + length;
  }
  return { stdout: Buffer.concat(stdout).toString('utf8'), stderr: Buffer.concat(stderr).toString('utf8') };
}
async function dockerRequest(socketPath: string, method: string, path: string, body?: unknown): Promise<Buffer> {
  const payload = body === undefined ? undefined : JSON.stringify(body);
  return new Promise((resolve, reject) => {
    const req = request({ socketPath, path, method, headers: payload
      ? { 'Content-Type': 'application/json', 'Content-Length': Buffer.byteLength(payload) } : {} }, (response) => {
      const chunks: Buffer[] = [];
      let size = 0;
      response.on('data', (chunk: Buffer) => {
        size += chunk.length;
        if (size > 16 * 1024 * 1024) req.destroy(new Error('Docker output exceeded 16 MiB'));
        else chunks.push(chunk);
      });
      response.on('error', reject);
      response.on('end', () => {
        const bytes = Buffer.concat(chunks);
        if (response.statusCode !== 200 && response.statusCode !== 201) {
          reject(new Error(`Docker API HTTP ${response.statusCode}: ${bytes.toString().slice(-8000)}`));
        } else resolve(bytes);
      });
    });
    req.on('error', reject);
    req.setTimeout(60_000, () => req.destroy(new Error('Docker exec timed out; its outcome may be unknown')));
    req.end(payload);
  });
}
export async function dockerExec(endpoint: string, container: string, command: string[]): Promise<string> {
  const socket = localDockerSocket(endpoint);
  const exec: { Id: string } = JSON.parse((await dockerRequest(socket, 'POST',
    `/containers/${encodeURIComponent(container)}/exec`,
    { AttachStdout: true, AttachStderr: true, AttachStdin: false, Tty: false, Cmd: command })).toString());
  const bytes = await dockerRequest(socket, 'POST', `/exec/${encodeURIComponent(exec.Id)}/start`, { Detach: false, Tty: false });
  const output = decodeDockerOutput(bytes);
  const result: { Running: boolean; ExitCode: number } =
    JSON.parse((await dockerRequest(socket, 'GET', `/exec/${encodeURIComponent(exec.Id)}/json`)).toString());
  if (result.Running) throw new Error('Docker exec stream ended before the command completed; outcome unknown');
  if (result.ExitCode !== 0) throw new Error(`Native command exited ${result.ExitCode}: ${(output.stderr || output.stdout).slice(-8000)}`);
  return output.stdout;
}
