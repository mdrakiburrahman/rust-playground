import { closeSync, fstatSync, lstatSync, openSync, readSync } from 'node:fs';
import { join } from 'node:path';
import { isCode, parseRequest, readRun, runDirectory } from './contract.js';

export function inspectRunDocument(workspace: string, id: string, document: string) {
  const run = readRun(workspace, id);
  const request = parseRequest(run.request);
  if (document === 'ddl' && request.mode === 'create') {
    return { available: true, name: 'definition.sql', text: request.ddl, truncated: false };
  }
  const documents: Record<string, string[]> = {
    schema: ['payload', 'contract', 'schema.proto'],
    options: ['payload', 'contract', 'options.proto'],
    manifest: ['payload', 'contract', 'manifest.json'],
    sdk: ['payload', 'sdk', 'sdk-manifest.json'],
    release: ['payload', 'release.json'],
    ...(run.registry.native ? { registration: ['registration-receipt.json'] } : {}),
    ...(request.codegen === 'protoc'
      ? { compiler: ['payload', 'compiler', request.protoEntry ?? 'schema.proto'] } : {}),
  };
  if (!Object.hasOwn(documents, document)) throw new Error('Unknown run document');
  const parts = documents[document];
  let path = runDirectory(workspace, id);
  try {
    for (const part of ['', ...parts]) {
      path = join(path, part);
      if (lstatSync(path).isSymbolicLink()) throw new Error('Run documents cannot be symbolic links');
    }
    const limit = 128 * 1024;
    const fd = openSync(path, 'r');
    try {
      if (!fstatSync(fd).isFile()) throw new Error('Run document must be a regular file');
      const buffer = Buffer.alloc(limit + 1);
      let bytes = 0;
      while (bytes < buffer.length) {
        const count = readSync(fd, buffer, bytes, buffer.length - bytes, bytes);
        if (!count) break;
        bytes += count;
      }
      return { available: true, name: parts.join('/'), text: buffer.subarray(0, Math.min(bytes, limit)).toString('utf8'), truncated: bytes > limit };
    } finally { closeSync(fd); }
  } catch (error) {
    if (isCode(error, 'ENOENT')) return { available: false, name: parts.join('/') };
    throw error;
  }
}
