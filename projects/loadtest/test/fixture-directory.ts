import { mkdirSync, mkdtempSync } from 'node:fs';
import path from 'node:path';
import { fileURLToPath } from 'node:url';

const root = fileURLToPath(new URL('../../../.temp/test-fixtures/', import.meta.url));

export function fixtureDirectory(prefix: string): string {
  mkdirSync(root, { recursive: true, mode: 0o700 });
  return mkdtempSync(path.join(root, prefix));
}
