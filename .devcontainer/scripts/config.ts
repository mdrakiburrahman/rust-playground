import { readFileSync } from 'node:fs';
import { join } from 'node:path';

export const imageRepository =
  'ghcr.io/mdrakiburrahman/rust-playground/devcontainer';
export const targetPlatform = 'linux/amd64';
export const contentHashFile = '.devcontainer/content-hash.txt';
export const composeFile = '.devcontainer/docker-compose.yml';
export const imageBuildConfigFile = '.devcontainer/devcontainer.build.json';

const contentHashPattern = /^[a-f0-9]{64}$/u;

export function readContentHash(repositoryRoot: string): string {
  const hash = readFileSync(join(repositoryRoot, contentHashFile), 'utf8').trim();
  if (!contentHashPattern.test(hash)) {
    throw new Error(
      `${contentHashFile} must contain exactly one lowercase SHA-256 hash.`,
    );
  }
  return hash;
}

export function immutableImage(hash: string): string {
  if (!contentHashPattern.test(hash)) {
    throw new Error(`Invalid devcontainer content hash: ${hash}`);
  }
  return `${imageRepository}:${hash}`;
}
