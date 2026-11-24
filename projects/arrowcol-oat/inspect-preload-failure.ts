import { resolve } from 'node:path';
import { inspectReviewedPreloadFailure } from './local-failed-disposition.js';

async function main(): Promise<void> {
  if (process.argv.length !== 4) throw new Error('Usage: inspect-preload-failure.ts RECEIPT MARKER');
  const result = await inspectReviewedPreloadFailure(resolve(process.argv[2]!), resolve(process.argv[3]!));
  console.log(JSON.stringify(result));
}

main().catch(error => {
  console.error(error);
  process.exitCode = 1;
});
