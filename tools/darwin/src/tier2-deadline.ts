import { lstat, open, readFile } from "node:fs/promises";

const DAY_MS = 24 * 60 * 60 * 1000;

export interface OriginalDeadline {
  version: 1;
  bindingSha256: string;
  startedAtUnixMs: number;
  deadlineUnixMs: number;
}

function assertDeadline(value: unknown, bindingSha256: string, now: number): asserts value is OriginalDeadline {
  if (!value || typeof value !== "object" || Array.isArray(value)) throw new Error("invalid original deadline receipt");
  const receipt = value as Record<string, unknown>;
  if (!/^[a-f0-9]{64}$/.test(bindingSha256) || receipt.version !== 1 || receipt.bindingSha256 !== bindingSha256
    || !Number.isSafeInteger(receipt.startedAtUnixMs) || !Number.isSafeInteger(receipt.deadlineUnixMs)
    || Number(receipt.deadlineUnixMs) - Number(receipt.startedAtUnixMs) !== DAY_MS
    || !Number.isSafeInteger(now) || now < Number(receipt.startedAtUnixMs) || now >= Number(receipt.deadlineUnixMs)) {
    throw new Error("original deadline is expired, inconsistent, or bound to another series");
  }
}

export async function createOriginalDeadline(path: string, bindingSha256: string, now = Date.now()): Promise<OriginalDeadline> {
  const receipt: OriginalDeadline = { version: 1, bindingSha256, startedAtUnixMs: now, deadlineUnixMs: now + DAY_MS };
  assertDeadline(receipt, bindingSha256, now);
  const file = await open(path, "wx", 0o400);
  try {
    await file.writeFile(`${JSON.stringify(receipt)}\n`);
    await file.sync();
  } finally { await file.close(); }
  return receipt;
}

export async function readOriginalDeadline(path: string, bindingSha256: string, now = Date.now()): Promise<OriginalDeadline> {
  const stat = await lstat(path);
  if (!stat.isFile() || stat.isSymbolicLink() || stat.size > 4096) throw new Error("invalid original deadline receipt");
  const receipt: unknown = JSON.parse(await readFile(path, "utf8"));
  assertDeadline(receipt, bindingSha256, now);
  return receipt;
}
