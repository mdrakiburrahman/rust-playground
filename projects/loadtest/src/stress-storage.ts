import { lstat, readdir } from 'node:fs/promises';
import path from 'node:path';

export interface PhysicalInventory {
  fileBytes: number;
  allocatedBytes: number;
  files: number;
  byKind: Record<string, { bytes: number; allocatedBytes: number; files: number }>;
  startedAt: number;
  finishedAt: number;
}

function kind(relative: string): string {
  if (/(?:^|\/)(?:staging|tmp|temp)(?:\/|$)|\.(?:tmp|partial)$/u.test(relative)) return 'staging';
  if (relative.split('/').includes('_delta_log')) {
    if (relative.includes('.checkpoint.') || relative.endsWith('/_last_checkpoint')) return 'delta-checkpoints';
    return relative.endsWith('.json') ? 'delta-log-json' : 'delta-log-other';
  }
  if (/(?:quiver|wal)/iu.test(relative)) return 'wal';
  if (/checkpoint/iu.test(relative)) return 'tenant-checkpoints';
  return relative.endsWith('.parquet') ? 'data-parquet' : 'other';
}

/** Counts all files, including uncommitted/stale files, not just active Delta Add actions. */
export async function physicalInventory(roots: Record<string, string>, deadline: number): Promise<PhysicalInventory> {
  if (!Object.keys(roots).length) throw new Error('No physical storage roots were declared');
  const result: PhysicalInventory = {
    fileBytes: 0, allocatedBytes: 0, files: 0, byKind: {}, startedAt: Date.now(), finishedAt: 0,
  };
  const seen = new Set<string>();
  const walk = async (directory: string, relative: string): Promise<void> => {
    if (Date.now() >= deadline) throw new Error('Complete physical inventory exceeded its predeclared deadline');
    const stat = await lstat(directory);
    if (stat.isSymbolicLink()) throw new Error('Physical inventory refuses symlink traversal');
    if (stat.isDirectory()) {
      for (const entry of (await readdir(directory)).sort()) await walk(path.join(directory, entry), `${relative}/${entry}`);
      return;
    }
    if (!stat.isFile() || !Number.isSafeInteger(stat.size) || stat.size < 0 ||
        !Number.isSafeInteger(stat.blocks) || stat.blocks < 0 || !Number.isSafeInteger(stat.ino) || !Number.isSafeInteger(stat.dev)) {
      throw new Error('Unsupported or imprecise physical inventory entry');
    }
    const inode = `${stat.dev}:${stat.ino}`;
    if (seen.has(inode)) return;
    seen.add(inode);
    if (++result.files > 1_000_000) throw new Error('Physical inventory exceeded its file guard; nothing was truncated');
    const category = result.byKind[kind(relative)] ??= { bytes: 0, allocatedBytes: 0, files: 0 };
    category.bytes += stat.size; category.allocatedBytes += stat.blocks * 512; category.files++;
    result.fileBytes += stat.size; result.allocatedBytes += stat.blocks * 512;
    if (!Number.isSafeInteger(result.fileBytes) || !Number.isSafeInteger(result.allocatedBytes)) {
      throw new Error('Physical byte counters exceed exact JavaScript integer range');
    }
  };
  for (const [label, root] of Object.entries(roots)) {
    if (!/^[a-z0-9-]+$/u.test(label)) throw new Error('Invalid public physical-root label');
    await walk(root, label);
  }
  result.finishedAt = Date.now();
  return result;
}
