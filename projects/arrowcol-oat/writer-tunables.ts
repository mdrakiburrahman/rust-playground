// Strict pre-flight for the Parquet writer and Delta statistics knobs added after PR #134.
// The pinned Rust parser (`arrowcol-oat-rust --validate-config`) remains authoritative;
// this rejects the same bad inputs before any build, image or run is prepared.

type JsonObject = Record<string, unknown>;

const limitFields = ['max_row_group_rows', 'max_row_group_bytes', 'column_index_truncate_length', 'statistics_truncate_length'] as const;
const cdcFields = ['min_chunk_size', 'max_chunk_size', 'norm_level'] as const;
const i32Max = 2_147_483_647;

function isRecord(value: unknown): value is JsonObject {
  return value !== null && typeof value === 'object' && !Array.isArray(value);
}

function present(object: JsonObject, field: string): boolean {
  return Object.hasOwn(object, field) && object[field] !== undefined;
}

function validateChunking(value: unknown, path: string): void {
  if (!isRecord(value)) throw new Error(`${path} must be an object with min_chunk_size, max_chunk_size and norm_level`);
  for (const field of Object.keys(value)) {
    if (!(cdcFields as readonly string[]).includes(field)) throw new Error(`${path}.${field} is not a supported field`);
  }
  for (const field of cdcFields) {
    if (!present(value, field)) throw new Error(`${path}.${field} is required`);
  }
  const { min_chunk_size: min, max_chunk_size: max, norm_level: norm } = value;
  if (!Number.isSafeInteger(min) || (min as number) <= 0) throw new Error(`${path}.min_chunk_size must be a positive integer`);
  if (!Number.isSafeInteger(max) || (max as number) <= (min as number)) {
    throw new Error(`${path}.max_chunk_size must be an integer greater than min_chunk_size`);
  }
  if (!Number.isInteger(norm) || (norm as number) < -i32Max - 1 || (norm as number) > i32Max) {
    throw new Error(`${path}.norm_level must be a 32-bit integer`);
  }
  // Mirrors the upstream chunker mask derivation enforced by the Rust writer.
  const low = BigInt(min as number);
  const target = ((low + BigInt(max as number)) / 2n - low) / 8n;
  const maskBits = target > 0n ? target.toString(2).length - 1 : 0;
  const effective = maskBits - (norm as number);
  if (effective < 1 || effective > 63) {
    throw new Error(`${path}.norm_level=${String(norm)} leaves ${effective} CDC mask bits; norm_level must be between ${maskBits - 63} and ${maskBits - 1}`);
  }
}

function validateWriter(writer: unknown, path: string): boolean {
  if (!isRecord(writer)) return false;
  if (present(writer, 'bloom_filter_position') && writer.bloom_filter_position !== 'after_row_group' && writer.bloom_filter_position !== 'end') {
    throw new Error(`${path}.bloom_filter_position must be after_row_group or end`);
  }
  for (const field of ['skip_arrow_metadata', 'coerce_types']) {
    if (present(writer, field) && typeof writer[field] !== 'boolean') throw new Error(`${path}.${field} must be a boolean`);
  }
  for (const field of limitFields) {
    if (!present(writer, field)) continue;
    const value = writer[field];
    // null would be ambiguous between "unlimited" (upstream None) and omission (today's default).
    if (value === null) throw new Error(`${path}.${field} must be a positive integer or "unlimited"; omit it to keep the default`);
    if (value === 'unlimited') continue;
    if (!Number.isSafeInteger(value) || (value as number) <= 0) throw new Error(`${path}.${field} must be a positive integer or "unlimited"`);
  }
  if (!present(writer, 'content_defined_chunking')) return false;
  validateChunking(writer.content_defined_chunking, `${path}.content_defined_chunking`);
  return true;
}

export function validateWriterGapTunables(tunables: unknown): void {
  if (!isRecord(tunables)) return;
  const collector = isRecord(tunables.collector) ? tunables.collector : {};
  const runtime = isRecord(collector.runtime) ? collector.runtime : {};
  if (validateWriter(tunables.parquet_writer, 'tunables.parquet_writer')) {
    if (runtime.parquet_encode_mode === 'column_pipeline') {
      throw new Error('tunables.parquet_writer.content_defined_chunking is unsupported with collector.runtime.parquet_encode_mode=column_pipeline');
    }
    if (typeof runtime.parquet_column_workers === 'number' && runtime.parquet_column_workers > 1) {
      throw new Error('tunables.parquet_writer.content_defined_chunking requires collector.runtime.parquet_column_workers=1 (the sequential writer)');
    }
  }
  if (isRecord(tunables.maintenance)) validateWriter(tunables.maintenance.parquet_writer, 'tunables.maintenance.parquet_writer');
  const commit = isRecord(collector.commit) ? collector.commit : {};
  if (!present(commit, 'delta_stats_num_indexed_cols')) return;
  const count = commit.delta_stats_num_indexed_cols;
  if (!Number.isInteger(count) || ((count as number) !== -1 && ((count as number) < 1 || (count as number) > i32Max))) {
    throw new Error('tunables.collector.commit.delta_stats_num_indexed_cols must be -1 (all columns) or an integer in 1..=2147483647; use delta_stats_columns: [] to disable statistics');
  }
  if (present(commit, 'delta_stats_columns')) {
    throw new Error('tunables.collector.commit.delta_stats_columns and delta_stats_num_indexed_cols are mutually exclusive');
  }
}
