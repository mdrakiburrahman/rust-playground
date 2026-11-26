import assert from 'node:assert/strict';
import test from 'node:test';
import { validateWriterGapTunables } from './writer-tunables.js';

const cdc = { min_chunk_size: 262_144, max_chunk_size: 1_048_576, norm_level: 0 };

test('writer gap knobs accept every documented value and leave omission alone', () => {
  validateWriterGapTunables({});
  validateWriterGapTunables({
    parquet_writer: {
      bloom_filter_position: 'end', skip_arrow_metadata: true, coerce_types: false, content_defined_chunking: cdc,
      max_row_group_rows: 'unlimited', max_row_group_bytes: 'unlimited', column_index_truncate_length: 'unlimited', statistics_truncate_length: 64,
    },
    maintenance: { parquet_writer: { bloom_filter_position: 'after_row_group', max_row_group_rows: 'unlimited' } },
    collector: { commit: { delta_stats_num_indexed_cols: -1 }, runtime: { parquet_column_workers: 1 } },
  });
  validateWriterGapTunables({ collector: { commit: { delta_stats_num_indexed_cols: 2_147_483_647 } } });
});

test('writer gap knobs reject bad enums, types, ranges, nulls and illegal combinations', () => {
  const rejects: Array<[unknown, RegExp]> = [
    [{ parquet_writer: { bloom_filter_position: 'End' } }, /bloom_filter_position must be after_row_group or end/],
    [{ parquet_writer: { bloom_filter_position: null } }, /bloom_filter_position/],
    [{ parquet_writer: { skip_arrow_metadata: 'true' } }, /skip_arrow_metadata must be a boolean/],
    [{ maintenance: { parquet_writer: { coerce_types: null } } }, /maintenance\.parquet_writer\.coerce_types must be a boolean/],
    [{ parquet_writer: { max_row_group_bytes: 'Unlimited' } }, /max_row_group_bytes must be a positive integer or "unlimited"/],
    [{ parquet_writer: { statistics_truncate_length: 0 } }, /statistics_truncate_length/],
    [{ parquet_writer: { max_row_group_rows: null } }, /max_row_group_rows must be a positive integer or "unlimited"; omit it/],
    [{ maintenance: { parquet_writer: { statistics_truncate_length: null } } }, /maintenance\.parquet_writer\.statistics_truncate_length must be/],
    [{ parquet_writer: { content_defined_chunking: null } }, /content_defined_chunking must be an object/],
    [{ parquet_writer: { content_defined_chunking: { ...cdc, extra: 1 } } }, /content_defined_chunking\.extra is not a supported field/],
    [{ parquet_writer: { content_defined_chunking: { min_chunk_size: 1, max_chunk_size: 2 } } }, /norm_level is required/],
    [{ parquet_writer: { content_defined_chunking: { ...cdc, min_chunk_size: 0 } } }, /min_chunk_size must be a positive integer/],
    [{ parquet_writer: { content_defined_chunking: { ...cdc, max_chunk_size: cdc.min_chunk_size } } }, /greater than min_chunk_size/],
    [{ parquet_writer: { content_defined_chunking: { ...cdc, norm_level: 15 } } }, /leaves 0 CDC mask bits/],
    [{ parquet_writer: { content_defined_chunking: cdc }, collector: { runtime: { parquet_column_workers: 4 } } }, /requires collector\.runtime\.parquet_column_workers=1/],
    [{ parquet_writer: { content_defined_chunking: cdc }, collector: { runtime: { parquet_encode_mode: 'column_pipeline' } } }, /unsupported with collector\.runtime\.parquet_encode_mode=column_pipeline/],
    [{ collector: { commit: { delta_stats_num_indexed_cols: 0 } } }, /must be -1 \(all columns\)/],
    [{ collector: { commit: { delta_stats_num_indexed_cols: 1.5 } } }, /delta_stats_num_indexed_cols/],
    [{ collector: { commit: { delta_stats_num_indexed_cols: 'all' } } }, /delta_stats_num_indexed_cols/],
    [{ collector: { commit: { delta_stats_num_indexed_cols: null } } }, /delta_stats_num_indexed_cols/],
    [{ collector: { commit: { delta_stats_num_indexed_cols: 2, delta_stats_columns: ['Time'] } } }, /mutually exclusive/],
  ];
  for (const [tunables, message] of rejects) {
    assert.throws(() => validateWriterGapTunables(tunables), message, JSON.stringify(tunables));
  }
});
