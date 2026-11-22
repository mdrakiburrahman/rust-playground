use std::{fs::File, path::Path};

use anyhow::{Context, Result, bail};
use parquet::{
    basic::Compression,
    file::{
        metadata::{PageIndexPolicy, ParquetMetaDataReader},
        page_index::column_index::ColumnIndexMetaData,
        reader::{FileReader, SerializedFileReader},
    },
};

fn inspect(path: &Path) -> Result<()> {
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let metadata = ParquetMetaDataReader::new()
        .with_page_index_policy(PageIndexPolicy::Required)
        .parse_and_finish(&file)
        .context("reading footer and page indexes")?;
    let column_indexes = metadata.column_index().context("missing column indexes")?;
    let offset_indexes = metadata.offset_index().context("missing offset indexes")?;
    let reader = SerializedFileReader::new(file).context("opening page reader")?;
    if metadata.num_row_groups() == 0 {
        bail!("file has no row groups");
    }
    for (group_index, group) in metadata.row_groups().iter().enumerate() {
        if group.num_columns() == 0 {
            bail!("row group has no column chunks");
        }
        let columns = column_indexes
            .get(group_index)
            .context("missing row-group column index")?;
        let offsets = offset_indexes
            .get(group_index)
            .context("missing row-group offset index")?;
        if columns.len() != group.num_columns() || offsets.len() != group.num_columns() {
            bail!("page index column count differs");
        }
        let group_reader = reader.get_row_group(group_index)?;
        for (column_index, column) in group.columns().iter().enumerate() {
            if column.compression() != Compression::UNCOMPRESSED {
                bail!("row group {group_index} column {column_index} is compressed");
            }
            if column.dictionary_page_offset().is_none() {
                bail!("row group {group_index} column {column_index} has no dictionary page");
            }
            let index = &columns[column_index];
            if matches!(index, ColumnIndexMetaData::NONE)
                || index.num_pages() == 0
                || index.num_pages() as usize != offsets[column_index].page_locations().len()
            {
                bail!("row group {group_index} column {column_index} lacks page statistics");
            }
            let mut pages = group_reader.get_column_page_reader(column_index)?;
            if !matches!(
                pages.get_next_page()?,
                Some(parquet::column::page::Page::DictionaryPage { .. })
            ) {
                bail!("row group {group_index} column {column_index} dictionary page is invalid");
            }
        }
    }
    Ok(())
}

fn main() -> Result<()> {
    let mut args = std::env::args_os().skip(1);
    let path = args.next().context("expected one Parquet file path")?;
    if args.next().is_some() {
        bail!("expected one Parquet file path");
    }
    inspect(Path::new(&path))
}

#[cfg(test)]
mod tests {
    use std::{fs, sync::Arc};

    use arrow::{
        array::{Int32Array, RecordBatch},
        datatypes::{DataType, Field, Schema},
    };
    use parquet::{
        arrow::ArrowWriter,
        file::properties::{EnabledStatistics, WriterProperties},
    };

    use super::*;

    fn fixture(compression: Compression, dictionary: bool, statistics: EnabledStatistics) -> std::path::PathBuf {
        let path = std::env::temp_dir().join(format!("parity-footer-{}.parquet", uuid::Uuid::new_v4()));
        let schema = Arc::new(Schema::new(vec![Field::new("value", DataType::Int32, false)]));
        let batch = RecordBatch::try_new(schema.clone(), vec![Arc::new(Int32Array::from(vec![1, 2, 1, 2]))]).unwrap();
        let props = WriterProperties::builder()
            .set_compression(compression)
            .set_dictionary_enabled(dictionary)
            .set_statistics_enabled(statistics)
            .build();
        let mut writer = ArrowWriter::try_new(File::create(&path).unwrap(), schema, Some(props)).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        path
    }

    #[test]
    fn requires_dictionary_and_page_statistics() {
        for (compression, dictionary, statistics, valid) in [
            (Compression::UNCOMPRESSED, true, EnabledStatistics::Page, true),
            (
                Compression::ZSTD(Default::default()),
                true,
                EnabledStatistics::Page,
                false,
            ),
            (Compression::UNCOMPRESSED, false, EnabledStatistics::Page, false),
            (Compression::UNCOMPRESSED, true, EnabledStatistics::Chunk, false),
            (Compression::UNCOMPRESSED, true, EnabledStatistics::None, false),
        ] {
            let path = fixture(compression, dictionary, statistics);
            assert_eq!(inspect(&path).is_ok(), valid);
            fs::remove_file(path).unwrap();
        }
    }
}
