use std::collections::{BTreeMap, BTreeSet, HashMap};

use anyhow::{Context, Result, bail};
use deltalake_core::logstore::object_store::ObjectStoreExt;
use parquet::file::reader::{FileReader, SerializedFileReader};
use sha2::{Digest, Sha256};

use crate::{
    FileSummary, SnapshotSummary, TableSummary, payload_digest, row_value, validate_partition_columns, with_partitions,
};

async fn open_cloud_table(root: &str, identity: &str) -> Result<deltalake_core::DeltaTable> {
    let url = url::Url::parse(root)?;
    if url.scheme() != "abfss"
        || url.query().is_some()
        || url.fragment().is_some()
        || url.password().is_some()
        || url.username().is_empty()
        || !url
            .host_str()
            .is_some_and(|host| host.ends_with(".dfs.core.windows.net"))
    {
        bail!("cloud oracle requires a credential-free ADLS abfss root");
    }
    if identity
        .split('.')
        .any(|part| part.is_empty() || !part.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_'))
    {
        bail!("invalid cloud table identity");
    }
    deltalake_azure::register_handlers(None);
    let url = url::Url::parse(&format!(
        "{}/{}",
        root.trim_end_matches('/'),
        identity.replace('.', "/")
    ))?;
    deltalake_core::DeltaTableBuilder::from_url(url)?
        .with_storage_options(HashMap::from([("azure_use_azure_cli".to_owned(), "true".to_owned())]))
        .load()
        .await
        .context("open independent cloud Delta snapshot")
}

/// Complete bounded corpus from precisely the captured files, not a new scan.
///
/// # Errors
///
/// Rejects a changed snapshot/corpus, unavailable captured files, or more than
/// 256 distinct rows.
pub async fn cloud_sample_rows(
    root: &str,
    identity: &str,
    summary: &TableSummary,
) -> Result<Vec<(serde_json::Value, u64)>> {
    let delta = open_cloud_table(root, identity).await?;
    if delta.snapshot()?.version() != summary.version {
        bail!("cloud snapshot changed after capture");
    }
    let mut corpus = BTreeMap::<String, (serde_json::Value, u64)>::new();
    let mut hashes = BTreeMap::<String, u64>::new();
    for file in &summary.files {
        validate_active_path(&file.path)?;
        let path = deltalake_core::Path::parse(&file.path)?;
        let data = delta.object_store().get(&path).await?.bytes().await?;
        let reader = SerializedFileReader::new(data)?;
        for row in reader.get_row_iter(None)? {
            let value = with_partitions(row_value(&row?), &summary.schema, &file.partition_values)?;
            let encoded = serde_json::to_string(&value)?;
            *hashes
                .entry(format!("sha256:{}", hex::encode(Sha256::digest(encoded.as_bytes()))))
                .or_default() += 1;
            corpus.entry(encoded).or_insert((value, 0)).1 += 1;
            if corpus.len() > 256 {
                bail!("validation corpus exceeds 256 distinct rows");
            }
        }
    }
    if hashes != summary.row_hashes || corpus.values().map(|(_, rows)| rows).sum::<u64>() != summary.rows {
        bail!("captured cloud validation corpus changed");
    }
    Ok(corpus.into_values().collect())
}

/// Summarizes an explicit table allowlist from ADLS using independent delta-rs
/// snapshots. Never discovers data by listing Parquet blobs.
///
/// # Errors
///
/// Rejects invalid roots/identities, unavailable credentials, unsupported
/// deletion vectors, unsafe active paths, and corrupt/missing committed files.
pub async fn summarize_cloud(root: &str, identities: &[String]) -> Result<SnapshotSummary> {
    let url = url::Url::parse(root)?;
    if url.scheme() != "abfss" || url.query().is_some() || url.fragment().is_some() || url.password().is_some() {
        bail!("cloud oracle requires a credential-free abfss root");
    }
    if identities.is_empty() || identities.iter().collect::<BTreeSet<_>>().len() != identities.len() {
        bail!("cloud oracle requires a nonempty unique explicit table allowlist");
    }
    let mut tables = BTreeMap::new();
    let mut total_files = 0;
    for identity in identities {
        if identity
            .split('.')
            .any(|part| part.is_empty() || !part.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_'))
        {
            bail!("invalid cloud table identity");
        }
        let delta = open_cloud_table(root, identity).await?;
        let snapshot = delta.snapshot()?;
        let schema = serde_json::to_value(snapshot.schema())?;
        let partition_columns = snapshot.metadata().partition_columns().to_vec();
        let mut files = Vec::new();
        let mut rows = 0_u64;
        let mut bytes = 0_u64;
        let mut row_hashes = BTreeMap::<String, u64>::new();
        let mut payload_row_hashes = BTreeMap::<String, u64>::new();
        for file in snapshot.log_data().iter() {
            if file.deletion_vector_descriptor().is_some() {
                bail!("independent cloud oracle does not support deletion vectors");
            }
            let path = file.path().into_owned();
            validate_active_path(&path)?;
            let partition_values = file.partition_values_map().into_iter().collect();
            validate_partition_columns(&partition_values, &partition_columns)?;
            if file.size() > 1_073_741_824 {
                bail!("cloud oracle file exceeds the QAP 1GB cache limit");
            }
            let data = delta
                .object_store()
                .get(&file.object_store_path())
                .await?
                .bytes()
                .await?;
            let file_bytes = u64::try_from(data.len()).context("file byte count overflow")?;
            let reader = SerializedFileReader::new(data)?;
            let file_rows =
                u64::try_from(reader.metadata().file_metadata().num_rows()).context("negative row count")?;
            for row in reader.get_row_iter(None)? {
                let value = with_partitions(row_value(&row?), &schema, &partition_values)?;
                *payload_row_hashes
                    .entry(payload_digest(value.clone(), &partition_columns)?)
                    .or_default() += 1;
                let hash = format!("sha256:{}", hex::encode(Sha256::digest(serde_json::to_vec(&value)?)));
                *row_hashes.entry(hash).or_default() += 1;
            }
            files.push(FileSummary {
                path,
                bytes: file_bytes,
                rows: file_rows,
                partition_values,
            });
            rows = rows.checked_add(file_rows).context("cloud row count overflow")?;
            bytes = bytes.checked_add(file_bytes).context("cloud byte count overflow")?;
        }
        files.sort_by(|left, right| left.path.cmp(&right.path));
        total_files += files.len();
        tables.insert(
            identity.clone(),
            TableSummary {
                schema,
                version: snapshot.version(),
                active_files: files.len(),
                rows,
                bytes,
                minimum_file_bytes: files.iter().map(|file| file.bytes).min().unwrap_or(0),
                maximum_file_bytes: files.iter().map(|file| file.bytes).max().unwrap_or(0),
                row_hashes,
                payload_row_hashes,
                files,
                partition_columns,
            },
        );
    }
    Ok(SnapshotSummary {
        files: total_files,
        tables,
    })
}

fn validate_active_path(path: &str) -> Result<()> {
    if path.is_empty()
        || path.starts_with('/')
        || path.contains(['\\', ':'])
        || path
            .split('/')
            .any(|part| part.is_empty() || part == "." || part == "..")
    {
        bail!("cloud active file escapes the table root");
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cloud_active_paths_cannot_escape_the_snapshot_root() -> Result<()> {
        validate_active_path("__arrowcol_ingestion=2026-10-27-02-00/part.parquet")?;
        for path in [
            "",
            "/other/file",
            "../file",
            "part/../../file",
            "https://other/file",
            "part\\file",
        ] {
            assert!(validate_active_path(path).is_err(), "{path}");
        }
        Ok(())
    }

    #[tokio::test]
    async fn cloud_oracle_requires_explicit_safe_inputs_before_authentication() {
        assert!(summarize_cloud("file:///tmp", &["table".to_owned()]).await.is_err());
        assert!(
            summarize_cloud("abfss://container@account.dfs.core.windows.net/root", &[])
                .await
                .is_err()
        );
        assert!(
            summarize_cloud(
                "abfss://container@account.dfs.core.windows.net/root",
                &["../escape".to_owned()]
            )
            .await
            .is_err()
        );
    }
}
