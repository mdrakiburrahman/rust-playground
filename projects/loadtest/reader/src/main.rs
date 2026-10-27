use std::{collections::BTreeMap, path::Path};

use anyhow::{Context, Result, bail};
use serde_json::json;

#[tokio::main]
async fn main() -> Result<()> {
    let args = std::env::args().skip(1).collect::<Vec<_>>();
    if args.len() == 1 {
        println!(
            "{}",
            serde_json::to_string(&delta_inspect::inspect(Path::new(&args[0])).await?)?
        );
        return Ok(());
    }
    if args.len() < 2 || !matches!(args[0].as_str(), "summarize" | "evidence" | "evidence-chunks") {
        bail!(
            "usage: delta-inspect <local directory> | summarize|evidence|evidence-chunks <root> [--table catalog.schema.table ...]"
        );
    }
    let root = &args[1];
    let mut identities = Vec::new();
    for pair in args[2..].chunks(2) {
        if pair.len() != 2 || pair[0] != "--table" {
            bail!("expected --table catalog.schema.table");
        }
        identities.push(pair[1].clone());
    }
    let cloud = root.starts_with("abfss://");
    let snapshot = if cloud {
        delta_inspect::summarize_cloud(root, &identities).await?
    } else {
        delta_inspect::summarize(Path::new(root)).await?
    };
    if args[0] == "summarize" {
        println!("{}", serde_json::to_string(&snapshot)?);
        return Ok(());
    }
    if args[0] == "evidence-chunks" {
        let mut corpus = BTreeMap::new();
        for (identity, summary) in &snapshot.tables {
            if !identities.is_empty() && !identities.contains(identity) {
                continue;
            }
            let chunks = if cloud {
                delta_inspect::cloud_sample_corpus(root, identity, summary).await?
            } else {
                delta_inspect::sample_corpus(Path::new(root), identity, summary)?
            };
            chunks.verify()?;
            corpus.insert(identity.clone(), chunks);
        }
        for identity in &identities {
            corpus.get(identity).context("requested evidence table is absent")?;
        }
        println!("{}", json!({"snapshot": snapshot, "corpus": corpus}));
        return Ok(());
    }
    let mut corpus = BTreeMap::new();
    for (identity, summary) in &snapshot.tables {
        if !identities.is_empty() && !identities.contains(identity) {
            continue;
        }
        let rows = if cloud {
            delta_inspect::cloud_sample_rows(root, identity, summary).await?
        } else {
            delta_inspect::sample_rows(Path::new(root), identity, summary)?
        };
        corpus.insert(identity.clone(), rows);
    }
    for identity in &identities {
        corpus.get(identity).context("requested evidence table is absent")?;
    }
    println!("{}", json!({"snapshot": snapshot, "corpus": corpus}));
    Ok(())
}
