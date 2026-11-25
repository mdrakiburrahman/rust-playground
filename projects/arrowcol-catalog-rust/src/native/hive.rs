use std::{
    collections::{BTreeMap, BTreeSet},
    net::{TcpStream, ToSocketAddrs},
    time::Duration,
};

use super::{Column, Definition, Observed, hms, normalized_location};
use anyhow::{Context, Result, ensure};
use hms::TThriftHiveMetastoreSyncClient;
use thrift::{
    protocol::{TBinaryInputProtocol, TBinaryOutputProtocol},
    transport::{ReadHalf, TIoChannel, TTcpChannel, WriteHalf},
};

type Client = hms::ThriftHiveMetastoreSyncClient<
    TBinaryInputProtocol<ReadHalf<TTcpChannel>>,
    TBinaryOutputProtocol<WriteHalf<TTcpChannel>>,
>;

fn connect(endpoint: &str) -> Result<Client> {
    let url = url::Url::parse(endpoint)?;
    let address = (
        url.host_str().context("missing Hive host")?,
        url.port().context("missing Hive port")?,
    )
        .to_socket_addrs()?
        .next()
        .context("Hive host did not resolve")?;
    let socket = TcpStream::connect_timeout(&address, Duration::from_secs(3))?;
    socket.set_read_timeout(Some(Duration::from_secs(10)))?;
    socket.set_write_timeout(Some(Duration::from_secs(10)))?;
    let (reader, writer) = TTcpChannel::with_stream(socket).split()?;
    Ok(Client::new(
        TBinaryInputProtocol::new(reader, true),
        TBinaryOutputProtocol::new(writer, true),
    ))
}

fn missing(error: &thrift::Error) -> bool {
    matches!(error, thrift::Error::User(error) if error.downcast_ref::<hms::NoSuchObjectException>().is_some())
}

fn native_type(value: &str) -> String {
    match value {
        "timestamp" => "timestamp with local time zone".to_owned(),
        "timestamp_ntz" => "timestamp".to_owned(),
        _ => value.to_owned(),
    }
}

fn read_sync(client: &mut Client, definition: &Definition) -> Result<Option<Observed>> {
    let request = hms::GetTableRequest {
        db_name: definition.database.clone(),
        tbl_name: definition.table.clone(),
        cat_name: Some(definition.catalog.clone()),
        processor_identifier: Some("arrowcol-contract-bridge".to_owned()),
        engine: Some("hive".to_owned()),
    };
    let table = match client.get_table_req(request.clone()) {
        Ok(result) => result.table,
        Err(error) if missing(&error) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let id = table
        .id
        .filter(|id| *id > 0)
        .context("Hive 4 table ID is required for replacement detection")?;
    ensure!(
        table.table_type.as_deref() == Some("EXTERNAL_TABLE")
            && table.partition_keys.as_ref().is_some_and(Vec::is_empty),
        "expected unpartitioned external Hive table"
    );
    let constraints = client
        .get_not_null_constraints(hms::NotNullConstraintsRequest {
            cat_name: definition.catalog.clone(),
            db_name: definition.database.clone(),
            tbl_name: definition.table.clone(),
            table_id: Some(id),
        })?
        .not_null_constraints;
    let mut required = BTreeSet::new();
    for constraint in constraints {
        ensure!(
            constraint.cat_name.as_deref() == Some(&definition.catalog)
                && constraint.table_db.as_deref() == Some(&definition.database)
                && constraint.table_name.as_deref() == Some(&definition.table)
                && constraint.enable_cstr == Some(true)
                && constraint.validate_cstr == Some(false)
                && constraint.rely_cstr == Some(true),
            "unexpected native NOT NULL constraint identity or enforcement flags"
        );
        ensure!(
            required.insert(constraint.column_name.context("constraint lacks column")?),
            "duplicate NOT NULL constraint"
        );
    }
    let sd = table.sd.as_ref().context("Hive table lacks storage descriptor")?;
    ensure!(
        sd.input_format.as_deref() == Some("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat")
            && sd.output_format.as_deref() == Some("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat")
            && sd
                .serde_info
                .as_ref()
                .and_then(|serde| serde.serialization_lib.as_deref())
                == Some("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe"),
        "Hive storage format changed"
    );
    ensure!(
        sd.bucket_cols.as_ref().is_some_and(Vec::is_empty)
            && sd.sort_cols.as_ref().is_some_and(Vec::is_empty)
            && sd.num_buckets == Some(-1)
            && sd.compressed == Some(false)
            && sd.parameters.as_ref().is_some_and(BTreeMap::is_empty)
            && sd
                .serde_info
                .as_ref()
                .and_then(|serde| serde.parameters.as_ref())
                .is_some_and(BTreeMap::is_empty),
        "Hive storage layout or SerDe options changed"
    );
    let columns = sd
        .cols
        .as_ref()
        .context("Hive table lacks columns")?
        .iter()
        .map(|column| {
            let name = column.name.clone().context("Hive column lacks name")?;
            let hive_type = column.type_.as_deref().context("Hive column lacks type")?;
            let data_type = match hive_type {
                "timestamp" => "timestamp_ntz",
                "timestamp with local time zone" | "timestamp with local time zone('UTC')" => "timestamp",
                value => value,
            }
            .to_owned();
            let nullable = !required.remove(&name);
            Ok(Column {
                name,
                data_type,
                nullable,
                comment: column.comment.clone().filter(|value| !value.is_empty()),
            })
        })
        .collect::<Result<Vec<_>>>()?;
    ensure!(required.is_empty(), "NOT NULL constraint refers to an unknown column");
    let mut properties = table.parameters.clone().context("Hive table lacks parameters")?;
    ensure!(
        properties.remove("EXTERNAL").as_deref() == Some("TRUE")
            && properties.remove("spark.sql.sources.provider").as_deref() == Some("delta"),
        "Hive Delta provider or external ownership changed"
    );
    let comment = properties.remove("comment").filter(|value| !value.is_empty());
    for key in [
        "transient_lastDdlTime",
        "numFiles",
        "numFilesErasureCoded",
        "totalSize",
        "COLUMN_STATS_ACCURATE",
        "numRows",
        "rawDataSize",
    ] {
        properties.remove(key);
    }
    // Constraints and Table are separate Thrift reads. Do not combine a changed
    // table with constraints read from another native revision.
    ensure!(
        client.get_table_req(request)?.table == table,
        "Hive table changed during metadata read-back"
    );
    Ok(Some(Observed {
        id: id.to_string(),
        definition: Definition {
            catalog: table.cat_name.context("Hive table lacks catalog")?,
            database: table.db_name.context("Hive table lacks database")?,
            table: table.table_name.context("Hive table lacks name")?,
            columns,
            location: normalized_location(sd.location.as_deref().context("Hive table lacks location")?)?,
            properties,
            comment,
        },
    }))
}

pub(super) async fn read(endpoint: String, definition: Definition) -> Result<Option<Observed>> {
    tokio::task::spawn_blocking(move || read_sync(&mut connect(&endpoint)?, &definition)).await?
}

pub(super) async fn probe(endpoint: String) -> Result<()> {
    tokio::task::spawn_blocking(move || {
        connect(&endpoint)?.get_database_req(hms::GetDatabaseRequest {
            name: Some("default".to_owned()),
            catalog_name: Some("hive".to_owned()),
        })?;
        Ok(())
    })
    .await?
}

pub(super) async fn inspect(
    endpoint: String,
    catalog: String,
    database: String,
    name: String,
) -> Result<serde_json::Value> {
    tokio::task::spawn_blocking(move || {
        let mut client = connect(&endpoint)?;
        let table = client
            .get_table_req(hms::GetTableRequest {
                db_name: database.clone(),
                tbl_name: name.clone(),
                cat_name: Some(catalog.clone()),
                processor_identifier: Some("arrowcol-native-inspector".to_owned()),
                engine: Some("hive".to_owned()),
            })?
            .table;
        let constraints = client
            .get_not_null_constraints(hms::NotNullConstraintsRequest {
                cat_name: catalog,
                db_name: database,
                tbl_name: name,
                table_id: table.id,
            })?
            .not_null_constraints;
        let required: BTreeSet<_> = constraints
            .iter()
            .filter_map(|constraint| constraint.column_name.as_deref())
            .collect();
        let sd = table.sd.as_ref().context("Hive table lacks storage descriptor")?;
        let columns = sd
            .cols
            .as_ref()
            .context("Hive table lacks columns")?
            .iter()
            .map(|column| {
                serde_json::json!({"name": column.name, "type": column.type_, "comment": column.comment,
                "nullable": !column.name.as_deref().is_some_and(|name| required.contains(name))})
            })
            .collect::<Vec<_>>();
        Ok(serde_json::json!({
            "catalog": table.cat_name, "database": table.db_name, "name": table.table_name,
            "table_id": table.id, "table_type": table.table_type, "columns": columns,
            "location": sd.location, "parameters": table.parameters,
        }))
    })
    .await?
}

pub(super) async fn namespaces(endpoint: String, definition: Definition) -> Result<()> {
    tokio::task::spawn_blocking(move || {
        let mut client = connect(&endpoint)?;
        let request = hms::GetDatabaseRequest {
            name: Some(definition.database.clone()),
            catalog_name: Some("hive".to_owned()),
        };
        match client.get_database_req(request.clone()) {
            Ok(_) => Ok(()),
            Err(error) if missing(&error) => {
                let created = client.create_database(hms::Database {
                    name: Some(definition.database),
                    catalog_name: Some("hive".to_owned()),
                    description: Some("Arrowcol contract demo".to_owned()),
                    parameters: Some(BTreeMap::new()),
                    ..Default::default()
                });
                if let Err(error) = created {
                    client
                        .get_database_req(request)
                        .with_context(|| format!("namespace CREATE failed: {error}"))?;
                }
                Ok(())
            }
            Err(error) => Err(error.into()),
        }
    })
    .await?
}

pub(super) async fn create(endpoint: String, definition: Definition) -> Result<Observed> {
    tokio::task::spawn_blocking(move || {
        let mut client = connect(&endpoint)?;
        let mut parameters = definition.properties.clone();
        parameters.insert("EXTERNAL".to_owned(), "TRUE".to_owned());
        parameters.insert("spark.sql.sources.provider".to_owned(), "delta".to_owned());
        if let Some(comment) = &definition.comment {
            parameters.insert("comment".to_owned(), comment.clone());
        }
        let constraints = definition
            .columns
            .iter()
            .filter(|column| !column.nullable)
            .map(|column| hms::SQLNotNullConstraint {
                cat_name: Some(definition.catalog.clone()),
                table_db: Some(definition.database.clone()),
                table_name: Some(definition.table.clone()),
                column_name: Some(column.name.clone()),
                nn_name: None,
                enable_cstr: Some(true),
                validate_cstr: Some(false),
                rely_cstr: Some(true),
            })
            .collect();
        let table = hms::Table {
            cat_name: Some(definition.catalog.clone()),
            db_name: Some(definition.database.clone()),
            table_name: Some(definition.table.clone()),
            owner: Some("arrowcol".to_owned()),
            table_type: Some("EXTERNAL_TABLE".to_owned()),
            partition_keys: Some(Vec::new()),
            parameters: Some(parameters),
            sd: Some(hms::StorageDescriptor {
                cols: Some(
                    definition
                        .columns
                        .iter()
                        .map(|column| hms::FieldSchema {
                            name: Some(column.name.clone()),
                            type_: Some(native_type(&column.data_type)),
                            comment: column.comment.clone(),
                        })
                        .collect(),
                ),
                location: Some(definition.location.clone()),
                input_format: Some("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat".to_owned()),
                output_format: Some("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat".to_owned()),
                compressed: Some(false),
                num_buckets: Some(-1),
                bucket_cols: Some(Vec::new()),
                sort_cols: Some(Vec::new()),
                parameters: Some(BTreeMap::new()),
                serde_info: Some(hms::SerDeInfo {
                    name: Some(definition.table.clone()),
                    serialization_lib: Some("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe".to_owned()),
                    parameters: Some(BTreeMap::new()),
                }),
                ..Default::default()
            }),
            ..Default::default()
        };
        client.create_table_with_constraints(table, vec![], vec![], vec![], constraints, vec![], vec![])?;
        read_sync(&mut client, &definition)?.context("Hive table absent after acknowledged CREATE")
    })
    .await?
}
