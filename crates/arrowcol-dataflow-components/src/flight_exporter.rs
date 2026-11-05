use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use arrow_array::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_descriptor::DescriptorType;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::sql::{CommandStatementUpdate, ProstMessageExt};
use arrow_flight::{FlightData, FlightDescriptor};
use arrowcol_catalog::SchemaVersion;
use arrowcol_flight::{DdlCompletionMetadata, PutResultMetadata, decode_ddl_completion_metadata};
use arrowcol_protocol::{
    ClaimedSchemaFingerprint, ImplementationPool, IngestMetadata, ProtocolVersion, TableIdentity, fingerprint_schema,
};
use async_trait::async_trait;
use futures::{TryStreamExt, stream};
use linkme::distributed_slice;
use otel_arrow_dfe_config::node::NodeUserConfig;
use otel_arrow_dfe_engine::ExporterFactory;
use otel_arrow_dfe_engine::config::ExporterConfig;
use otel_arrow_dfe_engine::context::PipelineContext;
use otel_arrow_dfe_engine::control::NodeControlMsg;
use otel_arrow_dfe_engine::error::Error;
use otel_arrow_dfe_engine::exporter::ExporterWrapper;
use otel_arrow_dfe_engine::local::exporter;
use otel_arrow_dfe_engine::message::{ExporterInbox, Message};
use otel_arrow_dfe_engine::node::NodeId;
use otel_arrow_dfe_engine::terminal_state::TerminalState;
use otel_arrow_dfe_otap::OTAP_EXPORTER_FACTORIES;
use otel_arrow_dfe_otap::pdata::OtapPdata;
use otel_arrow_dfe_pdata::PayloadData;
use prost::Message as _;
use serde::Deserialize;
use tonic::metadata::MetadataValue;
use tonic::transport::{Channel, Endpoint};
use tonic::{Code, Request, Status};

use crate::config::{config_error, parse_config};
use crate::envelope::{ArrowcolBatchEnvelope, decode_record_set};
use crate::exporter::{ExportFailure, complete_publication};

/// DFE URN for forwarding Arrowcol batches to a downstream Arrow Flight collector.
pub const ARROW_FLIGHT_EXPORTER_URN: &str = "urn:otel:exporter:arrow_flight";

const fn default_forward_timeout() -> Duration {
    Duration::from_secs(30)
}

/// Downstream Arrowcol Flight endpoint for collector-to-collector chaining.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FlightExporterConfig {
    /// Downstream `http://host:port` Arrow Flight endpoint.
    pub endpoint: String,
    /// Per-RPC deadline for DDL replay and batch forwarding.
    #[serde(default = "default_forward_timeout", with = "humantime_serde")]
    pub timeout: Duration,
}

impl FlightExporterConfig {
    fn endpoint(&self) -> Result<Endpoint, String> {
        if self.timeout.is_zero() {
            return Err("arrow_flight exporter timeout must be positive".to_owned());
        }
        Endpoint::from_shared(self.endpoint.clone())
            .map(|endpoint| endpoint.connect_timeout(self.timeout).timeout(self.timeout))
            .map_err(|error| format!("invalid arrow_flight exporter endpoint: {error}"))
    }
}

/// Forwards each admitted Arrowcol batch, with its catalog DDL, to the next collector.
pub struct ArrowFlightExporter {
    endpoint: Endpoint,
    timeout: Duration,
    downstream_versions: HashMap<(TableIdentity, SchemaVersion), SchemaVersion>,
}

impl ArrowFlightExporter {
    async fn forward(
        &mut self,
        client: &mut FlightServiceClient<Channel>,
        pdata: &OtapPdata,
    ) -> Result<(), ExportFailure> {
        let (_, payload) = pdata.clone().into_parts();
        let PayloadData::PdataArrowRecords(records) = payload.into_data() else {
            return Err(ExportFailure::Permanent(
                "arrow_flight exporter requires Arrowcol pdata".to_owned(),
            ));
        };
        for decoded in decode_record_set(*records).map_err(ExportFailure::Envelope)? {
            let key = (decoded.envelope.table.clone(), decoded.envelope.target_schema_version);
            let version = match self.downstream_versions.get(&key) {
                Some(version) => *version,
                None => {
                    let version = self.replay_ddl(client, &decoded.envelope).await?;
                    self.downstream_versions.insert(key.clone(), version);
                    version
                }
            };
            if let Err(error) = self.put(client, &key.0, version, decoded.batch).await {
                self.downstream_versions.remove(&key);
                return Err(error);
            }
        }
        Ok(())
    }

    async fn replay_ddl(
        &self,
        client: &mut FlightServiceClient<Channel>,
        envelope: &ArrowcolBatchEnvelope,
    ) -> Result<SchemaVersion, ExportFailure> {
        let table = &envelope.table;
        let history = crate::catalogs::history_through(table, envelope.target_schema_version)
            .await
            .map_err(ExportFailure::Transient)?
            .ok_or_else(|| {
                ExportFailure::Permanent(format!(
                    "no local catalog holds {}.{}.{} v{}",
                    table.catalog(),
                    table.schema(),
                    table.table(),
                    envelope.target_schema_version
                ))
            })?;
        let mut downstream = None;
        for version in &history {
            let completion = self.register(client, version.canonical_ddl()).await?;
            if completion.schema_fingerprint() != version.fingerprint() {
                return Err(ExportFailure::Permanent(
                    "downstream catalog schema diverged from the local catalog".to_owned(),
                ));
            }
            downstream = Some(completion.schema_version());
        }
        downstream.ok_or_else(|| ExportFailure::Permanent("local catalog history is empty".to_owned()))
    }

    async fn register(
        &self,
        client: &mut FlightServiceClient<Channel>,
        ddl: &str,
    ) -> Result<DdlCompletionMetadata, ExportFailure> {
        let command = CommandStatementUpdate {
            query: ddl.to_owned(),
            transaction_id: None,
        };
        let descriptor = FlightDescriptor {
            r#type: DescriptorType::Cmd as i32,
            cmd: command.as_any().encode_to_vec().into(),
            path: Vec::new(),
        };
        let mut request = Request::new(stream::iter(vec![FlightData {
            flight_descriptor: Some(descriptor),
            ..FlightData::default()
        }]));
        request.set_timeout(self.timeout);
        let response = client.do_put(request).await.map_err(status_failure)?;
        let completion = decode_ddl_completion_metadata(response.metadata())
            .map_err(|error| ExportFailure::Transient(error.to_string()))?;
        let mut results = response.into_inner();
        while results.message().await.map_err(status_failure)?.is_some() {}
        Ok(completion)
    }

    async fn put(
        &self,
        client: &mut FlightServiceClient<Channel>,
        table: &TableIdentity,
        version: SchemaVersion,
        batch: RecordBatch,
    ) -> Result<(), ExportFailure> {
        let schema = batch.schema();
        let descriptor = FlightDescriptor {
            r#type: DescriptorType::Path as i32,
            cmd: Default::default(),
            path: vec![
                table.catalog().to_string(),
                table.schema().to_string(),
                table.table().to_string(),
                format!("v{version}"),
            ],
        };
        let encoded = FlightDataEncoderBuilder::new()
            .with_schema(Arc::clone(&schema))
            .with_flight_descriptor(Some(descriptor))
            .build(stream::iter([Ok::<RecordBatch, FlightError>(batch)]))
            .try_collect::<Vec<_>>()
            .await
            .map_err(|error| ExportFailure::Permanent(error.to_string()))?;
        let mut request = Request::new(stream::iter(encoded));
        let metadata = IngestMetadata::new(
            ProtocolVersion::CURRENT,
            ImplementationPool::Dataflow,
            table.clone(),
            ClaimedSchemaFingerprint::new(fingerprint_schema(schema.as_ref())),
        );
        for (key, value) in metadata.to_ascii_pairs() {
            let value =
                MetadataValue::try_from(value.as_str()).map_err(|error| ExportFailure::Permanent(error.to_string()))?;
            request.metadata_mut().insert(key, value);
        }
        request.set_timeout(self.timeout);
        let mut results = client.do_put(request).await.map_err(status_failure)?.into_inner();
        let mut admitted = false;
        while let Some(result) = results.message().await.map_err(status_failure)? {
            match PutResultMetadata::from_put_result(&result)
                .map_err(|error| ExportFailure::Transient(error.to_string()))?
            {
                PutResultMetadata::Progress { .. } => {}
                PutResultMetadata::BestEffortAdmission { .. } | PutResultMetadata::DurableAdmission { .. }
                    if !admitted =>
                {
                    admitted = true;
                }
                _ => {
                    return Err(ExportFailure::Transient(
                        "downstream Flight acknowledgement contract was inconsistent".to_owned(),
                    ));
                }
            }
        }
        if admitted {
            Ok(())
        } else {
            Err(ExportFailure::Transient(
                "downstream Flight DoPut completed without an admission".to_owned(),
            ))
        }
    }
}

fn status_failure(status: Status) -> ExportFailure {
    let message = format!("downstream Flight {:?}: {}", status.code(), status.message());
    match status.code() {
        Code::InvalidArgument | Code::PermissionDenied | Code::Unauthenticated | Code::Unimplemented => {
            ExportFailure::Permanent(message)
        }
        _ => ExportFailure::Transient(message),
    }
}

#[async_trait(?Send)]
impl exporter::Exporter<OtapPdata> for ArrowFlightExporter {
    async fn start(
        mut self: Box<Self>,
        mut inbox: ExporterInbox<OtapPdata>,
        effect_handler: exporter::EffectHandler<OtapPdata>,
    ) -> Result<TerminalState, Error> {
        let mut client = FlightServiceClient::new(self.endpoint.connect_lazy());
        loop {
            match inbox.recv().await? {
                Message::PData(pdata) => {
                    let result = self.forward(&mut client, &pdata).await;
                    complete_publication(&effect_handler, pdata, result).await?;
                }
                Message::Control(NodeControlMsg::Shutdown { .. }) => break,
                Message::Control(_) => {}
            }
        }
        Ok(TerminalState::default())
    }
}

fn validate_exporter_config(value: &serde_json::Value) -> Result<(), otel_arrow_dfe_config::error::Error> {
    let config: FlightExporterConfig = parse_config(value)?;
    config.endpoint().map(drop).map_err(config_error)
}

fn create_exporter(
    _pipeline: PipelineContext,
    node: NodeId,
    node_config: Arc<NodeUserConfig>,
    exporter_config: &ExporterConfig,
    _capabilities: &otel_arrow_dfe_engine::capability::registry::Capabilities,
) -> Result<ExporterWrapper<OtapPdata>, otel_arrow_dfe_config::error::Error> {
    let config: FlightExporterConfig = parse_config(&node_config.config)?;
    let exporter = ArrowFlightExporter {
        endpoint: config.endpoint().map_err(config_error)?,
        timeout: config.timeout,
        downstream_versions: HashMap::new(),
    };
    Ok(ExporterWrapper::local(exporter, node, node_config, exporter_config))
}

/// External DFE exporter registration.
#[allow(unsafe_code)]
#[otel_arrow_dfe_engine::component_inventory(category = Exporter)]
#[distributed_slice(OTAP_EXPORTER_FACTORIES)]
pub static ARROW_FLIGHT_EXPORTER: ExporterFactory<OtapPdata> = ExporterFactory {
    name: ARROW_FLIGHT_EXPORTER_URN,
    create: create_exporter,
    context_declarations: None,
    wiring_contract: otel_arrow_dfe_engine::wiring_contract::WiringContract::UNRESTRICTED
        .with_pdata(otel_arrow_dfe_engine::wiring_contract::PdataWiringContract::PLUGGABLE_ARROW_SINK),
    validate_config: validate_exporter_config,
};

#[cfg(test)]
mod tests {
    #![allow(clippy::expect_used)]

    use std::sync::Mutex;

    use anyhow::Result;
    use arrow_array::{ArrayRef, Int64Array, StringArray};
    use arrowcol_catalog::{
        CatalogControlPlane, CatalogStore, DdlParser, FlightSqlDdlHandler, FlightSqlDdlRequest, MemoryCatalogStore,
        MemoryMetastore,
    };
    use arrowcol_flight::{ArrowColFlightService, BatchAdmission};
    use arrowcol_protocol::{CollectorInstanceId, FencingToken, LeaseId, ReceiptId};
    use arrowcol_routing::{LeaseKey, MemoryRoutingStore, RoutingStore};
    use otel_arrow_dfe_pdata::OtapPayload;
    use tokio_stream::wrappers::TcpListenerStream;

    use super::*;
    use crate::envelope::encode_record_batch;
    use crate::metrics::RuntimeMetrics;
    use crate::receiver::tests::{CaptureSender, ready_collector};
    use crate::receiver::{DfeAdmission, PdataSender, allow_all_authenticator};
    use crate::routing::StoreLeaseResolver;

    struct Downstream {
        endpoint: String,
        catalog: Arc<MemoryCatalogStore>,
        routing: Arc<MemoryRoutingStore>,
        instance_id: CollectorInstanceId,
        sender: Arc<CaptureSender>,
    }

    async fn downstream() -> Result<Downstream> {
        let catalog = Arc::new(MemoryCatalogStore::new());
        let control = Arc::new(CatalogControlPlane::new(
            DdlParser::default(),
            Arc::clone(&catalog) as Arc<dyn CatalogStore>,
            Arc::new(MemoryMetastore::new()),
        ));
        let routing = Arc::new(MemoryRoutingStore::default());
        let instance_id = CollectorInstanceId::new();
        ready_collector(&routing, instance_id).await?;
        let sender = Arc::new(CaptureSender {
            sent: Mutex::new(Vec::new()),
            gate: None,
        });
        let metrics = Arc::new(RuntimeMetrics::new(instance_id));
        metrics.configure_receiver(2, 1_000_000_000, 1_000_000, 10_000, 16, 2);
        let resolver = StoreLeaseResolver::new(Arc::clone(&routing) as Arc<dyn RoutingStore>, instance_id)
            .claiming(Duration::from_secs(60));
        let admission = Arc::new(DfeAdmission::new(
            Arc::clone(&sender) as Arc<dyn PdataSender>,
            Arc::new(resolver),
            instance_id,
            "dfe-flight-downstream",
            2,
            metrics,
        ));
        let service = ArrowColFlightService::new(
            ImplementationPool::Dataflow,
            Arc::clone(&catalog) as Arc<dyn CatalogStore>,
            Arc::new(allow_all_authenticator().map_err(anyhow::Error::msg)?),
            admission as Arc<dyn BatchAdmission>,
            arrowcol_flight::RuntimeLimits::default(),
        )?
        .with_ddl_handler(control as Arc<dyn FlightSqlDdlHandler>);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
        let endpoint = format!("http://{}", listener.local_addr()?);
        drop(tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(service.into_tonic_service())
                .serve_with_incoming(TcpListenerStream::new(listener)),
        ));
        Ok(Downstream {
            endpoint,
            catalog,
            routing,
            instance_id,
            sender,
        })
    }

    async fn upstream_table(ddl: &str) -> Result<(TableIdentity, Arc<arrow_schema::Schema>)> {
        let catalog = Arc::new(MemoryCatalogStore::new());
        let control = CatalogControlPlane::new(
            DdlParser::default(),
            Arc::clone(&catalog) as Arc<dyn CatalogStore>,
            Arc::new(MemoryMetastore::new()),
        );
        let completion = control.execute_ddl(FlightSqlDdlRequest::new(ddl)).await?;
        let current = catalog
            .current_schema(completion.identity())
            .await?
            .ok_or_else(|| anyhow::anyhow!("missing schema"))?;
        crate::catalogs::register(catalog as Arc<dyn CatalogStore>).map_err(anyhow::Error::msg)?;
        Ok((completion.identity().clone(), Arc::new(current.arrow_schema()?)))
    }

    fn pdata(table: &TableIdentity, schema: &Arc<arrow_schema::Schema>, ids: Vec<i64>) -> Result<OtapPdata> {
        let bodies = ids.iter().map(|id| Some(format!("row-{id}"))).collect::<Vec<_>>();
        let batch = RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(Int64Array::from(ids)) as ArrayRef,
                Arc::new(StringArray::from(bodies)) as ArrayRef,
            ],
        )?;
        let envelope = ArrowcolBatchEnvelope {
            publication_id: Some(ReceiptId::new()),
            ingested_at_ms: None,
            table: table.clone(),
            source_schema_version: SchemaVersion::FIRST,
            target_schema_version: SchemaVersion::FIRST,
            lease_id: LeaseId::new(),
            holder_instance_id: CollectorInstanceId::new(),
            fencing_token: FencingToken::new(1)?,
            instance_key: "dfe-flight-upstream".to_owned(),
        };
        Ok(OtapPdata::new_todo_context(OtapPayload::from_pdata_arrow(
            encode_record_batch(envelope, batch)?,
        )))
    }

    fn exporter(endpoint: &str) -> Result<(ArrowFlightExporter, FlightServiceClient<Channel>)> {
        let config = FlightExporterConfig {
            endpoint: endpoint.to_owned(),
            timeout: Duration::from_secs(10),
        };
        let endpoint = config.endpoint().map_err(anyhow::Error::msg)?;
        let client = FlightServiceClient::new(endpoint.connect_lazy());
        Ok((
            ArrowFlightExporter {
                endpoint,
                timeout: config.timeout,
                downstream_versions: HashMap::new(),
            },
            client,
        ))
    }

    /// Scenario: a middle collector forwards batches of a table only its local catalog knows.
    /// Guarantees: the exact DDL is replayed once, the downstream claims its own lease, and every batch is admitted.
    #[tokio::test]
    async fn forwards_batches_with_replayed_ddl_to_downstream_collector() -> Result<()> {
        let (table, schema) =
            upstream_table("CREATE TABLE chain.forward.events (id BIGINT NOT NULL, body VARCHAR)").await?;
        let downstream = downstream().await?;
        let (mut exporter, mut client) = exporter(&downstream.endpoint)?;

        for ids in [vec![1, 2], vec![3]] {
            exporter
                .forward(&mut client, &pdata(&table, &schema, ids)?)
                .await
                .map_err(|error| anyhow::anyhow!(error.to_string()))?;
        }

        let replicated = downstream
            .catalog
            .current_schema(&table)
            .await?
            .ok_or_else(|| anyhow::anyhow!("downstream catalog missing table"))?;
        assert_eq!(replicated.version(), SchemaVersion::FIRST);
        assert_eq!(
            fingerprint_schema(&replicated.arrow_schema()?),
            fingerprint_schema(&schema)
        );
        assert_eq!(exporter.downstream_versions.len(), 1);
        let view = downstream
            .routing
            .load_view(crate::config::now_unix_millis().map_err(anyhow::Error::msg)?)
            .await?;
        let lease = view
            .leases
            .get(&LeaseKey::new(ImplementationPool::Dataflow, table.clone()))
            .ok_or_else(|| anyhow::anyhow!("downstream claimed no lease"))?;
        assert_eq!(lease.holder_instance_id, downstream.instance_id);
        let rows = downstream
            .sender
            .sent
            .lock()
            .expect("capture lock")
            .iter()
            .map(|(_, pdata)| pdata.clone().num_items())
            .sum::<usize>();
        assert_eq!(rows, 3);
        Ok(())
    }

    /// Scenario: a batch references a table no local receiver catalog registered.
    /// Guarantees: forwarding fails permanently instead of retrying forever.
    #[tokio::test]
    async fn unknown_local_table_is_a_permanent_failure() -> Result<()> {
        let (_, schema) = upstream_table("CREATE TABLE chain.known.events (id BIGINT NOT NULL, body VARCHAR)").await?;
        let unknown = TableIdentity::new(
            arrowcol_protocol::CatalogName::new("chain")?,
            arrowcol_protocol::SchemaName::new("unknown")?,
            arrowcol_protocol::TableName::new("events")?,
        );
        let downstream = downstream().await?;
        let (mut exporter, mut client) = exporter(&downstream.endpoint)?;
        let error = exporter
            .forward(&mut client, &pdata(&unknown, &schema, vec![1])?)
            .await
            .expect_err("unknown table must fail");
        assert!(error.permanent());
        assert!(downstream.sender.sent.lock().expect("capture lock").is_empty());
        Ok(())
    }
}
