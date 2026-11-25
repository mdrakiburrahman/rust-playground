#![allow(clippy::expect_used)]
use arrow_array::{Int64Array, RecordBatch};
use arrow_flight::{
    decode::FlightRecordBatchStream,
    flight_service_server::{FlightService, FlightServiceServer},
    *,
};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use arrowcol_sdk_runtime::{
    ArrowcolContract, Contract, Error, GeneratedStream as ArrowcolStream, StreamOptions, decimal128,
};
use futures::{Stream, TryStreamExt};
use serde_json::json;
use std::{
    pin::Pin,
    sync::{Arc, Mutex},
    time::Duration,
};
use tonic::{Request, Response, Status, Streaming};

struct TestContract;
impl Contract for TestContract {
    type Event = i64;
    fn schema() -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new("id", DataType::Int64, false)]))
    }
    fn record_batch(events: &[i64]) -> arrowcol_sdk_runtime::Result<RecordBatch> {
        RecordBatch::try_new(Self::schema(), vec![Arc::new(Int64Array::from(events.to_vec()))]).map_err(Error::invalid)
    }
    fn descriptor() -> Vec<String> {
        vec!["demo".into(), "public".into(), "events".into(), "v1".into()]
    }
    fn metadata() -> Vec<(&'static str, String)> {
        vec![("x-test-contract", "expected".into())]
    }
    fn fingerprint() -> &'static str {
        "sha256:test"
    }
    fn version() -> u64 {
        1
    }
}
#[derive(Clone, Copy)]
enum Behavior {
    Good,
    Missing,
    WrongRows,
    WrongContract,
    Duplicate,
    Delay(u64),
}
#[derive(Clone)]
struct Mock {
    behavior: Behavior,
    rows: Arc<Mutex<Vec<i64>>>,
    contract: Option<ArrowcolContract>,
}
type Reply<T> = Pin<Box<dyn Stream<Item = std::result::Result<T, Status>> + Send>>;
#[tonic::async_trait]
impl FlightService for Mock {
    type HandshakeStream = Reply<HandshakeResponse>;
    type ListFlightsStream = Reply<FlightInfo>;
    type DoGetStream = Reply<FlightData>;
    type DoPutStream = Reply<PutResult>;
    type DoExchangeStream = Reply<FlightData>;
    type DoActionStream = Reply<arrow_flight::Result>;
    type ListActionsStream = Reply<ActionType>;
    async fn handshake(
        &self,
        _: Request<Streaming<HandshakeRequest>>,
    ) -> std::result::Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn list_flights(
        &self,
        _: Request<Criteria>,
    ) -> std::result::Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn get_flight_info(&self, _: Request<FlightDescriptor>) -> std::result::Result<Response<FlightInfo>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn poll_flight_info(&self, _: Request<FlightDescriptor>) -> std::result::Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn get_schema(&self, _: Request<FlightDescriptor>) -> std::result::Result<Response<SchemaResult>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn do_get(&self, _: Request<Ticket>) -> std::result::Result<Response<Self::DoGetStream>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn do_exchange(
        &self,
        _: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoExchangeStream>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn do_action(&self, _: Request<Action>) -> std::result::Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn list_actions(&self, _: Request<Empty>) -> std::result::Result<Response<Self::ListActionsStream>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn do_put(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoPutStream>, Status> {
        if let Some(contract) = &self.contract {
            for (key, value) in [
                ("x-arrowcol-protocol-version", "1"),
                ("x-arrowcol-implementation-pool", "dataflow"),
                ("x-arrowcol-catalog", "demo"),
                ("x-arrowcol-schema", "public"),
                ("x-arrowcol-table", contract.identity()[2].as_str()),
                ("x-arrowcol-schema-fingerprint", contract.fingerprint()),
            ] {
                assert_eq!(request.metadata().get(key).expect("routing header"), value);
            }
        } else {
            assert_eq!(
                request.metadata().get("x-test-contract").expect("contract header"),
                "expected"
            );
        }
        let messages: Vec<_> = request.into_inner().try_collect().await?;
        assert_eq!(
            messages[0].flight_descriptor.as_ref().expect("descriptor").path,
            self.contract
                .as_ref()
                .map(|contract| {
                    let mut path = contract.identity().to_vec();
                    path.push("v1".into());
                    path
                })
                .unwrap_or_else(TestContract::descriptor)
        );
        let batches: Vec<_> =
            FlightRecordBatchStream::new_from_flight_data(futures::stream::iter(messages.into_iter().map(Ok)))
                .try_collect()
                .await
                .map_err(|error| Status::invalid_argument(error.to_string()))?;
        let count = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
        for batch in batches {
            assert_eq!(
                batch.schema(),
                self.contract
                    .as_ref()
                    .map(ArrowcolContract::schema)
                    .unwrap_or_else(TestContract::schema)
            );
            let values = batch.column(0).as_any().downcast_ref::<Int64Array>().expect("int64");
            self.rows
                .lock()
                .expect("capture")
                .extend(values.values().iter().copied());
        }
        if let Behavior::Delay(ms) = self.behavior {
            tokio::time::sleep(Duration::from_millis(ms)).await;
        }
        let mut final_reply = json!({
            "kind": "best_effort_admission", "wire_version": 1, "protocol_version": 1,
            "delivery_guarantee": "best_effort", "source_schema_version": 1, "target_schema_version": 2,
            "schema_fingerprint": self.contract.as_ref().map(ArrowcolContract::fingerprint).unwrap_or_else(|| TestContract::fingerprint()), "summary": { "rows": count, "batches": 1, "bytes": count * 8 }
        });
        if matches!(self.behavior, Behavior::WrongRows) {
            final_reply["summary"]["rows"] = json!(count + 1);
        }
        if matches!(self.behavior, Behavior::WrongContract) {
            final_reply["schema_fingerprint"] = json!("wrong");
        }
        let replies = match self.behavior {
            Behavior::Missing => Vec::new(),
            Behavior::Duplicate => vec![final_reply.clone(), final_reply],
            _ => vec![
                json!({"kind":"progress", "wire_version":1, "protocol_version":1}),
                final_reply,
            ],
        };
        Ok(Response::new(Box::pin(futures::stream::iter(replies.into_iter().map(
            |value| {
                Ok(PutResult {
                    app_metadata: serde_json::to_vec(&value).expect("JSON").into(),
                })
            },
        )))))
    }
}
struct Server {
    endpoint: String,
    rows: Arc<Mutex<Vec<i64>>>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Server {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn server(behavior: Behavior) -> Server {
    server_with_contract(behavior, None).await
}
async fn server_with_contract(behavior: Behavior, contract: Option<ArrowcolContract>) -> Server {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("listen");
    let endpoint = format!("http://{}", listener.local_addr().expect("address"));
    let rows = Arc::new(Mutex::new(Vec::new()));
    let mock = Mock {
        behavior,
        rows: Arc::clone(&rows),
        contract,
    };
    let task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(FlightServiceServer::new(mock))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .expect("serve");
    });
    Server { endpoint, rows, task }
}
fn options() -> StreamOptions {
    StreamOptions {
        flush_interval_ms: 60_000,
        timeout_ms: 1000,
        ..Default::default()
    }
}

#[tokio::test]
async fn typed_push_batch_and_close_preserve_values_and_accept_newer_target() {
    let server = server(Behavior::Good).await;
    let mut stream = ArrowcolStream::<TestContract>::connect(&server.endpoint, options())
        .await
        .expect("connect");
    stream.push(10).await.expect("push");
    stream.push_batch(&[20, 30]).await.expect("batch");
    stream.flush().await.expect("flush");
    stream.push(40).await.expect("push after flush");
    stream.close().await.expect("close");
    assert_eq!(*server.rows.lock().expect("rows"), [10, 20, 30, 40]);
    assert_eq!(stream.push(50).await.expect_err("closed").kind, "closed");
}

#[tokio::test]
async fn timer_flushes_without_explicit_barrier() {
    let server = server(Behavior::Good).await;
    let mut stream = ArrowcolStream::<TestContract>::connect(
        &server.endpoint,
        StreamOptions {
            flush_interval_ms: 10,
            ..options()
        },
    )
    .await
    .expect("connect");
    stream.push(1).await.expect("push");
    tokio::time::timeout(Duration::from_secs(2), async {
        while server.rows.lock().expect("rows").is_empty() {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("automatic flush");
    stream.close().await.expect("close");
}

#[tokio::test]
async fn missing_wrong_or_duplicate_completions_are_latched_without_replay() {
    for behavior in [
        Behavior::Missing,
        Behavior::WrongRows,
        Behavior::WrongContract,
        Behavior::Duplicate,
    ] {
        let server = server(behavior).await;
        let mut stream = ArrowcolStream::<TestContract>::connect(&server.endpoint, options())
            .await
            .expect("connect");
        stream.push(1).await.expect("push");
        assert_eq!(stream.flush().await.expect_err("bad ack").kind, "outcome_unknown");
        assert_eq!(stream.push(2).await.expect_err("latched").kind, "outcome_unknown");
        assert!(stream.close().await.is_err());
        assert_eq!(*server.rows.lock().expect("rows"), [1]);
    }
}

#[tokio::test]
async fn capacity_cancellation_does_not_enqueue_or_leak_permits() {
    let server = server(Behavior::Delay(100)).await;
    let bytes = TestContract::record_batch(&[1]).expect("batch").get_array_memory_size();
    let mut stream = ArrowcolStream::<TestContract>::connect(
        &server.endpoint,
        StreamOptions {
            batch_rows: 1,
            batch_bytes: bytes,
            buffer_bytes: bytes,
            ..options()
        },
    )
    .await
    .expect("connect");
    stream.push(1).await.expect("first");
    assert!(
        tokio::time::timeout(Duration::from_millis(20), stream.push(2))
            .await
            .is_err()
    );
    stream.push(3).await.expect("capacity released");
    stream.close().await.expect("close");
    assert_eq!(*server.rows.lock().expect("rows"), [1, 3]);
}

#[tokio::test]
async fn whole_flush_deadline_stops_stream_with_unknown_outcome() {
    let server = server(Behavior::Delay(1000)).await;
    let mut stream = ArrowcolStream::<TestContract>::connect(
        &server.endpoint,
        StreamOptions {
            timeout_ms: 100,
            ..options()
        },
    )
    .await
    .expect("connect");
    stream.push(1).await.expect("push");
    assert_eq!(stream.flush().await.expect_err("deadline").kind, "outcome_unknown");
    assert!(stream.close().await.is_err());
    assert!(stream.push(2).await.is_err());
}

#[tokio::test]
async fn oversized_push_is_rejected_before_enqueueing() {
    let server = server(Behavior::Good).await;
    let mut stream = ArrowcolStream::<TestContract>::connect(
        &server.endpoint,
        StreamOptions {
            batch_rows: 1,
            ..options()
        },
    )
    .await
    .expect("connect");
    assert_eq!(
        stream.push_batch(&[1, 2]).await.expect_err("limit").kind,
        "invalid_input"
    );
    stream.push(3).await.expect("still usable");
    stream.close().await.expect("close");
    assert_eq!(*server.rows.lock().expect("rows"), [3]);
}

#[test]
fn decimals_are_exact_and_never_rounded() {
    assert_eq!(
        decimal128("12.34000000000000000000000000000000000000000000", 5, 2).expect("trailing zeroes"),
        1234
    );
    assert_eq!(decimal128("-12.340", 5, 2).expect("decimal"), -1234);
    assert_eq!(decimal128(".05", 2, 2).expect("fraction"), 5);
    assert_eq!(decimal128("1200", 3, -2).expect("negative scale"), 12);
    for (text, precision, scale) in [
        ("12.345", 5, 2),
        ("1000", 3, 0),
        ("12", 3, -2),
        ("1e2", 5, 0),
        (".", 1, 0),
    ] {
        assert!(decimal128(text, precision, scale).is_err(), "{text}");
    }
}

fn runtime_contract(table: &str) -> ArrowcolContract {
    use arrow_ipc::writer::{DictionaryTracker, IpcDataGenerator, IpcWriteOptions};
    use prost::Message;
    use prost_types::{
        DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
        field_descriptor_proto::{Label, Type},
    };
    let columns = [
        (
            "event_id",
            DataType::Int64,
            false,
            json!({"kind":"int64"}),
            Type::Int64,
            "int64",
        ),
        (
            "observed_at",
            DataType::Timestamp(arrow_schema::TimeUnit::Microsecond, Some("UTC".into())),
            false,
            json!({"kind":"timestamp","unit":"microsecond","timezone":"UTC"}),
            Type::Int64,
            "int64",
        ),
        (
            "payload",
            DataType::Utf8,
            true,
            json!({"kind":"utf8"}),
            Type::String,
            "string",
        ),
    ];
    let schema = Schema::new(
        columns
            .iter()
            .map(|(name, ty, nullable, ..)| Field::new(*name, ty.clone(), *nullable))
            .collect::<Vec<_>>(),
    );
    let arrow = IpcDataGenerator::default()
        .schema_to_bytes_with_dictionary_tracker(
            &schema,
            &mut DictionaryTracker::new(false),
            &IpcWriteOptions::default(),
        )
        .ipc_message;
    let manifest = json!({
        "artifact_manifest_version":1,
        "schema":{
            "table_id":"9b1e9392-b345-5573-b368-f0d7b52fb93e",
            "identity":{"catalog":"demo","schema":"public","table":table},
            "version":1,"fingerprint":"sha256:75c465746aa29c29b38b9edf9d639fd3f160cb81bda38e2cf20dd1f458498e6c",
            "fields":columns.iter().enumerate().map(|(i,(name,_,nullable,mapping,_,proto))| json!({
                "id":format!("00000000-0000-0000-0000-{i:012}"), "name":name, "position":i,
                "field_number":i+1, "nullable":nullable, "mapping":{"arrow":mapping,"protobuf":proto}
            })).collect::<Vec<_>>()
        }
    });
    let fields = columns
        .iter()
        .enumerate()
        .map(|(i, (name, _, _, _, ty, _))| FieldDescriptorProto {
            name: Some((*name).into()),
            json_name: Some((*name).into()),
            number: Some(i as i32 + 1),
            label: Some(Label::Optional as i32),
            r#type: Some(*ty as i32),
            ..Default::default()
        })
        .collect();
    let descriptors = FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("schema.proto".into()),
            package: Some("arrowcol.demo.public".into()),
            syntax: Some("proto2".into()),
            message_type: vec![DescriptorProto {
                name: Some(if table == "events" { "Events" } else { "Measurements" }.into()),
                field: fields,
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
    .encode_to_vec();
    ArrowcolContract::from_artifacts(&serde_json::to_vec(&manifest).expect("manifest"), &arrow, &descriptors)
        .expect("runtime contract")
}

#[derive(Clone, PartialEq, prost::Message)]
struct Events {
    #[prost(int64, optional, tag = "1")]
    event_id: Option<i64>,
    #[prost(int64, optional, tag = "2")]
    observed_at: Option<i64>,
    #[prost(string, optional, tag = "3")]
    payload: Option<String>,
}
impl prost_reflect::ReflectMessage for Events {
    fn descriptor(&self) -> prost_reflect::MessageDescriptor {
        runtime_contract("events").descriptor()
    }
}
#[derive(Clone, PartialEq, prost::Message)]
struct Measurements {
    #[prost(int64, optional, tag = "1")]
    event_id: Option<i64>,
    #[prost(int64, optional, tag = "2")]
    observed_at: Option<i64>,
    #[prost(string, optional, tag = "3")]
    payload: Option<String>,
}
impl prost_reflect::ReflectMessage for Measurements {
    fn descriptor(&self) -> prost_reflect::MessageDescriptor {
        runtime_contract("measurements").descriptor()
    }
}

#[tokio::test]
async fn ordinary_models_use_two_independent_contracts_and_atomic_validation() {
    use arrowcol_sdk_runtime::ArrowcolStream;
    let first = runtime_contract("events");
    let second = runtime_contract("measurements");
    let first_server = server_with_contract(Behavior::Good, Some(first.clone())).await;
    let second_server = server_with_contract(Behavior::Good, Some(second.clone())).await;
    let mut first = ArrowcolStream::<Events>::connect(&first_server.endpoint, first, options())
        .await
        .expect("first connect");
    let mut second = ArrowcolStream::<Measurements>::connect(&second_server.endpoint, second, options())
        .await
        .expect("second connect");
    let event = Events {
        event_id: Some(10),
        observed_at: Some(0),
        payload: None,
    };
    first.push(event.clone()).await.expect("first push");
    let mut missing = event.clone();
    missing.observed_at = None;
    assert!(first.push_batch(&[event, missing]).await.is_err());
    second
        .push(Measurements {
            event_id: Some(20),
            observed_at: Some(-1),
            payload: Some("second".into()),
        })
        .await
        .expect("second push");
    first.close().await.expect("first admitted");
    second.close().await.expect("second admitted");
    assert_eq!(*first_server.rows.lock().expect("rows"), [10]);
    assert_eq!(*second_server.rows.lock().expect("rows"), [20]);
    let error = ArrowcolStream::<Measurements>::connect("not an endpoint", runtime_contract("events"), options())
        .await
        .err()
        .expect("model mismatch");
    assert!(error.message.contains("compiled model"));
}

#[tokio::test]
async fn reflected_stream_latches_bad_admission_without_replay() {
    use arrowcol_sdk_runtime::ArrowcolStream;
    for behavior in [
        Behavior::Missing,
        Behavior::WrongRows,
        Behavior::WrongContract,
        Behavior::Duplicate,
    ] {
        let contract = runtime_contract("events");
        let server = server_with_contract(behavior, Some(contract.clone())).await;
        let mut stream = ArrowcolStream::<Events>::connect(&server.endpoint, contract, options())
            .await
            .expect("connect");
        let event = Events {
            event_id: Some(99),
            observed_at: Some(0),
            payload: None,
        };
        stream.push(event.clone()).await.expect("buffered");
        assert_eq!(stream.flush().await.expect_err("bad admission").kind, "outcome_unknown");
        assert_eq!(stream.push(event).await.expect_err("latched").kind, "outcome_unknown");
        assert!(stream.close().await.is_err());
        assert_eq!(*server.rows.lock().expect("rows"), [99]);
    }
}
