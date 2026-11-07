mod common;

use arrowcol_catalog::{
    CatalogField, CatalogSchemaVersion, CatalogType, FieldPath, SchemaVersion, TypeMapping, deterministic_field_id,
};
use arrowcol_catalog_client::rpc;
use arrowcol_sdk_gen::{ArtifactBundle, bundle_from_snapshot};

fn versions() -> Vec<CatalogSchemaVersion> {
    let first = common::fixture_schema();
    let mut fields = first.fields().to_vec();
    fields.push(
        CatalogField::new(
            deterministic_field_id(first.table_id(), &FieldPath::root("status")),
            "status",
            first.next_position().unwrap_or_else(|error| panic!("{error}")),
            first.next_field_number().unwrap_or_else(|error| panic!("{error}")),
            true,
            None,
            TypeMapping::new(CatalogType::Utf8),
        )
        .unwrap_or_else(|error| panic!("{error}")),
    );
    let second = CatalogSchemaVersion::new(
        first.table_id(),
        first.identity().clone(),
        first.version().next().unwrap_or_else(|error| panic!("{error}")),
        fields,
        "ALTER TABLE telemetry.prod.sdk_events ADD COLUMNS (status STRING)",
    )
    .and_then(|schema| schema.with_metadata(first.metadata().clone()))
    .unwrap_or_else(|error| panic!("{error}"));
    vec![first, second]
}

fn snapshot(history: &[CatalogSchemaVersion]) -> rpc::TableSnapshot {
    let last = history.last().unwrap_or_else(|| panic!("history"));
    let bundle = ArtifactBundle::from_schema(last.clone()).unwrap_or_else(|error| panic!("{error}"));
    rpc::TableSnapshot {
        table: Some(rpc::TableRequest {
            catalog: last.identity().catalog().as_str().to_owned(),
            database: last.identity().schema().as_str().to_owned(),
            table: last.identity().table().as_str().to_owned(),
        }),
        schema_history_json: serde_json::to_vec(history).unwrap_or_else(|error| panic!("{error}")),
        manifest: bundle.manifest().to_vec(),
        arrow_schema_ipc: bundle.arrow_schema().to_vec(),
        protobuf_descriptor: bundle.protobuf_descriptor().to_vec(),
        ..rpc::TableSnapshot::default()
    }
}

#[test]
fn committed_catalog_can_generate_a_historical_client_without_promoting_it_to_head() {
    let history = versions();
    let snapshot = snapshot(&history);
    let old = bundle_from_snapshot(&snapshot, history[0].identity(), Some(SchemaVersion::FIRST))
        .unwrap_or_else(|error| panic!("{error}"));
    let current =
        bundle_from_snapshot(&snapshot, history[0].identity(), None).unwrap_or_else(|error| panic!("{error}"));
    assert_eq!(old.schema(), &history[0]);
    assert_eq!(current.schema(), &history[1]);
    assert!(
        !std::str::from_utf8(old.protobuf_source())
            .unwrap_or_else(|error| panic!("{error}"))
            .contains("status")
    );
    assert!(
        std::str::from_utf8(current.protobuf_source())
            .unwrap_or_else(|error| panic!("{error}"))
            .contains("status")
    );
}

#[test]
fn inconsistent_head_artifacts_or_unregistered_versions_are_rejected() {
    let history = versions();
    let mut snapshot = snapshot(&history);
    let identity = history[0].identity();
    assert!(
        bundle_from_snapshot(
            &snapshot,
            identity,
            Some(SchemaVersion::new(3).unwrap_or_else(|error| panic!("{error}")))
        )
        .is_err()
    );
    snapshot.protobuf_descriptor.push(0);
    assert!(bundle_from_snapshot(&snapshot, identity, None).is_err());
}

#[test]
fn corrupt_history_cannot_bypass_schema_revalidation() {
    let history = versions();
    let mut snapshot = snapshot(&history);
    let mut value: serde_json::Value =
        serde_json::from_slice(&snapshot.schema_history_json).unwrap_or_else(|error| panic!("{error}"));
    value[0]["fields"][0]["field_number"] = serde_json::json!(0);
    snapshot.schema_history_json = serde_json::to_vec(&value).unwrap_or_else(|error| panic!("{error}"));
    assert!(bundle_from_snapshot(&snapshot, history[0].identity(), None).is_err());
}
