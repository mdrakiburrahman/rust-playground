use std::sync::OnceLock;

use otel_arrow_dfe_config::{NodeId, PipelineKey, engine::OtelDataflowSpec};
use otel_arrow_dfe_engine::context::PipelineContext;

use crate::{ARROW_DELTALAKE_EXPORTER_URN, ARROW_FLIGHT_RECEIVER_URN};

struct DirectCollector {
    pipeline: PipelineKey,
    receiver: NodeId,
}

static STARTUP_TOPOLOGY: OnceLock<Option<DirectCollector>> = OnceLock::new();

/// Records the SDK's accepted startup topology before it starts the controller.
///
/// # Errors
/// Returns an error if called more than once.
pub fn observe_validated_topology(spec: &OtelDataflowSpec) -> Result<(), &'static str> {
    STARTUP_TOPOLOGY
        .set(direct_collector(spec))
        .map_err(|_| "collector topology was already observed")?;
    if let Some(capacity) = resolved_channel_capacity(spec) {
        eprintln!(
            "df_engine_effective_config {}",
            serde_json::json!({"engine":{"channel_capacity":capacity}})
        );
    }
    Ok(())
}

fn resolved_channel_capacity(spec: &OtelDataflowSpec) -> Option<serde_json::Value> {
    if spec.engine.http_admin.is_some()
        || !spec.engine.controller.is_empty()
        || spec.engine.observability != Default::default()
        || !spec.topics.is_empty()
        || spec.groups.len() != 1
    {
        return None;
    }
    let (_, pipelines, _) = spec.resolve().into_parts();
    let [pipeline] = pipelines.as_slice() else {
        return None;
    };
    let capacity = &pipeline.policies.channel_capacity;
    Some(serde_json::json!({
        "control_node": capacity.control.node,
        "control_pipeline": capacity.control.pipeline,
        "control_completion": capacity.control.completion,
        "pdata": capacity.pdata,
    }))
}

pub(crate) fn is_direct_collector(pipeline: &PipelineContext, node: &NodeId) -> bool {
    STARTUP_TOPOLOGY
        .get()
        .and_then(Option::as_ref)
        .is_some_and(|direct| direct.pipeline == pipeline.pipeline_key() && &direct.receiver == node)
}

fn direct_collector(spec: &OtelDataflowSpec) -> Option<DirectCollector> {
    // A one-time startup observation is not proof if the controller can accept
    // topology replacements or if another graph can also feed this process.
    if spec.engine.http_admin.is_some()
        || !spec.engine.controller.is_empty()
        || spec.engine.observability != Default::default()
        || !spec.topics.is_empty()
        || spec.groups.len() != 1
    {
        return None;
    }
    let (group_id, group) = spec.groups.iter().next()?;
    if !group.topics.is_empty() || group.pipelines.len() != 1 {
        return None;
    }
    let (pipeline_id, pipeline) = group.pipelines.iter().next()?;
    let nodes = pipeline.node_iter().collect::<Vec<_>>();
    if nodes.len() != 2 || pipeline.extension_iter().next().is_some() {
        return None;
    }
    let (receiver, _) = nodes
        .iter()
        .copied()
        .find(|(_, node)| node.r#type.as_ref() == ARROW_FLIGHT_RECEIVER_URN)?;
    let (delta, _) = nodes
        .iter()
        .copied()
        .find(|(_, node)| node.r#type.as_ref() == ARROW_DELTALAKE_EXPORTER_URN)?;
    let mut connections = pipeline.connection_iter();
    let connection = connections.next()?;
    if connections.next().is_some() || connection.to_nodes().as_slice() != [delta.clone()] {
        return None;
    }
    let sources = connection.from_sources();
    let [source] = sources.as_slice() else {
        return None;
    };
    if source.node_id() != receiver || source.resolved_output_port().as_ref() != "default" {
        return None;
    }
    Some(DirectCollector {
        pipeline: PipelineKey::new(group_id.clone(), pipeline_id.clone()),
        receiver: receiver.clone(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const DIRECT: &str = r"
version: otel_dataflow/v1
groups:
  collector:
    pipelines:
      ingest:
        nodes:
          ingress:
            type: receiver:arrow_flight
          sink:
            type: exporter:arrow_deltalake
        connections:
          - from: ingress
            to: sink
";

    #[test]
    fn channel_capacity_reports_resolved_immutable_pipeline_policy() -> anyhow::Result<()> {
        let yaml = DIRECT.replace(
            "        nodes:",
            "        policies:\n          channel_capacity:\n            control:\n              node: 11\n              pipeline: 12\n              completion: 13\n            pdata: 14\n        nodes:",
        );
        let spec = OtelDataflowSpec::from_yaml(&yaml)?;
        assert_eq!(
            resolved_channel_capacity(&spec),
            Some(serde_json::json!({
                "control_node":11, "control_pipeline":12, "control_completion":13, "pdata":14
            }))
        );
        let mut multiple = spec.clone();
        let group = multiple
            .groups
            .values_mut()
            .next()
            .ok_or_else(|| anyhow::anyhow!("missing group"))?;
        let pipeline = group
            .pipelines
            .values()
            .next()
            .ok_or_else(|| anyhow::anyhow!("missing pipeline"))?
            .clone();
        group.add_pipeline("other".into(), pipeline)?;
        assert!(resolved_channel_capacity(&multiple).is_none());
        let mutable = yaml.replace(
            "groups:",
            "engine:\n  http_admin:\n    bind_address: '127.0.0.1:8080'\ngroups:",
        );
        assert!(resolved_channel_capacity(&OtelDataflowSpec::from_yaml(&mutable)?).is_none());
        Ok(())
    }

    #[test]
    fn only_a_single_immutable_direct_pipeline_is_attested() -> anyhow::Result<()> {
        let direct = OtelDataflowSpec::from_yaml(DIRECT)?;
        let observed = direct_collector(&direct).ok_or_else(|| anyhow::anyhow!("direct topology not attested"))?;
        assert_eq!(observed.pipeline.as_string(), "collector:ingest");
        assert_eq!(observed.receiver.as_ref(), "ingress");

        let buffered = DIRECT
            .replace(
                "          sink:",
                "          spool:\n            type: processor:durable_buffer\n          sink:",
            )
            .replace(
                "            to: sink",
                "            to: spool\n          - from: spool\n            to: sink",
            );
        let buffered = OtelDataflowSpec::from_yaml(&buffered)?;
        assert!(direct_collector(&buffered).is_none());

        let mut mixed = direct.clone();
        let group = mixed
            .groups
            .values_mut()
            .next()
            .ok_or_else(|| anyhow::anyhow!("missing fixture group"))?;
        let other = buffered
            .groups
            .values()
            .next()
            .and_then(|group| group.pipelines.values().next())
            .ok_or_else(|| anyhow::anyhow!("missing buffered pipeline"))?;
        group.add_pipeline("other".into(), other.clone())?;
        assert!(direct_collector(&mixed).is_none());

        let unrelated = OtelDataflowSpec::from_yaml(&DIRECT.replace("exporter:arrow_deltalake", "exporter:noop"))?;
        assert!(direct_collector(&unrelated).is_none());
        let mutable = OtelDataflowSpec::from_yaml(&DIRECT.replace(
            "groups:",
            "engine:\n  http_admin:\n    bind_address: '127.0.0.1:8080'\ngroups:",
        ))?;
        assert!(direct_collector(&mutable).is_none());
        Ok(())
    }
}
