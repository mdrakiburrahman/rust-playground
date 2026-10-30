use anyhow::{Context, Result};
use arrowcol_authority::{delta_store::*, sql_publication::Proposal, *};
use std::collections::BTreeMap;

fn clock(now: i64) -> Clock {
    Clock {
        now,
        live: None,
        new_lease: 0,
    }
}
fn setup() -> Result<(Snapshot, Controller, Writer, Writer)> {
    let mut state = Snapshot::default();
    apply(
        &mut state,
        &Command::Start {
            id: "controller".into(),
        },
        &clock(1),
    )?;
    let c = state.controller.clone().context("controller")?;
    for id in ["a", "b", "c"] {
        apply(
            &mut state,
            &Command::Register {
                controller: c.clone(),
                member: Member {
                    writer: Writer {
                        collector: id.into(),
                        process: format!("process-{id}"),
                        generation: String::new(),
                    },
                    endpoint: format!("http://{id}:50051"),
                    pressure: 0.1,
                    rss_bytes: 1024,
                    active: 0,
                    eligible: true,
                },
            },
            &clock(1),
        )?;
    }
    for id in ["a", "b", "c"] {
        let writer = state.members[id].member.writer.clone();
        apply(
            &mut state,
            &Command::Observe {
                writer,
                applied_revision: 0,
                observation: placement::Observation {
                    demand: placement::Demand {
                        cpu: 0.1,
                        memory: 0.1,
                        throughput: 0.1,
                    },
                    capacity: placement::Demand {
                        cpu: 1.0,
                        memory: 1.0,
                        throughput: 1.0,
                    },
                    sequence: 1,
                    allocation_revision: 0,
                    tables: BTreeMap::new(),
                    fingerprints: ["fp".to_owned()].into(),
                },
            },
            &clock(1),
        )?;
    }
    let a = state.members["a"].member.writer.clone();
    let b = state.members["b"].member.writer.clone();
    apply(
        &mut state,
        &Command::Catalog {
            controller: c.clone(),
            key: "table".into(),
            policy: placement::TablePolicy {
                revision: 1,
                fingerprints: vec!["fp".into()],
                demand: placement::Demand::default(),
            },
        },
        &clock(1),
    )?;
    apply(
        &mut state,
        &Command::Claim {
            controller: c.clone(),
            key: "table".into(),
            target: a.clone(),
            operation: "claim".into(),
        },
        &clock(1),
    )?;
    assert_eq!(state.tables["table"].record.lifecycle, Lifecycle::Recovering);
    let identity = state.tables["table"].identity.clone();
    apply(
        &mut state,
        &Command::Activate {
            table: "table".into(),
            identity,
            version: -1,
        },
        &clock(1),
    )?;
    Ok((state, c, a, b))
}
fn admit(state: &mut Snapshot, writer: &Writer, unit: &str) -> Result<()> {
    let epoch = state.tables["table"].record.epoch;
    apply(
        state,
        &Command::OpenStream {
            key: "table".into(),
            writer: writer.clone(),
            epoch,
            stream: unit.into(),
            fingerprint: "fp".into(),
        },
        &clock(2),
    )?;
    apply(
        state,
        &Command::AdmitStream {
            key: "table".into(),
            writer: writer.clone(),
            epoch,
            stream: unit.into(),
            unit: unit.into(),
        },
        &clock(2),
    )?;
    apply(
        state,
        &Command::CloseStream {
            key: "table".into(),
            writer: writer.clone(),
            stream: unit.into(),
        },
        &clock(2),
    )
}
fn proposal(state: &Snapshot, unit: &str) -> Proposal {
    let version = state.tables["table"].decisions.len() as i64;
    Proposal {
        table: "table".into(),
        operation: unit.into(),
        identity: state.tables["table"].identity.clone(),
        predecessor: version - 1,
        version,
        actions: "{\"commitInfo\":{}}\n".into(),
        data: vec![],
    }
}

#[test]
fn cold_claim_requires_recovery_and_fresh_capacity() -> Result<()> {
    let (state, c, a, _) = setup()?;
    let command = Command::Claim {
        controller: c,
        key: "cold".into(),
        target: a.clone(),
        operation: "cold".into(),
    };
    assert!(apply(&mut state.clone(), &command, &clock(4000)).is_err());
    let mut next = state.clone();
    apply(&mut next, &command, &clock(2))?;
    assert_eq!(next.tables["cold"].record.lifecycle, Lifecycle::Recovering);
    assert!(
        apply(
            &mut next.clone(),
            &Command::Admit {
                key: "cold".into(),
                writer: a,
                epoch: 1,
                unit: "unit".into()
            },
            &clock(2)
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn exclusion_and_admission_block_are_distinct_and_revisioned() -> Result<()> {
    let (mut state, c, a, _) = setup()?;
    apply(
        &mut state,
        &Command::Catalog {
            controller: c.clone(),
            key: "table".into(),
            policy: placement::TablePolicy {
                revision: 1,
                fingerprints: vec!["fp".into()],
                demand: placement::Demand::default(),
            },
        },
        &clock(2),
    )?;
    let open = |stream: &str| Command::OpenStream {
        key: "table".into(),
        writer: a.clone(),
        epoch: 1,
        stream: stream.into(),
        fingerprint: "fp".into(),
    };
    apply(&mut state, &open("existing"), &clock(2))?;
    let policy = Command::Policy {
        controller: c.clone(),
        collector: "a".into(),
        writer: a.clone(),
        revision: 0,
        operation: "exclude".into(),
        excluded: true,
        block_streams: false,
        retiring: false,
    };
    apply(&mut state, &policy, &clock(2))?;
    apply(&mut state, &policy, &clock(2))?;
    apply(&mut state, &open("after-exclusion"), &clock(2))?;
    assert!(state.eligible("a", "new-table").is_err());
    apply(
        &mut state,
        &Command::Policy {
            controller: c.clone(),
            collector: "a".into(),
            writer: a.clone(),
            revision: 1,
            operation: "block".into(),
            excluded: true,
            block_streams: true,
            retiring: false,
        },
        &clock(2),
    )?;
    assert!(apply(&mut state.clone(), &open("after-block"), &clock(2)).is_err());
    apply(
        &mut state,
        &Command::AdmitStream {
            key: "table".into(),
            writer: a.clone(),
            epoch: 1,
            stream: "existing".into(),
            unit: "one".into(),
        },
        &clock(2),
    )?;
    assert!(apply(&mut state.clone(), &policy, &clock(2)).is_err());
    assert!(
        apply(
            &mut state,
            &Command::ApproveRetirement {
                controller: c,
                collector: "a".into()
            },
            &clock(2)
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn recovering_reservations_remain_charged_until_activation() -> Result<()> {
    let (mut state, c, a, b) = setup()?;
    apply(
        &mut state,
        &Command::Reserve {
            controller: c.clone(),
            key: "table".into(),
            target: b.clone(),
            operation: "move".into(),
        },
        &clock(2),
    )?;
    assert!(state.tables["table"].reservation.is_some());
    assert!(state.projected("a", "table")?.cpu > 0.1);
    assert!(state.projected("b", "table")?.cpu > 0.1);
    apply(
        &mut state,
        &Command::Transfer {
            controller: c,
            key: "table".into(),
            operation: "move".into(),
            force: false,
        },
        &clock(2),
    )?;
    assert!(state.tables["table"].reservation.is_some());
    assert!(admit(&mut state, &a, "late").is_err());
    let identity = state.tables["table"].identity.clone();
    apply(
        &mut state,
        &Command::Activate {
            table: "table".into(),
            identity,
            version: -1,
        },
        &clock(2),
    )?;
    assert!(state.tables["table"].reservation.is_none());
    assert_eq!(state.tables["table"].record.writer, b);
    Ok(())
}

#[test]
fn retirement_waits_for_recovery_even_when_first_successor_fails() -> Result<()> {
    let (mut state, controller, a, b) = setup()?;
    apply(
        &mut state,
        &Command::Policy {
            controller: controller.clone(),
            collector: "a".into(),
            writer: a.clone(),
            revision: 0,
            operation: "retire".into(),
            excluded: true,
            block_streams: false,
            retiring: true,
        },
        &clock(2),
    )?;
    let mut observation = state.observations["a"].clone();
    observation.sequence = 2;
    apply(
        &mut state,
        &Command::Observe {
            writer: a,
            observation,
            applied_revision: 1,
        },
        &clock(2),
    )?;
    apply(
        &mut state,
        &Command::Reserve {
            controller: controller.clone(),
            key: "table".into(),
            target: b,
            operation: "move-b".into(),
        },
        &clock(2),
    )?;
    apply(
        &mut state,
        &Command::Transfer {
            controller: controller.clone(),
            key: "table".into(),
            operation: "move-b".into(),
            force: false,
        },
        &clock(2),
    )?;
    let approve = Command::ApproveRetirement {
        controller: controller.clone(),
        collector: "a".into(),
    };
    assert!(apply(&mut state.clone(), &approve, &clock(2)).is_err());
    apply(
        &mut state,
        &Command::Revoke {
            controller: controller.clone(),
            collector: "b".into(),
        },
        &clock(2),
    )?;
    let target = state.members["c"].member.writer.clone();
    apply(
        &mut state,
        &Command::Reserve {
            controller: controller.clone(),
            key: "table".into(),
            target,
            operation: "move-c".into(),
        },
        &clock(2),
    )?;
    apply(
        &mut state,
        &Command::Transfer {
            controller,
            key: "table".into(),
            operation: "move-c".into(),
            force: true,
        },
        &clock(2),
    )?;
    assert_eq!(state.tables["table"].record.epoch, 3);
    assert!(apply(&mut state.clone(), &approve, &clock(2)).is_err());
    let identity = state.tables["table"].identity.clone();
    apply(
        &mut state,
        &Command::Activate {
            table: "table".into(),
            identity,
            version: -1,
        },
        &clock(2),
    )?;
    apply(&mut state, &approve, &clock(2))?;
    assert!(state.policies["a"].approved);
    Ok(())
}

#[test]
fn measurement_watermark_does_not_double_charge_active_allocation() -> Result<()> {
    let (mut state, _, a, _) = setup()?;
    let reserved = state.projected("a", "table")?;
    let mut observation = state.observations["a"].clone();
    observation.sequence = 2;
    observation.tables.insert("table".into(), placement::Demand::default());
    observation.allocation_revision = state.tables["table"].activation_revision - 1;
    apply(
        &mut state,
        &Command::Observe {
            writer: a.clone(),
            observation: observation.clone(),
            applied_revision: 0,
        },
        &clock(2),
    )?;
    assert_eq!(state.projected("a", "table")?, reserved);
    observation.sequence = 3;
    observation.allocation_revision = state.tables["table"].activation_revision;
    apply(
        &mut state,
        &Command::Observe {
            writer: a,
            observation,
            applied_revision: 0,
        },
        &clock(2),
    )?;
    assert_eq!(state.projected("a", "table")?.cpu, 0.1);
    Ok(())
}

#[test]
fn publication_cannot_bypass_revisioned_schema_admission() -> Result<()> {
    let (mut state, controller, a, _) = setup()?;
    apply(
        &mut state,
        &Command::Admit {
            key: "table".into(),
            writer: a.clone(),
            epoch: 1,
            unit: "unrevisioned".into(),
        },
        &clock(2),
    )?;
    let unrevisioned = proposal(&state, "unrevisioned");
    assert!(apply(&mut state.clone(), &Command::Accept(unrevisioned), &clock(2)).is_err());
    admit(&mut state, &a, "valid")?;
    let valid = proposal(&state, "valid");
    let mut removed = state.clone();
    removed
        .tables
        .get_mut("table")
        .context("table")?
        .input_policy
        .remove("valid");
    assert!(apply(&mut removed, &Command::Accept(valid.clone()), &clock(2)).is_err());
    apply(
        &mut state,
        &Command::Catalog {
            controller,
            key: "table".into(),
            policy: placement::TablePolicy {
                revision: 2,
                fingerprints: vec!["different".into()],
                demand: placement::Demand::default(),
            },
        },
        &clock(2),
    )?;
    assert!(apply(&mut state, &Command::Accept(valid), &clock(2)).is_err());
    Ok(())
}
#[test]
fn expired_controller_process_and_local_generation_cannot_publish() -> Result<()> {
    let (state, c, a, _) = setup()?;
    let mut admitted = state.clone();
    admit(&mut admitted, &a, "one")?;
    let p = proposal(&admitted, "one");
    assert!(apply(&mut admitted.clone(), &Command::Accept(p.clone()), &clock(12002)).is_err());
    apply(
        &mut admitted,
        &Command::Replace {
            controller: c,
            writer: a.clone(),
        },
        &clock(3),
    )?;
    assert!(apply(&mut admitted, &Command::Accept(p), &clock(3)).is_err());
    assert!(admit(&mut admitted, &a, "two").is_err());
    Ok(())
}
#[test]
fn accepted_prefix_survives_transfer_and_blocks_activation_until_final() -> Result<()> {
    let (mut s, c, a, b) = setup()?;
    admit(&mut s, &a, "one")?;
    let p = proposal(&s, "one");
    apply(&mut s, &Command::Accept(p.clone()), &clock(2))?;
    apply(
        &mut s,
        &Command::Reserve {
            controller: c.clone(),
            key: "table".into(),
            target: b,
            operation: "move".into(),
        },
        &clock(3),
    )?;
    assert!(
        apply(
            &mut s.clone(),
            &Command::Transfer {
                controller: c.clone(),
                key: "table".into(),
                operation: "move".into(),
                force: false
            },
            &clock(3)
        )
        .is_err()
    );
    apply(
        &mut s,
        &Command::Revoke {
            controller: c.clone(),
            collector: "a".into(),
        },
        &clock(3),
    )?;
    apply(
        &mut s,
        &Command::Transfer {
            controller: c,
            key: "table".into(),
            operation: "move".into(),
            force: true,
        },
        &clock(4),
    )?;
    let activation = Command::Activate {
        table: "table".into(),
        identity: s.tables["table"].identity.clone(),
        version: 0,
    };
    assert!(apply(&mut s.clone(), &activation, &clock(4)).is_err());
    let mut stale = p.clone();
    stale.operation = "late".into();
    stale.predecessor = 0;
    stale.version = 1;
    assert!(apply(&mut s.clone(), &Command::Accept(stale), &clock(4)).is_err());
    apply(&mut s, &Command::Accept(p.clone()), &clock(4))?;
    apply(
        &mut s,
        &Command::Final {
            table: "table".into(),
            version: 0,
            digest: p.digest()?,
        },
        &clock(4),
    )?;
    apply(&mut s, &activation, &clock(4))?;
    assert_eq!(s.tables["table"].published, 0);
    Ok(())
}
#[test]
fn exact_operations_reconcile_and_conflicts_do_not_reallocate_versions() -> Result<()> {
    let (mut s, _, a, _) = setup()?;
    admit(&mut s, &a, "one")?;
    let bind = Command::Bind {
        key: "table".into(),
        writer: a.clone(),
        epoch: 1,
        unit: "one".into(),
        sha256: "a".repeat(64),
    };
    apply(&mut s, &bind, &clock(2))?;
    apply(&mut s, &bind, &clock(2))?;
    let collision = Command::Bind {
        key: "table".into(),
        writer: a,
        epoch: 1,
        unit: "one".into(),
        sha256: "b".repeat(64),
    };
    assert!(apply(&mut s.clone(), &collision, &clock(2)).is_err());
    let p = proposal(&s, "one");
    apply(&mut s, &Command::Accept(p.clone()), &clock(2))?;
    let revision = s.revision;
    apply(&mut s, &Command::Accept(p.clone()), &clock(2))?;
    assert_eq!(revision, s.revision);
    let mut other = p;
    other.actions = "{\"commitInfo\":{\"different\":true}}\n".into();
    assert!(apply(&mut s, &Command::Accept(other), &clock(2)).is_err());
    assert_eq!(s.tables["table"].decisions.len(), 1);
    Ok(())
}
#[test]
fn expired_process_never_revives_and_controller_restart_fences_tables() -> Result<()> {
    let (mut s, _, a, _) = setup()?;
    let old = s.tables["table"].identity.clone();
    apply(&mut s, &Command::Start { id: "successor".into() }, &clock(12002))?;
    let c = s.controller.clone().context("controller")?;
    let mut m = s.members["a"].member.clone();
    assert!(
        apply(
            &mut s.clone(),
            &Command::Register {
                controller: c.clone(),
                member: m.clone()
            },
            &clock(12002)
        )
        .is_err()
    );
    m.writer.generation.clear();
    assert!(
        apply(
            &mut s.clone(),
            &Command::Register {
                controller: c.clone(),
                member: m.clone()
            },
            &clock(12002)
        )
        .is_err()
    );
    m.writer.process = "new-process".into();
    apply(
        &mut s,
        &Command::Register {
            controller: c,
            member: m,
        },
        &clock(12002),
    )?;
    assert_ne!(s.members["a"].member.writer, a);
    assert_ne!(s.tables["table"].identity, old);
    assert_eq!(s.tables["table"].record.lifecycle, Lifecycle::Recovering);
    Ok(())
}
#[test]
fn dead_target_reservation_can_be_replaced_without_double_reserving() -> Result<()> {
    let (mut s, c, _, b) = setup()?;
    apply(
        &mut s,
        &Command::Reserve {
            controller: c.clone(),
            key: "table".into(),
            target: b,
            operation: "move".into(),
        },
        &clock(3),
    )?;
    let target = s.members["c"].member.writer.clone();
    let replace = Command::Reserve {
        controller: c.clone(),
        key: "table".into(),
        target,
        operation: "replacement".into(),
    };
    assert!(apply(&mut s.clone(), &replace, &clock(3)).is_err());
    apply(
        &mut s,
        &Command::Revoke {
            controller: c,
            collector: "b".into(),
        },
        &clock(3),
    )?;
    apply(&mut s, &replace, &clock(3))?;
    assert_eq!(
        s.tables["table"].reservation.as_ref().context("reservation")?.operation,
        "replacement"
    );
    Ok(())
}
#[test]
fn etcd_expiry_uses_live_keys_not_client_clock() -> Result<()> {
    let (mut s, _, a, _) = setup()?;
    admit(&mut s, &a, "one")?;
    let p = proposal(&s, "one");
    let live = [controller_key(), member_key("a")].into_iter().collect();
    apply(
        &mut s,
        &Command::Accept(p),
        &Clock {
            now: i64::MAX - 12000,
            live: Some(live),
            new_lease: 0,
        },
    )?;
    admit(&mut s, &a, "two")?;
    let p = proposal(&s, "two");
    let live = [controller_key()].into_iter().collect();
    assert!(
        apply(
            &mut s,
            &Command::Accept(p),
            &Clock {
                now: 0,
                live: Some(live),
                new_lease: 0
            }
        )
        .is_err()
    );
    Ok(())
}
#[test]
fn full_revisioned_snapshot_repairs_cache_gaps_and_removes_absent_entries() -> Result<()> {
    let (s, _, _, _) = setup()?;
    let mut directory = flight::Directory::default();
    directory.snapshot(s.revision, s.assignments())?;
    directory.snapshot(s.revision + 100, vec![])?;
    assert!(directory.entries.is_empty());
    directory.snapshot(s.revision, s.assignments())?;
    assert!(directory.entries.is_empty());
    assert_eq!(directory.revision, s.revision + 100);
    Ok(())
}
#[test]
fn legacy_configuration_defaults_to_manifest_and_etcd() -> Result<()> {
    let config = serde_json::json!({"tenant":"tenant","catalog_id":"catalog","tables":[],"storage":".",
        "controller":"http://control","operator_token":"operator","gateway_token":"gateway",
        "edge_secret":"e".repeat(32),"internal_secret":"i".repeat(32),"collectors":BTreeMap::<String,String>::new(),
        "etcd":[],"etcd_user":"","etcd_password":""});
    let config: Config = serde_json::from_value(config)?;
    assert!(config.delta.is_none());
    Ok(())
}

#[test]
fn overload_is_recorded_and_backpressures_new_placement() -> Result<()> {
    let (mut state, _, a, _) = setup()?;
    let mut observation = state.observations["a"].clone();
    observation.sequence += 1;
    observation.demand.cpu = 1.25;
    apply(
        &mut state,
        &Command::Observe {
            writer: a,
            observation,
            applied_revision: 0,
        },
        &clock(2),
    )?;
    assert_eq!(state.observations["a"].demand.cpu, 1.25);
    assert!(state.eligible("a", "new").is_err());
    Ok(())
}

#[test]
fn activation_rechecks_current_schema_capabilities_and_is_idempotent() -> Result<()> {
    let (mut state, controller, _, b) = setup()?;
    apply(
        &mut state,
        &Command::Reserve {
            controller: controller.clone(),
            key: "table".into(),
            target: b,
            operation: "move".into(),
        },
        &clock(2),
    )?;
    apply(
        &mut state,
        &Command::Transfer {
            controller,
            key: "table".into(),
            operation: "move".into(),
            force: false,
        },
        &clock(2),
    )?;
    let activate = Command::Activate {
        table: "table".into(),
        identity: state.tables["table"].identity.clone(),
        version: -1,
    };
    let mut incompatible = state.clone();
    incompatible
        .observations
        .get_mut("b")
        .context("observation")?
        .fingerprints
        .clear();
    assert!(apply(&mut incompatible, &activate, &clock(2)).is_err());
    assert!(apply(&mut state.clone(), &activate, &clock(4000)).is_err());
    apply(&mut state, &activate, &clock(2))?;
    let activation_revision = state.tables["table"].activation_revision;
    apply(&mut state, &activate, &clock(3))?;
    assert_eq!(state.tables["table"].activation_revision, activation_revision);
    assert_eq!(state.tables["table"].activated_at, 2);
    Ok(())
}
