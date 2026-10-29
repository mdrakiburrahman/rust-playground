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
    let a = state.members["a"].member.writer.clone();
    let b = state.members["b"].member.writer.clone();
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
    Ok((state, c, a, b))
}
fn admit(state: &mut Snapshot, writer: &Writer, unit: &str) -> Result<()> {
    apply(
        state,
        &Command::Admit {
            key: "table".into(),
            writer: writer.clone(),
            epoch: state.tables["table"].record.epoch,
            unit: unit.into(),
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
