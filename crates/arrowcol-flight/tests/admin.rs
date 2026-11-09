use std::{collections::BTreeSet, sync::Arc, time::Duration};

use anyhow::Result;
use arrowcol_flight::{
    AdminHandlers, CapabilityReporter, DrainController, DrainOutcome, HealthReporter, HealthStatus, PressureReporter,
    ReadinessReporter, ReadinessStatus, RuntimeResult,
};
use arrowcol_protocol::{
    BasisPoints, Capability, CollectorCapabilities, CollectorInstanceId, ImplementationPool, Measurement,
    PositiveMeasurement, PressureGauge, PressureSnapshot, UnixMillis,
};
use async_trait::async_trait;

#[tokio::test]
async fn admin_handlers_encode_health_readiness_capability_and_pressure() -> Result<()> {
    let instance_id = CollectorInstanceId::new();
    let capabilities = CollectorCapabilities::new(
        instance_id,
        ImplementationPool::Dataflow,
        PositiveMeasurement::new(1_024)?,
        PositiveMeasurement::new(8)?,
        BTreeSet::from([Capability::PressureReporting]),
    );
    let pressure = PressureSnapshot::new(
        instance_id,
        UnixMillis::new(1)?,
        PressureGauge::new(Measurement::new(1)?, PositiveMeasurement::new(8)?)?,
        PressureGauge::new(Measurement::new(128)?, PositiveMeasurement::new(1_024)?)?,
        PressureGauge::new(Measurement::new(2)?, PositiveMeasurement::new(32)?)?,
        BasisPoints::new(1_000)?,
        BasisPoints::new(2_000)?,
        true,
    );
    let status = Arc::new(TestStatus);
    let handlers = AdminHandlers::new(
        Arc::<TestStatus>::clone(&status),
        Arc::<TestStatus>::clone(&status),
        Arc::new(TestCapabilities(capabilities)),
        Arc::new(TestPressure(pressure)),
    );

    assert_eq!(handlers.health().status(), 200);
    assert_eq!(handlers.readiness().status(), 200);
    let capability = handlers.capabilities().await;
    assert_eq!(capability.status(), 200);
    assert!(std::str::from_utf8(capability.body())?.contains("\"pressure_reporting\""));
    let pressure = handlers.pressure().await;
    assert_eq!(pressure.status(), 200);
    assert!(std::str::from_utf8(pressure.body())?.contains("\"accepting_deliveries\":true"));
    Ok(())
}

#[tokio::test]
async fn drain_controller_stops_admission_and_reports_clean_drain() {
    let drain = DrainController::new();
    assert!(drain.is_accepting());
    assert_eq!(
        drain.graceful_stop(Duration::from_millis(10)).await,
        DrainOutcome::Drained
    );
    assert!(!drain.is_accepting());
    assert_eq!(drain.active_streams(), 0);
}

struct TestStatus;

impl HealthReporter for TestStatus {
    fn health(&self) -> HealthStatus {
        HealthStatus::healthy()
    }
}

impl ReadinessReporter for TestStatus {
    fn readiness(&self) -> ReadinessStatus {
        ReadinessStatus::new(true, true, 0, None)
    }
}

struct TestCapabilities(CollectorCapabilities);

#[async_trait]
impl CapabilityReporter for TestCapabilities {
    async fn capabilities(&self) -> RuntimeResult<CollectorCapabilities> {
        Ok(self.0.clone())
    }
}

struct TestPressure(PressureSnapshot);

#[async_trait]
impl PressureReporter for TestPressure {
    async fn pressure(&self) -> RuntimeResult<PressureSnapshot> {
        Ok(self.0.clone())
    }
}
