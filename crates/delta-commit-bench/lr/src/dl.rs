//! delta-rs benches: one body compiled against 1.1.1 (`v1`) and main@460c904 (`v2`).

#[cfg(feature = "dl1")]
pub mod v1 {
    use dl1 as dlc;
    async fn create_checkpoint(t: &dlc::DeltaTable) -> dlc::DeltaResult<()> {
        dlc::checkpoints::create_checkpoint(t, None).await
    }
    include!("dl_common.rs");
}

#[cfg(feature = "dl2")]
pub mod v2 {
    use dl2 as dlc;
    async fn create_checkpoint(t: &dlc::DeltaTable) -> dlc::DeltaResult<()> {
        dlc::checkpoints::create_checkpoint(t).await
    }
    include!("dl_common.rs");
}
