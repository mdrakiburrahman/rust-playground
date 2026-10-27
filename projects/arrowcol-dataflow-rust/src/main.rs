use arrowcol_dataflow_components as _;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    otel_arrow_dfe::initialize_crypto_provider()?;
    let metrics = arrowcol_buffer::MetricsGuard::install("arrowcol-dataflow-rust")?;
    otel_arrow_dfe_telemetry::tracing_init::install_auxiliary_layer_factory(|| {
        Box::new(delta_kernel::metrics::ReportGeneratorLayer::new(
            arrow_deltalake::telemetry::kernel_metrics_reporter(),
        ))
    })
    .map_err(|_| "DFE metrics layer factory already installed")?;
    otel_arrow_dfe_telemetry::tracing_init::init_global_auxiliary_subscriber()?;
    arrowcol_buffer::CloudGeneration::initialize_from_environment()?;
    if let Some(generation) = arrowcol_buffer::CloudGeneration::process() {
        arrowcol_dataflow_components::configure_cloud_generation(generation.generation())?;
    }
    arrowcol_dataflow_components::initialize_shared_io()?;
    otel_arrow_dfe::run_with_shutdown_status(move |clean| {
        let closed = if clean {
            arrow_deltalake::publisher::PublisherSession::close_shared()
        } else {
            Ok(())
        };
        drop(metrics);
        if let Err(error) = closed {
            eprintln!("publisher session close failed: {error:#}");
            std::process::exit(1);
        }
    })
}
