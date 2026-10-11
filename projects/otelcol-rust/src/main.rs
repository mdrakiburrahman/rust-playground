use otel_deltalake_exporter as _;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    otel_arrow_dfe::run()
}
