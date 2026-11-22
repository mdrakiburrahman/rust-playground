#[path = "../tests/darwin_commit_pipeline.rs"]
mod fixture;

#[tokio::main(flavor = "multi_thread", worker_threads = 2)]
async fn main() -> anyhow::Result<()> {
    anyhow::ensure!(
        std::env::args_os().len() == 1,
        "this fixed component case takes no arguments"
    );
    let result = fixture::continuous_component().await?;
    println!("{}", serde_json::to_string(&result)?);
    Ok(())
}
