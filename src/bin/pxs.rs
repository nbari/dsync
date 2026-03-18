use anyhow::Result;
use pxs::cli::{actions, start};

// Main function
#[tokio::main]
async fn main() -> Result<()> {
    let action = start()?;
    actions::run::handle(action).await?;

    pxs::cli::telemetry::shutdown();

    Ok(())
}
