use anyhow::Result;
use pxs::cli::{actions, actions::Action, start};

// Main function
#[tokio::main]
async fn main() -> Result<()> {
    // Start the program
    let action = start()?;

    match action {
        Action::Run { .. }
        | Action::Listen { .. }
        | Action::ListenSender { .. }
        | Action::Connect { .. }
        | Action::Pull { .. }
        | Action::Stdio { .. }
        | Action::StdioSender { .. } => {
            actions::run::handle(action).await?;
        }
    }

    pxs::cli::telemetry::shutdown();

    Ok(())
}
