use anyhow::Result;
use dsync::cli::{actions, actions::Action, start};

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

    dsync::cli::telemetry::shutdown();

    Ok(())
}
