use crate::cli::actions::Action;
use crate::dsync::{sync, tools};
use anyhow::Result;
use tracing::info;
use tracing::instrument;

/// Handle the create action
#[instrument(skip(action))]
pub async fn handle(action: Action) -> Result<()> {
    let Action::Run {
        src,
        dst,
        threshold,
    } = action;

    info!(
        "src: {:?}, dst: {:?}, threshold: {:?}",
        &src, &dst, threshold
    );

    // check size of src and dst
    let copy = tools::should_copy_file(&src, &dst, threshold).await?;

    println!("Should copy file from {:?} to {:?}: {}", src, dst, copy);

    // get hash of src
    // let src_hash = tools::blake3(&src).await?;
    // println!("Hash of source file {:?}: {}", src, src_hash);

    if !copy {
        println!("Syncing changed blocks from {:?} to {:?}", src, dst);
        sync::sync_changed_blocks(&src, &dst).await?;
        println!("File copied from {:?} to {:?}", src, dst);
    }

    Ok(())
}
