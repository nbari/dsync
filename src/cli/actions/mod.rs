pub mod run;

use std::path::PathBuf;

#[derive(Debug)]
pub enum Action {
    Run {
        src: PathBuf,
        dst: PathBuf,
        threshold: f32,
    },
}
