use std::{env, fs, path::PathBuf};

fn built_file_path() -> Result<PathBuf, env::VarError> {
    Ok(PathBuf::from(env::var("OUT_DIR")?).join("built.rs"))
}

fn sanitize_built_docs() -> std::io::Result<()> {
    let built_file = built_file_path().map_err(std::io::Error::other)?;
    let contents = fs::read_to_string(&built_file)?;
    let sanitized = contents
        .replace("Value of OPT_LEVEL", "Value of `OPT_LEVEL`")
        .replace("Value of DEBUG", "Value of `DEBUG`");

    if sanitized != contents {
        fs::write(built_file, sanitized)?;
    }

    Ok(())
}

fn main() {
    if let Err(e) = built::write_built_file().and_then(|()| sanitize_built_docs()) {
        eprintln!("cargo:warning=Failed to generate build info: {e}");
    }
}
