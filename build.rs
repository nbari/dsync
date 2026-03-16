fn main() {
    if let Err(e) = built::write_built_file() {
        eprintln!("cargo:warning=Failed to generate build info: {e}");
    }
}
