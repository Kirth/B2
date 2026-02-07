use std::fs;
use std::io::{self, Read};

use super::args::CliArgs;

pub fn load_source(args: &CliArgs) -> Result<(String, String), String> {
    if let Some(path) = &args.script_path {
        let src = fs::read_to_string(path)
            .map_err(|e| format!("Failed to read {path}: {e}"))?;
        return Ok((src, path.clone()));
    }

    let mut buf = String::new();
    io::stdin()
        .read_to_string(&mut buf)
        .map_err(|e| format!("Failed to read stdin: {e}"))?;

    Ok((buf, "<stdin>".to_string()))
}
