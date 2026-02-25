use std::process::Command;
use std::time::Instant;

use super::result::ShellResult;

pub fn run_sh(command: &str) -> Result<ShellResult, String> {
    let start = Instant::now();
    let output = Command::new("bash")
        .arg("-c")
        .arg(command)
        .output()
        .map_err(|e| format!("Failed to run shell: {e}"))?;

    let duration = start.elapsed();
    let stdout = String::from_utf8_lossy(&output.stdout)
        .trim_end()
        .to_string();
    let stderr = String::from_utf8_lossy(&output.stderr)
        .trim_end()
        .to_string();
    let exit_code = output.status.code().unwrap_or(-1);

    Ok(ShellResult {
        stdout,
        stderr,
        exit_code,
        duration,
    })
}
