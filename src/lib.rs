pub mod cli;
pub mod lexer;
pub mod parser;
pub mod repl;
pub mod runtime;
pub mod utils;

use std::sync::{Arc, Mutex};

/// Run a script with default behavior (log/echo go to stdout). Returns an error on parse or runtime failure.
pub fn run_script(source: &str, filename: &str) -> Result<(), String> {
    let mut scanner = lexer::scanner::Scanner::new(source);
    let tokens = scanner.scan_tokens()?;
    let mut parser = parser::parser::Parser::new(tokens, source, filename.to_string());
    let program = parser.parse_program()?;
    let mut executor = runtime::executor::Executor::new(filename.to_string(), source.to_string());
    runtime::builtins::register_builtins(&mut executor);
    executor.execute(&program).map_err(|e| e.to_string())?;
    Ok(())
}

/// Run a script and capture all log/echo output lines. Returns error on parse or runtime failure.
pub fn run_script_capture(source: &str) -> Result<Vec<String>, String> {
    let mut scanner = lexer::scanner::Scanner::new(source);
    let tokens = scanner.scan_tokens()?;
    let mut parser = parser::parser::Parser::new(tokens, source, "<test>".to_string());
    let program = parser.parse_program()?;
    let mut executor = runtime::executor::Executor::new("<test>".to_string(), source.to_string());
    runtime::builtins::register_builtins(&mut executor);
    let output = Arc::new(Mutex::new(Vec::<String>::new()));
    let out = output.clone();
    executor.set_output_sink(Some(Arc::new(move |line: String| {
        if let Ok(mut v) = out.lock() {
            v.push(line);
        }
    })));
    executor.execute(&program).map_err(|e| e.to_string())?;
    Ok(output.lock().map(|v| v.clone()).unwrap_or_default())
}
