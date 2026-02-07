mod cli;
mod lexer;
mod parser;
mod repl;
mod runtime;
mod utils;

use cli::args::{CliArgs, CliCommand};
use cli::io::load_source;
use runtime::builtins::register_builtins;
use runtime::executor::Executor;

fn main() {
    let args = CliArgs::parse();
    if args.command == CliCommand::Repl {
        if let Err(err) = repl::run() {
            eprintln!("{err}");
            std::process::exit(1);
        }
        return;
    }

    let (source, filename) = match load_source(&args) {
        Ok(v) => v,
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(1);
        }
    };

    let mut scanner = lexer::scanner::Scanner::new(&source);
    let tokens = match scanner.scan_tokens() {
        Ok(t) => t,
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(1);
        }
    };

    let mut parser = parser::parser::Parser::new(tokens, &source, filename.clone());
    let program = match parser.parse_program() {
        Ok(p) => p,
        Err(err) => {
            eprintln!("{err}");
            std::process::exit(1);
        }
    };

    let mut executor = Executor::new(filename, source);
    register_builtins(&mut executor);

    if let Err(err) = executor.execute(&program) {
        eprintln!("{err}");
        std::process::exit(1);
    }
}
