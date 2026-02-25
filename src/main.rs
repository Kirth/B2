use v2r::cli::args::{CliArgs, CliCommand};
use v2r::cli::io::load_source;
use v2r::repl;
use v2r::run_script;

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

    if let Err(err) = run_script(&source, &filename) {
        eprintln!("{err}");
        std::process::exit(1);
    }
}
