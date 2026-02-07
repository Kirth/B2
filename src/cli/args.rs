use std::env;

#[derive(Debug, Clone)]
pub struct CliArgs {
    pub command: CliCommand,
    pub script_path: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CliCommand {
    Run,
    Repl,
}

impl CliArgs {
    pub fn parse() -> Self {
        let mut args = env::args().skip(1);
        let first = args.next();
        if matches!(first.as_deref(), Some("repl")) {
            return CliArgs {
                command: CliCommand::Repl,
                script_path: None,
            };
        }

        CliArgs {
            command: CliCommand::Run,
            script_path: first,
        }
    }
}
