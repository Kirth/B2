use std::env;

#[derive(Debug, Clone)]
pub struct CliArgs {
    pub script_path: Option<String>,
}

impl CliArgs {
    pub fn parse() -> Self {
        let mut args = env::args().skip(1);
        let script_path = args.next();
        CliArgs { script_path }
    }
}
