pub mod core;
pub mod net;
pub mod numbers;
pub mod strings;

use crate::runtime::executor::Executor;

pub fn register_builtins(exec: &mut Executor) {
    core::register(exec);
    net::register(exec);
    exec.freeze_prelude();
}
