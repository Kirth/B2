use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::runtime::value::Value;

pub struct ShellResult {
    pub stdout: String,
    pub stderr: String,
    pub exit_code: i32,
    pub duration: Duration,
}

impl ShellResult {
    pub fn to_value(&self) -> Value {
        let mut map = HashMap::new();
        map.insert("stdout".to_string(), Value::String(self.stdout.clone()));
        map.insert("stderr".to_string(), Value::String(self.stderr.clone()));
        map.insert("exit_code".to_string(), Value::Number(self.exit_code as f64));
        map.insert("duration_ms".to_string(), Value::Number(self.duration.as_millis() as f64));
        Value::Dict(Arc::new(Mutex::new(map)))
    }
}
