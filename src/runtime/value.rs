use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::executor::Function;

#[derive(Clone)]
pub enum Value {
    Null,
    Bool(bool),
    Number(f64),
    String(String),
    Array(Arc<Mutex<Vec<Value>>>),
    Tuple(Arc<Mutex<Vec<Value>>>),
    Dict(Arc<Mutex<HashMap<String, Value>>>),
    Range(i64, i64),
    Duration(Duration),
    Function(Arc<Function>),
    NativeFunction(Arc<dyn Fn(Vec<Value>) -> Result<Value, String> + Send + Sync>),
    NativeFunctionExec(Arc<dyn Fn(&mut crate::runtime::executor::Executor, Vec<Value>, crate::parser::ast::Span) -> Result<Value, crate::runtime::errors::RuntimeError> + Send + Sync>),
    Ufcs { name: String, receiver: Box<Value> },
}

impl Value {
    pub fn array(items: Vec<Value>) -> Value {
        Value::Array(std::sync::Arc::new(std::sync::Mutex::new(items)))
    }
    pub fn tuple(items: Vec<Value>) -> Value {
        Value::Tuple(std::sync::Arc::new(std::sync::Mutex::new(items)))
    }
    pub fn parse_duration(text: &str) -> Result<Duration, String> {
        let mut idx = 0;
        for c in text.chars() {
            if c.is_ascii_digit() { idx += 1; } else { break; }
        }
        if idx == 0 || idx >= text.len() {
            return Err("Invalid duration literal".to_string());
        }
        let value: u64 = text[..idx].parse().map_err(|_| "Invalid duration value".to_string())?;
        let unit = &text[idx..];
        let dur = match unit {
            "ms" => Duration::from_millis(value),
            "s" => Duration::from_secs(value),
            "m" => Duration::from_secs(value * 60),
            "h" => Duration::from_secs(value * 3600),
            "d" => Duration::from_secs(value * 86400),
            _ => return Err("Invalid duration unit".to_string()),
        };
        Ok(dur)
    }

    pub fn is_truthy(&self) -> bool {
        match self {
            Value::Null => false,
            Value::Bool(b) => *b,
            Value::Number(n) => *n != 0.0,
            Value::String(s) => !s.is_empty(),
            Value::Array(a) => !a.lock().map(|v| v.is_empty()).unwrap_or(true),
            Value::Tuple(t) => !t.lock().map(|v| v.is_empty()).unwrap_or(true),
            Value::Dict(d) => !d.lock().map(|v| v.is_empty()).unwrap_or(true),
            Value::Range(_, _) => true,
            Value::Duration(d) => d.as_millis() != 0,
            Value::Function(_) | Value::NativeFunction(_) | Value::NativeFunctionExec(_) | Value::Ufcs { .. } => true,
        }
    }

    pub fn as_string(&self) -> String {
        match self {
            Value::Null => "null".to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Number(n) => {
                if n.fract() == 0.0 { format!("{}", *n as i64) } else { n.to_string() }
            }
            Value::String(s) => s.clone(),
            Value::Array(a) => {
                let items: Vec<String> = a.lock().map(|v| v.iter().map(|x| x.as_string()).collect()).unwrap_or_else(|_| Vec::new());
                format!("[{}]", items.join(", "))
            }
            Value::Tuple(t) => {
                let items: Vec<String> = t.lock().map(|v| v.iter().map(|x| x.as_string()).collect()).unwrap_or_else(|_| Vec::new());
                format!("({})", items.join(", "))
            }
            Value::Dict(d) => {
                let mut items = Vec::new();
                if let Ok(map) = d.lock() {
                    for (k, v) in map.iter() {
                        items.push(format!("{}: {}", k, v.as_string()));
                    }
                }
                format!("{{{}}}", items.join(", "))
            }
            Value::Range(a, b) => format!("{}..{}", a, b),
            Value::Duration(d) => format!("{}ms", d.as_millis()),
            Value::Function(_) => "<fn>".to_string(),
            Value::NativeFunction(_) | Value::NativeFunctionExec(_) => "<native fn>".to_string(),
            Value::Ufcs { name, .. } => format!("<ufcs {}>", name),
        }
    }
}

impl fmt::Debug for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.as_string())
    }
}
