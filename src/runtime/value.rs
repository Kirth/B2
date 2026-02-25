use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use super::executor::{Function, GeneratorHandle, TaskHandle};

#[derive(Clone)]
pub enum ModuleOrigin {
    Builtin,
    Script(String),
}

#[derive(Clone)]
pub struct ModuleValue {
    pub name: String,
    pub exports: HashMap<String, Value>,
    pub origin: ModuleOrigin,
}

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
    Nominal {
        name: String,
        inner: Box<Value>,
    },
    Task(Arc<TaskHandle>),
    Generator(Arc<GeneratorHandle>),
    Function(Arc<Function>),
    Module(Arc<ModuleValue>),
    NativeFunction(Arc<dyn Fn(Vec<Value>) -> Result<Value, String> + Send + Sync>),
    NativeFunctionExec(
        Arc<
            dyn Fn(
                    &mut crate::runtime::executor::Executor,
                    Vec<Value>,
                    crate::parser::ast::Span,
                ) -> Result<Value, crate::runtime::errors::RuntimeError>
                + Send
                + Sync,
        >,
    ),
    Ufcs {
        name: String,
        receiver: Box<Value>,
    },
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
            if c.is_ascii_digit() {
                idx += 1;
            } else {
                break;
            }
        }
        if idx == 0 || idx >= text.len() {
            return Err("Invalid duration literal".to_string());
        }
        let value: u64 = text[..idx]
            .parse()
            .map_err(|_| "Invalid duration value".to_string())?;
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
            Value::Nominal { inner, .. } => inner.is_truthy(),
            Value::Task(_) => true,
            Value::Generator(_) => true,
            Value::Function(_)
            | Value::Module(_)
            | Value::NativeFunction(_)
            | Value::NativeFunctionExec(_)
            | Value::Ufcs { .. } => true,
        }
    }

    pub fn as_string(&self) -> String {
        match self {
            Value::Null => "null".to_string(),
            Value::Bool(b) => b.to_string(),
            Value::Number(n) => {
                if n.fract() == 0.0 {
                    format!("{}", *n as i64)
                } else {
                    n.to_string()
                }
            }
            Value::String(s) => s.clone(),
            Value::Array(a) => {
                let items: Vec<String> = a
                    .lock()
                    .map(|v| v.iter().map(|x| x.as_string()).collect())
                    .unwrap_or_else(|_| Vec::new());
                format!("[{}]", items.join(", "))
            }
            Value::Tuple(t) => {
                let items: Vec<String> = t
                    .lock()
                    .map(|v| v.iter().map(|x| x.as_string()).collect())
                    .unwrap_or_else(|_| Vec::new());
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
            Value::Nominal { name, inner } => format!("{name}({})", inner.as_string()),
            Value::Task(_) => "<task>".to_string(),
            Value::Generator(_) => "<generator>".to_string(),
            Value::Function(_) => "<fn>".to_string(),
            Value::Module(module) => {
                let origin = match &module.origin {
                    ModuleOrigin::Builtin => "builtin".to_string(),
                    ModuleOrigin::Script(path) => format!("script:{path}"),
                };
                format!("<module {} ({origin})>", module.name)
            }
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

#[cfg(test)]
mod tests {
    use super::Value;

    #[test]
    fn value_is_truthy() {
        assert!(!Value::Null.is_truthy());
        assert!(!Value::Bool(false).is_truthy());
        assert!(Value::Bool(true).is_truthy());
        assert!(!Value::Number(0.0).is_truthy());
        assert!(Value::Number(1.0).is_truthy());
        assert!(!Value::String(String::new()).is_truthy());
        assert!(Value::String("x".to_string()).is_truthy());
    }

    #[test]
    fn value_as_string() {
        assert_eq!(Value::Null.as_string(), "null");
        assert_eq!(Value::Bool(true).as_string(), "true");
        assert_eq!(Value::Number(42.0).as_string(), "42");
        assert_eq!(Value::Number(3.14).as_string(), "3.14");
        assert_eq!(Value::String("hi".to_string()).as_string(), "hi");
        assert_eq!(Value::Range(1, 5).as_string(), "1..5");
    }

    #[test]
    fn value_parse_duration() {
        use std::time::Duration;
        assert_eq!(
            Value::parse_duration("100ms").unwrap(),
            Duration::from_millis(100)
        );
        assert_eq!(Value::parse_duration("2s").unwrap(), Duration::from_secs(2));
        assert!(Value::parse_duration("x").is_err());
    }

    #[test]
    fn value_array_as_string() {
        let arr = Value::array(vec![Value::Number(1.0), Value::Number(2.0)]);
        assert_eq!(arr.as_string(), "[1, 2]");
    }

    #[test]
    fn value_tuple_as_string() {
        let tup = Value::tuple(vec![Value::Bool(true), Value::String("x".to_string())]);
        assert_eq!(tup.as_string(), "(true, x)");
    }

    #[test]
    fn value_empty_array_truthy() {
        let empty = Value::array(vec![]);
        assert!(!empty.is_truthy());
        let non_empty = Value::array(vec![Value::Null]);
        assert!(non_empty.is_truthy());
    }
}
