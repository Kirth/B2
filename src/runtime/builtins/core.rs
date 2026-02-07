use std::fs;
use std::thread;
use std::time::Duration;

use crate::runtime::executor::Executor;
use crate::runtime::errors::RuntimeError;
use crate::runtime::value::Value;

pub fn register(exec: &mut Executor) {
    exec.register_native("log", |args| {
        if let Some(v) = args.get(0) {
            println!("[{}] {}", chrono_now(), v.as_string());
        }
        Ok(Value::Null)
    });

    exec.register_native("echo", |args| {
        if let Some(v) = args.get(0) {
            println!("{}", v.as_string());
        }
        Ok(Value::Null)
    });

    exec.register_native("read_file", |args| {
        let path = match args.get(0) {
            Some(Value::String(s)) => s.clone(),
            Some(v) => v.as_string(),
            None => return Err("read_file expects path".to_string()),
        };
        let contents = fs::read_to_string(&path)
            .map_err(|e| format!("Failed to read {path}: {e}"))?;
        Ok(Value::String(contents))
    });

    exec.register_native("wait", |args| {
        if let Some(v) = args.get(0) {
            match v {
                Value::Number(n) => thread::sleep(Duration::from_millis(*n as u64)),
                Value::Duration(d) => thread::sleep(*d),
                _ => return Err("wait expects number or duration".to_string()),
            }
        }
        Ok(Value::Null)
    });

    exec.register_native_exec("map", |exec, args, span| {
        let (iterable, func) = expect_two(args, exec, span)?;
        let items = iter_values(iterable, exec, span)?;
        let mut out = Vec::new();
        for item in items {
            let result = exec.call_value(func.clone(), vec![item])?;
            out.push(result);
        }
        Ok(Value::array(out))
    });

    exec.register_native_exec("filter", |exec, args, span| {
        let (iterable, func) = expect_two(args, exec, span)?;
        let items = iter_values(iterable, exec, span)?;
        let mut out = Vec::new();
        for item in items {
            let result = exec.call_value(func.clone(), vec![item.clone()])?;
            if result.is_truthy() {
                out.push(item);
            }
        }
        Ok(Value::array(out))
    });

    exec.register_native_exec("reduce", |exec, args, span| {
        if args.len() < 3 {
            return Err(exec.make_error("reduce expects iterable, fn, initial (or iterable, initial, fn)", span));
        }
        let iterable = args[0].clone();
        let (func, mut acc) = match (&args[1], &args[2]) {
            (Value::Function(_) | Value::NativeFunction(_) | Value::NativeFunctionExec(_), _) => {
                (args[1].clone(), args[2].clone())
            }
            (_, Value::Function(_) | Value::NativeFunction(_) | Value::NativeFunctionExec(_)) => {
                (args[2].clone(), args[1].clone())
            }
            _ => return Err(exec.make_error("reduce requires a function argument", span)),
        };
        let items = iter_values(iterable, exec, span)?;
        for item in items {
            let result = exec.call_value(func.clone(), vec![acc, item])?;
            acc = result;
        }
        Ok(acc)
    });
}

fn chrono_now() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = now.as_secs() % 86400;
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    format!("{:02}:{:02}:{:02}", h, m, s)
}

fn expect_two(mut args: Vec<Value>, exec: &Executor, span: crate::parser::ast::Span) -> Result<(Value, Value), RuntimeError> {
    if args.len() < 2 {
        return Err(exec.make_error("Expected 2 arguments", span));
    }
    let second = args.pop().unwrap();
    let first = args.pop().unwrap();
    Ok((first, second))
}

fn iter_values(value: Value, exec: &Executor, span: crate::parser::ast::Span) -> Result<Vec<Value>, RuntimeError> {
    match value {
        Value::Array(arr) => Ok(arr.lock().map(|v| v.clone()).unwrap_or_else(|_| Vec::new())),
        Value::Tuple(tup) => Ok(tup.lock().map(|v| v.clone()).unwrap_or_else(|_| Vec::new())),
        Value::Range(a, b) => {
            let mut out = Vec::new();
            if a <= b {
                for i in a..=b { out.push(Value::Number(i as f64)); }
            } else {
                for i in (b..=a).rev() { out.push(Value::Number(i as f64)); }
            }
            Ok(out)
        }
        Value::Dict(map) => {
            let mut out = Vec::new();
            if let Ok(guard) = map.lock() {
                for (k, v) in guard.iter() {
                    out.push(Value::array(vec![Value::String(k.clone()), v.clone()]));
                }
            }
            Ok(out)
        }
        other => Err(exec.make_error(&format!("Not iterable: {}", type_name(&other)), span)),
    }
}

fn type_name(value: &Value) -> &'static str {
    match value {
        Value::Null => "Null",
        Value::Bool(_) => "Bool",
        Value::Number(_) => "Number",
        Value::String(_) => "String",
        Value::Array(_) => "Array",
        Value::Tuple(_) => "Tuple",
        Value::Dict(_) => "Dict",
        Value::Range(_, _) => "Range",
        Value::Duration(_) => "Duration",
        Value::Task(_) => "Task",
        Value::Function(_) => "Function",
        Value::NativeFunction(_) => "NativeFunction",
        Value::NativeFunctionExec(_) => "NativeFunctionExec",
        Value::Ufcs { .. } => "Ufcs",
    }
}
