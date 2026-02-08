use std::cmp::Ordering;
use std::collections::{HashMap, HashSet};
use std::fs;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use crate::runtime::executor::{Executor, RuntimeCallArg};
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

    exec.register_native("getType", |args| {
        if args.len() != 1 {
            return Err("getType expects exactly 1 argument".to_string());
        }
        Ok(Value::String(value_type_label(&args[0])))
    });

    exec.register_native("unwrap", |args| {
        if args.len() != 1 {
            return Err("unwrap expects exactly 1 argument".to_string());
        }
        match &args[0] {
            Value::Nominal { inner, .. } => Ok((**inner).clone()),
            _ => Err("unwrap expects a nominal type value".to_string()),
        }
    });

    exec.register_native_exec("validateType", |exec, args, span| {
        if args.len() != 2 {
            return Err(exec.make_error("validateType expects (value, typeName)", span));
        }
        let type_name = match &args[1] {
            Value::String(s) => s.clone(),
            other => other.as_string(),
        };
        let ok = exec
            .validate_type_by_name(&args[0], &type_name)
            .map_err(|e| exec.make_error(&e, span))?;
        Ok(Value::Bool(ok))
    });

    exec.register_native_exec("map", |exec, args, span| {
        let (iterable, func) = expect_two(args, exec, span)?;
        let items = iter_values(iterable, exec, span)?;
        let mut out = Vec::new();
        for item in items {
            let result = exec.call_value(func.clone(), vec![RuntimeCallArg::Positional(item)])?;
            out.push(result);
        }
        Ok(Value::array(out))
    });

    exec.register_native_exec("filter", |exec, args, span| {
        let (iterable, func) = expect_two(args, exec, span)?;
        let items = iter_values(iterable, exec, span)?;
        let mut out = Vec::new();
        for item in items {
            let result = exec.call_value(func.clone(), vec![RuntimeCallArg::Positional(item.clone())])?;
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
            let result = exec.call_value(
                func.clone(),
                vec![RuntimeCallArg::Positional(acc), RuntimeCallArg::Positional(item)],
            )?;
            acc = result;
        }
        Ok(acc)
    });

    exec.register_native_exec("find", |exec, args, span| {
        let (iterable, func) = expect_two(args, exec, span)?;
        expect_callable(&func, exec, span, "find")?;
        let items = iter_values(iterable, exec, span)?;
        for item in items {
            if call_unary(exec, &func, item.clone())?.is_truthy() {
                return Ok(item);
            }
        }
        Ok(Value::Null)
    });

    exec.register_native_exec("findIndex", |exec, args, span| {
        let (iterable, func) = expect_two(args, exec, span)?;
        expect_callable(&func, exec, span, "findIndex")?;
        let items = iter_values(iterable, exec, span)?;
        for (idx, item) in items.into_iter().enumerate() {
            if call_unary(exec, &func, item)?.is_truthy() {
                return Ok(Value::Number(idx as f64));
            }
        }
        Ok(Value::Number(-1.0))
    });

    exec.register_native_exec("any", |exec, args, span| {
        let (iterable, func) = expect_two(args, exec, span)?;
        expect_callable(&func, exec, span, "any")?;
        let items = iter_values(iterable, exec, span)?;
        for item in items {
            if call_unary(exec, &func, item)?.is_truthy() {
                return Ok(Value::Bool(true));
            }
        }
        Ok(Value::Bool(false))
    });

    exec.register_native_exec("all", |exec, args, span| {
        let (iterable, func) = expect_two(args, exec, span)?;
        expect_callable(&func, exec, span, "all")?;
        let items = iter_values(iterable, exec, span)?;
        for item in items {
            if !call_unary(exec, &func, item)?.is_truthy() {
                return Ok(Value::Bool(false));
            }
        }
        Ok(Value::Bool(true))
    });

    exec.register_native_exec("flatMap", |exec, args, span| {
        let (iterable, func) = expect_two(args, exec, span)?;
        expect_callable(&func, exec, span, "flatMap")?;
        let items = iter_values(iterable, exec, span)?;
        let mut out = Vec::new();
        for item in items {
            let mapped = call_unary(exec, &func, item)?;
            match mapped {
                Value::Array(arr) => {
                    if let Ok(vals) = arr.lock() {
                        out.extend(vals.iter().cloned());
                    }
                }
                Value::Tuple(tup) => {
                    if let Ok(vals) = tup.lock() {
                        out.extend(vals.iter().cloned());
                    }
                }
                v => out.push(v),
            }
        }
        Ok(Value::array(out))
    });

    exec.register_native_exec("sort", |exec, args, span| {
        if args.is_empty() {
            return Err(exec.make_error("sort expects iterable[, comparator]", span));
        }
        let iterable = args[0].clone();
        let comparator = args.get(1).cloned();
        if args.len() > 2 {
            return Err(exec.make_error("sort expects iterable[, comparator]", span));
        }
        if let Some(cmp) = &comparator {
            expect_callable(cmp, exec, span, "sort")?;
        }

        let mut items = iter_values(iterable, exec, span)?;
        if let Some(cmp) = comparator {
            fallible_sort_by(&mut items, |a, b| {
                let r = call_binary(exec, &cmp, a.clone(), b.clone())?;
                let n = expect_number(&r, exec, span, "sort comparator")?;
                Ok(if n < 0.0 {
                    Ordering::Less
                } else if n > 0.0 {
                    Ordering::Greater
                } else {
                    Ordering::Equal
                })
            })?;
        } else {
            fallible_sort_by(&mut items, |a, b| Ok(compare_values(a, b)))?;
        }
        Ok(Value::array(items))
    });

    exec.register_native_exec("sortBy", |exec, args, span| {
        if args.len() < 2 || args.len() > 3 {
            return Err(exec.make_error("sortBy expects iterable, selector[, descending]", span));
        }
        let iterable = args[0].clone();
        let selector = args[1].clone();
        expect_callable(&selector, exec, span, "sortBy")?;
        let descending = if let Some(v) = args.get(2) {
            v.is_truthy()
        } else {
            false
        };

        let items = iter_values(iterable, exec, span)?;
        let mut decorated: Vec<(Value, Value)> = Vec::new();
        for item in items {
            let key = call_unary(exec, &selector, item.clone())?;
            decorated.push((item, key));
        }
        fallible_sort_by(&mut decorated, |a, b| Ok(compare_values(&a.1, &b.1)))?;
        if descending {
            decorated.reverse();
        }
        Ok(Value::array(decorated.into_iter().map(|(item, _)| item).collect()))
    });

    exec.register_native_exec("groupBy", |exec, args, span| {
        let (iterable, selector) = expect_two(args, exec, span)?;
        expect_callable(&selector, exec, span, "groupBy")?;
        let items = iter_values(iterable, exec, span)?;
        let mut grouped: HashMap<String, (String, Vec<Value>)> = HashMap::new();
        for item in items {
            let key_value = call_unary(exec, &selector, item.clone())?;
            let hash_key = as_hash_key(&key_value, exec, span, "groupBy")?;
            let display_key = key_value.as_string();
            grouped
                .entry(hash_key)
                .and_modify(|(_, bucket)| bucket.push(item.clone()))
                .or_insert_with(|| (display_key, vec![item]));
        }

        let mut out: HashMap<String, Value> = HashMap::new();
        for (_, (display_key, values)) in grouped {
            out.insert(display_key, Value::array(values));
        }
        Ok(Value::Dict(Arc::new(Mutex::new(out))))
    });

    exec.register_native_exec("partition", |exec, args, span| {
        let (iterable, predicate) = expect_two(args, exec, span)?;
        expect_callable(&predicate, exec, span, "partition")?;
        let items = iter_values(iterable, exec, span)?;
        let mut left = Vec::new();
        let mut right = Vec::new();
        for item in items {
            if call_unary(exec, &predicate, item.clone())?.is_truthy() {
                left.push(item);
            } else {
                right.push(item);
            }
        }
        Ok(Value::tuple(vec![Value::array(left), Value::array(right)]))
    });

    exec.register_native_exec("distinct", |exec, args, span| {
        if args.is_empty() || args.len() > 2 {
            return Err(exec.make_error("distinct expects iterable[, selector]", span));
        }
        let iterable = args[0].clone();
        let selector = args.get(1).cloned();
        if let Some(sel) = &selector {
            expect_callable(sel, exec, span, "distinct")?;
        }

        let items = iter_values(iterable, exec, span)?;
        let mut seen = HashSet::new();
        let mut out = Vec::new();
        for item in items {
            let key_val = if let Some(sel) = &selector {
                call_unary(exec, sel, item.clone())?
            } else {
                item.clone()
            };
            let key = as_hash_key(&key_val, exec, span, "distinct")?;
            if seen.insert(key) {
                out.push(item);
            }
        }
        Ok(Value::array(out))
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

fn expect_callable(value: &Value, exec: &Executor, span: crate::parser::ast::Span, name: &str) -> Result<(), RuntimeError> {
    match value {
        Value::Function(_) | Value::NativeFunction(_) | Value::NativeFunctionExec(_) => Ok(()),
        _ => Err(exec.make_error(&format!("{name} expects a callable argument"), span)),
    }
}

fn call_unary(exec: &mut Executor, func: &Value, arg: Value) -> Result<Value, RuntimeError> {
    exec.call_value(func.clone(), vec![RuntimeCallArg::Positional(arg)])
}

fn call_binary(exec: &mut Executor, func: &Value, a: Value, b: Value) -> Result<Value, RuntimeError> {
    exec.call_value(
        func.clone(),
        vec![RuntimeCallArg::Positional(a), RuntimeCallArg::Positional(b)],
    )
}

fn expect_number(value: &Value, exec: &Executor, span: crate::parser::ast::Span, where_: &str) -> Result<f64, RuntimeError> {
    match value {
        Value::Number(n) => Ok(*n),
        _ => Err(exec.make_error(&format!("{where_} must return number"), span)),
    }
}

fn compare_values(a: &Value, b: &Value) -> Ordering {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => x.partial_cmp(y).unwrap_or(Ordering::Equal),
        (Value::String(x), Value::String(y)) => x.cmp(y),
        (Value::Bool(x), Value::Bool(y)) => x.cmp(y),
        _ => a.as_string().cmp(&b.as_string()),
    }
}

fn as_hash_key(value: &Value, exec: &Executor, span: crate::parser::ast::Span, where_: &str) -> Result<String, RuntimeError> {
    match value {
        Value::Null => Ok("null:null".to_string()),
        Value::Bool(b) => Ok(format!("bool:{b}")),
        Value::Number(n) => Ok(format!("num:{n}")),
        Value::String(s) => Ok(format!("string:{s}")),
        _ => Err(exec.make_error(
            &format!("{where_} key must be hashable primitive (null/bool/num/string)"),
            span,
        )),
    }
}

fn fallible_sort_by<T, F>(items: &mut [T], mut cmp: F) -> Result<(), RuntimeError>
where
    F: FnMut(&T, &T) -> Result<Ordering, RuntimeError>,
{
    let len = items.len();
    for i in 1..len {
        let mut j = i;
        while j > 0 {
            let ord = cmp(&items[j - 1], &items[j])?;
            if ord == Ordering::Greater {
                items.swap(j - 1, j);
                j -= 1;
            } else {
                break;
            }
        }
    }
    Ok(())
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
        Value::Nominal { .. } => "Nominal",
        Value::Task(_) => "Task",
        Value::Function(_) => "Function",
        Value::NativeFunction(_) => "NativeFunction",
        Value::NativeFunctionExec(_) => "NativeFunctionExec",
        Value::Ufcs { .. } => "Ufcs",
    }
}

fn value_type_label(value: &Value) -> String {
    match value {
        Value::Null => "null".to_string(),
        Value::Bool(_) => "bool".to_string(),
        Value::Number(_) => "num".to_string(),
        Value::String(_) => "string".to_string(),
        Value::Array(_) => "array".to_string(),
        Value::Tuple(_) => "tuple".to_string(),
        Value::Dict(_) => "map".to_string(),
        Value::Range(_, _) => "range".to_string(),
        Value::Duration(_) => "duration".to_string(),
        Value::Nominal { name, .. } => format!("type<{name}>"),
        Value::Task(_) => "task".to_string(),
        Value::Function(_) | Value::NativeFunction(_) | Value::NativeFunctionExec(_) => "fn".to_string(),
        Value::Ufcs { .. } => "ufcs".to_string(),
    }
}
