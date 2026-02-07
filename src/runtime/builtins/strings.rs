use regex::Regex;

use crate::runtime::value::Value;
use std::sync::{Arc, Mutex};

pub fn get_method(name: &str, receiver: &str) -> Option<Value> {
    let receiver = receiver.to_string();
    let method = match name {
        "Length" => native(move |_args| Ok(Value::Number(receiver.chars().count() as f64))),
        "ToUpper" => native(move |_args| Ok(Value::String(receiver.to_uppercase()))),
        "ToLower" => native(move |_args| Ok(Value::String(receiver.to_lowercase()))),
        "Trim" => native(move |_args| Ok(Value::String(receiver.trim().to_string()))),
        "Split" => native(move |args| {
            let sep = expect_string(args.get(0))?;
            if sep.is_empty() {
                let parts: Vec<Value> = receiver.chars().map(|c| Value::String(c.to_string())).collect();
                Ok(Value::Array(Arc::new(Mutex::new(parts))))
            } else {
                let parts: Vec<Value> = receiver.split(&sep).map(|s| Value::String(s.to_string())).collect();
                Ok(Value::Array(Arc::new(Mutex::new(parts))))
            }
        }),
        "Lines" | "lines" => native(move |_args| {
            let parts: Vec<Value> = receiver
                .lines()
                .map(|s| Value::String(s.to_string()))
                .collect();
            Ok(Value::Array(Arc::new(Mutex::new(parts))))
        }),
        "Contains" => native(move |args| {
            let sub = expect_string(args.get(0))?;
            Ok(Value::Bool(receiver.contains(&sub)))
        }),
        "StartsWith" => native(move |args| {
            let sub = expect_string(args.get(0))?;
            Ok(Value::Bool(receiver.starts_with(&sub)))
        }),
        "EndsWith" => native(move |args| {
            let sub = expect_string(args.get(0))?;
            Ok(Value::Bool(receiver.ends_with(&sub)))
        }),
        "Replace" => native(move |args| {
            let from = expect_string(args.get(0))?;
            let to = expect_string(args.get(1))?;
            Ok(Value::String(receiver.replace(&from, &to)))
        }),
        "Substring" => native(move |args| {
            let start = expect_usize(args.get(0))?;
            let len = expect_usize(args.get(1))?;
            let chars: Vec<char> = receiver.chars().collect();
            if start >= chars.len() { return Ok(Value::String("".to_string())); }
            let end = (start + len).min(chars.len());
            Ok(Value::String(chars[start..end].iter().collect()))
        }),
        "IndexOf" => native(move |args| {
            let sub = expect_string(args.get(0))?;
            match receiver.find(&sub) {
                Some(idx) => Ok(Value::Number(idx as f64)),
                None => Ok(Value::Number(-1.0)),
            }
        }),
        "Join" => native(move |args| {
            let arr = expect_array(args.get(0))?;
            let parts: Vec<String> = arr.iter().map(|v| v.as_string()).collect();
            Ok(Value::String(parts.join(&receiver)))
        }),
        "PadLeft" => native(move |args| {
            let total = expect_usize(args.get(0))?;
            let ch = expect_string_opt(args.get(1)).unwrap_or(" ".to_string());
            let pad_char = ch.chars().next().unwrap_or(' ');
            let mut out = receiver.clone();
            while out.chars().count() < total {
                out.insert(0, pad_char);
            }
            Ok(Value::String(out))
        }),
        "PadRight" => native(move |args| {
            let total = expect_usize(args.get(0))?;
            let ch = expect_string_opt(args.get(1)).unwrap_or(" ".to_string());
            let pad_char = ch.chars().next().unwrap_or(' ');
            let mut out = receiver.clone();
            while out.chars().count() < total {
                out.push(pad_char);
            }
            Ok(Value::String(out))
        }),
        "Remove" => native(move |args| {
            let start = expect_usize(args.get(0))?;
            let len = expect_usize(args.get(1))?;
            let chars: Vec<char> = receiver.chars().collect();
            if start >= chars.len() { return Ok(Value::String(receiver.clone())); }
            let end = (start + len).min(chars.len());
            let mut out = String::new();
            out.push_str(&chars[..start].iter().collect::<String>());
            out.push_str(&chars[end..].iter().collect::<String>());
            Ok(Value::String(out))
        }),
        "Slice" => native(move |args| {
            let start = expect_isize(args.get(0))?;
            let end = expect_isize(args.get(1))?;
            let chars: Vec<char> = receiver.chars().collect();
            let len = chars.len() as isize;
            let mut s = if start < 0 { len + start } else { start };
            let mut e = if end < 0 { len + end } else { end };
            if s < 0 { s = 0; }
            if e < s { e = s; }
            if s as usize >= chars.len() { return Ok(Value::String("".to_string())); }
            let e_usize = (e as usize).min(chars.len());
            Ok(Value::String(chars[s as usize..e_usize].iter().collect()))
        }),
        "RegexMatch" => native(move |args| {
            let pattern = expect_string(args.get(0))?;
            let re = Regex::new(&pattern).map_err(|e| format!("Invalid regex: {e}"))?;
            Ok(Value::Bool(re.is_match(&receiver)))
        }),
        _ => return None,
    };

    Some(method)
}

fn native<F>(f: F) -> Value
where
    F: Fn(Vec<Value>) -> Result<Value, String> + Send + Sync + 'static,
{
    Value::NativeFunction(std::sync::Arc::new(f))
}

fn expect_string(value: Option<&Value>) -> Result<String, String> {
    match value {
        Some(Value::String(s)) => Ok(s.clone()),
        Some(v) => Ok(v.as_string()),
        None => Err("Expected string argument".to_string()),
    }
}

fn expect_string_opt(value: Option<&Value>) -> Option<String> {
    value.map(|v| v.as_string())
}

fn expect_usize(value: Option<&Value>) -> Result<usize, String> {
    match value {
        Some(Value::Number(n)) => Ok(*n as usize),
        Some(v) => v.as_string().parse::<usize>().map_err(|_| "Expected integer argument".to_string()),
        None => Err("Expected integer argument".to_string()),
    }
}

fn expect_isize(value: Option<&Value>) -> Result<isize, String> {
    match value {
        Some(Value::Number(n)) => Ok(*n as isize),
        Some(v) => v.as_string().parse::<isize>().map_err(|_| "Expected integer argument".to_string()),
        None => Err("Expected integer argument".to_string()),
    }
}

fn expect_array(value: Option<&Value>) -> Result<Vec<Value>, String> {
    match value {
        Some(Value::Array(arr)) => Ok(arr.lock().map(|v| v.clone()).unwrap_or_else(|_| Vec::new())),
        _ => Err("Expected array argument".to_string()),
    }
}
