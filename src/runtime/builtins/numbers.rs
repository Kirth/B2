use crate::runtime::value::Value;

pub fn get_method(name: &str, receiver: f64) -> Option<Value> {
    let method = match name {
        "abs" => native(move |_args| Ok(Value::Number(receiver.abs()))),
        "floor" => native(move |_args| Ok(Value::Number(receiver.floor()))),
        "ceil" => native(move |_args| Ok(Value::Number(receiver.ceil()))),
        "round" => native(move |_args| Ok(Value::Number(receiver.round()))),
        "min" => native(move |args| {
            let other = expect_number(args.get(0))?;
            Ok(Value::Number(receiver.min(other)))
        }),
        "max" => native(move |args| {
            let other = expect_number(args.get(0))?;
            Ok(Value::Number(receiver.max(other)))
        }),
        "clamp" => native(move |args| {
            let min = expect_number(args.get(0))?;
            let max = expect_number(args.get(1))?;
            Ok(Value::Number(receiver.clamp(min, max)))
        }),
        "pow" => native(move |args| {
            let exp = expect_number(args.get(0))?;
            Ok(Value::Number(receiver.powf(exp)))
        }),
        "sqrt" => native(move |_args| {
            if receiver < 0.0 {
                return Err("sqrt of negative number".to_string());
            }
            Ok(Value::Number(receiver.sqrt()))
        }),
        "toInt" => native(move |_args| Ok(Value::Number(receiver.trunc()))),
        "toFloat" => native(move |_args| Ok(Value::Number(receiver))),
        "isInt" => native(move |_args| Ok(Value::Bool(receiver.fract() == 0.0))),
        "toString" => native(move |_args| Ok(Value::String(receiver.to_string()))),
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

fn expect_number(value: Option<&Value>) -> Result<f64, String> {
    match value {
        Some(Value::Number(n)) => Ok(*n),
        Some(v) => v.as_string().parse::<f64>().map_err(|_| "Expected number argument".to_string()),
        None => Err("Expected number argument".to_string()),
    }
}
