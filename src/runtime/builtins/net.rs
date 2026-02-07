use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream, ToSocketAddrs};
use std::process::Command;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use crate::runtime::errors::RuntimeError;
use crate::runtime::executor::Executor;
use crate::runtime::value::Value;

const DEFAULT_TIMEOUT_MS: u64 = 5000;

pub fn register(exec: &mut Executor) {
    let mut net = HashMap::new();
    net.insert("ping".to_string(), native_exec(net_ping));
    net.insert("http".to_string(), native_exec(net_http));
    net.insert("tcp_connect".to_string(), native_exec(net_tcp_connect));
    net.insert("tcp_listen".to_string(), native_exec(net_tcp_listen));
    exec.register_value("net", Value::Dict(Arc::new(Mutex::new(net))));
}

fn native_exec<F>(f: F) -> Value
where
    F: Fn(&mut Executor, Vec<Value>, crate::parser::ast::Span) -> Result<Value, RuntimeError> + Send + Sync + 'static,
{
    Value::NativeFunctionExec(Arc::new(f))
}

fn err(exec: &Executor, span: crate::parser::ast::Span, msg: &str) -> RuntimeError {
    exec.make_error(msg, span)
}

fn value_dict(map: HashMap<String, Value>) -> Value {
    Value::Dict(Arc::new(Mutex::new(map)))
}

fn expect_string(value: Option<&Value>, name: &str) -> Result<String, String> {
    match value {
        Some(Value::String(s)) => Ok(s.clone()),
        Some(v) => Ok(v.as_string()),
        None => Err(format!("Expected {name}")),
    }
}

fn expect_number(value: Option<&Value>, name: &str) -> Result<f64, String> {
    match value {
        Some(Value::Number(n)) => Ok(*n),
        Some(v) => v.as_string().parse::<f64>().map_err(|_| format!("Expected numeric {name}")),
        None => Err(format!("Expected numeric {name}")),
    }
}

fn optional_timeout_ms(value: Option<&Value>) -> Result<Duration, String> {
    let ms = match value {
        Some(v) => expect_number(Some(v), "timeout_ms")? as i64,
        None => DEFAULT_TIMEOUT_MS as i64,
    };
    if ms < 0 {
        return Err("timeout_ms must be >= 0".to_string());
    }
    Ok(Duration::from_millis(ms as u64))
}

fn dict_get<'a>(dict: &'a HashMap<String, Value>, key: &str) -> Option<&'a Value> {
    dict.get(key)
}

fn value_to_bytes(value: &Value) -> Result<Vec<u8>, String> {
    match value {
        Value::String(s) => Ok(s.as_bytes().to_vec()),
        Value::Array(arr) => {
            let items = arr.lock().map_err(|_| "Array lock poisoned".to_string())?;
            let mut out = Vec::with_capacity(items.len());
            for v in items.iter() {
                match v {
                    Value::Number(n) => {
                        if *n < 0.0 || *n > 255.0 {
                            return Err("Byte array elements must be in range 0..255".to_string());
                        }
                        out.push(*n as u8);
                    }
                    _ => return Err("Byte array must contain only numbers".to_string()),
                }
            }
            Ok(out)
        }
        _ => Err("Expected string or array<num> payload".to_string()),
    }
}

fn bytes_to_value_array(bytes: &[u8]) -> Value {
    Value::array(bytes.iter().map(|b| Value::Number(*b as f64)).collect())
}

fn parse_ping_latency_ms(stdout: &str) -> Option<f64> {
    if let Some(idx) = stdout.find("time=") {
        let rem = &stdout[idx + 5..];
        let mut num = String::new();
        for ch in rem.chars() {
            if ch.is_ascii_digit() || ch == '.' {
                num.push(ch);
            } else {
                break;
            }
        }
        if let Ok(v) = num.parse::<f64>() {
            return Some(v);
        }
    }
    None
}

fn net_ping(exec: &mut Executor, args: Vec<Value>, span: crate::parser::ast::Span) -> Result<Value, RuntimeError> {
    let host = expect_string(args.get(0), "host").map_err(|e| err(exec, span, &e))?;
    let timeout = optional_timeout_ms(args.get(1)).map_err(|e| err(exec, span, &e))?;
    let timeout_secs = timeout.as_secs().max(1);

    let start = Instant::now();
    let output = Command::new("ping")
        .arg("-n")
        .arg("-c")
        .arg("1")
        .arg("-W")
        .arg(timeout_secs.to_string())
        .arg(&host)
        .output()
        .map_err(|e| err(exec, span, &format!("Failed to run ping: {e}")))?;
    let duration = start.elapsed();
    let stdout = String::from_utf8_lossy(&output.stdout).trim_end().to_string();
    let stderr = String::from_utf8_lossy(&output.stderr).trim_end().to_string();
    let exit_code = output.status.code().unwrap_or(-1);

    let mut out = HashMap::new();
    out.insert("host".to_string(), Value::String(host));
    out.insert("ok".to_string(), Value::Bool(exit_code == 0));
    out.insert("exit_code".to_string(), Value::Number(exit_code as f64));
    if let Some(ms) = parse_ping_latency_ms(&stdout) {
        out.insert("latency_ms".to_string(), Value::Number(ms));
    } else {
        out.insert("latency_ms".to_string(), Value::Null);
    }
    out.insert("stdout".to_string(), Value::String(stdout));
    out.insert("stderr".to_string(), Value::String(stderr));
    out.insert("duration_ms".to_string(), Value::Number(duration.as_millis() as f64));
    Ok(value_dict(out))
}

fn net_http(exec: &mut Executor, args: Vec<Value>, span: crate::parser::ast::Span) -> Result<Value, RuntimeError> {
    let req_map = match args.get(0) {
        Some(Value::Dict(map)) => map.lock().map_err(|_| err(exec, span, "Request map lock poisoned"))?.clone(),
        _ => return Err(err(exec, span, "net.http expects a request dict")),
    };

    let url = expect_string(dict_get(&req_map, "url"), "url").map_err(|e| err(exec, span, &e))?;
    let method_text = dict_get(&req_map, "method")
        .map(|v| v.as_string().to_uppercase())
        .unwrap_or_else(|| "GET".to_string());
    let method = reqwest::Method::from_bytes(method_text.as_bytes())
        .map_err(|e| err(exec, span, &format!("Invalid HTTP method: {e}")))?;
    let timeout = optional_timeout_ms(dict_get(&req_map, "timeout_ms")).map_err(|e| err(exec, span, &e))?;

    let client = reqwest::blocking::Client::builder()
        .timeout(timeout)
        .build()
        .map_err(|e| err(exec, span, &format!("Failed to build HTTP client: {e}")))?;

    let mut request = client.request(method, &url);
    if let Some(Value::Dict(headers)) = dict_get(&req_map, "headers") {
        let hdrs = headers.lock().map_err(|_| err(exec, span, "Headers map lock poisoned"))?;
        for (k, v) in hdrs.iter() {
            request = request.header(k, v.as_string());
        }
    }
    if let Some(body) = dict_get(&req_map, "body") {
        let bytes = value_to_bytes(body).map_err(|e| err(exec, span, &e))?;
        request = request.body(bytes);
    }

    let started = Instant::now();
    let response = request.send().map_err(|e| err(exec, span, &format!("HTTP request failed: {e}")))?;
    let duration = started.elapsed();
    let status = response.status().as_u16() as f64;

    let mut headers_map = HashMap::new();
    for (name, value) in response.headers() {
        let text = match value.to_str() {
            Ok(s) => s.to_string(),
            Err(_) => String::from_utf8_lossy(value.as_bytes()).to_string(),
        };
        headers_map.insert(name.to_string(), Value::String(text));
    }

    let bytes = response
        .bytes()
        .map_err(|e| err(exec, span, &format!("Failed to read HTTP body: {e}")))?;
    let body_bytes = bytes.to_vec();
    let body_text = String::from_utf8_lossy(&body_bytes).to_string();

    let mut out = HashMap::new();
    out.insert("status".to_string(), Value::Number(status));
    out.insert("ok".to_string(), Value::Bool((200.0..300.0).contains(&status)));
    out.insert("headers".to_string(), value_dict(headers_map));
    out.insert("body_text".to_string(), Value::String(body_text));
    out.insert("body_bytes".to_string(), bytes_to_value_array(&body_bytes));
    out.insert("duration_ms".to_string(), Value::Number(duration.as_millis() as f64));
    Ok(value_dict(out))
}

fn resolve_addr(addr: &str) -> Result<std::net::SocketAddr, String> {
    let mut iter = addr
        .to_socket_addrs()
        .map_err(|e| format!("Invalid address '{addr}': {e}"))?;
    iter.next().ok_or_else(|| format!("Could not resolve address '{addr}'"))
}

fn net_tcp_connect(exec: &mut Executor, args: Vec<Value>, span: crate::parser::ast::Span) -> Result<Value, RuntimeError> {
    let addr = expect_string(args.get(0), "addr").map_err(|e| err(exec, span, &e))?;
    let timeout = optional_timeout_ms(args.get(1)).map_err(|e| err(exec, span, &e))?;
    let socket_addr = resolve_addr(&addr).map_err(|e| err(exec, span, &e))?;
    let stream = TcpStream::connect_timeout(&socket_addr, timeout)
        .map_err(|e| err(exec, span, &format!("tcp_connect failed: {e}")))?;
    stream
        .set_read_timeout(Some(timeout))
        .map_err(|e| err(exec, span, &format!("Failed to set read timeout: {e}")))?;
    stream
        .set_write_timeout(Some(timeout))
        .map_err(|e| err(exec, span, &format!("Failed to set write timeout: {e}")))?;
    Ok(make_connection_value(stream, timeout))
}

fn net_tcp_listen(exec: &mut Executor, args: Vec<Value>, span: crate::parser::ast::Span) -> Result<Value, RuntimeError> {
    let addr = expect_string(args.get(0), "addr").map_err(|e| err(exec, span, &e))?;
    let accept_timeout = optional_timeout_ms(args.get(1)).map_err(|e| err(exec, span, &e))?;
    let listener = TcpListener::bind(&addr).map_err(|e| err(exec, span, &format!("tcp_listen failed: {e}")))?;
    listener
        .set_nonblocking(true)
        .map_err(|e| err(exec, span, &format!("Failed to set nonblocking listener: {e}")))?;
    Ok(make_listener_value(listener, accept_timeout))
}

fn make_connection_value(stream: TcpStream, default_timeout: Duration) -> Value {
    let state = Arc::new(Mutex::new(Some(stream)));
    let timeout_state = Arc::new(Mutex::new(default_timeout));
    let mut map = HashMap::new();

    {
        let state = Arc::clone(&state);
        map.insert(
            "send".to_string(),
            native_exec(move |exec, args, span| {
                let payload = args
                    .first()
                    .ok_or_else(|| err(exec, span, "send expects payload"))?;
                let bytes = value_to_bytes(payload).map_err(|e| err(exec, span, &e))?;
                let mut guard = state
                    .lock()
                    .map_err(|_| err(exec, span, "Socket lock poisoned"))?;
                let stream = guard
                    .as_mut()
                    .ok_or_else(|| err(exec, span, "Socket is closed"))?;
                stream
                    .write_all(&bytes)
                    .map_err(|e| err(exec, span, &format!("send failed: {e}")))?;
                Ok(Value::Number(bytes.len() as f64))
            }),
        );
    }

    {
        let state = Arc::clone(&state);
        let timeout_state = Arc::clone(&timeout_state);
        map.insert(
            "recv".to_string(),
            native_exec(move |exec, args, span| {
                let max = expect_number(args.first(), "max_bytes").map_err(|e| err(exec, span, &e))? as i64;
                if max <= 0 {
                    return Err(err(exec, span, "max_bytes must be > 0"));
                }
                let per_call_timeout = match args.get(1) {
                    Some(v) => optional_timeout_ms(Some(v)).map_err(|e| err(exec, span, &e))?,
                    None => *timeout_state
                        .lock()
                        .map_err(|_| err(exec, span, "Timeout lock poisoned"))?,
                };
                let mut guard = state
                    .lock()
                    .map_err(|_| err(exec, span, "Socket lock poisoned"))?;
                let stream = guard
                    .as_mut()
                    .ok_or_else(|| err(exec, span, "Socket is closed"))?;
                stream
                    .set_read_timeout(Some(per_call_timeout))
                    .map_err(|e| err(exec, span, &format!("Failed to set read timeout: {e}")))?;

                let mut buf = vec![0u8; max as usize];
                let n = match stream.read(&mut buf) {
                    Ok(n) => n,
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock || e.kind() == std::io::ErrorKind::TimedOut => {
                        return Err(err(exec, span, "recv timed out"));
                    }
                    Err(e) => return Err(err(exec, span, &format!("recv failed: {e}"))),
                };
                buf.truncate(n);

                let mut out = HashMap::new();
                out.insert("text".to_string(), Value::String(String::from_utf8_lossy(&buf).to_string()));
                out.insert("bytes".to_string(), bytes_to_value_array(&buf));
                out.insert("count".to_string(), Value::Number(n as f64));
                Ok(value_dict(out))
            }),
        );
    }

    {
        let state = Arc::clone(&state);
        map.insert(
            "peer_addr".to_string(),
            native_exec(move |exec, _args, span| {
                let guard = state
                    .lock()
                    .map_err(|_| err(exec, span, "Socket lock poisoned"))?;
                let stream = guard
                    .as_ref()
                    .ok_or_else(|| err(exec, span, "Socket is closed"))?;
                let addr = stream
                    .peer_addr()
                    .map_err(|e| err(exec, span, &format!("peer_addr failed: {e}")))?;
                Ok(Value::String(addr.to_string()))
            }),
        );
    }

    {
        let state = Arc::clone(&state);
        map.insert(
            "close".to_string(),
            native_exec(move |exec, _args, span| {
                let mut guard = state
                    .lock()
                    .map_err(|_| err(exec, span, "Socket lock poisoned"))?;
                if guard.is_none() {
                    return Err(err(exec, span, "Socket is already closed"));
                }
                *guard = None;
                Ok(Value::Null)
            }),
        );
    }

    value_dict(map)
}

fn make_listener_value(listener: TcpListener, accept_timeout: Duration) -> Value {
    let state = Arc::new(Mutex::new(Some(listener)));
    let timeout_state = Arc::new(Mutex::new(accept_timeout));
    let mut map = HashMap::new();

    {
        let state = Arc::clone(&state);
        let timeout_state = Arc::clone(&timeout_state);
        map.insert(
            "accept".to_string(),
            native_exec(move |exec, _args, span| {
                let timeout = *timeout_state
                    .lock()
                    .map_err(|_| err(exec, span, "Timeout lock poisoned"))?;
                let deadline = Instant::now() + timeout;
                loop {
                    let accepted = {
                        let guard = state
                            .lock()
                            .map_err(|_| err(exec, span, "Listener lock poisoned"))?;
                        let listener = guard
                            .as_ref()
                            .ok_or_else(|| err(exec, span, "Listener is closed"))?;
                        listener.accept()
                    };
                    match accepted {
                        Ok((stream, _)) => return Ok(make_connection_value(stream, timeout)),
                        Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                            if Instant::now() >= deadline {
                                return Err(err(exec, span, "accept timed out"));
                            }
                            thread::sleep(Duration::from_millis(10));
                        }
                        Err(e) => return Err(err(exec, span, &format!("accept failed: {e}"))),
                    }
                }
            }),
        );
    }

    {
        let state = Arc::clone(&state);
        map.insert(
            "local_addr".to_string(),
            native_exec(move |exec, _args, span| {
                let guard = state
                    .lock()
                    .map_err(|_| err(exec, span, "Listener lock poisoned"))?;
                let listener = guard
                    .as_ref()
                    .ok_or_else(|| err(exec, span, "Listener is closed"))?;
                let addr = listener
                    .local_addr()
                    .map_err(|e| err(exec, span, &format!("local_addr failed: {e}")))?;
                Ok(Value::String(addr.to_string()))
            }),
        );
    }

    {
        let state = Arc::clone(&state);
        map.insert(
            "close".to_string(),
            native_exec(move |exec, _args, span| {
                let mut guard = state
                    .lock()
                    .map_err(|_| err(exec, span, "Listener lock poisoned"))?;
                if guard.is_none() {
                    return Err(err(exec, span, "Listener is already closed"));
                }
                *guard = None;
                Ok(Value::Null)
            }),
        );
    }

    value_dict(map)
}
