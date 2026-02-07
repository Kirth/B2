use std::collections::HashMap;
use std::sync::{Arc, Mutex, atomic::{AtomicBool, Ordering}, mpsc};
use std::thread;
use std::time::Duration;

use crate::parser::ast::{Expr, InterpPart, Pattern, Program, Span, Stmt};
use crate::runtime::errors::{Frame, RuntimeError};
use crate::runtime::builtins::{numbers, strings};
use crate::runtime::shell::{sh::run_sh, ssh::run_ssh};
use crate::runtime::value::Value;

#[derive(Clone)]
pub struct Function {
    pub params: Vec<(String, Option<crate::parser::ast::TypeExpr>)>,
    pub return_type: Option<crate::parser::ast::TypeExpr>,
    pub body: Vec<Stmt>,
    pub captured: HashMap<String, Value>,
    pub name: String,
}

#[derive(Clone)]
pub struct TaskHandle {
    pub state: Arc<Mutex<TaskState>>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum TaskStatus {
    Running,
    Completed,
    Failed,
    Cancelled,
}

#[derive(Clone)]
pub struct TaskState {
    pub status: TaskStatus,
    pub result: Option<Value>,
    pub error: Option<RuntimeError>,
}

pub struct Executor {
    filename: String,
    source: String,
    scopes: Vec<HashMap<String, Value>>,
    stack: Vec<Frame>,
    last_span: Span,
    output_sink: Option<Arc<dyn Fn(String) + Send + Sync>>,
}

impl Executor {
    pub fn new(filename: String, source: String) -> Self {
        Executor {
            filename,
            source,
            scopes: vec![HashMap::new()],
            stack: Vec::new(),
            last_span: Span { line: 1, col: 1 },
            output_sink: None,
        }
    }

    pub fn set_output_sink(&mut self, sink: Option<Arc<dyn Fn(String) + Send + Sync>>) {
        self.output_sink = sink;
    }

    pub fn set_context(&mut self, filename: String, source: String) {
        self.filename = filename;
        self.source = source;
        self.last_span = Span { line: 1, col: 1 };
    }

    pub fn register_native<F>(&mut self, name: &str, func: F)
    where
        F: Fn(Vec<Value>) -> Result<Value, String> + Send + Sync + 'static,
    {
        self.scopes[0].insert(
            name.to_string(),
            Value::NativeFunction(Arc::new(func)),
        );
    }

    pub fn register_native_exec<F>(&mut self, name: &str, func: F)
    where
        F: Fn(&mut Executor, Vec<Value>, Span) -> Result<Value, RuntimeError> + Send + Sync + 'static,
    {
        self.scopes[0].insert(
            name.to_string(),
            Value::NativeFunctionExec(Arc::new(func)),
        );
    }

    pub fn register_value(&mut self, name: &str, value: Value) {
        self.scopes[0].insert(name.to_string(), value);
    }

    pub fn execute(&mut self, program: &Program) -> Result<(), RuntimeError> {
        for stmt in &program.statements {
            match self.exec_stmt(stmt)? {
                ExecResult::Normal => {}
                ExecResult::Return(_) => {}
            }
        }
        Ok(())
    }

    pub fn execute_repl(&mut self, program: &Program) -> Result<Option<Value>, RuntimeError> {
        let mut last_expr = None;
        for stmt in &program.statements {
            match stmt {
                Stmt::Expr { expr, .. } => {
                    self.last_span = stmt.span();
                    last_expr = Some(self.eval_expr(expr)?);
                }
                _ => match self.exec_stmt(stmt)? {
                    ExecResult::Normal => {}
                    ExecResult::Return(_) => {}
                },
            }
        }
        Ok(last_expr)
    }

    pub fn list_globals(&self) -> Vec<(String, Value)> {
        if self.scopes.is_empty() {
            return Vec::new();
        }
        let mut entries: Vec<(String, Value)> = self.scopes[0]
            .iter()
            .filter_map(|(k, v)| match v {
                Value::NativeFunction(_) | Value::NativeFunctionExec(_) => None,
                _ => Some((k.clone(), v.clone())),
            })
            .collect();
        entries.sort_by(|a, b| a.0.cmp(&b.0));
        entries
    }

    fn exec_stmt(&mut self, stmt: &Stmt) -> Result<ExecResult, RuntimeError> {
        self.last_span = stmt.span();
        match stmt {
            Stmt::Expr { expr, .. } => {
                self.eval_expr(expr)?;
                Ok(ExecResult::Normal)
            }
            Stmt::Let { name, expr, .. } => {
                let value = self.eval_expr(expr)?;
                self.define(name, value);
                Ok(ExecResult::Normal)
            }
            Stmt::LetDestructure { pattern, expr, span } => {
                let value = self.eval_expr(expr)?;
                self.bind_pattern(pattern, value, *span)?;
                Ok(ExecResult::Normal)
            }
            Stmt::Assign { name, expr, span } => {
                let value = self.eval_expr(expr)?;
                if !self.assign(name, value) {
                    return Err(self.err("Undefined variable", *span));
                }
                Ok(ExecResult::Normal)
            }
            Stmt::IndexAssign { target, index, expr, .. } => {
                let idx = self.eval_expr(index)?;
                let val = self.eval_expr(expr)?;
                match target {
                    Expr::Var { name, .. } => {
                        let obj = self.lookup(name).ok_or_else(|| self.err("Undefined variable", target.span()))?;
                        let updated = self.assign_index(obj, idx, val)?;
                        self.assign(name, updated);
                    }
                    _ => return Err(self.err("Index assignment requires variable target", target.span())),
                }
                Ok(ExecResult::Normal)
            }
            Stmt::If { cond, then_branch, else_branch, .. } => {
                let c = self.eval_expr(cond)?;
                if c.is_truthy() {
                    self.exec_block(then_branch)?;
                } else {
                    self.exec_block(else_branch)?;
                }
                Ok(ExecResult::Normal)
            }
            Stmt::For { pattern, iterable, body, span } => {
                let it = self.eval_expr(iterable)?;
                let items = self.iterate_value(it)?;
                for item in items {
                    self.push_scope();
                    self.bind_pattern(pattern, item, *span)?;
                    let res = self.exec_block(body)?;
                    self.pop_scope();
                    if let ExecResult::Return(v) = res {
                        return Ok(ExecResult::Return(v));
                    }
                }
                Ok(ExecResult::Normal)
            }
            Stmt::ParallelFor { pattern, iterable, body, .. } => {
                self.exec_parallel_for(pattern, iterable, body)?;
                Ok(ExecResult::Normal)
            }
            Stmt::While { cond, body, .. } => {
                loop {
                    let c = self.eval_expr(cond)?;
                    if !c.is_truthy() { break; }
                    let res = self.exec_block(body)?;
                    if let ExecResult::Return(v) = res {
                        return Ok(ExecResult::Return(v));
                    }
                }
                Ok(ExecResult::Normal)
            }
            Stmt::TryCatch { try_block, err_name, catch_block, .. } => {
                match self.exec_block(try_block) {
                    Ok(res) => Ok(res),
                    Err(e) => {
                        let err_value = self.error_to_value(&e);
                        self.push_scope();
                        self.define(err_name, err_value);
                        let res = self.exec_block(catch_block);
                        self.pop_scope();
                        res
                    }
                }
            }
            Stmt::Function { name, params, return_type, body, .. } => {
                let captured = self.flatten_scopes();
                let func = Function {
                    params: params.clone(),
                    return_type: return_type.clone(),
                    body: body.clone(),
                    captured,
                    name: name.clone(),
                };
                self.define(name, Value::Function(Arc::new(func)));
                Ok(ExecResult::Normal)
            }
            Stmt::Return { value, .. } => {
                let val = match value {
                    Some(expr) => self.eval_expr(expr)?,
                    None => Value::Null,
                };
                Ok(ExecResult::Return(val))
            }
            Stmt::Invoke { name, expr, span } => {
                let val = self.eval_expr(expr)?;
                match name.as_str() {
                    "log" => {
                        self.emit_output(format!("[{}] {}", now_hms(), val.as_string()));
                        Ok(ExecResult::Normal)
                    }
                    "echo" => {
                        self.emit_output(val.as_string());
                        Ok(ExecResult::Normal)
                    }
                    _ => Err(self.err("Unknown invocation", *span)),
                }
            }
        }
    }

    fn exec_block(&mut self, stmts: &[Stmt]) -> Result<ExecResult, RuntimeError> {
        self.push_scope();
        for stmt in stmts {
            let res = self.exec_stmt(stmt)?;
            if let ExecResult::Return(_) = res {
                self.pop_scope();
                return Ok(res);
            }
        }
        self.pop_scope();
        Ok(ExecResult::Normal)
    }

    fn exec_parallel_for(&mut self, pattern: &Pattern, iterable: &Expr, body: &[Stmt]) -> Result<(), RuntimeError> {
        let it = self.eval_expr(iterable)?;
        let items = self.iterate_value(it)?;
        if items.is_empty() {
            return Ok(());
        }

        let captured = self.flatten_scopes();
        let pool_size = parallelism_cap();
        let cancel = Arc::new(AtomicBool::new(false));

        let (job_tx, job_rx) = mpsc::channel::<Value>();
        let (err_tx, err_rx) = mpsc::channel::<RuntimeError>();

        let shared_rx = Arc::new(Mutex::new(job_rx));

        let mut handles = Vec::new();
        for _ in 0..pool_size {
            let rx = Arc::clone(&shared_rx);
            let err_sender = err_tx.clone();
            let cancel_flag = cancel.clone();
            let body = body.to_vec();
            let captured_env = captured.clone();
            let pattern = pattern.clone();
            let filename = self.filename.clone();
            let source = self.source.clone();
            let output_sink = self.output_sink.clone();

            let handle = thread::spawn(move || {
                while !cancel_flag.load(Ordering::Relaxed) {
                    let item = match rx.lock() {
                        Ok(guard) => match guard.recv() {
                            Ok(v) => v,
                            Err(_) => break,
                        },
                        Err(_) => break,
                    };
                    if cancel_flag.load(Ordering::Relaxed) {
                        break;
                    }
                    let mut exec = Executor { filename: filename.clone(), source: source.clone(), scopes: vec![captured_env.clone()], stack: Vec::new(), last_span: Span { line: 1, col: 1 }, output_sink: output_sink.clone() };
                    if let Err(e) = exec.bind_pattern(&pattern, item, Span { line: 1, col: 1 }) {
                        let _ = err_sender.send(e);
                        cancel_flag.store(true, Ordering::Relaxed);
                        break;
                    }
                    if let Err(e) = exec.exec_block(&body) {
                        let _ = err_sender.send(e);
                        cancel_flag.store(true, Ordering::Relaxed);
                        break;
                    }
                }
            });
            handles.push(handle);
        }

        for item in items {
            if cancel.load(Ordering::Relaxed) {
                break;
            }
            if job_tx.send(item).is_err() {
                break;
            }
        }
        drop(job_tx);

        if let Ok(err) = err_rx.try_recv() {
            cancel.store(true, Ordering::Relaxed);
            for h in handles {
                let _ = h.join();
            }
            return Err(err);
        }

        for h in handles {
            let _ = h.join();
        }

        if let Ok(err) = err_rx.try_recv() {
            return Err(err);
        }

        Ok(())
    }

    fn eval_expr(&mut self, expr: &Expr) -> Result<Value, RuntimeError> {
        self.last_span = expr.span();
        match expr {
            Expr::Literal { value, .. } => Ok(value.clone()),
            Expr::Var { name, span } => self.lookup(name).ok_or_else(|| self.err("Undefined variable", *span)),
            Expr::Binary { left, op, right, span } => {
                if op == "&&" {
                    let l = self.eval_expr(left)?;
                    if !l.is_truthy() {
                        return Ok(Value::Bool(false));
                    }
                    let r = self.eval_expr(right)?;
                    return Ok(Value::Bool(r.is_truthy()));
                }
                if op == "||" {
                    let l = self.eval_expr(left)?;
                    if l.is_truthy() {
                        return Ok(Value::Bool(true));
                    }
                    let r = self.eval_expr(right)?;
                    return Ok(Value::Bool(r.is_truthy()));
                }
                let l = self.eval_expr(left)?;
                let r = self.eval_expr(right)?;
                self.eval_binary(l, op, r, *span)
            }
            Expr::Unary { op, expr, span } => {
                let v = self.eval_expr(expr)?;
                self.eval_unary(op, v, *span)
            }
            Expr::Call { callee, args, span } => {
                if let Expr::Var { name, .. } = callee.as_ref() {
                    if self.lookup(name).is_none() {
                        return Err(self.unknown_global_function_error(name, *span));
                    }
                }
                let c = self.eval_expr(callee)?;
                let mut a = Vec::new();
                for arg in args {
                    a.push(self.eval_expr(arg)?);
                }
                self.call(c, a, *span)
            }
            Expr::Member { object, name, span } => {
                let obj = self.eval_expr(object)?;
                self.member_access(obj, name, *span)
            }
            Expr::Index { object, index, span } => {
                let obj = self.eval_expr(object)?;
                let idx = self.eval_expr(index)?;
                self.index_access(obj, idx, *span)
            }
            Expr::Array { items, .. } => {
                let mut vals = Vec::new();
                for e in items { vals.push(self.eval_expr(e)?); }
                Ok(Value::Array(Arc::new(Mutex::new(vals))))
            }
            Expr::Tuple { items, .. } => {
                let mut vals = Vec::new();
                for e in items { vals.push(self.eval_expr(e)?); }
                Ok(Value::tuple(vals))
            }
            Expr::Dict { items, .. } => {
                let mut map = HashMap::new();
                for (k, v) in items {
                    let key = self.eval_expr(k)?.as_string();
                    let val = self.eval_expr(v)?;
                    map.insert(key, val);
                }
                Ok(Value::Dict(Arc::new(Mutex::new(map))))
            }
            Expr::Range { start, end, .. } => {
                let s = self.eval_expr(start)?;
                let e = self.eval_expr(end)?;
                Ok(Value::Range(to_i64(s)? , to_i64(e)?))
            }
            Expr::IfExpr { cond, then_branch, else_branch, .. } => {
                let c = self.eval_expr(cond)?;
                if c.is_truthy() {
                    self.eval_block_expr(then_branch)
                } else {
                    self.eval_block_expr(else_branch)
                }
            }
            Expr::ParallelFor { pattern, iterable, body, .. } => {
                self.eval_parallel_for_expr(pattern, iterable, body)
            }
            Expr::TaskBlock { body, .. } => {
                let body = body.clone();
                let captured = self.flatten_scopes();
                let filename = self.filename.clone();
                let source = self.source.clone();
                let output_sink = self.output_sink.clone();
                Ok(self.spawn_task(move || {
                    let mut exec = Executor {
                        filename,
                        source,
                        scopes: vec![captured],
                        stack: Vec::new(),
                        last_span: Span { line: 1, col: 1 },
                        output_sink,
                    };
                    exec.eval_block_expr(&body)
                }))
            }
            Expr::TaskCall { callee, args, .. } => {
                let callee_value = self.eval_expr(callee)?;
                let mut arg_values = Vec::new();
                for arg in args {
                    arg_values.push(self.eval_expr(arg)?);
                }
                let captured = self.flatten_scopes();
                let filename = self.filename.clone();
                let source = self.source.clone();
                let output_sink = self.output_sink.clone();
                Ok(self.spawn_task(move || {
                    let mut exec = Executor {
                        filename,
                        source,
                        scopes: vec![captured],
                        stack: Vec::new(),
                        last_span: Span { line: 1, col: 1 },
                        output_sink,
                    };
                    exec.call_value(callee_value, arg_values)
                }))
            }
            Expr::Await { expr, safe, span } => {
                let awaited = self.eval_expr(expr)?;
                match awaited {
                    Value::Task(task) => {
                        if *safe {
                            match self.task_join(&task, *span) {
                                Ok(v) => Ok(v),
                                Err(_) => Ok(Value::Null),
                            }
                        } else {
                            self.task_join(&task, *span)
                        }
                    }
                    _ => Err(self.err("await expects task", *span)),
                }
            }
            Expr::Lambda { params, return_type, body, .. } => {
                let captured = self.flatten_scopes();
                let func = Function {
                    params: params.clone(),
                    return_type: return_type.clone(),
                    body: body.clone(),
                    captured,
                    name: "<lambda>".to_string(),
                };
                Ok(Value::Function(Arc::new(func)))
            }
            Expr::InterpolatedString { parts, .. } => {
                let mut out = String::new();
                for p in parts {
                    match p {
                        InterpPart::Literal(s) => out.push_str(s),
                        InterpPart::Expr(e) => out.push_str(&self.eval_expr(e)?.as_string()),
                    }
                }
                Ok(Value::String(out))
            }
            Expr::Sh { command, .. } => {
                let cmd = self.eval_expr(command)?.as_string();
                match run_sh(&cmd) {
                    Ok(res) => Ok(res.to_value()),
                    Err(e) => Err(self.err(&e, expr.span())),
                }
            }
            Expr::Ssh { host, command, .. } => {
                let h = self.eval_expr(host)?.as_string();
                let cmd = self.eval_expr(command)?.as_string();
                match run_ssh(&h, &cmd) {
                    Ok(res) => Ok(res.to_value()),
                    Err(e) => Err(self.err(&e, expr.span())),
                }
            }
        }
    }

    fn bind_pattern(&mut self, pattern: &Pattern, value: Value, span: Span) -> Result<(), RuntimeError> {
        match pattern {
            Pattern::Ignore => Ok(()),
            Pattern::Name(name) => {
                self.define(name, value);
                Ok(())
            }
            Pattern::Tuple(parts) => {
                let values = match value {
                    Value::Array(arr) => arr.lock().map(|v| v.clone()).unwrap_or_else(|_| Vec::new()),
                    Value::Tuple(tup) => tup.lock().map(|v| v.clone()).unwrap_or_else(|_| Vec::new()),
                    other => vec![other],
                };
                for (idx, pat) in parts.iter().enumerate() {
                    let val = values.get(idx).cloned().unwrap_or(Value::Null);
                    self.bind_pattern(pat, val, span)?;
                }
                Ok(())
            }
        }
    }

    fn eval_block_expr(&mut self, stmts: &[Stmt]) -> Result<Value, RuntimeError> {
        self.push_scope();
        let mut last = Value::Null;
        for stmt in stmts {
            match stmt {
                Stmt::Expr { expr, .. } => last = self.eval_expr(expr)?,
                _ => {
                    let res = self.exec_stmt(stmt)?;
                    if let ExecResult::Return(v) = res {
                        self.pop_scope();
                        return Ok(v);
                    }
                }
            }
        }
        self.pop_scope();
        Ok(last)
    }

    fn eval_parallel_for_expr(&mut self, pattern: &Pattern, iterable: &Expr, body: &[Stmt]) -> Result<Value, RuntimeError> {
        let it = self.eval_expr(iterable)?;
        let items = self.iterate_value(it)?;
        if items.is_empty() {
            return Ok(Value::array(Vec::new()));
        }

        let captured = self.flatten_scopes();
        let pool_size = parallelism_cap();
        let cancel = Arc::new(AtomicBool::new(false));

        let (job_tx, job_rx) = mpsc::channel::<(usize, Value)>();
        let (res_tx, res_rx) = mpsc::channel::<(usize, Value)>();
        let (err_tx, err_rx) = mpsc::channel::<RuntimeError>();
        let shared_rx = Arc::new(Mutex::new(job_rx));

        let mut handles = Vec::new();
        for _ in 0..pool_size {
            let rx = Arc::clone(&shared_rx);
            let result_sender = res_tx.clone();
            let err_sender = err_tx.clone();
            let cancel_flag = cancel.clone();
            let body = body.to_vec();
            let captured_env = captured.clone();
            let pattern = pattern.clone();
            let filename = self.filename.clone();
            let source = self.source.clone();
            let output_sink = self.output_sink.clone();

            let handle = thread::spawn(move || {
                while !cancel_flag.load(Ordering::Relaxed) {
                    let (idx, item) = match rx.lock() {
                        Ok(guard) => match guard.recv() {
                            Ok(v) => v,
                            Err(_) => break,
                        },
                        Err(_) => break,
                    };
                    if cancel_flag.load(Ordering::Relaxed) {
                        break;
                    }

                    let mut exec = Executor {
                        filename: filename.clone(),
                        source: source.clone(),
                        scopes: vec![captured_env.clone()],
                        stack: Vec::new(),
                        last_span: Span { line: 1, col: 1 },
                        output_sink: output_sink.clone(),
                    };
                    if let Err(e) = exec.bind_pattern(&pattern, item, Span { line: 1, col: 1 }) {
                        let _ = err_sender.send(e);
                        cancel_flag.store(true, Ordering::Relaxed);
                        break;
                    }
                    match exec.eval_block_expr(&body) {
                        Ok(v) => {
                            if result_sender.send((idx, v)).is_err() {
                                break;
                            }
                        }
                        Err(e) => {
                            let _ = err_sender.send(e);
                            cancel_flag.store(true, Ordering::Relaxed);
                            break;
                        }
                    }
                }
            });
            handles.push(handle);
        }
        drop(res_tx);

        let expected_count = items.len();
        for (idx, item) in items.into_iter().enumerate() {
            if cancel.load(Ordering::Relaxed) {
                break;
            }
            if job_tx.send((idx, item)).is_err() {
                break;
            }
        }
        drop(job_tx);

        if let Ok(err) = err_rx.try_recv() {
            cancel.store(true, Ordering::Relaxed);
            for h in handles {
                let _ = h.join();
            }
            return Err(err);
        }

        let mut ordered: Vec<Option<Value>> = vec![None; expected_count];
        for (idx, v) in res_rx {
            if idx >= ordered.len() {
                ordered.resize(idx + 1, None);
            }
            ordered[idx] = Some(v);
        }

        for h in handles {
            let _ = h.join();
        }

        if let Ok(err) = err_rx.try_recv() {
            return Err(err);
        }

        let values = ordered.into_iter().map(|v| v.unwrap_or(Value::Null)).collect();
        Ok(Value::array(values))
    }

    fn spawn_task<F>(&self, run: F) -> Value
    where
        F: FnOnce() -> Result<Value, RuntimeError> + Send + 'static,
    {
        let state = Arc::new(Mutex::new(TaskState {
            status: TaskStatus::Running,
            result: None,
            error: None,
        }));
        let task = Arc::new(TaskHandle {
            state: state.clone(),
        });
        thread::spawn(move || {
            let res = run();
            if let Ok(mut st) = state.lock() {
                if st.status != TaskStatus::Cancelled {
                    match &res {
                        Ok(v) => {
                            st.status = TaskStatus::Completed;
                            st.result = Some(v.clone());
                            st.error = None;
                        }
                        Err(e) => {
                            st.status = TaskStatus::Failed;
                            st.result = None;
                            st.error = Some(e.clone());
                        }
                    }
                }
            }
        });
        Value::Task(task)
    }

    fn task_join(&self, task: &Arc<TaskHandle>, span: Span) -> Result<Value, RuntimeError> {
        loop {
            let st = task
                .state
                .lock()
                .map_err(|_| self.err("Task state lock poisoned", span))?;
            match st.status {
                TaskStatus::Completed => {
                    return Ok(st.result.clone().unwrap_or(Value::Null));
                }
                TaskStatus::Failed => {
                    return Err(st
                        .error
                        .clone()
                        .unwrap_or_else(|| self.err("Task failed", span)));
                }
                TaskStatus::Cancelled => {
                    return Err(st
                        .error
                        .clone()
                        .unwrap_or_else(|| self.err("Task cancelled", span)));
                }
                TaskStatus::Running => {
                    drop(st);
                    thread::sleep(Duration::from_millis(1));
                }
            }
        }
    }

    fn call(&mut self, callee: Value, args: Vec<Value>, span: Span) -> Result<Value, RuntimeError> {
        match callee {
            Value::NativeFunction(f) => {
                f(args).map_err(|e| self.err(&e, span))
            }
            Value::NativeFunctionExec(f) => {
                f(self, args, span)
            }
            Value::Ufcs { name, receiver } => {
                let mut new_args = vec![*receiver];
                new_args.extend(args);
                let recv = new_args[0].clone();
                let target = self
                    .lookup(&name)
                    .ok_or_else(|| self.unknown_method_or_function_error(&name, &recv, span))?;
                self.call(target, new_args, span)
            }
            Value::Function(func) => {
                if func.params.len() != args.len() {
                    return Err(self.err("Arity mismatch", span));
                }
                self.push_frame(func.name.clone(), span);
                let mut env = func.captured.clone();
                for (i, (p, ty)) in func.params.iter().enumerate() {
                    if let Some(t) = ty {
                        if !type_matches(t, &args[i]) {
                            return Err(self.err(&format!("Type mismatch for {}: expected {}, got {}", p, type_str(t), value_type_str(&args[i])), span));
                        }
                    }
                    env.insert(p.clone(), args[i].clone());
                }
                self.push_scope_with(env);
                let res = self.exec_block(&func.body)?;
                self.pop_scope();
                self.pop_frame();
                let out = if let ExecResult::Return(v) = res { v } else { Value::Null };
                if let Some(t) = &func.return_type {
                    if !type_matches(t, &out) {
                        return Err(self.err(
                            &format!(
                                "Return type mismatch for {}: expected {}, got {}",
                                func.name,
                                type_str(t),
                                value_type_str(&out)
                            ),
                            span,
                        ));
                    }
                }
                Ok(out)
            }
            _ => Err(self.err("Not callable", span)),
        }
    }

    pub fn call_value(&mut self, callee: Value, args: Vec<Value>) -> Result<Value, RuntimeError> {
        self.call(callee, args, Span { line: 0, col: 0 })
    }

    fn eval_binary(&self, l: Value, op: &str, r: Value, span: Span) -> Result<Value, RuntimeError> {
        match op {
            "+" => match (l, r) {
                (Value::Number(a), Value::Number(b)) => Ok(Value::Number(a + b)),
                (Value::String(a), Value::String(b)) => Ok(Value::String(a + &b)),
                (Value::String(a), b) => Ok(Value::String(a + &b.as_string())),
                (a, Value::String(b)) => Ok(Value::String(a.as_string() + &b)),
                _ => Err(self.err("Invalid '+' operands", span)),
            },
            "-" => Ok(Value::Number(to_f64(l)? - to_f64(r)?)),
            "*" => Ok(Value::Number(to_f64(l)? * to_f64(r)?)),
            "/" => Ok(Value::Number(to_f64(l)? / to_f64(r)?)),
            "%" => Ok(Value::Number(to_f64(l)? % to_f64(r)?)),
            "==" => Ok(Value::Bool(l.as_string() == r.as_string())),
            "!=" => Ok(Value::Bool(l.as_string() != r.as_string())),
            "<" => Ok(Value::Bool(to_f64(l)? < to_f64(r)?)),
            "<=" => Ok(Value::Bool(to_f64(l)? <= to_f64(r)?)),
            ">" => Ok(Value::Bool(to_f64(l)? > to_f64(r)?)),
            ">=" => Ok(Value::Bool(to_f64(l)? >= to_f64(r)?)),
            _ => Err(self.err("Unknown operator", span)),
        }
    }

    fn eval_unary(&self, op: &str, v: Value, span: Span) -> Result<Value, RuntimeError> {
        match op {
            "-" => Ok(Value::Number(-to_f64(v)?)),
            "!" => Ok(Value::Bool(!v.is_truthy())),
            _ => Err(self.err("Unknown unary", span)),
        }
    }

    fn member_access(&self, obj: Value, name: &str, _span: Span) -> Result<Value, RuntimeError> {
        match obj {
            Value::Dict(map) => {
                if let Ok(guard) = map.lock() {
                    if let Some(v) = guard.get(name) {
                        return Ok(v.clone());
                    }
                    if name == "count" {
                        return Ok(Value::Number(guard.len() as f64));
                    }
                }
                Ok(Value::Ufcs { name: name.to_string(), receiver: Box::new(Value::Dict(map)) })
            }
            Value::Array(arr) => {
                match name {
                    "add" => {
                        let arr_ref = arr.clone();
                        let func = move |args: Vec<Value>| {
                            let mut list = arr_ref.lock().map_err(|_| "Array lock poisoned".to_string())?;
                            if let Some(v) = args.get(0) {
                                list.push(v.clone());
                                Ok(Value::Null)
                            } else {
                                Err("add expects 1 argument".to_string())
                            }
                        };
                        Ok(Value::NativeFunction(Arc::new(func)))
                    }
                    "count" => Ok(Value::Number(arr.lock().map(|v| v.len() as f64).unwrap_or(0.0))),
                    _ => Ok(Value::Ufcs { name: name.to_string(), receiver: Box::new(Value::Array(arr)) }),
                }
            }
            Value::Tuple(tup) => match name {
                "count" => Ok(Value::Number(tup.lock().map(|v| v.len() as f64).unwrap_or(0.0))),
                _ => Ok(Value::Ufcs { name: name.to_string(), receiver: Box::new(Value::Tuple(tup)) }),
            },
            Value::String(s) => {
                if let Some(method) = strings::get_method(name, &s) {
                    return Ok(method);
                }
                Ok(Value::Ufcs { name: name.to_string(), receiver: Box::new(Value::String(s)) })
            }
            Value::Task(task) => match name {
                "join" => {
                    let task_ref = task.clone();
                    Ok(Value::NativeFunctionExec(Arc::new(move |exec, _args, span| {
                        exec.task_join(&task_ref, span)
                    })))
                }
                "done" => {
                    let task_ref = task.clone();
                    Ok(Value::NativeFunctionExec(Arc::new(move |exec, _args, span| {
                        let done = task_ref
                            .state
                            .lock()
                            .map_err(|_| exec.err("Task state lock poisoned", span))?
                            .status
                            != TaskStatus::Running;
                        Ok(Value::Bool(done))
                    })))
                }
                "cancel" => {
                    let task_ref = task.clone();
                    Ok(Value::NativeFunctionExec(Arc::new(move |exec, _args, span| {
                        let mut state = task_ref
                            .state
                            .lock()
                            .map_err(|_| exec.err("Task state lock poisoned", span))?;
                        if state.status == TaskStatus::Running {
                            state.status = TaskStatus::Cancelled;
                            state.error = Some(exec.err("Task cancelled", span));
                            return Ok(Value::Bool(true));
                        }
                        Ok(Value::Bool(false))
                    })))
                }
                "error" => {
                    let task_ref = task.clone();
                    Ok(Value::NativeFunctionExec(Arc::new(move |exec, _args, span| {
                        let state = task_ref
                            .state
                            .lock()
                            .map_err(|_| exec.err("Task state lock poisoned", span))?;
                        if let Some(err) = &state.error {
                            Ok(Value::String(err.message.clone()))
                        } else {
                            Ok(Value::Null)
                        }
                    })))
                }
                _ => Ok(Value::Ufcs { name: name.to_string(), receiver: Box::new(Value::Task(task)) }),
            },
            Value::Number(n) => {
                if let Some(method) = numbers::get_method(name, n) {
                    return Ok(method);
                }
                Ok(Value::Ufcs { name: name.to_string(), receiver: Box::new(Value::Number(n)) })
            }
            _ => Ok(Value::Ufcs { name: name.to_string(), receiver: Box::new(obj) }),
        }
    }

    fn index_access(&self, obj: Value, idx: Value, span: Span) -> Result<Value, RuntimeError> {
        match obj {
            Value::Array(arr) => {
                let i = to_i64(idx)? as usize;
                arr.lock().ok().and_then(|v| v.get(i).cloned()).ok_or_else(|| self.err("Index out of range", span))
            }
            Value::Tuple(tup) => {
                let i = to_i64(idx)? as usize;
                tup.lock().ok().and_then(|v| v.get(i).cloned()).ok_or_else(|| self.err("Index out of range", span))
            }
            Value::Dict(map) => {
                let key = idx.as_string();
                map.lock().ok().and_then(|v| v.get(&key).cloned()).ok_or_else(|| self.err("Missing key", span))
            }
            _ => Err(self.err("Indexing not supported", span)),
        }
    }

    fn assign_index(&mut self, obj: Value, idx: Value, val: Value) -> Result<Value, RuntimeError> {
        match obj {
            Value::Array(arr) => {
                let i = to_i64(idx)? as usize;
                {
                    let mut list = arr.lock().map_err(|_| self.err("Array lock poisoned", Span { line: 0, col: 0 }))?;
                    if i >= list.len() { return Err(self.err("Index out of range", Span { line: 0, col: 0 })); }
                    list[i] = val;
                }
                Ok(Value::Array(arr))
            }
            Value::Tuple(_) => Err(self.err("Cannot assign to tuple index (tuples are immutable)", Span { line: 0, col: 0 })),
            Value::Dict(map) => {
                let key = idx.as_string();
                map.lock().map_err(|_| self.err("Dict lock poisoned", Span { line: 0, col: 0 }))?.insert(key, val);
                Ok(Value::Dict(map))
            }
            _ => Err(self.err("Index assignment not supported", Span { line: 0, col: 0 })),
        }
    }

    fn iterate_value(&self, val: Value) -> Result<Vec<Value>, RuntimeError> {
        match val {
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
            _ => Err(self.err("Not iterable", Span { line: 0, col: 0 })),
        }
    }

    fn define(&mut self, name: &str, value: Value) {
        if let Some(scope) = self.scopes.last_mut() {
            scope.insert(name.to_string(), value);
        }
    }

    fn assign(&mut self, name: &str, value: Value) -> bool {
        for scope in self.scopes.iter_mut().rev() {
            if scope.contains_key(name) {
                scope.insert(name.to_string(), value);
                return true;
            }
        }
        false
    }

    fn lookup(&self, name: &str) -> Option<Value> {
        for scope in self.scopes.iter().rev() {
            if let Some(v) = scope.get(name) { return Some(v.clone()); }
        }
        None
    }

    fn push_scope(&mut self) {
        self.scopes.push(HashMap::new());
    }

    fn push_scope_with(&mut self, env: HashMap<String, Value>) {
        self.scopes.push(env);
    }

    fn pop_scope(&mut self) {
        if self.scopes.len() > 1 { self.scopes.pop(); }
    }

    fn flatten_scopes(&self) -> HashMap<String, Value> {
        let mut map = HashMap::new();
        for scope in &self.scopes {
            for (k, v) in scope {
                map.insert(k.clone(), v.clone());
            }
        }
        map
    }

    fn push_frame(&mut self, name: String, span: Span) {
        self.stack.push(Frame { name, line: span.line, col: span.col });
    }

    fn pop_frame(&mut self) {
        self.stack.pop();
    }

    fn emit_output(&self, line: String) {
        if let Some(sink) = &self.output_sink {
            sink(line);
        } else {
            println!("{line}");
        }
    }

    fn err(&self, message: &str, span: Span) -> RuntimeError {
        let span = if span.line == 0 { self.last_span } else { span };
        let snippet = self.render_snippet(span);
        RuntimeError::new(message.to_string(), self.filename.clone(), span.line, span.col, snippet, self.stack.clone())
    }

    pub fn make_error(&self, message: &str, span: Span) -> RuntimeError {
        self.err(message, span)
    }

    fn render_snippet(&self, span: Span) -> Option<String> {
        let lines: Vec<&str> = self.source.lines().collect();
        if span.line == 0 || span.line > lines.len() { return None; }
        let line = lines[span.line - 1];
        let col = if span.col == 0 { 1 } else { span.col };
        let mut caret = String::new();
        caret.push_str(&" ".repeat(col.saturating_sub(1)));
        caret.push('^');
        Some(format!("  | {}\n  | {}", line, caret))
    }

    fn error_to_value(&self, err: &RuntimeError) -> Value {
        let mut map = HashMap::new();
        map.insert("message".to_string(), Value::String(err.message.clone()));
        map.insert("file".to_string(), Value::String(err.filename.clone()));
        map.insert("line".to_string(), Value::Number(err.line as f64));
        map.insert("col".to_string(), Value::Number(err.col as f64));

        let mut stack_vals = Vec::new();
        for frame in &err.stack {
            let mut f = HashMap::new();
            f.insert("name".to_string(), Value::String(frame.name.clone()));
            f.insert("line".to_string(), Value::Number(frame.line as f64));
            f.insert("col".to_string(), Value::Number(frame.col as f64));
            stack_vals.push(Value::Dict(Arc::new(Mutex::new(f))));
        }
        map.insert("stack".to_string(), Value::array(stack_vals));
        Value::Dict(Arc::new(Mutex::new(map)))
    }

    fn unknown_global_function_error(&self, name: &str, span: Span) -> RuntimeError {
        let candidates = self.known_global_function_names();
        let suggestions = suggest_names(name, &candidates);
        let mut msg = format!("Unknown function '{name}'");
        if !suggestions.is_empty() {
            msg.push_str(&format!(". Did you mean: {}", suggestions.join(", ")));
        }
        self.err(&msg, span)
    }

    fn unknown_method_or_function_error(&self, name: &str, receiver: &Value, span: Span) -> RuntimeError {
        let ty = value_type_str(receiver);
        let mut candidates = self.known_method_names_for(receiver);
        candidates.extend(self.known_global_function_names());
        let suggestions = suggest_names(name, &candidates);
        let mut msg = format!("Unknown function or method '{name}' on type {ty}");
        if !suggestions.is_empty() {
            msg.push_str(&format!(". Did you mean: {}", suggestions.join(", ")));
        }
        self.err(&msg, span)
    }

    fn known_global_function_names(&self) -> Vec<String> {
        if self.scopes.is_empty() {
            return Vec::new();
        }
        self.scopes[0]
            .iter()
            .filter_map(|(k, v)| match v {
                Value::Function(_) | Value::NativeFunction(_) | Value::NativeFunctionExec(_) => Some(k.clone()),
                _ => None,
            })
            .collect()
    }

    fn known_method_names_for(&self, value: &Value) -> Vec<String> {
        match value {
            Value::Task(_) => vec!["join", "done", "cancel", "error"]
                .into_iter()
                .map(str::to_string)
                .collect(),
            Value::Array(_) => vec!["add", "count"].into_iter().map(str::to_string).collect(),
            Value::Tuple(_) => vec!["count"].into_iter().map(str::to_string).collect(),
            Value::Dict(_) => vec!["count"].into_iter().map(str::to_string).collect(),
            Value::String(_) => vec![
                "length",
                "toUpper",
                "toLower",
                "trim",
                "split",
                "lines",
                "contains",
                "startsWith",
                "endsWith",
                "replace",
                "substring",
                "indexOf",
                "join",
                "padLeft",
                "padRight",
                "remove",
                "slice",
                "regexMatch",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
            Value::Number(_) => vec![
                "abs", "floor", "ceil", "round", "min", "max", "clamp", "pow", "sqrt", "toInt", "toFloat",
                "isInt", "toString",
            ]
            .into_iter()
            .map(str::to_string)
            .collect(),
            _ => Vec::new(),
        }
    }
}

enum ExecResult {
    Normal,
    Return(Value),
}

fn to_f64(v: Value) -> Result<f64, RuntimeError> {
    match v {
        Value::Number(n) => Ok(n),
        _ => Err(RuntimeError::new("Expected number".to_string(), "<unknown>".to_string(), 0, 0, None, Vec::new())),
    }
}

fn to_i64(v: Value) -> Result<i64, RuntimeError> {
    match v {
        Value::Number(n) => Ok(n as i64),
        _ => Err(RuntimeError::new("Expected number".to_string(), "<unknown>".to_string(), 0, 0, None, Vec::new())),
    }
}

fn now_hms() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    let secs = now.as_secs() % 86400;
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    format!("{:02}:{:02}:{:02}", h, m, s)
}

fn parallelism_cap() -> usize {
    if let Ok(val) = std::env::var("SHELLTRAC_PARALLELISM") {
        if let Ok(n) = val.parse::<usize>() {
            if n >= 1 {
                return n;
            }
        }
    }
    std::thread::available_parallelism().map(|n| n.get()).unwrap_or(4)
}

fn type_str(ty: &crate::parser::ast::TypeExpr) -> String {
    use crate::parser::ast::TypeExpr as T;
    match ty {
        T::Simple(s) => s.clone(),
        T::Array(inner) => format!("array<{}>", type_str(inner)),
        T::Tuple(items) => {
            let parts: Vec<String> = items.iter().map(type_str).collect();
            format!("tuple<{}>", parts.join(", "))
        }
        T::Map(k, v) => format!("map<{}, {}>", type_str(k), type_str(v)),
        T::Range(inner) => format!("range<{}>", type_str(inner)),
        T::Fn(args) => {
            let parts: Vec<String> = args.iter().map(type_str).collect();
            format!("fn<{}>", parts.join(", "))
        }
        T::Union(items) => items.iter().map(type_str).collect::<Vec<_>>().join("|"),
    }
}

fn type_matches(ty: &crate::parser::ast::TypeExpr, value: &Value) -> bool {
    use crate::parser::ast::TypeExpr as T;
    match ty {
        T::Simple(s) => match s.as_str() {
            "num" => matches!(value, Value::Number(_)),
            "string" => matches!(value, Value::String(_)),
            "bool" => matches!(value, Value::Bool(_)),
            "array" => matches!(value, Value::Array(_)),
            "tuple" => matches!(value, Value::Tuple(_)),
            "map" => matches!(value, Value::Dict(_)),
            "range" => matches!(value, Value::Range(_, _)),
            "task" => matches!(value, Value::Task(_)),
            "fn" => matches!(value, Value::Function(_) | Value::NativeFunction(_) | Value::NativeFunctionExec(_)),
            _ => true,
        },
        T::Array(inner) => match value {
            Value::Array(arr) => arr.lock().map(|items| items.iter().all(|v| type_matches(inner, v))).unwrap_or(true),
            _ => false,
        },
        T::Tuple(types) => match value {
            Value::Tuple(tup) => tup
                .lock()
                .map(|items| {
                    items.len() == types.len()
                        && items
                            .iter()
                            .zip(types.iter())
                            .all(|(v, t)| type_matches(t, v))
                })
                .unwrap_or(true),
            _ => false,
        },
        T::Map(k, v) => match value {
            Value::Dict(map) => {
                map.lock().map(|items| {
                    items.iter().all(|(key, val)| {
                        let key_val = Value::String(key.clone());
                        type_matches(k, &key_val) && type_matches(v, val)
                    })
                }).unwrap_or(true)
            }
            _ => false,
        },
        T::Range(inner) => match value {
            Value::Range(_, _) => type_matches(inner, &Value::Number(0.0)),
            _ => false,
        },
        T::Fn(_) => matches!(value, Value::Function(_) | Value::NativeFunction(_) | Value::NativeFunctionExec(_)),
        T::Union(items) => items.iter().any(|t| type_matches(t, value)),
    }
}

fn value_type_str(value: &Value) -> String {
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
        Value::Task(_) => "task".to_string(),
        Value::Function(_) => "fn".to_string(),
        Value::NativeFunction(_) => "fn".to_string(),
        Value::NativeFunctionExec(_) => "fn".to_string(),
        Value::Ufcs { .. } => "ufcs".to_string(),
    }
}

fn suggest_names(target: &str, candidates: &[String]) -> Vec<String> {
    let target_l = target.to_lowercase();
    let mut scored: Vec<(usize, String)> = candidates
        .iter()
        .filter_map(|c| {
            let c_l = c.to_lowercase();
            let score = if c_l == target_l {
                0
            } else if c_l.starts_with(&target_l) || target_l.starts_with(&c_l) {
                1
            } else if c_l.contains(&target_l) || target_l.contains(&c_l) {
                2
            } else {
                let d = levenshtein(&target_l, &c_l);
                if d <= 2 { 3 + d } else { return None; }
            };
            Some((score, c.clone()))
        })
        .collect();
    scored.sort_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
    scored.dedup_by(|a, b| a.1 == b.1);
    scored.into_iter().take(3).map(|(_, s)| s).collect()
}

fn levenshtein(a: &str, b: &str) -> usize {
    let a_bytes = a.as_bytes();
    let b_bytes = b.as_bytes();
    let mut prev: Vec<usize> = (0..=b_bytes.len()).collect();
    let mut curr: Vec<usize> = vec![0; b_bytes.len() + 1];
    for (i, &ac) in a_bytes.iter().enumerate() {
        curr[0] = i + 1;
        for (j, &bc) in b_bytes.iter().enumerate() {
            let cost = if ac == bc { 0 } else { 1 };
            curr[j + 1] = (curr[j] + 1).min(prev[j + 1] + 1).min(prev[j] + cost);
        }
        std::mem::swap(&mut prev, &mut curr);
    }
    prev[b_bytes.len()]
}
