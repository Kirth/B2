use std::collections::HashMap;
use std::sync::{Arc, Mutex, atomic::{AtomicBool, Ordering}, mpsc};
use std::thread;
use std::time::Duration;

use crate::parser::ast::{CallArg, Expr, InterpPart, MatchArmKind, ParamSpec, Pattern, Program, RecordField, Span, Stmt, TypeExpr};
use crate::runtime::errors::{Frame, RuntimeError};
use crate::runtime::builtins::{numbers, strings};
use crate::runtime::shell::{sh::run_sh, ssh::run_ssh};
use crate::runtime::value::Value;

#[derive(Clone)]
pub struct Function {
    pub params: Vec<ParamSpec>,
    pub return_type: Option<crate::parser::ast::TypeExpr>,
    pub body: Vec<Stmt>,
    pub captured: HashMap<String, Value>,
    pub name: String,
    pub is_generator: bool,
}

#[derive(Clone)]
pub enum RuntimeCallArg {
    Positional(Value),
    Spread(Value),
    Named { name: String, value: Value },
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

pub struct GeneratorHandle {
    state: Arc<Mutex<GeneratorState>>,
}

struct GeneratorState {
    pull_tx: mpsc::Sender<()>,
    event_rx: mpsc::Receiver<GeneratorEvent>,
    completed: bool,
    final_return: Option<Value>,
}

enum GeneratorEvent {
    Yield(Value),
    Return(Value),
    Error(RuntimeError),
}

struct GeneratorYieldContext {
    pull_rx: mpsc::Receiver<()>,
    event_tx: mpsc::Sender<GeneratorEvent>,
}

#[derive(Clone)]
struct RecordDef {
    fields: Vec<RecordField>,
}

pub struct Executor {
    filename: String,
    source: String,
    scopes: Vec<HashMap<String, Value>>,
    deferred_scopes: Vec<Vec<Vec<Stmt>>>,
    stack: Vec<Frame>,
    last_span: Span,
    output_sink: Option<Arc<dyn Fn(String) + Send + Sync>>,
    loop_depth: usize,
    type_aliases: HashMap<String, TypeExpr>,
    record_defs: HashMap<String, RecordDef>,
    generator_ctx: Option<GeneratorYieldContext>,
}

impl Executor {
    pub fn new(filename: String, source: String) -> Self {
        Executor {
            filename,
            source,
            scopes: vec![HashMap::new()],
            deferred_scopes: vec![Vec::new()],
            stack: Vec::new(),
            last_span: Span { line: 1, col: 1 },
            output_sink: None,
            loop_depth: 0,
            type_aliases: HashMap::new(),
            record_defs: HashMap::new(),
            generator_ctx: None,
        }
    }

    pub fn set_output_sink(&mut self, sink: Option<Arc<dyn Fn(String) + Send + Sync>>) {
        self.output_sink = sink;
    }

    pub fn set_context(&mut self, filename: String, source: String) {
        self.filename = filename;
        self.source = source;
        self.last_span = Span { line: 1, col: 1 };
        self.loop_depth = 0;
        self.deferred_scopes = (0..self.scopes.len()).map(|_| Vec::new()).collect();
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
                ExecResult::Continue => return Err(self.err("'continue' used outside loop", stmt.span())),
                ExecResult::Break => return Err(self.err("'break' used outside loop", stmt.span())),
                ExecResult::Throw(v) => {
                    return Err(self.err(&format!("Uncaught throw: {}", v.as_string()), stmt.span()))
                }
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
                    ExecResult::Continue => return Err(self.err("'continue' used outside loop", stmt.span())),
                    ExecResult::Break => return Err(self.err("'break' used outside loop", stmt.span())),
                    ExecResult::Throw(v) => {
                        return Err(self.err(&format!("Uncaught throw: {}", v.as_string()), stmt.span()))
                    }
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
                let res = if c.is_truthy() {
                    self.exec_block(then_branch)?
                } else {
                    self.exec_block(else_branch)?
                };
                Ok(res)
            }
            Stmt::For { pattern, iterable, body, span } => {
                let it = self.eval_expr(iterable)?;
                let items = self.iterate_value(it, *span)?;
                for item in items {
                    self.push_scope();
                    let bind_res = self.bind_pattern(pattern, item, *span);
                    if let Err(e) = bind_res {
                        let pending = self.exit_scope(Err(e));
                        return pending;
                    }
                    self.loop_depth += 1;
                    let body_res = self.exec_block(body);
                    self.loop_depth -= 1;
                    let res = self.exit_scope(body_res)?;
                    if let ExecResult::Return(v) = res {
                        return Ok(ExecResult::Return(v));
                    }
                    if let ExecResult::Continue = res {
                        continue;
                    }
                    if let ExecResult::Break = res {
                        break;
                    }
                    if let ExecResult::Throw(v) = res {
                        return Ok(ExecResult::Throw(v));
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
                    self.loop_depth += 1;
                    let body_res = self.exec_block(body);
                    self.loop_depth -= 1;
                    let res = body_res?;
                    if let ExecResult::Return(v) = res {
                        return Ok(ExecResult::Return(v));
                    }
                    if let ExecResult::Continue = res {
                        continue;
                    }
                    if let ExecResult::Break = res {
                        break;
                    }
                    if let ExecResult::Throw(v) = res {
                        return Ok(ExecResult::Throw(v));
                    }
                }
                Ok(ExecResult::Normal)
            }
            Stmt::Try { try_block, catch_name, catch_block, finally_block, .. } => {
                let mut pending: Result<ExecResult, RuntimeError> = match self.exec_block(try_block) {
                    Ok(ExecResult::Throw(v)) => {
                        if let (Some(name), Some(block)) = (catch_name.as_ref(), catch_block.as_ref()) {
                            self.push_scope();
                            self.define(name, v);
                            let catch_res = self.exec_block(block);
                            self.exit_scope(catch_res)
                        } else {
                            Ok(ExecResult::Throw(v))
                        }
                    }
                    Ok(other) => Ok(other),
                    Err(e) => {
                        if let (Some(name), Some(block)) = (catch_name.as_ref(), catch_block.as_ref()) {
                            let err_value = self.error_to_value(&e);
                            self.push_scope();
                            self.define(name, err_value);
                            let catch_res = self.exec_block(block);
                            self.exit_scope(catch_res)
                        } else {
                            Err(e)
                        }
                    }
                };

                if let Some(finally_block) = finally_block {
                    pending = match self.exec_block(finally_block) {
                        Ok(ExecResult::Normal) => pending,
                        Ok(other) => Ok(other),
                        Err(e) => Err(e),
                    };
                }
                pending
            }
            Stmt::Function {
                name,
                params,
                return_type,
                body,
                is_generator,
                ..
            } => {
                let captured = self.flatten_scopes();
                let func = Function {
                    params: params.clone(),
                    return_type: return_type.clone(),
                    body: body.clone(),
                    captured,
                    name: name.clone(),
                    is_generator: *is_generator,
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
            Stmt::Yield { value, span } => {
                if self.generator_ctx.is_none() {
                    return Err(self.err("'yield' used outside generator", *span));
                }
                let yielded = if let Some(expr) = value {
                    self.eval_expr(expr)?
                } else {
                    Value::Null
                };
                let ctx = self.generator_ctx.as_mut().expect("checked generator context");
                if ctx.event_tx.send(GeneratorEvent::Yield(yielded)).is_err() {
                    return Ok(ExecResult::Return(Value::Null));
                }
                if ctx.pull_rx.recv().is_err() {
                    return Ok(ExecResult::Return(Value::Null));
                }
                Ok(ExecResult::Normal)
            }
            Stmt::Continue { span } => {
                if self.loop_depth == 0 {
                    return Err(self.err("'continue' used outside loop", *span));
                }
                Ok(ExecResult::Continue)
            }
            Stmt::Break { span } => {
                if self.loop_depth == 0 {
                    return Err(self.err("'break' used outside loop", *span));
                }
                Ok(ExecResult::Break)
            }
            Stmt::Throw { value, .. } => {
                let v = self.eval_expr(value)?;
                Ok(ExecResult::Throw(v))
            }
            Stmt::Defer { body, .. } => {
                if let Some(scope) = self.deferred_scopes.last_mut() {
                    scope.push(body.clone());
                    Ok(ExecResult::Normal)
                } else {
                    Err(self.err("No active scope for defer", stmt.span()))
                }
            }
            Stmt::TypeAlias { name, target, span } => {
                if self.record_defs.contains_key(name) {
                    return Err(self.err(&format!("Type name '{}' conflicts with existing record", name), *span));
                }
                if self.type_aliases.contains_key(name) {
                    return Err(self.err(&format!("Type '{}' is already defined", name), *span));
                }
                self.type_aliases.insert(name.clone(), target.clone());
                Ok(ExecResult::Normal)
            }
            Stmt::RecordDef { name, fields, span } => {
                if self.type_aliases.contains_key(name) {
                    return Err(self.err(&format!("Record name '{}' conflicts with existing type alias", name), *span));
                }
                let mut seen = std::collections::HashSet::new();
                for field in fields {
                    if field.name == "__type" {
                        return Err(self.err("Record field name '__type' is reserved", *span));
                    }
                    if !seen.insert(field.name.clone()) {
                        return Err(self.err(&format!("Duplicate record field '{}'", field.name), *span));
                    }
                }
                self.record_defs.insert(name.clone(), RecordDef { fields: fields.clone() });
                Ok(ExecResult::Normal)
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
        let mut pending: Result<ExecResult, RuntimeError> = Ok(ExecResult::Normal);
        for stmt in stmts {
            match self.exec_stmt(stmt) {
                Ok(ExecResult::Normal) => {}
                Ok(other) => {
                    pending = Ok(other);
                    break;
                }
                Err(e) => {
                    pending = Err(e);
                    break;
                }
            }
        }
        self.exit_scope(pending)
    }

    fn exec_parallel_for(&mut self, pattern: &Pattern, iterable: &Expr, body: &[Stmt]) -> Result<(), RuntimeError> {
        let it = self.eval_expr(iterable)?;
        let items = self.iterate_value(it, iterable.span())?;
        if items.is_empty() {
            return Ok(());
        }

        let captured = self.flatten_scopes();
        let type_aliases = self.type_aliases.clone();
        let record_defs = self.record_defs.clone();
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
            let worker_aliases = type_aliases.clone();
            let worker_records = record_defs.clone();

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
                    let mut exec = Executor {
                        filename: filename.clone(),
                        source: source.clone(),
                        scopes: vec![captured_env.clone()],
                        deferred_scopes: vec![Vec::new()],
                        stack: Vec::new(),
                        last_span: Span { line: 1, col: 1 },
                        output_sink: output_sink.clone(),
                        loop_depth: 0,
                        type_aliases: worker_aliases.clone(),
                        record_defs: worker_records.clone(),
                        generator_ctx: None,
                    };
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
                let mut a = Vec::new();
                for arg in args {
                    match arg {
                        CallArg::Positional(expr) => {
                            a.push(RuntimeCallArg::Positional(self.eval_expr(expr)?));
                        }
                        CallArg::Spread(expr) => {
                            a.push(RuntimeCallArg::Spread(self.eval_expr(expr)?));
                        }
                        CallArg::Named { name, value } => {
                            a.push(RuntimeCallArg::Named {
                                name: name.clone(),
                                value: self.eval_expr(value)?,
                            });
                        }
                    }
                }
                if let Expr::Var { name, .. } = callee.as_ref() {
                    if let Some(v) = self.lookup(name) {
                        return self.call(v, a, *span);
                    }
                    if self.record_defs.contains_key(name) {
                        return self.construct_record(name, a, *span);
                    }
                    if self.type_aliases.contains_key(name) {
                        return self.construct_nominal(name, a, *span);
                    }
                    return Err(self.unknown_global_function_error(name, *span));
                }
                let c = self.eval_expr(callee)?;
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
            Expr::ArrayComprehension {
                pattern,
                iterable,
                guard,
                map_expr,
                span,
            } => {
                let it = self.eval_expr(iterable)?;
                let items = self.iterate_value(it, *span)?;
                let mut out = Vec::new();
                for item in items {
                    self.push_scope();
                    if let Err(e) = self.bind_pattern(pattern, item, *span) {
                        return match self.exit_scope(Err(e)) {
                            Ok(_) => Err(self.err("Array comprehension binding failed", *span)),
                            Err(e2) => Err(e2),
                        };
                    }
                    let should_include = if let Some(cond) = guard.as_ref() {
                        self.eval_expr(cond)?.is_truthy()
                    } else {
                        true
                    };
                    if should_include {
                        out.push(self.eval_expr(map_expr.as_ref())?);
                    }
                    self.exit_scope(Ok(ExecResult::Normal))?;
                }
                Ok(Value::array(out))
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
            Expr::Match { subject, arms, span } => {
                let subject_value = self.eval_expr(subject)?;
                for arm in arms {
                    let matched = match &arm.kind {
                        MatchArmKind::Wildcard => true,
                        MatchArmKind::Value(value_expr) => {
                            let candidate = self.eval_expr(value_expr)?;
                            subject_value.as_string() == candidate.as_string()
                        }
                        MatchArmKind::Compare { op, rhs } => {
                            let rhs_value = self.eval_expr(rhs)?;
                            let cmp = self.eval_binary(subject_value.clone(), op, rhs_value, *span)?;
                            cmp.is_truthy()
                        }
                    };
                    if matched {
                        return self.eval_block_expr(&arm.body);
                    }
                }
                Err(self.err("No match arm matched and wildcard arm was not reached", *span))
            }
            Expr::ParallelFor { pattern, iterable, body, .. } => {
                self.eval_parallel_for_expr(pattern, iterable, body)
            }
            Expr::TaskBlock { body, .. } => {
                let body = body.clone();
                let captured = self.flatten_scopes();
                let type_aliases = self.type_aliases.clone();
                let record_defs = self.record_defs.clone();
                let filename = self.filename.clone();
                let source = self.source.clone();
                let output_sink = self.output_sink.clone();
                Ok(self.spawn_task(move || {
                    let mut exec = Executor {
                        filename,
                        source,
                        scopes: vec![captured],
                        deferred_scopes: vec![Vec::new()],
                        stack: Vec::new(),
                        last_span: Span { line: 1, col: 1 },
                        output_sink,
                        loop_depth: 0,
                        type_aliases,
                        record_defs,
                        generator_ctx: None,
                    };
                    exec.eval_block_expr(&body)
                }))
            }
            Expr::TaskCall { callee, args, .. } => {
                let callee_value = self.eval_expr(callee)?;
                let mut arg_values = Vec::new();
                for arg in args {
                    match arg {
                        CallArg::Positional(expr) => {
                            arg_values.push(RuntimeCallArg::Positional(self.eval_expr(expr)?));
                        }
                        CallArg::Spread(expr) => {
                            arg_values.push(RuntimeCallArg::Spread(self.eval_expr(expr)?));
                        }
                        CallArg::Named { name, value } => {
                            arg_values.push(RuntimeCallArg::Named {
                                name: name.clone(),
                                value: self.eval_expr(value)?,
                            });
                        }
                    }
                }
                let captured = self.flatten_scopes();
                let type_aliases = self.type_aliases.clone();
                let record_defs = self.record_defs.clone();
                let filename = self.filename.clone();
                let source = self.source.clone();
                let output_sink = self.output_sink.clone();
                Ok(self.spawn_task(move || {
                    let mut exec = Executor {
                        filename,
                        source,
                        scopes: vec![captured],
                        deferred_scopes: vec![Vec::new()],
                        stack: Vec::new(),
                        last_span: Span { line: 1, col: 1 },
                        output_sink,
                        loop_depth: 0,
                        type_aliases,
                        record_defs,
                        generator_ctx: None,
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
                    is_generator: block_contains_yield(body),
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
        let mut pending: Result<ExecResult, RuntimeError> = Ok(ExecResult::Normal);
        for stmt in stmts {
            match stmt {
                Stmt::Expr { expr, .. } => last = self.eval_expr(expr)?,
                _ => {
                    match self.exec_stmt(stmt) {
                        Ok(ExecResult::Normal) => {}
                        Ok(other) => {
                            pending = Ok(other);
                            break;
                        }
                        Err(e) => {
                            pending = Err(e);
                            break;
                        }
                    }
                }
            }
        }
        match self.exit_scope(pending)? {
            ExecResult::Normal => Ok(last),
            ExecResult::Return(v) => Ok(v),
            ExecResult::Continue => Ok(last),
            ExecResult::Break => Ok(last),
            ExecResult::Throw(v) => Err(self.err(&format!("Uncaught throw: {}", v.as_string()), self.last_span)),
        }
    }

    fn eval_parallel_for_expr(&mut self, pattern: &Pattern, iterable: &Expr, body: &[Stmt]) -> Result<Value, RuntimeError> {
        let it = self.eval_expr(iterable)?;
        let items = self.iterate_value(it, iterable.span())?;
        if items.is_empty() {
            return Ok(Value::array(Vec::new()));
        }

        let captured = self.flatten_scopes();
        let type_aliases = self.type_aliases.clone();
        let record_defs = self.record_defs.clone();
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
            let worker_aliases = type_aliases.clone();
            let worker_records = record_defs.clone();

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
                        deferred_scopes: vec![Vec::new()],
                        stack: Vec::new(),
                        last_span: Span { line: 1, col: 1 },
                        output_sink: output_sink.clone(),
                        loop_depth: 0,
                        type_aliases: worker_aliases.clone(),
                        record_defs: worker_records.clone(),
                        generator_ctx: None,
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

    fn spawn_generator(
        &self,
        mut gen_exec: Executor,
        func: Arc<Function>,
        args: Vec<RuntimeCallArg>,
        span: Span,
    ) -> Value {
        let (pull_tx, pull_rx) = mpsc::channel::<()>();
        let (event_tx, event_rx) = mpsc::channel::<GeneratorEvent>();

        thread::spawn(move || {
            let _ = pull_rx.recv();
            gen_exec.generator_ctx = Some(GeneratorYieldContext {
                pull_rx,
                event_tx: event_tx.clone(),
            });
            let result = gen_exec.call_user_function(&func, args, span);
            match result {
                Ok(v) => {
                    let _ = event_tx.send(GeneratorEvent::Return(v));
                }
                Err(e) => {
                    let _ = event_tx.send(GeneratorEvent::Error(e));
                }
            }
        });

        let handle = GeneratorHandle {
            state: Arc::new(Mutex::new(GeneratorState {
                pull_tx,
                event_rx,
                completed: false,
                final_return: None,
            })),
        };
        Value::Generator(Arc::new(handle))
    }

    pub fn next_generator(&self, generator: &Arc<GeneratorHandle>, span: Span) -> Result<Value, RuntimeError> {
        match self.generator_next_step(generator, span)? {
            GeneratorStep::Yield(v) => Ok(generator_step_dict(false, v, Value::Null)),
            GeneratorStep::Done(v) => Ok(generator_step_dict(true, Value::Null, v)),
        }
    }

    fn generator_next_step(
        &self,
        generator: &Arc<GeneratorHandle>,
        span: Span,
    ) -> Result<GeneratorStep, RuntimeError> {
        let mut state = generator
            .state
            .lock()
            .map_err(|_| self.err("Generator state lock poisoned", span))?;
        if state.completed {
            return Ok(GeneratorStep::Done(
                state.final_return.clone().unwrap_or(Value::Null),
            ));
        }
        state
            .pull_tx
            .send(())
            .map_err(|_| self.err("Generator is closed", span))?;
        let event = state
            .event_rx
            .recv()
            .map_err(|_| self.err("Generator is closed", span))?;
        match event {
            GeneratorEvent::Yield(v) => Ok(GeneratorStep::Yield(v)),
            GeneratorEvent::Return(v) => {
                state.completed = true;
                state.final_return = Some(v.clone());
                Ok(GeneratorStep::Done(v))
            }
            GeneratorEvent::Error(e) => {
                state.completed = true;
                Err(e)
            }
        }
    }

    fn call(&mut self, callee: Value, args: Vec<RuntimeCallArg>, span: Span) -> Result<Value, RuntimeError> {
        match callee {
            Value::NativeFunction(f) => {
                let positional = self.positional_only_args(args, span)?;
                f(positional).map_err(|e| self.err(&e, span))
            }
            Value::NativeFunctionExec(f) => {
                let positional = self.positional_only_args(args, span)?;
                f(self, positional, span)
            }
            Value::Ufcs { name, receiver } => {
                let mut new_args = vec![RuntimeCallArg::Positional(*receiver)];
                new_args.extend(args);
                let recv = match &new_args[0] {
                    RuntimeCallArg::Positional(v) => v.clone(),
                    RuntimeCallArg::Spread(_) => Value::Null,
                    RuntimeCallArg::Named { .. } => Value::Null,
                };
                let target = self
                    .lookup(&name)
                    .ok_or_else(|| self.unknown_method_or_function_error(&name, &recv, span))?;
                self.call(target, new_args, span)
            }
            Value::Function(func) => {
                if func.is_generator {
                    let gen_exec = Executor {
                        filename: self.filename.clone(),
                        source: self.source.clone(),
                        scopes: vec![func.captured.clone()],
                        deferred_scopes: vec![Vec::new()],
                        stack: Vec::new(),
                        last_span: Span { line: 1, col: 1 },
                        output_sink: self.output_sink.clone(),
                        loop_depth: 0,
                        type_aliases: self.type_aliases.clone(),
                        record_defs: self.record_defs.clone(),
                        generator_ctx: None,
                    };
                    Ok(self.spawn_generator(gen_exec, func, args, span))
                } else {
                    self.push_frame(func.name.clone(), span);
                    let res = self.call_user_function(&func, args, span);
                    self.pop_frame();
                    res
                }
            }
            _ => Err(self.err("Not callable", span)),
        }
    }

    pub fn call_value(&mut self, callee: Value, args: Vec<RuntimeCallArg>) -> Result<Value, RuntimeError> {
        self.call(callee, args, Span { line: 0, col: 0 })
    }

    pub fn collect_iterable(&self, value: Value, span: Span) -> Result<Vec<Value>, RuntimeError> {
        self.iterate_value(value, span)
    }

    fn positional_only_args(&self, args: Vec<RuntimeCallArg>, span: Span) -> Result<Vec<Value>, RuntimeError> {
        let mut out = Vec::new();
        for arg in args {
            match arg {
                RuntimeCallArg::Positional(v) => out.push(v),
                RuntimeCallArg::Spread(v) => {
                    out.extend(self.expand_spread_value(v, span)?);
                }
                RuntimeCallArg::Named { .. } => {
                    return Err(self.err("Named arguments are only supported for user-defined functions in v1", span));
                }
            }
        }
        Ok(out)
    }

    fn call_user_function(&mut self, func: &Function, args: Vec<RuntimeCallArg>, span: Span) -> Result<Value, RuntimeError> {
        let mut positional = Vec::new();
        let mut named: HashMap<String, Value> = HashMap::new();
        for arg in args {
            match arg {
                RuntimeCallArg::Positional(v) => positional.push(v),
                RuntimeCallArg::Spread(v) => {
                    positional.extend(self.expand_spread_value(v, span)?);
                }
                RuntimeCallArg::Named { name, value } => {
                    if named.insert(name.clone(), value).is_some() {
                        return Err(self.err(&format!("Duplicate named argument '{name}' for {}", func.name), span));
                    }
                }
            }
        }

        self.push_scope_with(func.captured.clone());
        let mut scope_closed = false;
        let mut result = (|| -> Result<Value, RuntimeError> {
            let variadic_param = func.params.iter().position(|p| p.variadic);
            if let Some(vidx) = variadic_param {
                if vidx != func.params.len() - 1 {
                    return Err(self.err(
                        &format!("Variadic parameter for {} must be the final parameter", func.name),
                        span,
                    ));
                }
            }

            let fixed_count = if variadic_param.is_some() {
                func.params.len().saturating_sub(1)
            } else {
                func.params.len()
            };

            if variadic_param.is_none() && positional.len() > func.params.len() {
                return Err(self.err(
                    &format!(
                        "Too many positional arguments for {}: expected at most {}, got {}",
                        func.name,
                        func.params.len(),
                        positional.len()
                    ),
                    span,
                ));
            }

            let mut positional_index = 0usize;
            for (param_idx, param) in func.params.iter().enumerate() {
                if param.variadic {
                    if named.contains_key(&param.name) {
                        return Err(self.err(
                            &format!("Variadic parameter '{}' for {} cannot be passed by name", param.name, func.name),
                            span,
                        ));
                    }
                    let mut rest_items = Vec::new();
                    while positional_index < positional.len() {
                        let v = positional[positional_index].clone();
                        if let Some(t) = &param.ty {
                            if !self.type_matches_checked(t, &v, span)? {
                                return Err(self.err(
                                    &format!(
                                        "Type mismatch for variadic {}.{}: expected {}, got {}",
                                        func.name,
                                        param.name,
                                        self.type_str(t),
                                        value_type_str(&v)
                                    ),
                                    span,
                                ));
                            }
                        }
                        rest_items.push(v);
                        positional_index += 1;
                    }
                    self.define(&param.name, Value::array(rest_items));
                    continue;
                }

                if param_idx >= fixed_count {
                    break;
                }
                let value = if positional_index < positional.len() {
                    if named.contains_key(&param.name) {
                        return Err(self.err(
                            &format!("Argument '{}' for {} was provided both positionally and by name", param.name, func.name),
                            span,
                        ));
                    }
                    let v = positional[positional_index].clone();
                    positional_index += 1;
                    v
                } else if let Some(v) = named.remove(&param.name) {
                    v
                } else if let Some(default_expr) = &param.default {
                    self.eval_expr(default_expr)?
                } else {
                    return Err(self.err(
                        &format!("Missing required argument '{}' for {}", param.name, func.name),
                        span,
                    ));
                };

                if let Some(t) = &param.ty {
                    if !self.type_matches_checked(t, &value, span)? {
                        return Err(self.err(
                            &format!(
                                "Type mismatch for {}.{}: expected {}, got {}",
                                func.name,
                                param.name,
                                self.type_str(t),
                                value_type_str(&value)
                            ),
                            span,
                        ));
                    }
                }
                self.define(&param.name, value);
            }

            if variadic_param.is_none() && positional_index < positional.len() {
                return Err(self.err(
                    &format!(
                        "Too many positional arguments for {}: expected at most {}, got {}",
                        func.name,
                        func.params.len(),
                        positional.len()
                    ),
                    span,
                ));
            }

            if let Some((unknown, _)) = named.iter().next() {
                return Err(self.err(
                    &format!("Unknown named argument '{}' for {}", unknown, func.name),
                    span,
                ));
            }

            let body_res = self.exec_block(&func.body);
            let res = self.exit_scope(body_res);
            scope_closed = true;
            let res = res?;
            let out = match res {
                ExecResult::Normal => Value::Null,
                ExecResult::Return(v) => v,
                ExecResult::Continue => return Err(self.err("'continue' used outside loop", span)),
                ExecResult::Break => return Err(self.err("'break' used outside loop", span)),
                ExecResult::Throw(v) => {
                    return Err(self.err(&format!("Uncaught throw: {}", v.as_string()), span))
                }
            };
            if let Some(t) = &func.return_type {
                if !self.type_matches_checked(t, &out, span)? {
                    return Err(self.err(
                        &format!(
                            "Return type mismatch for {}: expected {}, got {}",
                            func.name,
                            self.type_str(t),
                            value_type_str(&out)
                        ),
                        span,
                    ));
                }
            }
            Ok(out)
        })();

        if !scope_closed {
            result = match result {
                Ok(v) => {
                    self.exit_scope(Ok(ExecResult::Normal))?;
                    Ok(v)
                }
                Err(e) => match self.exit_scope(Err(e)) {
                    Ok(_) => Ok(Value::Null),
                    Err(e2) => Err(e2),
                },
            };
        }

        result
    }

    fn expand_spread_value(&self, value: Value, span: Span) -> Result<Vec<Value>, RuntimeError> {
        match value {
            Value::Array(arr) => Ok(arr.lock().map(|v| v.clone()).unwrap_or_else(|_| Vec::new())),
            Value::Tuple(tup) => Ok(tup.lock().map(|v| v.clone()).unwrap_or_else(|_| Vec::new())),
            _ => Err(self.err("Spread argument expects array or tuple", span)),
        }
    }

    fn construct_record(&mut self, record_name: &str, args: Vec<RuntimeCallArg>, span: Span) -> Result<Value, RuntimeError> {
        let def = self
            .record_defs
            .get(record_name)
            .cloned()
            .ok_or_else(|| self.err(&format!("Unknown record '{}'", record_name), span))?;

        let mut provided = HashMap::<String, Value>::new();
        for arg in args {
            match arg {
                RuntimeCallArg::Named { name, value } => {
                    if provided.insert(name.clone(), value).is_some() {
                        return Err(self.err(&format!("Duplicate record field '{}'", name), span));
                    }
                }
                RuntimeCallArg::Positional(_) | RuntimeCallArg::Spread(_) => {
                    return Err(self.err(
                        &format!("Record constructor {}(...) expects named arguments only", record_name),
                        span,
                    ));
                }
            }
        }

        self.push_scope();
        let mut map = HashMap::<String, Value>::new();
        map.insert("__type".to_string(), Value::String(record_name.to_string()));

        let mut pending: Result<Value, RuntimeError> = Ok(Value::Null);
        for field in &def.fields {
            let val = if let Some(v) = provided.remove(&field.name) {
                v
            } else if let Some(default) = &field.default {
                self.eval_expr(default)?
            } else {
                pending = Err(self.err(
                    &format!("Record '{}' missing required field '{}'", record_name, field.name),
                    span,
                ));
                break;
            };

            if !self.type_matches_checked(&field.ty, &val, span)? {
                pending = Err(self.err(
                    &format!(
                        "Type mismatch for {}.{}: expected {}, got {}",
                        record_name,
                        field.name,
                        self.type_str(&field.ty),
                        value_type_str(&val)
                    ),
                    span,
                ));
                break;
            }
            self.define(&field.name, val.clone());
            map.insert(field.name.clone(), val);
        }

        if pending.is_ok() {
            if let Some((unknown, _)) = provided.iter().next() {
                pending = Err(self.err(
                    &format!("Record '{}' has unknown field '{}'", record_name, unknown),
                    span,
                ));
            } else {
                pending = Ok(Value::Dict(Arc::new(Mutex::new(map))));
            }
        }

        let scope_out = match pending {
            Ok(v) => {
                self.exit_scope(Ok(ExecResult::Normal))?;
                Ok(v)
            }
            Err(e) => match self.exit_scope(Err(e)) {
                Ok(_) => Ok(Value::Null),
                Err(e2) => Err(e2),
            },
        }?;
        Ok(scope_out)
    }

    fn construct_nominal(&mut self, type_name: &str, args: Vec<RuntimeCallArg>, span: Span) -> Result<Value, RuntimeError> {
        let base = self
            .type_aliases
            .get(type_name)
            .cloned()
            .ok_or_else(|| self.err(&format!("Unknown type '{}'", type_name), span))?;

        if args.len() != 1 {
            return Err(self.err(
                &format!("Type constructor {}(...) expects exactly 1 positional argument", type_name),
                span,
            ));
        }
        let value = match args.into_iter().next().unwrap() {
            RuntimeCallArg::Positional(v) => v,
            RuntimeCallArg::Spread(_) | RuntimeCallArg::Named { .. } => {
                return Err(self.err(
                    &format!("Type constructor {}(...) expects exactly 1 positional argument", type_name),
                    span,
                ));
            }
        };

        if !self.type_matches_checked(&base, &value, span)? {
            return Err(self.err(
                &format!(
                    "Type constructor {} expects {}, got {}",
                    type_name,
                    self.type_str(&base),
                    value_type_str(&value)
                ),
                span,
            ));
        }

        Ok(Value::Nominal {
            name: type_name.to_string(),
            inner: Box::new(value),
        })
    }

    fn type_str(&self, ty: &TypeExpr) -> String {
        type_str(ty)
    }

    fn type_matches_checked(&self, ty: &TypeExpr, value: &Value, span: Span) -> Result<bool, RuntimeError> {
        type_matches_with_resolver(ty, value, &self.type_aliases, &self.record_defs)
            .map_err(|e| self.err(&e, span))
    }

    pub fn validate_type_by_name(&self, value: &Value, type_name: &str) -> Result<bool, String> {
        type_matches_with_resolver(
            &TypeExpr::Simple(type_name.to_string()),
            value,
            &self.type_aliases,
            &self.record_defs,
        )
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

    fn iterate_value(&self, val: Value, span: Span) -> Result<Vec<Value>, RuntimeError> {
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
            Value::Generator(generator) => {
                let mut out = Vec::new();
                loop {
                    match self.generator_next_step(&generator, span)? {
                        GeneratorStep::Yield(v) => out.push(v),
                        GeneratorStep::Done(_) => break,
                    }
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
            _ => Err(self.err("Not iterable", span)),
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
        self.deferred_scopes.push(Vec::new());
    }

    fn push_scope_with(&mut self, env: HashMap<String, Value>) {
        self.scopes.push(env);
        self.deferred_scopes.push(Vec::new());
    }

    fn exit_scope(&mut self, mut pending: Result<ExecResult, RuntimeError>) -> Result<ExecResult, RuntimeError> {
        let deferred_blocks = self.deferred_scopes.pop().unwrap_or_default();
        for block in deferred_blocks.into_iter().rev() {
            match self.exec_block(&block) {
                Ok(ExecResult::Normal) => {}
                Ok(other) => pending = Ok(other),
                Err(e) => pending = Err(e),
            }
        }
        if self.scopes.len() > 1 {
            self.scopes.pop();
        }
        pending
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
        let mut names: Vec<String> = self.scopes[0]
            .iter()
            .filter_map(|(k, v)| match v {
                Value::Function(_) | Value::NativeFunction(_) | Value::NativeFunctionExec(_) => Some(k.clone()),
                _ => None,
            })
            .collect();
        names.extend(self.record_defs.keys().cloned());
        names.extend(self.type_aliases.keys().cloned());
        names
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
    Continue,
    Break,
    Throw(Value),
}

enum GeneratorStep {
    Yield(Value),
    Done(Value),
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

fn generator_step_dict(done: bool, value: Value, final_return: Value) -> Value {
    let mut map = HashMap::new();
    map.insert("done".to_string(), Value::Bool(done));
    map.insert("value".to_string(), value);
    map.insert("return".to_string(), final_return);
    Value::Dict(Arc::new(Mutex::new(map)))
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

fn type_matches_with_resolver(
    ty: &crate::parser::ast::TypeExpr,
    value: &Value,
    type_aliases: &HashMap<String, TypeExpr>,
    record_defs: &HashMap<String, RecordDef>,
) -> Result<bool, String> {
    use crate::parser::ast::TypeExpr as T;
    match ty {
        T::Simple(s) => match s.as_str() {
            "num" => Ok(matches!(value, Value::Number(_))),
            "string" => Ok(matches!(value, Value::String(_))),
            "bool" => Ok(matches!(value, Value::Bool(_))),
            "array" => Ok(matches!(value, Value::Array(_))),
            "tuple" => Ok(matches!(value, Value::Tuple(_))),
            "map" => Ok(matches!(value, Value::Dict(_))),
            "range" => Ok(matches!(value, Value::Range(_, _))),
            "task" => Ok(matches!(value, Value::Task(_))),
            "generator" => Ok(matches!(value, Value::Generator(_))),
            "fn" => Ok(matches!(value, Value::Function(_) | Value::NativeFunction(_) | Value::NativeFunctionExec(_))),
            other => {
                if record_defs.contains_key(other) {
                    return Ok(record_type_name(value).as_deref() == Some(other));
                }
                if type_aliases.contains_key(other) {
                    return Ok(matches!(
                        value,
                        Value::Nominal { name, .. } if name == other
                    ));
                }
                Err(format!("Unknown type '{}'", other))
            }
        },
        T::Array(inner) => match value {
            Value::Array(arr) => {
                let items = arr.lock().map(|v| v.clone()).unwrap_or_else(|_| Vec::new());
                for item in &items {
                    if !type_matches_with_resolver(inner, item, type_aliases, record_defs)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            _ => Ok(false),
        },
        T::Tuple(types) => match value {
            Value::Tuple(tup) => {
                let items = tup.lock().map(|v| v.clone()).unwrap_or_else(|_| Vec::new());
                if items.len() != types.len() {
                    return Ok(false);
                }
                for (item, ty) in items.iter().zip(types.iter()) {
                    if !type_matches_with_resolver(ty, item, type_aliases, record_defs)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            _ => Ok(false),
        },
        T::Map(k, v) => match value {
            Value::Dict(map) => {
                let items = map.lock().map(|m| m.clone()).unwrap_or_else(|_| HashMap::new());
                for (key, val) in items {
                    let key_val = Value::String(key);
                    if !type_matches_with_resolver(k, &key_val, type_aliases, record_defs)? {
                        return Ok(false);
                    }
                    if !type_matches_with_resolver(v, &val, type_aliases, record_defs)? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            _ => Ok(false),
        },
        T::Range(inner) => match value {
            Value::Range(_, _) => type_matches_with_resolver(inner, &Value::Number(0.0), type_aliases, record_defs),
            _ => Ok(false),
        },
        T::Fn(_) => Ok(matches!(value, Value::Function(_) | Value::NativeFunction(_) | Value::NativeFunctionExec(_))),
        T::Union(items) => {
            for ty in items {
                if type_matches_with_resolver(ty, value, type_aliases, record_defs)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
    }
}

fn record_type_name(value: &Value) -> Option<String> {
    match value {
        Value::Dict(map) => map
            .lock()
            .ok()
            .and_then(|m| m.get("__type").cloned())
            .and_then(|v| match v {
                Value::String(s) => Some(s),
                _ => None,
            }),
        _ => None,
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
        Value::Nominal { name, .. } => format!("type<{name}>"),
        Value::Task(_) => "task".to_string(),
        Value::Generator(_) => "generator".to_string(),
        Value::Function(_) => "fn".to_string(),
        Value::NativeFunction(_) => "fn".to_string(),
        Value::NativeFunctionExec(_) => "fn".to_string(),
        Value::Ufcs { .. } => "ufcs".to_string(),
    }
}

fn block_contains_yield(stmts: &[Stmt]) -> bool {
    stmts.iter().any(stmt_contains_yield)
}

fn stmt_contains_yield(stmt: &Stmt) -> bool {
    match stmt {
        Stmt::Yield { .. } => true,
        Stmt::If {
            then_branch,
            else_branch,
            ..
        } => block_contains_yield(then_branch) || block_contains_yield(else_branch),
        Stmt::For { body, .. }
        | Stmt::ParallelFor { body, .. }
        | Stmt::While { body, .. }
        | Stmt::Defer { body, .. } => block_contains_yield(body),
        Stmt::Try {
            try_block,
            catch_block,
            finally_block,
            ..
        } => {
            block_contains_yield(try_block)
                || catch_block
                    .as_ref()
                    .map(|b| block_contains_yield(b))
                    .unwrap_or(false)
                || finally_block
                    .as_ref()
                    .map(|b| block_contains_yield(b))
                    .unwrap_or(false)
        }
        Stmt::Expr { expr, .. } => expr_contains_yield(expr),
        Stmt::Let { expr, .. }
        | Stmt::LetDestructure { expr, .. }
        | Stmt::Assign { expr, .. }
        | Stmt::Return {
            value: Some(expr), ..
        }
        | Stmt::Throw { value: expr, .. }
        | Stmt::Invoke { expr, .. } => expr_contains_yield(expr),
        Stmt::IndexAssign {
            target, index, expr, ..
        } => {
            expr_contains_yield(target) || expr_contains_yield(index) || expr_contains_yield(expr)
        }
        Stmt::Return { value: None, .. }
        | Stmt::Continue { .. }
        | Stmt::Break { .. }
        | Stmt::TypeAlias { .. }
        | Stmt::RecordDef { .. }
        | Stmt::Function { .. } => false,
    }
}

fn expr_contains_yield(expr: &Expr) -> bool {
    match expr {
        Expr::Binary { left, right, .. } => expr_contains_yield(left) || expr_contains_yield(right),
        Expr::Unary { expr, .. } => expr_contains_yield(expr),
        Expr::Call { callee, args, .. } => {
            expr_contains_yield(callee)
                || args.iter().any(|arg| match arg {
                    CallArg::Positional(e) | CallArg::Spread(e) => expr_contains_yield(e),
                    CallArg::Named { value, .. } => expr_contains_yield(value),
                })
        }
        Expr::Member { object, .. } => expr_contains_yield(object),
        Expr::Index { object, index, .. } => expr_contains_yield(object) || expr_contains_yield(index),
        Expr::Array { items, .. } | Expr::Tuple { items, .. } => {
            items.iter().any(expr_contains_yield)
        }
        Expr::Dict { items, .. } => items
            .iter()
            .any(|(k, v)| expr_contains_yield(k) || expr_contains_yield(v)),
        Expr::ArrayComprehension {
            iterable,
            guard,
            map_expr,
            ..
        } => {
            expr_contains_yield(iterable)
                || guard
                    .as_ref()
                    .map(|g| expr_contains_yield(g))
                    .unwrap_or(false)
                || expr_contains_yield(map_expr)
        }
        Expr::Range { start, end, .. } => expr_contains_yield(start) || expr_contains_yield(end),
        Expr::IfExpr {
            cond,
            then_branch,
            else_branch,
            ..
        } => expr_contains_yield(cond) || block_contains_yield(then_branch) || block_contains_yield(else_branch),
        Expr::Match { subject, arms, .. } => {
            expr_contains_yield(subject)
                || arms.iter().any(|arm| match &arm.kind {
                    MatchArmKind::Value(e) => expr_contains_yield(e) || block_contains_yield(&arm.body),
                    MatchArmKind::Compare { rhs, .. } => {
                        expr_contains_yield(rhs) || block_contains_yield(&arm.body)
                    }
                    MatchArmKind::Wildcard => block_contains_yield(&arm.body),
                })
        }
        Expr::ParallelFor { iterable, body, .. } => {
            expr_contains_yield(iterable) || block_contains_yield(body)
        }
        Expr::TaskBlock { .. } => false,
        Expr::TaskCall { callee, args, .. } => {
            expr_contains_yield(callee)
                || args.iter().any(|arg| match arg {
                    CallArg::Positional(e) | CallArg::Spread(e) => expr_contains_yield(e),
                    CallArg::Named { value, .. } => expr_contains_yield(value),
                })
        }
        Expr::Await { expr, .. } => expr_contains_yield(expr),
        Expr::Lambda { .. } => false,
        Expr::InterpolatedString { parts, .. } => parts.iter().any(|p| match p {
            InterpPart::Literal(_) => false,
            InterpPart::Expr(e) => expr_contains_yield(e),
        }),
        Expr::Sh { command, .. } => expr_contains_yield(command),
        Expr::Ssh { host, command, .. } => expr_contains_yield(host) || expr_contains_yield(command),
        Expr::Literal { .. } | Expr::Var { .. } => false,
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
