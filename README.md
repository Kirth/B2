# B2

A toy scripting language runtime in Rust. B2 is built for **shell and SSH orchestration**: run local or remote commands, get structured results (stdout, stderr, exit code), then process them with a simple language—functions, arrays, loops, and parallel iteration.

Spiritual successor to Shelltrac: script with shell/SSH results, but faster.

---

## Prerequisites

- [Rust](https://rustup.rs) (1.85+ for edition 2024; or set `edition = "2021"` in `Cargo.toml` for older toolchains)

---

## Quick start

**Run a script:**

```bash
cargo run -- hello.s
```

**Interactive REPL:**

```bash
cargo run -- repl
```

**Run from stdin:**

```bash
cargo run -- < script.s
```

---

## Language basics

- **Variables:** `let x = 10`  
- **Functions:** `fn greet(name, suffix = "!") { return "hello, #{name}#{suffix}" }`  
- **Interpolation:** `"hello, #{name}"`  
- **Arrays:** `[1, 2, 3]`  
- **Named args:** `greet(name = "world", suffix = "!")`  
- **Lambdas:** `find(nums, n -> n > 3)`  
- **Control flow:** `if` / `else`, `for`, `while`, `try` / `catch` / `finally`

Example:

```bash
cat > hello.s <<'EOF'
fn greet(name, suffix = "!") {
  return "hello, #{name}#{suffix}"
}

let nums = [5, 1, 2, 2, 4, 3]
log greet("team")
log greet(name = "friend", suffix = " :) ")
log sort(nums)
log find(nums, n -> n > 3)
EOF

cargo run -- hello.s
```

---

## Shell and SSH

### `sh` — run local commands

Run a command string or a block. Returns a value with `stdout`, `stderr`, `exit_code`, and `duration_ms`.

**Expression form:**

```text
let r = sh "ls -la /tmp"
log r.stdout
if r.exit_code != 0 { log "Failed: " + r.stderr }
```

**Block form (multi-line):**

```text
let r = sh {
  echo "hello"
  whoami
  date
}
log r.stdout
```

### `ssh` — run remote commands

Same idea, with a host first: `ssh host command` or `ssh host { ... }`.

```text
let r = ssh "myserver" "uptime"
log r.stdout

parallel for host in ["server1", "server2"] {
  let r = ssh host "df -h /"
  log host + ": " + r.stdout
}
```

---

## Parallel loops

`parallel for` runs loop iterations in parallel (threads). Use it to run the same task across many items without waiting for each one in sequence.

```text
parallel for host in ["host1", "host2", "host3"] {
  let r = ssh host "uptime"
  log host + " -> " + r.stdout
}
```

---

## Built-in functions

| Function | Description |
|----------|-------------|
| `log(x)` | Print with timestamp (or no args for newline). |
| `echo(x)` | Print value, no timestamp. |
| `read_file(path)` | Read file contents as a string. |
| `wait(ms)` / `wait(duration)` | Sleep (e.g. `wait(100)` or `wait("2s")`). |
| `getType(x)` | Return type name as string. |
| `unwrap(x)` | Unwrap a nominal wrapper value. |
| `map(iterable, fn)` | Map over collection. |
| `filter(iterable, fn)` | Filter by predicate. |
| `find(iterable, fn)` | First element matching predicate (or null). |
| `findIndex(iterable, fn)` | Index of first match, or -1. |
| `reduce(iterable, fn, initial)` | Reduce to single value. |
| `flatMap(iterable, fn)` | Map and flatten. |
| `any(iterable, fn)` | True if any element matches. |
| `all(iterable, fn)` | True if all match. |
| `sort(iterable)` / `sort(iterable, comparator)` | Sort (optional comparator). |
| `sortBy(iterable, selector, descending?)` | Sort by key. |
| `groupBy(iterable, selector)` | Group into dict by key. |
| `partition(iterable, predicate)` | Split into [pass, fail]. |
| `distinct(iterable)` / `distinct(iterable, selector)` | Unique elements. |
| `collect(iterable)` | Collect iterator/generator to array. |
| `next(generator)` | Advance generator. |
| `validateType(value, typeName)` | Type check, returns bool. |

**String methods** (e.g. `s.method(args)`): `length`, `toUpper`, `toLower`, `trim`, `split(sep)`, `lines`, `contains(sub)`, `startsWith(sub)`, `endsWith(sub)`, `replace(from, to)`, `substring(start, len)`, `indexOf(sub)`, `join(array)`, `padLeft(n)`, `padRight(n)`, `remove(start, len)`.

**Number methods**: `abs`, `floor`, `ceil`, `round`, `min(x)`, `max(x)`, `clamp(lo, hi)`, `pow(exp)`, `sqrt`, `toInt`, `toFloat`, `isInt`, `toString`.

**`net` module** (e.g. `net.http(url)`): `ping(host)`, `http(url | options)`, `tcp_connect(host, port)`, `tcp_listen(host, port)`.

---

## REPL

```bash
cargo run -- repl
```

Evaluate expressions, call `sh`/`ssh`, inspect results. Use REPL commands (e.g. `vars`) if the REPL supports them—see the REPL help or source.

