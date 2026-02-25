# B2 

A toy scripting language runtime in Rust.  Spiritual successor to Shelltrac, which was intended to easily shell or ssh out and script with those results, but faster. 


## Running Scripts: 

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

Explore the code to discover: `sh`/`ssh` blocks, `parallel for` loops.

## REPL

```bash
cargo run -- repl
```
