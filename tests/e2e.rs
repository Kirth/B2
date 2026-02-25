//! End-to-end tests: run full scripts and assert on captured output.

use v2r::run_script_capture;

fn assert_output_contains(captured: &[String], needle: &str) {
    let found = captured.iter().any(|line| line.contains(needle));
    assert!(
        found,
        "Expected output to contain {:?}, got: {:?}",
        needle, captured
    );
}

#[test]
fn e2e_log_literal() {
    let out = run_script_capture("log 42").unwrap();
    assert_output_contains(&out, "42");
}

#[test]
fn e2e_log_string() {
    let out = run_script_capture(r#"log "hello""#).unwrap();
    assert_output_contains(&out, "hello");
}

#[test]
fn e2e_arithmetic() {
    let out = run_script_capture("log 1 + 2 * 3").unwrap();
    assert_output_contains(&out, "7"); // 1 + 6
}

#[test]
fn e2e_sort_builtin() {
    let out = run_script_capture("log sort([3, 1, 2])").unwrap();
    assert_output_contains(&out, "[1, 2, 3]");
}

#[test]
fn e2e_filter_find() {
    let out = run_script_capture("let nums = [5, 1, 2, 4, 3]; log find(nums, n -> n > 3)").unwrap();
    assert_output_contains(&out, "5");
}

#[test]
fn e2e_function_and_named_args() {
    let src = r#"
fn greet(name, suffix = "!") { return "hello, #{name}#{suffix}" }
log greet("world")
log greet(name = "team", suffix = " :)")
"#;
    let out = run_script_capture(src).unwrap();
    assert_output_contains(&out, "hello, world!");
    assert_output_contains(&out, "hello, team :)");
}

#[test]
fn e2e_interpolated_string() {
    let out = run_script_capture(r#"log "result: #{1 + 2}""#).unwrap();
    assert_output_contains(&out, "result: 3");
}

#[test]
fn e2e_parse_error() {
    let err = run_script_capture("let x = ").unwrap_err();
    assert!(err.contains("Expected") || err.to_lowercase().contains("parse") || err.len() > 0);
}

#[test]
fn e2e_runtime_error_undefined_var() {
    let err = run_script_capture("log nonexistent").unwrap_err();
    assert!(!err.is_empty());
}
