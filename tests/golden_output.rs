//! End-to-end output regression tests. Each case replays the adversarial
//! corpus in `tests/fixtures/golden/input.log` through `--stdin` and compares
//! stdout byte-for-byte with a checked-in expectation.
//!
//! To regenerate after an intentional output change, run with
//! `UPDATE_GOLDEN=1` and review the resulting fixture diff.
use std::{
    io::Write,
    path::PathBuf,
    process::{Command, Stdio},
};

const CASES: &[(&str, &[&str])] = &[
    ("plain-default.out", &[]),
    ("plain-multi.out", &["-f", "%t [%S %d] %l"]),
    ("plain-line.out", &["-f", "%l"]),
    (
        "plain-tokens.out",
        &[
            "-f",
            "%t|%d|%S|%sa|%sh|%sp|%sn|%ca|%ch|%cp|%C|%a|%%|%x|%s|%c",
        ],
    ),
    ("json.out", &["-o", "json"]),
    ("csv.out", &["-o", "csv"]),
    ("resp.out", &["-o", "resp"]),
    ("php.out", &["-o", "php"]),
];

fn fixture(name: &str) -> PathBuf {
    [
        env!("CARGO_MANIFEST_DIR"),
        "tests",
        "fixtures",
        "golden",
        name,
    ]
    .iter()
    .collect()
}

fn run(args: &[&str], input: &[u8]) -> Vec<u8> {
    let mut child = Command::new(env!("CARGO_BIN_EXE_redis-monitor"))
        .args(["--stdin", "--no-color"])
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(input).unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success(), "{args:?}: {output:?}");
    output.stdout
}

#[test]
fn output_matches_golden_fixtures() {
    let input = std::fs::read(fixture("input.log")).unwrap();
    let update = std::env::var_os("UPDATE_GOLDEN").is_some();
    let mut failures = Vec::new();

    for (name, args) in CASES {
        let actual = run(args, &input);
        let path = fixture(name);
        if update {
            std::fs::write(&path, &actual).unwrap();
            continue;
        }
        let expected = std::fs::read(&path).unwrap();
        if actual != expected {
            failures.push(format!(
                "{name} ({args:?}):\n--- expected\n{}\n--- actual\n{}",
                String::from_utf8_lossy(&expected),
                String::from_utf8_lossy(&actual)
            ));
        }
    }

    assert!(failures.is_empty(), "{}", failures.join("\n"));
}

/// Feeding the corpus one byte per write must not change the output: framing
/// is independent of how bytes arrive.
#[test]
fn output_is_independent_of_input_chunking() {
    let input = std::fs::read(fixture("input.log")).unwrap();
    let expected = std::fs::read(fixture("json.out")).unwrap();
    let mut child = Command::new(env!("CARGO_BIN_EXE_redis-monitor"))
        .args(["--stdin", "-o", "json"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .unwrap();
    let mut stdin = child.stdin.take().unwrap();
    for byte in &input {
        stdin.write_all(std::slice::from_ref(byte)).unwrap();
        stdin.flush().unwrap();
    }
    drop(stdin);
    let output = child.wait_with_output().unwrap();
    assert_eq!(
        String::from_utf8_lossy(&output.stdout),
        String::from_utf8_lossy(&expected)
    );
}
