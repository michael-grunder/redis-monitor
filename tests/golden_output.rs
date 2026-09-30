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
    ("json-source.out", &["-o", "json", "--source"]),
    ("csv.out", &["-o", "csv"]),
    ("resp.out", &["-o", "resp"]),
    ("php.out", &["-o", "php"]),
    ("plain-source.out", &["--source"]),
    ("csv-source.out", &["-o", "csv", "--source"]),
    ("php-source.out", &["-o", "php", "--source"]),
    ("resp-source.out", &["-o", "resp", "--source"]),
    (
        "json-fields.out",
        &["-o", "json", "--source", "-f", "%a %C %t"],
    ),
    (
        "php-fields.out",
        &["-o", "php", "--source", "-f", "%a %C %t"],
    ),
    (
        "csv-fields.out",
        &["-o", "csv", "--source", "-f", "%C %t %a"],
    ),
    (
        "resp-fields.out",
        &["-o", "resp", "--source", "-f", "%a %C %t"],
    ),
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

#[test]
fn field_selection_preserves_types_order_and_source() {
    let input = b"1.5 [2 lua] \"GET\" \"a,b\"\n";
    assert_eq!(run(&["-o", "json", "--source", "-f", "%C,%d %a"], input),
        b"{\"cmd\":\"GET\",\"db\":2,\"args\":[\"a,b\"],\"source\":{\"address\":null,\"name\":null}}\n");
    assert_eq!(run(&["-o", "php", "--source", "-f", "%C %d %a"], input),
        b"a:4:{s:3:\"cmd\";s:3:\"GET\";s:2:\"db\";i:2;s:4:\"args\";a:1:{i:0;s:3:\"a,b\";}s:6:\"source\";a:2:{s:7:\"address\";N;s:4:\"name\";N;}}\n");
    assert_eq!(
        run(&["-o", "csv", "--source", "-f", "%C %d %a"], input),
        b"source_address,source_name,cmd,db,args\n,,GET,2,\"a,b\"\n"
    );
    assert_eq!(
        run(&["--source", "-f", "%C|%d|%a"], input),
        b"[- -] GET|2|\"a,b\"\n"
    );

    let bytes = run(&["-o", "resp", "--source", "-f", "%C %d %a"], input);
    use redis::Value::{Array, BulkString, Nil};
    assert_eq!(
        redis::parse_redis_value(&bytes).unwrap(),
        Array(vec![
            Array(vec![Nil, Nil]),
            Array(vec![
                BulkString(b"GET".to_vec()),
                BulkString(b"2".to_vec()),
                Array(vec![BulkString(b"a,b".to_vec())])
            ]),
        ])
    );
    assert_eq!(
        run(&["-o", "resp", "--source"], input),
        b"*2\r\n*2\r\n$-1\r\n$-1\r\n*2\r\n$3\r\nGET\r\n$3\r\na,b\r\n"
    );
}

#[test]
fn selected_defaults_match_direct_serializers() {
    let input = std::fs::read(fixture("input.log")).unwrap();
    for kind in ["json", "php", "csv"] {
        for source in [false, true] {
            let mut defaults = vec!["-o", kind];
            if source {
                defaults.push("--source");
            }
            let mut selected = defaults.clone();
            selected.extend(["-f", "%t %d %ca %C %a"]);
            assert_eq!(
                run(&defaults, &input),
                run(&selected, &input),
                "{kind}, source={source}"
            );
        }
    }
    assert_eq!(run(&[], &input), run(&["-f", "%t [%d %ca] %l"], &input));
}

#[test]
fn empty_arguments_and_binary_values_keep_their_native_representation() {
    let input = b"1.0 [0 lua] \"PING\"\n";
    assert_eq!(run(&["-o", "json", "-f", "%a"], input), b"{\"args\":[]}\n");
    assert_eq!(
        run(&["-o", "php", "-f", "%a"], input),
        b"a:1:{s:4:\"args\";a:0:{}}\n"
    );
    assert_eq!(run(&["-o", "resp", "-f", "%a"], input), b"*1\r\n*0\r\n");
    let input = b"1.0 [0 lua] \"GET\" \"\\xff\\x00\"\n";
    assert_eq!(
        run(&["-o", "json", "-f", "%a"], input),
        "{\"args\":[\"�\\u0000\"]}\n".as_bytes()
    );
    assert_eq!(
        run(&["-o", "php", "-f", "%a"], input),
        b"a:1:{s:4:\"args\";a:1:{i:0;s:2:\"\xff\x00\";}}\n"
    );
    assert_eq!(
        run(&["-o", "resp", "-f", "%a"], input),
        b"*1\r\n*1\r\n$2\r\n\xff\x00\r\n"
    );
    assert_eq!(run(&["-o", "csv", "-f", "%a"], input), b"args\n\xff\x00\n");
}

#[test]
fn invalid_structured_formats_fail_before_connecting() {
    for kind in ["json", "php", "csv", "resp"] {
        for format in ["", " , ", "%x", "%C %C", "%t|%C", "%%", "%s", "%"] {
            // No --stdin: invalid configuration must fail before attempting Redis.
            let output = Command::new(env!("CARGO_BIN_EXE_redis-monitor"))
                .args(["-o", kind, "-f", format, "127.0.0.1:1"])
                .output()
                .unwrap();
            assert!(!output.status.success(), "{kind} {format}");
            let stderr = String::from_utf8_lossy(&output.stderr);
            assert!(
                stderr.contains("Structured --format")
                    || stderr.contains("Duplicate structured field"),
                "{stderr}"
            );
        }
    }
    let output = Command::new(env!("CARGO_BIN_EXE_redis-monitor"))
        .args(["--stdin", "-o", "json-source"])
        .output()
        .unwrap();
    assert!(!output.status.success());
}
