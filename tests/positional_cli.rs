use std::{
    io::Write,
    process::{Command, Output, Stdio},
};

fn run(filters: &[&str], format: &str, input: &[u8]) -> Output {
    let mut command = Command::new(env!("CARGO_BIN_EXE_redis-monitor"));
    command.args(["--stdin", "--no-color", "--output", format]);
    if format == "plain" {
        command.args(["--format", "%l"]);
    }
    for pattern in filters {
        command.args(["--filter", pattern]);
    }
    let mut child = command
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(input).unwrap();
    child.wait_with_output().unwrap()
}

#[test]
fn positional_stdin_filter_preserves_output_and_statistics() {
    let input = b"1.0 [0 127.0.0.1:1] \"SET\" \"foo\" \"bar\"\n1.0 [0 127.0.0.1:1] \"SET\" \"foo\" \"other\"\n1.0 [0 127.0.0.1:1] \"GET\" \"bar\"\n";
    let output = run(&["[2]/^bar$/"], "plain", input);
    assert!(output.status.success(), "{:?}", output.stderr);
    assert_eq!(output.stdout, b"\"SET\" \"foo\" \"bar\"\n");
    let stderr = String::from_utf8(output.stderr).unwrap();
    assert!(
        stderr.contains("Processed 3 lines (filtered: 2)"),
        "{stderr}"
    );
}

#[test]
fn mixed_positions_and_forced_literals_work_with_json() {
    let input = b"1.0 [0 127.0.0.1:1] \"SET\" \"[0]foo\" \"/value/\"\n1.0 [0 127.0.0.1:1] \"GET\" \"[0]foo\"\n1.0 [0 127.0.0.1:1] \"GET\" \"other\"\n";
    let output = run(&["[1]=[0]foo", "![2]=/value/"], "json", input);
    assert!(output.status.success(), "{:?}", output.stderr);
    let value: serde_json::Value =
        serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["cmd"], "GET");
    assert_eq!(value["args"], serde_json::json!(["[0]foo"]));
}

#[test]
fn invalid_selectors_fail_in_both_cli_options() {
    for option in ["--filter", "--key-filter"] {
        for pattern in [
            "[",
            "[-1]x",
            "[a]x",
            "[1:2]x",
            "[999999999999999999999999]x",
        ] {
            let output = Command::new(env!("CARGO_BIN_EXE_redis-monitor"))
                .args([option, pattern])
                .output()
                .unwrap();
            assert!(!output.status.success(), "{option} {pattern}");
            assert!(String::from_utf8_lossy(&output.stderr).contains("index"));
        }
    }
}

/// Arguments decoded for filtering are reused when formatting; each record
/// must still print its own, including identical and escaped ones.
#[test]
fn filtered_structured_records_keep_their_own_arguments() {
    let input = b"1.0 [0 lua] \"SET\" \"k1\" \"a\\\"b\"\n\
                  1.0 [0 lua] \"SET\" \"skip\" \"x\"\n\
                  1.0 [0 lua] \"SET\" \"k1\" \"a\\\"b\"\n\
                  1.0 [0 lua] \"SET\" \"k2\" \"\\x00\"\n";
    for format in ["json", "resp", "csv", "php"] {
        let unfiltered = run(&[], format, input);
        let filtered = run(&["![1]skip"], format, input);
        assert!(filtered.status.success(), "{format}: {:?}", filtered.stderr);
        let expected: Vec<u8> = unfiltered
            .stdout
            .split_inclusive(|&b| b == b'\n')
            .filter(|line| !line.windows(4).any(|w| w == b"skip"))
            .flatten()
            .copied()
            .collect();
        let expected = if format == "resp" {
            // RESP arrays span lines; remove the skipped array exactly.
            let skipped = b"*3\r\n$3\r\nSET\r\n$4\r\nskip\r\n$1\r\nx\r\n";
            let at = unfiltered
                .stdout
                .windows(skipped.len())
                .position(|w| w == skipped)
                .unwrap();
            [
                &unfiltered.stdout[..at],
                &unfiltered.stdout[at + skipped.len()..],
            ]
            .concat()
        } else {
            expected
        };
        assert_eq!(filtered.stdout, expected, "{format}");
    }
}
