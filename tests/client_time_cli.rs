use std::{
    io::Write,
    process::{Command, Output, Stdio},
};

fn run(options: &[&str], input: &[u8]) -> Output {
    let mut child = Command::new(env!("CARGO_BIN_EXE_redis-monitor"))
        .args(["--stdin", "--no-color", "--format", "%l"])
        .args(options)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child.stdin.take().unwrap().write_all(input).unwrap();
    child.wait_with_output().unwrap()
}

fn selected(options: &[&str], lines: &[&str], expected: &[usize]) {
    let input: String = lines
        .iter()
        .enumerate()
        .map(|(i, prefix)| format!("{prefix} \"GET\" \"{i}\"\n"))
        .collect();
    let output = run(options, input.as_bytes());
    assert!(
        output.status.success(),
        "{options:?}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let want: String = expected
        .iter()
        .map(|i| format!("\"GET\" \"{i}\"\n"))
        .collect();
    assert_eq!(output.stdout, want.as_bytes(), "{options:?}");
    let summary = format!(
        "Processed {} lines (filtered: {})",
        lines.len(),
        lines.len() - expected.len()
    );
    assert!(String::from_utf8_lossy(&output.stderr).contains(&summary));
}

#[test]
fn exact_clients_and_ip_hosts_are_typed_selectors() {
    let lines = [
        "1.0 [0 127.0.0.1:1]",
        "1.0 [0 127.0.0.1:2]",
        "1.0 [0 127.0.0.2:1]",
        "1.0 [0 [2001:0db8::1]:42]",
        "1.0 [0 [2001:db8::1]:43]",
        "1.0 [0 lua]",
        "1.0 [0 unix:/tmp/redis.sock]",
        "1.0 [0 ]",
        "1.0 [0 -]",
    ];
    selected(&["--client", "127.0.0.1:1"], &lines, &[0]);
    selected(&["--client-host", "127.0.0.1"], &lines, &[0, 1]);
    selected(&["--client", "[2001:db8:0:0::1]:42"], &lines, &[3]);
    selected(&["--client-host", "2001:db8::1"], &lines, &[3, 4]);
    selected(
        &["--client", "lua", "--client", "unix:/tmp/redis.sock"],
        &lines,
        &[5, 6],
    );
    selected(&["--client", "-"], &lines, &[7, 8]);
    selected(
        &["--client-host", "127.0.0.1", "--client-host", "2001:db8::1"],
        &lines,
        &[0, 1, 3, 4],
    );
    selected(
        &["--client", "lua", "--client-host", "127.0.0.1"],
        &lines,
        &[],
    );
}

#[test]
fn ranges_are_exact_half_open_and_do_not_assume_sorted_input() {
    let lines = [
        "10.09999999999999999999 [0 lua]",
        "10.10000000000000000000 [0 lua]",
        "10.10000000000000000001 [0 lua]",
        "11.0 [0 lua]",
        "00010.10 [0 lua]",
        "12.0 [0 lua]",
        "0.0 [0 lua]",
        "18446744073709551615.0 [0 lua]",
    ];
    selected(&["--time-range", "10.1-11"], &lines, &[1, 2, 4]);
    selected(
        &["--time-range", "10.1-10.10000000000000000001"],
        &lines,
        &[1, 4],
    );
    selected(&["--time-range", "-10.1"], &lines, &[0, 6]);
    selected(&["--time-range", "11-"], &lines, &[3, 5, 7]);
    selected(
        &["--time-range", "-10.1", "--time-range", "11-"],
        &lines,
        &[0, 3, 5, 6, 7],
    );
    selected(&["--time-range", "18446744073709551615-"], &lines, &[7]);
}

#[test]
fn dated_bounds_require_offsets_and_preserve_fractional_precision() {
    let lines = ["10.1 [0 lua]", "10.1000000001 [0 lua]", "11.0 [0 lua]"];
    selected(
        &[
            "--time-range",
            "1970-01-01T01:00:10.1+01:00/1970-01-01T00:00:10.1000000001Z",
        ],
        &lines,
        &[0],
    );
    selected(&["--time-range", "/1970-01-01T00:00:11Z"], &lines, &[0, 1]);
    selected(&["--time-range", "1970-01-01T00:00:11Z/"], &lines, &[2]);
}

#[test]
fn cron_selects_whole_minutes_each_hour() {
    let lines = [
        "0.0 [0 lua]",
        "59.999999 [0 lua]",
        "299.999999 [0 lua]",
        "300.0 [0 lua]",
        "3599.999999 [0 lua]",
        "3600.0 [0 lua]",
        "3899.999999 [0 lua]",
        "3900.0 [0 lua]",
        "18446744073709551615.0 [0 lua]",
        "8210266876799.0 [0 lua]",
    ];
    selected(&["--time-cron", "0-4 * * * *"], &lines, &[0, 1, 2, 5, 6]);
    selected(
        &["--time-cron", "0-4 * * * *", "--time-range", "3600-"],
        &lines,
        &[5, 6],
    );
    selected(
        &["--time-cron", "0 * * * *", "--time-cron", "5 * * * *"],
        &lines,
        &[0, 1, 3, 5, 7],
    );
}

#[test]
fn cron_lists_steps_names_and_day_or_semantics() {
    // Epoch was Thursday, January 1; Monday January 5 is four days later.
    let lines = [
        "0.0 [0 lua]",
        "120.0 [0 lua]",
        "180.0 [0 lua]",
        "345600.0 [0 lua]",
        "432000.0 [0 lua]",
    ];
    selected(&["--time-cron", "*/2 0 1 JAN MON"], &lines, &[0, 1, 3]);
    selected(&["--time-cron", "0,3 0 * JAN THU"], &lines, &[0, 2]);
}

#[test]
fn timezone_uses_record_instant_including_dst_folds_and_gaps() {
    use chrono::DateTime;
    let dates = [
        "2026-11-01T08:30:30Z", // 01:30 PDT
        "2026-11-01T09:30:30Z", // 01:30 PST: both repeated minutes match
        "2026-11-01T10:30:30Z", // 02:30 PST
        "2026-03-08T09:59:59Z", // 01:59:59 PST
        "2026-03-08T10:00:00Z", // 03:00 PDT: no 02:00 local time
    ];
    let lines: Vec<String> = dates
        .iter()
        .map(|s| {
            format!(
                "{}.0 [0 lua]",
                DateTime::parse_from_rfc3339(s).unwrap().timestamp()
            )
        })
        .collect();
    let lines: Vec<&str> = lines.iter().map(String::as_str).collect();
    selected(
        &[
            "--time-cron",
            "30 1 * * *",
            "--time-zone",
            "America/Los_Angeles",
        ],
        &lines,
        &[0, 1],
    );
    selected(
        &[
            "--time-cron",
            "* 2 * * *",
            "--time-zone",
            "America/Los_Angeles",
        ],
        &lines,
        &[2],
    );
    selected(
        &[
            "--time-cron",
            "0 3 * * *",
            "--time-zone",
            "America/Los_Angeles",
        ],
        &lines,
        &[4],
    );
    selected(&["--time-cron", "30 1 * * *"], &lines, &[]);
}

#[test]
fn prefix_and_existing_filters_combine_for_structured_output() {
    let input = b"1.0 [2 127.0.0.1:1] \"SET\" \"k\" \"a\\x00b\"\n\
        1.0 [2 127.0.0.2:1] \"SET\" \"k\" \"a\\x00b\"\n\
        1.0 [0 127.0.0.1:1] \"SET\" \"k\" \"a\\x00b\"\n\
        1.0 [2 127.0.0.1:1] \"SET\" \"other\" \"a\\x00b\"\n\
        2.0 [2 127.0.0.1:1] \"SET\" \"k\" \"a\\x00b\"";
    let output = Command::new(env!("CARGO_BIN_EXE_redis-monitor"))
        .args([
            "--stdin",
            "--output",
            "json",
            "--client-host",
            "127.0.0.1",
            "--time-range",
            "1-2",
            "--time-cron",
            "0 * * * *",
            "--db",
            "2",
            "--filter",
            "[1]/^k$/",
        ])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    let mut child = output;
    child.stdin.take().unwrap().write_all(input).unwrap();
    let output = child.wait_with_output().unwrap();
    assert!(output.status.success(), "{:?}", output.stderr);
    let value: serde_json::Value =
        serde_json::from_slice(&output.stdout).unwrap();
    assert_eq!(value["args"], serde_json::json!(["k", "a\0b"]));
    assert!(
        String::from_utf8_lossy(&output.stderr)
            .contains("Processed 5 lines (filtered: 4)")
    );
}

#[test]
fn malformed_prefixes_cannot_match() {
    let lines = [
        "broken",
        "1.0 [0 999.0.0.1:1]",
        "1. [0 lua]",
        "1.0 [0 lua",
        "1.0 [0 --]",
        "1.0 [0 lua]",
    ];
    selected(&["--client", "lua"], &lines, &[5]);
    selected(&["--time-range", "0-"], &lines, &[5]);
    selected(&["--time-cron", "* * * * *"], &lines, &[5]);
}

#[test]
fn invalid_configuration_fails_before_connecting() {
    for (option, values) in [
        (
            "--client",
            vec!["127.0.0.1", "host:42", "[::1]:65536", "unix:"],
        ),
        ("--client-host", vec!["localhost", "127.0.0.1:42", "lua"]),
        (
            "--time-range",
            vec![
                "-",
                "1",
                "1-1",
                "2-1",
                "NaN-",
                "1e2-",
                "1.-",
                "1--2",
                "18446744073709551616-",
                "2026-09-30T00:00:00/",
                "1969-12-31T23:59:59Z/",
                "2016-12-31T23:59:60Z/",
            ],
        ),
        (
            "--time-cron",
            vec![
                "* * * *",
                "* * * * * *",
                "60 * * * *",
                "*/0 * * * *",
                "@hourly",
                "x * * * *",
            ],
        ),
        ("--time-zone", vec!["Mars/Olympus"]),
    ] {
        for value in values {
            let output = Command::new(env!("CARGO_BIN_EXE_redis-monitor"))
                .args([option, value])
                .output()
                .unwrap();
            assert!(!output.status.success(), "{option} {value}");
            assert!(
                String::from_utf8_lossy(&output.stderr).contains("error:"),
                "{:?}",
                output.stderr
            );
        }
    }
    let output = run(&["--time-zone", "UTC"], b"");
    assert!(!output.status.success());
}
