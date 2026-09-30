//! MONITOR record parsing microbenchmark. Run with
//! `RUSTFLAGS='' cargo bench --bench parse`.
use std::{borrow::Cow, hint::black_box, time::Instant};

use redis_monitor::monitor::Record;

const ROUNDS: usize = 200_000;

fn sample(lines: &[&[u8]], decode: bool) -> f64 {
    let mut args: Vec<Cow<'_, [u8]>> = Vec::new();
    let start = Instant::now();
    for _ in 0..ROUNDS {
        for line in lines {
            let record = Record::parse(black_box(line)).unwrap();
            if decode {
                record.decode_args(&mut args).unwrap();
                black_box(&args);
            }
            black_box(&record);
        }
    }
    start.elapsed().as_secs_f64() * 1e9 / (ROUNDS * lines.len()) as f64
}

fn main() {
    let large = format!(
        r#"1783484211.311904 [0 10.1.2.3:5555] "SET" "k" "{}""#,
        r#"{\"id\":\"99\",\"wp\":{\"hits\":1}} "#.repeat(64)
    );
    let cases: [(&str, Vec<&[u8]>); 3] = [
        (
            "short",
            vec![
                br#"1783484211.311904 [0 10.0.1.17:40017] "SET" "user:17" "value-17""#,
                br#"1783484211.311905 [3 127.0.0.1:52460] "GET" "session:123""#,
                br#"1783484211.311906 [0 lua] "HSET" "h" "f" "v""#,
                br#"1783484211.311907 [0 unix:/tmp/redis.sock] "PING""#,
            ],
        ),
        (
            "escaped",
            vec![
                br#"1783484211.311904 [0 10.0.1.17:40017] "SET" "k" "a \"quoted\" \x00\xff value""#,
            ],
        ),
        ("large", vec![large.as_bytes()]),
    ];

    for (name, lines) in &cases {
        for (mode, decode) in [("prefix", false), ("args", true)] {
            let mut samples: Vec<f64> =
                (0..7).map(|_| sample(lines, decode)).collect();
            samples.sort_by(f64::total_cmp);
            println!(
                "{name} {mode}: median {:.2} ns/record; samples {samples:.2?}",
                samples[3]
            );
        }
    }
}
