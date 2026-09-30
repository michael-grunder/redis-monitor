#[path = "../tests/common/mod.rs"]
mod common;

use redis_monitor::commands::Lookup;
use std::{hint::black_box, time::Instant};

fn sample(
    lookup: &Lookup,
    cases: &[common::Case],
    mode: &str,
    rounds: usize,
) -> f64 {
    let start = Instant::now();
    for _ in 0..rounds {
        for case in cases {
            let command = black_box(case.command.as_bytes());
            let args = black_box(&case.args);
            match mode {
                "lookup" => {
                    black_box(lookup.get_bytes(command).unwrap());
                }
                "borrow" => {
                    for key in lookup.keys(command, args).unwrap() {
                        black_box(key);
                    }
                }
                "collect" => {
                    black_box(
                        lookup.keys(command, args).unwrap().collect::<Vec<_>>(),
                    );
                }
                _ => unreachable!(),
            }
        }
    }
    start.elapsed().as_secs_f64() * 1e9 / (rounds * cases.len()) as f64
}

fn main() {
    let lookup = common::lookup();
    let mut cases = common::cases();
    // Long values should cost nothing: only key positions/keywords are inspected.
    let payload: &'static [u8] = &[b'x'; 64 * 1024];
    cases.push(common::Case {
        command: "set",
        args: vec![b"key", payload],
        keys: vec![b"key"],
    });
    cases.push(common::Case {
        command: "mset",
        args: [b"key".as_slice(), b"value"].repeat(128),
        keys: vec![b"key"; 128],
    });
    for case in &cases {
        assert_eq!(
            lookup
                .keys(case.command.as_bytes(), &case.args)
                .unwrap()
                .collect::<Vec<_>>(),
            case.keys
        );
    }
    // `cargo test --all-targets` also executes harness=false benches.
    if cfg!(debug_assertions) || std::env::args().any(|arg| arg == "--test") {
        return;
    }
    let rounds = std::env::var("KEY_BENCH_ROUNDS")
        .ok()
        .map_or(100_000, |v| v.parse().unwrap());
    let selected = std::env::var("KEY_BENCH_MODE").ok();
    for mode in ["lookup", "borrow", "collect"] {
        if selected.as_deref().is_some_and(|selected| selected != mode) {
            continue;
        }
        let mut samples: Vec<_> = (0..7)
            .map(|_| sample(&lookup, &cases, mode, rounds))
            .collect();
        samples.sort_by(f64::total_cmp);
        println!(
            "{mode}: median {:.2} ns/command; samples {samples:.2?}",
            samples[3]
        );
    }
    let start = Instant::now();
    std::thread::scope(|scope| {
        for _ in 0..4 {
            scope.spawn(|| {
                black_box(sample(&lookup, &cases, "borrow", rounds));
            });
        }
    });
    println!(
        "four readers: {:.2} million commands/s",
        (4 * rounds * cases.len()) as f64 / start.elapsed().as_secs_f64() / 1e6
    );
}
