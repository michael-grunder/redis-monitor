use std::process::Command;

fn main() {
    let output = Command::new("git")
        .args(&["rev-parse", "--short", "HEAD"])
        .output()
        .expect("Failed to execute git rev-parse");
    let git_hash = String::from_utf8(output.stdout)
        .expect("Invalid UTF-8")
        .trim()
        .to_string();

    println!("cargo:rustc-env=GIT_HASH={}", git_hash);

    let status_output = Command::new("git")
        .args(&["diff", "--shortstat"])
        .output()
        .expect("Failed to execute git diff");

    if !status_output.stdout.is_empty() {
        println!("cargo:rustc-env=GIT_DIRTY=yes");
    };
}
