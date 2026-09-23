use std::process::Command;

/// Run a git command, returning its trimmed stdout, or `None` when git is
/// unavailable or this is not a git checkout (e.g. a packaged source tree).
fn git(args: &[&str]) -> Option<String> {
    let output = Command::new("git").args(args).output().ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn main() {
    let git_hash = git(&["rev-parse", "--short", "HEAD"])
        .unwrap_or_else(|| "unknown".into());
    println!("cargo:rustc-env=GIT_HASH={git_hash}");

    let git_dirty = match git(&["diff", "--shortstat"]) {
        Some(stat) if !stat.is_empty() => "yes",
        _ => "no",
    };
    println!("cargo:rustc-env=GIT_DIRTY={git_dirty}");
}
