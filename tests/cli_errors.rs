use std::{
    fs,
    io::{Read, Write},
    net::{Shutdown, TcpListener},
    os::{fd::OwnedFd, unix::net::UnixStream},
    path::{Path, PathBuf},
    process::{Command, Output, Stdio},
    sync::atomic::{AtomicU64, Ordering},
    thread,
};

struct TestDir(PathBuf);

impl TestDir {
    fn new() -> Self {
        static NEXT_ID: AtomicU64 = AtomicU64::new(0);

        let path = std::env::temp_dir().join(format!(
            "redis-monitor-cli-errors-{}-{}",
            std::process::id(),
            NEXT_ID.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }

    fn path(&self) -> &Path {
        &self.0
    }
}

impl Drop for TestDir {
    fn drop(&mut self) {
        fs::remove_dir_all(&self.0).unwrap();
    }
}

fn assert_clean_failure(output: &Output) -> String {
    assert!(!output.status.success(), "expected failure: {output:?}");
    assert!(
        output.status.code().is_some(),
        "process was terminated by a signal: {output:?}"
    );

    let stderr = std::str::from_utf8(&output.stderr).unwrap();
    assert!(!stderr.contains("panicked at"), "{stderr}");
    assert!(!stderr.contains("Processed 0 lines"), "{stderr}");
    stderr.to_string()
}

fn mock_redis(cluster_reply: &[u8]) -> (u16, thread::JoinHandle<()>) {
    let cluster_reply = cluster_reply.to_vec();
    let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
    let port = listener.local_addr().unwrap().port();

    let server = thread::spawn(move || {
        let (mut stream, _) = listener.accept().unwrap();
        let mut request = Vec::new();
        let mut chunk = [0; 1024];
        let mut client_commands = 0;

        loop {
            let read = stream.read(&mut chunk).unwrap();
            assert_ne!(read, 0, "client disconnected before CLUSTER SLOTS");
            request.extend_from_slice(&chunk[..read]);

            if request
                .windows(b"CLUSTER".len())
                .any(|window| window == b"CLUSTER")
            {
                stream.write_all(&cluster_reply).unwrap();
                stream.shutdown(Shutdown::Write).unwrap();
                break;
            }

            let observed = request
                .windows(b"CLIENT".len())
                .filter(|window| *window == b"CLIENT")
                .count();
            for _ in client_commands..observed {
                stream.write_all(b"+OK\r\n").unwrap();
            }
            client_commands = observed;
        }
    });

    (port, server)
}

#[test]
fn standalone_cluster_seed_exits_cleanly_with_helpful_error() {
    let (port, server) =
        mock_redis(b"-ERR This instance has cluster support disabled\r\n");

    let output = Command::new(env!("CARGO_BIN_EXE_redis-monitor"))
        .args(["--cluster", &port.to_string()])
        .output()
        .unwrap();
    server.join().unwrap();

    let stderr = assert_clean_failure(&output);
    assert!(stderr.contains(&format!(
        "Failed to discover a Redis Cluster from seed '{port}' (resolved to \
         127.0.0.1:{port})"
    )));
    assert!(
        stderr.contains("remove --cluster to monitor a standalone instance")
    );
    assert!(
        stderr.contains("Failed to execute CLUSTER SLOTS"),
        "{stderr}"
    );
    assert!(stderr.contains("cluster support disabled"), "{stderr}");
}

#[test]
fn malformed_cluster_node_exits_cleanly_with_protocol_context() {
    let reply = b"*1\r\n*3\r\n:0\r\n:16383\r\n*3\r\n$9\r\n127.0.0.1\r\n\
                  :70000\r\n$6\r\nnode-1\r\n";
    let (port, server) = mock_redis(reply);

    let output = Command::new(env!("CARGO_BIN_EXE_redis-monitor"))
        .args(["--cluster", &port.to_string()])
        .output()
        .unwrap();
    server.join().unwrap();

    let stderr = assert_clean_failure(&output);
    assert!(stderr.contains("Invalid CLUSTER SLOTS entry 0"), "{stderr}");
    assert!(stderr.contains("out-of-range node port: 70000"), "{stderr}");
}

#[test]
fn invalid_instance_address_exits_cleanly() {
    let output = Command::new(env!("CARGO_BIN_EXE_redis-monitor"))
        .arg("localhost:not-a-port")
        .output()
        .unwrap();

    let stderr = assert_clean_failure(&output);
    assert!(
        stderr.contains(
            "Unable to parse 'localhost:not-a-port' as a Redis address"
        ),
        "{stderr}"
    );
    assert!(
        stderr.contains("no configuration entry with that name exists"),
        "{stderr}"
    );
}

#[test]
fn absent_home_environment_does_not_panic_during_config_discovery() {
    let dir = TestDir::new();
    let output = Command::new(env!("CARGO_BIN_EXE_redis-monitor"))
        .current_dir(dir.path())
        .env_remove("HOME")
        .arg("localhost:not-a-port")
        .output()
        .unwrap();

    let stderr = assert_clean_failure(&output);
    assert!(
        stderr.contains(
            "Unable to parse 'localhost:not-a-port' as a Redis address"
        ),
        "{stderr}"
    );
}

#[test]
fn incomplete_named_instance_exits_cleanly() {
    let dir = TestDir::new();
    let config = dir.path().join("config.toml");
    fs::write(&config, "[broken]\ncolor = \"red\"\n").unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_redis-monitor"))
        .args(["--config-file", config.to_str().unwrap(), "broken"])
        .output()
        .unwrap();

    let stderr = assert_clean_failure(&output);
    assert!(
        stderr.contains("Invalid configuration for instance 'broken'"),
        "{stderr}"
    );
    assert!(stderr.contains("missing Redis address"), "{stderr}");
}

#[test]
fn named_cluster_discovery_failure_exits_cleanly() {
    let (port, server) =
        mock_redis(b"-ERR This instance has cluster support disabled\r\n");
    let dir = TestDir::new();
    let config = dir.path().join("config.toml");
    fs::write(
        &config,
        format!("[named]\naddresses = [\"{port}\"]\ncluster = true\n"),
    )
    .unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_redis-monitor"))
        .args(["--config-file", config.to_str().unwrap(), "named"])
        .output()
        .unwrap();
    server.join().unwrap();

    let stderr = assert_clean_failure(&output);
    assert!(
        stderr.contains(
            "Failed to discover the cluster for configured instance 'named'"
        ),
        "{stderr}"
    );
    assert!(stderr.contains("last attempted 127.0.0.1:"), "{stderr}");
    assert!(stderr.contains("cluster support disabled"), "{stderr}");
}

#[test]
fn malformed_discovered_config_exits_cleanly() {
    let dir = TestDir::new();
    let config = dir.path().join(".redis-monitor.toml");
    fs::write(&config, "[broken\ninvalid = true\n").unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_redis-monitor"))
        .current_dir(dir.path())
        .env("HOME", dir.path())
        .output()
        .unwrap();

    let stderr = assert_clean_failure(&output);
    assert!(stderr.contains("Failed to read config file"), "{stderr}");
    assert!(stderr.contains(".redis-monitor.toml"), "{stderr}");
}

#[test]
fn empty_client_certificate_exits_cleanly() {
    let dir = TestDir::new();
    let cert = dir.path().join("empty.pem");
    fs::write(&cert, []).unwrap();

    let output = Command::new(env!("CARGO_BIN_EXE_redis-monitor"))
        .args(["--tls", "--tls-cert", cert.to_str().unwrap(), "0"])
        .output()
        .unwrap();

    let stderr = assert_clean_failure(&output);
    assert!(
        stderr.contains("No certificate found in cert file"),
        "{stderr}"
    );
    assert!(stderr.contains("empty.pem"), "{stderr}");
}

#[test]
fn output_failure_sets_a_nonzero_exit_status() {
    let (output_stream, closed_peer) = UnixStream::pair().unwrap();
    drop(closed_peer);
    let output_fd: OwnedFd = output_stream.into();

    let mut child = Command::new(env!("CARGO_BIN_EXE_redis-monitor"))
        .arg("--stdin")
        .stdin(Stdio::piped())
        .stdout(Stdio::from(output_fd))
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(b"1.000000 [0 127.0.0.1:1] \"PING\"\n")
        .unwrap();
    let output = child.wait_with_output().unwrap();

    let stderr = assert_clean_failure(&output);
    assert!(stderr.contains("Output thread failed"), "{stderr}");
}
