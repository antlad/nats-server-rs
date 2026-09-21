use nats_test_harness::Server;

#[test]
fn starts_stops_and_exposes_url() {
    let srv = Server::start().expect("NATS_SERVER_BIN must be set and binary runnable");
    // server is listening: a plain TCP connect succeeds
    let addr = format!("{}:{}", srv.host, srv.port);
    let _conn = std::net::TcpStream::connect(&addr).expect("tcp connect");
    assert!(srv.url.starts_with("nats://127.0.0.1:"));
    drop(srv);
    // after drop the port is closed
    assert!(std::net::TcpStream::connect(&addr).is_err());
}

/// Re-executed in a child process with $NATS_SERVER_BIN removed, so the check is
/// real whether or not the parent run had the variable set.
#[test]
fn missing_env_var_fails_clearly() {
    if std::env::var("NATS_SERVER_BIN").is_err() {
        return harness_errors_without_env_var();
    }
    let exe = std::env::current_exe().expect("current_exe");
    let out = std::process::Command::new(exe)
        .args(["harness_errors_without_env_var", "--exact", "--nocapture"])
        .env_remove("NATS_SERVER_BIN")
        .output()
        .expect("re-exec of the test binary");
    assert!(
        out.status.success(),
        "child run failed:\n{}\n{}",
        String::from_utf8_lossy(&out.stdout),
        String::from_utf8_lossy(&out.stderr)
    );
}

#[test]
fn harness_errors_without_env_var() {
    // Guarded so the same body works in the parent (var set) and the child (unset).
    if std::env::var("NATS_SERVER_BIN").is_ok() {
        return;
    }
    let err = Server::start().unwrap_err().to_string();
    assert!(err.contains("NATS_SERVER_BIN"), "got: {err}");
    // The resolution helper is the single source of that error: both branches.
    assert!(nats_test_harness::resolve_bin(None).is_err());
    assert!(nats_test_harness::resolve_bin(Some(String::new())).is_err());
    assert_eq!(
        nats_test_harness::resolve_bin(Some("/bin/x".into())).unwrap(),
        "/bin/x"
    );
}

#[test]
fn unsupported_flag_never_becomes_a_silent_server() {
    // A binary that ignores an unknown flag would look healthy and every later
    // test would run against the wrong configuration. The reference prints usage
    // and exits (measured: status 0, "flag provided but not defined" on stderr),
    // so what the harness must guarantee is *no readiness*, not an exit code.
    let err = Server::start_with_args(&["--this_flag_does_not_exist"]).unwrap_err();
    assert!(
        err.to_string().contains("exited early") || err.to_string().contains("not ready"),
        "got: {err}"
    );
}

#[test]
fn config_file_reaches_the_server() {
    // -c is the only route to options the reference does not expose as flags.
    let srv = Server::start_with_config("max_payload: 1024\n").expect("config server");
    let mut s = std::net::TcpStream::connect(srv.client_addr()).unwrap();
    use std::io::{Read, Write};
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut buf = Vec::new();
    let mut byte = [0u8; 1];
    while buf.windows(2).all(|w| w != b"\r\n") && std::time::Instant::now() < deadline {
        match s.read(&mut byte) {
            Ok(1) => buf.push(byte[0]),
            Ok(_) => break,
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => continue,
            Err(e) => panic!("read: {e}"),
        }
    }
    let info = String::from_utf8_lossy(&buf).into_owned();
    assert!(info.starts_with("INFO "), "got {info:?}");
    assert!(
        info.contains("\"max_payload\":1024"),
        "config max_payload must show up in INFO, got {info}"
    );
    s.write_all(b"PING\r\n").unwrap();
    drop(srv);
}
