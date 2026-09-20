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

#[test]
fn missing_env_var_fails_clearly() {
    // Only meaningful if NATS_SERVER_BIN happens to be unset; skip otherwise.
    if std::env::var("NATS_SERVER_BIN").is_ok() {
        return;
    }
    let err = Server::start().unwrap_err().to_string();
    assert!(err.contains("NATS_SERVER_BIN"), "got: {err}");
}
