//! Unit tests for the two things the suite cannot see from outside: the shape of
//! the INFO line, and the CONNECT defaults table. Neither spawns a process.

use std::net::SocketAddr;

use nats_server_rs::client::Options;
use nats_server_rs::config::Config;
use nats_server_rs::info::Info;

/// The INFO line minus `INFO `, the reference's join-space, and the CR-LF.
fn trim_info(line: &[u8]) -> &[u8] {
    assert!(line.starts_with(b"INFO ") && line.ends_with(b"\r\n"), "{line:?}");
    let end = match line[line.len() - 3] {
        b' ' => line.len() - 3,
        _ => line.len() - 2,
    };
    &line[5..end]
}

fn peer() -> SocketAddr {
    "127.0.0.1:55555".parse().unwrap()
}

fn info() -> Info {
    Info {
        server_id: "N".to_string() + &"A".repeat(55),
        server_name: "N".to_string() + &"A".repeat(55),
        version: "0.1.0",
        host: "127.0.0.1".into(),
        port: 4222,
        max_payload: Config::default().max_payload,
    }
}

#[test]
fn info_line_is_one_line_of_json_with_the_reference_shape() {
    let line = info().line(7, &peer());
    assert!(line.starts_with(b"INFO "), "got {line:?}");
    assert!(line.ends_with(b"} \r\n"), "the join-space before CRLF: {line:?}");
    let body = std::str::from_utf8(trim_info(&line)).unwrap();
    let value: serde_json::Value = serde_json::from_str(body).expect("valid JSON on one line");
    assert!(!body.contains('\n'));
    assert_eq!(value["proto"].as_u64(), Some(1));
    assert_eq!(value["port"].as_u64(), Some(4222));
    assert_eq!(value["headers"].as_bool(), Some(true));
    assert_eq!(value["max_payload"].as_u64(), Some(1_048_576));
    assert_eq!(value["client_id"].as_u64(), Some(7));
    assert_eq!(value["client_ip"].as_str(), Some("127.0.0.1"));
    assert_eq!(value["server_id"], value["server_name"]);
}

#[test]
fn info_says_nothing_about_features_we_do_not_have() {
    let line = info().line(1, &peer());
    let body = std::str::from_utf8(trim_info(&line)).unwrap();
    let value: serde_json::Value = serde_json::from_str(body).unwrap();
    let obj = value.as_object().unwrap();
    for absent in [
        "jetstream",
        "connect_urls",
        "cluster",
        "domain",
        "auth_required",
        "tls_required",
        "tls_available",
        "nonce",
        "ldm",
        "compression",
        "xkey",
        "api_lvl",
        "git_commit",
        "go",
    ] {
        assert!(!obj.contains_key(absent), "INFO must not mention {absent}");
    }
}

#[test]
fn a_configured_server_name_does_not_become_the_id() {
    let mut i = info();
    i.server_name = "node-1".into();
    let line = i.line(1, &peer());
    let value: serde_json::Value =
        serde_json::from_slice(trim_info(&line)).expect("valid json");
    assert_eq!(value["server_name"].as_str(), Some("node-1"));
    assert_ne!(value["server_id"], value["server_name"]);
}

#[test]
fn a_quoted_server_name_cannot_break_the_line() {
    let mut i = info();
    i.server_name = "say \"hi\"\r\nnow".into();
    let line = i.line(1, &peer());
    let value: serde_json::Value =
        serde_json::from_slice(trim_info(&line)).expect("still one valid JSON object");
    assert_eq!(value["server_name"].as_str(), Some("say \"hi\"\r\nnow"));
}

/// The defaults table. Absent keys are **true**: the reference seeds
/// `defaultOpts = {Verbose:true, Pedantic:true, Echo:true}` and `json.Unmarshal`
/// leaves absent fields alone. Getting this backwards is invisible to most tests
/// and fatal to self-delivery and every `+OK`.
#[test]
fn connect_options_default_to_true() {
    let d = Options::default();
    assert!(d.verbose, "verbose");
    assert!(d.pedantic, "pedantic");
    assert!(d.echo, "echo");
    assert!(!d.headers, "headers is a capability, not a default");
    assert!(!d.no_responders, "no_responders likewise");
}

#[test]
fn absent_keys_keep_the_value_they_found() {
    let mut o = Options::default();
    assert!(o.apply_connect(br#"{"verbose":false}"#.as_slice()));
    assert!(!o.verbose);
    assert!(o.pedantic && o.echo, "untouched by a partial CONNECT");

    // A second CONNECT changes only what it names.
    assert!(o.apply_connect(br#"{"echo":false}"#.as_slice()));
    assert!(!o.echo);
    assert!(!o.verbose, "still off: it was never turned back on");

    assert!(o.apply_connect(b"{}"));
    assert!(!o.verbose && !o.echo, "an empty object changes nothing");
}

#[test]
fn unknown_keys_are_tolerated() {
    let mut o = Options::default();
    let line = br#"{"verbose":false,"lang":"rust","version":"2.0.0","protocol":1,
                    "name":"bench","tls_required":false,"pedantic":false,
                    "headers":true,"no_responders":true,"future_thing":{"a":1}}"#;
    assert!(o.apply_connect(line), "unknown keys must not fail a CONNECT");
    assert!(!o.verbose && !o.pedantic);
    assert!(o.headers && o.no_responders);
    assert!(o.echo, "not mentioned: still true");
}

#[test]
fn malformed_options_are_an_error_not_a_default() {
    // A CONNECT whose options cannot be read into the struct fails the
    // connection; it must not silently leave the defaults in place.
    for bad in [b"not json".as_slice(), b"[]".as_slice(), b"null".as_slice()] {
        let mut fresh = Options::default();
        assert!(!fresh.apply_connect(bad), "{bad:?} must be rejected");
    }
    let mut fresh = Options::default();
    assert!(fresh.apply_connect(b"{}"), "an empty object is legal");
}

#[test]
fn configured_limits_reach_the_info_line() {
    let cfg = Config {
        max_payload: 1024,
        ..Config::default()
    };
    let server = nats_server_rs::Server::new(cfg);
    // server_id is an opaque token of the reference's shape, so it must be
    // parseable as JSON regardless of what it contains.
    server.set_listen("127.0.0.1:4222".parse().unwrap());
    let line = server.info_line(3, &peer());
    let trimmed = trim_info(&line);
    let value: serde_json::Value = serde_json::from_slice(trimmed).unwrap();
    assert_eq!(value["max_payload"].as_u64(), Some(1024));
    assert_eq!(value["port"].as_u64(), Some(4222));
    assert_eq!(value["server_id"].as_str().map(str::len), Some(56));
}
