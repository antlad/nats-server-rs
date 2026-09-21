//! The INFO line.
//!
//! Built by hand, not with a serializer: the field order is part of what clients
//! see, the path runs once per connection, and `serde_json` would either sort or
//! randomise the keys. Shape and content are pinned in
//! `specs/protocol-contract.md` §2 and by `wire.rs::info_key_set_is_pinned`.

use std::net::SocketAddr;

/// What a client is told when it connects.
pub struct Info {
    pub server_id: String,
    pub server_name: String,
    pub version: &'static str,
    pub host: String,
    pub port: u16,
    pub max_payload: u64,
}

impl Info {
    /// `INFO <json> \r\n`.
    ///
    /// The space before the CR-LF is not a mistake: the reference builds the line
    /// with `bytes.Join(["INFO", json, CRLF], " ")` (`go:util.go:360`), and we
    /// reproduce it byte for byte.
    pub fn line(&self, cid: u64, peer: &SocketAddr) -> Vec<u8> {
        let mut out = Vec::with_capacity(192);
        out.extend_from_slice(b"INFO {");
        push_str(&mut out, "server_id", &self.server_id);
        out.push(b',');
        push_str(&mut out, "server_name", &self.server_name);
        out.push(b',');
        push_str(&mut out, "version", self.version);
        out.extend_from_slice(b",\"proto\":1");
        out.push(b',');
        push_str(&mut out, "host", &self.host);
        out.extend_from_slice(format!(",\"port\":{}", self.port).as_bytes());
        out.extend_from_slice(b",\"headers\":true");
        out.extend_from_slice(format!(",\"max_payload\":{}", self.max_payload).as_bytes());
        out.extend_from_slice(format!(",\"client_id\":{cid}").as_bytes());
        out.push(b',');
        push_str(&mut out, "client_ip", &peer.ip().to_string());
        out.extend_from_slice(b"} \r\n");
        out
    }
}

fn push_str(out: &mut Vec<u8>, key: &str, value: &str) {
    out.extend_from_slice(b"\"");
    out.extend_from_slice(key.as_bytes());
    out.extend_from_slice(b"\":\"");
    // Every value here is an identifier or an address; the escape path exists so
    // a configured server name cannot break the line.
    for c in value.chars() {
        match c {
            '"' => out.extend_from_slice(b"\\\""),
            '\\' => out.extend_from_slice(b"\\\\"),
            '\n' => out.extend_from_slice(b"\\n"),
            '\r' => out.extend_from_slice(b"\\r"),
            '\t' => out.extend_from_slice(b"\\t"),
            c if (c as u32) < 0x20 => out.extend_from_slice(format!("\\u{:04x}", c as u32).as_bytes()),
            c => {
                let mut b = [0u8; 4];
                out.extend_from_slice(c.encode_utf8(&mut b).as_bytes());
            }
        }
    }
    out.push(b'"');
}
