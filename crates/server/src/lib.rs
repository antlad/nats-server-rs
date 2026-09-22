//! A NATS Core server: the client protocol, routing and lifecycle, with nothing
//! else. No accounts, no authorization, no TLS, no JetStream, no clustering, no
//! monitoring endpoint — and INFO says nothing about any of them
//! (`specs/protocol-contract.md` §2).
//!
//! The binary in `main.rs` is a thin shell around [`Server`]: flags, config, the
//! ports file, signals. Everything the protocol tests assert lives here.

pub mod allocstats;

pub mod arena;
pub mod client;
pub mod config;
pub mod info;
pub mod net;
pub mod nuid;
pub mod proto;
pub mod routing;
pub mod subjects;

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::Instant;

pub use config::Config;
pub use nuid::Rng;
pub use routing::Registry;

/// The running server: options, the registry, and the counters that make INFO and
/// the connection limit work.
pub struct Server {
    pub cfg: Config,
    pub registry: Arc<Registry>,
    pub server_id: String,
    pub server_name: String,
    pub version: &'static str,
    /// Set once the listener is bound: INFO must carry the *resolved* port.
    listen: OnceLock<SocketAddr>,
    /// Monotonic per-connection id, like the reference's `gcid`.
    cids: AtomicU64,
    pub active: AtomicUsize,
    pub started: Instant,
}

impl Server {
    pub fn new(cfg: Config) -> Arc<Server> {
        let server_id = nuid::server_id(&mut Rng::new());
        let server_name = cfg.server_name.clone().unwrap_or_else(|| server_id.clone());
        Arc::new(Server {
            cfg,
            registry: Registry::new(),
            server_id,
            server_name,
            version: env!("CARGO_PKG_VERSION"),
            listen: OnceLock::new(),
            cids: AtomicU64::new(0),
            active: AtomicUsize::new(0),
            started: Instant::now(),
        })
    }

    pub fn set_listen(&self, addr: SocketAddr) {
        let _ = self.listen.set(addr);
    }

    pub fn listen_addr(&self) -> SocketAddr {
        *self.listen.get().expect("listening")
    }

    pub fn next_cid(&self) -> u64 {
        self.cids.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// The first line of every connection.
    pub fn info_line(&self, cid: u64, peer: &SocketAddr) -> Vec<u8> {
        let addr = self.listen_addr();
        info::Info {
            server_id: self.server_id.clone(),
            server_name: self.server_name.clone(),
            version: self.version,
            host: addr.ip().to_string(),
            port: addr.port(),
            max_payload: self.cfg.max_payload,
        }
        .line(cid, peer)
    }

    /// A class-B error: text, and the connection keeps working.
    pub fn send_err(&self, conn: &client::Conn, text: &str) {
        conn.enqueue(client::Frame::err(text));
    }

    /// The no-responder rule (contract §6): nothing was delivered, the publisher
    /// asked for it, and the publisher holds a subscription on the reply subject.
    pub fn no_responder_check(
        &self,
        conn: &Arc<client::Conn>,
        msg: &routing::Msg<'_>,
        reply: Option<&[u8]>,
        delivered: usize,
    ) {
        if delivered != 0 || !conn.opts().no_responders {
            return;
        }
        let Some(reply) = reply.filter(|r| !r.is_empty()) else {
            return;
        };
        for sub in self.registry.matching_of(conn, &reply) {
            conn.enqueue(routing::no_responder_frame(reply, &sub, msg.subject));
        }
    }
}
