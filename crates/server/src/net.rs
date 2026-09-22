//! Listening, accepting, and the ports file that tells the world what we bound.

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::TcpListener;

use crate::client;
use crate::Server;

/// Bind and start accepting. Returns the bound address once the listener is up —
/// before that, nothing may claim readiness.
pub async fn serve(server: Arc<Server>) -> std::io::Result<SocketAddr> {
    let cfg = &server.cfg;
    let host = normalize_host(&cfg.host);
    // The random-port sentinel is not a port: -1 asks the kernel for one, which
    // is port 0 in bind() terms. 0 in the *config* means "unset" and has already
    // resolved to 4222 (`config::bind_port`).
    let requested = match cfg.bind_port() {
        crate::config::RANDOM_PORT => 0,
        other => other,
    };
    let bind: SocketAddr = match format!("{host}:{requested}").parse() {
        Ok(addr) => addr,
        Err(e) => return Err(std::io::Error::new(std::io::ErrorKind::InvalidInput, e)),
    };
    let listener = TcpListener::bind(bind).await?;
    let addr = listener.local_addr()?;
    server.set_listen(addr);

    tokio::spawn(accept_loop(server.clone(), listener));
    Ok(addr)
}

async fn accept_loop(server: Arc<Server>, listener: TcpListener) {
    loop {
        match listener.accept().await {
            Ok((socket, _peer)) => {
                // Nagle off, before anything can be written. The reference never
                // calls SetNoDelay because it does not need to: Go's `net` sets
                // TCP_NODELAY on every TCP connection it hands out, so every byte
                // this server says — an `MSG` to a subscriber, an `-ERR` — leaves
                // immediately. Left at the kernel default we wait for an ACK
                // instead, and that is a latency difference the protocol tests
                // cannot see.
                if socket.set_nodelay(true).is_err() {
                    drop(socket);
                    continue;
                }
                let server = server.clone();
                if server.cfg.max_connections >= 0
                    && server.active.load(std::sync::atomic::Ordering::Relaxed)
                        >= server.cfg.max_connections as usize
                {
                    // Over the limit: the socket is closed without a word. (The
                    // reference's exact behaviour here is unmeasured — see
                    // specs/protocol-contract.md §9.)
                    drop(socket);
                    continue;
                }
                server
                    .active
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                let cid = server.next_cid();
                let srv = server.clone();
                tokio::spawn(async move {
                    let _guard = DropCount(srv.clone());
                    client::spawn(srv, socket, cid).await;
                });
            }
            Err(e) => {
                // Descriptor pressure and the like: back off rather than spin.
                if e.kind() != std::io::ErrorKind::Interrupted {
                    log(&server, &format!("accept failed: {e}"));
                }
                tokio::time::sleep(std::time::Duration::from_millis(20)).await;
            }
        }
    }
}

/// One fewer live connection when a connection task is gone.
struct DropCount(Arc<Server>);

impl Drop for DropCount {
    fn drop(&mut self) {
        self.0
            .active
            .fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
    }
}

/// `0.0.0.0` and `localhost` are bind addresses, but INFO must report something a
/// client can dial.
fn normalize_host(host: &str) -> String {
    match host {
        "0.0.0.0" | "::" | "*" => "0.0.0.0".into(),
        other => other.to_string(),
    }
}

/// Logging is off unless `-D`/`--debug` was given: the harness pipes stderr to
/// the void, but noise on the accept path still costs real throughput.
fn log(server: &Server, msg: &str) {
    if server.cfg.debug {
        eprintln!("[{:<26}] {}", server.server_id, msg);
    }
}
