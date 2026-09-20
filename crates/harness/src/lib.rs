use std::io::ErrorKind;
use std::net::{Ipv4Addr, SocketAddr, TcpStream};
use std::path::Path;
use std::process::{Child, Command, Stdio};
use std::time::{Duration, Instant};

use anyhow::{bail, Context, Result};
use tempfile::{tempdir, TempDir};

const READY_TIMEOUT: Duration = Duration::from_secs(10);

/// A nats-server binary process under test.
#[derive(Debug)]
pub struct Server {
    child: Child,
    dir: TempDir,
    pub url: String,
    pub host: String,
    pub port: u16,
}

impl Server {
    /// Spawn the binary from $NATS_SERVER_BIN on a random loopback port.
    pub fn start() -> Result<Server> {
        Server::start_with_args(&[])
    }

    /// Spawn with extra CLI args appended (e.g. ["--max_payload", "1024"]).
    pub fn start_with_args(extra_args: &[&str]) -> Result<Server> {
        let bin = std::env::var("NATS_SERVER_BIN")
            .context("NATS_SERVER_BIN is not set: point it at the nats-server binary under test")?;
        let dir = tempdir()?;
        let mut cmd = Command::new(&bin);
        cmd.args(["-a", "127.0.0.1", "-p", "-1", "--ports_file_dir"])
            .arg(dir.path())
            .args(extra_args)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null());
        let child = cmd
            .spawn()
            .with_context(|| format!("failed to spawn {bin}"))?;
        let mut srv = Server {
            child,
            dir,
            url: String::new(),
            host: "127.0.0.1".into(),
            port: 0,
        };
        srv.wait_until_ready()?;
        Ok(srv)
    }

    /// "host:port" string for clients.
    pub fn client_addr(&self) -> String {
        format!("{}:{}", self.host, self.port)
    }

    fn wait_until_ready(&mut self) -> Result<()> {
        let deadline = Instant::now() + READY_TIMEOUT;
        loop {
            if Instant::now() > deadline {
                bail!("server not ready within {}s", READY_TIMEOUT.as_secs());
            }
            if let Some(status) = self.child.try_wait()? {
                bail!("server exited early: {status}");
            }
            if let Some((host, port)) = read_ports_file(self.dir.path())? {
                if tcp_reachable(&host, port) {
                    self.host = host.clone();
                    self.port = port;
                    self.url = format!("nats://{host}:{port}");
                    return Ok(());
                }
            }
            std::thread::sleep(Duration::from_millis(20));
        }
    }
}

// The server writes <exe>_<pid>.ports: {"nats":["nats://127.0.0.1:PORT"],...}
// once its listeners are resolved. Presence of the file == server is up.
fn read_ports_file(dir: &Path) -> Result<Option<(String, u16)>> {
    let entries = match std::fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    for entry in entries {
        let path = entry?.path();
        if path.extension().and_then(|e| e.to_str()) != Some("ports") {
            continue;
        }
        let content = match std::fs::read_to_string(&path) {
            Ok(c) => c,
            Err(e) if e.kind() == ErrorKind::NotFound => continue,
            Err(e) => return Err(e.into()),
        };
        // The file may be observed mid-write; treat anything unparseable as "not yet".
        let Ok(value) = serde_json::from_str::<serde_json::Value>(&content) else {
            continue;
        };
        if let Some(url) = value
            .get("nats")
            .and_then(|v| v.get(0))
            .and_then(|v| v.as_str())
        {
            let authority = url.split("//").nth(1).unwrap_or(url);
            let (host, port) = authority
                .rsplit_once(':')
                .with_context(|| format!("bad ports url: {url}"))?;
            let port = match port.parse() {
                Ok(port) => port,
                Err(_) => continue,
            };
            return Ok(Some((host.to_string(), port)));
        }
    }
    Ok(None)
}

fn tcp_reachable(host: &str, port: u16) -> bool {
    let ip = match host.parse::<Ipv4Addr>() {
        Ok(ip) => ip,
        Err(_) => return false,
    };
    TcpStream::connect_timeout(&SocketAddr::from((ip, port)), Duration::from_millis(200)).is_ok()
}

impl Drop for Server {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}
