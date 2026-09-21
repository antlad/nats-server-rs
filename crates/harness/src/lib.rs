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

/// The single place where the binary under test is decided.
///
/// Kept separate from process spawning so both branches (set/unset) are testable
/// without touching the real environment of the test process.
pub fn resolve_bin(env_val: Option<String>) -> Result<String> {
    match env_val {
        Some(bin) if !bin.is_empty() => Ok(bin),
        _ => bail!(
            "NATS_SERVER_BIN is not set: point it at the nats-server binary under test"
        ),
    }
}

impl Server {
    /// Spawn the binary from $NATS_SERVER_BIN on a random loopback port.
    pub fn start() -> Result<Server> {
        Server::start_with_args(&[])
    }

    /// Spawn with extra CLI args appended.
    ///
    /// The reference binary has a deliberately small flag surface — everything
    /// that is not `-a`/`-p`/`--ports_file_dir`/`-c` lives in the config file, so
    /// tuning a server usually means [`Server::start_with_config`]. Passing a
    /// flag the binary does not know makes it print usage and exit immediately
    /// (measured against the Go reference: exit status 0, message on stderr),
    /// which the readiness loop reports as `server exited early`.
    ///
    /// ```no_run
    /// # use nats_test_harness::Server;
    /// // -D turns on debug logging in both the Go and the Rust server.
    /// let srv = Server::start_with_args(&["-D"]).unwrap();
    /// ```
    pub fn start_with_args(extra_args: &[&str]) -> Result<Server> {
        Server::launch(extra_args, tempdir()?)
    }

    /// Spawn with a config file (written into the server's temp dir, kept alive
    /// by the [`Server`] guard) passed via `-c`.
    ///
    /// This is the only way to move server options that the reference binary
    /// does not expose as flags, e.g.
    ///
    /// ```no_run
    /// # use nats_test_harness::Server;
    /// let srv = Server::start_with_config("max_payload: 1024\n").unwrap();
    /// ```
    pub fn start_with_config(config: &str) -> Result<Server> {
        let dir = tempdir()?;
        let path = dir.path().join("nats.conf");
        std::fs::write(&path, config)?;
        let cfg = path.display().to_string();
        Server::launch(&["-c", cfg.as_str()], dir)
    }

    fn launch(extra_args: &[&str], dir: TempDir) -> Result<Server> {
        let bin = resolve_bin(std::env::var("NATS_SERVER_BIN").ok())?;
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
        // Readiness can fail: the binary printed usage and exited, or bound
        // nothing. Kill it either way, or a doomed child outlives the test and
        // skews every run after it.
        if let Err(e) = srv.wait_until_ready() {
            let _ = srv.child.kill();
            let _ = srv.child.wait();
            return Err(e);
        }
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
