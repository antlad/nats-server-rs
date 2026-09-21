//! Process shell: flags, config, startup ordering, the ports file, signals.
//!
//! Startup order matters and is fixed by the harness contract
//! (`specs/protocol-contract.md` §1): bind → learn the resolved port → start
//! accepting → write the ports file *last*, so a readable file always implies a
//! connectable server. Shutdown removes the file; under SIGKILL it is left behind
//! in a scratch directory, which is exactly why the harness never trusts it
//! without a TCP connect.

use std::io::Write;
use std::path::PathBuf;
use std::process::ExitCode;

use nats_server_rs::{config::Config, net, Server};

/// Only with `--features allocstats`: the counting allocator, so the process can
/// report what one delivered message costs. See `crates/server/src/allocstats.rs`.
#[cfg(feature = "allocstats")]
#[global_allocator]
static ALLOC: nats_server_rs::allocstats::Counting = nats_server_rs::allocstats::Counting;

const PROGRAM: &str = "nats-server-rs";

#[tokio::main]
async fn main() -> ExitCode {
    match run().await {
        Ok(code) => code,
        Err(e) => {
            eprintln!("{PROGRAM}: {e:#}");
            ExitCode::from(1)
        }
    }
}

async fn run() -> anyhow::Result<ExitCode> {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let args = match nats_server_rs::config::parse_args(&mut argv.into_iter(), PROGRAM) {
        Ok(a) => a,
        Err(e) => {
            // The reference prints the reason on stderr and usage on stdout.
            eprintln!("{e:#}");
            print!("{}", nats_server_rs::config::usage(PROGRAM));
            let _ = std::io::stdout().flush();
            return Ok(ExitCode::from(1));
        }
    };

    if args.version {
        println!("{PROGRAM}: v{}", env!("CARGO_PKG_VERSION"));
        return Ok(ExitCode::SUCCESS);
    }
    if args.help {
        print!("{}", nats_server_rs::config::usage(PROGRAM));
        return Ok(ExitCode::SUCCESS);
    }

    let mut cfg = Config::default();
    if let Some(path) = &args.config_file {
        let text = std::fs::read_to_string(path).map_err(|e| {
            anyhow::anyhow!("could not read config file {}: {e}", path.display())
        })?;
        let (parsed, warnings) = nats_server_rs::config::parse_config(&text)?;
        for w in warnings {
            eprintln!("{PROGRAM}: {w}");
        }
        cfg = parsed;
    }
    nats_server_rs::config::apply_flags(&mut cfg, &args);
    cfg.validate()?;

    if args.test_config {
        println!("configuration OK");
        return Ok(ExitCode::SUCCESS);
    }

    #[cfg(feature = "allocstats")]
    nats_server_rs::allocstats::dump("start");

    let server = Server::new(cfg);
    let addr = bind(&server).await?;
    let ports_file = write_ports_file(&server, addr)?;

    if server.cfg.debug {
        eprintln!("{PROGRAM}: listening on {addr}");
    }

    // Run until a signal says stop. SIGTERM and SIGINT are the two the harness
    // and an operator use; SIGKILL leaves the ports file behind, which is why
    // readiness is never inferred from the file alone.
    let mut term = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())?;
    let mut int = tokio::signal::unix::signal(tokio::signal::unix::SignalKind::interrupt())?;
    tokio::select! {
        _ = term.recv() => {}
        _ = int.recv() => {}
    }

    #[cfg(feature = "allocstats")]
    nats_server_rs::allocstats::dump("exit");

    remove_ports_file(&ports_file);
    Ok(ExitCode::SUCCESS)
}

async fn bind(server: &std::sync::Arc<Server>) -> anyhow::Result<std::net::SocketAddr> {
    net::serve(server.clone()).await.map_err(|e| {
        anyhow::anyhow!(
            "process couldn't start server: failed to listen on {}:{} - {e}",
            server.cfg.host,
            server.cfg.bind_port()
        )
    })
}

/// `<exe_basename>_<pid>.ports` containing `{"nats":["nats://host:port"]}`.
fn write_ports_file(server: &Server, addr: std::net::SocketAddr) -> anyhow::Result<Option<PathBuf>> {
    let dir = match &server.cfg.ports_file_dir {
        Some(d) => d.clone(),
        None => return Ok(None),
    };
    let exe = std::env::current_exe()
        .ok()
        .and_then(|p| p.file_stem().map(|s| s.to_string_lossy().into_owned()))
        .unwrap_or_else(|| PROGRAM.to_string());
    let path = dir.join(format!("{exe}_{}.ports", std::process::id()));
    let body = format!(
        r#"{{"nats":["nats://{}:{}"]}}"#,
        server_host(addr),
        addr.port()
    );
    // A half-written file is unparseable and the harness treats anything
    // unparseable as "not yet", so a plain write is enough; it happens after the
    // listener is already accepting.
    std::fs::write(&path, body)?;
    Ok(Some(path))
}

fn server_host(addr: std::net::SocketAddr) -> String {
    addr.ip().to_string()
}

fn remove_ports_file(path: &Option<PathBuf>) {
    if let Some(p) = path {
        let _ = std::fs::remove_file(p);
    }
}
