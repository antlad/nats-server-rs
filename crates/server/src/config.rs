//! Runtime options: the flag surface of the reference binary plus the small
//! config-file subset this server enforces.
//!
//! Flags are parsed by hand rather than with a derive because the reference's
//! flag set *is* the compatibility surface (`-a`, `-p -1`, `--ports_file_dir`,
//! `-c`, `-v`, `-h`), quirks included: `-p 0` means "unset" and resolves to 4222,
//! only `-p -1` is the random-port sentinel (`specs/protocol-contract.md` §1).

use std::path::PathBuf;
use std::time::Duration;

use anyhow::{bail, Result};

pub const DEFAULT_PORT: i32 = 4222;
/// `RANDOM_PORT` in the reference (`go:const.go`): the only way to ask for an
/// ephemeral port. `0` is *not* it — measured: `-p 0` resolved to 4222.
pub const RANDOM_PORT: i32 = -1;
/// `go:const.go:88-120`.
pub const MAX_PAYLOAD: u64 = 1024 * 1024;
pub const MAX_PENDING: u64 = 64 * 1024 * 1024;
pub const MAX_CONTROL_LINE: usize = 4096;
pub const MAX_CONNECTIONS: i64 = 64 * 1024;
pub const PING_INTERVAL: Duration = Duration::from_secs(120);
pub const MAX_PINGS_OUT: u64 = 2;
pub const WRITE_DEADLINE: Duration = Duration::from_secs(10);
/// The reference's first keepalive probe is short whatever is configured
/// (`go:client.go:7026`), with up to 20 % of extra delay.
pub const FIRST_PING_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Debug, Clone)]
pub struct Config {
    pub host: String,
    /// `-1` = ephemeral, `0` = unset (resolved to [`DEFAULT_PORT`]).
    pub port: i32,
    pub ports_file_dir: Option<PathBuf>,
    pub server_name: Option<String>,
    pub max_payload: u64,
    pub max_pending: u64,
    pub max_control_line: usize,
    pub max_connections: i64,
    pub ping_interval: Duration,
    pub write_deadline: Duration,
    pub debug: bool,
    pub trace: bool,
}

impl Default for Config {
    fn default() -> Self {
        Config {
            host: "0.0.0.0".into(),
            port: 0,
            ports_file_dir: None,
            server_name: None,
            max_payload: MAX_PAYLOAD,
            max_pending: MAX_PENDING,
            max_control_line: MAX_CONTROL_LINE,
            max_connections: MAX_CONNECTIONS,
            ping_interval: PING_INTERVAL,
            write_deadline: WRITE_DEADLINE,
            debug: false,
            trace: false,
        }
    }
}

impl Config {
    pub fn bind_port(&self) -> i32 {
        if self.port == 0 {
            DEFAULT_PORT
        } else {
            self.port
        }
    }

    /// Cross-option sanity, in the reference's own words (measured).
    pub fn validate(&self) -> Result<()> {
        if self.max_payload > self.max_pending {
            bail!(
                "max_payload ({}) cannot be higher than max_pending ({})",
                self.max_payload,
                self.max_pending
            );
        }
        Ok(())
    }
}

/// What the command line said.
#[derive(Debug, Default, Clone)]
pub struct Args {
    pub config_file: Option<PathBuf>,
    pub test_config: bool,
    pub version: bool,
    pub help: bool,
    pub host: Option<String>,
    pub port: Option<i32>,
    pub ports_file_dir: Option<PathBuf>,
    pub server_name: Option<String>,
    pub debug: bool,
    pub trace: bool,
}

/// Parse argv. `Err` carries the message the reference would have printed.
pub fn parse_args<I: Iterator<Item = String>>(argv: &mut I, program: &str) -> Result<Args> {
    let mut out = Args::default();

    while let Some(arg) = argv.next() {
        if arg.is_empty() || !arg.starts_with('-') || arg == "-" {
            // Go's flag package stops at the first non-flag argument.
            break;
        }
        // `-x=v` is valid for long flags in Go's flag package.
        let (name, inline) = match arg.split_once('=') {
            Some((n, v)) if n.starts_with("--") => (n.to_string(), Some(v.to_string())),
            _ => (arg.clone(), None),
        };
        let mut value = || match inline.clone() {
            Some(v) => Ok(v),
            None => argv
                .next()
                .ok_or_else(|| anyhow::anyhow!("{program}: flag needs an argument: {name}")),
        };

        match name.as_str() {
            "-a" | "--addr" | "--net" => out.host = Some(value()?),
            "-p" | "--port" => {
                let v = value()?;
                match v.parse::<i32>() {
                    Ok(p) => out.port = Some(p),
                    Err(_) => bail!("{program}: invalid value \"{v}\" for flag -p: parse error"),
                }
            }
            "-n" | "--name" | "--server_name" => out.server_name = Some(value()?),
            "-c" | "--config" => out.config_file = Some(PathBuf::from(value()?)),
            "--ports_file_dir" => out.ports_file_dir = Some(PathBuf::from(value()?)),
            "-t" => out.test_config = true,
            "-v" | "--version" => out.version = true,
            "-h" | "--help" => out.help = true,
            "-D" | "--debug" => out.debug = true,
            "-V" | "--trace" => out.trace = true,
            other => bail!("{program}: flag provided but not defined: {other}"),
        }
    }
    Ok(out)
}

/// Command-line values win over the config file, as they do in the reference.
pub fn apply_flags(cfg: &mut Config, args: &Args) {
    if let Some(h) = &args.host {
        cfg.host = h.clone();
    }
    if let Some(p) = args.port {
        cfg.port = p;
    }
    if let Some(d) = &args.ports_file_dir {
        cfg.ports_file_dir = Some(d.clone());
    }
    if let Some(n) = &args.server_name {
        cfg.server_name = Some(n.clone());
    }
    cfg.debug |= args.debug;
    cfg.trace |= args.trace;
}

pub fn usage(program: &str) -> String {
    format!(
        "
Usage: {program} [options]

Server Options:
    -a, --addr, --net <host>         Bind to host address (default: 0.0.0.0)
    -p, --port <port>                Use port for clients (default: 4222)
    -n, --name, --server_name <name> Server name (default: auto)
    -c, --config <file>              Configuration file
    -t                               Test configuration and exit
        --ports_file_dir <dir>       Creates a ports file in the specified directory
    -v, --version                    Show version

Logging Options:
    -D, --debug                      Enable debugging output
    -V, --trace                      Trace the raw protocol
"
    )
}

/// Parse the subset of the NATS config format this server enforces. The syntax
/// is the reference's: `key: value`, `#` comments, quoted strings, durations as
/// strings (or as plain seconds, the old form, which draws a warning).
pub fn parse_config(text: &str) -> Result<(Config, Vec<String>)> {
    let mut cfg = Config::default();
    let mut warnings = Vec::new();

    for (idx, raw) in text.lines().enumerate() {
        let line = match raw.find('#') {
            Some(i) => raw[..i].trim(),
            None => raw.trim(),
        };
        if line.is_empty() {
            continue;
        }
        let (key, value) = match line.split_once(':') {
            Some(kv) => kv,
            None => bail!("config:{}:1: expected a key but got a value", idx + 1),
        };
        let key = key.trim().trim_matches('"');
        match key {
            "host" => cfg.host = parse_string(value)?,
            "port" => cfg.port = parse_int(value)? as i32,
            "server_name" | "name" => cfg.server_name = Some(parse_string(value)?),
            "max_payload" => cfg.max_payload = parse_int(value)? as u64,
            "max_pending" => cfg.max_pending = parse_int(value)? as u64,
            "max_control_line" => cfg.max_control_line = parse_int(value)? as usize,
            "max_connections" | "max_conn" => cfg.max_connections = parse_int(value)?,
            "ping_interval" => cfg.ping_interval = parse_duration(value, key, &mut warnings)?,
            "write_deadline" => cfg.write_deadline = parse_duration(value, key, &mut warnings)?,
            "debug" => cfg.debug = parse_bool(value)?,
            "trace" => cfg.trace = parse_bool(value)?,
            other => bail!("config: unknown field \"{other}\""),
        }
    }
    Ok((cfg, warnings))
}

fn unquote(v: &str) -> &str {
    let v = v.trim().trim_end_matches(',').trim();
    v.trim_matches(|c| c == '"' || c == '\'')
}

fn parse_string(v: &str) -> Result<String> {
    Ok(unquote(v).to_string())
}

fn parse_int(v: &str) -> Result<i64> {
    let s = unquote(v);
    if let Some(hex) = s.strip_prefix("0x").or_else(|| s.strip_prefix("0X")) {
        return i64::from_str_radix(hex, 16)
            .map_err(|_| anyhow::anyhow!("expected an integer but got {s}"));
    }
    s.parse().map_err(|_| anyhow::anyhow!("expected an integer but got {s}"))
}

fn parse_bool(v: &str) -> Result<bool> {
    match unquote(v) {
        "true" | "on" | "yes" => Ok(true),
        "false" | "off" | "no" => Ok(false),
        other => bail!("expected a boolean but got {other}"),
    }
}

fn parse_duration(v: &str, field: &str, warnings: &mut Vec<String>) -> Result<Duration> {
    let s = unquote(v);
    let quoted = v.trim().starts_with('"') || v.trim().starts_with('\'');
    if quoted {
        return match parse_duration_str(s) {
            Some(d) => Ok(d),
            None => bail!("error parsing {field}: invalid duration {s:?}"),
        };
    }
    match s.parse::<i64>() {
        Ok(secs) => {
            warnings.push(format!("{field} should be converted to a duration"));
            Ok(Duration::from_secs(secs as u64))
        }
        Err(_) => match parse_duration_str(s) {
            Some(d) => Ok(d),
            None => bail!("error parsing {field}: invalid duration {s:?}"),
        },
    }
}

/// Go's `time.ParseDuration`: a sequence of `number+unit`, `ns`/`us`/`ms`/`s`/
/// `m`/`h`. A bare number without a unit is seconds.
pub fn parse_duration_str(s: &str) -> Option<Duration> {
    let b = s.as_bytes();
    if b.is_empty() {
        return None;
    }
    let negative = b[0] == b'-';
    let mut i = usize::from(negative || b[0] == b'+');
    if i >= b.len() {
        return None;
    }
    let mut total: u128 = 0;
    let mut saw = false;
    while i < b.len() {
        let start = i;
        while i < b.len() && (b[i].is_ascii_digit() || b[i] == b'.') {
            i += 1;
        }
        if i == start {
            return None;
        }
        let num: f64 = s[start..i].parse().ok()?;
        let ustart = i;
        while i < b.len() && !b[i].is_ascii_digit() && b[i] != b'.' {
            i += 1;
        }
        let scale: u64 = match &s[ustart..i] {
            "ns" => 1,
            "us" | "\u{00b5}s" => 1_000,
            "ms" => 1_000_000,
            "s" => 1_000_000_000,
            "m" => 60 * 1_000_000_000,
            "h" => 3600 * 1_000_000_000,
            "" => 1_000_000_000,
            _ => return None,
        };
        total += num as u128 * scale as u128;
        saw = true;
    }
    if !saw || negative {
        return None;
    }
    Some(Duration::from_nanos(total as u64))
}
