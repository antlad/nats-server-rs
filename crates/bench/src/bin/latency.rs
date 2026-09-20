//! Request-reply round-trip latency, serialised (one request in flight).
//!
//! Env: ITERS (default 20_000), WARMUP (default 1_000).

use futures::StreamExt;
use nats_bench::{param, report_latency};
use nats_test_harness::Server;
use std::time::Instant;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let iters = param("ITERS", 20_000);
    let warmup = param("WARMUP", 1_000);
    let srv = Server::start()?;
    println!("# server: {}", srv.url);

    let nc = async_nats::connect(srv.client_addr()).await?;
    let mut sub = nc.subscribe("lat.echo").await?;
    let resp = nc.clone();
    tokio::spawn(async move {
        while let Some(msg) = sub.next().await {
            if let Some(reply) = msg.reply {
                let _ = resp.publish(reply, msg.payload).await;
            }
        }
    });

    for _ in 0..warmup {
        nc.request("lat.echo", "w".into()).await?;
    }
    let mut samples = Vec::with_capacity(iters as usize);
    for _ in 0..iters {
        let t = Instant::now();
        nc.request("lat.echo", "x".into()).await?;
        samples.push(t.elapsed().as_nanos() as u64);
    }
    report_latency("latency", samples);
    Ok(())
}
