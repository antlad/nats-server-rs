//! Fan-out: 1 publisher, SUBSCRIBERS subscribers, each receives every message.
//!
//! Env: MSGS (default 100_000), SIZE (default 256), SUBSCRIBERS (default 10).

use bytes::Bytes;
use futures::StreamExt;
use nats_bench::{param, report, target};
use std::time::{Duration, Instant};

const STALL_TIMEOUT: Duration = Duration::from_secs(30);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let msgs = param("MSGS", 100_000);
    let size = param("SIZE", 256) as usize;
    let nsubs = param("SUBSCRIBERS", 10);
    let (_srv, addr) = target()?;
    println!("# server: nats://{addr}");

    let pub_nc = async_nats::connect(addr.clone()).await?;
    let mut subs = Vec::new();
    for _ in 0..nsubs {
        let nc = async_nats::connect(addr.clone()).await?;
        let sub = nc.subscribe("fan").await?;
        // Barrier: a request on the *subscriber's* connection round-trips
        // through the server, and because SUB and PUB share that connection the
        // server must have registered "fan" before it answered. The error
        // (no responders) is expected and irrelevant.
        let _ = nc.request("fan.bARRIER", "".into()).await;
        subs.push(sub);
    }
    pub_nc.flush().await?;

    let payload = Bytes::from(vec![b'x'; size]);
    let start = Instant::now();
    let (p, pl) = (pub_nc.clone(), payload.clone());
    let publisher = tokio::spawn(async move {
        for _ in 0..msgs {
            p.publish("fan", pl.clone()).await?;
        }
        anyhow::Result::<()>::Ok(p.flush().await?)
    });

    // One counting task per subscriber: no racing, no round-robin.
    let mut counters = Vec::new();
    for mut sub in subs {
        counters.push(tokio::spawn(async move {
            for i in 0..msgs {
                match tokio::time::timeout(STALL_TIMEOUT, sub.next()).await {
                    Ok(Some(_)) => {}
                    Ok(None) => panic!("subscriber stream ended after {i} messages"),
                    Err(_) => panic!("subscriber stalled after {i} messages"),
                }
            }
            anyhow::Result::<()>::Ok(())
        }));
    }
    for c in counters {
        c.await??;
    }
    publisher.await??;

    report("fanout", msgs * nsubs, size as u64, start.elapsed());
    println!("# delivered={} subscribers={}", msgs * nsubs, nsubs);
    Ok(())
}
