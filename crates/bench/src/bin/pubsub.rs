//! Throughput: one publisher, one subscriber, N messages of SIZE bytes.
//!
//! Env: MSGS (default 1_000_000), SIZE (default 256).

use bytes::Bytes;
use futures::StreamExt;
use nats_bench::{param, report};
use nats_test_harness::Server;
use std::time::Instant;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let msgs = param("MSGS", 1_000_000);
    let size = param("SIZE", 256) as usize;
    let srv = Server::start()?;
    println!("# server: {}", srv.url);

    let pub_nc = async_nats::connect(srv.client_addr()).await?;
    let sub_nc = async_nats::connect(srv.client_addr()).await?;
    let mut sub = sub_nc.subscribe("bench").await?;
    sub_nc.flush().await?;

    let payload = Bytes::from(vec![b'x'; size]);
    let start = Instant::now();
    let (p, pl) = (pub_nc.clone(), payload.clone());
    let publisher = tokio::spawn(async move {
        for _ in 0..msgs {
            p.publish("bench", pl.clone()).await?;
        }
        anyhow::Result::<()>::Ok(p.flush().await?)
    });
    for _ in 0..msgs {
        sub.next().await.expect("delivery");
    }
    publisher.await??;
    report("pubsub", msgs, size as u64, start.elapsed());
    Ok(())
}
