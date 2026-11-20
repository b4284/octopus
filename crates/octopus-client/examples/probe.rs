//! Publish round-trip probe: sequential and concurrent publishes against running dispatchers.
//! usage: cargo run --release --example probe -- <ns> <group> <n> <concurrency>
use std::time::Instant;

use bytes::Bytes;
use octopus_client::{Producer, PublishItem};
use octopus_fabric::MqttConfig;

#[tokio::main]
async fn main() {
    let a: Vec<String> = std::env::args().collect();
    let (ns, group) = (a.get(1).cloned().unwrap_or("octopus".into()), a.get(2).cloned().unwrap_or("g0".into()));
    let n: usize = a.get(3).and_then(|s| s.parse().ok()).unwrap_or(2000);
    let conc: usize = a.get(4).and_then(|s| s.parse().ok()).unwrap_or(1);
    let big: usize = a.get(5).and_then(|s| s.parse().ok()).unwrap_or(0);
    let id = format!("probe{}", std::process::id());
    let p = Producer::connect(MqttConfig::new("127.0.0.1", 1883, id.clone()), &ns, &id).await.unwrap();
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    if big > 0 {
        // background: one large event every 200 ms, like the benchmark's large groups
        let (p, group, id) = (p.clone(), group.clone(), id.clone());
        tokio::spawn(async move {
            for q in 1.. {
                let item = PublishItem { group: group.clone(), source: format!("{id}-big"), seq: q, body: Bytes::from(vec![b'x'; big]) };
                let t = Instant::now();
                p.publish(vec![item]).await.unwrap();
                eprintln!("large publish {} ms", t.elapsed().as_millis());
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
        });
    }
    let start = Instant::now();
    let mut tasks = vec![];
    for c in 0..conc {
        let (p, group, id) = (p.clone(), group.clone(), id.clone());
        tasks.push(tokio::spawn(async move {
            let mut lat = vec![];
            for q in 1..=(n / conc) as i64 {
                let t = Instant::now();
                let item = PublishItem { group: group.clone(), source: format!("{id}-{c}"), seq: q, body: Bytes::from_static(b"{}") };
                p.publish(vec![item]).await.unwrap();
                lat.push(t.elapsed().as_micros() as u64);
            }
            lat
        }));
    }
    let mut lat: Vec<u64> = vec![];
    for t in tasks { lat.extend(t.await.unwrap()); }
    lat.sort();
    let el = start.elapsed().as_secs_f64();
    println!("{} publishes, concurrency {conc}: {:.0}/s, p50 {} us, p90 {} us, p99 {} us, max {} us",
        lat.len(), lat.len() as f64 / el, lat[lat.len() / 2], lat[lat.len() * 9 / 10], lat[lat.len() * 99 / 100], lat[lat.len() - 1]);
}
