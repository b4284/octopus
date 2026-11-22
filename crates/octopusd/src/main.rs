//! octopusd: one dispatcher instance. Configuration from environment variables (container friendly):
//!
//! | variable | default |
//! |---|---|
//! | `OCTOPUS_MEMBER_ID` | the host name |
//! | `OCTOPUS_NS` | `octopus` |
//! | `OCTOPUS_BACKEND` | `pg` (PostgreSQL), `mssql` (SQL Server 2025) or `mysql` (MySQL 8.4) |
//! | `OCTOPUS_PG` | `host=127.0.0.1 port=5432 user=octopus dbname=octopus` |
//! | `OCTOPUS_MSSQL` (ADO.NET style) | `server=tcp:127.0.0.1,1433;database=octopus;user=octopus;TrustServerCertificate=true` |
//! | `OCTOPUS_MSSQL_INMEMORY` (`1`: the event table memory-optimized; fixed per database) | `0` |
//! | `OCTOPUS_MYSQL` (URL) | `mysql://octopus@127.0.0.1:3306/octopus` |
//! | `OCTOPUS_PG_POOL` (any backend) | `32` |
//! | `OCTOPUS_SCHEMA` | `octopus` (MySQL: the table prefix `<schema>_`) |
//! | `OCTOPUS_APPLY_HOOK`, `OCTOPUS_LOAD_HOOK` | none |
//! | `OCTOPUS_TRANSPORT` | `mqtt`, `nats` or `zmq` (ZeroMQ, no broker) |
//! | `OCTOPUS_MQTT_HOST`, `OCTOPUS_MQTT_PORT` | `127.0.0.1`, `1883` |
//! | `OCTOPUS_MQTT_USER`, `OCTOPUS_MQTT_PASSWORD` | none |
//! | `OCTOPUS_NATS_URL` (several servers: comma-separated) | `nats://127.0.0.1:4222` |
//! | `OCTOPUS_NATS_USER`, `OCTOPUS_NATS_PASSWORD` | none |
//! | `OCTOPUS_ZMQ_PEERS` (every dispatcher as `id@host:port`: ROUTER on port, PUB on port + 1; must list this one) | none |
//! | `OCTOPUS_ZMQ_BIND_HOST` | `0.0.0.0` |
//! | `OCTOPUS_LEASE_TTL_MS`, `OCTOPUS_TICK_MS`, `OCTOPUS_DRAIN_MS` | `3000`, `500`, `5000` |
//! | `OCTOPUS_GAP_ALERT_MS`, `OCTOPUS_GAP_CHECK_MS` (report a source stopped by a missing event) | `30000`, `10000` |
//! | `OCTOPUS_GAP_SKIP_MS` (skip a gap after this long; per group: `octopus.grp.gap_skip_ms`, negative = never) | unset: never |
//! | `OCTOPUS_DELIVERY_TIMEOUT_MS` | `30000` |
//! | `OCTOPUS_PUBLISH_EXECUTORS`, `OCTOPUS_COMMIT_EXECUTORS` | `16`, `16` |
//! | `RUST_LOG` | `info` |

use std::time::Duration;

use octopus_core::Backend;
use octopus_fabric::{Fabric, MqttConfig, NatsConfig, ZmqConfig, ZmqPeer};
use octopus_mssql::{MssqlBackend, MssqlConfig};
use octopus_mysql::{MysqlBackend, MysqlConfig};
use octopus_pg::{PgBackend, PgConfig};
use octopus_proto::Topics;
use octopusd::{Config, Dispatcher};
use tokio::signal::unix::{SignalKind, signal};
use tokio::sync::watch;
use tracing::{error, info};

fn env(name: &str, default: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| default.to_owned())
}

fn env_ms(name: &str, default: u64) -> Duration {
    Duration::from_millis(std::env::var(name).ok().and_then(|v| v.parse().ok()).unwrap_or(default))
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();

    let host = std::env::var("HOSTNAME").unwrap_or_else(|_| "octopusd".into());
    let mut cfg = Config::new(env("OCTOPUS_MEMBER_ID", &host));
    cfg.ns = env("OCTOPUS_NS", "octopus");
    cfg.lease_ttl = env_ms("OCTOPUS_LEASE_TTL_MS", 3000);
    cfg.tick = env_ms("OCTOPUS_TICK_MS", 500);
    cfg.drain = env_ms("OCTOPUS_DRAIN_MS", 5000);
    cfg.gap_alert = env_ms("OCTOPUS_GAP_ALERT_MS", 30_000);
    cfg.gap_check = env_ms("OCTOPUS_GAP_CHECK_MS", 10_000);
    cfg.gap_skip = std::env::var("OCTOPUS_GAP_SKIP_MS").ok().and_then(|v| v.parse::<u64>().ok()).map(Duration::from_millis);
    cfg.delivery_timeout = env_ms("OCTOPUS_DELIVERY_TIMEOUT_MS", 30_000);
    // concurrent database transactions for publishes and for commits/removals (batches form while they run)
    cfg.publish_executors = env("OCTOPUS_PUBLISH_EXECUTORS", "16").parse().unwrap_or(16);
    cfg.commit_executors = env("OCTOPUS_COMMIT_EXECUTORS", "16").parse().unwrap_or(16);
    if !octopus_proto::valid_name(&cfg.member_id) {
        error!(member = %cfg.member_id, "invalid member id");
        std::process::exit(2);
    }

    let pool: usize = env("OCTOPUS_PG_POOL", "32").parse().unwrap_or(32);
    let schema = env("OCTOPUS_SCHEMA", "octopus");
    let apply_hook = std::env::var("OCTOPUS_APPLY_HOOK").ok().filter(|s| !s.is_empty());
    let load_hook = std::env::var("OCTOPUS_LOAD_HOOK").ok().filter(|s| !s.is_empty());
    // known heads are handed out straight away unless contexts must be preloaded for them
    cfg.fast_path = load_hook.is_none();
    match env("OCTOPUS_BACKEND", "pg").as_str() {
        "mssql" => {
            let mut c = MssqlConfig::new(env("OCTOPUS_MSSQL", "server=tcp:127.0.0.1,1433;database=octopus;user=octopus;TrustServerCertificate=true"));
            (c.pool_size, c.schema, c.apply_hook, c.load_hook) = (pool, schema, apply_hook, load_hook);
            c.in_memory = env("OCTOPUS_MSSQL_INMEMORY", "0") == "1";
            serve(cfg, retry(|| MssqlBackend::connect(c.clone())).await).await
        }
        "mysql" => {
            let mut c = MysqlConfig::new(env("OCTOPUS_MYSQL", "mysql://octopus@127.0.0.1:3306/octopus"));
            (c.pool_size, c.apply_hook, c.load_hook) = (pool, apply_hook, load_hook);
            c.prefix = format!("{schema}_");
            serve(cfg, retry(|| MysqlBackend::connect(c.clone())).await).await
        }
        "pg" => {
            let mut c = PgConfig::new(env("OCTOPUS_PG", "host=127.0.0.1 port=5432 user=octopus dbname=octopus"));
            (c.pool_size, c.schema, c.apply_hook, c.load_hook) = (pool, schema, apply_hook, load_hook);
            serve(cfg, retry(|| PgBackend::connect(c.clone())).await).await
        }
        other => {
            error!(backend = other, "OCTOPUS_BACKEND: use pg, mssql or mysql");
            std::process::exit(2);
        }
    }
}

/// Connects, retrying every second until the database is reachable.
async fn retry<B, F, Fut>(connect: F) -> B
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = octopus_core::Result<B>>,
{
    loop {
        match connect().await {
            Ok(b) => return b,
            Err(e) => {
                error!(error = %e, "database not reachable; retrying");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

async fn serve<B: Backend>(cfg: octopusd::Config, backend: B) {
    let topics = Topics::new(cfg.ns.clone());
    let client = format!("octopusd-{}-{}", cfg.ns, cfg.member_id);
    let transport = env("OCTOPUS_TRANSPORT", "mqtt");
    let (fabric, incoming) = if transport == "zmq" {
        // brokerless: this dispatcher binds its entry in the list, clients connect to every entry
        let peers = match ZmqPeer::parse_list(&env("OCTOPUS_ZMQ_PEERS", "")) {
            Ok(p) if p.iter().any(|p| p.id == cfg.member_id) => p,
            Ok(_) => panic!("OCTOPUS_ZMQ_PEERS must list this dispatcher ({}) as {}@host:port", cfg.member_id, cfg.member_id),
            Err(e) => panic!("OCTOPUS_ZMQ_PEERS: {e}"),
        };
        let mut z = ZmqConfig::new(cfg.member_id.clone(), peers);
        z.bind_host = env("OCTOPUS_ZMQ_BIND_HOST", "0.0.0.0");
        Fabric::connect_zmq(z).await.unwrap_or_else(|e| panic!("{e}"))
    } else if transport == "nats" {
        // NATS keeps no retained messages and has no last will: the periodic announcements carry membership and
        // ownership, and a dead member's groups are announced again by their next owner.
        let mut n = NatsConfig::new(env("OCTOPUS_NATS_URL", "nats://127.0.0.1:4222"), client);
        n.username = std::env::var("OCTOPUS_NATS_USER").ok();
        n.password = std::env::var("OCTOPUS_NATS_PASSWORD").ok();
        loop {
            match Fabric::connect_nats(n.clone()).await {
                Ok(f) => break f,
                Err(e) => {
                    error!(error = %e, "NATS not reachable; retrying");
                    tokio::time::sleep(Duration::from_secs(1)).await;
                }
            }
        }
    } else {
        let mut mq = MqttConfig::new(
            env("OCTOPUS_MQTT_HOST", "127.0.0.1"),
            env("OCTOPUS_MQTT_PORT", "1883").parse().unwrap_or(1883),
            client,
        );
        mq.username = std::env::var("OCTOPUS_MQTT_USER").ok();
        mq.password = std::env::var("OCTOPUS_MQTT_PASSWORD").ok();
        mq.last_will = Some(Dispatcher::<B>::last_will(&topics, &cfg.member_id));
        Fabric::connect_mqtt(mq)
    };

    let (stop_tx, stop_rx) = watch::channel(false);
    tokio::spawn(async move {
        let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
        tokio::select! {
            _ = term.recv() => {}
            _ = tokio::signal::ctrl_c() => {}
        }
        info!("stop requested");
        let _ = stop_tx.send(true);
    });

    Dispatcher::new(cfg, backend, fabric).run(incoming, stop_rx).await;
}
