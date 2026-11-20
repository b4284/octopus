//! octopusd: one dispatcher instance. Configuration from environment variables (container friendly):
//!
//! | variable | default |
//! |---|---|
//! | `OCTOPUS_MEMBER_ID` | the host name |
//! | `OCTOPUS_NS` | `octopus` |
//! | `OCTOPUS_PG` | `host=127.0.0.1 port=5432 user=octopus dbname=octopus` |
//! | `OCTOPUS_PG_POOL` | `32` |
//! | `OCTOPUS_SCHEMA` | `octopus` |
//! | `OCTOPUS_APPLY_HOOK`, `OCTOPUS_LOAD_HOOK` | none |
//! | `OCTOPUS_MQTT_HOST`, `OCTOPUS_MQTT_PORT` | `127.0.0.1`, `1883` |
//! | `OCTOPUS_MQTT_USER`, `OCTOPUS_MQTT_PASSWORD` | none |
//! | `OCTOPUS_LEASE_TTL_MS`, `OCTOPUS_TICK_MS`, `OCTOPUS_DRAIN_MS` | `3000`, `500`, `5000` |
//! | `OCTOPUS_DELIVERY_TIMEOUT_MS` | `30000` |
//! | `RUST_LOG` | `info` |

use std::time::Duration;

use octopus_fabric::{Fabric, MqttConfig};
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
    cfg.delivery_timeout = env_ms("OCTOPUS_DELIVERY_TIMEOUT_MS", 30_000);
    if !octopus_proto::valid_name(&cfg.member_id) {
        error!(member = %cfg.member_id, "invalid member id");
        std::process::exit(2);
    }

    let mut pg = PgConfig::new(env("OCTOPUS_PG", "host=127.0.0.1 port=5432 user=octopus dbname=octopus"));
    pg.pool_size = env("OCTOPUS_PG_POOL", "32").parse().unwrap_or(32);
    pg.schema = env("OCTOPUS_SCHEMA", "octopus");
    pg.apply_hook = std::env::var("OCTOPUS_APPLY_HOOK").ok().filter(|s| !s.is_empty());
    pg.load_hook = std::env::var("OCTOPUS_LOAD_HOOK").ok().filter(|s| !s.is_empty());
    // known heads are handed out straight away unless contexts must be preloaded for them
    cfg.fast_path = pg.load_hook.is_none();
    let backend = loop {
        match PgBackend::connect(pg.clone()).await {
            Ok(b) => break b,
            Err(e) => {
                error!(error = %e, "database not reachable; retrying");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    };

    let topics = Topics::new(cfg.ns.clone());
    let mut mq = MqttConfig::new(
        env("OCTOPUS_MQTT_HOST", "127.0.0.1"),
        env("OCTOPUS_MQTT_PORT", "1883").parse().unwrap_or(1883),
        format!("octopusd-{}-{}", cfg.ns, cfg.member_id),
    );
    mq.username = std::env::var("OCTOPUS_MQTT_USER").ok();
    mq.password = std::env::var("OCTOPUS_MQTT_PASSWORD").ok();
    mq.last_will = Some(Dispatcher::<PgBackend>::last_will(&topics, &cfg.member_id));
    let (fabric, incoming) = Fabric::connect_mqtt(mq);

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
