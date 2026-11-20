//! The pub/sub fabric Octopus runs over. MQTT 5 is the first binding: Solace PubSub+ speaks it natively, and
//! Mosquitto or EMQX run it in a container for tests. Other bindings (Solace's native API, AMQP 1.0) implement the
//! same small surface: publish, subscribe, and a stream of incoming messages.
//!
//! The fabric is never the durability point: callers treat every message as possibly lost, repeated or
//! reordered.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use bytes::Bytes;
use rumqttc::v5::mqttbytes::QoS;
use rumqttc::v5::mqttbytes::v5::{LastWill, Packet};
use rumqttc::NetworkOptions;
use rumqttc::v5::{AsyncClient, Event, MqttOptions};
use tokio::sync::mpsc;
use tracing::{debug, info, warn};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("fabric: {0}")]
    Fabric(String),
}

pub type Result<T> = std::result::Result<T, Error>;

/// A message as received.
#[derive(Clone, Debug)]
pub struct Incoming {
    pub topic: String,
    pub payload: Bytes,
}

#[derive(Clone, Debug)]
pub struct MqttConfig {
    pub host: String,
    pub port: u16,
    pub client_id: String,
    pub username: Option<String>,
    pub password: Option<String>,
    pub keep_alive: Duration,
    /// QoS 1 by default: fewer losses on reconnects. QoS 0 ("direct") is also safe, only slower to recover.
    pub qos: u8,
    /// Retained message to publish on an unclean disconnect (e.g. clear a member announcement).
    pub last_will: Option<(String, Bytes, bool)>,
    pub max_packet_size: u32,
    /// Capacity of the outgoing request queue and of the incoming channel.
    pub capacity: usize,
}

impl MqttConfig {
    pub fn new(host: impl Into<String>, port: u16, client_id: impl Into<String>) -> Self {
        Self {
            host: host.into(),
            port,
            client_id: client_id.into(),
            username: None,
            password: None,
            keep_alive: Duration::from_secs(5),
            qos: 1,
            last_will: None,
            max_packet_size: 64 * 1024 * 1024,
            capacity: 65_536,
        }
    }
}

/// Handle for publishing and subscribing; cheap to clone.
#[derive(Clone)]
pub struct Fabric {
    client: AsyncClient,
    qos: QoS,
    subscriptions: Arc<Mutex<BTreeSet<String>>>,
}

fn qos(q: u8) -> QoS {
    match q {
        0 => QoS::AtMostOnce,
        2 => QoS::ExactlyOnce,
        _ => QoS::AtLeastOnce,
    }
}

impl Fabric {
    /// Connects (reconnecting for ever in the background) and returns the handle and the incoming messages.
    pub fn connect_mqtt(cfg: MqttConfig) -> (Fabric, mpsc::Receiver<Incoming>) {
        let mut opts = MqttOptions::new(cfg.client_id.clone(), cfg.host.clone(), cfg.port);
        opts.set_keep_alive(cfg.keep_alive);
        opts.set_clean_start(true);
        opts.set_max_packet_size(Some(cfg.max_packet_size));
        // small, latency-sensitive messages: never wait for Nagle's algorithm
        let mut net = NetworkOptions::new();
        net.set_tcp_nodelay(true);
        opts.set_network_options(net);
        if let (Some(u), Some(p)) = (&cfg.username, &cfg.password) {
            opts.set_credentials(u.clone(), p.clone());
        }
        if let Some((topic, payload, retain)) = &cfg.last_will {
            opts.set_last_will(LastWill::new(topic.clone(), payload.clone(), qos(cfg.qos), *retain, None));
        }
        let (client, mut eventloop) = AsyncClient::new(opts, cfg.capacity);
        let (tx, rx) = mpsc::channel(cfg.capacity);
        let fabric = Fabric { client: client.clone(), qos: qos(cfg.qos), subscriptions: Arc::new(Mutex::new(BTreeSet::new())) };
        let subs = fabric.subscriptions.clone();
        let q = fabric.qos;
        let id = cfg.client_id.clone();
        tokio::spawn(async move {
            let mut connected = false;
            loop {
                match eventloop.poll().await {
                    Ok(Event::Incoming(Packet::Publish(p))) => {
                        let topic = String::from_utf8_lossy(&p.topic).into_owned();
                        if tx.send(Incoming { topic, payload: p.payload }).await.is_err() {
                            return; // receiver dropped: shut down
                        }
                    }
                    Ok(Event::Incoming(Packet::ConnAck(_))) => {
                        info!(client = %id, "fabric connected");
                        connected = true;
                        // clean start: the broker forgot our subscriptions; restore them
                        let all: Vec<String> = subs.lock().unwrap().iter().cloned().collect();
                        let c = client.clone();
                        tokio::spawn(async move {
                            for f in all {
                                if let Err(e) = c.subscribe(f.clone(), q).await {
                                    warn!(filter = %f, error = %e, "resubscribe failed");
                                }
                            }
                        });
                    }
                    Ok(_) => {}
                    Err(e) => {
                        if connected {
                            warn!(client = %id, error = %e, "fabric connection lost; reconnecting");
                        } else {
                            debug!(client = %id, error = %e, "fabric connect failed; retrying");
                        }
                        connected = false;
                        tokio::time::sleep(Duration::from_millis(250)).await;
                    }
                }
            }
        });
        (fabric, rx)
    }

    pub async fn publish(&self, topic: impl Into<String>, payload: Bytes) -> Result<()> {
        self.client.publish(topic, self.qos, false, payload).await.map_err(|e| Error::Fabric(e.to_string()))
    }

    pub async fn publish_retained(&self, topic: impl Into<String>, payload: Bytes) -> Result<()> {
        self.client.publish(topic, self.qos, true, payload).await.map_err(|e| Error::Fabric(e.to_string()))
    }

    /// Subscribes now and again after every reconnect.
    pub async fn subscribe(&self, filter: impl Into<String>) -> Result<()> {
        let f = filter.into();
        self.subscriptions.lock().unwrap().insert(f.clone());
        self.client.subscribe(f, self.qos).await.map_err(|e| Error::Fabric(e.to_string()))
    }

    pub async fn disconnect(&self) {
        let _ = self.client.disconnect().await;
    }
}
