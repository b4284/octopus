//! The pub/sub fabric Octopus runs over: MQTT 5 (Solace PubSub+ speaks it natively; Mosquitto or EMQX in a container
//! for tests) or NATS. Both implement the same small surface: publish, subscribe, and a stream of incoming messages.
//! Topics are MQTT-style everywhere (`ns/kind/name`, filters with `+` and `#`, `$share/<name>/...` for a shared
//! subscription); the NATS binding maps them to subjects (`/` -> `.`, `+` -> `*`, `#` -> `>`, a shared subscription
//! -> a queue group) and percent-encodes the characters NATS reserves inside a segment.
//!
//! NATS has neither retained messages nor a last will: a retained publish is a plain publish (Octopus re-announces
//! members and owners every 5 s, so a newcomer waits at most that long), and a dispatcher that dies without saying so
//! is noticed through lease expiry and worker timeouts.
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
    /// QoS 0 ("direct") by default. Octopus never relies on the fabric's delivery guarantees (every acknowledgement
    /// is an application reply after the database commit, and every message is retried), so QoS 1 only adds a broker
    /// acknowledgement and an in-flight window per message.
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
            qos: 0,
            last_will: None,
            max_packet_size: 64 * 1024 * 1024,
            capacity: 65_536,
        }
    }
}

#[derive(Clone, Debug)]
pub struct NatsConfig {
    /// e.g. `nats://127.0.0.1:4222` (several servers: comma-separated)
    pub url: String,
    pub name: String,
    pub username: Option<String>,
    pub password: Option<String>,
    /// Capacity of the incoming channel (and of each subscription's buffer).
    pub capacity: usize,
}

impl NatsConfig {
    pub fn new(url: impl Into<String>, name: impl Into<String>) -> Self {
        Self { url: url.into(), name: name.into(), username: None, password: None, capacity: 65_536 }
    }
}

/// A comma-separated server list (async-nats parses one URL per string).
fn nats_servers(url: &str) -> Result<Vec<async_nats::ServerAddr>> {
    url.split(',')
        .map(str::trim)
        .filter(|u| !u.is_empty())
        .map(|u| u.parse::<async_nats::ServerAddr>().map_err(|e| Error::Fabric(format!("NATS server {u:?}: {e}"))))
        .collect()
}

/// Handle for publishing and subscribing; cheap to clone.
#[derive(Clone)]
pub struct Fabric {
    inner: Inner,
    subscriptions: Arc<Mutex<BTreeSet<String>>>,
}

#[derive(Clone)]
enum Inner {
    Mqtt { client: AsyncClient, qos: QoS },
    Nats { client: async_nats::Client, tx: mpsc::Sender<Incoming> },
}

/// One topic segment as a NATS subject token: the characters NATS reserves are percent-encoded.
fn nats_token(seg: &str) -> String {
    let mut out = String::with_capacity(seg.len());
    for c in seg.chars() {
        match c {
            '%' => out.push_str("%25"),
            '.' => out.push_str("%2E"),
            '*' => out.push_str("%2A"),
            '>' => out.push_str("%3E"),
            ' ' => out.push_str("%20"),
            '\t' => out.push_str("%09"),
            c => out.push(c),
        }
    }
    out
}

/// An MQTT topic or filter as a NATS subject (wildcards mapped), and the queue group of a shared subscription.
fn nats_subject(topic: &str) -> (String, Option<String>) {
    let (queue, rest) = match topic.strip_prefix("$share/").and_then(|r| r.split_once('/')) {
        Some((q, rest)) => (Some(q.to_owned()), rest),
        None => (None, topic),
    };
    let subject = rest
        .split('/')
        .map(|seg| match seg {
            "+" => "*".to_owned(),
            "#" => ">".to_owned(),
            s => nats_token(s),
        })
        .collect::<Vec<_>>()
        .join(".");
    (subject, queue)
}

/// A NATS subject back as an MQTT-style topic.
fn mqtt_topic(subject: &str) -> String {
    subject
        .split('.')
        .map(|t| {
            if !t.contains('%') {
                return t.to_owned();
            }
            let b = t.as_bytes();
            let mut out = Vec::with_capacity(b.len());
            let mut i = 0;
            while i < b.len() {
                if b[i] == b'%' && i + 2 < b.len() {
                    if let Ok(v) = u8::from_str_radix(&t[i + 1..i + 3], 16) {
                        out.push(v);
                        i += 3;
                        continue;
                    }
                }
                out.push(b[i]);
                i += 1;
            }
            String::from_utf8_lossy(&out).into_owned()
        })
        .collect::<Vec<_>>()
        .join("/")
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
        let q = qos(cfg.qos);
        let fabric = Fabric { inner: Inner::Mqtt { client: client.clone(), qos: q }, subscriptions: Arc::new(Mutex::new(BTreeSet::new())) };
        let subs = fabric.subscriptions.clone();
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

    /// Connects to NATS (reconnecting for ever in the background; the first connect too) and returns the handle and
    /// the incoming messages. The last will of an MQTT connection has no NATS counterpart (see the module docs).
    pub async fn connect_nats(cfg: NatsConfig) -> Result<(Fabric, mpsc::Receiver<Incoming>)> {
        let mut opts = async_nats::ConnectOptions::new()
            .name(cfg.name.clone())
            .retry_on_initial_connect()
            .max_reconnects(None)
            .subscription_capacity(cfg.capacity)
            .event_callback(|e| async move { info!(event = %e, "nats") });
        if let (Some(u), Some(p)) = (cfg.username.clone(), cfg.password.clone()) {
            opts = opts.user_and_password(u, p);
        }
        let client = opts.connect(nats_servers(&cfg.url)?).await.map_err(|e| Error::Fabric(e.to_string()))?;
        let (tx, rx) = mpsc::channel(cfg.capacity);
        Ok((Fabric { inner: Inner::Nats { client, tx }, subscriptions: Arc::new(Mutex::new(BTreeSet::new())) }, rx))
    }

    pub async fn publish(&self, topic: impl Into<String>, payload: Bytes) -> Result<()> {
        match &self.inner {
            Inner::Mqtt { client, qos } => client.publish(topic, *qos, false, payload).await.map_err(|e| Error::Fabric(e.to_string())),
            Inner::Nats { client, .. } => {
                let (subject, _) = nats_subject(&topic.into());
                client.publish(subject, payload).await.map_err(|e| Error::Fabric(e.to_string()))
            }
        }
    }

    /// A retained publish on MQTT; a plain one on NATS (no retained messages: see the module docs).
    pub async fn publish_retained(&self, topic: impl Into<String>, payload: Bytes) -> Result<()> {
        match &self.inner {
            Inner::Mqtt { client, qos } => client.publish(topic, *qos, true, payload).await.map_err(|e| Error::Fabric(e.to_string())),
            Inner::Nats { .. } => self.publish(topic, payload).await,
        }
    }

    /// Subscribes now and again after every reconnect.
    pub async fn subscribe(&self, filter: impl Into<String>) -> Result<()> {
        let f = filter.into();
        if !self.subscriptions.lock().unwrap().insert(f.clone()) {
            if matches!(self.inner, Inner::Nats { .. }) {
                return Ok(()); // already subscribed (NATS keeps subscriptions across reconnects)
            }
        }
        match &self.inner {
            Inner::Mqtt { client, qos } => client.subscribe(f, *qos).await.map_err(|e| Error::Fabric(e.to_string())),
            Inner::Nats { client, tx } => {
                let (subject, queue) = nats_subject(&f);
                let mut sub = match queue {
                    Some(q) => client.queue_subscribe(subject, q).await,
                    None => client.subscribe(subject).await,
                }
                .map_err(|e| Error::Fabric(e.to_string()))?;
                let tx = tx.clone();
                tokio::spawn(async move {
                    use futures_util::StreamExt;
                    while let Some(m) = sub.next().await {
                        if tx.send(Incoming { topic: mqtt_topic(m.subject.as_str()), payload: m.payload }).await.is_err() {
                            return; // receiver dropped: shut down
                        }
                    }
                });
                Ok(())
            }
        }
    }

    pub async fn disconnect(&self) {
        match &self.inner {
            Inner::Mqtt { client, .. } => {
                let _ = client.disconnect().await;
            }
            Inner::Nats { client, .. } => {
                let _ = client.flush().await;
                let _ = client.drain().await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_server_list_is_split_on_commas() {
        assert_eq!(nats_servers("nats://127.0.0.1:6440, nats://127.0.0.1:6441,nats://h:4222").unwrap().len(), 3);
        assert_eq!(nats_servers("nats://127.0.0.1:4222").unwrap().len(), 1);
        assert!(nats_servers("nats://127.0.0.1:x").is_err());
    }

    #[test]
    fn topics_map_to_subjects_and_back() {
        assert_eq!(nats_subject("octopus/pub/g1"), ("octopus.pub.g1".into(), None));
        assert_eq!(nats_subject("$share/octopusd/octopus/pub/+"), ("octopus.pub.*".into(), Some("octopusd".into())));
        assert_eq!(nats_subject("octopus/member/#"), ("octopus.member.>".into(), None));
        assert_eq!(nats_subject("octopus/inbox/a.b c*d>e%f").0, "octopus.inbox.a%2Eb%20c%2Ad%3Ee%25f");
        assert_eq!(mqtt_topic("octopus.inbox.a%2Eb%20c%2Ad%3Ee%25f"), "octopus/inbox/a.b c*d>e%f");
        assert_eq!(mqtt_topic("octopus.owner.g7"), "octopus/owner/g7");
    }
}
