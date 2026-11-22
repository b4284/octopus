//! The fabric without a broker, over ZeroMQ: every message travels one hop, client <-> dispatcher.
//!
//! Every dispatcher binds a ROUTER socket (requests to it, replies from it) and a PUB socket (its announcements).
//! Every participant, client or dispatcher, holds a DEALER (identity = its own id, the last segment of its inbox
//! topic) and a SUB connected to every other dispatcher. The dispatchers are a configured list of
//! `id@host:port` (ROUTER on `port`, PUB on `port + 1`); ZeroMQ reconnects on its own.
//!
//! Topics keep their MQTT shape and are routed by kind:
//! - `<ns>/inbox/<id>`: to dispatcher `<id>` through its DEALER; to a client through this dispatcher's ROUTER (the
//!   client is connected to every dispatcher, with its id as identity); to oneself directly.
//! - any other topic (`<ns>/pub/<group>`, `<ns>/any`: the shared subscriptions of MQTT): to one dispatcher, round
//!   robin over those connected and heard from lately (a dispatcher announces itself every 5 s, on
//!   `<ns>/member/<id>`).
//! - retained publishes (announcements): PUB. Subscribing to `<ns>/member/+` subscribes every SUB to the prefix
//!   `<ns>/member/`; inbox and shared filters need no subscription (those messages come to the DEALER/ROUTER).
//!
//! Like with NATS there are no retained messages and no last will. A message to a peer that is down or whose queue
//! is full is dropped: Octopus treats every message as possibly lost.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use tokio::sync::{mpsc, watch};
use tracing::{debug, info, warn};
use zeromq::util::PeerIdentity;
use zeromq::{
    DealerSocket, PubSocket, RouterSocket, Socket, SocketEvent, SocketOptions, SocketRecv, SocketSend, SubSocket,
    ZmqMessage,
};

use crate::{Error, Incoming, Result};

/// A dispatcher endpoint: ROUTER on `port`, PUB on `port + 1`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ZmqPeer {
    pub id: String,
    pub host: String,
    pub port: u16,
}

impl ZmqPeer {
    /// `id@host:port,id@host:port,...`
    pub fn parse_list(s: &str) -> Result<Vec<ZmqPeer>> {
        s.split(',')
            .map(str::trim)
            .filter(|p| !p.is_empty())
            .map(|p| {
                let bad = || Error::Fabric(format!("ZeroMQ peer {p:?}: expected id@host:port"));
                let (id, addr) = p.split_once('@').ok_or_else(bad)?;
                let (host, port) = addr.rsplit_once(':').ok_or_else(bad)?;
                let port: u16 = port.parse().map_err(|_| bad())?;
                if id.is_empty() || host.is_empty() || port == u16::MAX {
                    return Err(bad());
                }
                Ok(ZmqPeer { id: id.to_owned(), host: host.to_owned(), port })
            })
            .collect()
    }
}

#[derive(Clone, Debug)]
pub struct ZmqConfig {
    /// This participant's id: a dispatcher's member id, or a client's id (its inbox is `<ns>/inbox/<id>`).
    pub me: String,
    /// Every dispatcher. A dispatcher finds itself here by `me` and binds its entry's ports.
    pub peers: Vec<ZmqPeer>,
    /// Address the dispatcher binds on (default all interfaces).
    pub bind_host: String,
    /// Queue length per peer and of the incoming channel.
    pub capacity: usize,
}

impl ZmqConfig {
    pub fn new(me: impl Into<String>, peers: Vec<ZmqPeer>) -> Self {
        Self { me: me.into(), peers, bind_host: "0.0.0.0".into(), capacity: 65_536 }
    }
}

/// A dispatcher not heard from for this long gets no shared messages (it announces itself every 5 s).
const SILENT: Duration = Duration::from_secs(15);

struct Link {
    id: String,
    tx: mpsc::Sender<ZmqMessage>,
    connected: AtomicBool,
    /// milliseconds since `start` when connected or last heard from
    seen_ms: AtomicU64,
}

pub(crate) struct Zmq {
    me: String,
    links: Vec<Arc<Link>>,
    by_id: HashMap<String, usize>,
    router_tx: Option<mpsc::Sender<ZmqMessage>>,
    pub_tx: Option<mpsc::Sender<ZmqMessage>>,
    incoming: mpsc::Sender<Incoming>,
    prefixes: watch::Sender<Vec<String>>,
    rr: AtomicUsize,
    start: Instant,
}

fn msg(frames: Vec<Bytes>) -> ZmqMessage {
    ZmqMessage::try_from(frames).expect("non-empty message")
}

fn identity(id: &str) -> Result<PeerIdentity> {
    PeerIdentity::try_from(id.as_bytes().to_vec()).map_err(|e| Error::Fabric(format!("ZeroMQ identity {id:?}: {e}")))
}

/// `<ns>/inbox/<id>` -> `<id>`
fn inbox_of(topic: &str) -> Option<&str> {
    let mut it = topic.splitn(3, '/');
    let (_ns, kind, id) = (it.next()?, it.next()?, it.next()?);
    (kind == "inbox" && !id.is_empty()).then_some(id)
}

/// `<ns>/member/<id>` -> `<id>`
fn member_of(topic: &str) -> Option<&str> {
    let mut it = topic.splitn(3, '/');
    let (_ns, kind, id) = (it.next()?, it.next()?, it.next()?);
    (kind == "member").then_some(id)
}

/// Incoming frames [topic, payload] as a message.
fn incoming(m: ZmqMessage) -> Option<Incoming> {
    let mut f = m.into_vec();
    if f.len() != 2 {
        return None;
    }
    let payload = f.pop()?;
    let topic = String::from_utf8(f.pop()?.to_vec()).ok()?;
    Some(Incoming { topic, payload })
}

impl Zmq {
    pub(crate) async fn connect(cfg: ZmqConfig) -> Result<(Arc<Zmq>, mpsc::Receiver<Incoming>)> {
        let (in_tx, in_rx) = mpsc::channel(cfg.capacity);
        let (prefixes, _) = watch::channel(Vec::<String>::new());
        let start = Instant::now();
        let mut router_tx = None;
        let mut pub_tx = None;

        // a dispatcher binds its own entry
        if let Some(own) = cfg.peers.iter().find(|p| p.id == cfg.me) {
            let mut router = RouterSocket::new();
            let ep = format!("tcp://{}:{}", cfg.bind_host, own.port);
            router.bind(&ep).await.map_err(|e| Error::Fabric(format!("ZeroMQ bind {ep}: {e}")))?;
            let mut publ = PubSocket::new();
            let ep_pub = format!("tcp://{}:{}", cfg.bind_host, own.port + 1);
            publ.bind(&ep_pub).await.map_err(|e| Error::Fabric(format!("ZeroMQ bind {ep_pub}: {e}")))?;
            info!(router = %ep, publ = %ep_pub, "fabric (ZeroMQ) bound");
            let (mut send, mut recv) = router.split();
            let (tx, mut rx) = mpsc::channel::<ZmqMessage>(cfg.capacity);
            tokio::spawn(async move {
                while let Some(m) = rx.recv().await {
                    // a client that is gone: dropped (the sender retries)
                    if let Err(e) = send.send(m).await {
                        debug!(error = %e, "ZeroMQ: reply to a client dropped");
                    }
                }
            });
            let itx = in_tx.clone();
            tokio::spawn(async move {
                loop {
                    match recv.recv().await {
                        Ok(m) => {
                            let mut f = m.into_vec();
                            if f.len() == 3 {
                                f.remove(0); // the sender's identity: replies are addressed by topic
                                if let Some(i) = incoming(msg(f))
                                    && itx.send(i).await.is_err()
                                {
                                    return;
                                }
                            }
                        }
                        Err(e) => {
                            warn!(error = %e, "ZeroMQ: ROUTER receive failed");
                            tokio::time::sleep(Duration::from_millis(100)).await;
                        }
                    }
                }
            });
            router_tx = Some(tx);
            let (tx, mut rx) = mpsc::channel::<ZmqMessage>(cfg.capacity);
            tokio::spawn(async move {
                while let Some(m) = rx.recv().await {
                    let _ = publ.send(m).await;
                }
            });
            pub_tx = Some(tx);
        }

        let mut links = vec![];
        let mut by_id = HashMap::new();
        for p in cfg.peers.iter().filter(|p| p.id != cfg.me) {
            let (tx, rx) = mpsc::channel::<ZmqMessage>(cfg.capacity);
            let link = Arc::new(Link { id: p.id.clone(), tx, connected: AtomicBool::new(false), seen_ms: AtomicU64::new(0) });
            by_id.insert(p.id.clone(), links.len());
            links.push(link.clone());
            spawn_dealer(&cfg.me, p, link.clone(), rx, in_tx.clone(), start)?;
            spawn_sub(p, link, prefixes.subscribe(), in_tx.clone(), start);
        }
        let z = Arc::new(Zmq { me: cfg.me, links, by_id, router_tx, pub_tx, incoming: in_tx, prefixes, rr: AtomicUsize::new(0), start });
        Ok((z, in_rx))
    }

    fn now_ms(&self) -> u64 {
        self.start.elapsed().as_millis() as u64
    }

    pub(crate) async fn publish(&self, topic: String, payload: Bytes) -> Result<()> {
        if let Some(id) = inbox_of(&topic).map(str::to_owned) {
            let id = id.as_str();
            if id == self.me {
                return self.incoming.send(Incoming { topic, payload }).await.map_err(|_| Error::Fabric("closed".into()));
            }
            if let Some(&i) = self.by_id.get(id) {
                return self.links[i]
                    .tx
                    .try_send(msg(vec![Bytes::from(topic), payload]))
                    .map_err(|e| Error::Fabric(format!("ZeroMQ: to {id}: {e}")));
            }
            return match &self.router_tx {
                Some(tx) => {
                    let id = Bytes::copy_from_slice(id.as_bytes());
                    tx.try_send(msg(vec![id, Bytes::from(topic), payload])).map_err(|e| Error::Fabric(format!("ZeroMQ: reply: {e}")))
                }
                None => Err(Error::Fabric(format!("ZeroMQ: no route to {id}"))),
            };
        }
        // a shared topic: one dispatcher, round robin over the live ones
        let now = self.now_ms();
        let live = |l: &Link| l.connected.load(Ordering::Relaxed) && now.saturating_sub(l.seen_ms.load(Ordering::Relaxed)) < SILENT.as_millis() as u64;
        let n = self.links.len();
        let first = self.rr.fetch_add(1, Ordering::Relaxed);
        for k in 0..n {
            let l = &self.links[(first + k) % n];
            if live(l) && l.tx.try_send(msg(vec![Bytes::from(topic.clone()), payload.clone()])).is_ok() {
                return Ok(());
            }
        }
        Err(Error::Fabric("ZeroMQ: no dispatcher reachable".into()))
    }

    pub(crate) async fn publish_retained(&self, topic: String, payload: Bytes) -> Result<()> {
        match &self.pub_tx {
            Some(tx) => tx.try_send(msg(vec![Bytes::from(topic), payload])).map_err(|e| Error::Fabric(format!("ZeroMQ: announce: {e}"))),
            None => Err(Error::Fabric("ZeroMQ: only a dispatcher announces".into())),
        }
    }

    pub(crate) fn subscribe(&self, filter: &str) {
        // inbox and shared filters: those messages come to the DEALER and ROUTER anyway
        if filter.starts_with("$share/") || inbox_of(filter).is_some() {
            return;
        }
        let prefix = filter.split(['+', '#']).next().unwrap_or("").to_owned();
        self.prefixes.send_if_modified(|v| {
            if v.contains(&prefix) {
                false
            } else {
                v.push(prefix);
                true
            }
        });
    }

    /// Ids of the dispatchers currently connected (for tests and diagnostics).
    pub(crate) fn connected(&self) -> Vec<String> {
        self.links.iter().filter(|l| l.connected.load(Ordering::Relaxed)).map(|l| l.id.clone()).collect()
    }
}

fn spawn_dealer(
    me: &str,
    p: &ZmqPeer,
    link: Arc<Link>,
    mut out: mpsc::Receiver<ZmqMessage>,
    in_tx: mpsc::Sender<Incoming>,
    start: Instant,
) -> Result<()> {
    let mut opts = SocketOptions::default();
    opts.peer_identity(identity(me)?);
    opts.no_connect_timeout();
    let endpoint = format!("tcp://{}:{}", p.host, p.port);
    tokio::spawn(async move {
        let mut s = DealerSocket::with_options(opts);
        let mut events = s.monitor();
        if let Err(e) = s.connect(&endpoint).await {
            warn!(peer = %link.id, error = %e, "ZeroMQ: connect failed");
            return;
        }
        link.seen_ms.store(start.elapsed().as_millis() as u64, Ordering::Relaxed);
        link.connected.store(true, Ordering::Relaxed);
        info!(peer = %link.id, %endpoint, "fabric (ZeroMQ) connected");
        let l = link.clone();
        tokio::spawn(async move {
            while let Ok(ev) = events.recv().await {
                match ev {
                    SocketEvent::Connected(..) => {
                        l.seen_ms.store(start.elapsed().as_millis() as u64, Ordering::Relaxed);
                        l.connected.store(true, Ordering::Relaxed);
                        info!(peer = %l.id, "fabric (ZeroMQ) reconnected");
                    }
                    SocketEvent::Disconnected(_) => {
                        l.connected.store(false, Ordering::Relaxed);
                        warn!(peer = %l.id, "fabric (ZeroMQ) disconnected");
                    }
                    _ => {}
                }
            }
        });
        let (mut send, mut recv) = s.split();
        tokio::spawn(async move {
            loop {
                match recv.recv().await {
                    Ok(m) => {
                        if let Some(i) = incoming(m)
                            && in_tx.send(i).await.is_err()
                        {
                            return;
                        }
                    }
                    Err(e) => {
                        debug!(error = %e, "ZeroMQ: DEALER receive failed");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                }
            }
        });
        while let Some(m) = out.recv().await {
            if let Err(e) = send.send(m).await {
                debug!(peer = %link.id, error = %e, "ZeroMQ: message dropped");
            }
        }
    });
    Ok(())
}

fn spawn_sub(p: &ZmqPeer, link: Arc<Link>, mut prefixes: watch::Receiver<Vec<String>>, in_tx: mpsc::Sender<Incoming>, start: Instant) {
    let mut opts = SocketOptions::default();
    opts.no_connect_timeout();
    let endpoint = format!("tcp://{}:{}", p.host, p.port + 1);
    tokio::spawn(async move {
        let mut s = SubSocket::with_options(opts);
        if let Err(e) = s.connect(&endpoint).await {
            warn!(peer = %link.id, error = %e, "ZeroMQ: SUB connect failed");
            return;
        }
        let mut done: Vec<String> = vec![];
        loop {
            let wanted = prefixes.borrow_and_update().clone();
            for pre in wanted {
                if !done.contains(&pre) {
                    if let Err(e) = s.subscribe(&pre).await {
                        warn!(prefix = %pre, error = %e, "ZeroMQ: subscribe failed");
                    }
                    done.push(pre);
                }
            }
            tokio::select! {
                r = s.recv() => match r {
                    Ok(m) => {
                        if let Some(i) = incoming(m) {
                            if member_of(&i.topic) == Some(link.id.as_str()) {
                                link.seen_ms.store(start.elapsed().as_millis() as u64, Ordering::Relaxed);
                            }
                            if in_tx.send(i).await.is_err() {
                                return;
                            }
                        }
                    }
                    Err(e) => {
                        debug!(error = %e, "ZeroMQ: SUB receive failed");
                        tokio::time::sleep(Duration::from_millis(100)).await;
                    }
                },
                c = prefixes.changed() => if c.is_err() { return; },
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn peers_parse() {
        let p = ZmqPeer::parse_list("d1@127.0.0.1:7101, d2@host.lan:7103").unwrap();
        assert_eq!(p[1], ZmqPeer { id: "d2".into(), host: "host.lan".into(), port: 7103 });
        assert!(ZmqPeer::parse_list("d1@127.0.0.1").is_err());
        assert!(ZmqPeer::parse_list("127.0.0.1:7101").is_err());
    }

    #[test]
    fn topics_route_by_kind() {
        assert_eq!(inbox_of("octopus/inbox/eb-1-p0"), Some("eb-1-p0"));
        assert_eq!(inbox_of("octopus/pub/g1"), None);
        assert_eq!(member_of("octopus/member/d2"), Some("d2"));
    }
}
