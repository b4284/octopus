//! Octopus client: [`Producer`] publishes events, [`Worker`] processes them.
//!
//! Both speak the wire protocol in `octopus-proto` over the fabric. Every call may be retried: publishes are
//! idempotent per `(source, seq)`, and a repeated delivery is turned into "already applied" by the database.

use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use octopus_fabric::{Fabric, Incoming, MqttConfig};
use octopus_proto::v1::{self, envelope::Body};
use octopus_proto::{Message, Topics};
use tokio::sync::{mpsc, oneshot, watch};
use tracing::{debug, warn};

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("publish rejected: {0}")]
    Rejected(String),
    #[error("no reply after {0} attempts")]
    Timeout(u32),
    #[error("fabric: {0}")]
    Fabric(String),
}

pub type Result<T> = std::result::Result<T, Error>;

pub use v1::{Delivery, PublishItem, PublishStatus};

fn envelope(body: Body) -> Bytes {
    Bytes::from(v1::Envelope { body: Some(body) }.encode_to_vec())
}

// =========================================================================================================
// Producer

pub struct Producer {
    fabric: Fabric,
    topics: Topics,
    inbox: String,
    pending: Arc<Mutex<HashMap<u64, oneshot::Sender<v1::PublishReply>>>>,
    next_id: AtomicU64,
    /// Wait this long for a reply before sending again.
    pub reply_timeout: Duration,
    pub max_attempts: u32,
}

impl Producer {
    /// `id` must be unique among producers (it names the reply inbox).
    pub async fn connect(mqtt: MqttConfig, ns: &str, id: &str) -> Result<Arc<Self>> {
        let topics = Topics::new(ns);
        let inbox = topics.inbox(id);
        let (fabric, mut rx) = Fabric::connect_mqtt(mqtt);
        fabric.subscribe(inbox.clone()).await.map_err(|e| Error::Fabric(e.to_string()))?;
        let p = Arc::new(Self {
            fabric,
            topics,
            inbox,
            pending: Arc::default(),
            next_id: AtomicU64::new(1),
            reply_timeout: Duration::from_secs(5),
            max_attempts: 20,
        });
        let pending = p.pending.clone();
        tokio::spawn(async move {
            while let Some(m) = rx.recv().await {
                if let Ok(v1::Envelope { body: Some(Body::PublishReply(r)) }) = v1::Envelope::decode(m.payload) {
                    if let Some(tx) = pending.lock().unwrap().remove(&r.request_id) {
                        let _ = tx.send(r);
                    }
                }
            }
        });
        Ok(p)
    }

    /// Publishes and returns one status per item, once the events are committed. Retries until acknowledged.
    pub async fn publish(&self, items: Vec<PublishItem>) -> Result<Vec<PublishStatus>> {
        let Some(first) = items.first() else { return Ok(vec![]) };
        let topic = self.topics.publish(&first.group);
        let mut req = v1::PublishRequest { reply_to: self.inbox.clone(), request_id: 0, items };
        let mut backoff = Duration::from_millis(50);
        for attempt in 1..=self.max_attempts {
            req.request_id = self.next_id.fetch_add(1, Relaxed);
            let (tx, rx) = oneshot::channel();
            self.pending.lock().unwrap().insert(req.request_id, tx);
            if let Err(e) = self.fabric.publish(topic.clone(), Bytes::from(req.encode_to_vec())).await {
                warn!(error = %e, "publish send failed");
            }
            match tokio::time::timeout(self.reply_timeout, rx).await {
                Ok(Ok(r)) if r.error.is_empty() => {
                    return Ok(r.status.into_iter().map(|s| PublishStatus::try_from(s).unwrap_or(PublishStatus::Unspecified)).collect());
                }
                Ok(Ok(r)) if r.error.starts_with("invalid") => return Err(Error::Rejected(r.error)),
                Ok(Ok(r)) => debug!(attempt, error = %r.error, "publish failed; retrying"),
                _ => {
                    self.pending.lock().unwrap().remove(&req.request_id);
                    debug!(attempt, "no publish reply; retrying");
                }
            }
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(Duration::from_secs(2));
        }
        Err(Error::Timeout(self.max_attempts))
    }
}

// =========================================================================================================
// Worker

/// What the handler decided.
pub enum Outcome {
    /// Done: the effect is passed to the apply hook in the commit transaction.
    Commit(Bytes),
    /// Failed: offer the event again after this delay.
    Retry(Duration),
}

pub trait Handler: Send + Sync + 'static {
    /// `(delivery.source, delivery.seq)` is the idempotency key for any external call: a delivery can repeat.
    fn handle(&self, delivery: Delivery) -> impl Future<Output = Outcome> + Send;
}

#[derive(Clone, Debug)]
pub struct WorkerConfig {
    pub id: String,
    pub ns: String,
    pub groups: Vec<String>,
    /// Events processed at the same time.
    pub slots: u32,
    /// Resend the outstanding credit to every owner this often (keeps the worker known and in sync).
    pub refresh: Duration,
    /// Report events running longer than this as still in progress.
    pub progress_after: Duration,
}

impl WorkerConfig {
    pub fn new(id: impl Into<String>, ns: impl Into<String>, groups: Vec<String>, slots: u32) -> Self {
        Self { id: id.into(), ns: ns.into(), groups, slots, refresh: Duration::from_secs(1), progress_after: Duration::from_secs(5) }
    }
}

#[derive(Default)]
pub struct WorkerStats {
    pub received: AtomicU64,
    pub committed: AtomicU64,
    pub retried: AtomicU64,
    pub applied: AtomicU64,
    pub duplicates: AtomicU64,
}

struct Running {
    member: String,
    member_inbox: String,
    started: Instant,
}

#[derive(Default)]
struct WState {
    /// group -> owner member
    owners: HashMap<String, String>,
    /// member -> inbox, for live members
    members: HashMap<String, String>,
    /// member -> credits granted and not yet used
    granted: HashMap<String, u32>,
    /// source -> the event being processed
    running: HashMap<String, Running>,
    rr: usize,
}

impl WState {
    /// Owners of our groups that are alive, with their inboxes (sorted for a stable round robin).
    fn owner_inboxes(&self) -> Vec<(String, String)> {
        let mut v: Vec<(String, String)> = self
            .owners
            .values()
            .filter_map(|m| self.members.get(m).map(|i| (m.clone(), i.clone())))
            .collect();
        v.sort();
        v.dedup();
        v
    }
}

pub struct Worker<H: Handler> {
    cfg: WorkerConfig,
    fabric: Fabric,
    topics: Topics,
    inbox: String,
    handler: Arc<H>,
    state: Mutex<WState>,
    next_req: AtomicU64,
    pub stats: WorkerStats,
}

impl<H: Handler> Worker<H> {
    pub async fn connect(mqtt: MqttConfig, cfg: WorkerConfig, handler: H) -> Result<(Arc<Self>, mpsc::Receiver<Incoming>)> {
        let topics = Topics::new(cfg.ns.clone());
        let inbox = topics.inbox(&cfg.id);
        let (fabric, rx) = Fabric::connect_mqtt(mqtt);
        let w = Arc::new(Self {
            fabric,
            inbox,
            handler: Arc::new(handler),
            state: Mutex::default(),
            next_req: AtomicU64::new(1),
            stats: WorkerStats::default(),
            topics,
            cfg,
        });
        for f in [w.inbox.clone(), w.topics.members_filter(), w.topics.owners_filter()] {
            w.fabric.subscribe(f).await.map_err(|e| Error::Fabric(e.to_string()))?;
        }
        Ok((w, rx))
    }

    pub async fn run(self: Arc<Self>, mut rx: mpsc::Receiver<Incoming>, mut stop: watch::Receiver<bool>) {
        let mut tick = tokio::time::interval(self.cfg.refresh);
        loop {
            tokio::select! {
                m = rx.recv() => match m { Some(m) => self.clone().on_message(m).await, None => return },
                _ = tick.tick() => self.refresh().await,
                _ = stop.changed() => if *stop.borrow() { break },
            }
        }
        // stop taking work: withdraw every credit
        let owners = self.state.lock().unwrap().owner_inboxes();
        for (_, inbox) in owners {
            self.send_credit(&inbox, 0, true).await;
        }
    }

    async fn on_message(self: Arc<Self>, m: Incoming) {
        let rest = m.topic.strip_prefix(&format!("{}/", self.cfg.ns)).unwrap_or("").to_owned();
        if let Some(member) = rest.strip_prefix("member/") {
            {
                let mut st = self.state.lock().unwrap();
                match v1::MemberInfo::decode(m.payload) {
                    Ok(i) if !i.inbox.is_empty() => {
                        st.members.insert(member.to_owned(), i.inbox);
                    }
                    _ => {
                        // gone: its credits are void
                        st.members.remove(member);
                        st.granted.remove(member);
                    }
                }
            }
            self.rebalance().await;
        } else if let Some(group) = rest.strip_prefix("owner/") {
            if !self.cfg.groups.iter().any(|g| g == group) {
                return;
            }
            {
                let mut st = self.state.lock().unwrap();
                match v1::OwnerInfo::decode(m.payload) {
                    Ok(o) if !o.member_id.is_empty() => {
                        st.owners.insert(group.to_owned(), o.member_id);
                    }
                    _ => {
                        st.owners.remove(group);
                    }
                }
                // credits held by members that no longer own any of our groups are void
                let owners: Vec<String> = st.owners.values().cloned().collect();
                st.granted.retain(|m, _| owners.contains(m));
            }
            self.rebalance().await;
        } else if m.topic == self.inbox {
            match v1::Envelope::decode(m.payload) {
                Ok(v1::Envelope { body: Some(Body::Deliver(d)) }) => self.on_deliver(d),
                Ok(v1::Envelope { body: Some(Body::ResultReply(r)) }) => {
                    for a in r.items {
                        match v1::ResultStatus::try_from(a.status) {
                            Ok(v1::ResultStatus::ResultApplied) => self.stats.applied.fetch_add(1, Relaxed),
                            Ok(v1::ResultStatus::ResultDuplicate) => self.stats.duplicates.fetch_add(1, Relaxed),
                            _ => 0,
                        };
                    }
                }
                _ => {}
            }
        }
    }

    fn on_deliver(self: Arc<Self>, d: v1::Deliver) {
        {
            let mut st = self.state.lock().unwrap();
            let g = st.granted.entry(d.member_id.clone()).or_default();
            *g = g.saturating_sub(d.items.len() as u32);
            for it in &d.items {
                st.running.insert(
                    it.source.clone(),
                    Running { member: d.member_id.clone(), member_inbox: d.inbox.clone(), started: Instant::now() },
                );
            }
        }
        for item in d.items {
            self.stats.received.fetch_add(1, Relaxed);
            let me = self.clone();
            let (member, member_inbox) = (d.member_id.clone(), d.inbox.clone());
            tokio::spawn(async move {
                let (group, source, seq) = (item.group.clone(), item.source.clone(), item.seq);
                let outcome = me.handler.handle(item).await;
                let mut r = v1::WorkResult { request_id: me.next_req.fetch_add(1, Relaxed), group, source: source.clone(), seq, ..Default::default() };
                match outcome {
                    Outcome::Commit(effect) => {
                        me.stats.committed.fetch_add(1, Relaxed);
                        r.ok = true;
                        r.effect = effect;
                    }
                    Outcome::Retry(delay) => {
                        me.stats.retried.fetch_add(1, Relaxed);
                        r.retry_delay_ms = delay.as_millis().min(u32::MAX as u128) as u32;
                    }
                }
                // the delivering dispatcher if it is alive, otherwise any dispatcher
                let alive = me.state.lock().unwrap().members.contains_key(&member);
                let topic = if alive { member_inbox } else { me.topics.any() };
                let batch = v1::ResultBatch { worker_id: me.cfg.id.clone(), reply_to: me.inbox.clone(), items: vec![r] };
                if let Err(e) = me.fabric.publish(topic, envelope(Body::Results(batch))).await {
                    warn!(error = %e, "result send failed (the delivery timeout will re-offer the event)");
                }
                me.state.lock().unwrap().running.remove(&source);
                me.rebalance().await;
            });
        }
    }

    /// Hand free slots out as credits, round robin over the live owners of our groups.
    async fn rebalance(&self) {
        let grants: Vec<(String, u32)> = {
            let mut st = self.state.lock().unwrap();
            let owners = st.owner_inboxes();
            if owners.is_empty() {
                return;
            }
            let outstanding: u32 = st.granted.values().sum();
            let busy = st.running.len() as u32;
            let mut free = self.cfg.slots.saturating_sub(outstanding + busy);
            let mut grants: HashMap<String, u32> = HashMap::new();
            while free > 0 {
                let i = st.rr % owners.len();
                st.rr = st.rr.wrapping_add(1);
                let (m, inbox) = &owners[i];
                *st.granted.entry(m.clone()).or_default() += 1;
                *grants.entry(inbox.clone()).or_default() += 1;
                free -= 1;
            }
            grants.into_iter().collect()
        };
        for (inbox, n) in grants {
            self.send_credit(&inbox, n, false).await;
        }
    }

    /// Periodic: resend each owner the exact outstanding credit (resync after losses), report long-running events.
    async fn refresh(&self) {
        let (resync, progress) = {
            let st = self.state.lock().unwrap();
            let resync: Vec<(String, u32)> =
                st.owner_inboxes().into_iter().map(|(m, inbox)| (inbox, st.granted.get(&m).copied().unwrap_or(0))).collect();
            let mut progress: HashMap<String, Vec<String>> = HashMap::new();
            for (s, r) in &st.running {
                if r.started.elapsed() >= self.cfg.progress_after && st.members.contains_key(&r.member) {
                    progress.entry(r.member_inbox.clone()).or_default().push(s.clone());
                }
            }
            (resync, progress)
        };
        for (inbox, n) in resync {
            self.send_credit(&inbox, n, true).await;
        }
        for (inbox, sources) in progress {
            let p = v1::Progress { worker_id: self.cfg.id.clone(), sources };
            let _ = self.fabric.publish(inbox, envelope(Body::Progress(p))).await;
        }
        self.rebalance().await;
    }

    async fn send_credit(&self, inbox: &str, credit: u32, reset: bool) {
        let c = v1::WorkerCredit { worker_id: self.cfg.id.clone(), inbox: self.inbox.clone(), groups: self.cfg.groups.clone(), credit, reset };
        if let Err(e) = self.fabric.publish(inbox.to_owned(), envelope(Body::Credit(c))).await {
            warn!(error = %e, "credit send failed");
        }
    }
}
