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

pub use v1::{Delivery, PublishItem, PublishStatus, Skipped};

/// What Octopus answered for one published item.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Ack {
    pub status: PublishStatus,
    /// Positions directly below this one (same epoch) that are neither stored nor applied. If the producer still
    /// has them in flight, nothing to do; if it lost them, it replays them or publishes a skip marker.
    pub missing_below: i64,
}

fn envelope(body: Body) -> Bytes {
    Bytes::from(v1::Envelope { body: Some(body) }.encode_to_vec())
}

/// The envelopes inside an envelope (an EnvelopeBatch, possibly nested), or the envelope itself.
fn flatten(env: v1::Envelope) -> Vec<v1::Envelope> {
    match env.body {
        Some(Body::Batch(b)) => b.items.into_iter().flat_map(flatten).collect(),
        _ => vec![env],
    }
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
    /// 0 (the default): retry until acknowledged. Giving up leaves a gap in the source's sequence, which holds
    /// back every later event of the source, so only tests should set a limit.
    pub max_attempts: u32,
    /// A publish not acknowledged after this long is reported (a warning and [`Producer::stuck`]).
    pub alert_after: Duration,
    stuck: AtomicU64,
    /// positions published by this producer and not acknowledged yet: (source, epoch, seq)
    in_flight: Mutex<std::collections::HashSet<(String, i64, i64)>>,
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
            max_attempts: 0,
            alert_after: Duration::from_secs(30),
            stuck: AtomicU64::new(0),
            in_flight: Mutex::default(),
        });
        let pending = p.pending.clone();
        tokio::spawn(async move {
            while let Some(m) = rx.recv().await {
                let Ok(env) = v1::Envelope::decode(m.payload) else { continue };
                for e in flatten(env) {
                    if let Some(Body::PublishReply(r)) = e.body {
                        if let Some(tx) = pending.lock().unwrap().remove(&r.request_id) {
                            let _ = tx.send(r);
                        }
                    }
                }
            }
        });
        Ok(p)
    }

    /// Publishes and returns one answer per item, once the events are committed. Retries until acknowledged.
    pub async fn publish(&self, items: Vec<PublishItem>) -> Result<Vec<Ack>> {
        let keys: Vec<(String, i64, i64)> =
            items.iter().filter(|i| !i.high_water).map(|i| (i.source.clone(), i.epoch, i.seq)).collect();
        self.in_flight.lock().unwrap().extend(keys.iter().cloned());
        let r = self.publish_once(items).await;
        let mut f = self.in_flight.lock().unwrap();
        for k in &keys {
            f.remove(k);
        }
        r
    }

    /// The source's last position is (epoch, seq): returns how many positions directly up to and including it are
    /// neither stored nor applied (a lost last event shows up here; no later publish would reveal it).
    pub async fn announce(&self, group: &str, source: &str, epoch: i64, seq: i64) -> Result<i64> {
        let item = PublishItem { group: group.into(), source: source.into(), epoch, seq, high_water: true, ..Default::default() };
        Ok(self.publish(vec![item]).await?.first().map(|a| a.missing_below).unwrap_or(0))
    }

    /// Repairs positions `from_seq ..= to_seq` of an epoch that Octopus reported missing (an acknowledgement's
    /// `missing_below`, or [`Producer::announce`]): positions this producer still has in flight are left alone (they
    /// arrive on their own); `replay` supplies the body of each other one it can (e.g. from an outbox), and every
    /// run of positions it cannot is declared lost with one skip marker. Returns (replayed, skipped).
    pub async fn repair(
        &self, group: &str, source: &str, epoch: i64, from_seq: i64, to_seq: i64, replay: impl Fn(i64) -> Option<Bytes>,
    ) -> Result<(u64, u64)> {
        let mut items = vec![];
        let (mut replayed, mut skipped) = (0u64, 0u64);
        let mut run: Option<(i64, i64)> = None; // a run of positions to declare lost
        let flush = |run: &mut Option<(i64, i64)>, items: &mut Vec<PublishItem>, skipped: &mut u64| {
            if let Some((a, b)) = run.take() {
                *skipped += (b - a + 1) as u64;
                items.push(PublishItem { group: group.into(), source: source.into(), epoch, seq: a, skip_to: Some(b), ..Default::default() });
            }
        };
        for q in from_seq..=to_seq {
            if self.in_flight.lock().unwrap().contains(&(source.to_owned(), epoch, q)) {
                flush(&mut run, &mut items, &mut skipped);
                continue;
            }
            match replay(q) {
                Some(body) => {
                    flush(&mut run, &mut items, &mut skipped);
                    replayed += 1;
                    items.push(PublishItem { group: group.into(), source: source.into(), epoch, seq: q, body, ..Default::default() });
                }
                None => run = Some((run.map_or(q, |r| r.0), q)),
            }
        }
        flush(&mut run, &mut items, &mut skipped);
        if !items.is_empty() {
            self.publish(items).await?;
        }
        Ok((replayed, skipped))
    }

    async fn publish_once(&self, items: Vec<PublishItem>) -> Result<Vec<Ack>> {
        let Some(first) = items.first() else { return Ok(vec![]) };
        let topic = self.topics.publish(&first.group);
        let mut req = v1::PublishRequest { reply_to: self.inbox.clone(), request_id: 0, items };
        let mut backoff = Duration::from_millis(50);
        let started = Instant::now();
        let mut alerted = false;
        let mut attempt: u32 = 0;
        // leaves the stuck count right however this call ends
        struct Unstick<'a>(&'a AtomicU64, bool);
        impl Drop for Unstick<'_> {
            fn drop(&mut self) {
                if self.1 {
                    self.0.fetch_sub(1, Relaxed);
                }
            }
        }
        let mut unstick = Unstick(&self.stuck, false);
        while self.max_attempts == 0 || attempt < self.max_attempts {
            attempt += 1;
            if !alerted && started.elapsed() >= self.alert_after {
                alerted = true;
                unstick.1 = true;
                self.stuck.fetch_add(1, Relaxed);
                let keys: Vec<String> = req.items.iter().take(5).map(|i| format!("{}#{}", i.source, i.seq)).collect();
                warn!(events = req.items.len(), first = ?keys, waited_s = started.elapsed().as_secs(), "publish not acknowledged yet; still retrying");
            }
            req.request_id = self.next_id.fetch_add(1, Relaxed);
            let (tx, rx) = oneshot::channel();
            self.pending.lock().unwrap().insert(req.request_id, tx);
            let batch = v1::PublishBatch { requests: vec![req.clone()] };
            if let Err(e) = self.fabric.publish(topic.clone(), Bytes::from(batch.encode_to_vec())).await {
                warn!(error = %e, "publish send failed");
            }
            match tokio::time::timeout(self.reply_timeout, rx).await {
                Ok(Ok(r)) if r.error.is_empty() => {
                    return Ok(r
                        .status
                        .iter()
                        .enumerate()
                        .map(|(i, s)| Ack {
                            status: PublishStatus::try_from(*s).unwrap_or(PublishStatus::Unspecified),
                            missing_below: r.missing_below.get(i).copied().unwrap_or(0),
                        })
                        .collect());
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

    /// Publishes currently retrying for longer than `alert_after` (a gauge for alerting).
    pub fn stuck(&self) -> u64 {
        self.stuck.load(Relaxed)
    }
}

// =========================================================================================================
// Worker

/// What the handler decided.
pub enum Outcome {
    /// The handler committed its own writes (ideally calling `octopus.complete(source, seq)` in that transaction):
    /// the dispatcher removes the event so it is never handed out again.
    Done,
    /// Done: the effect is passed to the apply hook in the commit transaction (hook mode).
    Commit(Bytes),
    /// Failed: offer the event again after this delay.
    Retry(Duration),
    /// Send nothing, as if the worker crashed now (tests: the delivery timeout offers the event again).
    #[doc(hidden)]
    Vanish,
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
    source: String,
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
    /// delivery id -> the event being processed (by delivery: two deliveries of one source are two entries)
    running: HashMap<u64, Running>,
    next_delivery: u64,
    rr: usize,
}

impl WState {
    /// Owners of our groups that are alive, with their inboxes (sorted for a stable round robin).
    /// The invariant: events held plus credits granted never exceed the slots. Takes back any excess (largest
    /// grants first); returns (inbox, new credit) for each owner told a lower amount.
    fn clamp(&mut self, slots: u32) -> Vec<(String, u32)> {
        let mut excess = (self.running.len() as u32 + self.granted.values().sum::<u32>()).saturating_sub(slots);
        let mut changed: HashMap<String, u32> = HashMap::new();
        while excess > 0 {
            let Some((m, n)) = self.granted.iter_mut().filter(|(_, n)| **n > 0).max_by_key(|(_, n)| **n) else { break };
            let take = (*n).min(excess);
            *n -= take;
            excess -= take;
            changed.insert(m.clone(), *n);
        }
        changed.into_iter().filter_map(|(m, n)| self.members.get(&m).map(|i| (i.clone(), n))).collect()
    }

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
            let Ok(env) = v1::Envelope::decode(m.payload) else { return };
            for e in flatten(env) {
                self.clone().on_envelope(e);
            }
        }
    }

    fn on_envelope(self: Arc<Self>, e: v1::Envelope) {
        {
            match e {
                v1::Envelope { body: Some(Body::Deliver(d)) } => self.on_deliver(d),
                v1::Envelope { body: Some(Body::Demand(d)) } => {
                    tokio::spawn(async move { self.on_demand(d).await });
                }
                v1::Envelope { body: Some(Body::ResultReply(r)) } => {
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
        let ids: Vec<u64> = {
            let mut st = self.state.lock().unwrap();
            let g = st.granted.entry(d.member_id.clone()).or_default();
            *g = g.saturating_sub(d.items.len() as u32);
            d.items
                .iter()
                .map(|it| {
                    st.next_delivery += 1;
                    let id = st.next_delivery;
                    st.running.insert(
                        id,
                        Running { source: it.source.clone(), member: d.member_id.clone(), member_inbox: d.inbox.clone(), started: Instant::now() },
                    );
                    id
                })
                .collect()
        };
        for (item, id) in d.items.into_iter().zip(ids) {
            self.stats.received.fetch_add(1, Relaxed);
            let me = self.clone();
            let (member, member_inbox) = (d.member_id.clone(), d.inbox.clone());
            tokio::spawn(async move {
                let (group, source, epoch, seq) = (item.group.clone(), item.source.clone(), item.epoch, item.seq);
                let outcome = me.handler.handle(item).await;
                let mut r =
                    v1::WorkResult { request_id: me.next_req.fetch_add(1, Relaxed), group, source: source.clone(), epoch, seq, ..Default::default() };
                match outcome {
                    Outcome::Vanish => {
                        me.state.lock().unwrap().running.remove(&id);
                        return;
                    }
                    Outcome::Done => {
                        me.stats.committed.fetch_add(1, Relaxed);
                        r.ok = true;
                        r.done = true;
                    }
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
                // the delivering dispatcher if it is alive, otherwise any dispatcher; the result also returns this
                // slot's credit to it (counted as granted from now on)
                let (alive, corrections) = {
                    let mut st = me.state.lock().unwrap();
                    let alive = st.members.contains_key(&member);
                    if alive && st.owners.values().any(|m| *m == member) {
                        *st.granted.entry(member.clone()).or_default() += 1;
                    }
                    // the handler is finished: its slot is free (and its credit is on its way back)
                    st.running.remove(&id);
                    // the implicit return assumes the delivery came out of a credit: correct any excess now
                    (alive, st.clamp(me.cfg.slots))
                };
                let topic = if alive { member_inbox } else { me.topics.any() };
                r.worker_id = me.cfg.id.clone();
                let batch = v1::ResultBatch { worker_id: me.cfg.id.clone(), reply_to: me.inbox.clone(), items: vec![r] };
                if let Err(e) = me.fabric.publish(topic, envelope(Body::Results(batch))).await {
                    warn!(error = %e, "result send failed (the delivery timeout will re-offer the event)");
                }
                for (inbox, n) in corrections {
                    me.send_credit(&inbox, n, true).await;
                }
                me.state.lock().unwrap().running.remove(&id);
                me.rebalance().await;
            });
        }
    }

    /// A dispatcher has ready events but none of our credit: give it our free slots, or move credit from owners that
    /// hold it idle (they are told the reduced amount; one they already spent just means a brief overcommit).
    async fn on_demand(&self, d: v1::Demand) {
        if !self.cfg.groups.contains(&d.group) {
            return;
        }
        let mut sends: Vec<(String, u32, bool)> = vec![];
        {
            let mut st = self.state.lock().unwrap();
            if !st.members.contains_key(&d.member_id) {
                return;
            }
            let outstanding: u32 = st.granted.values().sum();
            let busy = st.running.len() as u32;
            let mut want = d.ready.max(1);
            let mut give = self.cfg.slots.saturating_sub(outstanding + busy).min(want);
            want -= give;
            let others: Vec<String> = st.granted.iter().filter(|(m, n)| **m != d.member_id && **n > 0).map(|(m, _)| m.clone()).collect();
            for o in others {
                if want == 0 {
                    break;
                }
                let n = st.granted.get_mut(&o).unwrap();
                let take = (*n).min(want);
                *n -= take;
                let left = *n;
                want -= take;
                give += take;
                if let Some(inbox) = st.members.get(&o) {
                    sends.push((inbox.clone(), left, true));
                }
            }
            if give > 0 {
                *st.granted.entry(d.member_id.clone()).or_default() += give;
                sends.push((d.inbox.clone(), give, false));
            }
        }
        for (inbox, n, reset) in sends {
            self.send_credit(&inbox, n, reset).await;
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
            let mut st = self.state.lock().unwrap();
            st.clamp(self.cfg.slots);
            let resync: Vec<(String, u32)> =
                st.owner_inboxes().into_iter().map(|(m, inbox)| (inbox, st.granted.get(&m).copied().unwrap_or(0))).collect();
            let mut progress: HashMap<String, Vec<String>> = HashMap::new();
            for r in st.running.values() {
                if r.started.elapsed() >= self.cfg.progress_after && st.members.contains_key(&r.member) {
                    progress.entry(r.member_inbox.clone()).or_default().push(r.source.clone());
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
        let c = v1::WorkerCredit {
            worker_id: self.cfg.id.clone(),
            inbox: self.inbox.clone(),
            groups: self.cfg.groups.clone(),
            credit,
            reset,
            slots: self.cfg.slots,
        };
        if let Err(e) = self.fabric.publish(inbox.to_owned(), envelope(Body::Credit(c))).await {
            warn!(error = %e, "credit send failed");
        }
    }
}
