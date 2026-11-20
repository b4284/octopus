//! The Octopus dispatcher.
//!
//! Every instance does everything: accepts publishes, runs commits, and serves the groups it holds a lease on.
//! There is no leader; group ownership is split by rendezvous hashing over the live member list, guarded by
//! database leases. Nothing in this process is needed for correctness: the database checks every commit against
//! the source's cursor in the same transaction that applies it. Losing this process (or its memory) can only
//! cause duplicate deliveries, which the database turns into "already applied".

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering::Relaxed};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use octopus_core::{Backend, Commit, CommitOutcome, Event, EventKey, Head, HeadRequest, Member, PublishOutcome};
use octopus_fabric::{Fabric, Incoming};
use octopus_proto::v1::{self, envelope::Body};
use octopus_proto::{Message, Topics, valid_name};
use tokio::sync::{Notify, Semaphore, mpsc, watch};
use tracing::{debug, info, warn};

#[derive(Clone, Debug)]
pub struct Config {
    pub member_id: String,
    pub ns: String,
    /// A member is dead, and its leases free, this long after its last heartbeat or renewal.
    pub lease_ttl: Duration,
    /// Heartbeat, renewal and rebalance period.
    pub tick: Duration,
    /// On a planned hand-over, wait up to this long for a group's handed-out events to finish.
    pub drain: Duration,
    /// A handed-out event with no result in this time is offered again (lost message, dead worker).
    pub delivery_timeout: Duration,
    /// Re-read heads of busy groups at least this often, even without a hint.
    pub fallback_poll: Duration,
    /// Forget a worker (and its credits) that has been silent this long.
    pub worker_ttl: Duration,
    pub publish_executors: usize,
    pub publish_max_events: usize,
    pub commit_executors: usize,
    pub commit_max: usize,
    pub fetch_parallelism: usize,
    pub fetch_max_groups: usize,
    pub purge_every: Duration,
    pub stats_every: Duration,
}

impl Config {
    pub fn new(member_id: impl Into<String>) -> Self {
        Self {
            member_id: member_id.into(),
            ns: "octopus".into(),
            lease_ttl: Duration::from_secs(3),
            tick: Duration::from_millis(500),
            drain: Duration::from_secs(5),
            delivery_timeout: Duration::from_secs(30),
            fallback_poll: Duration::from_secs(1),
            worker_ttl: Duration::from_secs(10),
            publish_executors: 4,
            publish_max_events: 2000,
            commit_executors: 8,
            commit_max: 128,
            fetch_parallelism: 4,
            fetch_max_groups: 64,
            purge_every: Duration::from_secs(30),
            stats_every: Duration::from_secs(5),
        }
    }
}

// ---------------------------------------------------------------------------------------------------------
// In-memory state (a cache of the database plus who holds what; losing it is harmless)

struct ReadyHead {
    head: Head,
    context: Option<Bytes>,
}

struct InFlight {
    seq: i64,
    deadline: Instant,
}

#[derive(Default)]
struct GroupState {
    /// Owned and handing out. False while draining before a hand-over.
    serving: bool,
    draining_since: Option<Instant>,
    ready: VecDeque<ReadyHead>,
    /// source -> the event handed out for it (at most one per source: only heads are handed out)
    inflight: HashMap<String, InFlight>,
    dirty: bool,
    fetching: bool,
    last_fetch: Option<Instant>,
}

struct WorkerState {
    inbox: String,
    groups: Vec<String>,
    credit: u32,
    last_seen: Instant,
}

#[derive(Default)]
struct Core {
    groups: HashMap<String, GroupState>,
    workers: HashMap<String, WorkerState>,
    /// group -> workers serving it
    by_group: HashMap<String, Vec<String>>,
    /// group -> (owner member, its inbox), from the retained owner announcements
    owners: HashMap<String, (String, String)>,
    /// Hand out nothing after this instant unless the leases were renewed (local lease deadline).
    lease_deadline: Option<Instant>,
}

impl Core {
    fn credits(&self, group: &str) -> u32 {
        self.by_group
            .get(group)
            .map(|ws| ws.iter().filter_map(|w| self.workers.get(w)).map(|w| w.credit).sum())
            .unwrap_or(0)
    }

    fn reindex_worker(&mut self, id: &str) {
        for ws in self.by_group.values_mut() {
            ws.retain(|w| w != id);
        }
        if let Some(w) = self.workers.get(id) {
            for g in &w.groups {
                self.by_group.entry(g.clone()).or_default().push(id.to_owned());
            }
        }
    }

    fn handing_out(&self, now: Instant) -> bool {
        self.lease_deadline.is_some_and(|d| now < d)
    }

    /// Match ready heads with credited workers. Returns deliveries grouped by worker inbox.
    fn assign(&mut self, now: Instant, timeout: Duration) -> Vec<(String, Vec<v1::Delivery>)> {
        let mut out: HashMap<String, Vec<v1::Delivery>> = HashMap::new();
        if !self.handing_out(now) {
            return vec![];
        }
        let Core { groups, workers, by_group, .. } = self;
        for (g, st) in groups.iter_mut().filter(|(_, st)| st.serving && !st.ready.is_empty()) {
            let Some(ws) = by_group.get(g) else { continue };
            while !st.ready.is_empty() {
                // the credited worker with the most credit
                let Some(wid) = ws.iter().filter(|w| workers.get(*w).is_some_and(|x| x.credit > 0)).max_by_key(|w| workers[*w].credit)
                else {
                    break;
                };
                let r = st.ready.pop_front().unwrap();
                if st.inflight.contains_key(&r.head.key.source) {
                    continue; // already handed out meanwhile
                }
                let w = workers.get_mut(wid).unwrap();
                w.credit -= 1;
                st.inflight.insert(r.head.key.source.clone(), InFlight { seq: r.head.key.seq, deadline: now + timeout });
                out.entry(w.inbox.clone()).or_default().push(v1::Delivery {
                    group: r.head.group,
                    source: r.head.key.source,
                    seq: r.head.key.seq,
                    body: r.head.body,
                    context: r.context,
                    attempts: r.head.attempts,
                    enqueued_us: r.head.enqueued_at_us,
                });
            }
        }
        out.into_iter().collect()
    }

    /// Groups that need a head read: serving, credited beyond what is ready, and dirty or due for a fallback read.
    fn due_fetches(&mut self, now: Instant, fallback: Duration) -> Vec<HeadRequest> {
        if !self.handing_out(now) {
            return vec![];
        }
        let credits: HashMap<String, u32> = self.groups.keys().map(|g| (g.clone(), self.credits(g))).collect();
        let mut reqs = vec![];
        for (g, st) in self.groups.iter_mut() {
            if !st.serving || st.fetching {
                continue;
            }
            let want = credits[g] as usize;
            if want <= st.ready.len() {
                continue;
            }
            if !st.dirty && st.last_fetch.is_some_and(|t| now.duration_since(t) < fallback) {
                continue;
            }
            st.fetching = true;
            st.dirty = false;
            let exclude = st.inflight.keys().cloned().chain(st.ready.iter().map(|r| r.head.key.source.clone())).collect();
            reqs.push(HeadRequest { group: g.clone(), max: want - st.ready.len(), exclude_sources: exclude });
        }
        reqs
    }
}

// ---------------------------------------------------------------------------------------------------------

#[derive(Default)]
pub struct Stats {
    pub published: AtomicU64,
    pub publish_duplicates: AtomicU64,
    pub publish_batches: AtomicU64,
    pub applied: AtomicU64,
    pub duplicates: AtomicU64,
    pub gaps: AtomicU64,
    pub failures: AtomicU64,
    pub commit_batches: AtomicU64,
    pub commit_errors: AtomicU64,
    pub delivered: AtomicU64,
    pub fetches: AtomicU64,
    pub fetched_heads: AtomicU64,
    pub timeouts: AtomicU64,
    pub hints_sent: AtomicU64,
    pub claims: AtomicU64,
    pub releases: AtomicU64,
    pub leases_lost: AtomicU64,
}

struct PendingPublish {
    reply_to: String,
    request_id: u64,
    items: Vec<v1::PublishItem>,
}

struct PendingResults {
    reply_to: String,
    items: Vec<v1::WorkResult>,
}

pub struct Dispatcher<B: Backend> {
    cfg: Config,
    backend: Arc<B>,
    fabric: Fabric,
    topics: Topics,
    inbox: String,
    core: Mutex<Core>,
    wake: Notify,
    pub stats: Stats,
    started_us: i64,
}

fn now_us() -> i64 {
    std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map(|d| d.as_micros() as i64).unwrap_or(0)
}

fn envelope(body: Body) -> Bytes {
    Bytes::from(v1::Envelope { body: Some(body) }.encode_to_vec())
}

/// Rendezvous (highest random weight) hashing: the same live list gives every member the same owner.
pub fn owner_of<'a>(group: &str, members: &'a [String]) -> Option<&'a String> {
    members.iter().max_by_key(|m| (xxhash_rust::xxh3::xxh3_64(format!("{m}|{group}").as_bytes()), std::cmp::Reverse(*m)))
}

impl<B: Backend> Dispatcher<B> {
    pub fn new(cfg: Config, backend: B, fabric: Fabric) -> Arc<Self> {
        let topics = Topics::new(cfg.ns.clone());
        let inbox = topics.inbox(&cfg.member_id);
        Arc::new(Self {
            cfg,
            backend: Arc::new(backend),
            fabric,
            topics,
            inbox,
            core: Mutex::new(Core::default()),
            wake: Notify::new(),
            stats: Stats::default(),
            started_us: now_us(),
        })
    }

    /// The retained-message payload that clears this member's announcement (used as the MQTT last will).
    pub fn last_will(topics: &Topics, member_id: &str) -> (String, Bytes, bool) {
        (topics.member(member_id), Bytes::new(), true)
    }

    /// Runs until `shutdown` turns true; then drains, gives the leases back and leaves.
    pub async fn run(self: Arc<Self>, mut incoming: mpsc::Receiver<Incoming>, mut shutdown: watch::Receiver<bool>) {
        let t = &self.topics;
        for f in [t.publish_shared_filter(), t.any_shared_filter(), self.inbox.clone(), t.owners_filter()] {
            if let Err(e) = self.fabric.subscribe(f.clone()).await {
                warn!(filter = %f, error = %e, "subscribe failed");
            }
        }
        let info = v1::MemberInfo { member_id: self.cfg.member_id.clone(), inbox: self.inbox.clone(), started_us: self.started_us };
        let _ = self.fabric.publish_retained(t.member(&self.cfg.member_id), Bytes::from(info.encode_to_vec())).await;

        let (pub_tx, pub_rx) = mpsc::channel::<PendingPublish>(65_536);
        let (res_tx, res_rx) = mpsc::channel::<PendingResults>(65_536);
        let (stop_tx, stop_rx) = watch::channel(false);
        let mut tasks = vec![
            tokio::spawn(self.clone().publish_loop(pub_rx)),
            tokio::spawn(self.clone().commit_loop(res_rx)),
            tokio::spawn(self.clone().dispatch_loop(stop_rx.clone())),
            tokio::spawn(self.clone().sweep_loop(stop_rx.clone())),
            tokio::spawn(self.clone().lease_loop(stop_rx.clone())),
            tokio::spawn(self.clone().housekeeping_loop(stop_rx.clone())),
        ];
        info!(member = %self.cfg.member_id, ns = %self.cfg.ns, "dispatcher running");
        loop {
            tokio::select! {
                m = incoming.recv() => match m {
                    Some(m) => self.route(m, &pub_tx, &res_tx),
                    None => break,
                },
                _ = shutdown.changed() => if *shutdown.borrow() { break },
            }
        }
        // Planned leave: stop handing out, let in-flight events finish (their results still arrive), give back.
        info!("shutting down: draining");
        {
            let mut core = self.core.lock().unwrap();
            for st in core.groups.values_mut() {
                st.serving = false;
            }
        }
        let until = Instant::now() + self.cfg.drain;
        while Instant::now() < until && self.core.lock().unwrap().groups.values().any(|g| !g.inflight.is_empty()) {
            tokio::select! {
                m = incoming.recv() => if let Some(m) = m { self.route(m, &pub_tx, &res_tx) },
                _ = tokio::time::sleep(Duration::from_millis(50)) => {}
            }
        }
        let _ = stop_tx.send(true);
        drop(pub_tx);
        drop(res_tx);
        if let Err(e) = self.backend.leave(&self.cfg.member_id).await {
            warn!(error = %e, "leave failed (leases will expire)");
        }
        let _ = self.fabric.publish_retained(t.member(&self.cfg.member_id), Bytes::new()).await;
        for task in tasks.drain(..) {
            let _ = tokio::time::timeout(Duration::from_secs(5), task).await;
        }
        self.fabric.disconnect().await;
        info!("left the cluster");
    }

    // ---- incoming ----

    fn route(&self, m: Incoming, pub_tx: &mpsc::Sender<PendingPublish>, res_tx: &mpsc::Sender<PendingResults>) {
        let ns = &self.cfg.ns;
        let Some(rest) = m.topic.strip_prefix(ns.as_str()).and_then(|r| r.strip_prefix('/')) else { return };
        if rest.starts_with("pub/") {
            match v1::PublishRequest::decode(m.payload) {
                Ok(r) => {
                    let _ = pub_tx.try_send(PendingPublish { reply_to: r.reply_to, request_id: r.request_id, items: r.items });
                }
                Err(e) => debug!(error = %e, "bad publish request"),
            }
        } else if let Some(group) = rest.strip_prefix("owner/") {
            let mut core = self.core.lock().unwrap();
            match v1::OwnerInfo::decode(m.payload) {
                Ok(o) if !o.member_id.is_empty() => {
                    core.owners.insert(group.to_owned(), (o.member_id, o.inbox));
                }
                _ => {
                    core.owners.remove(group);
                }
            }
        } else if rest == "any" || m.topic == self.inbox {
            let Ok(env) = v1::Envelope::decode(m.payload) else { return };
            match env.body {
                Some(Body::Credit(c)) => self.on_credit(c),
                Some(Body::Results(r)) => {
                    self.seen(&r.worker_id);
                    let _ = res_tx.try_send(PendingResults { reply_to: r.reply_to, items: r.items });
                }
                Some(Body::Hint(h)) => {
                    let mut core = self.core.lock().unwrap();
                    for g in h.groups {
                        if let Some(st) = core.groups.get_mut(&g) {
                            st.dirty = true;
                        }
                    }
                    drop(core);
                    self.wake.notify_one();
                }
                Some(Body::Progress(p)) => {
                    self.seen(&p.worker_id);
                    let deadline = Instant::now() + self.cfg.delivery_timeout;
                    let mut core = self.core.lock().unwrap();
                    let sources: HashSet<&String> = p.sources.iter().collect();
                    for st in core.groups.values_mut() {
                        for (s, f) in st.inflight.iter_mut() {
                            if sources.contains(s) {
                                f.deadline = deadline;
                            }
                        }
                    }
                }
                _ => {}
            }
        }
    }

    fn seen(&self, worker: &str) {
        if let Some(w) = self.core.lock().unwrap().workers.get_mut(worker) {
            w.last_seen = Instant::now();
        }
    }

    fn on_credit(&self, c: v1::WorkerCredit) {
        if !valid_name(&c.worker_id) {
            return;
        }
        let mut core = self.core.lock().unwrap();
        let now = Instant::now();
        let reindex = match core.workers.get_mut(&c.worker_id) {
            Some(w) => {
                w.last_seen = now;
                w.credit = if c.reset { c.credit } else { w.credit.saturating_add(c.credit) };
                w.inbox = c.inbox;
                let changed = w.groups != c.groups;
                if changed {
                    w.groups = c.groups;
                }
                changed
            }
            None => {
                core.workers.insert(
                    c.worker_id.clone(),
                    WorkerState { inbox: c.inbox, groups: c.groups, credit: c.credit, last_seen: now },
                );
                true
            }
        };
        if reindex {
            core.reindex_worker(&c.worker_id);
        }
        drop(core);
        self.wake.notify_one();
    }

    // ---- publish: batched across producers, acknowledged after the commit ----

    async fn publish_loop(self: Arc<Self>, mut rx: mpsc::Receiver<PendingPublish>) {
        let permits = Arc::new(Semaphore::new(self.cfg.publish_executors));
        while let Some(first) = rx.recv().await {
            let permit = permits.clone().acquire_owned().await.unwrap();
            // everything that arrived while we waited for a free executor goes into this batch
            let mut batch = vec![first];
            let mut n = batch[0].items.len();
            while n < self.cfg.publish_max_events {
                match rx.try_recv() {
                    Ok(p) => {
                        n += p.items.len();
                        batch.push(p);
                    }
                    Err(_) => break,
                }
            }
            let me = self.clone();
            tokio::spawn(async move {
                me.publish_batch(batch).await;
                drop(permit);
            });
        }
    }

    async fn publish_batch(&self, batch: Vec<PendingPublish>) {
        let mut events = vec![];
        let mut bad = vec![false; batch.len()];
        for (i, p) in batch.iter().enumerate() {
            if p.items.iter().any(|it| !valid_name(&it.group) || it.source.is_empty() || it.seq < 1) {
                bad[i] = true;
                continue;
            }
            events.extend(p.items.iter().map(|it| Event {
                group: it.group.clone(),
                key: EventKey { source: it.source.clone(), seq: it.seq },
                body: it.body.clone(),
            }));
        }
        let result = self.backend.publish(&events).await;
        self.stats.publish_batches.fetch_add(1, Relaxed);
        let mut touched: HashSet<String> = HashSet::new();
        let mut k = 0;
        for (i, p) in batch.into_iter().enumerate() {
            let reply = if bad[i] {
                v1::PublishReply { request_id: p.request_id, status: vec![], error: "invalid group, source or seq".into() }
            } else {
                match &result {
                    Ok(outcomes) => {
                        let mine = &outcomes[k..k + p.items.len()];
                        k += p.items.len();
                        let status = mine
                            .iter()
                            .zip(&p.items)
                            .map(|(o, it)| match o {
                                PublishOutcome::Stored => {
                                    touched.insert(it.group.clone());
                                    self.stats.published.fetch_add(1, Relaxed);
                                    v1::PublishStatus::PublishStored as i32
                                }
                                PublishOutcome::Duplicate => {
                                    self.stats.publish_duplicates.fetch_add(1, Relaxed);
                                    v1::PublishStatus::PublishDuplicate as i32
                                }
                                PublishOutcome::GroupMismatch => v1::PublishStatus::PublishGroupMismatch as i32,
                            })
                            .collect();
                        v1::PublishReply { request_id: p.request_id, status, error: String::new() }
                    }
                    Err(e) => v1::PublishReply { request_id: p.request_id, status: vec![], error: e.to_string() },
                }
            };
            if !p.reply_to.is_empty() {
                let _ = self.fabric.publish(p.reply_to, envelope(Body::PublishReply(reply))).await;
            }
        }
        if let Err(e) = &result {
            warn!(error = %e, "publish batch failed");
            return;
        }
        // Wake the owners: ourselves directly, the others with a hint.
        let mut hints: HashMap<String, Vec<String>> = HashMap::new();
        {
            let mut core = self.core.lock().unwrap();
            for g in touched {
                if let Some(st) = core.groups.get_mut(&g) {
                    st.dirty = true;
                } else if let Some((m, inbox)) = core.owners.get(&g) {
                    if *m != self.cfg.member_id {
                        hints.entry(inbox.clone()).or_default().push(g);
                    }
                }
            }
        }
        self.wake.notify_one();
        for (inbox, groups) in hints {
            self.stats.hints_sent.fetch_add(1, Relaxed);
            let _ = self.fabric.publish(inbox, envelope(Body::Hint(v1::Hint { groups }))).await;
        }
    }

    // ---- results: batched commits ----

    async fn commit_loop(self: Arc<Self>, mut rx: mpsc::Receiver<PendingResults>) {
        let permits = Arc::new(Semaphore::new(self.cfg.commit_executors));
        while let Some(first) = rx.recv().await {
            let permit = permits.clone().acquire_owned().await.unwrap();
            let mut batch = vec![first];
            let mut n = batch[0].items.len();
            while n < self.cfg.commit_max {
                match rx.try_recv() {
                    Ok(p) => {
                        n += p.items.len();
                        batch.push(p);
                    }
                    Err(_) => break,
                }
            }
            let me = self.clone();
            tokio::spawn(async move {
                me.commit_batch(batch).await;
                drop(permit);
            });
        }
    }

    async fn commit_batch(&self, batch: Vec<PendingResults>) {
        let commits: Vec<Commit> = batch
            .iter()
            .flat_map(|b| b.items.iter().filter(|r| r.ok))
            .map(|r| Commit { key: EventKey { source: r.source.clone(), seq: r.seq }, effect: r.effect.clone() })
            .collect();
        let outcomes = if commits.is_empty() { Ok(vec![]) } else { self.backend.commit(&commits).await };
        self.stats.commit_batches.fetch_add(1, Relaxed);
        if let Err(e) = &outcomes {
            self.stats.commit_errors.fetch_add(1, Relaxed);
            warn!(error = %e, n = commits.len(), "commit batch failed");
        }
        let mut k = 0;
        let mut done: Vec<(String, String, i64)> = vec![]; // (group, source, seq) released from in-flight
        for b in batch {
            let mut acks = vec![];
            for r in &b.items {
                let status = if r.ok {
                    let st = match &outcomes {
                        Ok(o) => match o[k] {
                            CommitOutcome::Applied => {
                                self.stats.applied.fetch_add(1, Relaxed);
                                v1::ResultStatus::ResultApplied
                            }
                            CommitOutcome::Duplicate => {
                                self.stats.duplicates.fetch_add(1, Relaxed);
                                v1::ResultStatus::ResultDuplicate
                            }
                            CommitOutcome::Gap => {
                                self.stats.gaps.fetch_add(1, Relaxed);
                                v1::ResultStatus::ResultGap
                            }
                        },
                        Err(_) => v1::ResultStatus::ResultError,
                    };
                    k += 1;
                    st
                } else {
                    self.stats.failures.fetch_add(1, Relaxed);
                    let delay = Duration::from_millis(r.retry_delay_ms.max(1) as u64);
                    match self.backend.fail(&EventKey { source: r.source.clone(), seq: r.seq }, delay).await {
                        Ok(()) => v1::ResultStatus::ResultFailureRecorded,
                        Err(e) => {
                            warn!(error = %e, "recording a failure failed");
                            v1::ResultStatus::ResultError
                        }
                    }
                };
                if status != v1::ResultStatus::ResultError {
                    done.push((r.group.clone(), r.source.clone(), r.seq));
                }
                acks.push(v1::ResultAck { request_id: r.request_id, status: status as i32 });
            }
            if !b.reply_to.is_empty() {
                let _ = self.fabric.publish(b.reply_to, envelope(Body::ResultReply(v1::ResultReply { items: acks }))).await;
            }
        }
        {
            let mut core = self.core.lock().unwrap();
            for (g, s, q) in done {
                if let Some(st) = core.groups.get_mut(&g) {
                    if st.inflight.get(&s).is_some_and(|f| f.seq == q) {
                        st.inflight.remove(&s);
                    }
                    st.dirty = true;
                }
            }
        }
        self.wake.notify_one();
    }

    // ---- hand-out ----

    async fn dispatch_loop(self: Arc<Self>, mut stop: watch::Receiver<bool>) {
        let fetch_slots = Arc::new(Semaphore::new(self.cfg.fetch_parallelism));
        let idle = (self.cfg.fallback_poll / 4).max(Duration::from_millis(20));
        loop {
            tokio::select! {
                _ = self.wake.notified() => {}
                _ = tokio::time::sleep(idle) => {}
                _ = stop.changed() => return,
            }
            let now = Instant::now();
            let (sends, reqs) = {
                let mut core = self.core.lock().unwrap();
                let sends = core.assign(now, self.cfg.delivery_timeout);
                (sends, core.due_fetches(now, self.cfg.fallback_poll))
            };
            self.send_deliveries(sends).await;
            for chunk in reqs.chunks(self.cfg.fetch_max_groups) {
                let chunk = chunk.to_vec();
                let permit = fetch_slots.clone().acquire_owned().await.unwrap();
                let me = self.clone();
                tokio::spawn(async move {
                    me.fetch(chunk).await;
                    drop(permit);
                });
            }
        }
    }

    async fn fetch(&self, reqs: Vec<HeadRequest>) {
        self.stats.fetches.fetch_add(1, Relaxed);
        let result = match self.backend.read_heads(&reqs).await {
            Ok(heads) => match self.backend.load_context(&heads).await {
                Ok(ctx) => Ok((heads, ctx)),
                Err(e) => Err(e),
            },
            Err(e) => Err(e),
        };
        let now = Instant::now();
        let sends = {
            let mut core = self.core.lock().unwrap();
            let mut got: HashMap<String, usize> = HashMap::new();
            if let Ok((heads, ctx)) = &result {
                self.stats.fetched_heads.fetch_add(heads.len() as u64, Relaxed);
                for (h, c) in heads.iter().zip(ctx) {
                    *got.entry(h.group.clone()).or_default() += 1;
                    if let Some(st) = core.groups.get_mut(&h.group) {
                        let known = st.inflight.contains_key(&h.key.source) || st.ready.iter().any(|r| r.head.key.source == h.key.source);
                        if st.serving && !known {
                            st.ready.push_back(ReadyHead { head: h.clone(), context: c.clone() });
                        }
                    }
                }
            }
            for r in &reqs {
                if let Some(st) = core.groups.get_mut(&r.group) {
                    st.fetching = false;
                    st.last_fetch = Some(now);
                    if result.is_err() || got.get(&r.group).copied().unwrap_or(0) >= r.max {
                        st.dirty = true; // there may be more (or the read failed): look again
                    }
                }
            }
            core.assign(now, self.cfg.delivery_timeout)
        };
        if let Err(e) = result {
            warn!(error = %e, "head read failed");
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        self.send_deliveries(sends).await;
        self.wake.notify_one();
    }

    async fn send_deliveries(&self, sends: Vec<(String, Vec<v1::Delivery>)>) {
        for (inbox, items) in sends {
            self.stats.delivered.fetch_add(items.len() as u64, Relaxed);
            let d = v1::Deliver { member_id: self.cfg.member_id.clone(), inbox: self.inbox.clone(), items };
            if let Err(e) = self.fabric.publish(inbox, envelope(Body::Deliver(d))).await {
                warn!(error = %e, "deliver failed (the delivery timeout will re-offer it)");
            }
        }
    }

    // ---- timeouts ----

    async fn sweep_loop(self: Arc<Self>, mut stop: watch::Receiver<bool>) {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(250)) => {}
                _ = stop.changed() => return,
            }
            let now = Instant::now();
            let mut woke = false;
            {
                let mut core = self.core.lock().unwrap();
                for st in core.groups.values_mut() {
                    let before = st.inflight.len();
                    st.inflight.retain(|_, f| f.deadline > now);
                    let expired = before - st.inflight.len();
                    if expired > 0 {
                        self.stats.timeouts.fetch_add(expired as u64, Relaxed);
                        st.dirty = true;
                        woke = true;
                    }
                }
                let ttl = self.cfg.worker_ttl;
                let gone: Vec<String> =
                    core.workers.iter().filter(|(_, w)| now.duration_since(w.last_seen) > ttl).map(|(id, _)| id.clone()).collect();
                for id in gone {
                    core.workers.remove(&id);
                    core.reindex_worker(&id);
                    debug!(worker = %id, "worker silent: forgotten");
                }
                // a lapsed local lease deadline: stop handing out (the leases may already be someone else's)
                if core.lease_deadline.is_some_and(|d| now >= d) {
                    for st in core.groups.values_mut() {
                        st.ready.clear();
                    }
                }
            }
            if woke {
                self.wake.notify_one();
            }
        }
    }

    // ---- leases: leaderless ownership ----

    async fn lease_loop(self: Arc<Self>, mut stop: watch::Receiver<bool>) {
        let me = Member { id: self.cfg.member_id.clone(), address: self.inbox.clone() };
        let mut live: Vec<String> = vec![];
        let mut all_groups: Vec<String> = vec![];
        let mut ticks: u64 = 0;
        loop {
            let started = Instant::now();
            match self.lease_tick(&me, &mut live, &mut all_groups, ticks).await {
                Ok(()) => {
                    // nothing may be handed out once the database could consider our leases expired
                    let deadline = started + self.cfg.lease_ttl.saturating_sub(self.cfg.tick);
                    self.core.lock().unwrap().lease_deadline = Some(deadline);
                }
                Err(e) => warn!(error = %e, "lease tick failed"),
            }
            ticks += 1;
            tokio::select! {
                _ = tokio::time::sleep(self.cfg.tick.saturating_sub(started.elapsed())) => {}
                _ = stop.changed() => return,
            }
        }
    }

    async fn lease_tick(&self, me: &Member, live: &mut Vec<String>, all_groups: &mut Vec<String>, ticks: u64) -> octopus_core::Result<()> {
        let ttl = self.cfg.lease_ttl;
        let members: Vec<String> = self.backend.heartbeat(me, ttl).await?.into_iter().map(|m| m.id).collect();
        if members != *live {
            info!(members = ?members, "membership changed");
            *live = members;
        }
        if ticks % 4 == 0 || all_groups.is_empty() {
            *all_groups = self.backend.groups().await?;
        }
        let desired: HashSet<&String> = all_groups.iter().filter(|g| owner_of(g, live) == Some(&me.id)).collect();
        let owned: Vec<String> = self.core.lock().unwrap().groups.keys().cloned().collect();

        // renew; whatever was not renewed belongs to someone else now
        let renewed: HashSet<String> = self.backend.renew(&me.id, &owned, ttl).await?.into_iter().collect();
        let now = Instant::now();
        let mut release = vec![];
        {
            let mut core = self.core.lock().unwrap();
            for g in owned.iter().filter(|g| !renewed.contains(*g)) {
                core.groups.remove(g);
                self.stats.leases_lost.fetch_add(1, Relaxed);
                warn!(group = %g, "lease lost");
            }
            for (g, st) in core.groups.iter_mut() {
                if desired.contains(g) {
                    if st.draining_since.take().is_some() {
                        st.serving = true; // not handed over after all
                        st.dirty = true;
                    }
                    continue;
                }
                // belongs to another member now: stop handing out, drain, hand over
                if st.draining_since.is_none() {
                    st.serving = false;
                    st.ready.clear();
                    st.draining_since = Some(now);
                }
                if st.inflight.is_empty() || now.duration_since(st.draining_since.unwrap()) >= self.cfg.drain {
                    release.push(g.clone());
                }
            }
        }
        if !release.is_empty() {
            self.backend.release(&me.id, &release).await?;
            let mut core = self.core.lock().unwrap();
            for g in &release {
                core.groups.remove(g);
            }
            self.stats.releases.fetch_add(release.len() as u64, Relaxed);
            info!(n = release.len(), "handed over groups");
        }

        // claim what should be ours and is free
        let want: Vec<String> = {
            let core = self.core.lock().unwrap();
            desired.iter().filter(|g| !core.groups.contains_key(**g)).map(|g| (*g).clone()).collect()
        };
        if !want.is_empty() {
            let got = self.backend.claim(&me.id, &want, ttl).await?;
            if !got.is_empty() {
                {
                    let mut core = self.core.lock().unwrap();
                    for l in &got {
                        core.groups.insert(l.group.clone(), GroupState { serving: true, dirty: true, ..Default::default() });
                    }
                }
                self.stats.claims.fetch_add(got.len() as u64, Relaxed);
                for l in &got {
                    let o = v1::OwnerInfo { group: l.group.clone(), member_id: me.id.clone(), inbox: self.inbox.clone(), epoch: l.epoch };
                    let _ = self.fabric.publish_retained(self.topics.owner(&l.group), Bytes::from(o.encode_to_vec())).await;
                }
                info!(n = got.len(), total = self.core.lock().unwrap().groups.len(), "claimed groups");
                self.wake.notify_one();
            }
        }
        Ok(())
    }

    async fn housekeeping_loop(self: Arc<Self>, mut stop: watch::Receiver<bool>) {
        let mut last_purge = Instant::now();
        loop {
            tokio::select! {
                _ = tokio::time::sleep(self.cfg.stats_every) => {}
                _ = stop.changed() => return,
            }
            if last_purge.elapsed() >= self.cfg.purge_every {
                last_purge = Instant::now();
                match self.backend.purge_applied(10_000).await {
                    Ok(n) if n > 0 => info!(rows = n, "purged rows at or below their cursor"),
                    Ok(_) => {}
                    Err(e) => warn!(error = %e, "purge failed"),
                }
            }
            let (groups, workers, inflight, ready) = {
                let core = self.core.lock().unwrap();
                (
                    core.groups.values().filter(|g| g.serving).count(),
                    core.workers.len(),
                    core.groups.values().map(|g| g.inflight.len()).sum::<usize>(),
                    core.groups.values().map(|g| g.ready.len()).sum::<usize>(),
                )
            };
            let s = &self.stats;
            info!(
                groups, workers, inflight, ready,
                published = s.published.load(Relaxed), applied = s.applied.load(Relaxed),
                duplicates = s.duplicates.load(Relaxed), failures = s.failures.load(Relaxed),
                timeouts = s.timeouts.load(Relaxed), commit_batches = s.commit_batches.load(Relaxed),
                fetches = s.fetches.load(Relaxed), "stats"
            );
        }
    }
}
