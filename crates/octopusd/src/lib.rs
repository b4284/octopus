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
use octopus_core::{Backend, Commit, CommitOutcome, EpochEnd, Event, EventKey, Head, HeadRequest, Member, Pos, PublishOutcome, Skipped};
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
    /// Report a source whose next event is missing while a later one has waited this long (a gap).
    pub gap_alert: Duration,
    /// Look for gaps this often.
    pub gap_check: Duration,
    /// Skip a gap after this long, for groups without their own policy (`grp.gap_skip_ms`). None: never.
    pub gap_skip: Option<Duration>,
    /// Hand out events known to be heads (from publishes and "done" removals) without reading the group's heads.
    /// Off when contexts are preloaded by a load hook (the fast path has no context).
    pub fast_path: bool,
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
            publish_executors: 16,
            publish_max_events: 2000,
            commit_executors: 16,
            commit_max: 128,
            fetch_parallelism: 4,
            fetch_max_groups: 64,
            purge_every: Duration::from_secs(30),
            stats_every: Duration::from_secs(5),
            gap_alert: Duration::from_secs(30),
            gap_check: Duration::from_secs(10),
            gap_skip: None,
            fast_path: true,
        }
    }
}

// ---------------------------------------------------------------------------------------------------------
// In-memory state (a cache of the database plus who holds what; losing it is harmless)

struct ReadyHead {
    head: Head,
    context: Option<Bytes>,
}

/// A delivery outstanding this long is reported (once): its worker keeps it alive with progress reports.
const LONG_OUTSTANDING: Duration = Duration::from_secs(60);

struct InFlight {
    pos: Pos,
    /// The worker it was handed to: only its progress reports extend the deadline, and only its failure reports
    /// give the event back (a late report from an earlier holder must not cancel the current delivery).
    worker: String,
    deadline: Instant,
    since: Instant,
    /// already reported as long-outstanding
    warned: bool,
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
    /// Events this dispatcher handed out whose outcome is unknown (timed out, or "done" hit a database error):
    /// a repeat must carry possibly_redelivered.
    suspect: HashSet<(String, Pos)>,
    /// Events stored before this instant (Unix µs, with slack) may have been handed out by the previous owner.
    claimed_us: i64,
    last_demand: Option<Instant>,
    /// source -> last position known done by this owner (stale hints are ignored)
    cursor: HashMap<String, Pos>,
    /// source -> highest position known stored
    known_max: HashMap<String, Pos>,
    /// source -> bodies of its stored events that are not heads yet (from publishes and hints), so the next one can
    /// be handed out the moment its predecessor is reported done: position -> (body, enqueued µs, skip_to)
    pending: HashMap<String, std::collections::BTreeMap<Pos, (Bytes, i64, Option<i64>)>>,
    pending_count: usize,
    /// source -> the event to look up by key (its next one, not in memory); retried until found
    want: HashMap<String, Pos>,
    /// source -> positions skipped since its last delivery (told to the worker with the next one)
    skipped: HashMap<String, Vec<Skipped>>,
    want_fetching: bool,
    last_want_fetch: Option<Instant>,
}

/// Bodies kept for not-yet-head events, per group (beyond that, the database is read).
const PENDING_MAX_PER_GROUP: usize = 4096;

/// Largest head body carried in a hint (bigger ones are read from the database by the owner).
const HINT_BODY_MAX: usize = 64 * 1024;

impl GroupState {
    /// A wanted key has never been looked up yet (look it up now rather than after the retry interval).
    fn dirty_want(&self) -> bool {
        self.last_want_fetch.is_none()
    }

    /// Queue a known head for hand-out unless it is stale or its source already has an event out or queued.
    fn offer(&mut self, head: Head) -> bool {
        let s = &head.key.source;
        if !self.serving
            || self.inflight.contains_key(s)
            || self.cursor.get(s).is_some_and(|c| *c >= head.key.pos)
            || self.ready.iter().any(|r| r.head.key.source == *s)
        {
            return false;
        }
        self.ready.push_back(ReadyHead { head, context: None });
        true
    }

    /// A worker reported `pos` of `source` done: it has committed its writes, so the source is free and its next
    /// event can go out now (the removal from the database follows). Returns whether a next event was queued.
    /// (The next event of a new epoch is never taken from memory: only the database knows where the old epoch
    /// ended. The fallback head read finds it once the cursor is written.)
    fn done(&mut self, group: &str, source: &str, pos: Pos) -> bool {
        if self.inflight.get(source).is_some_and(|f| f.pos == pos) {
            self.inflight.remove(source);
        }
        self.suspect.remove(&(source.to_owned(), pos));
        let c = self.cursor.entry(source.to_owned()).or_default();
        *c = (*c).max(pos);
        let Some(p) = self.pending.get_mut(source) else {
            self.want.insert(source.to_owned(), pos.next());
            return false;
        };
        // forget what is done, then take the next one if we hold it
        while let Some((&q, _)) = p.first_key_value() {
            if q > pos {
                break;
            }
            p.pop_first();
            self.pending_count -= 1;
        }
        let next = match p.first_key_value() {
            Some((&q, _)) if q == pos.next() => p.pop_first().map(|(q, (body, enq, skip_to))| {
                self.pending_count -= 1;
                Head { group: group.to_owned(), key: EventKey::new(source, q), body, skip_to, attempts: 0, enqueued_at_us: enq }
            }),
            _ => None,
        };
        if p.is_empty() {
            self.pending.remove(source);
        }
        match next {
            Some(h) => self.offer(h),
            None => {
                self.want.insert(source.to_owned(), pos.next());
                false
            }
        }
    }

    /// Events stored for this group (by this dispatcher or, via a hint, another one).
    fn stored(&mut self, group: &str, events: Vec<v1::HintEvent>, fast_path: bool) {
        for e in events {
            let pos = Pos::new(e.epoch, e.seq);
            let m = self.known_max.entry(e.source.clone()).or_default();
            *m = (*m).max(pos);
            if !fast_path {
                self.dirty = true;
                continue;
            }
            if e.head {
                match e.body {
                    Some(body) => {
                        let head = Head {
                            group: group.to_owned(),
                            key: EventKey::new(e.source, pos),
                            body,
                            skip_to: e.skip_to,
                            attempts: 0,
                            enqueued_at_us: e.enqueued_us,
                        };
                        self.offer(head);
                    }
                    None => {
                        self.want.insert(e.source, pos);
                    }
                }
            } else if self.cursor.get(&e.source).map(|c| c.next()) == Some(pos) && !self.inflight.contains_key(&e.source) {
                // stored while its predecessor was being removed: it is the head now
                match e.body {
                    Some(body) => {
                        let head =
                            Head { group: group.to_owned(), key: EventKey::new(e.source, pos), body, skip_to: e.skip_to, attempts: 0, enqueued_at_us: e.enqueued_us };
                        self.offer(head);
                    }
                    None => {
                        self.want.insert(e.source, pos);
                    }
                }
            } else if let Some(body) = e.body {
                // not the head yet: keep its body for the moment its predecessor is reported done
                if self.pending_count < PENDING_MAX_PER_GROUP && self.cursor.get(&e.source).is_none_or(|c| *c < pos) {
                    let p = self.pending.entry(e.source).or_default();
                    if p.insert(pos, (body, e.enqueued_us, e.skip_to)).is_none() {
                        self.pending_count += 1;
                    }
                }
            }
        }
    }
}

struct WorkerState {
    inbox: String,
    groups: Vec<String>,
    credit: u32,
    last_seen: Instant,
    /// slots the worker declared (0 = not stated)
    slots: u32,
    /// deliveries of this dispatcher out with the worker (recomputed exactly every sweep)
    held: u32,
}

impl WorkerState {
    /// Credit to spend, and (if the worker declared its slots) room below them: a client whose credit accounting
    /// drifted cannot be handed more events than it can run.
    fn usable(&self) -> bool {
        self.credit > 0 && (self.slots == 0 || self.held < self.slots)
    }
}

fn unhold(workers: &mut HashMap<String, WorkerState>, worker: &str) {
    if let Some(w) = workers.get_mut(worker) {
        w.held = w.held.saturating_sub(1);
    }
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
    /// Cursors moved by "done" reports and not yet written to the database (flushed every ~100 ms).
    unflushed: HashMap<String, Pos>,
    /// Position ranges skipped so far (source markers consumed here, gaps given up after the timeout).
    skips: u64,
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

    /// Match ready heads with credited workers. Returns deliveries grouped by worker inbox, and demands (groups with
    /// ready events but no credit here) grouped by worker inbox.
    fn assign(&mut self, now: Instant, timeout: Duration) -> (Vec<(String, Vec<v1::Delivery>)>, Vec<(String, v1::Demand)>) {
        let mut out: HashMap<String, Vec<v1::Delivery>> = HashMap::new();
        let mut demands: Vec<(String, v1::Demand)> = vec![];
        if !self.handing_out(now) {
            return (vec![], vec![]);
        }
        let Core { groups, workers, by_group, unflushed, skips, .. } = self;
        for (g, st) in groups.iter_mut().filter(|(_, st)| st.serving && !st.ready.is_empty()) {
            // skip markers are never handed out: the cursor jumps over the declared range (written with the next
            // flush, which records the skip), the worker hears of it with the source's next delivery
            while let Some(i) = st.ready.iter().position(|r| r.head.skip_to.is_some()) {
                let r = st.ready.remove(i).unwrap();
                let (source, from) = (r.head.key.source, r.head.key.pos);
                let to = Pos::new(from.epoch, r.head.skip_to.unwrap_or(from.seq).max(from.seq));
                st.skipped.entry(source.clone()).or_default().push(Skipped { epoch: from.epoch, from_seq: from.seq, to_seq: Some(to.seq) });
                let u = unflushed.entry(source.clone()).or_default();
                *u = (*u).max(to);
                *skips += 1;
                st.done(g, &source, to);
            }
            if st.ready.is_empty() {
                continue;
            }
            let Some(ws) = by_group.get(g) else { continue };
            let credited = ws.iter().any(|w| workers.get(w).is_some_and(|x| x.usable()));
            if !credited {
                // events are ready but the group's workers hold no credit here: ask them to move idle credit
                if st.last_demand.is_none_or(|t| now.duration_since(t) >= Duration::from_millis(100)) {
                    st.last_demand = Some(now);
                    let mut per_inbox: HashMap<&str, Vec<String>> = HashMap::new();
                    for w in ws {
                        if let Some(x) = workers.get(w) {
                            per_inbox.entry(x.inbox.as_str()).or_default().push(w.clone());
                        }
                    }
                    for (inbox, worker_ids) in per_inbox {
                        demands.push((
                            inbox.to_owned(),
                            v1::Demand { member_id: String::new(), inbox: String::new(), group: g.clone(), ready: st.ready.len() as u32, worker_ids },
                        ));
                    }
                }
                continue;
            }
            while !st.ready.is_empty() {
                // the credited worker with the most credit
                let Some(wid) = ws.iter().filter(|w| workers.get(*w).is_some_and(|x| x.usable())).max_by_key(|w| workers[*w].credit)
                else {
                    break;
                };
                let r = st.ready.pop_front().unwrap();
                if st.inflight.contains_key(&r.head.key.source) {
                    continue; // already handed out meanwhile
                }
                let wid = wid.clone();
                let w = workers.get_mut(&wid).unwrap();
                w.credit -= 1;
                w.held += 1;
                st.inflight.insert(r.head.key.source.clone(), InFlight { pos: r.head.key.pos, worker: wid.clone(), deadline: now + timeout, since: now, warned: false });
                let possibly_redelivered =
                    st.suspect.contains(&(r.head.key.source.clone(), r.head.key.pos)) || r.head.enqueued_at_us <= st.claimed_us;
                let skipped = st
                    .skipped
                    .remove(&r.head.key.source)
                    .unwrap_or_default()
                    .into_iter()
                    .map(|k| v1::Skipped { epoch: k.epoch, from_seq: k.from_seq, to_seq: k.to_seq })
                    .collect();
                out.entry(w.inbox.clone()).or_default().push(v1::Delivery {
                    group: r.head.group,
                    source: r.head.key.source,
                    seq: r.head.key.pos.seq,
                    epoch: r.head.key.pos.epoch,
                    body: r.head.body,
                    context: r.context,
                    attempts: r.head.attempts,
                    enqueued_us: r.head.enqueued_at_us,
                    worker_id: wid.clone(),
                    possibly_redelivered,
                    skipped,
                });
            }
        }
        (out.into_iter().collect(), demands)
    }

    /// Next events to look up by key (their sources are free), every 100 ms per group at most.
    fn due_key_fetches(&mut self, now: Instant) -> Vec<(String, EventKey)> {
        let mut keys = vec![];
        if !self.handing_out(now) {
            return keys;
        }
        for (g, st) in self.groups.iter_mut() {
            if !st.serving || st.want.is_empty() || st.want_fetching {
                continue;
            }
            if st.last_want_fetch.is_some_and(|t| now.duration_since(t) < Duration::from_millis(100)) && !st.dirty_want() {
                continue;
            }
            let GroupState { want, inflight, cursor, ready, .. } = st;
            want.retain(|s, q| !inflight.contains_key(s) && cursor.get(s).is_none_or(|c| *c < *q));
            for (s, q) in want.iter() {
                if !ready.iter().any(|r| r.head.key.source == *s) {
                    keys.push((g.clone(), EventKey::new(s.clone(), *q)));
                }
            }
            if !st.want.is_empty() {
                st.want_fetching = true;
                st.last_want_fetch = Some(now);
            }
        }
        keys
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
    /// publishes rejected because the (source, seq) is stored with a different body
    pub publish_conflicts: AtomicU64,
    /// publishes rejected as from a stale epoch (fencing)
    pub publish_stale_epoch: AtomicU64,
    /// publishes of positions skipped before they arrived
    pub publish_late: AtomicU64,
    pub publish_batches: AtomicU64,
    pub applied: AtomicU64,
    /// "done" reports for events the worker already completed in its own transaction
    pub completed_by_worker: AtomicU64,
    pub duplicates: AtomicU64,
    pub gaps: AtomicU64,
    pub failures: AtomicU64,
    pub commit_batches: AtomicU64,
    pub commit_errors: AtomicU64,
    pub delivered: AtomicU64,
    pub fetches: AtomicU64,
    pub fetched_heads: AtomicU64,
    pub timeouts: AtomicU64,
    /// failure reports from a worker that no longer holds the event (ignored)
    pub stale_failures: AtomicU64,
    pub hints_sent: AtomicU64,
    pub claims: AtomicU64,
    pub releases: AtomicU64,
    pub leases_lost: AtomicU64,
    /// sources of owned groups currently stopped by a missing event (gauge, from the last gap check)
    pub gaps_open: AtomicU64,
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
    /// re-queues removals that hit a database error
    retry_tx: Mutex<Option<mpsc::Sender<PendingResults>>>,
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
            retry_tx: Mutex::new(None),
            stats: Stats::default(),
            started_us: now_us(),
        })
    }

    /// The retained "gone" announcement (a MemberInfo with no inbox), used as the MQTT last will and on leave.
    /// Not an empty payload: some brokers (NanoMQ) reject a zero-length will payload.
    pub fn last_will(topics: &Topics, member_id: &str) -> (String, Bytes, bool) {
        (topics.member(member_id), Self::gone(member_id), true)
    }

    fn gone(member_id: &str) -> Bytes {
        Bytes::from(v1::MemberInfo { member_id: member_id.to_owned(), inbox: String::new(), started_us: 0 }.encode_to_vec())
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
        *self.retry_tx.lock().unwrap() = Some(res_tx.clone());
        let (stop_tx, stop_rx) = watch::channel(false);
        let mut tasks = vec![
            tokio::spawn(self.clone().publish_loop(pub_rx)),
            tokio::spawn(self.clone().commit_loop(res_rx)),
            tokio::spawn(self.clone().dispatch_loop(stop_rx.clone())),
            tokio::spawn(self.clone().sweep_loop(stop_rx.clone())),
            tokio::spawn(self.clone().lease_loop(stop_rx.clone())),
            tokio::spawn(self.clone().housekeeping_loop(stop_rx.clone())),
            tokio::spawn(self.clone().flush_loop(stop_rx.clone())),
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
        self.flush_cursors().await;
        let _ = stop_tx.send(true);
        *self.retry_tx.lock().unwrap() = None;
        drop(pub_tx);
        drop(res_tx);
        if let Err(e) = self.backend.leave(&self.cfg.member_id).await {
            warn!(error = %e, "leave failed (leases will expire)");
        }
        let _ = self.fabric.publish_retained(t.member(&self.cfg.member_id), Self::gone(&self.cfg.member_id)).await;
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
            match v1::PublishBatch::decode(m.payload) {
                Ok(b) => {
                    for r in b.requests {
                        let _ = pub_tx.try_send(PendingPublish { reply_to: r.reply_to, request_id: r.request_id, items: r.items });
                    }
                }
                Err(e) => debug!(error = %e, "bad publish batch"),
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
            self.on_envelope(env, res_tx);
        }
    }

    fn on_envelope(&self, env: v1::Envelope, res_tx: &mpsc::Sender<PendingResults>) {
        {
            match env.body {
                Some(Body::Batch(b)) => {
                    for e in b.items {
                        self.on_envelope(e, res_tx);
                    }
                }
                Some(Body::Credit(c)) => self.on_credit(c),
                Some(Body::Results(mut r)) => {
                    // each result frees its slot: one credit back to the worker it came from
                    {
                        let mut core = self.core.lock().unwrap();
                        let now = Instant::now();
                        for it in r.items.iter_mut() {
                            if it.worker_id.is_empty() {
                                it.worker_id = r.worker_id.clone();
                            }
                            if let Some(w) = core.workers.get_mut(&it.worker_id) {
                                w.credit = w.credit.saturating_add(1);
                                w.last_seen = now;
                            }
                            // done = the worker committed: free the source and hand out its next event now; the
                            // cursor is written to the database with the next flush (~100 ms)
                            if it.ok && it.done && self.cfg.fast_path {
                                let Core { groups, workers, .. } = &mut *core;
                                if let Some(st) = groups.get_mut(&it.group) {
                                    if let Some(f) = st.inflight.get(&it.source).filter(|f| f.pos == Pos::new(it.epoch, it.seq)) {
                                        let w = f.worker.clone();
                                        st.inflight.remove(&it.source);
                                        unhold(workers, &w);
                                    }
                                    st.done(&it.group, &it.source, Pos::new(it.epoch, it.seq));
                                }
                                let c = core.unflushed.entry(it.source.clone()).or_default();
                                *c = (*c).max(Pos::new(it.epoch, it.seq));
                                self.stats.applied.fetch_add(1, Relaxed);
                            }
                        }
                    }
                    if self.cfg.fast_path {
                        r.items.retain(|it| !(it.ok && it.done));
                    }
                    // A failure report counts only from the worker that holds the event now (or when nobody does:
                    // its delivery timed out). A late one from an earlier holder would cancel the current delivery.
                    {
                        let core = self.core.lock().unwrap();
                        r.items.retain(|it| {
                            if it.ok {
                                return true;
                            }
                            let stale = core.groups.get(&it.group).and_then(|st| st.inflight.get(&it.source)).is_some_and(|f| f.pos != Pos::new(it.epoch, it.seq) || f.worker != it.worker_id);
                            if stale {
                                self.stats.stale_failures.fetch_add(1, Relaxed);
                            }
                            !stale
                        });
                    }
                    if !r.items.is_empty() {
                        let _ = res_tx.try_send(PendingResults { reply_to: r.reply_to, items: r.items });
                    }
                    self.wake.notify_one();
                }
                Some(Body::Hint(h)) => {
                    let mut core = self.core.lock().unwrap();
                    for g in h.groups {
                        if let Some(st) = core.groups.get_mut(&g) {
                            st.dirty = true;
                        }
                    }
                    let mut per_group: HashMap<String, Vec<v1::HintEvent>> = HashMap::new();
                    for e in h.events {
                        per_group.entry(e.group.clone()).or_default().push(e);
                    }
                    for (g, events) in per_group {
                        if let Some(st) = core.groups.get_mut(&g) {
                            st.stored(&g, events, self.cfg.fast_path);
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
                            if f.worker == p.worker_id && sources.contains(s) {
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
                w.slots = c.slots;
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
                    WorkerState { inbox: c.inbox, groups: c.groups, credit: c.credit, last_seen: now, slots: c.slots, held: 0 },
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
            if p.items.iter().any(|it| {
                !valid_name(&it.group)
                    || it.source.is_empty()
                    || it.seq < 1
                    || it.epoch < 0
                    || it.skip_to.is_some_and(|k| k < it.seq)
                    || it.prev_epoch.is_some_and(|e| e < 0 || e >= it.epoch)
                    || it.prev_epoch.is_some() != it.prev_last_seq.is_some()
            }) {
                bad[i] = true;
                continue;
            }
            events.extend(p.items.iter().map(|it| Event {
                group: it.group.clone(),
                key: EventKey::new(it.source.clone(), Pos::new(it.epoch, it.seq)),
                body: if it.skip_to.is_some() { Bytes::new() } else { it.body.clone() },
                skip_to: it.skip_to,
                prev: it.prev_epoch.zip(it.prev_last_seq).map(|(epoch, last_seq)| EpochEnd { epoch, last_seq }),
            }));
        }
        let result = self.backend.publish(&events).await;
        self.stats.publish_batches.fetch_add(1, Relaxed);
        let mut stored: HashMap<String, Vec<v1::HintEvent>> = HashMap::new();
        let mut replies: HashMap<String, Vec<v1::Envelope>> = HashMap::new();
        let mut k = 0;
        for (i, p) in batch.into_iter().enumerate() {
            let reply = if bad[i] {
                v1::PublishReply {
                    request_id: p.request_id,
                    status: vec![],
                    error: "invalid group, source, epoch, seq, skip_to or previous epoch".into(),
                    missing_below: vec![],
                }
            } else {
                match &result {
                    Ok(outcomes) => {
                        let mine = &outcomes[k..k + p.items.len()];
                        k += p.items.len();
                        let missing_below = mine.iter().map(|o| o.missing_below).collect();
                        let status = mine
                            .iter()
                            .zip(&p.items)
                            .map(|(o, it)| match o.outcome {
                                PublishOutcome::Stored | PublishOutcome::StoredHead => {
                                    let head = o.outcome == PublishOutcome::StoredHead;
                                    let body = if it.skip_to.is_some() {
                                        Some(Bytes::new())
                                    } else {
                                        (it.body.len() <= HINT_BODY_MAX).then(|| it.body.clone())
                                    };
                                    stored.entry(it.group.clone()).or_default().push(v1::HintEvent {
                                        group: it.group.clone(),
                                        source: it.source.clone(),
                                        seq: it.seq,
                                        head,
                                        body,
                                        enqueued_us: now_us(),
                                        epoch: it.epoch,
                                        skip_to: it.skip_to,
                                    });
                                    self.stats.published.fetch_add(1, Relaxed);
                                    v1::PublishStatus::PublishStored as i32
                                }
                                PublishOutcome::Duplicate => {
                                    self.stats.publish_duplicates.fetch_add(1, Relaxed);
                                    v1::PublishStatus::PublishDuplicate as i32
                                }
                                PublishOutcome::GroupMismatch => v1::PublishStatus::PublishGroupMismatch as i32,
                                PublishOutcome::Conflict => {
                                    self.stats.publish_conflicts.fetch_add(1, Relaxed);
                                    warn!(source = %it.source, epoch = it.epoch, seq = it.seq, "publish conflict: this position is stored with a different body");
                                    v1::PublishStatus::PublishConflict as i32
                                }
                                PublishOutcome::StaleEpoch => {
                                    self.stats.publish_stale_epoch.fetch_add(1, Relaxed);
                                    warn!(source = %it.source, epoch = it.epoch, seq = it.seq, "publish from a stale epoch rejected");
                                    v1::PublishStatus::PublishStaleEpoch as i32
                                }
                                PublishOutcome::Late => {
                                    self.stats.publish_late.fetch_add(1, Relaxed);
                                    v1::PublishStatus::PublishLate as i32
                                }
                            })
                            .collect();
                        v1::PublishReply { request_id: p.request_id, status, error: String::new(), missing_below }
                    }
                    Err(e) => v1::PublishReply { request_id: p.request_id, status: vec![], error: e.to_string(), missing_below: vec![] },
                }
            };
            if !p.reply_to.is_empty() {
                replies.entry(p.reply_to).or_default().push(v1::Envelope { body: Some(Body::PublishReply(reply)) });
            }
        }
        self.send_batched(replies).await;
        if let Err(e) = &result {
            warn!(error = %e, "publish batch failed");
            return;
        }
        // Tell the owners: ourselves directly, the others with a hint carrying the events.
        let mut hints: HashMap<String, Vec<v1::HintEvent>> = HashMap::new();
        {
            let mut core = self.core.lock().unwrap();
            for (g, events) in stored {
                if let Some(st) = core.groups.get_mut(&g) {
                    st.stored(&g, events, self.cfg.fast_path);
                } else if let Some((m, inbox)) = core.owners.get(&g) {
                    if *m != self.cfg.member_id {
                        hints.entry(inbox.clone()).or_default().extend(events);
                    }
                }
            }
        }
        self.wake.notify_one();
        for (inbox, events) in hints {
            self.stats.hints_sent.fetch_add(1, Relaxed);
            let _ = self.fabric.publish(inbox, envelope(Body::Hint(v1::Hint { groups: vec![], events }))).await;
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
            .flat_map(|b| b.items.iter().filter(|r| r.ok && !r.done))
            .map(|r| Commit { key: EventKey::new(r.source.clone(), Pos::new(r.epoch, r.seq)), effect: r.effect.clone() })
            .collect();
        // "done": the worker committed its own writes; remove the event if it is still pending (no hook)
        let removals: Vec<EventKey> = batch
            .iter()
            .flat_map(|b| b.items.iter().filter(|r| r.ok && r.done))
            .map(|r| EventKey::new(r.source.clone(), Pos::new(r.epoch, r.seq)))
            .collect();
        let removed: Vec<Option<(CommitOutcome, Option<Head>)>> = if removals.is_empty() {
            vec![]
        } else {
            match self.backend.remove(&removals).await {
                Ok(o) => o.into_iter().map(Some).collect(),
                Err(e) => {
                    self.stats.commit_errors.fetch_add(1, Relaxed);
                    warn!(error = %e, n = removals.len(), "remove batch failed; retrying");
                    // the workers have committed: the removal must happen; retry it shortly
                    let retry: Vec<v1::WorkResult> =
                        batch.iter().flat_map(|b| b.items.iter().filter(|r| r.ok && r.done).cloned()).collect();
                    if let Some(tx) = self.retry_tx.lock().unwrap().clone() {
                        tokio::spawn(async move {
                            tokio::time::sleep(Duration::from_millis(200)).await;
                            let _ = tx.send(PendingResults { reply_to: String::new(), items: retry }).await;
                        });
                    }
                    vec![None; removals.len()]
                }
            }
        };
        let mut j = 0;
        // One outcome per commit; None = not committed (database error).
        let outcomes: Vec<Option<CommitOutcome>> = if commits.is_empty() {
            vec![]
        } else {
            match self.backend.commit(&commits).await {
                Ok(o) => o.into_iter().map(Some).collect(),
                Err(e) => {
                    self.stats.commit_errors.fetch_add(1, Relaxed);
                    warn!(error = %e, n = commits.len(), "commit batch failed");
                    // One bad event (e.g. its apply hook raises) must not fail its neighbours: commit one by one.
                    let mut each = Vec::with_capacity(commits.len());
                    if commits.len() > 1 {
                        for c in &commits {
                            match self.backend.commit(std::slice::from_ref(c)).await {
                                Ok(o) => each.push(Some(o[0])),
                                Err(e) => {
                                    warn!(error = %e, source = %c.key.source, pos = %c.key.pos, "commit failed");
                                    each.push(None);
                                }
                            }
                        }
                    } else {
                        each.push(None);
                    }
                    each
                }
            }
        };
        self.stats.commit_batches.fetch_add(1, Relaxed);
        let mut k = 0;
        // (group, source, seq, outcome known, for "done" removals: the source's next event) released from in-flight
        let mut done: Vec<(String, String, Pos, bool, Option<Option<Head>>, bool, String)> = vec![];
        let mut result_acks: HashMap<String, Vec<v1::ResultAck>> = HashMap::new();
        for b in batch {
            let mut acks = vec![];
            for r in &b.items {
                let mut next: Option<Option<Head>> = None; // Some: a "done" removal, with the source's next event
                let status = if r.ok && r.done {
                    let (o, n) = match removed[j].clone() {
                        Some((o, n)) => (Some(o), n),
                        None => (None, None),
                    };
                    if o.is_some() {
                        next = Some(n);
                    }
                    let st = match o {
                        Some(CommitOutcome::Applied) => {
                            self.stats.applied.fetch_add(1, Relaxed);
                            v1::ResultStatus::ResultApplied
                        }
                        Some(CommitOutcome::Duplicate) => {
                            self.stats.completed_by_worker.fetch_add(1, Relaxed);
                            v1::ResultStatus::ResultDuplicate
                        }
                        Some(CommitOutcome::Gap) => {
                            self.stats.gaps.fetch_add(1, Relaxed);
                            v1::ResultStatus::ResultGap
                        }
                        None => v1::ResultStatus::ResultError,
                    };
                    j += 1;
                    st
                } else if r.ok {
                    let st = match outcomes[k] {
                        Some(o) => match o {
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
                        None => v1::ResultStatus::ResultError,
                    };
                    k += 1;
                    st
                } else {
                    self.stats.failures.fetch_add(1, Relaxed);
                    let delay = Duration::from_millis(r.retry_delay_ms.max(1) as u64);
                    match self.backend.fail(&EventKey::new(r.source.clone(), Pos::new(r.epoch, r.seq)), delay).await {
                        Ok(()) => v1::ResultStatus::ResultFailureRecorded,
                        Err(e) => {
                            warn!(error = %e, "recording a failure failed");
                            v1::ResultStatus::ResultError
                        }
                    }
                };
                // Release it either way: on an error nothing changed in Octopus's tables, so offer it again now (flagged:
                // a "done" worker has committed its own writes already).
                let known = status != v1::ResultStatus::ResultError && status != v1::ResultStatus::ResultGap;
                done.push((r.group.clone(), r.source.clone(), Pos::new(r.epoch, r.seq), known || !r.done, next, r.ok, r.worker_id.clone()));
                acks.push(v1::ResultAck { request_id: r.request_id, status: status as i32 });
            }
            if !b.reply_to.is_empty() {
                result_acks.entry(b.reply_to).or_default().extend(acks);
            }
        }
        let replies: HashMap<String, Vec<v1::Envelope>> = result_acks
            .into_iter()
            .map(|(to, items)| (to, vec![v1::Envelope { body: Some(Body::ResultReply(v1::ResultReply { items })) }]))
            .collect();
        self.send_batched(replies).await;
        {
            let mut core = self.core.lock().unwrap();
            for (g, s, q, outcome_known, next, ok, w) in done {
                let Core { groups, workers, .. } = &mut *core;
                if let Some(st) = groups.get_mut(&g) {
                    if st.inflight.get(&s).is_some_and(|f| f.pos == q && (ok || f.worker == w)) {
                        if let Some(f) = st.inflight.remove(&s) {
                            unhold(workers, &f.worker);
                        }
                    } else if !ok && st.inflight.contains_key(&s) {
                        // handed out again meanwhile: that delivery stands
                        self.stats.stale_failures.fetch_add(1, Relaxed);
                        continue;
                    }
                    if outcome_known {
                        st.suspect.remove(&(s.clone(), q));
                    } else {
                        st.suspect.insert((s.clone(), q));
                    }
                    match next {
                        // removed: the database told us the source's next event (or that none is due yet)
                        Some(n) if self.cfg.fast_path => {
                            let c = st.cursor.entry(s.clone()).or_default();
                            *c = (*c).max(q);
                            match n {
                                Some(mut h) => {
                                    h.group = g.clone();
                                    st.offer(h);
                                }
                                None => {
                                    if st.known_max.get(&s).is_some_and(|m| *m > q) {
                                        st.dirty = true; // stored meanwhile, or waiting for a retry delay
                                    }
                                }
                            }
                        }
                        // a failure: offer it again by key once its retry delay has passed
                        _ if self.cfg.fast_path && !ok => {
                            st.want.insert(s.clone(), q);
                        }
                        // committed through the apply hook: the database cursor is current, read the next head
                        _ => st.dirty = true,
                    }
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
            let (sends, reqs, keys) = {
                let mut core = self.core.lock().unwrap();
                let sends = core.assign(now, self.cfg.delivery_timeout);
                (sends, core.due_fetches(now, self.cfg.fallback_poll), core.due_key_fetches(now))
            };
            self.send_assignments(sends).await;
            if !keys.is_empty() {
                let permit = fetch_slots.clone().acquire_owned().await.unwrap();
                let me = self.clone();
                tokio::spawn(async move {
                    me.fetch_keys(keys).await;
                    drop(permit);
                });
            }
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
                        // a read that started before a "done" (or before its cursor was flushed) can return events
                        // already done: skip them and look up the real next one by key
                        if let Some(&c) = st.cursor.get(&h.key.source) {
                            if c >= h.key.pos {
                                if !st.inflight.contains_key(&h.key.source) {
                                    st.want.insert(h.key.source.clone(), c.next());
                                }
                                continue;
                            }
                        }
                        let known = st.inflight.contains_key(&h.key.source)
                            || st.ready.iter().any(|r| r.head.key.source == h.key.source);
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
        self.send_assignments(sends).await;
        self.wake.notify_one();
    }

    /// One message per destination: a single envelope as is, several as an EnvelopeBatch.
    async fn send_batched(&self, by_topic: HashMap<String, Vec<v1::Envelope>>) {
        for (topic, mut items) in by_topic {
            let payload = if items.len() == 1 {
                Bytes::from(items.pop().unwrap().encode_to_vec())
            } else {
                envelope(Body::Batch(v1::EnvelopeBatch { items }))
            };
            if let Err(e) = self.fabric.publish(topic, payload).await {
                warn!(error = %e, "reply send failed (the sender retries)");
            }
        }
    }

    /// Look up wanted next events by key and queue the ones found (missing ones stay wanted).
    async fn fetch_keys(&self, keys: Vec<(String, EventKey)>) {
        let groups: HashSet<String> = keys.iter().map(|(g, _)| g.clone()).collect();
        let list: Vec<EventKey> = keys.iter().map(|(_, k)| k.clone()).collect();
        let result = self.backend.read_events(&list).await;
        let now = Instant::now();
        let sends = {
            let mut core = self.core.lock().unwrap();
            if let Ok(heads) = &result {
                let found: HashSet<&EventKey> = heads.iter().map(|h| &h.key).collect();
                // Not stored (yet): stop looking unless a later event of the source is known to be stored (then
                // this one is waiting for a retry delay, or arriving). A next event published later reaches us
                // with its publish (hint), and the fallback head read catches anything lost.
                for (g, k) in &keys {
                    if found.contains(k) {
                        continue;
                    }
                    if let Some(st) = core.groups.get_mut(g) {
                        if st.want.get(&k.source) == Some(&k.pos) && st.known_max.get(&k.source).is_none_or(|m| *m < k.pos) {
                            st.want.remove(&k.source);
                        }
                    }
                }
                for h in heads {
                    if let Some(st) = core.groups.get_mut(&h.group) {
                        if st.want.get(&h.key.source) == Some(&h.key.pos) {
                            st.want.remove(&h.key.source);
                        }
                        st.offer(h.clone());
                    }
                }
            }
            for g in groups {
                if let Some(st) = core.groups.get_mut(&g) {
                    st.want_fetching = false;
                }
            }
            core.assign(now, self.cfg.delivery_timeout)
        };
        if let Err(e) = result {
            warn!(error = %e, "key read failed");
        }
        self.send_assignments(sends).await;
    }

    /// Give up the missing positions of a source stopped by a gap (its group's gap policy): the database records
    /// the skip and moves the cursor if it still stands where the gap was seen; the worker hears of it with the
    /// source's next delivery.
    async fn skip_gap(&self, g: &octopus_core::Gap) {
        // the database cursor must be current ("done" reports are written lazily)
        self.flush_cursors().await;
        let to = (g.first_stored.epoch == g.cursor.epoch).then(|| g.first_stored.seq - 1);
        match self.backend.skip(&g.source, g.cursor, to, "gap timeout").await {
            Ok(Some((new, skipped))) => {
                warn!(group = %g.group, source = %g.source, epoch = skipped.epoch, from = skipped.from_seq, to = ?skipped.to_seq,
                      cursor = %new, waiting_s = g.waiting.as_secs(), "gap skipped after the timeout");
                let mut core = self.core.lock().unwrap();
                core.skips += 1;
                if let Some(st) = core.groups.get_mut(&g.group) {
                    st.skipped.entry(g.source.clone()).or_default().push(skipped);
                    let c = st.cursor.entry(g.source.clone()).or_default();
                    *c = (*c).max(new);
                    if !st.inflight.contains_key(&g.source) {
                        st.want.insert(g.source.clone(), new.next());
                    }
                    st.dirty = true;
                }
                drop(core);
                self.wake.notify_one();
            }
            Ok(None) => debug!(source = %g.source, "gap moved meanwhile: nothing skipped"),
            Err(e) => warn!(error = %e, source = %g.source, "gap skip failed"),
        }
    }

    /// Write the cursors moved by "done" reports (one bulk statement). On failure they are kept for the next try.
    async fn flush_cursors(&self) {
        let batch: Vec<(String, Pos)> = {
            let mut core = self.core.lock().unwrap();
            if core.unflushed.is_empty() {
                return;
            }
            core.unflushed.drain().collect()
        };
        if let Err(e) = self.backend.advance(&batch).await {
            warn!(error = %e, n = batch.len(), "cursor flush failed; will retry");
            let mut core = self.core.lock().unwrap();
            for (s, q) in batch {
                let c = core.unflushed.entry(s).or_default();
                *c = (*c).max(q);
            }
        }
    }

    async fn flush_loop(self: Arc<Self>, mut stop: watch::Receiver<bool>) {
        loop {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(100)) => {}
                _ = stop.changed() => return,
            }
            self.flush_cursors().await;
        }
    }

    async fn send_assignments(&self, (sends, demands): (Vec<(String, Vec<v1::Delivery>)>, Vec<(String, v1::Demand)>)) {
        for (inbox, mut d) in demands {
            d.member_id = self.cfg.member_id.clone();
            d.inbox = self.inbox.clone();
            let _ = self.fabric.publish(inbox, envelope(Body::Demand(d))).await;
        }
        self.send_deliveries(sends).await;
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
                    let GroupState { inflight, suspect, .. } = st;
                    for (s, f) in inflight.iter_mut() {
                        let age = now.duration_since(f.since);
                        if !f.warned && age >= LONG_OUTSTANDING {
                            f.warned = true;
                            warn!(source = %s, pos = %f.pos, worker = %f.worker, age_s = age.as_secs(), "delivery outstanding for long (the worker reports progress)");
                        }
                    }
                    inflight.retain(|s, f| {
                        let keep = f.deadline > now;
                        if !keep {
                            suspect.insert((s.clone(), f.pos)); // a result may have been lost: the worker may have committed
                        }
                        keep
                    });
                    let expired = before - st.inflight.len();
                    if expired > 0 {
                        self.stats.timeouts.fetch_add(expired as u64, Relaxed);
                        st.dirty = true;
                        woke = true;
                    }
                }
                // the exact count of deliveries out per worker (corrects anything the incremental count missed)
                let mut held: HashMap<String, u32> = HashMap::new();
                for st in core.groups.values() {
                    for f in st.inflight.values() {
                        *held.entry(f.worker.clone()).or_default() += 1;
                    }
                }
                for (id, w) in core.workers.iter_mut() {
                    w.held = held.get(id).copied().unwrap_or(0);
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
            // the next owner reads cursors from the database: write ours first
            self.flush_cursors().await;
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
                        // events stored before now (+1 s for clock skew) may have been handed out by the previous owner
                        let claimed_us = now_us() + 1_000_000;
                        core.groups.insert(l.group.clone(), GroupState { serving: true, dirty: true, claimed_us, ..Default::default() });
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
        let mut last_gap_check = Instant::now();
        // (source, cursor) of gaps already reported: each gap is reported once
        let mut reported: HashSet<(String, Pos)> = HashSet::new();
        loop {
            tokio::select! {
                _ = tokio::time::sleep(self.cfg.stats_every.min(self.cfg.gap_check)) => {}
                _ = stop.changed() => return,
            }
            if last_gap_check.elapsed() >= self.cfg.gap_check {
                last_gap_check = Instant::now();
                let owned: Vec<String> = self.core.lock().unwrap().groups.iter().filter(|(_, g)| g.serving).map(|(n, _)| n.clone()).collect();
                // every source stopped for a second or more: reported after gap_alert, skipped after its group's
                // gap policy
                match self.backend.gaps(&owned, self.cfg.gap_alert.min(Duration::from_secs(1)), 1000).await {
                    Ok(gaps) => {
                        let open: HashSet<(String, Pos)> =
                            gaps.iter().filter(|g| g.waiting >= self.cfg.gap_alert).map(|g| (g.source.clone(), g.cursor)).collect();
                        self.stats.gaps_open.store(open.len() as u64, Relaxed);
                        for g in &gaps {
                            if g.waiting >= self.cfg.gap_alert && reported.insert((g.source.clone(), g.cursor)) {
                                let missing_to = if g.first_stored.epoch == g.cursor.epoch {
                                    (g.first_stored.seq - 1).to_string()
                                } else {
                                    "the end of the epoch (no declared end)".into()
                                };
                                warn!(group = %g.group, source = %g.source, epoch = g.cursor.epoch, missing_from = g.cursor.seq + 1,
                                      %missing_to, next_stored = %g.first_stored, waiting_s = g.waiting.as_secs(),
                                      "gap: the source's next event is missing, later events wait");
                            }
                            let policy = match g.gap_skip_ms {
                                Some(ms) if ms < 0 => None,
                                Some(ms) => Some(Duration::from_millis(ms as u64)),
                                None => self.cfg.gap_skip,
                            };
                            if policy.is_some_and(|after| g.waiting >= after) {
                                self.skip_gap(g).await;
                            }
                        }
                        reported.retain(|k| open.contains(k));
                    }
                    Err(e) => warn!(error = %e, "gap check failed"),
                }
            }
            if last_purge.elapsed() >= self.cfg.purge_every {
                last_purge = Instant::now();
                match self.backend.purge_applied(10_000).await {
                    Ok(n) if n > 0 => info!(rows = n, "purged rows at or below their cursor"),
                    Ok(_) => {}
                    Err(e) => warn!(error = %e, "purge failed"),
                }
            }
            let (groups, workers, inflight, ready, skips) = {
                let core = self.core.lock().unwrap();
                (
                    core.groups.values().filter(|g| g.serving).count(),
                    core.workers.len(),
                    core.groups.values().map(|g| g.inflight.len()).sum::<usize>(),
                    core.groups.values().map(|g| g.ready.len()).sum::<usize>(),
                    core.skips,
                )
            };
            let s = &self.stats;
            info!(
                groups, workers, inflight, ready,
                published = s.published.load(Relaxed), applied = s.applied.load(Relaxed),
                completed_by_worker = s.completed_by_worker.load(Relaxed),
                duplicates = s.duplicates.load(Relaxed), failures = s.failures.load(Relaxed),
                timeouts = s.timeouts.load(Relaxed), stale_failures = s.stale_failures.load(Relaxed), gaps_open = s.gaps_open.load(Relaxed),
                publish_conflicts = s.publish_conflicts.load(Relaxed), publish_stale_epoch = s.publish_stale_epoch.load(Relaxed),
                publish_late = s.publish_late.load(Relaxed), skips, commit_batches = s.commit_batches.load(Relaxed),
                fetches = s.fetches.load(Relaxed), "stats"
            );
        }
    }
}
