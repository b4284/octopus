//! End to end: real `octopusd` processes, an MQTT broker, PostgreSQL, and in-process producers and workers.
//! Needs Mosquitto on 127.0.0.1:1883 (`dev/up.sh`) and PostgreSQL (OCTOPUS_TEST_PG, see the octopus-pg tests).
//! Each test uses its own database schema and topic namespace.

use std::collections::HashMap;
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::Bytes;
use octopus_client::{Delivery, Handler, Outcome, Producer, PublishItem, Worker, WorkerConfig};
use octopus_fabric::MqttConfig;
use tokio::sync::watch;

fn pg() -> String {
    std::env::var("OCTOPUS_TEST_PG")
        .unwrap_or_else(|_| "host=127.0.0.1 port=5432 user=bench password=bench dbname=octopus_test".into())
}

static N: AtomicU64 = AtomicU64::new(0);

fn uniq(tag: &str) -> String {
    let t = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() % 100_000_000;
    format!("{tag}_{t}_{}", N.fetch_add(1, Ordering::Relaxed))
}

const HOOKS: &str = r#"
BEGIN;
SELECT pg_advisory_xact_lock(hashtext('octopus.test.hooks'));
CREATE SCHEMA IF NOT EXISTS octo_test;
CREATE TABLE IF NOT EXISTS octo_test.applied (source text, seq bigint, effect bytea, PRIMARY KEY (source, seq));
CREATE OR REPLACE FUNCTION octo_test.apply(p_src text[], p_seq bigint[], p_eff bytea[]) RETURNS void
LANGUAGE plpgsql AS $$
BEGIN
  IF 'boom'::bytea = ANY(p_eff) THEN RAISE EXCEPTION 'apply hook failed on purpose'; END IF;
  INSERT INTO octo_test.applied SELECT * FROM unnest(p_src, p_seq, p_eff);
END $$;
COMMIT;
"#;

async fn db() -> tokio_postgres::Client {
    let (c, conn) = tokio_postgres::connect(&pg(), tokio_postgres::NoTls).await.expect("postgres");
    tokio::spawn(conn);
    c
}

struct Dispatcher(Child);

impl Dispatcher {
    fn start(ns: &str, member: &str) -> Self {
        Self::start_with(ns, member, Some("octo_test.apply"))
    }

    fn start_with(ns: &str, member: &str, apply_hook: Option<&str>) -> Self {
        let mut cmd = Command::new(env!("CARGO_BIN_EXE_octopusd"));
        if let Some(h) = apply_hook {
            cmd.env("OCTOPUS_APPLY_HOOK", h);
        }
        let child = cmd
            .env("OCTOPUS_MEMBER_ID", member)
            .env("OCTOPUS_NS", ns)
            .env("OCTOPUS_SCHEMA", ns)
            .env("OCTOPUS_PG", pg())
            .env("OCTOPUS_LEASE_TTL_MS", "1500")
            .env("OCTOPUS_TICK_MS", "250")
            .env("OCTOPUS_DELIVERY_TIMEOUT_MS", "3000")
            .env("RUST_LOG", std::env::var("OCTOPUS_TEST_LOG").unwrap_or_else(|_| "warn".into()))
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("start octopusd");
        Dispatcher(child)
    }

    /// Like a crashed pod: no drain, no lease release, the connection just drops.
    fn kill(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }

    /// Planned stop (SIGTERM): drain, release the leases, leave.
    fn terminate(&mut self) {
        let _ = Command::new("kill").arg("-TERM").arg(self.0.id().to_string()).status();
        let _ = self.0.wait();
    }
}

impl Drop for Dispatcher {
    fn drop(&mut self) {
        self.kill();
    }
}

/// Commits everything; the effect "boom" makes the apply hook raise for one chosen event, for a while.
struct PoisonOnce {
    poison: (String, i64),
    hits: AtomicU64,
}

impl Handler for PoisonOnce {
    async fn handle(&self, d: Delivery) -> Outcome {
        if d.source == self.poison.0 && d.seq == self.poison.1 && self.hits.fetch_add(1, Ordering::Relaxed) < 3 {
            return Outcome::Commit(Bytes::from_static(b"boom"));
        }
        Outcome::Commit(Bytes::from(format!("{}:{}", d.source, d.seq)))
    }
}

/// Fails ~10% of attempts and is slow now and then.
struct Flaky {
    calls: AtomicU64,
}

impl Handler for Flaky {
    async fn handle(&self, d: Delivery) -> Outcome {
        let n = self.calls.fetch_add(1, Ordering::Relaxed);
        let h = (n.wrapping_mul(0x9E3779B97F4A7C15) ^ d.seq as u64) % 100;
        if h < 10 {
            return Outcome::Retry(Duration::from_millis(20));
        }
        if h < 13 {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Outcome::Commit(Bytes::from(format!("{}:{}", d.source, d.seq)))
    }
}

async fn start_workers(ns: &str, groups: &[String], n: usize, slots: u32) -> (Vec<tokio::task::JoinHandle<()>>, watch::Sender<bool>) {
    let (stop_tx, stop_rx) = watch::channel(false);
    let mut tasks = vec![];
    for i in 0..n {
        let id = format!("{ns}-w{i}");
        let cfg = WorkerConfig::new(id.clone(), ns, groups.to_vec(), slots);
        let (w, rx) = Worker::connect(MqttConfig::new("127.0.0.1", 1883, id), cfg, Flaky { calls: AtomicU64::new(i as u64) })
            .await
            .expect("worker");
        tasks.push(tokio::spawn(w.run(rx, stop_rx.clone())));
    }
    (tasks, stop_tx)
}

/// Publishes `seqs` events for each source, each source in order, sources concurrently; every 7th event is
/// published twice (a producer retry).
async fn publish_all(ns: &str, groups: &[String], sources: &[String], seqs: i64) {
    let producer = Producer::connect(MqttConfig::new("127.0.0.1", 1883, format!("{ns}-p")), ns, &format!("{ns}-p")).await.unwrap();
    let mut tasks = vec![];
    for (i, s) in sources.iter().enumerate() {
        let (p, s, g) = (producer.clone(), s.clone(), groups[i % groups.len()].clone());
        tasks.push(tokio::spawn(async move {
            for q in 1..=seqs {
                let item = PublishItem { group: g.clone(), source: s.clone(), seq: q, body: Bytes::from(format!("{s}/{q}")) };
                p.publish(vec![item.clone()]).await.expect("publish");
                if q % 7 == 0 {
                    p.publish(vec![item]).await.expect("republish");
                }
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
}

/// Waits until every source has applied 1..=seqs, then checks each was applied exactly once, in order.
async fn wait_all_applied(sources: &[String], seqs: i64, limit: Duration) {
    let c = db().await;
    let want = sources.len() as i64 * seqs;
    let until = Instant::now() + limit;
    loop {
        let row = c.query_one("SELECT count(*) FROM octo_test.applied WHERE source = ANY($1)", &[&sources]).await.unwrap();
        let n: i64 = row.get(0);
        if n == want {
            break;
        }
        assert!(Instant::now() < until, "only {n} of {want} events applied in {limit:?}");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let rows = c.query("SELECT source, seq FROM octo_test.applied WHERE source = ANY($1) ORDER BY source, seq", &[&sources]).await.unwrap();
    let mut per: HashMap<String, Vec<i64>> = HashMap::new();
    for r in rows {
        per.entry(r.get(0)).or_default().push(r.get(1));
    }
    for s in sources {
        assert_eq!(per.get(s).cloned().unwrap_or_default(), (1..=seqs).collect::<Vec<_>>(), "source {s}");
    }
}

async fn setup(ns: &str, groups: usize, sources: usize) -> (Vec<String>, Vec<String>) {
    db().await.batch_execute(HOOKS).await.unwrap();
    let groups: Vec<String> = (0..groups).map(|i| format!("g{i}")).collect();
    let sources: Vec<String> = (0..sources).map(|i| format!("{ns}-s{i}")).collect();
    (groups, sources)
}

async fn backlog(schema: &str) -> i64 {
    let c = db().await;
    let r = c
        .query_one(&format!("SELECT count(*) FROM {schema}.event e JOIN {schema}.source s USING (source) WHERE e.seq > s.last_seq"), &[])
        .await
        .unwrap();
    r.get(0)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_dispatcher_end_to_end() {
    let ns = uniq("e2e");
    let (groups, sources) = setup(&ns, 4, 40).await;
    let _d = Dispatcher::start(&ns, "d1");
    let (workers, stop) = start_workers(&ns, &groups, 2, 4).await;
    publish_all(&ns, &groups, &sources, 25).await;
    wait_all_applied(&sources, 25, Duration::from_secs(60)).await;
    assert_eq!(backlog(&ns).await, 0);
    let _ = stop.send(true);
    for w in workers {
        let _ = w.await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn three_dispatchers_survive_a_crash_and_a_planned_stop() {
    let ns = uniq("e2e");
    let (groups, sources) = setup(&ns, 12, 60).await;
    let mut d1 = Dispatcher::start(&ns, "d1");
    let mut d2 = Dispatcher::start(&ns, "d2");
    let _d3 = Dispatcher::start(&ns, "d3");
    let (workers, stop) = start_workers(&ns, &groups, 3, 4).await;
    let killer = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_secs(2)).await;
        d1.kill(); // crash: its groups move when the leases expire
        tokio::time::sleep(Duration::from_secs(3)).await;
        d2.terminate(); // planned: drains and hands over at once
    });
    publish_all(&ns, &groups, &sources, 60).await;
    wait_all_applied(&sources, 60, Duration::from_secs(120)).await;
    killer.await.unwrap();
    assert_eq!(backlog(&ns).await, 0);
    let _ = stop.send(true);
    for w in workers {
        let _ = w.await;
    }
}

/// One event's apply hook raises (three times): its neighbours in the same commit batch must still commit, and
/// the event itself must be applied once the hook stops failing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failing_apply_hook_does_not_hold_back_other_sources() {
    let ns = uniq("e2e");
    let (groups, sources) = setup(&ns, 2, 30).await;
    let _d = Dispatcher::start(&ns, "d1");
    let (stop_tx, stop_rx) = watch::channel(false);
    let id = format!("{ns}-w0");
    let handler = PoisonOnce { poison: (sources[3].clone(), 5), hits: AtomicU64::new(0) };
    let (w, rx) = Worker::connect(MqttConfig::new("127.0.0.1", 1883, id.clone()), WorkerConfig::new(id, ns.clone(), groups.clone(), 16), handler)
        .await
        .unwrap();
    let worker = tokio::spawn(w.run(rx, stop_rx));
    publish_all(&ns, &groups, &sources, 20).await;
    wait_all_applied(&sources, 20, Duration::from_secs(60)).await;
    let _ = stop_tx.send(true);
    let _ = worker.await;
}

/// Workers own their writes and never touch Octopus's tables: BEGIN; skip if (source, seq) is already in their table
/// (the idempotency key); INSERT; COMMIT; then report done. One time in ten the worker "crashes" right after its
/// commit (the done report is lost): Octopus hands the event out again after the delivery timeout, flagged
/// possibly_redelivered, the worker finds it already applied, and the table still holds each event exactly once.
struct OwnCommit {
    calls: AtomicU64,
    unflagged_repeats: Arc<AtomicU64>,
    pool: deadpool_postgres::Pool,
}

fn pool(size: usize) -> deadpool_postgres::Pool {
    let cfg: tokio_postgres::Config = pg().parse().unwrap();
    let mgr = deadpool_postgres::Manager::new(cfg, tokio_postgres::NoTls);
    deadpool_postgres::Pool::builder(mgr).max_size(size).build().unwrap()
}

impl Handler for OwnCommit {
    async fn handle(&self, d: Delivery) -> Outcome {
        let mut c = self.pool.get().await.unwrap();
        let tx = c.transaction().await.unwrap();
        let n = tx
            .execute("INSERT INTO octo_test.applied VALUES ($1, $2, 'own') ON CONFLICT (source, seq) DO NOTHING", &[&d.source, &d.seq])
            .await
            .unwrap();
        tx.commit().await.unwrap();
        if n == 0 && !d.possibly_redelivered {
            self.unflagged_repeats.fetch_add(1, Ordering::Relaxed); // a repeat Octopus did not warn about
        }
        if n == 1 && self.calls.fetch_add(1, Ordering::Relaxed) % 10 == 3 {
            return Outcome::Vanish; // committed, then "crashed" before saying done
        }
        Outcome::Done
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn workers_commit_their_own_writes_and_report_done() {
    let ns = uniq("e2e");
    let (groups, sources) = setup(&ns, 3, 20).await;
    let _d1 = Dispatcher::start_with(&ns, "d1", None);
    let _d2 = Dispatcher::start_with(&ns, "d2", None);
    let (stop_tx, stop_rx) = watch::channel(false);
    let unflagged = Arc::new(AtomicU64::new(0));
    let mut workers = vec![];
    for i in 0..2 {
        let id = format!("{ns}-w{i}");
        let h = OwnCommit { calls: AtomicU64::new(i), unflagged_repeats: unflagged.clone(), pool: pool(8) };
        // every worker serves every group, so its slots are split between two owning dispatchers
        let (w, rx) = Worker::connect(MqttConfig::new("127.0.0.1", 1883, id.clone()), WorkerConfig::new(id, ns.clone(), groups.clone(), 8), h)
            .await
            .unwrap();
        workers.push(tokio::spawn(w.run(rx, stop_rx.clone())));
    }
    publish_all(&ns, &groups, &sources, 15).await;
    wait_all_applied(&sources, 15, Duration::from_secs(90)).await;
    // the worker's table can be complete while Octopus still holds events whose done report was lost: they come
    // back after the delivery timeout, are found applied, reported done, and removed
    let until = Instant::now() + Duration::from_secs(20);
    while backlog(&ns).await > 0 {
        assert!(Instant::now() < until, "backlog did not drain");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    assert_eq!(unflagged.load(Ordering::Relaxed), 0, "every repeated delivery must be flagged possibly_redelivered");
    let _ = stop_tx.send(true);
    for w in workers {
        let _ = w.await;
    }
}

/// Applies each source strictly in order in memory, counting anything out of order.
struct OrderCheck {
    last: Mutex<HashMap<String, i64>>,
    out_of_order: Arc<AtomicU64>,
    applied: Arc<AtomicU64>,
}

impl Handler for OrderCheck {
    async fn handle(&self, d: Delivery) -> Outcome {
        let mut last = self.last.lock().unwrap();
        let l = last.entry(d.source.clone()).or_insert(0);
        if d.seq == *l + 1 {
            *l = d.seq;
            self.applied.fetch_add(1, Ordering::Relaxed);
        } else if d.seq > *l + 1 {
            self.out_of_order.fetch_add(1, Ordering::Relaxed);
        } // else: a repeat of something applied
        Outcome::Done
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn events_published_out_of_order_lost_and_repeated_are_handed_out_in_order() {
    let ns = uniq("e2e");
    let (groups, sources) = setup(&ns, 3, 30).await;
    let seqs: i64 = 20;
    let _d1 = Dispatcher::start_with(&ns, "d1", None);
    let _d2 = Dispatcher::start_with(&ns, "d2", None);
    let (stop_tx, stop_rx) = watch::channel(false);
    let (bad, applied) = (Arc::new(AtomicU64::new(0)), Arc::new(AtomicU64::new(0)));
    // one shared order check: both workers serve every group, so a source's events go to either
    let check = Arc::new(OrderCheck { last: Mutex::default(), out_of_order: bad.clone(), applied: applied.clone() });
    struct Shared(Arc<OrderCheck>);
    impl Handler for Shared {
        async fn handle(&self, d: Delivery) -> Outcome {
            self.0.handle(d).await
        }
    }
    let mut workers = vec![];
    for i in 0..2 {
        let id = format!("{ns}-w{i}");
        let (w, rx) = Worker::connect(MqttConfig::new("127.0.0.1", 1883, id.clone()), WorkerConfig::new(id, ns.clone(), groups.clone(), 8), Shared(check.clone()))
            .await
            .unwrap();
        workers.push(tokio::spawn(w.run(rx, stop_rx.clone())));
    }
    let producer = Producer::connect(MqttConfig::new("127.0.0.1", 1883, format!("{ns}-p")), &ns, &format!("{ns}-p")).await.unwrap();
    // every event at once, in a scrambled order; about 1 in 10 "lost" (sent 3 s late); 1 in 7 sent twice
    let mut all: Vec<(usize, i64)> = (0..sources.len()).flat_map(|i| (1..=seqs).map(move |q| (i, q))).collect();
    all.sort_by_key(|&(i, q)| (q as usize * 7919 + i * 104_729) % 1_000_003);
    let mut tasks = vec![];
    for (i, q) in all {
        let (p, s, g) = (producer.clone(), sources[i].clone(), groups[i % groups.len()].clone());
        let late = (i as i64 * 31 + q * 17) % 10 == 0 && q > 1;
        tasks.push(tokio::spawn(async move {
            if late {
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
            let item = PublishItem { group: g, source: s.clone(), seq: q, body: Bytes::from(format!("{s}/{q}")) };
            p.publish(vec![item.clone()]).await.expect("publish");
            if q % 7 == 0 {
                p.publish(vec![item]).await.expect("republish");
            }
        }));
    }
    for t in tasks {
        t.await.unwrap();
    }
    let want = sources.len() as u64 * seqs as u64;
    let until = Instant::now() + Duration::from_secs(60);
    while applied.load(Ordering::Relaxed) < want {
        assert!(Instant::now() < until, "only {} of {want} applied", applied.load(Ordering::Relaxed));
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_eq!(bad.load(Ordering::Relaxed), 0, "an event was handed out before its predecessor was done");
    let until = Instant::now() + Duration::from_secs(20);
    while backlog(&ns).await > 0 {
        assert!(Instant::now() < until, "backlog did not drain");
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let _ = stop_tx.send(true);
    for w in workers {
        let _ = w.await;
    }
}
