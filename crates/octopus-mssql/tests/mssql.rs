//! Integration tests against a real SQL Server 2025: the same tests as the PostgreSQL backend's (crates/octopus-pg/tests).
//! Connection: OCTOPUS_TEST_MSSQL (default: the lab container, database octopus_test).
//! Each test uses its own group and source names, so tests run in parallel on one database.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use octopus_core::{Backend, Commit, CommitOutcome, EpochEnd, Event, EventKey, HeadRequest, Member, Pos, PublishOutcome, Published, Skipped};
use octopus_mssql::{MssqlBackend, MssqlConfig};
use tiberius::{Client, Config};
use tokio::net::TcpStream;
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

fn conn_str() -> String {
    std::env::var("OCTOPUS_TEST_MSSQL").unwrap_or_else(|_| {
        "server=tcp:127.0.0.1,1433;database=octopus_test;user=sa;password=EventBench#2026;TrustServerCertificate=true".into()
    })
}

async fn client() -> Client<Compat<TcpStream>> {
    let cfg = Config::from_ado_string(&conn_str()).unwrap();
    let tcp = TcpStream::connect(cfg.get_addr()).await.unwrap();
    tcp.set_nodelay(true).unwrap();
    Client::connect(cfg, tcp.compat_write()).await.unwrap()
}

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// A unique name prefix per test run and test.
fn uniq(tag: &str) -> String {
    let t = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    format!("{tag}-{t:x}-{}", COUNTER.fetch_add(1, Ordering::Relaxed))
}

/// Test hooks: the apply hook records effects (and fails on the effect "boom"), the load hook returns
/// "source:seq:body" as the context.
const HOOKS: &[&str] = &[
    "IF SCHEMA_ID('octo_test') IS NULL EXEC('CREATE SCHEMA octo_test');
     IF OBJECT_ID('octo_test.applied') IS NULL
         CREATE TABLE octo_test.applied (source nvarchar(256) NOT NULL, seq bigint NOT NULL, effect varbinary(max), PRIMARY KEY (source, seq));",
    "CREATE OR ALTER PROCEDURE octo_test.apply @j nvarchar(max) AS
     BEGIN
         SET NOCOUNT ON;
         IF EXISTS (SELECT 1 FROM OPENJSON(@j) WITH (f varchar(max) '$.f') WHERE BASE64_DECODE(f) = 0x626F6F6D)
             THROW 50000, 'apply hook failed on purpose', 1;
         INSERT INTO octo_test.applied (source, seq, effect)
         SELECT s, q, BASE64_DECODE(f) FROM OPENJSON(@j) WITH (s nvarchar(256) '$.s', q bigint '$.q', f varchar(max) '$.f');
     END",
    "CREATE OR ALTER PROCEDURE octo_test.load_ctx @j nvarchar(max) AS
     BEGIN
         SET NOCOUNT ON;
         SELECT i, CAST(CAST(s + N':' + CAST(q AS nvarchar(20)) + N':' + CAST(CAST(BASE64_DECODE(b) AS varchar(max)) AS nvarchar(max))
                        COLLATE Latin1_General_100_BIN2_UTF8 AS varchar(max)) AS varbinary(max))
           FROM OPENJSON(@j) WITH (i int '$.i', s nvarchar(256) '$.s', q bigint '$.q', b varchar(max) '$.b');
     END",
];

async fn backend() -> MssqlBackend {
    let mut c = client().await;
    // one test at a time installs the hooks (concurrent CREATE OR ALTER would collide)
    c.simple_query("BEGIN TRANSACTION; EXEC sp_getapplock @Resource = N'octopus-test-hooks', @LockMode = 'Exclusive', @LockOwner = 'Transaction';")
        .await.unwrap().into_results().await.unwrap();
    for h in HOOKS {
        c.simple_query(*h).await.unwrap().into_results().await.unwrap();
    }
    c.simple_query("COMMIT").await.unwrap().into_results().await.unwrap();
    let mut cfg = MssqlConfig::new(conn_str());
    cfg.apply_hook = Some("octo_test.apply".into());
    cfg.load_hook = Some("octo_test.load_ctx".into());
    MssqlBackend::connect(cfg).await.expect("backend")
}

async fn applied_rows(source: &str) -> Vec<(i64, Vec<u8>)> {
    let mut c = client().await;
    c.query("SELECT seq, effect FROM octo_test.applied WHERE source = @P1 ORDER BY seq", &[&source])
        .await.unwrap().into_first_result().await.unwrap()
        .iter()
        .map(|r| (r.get::<i64, _>(0).unwrap(), r.get::<&[u8], _>(1).unwrap().to_vec()))
        .collect()
}

/// The epoch of single-epoch tests.
const E: i64 = 1;

fn p(seq: i64) -> Pos {
    Pos::new(E, seq)
}

fn key(source: &str, seq: i64) -> EventKey {
    EventKey::new(source, p(seq))
}

fn ev(group: &str, source: &str, seq: i64, body: &str) -> Event {
    ev_at(group, source, p(seq), body)
}

fn ev_at(group: &str, source: &str, pos: Pos, body: &str) -> Event {
    Event { group: group.into(), key: EventKey::new(source, pos), body: Bytes::from(body.to_owned()), skip_to: None, prev: None }
}

async fn outcomes(b: &MssqlBackend, evs: &[Event]) -> Vec<PublishOutcome> {
    b.publish(evs).await.unwrap().into_iter().map(|p| p.outcome).collect()
}

fn commit(source: &str, seq: i64, effect: &str) -> Commit {
    Commit { key: key(source, seq), effect: Bytes::from(effect.to_owned()) }
}

fn req(group: &str, max: usize) -> HeadRequest {
    HeadRequest { group: group.into(), max, exclude_sources: vec![] }
}

#[tokio::test]
async fn publish_is_idempotent_and_keeps_the_group() {
    let b = backend().await;
    let (g, g2, s) = (uniq("g"), uniq("g"), uniq("s"));
    let r = outcomes(&b, &[ev(&g, &s, 1, "a"), ev(&g, &s, 2, "b")]).await;
    assert_eq!(r, vec![PublishOutcome::StoredHead, PublishOutcome::Stored]);
    // a producer retry
    let r = outcomes(&b, &[ev(&g, &s, 2, "b"), ev(&g, &s, 3, "c")]).await;
    assert_eq!(r, vec![PublishOutcome::Duplicate, PublishOutcome::Stored]);
    // the source belongs to g
    let r = outcomes(&b, &[ev(&g2, &s, 4, "d")]).await;
    assert_eq!(r, vec![PublishOutcome::GroupMismatch]);
    assert!(b.groups().await.unwrap().contains(&g));
}

#[tokio::test]
async fn heads_are_cursor_plus_one_oldest_first() {
    let b = backend().await;
    let g = uniq("g");
    let (s1, s2, s3) = (uniq("s"), uniq("s"), uniq("s"));
    // s2 publishes 2 before 1: its head must still be 1, and only once 1 arrives
    b.publish(&[ev(&g, &s1, 1, "x"), ev(&g, &s1, 2, "y"), ev(&g, &s2, 2, "z")]).await.unwrap();
    let heads = b.read_heads(&[req(&g, 10)]).await.unwrap();
    let keys: Vec<_> = heads.iter().map(|h| h.key.clone()).collect();
    assert_eq!(keys, vec![key(&s1, 1)]);
    b.publish(&[ev(&g, &s2, 1, "w"), ev(&g, &s3, 1, "v")]).await.unwrap();
    let heads = b.read_heads(&[req(&g, 10)]).await.unwrap();
    let keys: Vec<_> = heads.iter().map(|h| h.key.clone()).collect();
    assert_eq!(keys, vec![key(&s1, 1), key(&s2, 1), key(&s3, 1)]);
    // limit and exclusion
    let heads = b
        .read_heads(&[HeadRequest { group: g.clone(), max: 1, exclude_sources: vec![s1.clone()] }])
        .await
        .unwrap();
    assert_eq!(heads.len(), 1);
    assert_eq!(heads[0].key, key(&s2, 1));
    // context from the load hook
    let ctx = b.load_context(&heads).await.unwrap();
    assert_eq!(ctx[0].as_deref(), Some(format!("{s2}:1:w").as_bytes()));
}

#[tokio::test]
async fn commit_applies_once_in_order() {
    let b = backend().await;
    let (g, s) = (uniq("g"), uniq("s"));
    b.publish(&[ev(&g, &s, 1, "a"), ev(&g, &s, 2, "b"), ev(&g, &s, 3, "c")]).await.unwrap();
    // 2 before 1 is a gap
    assert_eq!(b.commit(&[commit(&s, 2, "e2")]).await.unwrap(), vec![CommitOutcome::Gap]);
    assert_eq!(b.commit(&[commit(&s, 1, "e1")]).await.unwrap(), vec![CommitOutcome::Applied]);
    // a duplicate delivery of 1 changes nothing
    assert_eq!(b.commit(&[commit(&s, 1, "again")]).await.unwrap(), vec![CommitOutcome::Duplicate]);
    // a batch may carry consecutive seqs of one source, in any order
    let r = b.commit(&[commit(&s, 3, "e3"), commit(&s, 2, "e2")]).await.unwrap();
    assert_eq!(r, vec![CommitOutcome::Applied, CommitOutcome::Applied]);
    let rows = applied_rows(&s).await;
    assert_eq!(rows, vec![(1, b"e1".to_vec()), (2, b"e2".to_vec()), (3, b"e3".to_vec())]);
    assert!(b.read_heads(&[req(&g, 10)]).await.unwrap().is_empty());
    // publishing an applied event again is a duplicate and stores nothing
    assert_eq!(outcomes(&b, &[ev(&g, &s, 2, "b")]).await, vec![PublishOutcome::Duplicate]);
    assert!(b.read_heads(&[req(&g, 10)]).await.unwrap().is_empty());
}

#[tokio::test]
async fn failing_apply_hook_rolls_back_the_whole_batch() {
    let b = backend().await;
    let (g, s1, s2) = (uniq("g"), uniq("s"), uniq("s"));
    b.publish(&[ev(&g, &s1, 1, "a"), ev(&g, &s2, 1, "b")]).await.unwrap();
    assert!(b.commit(&[commit(&s1, 1, "ok"), commit(&s2, 1, "boom")]).await.is_err());
    assert!(applied_rows(&s1).await.is_empty());
    // nothing moved: both heads are still there
    assert_eq!(b.read_heads(&[req(&g, 10)]).await.unwrap().len(), 2);
    assert_eq!(b.commit(&[commit(&s1, 1, "ok")]).await.unwrap(), vec![CommitOutcome::Applied]);
}

#[tokio::test]
async fn remove_returns_the_next_event() {
    let b = backend().await;
    let (g, s) = (uniq("g"), uniq("s"));
    b.publish(&[ev(&g, &s, 1, "a"), ev(&g, &s, 2, "b")]).await.unwrap();
    let r = b.remove(&[key(&s, 1)]).await.unwrap();
    assert_eq!(r[0].0, CommitOutcome::Applied);
    let next = r[0].1.as_ref().expect("seq 2 is next");
    assert_eq!((next.key.pos.seq, next.body.as_ref()), (2, &b"b"[..]));
    // again: already removed; the next event is still 2
    let r = b.remove(&[key(&s, 1)]).await.unwrap();
    assert_eq!(r[0].0, CommitOutcome::Duplicate);
    let r = b.remove(&[key(&s, 2)]).await.unwrap();
    assert_eq!(r[0], (CommitOutcome::Applied, None));
    // a publish of the now-next event reports it as the head
    assert_eq!(outcomes(&b, &[ev(&g, &s, 3, "c")]).await, vec![PublishOutcome::StoredHead]);
}

#[tokio::test]
async fn advance_is_monotonic_and_read_events_finds_by_key() {
    let b = backend().await;
    let (g, s) = (uniq("g"), uniq("s"));
    b.publish(&[ev(&g, &s, 1, "a"), ev(&g, &s, 2, "b"), ev(&g, &s, 3, "c")]).await.unwrap();
    let found = b.read_events(&[key(&s, 2), key(&s, 9)]).await.unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!((found[0].group.as_str(), found[0].body.as_ref()), (g.as_str(), &b"b"[..]));
    // done up to 2, delivered out of order and repeated: the cursor ends at 2, events 1 and 2 are gone
    b.advance(&[(s.clone(), p(2)), (s.clone(), p(1))]).await.unwrap();
    b.advance(&[(s.clone(), p(1))]).await.unwrap();
    assert!(b.read_events(&[key(&s, 1), key(&s, 2)]).await.unwrap().is_empty());
    let heads = b.read_heads(&[req(&g, 10)]).await.unwrap();
    assert_eq!(heads.iter().map(|h| h.key.clone()).collect::<Vec<_>>(), vec![key(&s, 3)]);
}

#[tokio::test]
async fn failure_counts_attempts_and_delays_the_head() {
    let b = backend().await;
    let (g, s) = (uniq("g"), uniq("s"));
    b.publish(&[ev(&g, &s, 1, "a")]).await.unwrap();
    b.fail(&key(&s, 1), Duration::from_millis(400)).await.unwrap();
    assert!(b.read_heads(&[req(&g, 10)]).await.unwrap().is_empty());
    tokio::time::sleep(Duration::from_millis(500)).await;
    let heads = b.read_heads(&[req(&g, 10)]).await.unwrap();
    assert_eq!(heads.len(), 1);
    assert_eq!(heads[0].attempts, 1);
}

#[tokio::test]
async fn leases_are_exclusive_until_released_or_expired() {
    let b = backend().await;
    let (g1, g2) = (uniq("g"), uniq("g"));
    let (m1, m2) = (uniq("m"), uniq("m"));
    let groups = vec![g1.clone(), g2.clone()];
    let ttl = Duration::from_millis(600);
    for m in [&m1, &m2] {
        b.heartbeat(&Member { id: m.clone(), address: format!("addr/{m}") }, ttl).await.unwrap();
    }
    let got = b.claim(&m1, &groups, ttl).await.unwrap();
    assert_eq!(got.len(), 2);
    assert!(b.claim(&m2, &groups, ttl).await.unwrap().is_empty(), "held by m1");
    assert_eq!(b.renew(&m1, &groups, ttl).await.unwrap().len(), 2);
    b.release(&m1, &[g1.clone()]).await.unwrap();
    let got = b.claim(&m2, &groups, ttl).await.unwrap();
    assert_eq!(got.iter().map(|l| l.group.clone()).collect::<Vec<_>>(), vec![g1.clone()]);
    // m1 stops renewing g2: after the TTL m2 takes it, with a higher epoch
    tokio::time::sleep(Duration::from_millis(800)).await;
    let got = b.claim(&m2, &[g2.clone()], ttl).await.unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].epoch, 2);
    assert!(b.renew(&m1, &groups, ttl).await.unwrap().is_empty(), "m1 lost both");
    b.leave(&m1).await.unwrap();
    b.leave(&m2).await.unwrap();
}

/// Producers publish out of order with retries; competing "dispatchers" read heads without coordinating and
/// commit duplicates, stale and early events at random. Every event must be applied exactly once, in order.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_chaos_applies_each_event_once_in_order() {
    let b = Arc::new(backend().await);
    let g = uniq("g");
    let sources: Vec<String> = (0..20).map(|_| uniq("s")).collect();
    const N: i64 = 30;

    let mut tasks = vec![];
    for p in 0..4 {
        let (b, g, sources) = (b.clone(), g.clone(), sources.clone());
        tasks.push(tokio::spawn(async move {
            // each producer publishes every event (so each is published 4 times), in a shuffled order
            let mut all: Vec<(String, i64)> = sources.iter().flat_map(|s| (1..=N).map(move |q| (s.clone(), q))).collect();
            let mut x: u64 = 0x9e3779b97f4a7c15 ^ p;
            for i in (1..all.len()).rev() {
                x ^= x << 13; x ^= x >> 7; x ^= x << 17;
                all.swap(i, (x % (i as u64 + 1)) as usize);
            }
            for chunk in all.chunks(25) {
                let evs: Vec<Event> = chunk.iter().map(|(s, q)| ev(&g, s, *q, &format!("{s}/{q}"))).collect();
                b.publish(&evs).await.unwrap();
            }
        }));
    }
    for w in 0..6u64 {
        let (b, g, sources) = (b.clone(), g.clone(), sources.clone());
        tasks.push(tokio::spawn(async move {
            let mut x: u64 = 0x2545F4914F6CDD1D ^ (w + 1);
            let mut rnd = move || { x ^= x << 13; x ^= x >> 7; x ^= x << 17; x };
            for _ in 0..400 {
                let heads = b.read_heads(&[req(&g, 8)]).await.unwrap();
                let mut batch: Vec<Commit> = heads.iter().map(|h| commit(&h.key.source, h.key.pos.seq, "eff")).collect();
                // add a stale duplicate and an early (gap) commit now and then
                let s = &sources[(rnd() % sources.len() as u64) as usize];
                batch.push(commit(s, 1 + (rnd() % N as u64) as i64, "noise"));
                b.commit(&batch).await.unwrap();
                if heads.is_empty() { tokio::time::sleep(Duration::from_millis(5)).await; }
            }
        }));
    }
    for t in tasks { t.await.unwrap(); }
    // finish whatever is left
    loop {
        let heads = b.read_heads(&[req(&g, 100)]).await.unwrap();
        if heads.is_empty() { break; }
        let batch: Vec<Commit> = heads.iter().map(|h| commit(&h.key.source, h.key.pos.seq, "eff")).collect();
        b.commit(&batch).await.unwrap();
    }
    let mut per_source: HashMap<String, Vec<i64>> = HashMap::new();
    for s in &sources {
        per_source.insert(s.clone(), applied_rows(s).await.into_iter().map(|(q, _)| q).collect());
    }
    for (s, seqs) in per_source {
        assert_eq!(seqs, (1..=N).collect::<Vec<_>>(), "source {s}: every seq applied exactly once");
    }
}

#[tokio::test]
async fn a_reused_seq_with_a_different_body_is_a_conflict() {
    let b = backend().await;
    let (g, s) = (uniq("g-conf"), uniq("s-conf"));
    assert_eq!(outcomes(&b, &[ev(&g, &s, 1, "a"), ev(&g, &s, 2, "b")]).await, vec![PublishOutcome::StoredHead, PublishOutcome::Stored]);
    // the same body again: a retry; a different body under a stored number: a conflict, nothing changes
    assert_eq!(outcomes(&b, &[ev(&g, &s, 2, "b")]).await, vec![PublishOutcome::Duplicate]);
    assert_eq!(outcomes(&b, &[ev(&g, &s, 2, "B")]).await, vec![PublishOutcome::Conflict]);
    let got = b.read_events(&[key(&s, 2)]).await.unwrap();
    assert_eq!(got[0].body, Bytes::from("b"));
    // once applied, the row is gone: a repeat cannot be compared and is a duplicate
    b.advance(&[(s.clone(), p(2))]).await.unwrap();
    assert_eq!(outcomes(&b, &[ev(&g, &s, 2, "B")]).await, vec![PublishOutcome::Duplicate]);
}

#[tokio::test]
async fn gaps_report_sources_whose_next_event_is_missing() {
    let b = backend().await;
    let g = uniq("g-gap");
    let (holed, whole) = (uniq("s-holed"), uniq("s-whole"));
    // holed: 1 applied, 2 and 3 missing, 4 and 5 stored; whole: 1, 2 stored (no gap)
    b.publish(&[ev(&g, &holed, 1, "x"), ev(&g, &holed, 4, "x"), ev(&g, &holed, 5, "x"), ev(&g, &whole, 1, "x"), ev(&g, &whole, 2, "x")])
        .await
        .unwrap();
    b.advance(&[(holed.clone(), p(1))]).await.unwrap();
    // not old enough yet
    assert!(b.gaps(&[g.clone()], Duration::from_secs(60), 10).await.unwrap().is_empty());
    let gaps = b.gaps(&[g.clone()], Duration::ZERO, 10).await.unwrap();
    assert_eq!(gaps.len(), 1, "{gaps:?}");
    assert_eq!((gaps[0].source.as_str(), gaps[0].cursor, gaps[0].first_stored), (holed.as_str(), p(1), p(4)));
    // the missing events arrive: no gap any more
    b.publish(&[ev(&g, &holed, 2, "x"), ev(&g, &holed, 3, "x")]).await.unwrap();
    assert!(b.gaps(&[g.clone()], Duration::ZERO, 10).await.unwrap().is_empty());
}

async fn heads_of(b: &MssqlBackend, g: &str) -> Vec<EventKey> {
    b.read_heads(&[HeadRequest { group: g.into(), max: 100, exclude_sources: vec![] }]).await.unwrap().into_iter().map(|h| h.key).collect()
}

/// (epoch, from_seq, to_seq, reason) of the source's recorded skips, and the positions in the late table.
async fn skips_and_late(source: &str) -> (Vec<(i64, i64, Option<i64>, String)>, Vec<(i64, i64)>) {
    let mut c = client().await;
    let sk = c
        .query("SELECT k.epoch, k.from_seq, k.to_seq, k.reason FROM octopus.skipped k JOIN octopus.source s ON s.id = k.sid \
                 WHERE s.source = CAST(@P1 COLLATE Latin1_General_100_BIN2_UTF8 AS varchar(256)) ORDER BY 1, 2", &[&source])
        .await.unwrap().into_first_result().await.unwrap()
        .iter()
        .map(|r| (r.get::<i64, _>(0).unwrap(), r.get::<i64, _>(1).unwrap(), r.get::<i64, _>(2), r.get::<&str, _>(3).unwrap().to_owned()))
        .collect();
    let late = c
        .query("SELECT l.epoch, l.seq FROM octopus.late l JOIN octopus.source s ON s.id = l.sid \
                 WHERE s.source = CAST(@P1 COLLATE Latin1_General_100_BIN2_UTF8 AS varchar(256)) ORDER BY 1, 2", &[&source])
        .await.unwrap().into_first_result().await.unwrap()
        .iter()
        .map(|r| (r.get::<i64, _>(0).unwrap(), r.get::<i64, _>(1).unwrap()))
        .collect();
    (sk, late)
}

#[tokio::test]
async fn publish_reports_how_many_positions_below_are_missing() {
    let b = backend().await;
    let (g, s) = (uniq("g-miss"), uniq("s-miss"));
    let r = b.publish(&[ev(&g, &s, 1, "a"), ev(&g, &s, 3, "c")]).await.unwrap();
    assert_eq!(r, vec![Published { outcome: PublishOutcome::StoredHead, missing_below: 0 }, Published { outcome: PublishOutcome::Stored, missing_below: 1 }]);
    // batchmates count as present; an applied position counts as present
    b.advance(&[(s.clone(), p(1))]).await.unwrap();
    let r: Vec<i64> = b.publish(&[ev(&g, &s, 6, "f"), ev(&g, &s, 5, "e")]).await.unwrap().into_iter().map(|p| p.missing_below).collect();
    assert_eq!(r, vec![0, 1], "6 follows 5 (same batch); 4 is missing below 5");
    // the gap filled: a retry of 3 now reports nothing missing below it (2 is still missing)
    let r = b.publish(&[ev(&g, &s, 2, "b"), ev(&g, &s, 4, "d")]).await.unwrap();
    assert!(r.iter().all(|p| p.missing_below == 0), "{r:?}");
}

#[tokio::test]
async fn a_new_epoch_follows_the_declared_end_of_the_previous_one() {
    let b = backend().await;
    let (g, s) = (uniq("g-ep"), uniq("s-ep"));
    b.publish(&[ev(&g, &s, 1, "a"), ev(&g, &s, 2, "b")]).await.unwrap();
    // the source restarts with epoch 7, saying epoch 1 ended at 2
    let mut first = ev_at(&g, &s, Pos::new(7, 1), "x");
    first.prev = Some(EpochEnd { epoch: E, last_seq: 2 });
    assert_eq!(outcomes(&b, &[first, ev_at(&g, &s, Pos::new(7, 2), "y")]).await, vec![PublishOutcome::Stored, PublishOutcome::Stored]);
    assert_eq!(heads_of(&b, &g).await, vec![key(&s, 1)], "epoch 1 goes first");
    b.advance(&[(s.clone(), p(2))]).await.unwrap();
    assert_eq!(heads_of(&b, &g).await, vec![EventKey::new(&s, Pos::new(7, 1))], "then epoch 7 from its start");
    // fencing: an old-epoch position beyond its declared end is a stale producer; within it, a duplicate
    assert_eq!(outcomes(&b, &[ev(&g, &s, 3, "z"), ev(&g, &s, 2, "b")]).await, vec![PublishOutcome::StaleEpoch, PublishOutcome::Duplicate]);

    // a declaration arriving after the old epoch is already done moves the cursor at once
    let s2 = uniq("s-ep2");
    b.publish(&[ev(&g, &s2, 1, "a")]).await.unwrap();
    b.advance(&[(s2.clone(), p(1))]).await.unwrap();
    let mut first = ev_at(&g, &s2, Pos::new(9, 1), "x");
    first.prev = Some(EpochEnd { epoch: E, last_seq: 1 });
    assert_eq!(outcomes(&b, &[first]).await, vec![PublishOutcome::StoredHead]);
}

#[tokio::test]
async fn skip_markers_are_jumped_over_and_late_arrivals_kept_apart() {
    let b = backend().await;
    let (g, s) = (uniq("g-mark"), uniq("s-mark"));
    let mut marker = ev(&g, &s, 2, "");
    marker.skip_to = Some(4);
    b.publish(&[ev(&g, &s, 1, "a"), marker, ev(&g, &s, 5, "e")]).await.unwrap();
    b.advance(&[(s.clone(), p(1))]).await.unwrap();
    let heads = b.read_heads(&[HeadRequest { group: g.clone(), max: 10, exclude_sources: vec![] }]).await.unwrap();
    assert_eq!((heads[0].key.clone(), heads[0].skip_to), (key(&s, 2), Some(4)), "the marker is the head (the dispatcher consumes it)");
    // the dispatcher jumps the cursor to the marker's end: the skip is recorded, 5 is next
    b.advance(&[(s.clone(), p(4))]).await.unwrap();
    assert_eq!(heads_of(&b, &g).await, vec![key(&s, 5)]);
    assert_eq!(outcomes(&b, &[ev(&g, &s, 3, "c")]).await, vec![PublishOutcome::Late]);
    let (sk, late) = skips_and_late(&s).await;
    assert_eq!(sk, vec![(E, 2, Some(4), "declared by the source".to_owned())]);
    assert_eq!(late, vec![(E, 3)]);
}

#[tokio::test]
async fn a_gap_can_be_skipped_within_an_epoch_or_by_closing_it() {
    let b = backend().await;
    let (g, s) = (uniq("g-skip"), uniq("s-skip"));
    b.publish(&[ev(&g, &s, 1, "a"), ev(&g, &s, 4, "d")]).await.unwrap();
    b.advance(&[(s.clone(), p(1))]).await.unwrap();
    // a decision taken on an outdated cursor changes nothing
    assert_eq!(b.skip(&s, p(0), Some(3), "test").await.unwrap(), None);
    let r = b.skip(&s, p(1), Some(3), "test").await.unwrap();
    assert_eq!(r, Some((p(3), Skipped { epoch: E, from_seq: 2, to_seq: Some(3) })));
    assert_eq!(heads_of(&b, &g).await, vec![key(&s, 4)]);
    assert_eq!(outcomes(&b, &[ev(&g, &s, 2, "b")]).await, vec![PublishOutcome::Late]);

    // epoch 1 stops at 4 with no declared end while epoch 2 has events: close epoch 1
    b.advance(&[(s.clone(), p(4))]).await.unwrap();
    b.publish(&[ev_at(&g, &s, Pos::new(2, 1), "x")]).await.unwrap();
    let gaps = b.gaps(&[g.clone()], Duration::ZERO, 10).await.unwrap();
    assert_eq!((gaps[0].cursor, gaps[0].first_stored), (p(4), Pos::new(2, 1)));
    let r = b.skip(&s, p(4), None, "test").await.unwrap();
    assert_eq!(r, Some((Pos::new(2, 0), Skipped { epoch: E, from_seq: 5, to_seq: None })));
    assert_eq!(heads_of(&b, &g).await, vec![EventKey::new(&s, Pos::new(2, 1))]);
    assert_eq!(outcomes(&b, &[ev(&g, &s, 9, "late")]).await, vec![PublishOutcome::Late]);
}

#[tokio::test]
async fn a_skip_never_gives_up_an_event_stored_meanwhile() {
    let b = backend().await;
    let (g, s) = (uniq("g-race"), uniq("s-race"));
    b.publish(&[ev(&g, &s, 1, "a"), ev(&g, &s, 4, "d")]).await.unwrap();
    b.advance(&[(s.clone(), p(1))]).await.unwrap();
    // the gap 2..=3 was seen, but 2 arrives before the skip runs: only 3 may be given up... and 3 is not even
    // the next position, so nothing is skipped now
    assert_eq!(outcomes(&b, &[ev(&g, &s, 2, "b")]).await, vec![PublishOutcome::StoredHead]);
    assert_eq!(b.skip(&s, p(1), Some(3), "test").await.unwrap(), None);
    assert_eq!(heads_of(&b, &g).await, vec![key(&s, 2)]);
    // and an epoch with stored positions beyond the cursor is not closed: the skip stops below them
    b.advance(&[(s.clone(), p(2))]).await.unwrap();
    b.publish(&[ev_at(&g, &s, Pos::new(2, 1), "x")]).await.unwrap();
    let r = b.skip(&s, p(2), None, "test").await.unwrap();
    assert_eq!(r, Some((p(3), Skipped { epoch: E, from_seq: 3, to_seq: Some(3) })));
    assert_eq!(heads_of(&b, &g).await, vec![key(&s, 4)]);
}
