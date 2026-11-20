//! Integration tests against a real PostgreSQL.
//! Connection: OCTOPUS_TEST_PG (default: host=127.0.0.1 port=5432 user=bench password=bench dbname=octopus_test).
//! Each test uses its own group and source names, so tests run in parallel on one database.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use bytes::Bytes;
use octopus_core::{Backend, Commit, CommitOutcome, Event, EventKey, HeadRequest, Member, PublishOutcome};
use octopus_pg::{PgBackend, PgConfig};

fn conn_str() -> String {
    std::env::var("OCTOPUS_TEST_PG")
        .unwrap_or_else(|_| "host=127.0.0.1 port=5432 user=bench password=bench dbname=octopus_test".into())
}

static COUNTER: AtomicU64 = AtomicU64::new(0);

/// A unique name prefix per test run and test.
fn uniq(tag: &str) -> String {
    let t = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_nanos();
    format!("{tag}-{t:x}-{}", COUNTER.fetch_add(1, Ordering::Relaxed))
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
CREATE OR REPLACE FUNCTION octo_test.load(p_src text[], p_seq bigint[], p_body bytea[])
RETURNS TABLE(i int, context bytea) LANGUAGE sql AS $$
  SELECT o::int, convert_to(s || ':' || q || ':' || convert_from(b, 'UTF8'), 'UTF8')
    FROM unnest(p_src, p_seq, p_body) WITH ORDINALITY AS u(s, q, b, o)
$$;
COMMIT;
"#;

async fn backend() -> PgBackend {
    let (client, conn) = tokio_postgres::connect(&conn_str(), tokio_postgres::NoTls).await.expect("connect");
    tokio::spawn(conn);
    client.batch_execute(HOOKS).await.expect("hooks");
    let mut cfg = PgConfig::new(conn_str());
    cfg.apply_hook = Some("octo_test.apply".into());
    cfg.load_hook = Some("octo_test.load".into());
    PgBackend::connect(cfg).await.expect("backend")
}

async fn applied_rows(source: &str) -> Vec<(i64, Vec<u8>)> {
    let (client, conn) = tokio_postgres::connect(&conn_str(), tokio_postgres::NoTls).await.unwrap();
    tokio::spawn(conn);
    client
        .query("SELECT seq, effect FROM octo_test.applied WHERE source = $1 ORDER BY seq", &[&source])
        .await
        .unwrap()
        .into_iter()
        .map(|r| (r.get(0), r.get(1)))
        .collect()
}

fn ev(group: &str, source: &str, seq: i64, body: &str) -> Event {
    Event { group: group.into(), key: EventKey::new(source, seq), body: Bytes::from(body.to_owned()) }
}

fn commit(source: &str, seq: i64, effect: &str) -> Commit {
    Commit { key: EventKey::new(source, seq), effect: Bytes::from(effect.to_owned()) }
}

fn req(group: &str, max: usize) -> HeadRequest {
    HeadRequest { group: group.into(), max, exclude_sources: vec![] }
}

#[tokio::test]
async fn publish_is_idempotent_and_keeps_the_group() {
    let b = backend().await;
    let (g, g2, s) = (uniq("g"), uniq("g"), uniq("s"));
    let r = b.publish(&[ev(&g, &s, 1, "a"), ev(&g, &s, 2, "b")]).await.unwrap();
    assert_eq!(r, vec![PublishOutcome::Stored, PublishOutcome::Stored]);
    // a producer retry
    let r = b.publish(&[ev(&g, &s, 2, "b"), ev(&g, &s, 3, "c")]).await.unwrap();
    assert_eq!(r, vec![PublishOutcome::Duplicate, PublishOutcome::Stored]);
    // the source belongs to g
    let r = b.publish(&[ev(&g2, &s, 4, "d")]).await.unwrap();
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
    assert_eq!(keys, vec![EventKey::new(&s1, 1)]);
    b.publish(&[ev(&g, &s2, 1, "w"), ev(&g, &s3, 1, "v")]).await.unwrap();
    let heads = b.read_heads(&[req(&g, 10)]).await.unwrap();
    let keys: Vec<_> = heads.iter().map(|h| h.key.clone()).collect();
    assert_eq!(keys, vec![EventKey::new(&s1, 1), EventKey::new(&s2, 1), EventKey::new(&s3, 1)]);
    // limit and exclusion
    let heads = b
        .read_heads(&[HeadRequest { group: g.clone(), max: 1, exclude_sources: vec![s1.clone()] }])
        .await
        .unwrap();
    assert_eq!(heads.len(), 1);
    assert_eq!(heads[0].key, EventKey::new(&s2, 1));
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
    assert_eq!(b.publish(&[ev(&g, &s, 2, "b")]).await.unwrap(), vec![PublishOutcome::Duplicate]);
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
async fn failure_counts_attempts_and_delays_the_head() {
    let b = backend().await;
    let (g, s) = (uniq("g"), uniq("s"));
    b.publish(&[ev(&g, &s, 1, "a")]).await.unwrap();
    b.fail(&EventKey::new(&s, 1), Duration::from_millis(400)).await.unwrap();
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
                let mut batch: Vec<Commit> = heads.iter().map(|h| commit(&h.key.source, h.key.seq, "eff")).collect();
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
        let batch: Vec<Commit> = heads.iter().map(|h| commit(&h.key.source, h.key.seq, "eff")).collect();
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
