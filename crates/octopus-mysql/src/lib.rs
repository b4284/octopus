//! MySQL 8.4 (InnoDB) backend for Octopus.
//!
//! The same model and guarantees as the PostgreSQL backend; the logic lives in stored procedures
//! (`sql/001_init.sql`). A batch travels as one JSON parameter (bodies in base64), unpacked with `JSON_TABLE`.
//! Sessions run READ COMMITTED (no gap locks). Tables are named `<prefix>grp`, `<prefix>event`, ... in the
//! connection's database (MySQL has no schemas inside a database); the prefix defaults to `octopus_`.
//!
//! Connection: a URL, e.g. `mysql://octopus:secret@127.0.0.1:3306/app`.

use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use bytes::Bytes;
use mysql_async::prelude::*;
use mysql_async::{Opts, OptsBuilder, Pool, PoolConstraints, PoolOpts, Row};
use octopus_core::{
    Backend, Commit, CommitOutcome, Error, Event, EventKey, Gap, Head, HeadRequest, Lease, Member, Pos, PublishOutcome, Published,
    Result, Skipped,
};
use serde::Serialize;

const SCHEMA_SQL: &str = include_str!("../sql/001_init.sql");

/// Connection and hook settings.
#[derive(Clone, Debug)]
pub struct MysqlConfig {
    /// `mysql://user:password@host:port/database`
    pub connection: String,
    pub pool_size: usize,
    /// Create or update the schema on connect.
    pub migrate: bool,
    /// Table name prefix (default `octopus_`): separate prefixes isolate tenants or tests.
    pub prefix: String,
    /// Apply hook: a procedure `(j JSON)` called in the commit transaction with the applied events
    /// `[{"s": source, "q": seq, "f": base64 effect}]`.
    pub apply_hook: Option<String>,
    /// Load hook: a procedure `(j JSON)` with `[{"i", "s", "q", "b": base64 body}]` returning rows `(i, context)`.
    pub load_hook: Option<String>,
}

impl MysqlConfig {
    pub fn new(connection: impl Into<String>) -> Self {
        Self { connection: connection.into(), pool_size: 16, migrate: true, prefix: "octopus_".into(), apply_hook: None, load_hook: None }
    }
}

fn db(e: impl std::fmt::Display) -> Error {
    Error::Database(e.to_string())
}

/// Deadlock victim (1213) or lock wait timeout (1205): the transaction was rolled back; a retry is safe (every
/// operation is idempotent or monotonic).
fn retryable(e: &mysql_async::Error) -> bool {
    matches!(e, mysql_async::Error::Server(s) if s.code == 1213 || s.code == 1205)
}

pub struct MysqlBackend {
    pool: Pool,
    known: std::sync::Mutex<std::collections::HashSet<String>>,
    prefix: String,
    apply_hook: Option<String>,
    load_hook: Option<String>,
    deadlocks: std::sync::atomic::AtomicU64,
}

const KNOWN_MAX: usize = 1_000_000;

/// The script's statements (separated by lines holding only $$), for the given prefix.
fn schema_statements(prefix: &str) -> Vec<String> {
    let sql = SCHEMA_SQL.replace("octopus_", prefix);
    let mut out = vec![];
    let mut cur = String::new();
    for line in sql.lines() {
        if line.trim() == "$$" {
            if !cur.trim().is_empty() {
                out.push(std::mem::take(&mut cur));
            }
            cur.clear();
        } else if !(cur.is_empty() && line.trim_start().starts_with("--")) {
            cur.push_str(line);
            cur.push('\n');
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur);
    }
    out
}

#[derive(Serialize)]
struct KeyJ<'a> {
    i: usize,
    s: &'a str,
    e: i64,
    q: i64,
}

fn keys_json<'a>(it: impl Iterator<Item = (&'a str, Pos)>) -> String {
    serde_json::to_string(&it.enumerate().map(|(i, (s, p))| KeyJ { i: i + 1, s, e: p.epoch, q: p.seq }).collect::<Vec<_>>()).unwrap()
}

fn i64_(r: &Row, i: usize) -> i64 {
    r.get::<Option<i64>, _>(i).flatten().unwrap_or_default()
}
fn opt_i64(r: &Row, i: usize) -> Option<i64> {
    r.get::<Option<i64>, _>(i).flatten()
}
fn s(r: &Row, i: usize) -> String {
    r.get::<Option<String>, _>(i).flatten().unwrap_or_default()
}
fn bytes(r: &Row, i: usize) -> Bytes {
    Bytes::from(r.get::<Option<Vec<u8>>, _>(i).flatten().unwrap_or_default())
}

/// A head from columns (grp, source, epoch, seq, body, attempts, enqueued_us, skip_to).
fn head_row(r: &Row) -> Head {
    Head {
        group: s(r, 0),
        key: EventKey::new(s(r, 1), Pos::new(i64_(r, 2), i64_(r, 3))),
        body: bytes(r, 4),
        attempts: r.get::<Option<i32>, _>(5).flatten().unwrap_or_default(),
        enqueued_at_us: i64_(r, 6),
        skip_to: opt_i64(r, 7),
    }
}

fn outcome(code: i64) -> CommitOutcome {
    match code {
        0 => CommitOutcome::Applied,
        1 => CommitOutcome::Duplicate,
        _ => CommitOutcome::Gap,
    }
}

fn ms(d: Duration) -> i64 {
    d.as_millis().min(i64::MAX as u128) as i64
}

impl MysqlBackend {
    pub async fn connect(cfg: MysqlConfig) -> Result<Self> {
        if !cfg.prefix.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_') || cfg.prefix.is_empty() {
            return Err(Error::Config(format!("table prefix {:?}: use [a-z0-9_]", cfg.prefix)));
        }
        let opts = Opts::from_url(&cfg.connection).map_err(|e| Error::Config(format!("connection: {e}")))?;
        let constraints = PoolConstraints::new(1, cfg.pool_size.max(1)).expect("pool constraints");
        let opts = OptsBuilder::from_opts(opts)
            // no COM_RESET_CONNECTION when a connection goes back to the pool: it would drop the procedures'
            // per-session scratch tables and re-run the session setup on every operation (matrix-31: ~70
            // connection setups per second). Every procedure empties its scratch tables before use, and a failed one
            // is rolled back by the caller.
            .pool_opts(PoolOpts::default().with_constraints(constraints).with_reset_connection(false))
            .tcp_nodelay(true)
            .init(vec![
                "SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED",
                "SET SESSION time_zone = '+00:00'",
            ]);
        let b = Self {
            pool: Pool::new(opts),
            known: Default::default(),
            prefix: cfg.prefix.clone(),
            apply_hook: cfg.apply_hook,
            load_hook: cfg.load_hook,
            deadlocks: Default::default(),
        };
        if cfg.migrate {
            b.migrate().await?;
        }
        for h in b.apply_hook.iter().chain(b.load_hook.iter()) {
            if !h.chars().all(|c| c.is_ascii_alphanumeric() || c == '_') {
                return Err(Error::Config(format!("hook {h}: a procedure name of [A-Za-z0-9_]")));
            }
            let n: i64 = b
                .rows("SELECT COUNT(*) FROM information_schema.routines WHERE routine_schema = DATABASE() AND routine_name = ? AND routine_type = 'PROCEDURE'", (h.as_str(),))
                .await?
                .first()
                .map(|r| i64_(r, 0))
                .unwrap_or(0);
            if n == 0 {
                return Err(Error::Config(format!("hook {h}: not a stored procedure in this database")));
            }
        }
        Ok(b)
    }

    /// Deadlock victims retried so far (statistics).
    pub fn deadlocks(&self) -> u64 {
        self.deadlocks.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Runs the schema script when the installed version differs from this build's (hash in <prefix>meta), one
    /// migrator at a time (GET_LOCK), re-checking under the lock. DDL is not transactional in MySQL: every statement
    /// of the script is idempotent.
    async fn migrate(&self) -> Result<()> {
        let stmts = schema_statements(&self.prefix);
        let version = format!("{:016x}", xxhash_rust::xxh3::xxh3_64(stmts.concat().as_bytes()));
        let meta = format!("{}meta", self.prefix);
        let mut c = self.pool.get_conn().await.map_err(db)?;
        let check = format!("SELECT v FROM {meta} WHERE k = 'schema'");
        // (no meta table yet: an error, read as "no version")
        if c.query_first::<String, _>(check.as_str()).await.ok().flatten().as_deref() == Some(version.as_str()) {
            return Ok(());
        }
        let got: Option<i64> = c.query_first("SELECT GET_LOCK('octopus-migrate', 600)").await.map_err(db)?;
        if got != Some(1) {
            return Err(Error::Database("could not take the migration lock".into()));
        }
        let r: Result<()> = async {
            if c.query_first::<String, _>(check.as_str()).await.ok().flatten().as_deref() != Some(version.as_str()) {
                for st in &stmts {
                    c.query_drop(st.as_str()).await.map_err(|e| db(format!("{e}\n-- in: {}", st.lines().next().unwrap_or(""))))?;
                }
                c.query_drop(format!("CREATE TABLE IF NOT EXISTS {meta} (k VARCHAR(64) PRIMARY KEY, v VARCHAR(64) NOT NULL) ENGINE = InnoDB"))
                    .await
                    .map_err(db)?;
                c.exec_drop(format!("INSERT INTO {meta} (k, v) VALUES ('schema', ?) ON DUPLICATE KEY UPDATE v = VALUES(v)"), (version.as_str(),))
                    .await
                    .map_err(db)?;
            }
            Ok(())
        }
        .await;
        let _ = c.query_drop("SELECT RELEASE_LOCK('octopus-migrate')").await;
        r
    }

    /// Statement text with the configured prefix.
    fn q(&self, sql: &str) -> String {
        sql.replace("octopus_", &self.prefix)
    }

    /// All rows of the first result set; deadlock victims are retried.
    async fn rows<P: Into<mysql_async::Params> + Clone + Send>(&self, sql: &str, params: P) -> Result<Vec<Row>> {
        let mut attempt = 0u64;
        loop {
            let mut c = self.pool.get_conn().await.map_err(db)?;
            match c.exec::<Row, _, _>(sql, params.clone()).await {
                Ok(r) => return Ok(r),
                Err(e) if retryable(&e) && attempt < 10 => {
                    attempt += 1;
                    self.deadlocks.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    // a procedure that failed mid-transaction may leave it open on this connection
                    let _ = c.query_drop("ROLLBACK").await;
                    tokio::time::sleep(Duration::from_millis(2 * attempt * attempt)).await;
                }
                Err(e) => {
                    let _ = c.query_drop("ROLLBACK").await;
                    return Err(db(e));
                }
            }
        }
    }

    async fn exec<P: Into<mysql_async::Params> + Clone + Send>(&self, sql: &str, params: P) -> Result<()> {
        self.rows(sql, params).await.map(|_| ())
    }
}

impl Backend for MysqlBackend {
    async fn publish(&self, events: &[Event]) -> Result<Vec<Published>> {
        if events.is_empty() {
            return Ok(vec![]);
        }
        if events.iter().any(|e| e.key.pos.seq < 1 || e.key.pos.epoch < 0 || e.skip_to.is_some_and(|k| k < e.key.pos.seq)) {
            return Err(Error::Invalid("seq starts at 1, epoch is not negative, skip_to >= seq".into()));
        }
        #[derive(Serialize)]
        struct P<'a> {
            i: usize,
            g: &'a str,
            s: &'a str,
            e: i64,
            q: i64,
            b: String,
            h: i64,
            k: Option<i64>,
            pe: Option<i64>,
            pl: Option<i64>,
        }
        let items: Vec<P> = events
            .iter()
            .enumerate()
            .map(|(i, e)| P {
                i: i + 1,
                g: &e.group,
                s: &e.key.source,
                e: e.key.pos.epoch,
                q: e.key.pos.seq,
                b: B64.encode(&e.body),
                h: xxhash_rust::xxh3::xxh3_64(&e.body) as i64,
                k: e.skip_to,
                pe: e.prev.map(|p| p.epoch),
                pl: e.prev.map(|p| p.last_seq),
            })
            .collect();
        let all_known = {
            let k = self.known.lock().unwrap();
            events.iter().all(|e| k.contains(&e.key.source) && k.contains(&format!("\u{1}{}", e.group)))
        };
        let j = serde_json::to_string(&items).unwrap();
        let rows = self.rows(&self.q("CALL octopus_publish(?, ?)"), (j, all_known as i64)).await?;
        if !all_known {
            let mut k = self.known.lock().unwrap();
            if k.len() > KNOWN_MAX {
                k.clear();
            }
            for e in events {
                k.insert(e.key.source.clone());
                k.insert(format!("\u{1}{}", e.group));
            }
        }
        let mut out = vec![Published { outcome: PublishOutcome::Duplicate, missing_below: 0 }; events.len()];
        for r in rows {
            let i = i64_(&r, 0) as usize;
            if i == 0 || i > out.len() {
                continue;
            }
            out[i - 1] = Published {
                outcome: match i64_(&r, 1) {
                    0 => PublishOutcome::Stored,
                    3 => PublishOutcome::StoredHead,
                    2 => PublishOutcome::GroupMismatch,
                    4 => PublishOutcome::Conflict,
                    5 => PublishOutcome::StaleEpoch,
                    6 => PublishOutcome::Late,
                    _ => PublishOutcome::Duplicate,
                },
                missing_below: i64_(&r, 2),
            };
        }
        Ok(out)
    }

    async fn read_heads(&self, requests: &[HeadRequest]) -> Result<Vec<Head>> {
        if requests.is_empty() {
            return Ok(vec![]);
        }
        #[derive(Serialize)]
        struct G<'a> {
            g: &'a str,
            n: i64,
        }
        let groups = serde_json::to_string(&requests.iter().map(|r| G { g: &r.group, n: r.max.min(i32::MAX as usize) as i64 }).collect::<Vec<_>>()).unwrap();
        let exclude = serde_json::to_string(&requests.iter().flat_map(|r| r.exclude_sources.iter()).collect::<Vec<_>>()).unwrap();
        // per group: the heads (cursor + 1) of its sources, oldest first, at most n (a window function: LATERAL
        // with LIMIT per group)
        let sql = self.q(
            "SELECT t.grp, t.source, t.epoch, t.seq, t.body, t.attempts, t.enq, t.skip_to FROM ( \
               SELECT e.grp, s.source, e.epoch, e.seq, e.body, e.attempts, \
                      CAST(TIMESTAMPDIFF(MICROSECOND, '1970-01-01', e.enqueued_at) AS SIGNED) AS enq, e.skip_to, gq.n, \
                      ROW_NUMBER() OVER (PARTITION BY s.grp ORDER BY e.enqueued_at, s.source) AS rn \
                 FROM JSON_TABLE(?, '$[*]' COLUMNS (g VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin PATH '$.g', n INT PATH '$.n')) gq \
                 JOIN octopus_source s ON s.grp = gq.g \
                 JOIN octopus_event e ON e.sid = s.id AND e.epoch = s.epoch AND e.seq = s.last_seq + 1 \
                WHERE s.source NOT IN (SELECT x.v FROM JSON_TABLE(?, '$[*]' COLUMNS (v VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin PATH '$')) x) \
                  AND (e.not_before IS NULL OR e.not_before <= UTC_TIMESTAMP(6))) t \
              WHERE t.rn <= t.n",
        );
        Ok(self.rows(&sql, (groups, exclude)).await?.iter().map(head_row).collect())
    }

    async fn load_context(&self, heads: &[Head]) -> Result<Vec<Option<Bytes>>> {
        let mut out = vec![None; heads.len()];
        let Some(hook) = &self.load_hook else { return Ok(out) };
        if heads.is_empty() {
            return Ok(out);
        }
        #[derive(Serialize)]
        struct L<'a> {
            i: usize,
            s: &'a str,
            q: i64,
            b: String,
        }
        let j = serde_json::to_string(
            &heads.iter().enumerate().map(|(i, h)| L { i: i + 1, s: &h.key.source, q: h.key.pos.seq, b: B64.encode(&h.body) }).collect::<Vec<_>>(),
        )
        .unwrap();
        for r in self.rows(&format!("CALL `{hook}`(?)"), (j,)).await? {
            let i = i64_(&r, 0) as usize;
            if i >= 1 && i <= out.len() {
                out[i - 1] = r.get::<Option<Vec<u8>>, _>(1).flatten().map(Bytes::from);
            }
        }
        Ok(out)
    }

    async fn commit(&self, commits: &[Commit]) -> Result<Vec<CommitOutcome>> {
        if commits.is_empty() {
            return Ok(vec![]);
        }
        #[derive(Serialize)]
        struct C<'a> {
            i: usize,
            s: &'a str,
            e: i64,
            q: i64,
            f: String,
        }
        let j = serde_json::to_string(
            &commits
                .iter()
                .enumerate()
                .map(|(i, c)| C { i: i + 1, s: &c.key.source, e: c.key.pos.epoch, q: c.key.pos.seq, f: B64.encode(&c.effect) })
                .collect::<Vec<_>>(),
        )
        .unwrap();
        let hook: Option<String> = self.apply_hook.clone();
        let rows = self.rows(&self.q("CALL octopus_commit_many(?, ?)"), (j, hook)).await?;
        let mut out = vec![CommitOutcome::Gap; commits.len()];
        for r in rows {
            let i = i64_(&r, 0) as usize;
            if i >= 1 && i <= out.len() {
                out[i - 1] = outcome(i64_(&r, 1));
            }
        }
        Ok(out)
    }

    async fn remove(&self, keys: &[EventKey]) -> Result<Vec<(CommitOutcome, Option<Head>)>> {
        if keys.is_empty() {
            return Ok(vec![]);
        }
        let j = keys_json(keys.iter().map(|k| (k.source.as_str(), k.pos)));
        let rows = self.rows(&self.q("CALL octopus_remove_next(?)"), (j,)).await?;
        let mut out: Vec<(CommitOutcome, Option<Head>)> = vec![(CommitOutcome::Gap, None); keys.len()];
        for r in rows {
            let i = i64_(&r, 0) as usize;
            if i == 0 || i > out.len() {
                continue;
            }
            let next = match (opt_i64(&r, 2), opt_i64(&r, 3)) {
                (Some(e), Some(q)) => Some(Head {
                    group: String::new(), // filled in by the caller
                    key: EventKey::new(keys[i - 1].source.clone(), Pos::new(e, q)),
                    body: bytes(&r, 4),
                    attempts: r.get::<Option<i32>, _>(5).flatten().unwrap_or_default(),
                    enqueued_at_us: i64_(&r, 6),
                    skip_to: opt_i64(&r, 7),
                }),
                _ => None,
            };
            out[i - 1] = (outcome(i64_(&r, 1)), next);
        }
        Ok(out)
    }

    async fn advance(&self, cursors: &[(String, Pos)]) -> Result<()> {
        if cursors.is_empty() {
            return Ok(());
        }
        let j = keys_json(cursors.iter().map(|(s, p)| (s.as_str(), *p)));
        self.exec(&self.q("CALL octopus_advance(?)"), (j,)).await
    }

    async fn read_events(&self, keys: &[EventKey]) -> Result<Vec<Head>> {
        if keys.is_empty() {
            return Ok(vec![]);
        }
        let j = keys_json(keys.iter().map(|k| (k.source.as_str(), k.pos)));
        let sql = self.q(
            "SELECT e.grp, k.s, e.epoch, e.seq, e.body, e.attempts, \
                    CAST(TIMESTAMPDIFF(MICROSECOND, '1970-01-01', e.enqueued_at) AS SIGNED), e.skip_to \
               FROM JSON_TABLE(?, '$[*]' COLUMNS (s VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin PATH '$.s', \
                                                  e BIGINT PATH '$.e', q BIGINT PATH '$.q')) k \
               JOIN octopus_source s ON s.source = k.s \
               JOIN octopus_event e ON e.sid = s.id AND e.epoch = k.e AND e.seq = k.q \
              WHERE e.not_before IS NULL OR e.not_before <= UTC_TIMESTAMP(6)",
        );
        Ok(self.rows(&sql, (j,)).await?.iter().map(head_row).collect())
    }

    async fn fail(&self, key: &EventKey, delay: Duration) -> Result<()> {
        let sql = self.q(
            "UPDATE octopus_event e JOIN octopus_source s ON s.id = e.sid \
                SET e.attempts = e.attempts + 1, e.not_before = TIMESTAMPADD(MICROSECOND, ? * 1000, UTC_TIMESTAMP(6)) \
              WHERE s.source = ? AND e.epoch = ? AND e.seq = ?",
        );
        self.exec(&sql, (ms(delay), key.source.as_str(), key.pos.epoch, key.pos.seq)).await
    }

    async fn missing_up_to(&self, positions: &[(String, Pos)]) -> Result<Vec<i64>> {
        if positions.is_empty() {
            return Ok(vec![]);
        }
        let j = keys_json(positions.iter().map(|(s, p)| (s.as_str(), *p)));
        let sql = self.q(
            "SELECT k.i, CASE WHEN s.id IS NULL THEN k.q WHEN k.e < s.epoch THEN 0 \
                    ELSE GREATEST(0, k.q - GREATEST(CASE WHEN k.e = s.epoch THEN s.last_seq ELSE 0 END, \
                         COALESCE((SELECT COALESCE(y.skip_to, y.seq) FROM octopus_event y \
                                    WHERE y.sid = s.id AND y.epoch = k.e AND y.seq <= k.q ORDER BY y.seq DESC LIMIT 1), 0))) END \
               FROM JSON_TABLE(?, '$[*]' COLUMNS (i INT PATH '$.i', s VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin PATH '$.s', \
                                                  e BIGINT PATH '$.e', q BIGINT PATH '$.q')) k \
               LEFT JOIN octopus_source s ON s.source = k.s ORDER BY k.i",
        );
        Ok(self.rows(&sql, (j,)).await?.iter().map(|r| i64_(r, 1)).collect())
    }

    async fn skip(&self, source: &str, after: Pos, to_seq: Option<i64>, reason: &str) -> Result<Option<(Pos, Skipped)>> {
        let rows = self
            .rows(&self.q("CALL octopus_skip(?, ?, ?, ?, ?)"), (source, after.epoch, after.seq, to_seq, reason))
            .await?;
        let Some(r) = rows.first() else { return Ok(None) };
        Ok(match (opt_i64(r, 0), opt_i64(r, 1)) {
            (Some(e), Some(q)) => Some((
                Pos::new(e, q),
                Skipped { epoch: after.epoch, from_seq: opt_i64(r, 2).unwrap_or(after.seq + 1), to_seq: opt_i64(r, 3) },
            )),
            _ => None,
        })
    }

    async fn groups(&self) -> Result<Vec<String>> {
        Ok(self.rows(&self.q("SELECT name FROM octopus_grp ORDER BY name"), ()).await?.iter().map(|r| s(r, 0)).collect())
    }

    async fn backlog(&self) -> Result<i64> {
        let rows = self
            .rows(&self.q("SELECT COUNT(*) FROM octopus_event e JOIN octopus_source s ON s.id = e.sid \
                     WHERE e.epoch > s.epoch OR (e.epoch = s.epoch AND e.seq > s.last_seq)"), ())
            .await?;
        Ok(rows.first().map(|r| i64_(r, 0)).unwrap_or(0))
    }

    async fn heartbeat(&self, member: &Member, ttl: Duration) -> Result<Vec<Member>> {
        self.exec(
            &self.q("INSERT INTO octopus_member (member_id, address, heartbeat) VALUES (?, ?, UTC_TIMESTAMP(6)) \
                     ON DUPLICATE KEY UPDATE address = VALUES(address), heartbeat = VALUES(heartbeat)"),
            (member.id.as_str(), member.address.as_str()),
        )
        .await?;
        let rows = self
            .rows(&self.q("SELECT member_id, address FROM octopus_member \
                     WHERE heartbeat > TIMESTAMPADD(MICROSECOND, -? * 1000, UTC_TIMESTAMP(6)) ORDER BY member_id"), (ms(ttl),))
            .await?;
        Ok(rows.iter().map(|r| Member { id: s(r, 0), address: s(r, 1) }).collect())
    }

    async fn renew(&self, member: &str, groups: &[String], ttl: Duration) -> Result<Vec<String>> {
        if groups.is_empty() {
            return Ok(vec![]);
        }
        let g = serde_json::to_string(groups).unwrap();
        // lock in group order, extend, then read back what this member holds (UPDATE has no RETURNING in MySQL)
        let sql = self.q(
            "SELECT l.grp FROM octopus_lease l \
               JOIN JSON_TABLE(?, '$[*]' COLUMNS (v VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin PATH '$')) x ON x.v = l.grp \
              WHERE l.owner = ? ORDER BY l.grp FOR UPDATE OF l",
        );
        let mut c = self.pool.get_conn().await.map_err(db)?;
        let r: std::result::Result<Vec<String>, mysql_async::Error> = async {
            let mut tx = c.start_transaction(Default::default()).await?;
            let held: Vec<String> = tx.exec(sql.as_str(), (g.as_str(), member)).await?;
            if !held.is_empty() {
                let ids = serde_json::to_string(&held).unwrap();
                tx.exec_drop(
                    self.q("UPDATE octopus_lease l JOIN JSON_TABLE(?, '$[*]' COLUMNS (v VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin PATH '$')) x \
                              ON x.v = l.grp SET l.expires_at = TIMESTAMPADD(MICROSECOND, ? * 1000, UTC_TIMESTAMP(6)) WHERE l.owner = ?")
                        .as_str(),
                    (ids.as_str(), ms(ttl), member),
                )
                .await?;
            }
            tx.commit().await?;
            Ok(held)
        }
        .await;
        r.map_err(db)
    }

    async fn claim(&self, member: &str, groups: &[String], ttl: Duration) -> Result<Vec<Lease>> {
        if groups.is_empty() {
            return Ok(vec![]);
        }
        let mut sorted: Vec<&String> = groups.iter().collect();
        sorted.sort();
        sorted.dedup();
        let g = serde_json::to_string(&sorted).unwrap();
        let mut c = self.pool.get_conn().await.map_err(db)?;
        let r: std::result::Result<Vec<Lease>, mysql_async::Error> = async {
            let mut tx = c.start_transaction(Default::default()).await?;
            // free or expired leases (or our own): take them over with a higher epoch
            tx.exec_drop(
                self.q("UPDATE octopus_lease l JOIN JSON_TABLE(?, '$[*]' COLUMNS (v VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin PATH '$')) x \
                          ON x.v = l.grp \
                          SET l.epoch = l.epoch + 1, l.owner = ?, l.expires_at = TIMESTAMPADD(MICROSECOND, ? * 1000, UTC_TIMESTAMP(6)) \
                        WHERE l.expires_at < UTC_TIMESTAMP(6) OR l.owner = ?")
                    .as_str(),
                (g.as_str(), member, ms(ttl), member),
            )
            .await?;
            // groups with no lease yet (a concurrent claimer may win the key: IGNORE)
            tx.exec_drop(
                self.q("INSERT IGNORE INTO octopus_lease (grp, owner, epoch, expires_at) \
                        SELECT x.v, ?, 1, TIMESTAMPADD(MICROSECOND, ? * 1000, UTC_TIMESTAMP(6)) \
                          FROM JSON_TABLE(?, '$[*]' COLUMNS (v VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin PATH '$')) x \
                         ORDER BY x.v")
                    .as_str(),
                (member, ms(ttl), g.as_str()),
            )
            .await?;
            let rows: Vec<(String, String, i64)> = tx
                .exec(
                    self.q("SELECT l.grp, l.owner, l.epoch FROM octopus_lease l \
                              JOIN JSON_TABLE(?, '$[*]' COLUMNS (v VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin PATH '$')) x ON x.v = l.grp \
                             WHERE l.owner = ? AND l.expires_at > UTC_TIMESTAMP(6) ORDER BY l.grp")
                        .as_str(),
                    (g.as_str(), member),
                )
                .await?;
            tx.commit().await?;
            Ok(rows.into_iter().map(|(group, owner, epoch)| Lease { group, owner, epoch }).collect())
        }
        .await;
        r.map_err(db)
    }

    async fn release(&self, member: &str, groups: &[String]) -> Result<()> {
        if groups.is_empty() {
            return Ok(());
        }
        let g = serde_json::to_string(groups).unwrap();
        self.exec(
            &self.q("DELETE l FROM octopus_lease l \
                       JOIN JSON_TABLE(?, '$[*]' COLUMNS (v VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin PATH '$')) x ON x.v = l.grp \
                      WHERE l.owner = ?"),
            (g, member),
        )
        .await
    }

    async fn leave(&self, member: &str) -> Result<()> {
        self.exec(&self.q("DELETE FROM octopus_lease WHERE owner = ?"), (member,)).await?;
        self.exec(&self.q("DELETE FROM octopus_member WHERE member_id = ?"), (member,)).await
    }

    async fn leases(&self) -> Result<Vec<(Lease, String)>> {
        let rows = self
            .rows(&self.q("SELECT l.grp, l.owner, l.epoch, m.address FROM octopus_lease l JOIN octopus_member m ON m.member_id = l.owner \
                     WHERE l.expires_at > UTC_TIMESTAMP(6) ORDER BY l.grp"), ())
            .await?;
        Ok(rows.iter().map(|r| (Lease { group: s(r, 0), owner: s(r, 1), epoch: i64_(r, 2) }, s(r, 3))).collect())
    }

    async fn gaps(&self, groups: &[String], older_than: Duration, limit: i32) -> Result<Vec<Gap>> {
        if groups.is_empty() {
            return Ok(vec![]);
        }
        let g = serde_json::to_string(groups).unwrap();
        // sources with something stored beyond the cursor but no head; the first stored position tells the gap
        let sql = self.q(
            "SELECT s.grp, s.source, s.epoch, s.last_seq, \
                    (SELECT f.epoch FROM octopus_event f WHERE f.sid = s.id AND (f.epoch > s.epoch OR (f.epoch = s.epoch AND f.seq > s.last_seq)) \
                      ORDER BY f.epoch, f.seq LIMIT 1) AS fe, \
                    (SELECT f.seq FROM octopus_event f WHERE f.sid = s.id AND (f.epoch > s.epoch OR (f.epoch = s.epoch AND f.seq > s.last_seq)) \
                      ORDER BY f.epoch, f.seq LIMIT 1) AS fq, \
                    CAST(TIMESTAMPDIFF(MICROSECOND, w.first_enq, UTC_TIMESTAMP(6)) AS SIGNED) AS waited, gr.gap_skip_ms \
               FROM JSON_TABLE(?, '$[*]' COLUMNS (v VARCHAR(255) CHARACTER SET utf8mb4 COLLATE utf8mb4_bin PATH '$')) x \
               JOIN octopus_source s ON s.grp = x.v \
               JOIN octopus_grp gr ON gr.name = s.grp \
               JOIN LATERAL (SELECT MIN(e.enqueued_at) AS first_enq, COUNT(*) AS n FROM octopus_event e \
                              WHERE e.sid = s.id AND (e.epoch > s.epoch OR (e.epoch = s.epoch AND e.seq > s.last_seq))) w \
              WHERE w.n > 0 \
                AND NOT EXISTS (SELECT 1 FROM octopus_event h WHERE h.sid = s.id AND h.epoch = s.epoch AND h.seq = s.last_seq + 1) \
                AND (? = 0 OR w.first_enq <= TIMESTAMPADD(MICROSECOND, -? * 1000, UTC_TIMESTAMP(6))) \
              ORDER BY waited DESC LIMIT ?",
        );
        let older = ms(older_than);
        let rows = self.rows(&sql, (g, older, older, limit as i64)).await?;
        Ok(rows
            .iter()
            .map(|r| Gap {
                group: s(r, 0),
                source: s(r, 1),
                cursor: Pos::new(i64_(r, 2), i64_(r, 3)),
                first_stored: Pos::new(i64_(r, 4), i64_(r, 5)),
                waiting: Duration::from_micros(i64_(r, 6).max(0) as u64),
                gap_skip_ms: r.get::<Option<i32>, _>(7).flatten(),
            })
            .collect())
    }

    async fn purge_applied(&self, limit: i32) -> Result<i64> {
        let rows = self.rows(&self.q("CALL octopus_purge_applied(?)"), (limit as i64,)).await?;
        Ok(rows.first().map(|r| i64_(r, 0)).unwrap_or(0))
    }
}
