//! Microsoft SQL Server backend for Octopus (SQL Server 2025 / 17.x and later: it uses `BASE64_DECODE`).
//!
//! The same model and the same guarantees as the PostgreSQL backend; the logic lives in stored procedures
//! (`sql/001_init.sql`). SQL Server has no array parameters, so a batch travels as one JSON parameter (bodies in
//! base64), unpacked server-side with `OPENJSON`. Every operation is one round trip.
//!
//! Connection: an ADO.NET-style string, e.g.
//! `server=tcp:127.0.0.1,1433;database=app;user=octopus;password=...;TrustServerCertificate=true`.
//! The database should run with `READ_COMMITTED_SNAPSHOT ON` (readers don't wait for writers' row locks).

use std::sync::{Arc, Mutex};
use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as B64;
use bytes::Bytes;
use octopus_core::{
    Backend, Commit, CommitOutcome, Error, Event, EventKey, Gap, Head, HeadRequest, Lease, Member, Pos, PublishOutcome, Published,
    Result, Skipped,
};
use serde::Serialize;
use tiberius::{Client, Config, Row, ToSql};
use tokio::net::TcpStream;
use tokio::sync::{OwnedSemaphorePermit, Semaphore};
use tokio_util::compat::{Compat, TokioAsyncWriteCompatExt};

const SCHEMA_SQL: &str = include_str!("../sql/001_init.sql");

/// Names are stored as varchar in a binary UTF-8 collation; JSON strings arrive as nvarchar.
const U8: &str = "COLLATE Latin1_General_100_BIN2_UTF8 AS varchar(256))";

type Conn = Client<Compat<TcpStream>>;

/// Connection and hook settings.
#[derive(Clone, Debug)]
pub struct MssqlConfig {
    /// ADO.NET-style connection string.
    pub connection: String,
    pub pool_size: usize,
    /// Create or update the schema on connect.
    pub migrate: bool,
    /// Database schema holding Octopus's tables (default `octopus`).
    pub schema: String,
    /// Apply hook: a procedure `(@j nvarchar(max))` called in the commit transaction with the applied events as JSON
    /// `[{"s": source, "q": seq, "f": base64 effect}]`.
    pub apply_hook: Option<String>,
    /// Load hook: a procedure `(@j nvarchar(max))` with JSON `[{"i", "s", "q", "b": base64 body}]` returning rows
    /// `(i int, context varbinary(max))`, `i` the 1-based position.
    pub load_hook: Option<String>,
}

impl MssqlConfig {
    pub fn new(connection: impl Into<String>) -> Self {
        Self { connection: connection.into(), pool_size: 16, migrate: true, schema: "octopus".into(), apply_hook: None, load_hook: None }
    }
}

fn db(e: impl std::fmt::Display) -> Error {
    Error::Database(e.to_string())
}

// ---- a small connection pool ----

struct Pool {
    config: Config,
    idle: Mutex<Vec<Conn>>,
    slots: Arc<Semaphore>,
}

/// A pooled connection: back to the pool when dropped after `ok()`, closed otherwise (it may be broken).
struct Pooled<'a> {
    pool: &'a Pool,
    conn: Option<Conn>,
    keep: bool,
    _permit: OwnedSemaphorePermit,
}

impl Pooled<'_> {
    fn c(&mut self) -> &mut Conn {
        self.conn.as_mut().expect("connection")
    }
    fn ok(&mut self) {
        self.keep = true;
    }
}

impl Drop for Pooled<'_> {
    fn drop(&mut self) {
        if self.keep {
            if let Some(c) = self.conn.take() {
                self.pool.idle.lock().unwrap().push(c);
            }
        }
    }
}

impl Pool {
    async fn connect(config: &Config) -> Result<Conn> {
        let tcp = TcpStream::connect(config.get_addr()).await.map_err(db)?;
        tcp.set_nodelay(true).map_err(db)?;
        Client::connect(config.clone(), tcp.compat_write()).await.map_err(db)
    }

    async fn get(&self) -> Result<Pooled<'_>> {
        let permit = self.slots.clone().acquire_owned().await.map_err(db)?;
        let idle = self.idle.lock().unwrap().pop();
        let conn = match idle {
            Some(c) => c,
            None => Self::connect(&self.config).await?,
        };
        Ok(Pooled { pool: self, conn: Some(conn), keep: false, _permit: permit })
    }
}

pub struct MssqlBackend {
    pool: Pool,
    /// sources and groups known to be registered (publish skips registration for a batch of these)
    known: Mutex<std::collections::HashSet<String>>,
    schema: String,
    apply_hook: Option<String>,
    load_hook: Option<String>,
    deadlocks: std::sync::atomic::AtomicU64,
}

const KNOWN_MAX: usize = 1_000_000;

/// The script's statement batches (separated by lines holding only GO), for the given schema.
fn schema_batches(schema: &str) -> Vec<String> {
    let sql = SCHEMA_SQL
        .replace("SCHEMA_ID('octopus')", &format!("SCHEMA_ID('{schema}')"))
        .replace("CREATE SCHEMA octopus", &format!("CREATE SCHEMA {schema}"))
        .replace("octopus.", &format!("{schema}."));
    let mut out = vec![];
    let mut cur = String::new();
    for line in sql.lines() {
        if line.trim().eq_ignore_ascii_case("GO") {
            if !cur.trim().is_empty() {
                out.push(std::mem::take(&mut cur));
            }
            cur.clear();
        } else {
            cur.push_str(line);
            cur.push('\n');
        }
    }
    if !cur.trim().is_empty() {
        out.push(cur);
    }
    out
}

/// SQL Server error numbers worth a retry of a whole migration: deadlock victim, snapshot update conflict.
fn retryable(e: &tiberius::error::Error) -> bool {
    matches!(e, tiberius::error::Error::Server(t) if t.code() == 1205 || t.code() == 3960)
}

impl MssqlBackend {
    pub async fn connect(cfg: MssqlConfig) -> Result<Self> {
        if !cfg.schema.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_') || cfg.schema.is_empty() {
            return Err(Error::Config(format!("schema name {:?}: use [a-z0-9_]", cfg.schema)));
        }
        let config = Config::from_ado_string(&cfg.connection).map_err(|e| Error::Config(format!("connection string: {e}")))?;
        let pool = Pool { config, idle: Mutex::new(vec![]), slots: Arc::new(Semaphore::new(cfg.pool_size.max(1))) };
        let b = Self {
            pool,
            known: Default::default(),
            schema: cfg.schema.clone(),
            apply_hook: cfg.apply_hook,
            load_hook: cfg.load_hook,
            deadlocks: Default::default(),
        };
        if cfg.migrate {
            b.migrate().await?;
        }
        for h in b.apply_hook.iter().chain(b.load_hook.iter()) {
            let rows = b.rows("SELECT OBJECT_ID(@P1, 'P')", &[&h.as_str()]).await?;
            if rows.first().and_then(|r| r.get::<i32, _>(0)).is_none() {
                return Err(Error::Config(format!("hook {h}: not a stored procedure")));
            }
        }
        Ok(b)
    }

    /// Runs the schema script when the installed version differs from this build's (hash kept in <schema>.meta),
    /// one migrator at a time (an exclusive application lock), re-checking under the lock.
    async fn migrate(&self) -> Result<()> {
        let batches = schema_batches(&self.schema);
        let version = format!("{:016x}", xxhash_rust::xxh3::xxh3_64(batches.concat().as_bytes()));
        let meta = format!("{}.meta", self.schema);
        let check = format!("IF OBJECT_ID('{meta}') IS NULL SELECT CAST(NULL AS nvarchar(64)) ELSE SELECT value FROM {meta} WHERE [key] = N'schema'");
        let current = |rows: Vec<Row>| rows.first().and_then(|r| r.get::<&str, _>(0).map(str::to_owned));
        if current(self.rows(&check, &[]).await?).as_deref() == Some(version.as_str()) {
            return Ok(());
        }
        let mut attempt = 0u64;
        loop {
            let mut p = self.pool.get().await?;
            let r: std::result::Result<(), tiberius::error::Error> = async {
                let c = p.c();
                c.simple_query("SET XACT_ABORT ON; BEGIN TRANSACTION; \
                                EXEC sp_getapplock @Resource = N'octopus-migrate', @LockMode = 'Exclusive', \
                                     @LockOwner = 'Transaction', @LockTimeout = 600000;")
                    .await?
                    .into_results()
                    .await?;
                let rows = c.simple_query(&check).await?.into_first_result().await?;
                if rows.first().and_then(|r| r.get::<&str, _>(0)) != Some(version.as_str()) {
                    for b in &batches {
                        c.simple_query(b).await?.into_results().await?;
                    }
                    c.simple_query(&format!(
                        "IF OBJECT_ID('{meta}') IS NULL CREATE TABLE {meta} ([key] nvarchar(64) NOT NULL PRIMARY KEY, value nvarchar(64) NOT NULL); \
                         UPDATE {meta} SET value = N'{version}' WHERE [key] = N'schema'; \
                         IF @@ROWCOUNT = 0 INSERT INTO {meta} VALUES (N'schema', N'{version}');"
                    ))
                    .await?
                    .into_results()
                    .await?;
                }
                c.simple_query("COMMIT").await?.into_results().await?;
                Ok(())
            }
            .await;
            match r {
                Ok(()) => {
                    p.ok();
                    return Ok(());
                }
                Err(e) => {
                    // the connection may still hold the transaction: it is closed, not pooled
                    attempt += 1;
                    if !retryable(&e) || attempt >= 20 {
                        return Err(db(e));
                    }
                    tokio::time::sleep(Duration::from_millis(50 * attempt)).await;
                }
            }
        }
    }

    /// Statement text with the configured schema.
    fn q(&self, sql: &str) -> String {
        sql.replace("octopus.", &format!("{}.", self.schema))
    }

    /// The first result set of a parameterized statement.
    ///
    /// A deadlock victim (1205) is retried: SQL Server picks one when lock orders cross, and it does not guarantee that
    /// `INSERT ... SELECT ... ORDER BY` inserts in that order, so even ordered publishes can meet. The victim's
    /// transaction is rolled back whole, and every operation here is idempotent or monotonic, so a retry is safe.
    async fn rows(&self, sql: &str, params: &[&dyn ToSql]) -> Result<Vec<Row>> {
        let mut attempt = 0u64;
        loop {
            let mut p = self.pool.get().await?;
            match async { p.c().query(sql, params).await?.into_first_result().await }.await {
                Ok(r) => {
                    p.ok();
                    return Ok(r);
                }
                Err(e) if retryable(&e) && attempt < 10 => {
                    p.ok(); // the server rolled the victim back; the connection is fine
                    attempt += 1;
                    self.deadlocks.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    tokio::time::sleep(Duration::from_millis(2 * attempt * attempt)).await;
                }
                Err(e) => return Err(db(e)),
            }
        }
    }

    async fn exec(&self, sql: &str, params: &[&dyn ToSql]) -> Result<()> {
        self.rows(sql, params).await.map(|_| ())
    }

    /// Deadlock victims retried so far (statistics).
    pub fn deadlocks(&self) -> u64 {
        self.deadlocks.load(std::sync::atomic::Ordering::Relaxed)
    }
}

fn s<'a>(r: &'a Row, i: usize) -> String {
    r.get::<&str, _>(i).unwrap_or_default().to_owned()
}
fn i64_(r: &Row, i: usize) -> i64 {
    r.get::<i64, _>(i).unwrap_or_default()
}

/// A head from columns (grp, source, epoch, seq, body, attempts, enqueued_us, skip_to).
fn head_row(r: &Row) -> Head {
    Head {
        group: s(r, 0),
        key: EventKey::new(s(r, 1), Pos::new(i64_(r, 2), i64_(r, 3))),
        body: Bytes::copy_from_slice(r.get::<&[u8], _>(4).unwrap_or_default()),
        attempts: r.get::<i32, _>(5).unwrap_or_default(),
        enqueued_at_us: i64_(r, 6),
        skip_to: r.get::<i64, _>(7),
    }
}

fn json<T: Serialize>(v: &T) -> String {
    serde_json::to_string(v).expect("JSON")
}

#[derive(Serialize)]
struct KeyJ<'a> {
    i: usize,
    s: &'a str,
    e: i64,
    q: i64,
}

fn keys_json<'a>(it: impl Iterator<Item = (&'a str, Pos)>) -> String {
    json(&it.enumerate().map(|(i, (s, p))| KeyJ { i: i + 1, s, e: p.epoch, q: p.seq }).collect::<Vec<_>>())
}

fn names_json(names: &[String]) -> String {
    json(&names)
}

fn outcome(code: i16) -> CommitOutcome {
    match code {
        0 => CommitOutcome::Applied,
        1 => CommitOutcome::Duplicate,
        _ => CommitOutcome::Gap,
    }
}

/// Milliseconds for DATEADD (an int).
fn ms(d: Duration) -> i32 {
    d.as_millis().min(i32::MAX as u128) as i32
}

impl Backend for MssqlBackend {
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
        let j = json(&items);
        let rows = match self.rows(&self.q("EXEC octopus.publish @P1, @P2"), &[&j.as_str(), &all_known]).await {
            // a source the cache believed registered is not there: forget the batch's sources, retry with registration
            Err(Error::Database(m)) if m.contains("unknown source in publish batch") => {
                {
                    let mut k = self.known.lock().unwrap();
                    for e in events {
                        k.remove(&e.key.source);
                    }
                }
                self.rows(&self.q("EXEC octopus.publish @P1, @P2"), &[&j.as_str(), &false]).await?
            }
            r => r?,
        };
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
            let i = r.get::<i32, _>(0).unwrap_or(0) as usize;
            if i == 0 || i > out.len() {
                continue;
            }
            out[i - 1] = Published {
                outcome: match r.get::<i16, _>(1).unwrap_or(1) {
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
        let groups = json(&requests.iter().map(|r| G { g: &r.group, n: r.max.min(i32::MAX as usize) as i64 }).collect::<Vec<_>>());
        let exclude = json(&requests.iter().flat_map(|r| r.exclude_sources.iter()).collect::<Vec<_>>());
        let sql = self.q(&format!(
            "SET NOCOUNT ON; \
             DECLARE @x TABLE (s varchar(256) COLLATE Latin1_General_100_BIN2_UTF8 PRIMARY KEY WITH (IGNORE_DUP_KEY = ON)); \
             INSERT INTO @x SELECT CAST(v {U8} FROM OPENJSON(@P2) WITH (v nvarchar(256) '$'); \
             SELECT CAST(x.grp AS nvarchar(256)), CAST(x.source AS nvarchar(256)), x.epoch, x.seq, x.body, x.attempts, \
                    DATEDIFF_BIG(MICROSECOND, '1970-01-01', x.enqueued_at), x.skip_to \
               FROM OPENJSON(@P1) WITH (g nvarchar(256) '$.g', n int '$.n') AS gq \
               CROSS APPLY (SELECT TOP (gq.n) e.grp, s.source, e.epoch, e.seq, e.body, e.attempts, e.enqueued_at, e.skip_to \
                              FROM octopus.source s \
                              JOIN octopus.event e ON e.sid = s.id AND e.epoch = s.epoch AND e.seq = s.last_seq + 1 \
                             WHERE s.grp = CAST(gq.g {U8} AND NOT EXISTS (SELECT 1 FROM @x ex WHERE ex.s = s.source) \
                               AND (e.not_before IS NULL OR e.not_before <= SYSUTCDATETIME()) \
                             ORDER BY e.enqueued_at, s.source) x"
        ));
        Ok(self.rows(&sql, &[&groups.as_str(), &exclude.as_str()]).await?.iter().map(head_row).collect())
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
        let j = json(&heads.iter().enumerate().map(|(i, h)| L { i: i + 1, s: &h.key.source, q: h.key.pos.seq, b: B64.encode(&h.body) }).collect::<Vec<_>>());
        let sql = format!("EXEC {hook} @P1");
        for r in self.rows(&sql, &[&j.as_str()]).await? {
            let i = r.get::<i32, _>(0).unwrap_or(0) as usize;
            if i >= 1 && i <= out.len() {
                out[i - 1] = r.get::<&[u8], _>(1).map(Bytes::copy_from_slice);
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
        let j = json(
            &commits
                .iter()
                .enumerate()
                .map(|(i, c)| C { i: i + 1, s: &c.key.source, e: c.key.pos.epoch, q: c.key.pos.seq, f: B64.encode(&c.effect) })
                .collect::<Vec<_>>(),
        );
        let hook: Option<&str> = self.apply_hook.as_deref();
        let rows = self.rows(&self.q("EXEC octopus.commit_many @P1, @P2"), &[&j.as_str(), &hook]).await?;
        let mut out = vec![CommitOutcome::Gap; commits.len()];
        for r in rows {
            let i = r.get::<i32, _>(0).unwrap_or(0) as usize;
            if i >= 1 && i <= out.len() {
                out[i - 1] = outcome(r.get::<i16, _>(1).unwrap_or(2));
            }
        }
        Ok(out)
    }

    async fn remove(&self, keys: &[EventKey]) -> Result<Vec<(CommitOutcome, Option<Head>)>> {
        if keys.is_empty() {
            return Ok(vec![]);
        }
        let j = keys_json(keys.iter().map(|k| (k.source.as_str(), k.pos)));
        let rows = self.rows(&self.q("EXEC octopus.remove_next @P1"), &[&j.as_str()]).await?;
        let mut out: Vec<(CommitOutcome, Option<Head>)> = vec![(CommitOutcome::Gap, None); keys.len()];
        for r in rows {
            let i = r.get::<i32, _>(0).unwrap_or(0) as usize;
            if i == 0 || i > out.len() {
                continue;
            }
            let next = match (r.get::<i64, _>(2), r.get::<i64, _>(3)) {
                (Some(e), Some(q)) => Some(Head {
                    group: String::new(), // filled in by the caller
                    key: EventKey::new(keys[i - 1].source.clone(), Pos::new(e, q)),
                    body: Bytes::copy_from_slice(r.get::<&[u8], _>(4).unwrap_or_default()),
                    attempts: r.get::<i32, _>(5).unwrap_or_default(),
                    enqueued_at_us: i64_(&r, 6),
                    skip_to: r.get::<i64, _>(7),
                }),
                _ => None,
            };
            out[i - 1] = (outcome(r.get::<i16, _>(1).unwrap_or(2)), next);
        }
        Ok(out)
    }

    async fn advance(&self, cursors: &[(String, Pos)]) -> Result<()> {
        if cursors.is_empty() {
            return Ok(());
        }
        let j = keys_json(cursors.iter().map(|(s, p)| (s.as_str(), *p)));
        self.exec(&self.q("EXEC octopus.advance @P1"), &[&j.as_str()]).await
    }

    async fn read_events(&self, keys: &[EventKey]) -> Result<Vec<Head>> {
        if keys.is_empty() {
            return Ok(vec![]);
        }
        let j = keys_json(keys.iter().map(|k| (k.source.as_str(), k.pos)));
        let sql = self.q(&format!(
            "SELECT CAST(x.grp AS nvarchar(256)), k.s, x.epoch, x.seq, x.body, x.attempts, x.enq, x.skip_to \
               FROM OPENJSON(@P1) WITH (s nvarchar(256) '$.s', e bigint '$.e', q bigint '$.q') k \
              CROSS APPLY (SELECT e.grp, e.epoch, e.seq, e.body, e.attempts, \
                                  DATEDIFF_BIG(MICROSECOND, '1970-01-01', e.enqueued_at) AS enq, e.skip_to \
                             FROM octopus.source s JOIN octopus.event e ON e.sid = s.id \
                            WHERE s.source = CAST(k.s {U8} AND e.epoch = k.e AND e.seq = k.q \
                              AND (e.not_before IS NULL OR e.not_before <= SYSUTCDATETIME())) x"
        ));
        Ok(self.rows(&sql, &[&j.as_str()]).await?.iter().map(head_row).collect())
    }

    async fn fail(&self, key: &EventKey, delay: Duration) -> Result<()> {
        let sql = self.q(&format!(
            "UPDATE e SET attempts = e.attempts + 1, not_before = DATEADD(MILLISECOND, @P4, SYSUTCDATETIME()) \
               FROM octopus.event e JOIN octopus.source s ON s.id = e.sid \
              WHERE s.source = CAST(@P1 {U8} AND e.epoch = @P2 AND e.seq = @P3"
        ));
        self.exec(&sql, &[&key.source.as_str(), &key.pos.epoch, &key.pos.seq, &ms(delay)]).await
    }

    async fn missing_up_to(&self, positions: &[(String, Pos)]) -> Result<Vec<i64>> {
        if positions.is_empty() {
            return Ok(vec![]);
        }
        let j = keys_json(positions.iter().map(|(s, p)| (s.as_str(), *p)));
        // covered = the highest position of that epoch known stored (or skip-marked) or applied; missing = q - covered.
        let sql = self.q(&format!(
            "SELECT k.i, CASE WHEN s.id IS NULL THEN k.q WHEN k.e < s.epoch THEN CAST(0 AS bigint) \
                    ELSE GREATEST(CAST(0 AS bigint), k.q - GREATEST( \
                         CASE WHEN k.e = s.epoch THEN s.last_seq ELSE 0 END, \
                         (SELECT TOP (1) COALESCE(y.skip_to, y.seq) FROM octopus.event y \
                           WHERE y.sid = s.id AND y.epoch = k.e AND y.seq <= k.q ORDER BY y.seq DESC), \
                         CAST(0 AS bigint))) END \
               FROM OPENJSON(@P1) WITH (i int '$.i', s nvarchar(256) '$.s', e bigint '$.e', q bigint '$.q') k \
               LEFT JOIN octopus.source s ON s.source = CAST(k.s {U8} ORDER BY k.i"
        ));
        Ok(self.rows(&sql, &[&j.as_str()]).await?.iter().map(|r| i64_(r, 1)).collect())
    }

    async fn skip(&self, source: &str, after: Pos, to_seq: Option<i64>, reason: &str) -> Result<Option<(Pos, Skipped)>> {
        let rows = self
            .rows(&self.q("EXEC octopus.skip @P1, @P2, @P3, @P4, @P5"), &[&source, &after.epoch, &after.seq, &to_seq, &reason])
            .await?;
        let Some(r) = rows.first() else { return Ok(None) };
        Ok(match (r.get::<i64, _>(0), r.get::<i64, _>(1)) {
            (Some(e), Some(q)) => Some((
                Pos::new(e, q),
                Skipped { epoch: after.epoch, from_seq: r.get::<i64, _>(2).unwrap_or(after.seq + 1), to_seq: r.get::<i64, _>(3) },
            )),
            _ => None,
        })
    }

    async fn groups(&self) -> Result<Vec<String>> {
        let rows = self.rows(&self.q("SELECT CAST(name AS nvarchar(256)) FROM octopus.grp ORDER BY name"), &[]).await?;
        Ok(rows.iter().map(|r| s(r, 0)).collect())
    }

    async fn backlog(&self) -> Result<i64> {
        let rows = self
            .rows(&self.q("SELECT COUNT_BIG(*) FROM octopus.event e JOIN octopus.source s ON s.id = e.sid \
                     WHERE e.epoch > s.epoch OR (e.epoch = s.epoch AND e.seq > s.last_seq)"), &[])
            .await?;
        Ok(rows.first().map(|r| i64_(r, 0)).unwrap_or(0))
    }

    async fn heartbeat(&self, member: &Member, ttl: Duration) -> Result<Vec<Member>> {
        let sql = self.q(&format!(
            "SET NOCOUNT ON; DECLARE @m varchar(256) = CAST(@P1 {U8}; \
             UPDATE octopus.member SET address = @P2, heartbeat = SYSUTCDATETIME() WHERE member_id = @m; \
             IF @@ROWCOUNT = 0 INSERT INTO octopus.member (member_id, address, heartbeat) VALUES (@m, @P2, SYSUTCDATETIME()); \
             SELECT CAST(member_id AS nvarchar(256)), address FROM octopus.member \
              WHERE heartbeat > DATEADD(MILLISECOND, -@P3, SYSUTCDATETIME()) ORDER BY member_id"
        ));
        let rows = self.rows(&sql, &[&member.id.as_str(), &member.address.as_str(), &ms(ttl)]).await?;
        Ok(rows.iter().map(|r| Member { id: s(r, 0), address: s(r, 1) }).collect())
    }

    async fn renew(&self, member: &str, groups: &[String], ttl: Duration) -> Result<Vec<String>> {
        if groups.is_empty() {
            return Ok(vec![]);
        }
        let sql = self.q(&format!(
            "SET NOCOUNT ON; \
             UPDATE l SET expires_at = DATEADD(MILLISECOND, @P3, SYSUTCDATETIME()) \
             OUTPUT CAST(inserted.grp AS nvarchar(256)) \
               FROM octopus.lease l WITH (ROWLOCK) \
              WHERE l.owner = CAST(@P1 {U8} \
                AND l.grp IN (SELECT CAST(v {U8} FROM OPENJSON(@P2) WITH (v nvarchar(256) '$'))"
        ));
        let g = names_json(groups);
        Ok(self.rows(&sql, &[&member, &g.as_str(), &ms(ttl)]).await?.iter().map(|r| s(r, 0)).collect())
    }

    async fn claim(&self, member: &str, groups: &[String], ttl: Duration) -> Result<Vec<Lease>> {
        if groups.is_empty() {
            return Ok(vec![]);
        }
        let sql = self.q(&format!(
            "SET NOCOUNT ON; DECLARE @m varchar(256) = CAST(@P1 {U8}; \
             DECLARE @g TABLE (g varchar(256) COLLATE Latin1_General_100_BIN2_UTF8 PRIMARY KEY WITH (IGNORE_DUP_KEY = ON)); \
             DECLARE @r TABLE (grp varchar(256) COLLATE Latin1_General_100_BIN2_UTF8, owner varchar(256) COLLATE Latin1_General_100_BIN2_UTF8, epoch bigint); \
             INSERT INTO @g SELECT CAST(v {U8} FROM OPENJSON(@P2) WITH (v nvarchar(256) '$'); \
             UPDATE l SET owner = @m, epoch = l.epoch + 1, expires_at = DATEADD(MILLISECOND, @P3, SYSUTCDATETIME()) \
             OUTPUT inserted.grp, inserted.owner, inserted.epoch INTO @r \
               FROM octopus.lease l WITH (ROWLOCK) JOIN @g g ON g.g = l.grp \
              WHERE l.expires_at < SYSUTCDATETIME() OR l.owner = @m; \
             INSERT INTO octopus.lease (grp, owner, epoch, expires_at) \
             OUTPUT inserted.grp, inserted.owner, inserted.epoch INTO @r \
             SELECT g.g, @m, 1, DATEADD(MILLISECOND, @P3, SYSUTCDATETIME()) FROM @g g \
              WHERE NOT EXISTS (SELECT 1 FROM octopus.lease l WHERE l.grp = g.g) ORDER BY g.g; \
             SELECT CAST(grp AS nvarchar(256)), CAST(owner AS nvarchar(256)), epoch FROM @r ORDER BY grp"
        ));
        let g = names_json(groups);
        let rows = self.rows(&sql, &[&member, &g.as_str(), &ms(ttl)]).await?;
        Ok(rows.iter().map(|r| Lease { group: s(r, 0), owner: s(r, 1), epoch: i64_(r, 2) }).collect())
    }

    async fn release(&self, member: &str, groups: &[String]) -> Result<()> {
        if groups.is_empty() {
            return Ok(());
        }
        let sql = self.q(&format!(
            "DELETE l FROM octopus.lease l WITH (ROWLOCK) WHERE l.owner = CAST(@P1 {U8} \
                AND l.grp IN (SELECT CAST(v {U8} FROM OPENJSON(@P2) WITH (v nvarchar(256) '$'))"
        ));
        let g = names_json(groups);
        self.exec(&sql, &[&member, &g.as_str()]).await
    }

    async fn leave(&self, member: &str) -> Result<()> {
        let sql = self.q(&format!(
            "DECLARE @m varchar(256) = CAST(@P1 {U8}; \
             DELETE FROM octopus.lease WHERE owner = @m; DELETE FROM octopus.member WHERE member_id = @m;"
        ));
        self.exec(&sql, &[&member]).await
    }

    async fn leases(&self) -> Result<Vec<(Lease, String)>> {
        let rows = self
            .rows(&self.q("SELECT CAST(l.grp AS nvarchar(256)), CAST(l.owner AS nvarchar(256)), l.epoch, m.address \
                 FROM octopus.lease l JOIN octopus.member m ON m.member_id = l.owner \
                WHERE l.expires_at > SYSUTCDATETIME() ORDER BY l.grp"), &[])
            .await?;
        Ok(rows.iter().map(|r| (Lease { group: s(r, 0), owner: s(r, 1), epoch: i64_(r, 2) }, s(r, 3))).collect())
    }

    async fn gaps(&self, groups: &[String], older_than: Duration, limit: i32) -> Result<Vec<Gap>> {
        if groups.is_empty() {
            return Ok(vec![]);
        }
        // sources with something stored beyond the cursor but no head; the first stored position tells the gap.
        // (A zero age takes every gap: SYSUTCDATETIME read in two sessions can differ by milliseconds, so an event
        // stored just now could look younger than "now".)
        let sql = self.q(&format!(
            "SELECT CAST(s.grp AS nvarchar(256)), CAST(s.source AS nvarchar(256)), s.epoch, s.last_seq, f.epoch, f.seq, \
                    DATEDIFF_BIG(MICROSECOND, w.first_enq, SYSUTCDATETIME()), g.gap_skip_ms \
               FROM octopus.source s \
               JOIN octopus.grp g ON g.name = s.grp \
              CROSS APPLY (SELECT MIN(e.enqueued_at) AS first_enq, COUNT_BIG(*) AS n \
                             FROM octopus.event e \
                            WHERE e.sid = s.id AND (e.epoch > s.epoch OR (e.epoch = s.epoch AND e.seq > s.last_seq))) w \
              CROSS APPLY (SELECT TOP (1) e.epoch, e.seq FROM octopus.event e \
                            WHERE e.sid = s.id AND (e.epoch > s.epoch OR (e.epoch = s.epoch AND e.seq > s.last_seq)) \
                            ORDER BY e.epoch, e.seq) f \
              WHERE s.grp IN (SELECT CAST(v {U8} FROM OPENJSON(@P1) WITH (v nvarchar(256) '$')) \
                AND w.n > 0 AND (@P2 = 0 OR w.first_enq <= DATEADD(MILLISECOND, -@P2, SYSUTCDATETIME())) \
                AND NOT EXISTS (SELECT 1 FROM octopus.event h WHERE h.sid = s.id AND h.epoch = s.epoch AND h.seq = s.last_seq + 1) \
              ORDER BY 7 DESC OFFSET 0 ROWS FETCH NEXT @P3 ROWS ONLY"
        ));
        let g = names_json(groups);
        let rows = self.rows(&sql, &[&g.as_str(), &ms(older_than), &limit]).await?;
        Ok(rows
            .iter()
            .map(|r| Gap {
                group: s(r, 0),
                source: s(r, 1),
                cursor: Pos::new(i64_(r, 2), i64_(r, 3)),
                first_stored: Pos::new(i64_(r, 4), i64_(r, 5)),
                waiting: Duration::from_micros(i64_(r, 6).max(0) as u64),
                gap_skip_ms: r.get::<i32, _>(7),
            })
            .collect())
    }

    async fn purge_applied(&self, limit: i32) -> Result<i64> {
        let rows = self.rows(&self.q("EXEC octopus.purge_applied @P1"), &[&limit]).await?;
        Ok(rows.first().map(|r| i64_(r, 0)).unwrap_or(0))
    }
}
