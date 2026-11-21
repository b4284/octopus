//! PostgreSQL backend for Octopus.
//!
//! Every operation is one round trip: a server-side function (publish, commit) or a single statement. The schema
//! lives in the `octopus` schema and is created by [`PgBackend::connect`] when `migrate` is set.

use std::time::Duration;

use bytes::Bytes;
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use octopus_core::{
    Backend, Commit, CommitOutcome, Error, Event, EventKey, Gap, Head, HeadRequest, Lease, Member, Pos, PublishOutcome, Published,
    Result, Skipped,
};

/// A head from a row (grp, source, epoch, seq, body, attempts, enqueued_us, skip_to), starting at column `at`.
fn head_row(r: &tokio_postgres::Row, at: usize) -> Head {
    Head {
        group: r.get(at),
        key: EventKey::new(r.get::<_, String>(at + 1), Pos::new(r.get(at + 2), r.get(at + 3))),
        body: Bytes::from(r.get::<_, Vec<u8>>(at + 4)),
        attempts: r.get(at + 5),
        enqueued_at_us: r.get(at + 6),
        skip_to: r.get(at + 7),
    }
}
use tokio_postgres::NoTls;

const SCHEMA_SQL: &str = include_str!("../sql/001_init.sql");

/// Connection and hook settings.
#[derive(Clone, Debug)]
pub struct PgConfig {
    /// libpq-style connection string, e.g. `host=127.0.0.1 port=5432 user=octopus dbname=app`.
    /// A multi-host string with `target_session_attrs=read-write` follows a failover to the new primary.
    pub connection: String,
    pub pool_size: usize,
    /// Create or update the schema on connect.
    pub migrate: bool,
    /// Database schema holding Octopus's tables (default `octopus`). Separate schemas isolate tenants or tests.
    pub schema: String,
    /// Apply hook: `fn(sources text[], seqs bigint[], effects bytea[])`, called in the commit transaction with the
    /// events being applied. Writes the application's state.
    pub apply_hook: Option<String>,
    /// Load hook: `fn(sources text[], seqs bigint[], bodies bytea[]) RETURNS TABLE(i int, context bytea)`, with `i`
    /// the 1-based position. Supplies the context handed to workers with each event.
    pub load_hook: Option<String>,
}

impl PgConfig {
    pub fn new(connection: impl Into<String>) -> Self {
        Self { connection: connection.into(), pool_size: 16, migrate: true, schema: "octopus".into(), apply_hook: None, load_hook: None }
    }
}

pub struct PgBackend {
    pool: Pool,
    /// sources and groups known to be registered (publish skips the registration statements for a batch of these)
    known: std::sync::Mutex<std::collections::HashSet<String>>,
    schema: String,
    apply_hook: Option<String>,
    load_sql: Option<String>,
}

/// Most sources and groups a backend remembers as registered (beyond that the cache starts again).
const KNOWN_MAX: usize = 1_000_000;

fn db(e: impl Into<DbErr>) -> Error {
    Error::Database(e.into().0)
}

/// A database error as text, with the server's message and code when there is one ("db error" alone says nothing).
struct DbErr(String);

impl From<tokio_postgres::Error> for DbErr {
    fn from(e: tokio_postgres::Error) -> Self {
        match e.as_db_error() {
            Some(d) => DbErr(format!("{}: {} ({})", d.severity(), d.message(), d.code().code())),
            None => DbErr(e.to_string()),
        }
    }
}

impl From<deadpool_postgres::PoolError> for DbErr {
    fn from(e: deadpool_postgres::PoolError) -> Self {
        match e {
            deadpool_postgres::PoolError::Backend(b) => b.into(),
            other => DbErr(other.to_string()),
        }
    }
}

impl From<deadpool_postgres::BuildError> for DbErr {
    fn from(e: deadpool_postgres::BuildError) -> Self {
        DbErr(e.to_string())
    }
}

fn interval(d: Duration) -> String {
    format!("{} milliseconds", d.as_millis())
}

impl PgBackend {
    pub async fn connect(cfg: PgConfig) -> Result<Self> {
        let pg: tokio_postgres::Config = cfg.connection.parse().map_err(|e| Error::Config(format!("connection string: {e}")))?;
        let mgr = Manager::from_config(pg, NoTls, ManagerConfig { recycling_method: RecyclingMethod::Fast });
        let pool = Pool::builder(mgr).max_size(cfg.pool_size).build().map_err(db)?;
        let client = pool.get().await.map_err(db)?;
        if !cfg.schema.chars().all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_') || cfg.schema.is_empty() {
            return Err(Error::Config(format!("schema name {:?}: use [a-z0-9_]", cfg.schema)));
        }
        let schema_sql = SCHEMA_SQL.replace("SCHEMA IF NOT EXISTS octopus;", &format!("SCHEMA IF NOT EXISTS {};", cfg.schema)).replace("octopus.", &format!("{}.", cfg.schema));
        if cfg.migrate {
            // The script is idempotent but takes table locks (ALTER TABLE, CREATE INDEX IF NOT EXISTS) that can
            // deadlock with live traffic, so it runs only when the installed schema differs from this build's: the
            // hash of the script is kept in <schema>.meta. One migrator at a time.
            let version = format!("{:016x}", xxhash_rust::xxh3::xxh3_64(schema_sql.as_bytes()));
            let meta = format!("{}.meta", cfg.schema);
            let current: Option<String> = match client.query_opt(&format!("SELECT value FROM {meta} WHERE key = 'schema'"), &[]).await {
                Ok(row) => row.map(|r| r.get(0)),
                Err(_) => None, // no meta table yet
            };
            if current.as_deref() != Some(version.as_str()) {
                // Under the lock, look again: several members starting together must not each run the script
                // (the later ones would take its locks while the first is already serving).
                let migrate = async {
                    client.batch_execute("BEGIN; SELECT pg_advisory_xact_lock(hashtext('octopus-migrate'))").await?;
                    let has_meta: bool = client.query_one("SELECT to_regclass($1) IS NOT NULL", &[&meta]).await?.get(0);
                    let current: Option<String> = if has_meta {
                        client.query_opt(&format!("SELECT value FROM {meta} WHERE key = 'schema'"), &[]).await?.map(|r| r.get(0))
                    } else {
                        None
                    };
                    if current.as_deref() != Some(version.as_str()) {
                        client
                            .batch_execute(&format!(
                                "{schema_sql}; \
                                 CREATE TABLE IF NOT EXISTS {meta} (key text PRIMARY KEY, value text NOT NULL); \
                                 INSERT INTO {meta} VALUES ('schema', '{version}') \
                                 ON CONFLICT (key) DO UPDATE SET value = excluded.value"
                            ))
                            .await?;
                    }
                    client.batch_execute("COMMIT").await
                };
                if let Err(e) = migrate.await {
                    let _ = client.batch_execute("ROLLBACK").await;
                    return Err(db(e));
                }
            }
        }
        // Validate the hooks now (the cast fails for anything that is not an existing function).
        let mut load_sql = None;
        for (hook, is_load) in [(&cfg.apply_hook, false), (&cfg.load_hook, true)] {
            if let Some(h) = hook {
                let row = client.query_one("SELECT $1::text::regproc::text", &[h]).await.map_err(|e| Error::Config(format!("hook {h}: {e}")))?;
                let name: String = row.get(0);
                if is_load {
                    load_sql = Some(format!("SELECT i, context FROM {name}($1::text[], $2::bigint[], $3::bytea[])"));
                }
            }
        }
        Ok(Self { pool, known: Default::default(), schema: cfg.schema, apply_hook: cfg.apply_hook, load_sql })
    }

    /// Statement text with the configured schema.
    fn q(&self, sql: &str) -> String {
        sql.replace("octopus.", &format!("{}.", self.schema))
    }

    async fn client(&self) -> Result<deadpool_postgres::Object> {
        self.pool.get().await.map_err(db)
    }

}

impl Backend for PgBackend {
    async fn publish(&self, events: &[Event]) -> Result<Vec<Published>> {
        if events.is_empty() {
            return Ok(vec![]);
        }
        let grp: Vec<&str> = events.iter().map(|e| e.group.as_str()).collect();
        let src: Vec<&str> = events.iter().map(|e| e.key.source.as_str()).collect();
        let epoch: Vec<i64> = events.iter().map(|e| e.key.pos.epoch).collect();
        let seq: Vec<i64> = events.iter().map(|e| e.key.pos.seq).collect();
        let body: Vec<&[u8]> = events.iter().map(|e| e.body.as_ref()).collect();
        let hash: Vec<i64> = events.iter().map(|e| xxhash_rust::xxh3::xxh3_64(&e.body) as i64).collect();
        let skip_to: Vec<Option<i64>> = events.iter().map(|e| e.skip_to).collect();
        let prev_epoch: Vec<Option<i64>> = events.iter().map(|e| e.prev.map(|p| p.epoch)).collect();
        let prev_last: Vec<Option<i64>> = events.iter().map(|e| e.prev.map(|p| p.last_seq)).collect();
        if events.iter().any(|e| e.key.pos.seq < 1 || e.key.pos.epoch < 0 || e.skip_to.is_some_and(|k| k < e.key.pos.seq)) {
            return Err(Error::Invalid("seq starts at 1, epoch is not negative, skip_to >= seq".into()));
        }
        let all_known = {
            let k = self.known.lock().unwrap();
            events.iter().all(|e| k.contains(&e.key.source) && k.contains(&format!("\u{1}{}", e.group)))
        };
        let c = self.client().await?;
        let st = c
            .prepare_cached(&self.q("SELECT codes, missing FROM octopus.publish($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)"))
            .await
            .map_err(db)?;
        let row = c
            .query_one(&st, &[&grp, &src, &epoch, &seq, &body, &hash, &skip_to, &prev_epoch, &prev_last, &all_known])
            .await
            .map_err(db)?;
        let codes: Vec<i16> = row.get(0);
        let missing: Vec<i64> = row.get(1);
        if !all_known {
            // registered now (a group mismatch still means the source exists); bounded: cleared when it grows too big
            let mut k = self.known.lock().unwrap();
            if k.len() > KNOWN_MAX {
                k.clear();
            }
            for e in events {
                k.insert(e.key.source.clone());
                k.insert(format!("\u{1}{}", e.group));
            }
        }
        Ok(codes
            .into_iter()
            .zip(missing)
            .map(|(c, m)| Published {
                outcome: match c {
                    0 => PublishOutcome::Stored,
                    3 => PublishOutcome::StoredHead,
                    2 => PublishOutcome::GroupMismatch,
                    4 => PublishOutcome::Conflict,
                    5 => PublishOutcome::StaleEpoch,
                    6 => PublishOutcome::Late,
                    _ => PublishOutcome::Duplicate,
                },
                missing_below: m,
            })
            .collect())
    }

    async fn read_heads(&self, requests: &[HeadRequest]) -> Result<Vec<Head>> {
        if requests.is_empty() {
            return Ok(vec![]);
        }
        let grp: Vec<&str> = requests.iter().map(|r| r.group.as_str()).collect();
        let max: Vec<i32> = requests.iter().map(|r| r.max.min(i32::MAX as usize) as i32).collect();
        let exclude: Vec<&str> = requests.iter().flat_map(|r| r.exclude_sources.iter().map(String::as_str)).collect();
        let c = self.client().await?;
        let st = c
            .prepare_cached(&self.q("SELECT x.grp, x.source, x.epoch, x.seq, x.body, x.attempts, \
                        (extract(epoch FROM x.enqueued_at) * 1000000)::bigint, x.skip_to \
                   FROM unnest($1::text[], $2::int[]) AS g(name, n) CROSS JOIN LATERAL ( \
                        SELECT e.grp, s.source, e.epoch, e.seq, e.body, e.attempts, e.enqueued_at, e.skip_to \
                          FROM octopus.source s \
                          JOIN octopus.event e ON e.sid = s.id AND e.epoch = s.epoch AND e.seq = s.last_seq + 1 \
                         WHERE s.grp = g.name AND s.source <> ALL($3::text[]) \
                           AND (e.not_before IS NULL OR e.not_before <= now()) \
                         ORDER BY e.enqueued_at, s.source \
                         LIMIT g.n) x"),
            )
            .await
            .map_err(db)?;
        let rows = c.query(&st, &[&grp, &max, &exclude]).await.map_err(db)?;
        Ok(rows.into_iter().map(|r| head_row(&r, 0)).collect())
    }

    async fn load_context(&self, heads: &[Head]) -> Result<Vec<Option<Bytes>>> {
        let mut out = vec![None; heads.len()];
        let Some(sql) = &self.load_sql else { return Ok(out) };
        if heads.is_empty() {
            return Ok(out);
        }
        let src: Vec<&str> = heads.iter().map(|h| h.key.source.as_str()).collect();
        let seq: Vec<i64> = heads.iter().map(|h| h.key.pos.seq).collect();
        let body: Vec<&[u8]> = heads.iter().map(|h| h.body.as_ref()).collect();
        let c = self.client().await?;
        let st = c.prepare_cached(sql).await.map_err(db)?;
        for r in c.query(&st, &[&src, &seq, &body]).await.map_err(db)? {
            let i: i32 = r.get(0);
            if i >= 1 && (i as usize) <= out.len() {
                out[i as usize - 1] = r.get::<_, Option<Vec<u8>>>(1).map(Bytes::from);
            }
        }
        Ok(out)
    }

    async fn commit(&self, commits: &[Commit]) -> Result<Vec<CommitOutcome>> {
        if commits.is_empty() {
            return Ok(vec![]);
        }
        let src: Vec<&str> = commits.iter().map(|c| c.key.source.as_str()).collect();
        let epoch: Vec<i64> = commits.iter().map(|c| c.key.pos.epoch).collect();
        let seq: Vec<i64> = commits.iter().map(|c| c.key.pos.seq).collect();
        let eff: Vec<&[u8]> = commits.iter().map(|c| c.effect.as_ref()).collect();
        let c = self.client().await?;
        let st = c.prepare_cached(&self.q("SELECT octopus.commit_many($1, $2, $3, $4, $5)")).await.map_err(db)?;
        let row = c.query_one(&st, &[&src, &epoch, &seq, &eff, &self.apply_hook]).await.map_err(db)?;
        let codes: Vec<i16> = row.get(0);
        Ok(codes
            .into_iter()
            .map(|c| match c {
                0 => CommitOutcome::Applied,
                1 => CommitOutcome::Duplicate,
                _ => CommitOutcome::Gap,
            })
            .collect())
    }

    async fn remove(&self, keys: &[EventKey]) -> Result<Vec<(CommitOutcome, Option<Head>)>> {
        if keys.is_empty() {
            return Ok(vec![]);
        }
        let src: Vec<&str> = keys.iter().map(|k| k.source.as_str()).collect();
        let epoch: Vec<i64> = keys.iter().map(|k| k.pos.epoch).collect();
        let seq: Vec<i64> = keys.iter().map(|k| k.pos.seq).collect();
        let c = self.client().await?;
        let st = c
            .prepare_cached(&self.q(
                "SELECT i, rc, next_epoch, next_seq, next_body, next_attempts, next_enqueued_us, next_skip_to \
                   FROM octopus.remove_next($1, $2, $3) ORDER BY i",
            ))
            .await
            .map_err(db)?;
        let rows = c.query(&st, &[&src, &epoch, &seq]).await.map_err(db)?;
        let mut out: Vec<(CommitOutcome, Option<Head>)> = vec![(CommitOutcome::Gap, None); keys.len()];
        for r in rows {
            let i = r.get::<_, i32>(0) as usize - 1;
            let outcome = match r.get::<_, i16>(1) {
                0 => CommitOutcome::Applied,
                1 => CommitOutcome::Duplicate,
                _ => CommitOutcome::Gap,
            };
            let next = match (r.get::<_, Option<i64>>(2), r.get::<_, Option<i64>>(3)) {
                (Some(e), Some(q)) => Some(Head {
                    group: String::new(), // filled in by the caller (it knows the source's group)
                    key: EventKey::new(keys[i].source.clone(), Pos::new(e, q)),
                    body: Bytes::from(r.get::<_, Vec<u8>>(4)),
                    skip_to: r.get(7),
                    attempts: r.get(5),
                    enqueued_at_us: r.get(6),
                }),
                _ => None,
            };
            out[i] = (outcome, next);
        }
        Ok(out)
    }

    async fn advance(&self, cursors: &[(String, Pos)]) -> Result<()> {
        if cursors.is_empty() {
            return Ok(());
        }
        let src: Vec<&str> = cursors.iter().map(|(s, _)| s.as_str()).collect();
        let epoch: Vec<i64> = cursors.iter().map(|(_, p)| p.epoch).collect();
        let seq: Vec<i64> = cursors.iter().map(|(_, p)| p.seq).collect();
        let c = self.client().await?;
        let st = c.prepare_cached(&self.q("SELECT octopus.advance($1, $2, $3)")).await.map_err(db)?;
        c.execute(&st, &[&src, &epoch, &seq]).await.map_err(db)?;
        Ok(())
    }

    async fn read_events(&self, keys: &[EventKey]) -> Result<Vec<Head>> {
        if keys.is_empty() {
            return Ok(vec![]);
        }
        let src: Vec<&str> = keys.iter().map(|k| k.source.as_str()).collect();
        let epoch: Vec<i64> = keys.iter().map(|k| k.pos.epoch).collect();
        let seq: Vec<i64> = keys.iter().map(|k| k.pos.seq).collect();
        let c = self.client().await?;
        let st = c
            .prepare_cached(&self.q(
                // one index lookup per key (LATERAL): a generic plan must not turn this into a scan
                "SELECT x.grp, k.s, x.epoch, x.seq, x.body, x.attempts, x.enq, x.skip_to \
                   FROM unnest($1::text[], $2::bigint[], $3::bigint[]) AS k(s, ep, q) \
                   CROSS JOIN LATERAL ( \
                        SELECT e.grp, e.epoch, e.seq, e.body, e.attempts, \
                               (extract(epoch FROM e.enqueued_at) * 1000000)::bigint AS enq, e.skip_to \
                          FROM octopus.source s JOIN octopus.event e ON e.sid = s.id \
                         WHERE s.source = k.s AND e.epoch = k.ep AND e.seq = k.q \
                           AND (e.not_before IS NULL OR e.not_before <= now())) x",
            ))
            .await
            .map_err(db)?;
        let rows = c.query(&st, &[&src, &epoch, &seq]).await.map_err(db)?;
        Ok(rows.into_iter().map(|r| head_row(&r, 0)).collect())
    }

    async fn fail(&self, key: &EventKey, delay: Duration) -> Result<()> {
        let c = self.client().await?;
        let st = c
            .prepare_cached(&self.q("UPDATE octopus.event e SET attempts = e.attempts + 1, not_before = now() + $4::text::interval \
                  FROM octopus.source s WHERE s.source = $1 AND e.sid = s.id AND e.epoch = $2 AND e.seq = $3"),
            )
            .await
            .map_err(db)?;
        c.execute(&st, &[&key.source, &key.pos.epoch, &key.pos.seq, &interval(delay)]).await.map_err(db)?;
        Ok(())
    }

    async fn skip(&self, source: &str, after: Pos, to_seq: Option<i64>, reason: &str) -> Result<Option<(Pos, Skipped)>> {
        let c = self.client().await?;
        let row = c
            .query_one(&self.q("SELECT new_epoch, new_seq, from_seq, to_seq FROM octopus.skip($1, $2, $3, $4, $5)"),
                &[&source, &after.epoch, &after.seq, &to_seq, &reason],
            )
            .await
            .map_err(db)?;
        Ok(match (row.get::<_, Option<i64>>(0), row.get::<_, Option<i64>>(1)) {
            (Some(e), Some(q)) => Some((
                Pos::new(e, q),
                Skipped { epoch: after.epoch, from_seq: row.get::<_, Option<i64>>(2).unwrap_or(after.seq + 1), to_seq: row.get(3) },
            )),
            _ => None,
        })
    }

    async fn groups(&self) -> Result<Vec<String>> {
        let c = self.client().await?;
        let rows = c.query(&self.q("SELECT name FROM octopus.grp ORDER BY name"), &[]).await.map_err(db)?;
        Ok(rows.into_iter().map(|r| r.get(0)).collect())
    }

    async fn backlog(&self) -> Result<i64> {
        let c = self.client().await?;
        let row = c
            .query_one(&self.q("SELECT count(*) FROM octopus.event e JOIN octopus.source s ON s.id = e.sid \
                    WHERE (e.epoch, e.seq) > (s.epoch, s.last_seq)"),
                &[],
            )
            .await
            .map_err(db)?;
        Ok(row.get(0))
    }

    async fn heartbeat(&self, member: &Member, ttl: Duration) -> Result<Vec<Member>> {
        let c = self.client().await?;
        c.execute(&self.q("INSERT INTO octopus.member (member_id, address, heartbeat) VALUES ($1, $2, now()) \
             ON CONFLICT (member_id) DO UPDATE SET address = excluded.address, heartbeat = excluded.heartbeat"),
            &[&member.id, &member.address],
        )
        .await
        .map_err(db)?;
        let rows = c
            .query(&self.q("SELECT member_id, address FROM octopus.member WHERE heartbeat > now() - $1::text::interval ORDER BY member_id"),
                &[&interval(ttl)],
            )
            .await
            .map_err(db)?;
        Ok(rows.into_iter().map(|r| Member { id: r.get(0), address: r.get(1) }).collect())
    }

    async fn renew(&self, member: &str, groups: &[String], ttl: Duration) -> Result<Vec<String>> {
        let c = self.client().await?;
        let rows = c
            .query(&self.q("WITH l AS (SELECT grp FROM octopus.lease WHERE owner = $1 AND grp = ANY($2) ORDER BY grp FOR UPDATE) \
                 UPDATE octopus.lease x SET expires_at = now() + $3::text::interval FROM l \
                  WHERE x.grp = l.grp RETURNING x.grp"),
                &[&member, &groups, &interval(ttl)],
            )
            .await
            .map_err(db)?;
        Ok(rows.into_iter().map(|r| r.get(0)).collect())
    }

    async fn claim(&self, member: &str, groups: &[String], ttl: Duration) -> Result<Vec<Lease>> {
        if groups.is_empty() {
            return Ok(vec![]);
        }
        let c = self.client().await?;
        let rows = c
            .query(&self.q("INSERT INTO octopus.lease AS l (grp, owner, epoch, expires_at) \
                 SELECT g, $1, 1, now() + $3::text::interval FROM unnest($2::text[]) AS g ORDER BY g \
                 ON CONFLICT (grp) DO UPDATE SET owner = excluded.owner, epoch = l.epoch + 1, expires_at = excluded.expires_at \
                 WHERE l.expires_at < now() OR l.owner = excluded.owner \
                 RETURNING grp, owner, epoch"),
                &[&member, &groups, &interval(ttl)],
            )
            .await
            .map_err(db)?;
        Ok(rows.into_iter().map(|r| Lease { group: r.get(0), owner: r.get(1), epoch: r.get(2) }).collect())
    }

    async fn release(&self, member: &str, groups: &[String]) -> Result<()> {
        let c = self.client().await?;
        c.execute(
            &self.q("WITH l AS (SELECT grp FROM octopus.lease WHERE owner = $1 AND grp = ANY($2) ORDER BY grp FOR UPDATE) \
                     DELETE FROM octopus.lease x USING l WHERE x.grp = l.grp"),
            &[&member, &groups],
        )
            .await
            .map_err(db)?;
        Ok(())
    }

    async fn leave(&self, member: &str) -> Result<()> {
        let c = self.client().await?;
        c.execute(
            &self.q("WITH l AS (SELECT grp FROM octopus.lease WHERE owner = $1 ORDER BY grp FOR UPDATE) \
                     DELETE FROM octopus.lease x USING l WHERE x.grp = l.grp"),
            &[&member],
        )
        .await
        .map_err(db)?;
        c.execute(&self.q("DELETE FROM octopus.member WHERE member_id = $1"), &[&member]).await.map_err(db)?;
        Ok(())
    }

    async fn gaps(&self, groups: &[String], older_than: Duration, limit: i32) -> Result<Vec<Gap>> {
        if groups.is_empty() {
            return Ok(vec![]);
        }
        let older_us = older_than.as_micros().min(i64::MAX as u128) as i64;
        let c = self.client().await?;
        // sources with something stored beyond the cursor but no head; the first stored position tells the gap
        let st = c
            .prepare_cached(&self.q("SELECT s.grp, s.source, s.epoch, s.last_seq, \
                        (array_agg(e.epoch ORDER BY e.epoch, e.seq))[1], (array_agg(e.seq ORDER BY e.epoch, e.seq))[1], \
                        (extract(epoch FROM now() - min(e.enqueued_at)) * 1000000)::bigint, min(g.gap_skip_ms) \
                   FROM octopus.source s \
                   JOIN octopus.event e ON e.sid = s.id AND (e.epoch, e.seq) > (s.epoch, s.last_seq) \
                   JOIN octopus.grp g ON g.name = s.grp \
                  WHERE s.grp = ANY($1) \
                  GROUP BY s.grp, s.source, s.epoch, s.last_seq \
                 HAVING NOT bool_or(e.epoch = s.epoch AND e.seq = s.last_seq + 1) \
                    AND min(e.enqueued_at) < now() - $2::bigint * interval '1 microsecond' \
                  ORDER BY 7 DESC LIMIT $3::int"))
            .await
            .map_err(db)?;
        let rows = c.query(&st, &[&groups, &older_us, &limit]).await.map_err(db)?;
        Ok(rows
            .iter()
            .map(|r| Gap {
                group: r.get(0),
                source: r.get(1),
                cursor: Pos::new(r.get(2), r.get(3)),
                first_stored: Pos::new(r.get(4), r.get(5)),
                waiting: Duration::from_micros(r.get::<_, i64>(6).max(0) as u64),
                gap_skip_ms: r.get(7),
            })
            .collect())
    }

    async fn purge_applied(&self, limit: i32) -> Result<i64> {
        let c = self.client().await?;
        let row = c.query_one(&self.q("SELECT octopus.purge_applied($1)"), &[&limit]).await.map_err(db)?;
        Ok(row.get(0))
    }

    async fn leases(&self) -> Result<Vec<(Lease, String)>> {
        let c = self.client().await?;
        let rows = c
            .query(&self.q("SELECT l.grp, l.owner, l.epoch, m.address FROM octopus.lease l \
                 JOIN octopus.member m ON m.member_id = l.owner WHERE l.expires_at > now() ORDER BY l.grp"),
                &[],
            )
            .await
            .map_err(db)?;
        Ok(rows
            .into_iter()
            .map(|r| (Lease { group: r.get(0), owner: r.get(1), epoch: r.get(2) }, r.get(3)))
            .collect())
    }
}
