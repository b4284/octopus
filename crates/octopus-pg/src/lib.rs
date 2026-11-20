//! PostgreSQL backend for Octopus.
//!
//! Every operation is one round trip: a server-side function (publish, commit) or a single statement. The schema
//! lives in the `octopus` schema and is created by [`PgBackend::connect`] when `migrate` is set.

use std::time::Duration;

use bytes::Bytes;
use deadpool_postgres::{Manager, ManagerConfig, Pool, RecyclingMethod};
use octopus_core::{
    Backend, Commit, CommitOutcome, Error, Event, EventKey, Head, HeadRequest, Lease, Member, PublishOutcome, Result,
};
use tokio_postgres::NoTls;

const SCHEMA_SQL: &str = include_str!("../sql/001_init.sql");

/// Connection and hook settings.
#[derive(Clone, Debug)]
pub struct PgConfig {
    /// libpq-style connection string, e.g. `host=127.0.0.1 port=5432 user=octopus dbname=app`.
    /// A multi-host string with `target_session_attrs=read-write` follows a failover to the new primary.
    pub connection: String,
    pub pool_size: usize,
    /// Create or update the `octopus` schema on connect.
    pub migrate: bool,
    /// Apply hook: `fn(sources text[], seqs bigint[], effects bytea[])`, called in the commit transaction with the
    /// events being applied. Writes the application's state.
    pub apply_hook: Option<String>,
    /// Load hook: `fn(sources text[], seqs bigint[], bodies bytea[]) RETURNS TABLE(i int, context bytea)`, with `i`
    /// the 1-based position. Supplies the context handed to workers with each event.
    pub load_hook: Option<String>,
}

impl PgConfig {
    pub fn new(connection: impl Into<String>) -> Self {
        Self { connection: connection.into(), pool_size: 16, migrate: true, apply_hook: None, load_hook: None }
    }
}

pub struct PgBackend {
    pool: Pool,
    apply_hook: Option<String>,
    load_sql: Option<String>,
}

fn db(e: impl std::fmt::Display) -> Error {
    Error::Database(e.to_string())
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
        if cfg.migrate {
            // One migrator at a time; the script itself is idempotent.
            client
                .batch_execute(&format!("BEGIN; SELECT pg_advisory_xact_lock(hashtext('octopus.migrate')); {SCHEMA_SQL}; COMMIT;"))
                .await
                .map_err(db)?;
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
        Ok(Self { pool, apply_hook: cfg.apply_hook, load_sql })
    }

    async fn client(&self) -> Result<deadpool_postgres::Object> {
        self.pool.get().await.map_err(db)
    }

    /// Delete rows left at or below their cursor (see `octopus.purge_applied`). Run now and then.
    pub async fn purge_applied(&self, limit: i32) -> Result<i64> {
        let c = self.client().await?;
        let row = c.query_one("SELECT octopus.purge_applied($1)", &[&limit]).await.map_err(db)?;
        Ok(row.get(0))
    }
}

impl Backend for PgBackend {
    async fn publish(&self, events: &[Event]) -> Result<Vec<PublishOutcome>> {
        if events.is_empty() {
            return Ok(vec![]);
        }
        let grp: Vec<&str> = events.iter().map(|e| e.group.as_str()).collect();
        let src: Vec<&str> = events.iter().map(|e| e.key.source.as_str()).collect();
        let seq: Vec<i64> = events.iter().map(|e| e.key.seq).collect();
        let body: Vec<&[u8]> = events.iter().map(|e| e.body.as_ref()).collect();
        if seq.iter().any(|&q| q < 1) {
            return Err(Error::Invalid("seq starts at 1".into()));
        }
        let c = self.client().await?;
        let st = c.prepare_cached("SELECT octopus.publish($1, $2, $3, $4)").await.map_err(db)?;
        let row = c.query_one(&st, &[&grp, &src, &seq, &body]).await.map_err(db)?;
        let codes: Vec<i16> = row.get(0);
        Ok(codes
            .into_iter()
            .map(|c| match c {
                0 => PublishOutcome::Stored,
                2 => PublishOutcome::GroupMismatch,
                _ => PublishOutcome::Duplicate,
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
            .prepare_cached(
                "SELECT x.grp, x.source, x.seq, x.body, x.attempts, \
                        (extract(epoch FROM x.enqueued_at) * 1000000)::bigint \
                   FROM unnest($1::text[], $2::int[]) AS g(name, n) CROSS JOIN LATERAL ( \
                        SELECT e.grp, e.source, e.seq, e.body, e.attempts, e.enqueued_at \
                          FROM octopus.source s \
                          JOIN octopus.event e ON e.source = s.source AND e.seq = s.last_seq + 1 \
                         WHERE s.grp = g.name AND s.source <> ALL($3::text[]) \
                           AND (e.not_before IS NULL OR e.not_before <= now()) \
                         ORDER BY e.enqueued_at, e.source \
                         LIMIT g.n) x",
            )
            .await
            .map_err(db)?;
        let rows = c.query(&st, &[&grp, &max, &exclude]).await.map_err(db)?;
        Ok(rows
            .into_iter()
            .map(|r| Head {
                group: r.get(0),
                key: EventKey { source: r.get(1), seq: r.get(2) },
                body: Bytes::from(r.get::<_, Vec<u8>>(3)),
                attempts: r.get(4),
                enqueued_at_us: r.get(5),
            })
            .collect())
    }

    async fn load_context(&self, heads: &[Head]) -> Result<Vec<Option<Bytes>>> {
        let mut out = vec![None; heads.len()];
        let Some(sql) = &self.load_sql else { return Ok(out) };
        if heads.is_empty() {
            return Ok(out);
        }
        let src: Vec<&str> = heads.iter().map(|h| h.key.source.as_str()).collect();
        let seq: Vec<i64> = heads.iter().map(|h| h.key.seq).collect();
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
        let seq: Vec<i64> = commits.iter().map(|c| c.key.seq).collect();
        let eff: Vec<&[u8]> = commits.iter().map(|c| c.effect.as_ref()).collect();
        let c = self.client().await?;
        let st = c.prepare_cached("SELECT octopus.commit_many($1, $2, $3, $4)").await.map_err(db)?;
        let row = c.query_one(&st, &[&src, &seq, &eff, &self.apply_hook]).await.map_err(db)?;
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

    async fn fail(&self, key: &EventKey, delay: Duration) -> Result<()> {
        let c = self.client().await?;
        let st = c
            .prepare_cached(
                "UPDATE octopus.event SET attempts = attempts + 1, not_before = now() + $3::text::interval \
                  WHERE source = $1 AND seq = $2",
            )
            .await
            .map_err(db)?;
        c.execute(&st, &[&key.source, &key.seq, &interval(delay)]).await.map_err(db)?;
        Ok(())
    }

    async fn groups(&self) -> Result<Vec<String>> {
        let c = self.client().await?;
        let rows = c.query("SELECT name FROM octopus.grp ORDER BY name", &[]).await.map_err(db)?;
        Ok(rows.into_iter().map(|r| r.get(0)).collect())
    }

    async fn backlog(&self) -> Result<i64> {
        let c = self.client().await?;
        let row = c
            .query_one(
                "SELECT count(*) FROM octopus.event e JOIN octopus.source s ON s.source = e.source WHERE e.seq > s.last_seq",
                &[],
            )
            .await
            .map_err(db)?;
        Ok(row.get(0))
    }

    async fn heartbeat(&self, member: &Member, ttl: Duration) -> Result<Vec<Member>> {
        let c = self.client().await?;
        c.execute(
            "INSERT INTO octopus.member (member_id, address, heartbeat) VALUES ($1, $2, now()) \
             ON CONFLICT (member_id) DO UPDATE SET address = excluded.address, heartbeat = excluded.heartbeat",
            &[&member.id, &member.address],
        )
        .await
        .map_err(db)?;
        let rows = c
            .query(
                "SELECT member_id, address FROM octopus.member WHERE heartbeat > now() - $1::text::interval ORDER BY member_id",
                &[&interval(ttl)],
            )
            .await
            .map_err(db)?;
        Ok(rows.into_iter().map(|r| Member { id: r.get(0), address: r.get(1) }).collect())
    }

    async fn renew(&self, member: &str, groups: &[String], ttl: Duration) -> Result<Vec<String>> {
        let c = self.client().await?;
        let rows = c
            .query(
                "UPDATE octopus.lease SET expires_at = now() + $3::text::interval \
                  WHERE owner = $1 AND grp = ANY($2) RETURNING grp",
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
            .query(
                "INSERT INTO octopus.lease AS l (grp, owner, epoch, expires_at) \
                 SELECT g, $1, 1, now() + $3::text::interval FROM unnest($2::text[]) AS g ORDER BY g \
                 ON CONFLICT (grp) DO UPDATE SET owner = excluded.owner, epoch = l.epoch + 1, expires_at = excluded.expires_at \
                 WHERE l.expires_at < now() OR l.owner = excluded.owner \
                 RETURNING grp, owner, epoch",
                &[&member, &groups, &interval(ttl)],
            )
            .await
            .map_err(db)?;
        Ok(rows.into_iter().map(|r| Lease { group: r.get(0), owner: r.get(1), epoch: r.get(2) }).collect())
    }

    async fn release(&self, member: &str, groups: &[String]) -> Result<()> {
        let c = self.client().await?;
        c.execute("DELETE FROM octopus.lease WHERE owner = $1 AND grp = ANY($2)", &[&member, &groups])
            .await
            .map_err(db)?;
        Ok(())
    }

    async fn leave(&self, member: &str) -> Result<()> {
        let c = self.client().await?;
        c.execute("DELETE FROM octopus.lease WHERE owner = $1", &[&member]).await.map_err(db)?;
        c.execute("DELETE FROM octopus.member WHERE member_id = $1", &[&member]).await.map_err(db)?;
        Ok(())
    }

    async fn leases(&self) -> Result<Vec<(Lease, String)>> {
        let c = self.client().await?;
        let rows = c
            .query(
                "SELECT l.grp, l.owner, l.epoch, m.address FROM octopus.lease l \
                 JOIN octopus.member m ON m.member_id = l.owner WHERE l.expires_at > now() ORDER BY l.grp",
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
