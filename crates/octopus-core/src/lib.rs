//! Core types of Octopus and the interface every database backend implements.
//!
//! Correctness lives in the backend's transactions, not in dispatcher memory:
//! - [`Backend::publish`] stores an event only if its seq is beyond the source's cursor; the unique key
//!   `(source, seq)` absorbs retries.
//! - [`Backend::commit`] applies an event only if the source's cursor is exactly `seq - 1`, and advances the
//!   cursor, runs the application's apply hook and deletes the event, all in one transaction.
//!
//! So a duplicate or stale delivery can never be applied twice, out of order, or lost.

use std::future::Future;
use std::time::Duration;

use bytes::Bytes;

/// Identifies one event: the source that produced it and the source's gapless sequence number (from 1).
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EventKey {
    pub source: String,
    pub seq: i64,
}

impl EventKey {
    pub fn new(source: impl Into<String>, seq: i64) -> Self {
        Self { source: source.into(), seq }
    }
}

/// An event as published. `group` is the unit of ownership and of worker typing; a source belongs to exactly
/// one group, fixed by its first publish.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Event {
    pub group: String,
    pub key: EventKey,
    pub body: Bytes,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublishOutcome {
    /// Newly stored.
    Stored,
    /// Already stored, or already applied: nothing to do (a producer retry).
    Duplicate,
    /// The source already belongs to another group: rejected.
    GroupMismatch,
}

/// Ask for up to `max` heads of `group`, leaving out sources that already have an event in flight.
#[derive(Clone, Debug)]
pub struct HeadRequest {
    pub group: String,
    pub max: usize,
    pub exclude_sources: Vec<String>,
}

/// The next event of a source (seq = cursor + 1), ready to be handed out.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Head {
    pub group: String,
    pub key: EventKey,
    pub body: Bytes,
    /// Failed attempts so far.
    pub attempts: i32,
    /// Database time the event was stored, microseconds since the Unix epoch.
    pub enqueued_at_us: i64,
}

/// A worker's result: the effect is passed to the application's apply hook in the commit transaction.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Commit {
    pub key: EventKey,
    pub effect: Bytes,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CommitOutcome {
    /// The cursor was `seq - 1`: applied, cursor advanced, event deleted.
    Applied,
    /// The cursor was already at or beyond `seq`: nothing applied (a duplicate delivery); the event row, if any,
    /// is gone.
    Duplicate,
    /// The cursor is behind `seq - 1` (an earlier event is not applied yet), or the source is unknown.
    Gap,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Member {
    pub id: String,
    /// Where the member can be reached (e.g. its topic prefix on the fabric).
    pub address: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Lease {
    pub group: String,
    pub owner: String,
    pub epoch: i64,
}

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("database error: {0}")]
    Database(String),
    #[error("invalid input: {0}")]
    Invalid(String),
    #[error("configuration error: {0}")]
    Config(String),
}

pub type Result<T> = std::result::Result<T, Error>;

/// A database backend. Every method is one transaction; results are returned only after the commit (and, with a
/// synchronous standby, after the standby has it).
pub trait Backend: Send + Sync + 'static {
    /// Store events. Idempotent per `(source, seq)`; events at or below the cursor are not stored.
    fn publish(&self, events: &[Event]) -> impl Future<Output = Result<Vec<PublishOutcome>>> + Send;

    /// Read the heads of the requested groups (oldest first), skipping events waiting for a retry delay.
    fn read_heads(&self, requests: &[HeadRequest]) -> impl Future<Output = Result<Vec<Head>>> + Send;

    /// Context for the given heads from the application's load hook (None if no hook is configured).
    fn load_context(&self, heads: &[Head]) -> impl Future<Output = Result<Vec<Option<Bytes>>>> + Send;

    /// Apply results. One transaction for the whole batch; one outcome per commit, in order.
    fn commit(&self, commits: &[Commit]) -> impl Future<Output = Result<Vec<CommitOutcome>>> + Send;

    /// A worker reported "done" (it committed its own writes): remove these events so they are never handed out
    /// again. Idempotent: Applied = removed now; Duplicate = already removed. No apply hook runs.
    fn remove(&self, keys: &[EventKey]) -> impl Future<Output = Result<Vec<CommitOutcome>>> + Send;

    /// Record a failed attempt; the event is not handed out again before `delay` has passed.
    fn fail(&self, key: &EventKey, delay: Duration) -> impl Future<Output = Result<()>> + Send;

    /// All groups ever published to.
    fn groups(&self) -> impl Future<Output = Result<Vec<String>>> + Send;

    /// Events stored and not yet applied.
    fn backlog(&self) -> impl Future<Output = Result<i64>> + Send;

    /// Record this member as alive; returns every member seen within `ttl` (database clock), this one included.
    fn heartbeat(&self, member: &Member, ttl: Duration) -> impl Future<Output = Result<Vec<Member>>> + Send;

    /// Extend this member's leases on `groups`; returns the groups it still holds.
    fn renew(&self, member: &str, groups: &[String], ttl: Duration) -> impl Future<Output = Result<Vec<String>>> + Send;

    /// Take the leases on `groups` that are free or expired (database clock); returns the leases obtained.
    fn claim(&self, member: &str, groups: &[String], ttl: Duration) -> impl Future<Output = Result<Vec<Lease>>> + Send;

    /// Give leases back at once (planned hand-over).
    fn release(&self, member: &str, groups: &[String]) -> impl Future<Output = Result<()>> + Send;

    /// Leave the cluster: release every lease and drop the membership row.
    fn leave(&self, member: &str) -> impl Future<Output = Result<()>> + Send;

    /// Every unexpired lease, with its owner's address.
    fn leases(&self) -> impl Future<Output = Result<Vec<(Lease, String)>>> + Send;

    /// Housekeeping: delete up to `limit` rows left at or below their cursor by a publish/commit race.
    fn purge_applied(&self, limit: i32) -> impl Future<Output = Result<i64>> + Send;
}
