//! Core types of Octopus and the interface every database backend implements.
//!
//! Correctness lives in the backend's transactions, not in dispatcher memory:
//! - [`Backend::publish`] stores an event only if its position `(epoch, seq)` is beyond the source's cursor; the
//!   unique key `(source, epoch, seq)` absorbs retries.
//! - [`Backend::commit`] applies an event only if the source's cursor is exactly `seq - 1`, and advances the
//!   cursor, runs the application's apply hook and deletes the event, all in one transaction.
//!
//! So a duplicate or stale delivery can never be applied twice, out of order, or lost.

use std::future::Future;
use std::time::Duration;

use bytes::Bytes;

/// A position in a source's sequence. The source stamps both: `epoch` names one run of its counter (a new epoch
/// starts when the source cannot be sure of its counter, e.g. after losing state), `seq` counts from 1 within the
/// epoch, gapless. Positions order by epoch, then seq.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Pos {
    pub epoch: i64,
    pub seq: i64,
}

impl Pos {
    pub const fn new(epoch: i64, seq: i64) -> Self {
        Self { epoch, seq }
    }

    /// The next position in the same epoch.
    pub const fn next(self) -> Self {
        Self { epoch: self.epoch, seq: self.seq + 1 }
    }
}

impl std::fmt::Display for Pos {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}:{}", self.epoch, self.seq)
    }
}

/// Identifies one event: its source and its position.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct EventKey {
    pub source: String,
    pub pos: Pos,
}

impl EventKey {
    pub fn new(source: impl Into<String>, pos: Pos) -> Self {
        Self { source: source.into(), pos }
    }
}

/// Where a source's previous epoch ended, declared by the first events of a new epoch: once the cursor reaches
/// `(epoch, last_seq)`, it moves on to the declaring event's epoch.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EpochEnd {
    pub epoch: i64,
    pub last_seq: i64,
}

/// An event as published. `group` is the unit of ownership and of worker typing; a source belongs to exactly
/// one group, fixed by its first publish.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Event {
    pub group: String,
    pub key: EventKey,
    pub body: Bytes,
    /// A skip marker: the source declares that `key.pos.seq ..= skip_to` (same epoch) have no events (lost and not
    /// recoverable). It is never handed to a worker; the cursor jumps over the range when it becomes the head.
    pub skip_to: Option<i64>,
    /// Declared end of the source's previous epoch (on the first events of a new epoch).
    pub prev: Option<EpochEnd>,
}

/// The result of storing one event, plus how many positions directly below it (same epoch) are missing: neither
/// stored nor applied. A hint for the producer (the missing ones may still be in flight).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Published {
    pub outcome: PublishOutcome,
    pub missing_below: i64,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PublishOutcome {
    /// Newly stored; not its source's next event yet (an earlier one is still pending).
    Stored,
    /// Newly stored, and it is its source's next event (seq = cursor + 1) right now: ready to hand out.
    StoredHead,
    /// Already stored, or already applied: nothing to do (a producer retry).
    Duplicate,
    /// The source already belongs to another group: rejected.
    GroupMismatch,
    /// This (source, seq) is stored with a different body: the source reused a number. Rejected, nothing changed.
    Conflict,
    /// From an epoch older than the source's current one and beyond that epoch's declared end: a stale producer
    /// instance (fencing). Rejected.
    StaleEpoch,
    /// Its position was skipped (declared lost, or given up after the gap timeout) before it arrived. Not handed
    /// out; kept in the late table.
    Late,
}

/// Ask for up to `max` heads of `group`, leaving out sources that already have an event in flight.
#[derive(Clone, Debug)]
pub struct HeadRequest {
    pub group: String,
    pub max: usize,
    pub exclude_sources: Vec<String>,
}

/// The next event of a source (its position follows the cursor), ready to be handed out.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Head {
    pub group: String,
    pub key: EventKey,
    pub body: Bytes,
    /// A skip marker (see [`Event::skip_to`]): never handed to a worker.
    pub skip_to: Option<i64>,
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

/// A range of positions given up: no event will be handed out for them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Skipped {
    pub epoch: i64,
    pub from_seq: i64,
    /// None: the rest of the epoch (an epoch closed without a declared end).
    pub to_seq: Option<i64>,
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

/// A source whose next event (cursor + 1) is missing while a later one is stored: it cannot move on.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Gap {
    pub group: String,
    pub source: String,
    /// The last applied position (as written to the database).
    pub cursor: Pos,
    /// The lowest stored position above the cursor. Same epoch: `cursor.seq + 1 ..= first_stored.seq - 1` are
    /// missing. A later epoch: the rest of the cursor's epoch is unknown (it has no declared end).
    pub first_stored: Pos,
    /// How long the oldest stored event of the source has waited, database clock.
    pub waiting: Duration,
    /// The group's gap policy (`grp.gap_skip_ms`): None = the dispatchers' default, negative = never skip.
    pub gap_skip_ms: Option<i32>,
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
    /// Store events. Idempotent per `(source, epoch, seq)`; events at or below the cursor are not stored. Records
    /// declared epoch ends. One result per event, in order.
    fn publish(&self, events: &[Event]) -> impl Future<Output = Result<Vec<Published>>> + Send;

    /// Read the heads of the requested groups (oldest first), skipping events waiting for a retry delay.
    fn read_heads(&self, requests: &[HeadRequest]) -> impl Future<Output = Result<Vec<Head>>> + Send;

    /// Context for the given heads from the application's load hook (None if no hook is configured).
    fn load_context(&self, heads: &[Head]) -> impl Future<Output = Result<Vec<Option<Bytes>>>> + Send;

    /// Apply results. One transaction for the whole batch; one outcome per commit, in order.
    fn commit(&self, commits: &[Commit]) -> impl Future<Output = Result<Vec<CommitOutcome>>> + Send;

    /// A worker reported "done" (it committed its own writes): remove these events so they are never handed out
    /// again. Idempotent: Applied = removed now; Duplicate = already removed. No apply hook runs.
    /// Also returns each source's next event if it is stored and due (so it can be handed out without a search).
    fn remove(&self, keys: &[EventKey]) -> impl Future<Output = Result<Vec<(CommitOutcome, Option<Head>)>>> + Send;

    /// Persist cursors moved by "done" reports, in bulk: each cursor only moves forward; events at or below it are
    /// deleted. Monotonic and idempotent.
    fn advance(&self, cursors: &[(String, Pos)]) -> impl Future<Output = Result<()>> + Send;

    /// The stored events with these keys that are due (not waiting for a retry delay); missing ones are skipped.
    fn read_events(&self, keys: &[EventKey]) -> impl Future<Output = Result<Vec<Head>>> + Send;

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

    /// For each (source, position): how many positions of that epoch directly up to and including it (the run ending at
    /// it) are neither stored nor
    /// applied (a high-water check: catches a lost last event).
    fn missing_up_to(&self, positions: &[(String, Pos)]) -> impl Future<Output = Result<Vec<i64>>> + Send;

    /// Give up positions of a source that is stopped by a missing event, if its cursor is still `after` (otherwise
    /// nothing changes and None is returned: someone moved it meanwhile). `to_seq` Some: skip `after.seq + 1 ..=
    /// to_seq` in the cursor's epoch. None: close the cursor's epoch where it stands and move on to the next stored
    /// epoch. Records the skip; returns the new cursor and what was skipped.
    fn skip(&self, source: &str, after: Pos, to_seq: Option<i64>, reason: &str)
    -> impl Future<Output = Result<Option<(Pos, Skipped)>>> + Send;

    /// Sources of `groups` whose next event is missing while a later one has waited at least `older_than`
    /// (database clock), longest waiting first, at most `limit`.
    fn gaps(&self, groups: &[String], older_than: Duration, limit: i32) -> impl Future<Output = Result<Vec<Gap>>> + Send;

    /// Housekeeping: delete up to `limit` rows left at or below their cursor by a publish/commit race.
    fn purge_applied(&self, limit: i32) -> impl Future<Output = Result<i64>> + Send;
}
