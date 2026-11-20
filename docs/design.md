# Octopus: design (draft for review)

Status: **draft for review, 2026-11-20.** The owner's decisions are in §10. Nothing is built yet.

Octopus is a queue system for **ordered per-key event processing** on top of an ordinary SQL database. Producers
publish events per source (key). A cluster of dispatchers, with no leader, stores them, hands them to workers
with strict per-source order and no head-of-line blocking across sources, and applies each event's state change
exactly once. The SQL database is the only durable state.

## 1. Goals

1. **Zero loss.** An event the system has acknowledged to a producer is never lost. Losing any single node, or a
   whole data center (DC), must not lose it.
2. **Strict active-active dispatchers.**
   - There is no leader. Every instance accepts publishes, hands out work and runs commits.
   - Fewer instances only means less capacity.
   - Scaling is adding pods: a new instance joins and takes its share without configuration.
3. **Deploy anywhere.** Dispatchers are stateless containers. Every durable fact lives in the SQL database.
   An instance can be killed, moved or rescheduled at any time.
4. **The processing rules:**
   - per-source order;
   - each event's state change applied exactly once;
   - no cross-source blocking while a slot of the group is free;
   - 24k ev/s on one SQL platform.

### Non-goals

- **Exactly-once execution of external side effects.** No system can promise it. The guarantee is
  exactly-once **effect on the database state**, and at-least-once **execution** of the worker.
  - Workers that call outside systems must pass an idempotency key: `(source_id, seq)`.
- **Zero downtime during a database failover.** That failover is zero loss, but publishes and commits pause
  until the new primary is up: seconds with automatic failover.

## 2. Guarantees (invariants checked by an end-state oracle in every test)

| # | Invariant | Enforced by |
|---|---|---|
| G1 | A publish is acknowledged only after its row is committed on the primary **and** on a synchronous replica in another DC. | The database's synchronous quorum replication; the dispatcher never acknowledges before commit |
| G2 | Each event's state change is applied at most once and is never skipped: the source cursor moves from seq−1 to seq exactly once. | Cursor check, state change and queue delete in **one transaction** in the database |
| G3 | Per-source order: event seq is applied only after seq−1. | The same cursor check, in the database |
| G4 | A duplicate publish (producer retry) is stored at most once. | Unique key `(source_id, seq)`, and an insert guarded by the cursor |
| G5 | Every committed event is eventually processed while one dispatcher and the database primary are up. | Leases expire and are reclaimed; heads are re-read from the database on every ownership change; fallback scans |
| G6 | No cross-source blocking. | Hand-out only into free worker slots, with no read-ahead into busy workers |

**Safety (G1–G4) never depends on dispatcher memory, leases, clocks or the network.** Two dispatchers that both
believe they own a group can only cause a duplicate *delivery*, which the database then turns into "already
applied". This is the key design property: every split-brain or zombie case at the dispatcher layer is reduced
to a duplicate delivery, and those are harmless by construction.

## 3. Failure model

**Tolerated, and each tested by a drill (§9):**
- A dispatcher process crashes, is killed (`kill -9`), stopped cleanly (rolling upgrade) or paused (GC pause
  or `SIGSTOP` longer than the lease).
- A worker or producer pod dies at any point, including between commit and acknowledgement.
- Network partitions:
  - dispatcher ↔ database;
  - dispatcher ↔ some workers;
  - dispatcher ↔ dispatcher;
  - a whole DC isolated.
- The database primary dies, or its whole DC is lost.
- Clock skew between pods. Lease timing uses the database clock, plus each pod's monotonic clock for its own
  deadline.
- A slow database: back-pressure reaches producers; nothing is dropped.

**Not tolerated (outside the promise):**
- Losing the primary **and** every synchronous replica at the same time. That includes losing one DC and then a
  second node in the other DC before the first DC is back.
- Disk corruption that the database does not detect.
- Byzantine (malicious or wrong) nodes.

## 4. Architecture

```
 producers ──publish──▶ ┌──────────────────────────────┐ ◀──connect── workers (every worker pod connects
 (any instance)         │ dispatcher pods (N, no leader)│               to every dispatcher pod)
                        │  • accept publishes (batched)│ ──push work into free slots──▶
                        │  • own a share of the groups │ ◀──commit / fail─────────────
                        │  • batched commits           │
                        └──────────────┬───────────────┘
                                       │ few pooled connections per pod
                        ┌──────────────▼───────────────┐
                        │ SQL primary (DC1)            │──sync quorum──▶ replica DC2, replica DC3
                        └──────────────────────────────┘   automatic failover (Patroni + etcd over 3 sites)
```

### 4.1 The database layer: where zero loss comes from

A dispatcher cannot be more durable than the database it writes to.
- **One writable primary.** Synchronous **quorum** replication to replicas in other DCs, for example
  `synchronous_standby_names = 'ANY 1 (dc2, dc3)'`.
- **Automatic failover with fencing.** Patroni, with etcd over **two DCs plus a witness site** (§10).
  - The witness runs only the third etcd member; it holds no data.
  - Two DCs alone cannot tell "the other DC died" from "the link died". The witness gives the majority that
    decides which side may have a primary.
- **Replica placement** (§10: 2 DCs and a witness):
  - DC1: the primary plus a local replica. DC2: one or two replicas.
  - **Both DCs up:** an acknowledged write is on the primary **and at least one DC2 replica**. Losing DC1 then
    loses nothing.
  - **One DC down:** the synchronous replica switches to the surviving DC's local replica, so acknowledged
    writes are still on two nodes. Losing a second node in the same DC before the first DC is back is the one
    case outside the promise.
  - Patroni's synchronous and quorum modes must be checked to express exactly this rule; that check is the
    first infrastructure task.
- **Cost:** every acknowledged publish and every commit waits one round trip to the nearest other DC.
  - Throughput holds, because the dispatcher batches: one round trip covers a whole batch.
  - Latency grows by that round trip. A cross-DC test measures it.
- The dispatchers are active-active; the SQL layer is single-writer with synchronous replicas. That is the only
  way to have zero loss **and** one plain SQL database.
  - A distributed SQL database (Raft-based, Postgres-compatible: YugabyteDB, CockroachDB) would make storage
    multi-writer too, at a higher cost per write. The dispatcher's large batches suit such databases;
    a later option.

### 4.2 Data model (the same on every SQL engine)

| table | key | holds |
|---|---|---|
| `queue` | `(group_id, source_id, seq)` | event body, enqueued time, attempt count, `not_before` (retry delay) |
| `source` | `source_id` | `group_id`, `last_seq` (the cursor), plus the application state rows |
| `member` | `member_id` | address, heartbeat (database time) |
| `lease` | `group_id` | owner, epoch, expires_at (database time) |
| `dead_letter` (optional, §6.5) | `(source_id, seq)` | event, last error |

A source's **head** is the row with `seq = last_seq + 1`. Only heads are handed out. A head becomes available
only when its predecessor's commit has moved the cursor, so ordering comes from the database. It does not
depend on insert order or on ids.

## 5. Protocol over a pub/sub fabric (owner, 2026-11-20)

**The organisation already runs Solace,** so producers, dispatchers and workers talk over the existing pub/sub
fabric instead of gRPC.
- **The transport is pluggable.** The first binding is **MQTT 5**: Solace PubSub+ speaks it natively, and so do
  small brokers that run in a container (Mosquitto, EMQX). The same client code then runs against Solace in
  production and against a container in tests.
- **Later bindings:** AMQP 1.0, which Solace also speaks, or Solace's native API if its throughput is needed.
- **Tests:** the free Solace PubSub+ Standard container image can be used alongside Mosquitto, so the binding is
  checked against the real product.

**The fabric is never the durability point.** Every message is idempotent, and every acknowledgement that
matters is an *application* reply sent only after the SQL commit. So messages can travel as non-persistent
(direct) messages; a lost message is simply retried. Persistent queues on the fabric would add cost without
adding safety, and a Solace queue would bring back the "one slow event blocks the queue" problem. Payloads are
Protobuf, versioned. TLS and client authentication come from the fabric.

**Topics** (`{ns}` = a namespace prefix):

| topic | from → to | carries |
|---|---|---|
| `{ns}/pub/{group}/{source}` | producer → any dispatcher (shared subscription) | `Publish`; reply on the producer's response topic after the commit |
| `{ns}/disp/{member}/hint` | dispatcher → owner dispatcher | "group g has new heads" |
| `{ns}/work/{worker}` | dispatcher → one worker | `Deliver` |
| `{ns}/credit/{member}` | worker → one dispatcher | credits; the worker's hello (groups served, slots) |
| `{ns}/commit/{member}` | worker → the dispatcher that delivered (any dispatcher if it is gone) | `Commit` / `Fail`; reply on the worker's topic |
| `{ns}/members` (retained) | dispatcher → everyone | live members and their topics, for discovery; the database stays the authority |

Request/reply uses MQTT 5 response topics and correlation data, or Solace's own request/reply.

All messages are idempotent, so any of them can be retried.

### 5.1 Publish (producer → any dispatcher)

1. A producer sends `Publish(source_id, seq, group_id, body)`. It can send many at once and may retry freely.
2. The instance adds the event to a batch with every concurrent publish. There is no timer: a batch is whatever
   arrived while the previous batch was being written. It then runs
   `INSERT … SELECT … WHERE seq > cursor ON CONFLICT DO NOTHING`.
3. The instance replies `Accepted` or `Duplicate`, **only after** the commit has been replicated
   synchronously.

What happens next:
- **Fast path:** if this instance owns the group, the event goes straight into its in-memory ready queue and
  is handed out without any database read.
- **Otherwise:** it sends a hint ("group g has new heads") to the owner over the dispatcher mesh.
  - Hints are an optimisation only. If one is lost, the owner's fallback scan finds the row.
  - This replaces Postgres `NOTIFY`, so the design does not depend on the engine.

Back-pressure: each instance bounds publishes in flight. Past the bound, it replies `RetryAfter`. It never
buffers without limit and never drops.

### 5.2 Work (dispatcher ↔ worker)

1. **Connect.** Every worker pod connects to **every** dispatcher pod, found by DNS (a headless service), and
   says which groups (worker type) it serves.
   - A dispatcher pushes only the groups it owns, so there is no routing table.
   - If a worker cannot reach one dispatcher, other workers still serve that dispatcher's groups: partial
     partitions only reduce capacity.
2. **Credits.**
   - Each dispatcher advertises how many heads per group it has ready.
   - Each worker grants credits to the dispatchers from its **free slots only**. The credits a worker has
     granted never exceed its free slots, so no event waits inside a busy worker (G6).
3. **Deliver.** The dispatcher sends `Deliver(group, source, seq, body, preloaded state, attempt)` and records it
   in memory as handed out.
   - Workers call external systems (§10), so **`(source_id, seq)` is the idempotency key** for those calls.
     A delivery can repeat after a crash; the key lets the external system recognise it.
4. **Finish.** The worker answers with one of:
   - `Commit(state change)`: the dispatcher adds it to a batched transaction that checks the cursor, applies
     the change, moves the cursor and deletes the queue row. It replies `Applied` / `Duplicate` / `Conflict`.
   - `Fail(delay)`: the head is offered again after the delay; the attempt count is stored.
5. **Commits are accepted by any instance**, not only the owner. G2 and G3 are checked in the database, so a
   worker whose dispatcher died can still commit through another instance, and nothing is redone needlessly.

### 5.3 Ownership (leases, no leader)

- **Membership.** Every instance writes a heartbeat to `member`. Every instance computes the owner of each group
  from the same live list (rendezvous hashing), so N instances split the groups evenly. A join or leave moves
  only that instance's share.
- **Claiming.** A lease is claimed only when it is free or expired (database clock), and renewed every tick.
  - Each instance also keeps its own deadline on its monotonic clock, counted from when the renewal was
    *sent*. It **stops handing out before** the database could consider the lease expired.
  - A paused or partitioned instance therefore resumes as a non-owner.
  - Each claim increases the group's epoch.
- **Starting to own a group.** The new owner forgets any earlier state of the group and re-reads its heads from
  the database. Messages the previous owner handed out may be delivered again; they are harmless (G2).
- **Planned hand-over** (scale-out, rolling upgrade):
  1. The old owner stops handing out the group.
  2. It waits for the handed-out messages to commit, with a bound.
  3. It deletes the lease; the new owner claims it at once.

  The group pauses only as long as its slowest in-flight event.
- **Crash.** The leases expire after one TTL (target 2–3 s), and the other instances claim the groups. Publishes
  are not affected, because any instance accepts them.

## 6. Failure walk-through

| event | what happens | loss? |
|---|---|---|
| A dispatcher crashes | Its leases expire in 2–3 s and other instances re-read its groups' heads. Workers' commits go to any live instance. Producers re-send publishes that were not acknowledged. | None. Some events are delivered twice; the database applies them once. |
| A dispatcher pauses longer than the TTL | Its own deadline stops hand-outs. After resuming, its next tick finds the lease gone. | None |
| A dispatcher loses the database, but workers still reach it | It cannot renew, so it stops at its deadline; others take over. | None |
| A worker dies mid-event | The session closes and the dispatcher offers the head again. | None; the event may run twice |
| A commit's reply is lost | The worker retries through any instance and gets `Duplicate`. | None |
| A publish's reply is lost | The producer retries; the unique key absorbs it. | None |
| The database primary dies | Patroni promotes a synchronous replica. Dispatchers reconnect through the multi-host connection string and rebuild group state from the database. | None (G1). Seconds of pause. |
| A DC is lost | As above, plus that DC's dispatchers and workers disappear. The rest take over their leases and their work. | None, as long as one synchronous replica survives |
| A network split between DCs | Only the side with the etcd majority can have a primary. Dispatchers on the other side cannot commit or renew, so they stop. | None |

### 6.5 An event that always fails

Strict order means an event that never succeeds holds back **its own source only**; other sources are not
affected.

Policy (§10): **retry for ever and alert.**
- The retry delay grows from 250 ms to a cap of 60 s.
- Every attempt is counted in `queue.attempts`.
- An alert fires when a head has failed N times or is older than T.
- An operator tool can skip the event: it moves the event to `dead_letter` with who and why, and moves the
  cursor on. Nothing is ever skipped automatically.

A **missing seq** (a gap: seq+2 is stored but seq+1 never arrived) also holds back only its source. It raises
the same alert, and is resolved with the same tool.

## 7. Operations

- **Kubernetes.**
  - Dispatchers run as a Deployment, plus a headless Service for discovery.
  - A PodDisruptionBudget, and readiness that turns ready only after the first lease tick.
  - SIGTERM triggers the planned hand-over (§5.3).
- **Scaling.** Add pods; rendezvous hashing rebalances. Remove pods with SIGTERM.
- **Upgrades:** rolling, with backward-compatible Protobuf fields. A protocol version is exchanged on connect.
- **Observability:**
  - per instance: owned groups, ready/handed counts, batch sizes, commit latency, lease events;
  - per group: backlog and the age of the oldest head;
  - alerts for a stuck source, a group without an owner, and replication lag.
- **Security:** mTLS between all pods, and per-client authentication for producers and workers.

## 8. Engines

The dispatcher talks to the database through one small backend interface:
- publish batch;
- read heads for groups;
- batched commit;
- delete / fail;
- leases and members.

Postgres comes first. SQL Server (availability group with synchronous commit) and MySQL (semi-sync
`AFTER_SYNC`, or Group Replication) follow, each with its own failover drill.

## 9. Verification before anyone relies on it

1. **Oracle invariants G1–G6 on every run.** G1 is checked by having the producers record every
   acknowledgement; every acknowledged event must end up applied.
2. **Drills**, each repeated and run under the 24k load:
   - a dispatcher killed / stopped cleanly / paused longer than the TTL;
   - scale out and in during load;
   - partitions between dispatcher and database, dispatcher and workers, and between DCs, using network
     rules or `tc`;
   - the database primary killed;
   - a whole "DC" (database, dispatchers, workers) killed;
   - injected clock skew.
3. **Randomised chaos for hours**: faults at random times, with the oracle at the end (Jepsen style).
4. **A small TLA+ model** of lease, hand-over, commit and publish, checked for G2–G5 before the protocol is
   frozen. This is cheap compared with finding a protocol bug in production.

## 10. Decisions (owner, 2026-11-20)

1. **Ordering:** each source stamps a gapless per-source sequence number. Producers can publish concurrently and
   retry; a gap holds back its source and raises an alert.
2. **Sites:** two DCs plus a witness site (etcd only). §4.1 has the replica placement.
3. **Side effects:** workers also call external systems. `(source_id, seq)` is passed with every delivery as the
   idempotency key.
4. **Event that always fails:** retry for ever with capped backoff and alert; an operator tool can skip it.

## 10b. Product boundary (owner, 2026-11-20)

The dispatcher is a **standalone product in its own repository**:
- the server;
- the Protobuf protocol specification;
- client SDKs for producers and workers;
- backends (Postgres first);
- Kubernetes manifests and Helm;
- its own tests, the TLA+ model and the fault-injection suite.

The EventBench benchmark harness tests Octopus through an adapter, like any other broker, with its end-state oracle.

## 11. Build plan (each step ends with its tests passing)

1. **Protocol model.** TLA+ model of publish, leases, hand-over and commit. Model-check G2–G5, including a
   paused instance and lost messages.
2. **Tooling.** An MQTT 5 broker container (Mosquitto; EMQX for a clustered fabric) and the Solace PubSub+
   Standard container. The .NET MQTT client is MQTTnet. Dispatchers run as containers, like pods.
3. **Core.** The backend interface and its Postgres implementation:
   - seq-based heads;
   - guarded idempotent publish;
   - batched commit;
   - attempts and backoff;
   - leases and members.
4. **Dispatcher service** (pub/sub transport, MQTT 5 first): publish, work with credits, commit/fail, hints
   between dispatchers, local lease deadline, planned hand-over on SIGTERM, health, metrics.
5. **Client libraries** for producers and workers, also used by the EventBench adapter. Producers record
   every acknowledgement so the oracle can check G1.
6. **Single-DC drills at 24k:** kill, SIGTERM, pause, scale out and in, worker kill, dispatcher↔database and
   dispatcher↔worker partitions, database primary kill.
7. **Two-DC lab:** network namespaces with `tc` delay for DC1, DC2 and the witness. Patroni + etcd, the §4.1
   placement, DC-loss and DC-partition drills.
8. **Hours-long randomised chaos runs.**
9. **SQL Server and MySQL backends**, each with its failover drill.
