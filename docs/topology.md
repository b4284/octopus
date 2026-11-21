# Production topology

The deployment Octopus has been drilled against: active-active dispatchers on an MQTT fabric, and PostgreSQL
across two data centers with Patroni, etcd and a leader-following load balancer.

```
                    PRODUCERS (event sources)                      stamp (epoch, seq) per source,
                    publish one event at a time                    retry until acknowledged,
                                  │                                read "missing below" in each ack
                                  │ MQTT (request/reply)
                                  ▼
             ┌──────────────────────────────────────────┐
             │ MQTT broker                              │
             └──────┬──────────────┬──────────────┬─────┘
                    │              │              │
             ┌──────▼─────┐ ┌──────▼─────┐ ┌──────▼─────┐  OCTOPUS DISPATCHERS, active-active:
             │ octopusd 1 │ │ octopusd 2 │ │ octopusd 3 │  each group leased to one at a time;
             └──────┬─────┘ └──────┬─────┘ └──────┬─────┘  a crash, a stop or one more: nothing lost
                    │  stores events, leases, cursors     ▲ hand-outs over MQTT, "done" reports back
                    │                                     │
                    │                         ┌───────────┴───────────┐
                    │                         │ WORKERS (application) │  commit their own writes,
                    │                         └───────────┬───────────┘  then report done;
                    │                                     │              never cancel a commit
                    ▼              SQL, one fixed address ▼
             ┌────────────────────────────────────────────────────┐
             │ load balancer: HAProxy or a network LB (TCP 5432)  │  health check GET /primary :8008;
             │ routes only to the node answering 200              │  cuts sessions to a node that fails
             └────────────────────────┬───────────────────────────┘
                                      │
   ═════════════ DC1 ═══════════════════════╪═════════════════ DC2 ═══════════════════════
                                      │
   ┌────────────────┐  ┌────────────────┐   │   ┌────────────────┐  ┌────────────────┐
   │ pg-1a LEADER   │  │ pg-1b standby  │   │   │ pg-2a standby  │  │ pg-2b standby  │
   │ Patroni :8008  │─▶│ Patroni :8008  │   │   │ Patroni :8008  │  │ Patroni :8008  │
   └───────┬────────┘  └────────────────┘   │   └────────────────┘  └────────────────┘
           └── streaming replication ───────┴──────────▶ (to all three standbys)
               synchronous quorum: a commit waits for ANY 2 of the 3 standbys (remote_write)

   ┌────────────────┐                                 ┌────────────────┐
   │ etcd member    │◀──────── Raft (majority) ──────▶│ etcd member    │
   └────────────────┘                ▲                └────────────────┘
                                     │
                       ═══ WITNESS SITE (3rd location) ═══
                             ┌────────────────┐
                             │ etcd member    │  tie-breaker only, no data
                             └────────────────┘
   Patroni ↔ etcd: leader lock (ttl 20 s, renewed every 5 s), synchronous set, membership
```

## Who decides what

| component | decides | knows |
|---|---|---|
| etcd | nothing by itself, but what it accepts is the single truth | the leader lock, the synchronous set, the members |
| Patroni (one per node) | whether *its* node is primary, standby or demoted; when to promote | its node's replication state, and etcd |
| load balancer | where new connections go | only the nodes' answers to `GET /primary` |
| PostgreSQL | nothing about failover | the data; a commit waits for 2 of 3 standbys |
| Octopus dispatchers | which dispatcher serves which group (leases in the database) | events, cursors, workers' credit |

- **etcd** is a small key-value store replicated by Raft: a write succeeds only with a majority (2 of 3). Patroni
  keeps the **leader lock** there. Only the node holding it may run as primary; the primary renews it every 5 s,
  and it expires after 20 s. When it expires, the standbys race to take it with an atomic compare-and-set; only
  one that is healthy, in the synchronous set and caught up may try, and exactly one wins.
- **Fencing.** A primary cut off from etcd can't renew the lock, so its own Patroni demotes it before the lock
  expires. Two primaries never accept writes.
- **Three etcd members, one at a witness site.** Any two form a majority. Lose a DC, and the other DC plus the
  witness still decide. A DC cut off from both is a minority and demotes itself. The witness holds no data.
- **The load balancer doesn't decide anything:** it asks every node's Patroni once a second whether it is the
  primary and sends connections to the one that says yes. Clients always use one address. A transaction in flight
  on a failed primary still fails with its connection, so clients retry; Octopus's dispatchers, producers and
  workers all do.

## What each failure costs

| failure | what handles it | drilled result |
|---|---|---|
| a dispatcher crashes, stops, or one is added | its leases expire (or are released); others take its groups | nothing lost |
| half of every group's workers crash at once | a worker silent for 10 s is forgotten; its events go out again | nothing lost, p99 latency 10.6 s |
| the database primary dies | Patroni promotes a caught-up standby; the load balancer follows | outage ~20–26 s (detection ~17–21 s + 2–6 s), nothing lost |
| a whole DC dies | the other DC plus the witness keep etcd's majority | leader's DC killed: 10 out of 10 runs pass |
| a DC is cut off from the network (keeps running) | the cut-off primary demotes itself; the other DC takes over | demoted in 12 s, new leader in 33 s, nothing lost |
| the database is frozen for minutes | everyone retries; nobody cancels a commit | a 10-minute freeze passes |
| a dispatcher loses its database while alive | its leases expire under it; it stops handing out | passes |
| the MQTT broker restarts | retained announcements are re-sent every 5 s | passes |
| a source loses an event | "missing below" in acks, high-water announcements, skip markers, gap timeout | built and tested |

## Requirements this topology depends on

- **etcd 3.5.13 or newer.** 3.5.9 has a split-brain bug: a frozen member that resumes applies stale lease revocations.
- **Patroni:** quorum synchronous mode (`synchronous_mode: quorum`, `synchronous_node_count: 2`, strict), a
  watchdog where available, `basebackup: [checkpoint: fast]` (otherwise a rebuilding replica waits up to
  checkpoint_timeout for a spread checkpoint and stays out of the quorum), and
  `remove_data_directory_on_rewind_failure` / `..._on_diverged_timelines` so a former primary rejoins by itself.
- **Checkpoints spaced by time, not by WAL volume** (a large `max_wal_size`): forced checkpoints every few seconds
  multiply the WAL, and so the replication traffic between the DCs, with full-page images.
- **Replay lag alerting.** `remote_write` makes a commit wait until the standbys have *received* it, not *applied*
  it. A standby whose disk can't keep up falls behind in replay, and a failover to it waits for the backlog.
- **Clients never cancel a commit** (no statement timeout that cancels it). A cancelled commit waiting for the
  synchronous standbys is reported done without replication. Abandon the call instead of cancelling it.
- **A leader-following address.** A connection string that lists several hosts stalled after a DC kill:
  connections to a vanished host wait on TCP timeouts. Use a load balancer that checks `/primary`.

## The load balancer: HAProxy or a network load balancer

Any layer-4 load balancer works if:

1. **Health check:** HTTP `GET /primary` on Patroni's REST port (8008); only the leader answers 200. Traffic to
   TCP 5432 on every database node.
2. **Established sessions are cut when a target turns unhealthy.** This one matters most. HAProxy does it with
   `on-marked-down shutdown-sessions`. Many network load balancers keep the flow to an unhealthy target until an
   idle timeout (minutes) unless configured otherwise (AWS NLB: terminate connections on unhealthy targets).
   Without it, clients depend on their own TCP keepalives and operation timeouts. Measured (benchmark matrix-35,
   the leader's DC frozen for 60 s, 5 s checks): **83–84 s outage without cutting** (1,500–1,750 worker calls hung
   on the frozen leader until their 60 s timeout) **against 27–30 s with it**. A killed host's sessions get reset
   at once and don't show the difference; a frozen or partitioned one does.
3. **Fast detection.** Every second of check interval × failure threshold, and of interval × rise threshold before
   the new leader takes traffic, adds to the outage. Leader-DC kill drills: 18–22 s with 1 s checks, 24–30 s with
   5 s checks (matrix-19, matrix-35). Cloud load balancers often check every 5 s at best.
4. **Targets in both DCs**, or one load balancer per DC whose targets include the other DC's nodes, or DNS/global
   load balancing in front.
5. **Redundancy.** A managed load balancer is redundant by itself. HAProxy needs it arranged: one per application
   host, or a pair with a floating IP.

Alternatives without a load balancer: Patroni's `vip-manager` (a floating IP that follows the leader), or on
Kubernetes a Service whose endpoints Patroni maintains.
