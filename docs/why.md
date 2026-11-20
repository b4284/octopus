# Why build Octopus: the measurements

All numbers come from the EventBench harness. One host (24 cores), Postgres 18, and the same workload throughout:
- 500 groups, 16,000 sources, 24k events/s offered;
- 0.5% slow events (1–3 s), plus 1–4 MB events in a few groups.

An end-state oracle checks every source's cursor, every record, exactly-once and per-source order. It also
counts *cross-source blocking*: an event waiting more than 100 ms while a worker slot of its group sat idle.

## Existing options, each with one event per publish

| approach | best result | why it stops there |
|---|---|---|
| PGMQ (a table per queue), tuned | about 11k ev/s, Postgres pinned at 16–17 cores | Polling; a hide-until rewrite of every message on read; per-queue tables |
| MassTransit v8 SQL transport, `PartitionedOrdered` | about 4k ev/s, Postgres 15.7 cores | Each fetch runs a `row_number()` window over the whole queue; two tables, a lock update, a delete and a metrics insert per message. v9 is commercial. |
| Temporal, workflow per source | under 1k ev/s, 15 cores at 1.6k offered | About 7 history events per event: built for long workflows, not for event streams |
| Kafka cluster (3 brokers) + Postgres | 16.5–18.8k ev/s | Replication cost; a second system next to SQL; per-partition blocking without key-level workers |
| Redis Streams / Redis Sentinel | — | Loses or duplicates acknowledged work on failover |
| A single queue table in Postgres, read by an in-process dispatcher (prototype) | **24k ev/s on about 9 Postgres cores** with batched publishes | With concurrent single-event publishes, the per-insert `NOTIFY` serialises commits (collapse to 2.5k/s) |

## What the prototype showed

1. One Postgres primary carries **queue + state at 24k ev/s on about 9 cores** once a dispatcher:
   - reads only source heads, with no write on read;
   - batches many commits per transaction, with the queue delete inside the same transaction;
   - replaces polling with in-memory hand-out.
2. A **synchronous standby** costs nothing measurable at that rate (commits are batched), and a primary kill
   loses nothing. An **async** standby loses acknowledged work.
3. The remaining bottleneck was the **publish path**. Publishes therefore go through the dispatchers too: inserts
   are batched across producers, and no database notifications are needed.
