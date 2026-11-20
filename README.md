# Octopus

**Ordered per-key event processing on a plain SQL database, with an active-active dispatcher cluster.**

> **Status: early implementation, not production-ready.**
> - The design is in [docs/design.md](docs/design.md), the measurements behind it in [docs/why.md](docs/why.md), and
>   the formal model in [spec/](spec/).
> - Built so far: the Rust dispatcher (`octopusd`), the PostgreSQL backend, the MQTT 5 transport, and a Rust
>   client for producers and workers.
> - End-to-end tests pass with three dispatchers, one crashed (`kill -9`) and one stopped mid-run.

Many systems need to process events **in order per source** (device, account, entity) while keeping thousands of
sources moving in parallel. **A slow or failing event must hold back only its own source.** The state each event
changes should live in an ordinary SQL database, where it is easy to query and operate.

Message brokers order by partition or queue, so one slow event blocks every source behind it. SQL-backed job
queues keep the data in SQL, but they spend the database on polling and per-message bookkeeping.

Octopus puts a small, stateless, leaderless dispatcher cluster in front of the database:

```
 producers ──publish──▶ ┌─────────────────────────────┐ ──deliver──▶ workers
   (any instance)       │ octopus dispatchers (N pods)│ ◀──commit───
                        │ no leader, any pod does any │
                        │ work; leases split groups   │
                        └──────────────┬──────────────┘
                                       │ few connections, large batched transactions
                        ┌──────────────▼──────────────┐
                        │ SQL primary  ──sync──▶ DR    │
                        └─────────────────────────────┘
```

## Guarantees

- **Zero loss.** A publish is acknowledged only after it is committed to the database and to a synchronous
  replica in another data center.
- **Strict per-source order.** Every event carries a position `(epoch, seq)` stamped by its source, and only a
  source's next position is ever handed out, after the one before it is done. A dispatcher's memory, a lease or a
  clock never decides correctness. A failed dispatcher can only cause a duplicate *delivery*, flagged as such.
- **Exactly-once effect.** Either workers commit their own writes and check the idempotency key
  `(source, epoch, seq)`, or Octopus applies the state change through an apply hook in the same transaction that
  moves the cursor.
- **Gaps heal, visibly.** A missing event holds back only its source. The publish reply tells the producer what
  is missing below each event; it replays it or declares it lost (a skip marker); otherwise the group's gap policy
  gives it up after a timeout. Every skip is recorded and told to the worker with the source's next event; a late
  arrival is kept apart, never processed out of order.
- **No cross-source blocking.** Work goes only into free worker slots. A failing event is retried for ever with
  backoff and an alert, and holds back only its own source.
- **Active-active and elastic.** Any dispatcher accepts any publish or commit. Group ownership is spread by
  rendezvous hashing over database leases, with no leader. Scaling means adding pods; a lost pod's groups move
  within one lease TTL.
- **At-least-once execution.** Workers get `(source, epoch, seq)` as an idempotency key for any external call.

## Transport

Pluggable pub/sub. **MQTT 5** comes first: Solace PubSub+ supports it natively, and Mosquitto or EMQX run it in a
container for tests. The fabric is never the durability point: every acknowledgement is an application reply
sent after the SQL commit.

## Databases

Postgres first, then SQL Server and MySQL behind a small backend interface.

## Try it

Needs Rust, `protoc`, PostgreSQL and podman (or docker).

```
dev/up.sh                                   # Mosquitto (MQTT 5) on 127.0.0.1:1883
export OCTOPUS_TEST_PG="host=127.0.0.1 port=5432 user=... password=... dbname=octopus_test"
cargo test                                  # backend tests + end-to-end tests with real octopusd processes
```

Run a dispatcher: `OCTOPUS_PG=... OCTOPUS_MEMBER_ID=d1 cargo run --release -p octopusd`. The settings are listed in
`crates/octopusd/src/main.rs`.

## Licence

GPL-3.0. See [LICENSE](LICENSE).
