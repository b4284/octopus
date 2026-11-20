# Formal model (TLA+)

`Octopus.tla` models the core protocol: publish, hand-out, commit, and the failures listed below. It is checked
with TLC (`./check.sh <config>`), which runs in a container, so no local Java is needed.

## What the model assumes and checks

**Safety must not depend on ownership.** Any dispatcher may hand out any stored event at any time, with a
state snapshot that may be stale. That is the worst case of split-brain, paused or zombie dispatchers. Leases
matter only for liveness and efficiency.

**Failures:**
- The fabric loses messages and reorders them, and retries duplicate them.
- Dispatchers and workers crash and lose their memory.
- Workers fail events.
- Deliveries time out.
- The database fails over to its standby: synchronous (the design) or asynchronous (to show the difference).

| property | meaning (design doc §2) |
|---|---|
| `ExactlyOnce` | each event's state change takes effect at most once (G2) |
| `AppliedIsPrefix` | the applied events of a source are exactly 1..cursor: in order, none skipped (G2, G3) |
| `NoLoss` | an acknowledged event is stored or already applied (G1) |
| `NoStaleRows` | duplicate publishes leave no rows behind (G4) |
| `Progress` | once every seq up to q is acknowledged, the source reaches q (G5) |

## Configurations

| config | instance | expected |
|---|---|---|
| `Safety.cfg` | 1 source × 3 seqs, 2 dispatchers, 2 workers, synchronous standby | no violation |
| `Safety2.cfg` | 2 sources × 2 seqs, 2 dispatchers, 1 worker, synchronous standby | no violation. **Checked 2026-11-20 with `MaxNet = 2`: 71.4M distinct states, depth 51, 15 min on 8 cores, no error.** |
| `SafetyAsync.cfg` | 1 source × 2 seqs, async standby | **violation**: an acknowledged publish lost after failover (found in 6 steps) |
| `Liveness.cfg` | 1 source × 2 seqs, 2 dispatchers, synchronous standby | `Progress` holds under the fairness assumptions |

`MC.tla` adds **symmetry** (dispatchers, workers and sources are interchangeable) and a **bound on messages in
transit at once** (`MaxNet`). The checks are therefore exhaustive up to that bound: bounded model checking. Without
the bound, the 2-source instance passed 170M distinct states without finishing.

## Design requirements the model makes explicit

1. **Delivery timeout.** A handed-out event with no commit, skip or fail in time is offered again (`Timeout`).
   Without it, a lost message on a non-persistent fabric strands its source.
2. **The skip delete is guarded by the cursor** (`DELETE … WHERE seq <= cursor`). A snapshot that is newer than
   the database can only happen after a lossy (async) failover; the guard keeps it harmless anyway.
3. **Insert guarded by the cursor** (`seq > cursor`) plus a unique key: a producer retry after the event was
   applied leaves nothing behind.

## Not modelled yet

- Lease timing and planned hand-over (they affect liveness and duplicate rates, not safety).
- Credits and no-cross-source-blocking: a performance property, measured by the benchmark instead.
