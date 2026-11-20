------------------------------- MODULE Octopus -------------------------------
(***************************************************************************)
(* The core protocol of Octopus: publish, hand-out, commit, under failures. *)
(*                                                                         *)
(* Safety must not depend on which dispatcher believes it owns a group.    *)
(* So this model lets EVERY dispatcher hand out ANY stored event at any    *)
(* time, with a state snapshot that may be stale. That is the worst case of *)
(* split-brain, zombie or paused owners. Leases matter only for liveness   *)
(* and efficiency, not for safety.                                         *)
(*                                                                         *)
(* Failures modelled:                                                       *)
(*   - the fabric loses, duplicates (through retries) and reorders messages; *)
(*   - dispatchers crash and lose their memory;                            *)
(*   - workers crash, or fail an event;                                    *)
(*   - deliveries time out, and the dispatcher re-offers them;             *)
(*   - the database primary fails over to its standby. SyncReplica = TRUE  *)
(*     is the design (synchronous replication); FALSE shows what async     *)
(*     replication breaks.                                                 *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS Sources, MaxSeq, Dispatchers, Workers, SyncReplica

Seqs   == 1..MaxSeq
Events == Sources \X Seqs
Src(e) == e[1]
Seq(e) == e[2]

Msg ==      [type : {"pub", "puback"}, e : Events]
       \cup [type : {"deliver"}, e : Events, w : Workers, ctx : 0..MaxSeq]
       \cup [type : {"commit", "skip", "fail"}, e : Events, to : Dispatchers]

DbState == [queue : SUBSET Events, cursor : [Sources -> 0..MaxSeq]]

VARIABLES
    db,        \* the primary: stored events (queue) and each source's cursor (last applied seq)
    replica,   \* the standby's copy of db
    applied,   \* ghost: how many times each event's state change took effect
    acked,     \* ghost: events whose publish was acknowledged to the producer
    inflight,  \* dispatcher memory: events handed out and not yet released
    busy,      \* worker memory: events being processed, with the state snapshot they came with
    net        \* messages in transit

vars == <<db, replica, applied, acked, inflight, busy, net>>

TypeOK ==
    /\ db \in DbState
    /\ replica \in DbState
    /\ applied \in [Events -> Nat]
    /\ acked \subseteq Events
    /\ inflight \in [Dispatchers -> SUBSET Events]
    /\ busy \in [Workers -> SUBSET [e : Events, ctx : 0..MaxSeq]]
    /\ net \subseteq Msg

Init ==
    /\ db = [queue |-> {}, cursor |-> [s \in Sources |-> 0]]
    /\ replica = db
    /\ applied = [e \in Events |-> 0]
    /\ acked = {}
    /\ inflight = [d \in Dispatchers |-> {}]
    /\ busy = [w \in Workers |-> {}]
    /\ net = {}

(* A database transaction. With a synchronous standby, the commit returns only once the standby has it. *)
Write(new) ==
    /\ db' = new
    /\ replica' = IF SyncReplica THEN new ELSE replica

---------------------------------------------------------------------------
(* Producers: sources stamp the seq; any event can be (re)published at any time, in any order. *)
Publish(e) ==
    /\ e \notin acked
    /\ net' = net \cup {[type |-> "pub", e |-> e]}
    /\ UNCHANGED <<db, replica, applied, acked, inflight, busy>>

(* Any dispatcher accepts a publish: guarded, idempotent insert; the reply is sent after the commit. *)
Accept(m) ==
    /\ m \in net /\ m.type = "pub"
    /\ LET e == m.e IN
         Write([db EXCEPT !.queue = IF Seq(e) > db.cursor[Src(e)] THEN @ \cup {e} ELSE @])
    /\ net' = (net \ {m}) \cup {[type |-> "puback", e |-> m.e]}
    /\ UNCHANGED <<applied, acked, inflight, busy>>

PubAck(m) ==
    /\ m \in net /\ m.type = "puback"
    /\ acked' = acked \cup {m.e}
    /\ net' = net \ {m}
    /\ UNCHANGED <<db, replica, applied, inflight, busy>>

---------------------------------------------------------------------------
(* Hand-out. Adversarial: any dispatcher, any stored event, a snapshot c no newer than the database.
   The real dispatcher hands out only heads (seq = cursor + 1) with a fresh snapshot; fairness below
   requires only that. *)
Deliver(d, e, w, c) ==
    /\ e \in db.queue
    /\ e \notin inflight[d]
    /\ c \in 0..db.cursor[Src(e)]
    /\ inflight' = [inflight EXCEPT ![d] = @ \cup {e}]
    /\ net' = net \cup {[type |-> "deliver", e |-> e, w |-> w, ctx |-> c]}
    /\ UNCHANGED <<db, replica, applied, acked, busy>>

Receive(w, m) ==
    /\ m \in net /\ m.type = "deliver" /\ m.w = w
    /\ busy' = [busy EXCEPT ![w] = @ \cup {[e |-> m.e, ctx |-> m.ctx]}]
    /\ net' = net \ {m}
    /\ UNCHANGED <<db, replica, applied, acked, inflight>>

(* The worker checks its snapshot, runs the event (side effects, at least once) and reports to any dispatcher.
   ok = FALSE: the processing failed. *)
Finish(w, x, to, ok) ==
    /\ x \in busy[w]
    /\ LET kind == IF ~ok THEN "fail"
                   ELSE IF x.ctx >= Seq(x.e) THEN "skip"
                   ELSE IF x.ctx = Seq(x.e) - 1 THEN "commit"
                   ELSE "fail"
       IN net' = net \cup {[type |-> kind, e |-> x.e, to |-> to]}
    /\ busy' = [busy EXCEPT ![w] = @ \ {x}]
    /\ UNCHANGED <<db, replica, applied, acked, inflight>>

---------------------------------------------------------------------------
(* The commit transaction: cursor check, state change, cursor advance and queue delete, atomically. *)
CommitTx(d, m) ==
    /\ m \in net /\ m.type = "commit" /\ m.to = d
    /\ LET e == m.e
           s == Src(e)
           q == Seq(e)
       IN  IF db.cursor[s] = q - 1
           THEN /\ Write([queue |-> db.queue \ {e}, cursor |-> [db.cursor EXCEPT ![s] = q]])
                /\ applied' = [applied EXCEPT ![e] = @ + 1]
           ELSE IF db.cursor[s] >= q
           THEN /\ Write([db EXCEPT !.queue = @ \ {e}])    \* already applied: drop the leftover row
                /\ UNCHANGED applied
           ELSE UNCHANGED <<db, replica, applied>>         \* gap: an earlier event is missing
    /\ inflight' = [inflight EXCEPT ![d] = @ \ {m.e}]
    /\ net' = net \ {m}
    /\ UNCHANGED <<acked, busy>>

(* Already applied according to the worker's snapshot: delete the row, guarded by the cursor. *)
SkipTx(d, m) ==
    /\ m \in net /\ m.type = "skip" /\ m.to = d
    /\ IF Seq(m.e) <= db.cursor[Src(m.e)]
       THEN Write([db EXCEPT !.queue = @ \ {m.e}])
       ELSE UNCHANGED <<db, replica>>
    /\ inflight' = [inflight EXCEPT ![d] = @ \ {m.e}]
    /\ net' = net \ {m}
    /\ UNCHANGED <<applied, acked, busy>>

FailMsg(d, m) ==
    /\ m \in net /\ m.type = "fail" /\ m.to = d
    /\ inflight' = [inflight EXCEPT ![d] = @ \ {m.e}]
    /\ net' = net \ {m}
    /\ UNCHANGED <<db, replica, applied, acked, busy>>

(* No commit, skip or fail arrived in time (lost message, dead worker): offer the event again. *)
Timeout(d, e) ==
    /\ e \in inflight[d]
    /\ inflight' = [inflight EXCEPT ![d] = @ \ {e}]
    /\ UNCHANGED <<db, replica, applied, acked, busy, net>>

---------------------------------------------------------------------------
CrashDispatcher(d) ==
    /\ inflight[d] # {}
    /\ inflight' = [inflight EXCEPT ![d] = {}]
    /\ UNCHANGED <<db, replica, applied, acked, busy, net>>

CrashWorker(w) ==
    /\ busy[w] # {}
    /\ busy' = [busy EXCEPT ![w] = {}]
    /\ UNCHANGED <<db, replica, applied, acked, inflight, net>>

Drop(m) ==
    /\ m \in net
    /\ net' = net \ {m}
    /\ UNCHANGED <<db, replica, applied, acked, inflight, busy>>

Replicate ==                      \* async replication catching up
    /\ ~SyncReplica
    /\ replica # db
    /\ replica' = db
    /\ UNCHANGED <<db, applied, acked, inflight, busy, net>>

Failover ==                       \* the standby is promoted
    /\ db # replica
    /\ db' = replica
    /\ UNCHANGED <<replica, applied, acked, inflight, busy, net>>

---------------------------------------------------------------------------
Next ==
    \/ \E e \in Events : Publish(e)
    \/ \E m \in net : Accept(m) \/ PubAck(m) \/ Drop(m)
    \/ \E d \in Dispatchers, e \in Events, w \in Workers, c \in 0..MaxSeq : Deliver(d, e, w, c)
    \/ \E w \in Workers, m \in net : Receive(w, m)
    \/ \E w \in Workers : \E x \in busy[w], to \in Dispatchers, ok \in BOOLEAN : Finish(w, x, to, ok)
    \/ \E d \in Dispatchers, m \in net : CommitTx(d, m) \/ SkipTx(d, m) \/ FailMsg(d, m)
    \/ \E d \in Dispatchers, e \in Events : Timeout(d, e)
    \/ \E d \in Dispatchers : CrashDispatcher(d)
    \/ \E w \in Workers : CrashWorker(w)
    \/ Replicate \/ Failover

(* Liveness assumptions, per event. Producers keep retrying unacknowledged events. Each kind of message
   about an event that keeps being sent is eventually received (the fabric loses messages, but not all of
   them for ever). Dispatchers keep handing out heads with fresh snapshots, workers eventually succeed, and
   timeouts fire. Crashes, losses and failovers have no fairness: they may happen at any time. *)
Fairness ==
    \A e \in Events :
        /\ WF_vars(Publish(e))
        /\ SF_vars(\E m \in net : m.e = e /\ Accept(m))
        /\ SF_vars(\E m \in net : m.e = e /\ PubAck(m))
        \* the fresh hand-out of the head (stale copies exist only to stress safety) eventually arrives
        /\ SF_vars(\E m \in net, w \in Workers : m.type = "deliver" /\ m.e = e /\ m.ctx = Seq(e) - 1 /\ Receive(w, m))
        /\ SF_vars(\E m \in net, d \in Dispatchers : m.e = e /\ CommitTx(d, m))
        /\ SF_vars(\E m \in net, d \in Dispatchers : m.e = e /\ SkipTx(d, m))
        /\ SF_vars(\E m \in net, d \in Dispatchers : m.e = e /\ FailMsg(d, m))
        /\ SF_vars(\E d \in Dispatchers, w \in Workers :
                        /\ Seq(e) = db.cursor[Src(e)] + 1
                        /\ Deliver(d, e, w, db.cursor[Src(e)]))
        \* a worker holding the head with a fresh snapshot eventually succeeds (no poison events)
        /\ SF_vars(\E w \in Workers, to \in Dispatchers : Finish(w, [e |-> e, ctx |-> Seq(e) - 1], to, TRUE))
        /\ WF_vars(\E d \in Dispatchers : Timeout(d, e))

Spec == Init /\ [][Next]_vars /\ Fairness

---------------------------------------------------------------------------
(* G2: each event's state change takes effect at most once. *)
ExactlyOnce == \A e \in Events : applied[e] <= 1

(* G2 + G3: the applied events of a source are exactly 1..cursor, so they were applied in order, none skipped. *)
AppliedIsPrefix == \A e \in Events : (applied[e] >= 1) <=> (Seq(e) <= db.cursor[Src(e)])

(* G1: an acknowledged event is stored or already applied: never lost. *)
NoLoss == \A e \in acked : e \in db.queue \/ Seq(e) <= db.cursor[Src(e)]

(* G4: no row is left behind for an event that is already applied (duplicate publishes are absorbed). *)
NoStaleRows == \A e \in db.queue : Seq(e) > db.cursor[Src(e)]

Safety == ExactlyOnce /\ AppliedIsPrefix /\ NoLoss /\ NoStaleRows

(* G5: once every seq up to q of a source has been acknowledged, the source eventually reaches q. *)
AllAcked(s, q) == \A r \in 1..q : <<s, r>> \in acked

Progress == \A s \in Sources : \A q \in Seqs : AllAcked(s, q) ~> (db.cursor[s] >= q)
=============================================================================
