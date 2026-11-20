---------------------------- MODULE WorkerCommit ----------------------------
(***************************************************************************)
(* Octopus in its main mode: workers commit their own writes and report    *)
(* "done"; Octopus only removes the event. With positions (epoch, seq),    *)
(* cursors written lazily from dispatcher memory, skip markers from the    *)
(* source, gaps given up after a timeout, and epochs that end by           *)
(* declaration or are closed.                                              *)
(*                                                                         *)
(* As in Octopus.tla, safety must not depend on ownership: ANY dispatcher  *)
(* acts on ANY source at ANY time, with whatever (possibly stale) memory   *)
(* it has; it may crash and forget. The fabric loses messages. The         *)
(* database is a single primary with a synchronous standby (Octopus.tla    *)
(* covers failover), so a commit is never lost.                            *)
(*                                                                         *)
(* The worker is the weakest allowed application: it only checks its      *)
(* idempotency key (source, epoch, seq) in its own transaction. Ordering   *)
(* must come from Octopus.                                                 *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, Sequences

CONSTANTS Sources, MaxSeq, Dispatchers, Workers

Epochs    == 1..2
Positions == Epochs \X (1..MaxSeq)
Cursors   == Epochs \X (0..MaxSeq)
Events    == Sources \X Positions          \* <<source, <<epoch, seq>>>>
Src(x) == x[1]
Pos(x) == x[2]

LT(a, b)  == a[1] < b[1] \/ (a[1] = b[1] /\ a[2] < b[2])
LE(a, b)  == LT(a, b) \/ a = b
Max(a, b) == IF LT(a, b) THEN b ELSE a
Succ(c)   == <<c[1], c[2] + 1>>
Start     == <<1, 0>>

(* The first events of epoch 2 may declare that epoch 1 ended at MaxSeq. *)
EndOfEpoch1 == <<1, MaxSeq>>

Msg ==      [type : {"pub"}, x : Events, marker : BOOLEAN, decl : BOOLEAN]
       \cup [type : {"puback"}, x : Events, marker : BOOLEAN, status : {"stored", "dup", "late"}]
       \cup [type : {"deliver"}, x : Events, w : Workers]
       \cup [type : {"done", "fail"}, x : Events, to : Dispatchers]

VARIABLES
    db,          \* queue (stored events), markers (stored skip markers), cursor, declared (sources whose epoch 1 end
                 \* is declared), skipped (positions given up)
    lost,        \* source side: events the source lost before publishing them
    everStored,  \* ghost: events ever stored (a dispatcher may hold one's body in memory long after)
    acked,       \* ghost: events acknowledged as stored (or already stored) to the producer
    app,         \* the application's data, written by workers: per source, the positions applied, in order
    mem,         \* dispatcher memory: per source, the last position it believes done
    inflight,    \* dispatcher memory: events handed out
    busy,        \* worker memory
    net

vars == <<db, lost, everStored, acked, app, mem, inflight, busy, net>>

AppPos(s) == {app[s][i] : i \in 1..Len(app[s])}

TypeOK ==
    /\ db \in [queue : SUBSET Events, markers : SUBSET Events, cursor : [Sources -> Cursors],
               declared : SUBSET Sources, skipped : SUBSET Events]
    /\ lost \subseteq Events /\ everStored \subseteq Events /\ acked \subseteq Events
    /\ app \in [Sources -> Seq(Positions)]
    /\ mem \in [Dispatchers -> [Sources -> Cursors]]
    /\ inflight \in [Dispatchers -> SUBSET Events]
    /\ busy \in [Workers -> SUBSET Events]
    /\ net \subseteq Msg

Init ==
    /\ db = [queue |-> {}, markers |-> {}, cursor |-> [s \in Sources |-> Start], declared |-> {}, skipped |-> {}]
    /\ lost = {} /\ everStored = {} /\ acked = {}
    /\ app = [s \in Sources |-> <<>>]
    /\ mem = [d \in Dispatchers |-> [s \in Sources |-> Start]]
    /\ inflight = [d \in Dispatchers |-> {}]
    /\ busy = [w \in Workers |-> {}]
    /\ net = {}

(* The database after a source's cursor moved to c: an epoch at its declared end moves on to the next epoch;
   stored events and markers at or below the cursor are deleted (markers are recorded as skipped). *)
Settle(d0, s, c) ==
    LET c1   == IF c = EndOfEpoch1 /\ s \in d0.declared THEN <<2, 0>> ELSE c
        gone == {x \in d0.markers : Src(x) = s /\ LE(Pos(x), c1)}
    IN  [d0 EXCEPT !.cursor[s] = c1,
                   !.queue = {x \in @ : Src(x) # s \/ LT(c1, Pos(x))},
                   !.markers = @ \ gone,
                   !.skipped = @ \cup gone]

---------------------------------------------------------------------------
(* The source: it publishes its events in any order and retries until acknowledged; it may lose an event before
   ever sending it, and then may publish a skip marker for it. Epoch-2 events may carry the declaration. *)
Lose(x) ==
    /\ x \notin lost
    /\ ~\E m \in net : m.type = "pub" /\ m.x = x /\ ~m.marker
    /\ x \notin everStored
    /\ lost' = lost \cup {x}
    /\ UNCHANGED <<db, everStored, acked, app, mem, inflight, busy, net>>

Publish(x, marker, decl) ==
    /\ x \notin acked
    /\ marker = (x \in lost)
    /\ decl => Pos(x)[1] = 2
    /\ net' = net \cup {[type |-> "pub", x |-> x, marker |-> marker, decl |-> decl]}
    /\ UNCHANGED <<db, lost, everStored, acked, app, mem, inflight, busy>>

(* Any dispatcher accepts: one transaction (record the declaration, settle, store if beyond the cursor). *)
Accept(m) ==
    /\ m \in net /\ m.type = "pub"
    /\ LET s   == Src(m.x)
           d0  == IF m.decl THEN [db EXCEPT !.declared = @ \cup {s}] ELSE db
           d1  == Settle(d0, s, d0.cursor[s])
           new == LT(d1.cursor[s], Pos(m.x))
           st  == IF new THEN "stored" ELSE IF m.x \in d1.skipped THEN "late" ELSE "dup"
       IN  /\ db' = IF ~new THEN d1
                    ELSE IF m.marker THEN [d1 EXCEPT !.markers = @ \cup {m.x}]
                    ELSE [d1 EXCEPT !.queue = @ \cup {m.x}]
           /\ everStored' = IF new /\ ~m.marker THEN everStored \cup {m.x} ELSE everStored
           /\ net' = (net \ {m}) \cup {[type |-> "puback", x |-> m.x, marker |-> m.marker, status |-> st]}
    /\ UNCHANGED <<lost, acked, app, mem, inflight, busy>>

PubAck(m) ==
    /\ m \in net /\ m.type = "puback"
    /\ acked' = IF m.status # "late" /\ ~m.marker THEN acked \cup {m.x} ELSE acked
    /\ net' = net \ {m}
    /\ UNCHANGED <<db, lost, everStored, app, mem, inflight, busy>>

---------------------------------------------------------------------------
(* Dispatcher knowledge: a fresh read of the database cursor. *)
Read(d, s) ==
    /\ LT(mem[d][s], db.cursor[s])
    /\ mem' = [mem EXCEPT ![d][s] = db.cursor[s]]
    /\ UNCHANGED <<db, lost, everStored, acked, app, inflight, busy, net>>

(* Hand out the position after the one believed done. The body may come from memory (any event ever stored). *)
Deliver(d, x, w) ==
    /\ x \in everStored
    /\ Pos(x) = Succ(mem[d][Src(x)])
    /\ x \notin inflight[d]
    /\ inflight' = [inflight EXCEPT ![d] = @ \cup {x}]
    /\ net' = net \cup {[type |-> "deliver", x |-> x, w |-> w]}
    /\ UNCHANGED <<db, lost, everStored, acked, app, mem, busy>>

(* A skip marker at the head is never handed out: the dispatcher's cursor jumps over it. *)
ConsumeMarker(d, x) ==
    /\ x \in db.markers
    /\ Pos(x) = Succ(mem[d][Src(x)])
    /\ mem' = [mem EXCEPT ![d][Src(x)] = Pos(x)]
    /\ UNCHANGED <<db, lost, everStored, acked, app, inflight, busy, net>>

Receive(w, m) ==
    /\ m \in net /\ m.type = "deliver" /\ m.w = w
    /\ busy' = [busy EXCEPT ![w] = @ \cup {m.x}]
    /\ net' = net \ {m}
    /\ UNCHANGED <<db, lost, everStored, acked, app, mem, inflight>>

(* The worker's own transaction: apply unless its idempotency key is already there; then report done. *)
WorkerTx(w, x, to) ==
    /\ x \in busy[w]
    /\ app' = IF Pos(x) \in AppPos(Src(x)) THEN app ELSE [app EXCEPT ![Src(x)] = Append(@, Pos(x))]
    /\ busy' = [busy EXCEPT ![w] = @ \ {x}]
    /\ net' = net \cup {[type |-> "done", x |-> x, to |-> to]}
    /\ UNCHANGED <<db, lost, everStored, acked, mem, inflight>>

WorkerFail(w, x, to) ==
    /\ x \in busy[w]
    /\ busy' = [busy EXCEPT ![w] = @ \ {x}]
    /\ net' = net \cup {[type |-> "fail", x |-> x, to |-> to]}
    /\ UNCHANGED <<db, lost, everStored, acked, app, mem, inflight>>

DoneRecv(d, m) ==
    /\ m \in net /\ m.type = "done" /\ m.to = d
    /\ mem' = [mem EXCEPT ![d][Src(m.x)] = Max(@, Pos(m.x))]
    /\ inflight' = [inflight EXCEPT ![d] = @ \ {m.x}]
    /\ net' = net \ {m}
    /\ UNCHANGED <<db, lost, everStored, acked, app, busy>>

FailRecv(d, m) ==
    /\ m \in net /\ m.type = "fail" /\ m.to = d
    /\ inflight' = [inflight EXCEPT ![d] = @ \ {m.x}]
    /\ net' = net \ {m}
    /\ UNCHANGED <<db, lost, everStored, acked, app, mem, busy>>

Timeout(d, x) ==
    /\ x \in inflight[d]
    /\ inflight' = [inflight EXCEPT ![d] = @ \ {x}]
    /\ UNCHANGED <<db, lost, everStored, acked, app, mem, busy, net>>

(* The lazy cursor write: monotonic, from whatever this dispatcher believes (possibly stale or a zombie's). *)
Flush(d, s) ==
    /\ LT(db.cursor[s], mem[d][s])
    /\ db' = Settle(db, s, mem[d][s])
    /\ UNCHANGED <<lost, everStored, acked, app, mem, inflight, busy, net>>

---------------------------------------------------------------------------
(* Giving up a gap (after the timeout; here at ANY time, by anyone): the database skips only positions of the
   cursor's epoch that are not stored now, from the cursor up to `to`. *)
SkipGap(s, to) ==
    LET c     == db.cursor[s]
        range == {<<s, <<c[1], q>>>> : q \in (c[2] + 1)..to}
    IN  /\ to > c[2] /\ to <= MaxSeq
        /\ range \cap (db.queue \cup db.markers) = {}
        /\ db' = Settle([db EXCEPT !.skipped = @ \cup range], s, <<c[1], to>>)
        /\ UNCHANGED <<lost, everStored, acked, app, mem, inflight, busy, net>>

(* Closing epoch 1 where the cursor stands, because epoch 2 has events: only if nothing of epoch 1 beyond the
   cursor is stored now. *)
CloseEpoch(s) ==
    LET c    == db.cursor[s]
        rest == {<<s, <<1, q>>>> : q \in (c[2] + 1)..MaxSeq}
    IN  /\ c[1] = 1
        /\ rest \cap (db.queue \cup db.markers) = {}
        /\ \E x \in db.queue \cup db.markers : Src(x) = s /\ Pos(x)[1] = 2
        /\ db' = [db EXCEPT !.skipped = @ \cup rest, !.cursor[s] = <<2, 0>>]
        /\ UNCHANGED <<lost, everStored, acked, app, mem, inflight, busy, net>>

---------------------------------------------------------------------------
CrashDispatcher(d) ==
    /\ mem[d] # [s \in Sources |-> Start] \/ inflight[d] # {}
    /\ mem' = [mem EXCEPT ![d] = [s \in Sources |-> Start]]
    /\ inflight' = [inflight EXCEPT ![d] = {}]
    /\ UNCHANGED <<db, lost, everStored, acked, app, busy, net>>

CrashWorker(w) ==
    /\ busy[w] # {}
    /\ busy' = [busy EXCEPT ![w] = {}]
    /\ UNCHANGED <<db, lost, everStored, acked, app, mem, inflight, net>>

Drop(m) ==
    /\ m \in net
    /\ net' = net \ {m}
    /\ UNCHANGED <<db, lost, everStored, acked, app, mem, inflight, busy>>

Next ==
    \/ \E x \in Events : Lose(x)
    \/ \E x \in Events, mk \in BOOLEAN, dc \in BOOLEAN : Publish(x, mk, dc)
    \/ \E m \in net : Accept(m) \/ PubAck(m) \/ Drop(m)
    \/ \E d \in Dispatchers, s \in Sources : Read(d, s) \/ Flush(d, s)
    \/ \E d \in Dispatchers, x \in Events, w \in Workers : Deliver(d, x, w)
    \/ \E d \in Dispatchers, x \in Events : ConsumeMarker(d, x) \/ Timeout(d, x)
    \/ \E w \in Workers, m \in net : Receive(w, m)
    \/ \E w \in Workers, x \in Events, to \in Dispatchers : WorkerTx(w, x, to) \/ WorkerFail(w, x, to)
    \/ \E d \in Dispatchers, m \in net : DoneRecv(d, m) \/ FailRecv(d, m)
    \/ \E s \in Sources, to \in 1..MaxSeq : SkipGap(s, to)
    \/ \E s \in Sources : CloseEpoch(s)
    \/ \E d \in Dispatchers : CrashDispatcher(d)
    \/ \E w \in Workers : CrashWorker(w)

Spec == Init /\ [][Next]_vars

---------------------------------------------------------------------------
(* Each source's events take effect in strictly increasing position order (so each at most once). *)
InOrder == \A s \in Sources : \A i \in 1..Len(app[s]) : \A j \in (i + 1)..Len(app[s]) : LT(app[s][i], app[s][j])

(* "In order, minus declared skips": every position below an applied one was applied, or recorded as skipped,
   or declared lost by the source with a marker still stored (it is recorded when the database cursor passes it;
   a dispatcher may jump over it in memory before that, and if it crashes, the next reader meets the marker again). *)
NoSilentHole ==
    \A s \in Sources : \A p \in Positions :
        (\E i \in 1..Len(app[s]) : LT(p, app[s][i]))
            => (p \in AppPos(s) \/ <<s, p>> \in db.skipped \/ <<s, p>> \in db.markers)

(* A skipped position is never applied afterwards... *)
SkippedNeverApplied == \A x \in db.skipped : Pos(x) \notin AppPos(Src(x))

(* ...and an event acknowledged as stored is never given up: it is applied or still stored. *)
AckedNotLost == \A x \in acked : x \notin db.skipped /\ (Pos(x) \in AppPos(Src(x)) \/ x \in db.queue)

Safety == InOrder /\ NoSilentHole /\ SkippedNeverApplied /\ AckedNotLost
=============================================================================
