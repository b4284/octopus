---------------------------- MODULE MCWorkerCommit ----------------------------
(* Model-checking helpers for WorkerCommit: symmetry and a bound on messages in flight. *)
EXTENDS WorkerCommit, TLC

CONSTANT MaxNet

Symmetry == Permutations(Dispatchers) \cup Permutations(Workers) \cup Permutations(Sources)

NetBound == Cardinality(net) <= MaxNet
=============================================================================
