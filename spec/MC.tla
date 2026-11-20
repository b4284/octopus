---------------------------- MODULE MC ----------------------------
(* Model-checking helpers: symmetry and a bound on messages in flight. *)
EXTENDS Octopus, TLC

CONSTANT MaxNet

Symmetry == Permutations(Dispatchers) \cup Permutations(Workers) \cup Permutations(Sources)

(* Bounded model checking: every interleaving with at most MaxNet messages in transit at once. *)
NetBound == Cardinality(net) <= MaxNet

=====================================================================
