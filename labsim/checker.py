"""Safety invariant checker. Works purely on the trace, so it can be pointed at
real runs or hand-built traces.

Trace events it reads:
  became_leader  node, term, log=((term, cmd), ...)
  commit         node, index, term, cmd, commit_term (term of the leader that committed it)
  truncate       node, from_index, removed=((term, cmd), ...)
  final          node, commit_index, log   (one per node at end of run)

Invariants:
  election_safety  at most one leader per term
  committed_lost   a committed entry is never truncated, every leader of a later
                   term has it, and it is still on a majority at the end
  log_divergence   no two nodes commit different entries at the same index, and
                   committed prefixes agree at the end
"""

from dataclasses import dataclass
from itertools import combinations


@dataclass
class Violation:
    invariant: str
    time: int
    seq: int
    nodes: tuple
    detail: str


@dataclass
class _Commit:
    term: int
    cmd: object
    commit_term: int
    event: object


def _has(log, index, term, cmd):
    return len(log) >= index and tuple(log[index - 1]) == (term, cmd)


def check(trace):
    out = []
    reported = set()

    def flag(key, invariant, event, nodes, detail):
        if key in reported:
            return
        reported.add(key)
        out.append(Violation(invariant, event.time, event.seq, tuple(sorted(set(nodes))), detail))

    first_leader = {}   # term -> became_leader event
    leaders = []
    committed = {}      # index -> _Commit
    finals = []

    def check_completeness(leader, index, c, at):
        if leader.data["term"] > c.commit_term and not _has(leader.data["log"], index, c.term, c.cmd):
            flag(("complete", index, leader.data["term"]), "committed_lost", at,
                 [leader.node, c.event.node],
                 f"leader n{leader.node} of term {leader.data['term']} lacks entry {index} "
                 f"(t{c.term} {c.cmd!r}) committed in term {c.commit_term} by n{c.event.node}")

    for e in trace:
        d = e.data
        if e.kind == "became_leader":
            term = d["term"]
            first = first_leader.setdefault(term, e)
            if first.node != e.node:
                flag(("election", term), "election_safety", e, [first.node, e.node],
                     f"n{first.node} and n{e.node} both leader in term {term}")
            leaders.append(e)
            for index, c in committed.items():
                check_completeness(e, index, c, e)

        elif e.kind == "commit":
            index = d["index"]
            c = committed.get(index)
            if c is not None and (c.term, c.cmd) != (d["term"], d["cmd"]):
                flag(("diverge", index), "log_divergence", e, [c.event.node, e.node],
                     f"index {index}: n{c.event.node} committed t{c.term} {c.cmd!r}, "
                     f"n{e.node} committed t{d['term']} {d['cmd']!r}")
                continue
            if c is None:
                c = committed[index] = _Commit(d["term"], d["cmd"], d["commit_term"], e)
            elif d["commit_term"] < c.commit_term:
                c.commit_term = d["commit_term"]
            for leader in leaders:
                check_completeness(leader, index, c, e)

        elif e.kind == "truncate":
            for k, (term, cmd) in enumerate(d["removed"]):
                index = d["from_index"] + k
                c = committed.get(index)
                if c is not None and (c.term, c.cmd) == (term, cmd):
                    flag(("truncate", index, e.node), "committed_lost", e, [e.node, c.event.node],
                         f"n{e.node} truncated committed entry {index} (t{term} {cmd!r})")

        elif e.kind == "final":
            finals.append(e)

    if finals:
        at = finals[-1]
        majority = len(finals) // 2 + 1
        for index, c in committed.items():
            holders = [f.node for f in finals if _has(f.data["log"], index, c.term, c.cmd)]
            if len(holders) < majority:
                flag(("majority", index), "committed_lost", at, [c.event.node],
                     f"committed entry {index} (t{c.term} {c.cmd!r}) only on {holders} at end")
        for a, b in combinations(finals, 2):
            n = min(a.data["commit_index"], b.data["commit_index"])
            for i in range(n):
                if tuple(a.data["log"][i]) != tuple(b.data["log"][i]):
                    flag(("final_diverge", a.node, b.node), "log_divergence", at, [a.node, b.node],
                         f"n{a.node} and n{b.node} committed prefixes differ at index {i + 1}")
                    break

    out.sort(key=lambda v: v.seq)
    return out
