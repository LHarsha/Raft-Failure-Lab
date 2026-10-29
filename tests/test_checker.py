import unittest

from labsim.checker import check
from labsim.cluster import Cluster, Config
from labsim.sim import TraceEvent


class T:
    """Tiny builder for synthetic traces."""

    def __init__(self):
        self.events = []

    def add(self, kind, node, time=None, **data):
        seq = len(self.events)
        self.events.append(TraceEvent(seq, seq if time is None else time, kind, node, data))
        return self

    def leader(self, node, term, log=()):
        return self.add("became_leader", node, term=term, log=tuple(log))

    def commit(self, node, index, term, cmd, commit_term=None):
        return self.add("commit", node, index=index, term=term, cmd=cmd,
                        commit_term=term if commit_term is None else commit_term)

    def truncate(self, node, from_index, removed):
        return self.add("truncate", node, from_index=from_index, removed=tuple(removed))

    def final(self, node, commit_index, log):
        return self.add("final", node, commit_index=commit_index, log=tuple(log))


def invariants(trace):
    return [v.invariant for v in check(trace)]


class ElectionSafetyTest(unittest.TestCase):
    def test_clean_trace_passes(self):
        t = T().leader(0, 1).leader(1, 2).leader(0, 3)
        self.assertEqual(check(t.events), [])

    def test_two_leaders_same_term_flagged_at_second(self):
        t = T().leader(0, 3).add("noise", 2).leader(1, 3)
        [v] = check(t.events)
        self.assertEqual(v.invariant, "election_safety")
        self.assertEqual(v.seq, 2)
        self.assertEqual(set(v.nodes), {0, 1})

    def test_same_leader_reported_twice_is_not_violation(self):
        t = T().leader(0, 3).leader(0, 3)
        self.assertEqual(check(t.events), [])


class CommittedNeverLostTest(unittest.TestCase):
    def test_truncating_committed_entry_flagged(self):
        t = (T().leader(0, 1)
             .commit(0, 1, 1, "a")
             .truncate(2, 1, [(1, "a")]))
        [v] = check(t.events)
        self.assertEqual(v.invariant, "committed_lost")
        self.assertEqual(v.seq, 2)

    def test_truncating_uncommitted_entry_ok(self):
        t = (T().leader(0, 1)
             .commit(0, 1, 1, "a")
             .truncate(2, 2, [(1, "x")]))
        self.assertEqual(check(t.events), [])

    def test_truncating_different_entry_at_committed_index_ok(self):
        # node 2 had a stale entry at index 1; replacing it is how it catches up
        t = T().commit(0, 1, 2, "a").truncate(2, 1, [(1, "stale")])
        self.assertEqual(check(t.events), [])

    def test_later_leader_missing_committed_entry_flagged(self):
        t = (T().leader(0, 1)
             .commit(0, 1, 1, "a")
             .leader(1, 2, log=[]))
        [v] = check(t.events)
        self.assertEqual(v.invariant, "committed_lost")
        self.assertEqual(v.seq, 2)

    def test_leader_of_higher_term_elected_before_commit_still_checked(self):
        t = T().leader(1, 3, log=[]).commit(0, 1, 2, "a", commit_term=2)
        [v] = check(t.events)
        self.assertEqual(v.invariant, "committed_lost")
        self.assertEqual(v.seq, 1)

    def test_stale_lower_term_leader_need_not_have_entry(self):
        # a term-1 candidate collects delayed votes after a term-2 commit: legal
        t = T().commit(0, 1, 2, "a", commit_term=2).leader(1, 1, log=[])
        self.assertEqual(check(t.events), [])

    def test_leader_with_entry_ok(self):
        t = T().commit(0, 1, 1, "a").leader(1, 2, log=[(1, "a")])
        self.assertEqual(check(t.events), [])

    def test_committed_entry_missing_from_majority_at_end_flagged(self):
        t = (T().commit(0, 1, 1, "a")
             .final(0, 1, [(1, "a")])
             .final(1, 0, [])
             .final(2, 0, []))
        self.assertIn("committed_lost", invariants(t.events))

    def test_committed_entry_on_majority_at_end_ok(self):
        t = (T().commit(0, 1, 1, "a")
             .final(0, 1, [(1, "a")])
             .final(1, 0, [(1, "a")])
             .final(2, 0, []))
        self.assertEqual(check(t.events), [])


class NoDivergenceTest(unittest.TestCase):
    def test_conflicting_commits_flagged(self):
        t = T().commit(0, 1, 1, "a").commit(1, 1, 1, "b")
        [v] = check(t.events)
        self.assertEqual(v.invariant, "log_divergence")
        self.assertEqual(v.seq, 1)
        self.assertEqual(set(v.nodes), {0, 1})

    def test_same_commit_on_many_nodes_ok(self):
        t = T().commit(0, 1, 1, "a").commit(1, 1, 1, "a").commit(0, 1, 1, "a")
        self.assertEqual(check(t.events), [])

    def test_final_committed_prefixes_diverge_flagged(self):
        t = (T().final(0, 2, [(1, "a"), (1, "b")])
             .final(1, 2, [(1, "a"), (2, "c")])
             .final(2, 0, []))
        self.assertIn("log_divergence", invariants(t.events))

    def test_commit_index_beyond_log_is_flagged_not_crash(self):
        # a buggy node can truncate entries it already committed; its final
        # commit index then points past the end of its log
        t = (T().final(0, 3, [(1, "a")])
             .final(1, 3, [(1, "a"), (1, "b"), (1, "c")])
             .final(2, 3, [(1, "a"), (1, "b"), (1, "c")]))
        vs = check(t.events)
        self.assertEqual([v.invariant for v in vs], ["committed_lost"])
        self.assertEqual(vs[0].nodes, (0,))

    def test_uncommitted_suffix_may_differ(self):
        t = (T().final(0, 1, [(1, "a"), (1, "b")])
             .final(1, 1, [(1, "a"), (2, "c")]))
        self.assertEqual(check(t.events), [])

    def test_violations_sorted_and_reported_once(self):
        t = (T().leader(0, 3).leader(1, 3).leader(2, 3)
             .commit(0, 1, 3, "a").commit(1, 1, 3, "b").commit(2, 1, 3, "b"))
        vs = check(t.events)
        self.assertEqual([v.invariant for v in vs], ["election_safety", "log_divergence"])
        self.assertEqual(vs, sorted(vs, key=lambda v: v.seq))


class RealElectionRunsTest(unittest.TestCase):
    def test_no_violations_over_50_seeds(self):
        for seed in range(50):
            c = Cluster(seed, Config())
            c.start()
            c.run(2000)
            c.finish()
            with self.subTest(seed=seed):
                self.assertEqual(check(c.sim.trace), [])
                self.assertEqual(sum(e.kind == "final" for e in c.sim.trace), 5)


if __name__ == "__main__":
    unittest.main()
