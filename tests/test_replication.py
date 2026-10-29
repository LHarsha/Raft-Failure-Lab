import unittest

from labsim.checker import check
from labsim.cluster import Cluster, Config
from labsim.raft import AppendEntries, AppendEntriesReply, Entry


def run_with_client(seed, until=3000, client_until=2500, **kw):
    c = Cluster(seed, Config(client_interval=50, client_until=client_until, **kw))
    c.start()
    c.run(until)
    c.finish()
    return c


def logs(c):
    return [[(e.term, e.command) for e in n.log] for n in c.nodes]


class ReplicationTest(unittest.TestCase):
    def test_no_faults_all_nodes_converge(self):
        for seed in range(10):
            with self.subTest(seed=seed):
                c = run_with_client(seed)
                ls = logs(c)
                self.assertGreater(len(ls[0]), 20)
                self.assertTrue(all(l == ls[0] for l in ls))
                self.assertTrue(all(n.commit_index == len(ls[0]) for n in c.nodes))

    def test_commands_committed_in_submission_order(self):
        c = run_with_client(3)
        cmds = [cmd for _, cmd in logs(c)[0]]
        self.assertEqual(cmds, sorted(cmds, key=lambda s: int(s[1:])))
        self.assertEqual(len(cmds), len(set(cmds)))

    def test_client_accept_and_commit_are_traced(self):
        c = run_with_client(4)
        accepted = {e.data["cmd"] for e in c.sim.trace if e.kind == "client_accept"}
        committed = {e.data["cmd"] for e in c.sim.trace if e.kind == "commit"}
        self.assertTrue(accepted)
        self.assertEqual(committed, accepted)

    def test_checker_clean_over_50_seeds(self):
        for seed in range(50):
            with self.subTest(seed=seed):
                self.assertEqual(check(run_with_client(seed, until=1500, client_until=1200).sim.trace), [])

    def test_lagging_follower_catches_up(self):
        c = Cluster(5, Config(client_interval=50, client_until=2500))
        c.start()
        c.run(500)
        lagger = next(n for n in c.nodes if n.role == "follower")
        lagger.crash()
        c.run(1500)
        self.assertLess(len(lagger.log), len(c.leaders()[0].log))
        lagger.restart()
        self.assertEqual(lagger.commit_index, 0)
        c.run(3500)
        c.finish()
        ls = logs(c)
        self.assertTrue(all(l == ls[0] for l in ls))
        self.assertEqual(lagger.commit_index, len(ls[0]))
        self.assertEqual(check(c.sim.trace), [])

    def test_persistent_state_survives_crash(self):
        c = run_with_client(6, until=1000, client_until=1000)
        node = c.nodes[0]
        term, voted, log = node.current_term, node.voted_for, list(node.log)
        node.crash()
        node.restart()
        self.assertEqual((node.current_term, node.voted_for, node.log), (term, voted, log))
        self.assertEqual((node.role, node.commit_index, node.leader_id), ("follower", 0, None))


class FollowerAppendTest(unittest.TestCase):
    def setUp(self):
        self.c = Cluster(0, Config(n=3))   # not started: no timers firing
        self.f = self.c.nodes[1]
        self.f.current_term = 2
        self.f.log = [Entry(1, "a"), Entry(1, "b"), Entry(2, "x")]

    def deliver(self, msg):
        mark = len(self.c.sim.trace)
        self.f.receive(0, msg)
        return self.c.sim.trace[mark:]

    def reply(self, events):
        sends = [e for e in events if e.kind == "send"]
        return sends[-1].data["msg"]

    def test_conflicting_suffix_truncated(self):
        evs = self.deliver(AppendEntries(3, 0, 2, 1, (Entry(3, "y"),), 0))
        self.assertEqual(self.f.log, [Entry(1, "a"), Entry(1, "b"), Entry(3, "y")])
        [t] = [e for e in evs if e.kind == "truncate"]
        self.assertEqual((t.data["from_index"], t.data["removed"]), (3, ((2, "x"),)))
        self.assertIn("ok match=3", self.reply(evs))

    def test_stale_duplicate_does_not_truncate(self):
        evs = self.deliver(AppendEntries(2, 0, 0, 0, (Entry(1, "a"),), 0))
        self.assertEqual(len(self.f.log), 3)
        self.assertEqual([e for e in evs if e.kind == "truncate"], [])

    def test_missing_prev_entry_rejected_with_hint(self):
        evs = self.deliver(AppendEntries(2, 0, 7, 2, (), 0))
        self.assertIn("fail match=3", self.reply(evs))

    def test_prev_term_mismatch_rejected(self):
        evs = self.deliver(AppendEntries(2, 0, 3, 1, (), 0))
        self.assertIn("fail", self.reply(evs))
        self.assertEqual(len(self.f.log), 3)

    def test_follower_commit_capped_at_last_new_entry(self):
        # leader says commit=3, but only index 1 is known to match this leader
        self.deliver(AppendEntries(2, 0, 1, 1, (), 3))
        self.assertEqual(self.f.commit_index, 1)

    def test_stale_term_rejected(self):
        evs = self.deliver(AppendEntries(1, 0, 0, 0, (Entry(1, "z"),), 0))
        self.assertIn("fail", self.reply(evs))
        self.assertEqual(self.f.log[0], Entry(1, "a"))


class LeaderCommitRuleTest(unittest.TestCase):
    def test_previous_term_entry_not_committed_by_count(self):
        c = Cluster(0, Config(n=5))
        leader = c.nodes[0]
        leader.current_term = 3
        leader.log = [Entry(2, "a")]
        leader._become_leader()
        leader.receive(1, AppendEntriesReply(3, True, 1))
        leader.receive(2, AppendEntriesReply(3, True, 1))
        self.assertEqual(leader.commit_index, 0)   # Raft §5.4.2
        self.assertTrue(leader.client_submit("b"))
        leader.receive(1, AppendEntriesReply(3, True, 2))
        self.assertEqual(leader.commit_index, 0)
        leader.receive(2, AppendEntriesReply(3, True, 2))
        self.assertEqual(leader.commit_index, 2)
        commits = [(e.data["index"], e.data["commit_term"]) for e in c.sim.trace if e.kind == "commit"]
        self.assertEqual(commits, [(1, 3), (2, 3)])

    def test_reordered_old_success_reply_does_not_regress_match(self):
        c = Cluster(0, Config(n=3))
        leader = c.nodes[0]
        leader.current_term = 1
        leader._become_leader()
        for cmd in "abc":
            leader.client_submit(cmd)
        leader.receive(1, AppendEntriesReply(1, True, 3))
        leader.receive(1, AppendEntriesReply(1, True, 1))
        self.assertEqual((leader.match_index[1], leader.next_index[1]), (3, 4))

    def test_non_leader_rejects_client(self):
        c = Cluster(0, Config(n=3))
        self.assertFalse(c.nodes[0].client_submit("x"))


if __name__ == "__main__":
    unittest.main()
