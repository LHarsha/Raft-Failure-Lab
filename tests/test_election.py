import unittest
from collections import defaultdict

from labsim.cluster import Cluster, Config
from labsim.network import Network
from labsim.raft import RequestVote
from labsim.sim import Simulator


class FakeNode:
    def __init__(self, nid):
        self.id = nid
        self.alive = True
        self.inbox = []

    def receive(self, src, msg):
        self.inbox.append((self.id, src, msg))


class NetworkTest(unittest.TestCase):
    def setUp(self):
        self.sim = Simulator(seed=3)
        self.net = Network(self.sim, delay=(5, 15))
        self.a, self.b = FakeNode(0), FakeNode(1)
        self.net.register(self.a)
        self.net.register(self.b)

    def test_delivery_within_delay_bounds(self):
        self.net.send(0, 1, "hello")
        self.sim.run()
        self.assertEqual(self.b.inbox, [(1, 0, "hello")])
        deliver = [e for e in self.sim.trace if e.kind == "deliver"][0]
        self.assertTrue(5 <= deliver.time <= 15)

    def test_send_and_deliver_are_traced_with_shared_id(self):
        self.net.send(0, 1, "hello")
        self.sim.run()
        send, deliver = self.sim.trace
        self.assertEqual((send.kind, send.node, send.data["dst"]), ("send", 0, 1))
        self.assertEqual((deliver.kind, deliver.node, deliver.data["src"]), ("deliver", 1, 0))
        self.assertEqual(send.data["id"], deliver.data["id"])

    def test_crashed_receiver_drops(self):
        self.net.send(0, 1, "hello")
        self.b.alive = False
        self.sim.run()
        self.assertEqual(self.b.inbox, [])
        self.assertEqual([e.kind for e in self.sim.trace], ["send", "drop"])

    def test_crashed_sender_cannot_send(self):
        self.a.alive = False
        self.net.send(0, 1, "hello")
        self.sim.run()
        self.assertEqual(self.b.inbox, [])


def run_cluster(seed, n=5, until=2000):
    c = Cluster(seed, Config(n=n))
    c.start()
    c.run(until)
    return c


class ElectionTest(unittest.TestCase):
    def assert_single_stable_leader(self, c):
        leaders = c.leaders()
        self.assertEqual(len(leaders), 1)
        leader = leaders[0]
        for node in c.nodes:
            self.assertEqual(node.current_term, leader.current_term)
            self.assertEqual(node.leader_id, leader.id)

    def test_three_nodes_elect_one_leader(self):
        for seed in range(20):
            with self.subTest(seed=seed):
                self.assert_single_stable_leader(run_cluster(seed, n=3))

    def test_five_nodes_elect_one_leader(self):
        for seed in range(20):
            with self.subTest(seed=seed):
                self.assert_single_stable_leader(run_cluster(seed, n=5))

    def test_single_node_elects_itself(self):
        c = run_cluster(0, n=1, until=1000)
        self.assertEqual(len(c.leaders()), 1)

    def test_at_most_one_vote_per_term(self):
        for seed in range(20):
            votes = defaultdict(set)
            for e in run_cluster(seed).sim.trace:
                if e.kind == "vote":
                    votes[(e.node, e.data["term"])].add(e.data["candidate"])
            for key, cands in votes.items():
                self.assertLessEqual(len(cands), 1, f"seed {seed} {key}")

    def test_one_leader_per_term_in_trace(self):
        for seed in range(20):
            by_term = defaultdict(set)
            for e in run_cluster(seed).sim.trace:
                if e.kind == "became_leader":
                    by_term[e.data["term"]].add(e.node)
            for term, ids in by_term.items():
                self.assertEqual(len(ids), 1, f"seed {seed} term {term}")

    def test_leader_steps_down_on_higher_term(self):
        c = run_cluster(1)
        leader = c.leaders()[0]
        other = next(n for n in c.nodes if n is not leader)
        term = leader.current_term
        mark = len(c.sim.trace)
        leader.receive(other.id, RequestVote(term + 5, other.id, 0, 0))
        self.assertEqual(leader.role, "follower")
        self.assertEqual(leader.current_term, term + 5)
        kinds = [e.kind for e in c.sim.trace[mark:] if e.node == leader.id]
        self.assertIn("became_follower", kinds)

    def test_stale_term_vote_request_rejected(self):
        c = run_cluster(2)
        follower = next(n for n in c.nodes if n.role == "follower")
        before = follower.voted_for
        follower.receive(99, RequestVote(0, 99, 0, 0))
        self.assertEqual(follower.voted_for, before)

    def test_election_timeouts_drawn_from_config(self):
        c = Cluster(5, Config(n=3, election_timeout=(400, 500)))
        c.start()
        c.run(399)
        self.assertEqual([e for e in c.sim.trace if e.kind == "became_candidate"], [])

    def test_deterministic_per_seed(self):
        def sig(c):
            return [(e.seq, e.time, e.kind, e.node, e.data) for e in c.sim.trace]
        self.assertEqual(sig(run_cluster(7)), sig(run_cluster(7)))
        self.assertNotEqual(sig(run_cluster(7)), sig(run_cluster(8)))


if __name__ == "__main__":
    unittest.main()
