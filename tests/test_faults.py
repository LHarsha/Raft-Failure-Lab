import unittest
from unittest import mock

from labsim.cluster import Cluster, Config
from labsim.network import Network
from labsim.raft import RaftNode
from labsim.scenarios import SCENARIOS, run_scenario
from labsim.sim import Simulator
from tests.test_election import FakeNode


class NetworkFaultTest(unittest.TestCase):
    def setUp(self):
        self.sim = Simulator(seed=11)
        self.net = Network(self.sim, delay=(5, 15))
        self.nodes = [FakeNode(i) for i in range(3)]
        for n in self.nodes:
            self.net.register(n)

    def kinds(self):
        return [e.kind for e in self.sim.trace]

    def test_duplicate_delivers_twice(self):
        self.net.dup_prob = 1.0
        self.net.send(0, 1, "m")
        self.sim.run()
        self.assertEqual(len(self.nodes[1].inbox), 2)
        self.assertEqual(self.kinds().count("duplicate"), 1)
        ids = {e.data["id"] for e in self.sim.trace if e.kind == "deliver"}
        self.assertEqual(len(ids), 1)

    def test_drop_delivers_nothing(self):
        self.net.drop_prob = 1.0
        self.net.send(0, 1, "m")
        self.sim.run()
        self.assertEqual(self.nodes[1].inbox, [])
        drop = [e for e in self.sim.trace if e.kind == "drop"][0]
        self.assertEqual(drop.data["reason"], "random")

    def test_partition_blocks_across_groups_only(self):
        self.net.partition = [{0, 1}, {2}]
        self.net.send(0, 1, "same side")
        self.net.send(0, 2, "across")
        self.net.send(2, 0, "across back")
        self.sim.run()
        self.assertEqual([m for _, _, m in self.nodes[1].inbox], ["same side"])
        self.assertEqual(self.nodes[2].inbox + self.nodes[0].inbox, [])

    def test_partition_drops_message_already_in_flight(self):
        self.net.send(0, 2, "in flight")
        self.sim.schedule(1, setattr, self.net, "partition", [{0}, {1, 2}])
        self.sim.run()
        self.assertEqual(self.nodes[2].inbox, [])

    def test_heal_restores_delivery(self):
        self.net.partition = [{0}, {1, 2}]
        self.net.partition = None
        self.net.send(0, 2, "m")
        self.sim.run()
        self.assertEqual(len(self.nodes[2].inbox), 1)

    def test_link_delay_override(self):
        self.net.link_delay[(0, 1)] = (100, 100)
        self.net.send(0, 1, "slow")
        self.net.send(0, 2, "fast")
        self.sim.run()
        times = {e.node: e.time for e in self.sim.trace if e.kind == "deliver"}
        self.assertEqual(times[1], 100)
        self.assertLessEqual(times[2], 15)


class ClusterFaultTest(unittest.TestCase):
    def test_crashed_node_processes_nothing(self):
        c = Cluster(1, Config(client_interval=50))
        c.start()
        c.run(500)
        victim = next(n for n in c.nodes if n.role == "follower")
        c.crash(victim.id)
        log_len, term = len(victim.log), victim.current_term
        mark = len(c.sim.trace)
        c.run(1500)
        own = [e.kind for e in c.sim.trace[mark:] if e.node == victim.id]
        self.assertTrue(set(own) <= {"drop"}, own)
        self.assertEqual((len(victim.log), victim.current_term), (log_len, term))

    def test_crash_and_restart_are_traced_and_scheduled(self):
        c = Cluster(1, Config())
        c.at(100, c.crash, 2)
        c.at(300, c.restart, 2)
        c.start()
        c.run(400)
        faults = [(e.time, e.kind, e.node) for e in c.sim.trace if e.kind in ("crash", "restart")]
        self.assertEqual(faults, [(100, "crash", 2), (300, "restart", 2)])

    def test_partition_and_heal_traced(self):
        c = Cluster(1, Config())
        c.at(50, c.partition, [{0, 1}, {2, 3, 4}])
        c.at(90, c.heal)
        c.start()
        c.run(100)
        kinds = [(e.time, e.kind) for e in c.sim.trace if e.kind in ("partition", "heal")]
        self.assertEqual(kinds, [(50, "partition"), (90, "heal")])
        self.assertIsNone(c.net.partition)

    def test_set_net_changes_fault_knobs(self):
        c = Cluster(1, Config())
        c.set_net(drop=0.2, dup=0.3, delay=(1, 50))
        self.assertEqual((c.net.drop_prob, c.net.dup_prob, c.net.delay), (0.2, 0.3, (1, 50)))
        self.assertEqual(c.sim.trace[-1].kind, "net")

    def test_minority_leader_cannot_commit(self):
        c = Cluster(2, Config(client_interval=20))
        c.start()
        c.run(800)
        old = c.leaders()[0]
        buddy = next(n for n in c.nodes if n is not old)
        c.partition([{old.id, buddy.id}, {n.id for n in c.nodes} - {old.id, buddy.id}])
        c._client_target = old.id
        committed_before = old.commit_index
        c.run(1000)
        self.assertEqual(old.commit_index, committed_before)
        self.assertGreater(len(old.log), committed_before)   # accepted, never committed

    def test_paused_client_submits_nothing(self):
        c = Cluster(3, Config(client_interval=20))
        c.start()
        c.run(600)
        c.client_paused = True
        mark = len(c.sim.trace)
        c.run(1200)
        self.assertEqual([e for e in c.sim.trace[mark:] if e.kind.startswith("client")], [])
        c.client_paused = False
        c.run(1300)
        self.assertTrue(any(e.kind == "client_accept" for e in c.sim.trace[mark:]))


class ScenarioTest(unittest.TestCase):
    EXPECTED_FAULTS = {
        "lossy": {"net"},
        "slow": {"net"},
        "leader_crash": {"crash", "restart"},
        "partition_minority_leader": {"partition", "heal"},
        "flapping_partition": {"partition", "heal"},
        "crash_restart_storm": {"crash", "restart"},
        "figure8": {"partition", "crash", "restart", "heal"},
    }

    def test_all_scenarios_registered(self):
        self.assertEqual(set(SCENARIOS), {"baseline"} | set(self.EXPECTED_FAULTS))

    def test_scenarios_inject_their_faults(self):
        for name, kinds in self.EXPECTED_FAULTS.items():
            with self.subTest(scenario=name):
                r = run_scenario(name, seed=0)
                seen = {e.kind for e in r.trace}
                self.assertTrue(kinds <= seen, f"{name}: {kinds - seen} missing")

    def test_every_scenario_safe_over_20_seeds(self):
        for name in SCENARIOS:
            for seed in range(20):
                with self.subTest(scenario=name, seed=seed):
                    r = run_scenario(name, seed=seed)
                    self.assertEqual(r.violations, [], r.violations[:1])

    def test_scenarios_recover_and_commit(self):
        # every scenario ends with a fault-free tail, so the cluster should converge
        for name in SCENARIOS:
            with self.subTest(scenario=name):
                r = run_scenario(name, seed=1)
                finals = [e for e in r.trace if e.kind == "final"]
                self.assertTrue(all(f.data["alive"] for f in finals))
                commits = {f.data["commit_index"] for f in finals}
                self.assertEqual(len(commits), 1, commits)
                self.assertGreater(commits.pop(), 0)

    def test_figure8_script_reaches_the_end(self):
        for seed in range(20):
            with self.subTest(seed=seed):
                steps = [e.data["step"] for e in run_scenario("figure8", seed).trace if e.kind == "scenario"]
                self.assertEqual(steps[-1], "complete", steps)

    def test_figure8_replays_the_paper_shape(self):
        r = run_scenario("figure8", seed=0)
        old = [e for e in r.trace if e.kind == "client_accept" and e.data["cmd"] == "f8-old"]
        mid = [e for e in r.trace if e.kind == "client_accept" and e.data["cmd"] == "f8-mid"]
        self.assertEqual((len(old), len(mid)), (1, 1))
        self.assertEqual(old[0].data["index"], mid[0].data["index"])   # same slot, different terms
        self.assertLess(old[0].data["term"], mid[0].data["term"])
        # f8-old was on a majority yet must never be committed; f8-mid wins the slot
        self.assertNotIn("f8-old", {e.data["cmd"] for e in r.trace if e.kind == "commit"})
        self.assertIn("f8-mid", {e.data["cmd"] for e in r.trace if e.kind == "commit"})

    def test_figure8_catches_naive_commit_rule(self):
        # Raft §5.4.2 broken: count replicas for entries of any term.
        def naive(self):
            for n in range(len(self.log), self.commit_index, -1):
                if 1 + sum(self.match_index[p] >= n for p in self.peers) >= self.majority:
                    self._commit_up_to(n, self.current_term)
                    break
        with mock.patch.object(RaftNode, "_advance_leader_commit", naive):
            for seed in range(20):
                with self.subTest(seed=seed):
                    kinds = {v.invariant for v in run_scenario("figure8", seed).violations}
                    self.assertIn("committed_lost", kinds)

    def test_scenario_runs_are_deterministic(self):
        for name in SCENARIOS:
            a = run_scenario(name, seed=5).trace
            b = run_scenario(name, seed=5).trace
            self.assertEqual([(e.time, e.kind, e.node, e.data) for e in a],
                             [(e.time, e.kind, e.node, e.data) for e in b], name)


if __name__ == "__main__":
    unittest.main()
