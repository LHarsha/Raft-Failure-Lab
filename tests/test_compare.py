import contextlib
import io
import unittest

from labsim.__main__ import main
from labsim.compare import METRICS, STRATEGIES, compare, metrics, parse_seeds, table
from tests.test_checker import T


class MetricsTest(unittest.TestCase):
    def trace(self):
        return (T()
                .add("became_leader", 0, time=100, term=1, log=())
                .add("client_accept", 0, time=150, cmd="c1", index=1, term=1)
                .add("commit", 0, time=170, index=1, term=1, cmd="c1", commit_term=1)
                .add("commit", 1, time=180, index=1, term=1, cmd="c1", commit_term=1)
                .add("client_accept", 0, time=390, cmd="c2", index=2, term=1)
                .add("crash", 0, time=400, term=1)
                .add("client_reject", 1, time=450, cmd="c3")
                .add("became_candidate", 1, time=550, term=2)
                .add("became_leader", 1, time=600, term=2, log=())
                .events)

    def test_basic_metrics(self):
        m = metrics(self.trace(), n=3, until=1000)
        self.assertEqual(m["first_leader_ms"], 100)
        self.assertEqual(m["leaderless_ms"], 300)       # 0-100 and 400-600
        self.assertEqual(m["max_gap_ms"], 200)
        self.assertEqual(m["elections"], 1)
        self.assertEqual(m["max_term"], 2)
        self.assertEqual(m["commits"], 1)
        self.assertEqual(m["latency_ms"], 20)
        self.assertEqual(m["lost"], 1)
        self.assertEqual(m["rejects"], 1)

    def test_minority_leader_is_not_effective(self):
        t = (T()
             .add("became_leader", 0, time=100, term=1, log=())
             .add("partition", None, time=200, groups=((0, 1), (2, 3, 4)))
             .add("became_leader", 2, time=500, term=2, log=())
             .events)
        m = metrics(t, n=5, until=600)
        self.assertEqual(m["leaderless_ms"], 100 + 300)

    def test_no_leader_ever(self):
        m = metrics([], n=3, until=500)
        self.assertIsNone(m["first_leader_ms"])
        self.assertEqual(m["leaderless_ms"], 500)


class CompareTest(unittest.TestCase):
    def test_two_strategies_with_different_timeouts(self):
        self.assertEqual(len(STRATEGIES), 2)
        timeouts = {cfg.election_timeout for cfg in STRATEGIES.values()}
        self.assertEqual(len(timeouts), 2)

    def test_same_seeds_and_scenarios_for_both(self):
        res = compare(seeds=[0, 1], scenarios=["baseline", "leader_crash"])
        runs = {s: sorted((r["scenario"], r["seed"]) for r in res.runs if r["strategy"] == s)
                for s in STRATEGIES}
        a, b = runs.values()
        self.assertEqual(a, b)
        self.assertEqual(a, [("baseline", 0), ("baseline", 1), ("leader_crash", 0), ("leader_crash", 1)])

    def test_deterministic(self):
        a = compare(seeds=[2, 3], scenarios=["lossy"])
        b = compare(seeds=[2, 3], scenarios=["lossy"])
        self.assertEqual(a.runs, b.runs)

    def test_table_lists_every_metric_and_scenario(self):
        res = compare(seeds=[0], scenarios=["baseline", "slow"])
        out = table(res)
        for m in METRICS:
            self.assertIn(m, out)
        for s in ("baseline", "slow"):
            self.assertIn(s, out)
        for name in STRATEGIES:
            self.assertIn(name, out)

    def test_parse_seeds(self):
        self.assertEqual(parse_seeds("0..3"), [0, 1, 2, 3])
        self.assertEqual(parse_seeds("5,7"), [5, 7])

    def test_cli_compare(self):
        buf = io.StringIO()
        with contextlib.redirect_stdout(buf):
            self.assertEqual(main(["compare", "--seeds", "0..1", "--scenarios", "baseline"]), 0)
        self.assertIn("baseline", buf.getvalue())


if __name__ == "__main__":
    unittest.main()
