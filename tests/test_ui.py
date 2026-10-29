import json
import socket
import threading
import unittest
import urllib.request
from unittest import mock

from labsim import ui
from labsim.raft import RaftNode
from labsim.scenarios import SCENARIOS
from tests.test_timeline import parse


def get(path):
    status, ctype, body = ui.handle(path)
    return status, ctype, body.decode("utf-8")


class PageTest(unittest.TestCase):
    def test_index_loads_app(self):
        status, ctype, body = get("/")
        self.assertEqual(status, 200)
        self.assertIn("text/html", ctype)
        self.assertIn('src="static/app.js"', body)
        self.assertIn('href="static/app.css"', body)

    def test_index_is_well_formed(self):
        p = parse(get("/")[2])
        self.assertEqual((p.stack, p.errors), ([], []))

    def test_static_files_served_with_types(self):
        self.assertIn("javascript", get("/static/app.js")[1])
        self.assertIn("text/css", get("/static/app.css")[1])

    def test_static_rejects_unknown_and_traversal(self):
        for path in ["/static/nope.js", "/static/../ui.py", "/static/..%2Fui.py", "/static/"]:
            with self.subTest(path=path):
                self.assertEqual(get(path)[0], 404)

    def test_unknown_path_404(self):
        self.assertEqual(get("/nope")[0], 404)


class MetaApiTest(unittest.TestCase):
    def test_meta_describes_scenarios_strategies_and_invariants(self):
        status, _, body = get("/api/meta")
        self.assertEqual(status, 200)
        m = json.loads(body)
        self.assertEqual([s["name"] for s in m["scenarios"]], list(SCENARIOS))
        for s in m["scenarios"]:
            self.assertTrue(s["title"] and s["description"])
        self.assertEqual(len(m["strategies"]), 2)
        self.assertEqual({i["id"] for i in m["invariants"]},
                         {"election_safety", "committed_lost", "log_divergence"})
        self.assertEqual(m["cluster_size"], 5)
        self.assertGreater(m["run_until"], m["fault_window"][1])

    def test_findings_markdown(self):
        status, _, body = get("/api/findings")
        self.assertEqual(status, 200)
        self.assertIn("# Findings", json.loads(body)["markdown"])


class RunApiTest(unittest.TestCase):
    def test_run_returns_summary_and_metrics(self):
        status, ctype, body = get("/api/run?scenario=leader_crash&seed=7&lo=150&hi=300")
        self.assertEqual(status, 200)
        self.assertIn("json", ctype)
        data = json.loads(body)
        self.assertEqual((data["scenario"], data["seed"]), ("leader_crash", 7))
        self.assertEqual(data["violations"], [])
        self.assertIn("leaderless_ms", data["metrics"])
        self.assertGreater(data["events"], 100)
        self.assertTrue(data["timeline_url"].startswith("/timeline?"))

    def test_run_includes_full_trace_for_replay(self):
        data = json.loads(get("/api/run?scenario=leader_crash&seed=7&lo=150&hi=300")[2])
        trace = data["trace"]
        self.assertEqual(len(trace), data["events"])
        seq, time, kind, node, payload, text = trace[0]
        self.assertEqual(seq, 0)
        self.assertIsInstance(payload, dict)
        self.assertIsInstance(text, str)
        self.assertIn("crash", {row[2] for row in trace})
        self.assertEqual([row[0] for row in trace], list(range(len(trace))))

    def test_run_matches_cli_determinism(self):
        a = json.loads(get("/api/run?scenario=flapping_partition&seed=3&lo=150&hi=300")[2])
        b = json.loads(get("/api/run?scenario=flapping_partition&seed=3&lo=150&hi=300")[2])
        self.assertEqual(a, b)

    def test_violations_are_reported(self):
        def naive(self):
            for n in range(len(self.log), self.commit_index, -1):
                if 1 + sum(self.match_index[p] >= n for p in self.peers) >= self.majority:
                    self._commit_up_to(n, self.current_term)
                    break
        with mock.patch.object(RaftNode, "_advance_leader_commit", naive):
            data = json.loads(get("/api/run?scenario=figure8&seed=0&lo=150&hi=300")[2])
        self.assertTrue(data["violations"])
        v = data["violations"][0]
        self.assertEqual(set(v), {"invariant", "time", "seq", "nodes", "detail"})

    def test_timeline_page(self):
        status, ctype, body = get("/timeline?scenario=baseline&seed=1&lo=150&hi=300")
        self.assertEqual(status, 200)
        self.assertIn("<table>", body)

    def test_bad_input_rejected(self):
        for q in ["scenario=nope&seed=1&lo=150&hi=300",
                  "scenario=baseline&seed=x&lo=150&hi=300",
                  "scenario=baseline&seed=1&lo=300&hi=150",
                  "scenario=baseline&seed=1&lo=0&hi=10",
                  "scenario=baseline&seed=1&lo=150&hi=99999"]:
            with self.subTest(q=q):
                status, _, body = get("/api/run?" + q)
                self.assertEqual(status, 400)
                self.assertIn("error", json.loads(body))


class CompareApiTest(unittest.TestCase):
    def test_compare_rows(self):
        status, _, body = get("/api/compare?seeds=0..1&scenarios=baseline,leader_crash")
        self.assertEqual(status, 200)
        data = json.loads(body)
        self.assertEqual(data["seeds"], [0, 1])
        self.assertEqual(len(data["rows"]), 4)   # 2 scenarios x 2 strategies
        row = data["rows"][0]
        self.assertEqual(row["scenario"], "baseline")
        self.assertIn("violations", row["metrics"])
        self.assertTrue(data["metric_help"])

    def test_compare_limits_seed_count(self):
        status, _, _ = get("/api/compare?seeds=0..999&scenarios=baseline")
        self.assertEqual(status, 400)

    def test_compare_rejects_unknown_scenario(self):
        self.assertEqual(get("/api/compare?seeds=0..1&scenarios=bogus")[0], 400)


class ServerTest(unittest.TestCase):
    def test_idle_connection_does_not_block_requests(self):
        # Browsers keep idle connections open; a single-threaded server would hang here.
        server = ui.make_server(0)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        port = server.server_port
        idle = socket.create_connection(("127.0.0.1", port))
        try:
            url = f"http://127.0.0.1:{port}/api/run?scenario=baseline&seed=1&lo=150&hi=300"
            with urllib.request.urlopen(url, timeout=5) as resp:
                self.assertEqual(resp.status, 200)
                self.assertEqual(json.loads(resp.read())["seed"], 1)
        finally:
            idle.close()
            server.shutdown()
            server.server_close()


if __name__ == "__main__":
    unittest.main()
