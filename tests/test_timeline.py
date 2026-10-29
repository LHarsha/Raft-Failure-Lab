import contextlib
import io
import os
import tempfile
import unittest
from html.parser import HTMLParser

from labsim import timeline
from labsim.__main__ import main
from labsim.checker import check
from labsim.scenarios import run_scenario
from tests.test_checker import T

VOID = {"meta", "br", "input", "hr", "link", "img"}


class TagBalance(HTMLParser):
    def __init__(self):
        super().__init__()
        self.stack, self.errors, self.ids, self.classes = [], [], {}, {}

    def handle_starttag(self, tag, attrs):
        a = dict(attrs)
        if "id" in a:
            self.ids[a["id"]] = a.get("class", "")
        if tag not in VOID:
            self.stack.append(tag)

    def handle_endtag(self, tag):
        if not self.stack or self.stack.pop() != tag:
            self.errors.append(tag)


def parse(doc):
    p = TagBalance()
    p.feed(doc)
    p.close()
    return p


def bad_trace():
    return T().leader(0, 1).add("noise", 2).add("noise", 1).leader(1, 1).add("after", 0).events


class TextTimelineTest(unittest.TestCase):
    def test_one_line_per_event_in_order(self):
        trace = bad_trace()
        lines = timeline.text(trace, []).splitlines()
        seqs = [int(l.split("#")[1].split()[0]) for l in lines if "#" in l]
        self.assertEqual(seqs, [0, 1, 2, 3, 4])

    def test_violation_banner_follows_offending_event(self):
        trace = bad_trace()
        lines = timeline.text(trace, check(trace)).splitlines()
        i = next(k for k, l in enumerate(lines) if "!!! VIOLATION" in l)
        self.assertIn("#3", lines[i - 1])
        self.assertIn("election_safety", lines[i])

    def test_context_limits_output(self):
        trace = bad_trace()
        out = timeline.text(trace, check(trace), context=1)
        self.assertIn("#2", out)
        self.assertIn("#3", out)
        self.assertNotIn("#0 ", out)
        self.assertNotIn("#4 ", out)

    def test_hide_kinds(self):
        out = timeline.text(bad_trace(), [], hide={"noise"})
        self.assertNotIn("noise", out)

    def test_real_run_mentions_leader_and_commits(self):
        r = run_scenario("leader_crash", seed=0)
        out = timeline.text(r.trace, r.violations)
        self.assertIn("became_leader", out)
        self.assertIn("crash", out)
        self.assertIn("commit", out)


class HtmlTimelineTest(unittest.TestCase):
    def test_well_formed(self):
        r = run_scenario("partition_minority_leader", seed=3)
        p = parse(timeline.html(r.trace, r.violations, title="t"))
        self.assertEqual((p.stack, p.errors), ([], []))

    def test_violation_row_marked(self):
        trace = bad_trace()
        p = parse(timeline.html(trace, check(trace), title="bad"))
        self.assertIn("violation", p.ids["e3"])
        self.assertNotIn("violation", p.ids["e2"])
        self.assertIn("v0", p.ids)   # link target in the violation summary

    def test_one_lane_per_node(self):
        r = run_scenario("baseline", seed=0)
        doc = timeline.html(r.trace, r.violations)
        for n in range(5):
            self.assertIn(f'<th class="lane">n{n}</th>', doc)

    def test_crashed_lane_shaded(self):
        r = run_scenario("leader_crash", seed=0)
        doc = timeline.html(r.trace, r.violations)
        self.assertIn("crashed", doc)

    def test_partition_shown(self):
        r = run_scenario("partition_minority_leader", seed=0)
        self.assertIn("grp1", timeline.html(r.trace, r.violations))

    def test_byte_identical_per_seed(self):
        a = run_scenario("flapping_partition", seed=4)
        b = run_scenario("flapping_partition", seed=4)
        self.assertEqual(timeline.html(a.trace, a.violations), timeline.html(b.trace, b.violations))
        self.assertEqual(timeline.text(a.trace, a.violations), timeline.text(b.trace, b.violations))


class CliTest(unittest.TestCase):
    def test_run_writes_html_and_prints_summary(self):
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "out.html")
            buf = io.StringIO()
            with contextlib.redirect_stdout(buf):
                code = main(["run", "--scenario", "leader_crash", "--seed", "7", "--html", path, "--quiet"])
            self.assertEqual(code, 0)
            self.assertTrue(os.path.getsize(path) > 1000)
            self.assertIn("violations: 0", buf.getvalue())


if __name__ == "__main__":
    unittest.main()
