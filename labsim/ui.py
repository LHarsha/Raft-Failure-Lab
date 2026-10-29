"""Local web interface. Serves a single-page app (labsim/web/) plus a small
JSON API: run a scenario and get its full trace for replay, compare the two
strategies, read FINDINGS.md. Standard library only; serves on 127.0.0.1.

  python -m labsim ui            (opens http://127.0.0.1:8000)
"""

import json
from pathlib import Path
from dataclasses import asdict
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlencode, urlsplit

from labsim import compare, timeline
from labsim.cluster import Config
from labsim.scenarios import FAULT_END, FAULT_START, RUN_UNTIL, SCENARIOS, run_scenario

WEB_DIR = Path(__file__).parent / "web"
FINDINGS = Path(__file__).parent.parent / "FINDINGS.md"
STATIC_TYPES = {".js": "text/javascript; charset=utf-8", ".css": "text/css; charset=utf-8",
                ".svg": "image/svg+xml", ".html": "text/html; charset=utf-8"}

# name -> (title, description, fault tags)
SCENARIO_INFO = {
    "baseline": ("Baseline", "No faults. A healthy cluster for reference.", []),
    "lossy": ("Lossy network", "10% of messages are dropped and 10% are delivered twice.",
              ["drop", "duplicate"]),
    "slow": ("Slow network", "Message delay jumps from 1–10 ms to 1–120 ms.", ["delay"]),
    "leader_crash": ("Leader crash", "The leader crashes and comes back one second later.",
                     ["crash", "restart"]),
    "partition_minority_leader": ("Minority leader", "The leader and one follower are cut off from "
                                  "the other three.", ["partition"]),
    "flapping_partition": ("Flapping partition", "The network splits randomly every 300 ms, then heals.",
                           ["partition"]),
    "crash_restart_storm": ("Crash storm", "Random nodes crash and restart; at most two are down at once.",
                            ["crash", "restart"]),
    "figure8": ("Raft Figure 8", "Scripted replay of Raft paper Figure 8, the schedule that needs the "
                "§5.4.2 commit rule.", ["partition", "crash", "scripted"]),
}
DESCRIPTIONS = {name: info[1] for name, info in SCENARIO_INFO.items()}

INVARIANTS = [
    {"id": "election_safety", "name": "Election safety", "desc": "At most one leader per term."},
    {"id": "committed_lost", "name": "Committed never lost",
     "desc": "Committed entries are never erased, and every later leader has them."},
    {"id": "log_divergence", "name": "No divergence",
     "desc": "No two servers commit different entries at the same position."},
]

MAX_COMPARE_SEEDS = 100


class BadRequest(Exception):
    pass


def _int(q, key, lo, hi):
    try:
        v = int(q[key][0])
    except (KeyError, ValueError, IndexError):
        raise BadRequest(f"{key} must be a whole number")
    if not lo <= v <= hi:
        raise BadRequest(f"{key} must be between {lo} and {hi}")
    return v


def _run_params(q):
    scenario = q.get("scenario", [""])[0]
    if scenario not in SCENARIOS:
        raise BadRequest(f"unknown scenario {scenario!r}")
    seed = _int(q, "seed", 0, 10**9)
    lo = _int(q, "lo", 10, 5000)
    hi = _int(q, "hi", 10, 5000)
    if lo > hi:
        raise BadRequest("timeout min must not exceed max")
    return scenario, seed, lo, hi


def _run(q):
    scenario, seed, lo, hi = _run_params(q)
    cfg = Config(election_timeout=(lo, hi))
    return scenario, seed, lo, hi, cfg, run_scenario(scenario, seed, cfg)


def api_run(q):
    scenario, seed, lo, hi, cfg, r = _run(q)
    return {
        "scenario": scenario,
        "seed": seed,
        "timeout": [lo, hi],
        "events": len(r.trace),
        "violations": [asdict(v) | {"nodes": list(v.nodes)} for v in r.violations],
        "metrics": compare.metrics(r.trace, cfg.n, RUN_UNTIL),
        "trace": [[e.seq, e.time, e.kind, e.node, e.data, timeline.details(e)] for e in r.trace],
        "timeline_url": "/timeline?" + urlencode({"scenario": scenario, "seed": seed, "lo": lo, "hi": hi}),
    }


def page_timeline(q):
    scenario, seed, lo, hi, _, r = _run(q)
    return timeline.html(r.trace, r.violations, f"{scenario} · seed {seed} · timeout {lo}-{hi}ms")


def api_compare(q):
    try:
        seeds = compare.parse_seeds(q.get("seeds", ["0..9"])[0])
    except ValueError:
        raise BadRequest("seeds must look like 0..19 or 1,2,3")
    if not seeds or len(seeds) > MAX_COMPARE_SEEDS:
        raise BadRequest(f"choose between 1 and {MAX_COMPARE_SEEDS} seeds")
    names = [s for s in q.get("scenarios", [""])[0].split(",") if s] or list(SCENARIOS)
    unknown = [s for s in names if s not in SCENARIOS]
    if unknown:
        raise BadRequest(f"unknown scenario {unknown[0]!r}")
    res = compare.compare(seeds, names)
    summary = compare.summarize(res)
    rows = [{"scenario": s, "strategy": name, "metrics": summary[(s, name)]}
            for s in res.scenarios for name in compare.STRATEGIES]
    return {"seeds": seeds, "rows": rows, "metrics": compare.METRICS,
            "metric_help": compare.METRIC_HELP}


def api_meta():
    cfg = Config()
    return {
        "scenarios": [{"name": n, "title": SCENARIO_INFO[n][0], "description": SCENARIO_INFO[n][1],
                       "tags": SCENARIO_INFO[n][2]} for n in SCENARIOS],
        "strategies": {k: list(v.election_timeout) for k, v in compare.STRATEGIES.items()},
        "invariants": INVARIANTS,
        "metrics": compare.METRICS,
        "metric_help": compare.METRIC_HELP,
        "cluster_size": cfg.n,
        "heartbeat": cfg.heartbeat,
        "default_delay": list(cfg.delay),
        "run_until": RUN_UNTIL,
        "fault_window": [FAULT_START, FAULT_END],
    }


def api_findings():
    try:
        return {"markdown": FINDINGS.read_text(encoding="utf-8")}
    except OSError:
        return {"markdown": "# Findings\n\nNo FINDINGS.md file found."}


def static_file(name):
    """Serve a file that exists directly inside WEB_DIR; nothing else."""
    allowed = {p.name: p for p in WEB_DIR.iterdir() if p.is_file()}
    path = allowed.get(name)
    if path is None or path.suffix not in STATIC_TYPES:
        return None
    return STATIC_TYPES[path.suffix], path.read_bytes()


def handle(path):
    """Route one GET request. Returns (status, content_type, body bytes)."""
    url = urlsplit(path)
    q = parse_qs(url.query)
    try:
        if url.path == "/":
            return 200, "text/html; charset=utf-8", (WEB_DIR / "index.html").read_bytes()
        if url.path.startswith("/static/"):
            found = static_file(url.path[len("/static/"):])
            if found:
                return 200, found[0], found[1]
        if url.path == "/api/meta":
            return 200, "application/json", json.dumps(api_meta()).encode("utf-8")
        if url.path == "/api/findings":
            return 200, "application/json", json.dumps(api_findings()).encode("utf-8")
        if url.path == "/timeline":
            return 200, "text/html; charset=utf-8", page_timeline(q).encode("utf-8")
        if url.path == "/api/run":
            return 200, "application/json", json.dumps(api_run(q)).encode("utf-8")
        if url.path == "/api/compare":
            return 200, "application/json", json.dumps(api_compare(q)).encode("utf-8")
    except BadRequest as e:
        return 400, "application/json", json.dumps({"error": str(e)}).encode("utf-8")
    return 404, "text/plain; charset=utf-8", b"not found"


class _Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        status, ctype, body = handle(self.path)
        self.send_response(status)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, fmt, *args):
        pass


def make_server(port=8000):
    # Threaded so an idle browser keep-alive connection cannot block other
    # requests. Each request builds its own Simulator, so nothing is shared.
    server = ThreadingHTTPServer(("127.0.0.1", port), _Handler)
    server.daemon_threads = True
    return server


def serve(port=8000, open_browser=True):
    server = make_server(port)
    url = f"http://127.0.0.1:{server.server_port}/"
    print(f"Raft Failure Lab running at {url}  (Ctrl+C to stop)")
    if open_browser:
        import webbrowser
        webbrowser.open(url)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        server.server_close()
