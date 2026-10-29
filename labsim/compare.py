"""Compare two recovery strategies (election timeout settings) on the same
scenarios and seeds."""

import html as _html
from dataclasses import dataclass, replace

from labsim.cluster import Config
from labsim.scenarios import RUN_UNTIL, SCENARIOS, run_scenario

STRATEGIES = {
    "A 150-300ms": Config(election_timeout=(150, 300)),
    "B 50-100ms": Config(election_timeout=(50, 100)),
}

METRICS = ["violations", "first_leader_ms", "leaderless_ms", "max_gap_ms", "elections",
           "max_term", "commits", "latency_ms", "lost", "rejects"]
METRIC_HELP = {
    "violations": "safety violations (sum over seeds)",
    "first_leader_ms": "time until the first leader",
    "leaderless_ms": "total time with no leader able to reach a majority",
    "max_gap_ms": "longest single leaderless gap",
    "elections": "elections started (became_candidate)",
    "max_term": "highest term reached",
    "commits": "distinct log indexes committed",
    "latency_ms": "mean client accept -> first commit",
    "lost": "accepted commands never committed",
    "rejects": "client submissions rejected (no leader at target)",
}


def metrics(trace, n, until):
    """Liveness/performance metrics for one run, from its trace alone."""
    alive = {i: True for i in range(n)}
    role = {i: "follower" for i in range(n)}
    term = {i: 0 for i in range(n)}
    groups = None
    majority = n // 2 + 1

    def has_leader():
        for i in range(n):
            if not (alive[i] and role[i] == "leader"):
                continue
            side = [j for j in range(n) if groups is None or groups.get(j) == groups.get(i)]
            side = [j for j in side if alive[j]]
            if len(side) >= majority and all(term[i] >= term[j] for j in side):
                return True
        return False

    leaderless = max_gap = gap = 0
    last_t = 0
    first_leader = None
    elections = 0
    accepted = {}
    committed_at = {}
    indexes = set()
    rejects = 0

    for e in trace:
        if e.time > last_t:
            if not has_leader():
                leaderless += e.time - last_t
                gap += e.time - last_t
                max_gap = max(max_gap, gap)
            last_t = e.time
        d = e.data
        if e.kind == "became_candidate":
            elections += 1
            role[e.node], term[e.node] = "candidate", d["term"]
        elif e.kind == "became_leader":
            role[e.node], term[e.node] = "leader", d["term"]
            if first_leader is None:
                first_leader = e.time
        elif e.kind == "became_follower":
            role[e.node], term[e.node] = "follower", d["term"]
        elif e.kind == "vote":
            term[e.node] = max(term[e.node], d["term"])
        elif e.kind == "crash":
            alive[e.node] = False
        elif e.kind == "restart":
            alive[e.node], role[e.node] = True, "follower"
        elif e.kind == "partition":
            groups = {j: k for k, g in enumerate(d["groups"]) for j in g}
        elif e.kind == "heal":
            groups = None
        elif e.kind == "client_accept":
            accepted.setdefault(d["cmd"], e.time)
        elif e.kind == "client_reject":
            rejects += 1
        elif e.kind == "commit":
            committed_at.setdefault(d["cmd"], e.time)
            indexes.add(d["index"])
        if has_leader():
            gap = 0

    if until > last_t and not has_leader():
        leaderless += until - last_t
        max_gap = max(max_gap, gap + until - last_t)

    lat = [committed_at[c] - t for c, t in accepted.items() if c in committed_at]
    return {
        "first_leader_ms": first_leader,
        "leaderless_ms": leaderless,
        "max_gap_ms": max_gap,
        "elections": elections,
        "max_term": max(term.values(), default=0),
        "commits": len(indexes),
        "latency_ms": sum(lat) / len(lat) if lat else None,
        "lost": sum(c not in committed_at for c in accepted),
        "rejects": rejects,
    }


@dataclass
class CompareResult:
    seeds: list
    scenarios: list
    runs: list   # one dict per (strategy, scenario, seed)


def compare(seeds, scenarios=None):
    scenarios = list(scenarios or SCENARIOS)
    runs = []
    for scenario in scenarios:
        for name, cfg in STRATEGIES.items():
            for seed in seeds:
                r = run_scenario(scenario, seed, replace(cfg))
                row = {"strategy": name, "scenario": scenario, "seed": seed,
                       "violations": len(r.violations)}
                row.update(metrics(r.trace, cfg.n, RUN_UNTIL))
                runs.append(row)
    return CompareResult(list(seeds), scenarios, runs)


def summarize(res):
    """(scenario, strategy) -> {metric: aggregate}. Violations are summed,
    everything else is the mean over seeds (ignoring runs where it is None)."""
    out = {}
    for scenario in res.scenarios:
        for name in STRATEGIES:
            rows = [r for r in res.runs if r["scenario"] == scenario and r["strategy"] == name]
            agg = {}
            for m in METRICS:
                vals = [r[m] for r in rows if r[m] is not None]
                if m == "violations":
                    agg[m] = sum(vals)
                else:
                    agg[m] = sum(vals) / len(vals) if vals else None
            out[(scenario, name)] = agg
    return out


def _fmt(v):
    if v is None:
        return "-"
    return f"{v:.0f}" if abs(v) >= 10 or v == int(v) else f"{v:.1f}"


def table(res):
    summary = summarize(res)
    head = ["scenario", "strategy"] + METRICS
    rows = [[s, name] + [_fmt(summary[(s, name)][m]) for m in METRICS]
            for s in res.scenarios for name in STRATEGIES]
    widths = [max(len(str(x)) for x in col) for col in zip(head, *rows)]

    def line(cells):
        return "  ".join(str(c).ljust(w) if i < 2 else str(c).rjust(w)
                         for i, (c, w) in enumerate(zip(cells, widths)))

    seeds = res.seeds
    span = f"{seeds[0]}..{seeds[-1]}" if seeds == list(range(seeds[0], seeds[-1] + 1)) else ",".join(map(str, seeds))
    out = [f"seeds {span} ({len(seeds)} per cell); violations summed, other columns are means", "",
           line(head), line(["-" * w for w in widths])]
    prev = None
    for r in rows:
        if prev is not None and r[0] != prev:
            out.append("")
        out.append(line(r))
        prev = r[0]
    out.append("")
    out.extend(f"  {m:<16} {METRIC_HELP[m]}" for m in METRICS)
    return "\n".join(out) + "\n"


def html_table(res):
    esc = _html.escape
    summary = summarize(res)
    out = ['<!doctype html><html lang="en"><head><meta charset="utf-8">',
           '<meta name="viewport" content="width=device-width,initial-scale=1">',
           "<title>Strategy comparison</title><style>",
           ":root{--bg:#fbfbfa;--fg:#1d1d1b;--line:#e2e1dc;--muted:#6b6b66;--better:#d7f0dc;--bad:#c62828}"
           "@media (prefers-color-scheme:dark){:root{--bg:#171716;--fg:#e8e7e3;--line:#2e2e2b;"
           "--muted:#9a9993;--better:#173a20;--bad:#ff6b6b}}"
           "body{background:var(--bg);color:var(--fg);font:13px/1.4 system-ui,sans-serif;margin:16px}"
           "table{border-collapse:collapse}td,th{padding:4px 10px;border-bottom:1px solid var(--line);"
           "text-align:right;font-variant-numeric:tabular-nums;white-space:nowrap}td:nth-child(-n+2),th:nth-child(-n+2){text-align:left}"
           ".bad{color:var(--bad);font-weight:600}.muted{color:var(--muted)}",
           "</style></head><body><h1>Strategy comparison</h1>",
           f'<p class="muted">{len(res.seeds)} seeds per cell. Violations summed; other columns are means.</p>',
           "<table><thead><tr><th>scenario</th><th>strategy</th>"]
    out.extend(f'<th title="{esc(METRIC_HELP[m])}">{esc(m)}</th>' for m in METRICS)
    out.append("</tr></thead><tbody>")
    for s in res.scenarios:
        for name in STRATEGIES:
            agg = summary[(s, name)]
            cells = [f"<td>{esc(s)}</td><td>{esc(name)}</td>"]
            for m in METRICS:
                cls = ' class="bad"' if m == "violations" and agg[m] else ""
                cells.append(f"<td{cls}>{_fmt(agg[m])}</td>")
            out.append("<tr>" + "".join(cells) + "</tr>")
    out.append("</tbody></table><ul class='muted'>")
    out.extend(f"<li><b>{esc(m)}</b>: {esc(METRIC_HELP[m])}</li>" for m in METRICS)
    out.append("</ul></body></html>")
    return "\n".join(out)


def parse_seeds(spec):
    if ".." in spec:
        lo, hi = spec.split("..")
        return list(range(int(lo), int(hi) + 1))
    return [int(s) for s in spec.split(",")]
