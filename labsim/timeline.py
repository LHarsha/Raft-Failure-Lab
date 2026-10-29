"""Render a run's trace as a text timeline or a self-contained HTML page,
with invariant violations marked at the event that broke them."""

import html as _html
from collections import defaultdict

NET = {"send", "deliver", "drop", "duplicate"}
CATEGORIES = {
    "net": NET,
    "election": {"became_candidate", "became_leader", "became_follower", "vote"},
    "log": {"append", "truncate", "commit"},
    "client": {"client_accept", "client_reject"},
    "fault": {"crash", "restart", "partition", "heal", "net", "scenario"},
    "end": {"final"},
}
_CATEGORY_OF = {k: cat for cat, kinds in CATEGORIES.items() for k in kinds}


def category(kind):
    return _CATEGORY_OF.get(kind, "other")


def details(e):
    d = e.data
    if e.kind == "send":
        return f"->n{d['dst']} {d['msg']}  [m{d['id']}]"
    if e.kind == "deliver":
        return f"<-n{d['src']} {d['msg']}  [m{d['id']}]"
    if e.kind == "drop":
        peer = f"->n{d['dst']}" if "dst" in d else f"<-n{d['src']}"
        return f"{peer} {d['reason']}  [m{d['id']}]"
    if e.kind == "duplicate":
        return f"->n{d['dst']}  [m{d['id']}]"
    if e.kind == "became_leader":
        return f"term={d['term']} log_len={len(d['log'])}"
    if e.kind == "append":
        es = d["entries"]
        span = f"{d['from_index']}" if len(es) == 1 else f"{d['from_index']}..{d['from_index'] + len(es) - 1}"
        return f"[{span}] " + " ".join(f"t{t}:{c}" for t, c in es)
    if e.kind == "truncate":
        return f"from={d['from_index']} removed=" + " ".join(f"t{t}:{c}" for t, c in d["removed"])
    if e.kind == "commit":
        return f"[{d['index']}] t{d['term']}:{d['cmd']} (in term {d['commit_term']})"
    if e.kind == "partition":
        return " | ".join("{" + ",".join(f"n{i}" for i in g) + "}" for g in d["groups"])
    if e.kind == "final":
        return (f"alive={d['alive']} role={d['role']} term={d['term']} "
                f"commit={d['commit_index']} log_len={len(d['log'])}")
    return " ".join(f"{k}={v}" for k, v in d.items())


def _who(e):
    return "--" if e.node is None else f"n{e.node}"


def _visible(trace, violations, hide):
    marked = {v.seq for v in violations}
    return [e for e in trace if e.kind not in hide or e.seq in marked]


def text(trace, violations, context=None, hide=()):
    """One line per event. Each violation gets a banner right after the event
    that triggered it. With `context`, show only that many events before each
    violation."""
    by_seq = defaultdict(list)
    for v in violations:
        by_seq[v.seq].append(v)
    events = _visible(trace, violations, set(hide))
    if context is not None:
        keep = set()
        for i, e in enumerate(events):
            if e.seq in by_seq:
                keep.update(range(max(0, i - context), i + 1))
    lines = []
    skipped = False
    for i, e in enumerate(events):
        if context is not None and i not in keep:
            skipped = True
            continue
        if skipped:
            lines.append("    ...")
            skipped = False
        lines.append(f"{e.time:>7}ms  #{e.seq:<6} {_who(e):<4} {e.kind:<16} {details(e)}")
        for v in by_seq.get(e.seq, ()):
            nodes = ",".join(f"n{n}" for n in v.nodes)
            lines.append(f"!!! VIOLATION {v.invariant} [{nodes}] {v.detail}")
    if context is not None and not violations:
        lines.append("(no violations)")
    return "\n".join(lines) + "\n"


_CSS = """
:root{--bg:#fbfbfa;--fg:#1d1d1b;--muted:#6b6b66;--line:#e2e1dc;--follower:transparent;
--candidate:#fff3c4;--leader:#d7f0dc;--crashed:#e4e4e4;--bad:#c62828;--badbg:#fde4e4;
--g1:#3b6fd8;--g2:#d8843b;--g3:#8a3bd8;--g4:#3bd8b0}
@media (prefers-color-scheme:dark){:root{--bg:#171716;--fg:#e8e7e3;--muted:#9a9993;--line:#2e2e2b;
--candidate:#4a3f12;--leader:#173a20;--crashed:#333331;--bad:#ff6b6b;--badbg:#3d1717}}
body{background:var(--bg);color:var(--fg);font:13px/1.4 ui-monospace,Consolas,monospace;margin:16px}
h1{font:600 18px system-ui,sans-serif;margin:0 0 8px}
.muted{color:var(--muted)}
.controls{background:var(--bg);padding:8px 0;border-bottom:1px solid var(--line)}
.controls label{margin-right:12px;cursor:pointer}
.legend span{display:inline-block;padding:1px 8px;margin-right:6px;border:1px solid var(--line)}
.wrap{}
table{border-collapse:collapse;width:100%;min-width:900px}
th{position:sticky;top:0;z-index:2;background:var(--bg);text-align:left;border-bottom:1px solid var(--line);padding:4px}
tr{scroll-margin-top:40px}
td{padding:2px 6px;vertical-align:top;border-bottom:1px solid var(--line);white-space:nowrap}
td.lane{width:17%;white-space:normal;border-left:3px solid transparent}
.follower{background:var(--follower)}.candidate{background:var(--candidate)}
.leader{background:var(--leader)}.crashed{background:var(--crashed);color:var(--muted)}
.grp1{border-left-color:var(--g1)!important}.grp2{border-left-color:var(--g2)!important}
.grp3{border-left-color:var(--g3)!important}.grp4{border-left-color:var(--g4)!important}
tr.violation td{background:var(--badbg)!important}
tr.banner td{background:var(--bad);color:#fff;font-weight:600}
.violations li{color:var(--bad)}
.violations a{color:inherit}
b.kind{font-weight:600}
""" + "".join(f"body.hide-{c} tr.cat-{c}{{display:none}}\n" for c in CATEGORIES)

_JS = """
document.querySelectorAll('input[data-cat]').forEach(function(cb){
  cb.addEventListener('change',function(){document.body.classList.toggle('hide-'+cb.dataset.cat,!cb.checked);});
});
"""


def html(trace, violations, title="Raft run"):
    esc = _html.escape
    nodes = sorted({e.node for e in trace if e.node is not None})
    role = {n: "follower" for n in nodes}
    term = {n: 0 for n in nodes}
    group = {}
    by_seq = defaultdict(list)
    for i, v in enumerate(violations):
        by_seq[v.seq].append((i, v))

    out = ['<!doctype html><html lang="en"><head><meta charset="utf-8">',
           '<meta name="viewport" content="width=device-width,initial-scale=1">',
           f"<title>{esc(title)}</title><style>{_CSS}</style></head>",
           '<body class="hide-net">', f"<h1>{esc(title)}</h1>",
           f'<p class="muted">{len(trace)} events, {len(nodes)} nodes, '
           f"{len(violations)} violation(s)</p>"]
    if violations:
        out.append('<ol class="violations">')
        for i, v in enumerate(violations):
            out.append(f'<li id="v{i}"><a href="#e{v.seq}">t={v.time}ms #{v.seq} '
                       f"{esc(v.invariant)}</a>: {esc(v.detail)}</li>")
        out.append("</ol>")
    out.append('<div class="controls">Show: ')
    for cat in CATEGORIES:
        checked = "" if cat == "net" else " checked"
        out.append(f'<label><input type="checkbox" data-cat="{cat}"{checked}> {cat}</label>')
    out.append('<span class="legend"><span class="candidate">candidate</span>'
               '<span class="leader">leader</span><span class="crashed">crashed</span>'
               '<span class="muted">left bar = partition side</span></span></div>')
    out.append('<div class="wrap"><table><thead><tr><th>time</th><th>#</th>')
    out.extend(f'<th class="lane">n{n}</th>' for n in nodes)
    out.append("<th>cluster</th></tr></thead><tbody>")

    for e in trace:
        d = e.data
        if e.node is not None:
            if e.kind == "became_candidate":
                role[e.node] = "candidate"
            elif e.kind == "became_leader":
                role[e.node] = "leader"
            elif e.kind in ("became_follower", "restart"):
                role[e.node] = "follower"
            elif e.kind == "crash":
                role[e.node] = "crashed"
            if "term" in d and e.kind != "commit":
                term[e.node] = d["term"]
        if e.kind == "partition":
            group = {n: k + 1 for k, g in enumerate(d["groups"]) for n in g}
        elif e.kind == "heal":
            group = {}

        cls = f"cat-{category(e.kind)} k-{e.kind}"
        if e.seq in by_seq:
            cls += " violation"
        cells = [f'<tr id="e{e.seq}" class="{cls}"><td>{e.time}</td><td>{e.seq}</td>']
        body = f'<b class="kind">{esc(e.kind)}</b> {esc(details(e))}'
        for n in nodes:
            lane_cls = f"lane {role[n]}" + (f" grp{group[n]}" if n in group else "")
            content = body if e.node == n else ""
            cells.append(f'<td class="{lane_cls}" title="n{n} {role[n]} t{term[n]}">{content}</td>')
        cells.append(f"<td>{body if e.node is None else ''}</td></tr>")
        out.append("".join(cells))
        for i, v in by_seq.get(e.seq, ()):
            nodes_s = ",".join(f"n{n}" for n in v.nodes)
            out.append(f'<tr class="banner"><td colspan="{len(nodes) + 3}">!!! VIOLATION '
                       f"{esc(v.invariant)} [{nodes_s}] {esc(v.detail)}</td></tr>")

    out.append(f"</tbody></table></div><script>{_JS}</script></body></html>")
    return "\n".join(out)
