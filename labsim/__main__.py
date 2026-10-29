"""CLI.

  python -m labsim run --scenario leader_crash --seed 7 [--html out.html] [--text out.txt]
  python -m labsim compare --seeds 0..49 [--html compare.html]
  python -m labsim ui [--port 8000] [--no-browser]
  python -m labsim build-site [--out _site]
"""

import argparse
import sys

from labsim import compare, timeline
from labsim.cluster import Config
from labsim.scenarios import SCENARIOS, run_scenario


def cmd_run(args):
    config = Config(election_timeout=tuple(args.election_timeout))
    r = run_scenario(args.scenario, args.seed, config)
    print(f"scenario={r.scenario} seed={r.seed} events={len(r.trace)} violations: {len(r.violations)}")
    for v in r.violations:
        print(f"  t={v.time}ms #{v.seq} {v.invariant} {v.detail}")
    hide = () if args.show_net else timeline.NET
    if args.text:
        with open(args.text, "w", encoding="utf-8") as f:
            f.write(timeline.text(r.trace, r.violations, args.context, hide))
    if args.html:
        with open(args.html, "w", encoding="utf-8") as f:
            f.write(timeline.html(r.trace, r.violations, f"{r.scenario} · seed {r.seed}"))
    if not args.quiet and not args.text:
        sys.stdout.write(timeline.text(r.trace, r.violations, args.context, hide))
    return 0


def cmd_compare(args):
    scenarios = args.scenarios.split(",") if args.scenarios else None
    res = compare.compare(compare.parse_seeds(args.seeds), scenarios)
    sys.stdout.write(compare.table(res))
    if args.html:
        with open(args.html, "w", encoding="utf-8") as f:
            f.write(compare.html_table(res))
    return 0


def cmd_ui(args):
    from labsim import ui
    ui.serve(args.port, open_browser=not args.no_browser)
    return 0


def cmd_build_site(args):
    from labsim.build_site import build
    print(f"static site written to {build(args.out)}")
    return 0


def main(argv=None):
    p = argparse.ArgumentParser(prog="labsim")
    sub = p.add_subparsers(dest="cmd", required=True)

    run = sub.add_parser("run", help="run one scenario and show its timeline")
    run.add_argument("--scenario", choices=sorted(SCENARIOS), default="baseline")
    run.add_argument("--seed", type=int, default=0)
    run.add_argument("--election-timeout", type=int, nargs=2, default=[150, 300], metavar=("LO", "HI"))
    run.add_argument("--html", help="write HTML timeline to this path")
    run.add_argument("--text", help="write text timeline to this path instead of stdout")
    run.add_argument("--context", type=int, help="only show N events before each violation")
    run.add_argument("--show-net", action="store_true", help="include send/deliver/drop events")
    run.add_argument("--quiet", action="store_true", help="print only the summary")
    run.set_defaults(fn=cmd_run)

    cmp = sub.add_parser("compare", help="compare the two election-timeout strategies")
    cmp.add_argument("--seeds", default="0..49", help="e.g. 0..49 or 1,5,9")
    cmp.add_argument("--scenarios", help="comma-separated subset (default: all)")
    cmp.add_argument("--html", help="also write the table as HTML")
    cmp.set_defaults(fn=cmd_compare)

    web = sub.add_parser("ui", help="open the web interface in your browser")
    web.add_argument("--port", type=int, default=8000)
    web.add_argument("--no-browser", action="store_true", help="don't open a browser tab")
    web.set_defaults(fn=cmd_ui)

    site = sub.add_parser("build-site", help="build the static GitHub Pages site")
    site.add_argument("--out", default="_site")
    site.set_defaults(fn=cmd_build_site)

    args = p.parse_args(argv)
    return args.fn(args)


if __name__ == "__main__":
    sys.exit(main())
