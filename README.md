# Raft Failure Lab

A miniature distributed system where you inject network delays, node crashes, duplicated
messages and partitions, and see exactly how a Raft cluster fails, or doesn't.

- **Deterministic.** A single-process discrete-event simulator with one seeded RNG: no sockets,
  no threads, no wall clock. Every run is exactly repeatable from its seed.
- **Raft.** Leader election and log replication, 5 nodes. No membership changes or snapshots.
- **Faults.** Message delay, drop, duplication, node crash/restart, network partition.
- **Invariant checker.** Runs after every run: one leader per term, committed entries never
  lost, logs never diverge on committed entries. Each violation points at the exact event.
- **Timelines.** Text, HTML, and an interactive web app that replays the run.
- **Strategy comparison.** Two election-timeout settings, same scenarios, same seeds.

Python 3.10+, standard library only. Nothing to install.

## Quick start

```bash
python -m labsim ui
```

This opens the web app at http://127.0.0.1:8000. On Windows you can double-click `Start Lab.bat`.

Command line:

```bash
python -m labsim run --scenario figure8 --seed 0 --html out/figure8.html
python -m labsim compare --seeds 0..49 --html out/compare.html
python -m unittest discover -s tests -v
```

## Live demo

**https://raft-failure-lab.harshalolewar002.workers.dev**: no install needed. There is no backend; the `labsim`
package runs in your browser through [Pyodide](https://pyodide.org) (Python on WebAssembly)
inside a Web Worker. It answers the same API as the local server via `ui.handle()`, so a seed
gives exactly the same run online and locally. The first load takes 10–15 seconds while Python downloads.

Every push to `main` runs the test suite in GitHub Actions and, if it passes, redeploys the
site to Cloudflare (`.github/workflows/deploy.yml`). That needs a repository secret named
`CLOUDFLARE_API_TOKEN`, created from Cloudflare's "Edit Cloudflare Workers" token template.

Deploy your own copy manually (static, any host):

```bash
python -m labsim build-site --out _site
npx wrangler deploy             # Cloudflare Workers static assets, see wrangler.toml
```

## Scenarios

| Scenario | Fault |
|---|---|
| `baseline` | none |
| `lossy` | 10% drop, 10% duplicate |
| `slow` | delay 1–120 ms |
| `leader_crash` | crash the leader, restart after 1 s |
| `partition_minority_leader` | leader + one follower cut off |
| `flapping_partition` | random split every 300 ms |
| `crash_restart_storm` | random crashes/restarts, ≤ 2 down |
| `figure8` | scripted replay of Raft paper Figure 8 |

## Layout

```
labsim/
  sim.py        event loop, clock, seeded RNG, trace
  network.py    delay / drop / duplicate / partition
  raft.py       Raft node: election + replication
  cluster.py    wiring, client workload, fault controls
  checker.py    safety invariants over the trace
  scenarios.py  named fault schedules
  timeline.py   text + HTML timeline
  compare.py    two strategies, same seeds, metrics
  ui.py         local web server + JSON API
  web/          single-page app (vanilla JS, no dependencies)
tests/          unittest suite, written test-first per component
FINDINGS.md     every bug and violation found along the way
```

## Highlights from FINDINGS.md

- 0 safety violations for the correct implementation across all scenarios and seeds.
- With the vote up-to-date check removed, `flapping_partition` loses committed entries.
- With the §5.4.2 commit rule broken, 700 random fault runs found nothing. The scripted
  `figure8` scenario catches it on every seed.
- A 50–100 ms election timeout fails over faster, but churns because it sits too close to the
  50 ms heartbeat. Under a slow network it is leaderless for about half the run.
