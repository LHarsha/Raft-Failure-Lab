# Findings

Log of bugs and invariant violations found while building and running the lab.
Each entry: date, milestone, seed/scenario, symptom, root cause, fix (or "expected under this fault").

## 2026-10-29 · M2 · test bug: step-down assertion
- **Symptom:** `test_leader_steps_down_on_higher_term` failed: last trace event of the old leader was `send`, not `became_follower`.
- **Root cause:** the test, not Raft. After stepping down on a higher-term RequestVote the node correctly goes on to grant the vote and sends a VoteReply, so "last event" was the wrong thing to assert.
- **Fix:** assert `became_follower` appears among the node's events after the injected message.

## 2026-10-29 · M3 · test fixture accidentally violated Leader Completeness
- **Symptom:** `test_violations_sorted_and_reported_once` expected `[election_safety, log_divergence]` but the checker also reported `committed_lost`.
- **Root cause:** the hand-built trace had term-3 leaders with empty logs, plus an entry committed in term 1. That is a real Leader Completeness violation, so the checker was right and the fixture was wrong.
- **Fix:** the fixture's commits now happen in term 3. Takeaway: completeness must be judged by *term order*, not time order. The checker compares each leader against entries committed in strictly lower terms, whichever event came first in the trace.

## 2026-10-29 · M5 · all scenarios safe; checker validated against two deliberate bugs
- **Result:** 7 scenarios × 20 seeds, 0 safety violations with the correct implementation (`test_every_scenario_safe_over_20_seeds`).
- **Checker sanity check (throwaway monkeypatch, not in the codebase):** with the §5.4.1 vote up-to-date check removed, `flapping_partition` gave 411 `committed_lost` + 57 `log_divergence` violations over 20 seeds. Other scenarios stayed clean. So the checker does catch real Raft bugs in real traces, and flapping partitions are the scenario that exposes stale-log leaders.
- **Coverage gap:** with the §5.4.2 commit rule replaced by naive majority counting (the "Figure 8" bug), **0 violations over 7 scenarios × 100 seeds**. The bug needs a narrow schedule: an old-term entry reaches a majority, its leader crashes before committing a current-term entry, and a node without it wins. Steady client traffic makes new leaders append current-term entries almost immediately, which hides the window. None of the current scenarios is adversarial enough. A targeted Figure-8 scenario would close this gap. Not added: it's outside the agreed list.

## 2026-10-29 · M5/M7 · lost client writes under a minority-partitioned leader (expected)
- **Scenario:** `partition_minority_leader`, all seeds. On average ~26–29 accepted commands per run are never committed.
- **Root cause:** expected Raft behavior, not a bug. The old leader, cut off in the minority, keeps *accepting* client commands: it is still leader in its own term and has no way to know it's isolated. Those entries can never reach a majority, and they get truncated once the partition heals and the old leader learns the new term. The demo client doesn't wait for commit and doesn't retry, so these writes are simply lost. A real client would time out and resubmit. Safety holds (0 violations): nothing *committed* is lost.

## 2026-10-29 · M7 · strategy B (50–100 ms timeout) churns because timeout ≈ heartbeat
- **Data (`python -m labsim compare --seeds 0..49`):** B elects its first leader faster (68 vs 185 ms) and fails over faster after a crash (leaderless 183 vs 371 ms in `leader_crash`). But in `baseline`, with no faults at all, it starts 5.2 elections per run and reaches term 4.5, versus 1.2 and 1.0 for A.
- **Root cause:** the heartbeat interval is 50 ms and network delay is 1–10 ms, so a follower whose timeout draw is near 50 ms can fire before the next heartbeat arrives. Raft needs `broadcastTime ≪ electionTimeout`; B violates that.
- **Worst case, `slow` (delay 1–120 ms):** B spends 2053 ms of a 4000 ms run leaderless (A: 185), runs 103 elections, and commits only 17 entries vs 55. Every message delay above the timeout triggers a new election, which disrupts the current leader.
- Safety is unaffected by either setting: 0 violations in all 700 runs.

## 2026-10-29 · M5 follow-up · `figure8` scenario closes the §5.4.2 coverage gap
- **Added:** `figure8`, a scripted replay of Raft paper Figure 8. It's adaptive: each step waits for the state it needs, so which node plays which role depends on the seed. The regular client is paused while the script runs, and the script submits `f8-old` and `f8-mid` itself.
  - (a) Leader A and node B are cut off. `f8-old` reaches only those two.
  - (b) The majority side elects E. E is isolated, takes `f8-mid` at the same index, and crashes.
  - (c) A or B wins a later term, copies `f8-old` to C (it's now on a majority), and crashes 30 ms later.
  - (d) E restarts and wins with C and D, then overwrites `f8-old` on C.
- **Correct implementation:** the script reaches `complete` on 50/50 seeds under both timeout strategies with **0 violations**. `f8-old` is never committed, and `f8-mid` takes the slot.
- **Naive commit rule (patched in only by `test_figure8_catches_naive_commit_rule`):** caught on **50/50 seeds under both strategies**. Seed 0 shows the textbook trace. n2 (term 4) commits `[13] t1:f8-old` at 1243 ms and crashes. n3 wins term 6 with last log term 2 (> 1), and the checker reports `committed_lost` twice: once at that election (the new leader lacks a committed entry) and once when n0 truncates index 13.
- **Takeaway:** random fault injection, even across 700 runs, never hit this window. The adversarial schedule finds it every time. Random chaos and targeted schedules complement each other; neither is enough alone.

## 2026-10-29 · UI · web page hung on "Running…"
- **Symptom:** in the new web interface, pressing Run left the button on "Running…". The `/api/run` request stayed pending, while the same handler returned in ~30 ms in the unit tests.
- **Root cause:** `http.server.HTTPServer` serves one connection at a time. The browser held an idle keep-alive or pre-opened connection, so the server sat blocked reading it and never accepted the real request. Unit tests call `ui.handle()` directly and never go over a socket, so they couldn't catch this.
- **Fix:** use `ThreadingHTTPServer` (daemon threads). That's safe because every request builds its own `Simulator`; the simulation stays single-threaded and deterministic. The threads exist only in the web server.

## 2026-10-29 · UI · two layout bugs found by clicking through the page
- **Compare tab didn't hide the Run panel.** `.grid{display:grid}` overrode the browser's default `[hidden]{display:none}`. Fixed with `[hidden]{display:none!important}`.
- **Timeline column header didn't stay pinned.** The table's `overflow-x:auto` wrapper made `position:sticky` stick inside the wrapper instead of the page, and the sticky filter bar overlapped rows when it wrapped onto two lines. Now only the column header is sticky, and wide timelines scroll the page sideways.

## 2026-10-29 · UI v2 · interactive replay app; issues found while building it
- **What changed:** the web interface is now a single-page app (`labsim/web/`). It replays the full event trace of a run: an animated cluster with in-flight messages, a scrubbable per-node timeline, a replicated-log grid that rings conflicting entries in red, live invariant cards, and a filterable event feed. Compare now shows bar charts per measure, and FINDINGS.md is readable in-app. The server only adds `/api/meta`, `/api/findings`, the trace in `/api/run`, and static files from `labsim/web/` (anything else, including `..` paths, returns 404).
- **Bug:** a fallback string in `ui.api_findings()` got a literal newline while the file was being rewritten, so `ui.py` failed to import. The test suite caught it immediately (`test_ui` import error).
- **Verification of the failure view:** a correct Raft never produces violations, so the red state was checked against a throwaway server (scratchpad only) with the naive §5.4.2 commit rule patched in. For `figure8` seed 0 the UI showed `committed_lost` (5) and `log_divergence` (1). "Jump to violation" landed at 1566 ms, with the banner directly under n3's term-6 `became_leader`, which matches the CLI checker output.

