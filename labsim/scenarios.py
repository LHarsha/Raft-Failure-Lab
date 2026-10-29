"""Named fault schedules. Each scenario injects faults between FAULT_START and
FAULT_END, then leaves a fault-free tail so the cluster can recover. Choices
that depend on the run (who is leader, which node to crash) are made at fault
time from the simulator RNG, so they stay deterministic per seed."""

from dataclasses import dataclass, replace

from labsim.checker import check
from labsim.cluster import Cluster, Config

FAULT_START = 800
FAULT_END = 2800
CLIENT_UNTIL = 3000
RUN_UNTIL = 4000


def _leader_or_random(c):
    leaders = c.leaders()
    if leaders:
        return max(leaders, key=lambda n: n.current_term).id
    return c.sim.rng.randrange(len(c.nodes))


def baseline(c):
    pass


def lossy(c):
    c.at(FAULT_START, c.set_net, 0.1, 0.1)
    c.at(FAULT_END, c.set_net, 0.0, 0.0)


def slow(c):
    normal = c.config.delay
    c.at(FAULT_START, c.set_net, None, None, (1, 120))
    c.at(FAULT_END, c.set_net, None, None, normal)


def leader_crash(c):
    def crash_leader():
        victim = _leader_or_random(c)
        c.crash(victim)
        c.at(c.sim.now + 1000, c.restart, victim)
    c.at(FAULT_START, crash_leader)


def partition_minority_leader(c):
    def split():
        leader = _leader_or_random(c)
        others = [n.id for n in c.nodes if n.id != leader]
        buddy = c.sim.rng.choice(others)
        minority = {leader, buddy}
        c.partition([minority, set(others) - {buddy}])
    c.at(FAULT_START, split)
    c.at(FAULT_START + 1400, c.heal)


def flapping_partition(c):
    def random_split():
        ids = [n.id for n in c.nodes]
        c.sim.rng.shuffle(ids)
        cut = c.sim.rng.randint(1, len(ids) - 1)
        c.partition([set(ids[:cut]), set(ids[cut:])])
    t = FAULT_START
    while t < FAULT_END:
        c.at(t, random_split)
        c.at(t + 150, c.heal)
        t += 300


def crash_restart_storm(c):
    max_down = (len(c.nodes) - 1) // 2

    def churn():
        down = [n.id for n in c.nodes if not n.alive]
        up = [n.id for n in c.nodes if n.alive]
        if down and (len(down) >= max_down or c.sim.rng.random() < 0.5):
            c.restart(c.sim.rng.choice(down))
        else:
            c.crash(c.sim.rng.choice(up))

    for t in range(FAULT_START, FAULT_END, 200):
        c.at(t, churn)
    c.at(FAULT_END, lambda: [c.restart(n.id) for n in c.nodes])


def figure8(c):
    """Raft paper Figure 8, scripted. An entry from an old term (f8-old) is
    replicated to a majority by a later-term leader that then crashes before
    committing anything of its own term; a node holding a conflicting entry
    from an intermediate term (f8-mid) then wins and overwrites it. A correct
    leader never counts replicas of f8-old (§5.4.2), so nothing is lost.

    Each step waits for the cluster to reach the state it needs, so which node
    plays which role depends on the seed. The regular client is paused; the
    script submits f8-old / f8-mid itself."""
    nodes = c.nodes
    deadline = FAULT_END
    roles = {}

    def step(name):
        c.sim.record("scenario", None, step=name)

    def when(cond, then):
        def poll():
            if cond():
                then()
            elif c.sim.now >= deadline:
                step("aborted")
                finish()
            else:
                c.sim.schedule(1, poll)
        poll()

    def ids(*names):
        return {roles[n] for n in names}

    def has(nid, index, cmd):
        log = nodes[nid].log
        return len(log) >= index and log[index - 1].command == cmd

    def leader_in(group, min_term):
        for i in sorted(group):
            n = nodes[i]
            if n.alive and n.role == "leader" and n.current_term > min_term:
                return i
        return None

    # (a) leader A gets f8-old onto itself and B only
    def a():
        c.client_paused = True
        roles["A"] = _leader_or_random(c)
        rest = [n.id for n in nodes if n.id != roles["A"]]
        roles["B"] = c.sim.rng.choice(rest)
        roles["CDE"] = set(rest) - {roles["B"]}
        step(f"a: n{roles['A']} (leader) and n{roles['B']} cut off; f8-old goes to them only")
        c.partition([ids("A", "B"), roles["CDE"]])
        nodes[roles["A"]].client_submit("f8-old")
        roles["index"] = len(nodes[roles["A"]].log)
        roles["old_term"] = nodes[roles["A"]].current_term
        when(lambda: has(roles["B"], roles["index"], "f8-old"), b_wait)

    # (b) majority side elects E; E alone takes f8-mid at the same index, then crashes
    def b_wait():
        when(lambda: leader_in(roles["CDE"], roles["old_term"]) is not None, b)

    def b():
        roles["E"] = leader_in(roles["CDE"], roles["old_term"])
        roles["C"], roles["D"] = sorted(roles["CDE"] - {roles["E"]})
        e = nodes[roles["E"]]
        c.partition([ids("A", "B"), ids("C", "D"), {roles["E"]}])
        e.client_submit("f8-mid")
        if len(e.log) != roles["index"]:
            step("aborted: f8-mid landed at a different index")
            return finish()
        roles["mid_term"] = e.current_term
        step(f"b: n{roles['E']} leader of term {e.current_term} takes f8-mid at index "
             f"{roles['index']}, then crashes")
        c.crash(roles["E"])
        c.partition([ids("A", "B", "C"), {roles["D"]}])
        when(lambda: leader_in(ids("A", "B"), roles["mid_term"]) is not None, c_step)

    # (c) A or B wins a later term and copies f8-old to C: now on a majority
    def c_step():
        roles["L"] = leader_in(ids("A", "B"), roles["mid_term"])
        step(f"c: n{roles['L']} leader of term {nodes[roles['L']].current_term}, "
             f"replicating f8-old to n{roles['C']}")
        when(lambda: has(roles["C"], roles["index"], "f8-old"),
             lambda: c.sim.schedule(30, c_crash))

    def c_crash():
        step(f"c: f8-old on a majority; n{roles['L']} crashes before committing a term-"
             f"{nodes[roles['L']].current_term} entry")
        c.crash(roles["L"])
        c.restart(roles["E"])
        other = (ids("A", "B") - {roles["L"]}).pop()
        c.partition([ids("C", "D", "E"), {other}])
        when(lambda: leader_in(ids("C", "D", "E"), roles["mid_term"]) == roles["E"]
             and has(roles["C"], roles["index"], "f8-mid"), d)

    # (d) E (last log term = mid_term) wins with C and D, overwriting f8-old on C
    def d():
        step(f"d: n{roles['E']} won term {nodes[roles['E']].current_term} and overwrote "
             f"index {roles['index']} on n{roles['C']}")
        step("complete")
        finish()

    def finish():
        c.heal()
        for n in nodes:
            c.restart(n.id)
        c.client_paused = False

    c.at(FAULT_START, a)


SCENARIOS = {
    "baseline": baseline,
    "lossy": lossy,
    "slow": slow,
    "leader_crash": leader_crash,
    "partition_minority_leader": partition_minority_leader,
    "flapping_partition": flapping_partition,
    "crash_restart_storm": crash_restart_storm,
    "figure8": figure8,
}


@dataclass
class RunResult:
    scenario: str
    seed: int
    cluster: Cluster
    trace: list
    violations: list


def run_scenario(name, seed, config=None, until=RUN_UNTIL):
    config = config or Config()
    config = replace(config, client_interval=config.client_interval or 50, client_until=CLIENT_UNTIL)
    c = Cluster(seed, config)
    SCENARIOS[name](c)
    c.start()
    c.run(until)
    c.finish()
    return RunResult(name, seed, c, c.sim.trace, check(c.sim.trace))
