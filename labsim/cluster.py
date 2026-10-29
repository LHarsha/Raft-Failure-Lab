"""Wires N Raft nodes to one simulator and network."""

from dataclasses import dataclass

from labsim.network import Network
from labsim.raft import LEADER, RaftNode
from labsim.sim import Simulator


@dataclass
class Config:
    n: int = 5
    election_timeout: tuple = (150, 300)
    heartbeat: int = 50
    delay: tuple = (1, 10)
    max_batch: int = 64
    client_interval: int = None   # ms between client submissions; None = no client
    client_until: int = None      # client stops submitting at this time


class Cluster:
    def __init__(self, seed, config=None):
        self.config = config or Config()
        self.sim = Simulator(seed)
        self.net = Network(self.sim, self.config.delay)
        ids = range(self.config.n)
        self.nodes = [RaftNode(i, [j for j in ids if j != i], self.sim, self.net, self.config)
                      for i in ids]
        for node in self.nodes:
            self.net.register(node)
        self._client_target = 0
        self._next_cmd = 1
        self.client_paused = False

    def start(self):
        for node in self.nodes:
            node.start()
        if self.config.client_interval:
            self.sim.schedule(self.config.client_interval, self._client_tick)

    def _client_tick(self):
        """Submit the next command to the node we think is leader; on rejection
        follow the node's leader hint, or pick a random node."""
        until = self.config.client_until
        if until is not None and self.sim.now >= until:
            return
        if self.client_paused:
            self.sim.schedule(self.config.client_interval, self._client_tick)
            return
        node = self.nodes[self._client_target]
        cmd = f"c{self._next_cmd}"
        if node.client_submit(cmd):
            self._next_cmd += 1
        else:
            self.sim.record("client_reject", node.id, cmd=cmd)
            hint = node.leader_id if node.alive else None
            if hint is None or hint == node.id:
                hint = self.sim.rng.randrange(len(self.nodes))
            self._client_target = hint
        self.sim.schedule(self.config.client_interval, self._client_tick)

    def run(self, until):
        self.sim.run(until)

    # ---- fault injection -------------------------------------------------

    def at(self, time, fn, *args):
        """Run fn(*args) at absolute simulated time."""
        self.sim.schedule(max(0, time - self.sim.now), fn, *args)

    def crash(self, nid):
        if self.nodes[nid].alive:
            self.nodes[nid].crash()

    def restart(self, nid):
        if not self.nodes[nid].alive:
            self.nodes[nid].restart()

    def partition(self, groups):
        self.net.partition = [set(g) for g in groups]
        self.sim.record("partition", None, groups=tuple(tuple(sorted(g)) for g in groups))

    def heal(self):
        self.net.partition = None
        self.sim.record("heal", None)

    def set_net(self, drop=None, dup=None, delay=None):
        if drop is not None:
            self.net.drop_prob = drop
        if dup is not None:
            self.net.dup_prob = dup
        if delay is not None:
            self.net.delay = delay
        self.sim.record("net", None, drop=self.net.drop_prob, dup=self.net.dup_prob,
                        delay=self.net.delay)

    def finish(self):
        """Record each node's end state for the checker."""
        for n in self.nodes:
            self.sim.record("final", n.id, alive=n.alive, role=n.role, term=n.current_term,
                            commit_index=n.commit_index,
                            log=tuple((e.term, e.command) for e in n.log))

    def leaders(self):
        return [n for n in self.nodes if n.alive and n.role == LEADER]
