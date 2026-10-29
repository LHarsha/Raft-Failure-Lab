"""Simulated network. Every message gets a random delay from the simulator RNG.
Fault knobs (drop, duplicate, partition, per-link delay) default to off."""


def describe(msg):
    return msg.describe() if hasattr(msg, "describe") else repr(msg)


class Network:
    def __init__(self, sim, delay=(1, 10)):
        self.sim = sim
        self.delay = delay
        self.link_delay = {}      # (src, dst) -> (lo, hi) override
        self.drop_prob = 0.0
        self.dup_prob = 0.0
        self.partition = None     # None, or list of sets of node ids
        self.nodes = {}
        self._next_id = 0

    def register(self, node):
        self.nodes[node.id] = node

    def reachable(self, src, dst):
        if self.partition is None:
            return True
        return any(src in group and dst in group for group in self.partition)

    def send(self, src, dst, msg):
        if not self.nodes[src].alive:
            return
        mid = self._next_id
        self._next_id += 1
        self.sim.record("send", src, dst=dst, id=mid, msg=describe(msg))
        if not self.reachable(src, dst):
            self.sim.record("drop", src, dst=dst, id=mid, reason="partition")
            return
        if self.drop_prob and self.sim.rng.random() < self.drop_prob:
            self.sim.record("drop", src, dst=dst, id=mid, reason="random")
            return
        copies = 1
        if self.dup_prob and self.sim.rng.random() < self.dup_prob:
            copies = 2
            self.sim.record("duplicate", src, dst=dst, id=mid)
        lo, hi = self.link_delay.get((src, dst), self.delay)
        for _ in range(copies):
            self.sim.schedule(self.sim.rng.randint(lo, hi), self._deliver, src, dst, mid, msg)

    def _deliver(self, src, dst, mid, msg):
        node = self.nodes[dst]
        if not node.alive:
            self.sim.record("drop", dst, src=src, id=mid, reason="crashed")
            return
        if not self.reachable(src, dst):
            self.sim.record("drop", dst, src=src, id=mid, reason="partition")
            return
        self.sim.record("deliver", dst, src=src, id=mid, msg=describe(msg))
        node.receive(src, msg)
