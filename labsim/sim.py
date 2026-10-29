"""Discrete-event simulator: a virtual clock, an event queue, one seeded RNG
and a trace log. Single-threaded; every run is a pure function of its seed."""

import heapq
import random
from dataclasses import dataclass, field


@dataclass
class TraceEvent:
    seq: int
    time: int
    kind: str
    node: object = None
    data: dict = field(default_factory=dict)


class Simulator:
    def __init__(self, seed):
        self.seed = seed
        self.rng = random.Random(seed)
        self.now = 0
        self.trace = []
        self._queue = []          # (time, handle, callback, args)
        self._next_handle = 0
        self._cancelled = set()

    def schedule(self, delay, callback, *args):
        """Run callback(*args) `delay` time units from now. Returns a handle."""
        if delay < 0:
            raise ValueError(f"negative delay {delay}")
        handle = self._next_handle
        self._next_handle += 1
        heapq.heappush(self._queue, (self.now + delay, handle, callback, args))
        return handle

    def cancel(self, handle):
        if handle is not None:
            self._cancelled.add(handle)

    def run(self, until=None):
        """Process events in (time, schedule order). With `until`, stop before
        any event later than it and advance the clock to `until`."""
        while self._queue:
            time, handle, callback, args = self._queue[0]
            if until is not None and time > until:
                break
            heapq.heappop(self._queue)
            if handle in self._cancelled:
                self._cancelled.discard(handle)
                continue
            self.now = time
            callback(*args)
        if until is not None and until > self.now:
            self.now = until

    def record(self, kind, node=None, **data):
        event = TraceEvent(len(self.trace), self.now, kind, node, data)
        self.trace.append(event)
        return event
