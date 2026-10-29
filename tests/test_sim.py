import unittest

from labsim.sim import Simulator


class SimulatorTest(unittest.TestCase):
    def test_events_fire_in_time_order(self):
        sim = Simulator(seed=1)
        fired = []
        sim.schedule(30, fired.append, "c")
        sim.schedule(10, fired.append, "a")
        sim.schedule(20, fired.append, "b")
        sim.run()
        self.assertEqual(fired, ["a", "b", "c"])
        self.assertEqual(sim.now, 30)

    def test_equal_times_are_fifo(self):
        sim = Simulator(seed=1)
        fired = []
        for name in "xyz":
            sim.schedule(5, fired.append, name)
        sim.run()
        self.assertEqual(fired, ["x", "y", "z"])

    def test_callbacks_can_schedule_more_events(self):
        sim = Simulator(seed=1)
        times = []

        def tick():
            times.append(sim.now)
            if len(times) < 3:
                sim.schedule(10, tick)

        sim.schedule(0, tick)
        sim.run()
        self.assertEqual(times, [0, 10, 20])

    def test_cancelled_event_does_not_fire(self):
        sim = Simulator(seed=1)
        fired = []
        h = sim.schedule(10, fired.append, "cancelled")
        sim.schedule(20, fired.append, "kept")
        sim.cancel(h)
        sim.run()
        self.assertEqual(fired, ["kept"])

    def test_cancel_none_is_noop(self):
        sim = Simulator(seed=1)
        sim.cancel(None)

    def test_negative_delay_rejected(self):
        sim = Simulator(seed=1)
        with self.assertRaises(ValueError):
            sim.schedule(-1, lambda: None)

    def test_run_until_stops_on_time(self):
        sim = Simulator(seed=1)
        fired = []
        sim.schedule(10, fired.append, 10)
        sim.schedule(50, fired.append, 50)
        sim.run(until=20)
        self.assertEqual(fired, [10])
        self.assertEqual(sim.now, 20)
        sim.run(until=100)
        self.assertEqual(fired, [10, 50])
        self.assertEqual(sim.now, 100)

    def test_event_at_until_boundary_fires(self):
        sim = Simulator(seed=1)
        fired = []
        sim.schedule(20, fired.append, "edge")
        sim.run(until=20)
        self.assertEqual(fired, ["edge"])

    def test_record_stamps_time_and_seq(self):
        sim = Simulator(seed=1)
        sim.schedule(7, lambda: sim.record("ping", 2, x=1))
        sim.schedule(7, lambda: sim.record("pong", 3))
        sim.run()
        a, b = sim.trace
        self.assertEqual((a.seq, a.time, a.kind, a.node, a.data), (0, 7, "ping", 2, {"x": 1}))
        self.assertEqual((b.seq, b.time, b.kind, b.node), (1, 7, "pong", 3))

    def _random_run(self, seed):
        sim = Simulator(seed=seed)

        def step(i):
            sim.record("step", i % 3, r=sim.rng.random())
            if i < 20:
                sim.schedule(sim.rng.randint(1, 10), step, i + 1)

        sim.schedule(0, step, 0)
        sim.run()
        return [(e.seq, e.time, e.kind, e.node, e.data) for e in sim.trace]

    def test_same_seed_identical_trace(self):
        self.assertEqual(self._random_run(42), self._random_run(42))

    def test_different_seed_different_trace(self):
        self.assertNotEqual(self._random_run(42), self._random_run(43))


if __name__ == "__main__":
    unittest.main()
