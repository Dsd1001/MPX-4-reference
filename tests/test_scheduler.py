import unittest

from mpx4.carrier import CarrierIdentity
from mpx4.reliability import ReliabilityLoop
from mpx4.scheduler import (
    AggregateScheduler,
    AutoScheduler,
    ProtectScheduler,
    WeightedScheduler,
)
from mpx4.stream import TransmissionLedger


class FakeClock:
    def __init__(self):
        self.now = 0.0

    def __call__(self):
        return self.now

    def advance(self, seconds: float):
        self.now += seconds


class DummyCarrier:
    def __init__(self, carrier_id: int, generation: int = 0):
        self.identity = CarrierIdentity(carrier_id, generation)
        self.active = True
        self.sent = []

    def mark_lost(self):
        self.active = False

    def send(self, *frames: bytes):
        if not self.active:
            from mpx4.errors import CarrierLostError

            raise CarrierLostError("dummy Carrier inactive")
        self.sent.append(b"".join(frames))


class AggregateSchedulerTests(unittest.TestCase):
    def test_outstanding_queue_pressure_changes_selection(self):
        c1 = DummyCarrier(1)
        c2 = DummyCarrier(2)
        scheduler = AggregateScheduler()
        m1 = scheduler.register(
            c1,
            latest_rtt_s=0.010,
            delivery_rate_bps=1_000.0,
        )
        scheduler.register(
            c2,
            latest_rtt_s=0.020,
            delivery_rate_bps=1_000.0,
        )

        self.assertEqual(scheduler.choose(100).identity, (1, 0))
        m1.outstanding_bytes = 1_000
        self.assertEqual(scheduler.choose(100).identity, (2, 0))

    def test_timeout_reinjects_to_unused_carrier_and_multi_attempt_ack_is_ambiguous(self):
        clock = FakeClock()
        c1 = DummyCarrier(1)
        c2 = DummyCarrier(2)
        scheduler = AggregateScheduler()
        m1 = scheduler.register(c1, latest_rtt_s=0.010, delivery_rate_bps=10_000.0)
        m2 = scheduler.register(c2, latest_rtt_s=0.020, delivery_rate_bps=10_000.0)
        initial_rates = (m1.delivery_rate_bps, m2.delivery_rate_bps)

        ledger = TransmissionLedger()
        txid = ledger.allocate(1, "data")
        wire = b"reliable-frame"
        ledger.bind_frame(txid, wire)

        loop = ReliabilityLoop(
            ledger,
            scheduler,
            retransmit_after_s=0.200,
            clock=clock,
        )
        first = loop.transmit_new(txid)
        self.assertEqual((first.carrier_id, first.generation), (1, 0))
        self.assertEqual(c1.sent, [wire])

        clock.advance(0.199)
        self.assertEqual(loop.poll(), ())

        clock.advance(0.002)
        retried = loop.poll()
        self.assertEqual(len(retried), 1)
        self.assertEqual(retried[0].reason, "timeout")
        self.assertEqual(
            (retried[0].carrier_id, retried[0].generation),
            (2, 0),
        )
        self.assertEqual(c2.sent, [wire])
        self.assertEqual(ledger.pending[txid].attempts, [(1, 0), (2, 0)])

        clock.advance(0.050)
        loop.acknowledge(1, txid, ack_carrier=(2, 0))
        self.assertNotIn(txid, ledger.pending)
        self.assertIn(txid, ledger.settled)
        self.assertEqual(m1.outstanding_bytes, 0)
        self.assertEqual(m2.outstanding_bytes, 0)
        self.assertEqual(
            (m1.delivery_rate_bps, m2.delivery_rate_bps),
            initial_rates,
        )

    def test_carrier_loss_reinjects_immediately_without_waiting_for_timer(self):
        clock = FakeClock()
        c1 = DummyCarrier(1)
        c2 = DummyCarrier(2)
        scheduler = AggregateScheduler()
        scheduler.register(c1, latest_rtt_s=0.010)
        scheduler.register(c2, latest_rtt_s=0.020)

        ledger = TransmissionLedger()
        txid = ledger.allocate(1, "data")
        wire = b"survive-loss"
        ledger.bind_frame(txid, wire)

        loop = ReliabilityLoop(
            ledger,
            scheduler,
            retransmit_after_s=10.0,
            clock=clock,
        )
        first = loop.transmit_new(txid)
        self.assertEqual(first.carrier_id, 1)

        scheduler.mark_failure((1, 0))
        retried = loop.poll()
        self.assertEqual(len(retried), 1)
        self.assertEqual(retried[0].reason, "carrier-loss")
        self.assertEqual(retried[0].carrier_id, 2)
        self.assertEqual(ledger.pending[txid].attempts, [(1, 0), (2, 0)])

    def test_single_attempt_same_carrier_ack_updates_rtt_and_rate(self):
        clock = FakeClock()
        c1 = DummyCarrier(1)
        scheduler = AggregateScheduler()
        metrics = scheduler.register(
            c1,
            latest_rtt_s=0.010,
            delivery_rate_bps=100_000.0,
        )

        ledger = TransmissionLedger()
        txid = ledger.allocate(1, "data")
        ledger.bind_frame(txid, b"x" * 1000)

        loop = ReliabilityLoop(
            ledger,
            scheduler,
            retransmit_after_s=1.0,
            clock=clock,
        )
        loop.transmit_new(txid)
        clock.advance(0.050)
        loop.acknowledge(1, txid, ack_carrier=(1, 0))

        self.assertAlmostEqual(metrics.latest_rtt_s, 0.050, places=6)
        self.assertLess(metrics.min_rtt_s, metrics.latest_rtt_s)
        self.assertNotEqual(metrics.delivery_rate_bps, 100_000.0)
        self.assertEqual(metrics.outstanding_bytes, 0)


class ProtectAndAutoSchedulerTests(unittest.TestCase):
    def test_protect_keeps_new_traffic_on_primary_but_allows_backup_retry(self):
        c1 = DummyCarrier(1)
        c2 = DummyCarrier(2)
        scheduler = ProtectScheduler()
        scheduler.register(c1, latest_rtt_s=0.030, role="primary")
        scheduler.register(c2, latest_rtt_s=0.005, role="backup")

        self.assertEqual(scheduler.choose(100).identity, (1, 0))
        self.assertEqual(
            scheduler.choose(100, previous_attempts=((1, 0),)).identity,
            (2, 0),
        )

    def test_auto_switches_to_protect_when_rtt_diverges(self):
        c1 = DummyCarrier(1)
        c2 = DummyCarrier(2)
        scheduler = AutoScheduler(rtt_ratio_threshold=2.0)
        scheduler.register(c1, latest_rtt_s=0.010)
        scheduler.register(c2, latest_rtt_s=0.012)
        self.assertEqual(scheduler.refresh_mode(), "aggregate")

        scheduler.observe_rtt((2, 0), 0.030)
        self.assertEqual(scheduler.refresh_mode(), "protect")
        self.assertEqual(scheduler.choose(100).identity, (1, 0))

    def test_auto_switches_to_protect_after_failure_penalty(self):
        c1 = DummyCarrier(1)
        c2 = DummyCarrier(2)
        scheduler = AutoScheduler()
        scheduler.register(c1, latest_rtt_s=0.010)
        scheduler.register(c2, latest_rtt_s=0.011)
        self.assertEqual(scheduler.refresh_mode(), "aggregate")
        scheduler.metrics_for((2, 0)).failures = 1
        self.assertEqual(scheduler.refresh_mode(), "protect")

    def test_weighted_capacity_clamps_effective_rate(self):
        c1 = DummyCarrier(1)
        c2 = DummyCarrier(2)
        scheduler = WeightedScheduler()
        scheduler.register(
            c1,
            latest_rtt_s=0.010,
            delivery_rate_bps=10_000_000.0,
            configured_capacity_bps=100_000.0,
        )
        scheduler.register(
            c2,
            latest_rtt_s=0.010,
            delivery_rate_bps=10_000_000.0,
            configured_capacity_bps=1_000_000.0,
        )
        self.assertEqual(scheduler.choose(10_000).identity, (2, 0))


if __name__ == "__main__":
    unittest.main()
