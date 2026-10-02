import unittest

from mpx4.carrier import (
    CarrierIdentity,
    ClientSessionState,
    ServerSessionState,
)
from mpx4.codec import (
    decode_frames,
    decode_reset_stream,
    decode_stream_consumed,
    decode_stream_open,
    encode_reset_stream,
    encode_session_credit,
    encode_stop_sending,
    encode_stream_credit,
    encode_stream_fin,
    encode_stream_open,
    encode_stream_open_ok,
    encode_transmission_ack,
)
from mpx4.constants import ErrorCode, FrameType, SchedulerID
from mpx4.endpoint import EndpointLimits
from mpx4.errors import CarrierLostError, SessionConflictError
from mpx4.session_engine import ReferenceSessionEngine


class FakeCarrier:
    def __init__(self, carrier_id: int, generation: int = 0):
        self.identity = CarrierIdentity(carrier_id, generation)
        self.active = True
        self.close_kind = None
        self.close_error_code = None
        self.close_trigger_frame_type = 0
        self.close_reason = ""
        self.sent = []

    @property
    def gracefully_closed(self):
        return self.close_kind is not None

    def send(self, *frames: bytes):
        if not self.active:
            raise CarrierLostError("inactive")
        self.sent.append(b"".join(frames))

    def mark_lost(self):
        self.active = False

    def mark_graceful_close(
        self,
        kind: str,
        *,
        error_code: int,
        trigger_frame_type: int,
        reason: str,
    ):
        self.active = False
        self.close_kind = kind
        self.close_error_code = error_code
        self.close_trigger_frame_type = trigger_frame_type
        self.close_reason = reason


def limits():
    return EndpointLimits()


def client_state():
    return ClientSessionState(
        session_id=bytes.fromhex("00112233445566778899aabbccddeeff"),
        client_limits=limits(),
        server_limits=limits(),
        scheduler=int(SchedulerID.AGGREGATE),
        generations={1: 0, 2: 0},
    )


def server_state():
    return ServerSessionState(
        session_id=bytes.fromhex("00112233445566778899aabbccddeeff"),
        client_limits=limits(),
        server_limits=limits(),
        scheduler=int(SchedulerID.AGGREGATE),
        generations={1: 0, 2: 0},
    )


def frames(wire: bytes):
    return decode_frames(wire)


class UnifiedClientEngineTests(unittest.TestCase):
    def _engine(self):
        engine = ReferenceSessionEngine.client(
            client_state(),
            retransmit_after_s=10.0,
        )
        c1 = FakeCarrier(1)
        c2 = FakeCarrier(2)
        engine.register_carrier(c1)
        engine.register_carrier(c2)
        engine.handle_frames(
            c1,
            frames(encode_session_credit(0, 1024 * 1024)),
        )
        return engine, c1, c2

    def _accept(self, engine, carrier, stream_id, open_txid):
        engine.handle_frames(
            carrier,
            frames(
                encode_stream_open_ok(stream_id, open_txid)
                + encode_stream_credit(stream_id, 0, 65536)
            ),
        )

    def test_two_streams_share_ledger_and_loss_reinjects_without_new_credit(self):
        engine, c1, c2 = self._engine()

        s1, open1 = engine.open_stream()
        engine.set_carrier_role((1, 0), "disabled")
        s3, open3 = engine.open_stream()
        engine.set_carrier_role((1, 0), "active")
        self.assertEqual((s1, s3), (1, 3))
        self.assertEqual(
            sorted([open1.transmission_id, open3.transmission_id]),
            [1, 2],
        )

        carrier_for = {
            (1, 0): c1,
            (2, 0): c2,
        }
        self._accept(
            engine,
            carrier_for[(open1.carrier_id, open1.generation)],
            s1,
            open1.transmission_id,
        )
        self._accept(
            engine,
            carrier_for[(open3.carrier_id, open3.generation)],
            s3,
            open3.transmission_id,
        )

        engine.set_carrier_role((2, 0), "disabled")
        a1 = engine.send_data(s1, b"alpha")
        engine.set_carrier_role((2, 0), "active")
        engine.set_carrier_role((1, 0), "disabled")
        a3 = engine.send_data(s3, b"beta")
        engine.set_carrier_role((1, 0), "active")
        self.assertNotEqual(
            (a1.carrier_id, a1.generation),
            (a3.carrier_id, a3.generation),
        )
        self.assertEqual(engine.send_flow.session_committed, 9)

        failed = (a3.carrier_id, a3.generation)
        surviving = (a1.carrier_id, a1.generation)
        engine.mark_carrier_lost(failed)
        retries = engine.poll_reliability()
        retry = next(
            attempt
            for attempt in retries
            if attempt.transmission_id == a3.transmission_id
        )
        self.assertEqual(retry.reason, "carrier-loss")
        self.assertEqual(
            (retry.carrier_id, retry.generation),
            surviving,
        )
        self.assertEqual(engine.send_flow.session_committed, 9)

        ack_carrier = carrier_for[surviving]
        engine.handle_frames(
            ack_carrier,
            frames(
                encode_transmission_ack(
                    s1,
                    a1.transmission_id,
                    0,
                )
            ),
        )
        engine.handle_frames(
            ack_carrier,
            frames(
                encode_transmission_ack(
                    s3,
                    a3.transmission_id,
                    0,
                )
            ),
        )
        self.assertNotIn(a1.transmission_id, engine.ledger.pending)
        self.assertNotIn(a3.transmission_id, engine.ledger.pending)

    def test_fin_consumed_and_stop_reset_can_retire_different_streams(self):
        engine, c1, c2 = self._engine()

        s1, o1 = engine.open_stream()
        s3, o3 = engine.open_stream()
        carriers = {(1, 0): c1, (2, 0): c2}
        self._accept(
            engine,
            carriers[(o1.carrier_id, o1.generation)],
            s1,
            o1.transmission_id,
        )
        self._accept(
            engine,
            carriers[(o3.carrier_id, o3.generation)],
            s3,
            o3.transmission_id,
        )

        # Give the peer reverse-direction credit state by opening local receive
        # state as part of OPEN_OK; no DATA is required for the terminal paths.
        fin_attempt = engine.send_fin(s1)
        fin_carrier = carriers[(fin_attempt.carrier_id, fin_attempt.generation)]
        engine.handle_frames(
            fin_carrier,
            frames(
                encode_transmission_ack(
                    s1,
                    fin_attempt.transmission_id,
                    0,
                )
            ),
        )

        # Peer sends FIN(0), then Client releases it with STREAM_CONSUMED.
        peer_fin_txid = 700
        engine.handle_frames(
            fin_carrier,
            frames(encode_stream_fin(s1, peer_fin_txid, 0)),
        )
        consumed_attempt = engine.release_receive(s1)
        self.assertIsNotNone(consumed_attempt)
        assert consumed_attempt is not None
        engine.handle_frames(
            carriers[
                (
                    consumed_attempt.carrier_id,
                    consumed_attempt.generation,
                )
            ],
            frames(
                encode_transmission_ack(
                    s1,
                    consumed_attempt.transmission_id,
                    0,
                )
            ),
        )
        self.assertIn(s1, engine.retired_streams)

        # On Stream 3, STOP_SENDING creates a reliable RESET(0).
        stop_txid = 701
        engine.handle_frames(
            c1,
            frames(
                encode_stop_sending(
                    s3,
                    stop_txid,
                    int(ErrorCode.NO_ERROR),
                )
            ),
        )
        active = engine.client_streams[s3]
        reset_txid = active.local_terminal_transmission_id
        self.assertIsNotNone(reset_txid)
        assert reset_txid is not None

        reset_pending = engine.ledger.pending[reset_txid]
        self.assertEqual(reset_pending.kind, "reset")
        reset_frame = decode_reset_stream(
            decode_frames(reset_pending.wire_frame or b"")[0]
        )
        self.assertEqual(reset_frame.final_offset, 0)

        reset_attempt = reset_pending.attempts[-1]
        engine.handle_frames(
            carriers[reset_attempt],
            frames(
                encode_transmission_ack(
                    s3,
                    reset_txid,
                    0,
                )
            ),
        )

        engine.handle_frames(
            c1,
            frames(
                encode_reset_stream(
                    s3,
                    702,
                    0,
                    int(ErrorCode.NO_ERROR),
                )
            ),
        )
        self.assertIn(s3, engine.retired_streams)
        self.assertEqual(engine.client_registry.active, set())

    def test_session_close_blocks_future_open(self):
        engine, c1, c2 = self._engine()
        close_wire = __import__(
            "mpx4.codec",
            fromlist=["encode_close"],
        ).encode_close(
            FrameType.SESSION_CLOSE,
            int(ErrorCode.NO_ERROR),
            0,
            "done",
        )
        events = engine.handle_frames(c1, frames(close_wire))
        self.assertTrue(events.session_closed)
        self.assertEqual(engine.state.lifecycle, "closed")
        self.assertFalse(c1.active)
        self.assertFalse(c2.active)
        with self.assertRaises(SessionConflictError):
            engine.open_stream()


class UnifiedServerEngineTests(unittest.TestCase):
    def test_server_open_data_and_reset_use_one_state_owner(self):
        engine = ReferenceSessionEngine.server(server_state())
        c1 = FakeCarrier(1)
        c2 = FakeCarrier(2)
        engine.register_carrier(c1)
        engine.register_carrier(c2)

        engine.handle_frames(
            c1,
            frames(encode_session_credit(0, 1024 * 1024)),
        )
        events = engine.handle_frames(
            c1,
            frames(encode_stream_open(1, 10)),
        )
        self.assertEqual(events.opened, (1,))
        self.assertIn(1, engine.server_terminal.active)

        engine.handle_frames(
            c1,
            frames(encode_stream_credit(1, 0, 65536)),
        )

        data_wire = __import__(
            "mpx4.codec",
            fromlist=["encode_stream_data"],
        ).encode_stream_data(1, 0, 11, b"hello")
        events = engine.handle_frames(c2, frames(data_wire))
        self.assertEqual(events.deliveries, ((1, b"hello"),))
        self.assertEqual(engine.receive_flow.session_committed, 5)

        engine.handle_frames(
            c1,
            frames(
                encode_reset_stream(
                    1,
                    12,
                    5,
                    int(ErrorCode.NO_ERROR),
                )
            ),
        )
        state = engine.server_terminal.active[1]
        self.assertEqual(state.receive.terminal_kind, "reset")
        self.assertTrue(state.peer_accounting_released)


if __name__ == "__main__":
    unittest.main()
