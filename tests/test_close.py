import unittest

from mpx4.carrier import CarrierIdentity, ClientSessionState
from mpx4.close import SessionCloseController
from mpx4.codec import decode_close, decode_frames, encode_close
from mpx4.constants import ErrorCode, FrameType, SchedulerID
from mpx4.endpoint import EndpointLimits
from mpx4.errors import CarrierLostError, SessionConflictError


class DummyCarrier:
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


def state():
    limits = EndpointLimits()
    return ClientSessionState(
        session_id=bytes.fromhex("00112233445566778899aabbccddeeff"),
        client_limits=limits,
        server_limits=limits,
        scheduler=int(SchedulerID.AGGREGATE),
        generations={1: 0, 2: 0},
    )


def one_frame(wire: bytes):
    frames = decode_frames(wire)
    if len(frames) != 1:
        raise AssertionError("expected one Frame")
    return frames[0]


class CloseCodecTests(unittest.TestCase):
    def test_close_round_trip_utf8_reason(self):
        wire = encode_close(
            FrameType.CARRIER_CLOSE,
            int(ErrorCode.NO_ERROR),
            0,
            "graceful 关闭",
        )
        frame = one_frame(wire)
        close = decode_close(frame)
        self.assertEqual(close.error_code, int(ErrorCode.NO_ERROR))
        self.assertEqual(close.trigger_frame_type, 0)
        self.assertEqual(close.reason, "graceful 关闭")

    def test_close_reason_maximum_is_256_utf8_octets(self):
        encode_close(FrameType.SESSION_CLOSE, 0, 0, "x" * 256)
        with self.assertRaises(ValueError):
            encode_close(FrameType.SESSION_CLOSE, 0, 0, "x" * 257)


class CloseStateTests(unittest.TestCase):
    def test_carrier_close_only_closes_target_carrier(self):
        session = state()
        c1 = DummyCarrier(1)
        c2 = DummyCarrier(2)
        controller = SessionCloseController(session)
        controller.add_carrier(c1)
        controller.add_carrier(c2)

        close = controller.send_carrier_close(
            c2,
            reason="retire path",
        )
        self.assertEqual(close.reason, "retire path")
        self.assertTrue(c1.active)
        self.assertFalse(c2.active)
        self.assertTrue(c2.gracefully_closed)
        self.assertEqual(c2.close_kind, "carrier")
        self.assertEqual(session.lifecycle, "active")

        frame = one_frame(c2.sent[0])
        self.assertEqual(frame.type, int(FrameType.CARRIER_CLOSE))
        self.assertEqual(decode_close(frame).reason, "retire path")

    def test_session_close_closes_all_carriers_and_blocks_new_work(self):
        session = state()
        c1 = DummyCarrier(1)
        c2 = DummyCarrier(2)
        controller = SessionCloseController(session)
        controller.add_carrier(c1)
        controller.add_carrier(c2)

        close, sent = controller.send_session_close(
            error_code=int(ErrorCode.NO_ERROR),
            reason="shutdown",
        )
        self.assertEqual(close.reason, "shutdown")
        self.assertEqual(sent, 2)
        self.assertEqual(session.lifecycle, "closed")
        self.assertFalse(c1.active)
        self.assertFalse(c2.active)
        self.assertEqual(c1.close_kind, "session")
        self.assertEqual(c2.close_kind, "session")

        with self.assertRaises(SessionConflictError):
            controller.ensure_new_stream_allowed()
        with self.assertRaises(SessionConflictError):
            session.validate_local_join(3, 0)

        # Repeating local SESSION_CLOSE is idempotent and emits no new Record.
        _, repeated = controller.send_session_close(reason="shutdown")
        self.assertEqual(repeated, 0)

    def test_received_duplicate_session_close_is_idempotent(self):
        session = state()
        c1 = DummyCarrier(1)
        c2 = DummyCarrier(2)
        controller = SessionCloseController(session)
        controller.add_carrier(c1)
        controller.add_carrier(c2)

        frame = one_frame(
            encode_close(
                FrameType.SESSION_CLOSE,
                int(ErrorCode.NO_ERROR),
                0,
                "peer shutdown",
            )
        )
        first = controller.receive_session_close(c1, frame)
        second = controller.receive_session_close(c1, frame)

        self.assertEqual(first, second)
        self.assertEqual(session.lifecycle, "closed")
        self.assertFalse(c1.active)
        self.assertFalse(c2.active)

    def test_received_carrier_close_keeps_session_active(self):
        session = state()
        c1 = DummyCarrier(1)
        c2 = DummyCarrier(2)
        controller = SessionCloseController(session)
        controller.add_carrier(c1)
        controller.add_carrier(c2)

        frame = one_frame(
            encode_close(
                FrameType.CARRIER_CLOSE,
                int(ErrorCode.NO_ERROR),
                0,
                "path maintenance",
            )
        )
        controller.receive_carrier_close(c2, frame)

        self.assertEqual(session.lifecycle, "active")
        self.assertTrue(c1.active)
        self.assertFalse(c2.active)
        controller.ensure_new_stream_allowed()


if __name__ == "__main__":
    unittest.main()
