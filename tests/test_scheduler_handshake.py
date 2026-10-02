import unittest

from mpx4.codec import decode_handshake_message
from mpx4.constants import SchedulerID
from mpx4.endpoint import EndpointLimits, build_client_init, parse_client_init


SESSION_ID = bytes.fromhex("00112233445566778899aabbccddeeff")
NONCE = bytes(range(32))
LIMITS = EndpointLimits()


class SchedulerHandshakeTests(unittest.TestCase):
    def _parse(self, wire: bytes):
        message, end = decode_handshake_message(wire)
        self.assertEqual(end, len(wire))
        return parse_client_init(message.body)

    def test_auto_and_protect_are_valid_core_scheduler_ids(self):
        for scheduler in (SchedulerID.AUTO, SchedulerID.PROTECT):
            parsed = self._parse(
                build_client_init(
                    session_id=SESSION_ID,
                    client_nonce=NONCE,
                    limits=LIMITS,
                    scheduler=int(scheduler),
                )
            )
            self.assertEqual(parsed.scheduler, int(scheduler))
            self.assertIsNone(parsed.path_capacity)

    def test_weighted_requires_and_round_trips_path_capacity(self):
        parsed = self._parse(
            build_client_init(
                session_id=SESSION_ID,
                client_nonce=NONCE,
                limits=LIMITS,
                scheduler=int(SchedulerID.WEIGHTED),
                path_capacity=(1000, 500),
            )
        )
        self.assertEqual(parsed.scheduler, int(SchedulerID.WEIGHTED))
        self.assertEqual(parsed.path_capacity, (1000, 500))

    def test_weighted_without_capacity_is_rejected(self):
        with self.assertRaises(ValueError):
            build_client_init(
                session_id=SESSION_ID,
                client_nonce=NONCE,
                limits=LIMITS,
                scheduler=int(SchedulerID.WEIGHTED),
            )

    def test_non_weighted_capacity_is_rejected(self):
        with self.assertRaises(ValueError):
            build_client_init(
                session_id=SESSION_ID,
                client_nonce=NONCE,
                limits=LIMITS,
                scheduler=int(SchedulerID.AUTO),
                path_capacity=(100, 0),
            )


if __name__ == "__main__":
    unittest.main()
