import queue
import socket
import threading
import unittest

from mpx4.carrier import ServerSessionState
from mpx4.constants import SchedulerID, SessionAction
from mpx4.endpoint import ClientInitParameters, EndpointLimits
from mpx4.errors import CarrierConflictError, SessionConflictError
from mpx4.multipath_endpoint import (
    client_reinjection_exchange,
    server_reinjection_exchange,
)


KEY = bytes.fromhex(
    "a0a1a2a3a4a5a6a7a8a9aaabacadaeaf"
    "b0b1b2b3b4b5b6b7b8b9babbbcbdbebf"
)
SESSION_ID = bytes.fromhex("00112233445566778899aabbccddeeff")


def join_init(
    *,
    carrier_id: int,
    generation: int,
    limits: EndpointLimits = EndpointLimits(),
    session_id: bytes = SESSION_ID,
) -> ClientInitParameters:
    return ClientInitParameters(
        session_id=session_id,
        action=int(SessionAction.JOIN),
        carrier_id=carrier_id,
        generation=generation,
        client_nonce=bytes(range(32)),
        limits=limits,
        scheduler=int(SchedulerID.AGGREGATE),
    )


class CarrierGenerationTests(unittest.TestCase):
    def setUp(self):
        self.state = ServerSessionState(
            session_id=SESSION_ID,
            client_limits=EndpointLimits(),
            server_limits=EndpointLimits(),
            scheduler=int(SchedulerID.AGGREGATE),
            generations={1: 0},
        )

    def test_new_carrier_id_starts_at_generation_zero(self):
        self.state.validate_join(join_init(carrier_id=2, generation=0))
        with self.assertRaises(CarrierConflictError):
            self.state.validate_join(join_init(carrier_id=3, generation=1))

    def test_equal_generation_conflicts(self):
        with self.assertRaises(CarrierConflictError):
            self.state.validate_join(join_init(carrier_id=1, generation=0))

    def test_higher_generation_supersedes_and_old_becomes_stale(self):
        replacement = join_init(carrier_id=1, generation=1)
        self.state.validate_join(replacement)
        self.state.commit_join(replacement)
        self.assertEqual(self.state.generations[1], 1)
        with self.assertRaises(CarrierConflictError):
            self.state.validate_join(join_init(carrier_id=1, generation=0))

    def test_session_scoped_limit_change_rejected(self):
        changed = EndpointLimits(
            max_frame_payload=16384,
            max_record_size=65536,
            max_streams=32,
        )
        with self.assertRaises(SessionConflictError):
            self.state.validate_join(
                join_init(carrier_id=2, generation=0, limits=changed)
            )

    def test_carrier_scoped_record_size_may_change(self):
        changed = EndpointLimits(
            max_frame_payload=32768,
            max_record_size=32768,
            max_streams=32,
        )
        self.state.validate_join(
            join_init(carrier_id=2, generation=0, limits=changed)
        )


class TwoCarrierReinjectionTests(unittest.TestCase):
    def test_join_fresh_keys_independent_sequences_and_duplicate_suppression(self):
        payload = b"one Transmission, two Carrier Attempts"
        results = queue.Queue()

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(("127.0.0.1", 0))
            listener.listen(2)
            host, port = listener.getsockname()

            create_client = socket.create_connection((host, port), timeout=5)
            join_client = socket.create_connection((host, port), timeout=5)

            def server():
                create_server = None
                join_server = None
                try:
                    create_server, _ = listener.accept()
                    join_server, _ = listener.accept()
                    result = server_reinjection_exchange(
                        create_server,
                        join_server,
                        KEY,
                        expected_payload=payload,
                        create_nonce=bytes(range(32, 64)),
                        join_nonce=bytes(range(64, 96)),
                    )
                    results.put(("ok", result))
                except BaseException as exc:
                    results.put(("error", exc))
                finally:
                    if join_server is not None:
                        join_server.close()
                    if create_server is not None:
                        create_server.close()

            thread = threading.Thread(target=server, daemon=True)
            thread.start()

            try:
                client = client_reinjection_exchange(
                    create_client,
                    join_client,
                    KEY,
                    payload,
                    session_id=SESSION_ID,
                    create_nonce=bytes(range(32)),
                    join_nonce=bytes(range(96, 128)),
                )
            finally:
                join_client.close()
                create_client.close()

            thread.join(timeout=5)
            self.assertFalse(thread.is_alive())
            status, value = results.get_nowait()
            if status == "error":
                raise value
            server_result = value

        self.assertEqual(client.session_id, SESSION_ID)
        self.assertEqual(server_result.session_id, SESSION_ID)
        self.assertEqual(client.carrier_generations, ((1, 0), (2, 0)))
        self.assertEqual(server_result.carrier_generations, ((1, 0), (2, 0)))
        self.assertTrue(client.fresh_carrier_keys)
        self.assertTrue(server_result.fresh_carrier_keys)
        self.assertEqual(client.carrier2_first_record_sequence, 0)
        self.assertEqual(server_result.carrier2_first_record_sequence, 0)

        self.assertEqual(client.transmission_id, server_result.transmission_id)
        self.assertEqual(server_result.payload, payload)
        self.assertEqual(server_result.application_deliveries, 1)
        self.assertEqual(server_result.session_committed_bytes, len(payload))
        self.assertEqual(client.session_committed_bytes, len(payload))


if __name__ == "__main__":
    unittest.main()
