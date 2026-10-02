import queue
import socket
import threading
import unittest

from mpx4.replacement_endpoint import (
    client_replacement_exchange,
    server_replacement_exchange,
)


KEY = bytes.fromhex(
    "a0a1a2a3a4a5a6a7a8a9aaabacadaeaf"
    "b0b1b2b3b4b5b6b7b8b9babbbcbdbebf"
)
SESSION_ID = bytes.fromhex("102132435465768798a9bacbdcedfe0f")


class CarrierReplacementTests(unittest.TestCase):
    def test_loss_generation_one_join_and_outstanding_reinjection(self):
        payload = b"survive carrier loss without losing stream state"
        results = queue.Queue()

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(("127.0.0.1", 0))
            listener.listen(3)
            host, port = listener.getsockname()

            def server():
                try:
                    result = server_replacement_exchange(
                        listener,
                        KEY,
                        payload,
                    )
                    results.put(("ok", result))
                except BaseException as exc:
                    results.put(("error", exc))

            thread = threading.Thread(target=server, daemon=True)
            thread.start()

            client = client_replacement_exchange(
                host,
                port,
                KEY,
                payload,
                session_id=SESSION_ID,
            )

            thread.join(timeout=10)
            self.assertFalse(thread.is_alive())
            status, value = results.get_nowait()
            if status == "error":
                raise value
            server_result = value

        for result in (client, server_result):
            self.assertEqual(result.session_id, SESSION_ID)
            self.assertEqual(result.payload, payload)
            self.assertEqual(result.attempts, ((2, 0), (2, 1)))
            self.assertEqual(result.scheduler_initial_carrier, (2, 0))
            self.assertEqual(result.recovery_reason, "carrier-loss")
            self.assertTrue(result.failed_carrier_inactive)
            self.assertEqual(result.replacement_generation, 1)
            self.assertEqual(result.replacement_first_record_sequence, 0)
            self.assertTrue(result.fresh_replacement_keys)
            self.assertEqual(result.carrier_generations, ((1, 0), (2, 1)))
            self.assertEqual(result.application_deliveries, 1)
            self.assertEqual(result.session_committed_bytes, len(payload))
            self.assertTrue(result.surviving_carrier_active)

        self.assertEqual(client.transmission_id, server_result.transmission_id)


if __name__ == "__main__":
    unittest.main()
