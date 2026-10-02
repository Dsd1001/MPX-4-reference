import queue
import socket
import threading
import time
import unittest

from mpx4.session_engine_endpoint import (
    run_unified_client,
    run_unified_server,
)


KEY = bytes.fromhex(
    "a0a1a2a3a4a5a6a7a8a9aaabacadaeaf"
    "b0b1b2b3b4b5b6b7b8b9babbbcbdbebf"
)


class UnifiedSessionEndpointTests(unittest.TestCase):
    def test_real_two_carrier_unified_session_lifecycle(self):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as reserve:
            reserve.bind(("127.0.0.1", 0))
            host, port = reserve.getsockname()

        results = queue.Queue()

        def server():
            try:
                results.put(
                    (
                        "ok",
                        run_unified_server(
                            host,
                            port,
                            KEY,
                            expect1=b"alpha",
                            expect3=b"beta",
                            reply1=b"reply-alpha",
                            reply3=b"reply-beta",
                        ),
                    )
                )
            except BaseException as exc:
                results.put(("error", exc))

        thread = threading.Thread(target=server, daemon=True)
        thread.start()

        deadline = time.monotonic() + 5
        client = None
        last_error = None
        while time.monotonic() < deadline:
            try:
                client = run_unified_client(
                    host,
                    port,
                    KEY,
                    send1=b"alpha",
                    send3=b"beta",
                    expect_reply1=b"reply-alpha",
                    expect_reply3=b"reply-beta",
                )
                break
            except ConnectionRefusedError as exc:
                last_error = exc
                time.sleep(0.01)

        if client is None:
            raise last_error or RuntimeError("unified client did not connect")

        thread.join(timeout=5)
        self.assertFalse(thread.is_alive())
        status, value = results.get_nowait()
        if status == "error":
            raise value
        server_result = value

        self.assertEqual(client.session_id, server_result.session_id)
        self.assertEqual(client.stream_ids, (1, 3))
        self.assertEqual(server_result.stream_ids, (1, 3))

        self.assertEqual(client.received_stream1, b"reply-alpha")
        self.assertEqual(client.received_stream3, b"reply-beta")
        self.assertEqual(server_result.received_stream1, b"alpha")
        self.assertEqual(server_result.received_stream3, b"beta")

        self.assertEqual(client.stream1_deliveries, 1)
        self.assertEqual(client.stream3_deliveries, 1)
        self.assertEqual(server_result.stream1_deliveries, 1)
        self.assertEqual(server_result.stream3_deliveries, 1)

        self.assertGreater(client.reinjected_transmission_id, 0)
        self.assertEqual(
            server_result.reinjected_transmission_id,
            client.reinjected_transmission_id,
        )
        self.assertEqual(
            client.reinjection_attempts,
            ((2, 0), (1, 0)),
        )
        self.assertTrue(client.late_original_suppressed)
        self.assertTrue(server_result.late_original_suppressed)

        self.assertTrue(client.carrier2_lost)
        self.assertTrue(server_result.carrier2_lost)

        self.assertEqual(client.retired_stream_ids, (1, 3))
        self.assertEqual(server_result.server_tombstones, (1, 3))

        self.assertTrue(client.session_closed)
        self.assertTrue(server_result.session_closed)
        self.assertEqual(server_result.close_records_sent, 1)

        # One Session-wide monotonic namespace per endpoint:
        # Client: OPEN 1/2, DATA 3/4, FIN 5, CONSUMED 6, RESET 7.
        # Server: DATA 1/2, CONSUMED 3, FIN 4, STOP 5, RESET 6.
        self.assertEqual(client.max_transmission_id, 7)
        self.assertEqual(server_result.max_transmission_id, 6)


if __name__ == "__main__":
    unittest.main()
