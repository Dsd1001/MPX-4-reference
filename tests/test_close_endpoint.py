import queue
import socket
import threading
import time
import unittest

from mpx4.close_endpoint import run_close_client, run_close_server


KEY = bytes.fromhex(
    "a0a1a2a3a4a5a6a7a8a9aaabacadaeaf"
    "b0b1b2b3b4b5b6b7b8b9babbbcbdbebf"
)


class RealCloseBehaviorTests(unittest.TestCase):
    def _run_mode(self, mode: str):
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as reserve:
            reserve.bind(("127.0.0.1", 0))
            host, port = reserve.getsockname()

        results = queue.Queue()

        def server():
            try:
                results.put(
                    (
                        "ok",
                        run_close_server(
                            host,
                            port,
                            KEY,
                            mode=mode,
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
                client = run_close_client(
                    host,
                    port,
                    KEY,
                    mode=mode,
                )
                break
            except ConnectionRefusedError as exc:
                last_error = exc
                time.sleep(0.01)

        if client is None:
            raise last_error or RuntimeError("close client did not connect")

        thread.join(timeout=5)
        self.assertFalse(thread.is_alive())
        status, value = results.get_nowait()
        if status == "error":
            raise value
        return client, value

    def test_carrier_close_is_graceful_and_session_survives(self):
        client, server = self._run_mode("carrier")
        for result in (client, server):
            self.assertEqual(result.session_lifecycle, "active")
            self.assertTrue(result.carrier1_active)
            self.assertFalse(result.carrier2_active)
            self.assertTrue(result.carrier2_graceful)
            self.assertEqual(result.carrier2_close_kind, "carrier")
            self.assertTrue(result.surviving_ping)
            self.assertFalse(result.transport_loss_detected)
            self.assertFalse(result.new_stream_blocked)
            self.assertFalse(result.new_join_blocked)
        self.assertEqual(server.close_records_sent, 1)

    def test_bare_tcp_eof_is_carrier_loss_not_graceful_close(self):
        client, server = self._run_mode("bare-eof")
        for result in (client, server):
            self.assertEqual(result.session_lifecycle, "active")
            self.assertTrue(result.carrier1_active)
            self.assertFalse(result.carrier2_active)
            self.assertFalse(result.carrier2_graceful)
            self.assertIsNone(result.carrier2_close_kind)
            self.assertTrue(result.surviving_ping)
            self.assertTrue(result.transport_loss_detected)

    def test_tcp_half_close_is_not_mpx_close(self):
        client, server = self._run_mode("half-close")
        for result in (client, server):
            self.assertEqual(result.session_lifecycle, "active")
            self.assertTrue(result.carrier1_active)
            self.assertFalse(result.carrier2_active)
            self.assertFalse(result.carrier2_graceful)
            self.assertIsNone(result.carrier2_close_kind)
            self.assertTrue(result.surviving_ping)
            self.assertTrue(result.transport_loss_detected)

    def test_session_close_closes_all_carriers_and_blocks_streams_and_join(self):
        client, server = self._run_mode("session")
        for result in (client, server):
            self.assertEqual(result.session_lifecycle, "closed")
            self.assertFalse(result.carrier1_active)
            self.assertFalse(result.carrier2_active)
            self.assertTrue(result.carrier2_graceful)
            self.assertEqual(result.carrier2_close_kind, "session")
            self.assertFalse(result.surviving_ping)
            self.assertTrue(result.new_stream_blocked)
            self.assertTrue(result.new_join_blocked)
        self.assertEqual(server.close_records_sent, 2)


if __name__ == "__main__":
    unittest.main()
