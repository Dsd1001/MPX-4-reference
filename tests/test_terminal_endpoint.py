import queue
import socket
import threading
import unittest

from mpx4.terminal_endpoint import (
    client_terminal_scenario,
    server_terminal_scenario,
)


KEY = bytes.fromhex(
    "a0a1a2a3a4a5a6a7a8a9aaabacadaeaf"
    "b0b1b2b3b4b5b6b7b8b9babbbcbdbebf"
)
SESSION_ID = bytes.fromhex("cafebabefeedface0011223344556677")


class TerminalEndpointTests(unittest.TestCase):
    def _run(self, mode: str):
        results = queue.Queue()

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(("127.0.0.1", 0))
            listener.listen(1)
            host, port = listener.getsockname()

            def server():
                try:
                    conn, _ = listener.accept()
                    with conn:
                        result = server_terminal_scenario(
                            conn,
                            KEY,
                            expected_mode=mode,
                        )
                        results.put(("ok", result))
                except BaseException as exc:
                    results.put(("error", exc))

            thread = threading.Thread(target=server, daemon=True)
            thread.start()

            with socket.create_connection((host, port), timeout=5) as sock:
                client = client_terminal_scenario(
                    sock,
                    KEY,
                    mode=mode,
                    session_id=SESSION_ID,
                )

            thread.join(timeout=5)
            self.assertFalse(thread.is_alive())
            status, value = results.get_nowait()
            if status == "error":
                raise value
            server_result = value

        for result in (client, server_result):
            self.assertEqual(result.session_id, SESSION_ID)
            self.assertEqual(result.mode, mode)
            self.assertEqual(result.stream_id, 1)
            self.assertTrue(result.cancellation_acked)
            self.assertTrue(result.open_rejected)
            self.assertFalse(result.application_created)
            self.assertTrue(result.tombstone_recorded)
            self.assertTrue(result.retired_identity)
            self.assertTrue(result.stale_data_ignored)
            self.assertEqual(result.session_committed_bytes, 0)

        return client, server_result

    def test_real_tcp_preopen_reset_tombstone_and_retirement(self):
        client, server = self._run("reset")
        self.assertFalse(client.reset_sent)
        self.assertFalse(server.reset_sent)

    def test_real_tcp_preopen_stop_generates_reset_then_retires(self):
        client, server = self._run("stop")
        self.assertTrue(client.reset_sent)
        self.assertTrue(server.reset_sent)


if __name__ == "__main__":
    unittest.main()
