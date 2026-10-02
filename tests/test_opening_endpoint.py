import queue
import socket
import threading
import unittest

from mpx4.opening_endpoint import (
    run_opening_client,
    run_opening_server,
)


KEY = bytes.fromhex(
    "a0a1a2a3a4a5a6a7a8a9aaabacadaeaf"
    "b0b1b2b3b4b5b6b7b8b9babbbcbdbebf"
)


class RealOpeningEvidenceTests(unittest.TestCase):
    def _run_mode(self, mode: str):
        results = queue.Queue()

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(("127.0.0.1", 0))
            listener.listen(1)
            host, port = listener.getsockname()

        # run_opening_server creates its own listener, so reserve a free port
        # above and immediately release it before starting the server thread.
        def server():
            try:
                results.put(
                    (
                        "ok",
                        run_opening_server(
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

        # The listener is created in the server thread; retry briefly to avoid
        # making the test depend on thread scheduling.
        import time

        deadline = time.monotonic() + 5
        client = None
        last_error = None
        while time.monotonic() < deadline:
            try:
                client = run_opening_client(
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
            raise last_error or RuntimeError("opening client did not connect")

        thread.join(timeout=5)
        self.assertFalse(thread.is_alive())
        status, value = results.get_nowait()
        if status == "error":
            raise value
        server_result = value

        for result in (client, server_result):
            self.assertEqual(result.mode, mode)
            self.assertEqual(result.stream_id, 1)
            self.assertEqual(
                result.phase_before_open_ok,
                "OPENING_WITH_ACCEPTANCE_EVIDENCE",
            )
            self.assertEqual(result.final_phase, "OPEN")
            self.assertTrue(result.acceptance_evidence)
            self.assertTrue(result.open_settled)

        return client, server_result

    def test_credit_overtakes_open_ok(self):
        client, server = self._run_mode("credit")
        self.assertGreater(client.stream_credit_maximum, 0)
        self.assertEqual(
            client.stream_credit_maximum,
            server.stream_credit_maximum,
        )
        self.assertFalse(client.evidence_acked)

    def test_fin_zero_overtakes_open_ok(self):
        client, server = self._run_mode("fin")
        self.assertEqual(client.peer_terminal_kind, "fin")
        self.assertTrue(client.evidence_acked)
        self.assertTrue(server.evidence_acked)

    def test_reset_zero_overtakes_open_ok(self):
        client, server = self._run_mode("reset")
        self.assertEqual(client.peer_terminal_kind, "reset")
        self.assertTrue(client.evidence_acked)
        self.assertTrue(server.evidence_acked)

    def test_stop_overtakes_open_ok_and_client_reset_is_confirmed(self):
        client, server = self._run_mode("stop")
        self.assertTrue(client.evidence_acked)
        self.assertTrue(server.evidence_acked)
        self.assertTrue(client.client_reset_sent)
        self.assertTrue(server.client_reset_sent)
        self.assertTrue(client.client_reset_acked)
        self.assertTrue(server.client_reset_acked)


if __name__ == "__main__":
    unittest.main()
