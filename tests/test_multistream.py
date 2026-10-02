import queue
import socket
import threading
import unittest

from mpx4.multistream_endpoint import (
    client_multistream_exchange,
    server_multistream_exchange,
)


KEY = bytes.fromhex(
    "a0a1a2a3a4a5a6a7a8a9aaabacadaeaf"
    "b0b1b2b3b4b5b6b7b8b9babbbcbdbebf"
)
SESSION_ID = bytes.fromhex("11223344556677889900aabbccddeeff")


class MultiStreamEndpointTests(unittest.TestCase):
    def test_two_simultaneous_streams_share_one_session(self):
        payload1 = b"stream-one-payload"
        payload2 = b"stream-three-payload"
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
                        result = server_multistream_exchange(
                            conn,
                            KEY,
                            expected_payload1=payload1,
                            expected_payload2=payload2,
                            server_nonce=bytes(range(32, 64)),
                        )
                        results.put(("ok", result))
                except BaseException as exc:
                    results.put(("error", exc))

            thread = threading.Thread(target=server, daemon=True)
            thread.start()

            with socket.create_connection((host, port), timeout=5) as sock:
                client = client_multistream_exchange(
                    sock,
                    KEY,
                    payload1,
                    payload2,
                    session_id=SESSION_ID,
                    client_nonce=bytes(range(32)),
                )

            thread.join(timeout=5)
            self.assertFalse(thread.is_alive())
            status, value = results.get_nowait()
            if status == "error":
                raise value
            server_result = value

        for result in (client, server_result):
            self.assertEqual(result.session_id, SESSION_ID)
            self.assertEqual(result.stream_ids, (1, 3))
            self.assertEqual(result.payloads, (payload1, payload2))
            self.assertEqual(result.retired_stream_ids, (1, 3))
            self.assertEqual(
                result.session_committed_bytes,
                len(payload1) + len(payload2),
            )

        self.assertEqual(client.transmission_ids, server_result.transmission_ids)
        self.assertNotEqual(
            client.transmission_ids[0],
            client.transmission_ids[1],
        )


if __name__ == "__main__":
    unittest.main()
