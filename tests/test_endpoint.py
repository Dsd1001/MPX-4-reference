import queue
import socket
import threading
import unittest

from mpx4.endpoint import client_exchange, server_exchange


class EndpointTests(unittest.TestCase):
    def test_real_tcp_create_handshake_and_ping_pong(self):
        key = bytes.fromhex(
            "a0a1a2a3a4a5a6a7a8a9aaabacadaeaf"
            "b0b1b2b3b4b5b6b7b8b9babbbcbdbebf"
        )
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
                        results.put(("ok", server_exchange(conn, key)))
                except BaseException as exc:
                    results.put(("error", exc))

            thread = threading.Thread(target=server, daemon=True)
            thread.start()

            with socket.create_connection((host, port), timeout=5) as sock:
                client = client_exchange(
                    sock,
                    key,
                    token=15293,
                    session_id=bytes.fromhex("00112233445566778899aabbccddeeff"),
                    client_nonce=bytes(range(32)),
                )

            thread.join(timeout=5)
            self.assertFalse(thread.is_alive())
            status, value = results.get_nowait()
            if status == "error":
                raise value
            server_result = value

        self.assertEqual(client.session_id, server_result.session_id)
        self.assertEqual(client.ping_token, 15293)
        self.assertEqual(server_result.ping_token, 15293)
        self.assertEqual(client.carrier_id, server_result.carrier_id)


if __name__ == "__main__":
    unittest.main()
