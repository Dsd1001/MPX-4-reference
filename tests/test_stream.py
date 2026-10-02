import queue
import socket
import threading
import unittest

from mpx4.errors import FinalSizeError, FlowControlError, StreamStateError, TransmissionIDError
from mpx4.stream import (
    ClientStreamRegistry,
    ReceiveFlow,
    ReceiveStream,
    SendFlow,
    ServerStreamRegistry,
    TransmissionLedger,
)
from mpx4.stream_endpoint import client_stream_exchange, server_stream_exchange


KEY = bytes.fromhex(
    "a0a1a2a3a4a5a6a7a8a9aaabacadaeaf"
    "b0b1b2b3b4b5b6b7b8b9babbbcbdbebf"
)


class StreamStateTests(unittest.TestCase):
    def test_reassembly_out_of_order(self):
        stream = ReceiveStream(1)
        self.assertEqual(stream.insert(5, b" world"), b"")
        self.assertEqual(stream.insert(0, b"hello"), b"hello world")
        stream.set_final(11)
        self.assertTrue(stream.complete)

    def test_conflicting_overlap_rejected(self):
        stream = ReceiveStream(1)
        stream.insert(0, b"abc")
        with self.assertRaises(StreamStateError):
            stream.insert(1, b"ZZ")

    def test_data_beyond_final_rejected(self):
        stream = ReceiveStream(1)
        stream.insert(0, b"abc")
        stream.set_final(3)
        with self.assertRaises(FinalSizeError):
            stream.insert(3, b"x")

    def test_flow_control_requires_both_windows(self):
        send = SendFlow()
        send.update_session_credit(0, 20)
        send.update_stream_credit(1, 0, 10)
        self.assertEqual(send.commit(1, 10), 10)
        with self.assertRaises(FlowControlError):
            send.commit(1, 11)

    def test_never_allocated_ack_rejected(self):
        ledger = TransmissionLedger()
        ledger.allocate(1, "data")
        with self.assertRaises(TransmissionIDError):
            ledger.settle(1, 2)


class StreamRegistryTests(unittest.TestCase):
    def test_client_allocates_positive_odd_ids_monotonically_and_never_reuses(self):
        registry = ClientStreamRegistry(max_active=2)
        first = registry.allocate()
        second = registry.allocate()
        self.assertEqual((first, second), (1, 3))
        with self.assertRaises(StreamStateError):
            registry.allocate()

        registry.retire(first)
        third = registry.allocate()
        self.assertEqual(third, 5)
        self.assertNotIn(1, registry.active)
        self.assertIn(1, registry.used)

    def test_server_allows_cross_carrier_open_reordering_but_rejects_reuse(self):
        registry = ServerStreamRegistry(max_active=2)
        registry.accept_open(3)
        registry.accept_open(1)
        self.assertEqual(registry.active, {1, 3})
        registry.retire(1)
        with self.assertRaises(StreamStateError):
            registry.accept_open(1)
        with self.assertRaises(StreamStateError):
            registry.accept_open(2)


class StreamEndpointTests(unittest.TestCase):
    def test_bidirectional_stream_credit_data_fin_consumed(self):
        client_payload = b"client payload over MPX/4"
        server_payload = b"server reply over the same Stream"
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
                        results.put(
                            (
                                "ok",
                                server_stream_exchange(
                                    conn,
                                    KEY,
                                    reply=server_payload,
                                    expected_payload=client_payload,
                                    server_nonce=bytes(range(32, 64)),
                                ),
                            )
                        )
                except BaseException as exc:
                    results.put(("error", exc))

            thread = threading.Thread(target=server, daemon=True)
            thread.start()

            with socket.create_connection((host, port), timeout=5) as sock:
                client = client_stream_exchange(
                    sock,
                    KEY,
                    client_payload,
                    reply_expected=server_payload,
                    session_id=bytes.fromhex("00112233445566778899aabbccddeeff"),
                    client_nonce=bytes(range(32)),
                )

            thread.join(timeout=5)
            self.assertFalse(thread.is_alive())
            status, result = results.get_nowait()
            if status == "error":
                raise result

        self.assertEqual(client.received, server_payload)
        self.assertEqual(result.received, client_payload)
        self.assertEqual(client.session_id, result.session_id)
        self.assertEqual(client.stream_id, 1)


if __name__ == "__main__":
    unittest.main()
