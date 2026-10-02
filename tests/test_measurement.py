import queue
import socket
import threading
import unittest

from mpx4.carrier import (
    client_create_carrier,
    client_join_carrier,
    server_create_carrier,
    server_join_carrier,
)
from mpx4.constants import SchedulerID
from mpx4.measurement import PathProber, respond_to_probe
from mpx4.scheduler import AutoScheduler


KEY = bytes.fromhex(
    "a0a1a2a3a4a5a6a7a8a9aaabacadaeaf"
    "b0b1b2b3b4b5b6b7b8b9babbbcbdbebf"
)
SESSION_ID = bytes.fromhex("8899aabbccddeeff0011223344556677")


class RealPathMeasurementTests(unittest.TestCase):
    def test_two_carrier_ping_pong_updates_real_rtt_and_auto_mode(self):
        results = queue.Queue()

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as listener:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listener.bind(("127.0.0.1", 0))
            listener.listen(2)
            host, port = listener.getsockname()

            def server():
                create_sock = None
                join_sock = None
                try:
                    create_sock, _ = listener.accept()
                    carrier1, session = server_create_carrier(
                        create_sock,
                        KEY,
                        server_nonce=bytes(range(32, 64)),
                    )
                    join_sock, _ = listener.accept()
                    carrier2 = server_join_carrier(
                        join_sock,
                        KEY,
                        session,
                        server_nonce=bytes(range(64, 96)),
                    )
                    token1 = respond_to_probe(carrier1, delay_s=0.005)
                    token2 = respond_to_probe(carrier2, delay_s=0.150)
                    results.put(
                        (
                            "ok",
                            (
                                session.session_id,
                                session.scheduler,
                                token1,
                                token2,
                            ),
                        )
                    )
                except BaseException as exc:
                    results.put(("error", exc))
                finally:
                    if join_sock is not None:
                        join_sock.close()
                    if create_sock is not None:
                        create_sock.close()

            thread = threading.Thread(target=server, daemon=True)
            thread.start()

            create_client = socket.create_connection((host, port), timeout=5)
            try:
                carrier1, session = client_create_carrier(
                    create_client,
                    KEY,
                    session_id=SESSION_ID,
                    client_nonce=bytes(range(32)),
                    scheduler=int(SchedulerID.AUTO),
                )
                join_client = socket.create_connection((host, port), timeout=5)
                try:
                    carrier2 = client_join_carrier(
                        join_client,
                        KEY,
                        session,
                        carrier_id=2,
                        generation=0,
                        client_nonce=bytes(range(96, 128)),
                    )

                    scheduler = AutoScheduler(rtt_ratio_threshold=2.0)
                    scheduler.register(carrier1)
                    scheduler.register(carrier2)
                    prober = PathProber(scheduler)

                    probe1 = prober.probe(carrier1, token=101)
                    probe2 = prober.probe(carrier2, token=102)

                    self.assertGreater(probe1.rtt_s, 0.003)
                    self.assertGreater(probe2.rtt_s, 0.120)
                    self.assertGreater(probe2.rtt_s, probe1.rtt_s * 2.0)
                    self.assertAlmostEqual(
                        scheduler.metrics_for((1, 0)).latest_rtt_s,
                        probe1.rtt_s,
                        places=6,
                    )
                    self.assertAlmostEqual(
                        scheduler.metrics_for((2, 0)).latest_rtt_s,
                        probe2.rtt_s,
                        places=6,
                    )
                    self.assertEqual(scheduler.refresh_mode(), "protect")
                    self.assertEqual(scheduler.choose(100).identity, (1, 0))
                finally:
                    join_client.close()
            finally:
                create_client.close()

            thread.join(timeout=5)
            self.assertFalse(thread.is_alive())
            status, value = results.get_nowait()
            if status == "error":
                raise value
            server_session, server_scheduler, token1, token2 = value

        self.assertEqual(server_session, SESSION_ID)
        self.assertEqual(server_scheduler, int(SchedulerID.AUTO))
        self.assertEqual((token1, token2), (101, 102))


if __name__ == "__main__":
    unittest.main()
