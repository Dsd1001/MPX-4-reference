from __future__ import annotations

import argparse
import json
from pathlib import Path

from .endpoint import run_client, run_server
from .multipath_endpoint import run_multipath_client, run_multipath_server
from .multistream_endpoint import run_multistream_client, run_multistream_server
from .probe_endpoint import run_probe_client, run_probe_server
from .replacement_endpoint import run_replacement_client, run_replacement_server
from .stream_endpoint import run_stream_client, run_stream_server
from .terminal_endpoint import run_terminal_client, run_terminal_server
from .vectorcheck import verify_all


def parse_key(value: str) -> bytes:
    try:
        key = bytes.fromhex(value)
    except ValueError as exc:
        raise argparse.ArgumentTypeError("key must be hexadecimal") from exc
    if len(key) != 32:
        raise argparse.ArgumentTypeError("key must encode exactly 32 octets")
    return key


def split_address(value: str) -> tuple[str, int]:
    if value.startswith("["):
        end = value.find("]")
        if end < 0 or end + 2 > len(value) or value[end + 1] != ":":
            raise argparse.ArgumentTypeError("expected [IPv6]:port")
        return value[1:end], int(value[end + 2 :])
    host, sep, port = value.rpartition(":")
    if not sep or not host:
        raise argparse.ArgumentTypeError("expected host:port")
    return host, int(port)


def result_json(result) -> str:
    if hasattr(result, "stale_data_ignored"):
        payload = {
            "session_id": result.session_id.hex(),
            "mode": result.mode,
            "stream_id": result.stream_id,
            "cancellation_acked": result.cancellation_acked,
            "reset_sent": result.reset_sent,
            "open_rejected": result.open_rejected,
            "application_created": result.application_created,
            "tombstone_recorded": result.tombstone_recorded,
            "retired_identity": result.retired_identity,
            "stale_data_ignored": result.stale_data_ignored,
            "session_committed_bytes": result.session_committed_bytes,
        }
    elif hasattr(result, "carrier1_rtt_ms"):
        payload = {
            "session_id": result.session_id.hex(),
            "scheduler": result.scheduler,
            "scheduler_name": result.scheduler_name,
            "carrier1_rtt_ms": round(result.carrier1_rtt_ms, 3),
            "carrier2_rtt_ms": round(result.carrier2_rtt_ms, 3),
            "mode": result.mode,
            "selected_carrier": list(result.selected_carrier),
        }
    elif hasattr(result, "carrier1_delay_ms"):
        payload = {
            "session_id": result.session_id.hex(),
            "scheduler": result.scheduler,
            "scheduler_name": result.scheduler_name,
            "carrier1_delay_ms": result.carrier1_delay_ms,
            "carrier2_delay_ms": result.carrier2_delay_ms,
        }
    elif hasattr(result, "retired_stream_ids"):
        payload = {
            "session_id": result.session_id.hex(),
            "stream_ids": list(result.stream_ids),
            "payloads_utf8": [value.decode("utf-8", "replace") for value in result.payloads],
            "transmission_ids": list(result.transmission_ids),
            "session_committed_bytes": result.session_committed_bytes,
            "retired_stream_ids": list(result.retired_stream_ids),
        }
    elif hasattr(result, "ping_token"):
        payload = {
            "session_id": result.session_id.hex(),
            "carrier_id": result.carrier_id,
            "scheduler": result.scheduler,
            "ping_token": result.ping_token,
        }
    elif hasattr(result, "replacement_generation"):
        payload = {
            "session_id": result.session_id.hex(),
            "stream_id": result.stream_id,
            "transmission_id": result.transmission_id,
            "payload_utf8": result.payload.decode("utf-8", "replace"),
            "attempts": list(result.attempts),
            "scheduler_initial_carrier": list(result.scheduler_initial_carrier),
            "recovery_reason": result.recovery_reason,
            "failed_carrier_inactive": result.failed_carrier_inactive,
            "replacement_generation": result.replacement_generation,
            "replacement_first_record_sequence": result.replacement_first_record_sequence,
            "fresh_replacement_keys": result.fresh_replacement_keys,
            "carrier_generations": list(result.carrier_generations),
            "application_deliveries": result.application_deliveries,
            "session_committed_bytes": result.session_committed_bytes,
            "surviving_carrier_active": result.surviving_carrier_active,
        }
    elif hasattr(result, "application_deliveries"):
        payload = {
            "session_id": result.session_id.hex(),
            "stream_id": result.stream_id,
            "transmission_id": result.transmission_id,
            "payload_utf8": result.payload.decode("utf-8", "replace"),
            "application_deliveries": result.application_deliveries,
            "session_committed_bytes": result.session_committed_bytes,
            "carrier_generations": list(result.carrier_generations),
            "fresh_carrier_keys": result.fresh_carrier_keys,
            "carrier2_first_record_sequence": result.carrier2_first_record_sequence,
        }
    else:
        payload = {
            "session_id": result.session_id.hex(),
            "stream_id": result.stream_id,
            "sent_utf8": result.sent.decode("utf-8", "replace"),
            "received_utf8": result.received.decode("utf-8", "replace"),
        }
    return json.dumps(payload, sort_keys=True)


def main() -> None:
    parser = argparse.ArgumentParser(prog="mpx4-ref")
    sub = parser.add_subparsers(dest="command", required=True)

    vectors = sub.add_parser("vectors", help="verify the MPX/4 specification test vectors")
    vectors.add_argument("--spec", required=True, type=Path)

    server = sub.add_parser("server", help="run the minimal Draft 03 PING/PONG server")
    server.add_argument("--listen", default="127.0.0.1:24004")
    server.add_argument("--key", required=True, type=parse_key)

    client = sub.add_parser("client", help="run the minimal Draft 03 PING/PONG client")
    client.add_argument("--connect", default="127.0.0.1:24004")
    client.add_argument("--key", required=True, type=parse_key)
    client.add_argument("--token", type=int, default=1)

    stream_server = sub.add_parser(
        "stream-server",
        help="run the Draft 03 single-Carrier bidirectional Stream reference server",
    )
    stream_server.add_argument("--listen", default="127.0.0.1:24004")
    stream_server.add_argument("--key", required=True, type=parse_key)
    stream_server.add_argument("--expect", required=True)
    stream_server.add_argument("--reply", required=True)

    stream_client = sub.add_parser(
        "stream-client",
        help="run the Draft 03 single-Carrier bidirectional Stream reference client",
    )
    stream_client.add_argument("--connect", default="127.0.0.1:24004")
    stream_client.add_argument("--key", required=True, type=parse_key)
    stream_client.add_argument("--send", required=True)
    stream_client.add_argument("--expect-reply", required=True)

    multipath_server = sub.add_parser(
        "multipath-server",
        help="run the Draft 03 two-Carrier JOIN/reinjection server",
    )
    multipath_server.add_argument("--listen", default="127.0.0.1:24004")
    multipath_server.add_argument("--key", required=True, type=parse_key)
    multipath_server.add_argument("--expect", required=True)

    multipath_client = sub.add_parser(
        "multipath-client",
        help="run the Draft 03 two-Carrier JOIN/reinjection client",
    )
    multipath_client.add_argument("--connect", default="127.0.0.1:24004")
    multipath_client.add_argument("--key", required=True, type=parse_key)
    multipath_client.add_argument("--send", required=True)

    replacement_server = sub.add_parser(
        "replacement-server",
        help="run the Draft 03 Carrier-loss and Generation replacement server",
    )
    replacement_server.add_argument("--listen", default="127.0.0.1:24004")
    replacement_server.add_argument("--key", required=True, type=parse_key)
    replacement_server.add_argument("--expect", required=True)

    replacement_client = sub.add_parser(
        "replacement-client",
        help="run the Draft 03 Carrier-loss and Generation replacement client",
    )
    replacement_client.add_argument("--connect", default="127.0.0.1:24004")
    replacement_client.add_argument("--key", required=True, type=parse_key)
    replacement_client.add_argument("--send", required=True)

    probe_server = sub.add_parser(
        "probe-server",
        help="run a two-Carrier PING/PONG path-measurement server",
    )
    probe_server.add_argument("--listen", default="127.0.0.1:24004")
    probe_server.add_argument("--key", required=True, type=parse_key)
    probe_server.add_argument("--delay1-ms", type=float, default=5.0)
    probe_server.add_argument("--delay2-ms", type=float, default=150.0)

    probe_client = sub.add_parser(
        "probe-client",
        help="measure two Carriers and show scheduler mode/selection",
    )
    probe_client.add_argument("--connect", default="127.0.0.1:24004")
    probe_client.add_argument("--key", required=True, type=parse_key)
    probe_client.add_argument(
        "--scheduler",
        choices=["auto", "aggregate", "protect"],
        default="auto",
    )
    probe_client.add_argument("--candidate-bytes", type=int, default=1200)

    multistream_server = sub.add_parser(
        "multistream-server",
        help="run a two-Stream Draft 03 lifecycle server",
    )
    multistream_server.add_argument("--listen", default="127.0.0.1:24004")
    multistream_server.add_argument("--key", required=True, type=parse_key)
    multistream_server.add_argument("--expect1", required=True)
    multistream_server.add_argument("--expect2", required=True)

    multistream_client = sub.add_parser(
        "multistream-client",
        help="open Stream 1 and Stream 3 concurrently in one Session",
    )
    multistream_client.add_argument("--connect", default="127.0.0.1:24004")
    multistream_client.add_argument("--key", required=True, type=parse_key)
    multistream_client.add_argument("--send1", required=True)
    multistream_client.add_argument("--send2", required=True)

    terminal_server = sub.add_parser(
        "terminal-server",
        help="run Draft 03 pre-open cancellation / retirement scenarios",
    )
    terminal_server.add_argument("--listen", default="127.0.0.1:24004")
    terminal_server.add_argument("--key", required=True, type=parse_key)
    terminal_server.add_argument("--mode", choices=["reset", "stop"], required=True)

    terminal_client = sub.add_parser(
        "terminal-client",
        help="send RESET_STREAM or STOP_SENDING before STREAM_OPEN",
    )
    terminal_client.add_argument("--connect", default="127.0.0.1:24004")
    terminal_client.add_argument("--key", required=True, type=parse_key)
    terminal_client.add_argument("--mode", choices=["reset", "stop"], required=True)

    args = parser.parse_args()

    if args.command == "vectors":
        passed = verify_all(args.spec)
        print(json.dumps({"revision": "Draft 03", "passed": passed}))
        return

    if args.command == "server":
        host, port = split_address(args.listen)
        print(json.dumps({"listening": args.listen, "revision": "Draft 03"}), flush=True)
        print(result_json(run_server(host, port, args.key)))
        return

    if args.command == "client":
        host, port = split_address(args.connect)
        print(result_json(run_client(host, port, args.key, token=args.token)))
        return

    if args.command == "stream-server":
        host, port = split_address(args.listen)
        print(json.dumps({"listening": args.listen, "mode": "stream"}), flush=True)
        print(
            result_json(
                run_stream_server(
                    host,
                    port,
                    args.key,
                    expected_payload=args.expect.encode(),
                    reply=args.reply.encode(),
                )
            )
        )
        return

    if args.command == "stream-client":
        host, port = split_address(args.connect)
        print(
            result_json(
                run_stream_client(
                    host,
                    port,
                    args.key,
                    payload=args.send.encode(),
                    reply_expected=args.expect_reply.encode(),
                )
            )
        )
        return

    if args.command == "multipath-server":
        host, port = split_address(args.listen)
        print(json.dumps({"listening": args.listen, "mode": "multipath"}), flush=True)
        print(
            result_json(
                run_multipath_server(
                    host,
                    port,
                    args.key,
                    expected_payload=args.expect.encode(),
                )
            )
        )
        return

    if args.command == "multipath-client":
        host, port = split_address(args.connect)
        print(
            result_json(
                run_multipath_client(
                    host,
                    port,
                    args.key,
                    payload=args.send.encode(),
                )
            )
        )
        return

    if args.command == "replacement-server":
        host, port = split_address(args.listen)
        print(json.dumps({"listening": args.listen, "mode": "replacement"}), flush=True)
        print(
            result_json(
                run_replacement_server(
                    host,
                    port,
                    args.key,
                    expected_payload=args.expect.encode(),
                )
            )
        )
        return

    if args.command == "replacement-client":
        host, port = split_address(args.connect)
        print(
            result_json(
                run_replacement_client(
                    host,
                    port,
                    args.key,
                    payload=args.send.encode(),
                )
            )
        )
        return

    if args.command == "probe-server":
        host, port = split_address(args.listen)
        print(json.dumps({"listening": args.listen, "mode": "probe"}), flush=True)
        print(
            result_json(
                run_probe_server(
                    host,
                    port,
                    args.key,
                    carrier1_delay_ms=args.delay1_ms,
                    carrier2_delay_ms=args.delay2_ms,
                )
            )
        )
        return

    if args.command == "probe-client":
        host, port = split_address(args.connect)
        print(
            result_json(
                run_probe_client(
                    host,
                    port,
                    args.key,
                    scheduler_name_value=args.scheduler,
                    candidate_bytes=args.candidate_bytes,
                )
            )
        )
        return

    if args.command == "multistream-server":
        host, port = split_address(args.listen)
        print(json.dumps({"listening": args.listen, "mode": "multistream"}), flush=True)
        print(
            result_json(
                run_multistream_server(
                    host,
                    port,
                    args.key,
                    expected_payload1=args.expect1.encode(),
                    expected_payload2=args.expect2.encode(),
                )
            )
        )
        return

    if args.command == "multistream-client":
        host, port = split_address(args.connect)
        print(
            result_json(
                run_multistream_client(
                    host,
                    port,
                    args.key,
                    payload1=args.send1.encode(),
                    payload2=args.send2.encode(),
                )
            )
        )
        return

    if args.command == "terminal-server":
        host, port = split_address(args.listen)
        print(
            json.dumps({"listening": args.listen, "mode": f"terminal-{args.mode}"}),
            flush=True,
        )
        print(
            result_json(
                run_terminal_server(
                    host,
                    port,
                    args.key,
                    mode=args.mode,
                )
            )
        )
        return

    if args.command == "terminal-client":
        host, port = split_address(args.connect)
        print(
            result_json(
                run_terminal_client(
                    host,
                    port,
                    args.key,
                    mode=args.mode,
                )
            )
        )
        return


if __name__ == "__main__":
    main()
