from __future__ import annotations

import argparse
import json
from pathlib import Path

from .endpoint import run_client, run_server
from .multipath_endpoint import run_multipath_client, run_multipath_server
from .replacement_endpoint import run_replacement_client, run_replacement_server
from .stream_endpoint import run_stream_client, run_stream_server
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
    if hasattr(result, "ping_token"):
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


if __name__ == "__main__":
    main()
