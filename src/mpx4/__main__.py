from __future__ import annotations

import argparse
import json
from pathlib import Path

from .endpoint import run_client, run_server
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
    return json.dumps(
        {
            "session_id": result.session_id.hex(),
            "carrier_id": result.carrier_id,
            "scheduler": result.scheduler,
            "ping_token": result.ping_token,
        },
        sort_keys=True,
    )


def main() -> None:
    parser = argparse.ArgumentParser(prog="mpx4-ref")
    sub = parser.add_subparsers(dest="command", required=True)

    vectors = sub.add_parser("vectors", help="verify the MPX/4 specification test vectors")
    vectors.add_argument("--spec", required=True, type=Path)

    server = sub.add_parser("server", help="run the minimal Draft 03 TCP reference server")
    server.add_argument("--listen", default="127.0.0.1:24004")
    server.add_argument("--key", required=True, type=parse_key)

    client = sub.add_parser("client", help="run the minimal Draft 03 TCP reference client")
    client.add_argument("--connect", default="127.0.0.1:24004")
    client.add_argument("--key", required=True, type=parse_key)
    client.add_argument("--token", type=int, default=1)

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


if __name__ == "__main__":
    main()
