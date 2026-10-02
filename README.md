# MPX/4 Reference Implementation

A small, independent reference implementation and interoperability harness for the [MPX/4 protocol specification](https://github.com/Dsd1001/MPX-4).

**Target protocol:** MPX/4 Draft 03

**Language:** Python 3.11+

**Purpose:** readable wire-format reference, cryptographic vector verification, and cross-implementation testing.

This repository is intentionally separate from production MPX implementations. The code favors direct correspondence with the specification over throughput, allocation efficiency, or platform-specific optimization.

## Current coverage

Implemented in version 0.1:

- canonical 1/2/4/8-octet MPX VarInt;
- Handshake Message framing;
- canonical ordered handshake Parameters;
- typed Frame framing;
- STREAM_DATA / TRANSMISSION_ACK / STREAM_CREDIT / SESSION_CREDIT / CREDIT_PROBE encoders;
- PING / PONG Frames;
- Draft 03 HKDF-SHA256 key schedule;
- CLIENT_FINISHED and SERVER_FINISHED derivation;
- AES-256-GCM Secure Records;
- per-direction Record Sequence Numbers;
- incremental TCP byte-stream parsing;
- TCP fragmentation and coalescing behavior;
- minimal Session CREATE handshake;
- encrypted PING/PONG exchange over a real TCP socket;
- direct verification against the specification repository test vectors.

Not yet implemented:

- Carrier JOIN and replacement endpoint behavior;
- Stream application data state machine;
- retransmission/reinjection engine;
- Session and Stream flow-control engine;
- tombstone/retired-identity engine;
- multi-Carrier scheduler implementations;
- full Draft 03 interoperability profile.

See [COVERAGE.md](COVERAGE.md) for the detailed conformance map.

## Repository layout

```text
src/mpx4/
├── constants.py     Protocol registries used by the reference code
├── varint.py        Canonical MPX VarInt
├── codec.py         Parameters, Handshake Messages, and Frames
├── crypto.py        Draft 03 key schedule and Secure Records
├── tcp.py           Incremental TCP binding parser
├── endpoint.py      Minimal CREATE + PING/PONG reference endpoint
├── vectorcheck.py   Specification-vector verifier
└── __main__.py      Command-line interface

tests/
├── test_core.py
├── test_endpoint.py
└── test_vectors.py
```

## Installation

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install .
```

The only runtime dependency is `cryptography`, used for AES-256-GCM. HKDF-SHA256, HMAC-SHA256, transcript construction, protocol framing, and validation logic are implemented directly in the reference package.

## Verify the specification vectors

Clone the specification next to this repository:

```bash
git clone https://github.com/Dsd1001/MPX-4.git
git clone https://github.com/Dsd1001/MPX-4-reference.git
cd MPX-4-reference
python3 -m venv .venv
. .venv/bin/activate
python -m pip install .
python -m mpx4 vectors --spec ../MPX-4
```

Expected result:

```json
{"revision": "Draft 03", "passed": ["varint", "frame-encoding", "key-schedule", "secure-record", "tcp-binding"]}
```

## Run the tests

```bash
MPX4_SPEC_DIR=../MPX-4 python -m unittest discover -s tests -v
```

The test suite includes a real localhost TCP exchange rather than only in-memory codec tests.

## Minimal TCP interoperability endpoint

Choose the same 32-octet transport key on both sides. For example:

```text
a0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebf
```

Start the reference server:

```bash
python -m mpx4 server \
  --listen 127.0.0.1:24004 \
  --key a0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebf
```

Then run the client:

```bash
python -m mpx4 client \
  --connect 127.0.0.1:24004 \
  --key a0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebf \
  --token 15293
```

The exchange performs:

```text
TCP connect
  ↓
MPX/4 Connection Preface
  ↓
CLIENT_INIT
  ↓
SERVER_INIT
  ↓
CLIENT_FINISHED
  ↓
SERVER_FINISHED
  ↓
AES-256-GCM Secure Record
  ↓
PING
  ↓
PONG
```

The minimal endpoint currently supports `SESSION_ACTION=CREATE`, Carrier ID 1 / Generation 0, and the AGGREGATE Scheduler. Its purpose is to provide an independent wire-level peer for implementation development, not to act as a production transport.

## Continuous conformance

GitHub Actions checks out the current `Dsd1001/MPX-4` specification and runs:

1. unit tests;
2. localhost TCP handshake/PING-PONG test;
3. all implemented specification vectors.

A scheduled run also checks whether newer specification changes have broken the reference implementation.

## Design rules

The reference implementation follows a few deliberate constraints:

- no source code is copied from `mptcp-userspace`;
- protocol constants and behavior are derived from the published MPX/4 specification;
- TCP read/write boundaries are never treated as protocol boundaries;
- cryptographic test vectors must match byte-for-byte;
- unsupported protocol areas are reported as unsupported rather than approximated;
- clarity is preferred over performance.

## License

BSD 3-Clause. See [LICENSE](LICENSE).
