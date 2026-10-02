# MPX/4 Reference Implementation

A small, independent reference implementation and interoperability harness for the [MPX/4 protocol specification](https://github.com/Dsd1001/MPX-4).

**Target protocol:** MPX/4 Draft 03

**Reference version:** 0.3.0

**Language:** Python 3.11+

**Purpose:** readable wire-format reference, cryptographic vector verification, and cross-implementation testing.

This repository is intentionally separate from production MPX implementations. The code favors direct correspondence with the specification over throughput, allocation efficiency, or platform-specific optimization.

## Current coverage

Version 0.3 implements:

- canonical MPX VarInt;
- Handshake Message and Parameter codec;
- Core Frame codec;
- Draft 03 HKDF-SHA256 key schedule and Finished verification;
- AES-256-GCM Secure Records;
- incremental TCP byte-stream parsing;
- Session CREATE;
- one real bidirectional Stream with explicit Stream and Session flow control;
- Carrier JOIN into an existing authenticated Session;
- two simultaneously active TCP Carriers;
- independent traffic keys, IVs, and Record sequence spaces per Carrier;
- Carrier ID / Generation validation;
- one deterministic cross-Carrier reinjection path;
- duplicate application-delivery suppression;
- Session-credit accounting that counts reinjection only once;
- direct verification against the specification repository test vectors.

Not yet implemented:

- automatic retransmission timers and scheduler-driven reinjection;
- full TCP Carrier replacement after transport loss;
- multiple simultaneous Streams;
- RESET / STOP_SENDING terminal state engine;
- tombstone / retired-identity engine;
- AUTO / AGGREGATE / PROTECT / WEIGHTED scheduler policy engines;
- full Draft 03 interoperability profile.

See [COVERAGE.md](COVERAGE.md) for the detailed conformance map.

## Repository layout

```text
src/mpx4/
├── constants.py          Protocol registries used by the reference code
├── varint.py             Canonical MPX VarInt
├── codec.py              Parameters, Handshake Messages, and Frames
├── crypto.py             Draft 03 key schedule and Secure Records
├── tcp.py                Incremental TCP binding parser
├── endpoint.py           CREATE + PING/PONG endpoint
├── stream.py             Reassembly, credit, final-size and Tx state
├── stream_endpoint.py    Single-Carrier Stream exchange
├── carrier.py            CREATE/JOIN Carrier lifecycle and Session state
├── multipath_endpoint.py Two-Carrier reinjection reference exchange
├── vectorcheck.py        Specification-vector verifier
└── __main__.py           Command-line interface

tests/
├── test_core.py
├── test_endpoint.py
├── test_stream.py
├── test_multipath.py
└── test_vectors.py
```

## Installation

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install .
```

The only runtime dependency is `cryptography`, used for AES-256-GCM. HKDF-SHA256, HMAC-SHA256, transcript construction, protocol framing, Session state, and validation logic are implemented directly in the reference package.

## Verify specification vectors

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

## Run tests

```bash
MPX4_SPEC_DIR=../MPX-4 python -m unittest discover -s tests -v
```

The suite includes real localhost TCP tests for CREATE, a complete single-Carrier Stream, JOIN, and two-Carrier reinjection.

## Minimal PING/PONG endpoint

Server:

```bash
python -m mpx4 server \
  --listen 127.0.0.1:24004 \
  --key a0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebf
```

Client:

```bash
python -m mpx4 client \
  --connect 127.0.0.1:24004 \
  --key a0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebf \
  --token 15293
```

## Single-Carrier bidirectional Stream

Server:

```bash
python -m mpx4 stream-server \
  --listen 127.0.0.1:24004 \
  --key a0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebf \
  --expect "hello from client" \
  --reply "hello from server"
```

Client:

```bash
python -m mpx4 stream-client \
  --connect 127.0.0.1:24004 \
  --key a0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebf \
  --send "hello from client" \
  --expect-reply "hello from server"
```

## Two-Carrier JOIN and reinjection

Version 0.3 opens two ordinary TCP connections to the same listener.

Carrier 1 creates the Session:

```text
TCP #1
  -> Connection Preface
  -> CLIENT_INIT SESSION_ACTION=CREATE
  -> Finished
  -> Carrier 1 / Generation 0
```

Carrier 2 then joins it:

```text
TCP #2
  -> Connection Preface
  -> CLIENT_INIT
       SESSION_ID = existing Session
       SESSION_ACTION = JOIN
       CARRIER_ID = 2
       GENERATION = 0
       fresh CLIENT_NONCE
  -> fresh Finished exchange
  -> fresh application keys
  -> Record sequence starts at 0
```

Start the server:

```bash
python -m mpx4 multipath-server \
  --listen 127.0.0.1:24004 \
  --key a0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebf \
  --expect "multipath payload"
```

Then run the client:

```bash
python -m mpx4 multipath-client \
  --connect 127.0.0.1:24004 \
  --key a0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebf \
  --send "multipath payload"
```

The reference sends one reliable DATA Transmission twice:

```text
Transmission N
    |
    +-- Attempt 1 -> Carrier 1
    |
    +-- Attempt 2 -> Carrier 2
```

The Server deliberately reads Carrier 2 first. Both Attempts use the same Transmission ID and identical Stream bytes. The application receives the payload once, and Session committed-byte accounting increases once.

This is a deterministic reinjection conformance exercise, not yet an adaptive scheduler.

## JOIN rejection behavior

Draft 03 defines JOIN validation failures such as `SESSION_CONFLICT`, `SCHEDULER_MISMATCH`, and `CARRIER_CONFLICT`, but it does not define a dedicated pre-authentication handshake-error response message.

The reference validates those rules and terminates an invalid JOIN handshake rather than inventing an on-wire extension.

## Continuous conformance

GitHub Actions checks out the current `Dsd1001/MPX-4` specification and runs:

1. unit tests;
2. localhost TCP endpoint tests;
3. CREATE/JOIN and reinjection tests;
4. all implemented specification vectors.

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
