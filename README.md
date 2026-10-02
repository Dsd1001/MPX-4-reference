# MPX/4 Reference Implementation

A small, independent reference implementation and interoperability harness for the [MPX/4 protocol specification](https://github.com/Dsd1001/MPX-4).

**Target protocol:** MPX/4 Draft 03

**Reference version:** 0.6.0

**Language:** Python 3.11+

**Purpose:** readable wire-format reference, cryptographic vector verification, and cross-implementation testing.

This repository is intentionally separate from production MPX implementations. The code favors direct correspondence with the specification over throughput, allocation efficiency, or platform-specific optimization.

## Current coverage

Version 0.6 implements:

- canonical MPX VarInt;
- Handshake Message and Parameter codec;
- Core Frame codec;
- Draft 03 HKDF-SHA256 key schedule and Finished verification;
- AES-256-GCM Secure Records;
- incremental TCP byte-stream parsing;
- Session CREATE and Carrier JOIN;
- independent traffic keys, IVs, and Record sequence spaces per Carrier;
- Carrier ID / Generation validation and replacement after loss;
- Stream and Session flow control;
- single-Stream bidirectional DATA/ACK/FIN/CONSUMED;
- two simultaneously active Client Streams with IDs 1 and 3;
- Session-wide Transmission IDs across multiple Streams;
- out-of-order reassembly, overlap validation, final-size checks, and retired Stream-ID tracking;
- cross-Carrier reinjection and duplicate-delivery suppression;
- timer-driven retry and immediate reinjection after Carrier loss;
- AGGREGATE, PROTECT, AUTO, and WEIGHTED reference scheduler classes;
- Draft 03 Scheduler-ID validation;
- WEIGHTED PATH_CAPACITY encoding/decoding;
- real encrypted PING/PONG Carrier RTT sampling;
- AUTO mode switching based on measured path divergence or failure state;
- direct verification against the specification repository test vectors.

Still intentionally incomplete:

- RESET_STREAM / STOP_SENDING terminal state engine;
- tombstone compaction and the complete retired-identity rules from STATE-MACHINES.md;
- CARRIER_CLOSE / SESSION_CLOSE endpoint behavior;
- continuously running background path probes;
- production-grade congestion-control interaction;
- a full end-to-end WEIGHTED CLI profile with per-direction capacity configuration;
- the complete Draft 03 interoperability profile.

See [COVERAGE.md](COVERAGE.md) for the detailed conformance map.

## Repository layout

```text
src/mpx4/
├── constants.py           Protocol registries
├── varint.py              Canonical MPX VarInt
├── codec.py               Parameters, Handshake Messages, and Frames
├── crypto.py              Draft 03 key schedule and Secure Records
├── tcp.py                 Incremental TCP binding parser
├── endpoint.py            Minimal CREATE + PING/PONG endpoint
├── carrier.py             CREATE/JOIN Carrier lifecycle and Session state
├── stream.py              Reassembly, credit, Stream registry, and Tx state
├── stream_endpoint.py     Single-Stream bidirectional exchange
├── multistream_endpoint.py Two simultaneous Stream lifecycles
├── reliability.py         Timer-driven Transmission reliability loop
├── scheduler.py           AGGREGATE / PROTECT / AUTO / WEIGHTED reference policies
├── measurement.py         Carrier PING/PONG RTT measurement
├── probe_endpoint.py      Two-Carrier measured-path scheduler demo
├── multipath_endpoint.py  Cross-Carrier reinjection reference exchange
├── replacement_endpoint.py Carrier-loss and Generation replacement exchange
├── vectorcheck.py         Specification-vector verifier
└── __main__.py            Command-line interface

tests/
├── test_core.py
├── test_endpoint.py
├── test_measurement.py
├── test_multistream.py
├── test_multipath.py
├── test_replacement.py
├── test_scheduler.py
├── test_scheduler_handshake.py
├── test_stream.py
└── test_vectors.py
```

## Installation

```bash
python3 -m venv .venv
. .venv/bin/activate
python -m pip install .
```

The only runtime dependency is `cryptography`, used for AES-256-GCM. HKDF-SHA256, HMAC-SHA256, transcript construction, protocol framing, Session state, flow-control state, and scheduler logic are implemented directly in the reference package.

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

## Run the complete test suite

```bash
MPX4_SPEC_DIR=../MPX-4 python -m unittest discover -s tests -v
```

The suite includes real localhost TCP tests for CREATE, JOIN, two-Carrier reinjection, Generation replacement, encrypted path probes, single-Stream operation, and two simultaneously active Streams.

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

## Two simultaneous Streams

Version 0.6 can keep Stream 1 and Stream 3 active at the same time in one Session.

Server:

```bash
python -m mpx4 multistream-server \
  --listen 127.0.0.1:24004 \
  --key a0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebf \
  --expect1 "first stream" \
  --expect2 "second stream"
```

Client:

```bash
python -m mpx4 multistream-client \
  --connect 127.0.0.1:24004 \
  --key a0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebf \
  --send1 "first stream" \
  --send2 "second stream"
```

The exchange performs:

```text
SESSION_CREDIT
  |
  +-- STREAM_OPEN 1
  +-- STREAM_OPEN 3
          |
          +-- OPEN_OK + STREAM_CREDIT for each
          |
          +-- STREAM_DATA 1  (Session-wide Tx ID N)
          +-- STREAM_DATA 3  (Session-wide Tx ID N+1)
          |
          +-- ACK each
          |
          +-- FIN each
          |
          +-- STREAM_CONSUMED / peer FIN each
          |
          +-- retire Stream IDs 1 and 3
```

The Stream registry allocates positive odd IDs monotonically. Retiring Stream 1 does not make ID 1 reusable; the next locally allocated Stream is 5.

## Real Carrier RTT measurement

Draft 03 allows PING/PONG to be used for Carrier RTT estimation. Version 0.6 exposes that path directly.

Start a two-Carrier probe server. The delay options intentionally emulate different path latency in the reference harness; the PING/PONG Frames themselves are real authenticated and encrypted MPX/4 traffic:

```bash
python -m mpx4 probe-server \
  --listen 127.0.0.1:24004 \
  --key a0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebf \
  --delay1-ms 5 \
  --delay2-ms 150
```

Then measure and select:

```bash
python -m mpx4 probe-client \
  --connect 127.0.0.1:24004 \
  --key a0a1a2a3a4a5a6a7a8a9aaabacadaeafb0b1b2b3b4b5b6b7b8b9babbbcbdbebf \
  --scheduler auto \
  --candidate-bytes 1200
```

The client reports the measured RTT of both Carriers, the current reference scheduler mode, and the selected Carrier.

## Reference scheduler policies

Draft 03 standardizes Scheduler IDs and the Carrier state that schedulers need, but deliberately leaves the exact path-selection algorithm implementation-defined. The policies below are therefore reference algorithms, not additional protocol requirements.

### AGGREGATE

The reference score is:

```text
predicted completion
  = latest RTT / 2
  + (outstanding bytes + candidate Frame bytes) / effective delivery rate
  + penalty
```

A later Attempt prefers an active Carrier that the same Transmission has not yet used, when one exists.

### PROTECT

New Transmissions prefer paths whose role is `primary` or ordinary `active`. Backup/degraded paths remain available for a later Attempt when reliability requires them.

The probe CLI assigns the lowest measured-RTT Carrier as primary for its PROTECT demonstration.

### AUTO

The reference AUTO policy begins in aggregate behavior. It changes to protect behavior when either:

- active-path RTT divergence reaches a 2.0 ratio; or
- an active Carrier has failure/penalty/degraded state.

This threshold is a reference implementation choice, not a Draft 03 wire requirement.

### WEIGHTED

Draft 03 WEIGHTED requires a per-Carrier `PATH_CAPACITY` in CLIENT_INIT:

```text
Downlink Capacity Units
Uplink Capacity Units

1 unit = 100,000 bit/s
```

Version 0.6 validates and round-trips that Parameter and provides a `WeightedScheduler` that clamps effective path rate to configured capacity. A full CLI/deployment profile for asymmetric per-Carrier capacities remains future work.

## Reliability and reinjection

The Session keeps reliable Transmissions independently from Carrier lifetime.

```text
Transmission N
  Attempt 1 -> Carrier A
       |
       X transport loss or retry timeout
       |
  Attempt 2 -> Carrier B

same Transmission ID
same semantic Frame
same logical flow-control commitment
```

`ReliabilityLoop.poll()` immediately reinjects when the most recent Carrier becomes inactive and uses a retry timer while that Carrier remains active.

A valid ACK retires the Transmission and clears outstanding-byte accounting for all Attempts.

A single-Attempt ACK returned on the same Carrier can update RTT/delivery-rate state. Once a Transmission has multiple Attempts, Draft 03 attribution is ambiguous; the reference settles reliability but does not guess a per-Carrier delivery-rate sample.

## Carrier replacement

Unexpected transport loss does not destroy Session/Stream state.

```text
Carrier 2 / Generation 0
        X
        |
new TCP connection
        |
JOIN same Carrier ID
Generation 1
        |
fresh nonces
fresh traffic keys
Record sequence = 0
        |
outstanding Transmission reinjected
```

The existing `replacement-server` and `replacement-client` commands exercise this path with the scheduler/reliability loop.

## JOIN rejection behavior

Draft 03 defines JOIN validation failures such as `SESSION_CONFLICT`, `SCHEDULER_MISMATCH`, and `CARRIER_CONFLICT`, but does not define a dedicated pre-authentication handshake-error response message.

The reference validates those rules and terminates an invalid JOIN handshake rather than inventing an on-wire extension.

## Continuous conformance

GitHub Actions checks out the current `Dsd1001/MPX-4` specification and runs:

1. unit tests;
2. real localhost TCP endpoint tests;
3. Stream, multi-Stream, JOIN, reinjection, replacement, and path-measurement tests;
4. all implemented specification vectors.

A scheduled run also checks whether newer specification changes have broken the reference implementation.

## Design rules

The reference implementation follows a few deliberate constraints:

- no source code is copied from `mptcp-userspace`;
- protocol constants and behavior are derived from the published MPX/4 specification;
- TCP read/write boundaries are never treated as protocol boundaries;
- cryptographic test vectors must match byte-for-byte;
- unsupported protocol areas are reported as unsupported rather than approximated;
- reference scheduler algorithms are clearly separated from normative wire behavior;
- clarity is preferred over performance.

## License

BSD 3-Clause. See [LICENSE](LICENSE).
