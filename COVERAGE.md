# MPX/4 Reference Coverage

**Reference version:** 0.2

**Protocol target:** MPX/4 Draft 03

This file distinguishes implemented reference behavior from protocol areas that remain specification-only.

## Core encoding and cryptography

| Area | Status |
|---|---|
| Canonical VarInt | Implemented |
| Handshake Message framing | Implemented |
| Parameter encode/decode and ordering | Implemented |
| Core Frame envelope | Implemented |
| Draft 03 key schedule | Implemented |
| Finished derivation and verification | Implemented |
| AES-256-GCM Secure Record | Implemented |
| Record sequence / nonce construction | Implemented |
| Consecutive record vectors | Implemented |
| Unknown extension handling | Partial |

## TCP binding

| Area | Status |
|---|---|
| Connection Preface | Implemented |
| Arbitrary TCP fragmentation | Implemented |
| TCP coalescing | Implemented |
| Partial record detection | Implemented |
| Real localhost TCP tests | Implemented |
| CREATE handshake | Implemented |
| Encrypted PING/PONG | Implemented |
| Single-Carrier bidirectional Stream | Implemented |
| JOIN | Not yet implemented |
| Carrier replacement | Not yet implemented |
| CARRIER_CLOSE / SESSION_CLOSE endpoint behavior | Not yet implemented |

## Streams and Session behavior

| Area | Status |
|---|---|
| STREAM_OPEN / STREAM_OPEN_OK | Implemented for Client Stream 1 |
| STREAM_DATA encode/decode | Implemented |
| TRANSMISSION_ACK encode/decode | Implemented |
| STREAM_CREDIT encode/decode | Implemented |
| SESSION_CREDIT encode/decode | Implemented |
| CREDIT_PROBE encoding | Implemented |
| STREAM_FIN encode/decode | Implemented |
| STREAM_CONSUMED encode/decode | Implemented |
| Bidirectional Stream delivery | Implemented for one Stream |
| Out-of-order reassembly | Implemented |
| Identical-overlap validation | Implemented |
| Stream + Session flow-control accounting | Implemented |
| Final-size validation | Implemented |
| Session-wide local Transmission ledger | Implemented |
| Retransmission | Not yet implemented |
| Cross-Carrier reinjection | Not yet implemented |
| RESET / STOP_SENDING state engine | Partial codec only |
| Tombstones / retired identities | Not yet implemented |
| Multiple simultaneous Streams | Not yet implemented |

## Multipath and schedulers

| Area | Status |
|---|---|
| Carrier identity constants | Implemented |
| AGGREGATE handshake identifier | Implemented |
| Multiple active Carriers | Not yet implemented |
| AUTO scheduler | Not yet implemented |
| AGGREGATE scheduler engine | Not yet implemented |
| PROTECT scheduler | Not yet implemented |
| WEIGHTED scheduler | Not yet implemented |
| PATH_CAPACITY behavior | Not yet implemented |

## Specification vectors

| Vector set | Status |
|---|---|
| varint.json | Verified |
| frame-encoding.json | Verified |
| key-schedule.json | Verified |
| secure-record.json | Verified |
| tcp-binding.json | Verified |
| state-validity.json | Partially represented by executable Stream-state tests |

## Interoperability profile

Version 0.2 exercises the Draft 03 single-Carrier foundation through a real TCP exchange:

```text
CREATE
  -> SESSION_CREDIT
  -> STREAM_OPEN / STREAM_OPEN_OK
  -> bidirectional STREAM_CREDIT
  -> bidirectional STREAM_DATA
  -> TRANSMISSION_ACK
  -> STREAM_FIN
  -> STREAM_CONSUMED
```

It still does **not** claim the full Mandatory profile in `MPX-4/INTEROPERABILITY.md`.

The next major milestone is Carrier JOIN plus two simultaneously authenticated Carriers. That enables the reference to test retransmission/reinjection and cross-Carrier ordering without changing the codec, crypto, or single-Stream flow-control foundation.
