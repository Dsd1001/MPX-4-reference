# MPX/4 Reference Coverage

**Reference version:** 0.1  
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
| Real localhost TCP test | Implemented |
| CREATE handshake | Implemented |
| Encrypted PING/PONG | Implemented |
| JOIN | Not yet implemented |
| Carrier replacement | Not yet implemented |
| CARRIER_CLOSE / SESSION_CLOSE endpoint behavior | Not yet implemented |

## Streams and Session behavior

| Area | Status |
|---|---|
| STREAM_DATA encoding | Implemented |
| TRANSMISSION_ACK encoding | Implemented |
| STREAM_CREDIT encoding | Implemented |
| SESSION_CREDIT encoding | Implemented |
| CREDIT_PROBE encoding | Implemented |
| STREAM_OPEN lifecycle | Not yet implemented |
| Bidirectional Stream delivery | Not yet implemented |
| Reassembly | Not yet implemented |
| Flow-control accounting | Not yet implemented |
| Retransmission | Not yet implemented |
| Cross-Carrier reinjection | Not yet implemented |
| Final-size engine | Not yet implemented |
| Tombstones / retired identities | Not yet implemented |

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
| state-validity.json | Data available; state engine not yet implemented |

## Interoperability profile

The reference currently covers the codec/crypto/TCP-handshake foundation of the Draft 03 interoperability profile. It does **not** yet claim the full Mandatory profile in `MPX-4/INTEROPERABILITY.md`.

The next implementation milestone is Stream state plus explicit Stream/Session credit. After that, Carrier JOIN and reinjection can be implemented without changing the codec or cryptographic layer.
