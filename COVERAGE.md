# MPX/4 Reference Coverage

**Reference version:** 0.3

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

## TCP binding and Carrier lifecycle

| Area | Status |
|---|---|
| Connection Preface | Implemented |
| Arbitrary TCP fragmentation | Implemented |
| TCP coalescing | Implemented |
| Partial record detection | Implemented |
| Real localhost TCP tests | Implemented |
| CREATE handshake | Implemented |
| JOIN handshake | Implemented |
| Two simultaneous authenticated Carriers | Implemented |
| Fresh traffic keys per Carrier | Implemented |
| Independent Record sequence spaces | Implemented |
| New Carrier ID starts at Generation 0 | Implemented |
| Higher Generation supersedes lower Generation | State validation implemented |
| Equal/stale Generation rejection | Implemented |
| Session-scoped JOIN receive-limit validation | Implemented |
| Carrier-scoped MAX_RECORD_SIZE variation | Implemented |
| Full replacement TCP lifecycle | Not yet implemented |
| CARRIER_CLOSE / SESSION_CLOSE endpoint behavior | Not yet implemented |

Draft 03 defines JOIN rejection error classes but no dedicated handshake-error wire message. The reference therefore rejects invalid JOINs by terminating the incomplete Carrier handshake and does not invent an extension message.

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
| Same Transmission ID across Carrier Attempts | Implemented |
| Cross-Carrier reinjection | Implemented for one deterministic DATA Transmission |
| Duplicate application suppression | Implemented |
| Reinjection consumes no additional logical credit | Implemented |
| Automatic retransmission timer | Not yet implemented |
| RESET / STOP_SENDING state engine | Partial codec only |
| Tombstones / retired identities | Not yet implemented |
| Multiple simultaneous Streams | Not yet implemented |

## Multipath and schedulers

| Area | Status |
|---|---|
| Carrier identity and Generation | Implemented |
| AGGREGATE handshake identifier | Implemented |
| Two active Carriers | Implemented |
| Cross-Carrier arrival-order test | Implemented |
| AUTO scheduler | Not yet implemented |
| AGGREGATE scheduler policy engine | Not yet implemented |
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
| state-validity.json | Partially represented by executable Stream/Carrier tests |

## Interoperability profile

Version 0.3 exercises an actual two-Carrier MPX/4 Session:

```text
Carrier 1
  CREATE
    |
    +---- Session state ----+
                           |
Carrier 2                  |
  JOIN --------------------+
    |
    +-- fresh key / IV
    +-- Record sequence = 0

Stream 1:
  STREAM_OPEN / credit on Carrier 1

Transmission N:
  Attempt 1 -> Carrier 1
  Attempt 2 -> Carrier 2   (same Transmission ID and bytes)

Server intentionally reads Carrier 2 first
  -> application delivery exactly once
  -> Session commitment counted exactly once
  -> ACK may return on either Carrier
```

It still does **not** claim the full Mandatory profile in `MPX-4/INTEROPERABILITY.md`.

The next major milestone is an actual Carrier replacement after transport loss using a higher Generation, followed by automatic retransmission/reinjection of an outstanding Transmission.
