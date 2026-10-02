# MPX/4 Reference Coverage

**Reference version:** 0.5

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
| Higher Generation supersedes lower Generation | Implemented |
| Equal/stale Generation rejection | Implemented |
| Session-scoped JOIN receive-limit validation | Implemented |
| Carrier-scoped MAX_RECORD_SIZE variation | Implemented |
| Unexpected TCP loss marks Carrier inactive | Implemented |
| Same-ID Generation replacement after loss | Implemented |
| Replacement fresh key / IV state | Implemented |
| Replacement Record sequence restarts at 0 | Implemented |
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
| Pending wire Frame retained across Carrier loss | Implemented |
| Automatic reinjection after replacement | Implemented |
| Timer-driven retry eligibility | Implemented |
| Immediate retry after last Carrier becomes inactive | Implemented |
| Attempt history records Carrier ID / Generation and send time | Implemented |
| Duplicate application suppression | Implemented |
| Reinjection consumes no additional logical credit | Implemented |
| Timer-driven retransmission / reinjection loop | Implemented |
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
| Surviving Carrier remains usable through peer Carrier loss | Implemented |
| Carrier replacement Gen 0 -> Gen 1 | Implemented |
| Carrier latest/min RTT | Implemented |
| Carrier observed delivery rate | Implemented |
| Carrier outstanding scheduled bytes | Implemented |
| Carrier role / failure / penalty state | Implemented |
| Minimal AGGREGATE scheduler policy engine | Implemented |
| Queue-pressure-aware path selection | Implemented |
| Prefer unused Carrier for later Attempt | Implemented |
| Ambiguous multi-Attempt ACK excluded from rate attribution | Implemented |
| AUTO scheduler | Not yet implemented |
| PROTECT scheduler | Not yet implemented |
| WEIGHTED scheduler | Not yet implemented |
| PATH_CAPACITY configured-capacity clamp | Implemented in local metrics; handshake behavior remains partial |

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

Version 0.5 adds a Session reliability loop and a minimal AGGREGATE path-selection policy on top of the 0.4 replacement scenario:

```text
Carrier 1 / Gen 0
  remains active throughout

Scheduler chooses Carrier 2 / Gen 0
  using RTT + outstanding/rate score

Carrier 2 / Gen 0
  Attempt N sent
  no TRANSMISSION_ACK
       |
       X unexpected TCP loss
       |
Session keeps Stream, credit, and Transmission N
       |
Carrier 2 / Gen 1
  fresh JOIN
  fresh traffic keys
  Record sequence = 0
       |
ReliabilityLoop.poll()
  sees last Carrier inactive
  chooses best unused active Carrier
       |
ledger reinjects the original wire Frame
       |
TRANSMISSION_ACK N

Result:
  -> application delivery exactly once
  -> Session commitment counted exactly once
  -> Attempt history = (2,0), (2,1)
```

It still does **not** claim the full Mandatory profile in `MPX-4/INTEROPERABILITY.md`.

The next major milestone is to extend the scheduler surface beyond the minimal AGGREGATE policy: measured PING/PONG-driven path sampling, multiple simultaneous Streams, and then PROTECT/AUTO behavior with explicit degraded-path roles.
