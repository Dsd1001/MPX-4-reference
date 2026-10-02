# MPX/4 Reference Coverage

**Reference version:** 0.6

**Protocol target:** MPX/4 Draft 03

This file distinguishes implemented reference behavior from protocol areas that remain specification-only.

## Core encoding and cryptography

| Area | Status |
|---|---|
| Canonical VarInt | Implemented |
| Handshake Message framing | Implemented |
| Parameter encode/decode and canonical order | Implemented |
| Core Frame envelope | Implemented |
| Draft 03 key schedule | Implemented |
| Finished derivation and verification | Implemented |
| AES-256-GCM Secure Record | Implemented |
| Record sequence / nonce construction | Implemented |
| Consecutive record vectors | Implemented |
| Unknown extension handling | Partial |

## Handshake and scheduler parameters

| Area | Status |
|---|---|
| AUTO Scheduler ID | Implemented |
| AGGREGATE Scheduler ID | Implemented |
| PROTECT Scheduler ID | Implemented |
| WEIGHTED Scheduler ID | Implemented |
| Scheduler mismatch validation | Implemented |
| PATH_CAPACITY for WEIGHTED | Implemented |
| PATH_CAPACITY forbidden for non-WEIGHTED | Implemented |
| Downlink/uplink capacity-unit validation | Implemented |
| Full asymmetric WEIGHTED CLI profile | Not yet implemented |

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

Draft 03 defines JOIN rejection error classes but no dedicated pre-authentication handshake-error wire message. The reference therefore rejects invalid JOINs by terminating the incomplete Carrier handshake and does not invent an extension message.

## Streams and Session behavior

| Area | Status |
|---|---|
| Client odd Stream-ID allocator | Implemented |
| Monotonic Stream IDs 1,3,5,... | Implemented |
| MAX_STREAMS active-count enforcement | Implemented |
| Retired Stream IDs never reused locally | Implemented |
| Server accepts valid cross-Carrier OPEN reordering | Implemented |
| STREAM_OPEN / STREAM_OPEN_OK | Implemented |
| STREAM_DATA encode/decode | Implemented |
| TRANSMISSION_ACK encode/decode | Implemented |
| STREAM_CREDIT encode/decode | Implemented |
| SESSION_CREDIT encode/decode | Implemented |
| CREDIT_PROBE encoding | Implemented |
| STREAM_FIN encode/decode | Implemented |
| STREAM_CONSUMED encode/decode | Implemented |
| Single-Stream bidirectional delivery | Implemented |
| Two simultaneously active Streams | Implemented |
| Session-wide Transmission IDs across Streams | Implemented |
| Out-of-order reassembly | Implemented |
| Identical-overlap validation | Implemented |
| Stream + Session flow-control accounting | Implemented |
| Final-size validation | Implemented |
| Full two-Stream FIN / CONSUMED lifecycle | Implemented |
| RESET / STOP_SENDING state engine | Partial codec only |
| Tombstone compaction | Not yet implemented |
| Complete retired-identity state machine | Not yet implemented |

The two-Stream reference endpoint currently uses one Carrier. Multipath scheduling across many simultaneous Streams is a later integration step.

## Reliability and reinjection

| Area | Status |
|---|---|
| Session-wide local Transmission ledger | Implemented |
| Same Transmission ID across Carrier Attempts | Implemented |
| Cross-Carrier reinjection | Implemented |
| Pending wire Frame retained across Carrier loss | Implemented |
| Automatic reinjection after replacement | Implemented |
| Timer-driven retry eligibility | Implemented |
| Immediate retry after last Carrier becomes inactive | Implemented |
| Attempt history Carrier ID / Generation / send time | Implemented |
| Duplicate application suppression | Implemented |
| Reinjection consumes no additional logical credit | Implemented |
| Duplicate ACK settlement | Implemented |
| Never-allocated ACK rejection | Implemented |
| Multi-Attempt ACK excluded from path-rate attribution | Implemented |

## Path measurement and schedulers

| Area | Status |
|---|---|
| Carrier latest/min RTT | Implemented |
| Real encrypted PING/PONG RTT sample | Implemented |
| Same-Carrier PONG validation | Implemented in reference probe flow |
| Carrier observed delivery rate | Implemented |
| Carrier outstanding scheduled bytes | Implemented |
| Carrier configured capacity | Implemented |
| Carrier role / failure / penalty state | Implemented |
| AGGREGATE score | Implemented |
| Queue-pressure-aware AGGREGATE selection | Implemented |
| Prefer unused Carrier for later Attempt | Implemented |
| PROTECT primary/backup policy | Implemented |
| AUTO aggregate/protect mode switch | Implemented |
| AUTO measured-RTT divergence trigger | Implemented |
| AUTO failure/penalty trigger | Implemented |
| WEIGHTED configured-capacity clamp | Implemented |
| Continuously scheduled background probes | Not yet implemented |
| Production congestion-control coupling | Not yet implemented |

Reference scheduler algorithms are local implementation choices. Draft 03 standardizes the Scheduler IDs and protocol invariants, not the exact path-scoring formula.

## Real endpoint exercises

| Exercise | Status |
|---|---|
| CREATE + encrypted PING/PONG | Implemented |
| Single-Stream bidirectional lifecycle | Implemented |
| Two simultaneous Stream lifecycle | Implemented |
| Two-Carrier JOIN | Implemented |
| Cross-Carrier duplicate/reinjection | Implemented |
| Carrier loss + Generation replacement | Implemented |
| Scheduler-driven replacement recovery | Implemented |
| Two-Carrier measured RTT + AUTO selection | Implemented |
| End-to-end WEIGHTED traffic-placement CLI | Not yet implemented |

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

Version 0.6 now exercises three independent Session behaviors that can be combined by a production implementation:

```text
Path measurement:
  Carrier 1 -- PING/PONG --> measured RTT
  Carrier 2 -- PING/PONG --> measured RTT
      |
      +--> AUTO / PROTECT / AGGREGATE local policy

Multiple Streams:
  Session
    +-- Stream 1 -- DATA / ACK / FIN / CONSUMED
    +-- Stream 3 -- DATA / ACK / FIN / CONSUMED

Reliability:
  Transmission N
    Attempt 1 -> Carrier A
    retry/loss
    Attempt 2 -> Carrier B
```

The reference still does **not** claim the full Mandatory profile in `MPX-4/INTEROPERABILITY.md`.

The next major milestones are the remaining state-machine-heavy areas: RESET/STOP_SENDING, tombstones/retired identities, and then a combined multi-Stream + multi-Carrier adaptive scheduling exercise.
