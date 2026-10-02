# MPX/4 Reference Coverage

**Reference version:** 0.8

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
| CARRIER_CLOSE codec and graceful Carrier state | Implemented |
| CARRIER_CLOSE leaves Session/other Carrier usable | Implemented |
| SESSION_CLOSE codec and Session lifecycle | Implemented |
| SESSION_CLOSE blocks new Streams | Implemented |
| SESSION_CLOSE blocks new Carrier JOINs | Implemented |
| Duplicate SESSION_CLOSE idempotence | Implemented |
| Bare TCP EOF distinguished from CARRIER_CLOSE | Implemented |
| TCP half-close distinguished from MPX close | Implemented |

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
| RESET_STREAM typed decode | Implemented |
| STOP_SENDING typed decode | Implemented |
| STREAM_OPEN_REJECT typed decode | Implemented |
| Active FIN -> RESET same-final transition | Implemented |
| RESET suppresses later application delivery | Implemented |
| STOP_SENDING supersedes pending local FIN | Implemented |
| Pre-open RESET Final Offset 0 | Implemented |
| Pre-open RESET non-zero rejection | Implemented |
| Pre-open STOP + generated RESET | Implemented |
| Late OPEN after pre-open cancellation | Implemented |
| Lightweight cancellation tombstone | Implemented |
| Accepted-Stream terminal tombstone | Implemented |
| Tombstone duplicate terminal idempotence | Implemented |
| Tombstone final-size conflict detection | Implemented |
| Tombstone compaction to retired identity | Implemented |
| Retired identity never recreates application state | Implemented |
| Retired stale DATA adds no Session commitment | Implemented |
| Advanced tombstone range/bitmap compaction | Not yet implemented |
| Client OPENING STREAM_CREDIT acceptance evidence | Implemented |
| Client OPENING FIN(0) acceptance evidence | Implemented |
| Client OPENING RESET(0) acceptance evidence | Implemented |
| Client OPENING STOP_SENDING acceptance evidence | Implemented |
| Client STOP evidence generates RESET(0) | Implemented |
| STREAM_DATA before OPEN_OK rejected | Implemented |
| OPEN_REJECT after acceptance evidence rejected | Implemented |
| Non-zero FIN/RESET acceptance evidence rejected | Implemented |
| Complete initiator OPENING acceptance-evidence state machine | Implemented |

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
| Real pre-open RESET -> tombstone -> retired identity | Implemented |
| Real pre-open STOP -> RESET -> tombstone -> retired identity | Implemented |
| Retired stale DATA + Carrier liveness PING/PONG | Implemented |
| Real STREAM_CREDIT overtakes OPEN_OK | Implemented |
| Real FIN(0) overtakes OPEN_OK | Implemented |
| Real RESET(0) overtakes OPEN_OK | Implemented |
| Real STOP_SENDING overtakes OPEN_OK + Client RESET | Implemented |
| Real graceful CARRIER_CLOSE + surviving Carrier PING/PONG | Implemented |
| Real SESSION_CLOSE on two Carriers | Implemented |
| Real bare TCP EOF classified as Carrier loss | Implemented |
| Real TCP half-close classified as Carrier loss | Implemented |
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

Version 0.8 adds initiator-side opening reordering and authenticated close behavior to the existing path, Stream, reliability, and retirement exercises:

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

Terminal / retirement:
  RESET or STOP before OPEN
      -> cancellation tombstone
      -> reject late OPEN
      -> duplicate terminal is idempotent
      -> compact to retired identity
      -> stale DATA ignored without credit commitment

Opening reordering:
  Carrier 1: STREAM_OPEN -----------------------> STREAM_OPEN_OK
  Carrier 2:          STREAM_CREDIT / FIN(0) / RESET(0) / STOP
                      -> acceptance evidence while Client stays OPENING

Close behavior:
  CARRIER_CLOSE -> target Carrier closed, Session survives
  bare EOF / half-close -> Carrier loss, not graceful MPX close
  SESSION_CLOSE -> Session closed, new Stream/JOIN blocked
```

The reference still does **not** claim the full Mandatory profile in `MPX-4/INTEROPERABILITY.md`.

The next major milestones are full bidirectional RESET/STOP integration across the existing Stream endpoints, automatic SESSION_CLOSE generation for Session-scoped protocol errors, stronger state-validity vector coverage, and then a combined multi-Stream + multi-Carrier adaptive scheduling exercise.
