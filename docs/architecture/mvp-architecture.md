# MVP architecture (current state)

Status: describes the code as of 2026-10-08. Scope: the four services of the one-week MVP. It is derived from the code and tests, not from the plan; where they disagree the code is described and the difference is listed under [Known gaps](#known-gaps). The full design is in the root [README](../../README.md); the plan this build followed is [mvp-plan.md](../product/mvp-plan.md); the rules the services share are in [MVP service conventions](../guidelines/mvp-service-conventions.md).

Clients reach the services through an Envoy gateway (REST/JSON on `:8080`, JWT validated at the edge, gRPC transcoding; config in `gateway/`) or directly over gRPC. The gateway is only described where it affects identity and the API surface; see [API conventions](../guidelines/api-conventions.md) for the routes, [Running locally](../guidelines/running-locally.md) and the [demo walkthrough](../product/demo-walkthrough.md).

## Service map

| Service | Language | gRPC port | Database | Tables | Public RPCs | Publishes | Consumes (group) |
|---|---|---|---|---|---|---|---|
| ad | Go | 9001 | `ad` | `ad`, `ad_version`, `ad_lock`, `outbox`, `processed_events` | `CreateAd`, `EditAd`, `PublishAd`, `HideAd`, `GetAd`, `ListMyAds`, `LockAds` (only as `system:swap`) | `ad.events` | `swap.events` (`ad`) |
| matching | Go | 9002 | `matching` | `ad_index`, `notified_pair`, `outbox`, `processed_events` | `Search`, `FindMatches` | `matching.events` | `ad.events` (`matching`) |
| negotiation | .NET 10 | 9003 | `negotiation` | `negotiation`, `proposal`, `approval`, `ad_ref`, `outbox`, `processed_events` | `OpenNegotiation`, `ApproveAd`, `ReviseProposal`, `ApproveProposal`, `RejectProposal`, `CloseNegotiation`, `GetNegotiation`, `ListNegotiations` | `negotiation.events` | `ad.events`, `swap.events` (`negotiation`) |
| swap | .NET 10 | 9004 | `swap` | `swap`, `outbox`, `processed_events` | `GetSwap`, `ListMySwaps`, `SimulateLockerFeePaid` (dev only) | `swap.events` | `negotiation.events` (`swap`) |

Shared code: `libs/goplatform` (Go) and `libs/dotnet/Taakht.Platform` (.NET) hold the identity interceptors, migrations runner, outbox writer and relay, and idempotent consumer. They have the same responsibilities in both languages; no code is shared across languages, only the Protobuf contracts in `api/proto`.

Infrastructure (`deploy/docker-compose.yml`): one Postgres 17 instance with one database per service, and one single-node Kafka (KRaft) whose auto-created topics get 3 partitions. Services run on the host, not in containers.

```mermaid
flowchart LR
  client([Client / grpcurl])
  envoy[Envoy :8080]
  subgraph go [Go]
    ad[ad :9001]
    matching[matching :9002]
  end
  subgraph dotnet [.NET 10]
    neg[negotiation :9003]
    swap[swap :9004]
  end
  kafka[(Kafka)]
  client -->|REST + JWT| envoy
  client -.->|gRPC + x-user-id, dev| ad
  envoy -->|gRPC + x-user-id| ad
  envoy -->|gRPC + x-user-id| matching
  envoy -->|gRPC + x-user-id| neg
  envoy -->|gRPC + x-user-id| swap
  neg -->|GetAd, sync| ad
  swap -->|LockAds, sync| ad
  ad -->|ad.events| kafka
  matching -->|matching.events| kafka
  neg -->|negotiation.events| kafka
  swap -->|swap.events| kafka
  kafka -->|ad.events| matching
  kafka -->|ad.events, swap.events| neg
  kafka -->|negotiation.events| swap
  kafka -->|swap.events| ad
```

## Data ownership

Each service reads and writes only its own database. Copies of other services' data are read models fed by events:

| Data | Owner | Copies elsewhere |
|---|---|---|
| Ad head (`status`, `current_version`), immutable `ad_version` snapshots, lock claims (`ad_lock`) | ad | matching `ad_index` (published ads only, full snapshot, built from `ad.events`); negotiation `ad_ref` (id, owner, current version; built from `AdPublished`/`AdEdited`/`AdReleased` and refreshed from every synchronous `GetAd`) |
| Negotiation, Proposals (immutable, numbered), approvals | negotiation | swap receives what it needs inside `AgreementReached` (both ads with versions and owners, the agreed terms) |
| Swap, legs, fee flags, payment deadline | swap | none (ad and negotiation learn the outcome from events) |
| `MatchFound` dedupe (`notified_pair`) | matching | none |

Identity is the `x-user-id` gRPC metadata key, read by one interceptor per language (an opaque token of 1..64 printable ASCII bytes (0x21-0x7E: no spaces, control characters or non-ASCII), identical in the Go and .NET libs; a repeated `x-user-id` or `x-internal-token` header is rejected too; anything else is `UNAUTHENTICATED`); there is no token validation in the services. Envoy verifies the JWT and writes `sub` into `x-user-id`, replacing anything the client sent. Outgoing calls forward the header, except the two service identities:

- `system:swap`: the Swap service's `LockAds` call. `LockAds` rejects any other caller with `PERMISSION_DENIED`.
- `system:negotiation`: Negotiation's `GetAd` calls, set explicitly in the call metadata. For `GetAd`, the ad's owner and any `system:*` caller read anything (any version, any status); everyone else gets only a `PUBLISHED` ad at its current version and `NOT_FOUND` otherwise. Negotiation authorizes the real caller itself.

Service identities need proof: a `system:` `x-user-id` counts only with the metadata `x-internal-token` equal to the shared secret `INTERNAL_AUTH_TOKEN` (constant-time comparison; default `dev-internal-token` locally, DEV ONLY, with a startup warning while it is in use). The identity interceptor of each service answers `UNAUTHENTICATED` to a `system:` id without a valid token, and the client interceptors attach the token on calls made as `system:*`. Trust assumption: normal user ids are believed as sent (the gateway overwrites the header from the JWT; service ports are not meant to be reachable by clients). The gateway also strips a client-sent `x-internal-token`, denies a JWT whose `sub` starts with `system:` (rbac filter), and `tools/devtoken` refuses to mint such ids. Seed users are `user-1` to `user-4` in `config/eligibility.json`, together with the allowed categories and neighborhoods (with coordinates). Ad validates specs against that file; matching reads it for the distance part of the score.

## Synchronous calls (gRPC)

| Caller | Callee | Call | Timeout | When | On failure |
|---|---|---|---|---|---|
| negotiation | ad | `GetAd` (current version, as `system:negotiation`) | 5 s | opening a negotiation (both ads), `ApproveAd` (the other side's ad), and when all four approvals look valid locally (both ads, the final check) | `NotFound` becomes a domain error; any other error (for example `Unavailable`) propagates to the caller and the negotiation is unchanged |
| swap | ad | `LockAds(swap_id, [(ad_id, version)] x2)` (as `system:swap`) | 10 s | handling `AgreementReached` | `FailedPrecondition`, `NotFound`, `InvalidArgument` and `PermissionDenied` are permanent refusals and end in a rejection (`SwapRejected`); any other error throws, so the consumer retries |

## Asynchronous events (Kafka)

Every event is a `taakht.common.v1.Envelope` (`event_id`, `type`, `aggregate_id`, `occurred_at`, serialized payload). Each service publishes only to its own topic, written through its outbox; the Kafka key is the aggregate id shown below. Consumers skip envelope types they have no handler for. `AdPublished`, `AdEdited` and `AdReleased` are the only ad events negotiation reads.

| Event | Topic | Producer | Kafka key | Consumers and effect |
|---|---|---|---|---|
| `AdPublished` | `ad.events` | ad | ad id | matching: upsert into the index and emit `MatchFound`; negotiation: update `ad_ref` |
| `AdEdited` | `ad.events` | ad | ad id | matching: upsert if the snapshot is published, else delete; negotiation: update `ad_ref` (this is what turns approvals stale) |
| `AdHidden` | `ad.events` | ad | ad id | matching: delete from the index |
| `AdLocked` | `ad.events` | ad | ad id | matching: delete from the index |
| `AdReleased` (full snapshot) | `ad.events` | ad | ad id | matching: upsert if published, else delete; negotiation: update `ad_ref` |
| `AdClosed` | `ad.events` | ad | ad id | matching: delete from the index |
| `MatchFound` | `matching.events` | matching | ad id | none; matching also logs a `MATCH` line |
| `NegotiationOpened` | `negotiation.events` | negotiation | negotiation id | none |
| `NegotiationClosed` (status, reason) | `negotiation.events` | negotiation | negotiation id | none. Written on decline, withdraw and every cancel (including an `AGREED` negotiation cancelled by `SwapCancelled`), not on agreement |
| `AgreementReached` (both ads with versions and owners, terms) | `negotiation.events` | negotiation | negotiation id | swap: create the swap and take the lock |
| `ExclusiveLockAcquired` | `swap.events` | swap | swap id | negotiation: mark the winner `AGREED`, cancel every other live negotiation that involves either ad |
| `SwapRejected` | `swap.events` | swap | swap id | negotiation: cancel the negotiation if it is still `AGREEMENT_PENDING` |
| `SwapCompleted` | `swap.events` | swap | swap id | ad: close both ads (negotiation ignores it) |
| `SwapCancelled` | `swap.events` | swap | swap id | ad: restore both ads to the status held before the lock; negotiation: the `AGREED` negotiation for that ad pair becomes `CANCELLED` (found by ad ids, the event has no negotiation id; ignored if none) |

`AdLocked`, `AdClosed` and `AdReleased` carry the swap id. A new ad is created `hidden` and no event is written for the creation; the first event of an ad is `AdPublished` (or `AdEdited`).

## The lock saga

The exclusive lock is claimed in the Ad service, in one database transaction, and driven by the Swap service. The Negotiation service never calls `LockAds`.

```mermaid
sequenceDiagram
  autonumber
  participant C as Client
  participant N as negotiation
  participant A as ad
  participant K as Kafka
  participant S as swap
  participant M as matching

  C->>N: ApproveProposal (fourth valid approval)
  N->>A: GetAd (both ads, sync)
  A-->>N: current versions
  Note over N: tx: status = AGREEMENT_PENDING<br/>+ outbox AgreementReached
  N-->>C: negotiation (AGREEMENT_PENDING)
  N-)K: AgreementReached (via outbox relay)
  K-)S: AgreementReached
  Note over S: insert swap LOCKING (unique per negotiation)
  S->>A: LockAds(swap_id, [(ad_a, v), (ad_b, v)])
  Note over A: tx: row locks in id order, check status + version,<br/>status = locked, ad_lock rows, outbox AdLocked x2
  A-->>S: ok
  Note over S: tx: AWAITING_PAYMENT (or COMPLETED)<br/>+ outbox ExclusiveLockAcquired
  A-)K: AdLocked x2
  K-)M: AdLocked: remove from index
  S-)K: ExclusiveLockAcquired
  K-)N: ExclusiveLockAcquired
  Note over N: winner AGREED; competing live negotiations CANCELLED<br/>(+ NegotiationClosed each)
```

If the Ad service refuses the lock (an ad is locked, closed, or at another version than the agreed one), the swap goes to `REJECTED`:

```mermaid
sequenceDiagram
  participant S as swap
  participant A as ad
  participant K as Kafka
  participant N as negotiation
  S->>A: LockAds(swap_id, ...)
  A-->>S: FAILED_PRECONDITION (or NOT_FOUND)
  Note over S: tx: REJECTED + outbox SwapRejected
  S-)K: SwapRejected
  K-)N: SwapRejected
  Note over N: AGREEMENT_PENDING -> CANCELLED (+ NegotiationClosed)
```

After a successful lock the swap waits for locker fees. Legs with delivery method `LOCKER` owe a fee; a swap without locker legs completes at once (and emits both `ExclusiveLockAcquired` and `SwapCompleted`).

```mermaid
sequenceDiagram
  participant U as Client
  participant S as swap
  participant K as Kafka
  participant A as ad
  participant M as matching
  participant N as negotiation
  alt every locker fee paid before the deadline
    U->>S: SimulateLockerFeePaid (per locker leg, mock webhook)
    Note over S: last fee: COMPLETED + outbox SwapCompleted
    S-)K: SwapCompleted
    K-)A: SwapCompleted
    Note over A: both ads closed, AdClosed x2
    A-)K: AdClosed x2
    K-)M: AdClosed: no-op (already removed)
  else deadline passes (sweeper, every 5 s)
    Note over S: ask the partner mock, then CANCELLED + outbox SwapCancelled<br/>(defaulting user = first unpaid locker leg)
    S-)K: SwapCancelled
    K-)A: SwapCancelled
    Note over A: both ads back to published or hidden, AdReleased x2
    A-)K: AdReleased x2
    K-)M: re-index published ads
    K-)N: SwapCancelled
    Note over N: AGREED -> CANCELLED (+ NegotiationClosed)
  end
```

Race on one ad: two negotiations on ad A reach agreement at the same time. Each produces its own `AgreementReached` and its own swap. Both swaps call `LockAds`; the Ad service locks the ad rows `FOR UPDATE`, so the second transaction sees the ad already `locked` and answers `FAILED_PRECONDITION`. That swap becomes `REJECTED`, and its negotiation ends `CANCELLED` (by `SwapRejected`, or earlier by the winner's `ExclusiveLockAcquired`, whichever arrives first; the second is a no-op). `TestLockAdsConcurrentOverlap` exercises this in the Ad service.

## State machines

### Ad

`status` and `current_version` live on the `ad` row; every version of the spec is an immutable `ad_version` row.

```mermaid
stateDiagram-v2
  [*] --> hidden: CreateAd (version 1, no event)
  hidden --> published: PublishAd (needs a want category or neighborhood) / AdPublished
  published --> hidden: HideAd / AdHidden
  hidden --> locked: LockAds / AdLocked
  published --> locked: LockAds / AdLocked
  locked --> closed: SwapCompleted / AdClosed
  locked --> hidden: SwapCancelled (was hidden) / AdReleased
  locked --> published: SwapCancelled (was published) / AdReleased
  closed --> [*]
```

`EditAd` is allowed only in `published` and `hidden`, needs `expected_version` to equal the current version (`ABORTED` otherwise), creates version n+1 and always emits `AdEdited` with the full snapshot. The previous status is kept in `status_before_lock` for the release. Only the owner can edit, publish or hide.

### Negotiation

```mermaid
stateDiagram-v2
  [*] --> OPEN: OpenNegotiation (Proposal 1, requester's AD approval)
  OPEN --> AGREEMENT_PENDING: four valid approvals + synchronous GetAd check / AgreementReached
  OPEN --> DECLINED: target owner closes / NegotiationClosed
  OPEN --> WITHDRAWN: requester closes / NegotiationClosed
  OPEN --> CANCELLED: ExclusiveLockAcquired for an ad of this negotiation
  AGREEMENT_PENDING --> AGREED: ExclusiveLockAcquired for this negotiation
  AGREEMENT_PENDING --> CANCELLED: SwapRejected, or another swap locked an ad
  AGREED --> CANCELLED: SwapCancelled for this ad pair / NegotiationClosed
```

Each party gives two approvals: `AD` (target = the version of the other side's ad) and `TERMS` (target = the number of the active Proposal). Proposals are numbered, immutable and never reused; `ReviseProposal` creates number n+1 and counts as the author's `TERMS` approval; `RejectProposal` makes the previous Proposal active again (only one level of history is kept). Once in `AGREEMENT_PENDING` the parties can no longer change anything.

### Swap

```mermaid
stateDiagram-v2
  [*] --> LOCKING: AgreementReached (one swap per negotiation)
  LOCKING --> REJECTED: LockAds refused / SwapRejected
  LOCKING --> AWAITING_PAYMENT: lock taken, a locker leg exists / ExclusiveLockAcquired
  LOCKING --> COMPLETED: lock taken, no locker leg / ExclusiveLockAcquired + SwapCompleted
  AWAITING_PAYMENT --> COMPLETED: all locker fees paid / SwapCompleted
  AWAITING_PAYMENT --> CANCELLED: deadline passed, a fee unpaid / SwapCancelled
```

The deadline is `PAYMENT_DEADLINE` (default `1h`; the demo uses `2m`) counted from the moment the lock is taken.

## Approval validity, versions and staleness

An approval stores its target and is never deleted or invalidated. Whether it is valid is computed every time it is read:

- `AD` approval: valid while its target equals the current version of that ad as known to the Negotiation service (`ad_ref`, fed by `AdEdited` events and refreshed from each synchronous `GetAd`).
- `TERMS` approval: valid while its target equals the number of the active Proposal.

Because `ad_ref` is eventually consistent, the fourth approval is not trusted locally: Negotiation calls `GetAd` for both ads and recomputes validity against those versions before it writes `AgreementReached`. The agreed versions travel in the event, and `LockAds` checks them again against the Ad database, which is the authority. See [bind approvals to versions](../adr/drafts/bind-approvals-to-versions.md).

## Idempotency and outbox mechanics

Writer side (every service that publishes, see [transactional outbox](../adr/drafts/use-a-transactional-outbox-for-events.md)):

- A business change and its event rows are written in one database transaction (`outbox.Add` in Go, `Outbox.AddAsync` in .NET). Rows are inserted with `created_at = clock_timestamp()` so events written in one transaction keep their order.
- One relay loop per service polls every 200 ms for up to 100 unpublished rows, `ORDER BY created_at ... FOR UPDATE SKIP LOCKED`, produces them to Kafka with `acks=all`, waits for the acknowledgement, then sets `published_at`. A crash between the acknowledgement and the commit publishes the rows again: delivery is at-least-once.

Reader side (every service that consumes, see [processed events](../adr/drafts/make-consumers-idempotent-with-a-processed-events-table.md)):

- For each known event, the consumer inserts `(group, event_id)` into `processed_events` with `ON CONFLICT DO NOTHING` and runs the handler in the same transaction. If nothing was inserted, the event was already handled and is skipped. The Kafka offset is committed after the transaction.
- A failing handler is retried with backoff (200 ms doubling to 5 s) and blocks its partition; there is no dead-letter queue. A handler can instead report a permanent failure (`consume.Permanent` in Go, `PermanentEventException` in .NET). The consumer then logs it, records the event in `processed_events` (the handler's writes are rolled back) and moves on, so one poison message cannot block the partition. Payloads that fail to decode, an invalid event id, and a `LockAds` refusal with `InvalidArgument` or `PermissionDenied` (which becomes a rejected swap) are treated this way; every other error is transient and retried forever. The .NET consumer survives `KafkaException` on consume and commit (log and back off). An undecodable envelope (no readable event id) is logged and skipped without a record.
- Groups start from the earliest offset.
- Handlers also check state, because events can arrive twice or late: Ad's swap handlers only touch ads whose `ad_lock` row for that swap is still `locked`; Matching ignores a snapshot older than the indexed version; Negotiation's handlers act only on negotiations in the expected status; Swap transitions are state-checked functions (`SwapMachine`) applied under a row lock with an `UPDATE ... WHERE status = <status read>`.

Lock idempotency: `LockAds` is idempotent by `swap_id`. The `ad_lock (swap_id, ad_id)` primary key records the claim and the agreed version. A retry with the same ads and versions returns success if the swap's lock is still held; a retry with different ads or versions is `FAILED_PRECONDITION`; a retry after release or close is `FAILED_PRECONDITION`. The Swap service also makes the swap row itself idempotent (`UNIQUE (negotiation_id)`, `INSERT ... ON CONFLICT DO NOTHING`), so a redelivered `AgreementReached` finds the existing swap and repeats the same `LockAds`. See [the lock ADR](../adr/drafts/take-the-exclusive-ad-lock-in-the-ad-service.md).

## Failure behavior

Derived from the code; none of this was exercised by a fault-injection test, apart from the redelivery and retry cases covered by the tests listed in each service README.

| What fails | Effect | Recovery |
|---|---|---|
| Kafka unavailable | Business operations keep working: events stay in the outbox (`published_at IS NULL`). The relays log errors and retry every poll interval. Consumers get fetch errors and process nothing. Read models go stale (matching index, negotiation `ad_ref`), and no saga step that needs an event advances (a negotiation stays `AGREEMENT_PENDING`, a swap is never created) | Relays drain the outbox in `created_at` order, consumers continue from their committed offsets, duplicates are absorbed by `processed_events` |
| ad down | Ad RPCs fail. Negotiation cannot open, approve an ad, or reach agreement (`GetAd` times out after 5 s; the negotiation is unchanged). Swap's `LockAds` fails with a transient error: the `AgreementReached` handler throws and is retried with backoff, which blocks the rest of that partition of `negotiation.events`. Matching still answers `Search` from its index (possibly stale) | Ad restarts; the blocked handler succeeds on a retry; `LockAds` is idempotent |
| matching down | No search or match notification. Nothing else depends on it | Restart; it consumes `ad.events` from its committed offset (or from the earliest on first start) and then emits any missing `MatchFound` |
| negotiation down | No new negotiations or approvals. `AgreementReached` events already published to Kafka are still processed by swap and the ads are locked; one still in the negotiation outbox waits for the restart (the relay runs inside the service). `ExclusiveLockAcquired` waits in Kafka | On restart the negotiation moves to `AGREED` and competitors are cancelled |
| swap down | `AgreementReached` waits in Kafka; ads stay unlocked and the negotiation stays `AGREEMENT_PENDING` (it has no timeout). No fee simulation and no deadline sweep | On restart swaps are created and locked; overdue swaps are cancelled on the first sweep (every 5 s) |
| A service's Postgres down | That service's RPCs fail, its relay and consumers retry. Other services continue and see its events late | Restart; the outbox and offsets make the work resume |
| Crash after `LockAds` committed but before the swap row was updated | Swap stays `LOCKING` and the consumer offset is not committed | The event is redelivered, the existing swap is found, `LockAds` returns success again, the swap moves on |
| Crash after an outbox row was produced but before `published_at` was set | The event is produced twice | Consumers dedupe by `event_id` |
| A handler fails with a permanent error (undecodable payload, rejected business input) | The event is logged, recorded in `processed_events` and skipped; later events continue. The skipped event is not stored anywhere else | Look in the service log; there is no DLQ to replay from |
| A handler fails with a transient or unknown error that never clears | Its partition is blocked, retrying forever with a 5 s backoff cap | Manual: fix the cause or the data |

## Differences from the full design

| Area | Full design (README / architecture decisions) | MVP |
|---|---|---|
| Services | Ad, Matching, Deal (negotiation and settlement), Reputation, Communication, Delivery | Ad, Matching, Negotiation, Swap. Reputation and Communication are not built; `MatchFound` and cancellations are log lines or unconsumed events |
| Delivery, KYC, locker partner, auth | External providers behind ports | In-process mocks: `ILockerEligibility` (rejects `user-4` when a Proposal asks for a locker leg of that user), `IDeliveryProvider` (reports what `SimulateLockerFeePaid` recorded). Locker fees are simulated through an RPC that exists only when `ENABLE_DEV_ENDPOINTS=true` or the .NET environment is Development, and a caller can mark only their own leg paid |
| Edge | Envoy, REST transcoding, JWT at the edge | Built: Envoy on `:8080` transcodes REST to gRPC and validates HS256 dev JWTs against a static key in `gateway/envoy.yaml` (`tools/devtoken` signs them). No real identity provider, no key rotation. The dev route `/v1/dev/swaps/...` is routed unconditionally at the edge |
| Search | Elasticsearch, Redis geo index | Postgres table `ad_index` filtered in SQL, scored in Go; six static neighborhoods with coordinates |
| Outbox relay | CDC (Debezium) considered | Polling relay in each service |
| Eligibility Config | Managed configuration | A static JSON file validated by Ad and read by Matching |
| Open-negotiation cap | Per requester Ad, with auto-hide when capped | `NEGOTIATION_CAP` (default 10) counts live negotiations (`OPEN` and `AGREEMENT_PENDING`, as requester or target) per ad, at open time. No auto-hide |
| Reputation, penalties | `PaymentTimedOut` feeds Reputation | `SwapCancelled` carries `defaulting_user_id`; nobody consumes it for reputation |
| Expiry, TTL, home page, Pricing, Hotspots, item-condition claims | Designed | Cut |
| Payment deadline | One hour | Configurable (`PAYMENT_DEADLINE`), 1 h default, 2 min in the demo |
| Deployment | Independent deployables | Processes on the host; one Postgres instance with four databases; one Kafka node |
| Observability, load tests | Designed | Log lines only; no load or fault-injection tests (the `tests/e2e` suite and `scripts/demo.sh` cover the functional flows, not load) |
| Service-to-service auth | Shared `INTERNAL_AUTH_TOKEN` proves `system:*` callers now; mTLS in production ([ADR draft](../adr/drafts/authenticate-service-to-service-calls.md)) | One secret for all services, no rotation; see [Identity](#data-ownership) |
| gRPC reflection | n/a | Off by default; on with `TAAKHT_GRPC_REFLECTION=on` (Go) or the Development environment (.NET). `scripts/dev.sh` turns it on for all four |

## Known gaps

Differences between the plan and the code, and weaknesses that remain. Gaps that were found earlier and have since been fixed are not listed.

1. **Lock query.** The plan describes one conditional `UPDATE ... WHERE status IN (...) AND current_version = $v`. The code takes `SELECT ... FOR UPDATE` row locks on both ads in id order, checks status and version in Go, then updates. The guarantee (atomic, all-or-nothing, serialized per ad) is the same. There are no `ReleaseAds` or `CloseAds` RPCs; release and close happen only when the Ad service consumes `SwapCancelled` and `SwapCompleted`.
2. **Status names.** The plan says a client reads `AgreedLocked` or `Cancelled`. The code uses `AGREEMENT_PENDING`, `AGREED` and `CANCELLED`.
3. **`AGREEMENT_PENDING` has no timeout or repair.** If the swap service never answers (or never receives the event), the negotiation stays pending indefinitely. The same holds for a swap stuck in `LOCKING` beyond redelivery of `AgreementReached`.
4. **No terminal negotiation status for a completed swap.** `SwapCompleted` leaves the negotiation `AGREED`. A cancelled swap now cancels its `AGREED` negotiation, but the ads are then available again and the users must open a new negotiation.
5. **`system:` identity trust.** `system:*` callers are authenticated by one shared secret (`INTERNAL_AUTH_TOKEN`), not by per-service identity: there is no rotation, anyone holding the secret or able to read internal traffic (plaintext gRPC) can claim any `system:` identity, and the local default `dev-internal-token` is public. Production needs mTLS or per-service signed tokens ([ADR draft](../adr/drafts/authenticate-service-to-service-calls.md)).
6. **Consumer transaction across a gRPC call.** The platform consumer opens its dedupe transaction before the handler runs, and the swap handler performs the `LockAds` call inside that window (it ignores the transaction it is given and uses its own short ones). The dedupe transaction, and the connection, are therefore open and idle for up to the 10 s call timeout.
7. **No DLQ.** Permanent failures are skipped and only logged plus recorded in `processed_events`; the payload is not kept. A transient error that never clears (a handler bug) blocks its partition.
8. **Relay assumptions and growth.** One relay per service is assumed. `SKIP LOCKED` would let two relays publish different batches in parallel, so global `created_at` order across them is not guaranteed; only per-key order within one relay is. Published `outbox` rows (older than `OUTBOX_RETENTION`, 24h) and `processed_events` rows (older than `PROCESSED_EVENTS_RETENTION`, 7d) are pruned by the housekeeping job; the cost is that a redelivery older than the processed-events retention would be handled twice.
9. **`MatchFound` once per pair, forever.** `notified_pair` is never cleared, so an ad that is released and re-indexed does not produce a second `MatchFound` for the same pair.
10. **Hidden ads can be negotiated and locked.** `Lockable` is published or hidden, and the requester's own ad may be hidden when a negotiation is opened. The target ad must be published at open time.
11. **Ad edited between agreement and lock.** The lock is refused (version mismatch), the swap is `REJECTED`, the negotiation is `CANCELLED`, and the users must negotiate again.
12. **Simulation endpoint is a mock.** `SimulateLockerFeePaid` is limited to the caller's own leg and is disabled outside development, but it is still a stand-in for a partner webhook that does not exist.
13. **Table list in the plan** names only the main tables; `ad_lock`, `ad_ref`, `notified_pair` and `processed_events` are additions.
14. **Not measured.** No load test results exist and no fault-injection test kills a service or Kafka in the middle of the saga; the failure table above is derived from the code and from unit/integration tests of the redelivery cases.
