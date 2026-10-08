# Demo walkthrough (REST)

`scripts/demo.sh` runs the whole swap flow through the Envoy REST edge only: `curl` + `jq` + a dev JWT per user (`user-1` .. `user-4`). Every call is printed as `user METHOD path`, followed by the key fields of the response. No gRPC, no direct service access.

## What the demo proves

| Step | What happens | Distributed-systems point |
|---|---|---|
| 1. Publish | Four users create and publish ads (`POST /v1/ads`, `:publish`) | Edge auth: Envoy verifies the JWT and injects `x-user-id`; services never see a client-supplied identity |
| 2. Match | `GET /v1/matching/ads/{id}/matches` is polled until the other ad shows up | **Outbox / eventual consistency:** the ad service writes the event in the same transaction as the state change; a relay publishes it to Kafka; matching updates its own index later. The poll is the honest way to show "not instant, but it converges" |
| 3. Three negotiations | user-2, user-3, user-4 open negotiations on user-1's ad | Many contenders for one scarce resource (the ad) |
| 4. Approve ads | user-1 approves each requester's ad with the ad version seen | **Versioned approvals:** an approval is bound to the exact version it was given for |
| 5. Revise | user-2 approves proposal v1, user-1 revises to v2; user-2's approval shows `valid=false`; re-approving v1 returns 409 | **Versioned approvals** again: approvals go stale when their target changes; the stale-version check turns a lost update into `ABORTED` |
| 6. Agreement | user-2 approves v2 (fourth valid approval); the call returns `AGREEMENT_PENDING`, the poll later shows `AGREED` | **Asynchronous agreement, eventual consistency:** the negotiation emits `AgreementReached` through the outbox, the swap service consumes it. Consumers are **idempotent** (event redelivery must not create a second swap) |
| 7. Lock | Swap `AWAITING_PAYMENT`, both ads `LOCKED` | **Atomic lock saga:** swap calls `ad.LockAds`, which claims both ads in one DB transaction (row locks, then checks on status and version): all or nothing |
| 8. Race | The negotiations of user-3 and user-4 become `CANCELLED` ("ad locked by another swap"); locked ads disappear from matching | **Race handling:** exactly one agreement wins the lock; the losers are cancelled by a consumed event, not by a distributed lock. user-3's own ad stays `PUBLISHED` |
| 9. Pay | Each locker leg owner calls the dev endpoint (`caller == userId`); the swap stays `AWAITING_PAYMENT` after the first fee | Partial progress is persisted; the state machine only advances when all legs are paid |
| 10. Complete | Swap `COMPLETED`, both ads `CLOSED` | Event-driven completion across service boundaries |
| End | Ads still `PUBLISHED` are hidden again | Re-runnability: titles carry a random suffix, nothing is reused |

`scripts/demo.sh timeout` replays the flow with two users, then leaves one fee unpaid: after the payment deadline the swap becomes `CANCELLED`, both ads return to `PUBLISHED` and re-enter the match index, and the negotiation is `CANCELLED`. That is the **timeout / compensation** point: the lock is a reservation with a deadline, and releasing it is an event too.

## How to run

```bash
make up                  # Postgres, Kafka, Envoy (docker compose)
scripts/dev.sh start     # the four services on the host
scripts/dev.sh status    # all four must be "up"

scripts/demo.sh                  # happy path, about 25 s
scripts/demo.sh --pause          # wait for Enter between steps (live demo)
scripts/demo.sh --no-color       # plain output (also automatic when piped)
make demo                        # same as scripts/demo.sh; DEMO_ARGS="--pause" or DEMO_ARGS=timeout
```

Timeout variant (the swap needs a short payment deadline; the default is 2 minutes):

```bash
PAYMENT_DEADLINE=20s scripts/dev.sh restart swap
scripts/demo.sh timeout          # refuses to run if the deadline is more than 90 s away
scripts/dev.sh restart swap      # back to the default deadline
```

Requirements: `curl`, `jq` (`winget install jqlang.jq`), and `.run/bin/devtoken` (built automatically if Go is installed). `GATEWAY` overrides the Envoy URL (default `http://localhost:8080`). The script exits non-zero with a `DEMO FAILED:` message on any unexpected HTTP status or poll timeout.

## Expected output (excerpt from a real run)

```text
== Step 5: Versioned approvals: user-2 approves proposal v1, then user-1 revises -> the old approval goes stale
   user-2 POST /v1/negotiations/752b24b4-...:approve-proposal
   -> HTTP 200
     approvals: user-2 AD v1 valid=true; user-1 AD v1 valid=true; user-2 TERMS v1 valid=true
   user-1 POST /v1/negotiations/752b24b4-...:revise
   -> HTTP 200
     active proposal is now v2: legA=DELIVERY_METHOD_LOCKER legB=DELIVERY_METHOD_LOCKER price=50000 payer=user-2
     approvals: user-2 AD v1 valid=true; user-1 AD v1 valid=true; user-2 TERMS v1 valid=false; user-1 TERMS v2 valid=true
   user-2's approval of v1 is stale (valid=false): approvals are only valid while their target is current.
   user-2 POST /v1/negotiations/752b24b4-...:approve-proposal
   -> HTTP 409
     proposal 1 is not active (active is 2); re-read the negotiation

== Step 6: user-2 approves v2: the fourth valid approval -> AgreementReached (asynchronous)
     negotiation status right after the call: NEGOTIATION_STATUS_AGREEMENT_PENDING
   -> HTTP 200 (after 2s)
     negotiation 752b24b4-... -> NEGOTIATION_STATUS_AGREED

== Step 7: Swap service takes the atomic lock on both ads (saga: ad.LockAds in ONE DB transaction)
     swap 6c019040-... status=SWAP_STATUS_AWAITING_PAYMENT
     legA: owner=user-2 method=DELIVERY_METHOD_LOCKER feePaid=false
     legB: owner=user-1 method=DELIVERY_METHOD_LOCKER feePaid=false
     ad A ... -> AD_STATUS_LOCKED
     ad B ... -> AD_STATUS_LOCKED

== Step 8: Race handling: the competing negotiations on the locked ad are cancelled by the system
     user-3 negotiation a6dc2770-... -> NEGOTIATION_STATUS_CANCELLED  reason: ad locked by another swap
     user-4 negotiation 876a7b76-... -> NEGOTIATION_STATUS_CANCELLED  reason: ad locked by another swap

== Step 9: Locker fees: the locker partner is mocked; each leg owner reports the fee (dev endpoint)
     after user-2 paid: swap status=SWAP_STATUS_AWAITING_PAYMENT legA.feePaid=true legB.feePaid=false
     after user-1 paid: swap status=SWAP_STATUS_COMPLETED legA.feePaid=true legB.feePaid=true

DEMO OK: published -> matched -> 3 negotiations -> stale approval -> agreement -> locked -> paid -> COMPLETED, ads CLOSED, rivals CANCELLED.
```

Timeout variant, the decisive lines:

```text
     paymentDeadline=2026-10-08T19:30:06Z  (in 17s)
     swap 43a0541f-... -> SWAP_STATUS_CANCELLED  reason: locker fee not paid in time
     ad A -> AD_STATUS_PUBLISHED
     ad B -> AD_STATUS_PUBLISHED
DEMO OK (timeout): locker fee unpaid -> swap CANCELLED -> ads PUBLISHED again and back in matching.
```

## What to say

- "Everything you see is plain REST through Envoy with a JWT per user. Envoy verifies the token and hands the user id to the services; the services trust nothing from the client."
- Step 2: "The ad service does not call matching. It writes an event to its outbox in the same transaction as the ad, a relay puts it on Kafka, matching builds its own index. The polling dots are the eventual consistency."
- Steps 3-4: "Three people want the same ad. All three negotiations are legitimate until the ad is gone; nothing is locked while people talk."
- Step 5: "An approval is only worth something for the version it was given for. The moment user-1 changes the terms, user-2's approval turns `valid=false`, and replaying the old approval gets a 409. No one can be bound to terms they did not see."
- Step 6: "The last approval does not do the swap. It returns `AGREEMENT_PENDING` and publishes an event. The swap service consumes it, and consuming it twice is harmless."
- Step 7: "The lock is one database transaction in the ad service: both ads or none, checked against status and version. That is the only place where the race is decided."
- Step 8: "The other two negotiations did not need a distributed lock to be stopped. They lost the claim, the system cancelled them with a reason, and their ads are untouched and still in the index."
- Steps 9-10: "Payment is a mock of the locker partner. The swap completes only when every locker leg is paid, and then the ads close."
- Timeout variant: "If someone does not pay, nothing stays stuck. The swap cancels at the deadline and the ads are released and matchable again. That is compensation, driven by a timer and an event."
- Closing: "Every arrow between services is either a synchronous call with a clear failure mode (the lock) or an at-least-once event with an idempotent consumer."

## Troubleshooting

- `Envoy is not reachable`: `make up`, then `scripts/dev.sh start`.
- `HTTP 503 ... Connection refused`: a service is down or restarting; check `scripts/dev.sh status`. The script retries for about 30 s.
- Timeout variant says the deadline is too far: restart swap with `PAYMENT_DEADLINE=20s` (see above).
- Leftover ads of an aborted run are hidden by the script on exit; if it was killed, they stay `PUBLISHED` and show up as extra candidates (harmless).
