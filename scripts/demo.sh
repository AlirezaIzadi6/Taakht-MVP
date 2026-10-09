#!/usr/bin/env bash
# Taakht MVP demo: the full swap flow, narrated, driven ONLY through the Envoy REST edge with JWTs.
#
# Usage:
#   scripts/demo.sh [--pause] [--no-color]            happy path (publish -> match -> negotiate -> lock -> pay -> done)
#   scripts/demo.sh [--pause] [--no-color] timeout    payment-timeout variant (locker fee unpaid -> swap cancelled -> ads released)
#
# Flags:
#   --pause     wait for Enter between steps (live demos)
#   --no-color  plain output (also honoured: NO_COLOR env var, or stdout not being a terminal)
#
# Environment:
#   GATEWAY     Envoy base URL (default http://localhost:8080)
#
# Needs: curl, jq, and the devtoken binary (built automatically into .run/bin when Go is available).
# The timeout variant needs the swap service started with a short deadline, e.g.:
#   PAYMENT_DEADLINE=20s scripts/dev.sh restart swap
# (restore the normal deadline afterwards with: scripts/dev.sh restart swap)
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
GATEWAY="${GATEWAY:-http://localhost:8080}"
PAUSE=0
COLOR=1
MODE=happy
[ -n "${NO_COLOR:-}" ] && COLOR=0
[ -t 1 ] || COLOR=0

for arg in "$@"; do
  case "$arg" in
    --pause) PAUSE=1 ;;
    --no-color) COLOR=0 ;;
    timeout) MODE=timeout ;;
    happy) MODE=happy ;;
    -h | --help)
      sed -n '2,17p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'
      exit 0
      ;;
    *)
      echo "unknown argument: $arg (see --help)" >&2
      exit 2
      ;;
  esac
done

if [ "$COLOR" = 1 ]; then
  B=$'\033[1m'; DIM=$'\033[2m'; RED=$'\033[31m'; GRN=$'\033[32m'; YEL=$'\033[33m'; BLU=$'\033[34m'; CYN=$'\033[36m'; RST=$'\033[0m'
else
  B=""; DIM=""; RED=""; GRN=""; YEL=""; BLU=""; CYN=""; RST=""
fi

# ---------- tooling ----------
export PATH="$PATH:$HOME/go/bin:/c/Program Files/Go/bin"
for d in "${LOCALAPPDATA:+$(cygpath -u "$LOCALAPPDATA" 2>/dev/null)}"/Microsoft/WinGet/Packages/jqlang.jq_*; do
  [ -d "$d" ] && PATH="$PATH:$d"
done

fail() {
  echo "${RED}${B}DEMO FAILED:${RST}${RED} $*${RST}" >&2
  exit 1
}

command -v curl >/dev/null 2>&1 || fail "curl not found on PATH"
command -v jq >/dev/null 2>&1 || fail "jq not found on PATH (install: winget install jqlang.jq, then reopen the shell)"

TOKBIN=""
for c in "$ROOT/.run/bin/devtoken" "$ROOT/.run/bin/devtoken.exe"; do
  [ -x "$c" ] && TOKBIN="$c" && break
done
if [ -z "$TOKBIN" ]; then
  command -v go >/dev/null 2>&1 || fail "devtoken binary missing and Go not found; build it: (cd tools/devtoken && go build -o ../../.run/bin/devtoken .)"
  mkdir -p "$ROOT/.run/bin"
  (cd "$ROOT/tools/devtoken" && go build -o ../../.run/bin/devtoken .) || fail "could not build tools/devtoken"
  for c in "$ROOT/.run/bin/devtoken" "$ROOT/.run/bin/devtoken.exe"; do
    [ -x "$c" ] && TOKBIN="$c" && break
  done
fi

# One token per demo user, generated once up front (token() runs in command substitutions, so a
# lazily filled cache would be lost with each subshell and devtoken would be forked per request).
declare -A TOKEN
for u in user-1 user-2 user-3 user-4; do
  tok="$("$TOKBIN" "$u")" || fail "devtoken failed for $u"
  tok="$(printf '%s' "$tok" | tr -d '\r')"
  [ -n "$tok" ] || fail "devtoken printed no token for $u"
  TOKEN[$u]="$tok"
done
token() { printf '%s' "${TOKEN[$1]:?no token for $1}"; }

# ---------- output helpers ----------
STEP=0
step() {
  STEP=$((STEP + 1))
  if [ "$PAUSE" = 1 ] && [ "$STEP" -gt 1 ]; then
    printf '%s' "${DIM}  [press Enter to continue]${RST}"
    read -r _ </dev/tty 2>/dev/null || read -r _
    echo
  fi
  echo
  echo "${B}${BLU}== Step $STEP: $*${RST}"
}
say() { echo "   ${CYN}$*${RST}"; }
note() { echo "   ${DIM}$*${RST}"; }
kv() { echo "     ${GRN}$*${RST}"; }

# ---------- HTTP ----------
RESP=""
CODE=""
# call USER METHOD PATH [JSON_BODY] [EXPECTED_HTTP_STATUS=200]
# Prints "METHOD path", stores the body in $RESP, fails the demo on an unexpected status.
# Connection failures (curl code 000, nothing received) are retried a few times; 502/503/504 only for GET,
# because a POST may already have taken effect and must not be replayed.
call() {
  local user="$1" method="$2" path="$3" body="${4:-}" want="${5:-200}" quiet="${QUIET:-0}"
  [ "$quiet" = 1 ] || echo "   ${YEL}${user}${RST} ${B}${method} ${path}${RST}"
  local tmp attempt
  tmp="$(mktemp)"
  for attempt in $(seq 1 15); do
    [ -n "$body" ] || { [ "$method" = POST ] && body="{}"; }
    if [ -n "$body" ]; then
      CODE="$(curl -sS -o "$tmp" -w '%{http_code}' -X "$method" "$GATEWAY$path" \
        -H "Authorization: Bearer $(token "$user")" -H 'Content-Type: application/json' -d "$body" 2>/dev/null)"
    else
      CODE="$(curl -sS -o "$tmp" -w '%{http_code}' -X "$method" "$GATEWAY$path" \
        -H "Authorization: Bearer $(token "$user")" 2>/dev/null)"
    fi
    case "$CODE" in
      000) sleep 2 ;;
      502 | 503 | 504) if [ "$method" = GET ]; then sleep 2; else break; fi ;;
      *) break ;;
    esac
  done
  RESP="$(cat "$tmp")"
  rm -f "$tmp"
  if [ "$CODE" != "$want" ]; then
    fail "$user $method $path: expected HTTP $want, got $CODE: $RESP"
  fi
  [ "$quiet" = 1 ] || note "-> HTTP $CODE"
}

j() { printf '%s' "$RESP" | jq -r "$1" | tr -d '\r'; }

# poll DESC USER PATH JQ_BOOL_FILTER [TIMEOUT_SECS=30]
# GETs PATH quietly until the filter is true on the response (leaves it in $RESP); prints dots meanwhile.
poll() {
  local desc="$1" user="$2" path="$3" filter="$4" timeout="${5:-30}" t0=$SECONDS
  echo "   ${YEL}${user}${RST} ${B}GET ${path}${RST}  ${DIM}(polling until ${desc})${RST}"
  printf '     %s' "${DIM}"
  while :; do
    QUIET=1 call "$user" GET "$path"
    if [ "$(printf '%s' "$RESP" | jq -r "$filter" 2>/dev/null)" = true ]; then
      printf '%s\n' "${RST}"
      note "-> HTTP $CODE (after $((SECONDS - t0))s)"
      return 0
    fi
    if [ $((SECONDS - t0)) -ge "$timeout" ]; then
      printf '%s\n' "${RST}"
      fail "timed out after ${timeout}s waiting for ${desc}; last response: $RESP"
    fi
    printf '.'
    sleep 1
  done
}

# ---------- cleanup ----------
# soft_call USER METHOD PATH: one best-effort request that never exits the script (used by cleanup).
# Retries a few times on connection failure only; returns 0 on HTTP 2xx and leaves the body in $RESP.
soft_call() {
  local tmp attempt
  tmp="$(mktemp)"
  for attempt in 1 2 3; do
    CODE="$(curl -sS -m 10 -o "$tmp" -w '%{http_code}' -X "$2" "$GATEWAY$3"       -H "Authorization: Bearer $(token "$1")" -H 'Content-Type: application/json'       ${4:+-d "$4"} 2>/dev/null)" || CODE=000
    [ "$CODE" = 000 ] && { sleep 1; continue; }
    break
  done
  RESP="$(cat "$tmp")"
  rm -f "$tmp"
  case "$CODE" in 2??) return 0 ;; *) return 1 ;; esac
}

CREATED=() # "owner:adId"
cleanup() {
  local rc=$? e owner id hid=0 miss=0
  for e in "${CREATED[@]:-}"; do
    [ -n "$e" ] || continue
    owner="${e%%:*}"
    id="${e#*:}"
    if ! soft_call "$owner" GET "/v1/ads/$id"; then miss=$((miss + 1)); continue; fi
    if [ "$(j .status)" = AD_STATUS_PUBLISHED ]; then
      if soft_call "$owner" POST "/v1/ads/$id:hide" '{}'; then hid=$((hid + 1)); else miss=$((miss + 1)); fi
    fi
  done
  [ "$hid" -gt 0 ] && note "cleanup: hid $hid of the demo's ads that were still PUBLISHED"
  [ "$miss" -gt 0 ] && note "cleanup: $miss request(s) failed; some demo ads may still be PUBLISHED"
  return $rc
}
# fail() calls exit inside call(), whose subshell-free design lets this trap run.
trap cleanup EXIT

SUFFIX="$(head -c 3 /dev/urandom | od -An -tx1 | tr -d ' \n')"

publish_ad() { # USER TITLE HAVE WANT -> sets AD_ID, AD_VER
  local body
  body="$(jq -nc --arg t "$2 [$SUFFIX]" --arg h "$3" --arg w "$4" \
    '{title:$t, description:"created by scripts/demo.sh", haveCategory:$h, wantCategories:[$w], neighborhoodIds:["n-valiasr"], valueEstimate:"500000"}')"
  call "$1" POST /v1/ads "$body"
  AD_ID="$(j .id)"
  CREATED+=("$1:$AD_ID")
  call "$1" POST "/v1/ads/$AD_ID:publish"
  AD_VER="$(j .version)"
  kv "ad $AD_ID  \"$(j .spec.title)\"  status=$(j .status)  version=$AD_VER"
}

approvals_line() {
  printf '%s' "$RESP" | jq -r '[.approvals[]? | "\(.userId) \(.kind|sub("APPROVAL_KIND_";"")) v\(.target) valid=\(.valid // false)"] | join("; ")'
}

# Runs steps up to and including "swap AWAITING_PAYMENT" for user-1 (ad A) and user-2 (ad B).
# Sets: AD_A AD_B (ids), N2 (winning negotiation id), SWAP (swap id).
open_agree_and_lock() {
  local with_competitors="$1" banner_n=0
  step "Users publish ads (Envoy REST, one JWT per user)"
  say "user-1 offers books and wants tools; the others offer tools and want books."
  publish_ad user-1 "Cookbook collection" books tools; AD_A=$AD_ID; VER_A=$AD_VER
  publish_ad user-2 "Cordless drill" tools books; AD_B=$AD_ID; VER_B=$AD_VER
  if [ "$with_competitors" = 1 ]; then
    publish_ad user-3 "Hand saw" tools books; AD_C=$AD_ID; VER_C=$AD_VER
    publish_ad user-4 "Toolbox" tools books; AD_D=$AD_ID; VER_D=$AD_VER
  fi

  step "Matching finds the pair (index fed asynchronously through Kafka -> eventual consistency)"
  poll "user-2's ad appears in user-1's matches" user-1 "/v1/matching/ads/$AD_A/matches?limit=100" \
    "any(.candidates[]?; .ad.id == \"$AD_B\")"
  kv "candidates for user-1's ad: $(j '[.candidates[]? | "\(.ad.spec.title) (score \(.score))"] | join(", ")')"
  if [ "$with_competitors" = 1 ]; then
    poll "user-1's ad appears in user-3's matches" user-3 "/v1/matching/ads/$AD_C/matches?limit=100" \
      "any(.candidates[]?; .ad.id == \"$AD_A\")"
    say "MatchFound is just the index appearing here; no notifications in the MVP."
  fi

  if [ "$with_competitors" = 1 ]; then
    step "Three competing negotiations on user-1's ad (user-2, user-3, user-4 each send a request)"
    call user-2 POST /v1/negotiations "$(jq -nc --arg a "$AD_B" --arg b "$AD_A" '{requesterAdId:$a, targetAdId:$b}')"
    N2="$(j .id)"; kv "user-2 -> negotiation $N2 status=$(j .status) proposal v$(j .activeProposal.number)"
    call user-3 POST /v1/negotiations "$(jq -nc --arg a "$AD_C" --arg b "$AD_A" '{requesterAdId:$a, targetAdId:$b}')"
    N3="$(j .id)"; kv "user-3 -> negotiation $N3 status=$(j .status) proposal v$(j .activeProposal.number)"
    call user-4 POST /v1/negotiations "$(jq -nc --arg a "$AD_D" --arg b "$AD_A" '{requesterAdId:$a, targetAdId:$b}')"
    N4="$(j .id)"; kv "user-4 -> negotiation $N4 status=$(j .status) proposal v$(j .activeProposal.number)"
  else
    step "user-2 opens a negotiation on user-1's ad"
    call user-2 POST /v1/negotiations "$(jq -nc --arg a "$AD_B" --arg b "$AD_A" '{requesterAdId:$a, targetAdId:$b}')"
    N2="$(j .id)"; kv "negotiation $N2 status=$(j .status) proposal v$(j .activeProposal.number)"
  fi

  step "user-1 approves each requester's ad (approval is bound to the ad VERSION that was seen)"
  call user-1 POST "/v1/negotiations/$N2:approve-ad" "$(jq -nc --argjson v "$VER_B" '{adVersion:$v}')"
  kv "approvals: $(approvals_line)"
  if [ "$with_competitors" = 1 ]; then
    call user-1 POST "/v1/negotiations/$N3:approve-ad" "$(jq -nc --argjson v "$VER_C" '{adVersion:$v}')"
    call user-1 POST "/v1/negotiations/$N4:approve-ad" "$(jq -nc --argjson v "$VER_D" '{adVersion:$v}')"
  fi

  step "Versioned approvals: user-2 approves proposal v1, then user-1 revises -> the old approval goes stale"
  call user-2 POST "/v1/negotiations/$N2:approve-proposal" '{"proposalNumber":1}'
  kv "approvals: $(approvals_line)"
  call user-1 POST "/v1/negotiations/$N2:revise" \
    "$(jq -nc '{seenProposalNumber:1, terms:{legA:"DELIVERY_METHOD_LOCKER", legB:"DELIVERY_METHOD_LOCKER", priceDifference:"50000", payerUserId:"user-2"}}')"
  kv "active proposal is now v$(j .activeProposal.number): legA=$(j .activeProposal.terms.legA) legB=$(j .activeProposal.terms.legB) price=$(j '.activeProposal.terms.priceDifference') payer=$(j .activeProposal.terms.payerUserId)"
  kv "approvals: $(approvals_line)"
  local stale
  stale="$(j '[.approvals[]? | select(.kind=="APPROVAL_KIND_TERMS" and .target==1 and .userId=="user-2" and ((.valid // false)==false))] | length')"
  [ "$stale" = 1 ] || fail "expected user-2's TERMS approval of v1 to be stale (valid=false) after the revision; got: $(approvals_line)"
  say "user-2's approval of v1 is stale (valid=false): approvals are only valid while their target is current."
  say "Approving the superseded v1 again is refused (ABORTED -> HTTP 409):"
  call user-2 POST "/v1/negotiations/$N2:approve-proposal" '{"proposalNumber":1}' 409
  kv "$(j '.message // .')"

  step "user-2 approves v2: the fourth valid approval -> AgreementReached (asynchronous)"
  call user-2 POST "/v1/negotiations/$N2:approve-proposal" '{"proposalNumber":2}'
  kv "negotiation status right after the call: $(j .status)  (agreement is processed asynchronously)"
  poll "negotiation is AGREED" user-2 "/v1/negotiations/$N2" '.status == "NEGOTIATION_STATUS_AGREED"' 40
  kv "negotiation $N2 -> $(j .status)"

  step "Swap service takes the atomic lock on both ads (saga: ad.LockAds in ONE DB transaction)"
  poll "swap for the negotiation is AWAITING_PAYMENT" user-1 "/v1/swaps?pageSize=200" \
    "any(.swaps[]?; .negotiationId == \"$N2\" and .status == \"SWAP_STATUS_AWAITING_PAYMENT\")" 40
  SWAP="$(j ".swaps[] | select(.negotiationId == \"$N2\") | .id")"
  SWAP_JSON="$(j ".swaps[] | select(.negotiationId == \"$N2\")")"
  call user-1 GET "/v1/swaps/$SWAP"
  kv "swap $SWAP status=$(j .status)"
  kv "legA: owner=$(j .legA.ownerUserId) method=$(j .legA.method) feePaid=$(j '.legA.feePaid // false')"
  kv "legB: owner=$(j .legB.ownerUserId) method=$(j .legB.method) feePaid=$(j '.legB.feePaid // false')"
  kv "paymentDeadline=$(j .paymentDeadline)"
  DEADLINE="$(j .paymentDeadline)"
  poll "user-1's ad is LOCKED" user-1 "/v1/ads/$AD_A" '.status == "AD_STATUS_LOCKED"'
  kv "ad A $AD_A -> $(j .status)"
  poll "user-2's ad is LOCKED" user-2 "/v1/ads/$AD_B" '.status == "AD_STATUS_LOCKED"'
  kv "ad B $AD_B -> $(j .status)"

  if [ "$with_competitors" = 1 ]; then
    step "Race handling: the competing negotiations on the locked ad are cancelled by the system"
    local n who
    for pair in "user-3:$N3" "user-4:$N4"; do
      who="${pair%%:*}"; n="${pair#*:}"
      poll "$who's competing negotiation is CANCELLED" "$who" "/v1/negotiations/$n" '.status == "NEGOTIATION_STATUS_CANCELLED"' 40
      kv "$who negotiation $n -> $(j .status)  reason: $(j '.cancelReason // ""')"
    done
    poll "locked ads left user-3's match index" user-3 "/v1/matching/ads/$AD_C/matches?limit=100" \
      "(any(.candidates[]?; .ad.id == \"$AD_A\" or .ad.id == \"$AD_B\")) | not"
    QUIET=1 call user-3 GET "/v1/ads/$AD_C"
    kv "user-3's ad is still: $(j .status)"
  fi
}

# Locker legs of the current swap JSON ($RESP) as "owner" lines.
locker_owners() {
  printf '%s' "$RESP" | jq -r '[.legA, .legB][] | select(.method == "DELIVERY_METHOD_LOCKER") | .ownerUserId' | tr -d '\r'
}

preflight() {
  local code
  code="$(curl -s -o /dev/null -w '%{http_code}' "$GATEWAY/v1/ads" 2>/dev/null || true)"
  case "$code" in
    401) ;; # reachable, unauthenticated -> as expected
    000 | "") fail "Envoy is not reachable at $GATEWAY. Start the stack: make up && scripts/dev.sh start (see scripts/dev.sh status)" ;;
  esac
  echo "${B}Taakht MVP demo (${MODE})${RST}  gateway=$GATEWAY  run=$SUFFIX"
  if [ "$code" = 401 ]; then note "gateway rejects unauthenticated calls with 401 (JWT enforced at the edge)"; fi
}

# ---------- scenarios ----------
happy() {
  open_agree_and_lock 1

  step "Locker fees: the locker partner is mocked; each leg owner reports the fee (dev endpoint)"
  call user-1 GET "/v1/swaps/$SWAP"
  local owners o count=0
  owners="$(locker_owners)"
  for o in $owners; do
    count=$((count + 1))
    call "$o" POST "/v1/dev/swaps/$SWAP/locker-fee-paid:simulate" "$(jq -nc --arg u "$o" '{userId:$u}')"
    kv "after $o paid: swap status=$(j .status) legA.feePaid=$(j '.legA.feePaid // false') legB.feePaid=$(j '.legB.feePaid // false')"
  done
  [ "$count" -gt 0 ] || fail "swap has no locker legs; nothing to pay"

  step "Completion: SwapCompleted closes both ads"
  poll "swap is COMPLETED" user-1 "/v1/swaps/$SWAP" '.status == "SWAP_STATUS_COMPLETED"' 40
  kv "swap $SWAP -> $(j .status)"
  poll "ad A CLOSED" user-1 "/v1/ads/$AD_A" '.status == "AD_STATUS_CLOSED"'
  kv "ad A -> $(j .status)"
  poll "ad B CLOSED" user-2 "/v1/ads/$AD_B" '.status == "AD_STATUS_CLOSED"'
  kv "ad B -> $(j .status)"

  step "Summary: ads of the losing requesters stay available"
  call user-1 GET "/v1/negotiations?adId=$AD_A"
  printf '%s' "$RESP" | jq -r '.negotiations[] | "     \(.requesterUserId)  \(.status)  \(.cancelReason // "")"' | sed "s/^/${GRN}/;s/\$/${RST}/"
  call user-3 GET "/v1/ads/$AD_C"
  kv "user-3's ad $AD_C is $(j .status) (will be hidden at the end of the demo)"

  echo
  echo "${GRN}${B}DEMO OK: published -> matched -> 3 negotiations -> stale approval -> agreement -> locked -> paid -> COMPLETED, ads CLOSED, rivals CANCELLED.${RST}"
}

timeout_variant() {
  open_agree_and_lock 0

  step "Payment timeout: the swap deadline decides whether we can wait here"
  local dl_epoch now remain
  dl_epoch="$(date -u -d "$DEADLINE" +%s 2>/dev/null)" || fail "cannot parse paymentDeadline '$DEADLINE'"
  now="$(date -u +%s)"
  remain=$((dl_epoch - now))
  kv "paymentDeadline=$DEADLINE  (in ${remain}s)"
  if [ "$remain" -gt 90 ]; then
    fail "payment deadline is ${remain}s away (swap uses the default). Restart the swap service with a short deadline and re-run:
    PAYMENT_DEADLINE=20s scripts/dev.sh restart swap
    scripts/demo.sh timeout
  Restore afterwards with: scripts/dev.sh restart swap"
  fi

  say "user-2 pays their locker fee, user-1 does NOT."
  call user-2 POST "/v1/dev/swaps/$SWAP/locker-fee-paid:simulate" '{"userId":"user-2"}'
  kv "swap status=$(j .status) legA.feePaid=$(j '.legA.feePaid // false') legB.feePaid=$(j '.legB.feePaid // false')"

  step "Waiting for the deadline: the swap service cancels the swap and releases the ads (compensation)"
  [ "$remain" -gt 0 ] && note "sleeping about ${remain}s until the deadline..."
  poll "swap is CANCELLED" user-1 "/v1/swaps/$SWAP" '.status == "SWAP_STATUS_CANCELLED"' $((remain + 60))
  kv "swap $SWAP -> $(j .status)  reason: $(j '.cancelReason // ""')"

  step "Compensation: the ads are released (LOCKED -> PUBLISHED) and re-enter the match index"
  poll "ad A PUBLISHED again" user-1 "/v1/ads/$AD_A" '.status == "AD_STATUS_PUBLISHED"' 40
  kv "ad A -> $(j .status)"
  poll "ad B PUBLISHED again" user-2 "/v1/ads/$AD_B" '.status == "AD_STATUS_PUBLISHED"' 40
  kv "ad B -> $(j .status)"
  poll "user-2's ad is back in user-1's matches" user-1 "/v1/matching/ads/$AD_A/matches?limit=100" \
    "any(.candidates[]?; .ad.id == \"$AD_B\")" 40
  kv "candidates for user-1's ad: $(j '[.candidates[]? | .ad.spec.title] | join(", ")')"
  call user-2 GET "/v1/negotiations/$N2"
  kv "negotiation $N2 -> $(j .status)"

  echo
  echo "${GRN}${B}DEMO OK (timeout): locker fee unpaid -> swap CANCELLED -> ads PUBLISHED again and back in matching.${RST}"
}

preflight
case "$MODE" in
  happy) happy ;;
  timeout) timeout_variant ;;
esac
