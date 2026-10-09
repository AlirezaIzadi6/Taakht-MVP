#!/usr/bin/env bash
# Edge (Envoy) tests against the running gateway on :8080 (needs `make dev`: infra + services; curl, jq, openssl).
# Usage: scripts/test-gateway.sh        (make gateway-test)
#
# Covers: 401/403 auth, forged system: tokens, spoofed identity headers, request ids, 1 MiB body limit,
# CORS preflight, JSON 404 for unknown paths, the /v1/dev/ switch, JWT key rotation (multi-key JWKS), GET-only
# retries and the config validating in Envoy. The last sections restart Envoy and (retries) stop and restart the
# matching service; the original Envoy config is restored on exit. Skip them with GATEWAY_TEST_DISRUPTIVE=0.
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
GATEWAY="${GATEWAY:-http://127.0.0.1:8080}"
ADMIN="${ADMIN:-http://127.0.0.1:9901}"
COMPOSE=(docker compose -f deploy/docker-compose.yml)
DISRUPTIVE="${GATEWAY_TEST_DISRUPTIVE:-1}"
ORIGIN_OK="http://localhost:3000"
ORIGIN_BAD="http://evil.example"
DEV_KEY="taakht-dev-secret-0123456789abcdef"

add_path() { [ -d "$1" ] && case ":$PATH:" in *":$1:"*) ;; *) PATH="$PATH:$1" ;; esac; }
add_path "$HOME/go/bin"
add_path "/c/Program Files/Go/bin"
LOCALAPPS="$(cygpath -u "${LOCALAPPDATA:-$HOME/AppData/Local}" 2>/dev/null || echo "$HOME/AppData/Local")"
for d in "$LOCALAPPS"/Microsoft/WinGet/Packages/jqlang.jq_*; do add_path "$d"; done
export PATH
for t in curl jq openssl; do command -v "$t" >/dev/null || { echo "missing tool: $t" >&2; exit 2; }; done

mkdir -p .run/bin
(cd tools/devtoken && go build -o ../../.run/bin/devtoken .) || { echo "cannot build tools/devtoken" >&2; exit 2; }
DEVTOKEN=".run/bin/devtoken"
[ -x "$DEVTOKEN" ] || DEVTOKEN=".run/bin/devtoken.exe"
tok() { "$DEVTOKEN" "$1" | tr -d '\r\n'; }

TMP="$(mktemp -d)"
PASS=0
FAIL=0
ok() { PASS=$((PASS + 1)); echo "  ok    $*"; }
bad() { FAIL=$((FAIL + 1)); echo "  FAIL  $*"; }
check() { # DESCRIPTION ACTUAL EXPECTED
  if [ "$2" = "$3" ]; then ok "$1 ($2)"; else bad "$1: got '$2', want '$3'"; fi
}
section() { echo; echo "== $*"; }

# req METHOD PATH [curl args...] -> CODE (HTTP status), $TMP/body, $TMP/hdr
CODE=""
req() {
  local m="$1" p="$2"
  shift 2
  CODE="$(curl -s -o "$TMP/body" -D "$TMP/hdr" -w '%{http_code}' -X "$m" "$GATEWAY$p" "$@")"
}
hdr() { grep -i "^$1:" "$TMP/hdr" | head -1 | cut -d: -f2- | tr -d '\r' | sed 's/^ *//'; }
jcode() { jq -r '.code // empty' "$TMP/body" 2>/dev/null; }
b64url() { openssl base64 -A | tr '+/' '-_' | tr -d '='; }
# forge SUB [KEY] [KID]: an HS256 token with the right issuer/audience, signed with KEY (default: the dev key).
forge() {
  local sub="$1" key="${2:-$DEV_KEY}" kid="${3:-dev-1}" h c s
  h="$(printf '{"alg":"HS256","typ":"JWT","kid":"%s"}' "$kid" | b64url)"
  c="$(printf '{"iss":"taakht-dev","aud":"taakht-api","sub":"%s","iat":%s,"exp":%s}' "$sub" "$(date +%s)" "$(($(date +%s) + 3600))" | b64url)"
  s="$(printf '%s.%s' "$h" "$c" | openssl dgst -sha256 -hmac "$key" -binary | b64url)"
  printf '%s.%s.%s' "$h" "$c" "$s"
}

wait_ready() { # wait until /readyz is 200 (Envoy up and every upstream healthy)
  local i
  for i in $(seq 1 60); do
    [ "$(curl -s -o /dev/null -w '%{http_code}' "$GATEWAY/readyz" 2>/dev/null)" = 200 ] && return 0
    sleep 1
  done
  return 1
}
restart_envoy() {
  "${COMPOSE[@]}" restart envoy >/dev/null 2>&1 || return 1
  wait_ready
}
render() { env "$@" scripts/gen-envoy-config.sh >/dev/null 2>&1; } # render NAME=value...

# Remember the running config so it can be put back whatever happens.
cp deploy/envoy/envoy.yaml "$TMP/envoy.yaml.orig" 2>/dev/null
cp deploy/envoy/jwks.json "$TMP/jwks.json.orig" 2>/dev/null
RESTORE_ENVOY=0
MATCHING_KILLED=0
cleanup() {
  if [ "$MATCHING_KILLED" = 1 ]; then scripts/dev.sh start matching >/dev/null 2>&1; fi
  if [ "$RESTORE_ENVOY" = 1 ]; then
    cat "$TMP/envoy.yaml.orig" >deploy/envoy/envoy.yaml
    cat "$TMP/jwks.json.orig" >deploy/envoy/jwks.json
    restart_envoy || echo "WARN: envoy did not become ready after restoring its config" >&2
  fi
  rm -rf "$TMP"
}
trap cleanup EXIT

section "health"
check "/healthz" "$(curl -s -o /dev/null -w '%{http_code}' "$GATEWAY/healthz")" 200
wait_ready && ok "/readyz is 200 (every cluster has healthy upstreams)" || bad "/readyz did not become 200 (are all four services up?)"

section "authentication"
req GET /v1/ads
check "no token -> 401" "$CODE" 401
check "401 body is JSON {code:16}" "$(jcode)" 16
req GET /v1/ads -H "Authorization: Bearer not.a.jwt"
check "garbage token -> 401" "$CODE" 401
req GET /v1/ads -H "Authorization: Bearer $(forge user-1 "some-other-key-that-is-long-enough-123456")"
check "token signed with another key -> 401" "$CODE" 401
req GET /v1/ads -H "Authorization: Bearer $(tok user-1)"
check "valid token: GET /v1/ads -> 200" "$CODE" 200
check "response carries x-request-id" "$([ -n "$(hdr x-request-id)" ] && echo yes || echo no)" yes

section "forged system identities (token signed with the dev key)"
if [ -z "${JWT_SIGNING_KEY:-}" ]; then
  for sub in system:swap system:negotiation SYSTEM:SWAP System:x; do
    req GET /v1/swaps -H "Authorization: Bearer $(forge "$sub")"
    check "sub=$sub -> 403" "$CODE" 403
  done
  req GET /v1/swaps -H "Authorization: Bearer $(forge "")"
  check "token with empty sub -> 403" "$CODE" 403
else
  echo "  skip  JWT_SIGNING_KEY is set; forging with the public dev key would only prove 401"
fi
req GET /v1/swaps -H "Authorization: Bearer $(forge user-1)"
check "script-forged token for a normal user is accepted (sanity: the forge helper signs correctly)" "$CODE" 200

section "spoofed headers are ignored"
req GET /v1/ads -H "x-user-id: user-1" -H "x-internal-token: dev-internal-token"
check "spoofed identity headers without a JWT -> 401" "$CODE" 401
req POST /v1/ads -H "Authorization: Bearer $(tok user-1)" -H "x-user-id: user-2" -H "x-internal-token: dev-internal-token" \
  -H 'Content-Type: application/json' -d '{"title":"gateway-test spoof","description":"x","haveCategory":"books","wantCategories":["tools"],"neighborhoodIds":["n-valiasr"],"valueEstimate":"1"}'
check "POST /v1/ads with spoofed x-user-id: user-2 -> 200" "$CODE" 200
check "the ad belongs to the JWT subject, not the header" "$(jq -r '.ownerId // .owner_id // empty' "$TMP/body")" user-1
AD="$(jq -r '.id // empty' "$TMP/body")"
req GET /v1/swaps -H "Authorization: Bearer $(tok user-1)" -H "x-user-id: system:swap" -H "x-internal-token: dev-internal-token"
check "spoofed x-user-id: system:swap next to a user JWT is dropped (200, not 403/401)" "$CODE" 200
[ -n "$AD" ] && req POST "/v1/ads/$AD:hide" -H "Authorization: Bearer $(tok user-1)" -H 'Content-Type: application/json' -d '{}'

section "request ids"
req GET /v1/ads -H "Authorization: Bearer $(tok user-1)" -H "x-request-id: gateway-test-0001"
check "a client-sent x-request-id is kept" "$(hdr x-request-id)" gateway-test-0001

section "body size limit (1 MiB)"
head -c 1500000 /dev/zero | tr '\0' 'a' | sed 's/^/{"title":"/; s/$/"}/' >"$TMP/big.json"
req POST /v1/ads -H "Authorization: Bearer $(tok user-1)" -H 'Content-Type: application/json' --data-binary @"$TMP/big.json"
check "1.5 MB body -> 413" "$CODE" 413
check "413 body is JSON {code:8}" "$(jcode)" 8
head -c 200000 /dev/zero | tr '\0' 'a' | sed 's/^/{"title":"/; s/$/"}/' >"$TMP/mid.json"
req POST /v1/ads -H "Authorization: Bearer $(tok user-1)" -H 'Content-Type: application/json' --data-binary @"$TMP/mid.json"
check "200 KB body is not rejected by the edge (service answers 400)" "$([ "$CODE" != 413 ] && echo passed || echo 413)" passed

section "CORS"
req OPTIONS /v1/ads -H "Origin: $ORIGIN_OK" -H "Access-Control-Request-Method: POST" -H "Access-Control-Request-Headers: authorization,content-type"
echo "  info  preflight status from an allowed origin: $CODE"
check "preflight from an allowed origin succeeds without a JWT" "$CODE" 204
check "allow-origin echoes the origin" "$(hdr access-control-allow-origin)" "$ORIGIN_OK"
check "allow-methods present" "$([ -n "$(hdr access-control-allow-methods)" ] && echo yes || echo no)" yes
check "allow-headers include authorization" "$(hdr access-control-allow-headers | grep -ci authorization)" 1
check "credentials are off" "$(hdr access-control-allow-credentials)" ""
req OPTIONS /v1/ads -H "Origin: $ORIGIN_BAD" -H "Access-Control-Request-Method: POST"
check "preflight from a disallowed origin: no allow-origin" "$(hdr access-control-allow-origin)" ""
echo "  info  preflight status from a disallowed origin: $CODE"
req GET /v1/ads -H "Origin: $ORIGIN_OK" -H "Authorization: Bearer $(tok user-1)"
check "actual request: allow-origin on the response" "$(hdr access-control-allow-origin)" "$ORIGIN_OK"
check "actual request: x-request-id is exposed" "$(hdr access-control-expose-headers | grep -ci x-request-id)" 1
req GET /v1/ads -H "Origin: $ORIGIN_BAD" -H "Authorization: Bearer $(tok user-1)"
check "actual request from a disallowed origin: no allow-origin" "$(hdr access-control-allow-origin)" ""

section "unknown paths"
req GET /nope
check "GET /nope -> 404" "$CODE" 404
check "body {code:5, message:'not found'}" "$(jq -c '[.code,.message]' "$TMP/body")" '[5,"not found"]'
check "content-type is JSON" "$(hdr content-type | grep -ci json)" 1
for p in /v1/ads/a/b/c /v1/matching/nope /v1/negotiations/x/y /v1/swaps/x/y/z; do
  req GET "$p" -H "Authorization: Bearer $(tok user-1)"
  check "GET $p -> 404 JSON" "$CODE/$(jcode)" "404/5"
done
req DELETE /v1/ads -H "Authorization: Bearer $(tok user-1)"
check "unsupported verb on a known path is not a 415" "$([ "$CODE" != 415 ] && echo ok || echo 415)" ok

if [ "$DISRUPTIVE" != 1 ]; then
  echo
  echo "(GATEWAY_TEST_DISRUPTIVE=0: skipping config validation, the dev-route switch, key rotation and retries)"
else
  RESTORE_ENVOY=1

  section "config validates in Envoy"
  if docker image inspect envoyproxy/envoy:v1.34.14 >/dev/null 2>&1; then
    for devflag in false true; do
      render ENVOY_ENABLE_DEV_ROUTES=$devflag
      W="$(pwd -W 2>/dev/null || pwd)"
      out="$(MSYS_NO_PATHCONV=1 docker run --rm -v "$W/deploy/envoy/envoy.yaml:/etc/envoy/envoy.yaml:ro" \
        -v "$W/deploy/envoy/jwks.json:/etc/envoy/jwks.json:ro" \
        -v "$W/gateway/descriptor.binpb:/etc/envoy/descriptor.binpb:ro" \
        envoyproxy/envoy:v1.34.14 --mode validate -c /etc/envoy/envoy.yaml 2>&1 | grep -E "OK$|rror" | head -1)"
      case "$out" in *OK*) ok "envoy --mode validate (ENVOY_ENABLE_DEV_ROUTES=$devflag)" ;; *) bad "envoy --mode validate (dev=$devflag): $out" ;; esac
    done
  else
    echo "  skip  envoy image not present locally"
  fi

  section "dev route /v1/dev/ is off unless ENVOY_ENABLE_DEV_ROUTES=true"
  DEVBODY='{"userId":"user-1"}'
  render ENVOY_ENABLE_DEV_ROUTES=false && restart_envoy || bad "could not restart envoy"
  req POST "/v1/dev/swaps/none/locker-fee-paid:simulate" -H "Authorization: Bearer $(tok user-1)" -H 'Content-Type: application/json' -d "$DEVBODY"
  check "default: blocked with the edge's JSON 404" "$CODE/$(jcode)" "404/5"
  check "default: never reached an upstream" "$(hdr x-envoy-upstream-service-time)" ""
  render ENVOY_ENABLE_DEV_ROUTES=true && restart_envoy || bad "could not restart envoy"
  req POST "/v1/dev/swaps/none/locker-fee-paid:simulate" -H "Authorization: Bearer $(tok user-1)" -H 'Content-Type: application/json' -d "$DEVBODY"
  check "enabled: the request reaches the swap service" "$([ -n "$(hdr x-envoy-upstream-service-time)" ] && echo yes || echo no)" yes

  section "JWT key rotation (multi-key JWKS with kid)"
  NEWKEY="rotated-key-$(openssl rand -hex 16)"
  render JWT_SIGNING_KEY="$NEWKEY" JWT_KEY_ID=k2 JWT_SIGNING_KEY_PREVIOUS="$DEV_KEY" JWT_KEY_ID_PREVIOUS=dev-1 ENVOY_ENABLE_DEV_ROUTES=true && restart_envoy || bad "could not restart envoy"
  OLD="$(tok user-1)"
  NEW="$(JWT_SIGNING_KEY="$NEWKEY" JWT_KEY_ID=k2 tok user-1)"
  req GET /v1/ads -H "Authorization: Bearer $NEW"
  check "rotation window: token signed with the new key (kid k2) -> 200" "$CODE" 200
  req GET /v1/ads -H "Authorization: Bearer $OLD"
  check "rotation window: token signed with the previous key -> 200" "$CODE" 200
  req GET /v1/ads -H "Authorization: Bearer $(forge user-1 "a-third-key-that-is-also-long-enough-99" k3)"
  check "rotation window: token signed with an unknown key -> 401" "$CODE" 401
  render JWT_SIGNING_KEY="$NEWKEY" JWT_KEY_ID=k2 ENVOY_ENABLE_DEV_ROUTES=true && restart_envoy || bad "could not restart envoy"
  req GET /v1/ads -H "Authorization: Bearer $OLD"
  check "after the window (previous key dropped): old token -> 401" "$CODE" 401
  req GET /v1/ads -H "Authorization: Bearer $NEW"
  check "after the window: new token -> 200" "$CODE" 200
  # Back to the original config (default dev key) before the retry checks.
  cat "$TMP/envoy.yaml.orig" >deploy/envoy/envoy.yaml
  cat "$TMP/jwks.json.orig" >deploy/envoy/jwks.json
  restart_envoy || bad "could not restart envoy with the original config"

  section "retries: GET yes, POST no (matching service stopped)"
  port=9002
  retry_count() { curl -s "$ADMIN/stats?filter=cluster.matching_service.upstream_rq_retry\$" | awk -F': ' '/upstream_rq_retry:/ {print $2; exit}'; }
  pid="$(netstat -ano -p tcp 2>/dev/null | tr -d '\r' | awk -v p=":$port" '$1=="TCP" && $4=="LISTENING" && substr($2, length($2)-length(p)+1)==p {print $5; exit}')"
  if [ -z "$pid" ]; then
    echo "  skip  matching is not listening on :$port"
  else
    MATCHING_KILLED=1
    taskkill //PID "$pid" //T //F >/dev/null 2>&1
    for _ in $(seq 1 20); do (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null || break; sleep 0.5; done
    before="$(retry_count)"
    req GET /v1/matching/ads/none/matches -H "Authorization: Bearer $(tok user-1)"
    after_get="$(retry_count)"
    check "GET to a dead upstream -> 503" "$CODE" 503
    check "GET was retried twice (upstream_rq_retry +2)" "$((after_get - before))" 2
    req POST /v1/matching/search -H "Authorization: Bearer $(tok user-1)" -H 'Content-Type: application/json' -d '{"criteria":{},"limit":1}'
    after_post="$(retry_count)"
    check "POST to a dead upstream -> 503" "$CODE" 503
    check "POST was NOT retried (upstream_rq_retry +0)" "$((after_post - after_get))" 0
    scripts/dev.sh start matching >/dev/null 2>&1 && MATCHING_KILLED=0
    wait_ready && ok "matching restarted, /readyz is 200 again" || bad "/readyz did not recover after restarting matching"
  fi
fi

echo
echo "gateway tests: $PASS passed, $FAIL failed"
[ "$FAIL" -eq 0 ]
