#!/usr/bin/env bash
# Renders the Envoy edge config for docker compose into deploy/envoy/ (git-ignored):
#   jwks.json  the HS256 key set Envoy validates JWTs against, built from JWT_SIGNING_KEY
#   envoy.yaml gateway/envoy.yaml with the CORS origin list and the dev route filled in
# Run by `make up`. For plain `docker compose -f deploy/docker-compose.yml up` run this script once first
# (compose bind-mounts both files; without them Docker would create empty directories in their place).
# Prints "updated" when a file changed (the caller should then restart Envoy), else "unchanged".
#
# Environment (all optional):
#   JWT_SIGNING_KEY            HS256 secret, at least 32 characters. Default: the public DEV key (also the
#                              default of tools/devtoken). Set a random one in any shared environment, e.g.
#                              JWT_SIGNING_KEY=$(openssl rand -base64 48)
#   JWT_KEY_ID                 kid of that key (default dev-1); tools/devtoken puts it in the token header
#   JWT_SIGNING_KEY_PREVIOUS   old key kept in the set during rotation (tokens signed with it stay valid)
#   JWT_KEY_ID_PREVIOUS        its kid (default: previous)
#   ENVOY_CORS_ORIGINS         comma-separated allowed browser origins (default http://localhost:3000,http://127.0.0.1:3000)
#   ENVOY_ENABLE_DEV_ROUTES    "true" keeps the dev-only /v1/dev/ route (locker-fee simulation); otherwise it is removed
set -euo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
OUT="$ROOT/deploy/envoy"
DEV_KEY="taakht-dev-secret-0123456789abcdef"
KEY="${JWT_SIGNING_KEY:-$DEV_KEY}"
KID="${JWT_KEY_ID:-dev-1}"
PREV="${JWT_SIGNING_KEY_PREVIOUS:-}"
PREV_KID="${JWT_KEY_ID_PREVIOUS:-previous}"
ORIGINS="${ENVOY_CORS_ORIGINS:-http://localhost:3000,http://127.0.0.1:3000}"
DEV_ROUTES="${ENVOY_ENABLE_DEV_ROUTES:-false}"

die() { echo "gen-envoy-config: $*" >&2; exit 1; }

check_key() { [ "${#1}" -ge 32 ] || die "$2 must be at least 32 characters (HS256)"; }
check_kid() { [[ "$1" =~ ^[A-Za-z0-9._-]{1,64}$ ]] || die "invalid key id '$1' (use letters, digits, . _ -)"; }
b64url() { printf '%s' "$1" | base64 | tr -d '\r\n=' | tr '+/' '-_'; }
jwk() { printf '{"kty":"oct","alg":"HS256","use":"sig","kid":"%s","k":"%s"}' "$1" "$(b64url "$2")"; }

check_key "$KEY" JWT_SIGNING_KEY
check_kid "$KID"
keys="$(jwk "$KID" "$KEY")"
if [ -n "$PREV" ]; then
  check_key "$PREV" JWT_SIGNING_KEY_PREVIOUS
  check_kid "$PREV_KID"
  [ "$PREV_KID" != "$KID" ] || die "JWT_KEY_ID_PREVIOUS must differ from JWT_KEY_ID"
  keys="$keys,$(jwk "$PREV_KID" "$PREV")"
fi
[ "$KEY" != "$DEV_KEY" ] || echo "gen-envoy-config: using the public DEV signing key; set JWT_SIGNING_KEY in any shared environment" >&2

origins_yaml=""
IFS=',' read -ra arr <<<"$ORIGINS"
for o in "${arr[@]}"; do
  o="${o// /}"
  [ -n "$o" ] || continue
  [[ "$o" =~ ^https?://[A-Za-z0-9._-]+(:[0-9]+)?$ ]] || die "invalid origin '$o' in ENVOY_CORS_ORIGINS (want scheme://host[:port], no wildcard, no path)"
  origins_yaml+="                            - { exact: \"$o\" }"$'\n'
done
[ -n "$origins_yaml" ] || die "ENVOY_CORS_ORIGINS has no origin"

case "$DEV_ROUTES" in true | false) ;; *) die "ENVOY_ENABLE_DEV_ROUTES must be true or false" ;; esac

mkdir -p "$OUT"
tmp="$(mktemp)"
trap 'rm -f "$tmp" "$tmp.2"' EXIT

printf '{"keys":[%s]}\n' "$keys" >"$tmp"
status=unchanged
# Write in place (same inode): compose bind-mounts these single files.
if ! cmp -s "$tmp" "$OUT/jwks.json" 2>/dev/null; then cat "$tmp" >"$OUT/jwks.json"; status=updated; fi

awk -v dev="$DEV_ROUTES" -v origins="$origins_yaml" '
  /@cors-origins-begin/ { print; printf "%s", origins; skip = 1; next }
  /@cors-origins-end/   { skip = 0 }
  skip { next }
  /@dev-routes/ && dev != "true" { next }
  { print }
' "$ROOT/gateway/envoy.yaml" >"$tmp.2"
if ! cmp -s "$tmp.2" "$OUT/envoy.yaml" 2>/dev/null; then cat "$tmp.2" >"$OUT/envoy.yaml"; status=updated; fi

echo "$status"
