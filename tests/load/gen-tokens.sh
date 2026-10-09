#!/usr/bin/env bash
# Writes tests/load/.tokens.json ({"user-1": "<jwt>", ...}) for k6, which cannot exec processes.
# Tokens come from tools/devtoken (dev JWTs for the Envoy edge). The file is git-ignored.
set -euo pipefail
export PATH="$PATH:/c/Users/USER/go/bin:/c/Program Files/Go/bin"
HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT="$(cd "$HERE/../.." && pwd)"

# Always rebuild: a stale binary mints tokens the edge rejects (403, audience claim).
mkdir -p "$ROOT/.run/bin"
(cd "$ROOT/tools/devtoken" && go build -o ../../.run/bin/devtoken .)
BIN=""
for c in "$ROOT/.run/bin/devtoken" "$ROOT/.run/bin/devtoken.exe"; do
  [ -x "$c" ] && BIN="$c" && break
done
[ -n "$BIN" ] || { echo "devtoken binary not found" >&2; exit 1; }

out="$HERE/.tokens.json"
{
  printf '{'
  sep=""
  for u in user-1 user-2 user-3 user-4; do
    printf '%s"%s":"%s"' "$sep" "$u" "$("$BIN" "$u" | tr -d '\r\n')"
    sep=","
  done
  printf '}\n'
} >"$out"
echo "wrote $out"
