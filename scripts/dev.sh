#!/usr/bin/env bash
# Run the whole Taakht MVP locally: infra via docker compose, four services on the host.
# Usage: scripts/dev.sh start [svc...] | stop | status | logs <svc> | restart [svc]
set -uo pipefail

ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$ROOT"
WROOT="$(pwd -W 2>/dev/null || pwd)" # Windows-style path (D:/...), understood by Go and .NET
RUN="$ROOT/.run"
BIN="$RUN/bin"
PIDS="$RUN/pids"
LOGS="$RUN/logs"
mkdir -p "$BIN" "$PIDS" "$LOGS"

export PATH="$PATH:/c/Users/USER/go/bin:/c/Program Files/Go/bin:/c/Program Files/dotnet"
for d in /c/Users/USER/AppData/Local/Microsoft/WinGet/Packages/ezwinports.make_*/bin; do
  [ -d "$d" ] && PATH="$PATH:$d"
done

SERVICES=(ad matching negotiation swap)
declare -A PORT=([ad]=9001 [matching]=9002 [negotiation]=9003 [swap]=9004)
declare -A KIND=([ad]=go [matching]=go [negotiation]=dotnet [swap]=dotnet)
WAIT_SECS="${WAIT_SECS:-60}"
PAYMENT_DEADLINE="${PAYMENT_DEADLINE:-2m}"
KAFKA_BROKERS="${KAFKA_BROKERS:-localhost:9094}"
DB_BASE="${DB_BASE:-postgres://taakht:taakht@localhost:5432}"

warn() { echo "WARN: $*" >&2; }
die() {
  echo "ERROR: $*" >&2
  exit 1
}

# .NET host project (Microsoft.NET.Sdk.Web) of a service, or empty.
dotnet_proj() {
  local f
  for f in "src/$1"/*/*.csproj; do
    if [ -f "$f" ] && grep -q 'Microsoft.NET.Sdk.Web' "$f"; then
      echo "$f"
      return
    fi
  done
}

exists() {
  case "${KIND[$1]}" in
    go) [ -f "src/$1/go.mod" ] && [ -f "src/$1/main.go" ] ;;
    dotnet) [ -n "$(dotnet_proj "$1")" ] ;;
  esac
}

port_open() { (exec 3<>"/dev/tcp/127.0.0.1/$1") 2>/dev/null; }

# Windows PIDs listening on a TCP port.
listeners() {
  netstat -ano -p tcp 2>/dev/null | tr -d '\r' |
    awk -v p=":$1" '$1=="TCP" && $4=="LISTENING" && substr($2, length($2)-length(p)+1)==p {print $5}' | sort -u
}

pidfile() { echo "$PIDS/$1.pid"; }
svc_pid() { [ -f "$(pidfile "$1")" ] && cat "$(pidfile "$1")"; }
pid_alive() { [ -n "${1:-}" ] && tasklist //FI "PID eq $1" //NH 2>/dev/null | grep -q " $1 "; }
kill_tree() { taskkill //PID "$1" //T //F >/dev/null 2>&1; }

ensure_infra() {
  echo "==> infrastructure (make up)"
  if ! make up >"$LOGS/infra.log" 2>&1; then
    tail -n 20 "$LOGS/infra.log"
    die "make up failed (is Docker running?). Log: .run/logs/infra.log"
  fi
  echo "    postgres + kafka healthy"
}

build_svc() {
  local s="$1" proj
  echo "    building $s"
  case "${KIND[$s]}" in
    go) (cd "src/$s" && go build -o "$BIN/$s.exe" .) ;;
    dotnet)
      proj="$(dotnet_proj "$s")"
      dotnet build "$proj" -c Debug -o "$BIN/$s" --nologo -v q -nodeReuse:false -p:UseSharedCompilation=false </dev/null
      ;;
  esac
}

run_svc() {
  local s="$1" p="${PORT[$1]}" log="$LOGS/$1.log" proj dll wp
  : >"$log"
  export DATABASE_URL="${DB_BASE}/$s?sslmode=disable" KAFKA_BROKERS GRPC_ADDR=":$p" AD_ADDR="${AD_ADDR:-localhost:9001}"
  export ELIGIBILITY_FILE="${ELIGIBILITY_FILE:-$WROOT/config/eligibility.json}"
  export PAYMENT_DEADLINE TAAKHT_GRPC_REFLECTION="${TAAKHT_GRPC_REFLECTION:-on}"
  export ConnectionStrings__Default="${DB_BASE}/$s?sslmode=disable"
  export ASPNETCORE_ENVIRONMENT=Development DOTNET_ENVIRONMENT=Development
  local wdir cmdline
  case "${KIND[$s]}" in
    go)
      wdir="$(cygpath -w "$ROOT/src/$s")"
      cmdline="\"$(cygpath -w "$BIN/$s.exe")\""
      ;;
    dotnet)
      proj="$(dotnet_proj "$s")"
      dll="$BIN/$s/$(basename "${proj%.csproj}").dll"
      wdir="$(cygpath -w "$ROOT/$(dirname "$proj")")"
      cmdline="dotnet \"$(cygpath -w "$dll")\""
      ;;
  esac
  # A small .cmd wrapper does the log redirection. Start-Process detaches the child from our stdout pipe
  # (a plain `&` job keeps `make dev | tail` waiting forever on Windows); taskkill /T later kills the tree.
  printf '@%s > "%s" 2>&1\r\n' "$cmdline" "$(cygpath -w "$log")" >"$BIN/$s.cmd"
  wp="$(powershell -NoProfile -Command "(Start-Process -FilePath cmd.exe -WorkingDirectory '$wdir' -WindowStyle Hidden -PassThru -ArgumentList '/c', '$(cygpath -w "$BIN/$s.cmd")').Id" | tr -d '\r')"
  echo "$wp" >"$(pidfile "$s")"
}

wait_ready() {
  local s="$1" p="${PORT[$1]}" i pid
  pid="$(svc_pid "$s")"
  for ((i = 0; i < WAIT_SECS * 2; i++)); do
    if port_open "$p"; then
      echo "    $s ready on :$p (pid $pid)"
      return 0
    fi
    if ! pid_alive "$pid"; then
      echo "ERROR: $s exited during startup. Last log lines:" >&2
      tail -n 25 "$LOGS/$s.log" >&2
      return 1
    fi
    sleep 0.5
  done
  echo "ERROR: $s did not open :$p within ${WAIT_SECS}s. Last log lines:" >&2
  tail -n 25 "$LOGS/$s.log" >&2
  return 1
}

stop_svc() {
  local s="$1" p="${PORT[$1]}" pid l i
  pid="$(svc_pid "$s")"
  if [ -n "$pid" ] && pid_alive "$pid"; then kill_tree "$pid"; fi
  for l in $(listeners "$p"); do kill_tree "$l"; done # strays not tracked by a pid file
  rm -f "$(pidfile "$s")"
  for i in 1 2 3 4 5 6 7 8 9 10; do
    if [ -z "$(listeners "$p")" ]; then
      echo "    $s stopped (:$p free)"
      return 0
    fi
    sleep 0.5
  done
  warn "$s: port $p is still in use"
  return 1
}

# Returns 0 started/already running, 2 skipped (no project), 1 failure.
start_one() {
  local s="$1" pid
  if ! exists "$s"; then
    warn "src/$s has no project yet, skipping"
    return 2
  fi
  if [ -n "$(listeners "${PORT[$s]}")" ]; then
    pid="$(svc_pid "$s")"
    if [ -n "$pid" ] && pid_alive "$pid"; then
      echo "    $s already running (pid $pid)"
      return 0
    fi
    warn "$s: port ${PORT[$s]} is busy with a process not started by dev.sh; run '$0 stop' or free it"
    return 1
  fi
  if ! build_svc "$s"; then
    warn "build of $s failed"
    return 1
  fi
  run_svc "$s"
}

cmd_start() {
  local only=("$@") s rc failed=0 started=()
  [ ${#only[@]} -eq 0 ] && only=("${SERVICES[@]}")
  command -v make >/dev/null || die "make not on PATH (see docs/guidelines/running-locally.md)"
  ensure_infra
  echo "==> building and starting services"
  for s in "${only[@]}"; do
    start_one "$s"
    rc=$?
    case $rc in 0) started+=("$s") ;; 2) ;; *) failed=1 ;; esac
  done
  echo "==> waiting for gRPC ports"
  for s in "${started[@]}"; do wait_ready "$s" || failed=1; done
  echo
  cmd_status
  [ $failed -eq 0 ] || die "some services failed to start (logs: .run/logs/<svc>.log)"
}

cmd_stop() {
  local s rc=0
  echo "==> stopping services"
  for s in "${SERVICES[@]}"; do stop_svc "$s" || rc=1; done
  return $rc
}

cmd_status() {
  printf '%-12s %-6s %-9s %s\n' SERVICE PORT STATE PID
  local s pid state
  for s in "${SERVICES[@]}"; do
    pid="$(svc_pid "$s")"
    if ! exists "$s"; then
      state="missing"
    elif port_open "${PORT[$s]}"; then
      state="up"
    elif [ -n "$pid" ] && pid_alive "$pid"; then
      state="starting"
    else
      state="down"
    fi
    printf '%-12s %-6s %-9s %s\n' "$s" "${PORT[$s]}" "$state" "${pid:--}"
  done
}

valid_svc() { [ -n "${KIND[${1:-}]:-}" ] || die "unknown service '${1:-}' (one of: ${SERVICES[*]})"; }

case "${1:-}" in
  start)
    shift
    for a in "$@"; do valid_svc "$a"; done
    cmd_start "$@"
    ;;
  stop) cmd_stop ;;
  status) cmd_status ;;
  logs)
    valid_svc "${2:-}"
    [ -f "$LOGS/$2.log" ] || die "no log yet: .run/logs/$2.log"
    tail -n "${LINES:-100}" -f "$LOGS/$2.log"
    ;;
  restart)
    if [ -n "${2:-}" ]; then
      valid_svc "$2"
      stop_svc "$2"
      cmd_start "$2"
    else
      cmd_stop
      cmd_start
    fi
    ;;
  *)
    echo "usage: $0 start [svc...] | stop | status | logs <svc> | restart [svc]   (services: ${SERVICES[*]})"
    exit 1
    ;;
esac
