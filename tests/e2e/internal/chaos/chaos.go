// Package chaos injects faults into the locally running stack: it stops and starts the four services
// (through scripts/dev.sh and taskkill), stops and starts the Kafka and Postgres containers
// (docker compose), waits for ports and runs SQL against the service databases for verification.
//
// It is meant for Windows + Git Bash, like the rest of the local tooling.
package chaos

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// Ports of the gRPC services, as in scripts/dev.sh.
var ports = map[string]int{"ad": 9001, "matching": 9002, "negotiation": 9003, "swap": 9004}

// TB is the part of testing.TB the helpers use.
type TB interface {
	Helper()
	Fatalf(format string, args ...any)
	Logf(format string, args ...any)
}

// Root returns the repository root (the directory holding scripts/dev.sh).
func Root() (string, error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", err
	}
	for {
		if _, err := os.Stat(filepath.Join(dir, "scripts", "dev.sh")); err == nil {
			return dir, nil
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			return "", fmt.Errorf("scripts/dev.sh not found above the working directory")
		}
		dir = parent
	}
}

func mustRoot(t TB) string {
	t.Helper()
	r, err := Root()
	if err != nil {
		t.Fatalf("%v", err)
	}
	return r
}

func run(dir string, env []string, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(context.Background(), name, args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), env...)
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err := cmd.Run()
	return strings.TrimSpace(out.String()), err
}

// PortOpen reports whether something accepts TCP connections on addr.
func PortOpen(addr string) bool {
	d := net.Dialer{Timeout: time.Second}
	conn, err := d.DialContext(context.Background(), "tcp", addr)
	if err != nil {
		return false
	}
	_ = conn.Close()
	return true
}

// WaitPort waits until addr accepts (up=true) or refuses (up=false) connections.
func WaitPort(t TB, addr string, up bool, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for PortOpen(addr) != up {
		if time.Now().After(deadline) {
			t.Fatalf("%s did not become up=%v within %s", addr, up, timeout)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

// listeners returns the PIDs listening on a TCP port (netstat -ano).
func listeners(port int) []string {
	out, err := exec.CommandContext(context.Background(), "netstat", "-ano", "-p", "tcp").Output()
	if err != nil {
		return nil
	}
	suffix := ":" + strconv.Itoa(port)
	seen := map[string]bool{}
	var pids []string
	for _, line := range strings.Split(strings.ReplaceAll(string(out), "\r", ""), "\n") {
		f := strings.Fields(line)
		if len(f) >= 5 && f[0] == "TCP" && f[3] == "LISTENING" && strings.HasSuffix(f[1], suffix) && !seen[f[4]] {
			seen[f[4]] = true
			pids = append(pids, f[4])
		}
	}
	return pids
}

// LogPath returns the log file of a service written by scripts/dev.sh.
func LogPath(t TB, svc string) string {
	return filepath.Join(mustRoot(t), ".run", "logs", svc+".log")
}

// SnapshotLog copies the current service log to .run/chaos-logs/<time>-<svc>-<label>.log, because
// scripts/dev.sh truncates the log when the service is started again. It returns the copy's path.
func SnapshotLog(t TB, svc, label string) string {
	t.Helper()
	root := mustRoot(t)
	data, err := os.ReadFile(LogPath(t, svc))
	if err != nil {
		t.Logf("snapshot log %s: %v", svc, err)
		return ""
	}
	dir := filepath.Join(root, ".run", "chaos-logs")
	_ = os.MkdirAll(dir, 0o755)
	dst := filepath.Join(dir, fmt.Sprintf("%s-%s-%s.log", time.Now().Format("150405"), svc, label))
	if err := os.WriteFile(dst, data, 0o644); err != nil {
		t.Logf("snapshot log %s: %v", svc, err)
	}
	return dst
}

// StopService kills a service (process tree from its pid file plus whatever listens on its port, like
// scripts/dev.sh stop for one service). The process dies without leaving its Kafka consumer group, so the
// group takes up to the session timeout (about 45 s) to notice and rebalance. The log is saved first.
func StopService(t TB, svc string) {
	t.Helper()
	root := mustRoot(t)
	port, ok := ports[svc]
	if !ok {
		t.Fatalf("unknown service %q", svc)
	}
	pidFile := filepath.Join(root, ".run", "pids", svc+".pid")
	if b, err := os.ReadFile(pidFile); err == nil {
		if pid := strings.TrimSpace(string(b)); pid != "" {
			_ = exec.CommandContext(context.Background(), "taskkill", "/PID", pid, "/T", "/F").Run()
		}
	}
	for _, pid := range listeners(port) {
		_ = exec.CommandContext(context.Background(), "taskkill", "/PID", pid, "/T", "/F").Run()
	}
	_ = os.Remove(pidFile)
	WaitPort(t, "localhost:"+strconv.Itoa(port), false, 20*time.Second)
	SnapshotLog(t, svc, "stopped")
	t.Logf("chaos: %s stopped", svc)
}

// StartService starts a service through scripts/dev.sh start <svc> (it rebuilds the binary first and also runs
// make up, which is a no-op while the containers run) and waits for the gRPC port. env entries
// ("PAYMENT_DEADLINE=15s") are passed to the script and so to the service.
func StartService(t TB, svc string, env ...string) {
	t.Helper()
	root := mustRoot(t)
	port, ok := ports[svc]
	if !ok {
		t.Fatalf("unknown service %q", svc)
	}
	start := time.Now()
	out, err := run(root, env, "bash", "scripts/dev.sh", "start", svc)
	if err != nil {
		t.Fatalf("scripts/dev.sh start %s: %v\n%s", svc, err, out)
	}
	WaitPort(t, "localhost:"+strconv.Itoa(port), true, 60*time.Second)
	t.Logf("chaos: %s started in %s", svc, time.Since(start).Round(time.Second))
}

// RestartService stops and starts a service.
func RestartService(t TB, svc string, env ...string) {
	t.Helper()
	StopService(t, svc)
	StartService(t, svc, env...)
}

func compose(t TB, args ...string) string {
	t.Helper()
	root := mustRoot(t)
	out, err := run(root, nil, "docker", append([]string{"compose", "-f", "deploy/docker-compose.yml"}, args...)...)
	if err != nil {
		t.Fatalf("docker compose %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return out
}

// StopKafka stops the Kafka container (data stays inside the stopped container).
func StopKafka(t TB) {
	t.Helper()
	compose(t, "stop", "kafka")
	t.Logf("chaos: kafka stopped")
}

// StartKafka starts the container again and waits until the external listener (localhost:9094) answers.
func StartKafka(t TB) {
	t.Helper()
	compose(t, "start", "kafka")
	WaitPort(t, "127.0.0.1:9094", true, 90*time.Second)
	// The port opens before the broker is able to serve metadata; give it a moment.
	time.Sleep(5 * time.Second)
	t.Logf("chaos: kafka started")
}

// StopPostgres stops the Postgres container (the data volume is kept).
func StopPostgres(t TB) {
	t.Helper()
	compose(t, "stop", "postgres")
	t.Logf("chaos: postgres stopped")
}

// StartPostgres starts the container again and waits until pg_isready answers.
func StartPostgres(t TB) {
	t.Helper()
	compose(t, "start", "postgres")
	root := mustRoot(t)
	deadline := time.Now().Add(60 * time.Second)
	for {
		if _, err := run(root, nil, "docker", "compose", "-f", "deploy/docker-compose.yml", "exec", "-T", "postgres", "pg_isready", "-U", "taakht"); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("postgres not ready after 60s")
		}
		time.Sleep(time.Second)
	}
	t.Logf("chaos: postgres started")
}

// Psql runs one SQL statement against a service database (ad, matching, negotiation, swap) inside the
// Postgres container and returns the trimmed, unaligned output (rows separated by newlines, columns by |).
func Psql(t TB, db, sql string) string {
	t.Helper()
	out, err := PsqlErr(db, sql)
	if err != nil {
		t.Fatalf("psql %s %q: %v\n%s", db, sql, err, out)
	}
	return out
}

// PsqlErr is Psql returning the error instead of failing.
func PsqlErr(db, sql string) (string, error) {
	root, err := Root()
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", "compose", "-f", "deploy/docker-compose.yml", "exec", "-T", "postgres",
		"psql", "-U", "taakht", "-d", db, "-tA", "-c", sql)
	cmd.Dir = root
	var out bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &out
	err = cmd.Run()
	return strings.TrimSpace(out.String()), err
}

// PsqlInt runs a query returning one integer.
func PsqlInt(t TB, db, sql string) int {
	t.Helper()
	s := Psql(t, db, sql)
	n, err := strconv.Atoi(s)
	if err != nil {
		t.Fatalf("psql %s %q: expected an integer, got %q", db, sql, s)
	}
	return n
}

// LogLines returns the lines of the service log (current run) containing substr, at most max, newest last.
func LogLines(t TB, svc, substr string, max int) []string {
	b, err := os.ReadFile(LogPath(t, svc))
	if err != nil {
		return nil
	}
	var out []string
	for _, l := range strings.Split(strings.ReplaceAll(string(b), "\r", ""), "\n") {
		if strings.Contains(l, substr) {
			out = append(out, l)
		}
	}
	if len(out) > max {
		out = out[len(out)-max:]
	}
	return out
}

// Heal makes sure everything is up again: containers started, all four services running. It is safe to call
// from a cleanup even when nothing was broken.
func Heal(t TB, env ...string) {
	t.Helper()
	root := mustRoot(t)
	_, _ = run(root, nil, "docker", "compose", "-f", "deploy/docker-compose.yml", "start", "postgres", "kafka")
	for _, svc := range []string{"ad", "matching", "negotiation", "swap"} {
		if !PortOpen("localhost:" + strconv.Itoa(ports[svc])) {
			StartService(t, svc, env...)
		}
	}
}
