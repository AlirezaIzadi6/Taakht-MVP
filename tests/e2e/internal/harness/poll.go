package harness

import (
	"fmt"
	"strings"
	"time"
)

// TB is the subset of testing.TB the harness needs; the demo program implements it too.
type TB interface {
	Helper()
	Fatalf(format string, args ...any)
	Logf(format string, args ...any)
	Cleanup(func())
}

// DefaultTimeout is how long asynchronous (Kafka-driven) effects may take.
const DefaultTimeout = 20 * time.Second

const pollInterval = 200 * time.Millisecond

// Poll calls check until it reports done or the timeout expires. check returns a short description of
// the current state, which is included in the failure message so a timeout says what was actually seen.
func Poll(t TB, what string, timeout time.Duration, check func() (done bool, state string)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var state string
	for {
		var done bool
		done, state = check()
		if done {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s; last seen: %s", timeout, what, state)
		}
		time.Sleep(pollInterval)
	}
}

// Require fails the test with a formatted message when cond is false.
func Require(t TB, cond bool, format string, args ...any) {
	t.Helper()
	if !cond {
		t.Fatalf(format, args...)
	}
}

// NoErr fails the test when err is not nil.
func NoErr(t TB, err error, what string) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", what, err)
	}
}

func join[T fmt.Stringer](items []T) string {
	parts := make([]string, len(items))
	for i, it := range items {
		parts[i] = it.String()
	}
	return strings.Join(parts, ",")
}
