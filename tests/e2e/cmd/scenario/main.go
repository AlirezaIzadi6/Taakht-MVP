// Command scenario narrates the Taakht demo flow against the running services and exits non-zero on failure.
package main

import (
	"fmt"
	"os"
	"time"

	"github.com/taakht/taakht/tests/e2e/internal/harness"
)

// narrator implements harness.TB for a plain program: Fatalf aborts the run through a panic.
type narrator struct {
	cleanups []func()
}

type abort struct{ msg string }

func (n *narrator) Helper()                   {}
func (n *narrator) Logf(f string, a ...any)   { fmt.Printf("  "+f+"\n", a...) }
func (n *narrator) Cleanup(fn func())         { n.cleanups = append(n.cleanups, fn) }
func (n *narrator) Fatalf(f string, a ...any) { panic(abort{fmt.Sprintf(f, a...)}) }
func (n *narrator) runCleanups() {
	for i := len(n.cleanups) - 1; i >= 0; i-- {
		n.cleanups[i]()
	}
}

func main() {
	os.Exit(run())
}

func run() (code int) {
	c, err := harness.Dial(harness.AddrsFromEnv())
	if err != nil {
		fmt.Fprintln(os.Stderr, "FAILED:", err)
		return 1
	}
	defer c.Close()
	if down := c.Unreachable("ad", "matching", "negotiation", "swap"); len(down) > 0 {
		fmt.Fprintf(os.Stderr, "FAILED: services not reachable: %v\n", down)
		return 1
	}

	n := &narrator{}
	start := time.Now()
	defer func() {
		n.runCleanups()
		if r := recover(); r != nil {
			a, ok := r.(abort)
			if !ok {
				panic(r)
			}
			fmt.Fprintln(os.Stderr, "\nFAILED:", a.msg)
			code = 1
		}
	}()

	say := func(format string, args ...any) { fmt.Printf(format+"\n", args...) }
	say("Taakht demo scenario against ad=%s matching=%s negotiation=%s swap=%s\n",
		c.Addrs.Ad, c.Addrs.Matching, c.Addrs.Negotiation, c.Addrs.Swap)
	harness.HappyPath(n, c, say)
	say("\nScenario finished successfully in %s.", time.Since(start).Round(100*time.Millisecond))
	return 0
}
