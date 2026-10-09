// Package reload keeps a configuration file's parsed value fresh without a restart or a signal.
//
// A Reloadable holds the value loaded at startup. Check (called by Run every interval) compares the
// file's modification time and size with the last attempt and, when they changed, loads the file again.
// A load that fails (unreadable, unparsable, rejected by the loader's validation) keeps the previous value
// and is logged at error level once per file change; the next change is tried again. Get is safe for
// concurrent use and never returns nil.
package reload

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// DefaultInterval is how often Run looks at the file.
const DefaultInterval = 30 * time.Second

// Reloadable is an atomically swapped, file-backed value.
type Reloadable[T any] struct {
	path string
	load func(path string) (*T, error)
	log  *slog.Logger
	cur  atomic.Pointer[T]

	mu    sync.Mutex // serializes Check
	mtime time.Time
	size  int64
}

// New loads the file once; a failure here is returned (startup must not continue without a valid config).
func New[T any](path string, load func(path string) (*T, error), log *slog.Logger) (*Reloadable[T], error) {
	if log == nil {
		log = slog.Default()
	}
	v, err := load(path)
	if err != nil {
		return nil, err
	}
	r := &Reloadable[T]{path: path, load: load, log: log}
	r.cur.Store(v)
	if fi, err := os.Stat(path); err == nil {
		r.mtime, r.size = fi.ModTime(), fi.Size()
	}
	return r, nil
}

// Static wraps a fixed value (tests, or callers that do not want reloading).
func Static[T any](v *T) *Reloadable[T] {
	r := &Reloadable[T]{log: slog.Default()}
	r.cur.Store(v)
	return r
}

// Get returns the current value.
func (r *Reloadable[T]) Get() *T { return r.cur.Load() }

// Check reloads the file if its modification time or size changed since the last attempt. It reports
// whether a new value was installed; a failed load returns the error and keeps the old value.
func (r *Reloadable[T]) Check() (bool, error) {
	if r.path == "" {
		return false, nil
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	fi, err := os.Stat(r.path)
	if err != nil {
		// A missing file (an editor replacing it) is retried at the next change of state, not an error spam.
		if os.IsNotExist(err) {
			return false, nil
		}
		return false, fmt.Errorf("reload %s: %w", r.path, err)
	}
	if fi.ModTime().Equal(r.mtime) && fi.Size() == r.size {
		return false, nil
	}
	r.mtime, r.size = fi.ModTime(), fi.Size()
	v, err := r.load(r.path)
	if err != nil {
		return false, fmt.Errorf("reload %s (keeping the previous configuration): %w", r.path, err)
	}
	r.cur.Store(v)
	return true, nil
}

// Run checks the file every interval (DefaultInterval when zero) until ctx is cancelled.
func (r *Reloadable[T]) Run(ctx context.Context, interval time.Duration) {
	if interval <= 0 {
		interval = DefaultInterval
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		changed, err := r.Check()
		switch {
		case err != nil:
			r.log.Error("configuration reload failed", "path", r.path, "err", err)
		case changed:
			r.log.Info("configuration reloaded", "path", r.path)
		}
	}
}
