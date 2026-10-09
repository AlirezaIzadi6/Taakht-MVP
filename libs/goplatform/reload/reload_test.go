package reload_test

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/taakht/taakht/libs/goplatform/reload"
)

type cfg struct{ Name string }

func load(path string) (*cfg, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	c := &cfg{}
	if err := json.Unmarshal(raw, c); err != nil {
		return nil, err
	}
	if c.Name == "" {
		return nil, errors.New("name is required")
	}
	return c, nil
}

func write(t *testing.T, path, body string, mtime time.Time) {
	t.Helper()
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chtimes(path, mtime, mtime); err != nil {
		t.Fatal(err)
	}
}

func TestReloadOnChangeKeepsOldOnFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.json")
	t0 := time.Now().Add(-time.Hour)
	write(t, path, `{"Name":"a"}`, t0)
	r, err := reload.New(path, load, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if changed, err := r.Check(); changed || err != nil {
		t.Fatalf("unchanged file: %v %v", changed, err)
	}

	write(t, path, `{"Name":"b"}`, t0.Add(time.Minute))
	if changed, err := r.Check(); !changed || err != nil || r.Get().Name != "b" {
		t.Fatalf("change not applied: %v %v %v", changed, err, r.Get())
	}

	// Broken JSON, then a validation failure: the old value stays, and each bad version is reported once.
	write(t, path, `{nope`, t0.Add(2*time.Minute))
	if _, err := r.Check(); err == nil || r.Get().Name != "b" {
		t.Fatalf("broken file: err=%v cur=%v", err, r.Get())
	}
	if changed, err := r.Check(); changed || err != nil {
		t.Fatalf("same broken version reported again: %v %v", changed, err)
	}
	write(t, path, `{"Name":""}`, t0.Add(3*time.Minute))
	if _, err := r.Check(); err == nil || r.Get().Name != "b" {
		t.Fatalf("invalid file: err=%v cur=%v", err, r.Get())
	}

	// Recovery.
	write(t, path, `{"Name":"c"}`, t0.Add(4*time.Minute))
	if changed, err := r.Check(); !changed || err != nil || r.Get().Name != "c" {
		t.Fatalf("recovery: %v %v %v", changed, err, r.Get())
	}

	// A file that vanished is not an error and keeps the value.
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if changed, err := r.Check(); changed || err != nil || r.Get().Name != "c" {
		t.Fatalf("missing file: %v %v %v", changed, err, r.Get())
	}
}

func TestNewFailsOnInvalidFile(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.json")
	write(t, path, `{}`, time.Now())
	if _, err := reload.New(path, load, nil); err == nil {
		t.Fatal("invalid startup config accepted")
	}
}

func TestRunPicksUpChangesAndConcurrentGet(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.json")
	t0 := time.Now().Add(-time.Hour)
	write(t, path, `{"Name":"a"}`, t0)
	r, err := reload.New(path, load, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); r.Run(ctx, 10*time.Millisecond) }()
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				if r.Get() == nil {
					t.Error("nil config")
					return
				}
			}
		}()
	}
	write(t, path, `{"Name":"z"}`, t0.Add(time.Minute))
	deadline := time.Now().Add(5 * time.Second)
	for r.Get().Name != "z" && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	wg.Wait()
	if r.Get().Name != "z" {
		t.Fatal("Run did not reload")
	}
}

func TestStatic(t *testing.T) {
	r := reload.Static(&cfg{Name: "s"})
	if changed, err := r.Check(); changed || err != nil || r.Get().Name != "s" {
		t.Fatal("static")
	}
}
